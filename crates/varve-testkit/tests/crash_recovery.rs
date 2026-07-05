//! Slice 3/4 durability harness: spawn a child process doing acked writes
//! against a `Db` (local log + local block store, `max_block_rows = K`),
//! `kill -9` it at an injected fault point, then reopen and assert the
//! recovery contract (slice-3 decision 8; slice-4 decisions 5, 6, 7, 8, 10):
//!
//! - **pre-append** (killed before any byte of the in-flight (K+1)th tx hit
//!   the log): it NEVER surfaces; survivors == acked. The K-th ack already
//!   tripped a flush (trigger wasn't armed until after the K inserts), so
//!   the log is trimmed down to its structural minimum and a manifest
//!   exists (see the segment-count note below).
//! - **post-append** (durable but unacked — fsync completed, the ack never
//!   fired): the final tx MAY legally surface; acked ⊆ survivors.
//! - **post-ack**: killed at an arbitrary point inside the flush; recovery
//!   serves exactly the acked set regardless of how far the flush got.
//! - **pre-manifest-put** (killed after the data+meta PUTs but before the
//!   manifest PUT — THE atomic commit point): data/meta objects may exist
//!   as orphan garbage, but no manifest exists, so the block is invisible;
//!   recovery replays the intact, untrimmed log — clean, no corruption.
//! - **post-manifest-put** (killed after the manifest committed but before
//!   the trim ran): recovery sees the block AND the full untrimmed log;
//!   replay-from-watermark must not double-apply those records (pinned
//!   deterministically by `recover_skips_records_below_the_manifest_watermark`
//!   in `varve-engine`) — the matrix here pins the end-to-end contract.
//!
//! In every case the log must parse cleanly end-to-end (frames + protobuf +
//! Arrow payload) and the `Db` must reopen without error.
//!
//! Segment-count note: `blocks_config` sets `segment_max_bytes = 1` (the
//! same 1-byte budget `varve-log`'s own trim tests use), so every append
//! rolls a fresh segment first. `LocalLog::trim` never removes the
//! still-open active segment (`varve-log/tests/trim.rs`'s
//! `local_trim_never_touches_the_active_segment` pins this deliberately —
//! a fully-empty log after trim is unreachable by design), so a completed
//! trim leaves exactly ONE physical record on disk: whatever's in the tail
//! segment. Below, "the log is trimmed" therefore means `records == 1`, not
//! `records == 0`.
//
// Helper functions below are not themselves `#[test]`-annotated, so
// clippy's `allow-unwrap-in-tests`/`allow-expect-in-tests` (clippy.toml)
// doesn't cover them; the crate-level allow does.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::Duration;
use varve_log::{LocalLog, Log, DEFAULT_SEGMENT_MAX_BYTES};
use varve_types::LogPosition;

const K: u64 = 5;

fn iterations() -> usize {
    std::env::var("VARVE_CRASH_ITERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3)
}

fn spawn_child(work: &Path, point: &str) -> Child {
    Command::new(env!("CARGO_BIN_EXE_crash_child"))
        .arg(work)
        .arg(point)
        .arg(K.to_string())
        .env("VARVE_CRASH_TRIGGER", work.join("trigger"))
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn crash_child")
}

/// Waits for the child's `CRASH_POINT <point>` marker on stdout (30s
/// deadline), then delivers `kill -9` (SIGKILL on Unix — no destructors, no
/// flush). A dead/missing crash hook means the child never announces, so
/// this times out and fails the test — the guard against a vacuous pass.
fn wait_for_crash_then_kill(child: &mut Child, point: &str) {
    let stdout = child.stdout.take().expect("child stdout was piped");
    let expected = format!("CRASH_POINT {point}");
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            let line = line.expect("read child stdout");
            if line.starts_with("CRASH_POINT") {
                let _ = tx.send(line);
                return;
            }
        }
    });
    let line = rx
        .recv_timeout(Duration::from_secs(30))
        .expect("child never announced CRASH_POINT within 30s");
    assert_eq!(line, expected, "wrong crash point fired");
    child.kill().expect("SIGKILL the child"); // SIGKILL on Unix: no destructors, no flush
    child.wait().expect("reap killed child");
}

/// The harness's ground truth: every seq the child durably recorded as
/// acked before it was killed (or before it exited, for the clean run).
fn acked_seqs(work: &Path) -> Vec<u64> {
    let text = std::fs::read_to_string(work.join("acked.txt")).expect("read acked.txt");
    text.lines().map(|l| l.parse().unwrap()).collect()
}

/// Full log validation: every frame, every protobuf record, every Arrow
/// effect payload must decode ("log parses cleanly"). Returns record count.
///
/// The child/`surviving_seqs` open via `blocks_config`, whose `[log.local]
/// dir` points directly at `work/log` (unlike `Db::local`'s `dir/log`
/// nesting), so the segments live directly under `work/log`.
async fn parse_log(work: &Path) -> usize {
    let log = LocalLog::open(&work.join("log"), DEFAULT_SEGMENT_MAX_BYTES).unwrap();
    let records = log.tail(LogPosition::ZERO).await.unwrap();
    for (_, record) in &records {
        for effect in &record.effects {
            varve_index::decode_events(&effect.arrow_ipc).unwrap();
        }
    }
    records.len()
}

/// `dir/log` + `dir/store`, both `local`, `max_block_rows = k` — IDENTICAL
/// to the child's config, so reopening after a kill replays through the
/// same blocks+manifest machinery the child was exercising.
/// `segment_max_bytes = 1` forces one segment per append (see the
/// segment-count note in the module doc comment above).
fn blocks_config(work: &Path, k: u64) -> varve::Config {
    let log_dir = format!("{:?}", work.join("log").display().to_string());
    let store_dir = format!("{:?}", work.join("store").display().to_string());
    varve::Config::from_toml_str(&format!(
        "[log]\nbackend = \"local\"\ngroup_commit_window_ms = 1\n\
         [log.local]\ndir = {log_dir}\nsegment_max_bytes = 1\n\
         [storage]\nbackend = \"local\"\nmax_block_rows = {k}\n\
         [storage.local]\ndir = {store_dir}\n"
    ))
    .expect("parent config")
}

/// Reopens the log+store through `Db::open` (replay-on-open) and returns
/// the sorted `seq`s of every surviving `:Crash` node.
async fn surviving_seqs(work: &Path) -> Vec<u64> {
    use arrow::array::Int64Array; // workspace arrow == DataFusion's re-export (slice-1 pin invariant)
    let db = varve::Db::open(blocks_config(work, K)).await.unwrap();
    let batches = db
        .query("MATCH (c:Crash) RETURN c.seq AS seq")
        .await
        .unwrap();
    let mut seqs = Vec::new();
    for batch in &batches {
        let col: &Int64Array = batch
            .column_by_name("seq")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        for i in 0..col.len() {
            seqs.push(col.value(i) as u64);
        }
    }
    seqs.sort_unstable();
    seqs
}

/// The latest block manifest, if any, in `work/store`.
async fn manifest_in(work: &Path) -> Option<varve_storage::BlockManifest> {
    let store = varve_storage::local_store(&work.join("store")).unwrap();
    varve_storage::latest_manifest(store.as_ref())
        .await
        .unwrap()
}

#[tokio::test]
async fn clean_run_sanity() {
    let work = tempfile::tempdir().unwrap();
    let status = spawn_child(work.path(), "none").wait().unwrap();
    assert!(status.success());
    assert_eq!(acked_seqs(work.path()), (1..=K).collect::<Vec<_>>());
    assert_eq!(
        surviving_seqs(work.path()).await,
        (1..=K).collect::<Vec<_>>()
    );
    // The K-th ack tripped a block flush: manifest committed, log trimmed.
    let manifest = manifest_in(work.path())
        .await
        .expect("manifest after clean flush");
    assert_eq!(manifest.watermark, K);
    assert_eq!(
        manifest.tables[0]
            .tries
            .iter()
            .map(|t| t.row_count)
            .sum::<u64>(),
        K
    );
    // `LocalLog::trim` never removes the still-open active segment (see the
    // module doc comment's segment-count note), so exactly one record — the
    // K-th, already covered by the manifest watermark — survives on disk.
    assert_eq!(
        parse_log(work.path()).await,
        1,
        "log trimmed to its structural minimum after flush"
    );
}

#[tokio::test]
async fn crash_matrix() {
    for point in [
        "pre-append",
        "post-append",
        "post-ack",
        "pre-manifest-put",
        "post-manifest-put",
    ] {
        for _ in 0..iterations() {
            let work = tempfile::tempdir().unwrap();
            let mut child = spawn_child(work.path(), point);
            wait_for_crash_then_kill(&mut child, point);

            let acked = acked_seqs(work.path());
            let records = parse_log(work.path()).await;
            let survived = surviving_seqs(work.path()).await;
            let manifest = manifest_in(work.path()).await;

            // The fundamental contract, true at every fault point: nothing
            // acked is ever lost.
            for a in &acked {
                assert!(
                    survived.contains(a),
                    "acked seq {a} missing after crash at {point}"
                );
            }
            // All K acked inserts complete and are durably recorded before
            // any point arms — this holds regardless of which point fires.
            assert_eq!(acked, (1..=K).collect::<Vec<_>>(), "{point}");

            match point {
                "pre-append" => {
                    // Flush completed (trigger wasn't armed yet), THEN the
                    // in-flight (K+1)th died before any byte hit the log.
                    // Trim already ran, so the log is at its one-record
                    // structural minimum (see module doc comment).
                    assert_eq!(survived, acked, "{point}");
                    assert_eq!(records, 1, "{point}: log trimmed by the flush");
                    assert!(manifest.is_some(), "{point}");
                }
                "post-append" => {
                    // Durable but unacked: the K+1th MAY legally surface.
                    // The write always completes before this crash_point
                    // fires, so the K+1th record's own fresh segment is
                    // deterministically present alongside the prior flush's
                    // one-record structural minimum: exactly 2 on disk.
                    assert!(
                        survived.len() == acked.len() || survived.len() == acked.len() + 1,
                        "{point}"
                    );
                    assert_eq!(records, 2, "{point}");
                    assert!(manifest.is_some(), "{point}");
                }
                "post-ack" => {
                    // Killed at an arbitrary flush stage: whatever committed,
                    // recovery serves exactly the acked set.
                    assert_eq!(survived, acked, "{point}");
                    // `trim_sync` deletes the K-1 single-record segments one
                    // `fs::remove_file` at a time with no fsync between
                    // unlinks, so a SIGKILL mid-trim can leave the on-disk
                    // log at ANY record count between the untouched K and
                    // the fully-trimmed one-record minimum. Trim strictly
                    // follows the manifest PUT, so any trimming at all
                    // (records < K) implies a committed manifest, however
                    // far the trim itself got.
                    assert!(
                        (1..=K as usize).contains(&records),
                        "{point}: log record count {records} out of range 1..={K}"
                    );
                    if records < K as usize {
                        assert!(
                            manifest.is_some(),
                            "{point}: a trimmed log implies a committed manifest"
                        );
                    }
                }
                "pre-manifest-put" => {
                    // Data/meta orphans may exist, but no manifest: the
                    // block does not exist; recovery replays the intact log.
                    assert!(manifest.is_none(), "{point}: manifest must be absent");
                    assert_eq!(records, K as usize, "{point}: log untrimmed");
                    assert_eq!(survived, acked, "{point}");
                }
                "post-manifest-put" => {
                    // Manifest committed, trim never ran: recovery reads the
                    // block AND the full log — replay-from-watermark must
                    // not double-apply (pinned deterministically in Task 11).
                    let m = manifest.expect("manifest present");
                    assert_eq!(m.watermark, K, "{point}");
                    assert_eq!(records, K as usize, "{point}: log untrimmed");
                    assert_eq!(survived, acked, "{point}");
                }
                other => unreachable!("unknown point {other}"),
            }
        }
    }
}
