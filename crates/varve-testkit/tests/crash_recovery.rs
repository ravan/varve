//! Slice 3 durability harness: spawn a child process doing acked writes
//! against a local-log `Db`, `kill -9` it at an injected fault point, then
//! reopen and assert the recovery contract (design decision 8):
//!
//! - **pre-append** (killed before any byte hit the log): the in-flight tx
//!   NEVER surfaces; survivors == acked; the log has exactly K records.
//! - **post-append** (durable but unacked — fsync completed, the ack never
//!   fired): the final tx MAY legally surface; acked ⊆ survivors; the log
//!   has K or K+1 records.
//! - **post-ack**: survivors == acked == K.
//!
//! In every case the log must parse cleanly end-to-end (frames + protobuf +
//! Arrow payload) and `Db::local` must reopen.
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
/// flush).
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

/// Reopens the log through `Db::local` (replay-on-open) and returns the
/// sorted `seq`s of every surviving `:Crash` node.
async fn surviving_seqs(work: &Path) -> Vec<u64> {
    use arrow::array::Int64Array; // workspace arrow == DataFusion's re-export (slice-1 pin invariant)
    let db = varve::Db::local(work.join("log")).await.unwrap();
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
    assert_eq!(parse_log(work.path()).await, K as usize);
}

#[tokio::test]
async fn crash_matrix() {
    for point in ["pre-append", "post-append", "post-ack"] {
        for _ in 0..iterations() {
            let work = tempfile::tempdir().unwrap();
            let mut child = spawn_child(work.path(), point);
            wait_for_crash_then_kill(&mut child, point);

            let acked = acked_seqs(work.path());
            let records = parse_log(work.path()).await;
            let survived = surviving_seqs(work.path()).await;

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
                    // Killed before any byte of the in-flight (K+1)th tx hit
                    // the log: it NEVER surfaces.
                    assert_eq!(survived, acked, "{point}");
                    assert_eq!(records, K as usize, "{point}");
                }
                "post-append" => {
                    // Durable but unacked (fsync completed, the ack never
                    // fired): the final tx MAY legally surface either way.
                    assert!(
                        survived.len() == acked.len() || survived.len() == acked.len() + 1,
                        "{point}"
                    );
                    assert!(
                        records == K as usize || records == K as usize + 1,
                        "{point}"
                    );
                }
                "post-ack" => {
                    assert_eq!(survived, acked, "{point}");
                    assert_eq!(records, K as usize, "{point}");
                }
                _ => unreachable!(),
            }
        }
    }
}
