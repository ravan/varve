//! Chaos harness (Slice 10 Task 15). Roadmap exit criterion: "chaos test
//! (random writer kills under load, 30min) — no corruption, no acked loss".
//! Env-gated so `just check` never pays for it (same silent-skip precedent
//! as `backend_matrix.rs`'s `VARVE_S3_BACKENDS`): unset `VARVE_CHAOS_SECS`
//! means skip; `just chaos` runs 60s locally; the `chaos-nightly` CI job
//! runs 30min (`VARVE_CHAOS_SECS=1800`).
//!
//! A `chaos_writer` child (`src/bin/chaos_writer.rs`) inserts continuously
//! against `Db::local` (no `[coordinator]` section — decision 5: no
//! coordinator means the next life becomes the writer instantly, with no
//! lease to wait out) and prints `ACKED <n>` after every acknowledged
//! insert. This harness spawns it, waits for its `CHAOS_WRITER_READY` line,
//! sleeps a deterministic pseudo-random 200..1500ms, `kill -9`s it, and
//! restarts with a fresh `start_n` beyond the highest acked id so ids never
//! collide across lives. At the end it reopens the same directory and
//! proves every acked id is present and `verify()` is clean.
//
// Helper functions below are not themselves `#[test]`-annotated, so
// clippy's allow-unwrap-in-tests/allow-expect-in-tests (clippy.toml)
// doesn't cover them; the crate-level allow does (same rationale as
// crash_recovery.rs's module doc comment).
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeSet;
use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::time::{Duration, Instant};

/// Deterministic xorshift64 — reproducible kill timing for a fixed seed.
fn xorshift(seed: &mut u64) -> u64 {
    *seed ^= *seed << 13;
    *seed ^= *seed >> 7;
    *seed ^= *seed << 17;
    *seed
}

/// Real `kill -9 <pid>` (external command, not `Child::kill`): the brief
/// calls for the same signal-delivery path an operator would use.
fn kill_9(pid: u32) {
    let status = Command::new("kill")
        .args(["-9", &pid.to_string()])
        .status()
        .expect("run kill -9");
    assert!(status.success(), "kill -9 {pid} failed: {status:?}");
}

/// Spawns the reader thread on `child`'s stdout BEFORE the caller sleeps, so
/// the pipe never fills while a life is running. Forwards every line
/// (`CHAOS_WRITER_READY`, `ACKED <n>`, `ERR <n> <msg>`) verbatim over an
/// unbounded channel — the reader thread never blocks on a slow consumer.
fn spawn_reader(child: &mut Child) -> Receiver<String> {
    let stdout = child.stdout.take().expect("child stdout was piped");
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            let Ok(line) = line else { break };
            if tx.send(line).is_err() {
                break;
            }
        }
    });
    rx
}

fn collect_ids(batches: &[varve::RecordBatch]) -> BTreeSet<i64> {
    use arrow::array::Int64Array;
    let mut ids = BTreeSet::new();
    for batch in batches {
        let col: &Int64Array = batch
            .column_by_name("id")
            .expect("id column")
            .as_any()
            .downcast_ref()
            .expect("id is Int64");
        for i in 0..col.len() {
            ids.insert(col.value(i));
        }
    }
    ids
}

#[tokio::test]
async fn random_writer_kills_lose_no_acked_transactions() {
    let Some(secs) = std::env::var("VARVE_CHAOS_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
    else {
        eprintln!("VARVE_CHAOS_SECS unset; skipping chaos run");
        return;
    };

    let dir = tempfile::tempdir().unwrap();
    let deadline = Instant::now() + Duration::from_secs(secs);
    let mut acked: Vec<i64> = Vec::new();
    let mut next_start: i64 = 1;
    let mut seed: u64 = 0x5eed_cafe;
    let mut kills = 0u32;

    while Instant::now() < deadline {
        let mut child = Command::new(env!("CARGO_BIN_EXE_chaos_writer"))
            .arg(dir.path())
            .arg(next_start.to_string())
            .stdout(Stdio::piped())
            .spawn()
            .expect("spawn chaos_writer");

        // The reader thread MUST start before the sleep below so the pipe
        // never fills while the writer is racking up acks.
        let rx = spawn_reader(&mut child);

        // Wait for readiness before starting the kill countdown, so a slow
        // Db::local open/recover on a loaded box can never race a kill that
        // lands before the writer loop under test has even started.
        match rx.recv_timeout(Duration::from_secs(30)) {
            Ok(line) if line == "CHAOS_WRITER_READY" => {}
            other => panic!("chaos_writer never announced ready: {other:?}"),
        }

        let kill_after_ms = 200 + xorshift(&mut seed) % 1300; // 200..1500 ms
        std::thread::sleep(Duration::from_millis(kill_after_ms));

        kill_9(child.id());
        let status = child.wait().expect("reap killed child");
        assert!(
            !status.success(),
            "chaos_writer exited on its own instead of being killed: {status:?}"
        );
        kills += 1;

        // Drain every line the reader thread captured. Only ACKED lines
        // read from the pipe up to this point count as acked — an ACKED
        // line is only ever printed after the engine's own ack, so this is
        // conservative in the direction that matters: it can under-count
        // acked ids (tolerated by the assertion below, which only checks
        // membership of the ids it knows about) but never invents one.
        while let Ok(line) = rx.recv_timeout(Duration::from_millis(200)) {
            if let Some(n) = line.strip_prefix("ACKED ") {
                acked.push(n.parse().expect("ACKED line carries an integer"));
            } else if let Some(rest) = line.strip_prefix("ERR ") {
                panic!("chaos_writer reported an execute error before being killed: {rest}");
            }
        }

        // Ids never collide across lives.
        next_start = acked.last().copied().unwrap_or(0) + 1_000;
    }

    assert!(
        kills > 0,
        "VARVE_CHAOS_SECS too short to observe a single kill"
    );

    // Verdict: reopen; every acked id visible; verify() clean.
    let db = varve::Db::local(dir.path()).await.unwrap();
    let batches = db
        .query("MATCH (c:Chaos) RETURN c._id AS id")
        .await
        .unwrap();
    let present = collect_ids(&batches);
    for id in &acked {
        assert!(present.contains(id), "acked _id {id} lost after kill -9");
    }
    db.verify().await.unwrap();
    println!(
        "chaos: {kills} kills survived, {} acked txs all present",
        acked.len()
    );
}
