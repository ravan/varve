//! Crash-test child (slice 3 harness, extended in slice 4 Task 13): does K
//! acked inserts against a `Db` opened with local log + local block store
//! (`max_block_rows = K`), so the K-th ack trips a real block flush every
//! run — durably recording each ack, then arms the requested crash point and
//! lets the parent deliver `kill -9`.
//
// This is a test-support binary, not library code: unwrap/expect read
// better here than error plumbing would — the process is about to be
// killed anyway.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::io::Write as _;
use std::path::{Path, PathBuf};

fn append_acked(path: &PathBuf, seq: u64) {
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .expect("open acked file");
    writeln!(file, "{seq}").expect("record ack");
    // The acked file is the harness's ground truth — it must not itself
    // lose acked lines to the kill.
    file.sync_all().expect("fsync acked file");
}

fn park_for_kill(point: &str) -> ! {
    println!("CRASH_POINT {point}");
    std::io::stdout().flush().expect("flush stdout");
    loop {
        std::thread::sleep(std::time::Duration::from_secs(3600));
    }
}

/// `dir/log` + `dir/store`, both `local` (decision 11), with the block
/// flush trigger set to exactly `k` rows so the K-th acked insert always
/// trips a flush — every matrix point now exercises the blocks+manifest
/// machinery, not just the log. `segment_max_bytes = 1` is the same 1-byte
/// budget `varve-log`'s own trim tests use: every append rolls a fresh
/// segment first, so a completed trim leaves exactly one physical record on
/// disk (`LocalLog::trim` never removes the still-open active segment — see
/// `local_trim_never_touches_the_active_segment` — so a literal empty log
/// is unreachable by design; the harness checks for that one-record
/// structural minimum instead).
fn blocks_config(work: &Path, k: u64) -> varve::Config {
    let log_dir = format!("{:?}", work.join("log").display().to_string());
    let store_dir = format!("{:?}", work.join("store").display().to_string());
    varve::Config::from_toml_str(&format!(
        "[log]\nbackend = \"local\"\ngroup_commit_window_ms = 1\n\
         [log.local]\ndir = {log_dir}\nsegment_max_bytes = 1\n\
         [storage]\nbackend = \"local\"\nmax_block_rows = {k}\n\
         [storage.local]\ndir = {store_dir}\n"
    ))
    .expect("child config")
}

/// The flush runs asynchronously after the K-th ack; "none" runs must not
/// exit before the manifest lands (else the parent sees a clean run with no
/// flush at all).
fn wait_for_manifest_file(work: &Path) {
    let blocks = work.join("store").join("v1").join("blocks");
    for _ in 0..200 {
        if blocks
            .read_dir()
            .map(|mut d| d.next().is_some())
            .unwrap_or(false)
        {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(25));
    }
    panic!("child: flush produced no manifest within 5s");
}

#[tokio::main]
async fn main() {
    let mut args = std::env::args().skip(1);
    let work: PathBuf = args.next().expect("usage: <work> <point> <k>").into();
    let point = args.next().expect("crash point");
    let k: u64 = args
        .next()
        .expect("acked count")
        .parse()
        .expect("acked count is a number");

    // The flush points fire inside the K-th ack's block flush, so they must
    // be armed BEFORE any insert; the append points arm after the K acked
    // inserts (as in slice 3) since they fire on the (K+1)th, in-flight tx.
    if matches!(point.as_str(), "pre-manifest-put" | "post-manifest-put") {
        std::fs::write(work.join("trigger"), &point).expect("arm trigger");
    }

    // VARVE_CRASH_TRIGGER is set by the parent (the test process) before
    // spawning us; the `crash_point()` hooks inside `LocalLog::append` and
    // `flush_block` read it on every call and stay inert until the trigger
    // file's content matches the armed point.
    let db = varve::Db::open(blocks_config(&work, k))
        .await
        .expect("open db");
    let acked_path = work.join("acked.txt");

    for i in 1..=k {
        db.execute(&format!("INSERT (:Crash {{_id: {i}, seq: {i}}})"))
            .await
            .expect("acked insert");
        append_acked(&acked_path, i);
    }

    match point.as_str() {
        "none" => {
            wait_for_manifest_file(&work); // let the flush commit + trim
        }
        "post-ack" => park_for_kill("post-ack"),
        // The writer task's crash_point announces and parks inside
        // flush_block; main just waits for the parent's SIGKILL.
        "pre-manifest-put" | "post-manifest-put" => loop {
            std::thread::sleep(std::time::Duration::from_secs(3600));
        },
        p @ ("pre-append" | "post-append") => {
            std::fs::write(work.join("trigger"), p).expect("arm trigger");
            // The (K+1)th insert should hit the armed hook inside
            // LocalLog::append and park there; the parent kills us once it
            // observes the CRASH_POINT marker on stdout. Reaching the line
            // after this await means the hook failed to fire — a harness
            // bug, not the crash condition under test.
            let n = k + 1;
            let _ = db
                .execute(&format!("INSERT (:Crash {{_id: {n}, seq: {n}}})"))
                .await;
            eprintln!("crash point {p} never fired");
            std::process::exit(2);
        }
        other => {
            eprintln!("unknown crash point '{other}'");
            std::process::exit(2);
        }
    }
}
