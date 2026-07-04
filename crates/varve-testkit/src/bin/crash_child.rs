//! Crash-test child (slice 3 harness): does K acked inserts against a
//! local-log `Db`, durably recording each ack, then arms the requested
//! crash point and lets the parent deliver `kill -9`.
//
// This is a test-support binary, not library code: unwrap/expect read
// better here than error plumbing would — the process is about to be
// killed anyway.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::io::Write as _;
use std::path::PathBuf;

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

    // VARVE_CRASH_TRIGGER is set by the parent (the test process) before
    // spawning us; `crash_point()` inside `LocalLog::append` reads it on
    // every call and stays inert until the trigger file's content matches
    // the armed point.
    let db = varve::Db::local(work.join("log")).await.expect("open db");
    let acked_path = work.join("acked.txt");

    for i in 1..=k {
        db.execute(&format!("INSERT (:Crash {{_id: {i}, seq: {i}}})"))
            .await
            .expect("acked insert");
        append_acked(&acked_path, i);
    }

    match point.as_str() {
        "none" => {} // clean run: exit 0
        "post-ack" => park_for_kill("post-ack"),
        p @ ("pre-append" | "post-append") => {
            std::fs::write(work.join("trigger"), p).expect("arm trigger");
            // This insert should hit the armed hook inside LocalLog::append
            // and park there; the parent kills us once it observes the
            // CRASH_POINT marker on stdout. Reaching the line after this
            // await means the hook failed to fire — a harness bug, not the
            // crash condition under test.
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
