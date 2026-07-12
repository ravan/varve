//! Chaos harness writer child (Slice 10 Task 15): a continuous, acked
//! inserter against `Db::local` that the parent (`tests/chaos.rs`) kills
//! with SIGKILL at pseudo-random intervals and restarts. Args: `<dir>
//! <start_n>`. Opens `Db::local(dir)` — no `[coordinator]` section, so this
//! is designated-writer restart chaos (decision 5: no coordinator means the
//! next life becomes the writer instantly, unguarded by any lease).
//!
//! Stdout contract (line-buffered, every line flushed immediately so the
//! parent's reader thread never blocks on a partial line):
//!   `CHAOS_WRITER_READY`        — printed once, right after the db opens
//!   `ACKED <n>`                 — printed after every acknowledged insert
//!   `ERR <n> <message>`         — printed if an insert fails, then exit(1)
//!
//! Runs until killed; an execute error is NOT expected in normal operation
//! (the parent only ever stops this process via SIGKILL) — it prints ERR
//! and exits 1 so the parent can fail the test on an unexpected child exit.
//
// This is a test-support binary, not library code (same rationale as
// crash_child.rs): unwrap/expect on setup read better here than error
// plumbing, since the process is either killed or about to exit(1) anyway.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::io::Write as _;

#[tokio::main]
async fn main() {
    let mut args = std::env::args().skip(1);
    let dir = args.next().expect("usage: <dir> <start_n>");
    let start_n: i64 = args
        .next()
        .expect("usage: <dir> <start_n>")
        .parse()
        .expect("start_n is an integer");

    let db = varve::Db::local(&dir).await.expect("open db");

    println!("CHAOS_WRITER_READY");
    std::io::stdout().flush().expect("flush stdout");

    let mut n = start_n;
    loop {
        match db.execute(&format!("INSERT (:Chaos {{_id: {n}}})")).await {
            Ok(_) => {
                println!("ACKED {n}");
                std::io::stdout().flush().expect("flush stdout");
            }
            Err(e) => {
                println!("ERR {n} {e}");
                std::io::stdout().flush().expect("flush stdout");
                std::process::exit(1);
            }
        }
        n += 1;
    }
}
