//! Roadmap slice-6 exit-criteria perf smoke (task 11): full 10k-node/60k-edge
//! deterministic social graph (`varve_testkit::fixture::social_graph`) →
//! ingest via GQL → force-flush via config (`max_block_rows`, mirrors
//! `block_bench.rs`) → reopen → timed 2-hop friend-of-friend and
//! `-[:KNOWS]->{1,3}` expansion from anchor `_id 0`: cold once, then 100 warm
//! iterations (avg + p50). Exit criterion: warm 2-hop < 50 ms (record the
//! printed numbers in STATUS.md).
//! Run: cargo run --release --example traversal_bench -p varve

use std::path::Path;
use std::time::{Duration, Instant};
use varve::{Config, Db};
use varve_testkit::fixture::social_graph;

const PEOPLE: usize = 10_000;
const FRIENDSHIPS: usize = 60_000;
const SEED: u64 = 42;
const NODE_BATCH: usize = 1_000;
// Small enough that a handful of block flushes happen over the course of
// ingest (70k events / 20,000 ≈ 3-4 flushes), so the persisted path is
// genuinely exercised (mirrors block_bench.rs's MAX_BLOCK_ROWS rationale).
const MAX_BLOCK_ROWS: usize = 20_000;
const WARM_ITERS: usize = 100;
const ANCHOR: i64 = 0;

fn config(dir: &Path) -> Result<Config, Box<dyn std::error::Error>> {
    let log_dir = format!("{:?}", dir.join("log").display().to_string());
    let store_dir = format!("{:?}", dir.join("store").display().to_string());
    Ok(Config::from_toml_str(&format!(
        // group_commit_window_ms = 1 (mirrors tests/blocks.rs's blocks_config
        // helper): ingest here is a single sequential writer awaiting each
        // tx's ack in turn, so there is never a second commit in flight to
        // batch with — every default 15ms window would be paid in full, per
        // tx, for nothing. A tiny window keeps the "one tx per edge" v1
        // write path honest without that artificial latency floor.
        "[log]\nbackend = \"local\"\ngroup_commit_window_ms = 1\n\
         [log.local]\ndir = {log_dir}\n\
         [storage]\nbackend = \"local\"\nmax_block_rows = {MAX_BLOCK_ROWS}\n\
         [storage.local]\ndir = {store_dir}\n"
    ))?)
}

async fn two_hop(db: &Db) -> Result<usize, Box<dyn std::error::Error>> {
    let rows = db
        .query(&format!(
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) \
             WHERE a._id = {ANCHOR} RETURN c._id"
        ))
        .await?;
    Ok(rows.iter().map(|b| b.num_rows()).sum())
}

async fn quantified_1_3(db: &Db) -> Result<usize, Box<dyn std::error::Error>> {
    let rows = db
        .query(&format!(
            "MATCH (a:Person)-[:KNOWS]->{{1,3}}(b:Person) WHERE a._id = {ANCHOR} RETURN b._id"
        ))
        .await?;
    Ok(rows.iter().map(|b| b.num_rows()).sum())
}

fn p50(mut xs: Vec<Duration>) -> Duration {
    xs.sort_unstable();
    xs[xs.len() / 2]
}

fn avg(xs: &[Duration]) -> Duration {
    xs.iter().sum::<Duration>() / xs.len() as u32
}

/// Times `WARM_ITERS` back-to-back calls to `f`, asserting every call
/// returns the same row count as `expect` (traversal answers over a static
/// graph must be stable across repeated warm reads).
async fn warm_timings<F, Fut>(
    f: F,
    expect: usize,
    label: &str,
) -> Result<Vec<Duration>, Box<dyn std::error::Error>>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<usize, Box<dyn std::error::Error>>>,
{
    let mut times = Vec::with_capacity(WARM_ITERS);
    for _ in 0..WARM_ITERS {
        let t0 = Instant::now();
        let rows = f().await?;
        times.push(t0.elapsed());
        assert_eq!(rows, expect, "{label}: row count stable across warm iters");
    }
    Ok(times)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let g = social_graph(PEOPLE, FRIENDSHIPS, SEED);
    let node_stmts = g.node_statements(NODE_BATCH);
    let edge_stmts = g.edge_statements();
    let total_stmts = node_stmts.len() + edge_stmts.len();

    // Phase 1: ingest the full fixture (timed), then drop — acked txs are
    // durable (log) or flushed (blocks).
    let ingest_started = Instant::now();
    {
        let db = Db::open(config(dir.path())?).await?;
        for stmt in &node_stmts {
            db.execute(stmt).await?;
        }
        for stmt in &edge_stmts {
            db.execute(stmt).await?;
        }
    }
    let ingest = ingest_started.elapsed();
    let tx_per_sec = total_stmts as f64 / ingest.as_secs_f64();
    println!(
        "ingest {total_stmts} stmts ({} nodes, {} edges) in {ingest:.2?} ({tx_per_sec:.0} tx/s)",
        g.people,
        g.edges.len()
    );

    // Phase 2: restart = latest manifest + log tail replay.
    let reopen_started = Instant::now();
    let db = Db::open(config(dir.path())?).await?;
    println!(
        "reopen (manifest + log tail): {:.2?}",
        reopen_started.elapsed()
    );

    // Phase 3: 2-hop friend-of-friend — cold once, then warm.
    let cold_started = Instant::now();
    let two_hop_rows = two_hop(&db).await?;
    let two_hop_cold = cold_started.elapsed();
    let two_hop_warm = warm_timings(|| two_hop(&db), two_hop_rows, "2-hop").await?;
    let two_hop_warm_avg = avg(&two_hop_warm);
    let two_hop_warm_p50 = p50(two_hop_warm);

    // Phase 4: -[:KNOWS]->{1,3} — same shape.
    let cold_started = Instant::now();
    let q13_rows = quantified_1_3(&db).await?;
    let q13_cold = cold_started.elapsed();
    let q13_warm = warm_timings(|| quantified_1_3(&db), q13_rows, "{1,3}").await?;
    let q13_warm_avg = avg(&q13_warm);
    let q13_warm_p50 = p50(q13_warm);

    println!(
        "ingest {:.2}s · {tx_per_sec:.0} tx/s · \
         2-hop ({two_hop_rows} rows) cold {two_hop_cold:.2?} warm avg {two_hop_warm_avg:.2?} p50 {two_hop_warm_p50:.2?} · \
         {{1,3}} ({q13_rows} rows) cold {q13_cold:.2?} warm avg {q13_warm_avg:.2?} p50 {q13_warm_p50:.2?}",
        ingest.as_secs_f64(),
    );
    println!(
        "exit criterion (warm 2-hop < 50ms): {}",
        if two_hop_warm_avg.as_millis() < 50 {
            "PASS"
        } else {
            "FAIL"
        }
    );
    Ok(())
}
