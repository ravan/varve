//! Roadmap slice-11 ship-task 8: end-to-end social-workload benchmark. Same
//! deterministic 10k-node/60k-edge fixture as `traversal_bench.rs`
//! (`varve_testkit::fixture::social_graph`), on the same durable local
//! profile (log + block store, flush via `max_block_rows`), but produces the
//! report's core table (roadmap doc §13 "write ops/s" unit): batched ingest
//! rate in both events/s and tx/s, warm point read, warm 2-hop, and the
//! AS-OF-historical/current-time 2-hop ratio (spec target: <= 2x).
//!
//! The AS-OF section has two anchors:
//! - `mid_receipt`: the receipt of the LAST node-INSERT statement. At that
//!   instant zero edges exist, so the 2-hop AS-OF query returns 0 rows —
//!   timed and reported, but not the headline number.
//! - `late_receipt`: the receipt of the edge program at roughly the
//!   midpoint of the edge-ingest loop, by which point the anchor node has
//!   picked up some out-edges, so the 2-hop AS-OF query returns a non-empty
//!   historical result. This is the headline AS-OF number.
//!
//! Run: cargo run --release --example social_bench -p varve

use std::path::Path;
use std::time::{Duration, Instant};

use varve::{Config, Db, TxReceipt};
use varve_testkit::fixture::{social_graph, EDGE_PROGRAM_BATCH};

const PEOPLE: usize = 10_000;
const FRIENDSHIPS: usize = 60_000;
const SEED: u64 = 42;
const NODE_BATCH: usize = 1_000;
// Mirrors traversal_bench.rs: small enough that a handful of block flushes
// happen over the course of ingest, so the persisted path is genuinely
// exercised by the warm reads below (which run against a reopened Db).
const MAX_BLOCK_ROWS: usize = 20_000;
const WARM_ITERS: usize = 100;
// Friend-of-friend anchor for the 2-hop query — same node as
// traversal_bench.rs's ANCHOR.
const ANCHOR: i64 = 0;
// Point-read target: an arbitrary mid-range `_id` in the fixture's dense
// 0..10_000 id space.
const POINT_ID: i64 = 4242;

fn config(dir: &Path) -> Result<Config, Box<dyn std::error::Error>> {
    let log_dir = format!("{:?}", dir.join("log").display().to_string());
    let store_dir = format!("{:?}", dir.join("store").display().to_string());
    Ok(Config::from_toml_str(&format!(
        // group_commit_window_ms = 1 (mirrors traversal_bench.rs /
        // tests/blocks.rs's blocks_config helper): ingest here is a single
        // sequential writer awaiting each program's ack in turn, so there is
        // never a second commit in flight to batch with.
        "[log]\nbackend = \"local\"\ngroup_commit_window_ms = 1\n\
         [log.local]\ndir = {log_dir}\n\
         [storage]\nbackend = \"local\"\nmax_block_rows = {MAX_BLOCK_ROWS}\n\
         [storage.local]\ndir = {store_dir}\n"
    ))?)
}

async fn point_read(db: &Db) -> Result<usize, Box<dyn std::error::Error>> {
    let rows = db
        .query(format!(
            "MATCH (p:Person {{_id: {POINT_ID}}}) RETURN p.name"
        ))
        .await?;
    Ok(rows.iter().map(|b| b.num_rows()).sum())
}

/// Verbatim (module-local) copy of `traversal_bench.rs`'s `two_hop` query —
/// same anchor, same shape — so the two benches' 2-hop numbers are directly
/// comparable.
async fn two_hop(db: &Db) -> Result<usize, Box<dyn std::error::Error>> {
    let rows = db
        .query(format!(
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) \
             WHERE a._id = {ANCHOR} RETURN c._id"
        ))
        .await?;
    Ok(rows.iter().map(|b| b.num_rows()).sum())
}

/// The same 2-hop query, wrapped in `FOR SYSTEM_TIME AS OF TIMESTAMP`
/// against a historical anchor (RFC3339-microseconds `Display` of a
/// `varve::Instant`, same formatting `examples/time_travel.rs` uses for
/// `TxReceipt.system_time`).
async fn two_hop_as_of(db: &Db, anchor: &str) -> Result<usize, Box<dyn std::error::Error>> {
    let rows = db
        .query(format!(
            "FOR SYSTEM_TIME AS OF TIMESTAMP '{anchor}' \
             MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) \
             WHERE a._id = {ANCHOR} RETURN c._id"
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
/// returns the same row count as `expect` (mirrors `traversal_bench.rs`:
/// a fixed graph and a fixed AS-OF anchor must answer identically across
/// repeated warm reads).
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

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1_000.0
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let g = social_graph(PEOPLE, FRIENDSHIPS, SEED);
    let node_stmts = g.node_statements(NODE_BATCH);
    let edge_programs = g.edge_programs(EDGE_PROGRAM_BATCH);
    let total_programs = node_stmts.len() + edge_programs.len();
    let total_events = g.people + g.edges.len();
    // Capture the edge program roughly halfway through the edge-ingest loop
    // (Task-8 brief: "late anchor" — after this many edges have landed, the
    // anchor node has picked up out-edges, so the AS-OF 2-hop below this
    // point is non-empty).
    let half_edge_idx = edge_programs.len() / 2;

    // Phase 1: ingest the full fixture (timed), then drop — acked txs are
    // durable (log) or flushed (blocks).
    let mut mid_receipt: Option<TxReceipt> = None;
    let mut late_receipt: Option<TxReceipt> = None;
    let ingest_started = Instant::now();
    {
        let db = Db::open(config(dir.path())?).await?;
        for stmt in &node_stmts {
            // Overwritten every iteration; after the loop this holds the
            // receipt of the LAST node statement — the "mid" AS-OF anchor
            // (zero edges exist yet).
            mid_receipt = Some(db.execute(stmt).await?);
        }
        for (i, program) in edge_programs.iter().enumerate() {
            let receipt = db.execute(program).await?;
            if i == half_edge_idx {
                late_receipt = Some(receipt);
            }
        }
    }
    let ingest = ingest_started.elapsed();
    let mid_receipt = mid_receipt.ok_or("no node statements were ingested")?;
    let late_receipt = late_receipt.ok_or("edge programs did not cover the midpoint index")?;

    let events_per_sec = total_events as f64 / ingest.as_secs_f64();
    let tx_per_sec = total_programs as f64 / ingest.as_secs_f64();
    println!(
        "ingest {total_programs} txs ({} nodes, {} edges = {total_events} events) in \
         {ingest:.2?} ({events_per_sec:.0} events/s, {tx_per_sec:.0} tx/s)",
        g.people,
        g.edges.len(),
    );

    // Phase 2: restart = latest manifest + log tail replay, so the warm
    // reads below hit the persisted path.
    let reopen_started = Instant::now();
    let db = Db::open(config(dir.path())?).await?;
    println!(
        "reopen (manifest + log tail): {:.2?}",
        reopen_started.elapsed()
    );

    // Phase 3: point read — cold once, then warm.
    let point_cold_started = Instant::now();
    let point_rows = point_read(&db).await?;
    let point_cold = point_cold_started.elapsed();
    if point_rows != 1 {
        return Err(format!("point read: expected 1 row, got {point_rows}").into());
    }
    let point_warm = warm_timings(|| point_read(&db), point_rows, "point read").await?;
    let point_warm_avg = avg(&point_warm);
    let point_warm_p50 = p50(point_warm);
    println!(
        "point read (1 row) cold {point_cold:.2?} warm avg {point_warm_avg:.2?} p50 {point_warm_p50:.2?}"
    );

    // Phase 4: 2-hop friend-of-friend at current time — cold once, then warm.
    let two_hop_cold_started = Instant::now();
    let two_hop_rows = two_hop(&db).await?;
    let two_hop_cold = two_hop_cold_started.elapsed();
    let two_hop_warm = warm_timings(|| two_hop(&db), two_hop_rows, "2-hop").await?;
    let two_hop_warm_avg = avg(&two_hop_warm);
    let two_hop_warm_p50 = p50(two_hop_warm);
    println!(
        "2-hop current ({two_hop_rows} rows) cold {two_hop_cold:.2?} warm avg {two_hop_warm_avg:.2?} p50 {two_hop_warm_p50:.2?}"
    );

    // Phase 5: 2-hop AS OF the mid anchor (zero edges exist yet — expect 0
    // rows). Reported, but not the headline AS-OF number.
    let mid_anchor = mid_receipt.system_time.to_string();
    let mid_cold_started = Instant::now();
    let mid_rows = two_hop_as_of(&db, &mid_anchor).await?;
    let mid_cold = mid_cold_started.elapsed();
    if mid_rows != 0 {
        return Err(
            format!("AS-OF mid anchor: expected 0 rows (no edges yet), got {mid_rows}").into(),
        );
    }
    let mid_warm = warm_timings(
        || two_hop_as_of(&db, &mid_anchor),
        mid_rows,
        "AS-OF mid 2-hop",
    )
    .await?;
    let mid_warm_avg = avg(&mid_warm);
    let mid_warm_p50 = p50(mid_warm);
    println!(
        "2-hop AS-OF mid anchor ({mid_rows} rows, 0 edges exist yet) cold {mid_cold:.2?} \
         warm avg {mid_warm_avg:.2?} p50 {mid_warm_p50:.2?} — not the headline AS-OF number"
    );

    // Phase 6: 2-hop AS OF the late anchor (~half the edges landed — expect
    // a non-empty historical result). This is the headline AS-OF number.
    let late_anchor = late_receipt.system_time.to_string();
    let late_cold_started = Instant::now();
    let late_rows = two_hop_as_of(&db, &late_anchor).await?;
    let late_cold = late_cold_started.elapsed();
    if late_rows == 0 {
        return Err(
            "AS-OF late anchor: expected a non-empty historical 2-hop result, got 0 rows".into(),
        );
    }
    let late_warm = warm_timings(
        || two_hop_as_of(&db, &late_anchor),
        late_rows,
        "AS-OF late 2-hop",
    )
    .await?;
    let late_warm_avg = avg(&late_warm);
    let late_warm_p50 = p50(late_warm);
    println!(
        "2-hop AS-OF late anchor ({late_rows} rows, ~half the edges landed) cold {late_cold:.2?} \
         warm avg {late_warm_avg:.2?} p50 {late_warm_p50:.2?}"
    );

    let as_of_ratio = late_warm_p50.as_secs_f64() / two_hop_warm_p50.as_secs_f64();

    println!();
    println!("| metric | value |");
    println!("|---|---|");
    println!(
        "| ingest (batched) | {events_per_sec:.0} events/s ({total_programs} txs, {:.1} s) |",
        ingest.as_secs_f64()
    );
    println!(
        "| warm point read (p50/avg of {WARM_ITERS}) | {:.2} ms / {:.2} ms |",
        ms(point_warm_p50),
        ms(point_warm_avg)
    );
    println!(
        "| warm 2-hop (p50/avg of {WARM_ITERS}) | {:.2} ms / {:.2} ms |",
        ms(two_hop_warm_p50),
        ms(two_hop_warm_avg)
    );
    println!(
        "| AS-OF historical 2-hop (p50) | {:.2} ms ({as_of_ratio:.2}x of current) |",
        ms(late_warm_p50)
    );

    Ok(())
}
