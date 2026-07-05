//! Slice-4 exit-criteria smoke: 1M-event ingest → restart → warm point
//! lookup (< 100 ms target; record the printed numbers in STATUS.md).
//! Run: cargo run --release --example block_bench -p varve

use std::path::Path;
use std::time::Instant;
use varve::{Config, Db};

const NODES_PER_INSERT: usize = 1_000;
const INSERTS: usize = 1_000; // 1M nodes total
                              // Small enough that ~40 blocks flush over the course of ingest, so the
                              // persisted path (Task 6 prune, Task 8 iid_point, Task 9 merged scan) is
                              // genuinely exercised rather than everything staying in the live table.
const MAX_BLOCK_ROWS: usize = 25_000;
// Block 20 of ~40 (batches 500-524) — comfortably flushed and durable long
// before ingest finishes, unlike the very last row, which could still be
// sitting in the live/log-recovered tail after restart. Probing this id
// guarantees the cold lookup actually pays for a persisted page GET+decode.
const PROBE_ID: usize = 500_000;

fn config(dir: &Path) -> Result<Config, Box<dyn std::error::Error>> {
    let log_dir = format!("{:?}", dir.join("log").display().to_string());
    let store_dir = format!("{:?}", dir.join("store").display().to_string());
    Ok(Config::from_toml_str(&format!(
        "[log]\nbackend = \"local\"\n[log.local]\ndir = {log_dir}\n\
         [storage]\nbackend = \"local\"\nmax_block_rows = {MAX_BLOCK_ROWS}\n\
         [storage.local]\ndir = {store_dir}\n"
    ))?)
}

fn insert_statement(batch: usize) -> String {
    let mut stmt = String::with_capacity(NODES_PER_INSERT * 40);
    stmt.push_str("INSERT ");
    for j in 0..NODES_PER_INSERT {
        let id = batch * NODES_PER_INSERT + j;
        if j > 0 {
            stmt.push_str(", ");
        }
        stmt.push_str(&format!("(:Bench {{_id: {id}, v: {id}}})"));
    }
    stmt
}

async fn point_lookup(db: &Db) -> Result<i64, Box<dyn std::error::Error>> {
    use arrow::array::Int64Array;
    let batches = db
        .query(&format!(
            "MATCH (b:Bench) WHERE b._id = {PROBE_ID} RETURN b.v AS v"
        ))
        .await?;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 1, "point lookup must return exactly one row");
    let col: &Int64Array = batches[0]
        .column_by_name("v")
        .ok_or("missing v column")?
        .as_any()
        .downcast_ref()
        .ok_or("v is not Int64")?;
    Ok(col.value(0))
}

/// Count persisted block files under `dir/store/v1/blocks` — a filesystem
/// peek for reporting purposes only (mirrors the layout `wait_for_flush`
/// polls in `tests/blocks.rs`), not part of the measured public-API path.
fn count_blocks(dir: &Path) -> usize {
    dir.join("store")
        .join("v1")
        .join("blocks")
        .read_dir()
        .map(|entries| entries.count())
        .unwrap_or(0)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;

    // Phase 1: ingest 1M nodes (1000 txs × 1000 nodes; max_block_rows =
    // 25,000 → ~40 block flushes along the way, so the persisted path is
    // genuinely exercised rather than everything staying live).
    let started = Instant::now();
    {
        let db = Db::open(config(dir.path())?).await?;
        for batch in 0..INSERTS {
            db.execute(&insert_statement(batch)).await?;
        }
        let ingest = started.elapsed();
        println!(
            "ingest: {} events in {:.2?} ({:.0} events/s)",
            NODES_PER_INSERT * INSERTS,
            ingest,
            (NODES_PER_INSERT * INSERTS) as f64 / ingest.as_secs_f64()
        );
    } // drop: acked txs are durable (log) or flushed (blocks)

    println!(
        "blocks flushed: {} persisted block files under store/v1/blocks",
        count_blocks(dir.path())
    );

    // Phase 2: restart = latest manifest + log tail replay.
    let reopen_started = Instant::now();
    let db = Db::open(config(dir.path())?).await?;
    println!(
        "reopen (manifest + log tail): {:.2?}",
        reopen_started.elapsed()
    );

    // Phase 3: point lookup — cold, then warm (cache + meta in memory).
    let cold_started = Instant::now();
    assert_eq!(point_lookup(&db).await?, PROBE_ID as i64);
    let cold = cold_started.elapsed();
    let warm_started = Instant::now();
    assert_eq!(point_lookup(&db).await?, PROBE_ID as i64);
    let warm = warm_started.elapsed();
    println!("point lookup: cold {cold:.2?}, warm {warm:.2?}");
    println!(
        "exit criterion (<100ms warm point lookup): {}",
        if warm.as_millis() < 100 {
            "PASS"
        } else {
            "FAIL"
        }
    );

    // Correctness across the restart: total row count.
    let all = db.query("MATCH (b:Bench) RETURN b._id").await?;
    let rows: usize = all.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        rows,
        NODES_PER_INSERT * INSERTS,
        "all rows visible after restart"
    );
    println!("full scan after restart: {rows} rows OK");
    Ok(())
}
