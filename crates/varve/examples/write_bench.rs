//! Write-throughput smoke bench (slice-3 exit criterion; record the printed
//! numbers in STATUS.md). Not criterion — the real suite is slice 11.
//! Run: cargo run --release --example write_bench -p varve

use std::sync::Arc;
use std::time::Instant;
use varve::Db;

const TOTAL: u64 = 4_000;
const WORKERS: u64 = 8;

async fn bench(label: &str, db: Db) -> Result<(), Box<dyn std::error::Error>> {
    let db = Arc::new(db);
    let start = Instant::now();
    let mut handles = Vec::new();
    for worker in 0..WORKERS {
        let db = Arc::clone(&db);
        handles.push(tokio::spawn(async move {
            for i in 0..TOTAL / WORKERS {
                let id = worker * 1_000_000 + i;
                db.execute(&format!("INSERT (:Bench {{_id: {id}, v: {i}}})"))
                    .await?;
            }
            Ok::<(), varve::EngineError>(())
        }));
    }
    for handle in handles {
        handle.await??;
    }
    let elapsed = start.elapsed();
    println!(
        "{label:>14}: {TOTAL} txs / {WORKERS} workers → {:>8.0} tx/s  ({elapsed:.2?})",
        TOTAL as f64 / elapsed.as_secs_f64()
    );
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    bench("memory", Db::memory()).await?;

    let dir = std::env::temp_dir().join(format!("varve-write-bench-{}", std::process::id()));
    bench("local (fsync)", Db::local(&dir).await?).await?;
    std::fs::remove_dir_all(&dir)?;
    Ok(())
}
