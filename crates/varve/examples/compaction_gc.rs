//! Slice-8 smoke: write flushed L0 blocks, compact them, run GC, and
//! print object counts before/after collection.
//!
//! Run: cargo run --release --example compaction_gc -p varve

use std::path::Path;
use std::time::Duration;
use varve::{Config, Db};

fn config(dir: &Path) -> Result<Config, Box<dyn std::error::Error>> {
    let log_dir = toml_escaped(&dir.join("log"));
    let store_dir = toml_escaped(&dir.join("store"));
    Ok(Config::from_toml_str(&format!(
        "[log]\n\
         backend = \"local\"\n\
         group_commit_window_ms = 1\n\
         [log.local]\n\
         dir = {log_dir}\n\
         [storage]\n\
         backend = \"local\"\n\
         max_block_rows = 1\n\
         [storage.local]\n\
         dir = {store_dir}\n\
         [gc]\n\
         enabled = true\n\
         blocks_to_keep = 0\n\
         garbage_lifetime_hours = 0\n"
    ))?)
}

fn toml_escaped(dir: &Path) -> String {
    format!("{:?}", dir.display().to_string())
}

async fn object_count(dir: &Path) -> Result<usize, Box<dyn std::error::Error>> {
    let store = varve_storage::local_store(&dir.join("store"))?;
    Ok(store.list("v1/graphs").await?.len() + store.list("v1/blocks").await?.len())
}

async fn l0_data_count(dir: &Path) -> Result<usize, Box<dyn std::error::Error>> {
    let store = varve_storage::local_store(&dir.join("store"))?;
    Ok(store
        .list("v1/graphs")
        .await?
        .into_iter()
        .filter(|key| key.contains("/data/l00-rc-b"))
        .count())
}

async fn wait_for_l0_data(dir: &Path, count: usize) -> Result<(), Box<dyn std::error::Error>> {
    for _ in 0..200 {
        if l0_data_count(dir).await? >= count {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    Err(format!("expected at least {count} L0 data objects").into())
}

async fn compact_until_idle(db: &Db) -> Result<usize, Box<dyn std::error::Error>> {
    let mut jobs = 0;
    for _ in 0..8 {
        let report = db.compact_once().await?;
        if report.jobs == 0 {
            return Ok(jobs);
        }
        jobs += report.jobs;
    }
    Err("compaction did not become idle".into())
}

fn row_count(batches: &[varve::RecordBatch]) -> usize {
    batches.iter().map(|batch| batch.num_rows()).sum()
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let db = Db::open(config(dir.path())?).await?;

    db.execute("INSERT (:P {_id: 1, token: 'erase-me'})")
        .await?;
    db.execute("MATCH (p:P {_id: 1}) ERASE p").await?;
    db.execute("INSERT (:P {_id: 1, token: 'fresh'})").await?;
    for id in 2..=62 {
        db.execute(&format!("INSERT (:P {{_id: {id}, token: 'p{id}'}})"))
            .await?;
    }
    wait_for_l0_data(dir.path(), 64).await?;

    let before = object_count(dir.path()).await?;
    let jobs = compact_until_idle(&db).await?;
    let after_compaction = object_count(dir.path()).await?;
    let gc = db.gc_once().await?;
    let after_gc = object_count(dir.path()).await?;
    let rows = row_count(&db.query("MATCH (p:P) RETURN p._id").await?);

    println!("objects before compaction: {before}");
    println!("compaction jobs: {jobs}");
    println!("objects after compaction: {after_compaction}");
    println!(
        "gc deleted: {} planned / {} deleted",
        gc.planned_objects, gc.deleted_objects
    );
    println!("objects after gc: {after_gc}");
    println!("current rows: {rows}");
    Ok(())
}
