//! Slice-8 smoke: write flushed L0 blocks, compact them, run GC, and
//! print object counts before/after collection.
//!
//! Run: cargo run --release --example compaction_gc -p varve

use std::path::Path;
use std::time::Duration;
use varve::Db;
use varve_testkit::db_harness::{compact_until_idle, local_gc_blocks_config, row_count};

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

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let db = Db::open(local_gc_blocks_config(dir.path(), 1)).await?;

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
