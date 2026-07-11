//! Slice-5 exit criterion: cold vs warm query latency demonstrates the disk
//! cache. Flow: ingest → flush to blocks → reopen with a COLD disk cache
//! (the point query fills it) → reopen again with the SAME cache dir (the
//! cache survived the restart) → the same query is served from disk.
//!
//! Default backend: local FS. Set VARVE_S3_ENDPOINT (+ VARVE_S3_BUCKET,
//! VARVE_S3_ACCESS_KEY_ID, VARVE_S3_SECRET_ACCESS_KEY, optional
//! VARVE_S3_REGION, default "garage") to run against a real S3 backend.
//!
//! Run: cargo run --release --example cache_bench -p varve
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::Path;
use std::time::{Duration, Instant};
use varve::{Config, Db};

const EVENTS: usize = 100_000;
const BATCH: usize = 500;
const LOOKUP_ID: usize = 73_123;

fn toml_escaped(dir: &Path) -> String {
    format!("{:?}", dir.display().to_string())
}

fn storage_toml(root: &Path) -> String {
    if let Ok(endpoint) = std::env::var("VARVE_S3_ENDPOINT") {
        let need = |k: &str| {
            std::env::var(k).unwrap_or_else(|_| panic!("{k} is required with VARVE_S3_ENDPOINT"))
        };
        format!(
            "[storage]\nbackend = \"s3\"\nmax_block_rows = 25000\n[storage.s3]\n\
             endpoint = \"{endpoint}\"\nbucket = \"{}\"\nregion = \"{}\"\n\
             access_key_id = \"{}\"\nsecret_access_key = \"{}\"\n",
            need("VARVE_S3_BUCKET"),
            std::env::var("VARVE_S3_REGION").unwrap_or_else(|_| "garage".into()),
            need("VARVE_S3_ACCESS_KEY_ID"),
            need("VARVE_S3_SECRET_ACCESS_KEY"),
        )
    } else {
        format!(
            "[storage]\nbackend = \"local\"\nmax_block_rows = 25000\n\
             [storage.local]\ndir = {}\n",
            toml_escaped(&root.join("store"))
        )
    }
}

fn config(root: &Path, cache_dir: &Path) -> Config {
    Config::from_toml_str(&format!(
        "[log]\nbackend = \"local\"\ngroup_commit_window_ms = 1\n[log.local]\ndir = {}\n\
         {}\
         [cache]\ntiers = [\"disk\"]\n[cache.disk]\ndir = {}\n",
        toml_escaped(&root.join("log")),
        storage_toml(root),
        toml_escaped(cache_dir),
    ))
    .expect("valid bench config")
}

async fn timed_lookup(root: &Path, cache_dir: &Path) -> Duration {
    let start = Instant::now();
    let db = Db::open(config(root, cache_dir)).await.expect("open");
    let batches = db
        .query(format!(
            "MATCH (p:Person) WHERE p._id = {LOOKUP_ID} RETURN p.name"
        ))
        .await
        .expect("point lookup");
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 1, "the looked-up person must exist");
    start.elapsed()
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let root = tempfile::tempdir().expect("tempdir");
    let cache_dir = root.path().join("cache");

    let start = Instant::now();
    {
        let db = Db::open(config(root.path(), &cache_dir))
            .await
            .expect("open");
        for batch in 0..(EVENTS / BATCH) {
            let mut stmt = String::from("INSERT ");
            for i in 0..BATCH {
                let id = batch * BATCH + i;
                if i > 0 {
                    stmt.push_str(", ");
                }
                stmt.push_str(&format!("(:Person {{_id: {id}, name: 'p{id}'}})"));
            }
            db.execute(&stmt).await.expect("insert batch");
        }
        // 100k events at max_block_rows = 25000 ⇒ 4 blocks; the last flush
        // runs just after the final ack — give it a moment to commit.
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    println!(
        "ingest  {EVENTS} events ({} txs): {:?}",
        EVENTS / BATCH,
        start.elapsed()
    );

    // Ingest only wrote; the cache dir starts effectively empty.
    let cold = timed_lookup(root.path(), &cache_dir).await;
    println!("cold    (disk cache empty):     open+lookup {cold:?}");

    let warm = timed_lookup(root.path(), &cache_dir).await;
    println!("warm    (cache survived reopen): open+lookup {warm:?}");

    let files = std::fs::read_dir(&cache_dir)
        .map(|d| d.count())
        .unwrap_or(0);
    println!("disk cache: {files} entries under {}", cache_dir.display());
}
