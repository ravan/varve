#![allow(clippy::unwrap_used)]

use std::path::Path;
use std::time::Duration;
use varve::{Config, Db};

fn gc_config(dir: &Path, max_block_rows: usize) -> Config {
    let log_dir = toml_escaped(&dir.join("log"));
    let store_dir = toml_escaped(&dir.join("store"));
    Config::from_toml_str(&format!(
        "[log]\n\
         backend = \"local\"\n\
         group_commit_window_ms = 1\n\
         [log.local]\n\
         dir = {log_dir}\n\
         [storage]\n\
         backend = \"local\"\n\
         max_block_rows = {max_block_rows}\n\
         [storage.local]\n\
         dir = {store_dir}\n\
         [gc]\n\
         enabled = true\n\
         blocks_to_keep = 0\n\
         garbage_lifetime_hours = 0\n"
    ))
    .unwrap()
}

fn toml_escaped(dir: &Path) -> String {
    format!("{:?}", dir.display().to_string())
}

fn rows(batches: &[varve::RecordBatch]) -> usize {
    batches.iter().map(|batch| batch.num_rows()).sum()
}

async fn wait_for_manifest_count(dir: &Path, count: usize) {
    let blocks = dir.join("store").join("v1").join("blocks");
    for _ in 0..200 {
        let got = blocks
            .read_dir()
            .map(|entries| entries.count())
            .unwrap_or(0);
        if got >= count {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("expected at least {count} manifests under {blocks:?}");
}

async fn graph_object_bytes(dir: &Path) -> Vec<u8> {
    let store = varve_storage::local_store(&dir.join("store")).unwrap();
    let mut bytes = Vec::new();
    for key in store.list("v1/graphs").await.unwrap() {
        if key.ends_with(".arrow") {
            bytes.extend_from_slice(&store.get(&key).await.unwrap());
        }
    }
    bytes
}

#[tokio::test]
async fn erased_property_bytes_absent_after_compaction_and_gc() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(gc_config(dir.path(), 1)).await.unwrap();
    let secret = "gdpr-secret-sentinel-8f2f1de0";

    db.execute(&format!("INSERT (:P {{_id: 1, token: '{secret}'}})"))
        .await
        .unwrap();
    db.execute("MATCH (p:P {_id: 1}) ERASE p").await.unwrap();
    db.execute("INSERT (:P {_id: 1, token: 'fresh-public'})")
        .await
        .unwrap();
    for id in 2..=62 {
        db.execute(&format!("INSERT (:P {{_id: {id}, token: 'filler-{id}'}})"))
            .await
            .unwrap();
    }
    wait_for_manifest_count(dir.path(), 64).await;

    let before = graph_object_bytes(dir.path()).await;
    assert!(String::from_utf8_lossy(&before).contains(secret));

    let compact = db.compact_once().await.unwrap();
    assert_eq!(compact.jobs, 1);
    db.gc_once().await.unwrap();

    let after = graph_object_bytes(dir.path()).await;
    assert!(!String::from_utf8_lossy(&after).contains(secret));
    let current = db
        .query("MATCH (p:P {_id: 1}) WHERE p.token = 'fresh-public' RETURN p.token")
        .await
        .unwrap();
    assert_eq!(rows(&current), 1);
}
