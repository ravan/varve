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

async fn compacted_db(dir: &Path) -> Db {
    let db = Db::open(gc_config(dir, 1)).await.unwrap();
    for id in 1..=64 {
        db.execute(&format!("INSERT (:P {{_id: {id}, v: {id}}})"))
            .await
            .unwrap();
    }
    wait_for_manifest_count(dir, 64).await;
    let report = db.compact_once().await.unwrap();
    assert_eq!(report.jobs, 1);
    assert_eq!(report.input_tries, 64);
    assert_eq!(report.output_tries, 1);
    db
}

async fn listed_store_keys(dir: &Path, prefix: &str) -> Vec<String> {
    let store = varve_storage::local_store(&dir.join("store")).unwrap();
    store.list(prefix).await.unwrap()
}

#[tokio::test]
async fn gc_once_deletes_unreferenced_objects() {
    let dir = tempfile::tempdir().unwrap();
    let db = compacted_db(dir.path()).await;
    let before = listed_store_keys(dir.path(), "v1/graphs").await;
    assert!(before.iter().any(|key| key.contains("/l00-rc-b")));
    assert!(before.iter().any(|key| key.contains("/l01-rc-b")));

    let report = db.gc_once().await.unwrap();

    assert!(report.deleted_objects >= 128, "{report:?}");
    let graph_keys = listed_store_keys(dir.path(), "v1/graphs").await;
    assert!(!graph_keys.iter().any(|key| key.contains("/l00-rc-b")));
    assert!(graph_keys.iter().any(|key| key.contains("/l01-rc-b")));
    let manifests = listed_store_keys(dir.path(), "v1/blocks").await;
    assert_eq!(manifests.len(), 1);
}

#[tokio::test]
async fn gc_once_is_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let db = compacted_db(dir.path()).await;

    let first = db.gc_once().await.unwrap();
    let second = db.gc_once().await.unwrap();

    assert!(first.deleted_objects > 0, "{first:?}");
    assert_eq!(second.deleted_objects, 0);
}

#[tokio::test]
async fn gc_once_does_not_break_restart_from_latest_manifest() {
    let dir = tempfile::tempdir().unwrap();
    let db = compacted_db(dir.path()).await;
    db.gc_once().await.unwrap();

    let restarted = Db::open(gc_config(dir.path(), 1)).await.unwrap();
    let result = restarted.query("MATCH (p:P) RETURN p.v").await.unwrap();

    assert_eq!(rows(&result), 64);
}
