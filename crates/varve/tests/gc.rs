#![allow(clippy::unwrap_used)]

use std::path::Path;

use varve::Db;
use varve_testkit::db_harness::{
    local_gc_blocks_config as gc_config, row_count as rows, wait_for_manifest_count,
};

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
