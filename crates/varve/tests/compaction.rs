#![allow(clippy::unwrap_used)]

use varve::Db;
use varve_testkit::db_harness::{
    local_blocks_config as blocks_config, row_count as rows, wait_for_manifest_count,
};

#[tokio::test]
async fn compact_once_replaces_input_tries_after_manifest_commit() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(blocks_config(dir.path(), 1)).await.unwrap();

    for id in 1..=64 {
        db.execute(&format!("INSERT (:P {{_id: {id}, v: {id}}})"))
            .await
            .unwrap();
        wait_for_manifest_count(dir.path(), id).await;
    }

    assert_eq!(rows(&db.query("MATCH (p:P) RETURN p.v").await.unwrap()), 64);

    let report = db.compact_once().await.unwrap();
    assert_eq!(report.jobs, 1);
    assert_eq!(report.input_tries, 64);
    assert_eq!(report.output_tries, 1);

    let store = varve_storage::local_store(&dir.path().join("store")).unwrap();
    let manifest = varve_storage::latest_manifest(store.as_ref())
        .await
        .unwrap()
        .unwrap();
    let nodes = manifest
        .tables
        .iter()
        .find(|table| table.graph == "default" && table.table == "nodes" && table.family.is_empty())
        .unwrap();
    assert_eq!(nodes.tries.len(), 1);
    assert!(nodes.tries[0].trie_key.starts_with("l01-rc-b"));
    assert!(!nodes
        .tries
        .iter()
        .any(|entry| entry.trie_key.starts_with("l00-")));

    drop(db);
    let restarted = Db::open(blocks_config(dir.path(), 1)).await.unwrap();
    assert_eq!(
        rows(&restarted.query("MATCH (p:P) RETURN p.v").await.unwrap()),
        64
    );
}
