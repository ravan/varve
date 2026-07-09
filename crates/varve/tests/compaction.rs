#![allow(clippy::unwrap_used)]

use std::path::Path;
use std::time::Duration;
use varve::{Config, Db};

fn blocks_config(dir: &Path, max_block_rows: usize) -> Config {
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
         dir = {store_dir}\n"
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
