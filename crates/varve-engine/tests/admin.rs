#![allow(clippy::unwrap_used)]

use bytes::Bytes;
use tempfile::TempDir;
use varve_config::Config;
use varve_engine::{Db, EngineError, NodeRole};

fn config(root: &TempDir, roles: &[&str], max_block_rows: usize) -> Config {
    let roles = roles
        .iter()
        .map(|role| format!("\"{role}\""))
        .collect::<Vec<_>>()
        .join(", ");
    Config::from_toml_str(&format!(
        "[node]\nroles = [{roles}]\ntail_poll_interval_ms = 5\n\
         tail_batch_records = 1024\nbasis_timeout_ms = 1000\n\
         [log]\nbackend = \"local\"\ngroup_commit_window_ms = 0\n\
         [log.local]\ndir = {:?}\n\
         [storage]\nbackend = \"local\"\nmax_block_rows = {max_block_rows}\n\
         flush_interval_ms = 300000\n\
         [storage.local]\ndir = {:?}\n",
        root.path().join("log").display().to_string(),
        root.path().join("store").display().to_string(),
    ))
    .unwrap()
}

fn writer_config(root: &TempDir, max_block_rows: usize) -> Config {
    config(root, &["writer", "query", "compactor"], max_block_rows)
}

fn query_config(root: &TempDir) -> Config {
    config(root, &["query"], 100_000)
}

#[tokio::test]
async fn writer_advertisement_round_trips_through_the_store() {
    let root = TempDir::new().unwrap();
    let writer = Db::open(writer_config(&root, 100_000)).await.unwrap();
    let query = Db::open(query_config(&root)).await.unwrap();

    writer
        .publish_writer("https://writer.internal:8443")
        .await
        .unwrap();
    assert_eq!(
        query.writer_advertisement().await.unwrap().unwrap().address,
        "https://writer.internal:8443"
    );
    assert!(matches!(
        query.publish_writer("https://wrong").await,
        Err(EngineError::RoleDisabled(NodeRole::Writer))
    ));
    assert!(matches!(
        query.compact_once().await,
        Err(EngineError::RoleDisabled(NodeRole::Compactor))
    ));
}

#[tokio::test]
async fn writer_advertisement_requires_the_exact_object_key() {
    let root = TempDir::new().unwrap();
    let store = varve_storage::local_store(&root.path().join("store")).unwrap();
    store
        .put(
            "v1/writer.json/not-the-advertisement",
            Bytes::from_static(br#"{"address":"https://wrong"}"#),
        )
        .await
        .unwrap();
    let query = Db::open(query_config(&root)).await.unwrap();

    assert_eq!(query.writer_advertisement().await.unwrap(), None);
}

#[tokio::test]
async fn malformed_writer_advertisement_is_an_error() {
    let root = TempDir::new().unwrap();
    let store = varve_storage::local_store(&root.path().join("store")).unwrap();
    store
        .put("v1/writer.json", Bytes::from_static(b"not-json"))
        .await
        .unwrap();
    let query = Db::open(query_config(&root)).await.unwrap();

    assert!(query.writer_advertisement().await.is_err());
}

#[tokio::test]
async fn verify_checks_the_latest_snapshot_and_log_tail() {
    let root = TempDir::new().unwrap();
    let db = Db::open(writer_config(&root, 2)).await.unwrap();
    db.execute("INSERT (:X {_id: 1})").await.unwrap();
    db.execute("INSERT (:X {_id: 2})").await.unwrap();
    db.compact_once().await.unwrap();
    db.execute("INSERT (:X {_id: 3})").await.unwrap();

    let report = db.verify().await.unwrap();
    assert!(report.manifest_block_id.is_some());
    assert!(report.tries_checked > 0);
    assert!(report.pages_checked > 0);
    assert!(report.events_checked >= 2);
    assert_eq!(report.log_records_checked, 1);
}

#[tokio::test]
async fn verify_reports_truncated_referenced_data_as_corruption() {
    let root = TempDir::new().unwrap();
    let db = Db::open(writer_config(&root, 1)).await.unwrap();
    db.execute("INSERT (:X {_id: 1})").await.unwrap();
    db.compact_once().await.unwrap();
    db.verify().await.unwrap();
    drop(db);

    let store = varve_storage::local_store(&root.path().join("store")).unwrap();
    let manifest = varve_storage::latest_manifest(store.as_ref())
        .await
        .unwrap()
        .unwrap();
    let trie = manifest.trie_entries().next().unwrap();
    let scoped = trie.scoped_trie_key();
    let data = store.get(&scoped.data_key()).await.unwrap();
    store
        .put(
            &scoped.data_key(),
            data.slice(..data.len().saturating_sub(1)),
        )
        .await
        .unwrap();

    let query = Db::open(query_config(&root)).await.unwrap();
    assert!(matches!(
        query.verify().await,
        Err(EngineError::Storage(_) | EngineError::Index(_))
    ));
}
