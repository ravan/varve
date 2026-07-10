#![allow(clippy::unwrap_used)]

use arrow::array::{Array, Int64Array};
use async_trait::async_trait;
use bytes::Bytes;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use varve::{Config, Db, RecordBatch, Registries};
use varve_config::{BuildContext, ComponentFactory, ConfigSection, RegistryError};
use varve_storage::{ObjectStore, StorageError};
use varve_testkit::db_harness::{
    local_blocks_config as blocks_config, row_count as rows, toml_escaped_path,
    wait_for_manifest_count,
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

    let trie_keys = latest_node_trie_keys(dir.path()).await;
    assert_eq!(trie_keys.len(), 1);
    assert!(trie_keys[0].starts_with("l01-rc-b"));
    assert!(!trie_keys.iter().any(|key| key.starts_with("l00-")));

    drop(db);
    let restarted = Db::open(blocks_config(dir.path(), 1)).await.unwrap();
    assert_eq!(
        rows(&restarted.query("MATCH (p:P) RETURN p.v").await.unwrap()),
        64
    );
}

#[tokio::test]
async fn flush_after_compaction_preserves_manifest_and_full_inventory() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(blocks_config(dir.path(), 1)).await.unwrap();

    for id in 1..=64 {
        db.execute(&format!("INSERT (:P {{_id: {id}, v: {id}}})"))
            .await
            .unwrap();
        wait_for_manifest_count(dir.path(), id).await;
    }

    db.compact_once().await.unwrap();
    wait_for_manifest_count(dir.path(), 65).await;
    let store = varve_storage::local_store(&dir.path().join("store")).unwrap();
    let compacted_bytes = store
        .get(&varve_storage::keys::manifest_key(64))
        .await
        .unwrap();

    db.execute("INSERT (:P {_id: 65, v: 65})").await.unwrap();
    wait_for_manifest_count(dir.path(), 66).await;

    assert_eq!(
        store
            .get(&varve_storage::keys::manifest_key(64))
            .await
            .unwrap(),
        compacted_bytes,
        "the next flush must not overwrite the compaction generation"
    );
    let history = varve_storage::manifest_history(store.as_ref())
        .await
        .unwrap();
    let ids = history
        .iter()
        .map(|manifest| manifest.block_id)
        .collect::<Vec<_>>();
    assert_eq!(ids, (0..=65).collect::<Vec<_>>());

    let latest = history.last().unwrap();
    let tries = latest
        .tables
        .iter()
        .find(|table| table.graph == "default" && table.table == "nodes" && table.family.is_empty())
        .unwrap()
        .tries
        .iter()
        .map(|entry| entry.trie_key.as_str())
        .collect::<Vec<_>>();
    assert!(tries.iter().any(|key| key.starts_with("l01-")));
    let new_l0 = varve_storage::keys::l0_trie_key(65);
    assert!(tries.contains(&new_l0.as_str()));
    assert_eq!(rows(&db.query("MATCH (p:P) RETURN p.v").await.unwrap()), 65);
}

#[tokio::test]
async fn concurrent_compaction_and_flush_use_distinct_generations_in_both_queue_orders() {
    for execute_first in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open(blocks_config(dir.path(), 1)).await.unwrap();
        for id in 1..=64 {
            db.execute(&format!("INSERT (:P {{_id: {id}, v: {id}}})"))
                .await
                .unwrap();
            wait_for_manifest_count(dir.path(), id).await;
        }

        if execute_first {
            let (receipt, report) = tokio::join!(
                biased;
                db.execute("INSERT (:P {_id: 65, v: 65})"),
                db.compact_once(),
            );
            receipt.unwrap();
            assert_eq!(report.unwrap().jobs, 1);
        } else {
            let (report, receipt) = tokio::join!(
                biased;
                db.compact_once(),
                db.execute("INSERT (:P {_id: 65, v: 65})"),
            );
            assert_eq!(report.unwrap().jobs, 1);
            receipt.unwrap();
        }
        wait_for_manifest_count(dir.path(), 66).await;

        let store = varve_storage::local_store(&dir.path().join("store")).unwrap();
        let ids = varve_storage::manifest_history(store.as_ref())
            .await
            .unwrap()
            .into_iter()
            .map(|manifest| manifest.block_id)
            .collect::<Vec<_>>();
        assert_eq!(ids, (0..=65).collect::<Vec<_>>());

        drop(db);
        let restarted = Db::open(blocks_config(dir.path(), 1)).await.unwrap();
        assert_eq!(
            rows(&restarted.query("MATCH (p:P) RETURN p.v").await.unwrap()),
            65
        );
    }
}

#[tokio::test]
async fn compact_failure_before_manifest_keeps_inputs_live() {
    let dir = tempfile::tempdir().unwrap();
    let fail_next_manifest_put = Arc::new(AtomicBool::new(false));
    let mut registries = Registries::with_builtins();
    registries
        .storage
        .register(Box::new(FailingManifestStoreFactory {
            dir: dir.path().join("store"),
            fail_next_manifest_put: Arc::clone(&fail_next_manifest_put),
        }))
        .unwrap();

    let db = Db::open_with(&failing_manifest_config(dir.path()), &registries)
        .await
        .unwrap();

    for id in 1..=64 {
        db.execute(&format!("INSERT (:P {{_id: {id}, v: {id}}})"))
            .await
            .unwrap();
        wait_for_manifest_count(dir.path(), id).await;
    }

    assert_eq!(rows(&db.query("MATCH (p:P) RETURN p.v").await.unwrap()), 64);

    fail_next_manifest_put.store(true, Ordering::SeqCst);
    assert!(db.compact_once().await.is_err());
    assert!(!fail_next_manifest_put.load(Ordering::SeqCst));

    let trie_keys = latest_node_trie_keys(dir.path()).await;
    assert_eq!(trie_keys.len(), 64);
    assert!(trie_keys.iter().all(|key| key.starts_with("l00-")));

    assert_eq!(rows(&db.query("MATCH (p:P) RETURN p.v").await.unwrap()), 64);

    let store = varve_storage::local_store(&dir.path().join("store")).unwrap();
    let previous_manifest = store
        .get(&varve_storage::keys::manifest_key(63))
        .await
        .unwrap();

    db.execute("INSERT (:P {_id: 65, v: 65})").await.unwrap();
    wait_for_manifest_count(dir.path(), 65).await;
    let latest = varve_storage::latest_manifest(store.as_ref())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(latest.block_id, 64, "failed compaction must not consume 64");
    assert!(latest.tables[0]
        .tries
        .iter()
        .any(|entry| entry.trie_key == varve_storage::keys::l0_trie_key(64)));
    assert_eq!(
        store
            .get(&varve_storage::keys::manifest_key(63))
            .await
            .unwrap(),
        previous_manifest
    );

    drop(db);
    let restarted = Db::open(blocks_config(dir.path(), 1)).await.unwrap();
    assert_eq!(
        rows(&restarted.query("MATCH (p:P) RETURN p.v").await.unwrap()),
        65
    );
}

#[tokio::test]
async fn duplicate_compaction_job_last_write_wins() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(blocks_config(dir.path(), 1)).await.unwrap();

    for v in 0..64 {
        db.execute(&format!("INSERT (:P {{_id: 1, v: {v}}})"))
            .await
            .unwrap();
        wait_for_manifest_count(dir.path(), v + 1).await;
    }

    let query = "MATCH (p:P) RETURN p.v AS v";
    assert_eq!(query_i64(&db, query, "v").await, vec![63]);

    let report = db.compact_once().await.unwrap();
    assert_eq!(report.jobs, 1);
    assert_eq!(report.input_tries, 64);
    // The current row and retained history land in separate recency partitions.
    assert_eq!(report.output_tries, 2);

    assert_eq!(query_i64(&db, query, "v").await, vec![63]);

    drop(db);
    let restarted = Db::open(blocks_config(dir.path(), 1)).await.unwrap();
    assert_eq!(query_i64(&restarted, query, "v").await, vec![63]);
}

async fn latest_node_trie_keys(root: &Path) -> Vec<String> {
    let store = varve_storage::local_store(&root.join("store")).unwrap();
    let manifest = varve_storage::latest_manifest(store.as_ref())
        .await
        .unwrap()
        .unwrap();
    manifest
        .tables
        .iter()
        .find(|table| table.graph == "default" && table.table == "nodes" && table.family.is_empty())
        .unwrap()
        .tries
        .iter()
        .map(|entry| entry.trie_key.clone())
        .collect()
}

async fn query_i64(db: &Db, gql: &str, name: &str) -> Vec<i64> {
    column_i64(&db.query(gql).await.unwrap(), name)
}

fn column_i64(batches: &[RecordBatch], name: &str) -> Vec<i64> {
    let mut out = Vec::new();
    for batch in batches {
        let Some(column) = batch.column_by_name(name) else {
            continue;
        };
        let values = column.as_any().downcast_ref::<Int64Array>().unwrap();
        for row in 0..values.len() {
            out.push(values.value(row));
        }
    }
    out.sort_unstable();
    out
}

fn failing_manifest_config(root: &Path) -> Config {
    let log_dir = toml_escaped_path(&root.join("log"));
    Config::from_toml_str(&format!(
        "[log]\n\
         backend = \"local\"\n\
         group_commit_window_ms = 1\n\
         [log.local]\n\
         dir = {log_dir}\n\
         [storage]\n\
         backend = \"fail_manifest\"\n\
         max_block_rows = 1\n"
    ))
    .unwrap()
}

struct FailingManifestStoreFactory {
    dir: PathBuf,
    fail_next_manifest_put: Arc<AtomicBool>,
}

impl ComponentFactory<dyn ObjectStore> for FailingManifestStoreFactory {
    fn name(&self) -> &'static str {
        "fail_manifest"
    }

    fn build(
        &self,
        _cfg: &ConfigSection,
        _ctx: &BuildContext,
    ) -> Result<Arc<dyn ObjectStore>, RegistryError> {
        let inner = varve_storage::local_store(&self.dir).map_err(|e| RegistryError::Build {
            kind: "storage",
            name: self.name().into(),
            source: Box::new(e),
        })?;
        Ok(Arc::new(FailingManifestStore {
            inner,
            fail_next_manifest_put: Arc::clone(&self.fail_next_manifest_put),
        }))
    }
}

struct FailingManifestStore {
    inner: Arc<dyn ObjectStore>,
    fail_next_manifest_put: Arc<AtomicBool>,
}

#[async_trait]
impl ObjectStore for FailingManifestStore {
    async fn put(&self, key: &str, bytes: Bytes) -> Result<(), StorageError> {
        if is_manifest_key(key) && self.fail_next_manifest_put.swap(false, Ordering::SeqCst) {
            return Err(StorageError::Io(std::io::Error::other(
                "injected manifest put failure",
            )));
        }
        self.inner.put(key, bytes).await
    }

    async fn get(&self, key: &str) -> Result<Bytes, StorageError> {
        self.inner.get(key).await
    }

    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Bytes, StorageError> {
        self.inner.get_range(key, range).await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
        self.inner.list(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), StorageError> {
        self.inner.delete(key).await
    }

    fn conditional(&self) -> Option<&dyn varve_storage::ConditionalStore> {
        self.inner.conditional()
    }
}

fn is_manifest_key(key: &str) -> bool {
    key.starts_with("v1/blocks/") && key.ends_with(".manifest")
}
