//! Anchor-reachable NODE-scan pruning (follow-up to the slice-6/task-12
//! reachable-EDGE pruning): an anchored traversal must not full-scan the
//! nodes table for its non-anchor node elements. Each test builds 64
//! one-node blocks (max_block_rows = 1) behind a store that counts
//! `get_range` reads of nodes-table data pages, then asserts an anchored
//! traversal touches only pages that can hold anchor-reachable nodes —
//! while returning exactly the same rows as the unpruned scan would.

#![allow(clippy::unwrap_used)]

use arrow::array::{Array, Int64Array};
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::BTreeSet;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use varve::{Config, Db, RecordBatch, Registries};
use varve_config::{BuildContext, ComponentFactory, ConfigSection, RegistryError};
use varve_storage::{ConditionalStore, ObjectStore, StorageError};
use varve_testkit::db_harness::{toml_escaped_path, wait_for_manifest_count};

/// Distinct nodes-table data pages an anchored traversal may touch here:
/// anchor point read + two set-pruned element scans over the 4 reachable
/// nodes (one page per node at max_block_rows = 1), with headroom. Far
/// below the 64 pages a single full node scan reads.
const MAX_ANCHORED_NODE_PAGE_READS: usize = 12;

const PEOPLE: i64 = 64;

struct CountingStoreFactory {
    dir: PathBuf,
    reads: Arc<Mutex<Vec<String>>>,
}

impl ComponentFactory<dyn ObjectStore> for CountingStoreFactory {
    fn name(&self) -> &'static str {
        "counting"
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
        Ok(Arc::new(CountingStore {
            inner,
            reads: Arc::clone(&self.reads),
        }))
    }
}

struct CountingStore {
    inner: Arc<dyn ObjectStore>,
    reads: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl ObjectStore for CountingStore {
    async fn put(&self, key: &str, bytes: Bytes) -> Result<(), StorageError> {
        self.inner.put(key, bytes).await
    }

    async fn get(&self, key: &str) -> Result<Bytes, StorageError> {
        self.inner.get(key).await
    }

    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Bytes, StorageError> {
        self.reads
            .lock()
            .unwrap()
            .push(format!("{key}@{}..{}", range.start, range.end));
        self.inner.get_range(key, range).await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
        self.inner.list(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), StorageError> {
        self.inner.delete(key).await
    }

    fn conditional(&self) -> Option<&dyn ConditionalStore> {
        self.inner.conditional()
    }
}

fn counting_config(root: &Path) -> Config {
    let log_dir = toml_escaped_path(&root.join("log"));
    Config::from_toml_str(&format!(
        "[log]\n\
         backend = \"local\"\n\
         group_commit_window_ms = 1\n\
         [log.local]\n\
         dir = {log_dir}\n\
         [storage]\n\
         backend = \"counting\"\n\
         max_block_rows = 1\n"
    ))
    .unwrap()
}

fn registries(root: &Path, reads: &Arc<Mutex<Vec<String>>>) -> Registries {
    let mut registries = Registries::with_builtins();
    registries
        .storage
        .register(Box::new(CountingStoreFactory {
            dir: root.join("store"),
            reads: Arc::clone(reads),
        }))
        .unwrap();
    registries
}

fn node_page_reads(reads: &Arc<Mutex<Vec<String>>>) -> Vec<String> {
    reads
        .lock()
        .unwrap()
        .iter()
        .filter(|key| key.contains("/tables/nodes/data/"))
        .cloned()
        .collect()
}

fn ids(batches: &[RecordBatch]) -> BTreeSet<i64> {
    let mut out = BTreeSet::new();
    for batch in batches {
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..col.len() {
            if !col.is_null(i) {
                out.insert(col.value(i));
            }
        }
    }
    out
}

/// 64 Person nodes in 64 one-row blocks; KNOWS edges 0→1, 1→2, 1→3 (the
/// anchor-reachable island) plus 5→6 and 10→11 elsewhere.
async fn build_graph(root: &Path, reads: &Arc<Mutex<Vec<String>>>) {
    let db = Db::open_with(&counting_config(root), &registries(root, reads))
        .await
        .unwrap();
    for id in 0..PEOPLE {
        db.execute(&format!("INSERT (:Person {{_id: {id}}})"))
            .await
            .unwrap();
    }
    for (src, dst) in [(0, 1), (1, 2), (1, 3), (5, 6), (10, 11)] {
        db.execute(&format!(
            "MATCH (a:Person {{_id: {src}}}), (b:Person {{_id: {dst}}}) \
             INSERT (a)-[:KNOWS]->(b)"
        ))
        .await
        .unwrap();
    }
    wait_for_manifest_count(root, PEOPLE as usize).await;
    drop(db);
}

#[tokio::test]
async fn anchored_fixed_two_hop_reads_only_reachable_node_pages() {
    let dir = tempfile::tempdir().unwrap();
    let reads = Arc::new(Mutex::new(Vec::new()));
    build_graph(dir.path(), &reads).await;

    // Fresh Db so no page read is hidden by state warmed during setup.
    let db = Db::open_with(
        &counting_config(dir.path()),
        &registries(dir.path(), &reads),
    )
    .await
    .unwrap();
    reads.lock().unwrap().clear();

    let batches = db
        .query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) \
             WHERE a._id = 0 RETURN c._id",
        )
        .await
        .unwrap();
    assert_eq!(ids(&batches), BTreeSet::from([2, 3]));

    let node_reads = node_page_reads(&reads);
    assert!(
        node_reads.len() <= MAX_ANCHORED_NODE_PAGE_READS,
        "anchored 2-hop read {} nodes-table data pages (expected <= {}): {:#?}",
        node_reads.len(),
        MAX_ANCHORED_NODE_PAGE_READS,
        node_reads
    );
}

#[tokio::test]
async fn anchored_quantified_hop_reads_only_reachable_node_pages() {
    let dir = tempfile::tempdir().unwrap();
    let reads = Arc::new(Mutex::new(Vec::new()));
    build_graph(dir.path(), &reads).await;

    let db = Db::open_with(
        &counting_config(dir.path()),
        &registries(dir.path(), &reads),
    )
    .await
    .unwrap();
    reads.lock().unwrap().clear();

    let batches = db
        .query(
            "MATCH (a:Person)-[:KNOWS]->{1,2}(b:Person) \
             WHERE a._id = 0 RETURN b._id",
        )
        .await
        .unwrap();
    assert_eq!(ids(&batches), BTreeSet::from([1, 2, 3]));

    let node_reads = node_page_reads(&reads);
    assert!(
        node_reads.len() <= MAX_ANCHORED_NODE_PAGE_READS,
        "anchored quantified hop read {} nodes-table data pages (expected <= {}): {:#?}",
        node_reads.len(),
        MAX_ANCHORED_NODE_PAGE_READS,
        node_reads
    );
}

/// The pruned inputs must not change answers: an anchored pattern whose
/// non-anchor elements carry their own predicates still evaluates them,
/// and an anchor with no qualifying paths returns nothing.
#[tokio::test]
async fn anchored_pruning_preserves_results() {
    let dir = tempfile::tempdir().unwrap();
    let reads = Arc::new(Mutex::new(Vec::new()));
    build_graph(dir.path(), &reads).await;

    let db = Db::open_with(
        &counting_config(dir.path()),
        &registries(dir.path(), &reads),
    )
    .await
    .unwrap();

    // Non-anchor predicate on a pruned element still filters.
    let batches = db
        .query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) \
             WHERE a._id = 0 AND c._id = 3 RETURN c._id",
        )
        .await
        .unwrap();
    assert_eq!(ids(&batches), BTreeSet::from([3]));

    // An anchor with no outgoing edges yields the empty result, not an error.
    let batches = db
        .query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) \
             WHERE a._id = 20 RETURN c._id",
        )
        .await
        .unwrap();
    assert_eq!(ids(&batches), BTreeSet::new());

    // Quantified: anchor island only, full label scan would also see 5→6.
    let batches = db
        .query(
            "MATCH (a:Person)-[:KNOWS]->{1,3}(b:Person) \
             WHERE a._id = 0 RETURN b._id",
        )
        .await
        .unwrap();
    assert_eq!(ids(&batches), BTreeSet::from([1, 2, 3]));
}
