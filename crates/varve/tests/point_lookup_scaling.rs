//! Does an anchored iid POINT lookup cost scale with TABLE SIZE rather than with
//! the degree of the node looked up?
//!
//! `docs/benchmarks/v1.md` recorded this as suspected but **not established**,
//! bearing on the §13 "warm 2-hop < 50 ms" number, and named three possible
//! artefacts of the corpus it was seen in (one compacted trie, scattered derived
//! iids, `max_block_rows = 2000`). These tests answer it by COUNTING object-store
//! reads rather than by timing, which takes the machine and the fixture out of
//! the question.
//!
//! **Answer: no — not in the compacted steady state.** Fully compacted, with an
//! edge table present, reads are flat as the table grows 16x (degree held
//! constant by chaining `0 -> 1 -> 2 -> ...`):
//!
//! | nodes | 0-hop reads | 1-hop reads |
//! |-------|-------------|-------------|
//! |   128 |           1 |           1 |
//! |   512 |           1 |           1 |
//! |  2048 |           1 |           2 |
//!
//! What IS real is that **pruning depends on compaction**, which matches the
//! existing note that a full sweep "drains every full-iid-space L0 trie so
//! anchored node/edge scans can prune pages". A freshly flushed L0 block spans
//! the whole iid space, so page selection — which tests a page's `[min, max]` iid
//! range — can exclude nothing, because derived iids are hash-distributed and any
//! page holding more than a row or two spans nearly the entire space. Uncompacted
//! at 512 nodes, a 0-hop lookup reads:
//!
//! | rows per page | pages | pages read |
//! |---------------|-------|------------|
//! | 1             |   512 |          1 |
//! | 4             |   128 |         91 |
//! | 16            |    32 |         32 |
//! | 64            |     8 |          8 |
//!
//! One row per page prunes perfectly (the range IS a point); by 16 rows per page
//! every page is read. Compact the same data and it drops to one page at every
//! size. So the load -> compact -> serve procedure is not a nicety: serving
//! uncompacted, a point lookup is O(pages) and therefore linear in table size.
//!
//! **Two harness faults will fake a linear-in-table-size result. Check both
//! before concluding anything here.** `wait_for_manifest_count` counts
//! `.manifest` objects across ALL tables and returns on `>=`, so with edges
//! present it is satisfied before the node blocks finish flushing; and a single
//! `compact_full_once` can leave work behind. Either one leaves a
//! partly-uncompacted layout whose reads do track table size — which also looks
//! like evidence that compaction does not help.
//! `compaction_is_what_makes_point_lookups_prune` asserts the object count
//! actually falls, so a no-op compaction cannot be read as a negative result.
//!
//! These tests establish read COUNTS, not wall clock — the stronger metric for an
//! object-store-backed read, but a different one. The earlier timing observation
//! that prompted this (0-hop 1.15 ms -> 4.0 ms at 4.5x table growth) does not
//! reproduce here; on this evidence that corpus was most likely also only partly
//! compacted.
//!
//! Every test is `#[ignore]`d: they insert up to 2048 nodes one statement at a
//! time and take ~30-70 s each, which does not belong in the default gate. Run
//! them with `cargo test -p varve --test point_lookup_scaling -- --ignored
//! --nocapture`.

#![allow(clippy::unwrap_used)]

use async_trait::async_trait;
use bytes::Bytes;
use std::collections::BTreeMap;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use varve::{Config, Db, Registries};
use varve_config::{ComponentFactory, ConfigSection, RegistryError};
use varve_storage::{ConditionalStore, ObjectStore, StorageError};
use varve_testkit::db_harness::{toml_escaped_path, wait_for_manifest_count};

/// Pages a point lookup may read: the one holding the anchor, plus headroom for
/// a boundary case. Independent of table size — that is the whole claim.
const POINT_LOOKUP_PAGE_BUDGET: usize = 4;

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
        _ctx: &(),
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
    fn durability(&self) -> varve_types::Durability {
        self.inner.durability()
    }
    async fn put(&self, key: &str, bytes: Bytes) -> Result<(), StorageError> {
        self.inner.put(key, bytes).await
    }

    async fn get(&self, key: &str) -> Result<Bytes, StorageError> {
        self.reads.lock().unwrap().push(format!("GET {key}"));
        self.inner.get(key).await
    }

    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Bytes, StorageError> {
        self.reads.lock().unwrap().push(format!("RANGE {key}"));
        self.inner.get_range(key, range).await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
        self.reads.lock().unwrap().push(format!("LIST {prefix}"));
        self.inner.list(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), StorageError> {
        self.inner.delete(key).await
    }

    fn conditional(&self) -> Option<&dyn ConditionalStore> {
        self.inner.conditional()
    }
}

fn config(root: &Path, max_block_rows: usize) -> Config {
    let log_dir = toml_escaped_path(&root.join("log"));
    Config::from_toml_str(&format!(
        "[log]\n\
         backend = \"local\"\n\
         group_commit_window_ms = 1\n\
         [log.local]\n\
         dir = {log_dir}\n\
         [storage]\n\
         backend = \"counting\"\n\
         max_block_rows = {max_block_rows}\n"
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

fn node_data_page_reads(reads: &Arc<Mutex<Vec<String>>>) -> usize {
    reads
        .lock()
        .unwrap()
        .iter()
        .filter(|entry| entry.contains("/tables/nodes/data/"))
        .count()
}

/// Coarse category per logged key, so counts stay comparable across sizes.
fn tally(reads: &Arc<Mutex<Vec<String>>>) -> BTreeMap<String, usize> {
    let mut out = BTreeMap::new();
    for entry in reads.lock().unwrap().iter() {
        let (verb, key) = entry.split_once(' ').unwrap();
        let bucket = [
            "/tables/nodes/data/",
            "/tables/nodes/meta/",
            "/tables/edges/data/",
            "/tables/edges/meta/",
            "/manifest",
            "/catalog",
        ]
        .into_iter()
        .find(|marker| key.contains(marker))
        .unwrap_or("other");
        *out.entry(format!("{verb} {bucket}")).or_insert(0) += 1;
    }
    out
}

/// `people` nodes chained `0 -> 1 -> 2 -> ...`, so every node has degree <= 2 no
/// matter how large the table gets. Degree is held constant; only size varies.
async fn build(
    root: &Path,
    reads: &Arc<Mutex<Vec<String>>>,
    people: i64,
    max_block_rows: usize,
    edges: bool,
    compact: bool,
) {
    let db = Db::open_with(&config(root, max_block_rows), &registries(root, reads))
        .await
        .unwrap();
    for id in 0..people {
        db.execute(&format!("INSERT (:Person {{_id: {id}}})"))
            .await
            .unwrap();
    }
    if edges {
        for src in 0..people - 1 {
            db.execute(&format!(
                "MATCH (a:Person {{_id: {src}}}), (b:Person {{_id: {dst}}}) \
                 INSERT (a)-[:KNOWS]->(b)",
                dst = src + 1
            ))
            .await
            .unwrap();
        }
    }
    wait_for_manifest_count(root, (people as usize).div_ceil(max_block_rows)).await;
    if compact {
        // To IDLE, not once: a single `compact_full_once` can leave work behind,
        // and the manifest wait above can return early (see the file header).
        for _ in 0..16 {
            if db.compact_full_once().await.unwrap().jobs == 0 {
                break;
            }
        }
    }
    drop(db);
}

/// Nodes-table data pages a 0-hop point lookup reads, from a freshly opened `Db`
/// so nothing is hidden by state warmed during the build.
async fn point_lookup_pages(people: i64, max_block_rows: usize, compact: bool) -> usize {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let reads = Arc::new(Mutex::new(Vec::new()));
    build(root, &reads, people, max_block_rows, false, compact).await;

    let db = Db::open_with(&config(root, max_block_rows), &registries(root, &reads))
        .await
        .unwrap();
    reads.lock().unwrap().clear();
    let batches = db
        .query("MATCH (a:Person) WHERE a._id = 0 RETURN a._id")
        .await
        .unwrap();
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    let pages = node_data_page_reads(&reads);
    eprintln!(
        "people={people:>5} rows_per_page={max_block_rows:<3} compacted={compact:<5} \
         pages_of_{total:<4} read={pages:<4} {tally:?}",
        total = (people as usize).div_ceil(max_block_rows),
        tally = tally(&reads)
    );
    pages
}

/// The headline invariant: a point lookup names one iid, so once the data is
/// compacted it reads a constant number of pages however large the table is.
#[tokio::test]
#[ignore = "slow (~40 s): inserts 2048 nodes one statement at a time"]
async fn point_lookup_prunes_to_one_page_regardless_of_table_size() {
    let small = point_lookup_pages(128, 64, true).await;
    let large = point_lookup_pages(2048, 64, true).await;

    for (people, pages) in [(128, small), (2048, large)] {
        assert!(
            pages <= POINT_LOOKUP_PAGE_BUDGET,
            "point lookup read {pages} pages at {people} nodes \
             (budget {POINT_LOOKUP_PAGE_BUDGET})"
        );
    }
}

/// Compaction is what makes that pruning possible, and this pins BOTH halves so
/// neither can be mistaken for the other: the same data, same page size, reads
/// every page uncompacted and one page compacted.
#[tokio::test]
#[ignore = "slow (~30 s)"]
async fn compaction_is_what_makes_point_lookups_prune() {
    // 512 nodes at 64 rows per page = 8 pages.
    let uncompacted = point_lookup_pages(512, 64, false).await;
    let compacted = point_lookup_pages(512, 64, true).await;

    assert_eq!(
        uncompacted, 8,
        "expected an uncompacted point lookup to read every one of the 8 pages"
    );
    assert!(
        compacted <= POINT_LOOKUP_PAGE_BUDGET,
        "expected compaction to restore pruning, but the lookup read \
         {compacted} pages (budget {POINT_LOOKUP_PAGE_BUDGET})"
    );
}

/// The traversal version, and the one that bears on §13: a 1-hop off a point
/// anchor with an edge table present, fully compacted. Reads are split by
/// category so node-side and edge-side scaling can be told apart.
#[tokio::test]
#[ignore = "diagnostic, ~70 s: prints the 1-hop read table in this file's header"]
async fn one_hop_reads_versus_table_size_compacted() {
    for people in [128, 512, 2048] {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let reads = Arc::new(Mutex::new(Vec::new()));
        build(root, &reads, people, 64, true, true).await;

        let db = Db::open_with(&config(root, 64), &registries(root, &reads))
            .await
            .unwrap();
        for (label, gql) in [
            ("0-hop", "MATCH (a:Person) WHERE a._id = 0 RETURN a._id"),
            (
                "1-hop",
                "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a._id = 0 RETURN b._id",
            ),
        ] {
            reads.lock().unwrap().clear();
            let batches = db.query(gql).await.unwrap();
            let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            let tally = tally(&reads);
            let total: usize = tally.values().sum();
            eprintln!("people={people:>5} {label} rows={rows} total_reads={total:<4} {tally:?}");
        }
    }
}

/// The mechanism behind the uncompacted case: rows per page is what decides
/// whether page pruning can work at all, at a FIXED table size.
#[tokio::test]
#[ignore = "diagnostic, ~50 s: prints the rows-per-page table in this file's header"]
async fn point_lookup_reads_versus_rows_per_page_uncompacted() {
    for rows_per_page in [1, 4, 16, 64] {
        point_lookup_pages(512, rows_per_page, false).await;
    }
}
