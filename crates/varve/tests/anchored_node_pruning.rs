//! Anchor-reachable NODE-scan pruning (follow-up to the slice-6/task-12
//! reachable-EDGE pruning): an anchored traversal must not full-scan the
//! nodes table for its non-anchor node elements. Each test builds 64
//! one-node blocks (max_block_rows = 1) behind a store that counts
//! `get_range` reads of nodes-table data pages, then asserts an anchored
//! traversal touches only pages that can hold anchor-reachable nodes —
//! while returning exactly the same rows as the unpruned scan would.

#![allow(clippy::unwrap_used)]

use arrow::array::{Array, Int64Array, StringArray};
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::BTreeSet;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use varve::{Config, Db, RecordBatch, Registries};
use varve_config::{ComponentFactory, ConfigSection, RegistryError};
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

fn strings(batches: &[RecordBatch]) -> BTreeSet<Option<String>> {
    let mut out = BTreeSet::new();
    for batch in batches {
        let col = batch.column(0);
        // A property absent from every row of the batch has no typed column to
        // read: the planner lowers the reference to a NULL literal, so the
        // projection is an all-null column of `Null` type rather than Utf8.
        // (`NullArray` carries no null buffer, so `is_null` reports false on
        // it — the data type is what says every value is null.)
        let Some(col) = col.as_any().downcast_ref::<StringArray>() else {
            assert_eq!(
                col.data_type(),
                &arrow::datatypes::DataType::Null,
                "unexpected result column type"
            );
            if !col.is_empty() {
                out.insert(None);
            }
            continue;
        };
        for i in 0..col.len() {
            out.insert((!col.is_null(i)).then(|| col.value(i).to_string()));
        }
    }
    out
}

/// 64 Person nodes in 64 one-row blocks; KNOWS edges 0→1, 1→2, 1→3 (the
/// anchor-reachable island) plus 5→6 and 10→11 elsewhere.
///
/// Every edge carries a `kind` property, so the fixed-hop fast path is
/// exercised on the shape real consumers use (GUAC models every edge type as
/// an edge property). `10→11` additionally carries `note`, a property NO
/// anchor-reachable edge has — that is the case where the pruned batch's
/// doc-column schema is genuinely narrower than the full edge scan's.
async fn build_graph(root: &Path, reads: &Arc<Mutex<Vec<String>>>) {
    let db = Db::open_with(&counting_config(root), &registries(root, reads))
        .await
        .unwrap();
    for id in 0..PEOPLE {
        db.execute(&format!("INSERT (:Person {{_id: {id}}})"))
            .await
            .unwrap();
    }
    for (src, dst, props) in [
        (0, 1, "kind: 'strong'"),
        (1, 2, "kind: 'strong'"),
        (1, 3, "kind: 'weak'"),
        (5, 6, "kind: 'strong'"),
        (10, 11, "kind: 'weak', note: 'unreachable'"),
    ] {
        db.execute(&format!(
            "MATCH (a:Person {{_id: {src}}}), (b:Person {{_id: {dst}}}) \
             INSERT (a)-[:KNOWS {{{props}}}]->(b)"
        ))
        .await
        .unwrap();
    }
    wait_for_manifest_count(root, PEOPLE as usize).await;
    drop(db);
}

/// Fresh `Db` over the same store, with the read log cleared — so no page
/// read is hidden by state warmed during setup.
async fn fresh_db(root: &Path, reads: &Arc<Mutex<Vec<String>>>) -> Db {
    let db = Db::open_with(&counting_config(root), &registries(root, reads))
        .await
        .unwrap();
    reads.lock().unwrap().clear();
    db
}

fn assert_pruned(reads: &Arc<Mutex<Vec<String>>>, what: &str) {
    let node_reads = node_page_reads(reads);
    assert!(
        node_reads.len() <= MAX_ANCHORED_NODE_PAGE_READS,
        "{what} read {} nodes-table data pages (expected <= {}): {:#?}",
        node_reads.len(),
        MAX_ANCHORED_NODE_PAGE_READS,
        node_reads
    );
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

/// `RETURN` modifiers must not disable pruning. `DISTINCT`/`ORDER BY`/`SKIP`/
/// `LIMIT` are applied after the pattern is matched, so they cannot change
/// which entities the anchor can reach — but the pruning aid used to reject
/// them outright (via `degenerate_query`), which first failed the query and
/// then, once that was fixed, silently downgraded it to a full scan. The
/// gallery blast-radius query is exactly this shape.
#[tokio::test]
async fn anchored_two_hop_with_distinct_and_limit_still_prunes() {
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
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) \
             WHERE a._id = 0 RETURN DISTINCT c._id ORDER BY c._id LIMIT 10",
        )
        .await
        .unwrap();
    assert_eq!(ids(&batches), BTreeSet::from([2, 3]));

    let node_reads = node_page_reads(&reads);
    assert!(
        node_reads.len() <= MAX_ANCHORED_NODE_PAGE_READS,
        "anchored 2-hop with DISTINCT/ORDER BY/LIMIT read {} nodes-table data \
         pages (expected <= {}): {:#?}",
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

/// An edge PROPERTY must not disable pruning either. The BFS deliberately
/// ignores inline props (collecting a wider, still-correct superset) and
/// `apply_element_predicates` re-applies them per element, so the pruned
/// batch filters to the same rows the full scan would. Every GUAC traversal
/// is this shape — GUAC models each edge type as an edge property.
#[tokio::test]
async fn anchored_two_hop_with_inline_edge_prop_still_prunes() {
    let dir = tempfile::tempdir().unwrap();
    let reads = Arc::new(Mutex::new(Vec::new()));
    build_graph(dir.path(), &reads).await;
    let db = fresh_db(dir.path(), &reads).await;

    let batches = db
        .query(
            "MATCH (a:Person)-[:KNOWS {kind: 'strong'}]->(b:Person)-[:KNOWS]->(c:Person) \
             WHERE a._id = 0 RETURN c._id",
        )
        .await
        .unwrap();
    assert_eq!(ids(&batches), BTreeSet::from([2, 3]));
    assert_pruned(&reads, "anchored 2-hop with an inline edge prop");
}

/// Same relaxation, reached through `WHERE` on a named edge variable rather
/// than an inline map. These two forms cost the same today because they hit
/// two adjacent bails with the same consequence.
#[tokio::test]
async fn anchored_two_hop_with_where_on_edge_var_still_prunes() {
    let dir = tempfile::tempdir().unwrap();
    let reads = Arc::new(Mutex::new(Vec::new()));
    build_graph(dir.path(), &reads).await;
    let db = fresh_db(dir.path(), &reads).await;

    let batches = db
        .query(
            "MATCH (a:Person)-[e:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) \
             WHERE a._id = 0 AND e.kind = 'strong' RETURN c._id",
        )
        .await
        .unwrap();
    assert_eq!(ids(&batches), BTreeSet::from([2, 3]));
    assert_pruned(&reads, "anchored 2-hop with WHERE on an edge var");
}

/// Projecting an edge property must not disable pruning.
#[tokio::test]
async fn anchored_hop_returning_edge_prop_still_prunes() {
    let dir = tempfile::tempdir().unwrap();
    let reads = Arc::new(Mutex::new(Vec::new()));
    build_graph(dir.path(), &reads).await;
    let db = fresh_db(dir.path(), &reads).await;

    let batches = db
        .query("MATCH (a:Person)-[e:KNOWS]->(b:Person) WHERE a._id = 0 RETURN e.kind")
        .await
        .unwrap();
    assert_eq!(
        strings(&batches),
        BTreeSet::from([Some("strong".to_string())])
    );
    assert_pruned(&reads, "anchored hop returning an edge prop");
}

/// The case the bail was guarding: `note` exists only on the unreachable
/// `10→11` edge, so the pruned batch has NO `note` column while the full edge
/// scan does. Both forms must still agree with the full scan — an absent
/// column lowers to a NULL literal, so the filter yields the empty result and
/// the projection yields NULL. Neither may become an `UnknownColumn` error.
#[tokio::test]
async fn edge_prop_absent_from_pruned_batch_matches_full_scan() {
    let dir = tempfile::tempdir().unwrap();
    let reads = Arc::new(Mutex::new(Vec::new()));
    build_graph(dir.path(), &reads).await;
    let db = fresh_db(dir.path(), &reads).await;

    // Filtering on it: no reachable edge can match, so the empty result.
    let batches = db
        .query(
            "MATCH (a:Person)-[e:KNOWS]->(b:Person) \
             WHERE a._id = 0 AND e.note = 'unreachable' RETURN b._id",
        )
        .await
        .unwrap();
    assert_eq!(ids(&batches), BTreeSet::new());

    // Projecting it: one row, NULL — the value the full scan also produces
    // for this edge, since only 10→11 carries `note`.
    let batches = db
        .query("MATCH (a:Person)-[e:KNOWS]->(b:Person) WHERE a._id = 0 RETURN e.note")
        .await
        .unwrap();
    assert_eq!(strings(&batches), BTreeSet::from([None]));

    // ...and it must survive the user-visible JSON boundary as an explicit
    // null. A `Null`-typed column is a shape the full scan never produces, so
    // the row writer has to handle it rather than error.
    let rows: Vec<_> = varve::rows(&batches).unwrap().collect();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("e.note"), Some(&serde_json::Value::Null));

    // The inline-map spelling of the same absent property.
    let batches = db
        .query(
            "MATCH (a:Person)-[:KNOWS {note: 'unreachable'}]->(b:Person) \
             WHERE a._id = 0 RETURN b._id",
        )
        .await
        .unwrap();
    assert_eq!(ids(&batches), BTreeSet::new());
}

/// Mixed hop DIRECTIONS prune too. Hops may differ in direction as long as
/// they share a label: the BFS drives each level from its own hop and switches
/// to per-level dedup when the families differ, so a node expanded at an `In`
/// level is still expanded at a later `Out` level (a global dedup would drop
/// its outgoing edges and break the superset). This is the shape the gallery
/// blast-radius query uses — four `<-` hops then four `->`.
#[tokio::test]
async fn mixed_direction_hops_with_edge_props_still_prune() {
    let dir = tempfile::tempdir().unwrap();
    let reads = Arc::new(Mutex::new(Vec::new()));
    build_graph(dir.path(), &reads).await;
    let db = fresh_db(dir.path(), &reads).await;

    // 2 <-[strong]- 1 -[*]-> {2, 3}: an `In` hop then an `Out` hop. Node 1 is
    // the far endpoint of the In level AND the node the Out level must expand,
    // so a global dedup would return only {} here.
    let batches = db
        .query(
            "MATCH (a:Person)<-[:KNOWS {kind: 'strong'}]-(b:Person)-[:KNOWS]->(c:Person) \
             WHERE a._id = 2 RETURN c._id",
        )
        .await
        .unwrap();
    assert_eq!(ids(&batches), BTreeSet::from([2, 3]));
    assert_pruned(&reads, "anchored mixed-direction 2-hop");
}

/// The re-expansion the per-level dedup exists for, in its starkest form:
/// `Out` then `In` off anchor 0. Hop 1 walks 0→1; hop 2 walks back In to 1's
/// predecessors, which is 0 — the ANCHOR, already expanded at level 0. A global
/// dedup would skip it and miss the answer entirely.
#[tokio::test]
async fn mixed_direction_revisits_the_anchor_on_a_later_level() {
    let dir = tempfile::tempdir().unwrap();
    let reads = Arc::new(Mutex::new(Vec::new()));
    build_graph(dir.path(), &reads).await;
    let db = fresh_db(dir.path(), &reads).await;

    let batches = db
        .query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person)<-[:KNOWS]-(c:Person) \
             WHERE a._id = 0 RETURN c._id",
        )
        .await
        .unwrap();
    assert_eq!(ids(&batches), BTreeSet::from([0]));
    assert_pruned(&reads, "anchored out-then-in 2-hop");

    // Three levels alternating, ending back where it can see 2 and 3:
    // 0 ->1 ; 1 <-0 ; 0 ->1 . The middle level re-expands the anchor.
    let batches = db
        .query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person)<-[:KNOWS]-(c:Person)-[:KNOWS]->(d:Person) \
             WHERE a._id = 0 RETURN d._id",
        )
        .await
        .unwrap();
    assert_eq!(ids(&batches), BTreeSet::from([1]));
}

/// Per-hop predicates on the SHARED reachable-edge batch: every `Edge` spec
/// is fed the same batch, so each hop's own props must be applied to its own
/// mangled copy and not leak across hops.
#[tokio::test]
async fn per_hop_edge_props_filter_independently_on_the_shared_batch() {
    let dir = tempfile::tempdir().unwrap();
    let reads = Arc::new(Mutex::new(Vec::new()));
    build_graph(dir.path(), &reads).await;
    let db = fresh_db(dir.path(), &reads).await;

    // Hop 2 restricted to 'weak' selects 1→3 only.
    let batches = db
        .query(
            "MATCH (a:Person)-[:KNOWS {kind: 'strong'}]->(b:Person)\
             -[:KNOWS {kind: 'weak'}]->(c:Person) \
             WHERE a._id = 0 RETURN c._id",
        )
        .await
        .unwrap();
    assert_eq!(ids(&batches), BTreeSet::from([3]));

    // Hop 2 restricted to 'strong' selects 1→2 only.
    let batches = db
        .query(
            "MATCH (a:Person)-[:KNOWS {kind: 'strong'}]->(b:Person)\
             -[:KNOWS {kind: 'strong'}]->(c:Person) \
             WHERE a._id = 0 RETURN c._id",
        )
        .await
        .unwrap();
    assert_eq!(ids(&batches), BTreeSet::from([2]));

    // Hop 1 restricted to 'weak' has no match at the anchor at all.
    let batches = db
        .query(
            "MATCH (a:Person)-[:KNOWS {kind: 'weak'}]->(b:Person)-[:KNOWS]->(c:Person) \
             WHERE a._id = 0 RETURN c._id",
        )
        .await
        .unwrap();
    assert_eq!(ids(&batches), BTreeSet::new());
}

/// A `WHERE` conjunct that is not a property equality must not disable
/// pruning. A filter can only REMOVE rows, so whatever its shape it cannot
/// make the anchor reach further than the MATCH structure already says — the
/// prop-eq restriction was about what the aid was willing to reason about, not
/// about correctness.
#[tokio::test]
async fn anchored_two_hop_with_non_prop_eq_where_still_prunes() {
    let dir = tempfile::tempdir().unwrap();
    let reads = Arc::new(Mutex::new(Vec::new()));
    build_graph(dir.path(), &reads).await;
    let db = fresh_db(dir.path(), &reads).await;

    // Reachable ends are {2, 3}; `<>` removes 3.
    let batches = db
        .query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) \
             WHERE a._id = 0 AND c._id <> 3 RETURN c._id",
        )
        .await
        .unwrap();
    assert_eq!(ids(&batches), BTreeSet::from([2]));
    assert_pruned(&reads, "anchored 2-hop with a `<>` conjunct");

    // Other non-prop-eq spellings must agree with the unpruned result too.
    // (Reads accumulate in the shared log, so only the first query above
    // carries the page-count assertion — same convention as the tests below.)
    let batches = db
        .query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) \
             WHERE a._id = 0 AND c._id > 2 RETURN c._id",
        )
        .await
        .unwrap();
    assert_eq!(ids(&batches), BTreeSet::from([3]));

    let batches = db
        .query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) \
             WHERE a._id = 0 AND (c._id = 2 OR c._id = 3) RETURN c._id",
        )
        .await
        .unwrap();
    assert_eq!(ids(&batches), BTreeSet::from([2, 3]));

    // A non-eq conjunct on an edge DOC property: the pruned batch carries the
    // column, and the filter applies to it exactly as on a full scan.
    let batches = db
        .query(
            "MATCH (a:Person)-[e:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) \
             WHERE a._id = 0 AND e.kind <> 'weak' RETURN c._id",
        )
        .await
        .unwrap();
    assert_eq!(ids(&batches), BTreeSet::from([2, 3]));
}

/// A computed `RETURN` item must not disable pruning. Pruning narrows which
/// ROWS the shared edge batch holds; it never changes the values a row it does
/// hold carries. So an expression over those values — arithmetic, a literal, a
/// `CASE`, a cast, a nested function argument — is evaluated identically either
/// way, and the item's shape is not the aid's business.
#[tokio::test]
async fn anchored_two_hop_with_a_computed_return_item_still_prunes() {
    let dir = tempfile::tempdir().unwrap();
    let reads = Arc::new(Mutex::new(Vec::new()));
    build_graph(dir.path(), &reads).await;
    let db = fresh_db(dir.path(), &reads).await;

    // Reachable ends {2, 3}, each incremented.
    let batches = db
        .query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) \
             WHERE a._id = 0 RETURN c._id + 1",
        )
        .await
        .unwrap();
    assert_eq!(ids(&batches), BTreeSet::from([3, 4]));
    assert_pruned(&reads, "anchored 2-hop returning a computed item");

    // Other computed spellings must agree with the unpruned result. (Only the
    // first query carries the page-count assertion — reads accumulate.)
    let batches = db
        .query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) \
             WHERE a._id = 0 RETURN CASE WHEN c._id = 2 THEN 10 ELSE 20 END",
        )
        .await
        .unwrap();
    assert_eq!(ids(&batches), BTreeSet::from([10, 20]));

    // An aggregate over a computed argument — the nested-arg case the FnCall
    // whitelist rejected outright.
    let batches = db
        .query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) \
             WHERE a._id = 0 RETURN sum(c._id + 1)",
        )
        .await
        .unwrap();
    assert_eq!(ids(&batches), BTreeSet::from([7]));

    // A computed item over an edge DOC property, which lowers to NULL when the
    // pruned batch lacks the column entirely.
    let batches = db
        .query(
            "MATCH (a:Person)-[e:KNOWS]->(b:Person) \
             WHERE a._id = 0 RETURN upper(e.kind)",
        )
        .await
        .unwrap();
    assert_eq!(
        strings(&batches),
        BTreeSet::from([Some("STRONG".to_string())])
    );
}

/// The limit of the `RETURN` relaxation, and the reason the bare-edge-variable
/// bail stays. `project_bare_element` emits one output column per doc column the
/// batch actually HAS, so a narrower pruned schema would change the result's
/// COLUMN SET, not merely a value — `note` (carried only by the unreachable
/// 10→11 edge) would vanish from the projection. The unanchored full scan is the
/// oracle: the anchored form must produce the same columns.
#[tokio::test]
async fn returning_a_bare_edge_var_keeps_the_full_scans_column_set() {
    let dir = tempfile::tempdir().unwrap();
    let reads = Arc::new(Mutex::new(Vec::new()));
    build_graph(dir.path(), &reads).await;
    let db = fresh_db(dir.path(), &reads).await;

    let columns = |batches: &[RecordBatch]| -> Vec<String> {
        batches
            .first()
            .map(|b| {
                b.schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().to_string())
                    .collect()
            })
            .unwrap_or_default()
    };

    let full = db
        .query("MATCH (a:Person)-[e:KNOWS]->(b:Person) RETURN e")
        .await
        .unwrap();
    let anchored = db
        .query("MATCH (a:Person)-[e:KNOWS]->(b:Person) WHERE a._id = 0 RETURN e")
        .await
        .unwrap();
    assert!(
        columns(&full).contains(&"e.note".to_string()),
        "expected the full scan to carry `note`: {:?}",
        columns(&full)
    );
    assert_eq!(columns(&anchored), columns(&full));
}

/// The other half of the bare-variable bail. Unlike the `WHERE` position, a
/// temporal function DOES work in `RETURN` — so it must keep working. Its
/// argument is a bare `Expr::Var`, and `lower_temporal_fn` requires the mangled
/// structural column to be PRESENT, erroring rather than lowering to NULL the
/// way a doc property does. A `Null`-typed or null-valued result would mean the
/// column was lost, so both are rejected here.
#[tokio::test]
async fn returning_a_temporal_fn_on_an_edge_var_still_answers() {
    let dir = tempfile::tempdir().unwrap();
    let reads = Arc::new(Mutex::new(Vec::new()));
    build_graph(dir.path(), &reads).await;
    let db = fresh_db(dir.path(), &reads).await;

    // Anchor 0 reaches exactly one edge, 0→1.
    let batches = db
        .query("MATCH (a:Person)-[e:KNOWS]->(b:Person) WHERE a._id = 0 RETURN valid_from(e)")
        .await
        .unwrap();
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    let col = batches[0].column(0);
    assert_ne!(
        col.data_type(),
        &arrow::datatypes::DataType::Null,
        "valid_from(e) lowered to a NULL literal — the structural column was lost"
    );
    assert!(!col.is_null(0), "valid_from(e) came back NULL");
    assert_pruned(&reads, "anchored hop returning valid_from(e)");
}

/// A temporal function applied to a pattern element inside `WHERE` is
/// unsupported on EVERY path: `lower_temporal_fn` resolves the element through
/// `Scope`, which a `WHERE` residual does not populate for pattern variables —
/// true of node variables as much as edge ones, and true of the unanchored full
/// scan. (`RETURN valid_from(e)` does work; only the `WHERE` position does not.)
///
/// Dropping the prop-eq restriction lets this shape reach the fast path for the
/// first time, so what has to hold is that pruning never turns a right answer
/// into a wrong one. It does not — but the outcome is not uniform either, and
/// that is deliberate rather than overlooked:
///
///   * an anchor that reaches an edge errors exactly as the unanchored full
///     scan does, and
///   * an anchor that reaches none succeeds with the EMPTY result, because the
///     pattern binds no rows and the plan short-circuits before the unsupported
///     expression is ever lowered. Empty is the true answer there — a filter
///     over zero rows yields zero rows — so this is a query that used to fail
///     now returning the correct result, not a wrong one.
///
/// Making the error uniform would mean re-adding a bail for a shape that is
/// unsupported on every path anyway, so the divergence is documented instead.
#[tokio::test]
async fn temporal_fn_in_where_never_yields_a_wrong_answer_when_pruned() {
    let dir = tempfile::tempdir().unwrap();
    let reads = Arc::new(Mutex::new(Vec::new()));
    build_graph(dir.path(), &reads).await;
    let db = fresh_db(dir.path(), &reads).await;

    let query = |anchor: Option<i64>| {
        let filter = match anchor {
            Some(id) => format!("a._id = {id} AND "),
            None => String::new(),
        };
        format!(
            "MATCH (a:Person)-[e:KNOWS]->(b:Person) \
             WHERE {filter}valid_from(e) IS NOT NULL RETURN b._id"
        )
    };

    // The oracle: unanchored, so no fast path, and unsupported.
    let unanchored = db.query(query(None)).await.unwrap_err().to_string();

    // Anchor 0 reaches 0→1, so a batch exists and the expression is lowered.
    let anchored = db.query(query(Some(0))).await.unwrap_err().to_string();
    assert_eq!(anchored, unanchored, "anchor 0 diverged from the full scan");

    // Anchor 20 reaches nothing, so there are no rows to filter.
    let batches = db.query(query(Some(20))).await.unwrap();
    assert_eq!(ids(&batches), BTreeSet::new());
}

/// An aggregate must not disable pruning. `count(*)` is applied after the
/// pattern binds — the same class as the `RETURN` modifiers above — so it
/// cannot change what the anchor reaches, and it references no element at all.
#[tokio::test]
async fn anchored_two_hop_returning_count_star_still_prunes() {
    let dir = tempfile::tempdir().unwrap();
    let reads = Arc::new(Mutex::new(Vec::new()));
    build_graph(dir.path(), &reads).await;
    let db = fresh_db(dir.path(), &reads).await;

    // Two qualifying paths, 0→1→2 and 0→1→3, so the count is 2 (read out of
    // column 0 by `ids`, which is a count here rather than a node id).
    let batches = db
        .query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) \
             WHERE a._id = 0 RETURN count(*)",
        )
        .await
        .unwrap();
    assert_eq!(ids(&batches), BTreeSet::from([2]));
    assert_pruned(&reads, "anchored 2-hop returning count(*)");
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

    // An anchor with no outgoing edges reaches no edge at all, so there is no
    // batch and hence no columns whatsoever — an edge-prop predicate must
    // still yield the empty result rather than failing to resolve `e.kind`.
    let batches = db
        .query(
            "MATCH (a:Person)-[:KNOWS {kind: 'strong'}]->(b:Person) \
             WHERE a._id = 20 RETURN b._id",
        )
        .await
        .unwrap();
    assert_eq!(ids(&batches), BTreeSet::new());
    let batches = db
        .query("MATCH (a:Person)-[e:KNOWS]->(b:Person) WHERE a._id = 20 RETURN e.kind")
        .await
        .unwrap();
    assert_eq!(strings(&batches), BTreeSet::new());

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
