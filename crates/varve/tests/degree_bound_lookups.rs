//! Anchored lookups must be degree-bound in BOTH residency states.
//!
//! Page pruning already gets an anchored adjacency lookup down to the one page
//! that can hold the anchor's run (`anchored_node_pruning.rs` pins that). This
//! file pins the half that page pruning does not cover: how much of that page
//! gets *materialized*. Decoding the whole page to answer a point question
//! costs O(`PAGE_LIMIT`) doc deserializations for an O(degree) answer, which is
//! what made the blast-radius gallery's 8-hop query ~460 ms against flushed
//! blocks and ~19 ms against the same rows in the writer's live table — a 30×
//! gap with the same binary, store, and query
//! (`docs/plans/2026-07-28-degree-bound-lookups.md`).
//!
//! So the assertions here are on `block_events_decoded` — a count, not a
//! millisecond — measured as a delta across one query against a store whose
//! rows are entirely block-resident. Two graph sizes run the same anchored
//! query over the same 2-degree anchor: the decoded count must not follow the
//! edge table.

#![allow(clippy::unwrap_used)]

use std::path::Path;

use varve::{Config, Db, Doc, EdgePut, NodePut, Value};
use varve_testkit::db_harness::{row_count, toml_escaped_path};

/// Rows per flushed block, and so rows per data page: at `PAGE_LIMIT` (1024)
/// a page is full, which is the case a whole-page decode is worst at and the
/// case a real store is normally in.
const BLOCK_ROWS: usize = 1024;

/// The anchor's out-degree. Small and fixed while the edge table grows — the
/// whole point of the measurement.
const ANCHOR_DEGREE: i64 = 2;

/// Events an anchored 1-hop query may materialize out of flushed blocks: the
/// anchor's node versions, its `ANCHOR_DEGREE` adjacency rows, and the node
/// versions of the reachable set (anchor + its neighbours), across however
/// many tries hold them.
///
/// Measured at **6** — one row per page over 6 pages — against **6,144** when
/// each of those pages was decoded whole. The bound sits an order of magnitude
/// above the real figure and two below the regression, so it fails on a return
/// to whole-page decoding without being brittle about trie layout.
const MAX_BLOCK_EVENTS_DECODED: u64 = 64;

/// `max_block_rows` fills pages; a short `flush_interval_ms` (default: 5
/// minutes) lets the background timer take the unflushed TAIL out to blocks
/// too, which is what `wait_for_empty_live_tables` waits on. Without that, a
/// reopened node replays the tail back into its live tables and the query
/// under test would be measuring live-resident rows again — the exact
/// confusion this plan's "how this was found" section is about.
fn block_resident_config(root: &Path) -> Config {
    let log_dir = toml_escaped_path(&root.join("log"));
    let store_dir = toml_escaped_path(&root.join("store"));
    Config::from_toml_str(&format!(
        "[log]\n\
         backend = \"local\"\n\
         group_commit_window_ms = 1\n\
         [log.local]\n\
         dir = {log_dir}\n\
         [storage]\n\
         backend = \"local\"\n\
         max_block_rows = {BLOCK_ROWS}\n\
         flush_interval_ms = 25\n\
         [storage.local]\n\
         dir = {store_dir}\n"
    ))
    .unwrap()
}

async fn wait_for_empty_live_tables(db: &Db) {
    for _ in 0..400 {
        if db.metrics().live_rows == 0 {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    panic!(
        "live tables never drained: {} rows still unflushed",
        db.metrics().live_rows
    );
}

fn pkg(id: i64) -> NodePut {
    let mut doc = Doc::new();
    doc.insert("_id".to_string(), Value::Int(id));
    doc.insert("purl".to_string(), Value::Str(format!("pkg:generic/p{id}")));
    NodePut {
        labels: vec!["Pkg".to_string()],
        doc,
        valid_from: None,
        valid_to: None,
    }
}

/// A dependency edge carrying a realistic doc: the cost being pinned is doc
/// deserialization, so an edge with no properties would understate it.
fn depends_on(src: i64, dst: i64) -> EdgePut {
    let mut doc = Doc::new();
    doc.insert(
        "dependency_type".to_string(),
        Value::Str("DIRECT".to_string()),
    );
    doc.insert(
        "justification".to_string(),
        Value::Str(format!("dep-{src}-{dst}")),
    );
    EdgePut {
        label: "DEPENDS_ON".to_string(),
        src: Value::Int(src),
        dst: Value::Int(dst),
        doc,
        valid_from: None,
        valid_to: None,
    }
}

/// `edges` DEPENDS_ON rows over `edges + 2` Pkg nodes, of which exactly
/// `ANCHOR_DEGREE` leave node 0, all of it flushed out to blocks.
async fn build(root: &Path, edges: i64) {
    let db = Db::open(block_resident_config(root)).await.unwrap();
    let nodes: Vec<NodePut> = (0..=edges + ANCHOR_DEGREE).map(pkg).collect();
    for chunk in nodes.chunks(500) {
        db.ingest(chunk.to_vec(), Vec::new()).await.unwrap();
    }

    let mut puts: Vec<EdgePut> = (1..=ANCHOR_DEGREE).map(|d| depends_on(0, d)).collect();
    // Filler: every other edge leaves a DIFFERENT node, so growing the table
    // grows the pages the anchor's page sits among without growing its degree.
    puts.extend((1..=edges - ANCHOR_DEGREE).map(|n| depends_on(n, n + 1)));
    for chunk in puts.chunks(500) {
        db.ingest(Vec::new(), chunk.to_vec()).await.unwrap();
    }

    wait_for_empty_live_tables(&db).await;
    drop(db);
}

/// Runs the anchored 1-hop query on a freshly opened `Db` over `root`'s store
/// — block-resident, because a fresh node starts with empty live tables — and
/// returns `(rows, block_pages_read, block_events_decoded)` for the query
/// alone, with everything open/recovery touched excluded.
async fn anchored_hop(root: &Path) -> (usize, u64, u64) {
    let db = Db::open(block_resident_config(root)).await.unwrap();
    let before = db.metrics();
    assert_eq!(
        before.live_rows, 0,
        "a reopened node must be block-resident for this measurement to mean anything"
    );

    let batches = db
        .query("MATCH (a:Pkg {_id: 0})-[:DEPENDS_ON]->(b:Pkg) RETURN b._id")
        .await
        .unwrap();

    let after = db.metrics();
    (
        row_count(&batches),
        after.block_pages_read - before.block_pages_read,
        after.block_events_decoded - before.block_events_decoded,
    )
}

/// The core pin. Both graphs share one 2-degree anchor; the second has 4× the
/// edges. The query must read pages (it is genuinely block-resident) yet
/// materialize a degree-sized handful of events, and the same handful at both
/// sizes.
#[tokio::test]
async fn a_block_resident_anchored_hop_decodes_its_degree_not_its_pages() {
    let small_dir = tempfile::tempdir().unwrap();
    let large_dir = tempfile::tempdir().unwrap();
    build(small_dir.path(), 3_000).await;
    build(large_dir.path(), 12_000).await;

    let (small_rows, small_pages, small_decoded) = anchored_hop(small_dir.path()).await;
    let (large_rows, large_pages, large_decoded) = anchored_hop(large_dir.path()).await;

    assert_eq!(
        (small_rows, large_rows),
        (ANCHOR_DEGREE as usize, ANCHOR_DEGREE as usize),
        "the anchored hop must still return the anchor's neighbours"
    );
    assert!(
        small_pages > 0 && large_pages > 0,
        "no block pages read ({small_pages}, {large_pages}) — the rows were not \
         block-resident, so this test measured nothing"
    );

    assert!(
        small_decoded <= MAX_BLOCK_EVENTS_DECODED,
        "3k-edge anchored hop materialized {small_decoded} block events \
         (expected <= {MAX_BLOCK_EVENTS_DECODED}) over {small_pages} pages"
    );
    assert!(
        large_decoded <= MAX_BLOCK_EVENTS_DECODED,
        "12k-edge anchored hop materialized {large_decoded} block events \
         (expected <= {MAX_BLOCK_EVENTS_DECODED}) over {large_pages} pages"
    );
    // 4× the edges, same degree: the decoded count must not follow the table.
    // Compared as a bound rather than for equality because trie COUNT differs
    // between the two stores, so per-trie probes differ by a few events.
    assert!(
        large_decoded <= small_decoded + ANCHOR_DEGREE as u64 * 4,
        "quadrupling the edge table took decoded block events {small_decoded} -> \
         {large_decoded}: an anchored lookup is still table-bound, not degree-bound"
    );
}
