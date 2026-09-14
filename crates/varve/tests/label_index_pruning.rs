//! The per-block label index: a labelled scan touches only entities that
//! carry the label, and never changes the answer.

#![allow(clippy::unwrap_used)]

use std::path::Path;

use varve::{Config, Db, Doc, NodePut, Value};
use varve_testkit::db_harness::{row_count, toml_escaped_path};

const BLOCK_ROWS: usize = 1024;
const DOCS: i64 = 3_000;
const JOBS: i64 = 5;

fn config(root: &Path, flush_interval_ms: u64) -> Config {
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
         flush_interval_ms = {flush_interval_ms}\n\
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
    panic!("live tables never drained");
}

fn node(label: &str, id: i64) -> NodePut {
    let mut doc = Doc::new();
    doc.insert("_id".to_string(), Value::Int(id));
    doc.insert("state".to_string(), Value::Str("queued".to_string()));
    NodePut {
        labels: vec![label.to_string()],
        doc,
        valid_from: None,
        valid_to: None,
    }
}

/// `DOCS` `:Doc` nodes and `JOBS` `:Job` nodes, all flushed to blocks.
async fn build(root: &Path) {
    let db = Db::open(config(root, 25)).await.unwrap();
    let mut nodes: Vec<NodePut> = (0..DOCS).map(|id| node("Doc", id)).collect();
    nodes.extend((0..JOBS).map(|id| node("Job", 100_000 + id)));
    for chunk in nodes.chunks(500) {
        db.ingest(chunk.to_vec(), Vec::new()).await.unwrap();
    }
    wait_for_empty_live_tables(&db).await;
    drop(db);
}

async fn run(db: &Db, gql: &str) -> (usize, u64) {
    let before = db.metrics();
    let batches = db.query(gql).await.unwrap();
    let after = db.metrics();
    (
        row_count(&batches),
        after.block_events_decoded - before.block_events_decoded,
    )
}

#[tokio::test]
async fn a_rare_label_scan_decodes_only_its_entities() {
    let dir = tempfile::tempdir().unwrap();
    build(dir.path()).await;
    let db = Db::open(config(dir.path(), 25)).await.unwrap();
    assert_eq!(db.metrics().live_rows, 0);

    let (rows, decoded) = run(&db, "MATCH (j:Job {state: 'queued'}) RETURN j._id").await;
    assert_eq!(rows, JOBS as usize);
    assert_eq!(
        decoded, JOBS as u64,
        "a :Job scan must not decode :Doc rows"
    );

    let (rows, _) = run(&db, "MATCH (n:Nobody) RETURN n._id").await;
    assert_eq!(rows, 0);

    // Unlabelled scan still sees everything.
    let (rows, _) = run(&db, "MATCH (n) RETURN n._id").await;
    assert_eq!(rows, (DOCS + JOBS) as usize);
}

#[tokio::test]
async fn relabelled_and_deleted_entities_are_not_returned_under_the_old_label() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(config(dir.path(), 25)).await.unwrap();
    db.ingest(vec![node("A", 1), node("A", 2), node("A", 3)], Vec::new())
        .await
        .unwrap();
    wait_for_empty_live_tables(&db).await;
    // Later block: entity 1 becomes :B, entity 2 is deleted.
    db.ingest(vec![node("B", 1)], Vec::new()).await.unwrap();
    db.execute("MATCH (n:A {_id: 2}) DETACH DELETE n")
        .await
        .unwrap();
    wait_for_empty_live_tables(&db).await;
    drop(db);

    let db = Db::open(config(dir.path(), 25)).await.unwrap();
    let a = db.query("MATCH (n:A) RETURN n._id").await.unwrap();
    assert_eq!(row_count(&a), 1, ":A must be only entity 3");
    let b = db.query("MATCH (n:B) RETURN n._id").await.unwrap();
    assert_eq!(row_count(&b), 1);
}

#[tokio::test]
async fn a_live_only_entity_is_still_found_by_its_label() {
    let dir = tempfile::tempdir().unwrap();
    build(dir.path()).await;
    // Long flush interval: the new node stays in the live table.
    let db = Db::open(config(dir.path(), 600_000)).await.unwrap();
    db.ingest(vec![node("Job", 999)], Vec::new()).await.unwrap();
    assert!(db.metrics().live_rows > 0);

    let (rows, _) = run(&db, "MATCH (j:Job) RETURN j._id").await;
    assert_eq!(rows, JOBS as usize + 1);
}

#[tokio::test]
async fn blocks_without_an_index_fall_back_and_compaction_backfills_it() {
    let dir = tempfile::tempdir().unwrap();
    build(dir.path()).await;
    let labels_dir = dir
        .path()
        .join("store")
        .join("v1/graphs/default/tables/nodes/labels");
    let indexes: Vec<_> = std::fs::read_dir(&labels_dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();
    assert!(!indexes.is_empty());
    for path in &indexes {
        std::fs::remove_file(path).unwrap();
    }

    let db = Db::open(config(dir.path(), 25)).await.unwrap();
    let (rows, decoded) = run(&db, "MATCH (j:Job) RETURN j._id").await;
    assert_eq!(rows, JOBS as usize);
    assert_eq!(decoded, (DOCS + JOBS) as u64, "no index means a full scan");

    // Full compaction rewrites the tries and writes the index for them.
    db.compact_full_once().await.unwrap();
    db.verify().await.unwrap();
    let (rows, decoded) = run(&db, "MATCH (j:Job) RETURN j._id").await;
    assert_eq!(rows, JOBS as usize);
    assert!(
        decoded <= JOBS as u64,
        "compacted blocks must prune again: {decoded}"
    );
}

#[tokio::test]
async fn verify_rejects_a_label_index_that_disagrees_with_its_block() {
    let dir = tempfile::tempdir().unwrap();
    build(dir.path()).await;
    let db = Db::open(config(dir.path(), 25)).await.unwrap();
    db.verify().await.unwrap();

    let labels_dir = dir
        .path()
        .join("store")
        .join("v1/graphs/default/tables/nodes/labels");
    let path = std::fs::read_dir(&labels_dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .next()
        .unwrap();
    // Replace with a valid but empty index; reopen so the byte cache is cold.
    drop(db);
    std::fs::write(&path, varve_index::LabelIndex::default().encode().unwrap()).unwrap();
    let db = Db::open(config(dir.path(), 25)).await.unwrap();
    let err = db.verify().await.unwrap_err().to_string();
    assert!(err.contains("label index"), "{err}");
}
