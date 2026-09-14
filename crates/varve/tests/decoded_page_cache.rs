//! The decoded-page cache: a repeated block scan must not decode again.

#![allow(clippy::unwrap_used)]

use std::path::Path;

use varve::{Config, Db, Doc, NodePut, Value};
use varve_testkit::db_harness::{row_count, toml_escaped_path};

const BLOCK_ROWS: usize = 1024;
const NODES: i64 = 3_000;

fn block_resident_config(root: &Path, cache: &str) -> Config {
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
         dir = {store_dir}\n\
         [query]\n\
         decoded_page_cache_bytes = \"{cache}\"\n"
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

fn job(id: i64) -> NodePut {
    let mut doc = Doc::new();
    doc.insert("_id".to_string(), Value::Int(id));
    doc.insert(
        "state".to_string(),
        Value::Str(if id % 100 == 0 { "queued" } else { "done" }.to_string()),
    );
    NodePut {
        labels: vec!["Job".to_string()],
        doc,
        valid_from: None,
        valid_to: None,
    }
}

async fn build(root: &Path) {
    let db = Db::open(block_resident_config(root, "256MiB"))
        .await
        .unwrap();
    let nodes: Vec<NodePut> = (0..NODES).map(job).collect();
    for chunk in nodes.chunks(500) {
        db.ingest(chunk.to_vec(), Vec::new()).await.unwrap();
    }
    wait_for_empty_live_tables(&db).await;
    drop(db);
}

struct Run {
    rows: usize,
    pages_read: u64,
    decoded: u64,
    cached: u64,
}

async fn run(db: &Db, gql: &str) -> Run {
    let before = db.metrics();
    let batches = db.query(gql).await.unwrap();
    let after = db.metrics();
    Run {
        rows: row_count(&batches),
        pages_read: after.block_pages_read - before.block_pages_read,
        decoded: after.block_events_decoded - before.block_events_decoded,
        cached: after.block_pages_cached - before.block_pages_cached,
    }
}

#[tokio::test]
async fn a_repeated_label_scan_decodes_nothing_the_second_time() {
    let dir = tempfile::tempdir().unwrap();
    build(dir.path()).await;
    let db = Db::open(block_resident_config(dir.path(), "256MiB"))
        .await
        .unwrap();
    assert_eq!(db.metrics().live_rows, 0);

    let q = "MATCH (j:Job {state: 'queued'}) RETURN j._id";
    let first = run(&db, q).await;
    assert_eq!(first.rows, (NODES / 100) as usize);
    assert!(first.pages_read > 0);
    assert_eq!(first.decoded, NODES as u64);
    assert_eq!(first.cached, 0);

    let second = run(&db, q).await;
    assert_eq!(second.rows, first.rows);
    assert_eq!(second.pages_read, first.pages_read);
    assert_eq!(
        second.decoded, 0,
        "second scan must hit the decoded-page cache"
    );
    assert_eq!(second.cached, first.pages_read);

    // A point lookup after a full scan filters the cached pages: no decode,
    // same answer.
    let point = run(&db, "MATCH (j:Job {_id: 100}) RETURN j.state").await;
    assert_eq!(point.rows, 1);
    assert_eq!(point.decoded, 0);
    assert!(point.cached >= 1);
}

#[tokio::test]
async fn a_zero_budget_disables_the_cache() {
    let dir = tempfile::tempdir().unwrap();
    build(dir.path()).await;
    let db = Db::open(block_resident_config(dir.path(), "0B"))
        .await
        .unwrap();

    let q = "MATCH (j:Job {state: 'queued'}) RETURN j._id";
    let first = run(&db, q).await;
    let second = run(&db, q).await;
    assert_eq!(second.rows, first.rows);
    assert_eq!(second.decoded, first.decoded);
    assert_eq!(second.cached, 0);
}
