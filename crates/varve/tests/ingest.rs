//! Bulk ingest — the xtdb-style data-op write path (`Db::ingest`). Ops are
//! plain data (`NodePut`/`EdgePut`), not GQL: no parse, no plan, and no
//! endpoint MATCH — edges reference their endpoints by `_id` and existence
//! is deliberately NOT verified (xtdb `put-docs` semantics; a dangling edge
//! simply never matches a traversal). Iids derive from `_id` exactly as GQL
//! INSERT derives them, so bulk-ingested and GQL-ingested data interoperate:
//! the oracle-equivalence test below is the proof.
#![allow(clippy::unwrap_used)]

use std::path::Path;

use varve::{Config, Db, Doc, EdgePut, EngineError, NodePut, Value};
use varve_testkit::fixture::social_graph;
use varve_testkit::oracle::{column_i64, OracleDir};
use varve_types::{Iid, Instant};

fn person(id: i64) -> NodePut {
    let mut doc = Doc::new();
    doc.insert("_id".to_string(), Value::Int(id));
    doc.insert("name".to_string(), Value::Str(format!("p{id}")));
    NodePut {
        labels: vec!["Person".to_string()],
        doc,
    }
}

fn knows(src: i64, dst: i64) -> EdgePut {
    EdgePut {
        label: "KNOWS".to_string(),
        src: Value::Int(src),
        dst: Value::Int(dst),
        doc: Doc::new(),
    }
}

fn local_config(dir: &Path) -> Config {
    let log_dir = format!("{:?}", dir.join("log").display().to_string());
    let store_dir = format!("{:?}", dir.join("store").display().to_string());
    Config::from_toml_str(&format!(
        "[log]\nbackend = \"local\"\ngroup_commit_window_ms = 1\n\
         [log.local]\ndir = {log_dir}\n\
         [storage]\nbackend = \"local\"\n\
         [storage.local]\ndir = {store_dir}\n"
    ))
    .unwrap()
}

fn rows(batches: &[varve::RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

/// The core interop pin: a graph ingested purely through `Db::ingest` answers
/// traversals identically to the oracle (and therefore identically to the
/// same fixture ingested through GQL — `social_graph.rs`'s pin).
#[tokio::test]
async fn bulk_ingest_matches_gql_oracle_for_traversals() {
    let g = social_graph(500, 3_000, 42);
    let db = Db::memory();

    let nodes: Vec<NodePut> = (0..g.people as i64).map(person).collect();
    for chunk in nodes.chunks(100) {
        db.ingest(chunk.to_vec(), Vec::new()).await.unwrap();
    }
    let edges: Vec<EdgePut> = g.edges.iter().map(|&(s, d)| knows(s, d)).collect();
    for chunk in edges.chunks(500) {
        db.ingest(Vec::new(), chunk.to_vec()).await.unwrap();
    }

    let oracle = g.oracle();
    let anchor = 0i64;
    let anchor_iid = Iid::derive("default", "nodes", &Value::Int(anchor).id_bytes().unwrap());
    let now = (
        Instant::from_micros(i64::MAX - 1),
        Instant::from_micros(i64::MAX),
    );

    let rows2 = db
        .query(format!(
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) \
             WHERE a._id = {anchor} RETURN c._id AS _id"
        ))
        .await
        .unwrap();
    let mut got2: Vec<i64> = column_i64(&rows2, "_id");
    got2.sort_unstable();
    let mut want2: Vec<i64> = oracle
        .walk(anchor_iid, "KNOWS", OracleDir::Out, 2, 2, now.0, now.1)
        .into_iter()
        .map(|(end, _)| g.node_id_of(end))
        .collect();
    want2.sort_unstable();
    assert_eq!(got2, want2, "2-hop friend-of-friend vs oracle");

    let rows13 = db
        .query(format!(
            "MATCH (a:Person)-[:KNOWS]->{{1,3}}(b:Person) \
             WHERE a._id = {anchor} RETURN b._id AS _id"
        ))
        .await
        .unwrap();
    let mut got13: Vec<i64> = column_i64(&rows13, "_id");
    got13.sort_unstable();
    let mut want13: Vec<i64> = oracle
        .walk(anchor_iid, "KNOWS", OracleDir::Out, 1, 3, now.0, now.1)
        .into_iter()
        .map(|(end, _)| g.node_id_of(end))
        .collect();
    want13.sort_unstable();
    assert_eq!(got13, want13, "{{1,3}} expansion vs oracle");
}

/// Acked bulk ingests are durable: nodes and edges (submitted in ONE call —
/// edges may land in the same tx as their endpoints) survive a reopen.
#[tokio::test]
async fn bulk_ingest_is_durable_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Db::open(local_config(dir.path())).await.unwrap();
        let nodes: Vec<NodePut> = (0..10).map(person).collect();
        let edges: Vec<EdgePut> = (0..5).map(|i| knows(i, i + 5)).collect();
        db.ingest(nodes, edges).await.unwrap();
    } // drop closes the writer; every acked tx is already durable

    let db = Db::open(local_config(dir.path())).await.unwrap();
    assert_eq!(
        rows(&db.query("MATCH (p:Person) RETURN p._id").await.unwrap()),
        10,
        "all bulk-ingested nodes survive reopen"
    );
    assert_eq!(
        rows(
            &db.query("MATCH (:Person)-[:KNOWS]->(b:Person) RETURN b._id")
                .await
                .unwrap()
        ),
        5,
        "all bulk-ingested edges survive reopen"
    );
}

/// The receipt reports the same side-effect counts GQL INSERT would: one
/// created node/relationship per put, `properties_set` excluding `_id`.
#[tokio::test]
async fn bulk_ingest_reports_side_effect_counts() {
    let db = Db::memory();
    let receipt = db
        .ingest((0..3).map(person).collect(), vec![knows(0, 1), knows(1, 2)])
        .await
        .unwrap();
    assert_eq!(receipt.side_effects.nodes_created, 3);
    assert_eq!(receipt.side_effects.relationships_created, 2);
    // each person carries one non-`_id` property (`name`); edges carry none.
    assert_eq!(receipt.side_effects.properties_set, 3);
}

/// An empty batch is not a mutation — same contract as an empty GQL program.
#[tokio::test]
async fn empty_bulk_ingest_is_rejected() {
    let db = Db::memory();
    let result = db.ingest(Vec::new(), Vec::new()).await;
    assert!(
        matches!(result, Err(EngineError::NotAMutation)),
        "empty bulk ingest must be NotAMutation, got {result:?}"
    );
}

/// Puts without `_id` get distinct generated ids (same scheme as GQL INSERT).
#[tokio::test]
async fn bulk_nodes_without_ids_get_distinct_generated_ids() {
    let db = Db::memory();
    let anonymous = |name: &str| {
        let mut doc = Doc::new();
        doc.insert("name".to_string(), Value::Str(name.to_string()));
        NodePut {
            labels: vec!["Person".to_string()],
            doc,
        }
    };
    db.ingest(vec![anonymous("a"), anonymous("b")], Vec::new())
        .await
        .unwrap();
    let batches = db.query("MATCH (p:Person) RETURN p._id").await.unwrap();
    assert_eq!(
        rows(&batches),
        2,
        "two anonymous puts are two distinct nodes"
    );
}

/// Re-putting an existing `_id` supersedes the previous version (upsert by
/// id — xtdb `put-docs` semantics; also what GQL INSERT of a duplicate `_id`
/// does, since the iid derivation is identical).
#[tokio::test]
async fn bulk_put_of_existing_id_supersedes() {
    let db = Db::memory();
    db.ingest(vec![person(1)], Vec::new()).await.unwrap();

    let mut doc = Doc::new();
    doc.insert("_id".to_string(), Value::Int(1));
    doc.insert("name".to_string(), Value::Str("renamed".to_string()));
    db.ingest(
        vec![NodePut {
            labels: vec!["Person".to_string()],
            doc,
        }],
        Vec::new(),
    )
    .await
    .unwrap();

    let batches = db
        .query("MATCH (p:Person) WHERE p._id = 1 RETURN p.name AS name")
        .await
        .unwrap();
    assert_eq!(rows(&batches), 1, "one visible version after re-put");
    let names: Vec<String> = batches
        .iter()
        .flat_map(|batch| {
            let col = batch
                .column_by_name("name")
                .expect("name column")
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StringArray>()
                .expect("utf8 name column")
                .iter()
                .map(|v| v.unwrap().to_string())
                .collect::<Vec<_>>();
            col
        })
        .collect();
    assert_eq!(names, vec!["renamed".to_string()]);
}
