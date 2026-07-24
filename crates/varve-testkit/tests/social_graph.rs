//! Roadmap slice-6 exit-shape artifact (task 11): a real `Db`, driven
//! through chunked bulk ingestion over the deterministic social-graph fixture
//! (`varve_testkit::fixture::social_graph`), cross-checked against the
//! traversal oracle (task 10) for 2-hop friend-of-friend AND `{1,3}` KNOWS
//! expansion from anchor `_id 0`. Complements `traversal_oracle.rs`'s random
//! `arb_graph` property suites with one FIXED, deterministic, roadmap-shaped
//! graph — the same content the perf-smoke bench
//! (`varve/examples/traversal_bench.rs`) ingests at full 10k/60k scale.
//!
//! This test keeps the full `social_graph(2_000, 12_000, 42)` fixture used by
//! the suite, but submits nodes and edges in bounded batches. That avoids the
//! former ~12k sequential GQL edge transactions while exercising the same
//! writer and preserving the exact graph queried below.
#![allow(clippy::unwrap_used)]

use std::time::Instant as WallInstant;

use varve::{Doc, EdgePut, NodePut};
use varve_testkit::fixture::social_graph;
use varve_testkit::oracle::{column_i64, OracleDir};
use varve_types::{Iid, Instant, Value};

const PEOPLE: usize = 2_000;
const FRIENDSHIPS: usize = 12_000;
const SEED: u64 = 42;
const NODE_BATCH: usize = 1_000;
const EDGE_BATCH: usize = 1_000;

fn person(id: i64) -> NodePut {
    let mut doc = Doc::new();
    doc.insert("_id".to_string(), Value::Int(id));
    doc.insert("name".to_string(), Value::Str(format!("p{id}")));
    NodePut {
        labels: vec!["Person".to_string()],
        doc,
        valid_from: None,
        valid_to: None,
    }
}

fn knows(src: i64, dst: i64) -> EdgePut {
    EdgePut {
        label: "KNOWS".to_string(),
        src: Value::Int(src),
        dst: Value::Int(dst),
        doc: Doc::new(),
        valid_from: None,
        valid_to: None,
    }
}

/// Roadmap exit shape: 2-hop friend-of-friend and {1,3} over the deterministic
/// fixture, with both answers cross-checked against the oracle.
#[tokio::test]
async fn fixture_two_hop_and_quantified_match_oracle() {
    let started = WallInstant::now();
    let g = social_graph(PEOPLE, FRIENDSHIPS, SEED);
    let db = varve::Db::memory();

    for start in (0..g.people).step_by(NODE_BATCH) {
        let end = (start + NODE_BATCH).min(g.people);
        let nodes = (start..end).map(|id| person(id as i64)).collect();
        db.ingest(nodes, Vec::new()).await.unwrap();
    }
    for edges in g.edges.chunks(EDGE_BATCH) {
        let edges = edges.iter().map(|&(src, dst)| knows(src, dst)).collect();
        db.ingest(Vec::new(), edges).await.unwrap();
    }
    eprintln!(
        "fixture_two_hop_and_quantified_match_oracle: ingested {} nodes / {} edges in {:.2?}",
        g.people,
        g.edges.len(),
        started.elapsed()
    );

    // Build the oracle from the same fixture (valid ALL, current system).
    let oracle = g.oracle();
    let anchor = 0i64;
    let anchor_iid = Iid::derive("default", "nodes", &Value::Int(anchor).id_bytes().unwrap());
    let now = (
        Instant::from_micros(i64::MAX - 1),
        Instant::from_micros(i64::MAX),
    );

    let rows = db
        .query(format!(
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) WHERE a._id = {anchor} RETURN c._id AS _id"
        ))
        .await
        .unwrap();
    let mut got2: Vec<i64> = column_i64(&rows, "_id");
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
            "MATCH (a:Person)-[:KNOWS]->{{1,3}}(b:Person) WHERE a._id = {anchor} RETURN b._id AS _id"
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
