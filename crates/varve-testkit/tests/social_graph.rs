//! Roadmap slice-6 exit-shape artifact (task 11): a real `Db`, driven
//! through GQL over the deterministic social-graph fixture
//! (`varve_testkit::fixture::social_graph`), cross-checked against the
//! traversal oracle (task 10) for 2-hop friend-of-friend AND `{1,3}` KNOWS
//! expansion from anchor `_id 0`. Complements `traversal_oracle.rs`'s random
//! `arb_graph` property suites with one FIXED, deterministic, roadmap-shaped
//! graph — the same content the perf-smoke bench
//! (`varve/examples/traversal_bench.rs`) ingests at full 10k/60k scale.
//!
//! Fixture size: the brief's runtime guard fires here. The full 10k/60k
//! fixture (~70k sequential statements through the writer, debug build)
//! measured well over 5 minutes locally — nowhere near the ~90s budget — so
//! per the brief this test ships at the REDUCED `social_graph(2_000,
//! 12_000, 42)` size. The full 10k/60k fixture is exercised at release-build
//! speed by the bench (`varve/examples/traversal_bench.rs`) instead; see
//! task-11-report.md for both measured wall times.
#![allow(clippy::unwrap_used)]

use std::time::Instant as WallInstant;

use varve_testkit::fixture::social_graph;
use varve_testkit::oracle::{column_i64, OracleDir};
use varve_types::{Iid, Instant, Value};

/// Reduced fixture shape (see the module doc's runtime-guard note): still
/// dense enough to give the anchor a rich 2-hop/{1,3} neighborhood, small
/// enough to stay well inside a debug `cargo test` run.
const PEOPLE: usize = 2_000;
const FRIENDSHIPS: usize = 12_000;
const SEED: u64 = 42;

/// Roadmap exit shape: 2-hop friend-of-friend and {1,3} over the 10k/60k
/// fixture, answers cross-checked against the oracle. Ingest via GQL.
#[tokio::test]
async fn fixture_two_hop_and_quantified_match_oracle() {
    let started = WallInstant::now();
    let g = social_graph(PEOPLE, FRIENDSHIPS, SEED);
    let db = varve::Db::memory();
    for stmt in g.node_statements(1000) {
        db.execute(&stmt).await.unwrap();
    }
    for stmt in g.edge_statements() {
        db.execute(&stmt).await.unwrap();
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
        .query(&format!(
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) WHERE a._id = {anchor} RETURN c._id"
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
        .query(&format!(
            "MATCH (a:Person)-[:KNOWS]->{{1,3}}(b:Person) WHERE a._id = {anchor} RETURN b._id"
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
