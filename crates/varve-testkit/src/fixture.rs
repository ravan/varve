//! Deterministic social-graph fixture (roadmap slice 6, task 11 — exit-shape
//! artifact). [`social_graph`] generates a `people`-node / `friendships`-edge
//! `Person`-`KNOWS` graph from a seeded PCG-style LCG: same seed ⇒ identical
//! `edges`, every run, on every machine (no OS randomness, no new
//! dependency). It underlies both the integration test
//! (`tests/social_graph.rs`, cross-checked against
//! [`crate::oracle::GraphOracle`]) and the perf-smoke bench
//! (`varve/examples/traversal_bench.rs`).
//!
//! Non-test library code in this crate is exempt from the workspace
//! `unwrap_used`/`expect_used` deny (matches `oracle.rs`/`backends.rs`):
//! [`SocialGraph::node_id_of`]'s panic-on-unknown-iid is deliberate
//! test-infra behaviour, mirroring `oracle::ArbGraph::node_id`.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeMap;

use varve_index::{Event, Op};
use varve_types::{Doc, Iid, Instant};

use crate::oracle::{edge_iid, node_iid, GraphOracle};

/// Deterministic social graph: `people` Person nodes (`_id` `0..people`) and
/// `friendships` KNOWS edges from a seeded LCG (no self-loops, duplicates
/// allowed — they're distinct edges). Same seed ⇒ same graph, every run.
pub struct SocialGraph {
    pub people: usize,
    pub edges: Vec<(i64, i64)>,
    /// Node iid -> `_id`, precomputed once (dense ids, `Iid` is a one-way
    /// hash so this can't be inverted on demand) — mirrors `ArbGraph::ids`.
    ids: BTreeMap<Iid, i64>,
}

/// One PCG-style LCG step (constants per Global Constraint: deterministic,
/// no new dependency). Advances `state` and returns a draw in `0..bound`
/// from the high bits — an LCG's low bits are low-quality, so the draw uses
/// the top 32.
fn next_draw(state: &mut u64, bound: usize) -> usize {
    *state = state
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    ((*state >> 32) as usize) % bound
}

/// Deterministic social graph: `people` nodes, `friendships` KNOWS edges from
/// a seeded LCG. No self-loops (`s != d`); duplicate `(s, d)` pairs are
/// allowed — each is a distinct edge (multi-edge, matching `ArbGraph`'s
/// convention). Same `(people, friendships, seed)` ⇒ identical `edges`.
pub fn social_graph(people: usize, friendships: usize, seed: u64) -> SocialGraph {
    let mut state = seed;
    let mut edges = Vec::with_capacity(friendships);
    while edges.len() < friendships {
        let s = next_draw(&mut state, people);
        let d = next_draw(&mut state, people);
        if s != d {
            edges.push((s as i64, d as i64));
        }
    }
    let ids = (0..people as i64).map(|id| (node_iid(id), id)).collect();
    SocialGraph { people, edges, ids }
}

impl SocialGraph {
    /// `batch` nodes per multi-node INSERT statement (fewer, larger
    /// transactions than one-node-per-tx — mirrors `block_bench.rs`'s
    /// `insert_statement`). `people` need not be a multiple of `batch`; the
    /// last chunk is simply smaller.
    pub fn node_statements(&self, batch: usize) -> Vec<String> {
        let mut out = Vec::new();
        let mut start = 0usize;
        while start < self.people {
            let end = (start + batch).min(self.people);
            let parts: Vec<String> = (start..end)
                .map(|id| format!("(:Person {{_id: {id}, name: 'p{id}'}})"))
                .collect();
            out.push(format!("INSERT {}", parts.join(", ")));
            start = end;
        }
        out
    }

    /// One `MATCH (a:Person {_id: s}), (b:Person {_id: d}) INSERT
    /// (a)-[:KNOWS]->(b)` statement per edge — the v1 mutation surface
    /// (multi-edge INSERT bodies land with slice 7's statement blocks), so
    /// each edge is its own transaction.
    pub fn edge_statements(&self) -> Vec<String> {
        self.edges
            .iter()
            .map(|(s, d)| {
                format!(
                    "MATCH (a:Person {{_id: {s}}}), (b:Person {{_id: {d}}}) \
                     INSERT (a)-[:KNOWS]->(b)"
                )
            })
            .collect()
    }

    /// The same graph as a [`GraphOracle`] (nodes + edges, valid from the
    /// epoch to `Instant::END_OF_TIME` — "valid ALL" — so any current-time
    /// probe sees every node/edge regardless of the engine's actual insert
    /// wall-clock `VALID FROM`; only the ordering relative to the probe
    /// matters, per `oracle::ArbGraph`'s convention). Edge iids are unique
    /// placeholders (`edge_iid(k)` by index): the fixture's GQL never sets
    /// an edge `_id`, so the oracle's edge iids can't and don't need to
    /// match the engine's auto-assigned ones — only the walk's resulting
    /// node ids are ever compared (via [`SocialGraph::node_id_of`]).
    pub fn oracle(&self) -> GraphOracle {
        let mut oracle = GraphOracle::new();
        for id in 0..self.people as i64 {
            oracle.append_node(Event {
                iid: node_iid(id),
                system_from: Instant::from_micros(0),
                valid_from: Instant::from_micros(0),
                valid_to: Instant::END_OF_TIME,
                src: None,
                dst: None,
                op: Op::Put {
                    labels: vec!["Person".into()],
                    doc: Doc::new(),
                },
            });
        }
        for (k, &(s, d)) in self.edges.iter().enumerate() {
            oracle.append_edge(Event {
                iid: edge_iid(k as i64),
                system_from: Instant::from_micros(k as i64),
                valid_from: Instant::from_micros(0),
                valid_to: Instant::END_OF_TIME,
                src: Some(node_iid(s)),
                dst: Some(node_iid(d)),
                op: Op::Put {
                    labels: vec!["KNOWS".into()],
                    doc: Doc::new(),
                },
            });
        }
        oracle
    }

    /// The `_id` of a node iid; mirrors `oracle::ArbGraph::node_id`. Panics
    /// (debug builds too — deliberate test-infra behaviour) on an unknown
    /// iid: a walk over this graph's oracle only ever ends on one of its own
    /// nodes.
    pub fn node_id_of(&self, iid: Iid) -> i64 {
        *self
            .ids
            .get(&iid)
            .expect("SocialGraph::node_id_of: iid is not a node of this graph")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixture_is_deterministic_and_shaped() {
        let a = social_graph(10_000, 60_000, 42);
        let b = social_graph(10_000, 60_000, 42);
        assert_eq!(a.edges, b.edges);
        assert_eq!(a.edges.len(), 60_000);
        assert!(a
            .edges
            .iter()
            .all(|(s, d)| s != d && (0..10_000).contains(s) && (0..10_000).contains(d)));
        assert_ne!(social_graph(10_000, 60_000, 43).edges, a.edges);
        assert_eq!(a.node_statements(1000).len(), 10);
        assert_eq!(a.edge_statements().len(), 60_000);
    }
}
