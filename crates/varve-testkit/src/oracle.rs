//! Traversal oracle (roadmap slice 6, task 10) — an INDEPENDENT, naive graph
//! walker used to cross-check the database's traversal engine.
//!
//! [`GraphOracle`] answers "which nodes are reachable in `min..=max` hops from
//! a start node, at a bitemporal `(valid, system)` point" with a from-scratch
//! BFS ([`GraphOracle::walk`]) over visible edges ([`GraphOracle::neighbors`]).
//! It deliberately does NOT call `varve_plan::expand::expand_paths` — the
//! whole value of the property tests is that two independent implementations
//! agree. Bitemporal visibility reuses the already-equivalence-tested
//! [`ReferenceStore`] (slice 2), so the oracle adds no new bitemporal logic.
//!
//! [`arb_graph`] is a proptest strategy that emits a random graph BOTH as
//! replayable GQL ([`ArbGraph::inserts`]) and as a parallel [`GraphOracle`],
//! so the same content can be driven through a real `Db` and the oracle and
//! compared.
//!
//! Non-test library code in this crate is exempt from the workspace
//! `unwrap_used`/`expect_used` deny (matches `backends.rs`): the `id_bytes()`
//! of an integer `_id` is infallible, and `node_id`'s panic-on-unknown-iid is
//! deliberate test-infra behaviour.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeMap;

use proptest::prelude::*;
use varve_index::{Event, Op};
use varve_types::{Doc, Iid, Instant, Value};

use crate::ReferenceStore;

/// Traversal direction for [`GraphOracle::neighbors`]: `Out` follows an edge
/// from its `src` to its `dst`; `In` follows it backwards (`dst` to `src`).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum OracleDir {
    Out,
    In,
}

/// Naive in-memory bitemporal graph walker — the traversal oracle. Nodes and
/// edges each live in a [`ReferenceStore`]; `endpoints` records each edge's
/// immutable `(src, dst)` so [`neighbors`](Self::neighbors) can orient it.
#[derive(Default)]
pub struct GraphOracle {
    nodes: ReferenceStore,
    edges: ReferenceStore,
    endpoints: BTreeMap<Iid, (Iid, Iid)>, // edge -> (src, dst), immutable
}

impl GraphOracle {
    pub fn new() -> Self {
        Self::default()
    }

    /// Append a node event (arrival order). Node visibility never restricts
    /// traversal in the property suites (every node is inserted epoch-valid
    /// forever), but the store is kept faithful to the model.
    pub fn append_node(&mut self, event: Event) {
        self.nodes.append(event);
    }

    /// Append an edge event (arrival order) and record its endpoints. Panics
    /// (debug builds) if the event lacks either endpoint — an edges-table event
    /// always carries both (spec §5.2).
    pub fn append_edge(&mut self, event: Event) {
        debug_assert!(
            event.src.is_some() && event.dst.is_some(),
            "GraphOracle::append_edge requires an edge event with both endpoints"
        );
        if let (Some(src), Some(dst)) = (event.src, event.dst) {
            // Endpoints are immutable per edge iid; first writer wins (later
            // events for the same edge must agree).
            self.endpoints.entry(event.iid).or_insert((src, dst));
        }
        self.edges.append(event);
    }

    /// Edges with `label` visible at `(valid, system)` leaving `node`
    /// (`OracleDir::Out`) or entering it (`OracleDir::In`), as `(neighbor,
    /// edge)` pairs sorted by `(neighbor, edge)` — the same deterministic order
    /// `EdgeAdjacency` imposes.
    pub fn neighbors(
        &self,
        node: Iid,
        label: &str,
        dir: OracleDir,
        valid: Instant,
        system: Instant,
    ) -> Vec<(Iid, Iid)> {
        let mut out = Vec::new();
        for (&edge, &(src, dst)) in &self.endpoints {
            let (from, neighbor) = match dir {
                OracleDir::Out => (src, dst),
                OracleDir::In => (dst, src),
            };
            if from != node {
                continue;
            }
            // `visible_at` returns Some only for a visible `Op::Put`; filter by
            // its label set.
            if let Some(ev) = self.edges.visible_at(edge, valid, system) {
                if let Op::Put { labels, .. } = &ev.op {
                    if labels.iter().any(|l| l == label) {
                        out.push((neighbor, edge));
                    }
                }
            }
        }
        out.sort_unstable();
        out
    }

    /// WALK expansion of `min..=max` hops from `start`, returning `(end,
    /// interleaved [n0, e1, n1, …] path)` in breadth order. A naive
    /// from-scratch BFS that MIRRORS
    /// `varve_plan::expand::expand_paths`:
    /// WALK semantics (repeated nodes/edges allowed, termination by depth cap
    /// alone), `min == 0` yields the zero-length `(start, [start])`, and the
    /// per-node frontier order comes from `neighbors`' `(neighbor, edge)` sort.
    #[allow(clippy::too_many_arguments)]
    pub fn walk(
        &self,
        start: Iid,
        label: &str,
        dir: OracleDir,
        min: u32,
        max: u32,
        valid: Instant,
        system: Instant,
    ) -> Vec<(Iid, Vec<Iid>)> {
        let mut out = Vec::new();
        let mut frontier: Vec<(Iid, Vec<Iid>)> = vec![(start, vec![start])];
        if min == 0 {
            out.push((start, vec![start]));
        }
        for depth in 1..=max {
            let mut next = Vec::new();
            for (node, path) in &frontier {
                for (neighbor, edge) in self.neighbors(*node, label, dir, valid, system) {
                    let mut p = path.clone();
                    p.push(edge);
                    p.push(neighbor);
                    if depth >= min {
                        out.push((neighbor, p.clone()));
                    }
                    next.push((neighbor, p));
                }
            }
            if next.is_empty() {
                break;
            }
            frontier = next;
        }
        out
    }
}

/// Node iid derivation — MUST mirror the engine (`DEFAULT_GRAPH`/`NODES_TABLE`
/// over `Value::Int(_id).id_bytes()`). `pub(crate)`: `fixture.rs` reuses this
/// exact derivation for the social-graph fixture's node/oracle iids.
pub(crate) fn node_iid(id: i64) -> Iid {
    Iid::derive("default", "nodes", &Value::Int(id).id_bytes().unwrap())
}

/// Edge iid derivation — MUST mirror the engine (`DEFAULT_GRAPH`/`EDGES_TABLE`
/// over `Value::Int(_id).id_bytes()`). `pub(crate)`: `fixture.rs` reuses this
/// as a unique-per-edge placeholder iid (the fixture's GQL never sets an
/// edge `_id`, so the oracle's edge iids need not — and cannot — match the
/// engine's auto-assigned ones; only the resulting node ids are compared).
pub(crate) fn edge_iid(id: i64) -> Iid {
    Iid::derive("default", "edges", &Value::Int(id).id_bytes().unwrap())
}

/// Collects every value of an `Int64` column named `col` across `batches`,
/// preserving duplicates (WALK results are multiset — one row per path).
/// Shared by `tests/traversal_oracle.rs` and `tests/social_graph.rs` (was
/// `collect_i64`, duplicated per test file — task 11 hoisted it here so both
/// use one copy).
pub fn column_i64(batches: &[varve::RecordBatch], col: &str) -> Vec<i64> {
    use arrow::array::Int64Array;
    let mut out = Vec::new();
    for b in batches {
        let Some(arr_ref) = b.column_by_name(col) else {
            continue;
        };
        let arr: &Int64Array = arr_ref
            .as_any()
            .downcast_ref()
            .expect("column_i64: column is not Int64");
        for i in 0..arr.len() {
            out.push(arr.value(i));
        }
    }
    out
}

/// Renders a grid microsecond offset as an RFC3339 timestamp that the GQL
/// `TIMESTAMP '…'` parser round-trips exactly (µs precision, `Z` offset). Grid
/// instants are small non-negative µs; `Instant::END_OF_TIME` is never passed
/// here (open-ended edges omit the `TO` clause), and the fallback keeps this
/// total for any stray out-of-range value.
pub fn micros_to_rfc3339(us: i64) -> String {
    chrono::DateTime::<chrono::Utc>::from_timestamp_micros(us)
        .map(|dt| dt.to_rfc3339_opts(chrono::SecondsFormat::Micros, true))
        .unwrap_or_else(|| format!("{us}us"))
}

/// A random graph as GQL statements + a parallel [`GraphOracle`]. Nodes get
/// dense `_id`s `0..n` and label `"P"`; edges get label `"K"`, a valid range
/// from the `T_POOL` grid, and strictly increasing system order (the insertion
/// index).
pub struct ArbGraph {
    /// GQL, replayable against a `Db` in order (node insert first, then edges).
    pub inserts: Vec<String>,
    /// The same content as an oracle.
    pub oracle: GraphOracle,
    /// Dense node `_id`s `0..n`, for picking anchors.
    pub node_ids: Vec<i64>,
    /// Node iid -> `_id`, for oracle-vs-engine end-node comparison.
    ids: BTreeMap<Iid, i64>,
}

impl ArbGraph {
    /// The `_id` of a node iid. Panics on an unknown iid (test infra — a walk
    /// only ever ends on a node of this graph).
    pub fn node_id(&self, iid: Iid) -> i64 {
        *self
            .ids
            .get(&iid)
            .expect("ArbGraph::node_id: iid is not a node of this graph")
    }
}

// Debug prints the replayable GQL + node ids (a shrunk counterexample is then
// directly reproducible); the oracle/ids are derived from `inserts`.
impl std::fmt::Debug for ArbGraph {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArbGraph")
            .field("node_ids", &self.node_ids)
            .field("inserts", &self.inserts)
            .finish()
    }
}

fn build_arb_graph(n: usize, edges: Vec<(usize, usize, (Instant, Instant))>) -> ArbGraph {
    let mut oracle = GraphOracle::new();
    let mut ids = BTreeMap::new();
    let node_ids: Vec<i64> = (0..n as i64).collect();

    // Nodes: epoch VALID FROM is LOAD-BEARING — probes come from the small
    // T_POOL µs grid, so a wall-clock (insert-time) valid_from would make every
    // node invisible at every grid probe and vacuously empty the property.
    for &id in &node_ids {
        let iid = node_iid(id);
        ids.insert(iid, id);
        oracle.append_node(Event {
            iid,
            system_from: Instant::from_micros(0),
            valid_from: Instant::from_micros(0), // 1970-01-01T00:00:00Z
            valid_to: Instant::END_OF_TIME,
            src: None,
            dst: None,
            op: Op::Put {
                labels: vec!["P".into()],
                doc: Doc::new(),
            },
        });
    }
    let node_paths: Vec<String> = node_ids
        .iter()
        .map(|id| format!("(:P {{_id: {id}}})"))
        .collect();
    let mut inserts = vec![format!(
        "INSERT {} VALID FROM TIMESTAMP '1970-01-01T00:00:00Z'",
        node_paths.join(", ")
    )];

    // Edges: iid mirrors the engine (`_id = 1000 + k`); system_from = insertion
    // index (order-faithful, not value-faithful — so oracle system probes use
    // i64::MAX "current"); valid range from the grid (open-ended edges omit the
    // TO clause). Self-loops and duplicate (src, dst) pairs are allowed on
    // purpose — each edge has a distinct iid so multiset path counts match.
    for (k, (s, d, (vf, vt))) in edges.into_iter().enumerate() {
        let e_id = 1000 + k as i64;
        let (s_id, d_id) = (s as i64, d as i64);
        oracle.append_edge(Event {
            iid: edge_iid(e_id),
            system_from: Instant::from_micros(k as i64),
            valid_from: vf,
            valid_to: vt,
            src: Some(node_iid(s_id)),
            dst: Some(node_iid(d_id)),
            op: Op::Put {
                labels: vec!["K".into()],
                doc: Doc::new(),
            },
        });
        let valid_clause = if vt == Instant::END_OF_TIME {
            format!(
                "VALID FROM TIMESTAMP '{}'",
                micros_to_rfc3339(vf.as_micros())
            )
        } else {
            format!(
                "VALID FROM TIMESTAMP '{}' TO TIMESTAMP '{}'",
                micros_to_rfc3339(vf.as_micros()),
                micros_to_rfc3339(vt.as_micros())
            )
        };
        inserts.push(format!(
            "MATCH (a:P {{_id: {s_id}}}), (b:P {{_id: {d_id}}}) \
             INSERT (a)-[:K {{_id: {e_id}}}]->(b) {valid_clause}"
        ));
    }

    ArbGraph {
        inserts,
        oracle,
        node_ids,
        ids,
    }
}

/// Proptest strategy: a random graph as GQL statements + a parallel oracle.
/// `1..=max_nodes` nodes with dense `_id`s and label `"P"`; `0..=max_edges`
/// `"K"` edges over random endpoint pairs with grid valid ranges.
pub fn arb_graph(max_nodes: usize, max_edges: usize) -> impl Strategy<Value = ArbGraph> {
    (1..=max_nodes).prop_flat_map(move |n| {
        prop::collection::vec(
            (0..n, 0..n, crate::strategy::arb_valid_range()),
            0..=max_edges,
        )
        .prop_map(move |edges| build_arb_graph(n, edges))
    })
}
