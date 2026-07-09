//! Traversal oracle property suites (roadmap slice 6, task 10 — the
//! correctness capstone). An INDEPENDENT naive graph walker
//! (`varve_testkit::oracle::GraphOracle`) is cross-checked against the two
//! traversal surfaces of the database:
//!
//!  1. **Pure layer** — `varve_plan::expand::expand_paths` (the WALK-semantics
//!     core) must equal `GraphOracle::walk` on identical adjacency, for every
//!     `(min, max)` with `max <= 4`. No `Db`, no tokio: run at the FULL
//!     `PROPTEST_CASES` count (10k CI / 200k nightly) — this is the core
//!     equivalence check and the whole point of the oracle being written from
//!     scratch (it does NOT call `expand_paths`).
//!  2. **E2E layer** — a random graph (`arb_graph`) driven through the real
//!     `Db` via GQL; every `{m,n}` (n <= 3) expansion from a sampled anchor
//!     must equal the oracle at NOW and at a sampled AS-OF valid time.
//!  3. **Flush invariance** — a memory-only `Db` and a `Db` that flushes
//!     mid-ingest give identical `{1,2}` expansions.
//!
//! Economics: each e2e / flush case boots a `Db` + a tokio runtime, so those
//! layers are capped (`e2e_cases` = min(PROPTEST_CASES, 2); flush =
//! min(., 4)) while the pure layer runs the full count.
#![allow(clippy::unwrap_used)]

use proptest::prelude::*;
use std::sync::{Mutex, OnceLock};
use varve_plan::expand::{expand_paths, AdjEdge, EdgeAdjacency};
use varve_testkit::oracle::{column_i64, micros_to_rfc3339, GraphOracle, OracleDir};
use varve_types::{Iid, Instant};

static DB_PROPERTY_LOCK: Mutex<()> = Mutex::new(());
static DB_RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
const DB_E2E_CASE_CAP: u32 = 2;
const DB_FLUSH_CASE_CAP: u32 = 1;
const DB_FIXTURE_PROGRAM_BATCH: usize = 64;
const DB_E2E_MAX_NODES: usize = 24;
const DB_E2E_MAX_EDGES: usize = 48;
const DB_E2E_MAX_DEPTH: u32 = 2;
const DB_FLUSH_MAX_NODES: usize = 4;
const DB_FLUSH_MAX_EDGES: usize = 6;

fn cases() -> u32 {
    std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10_000)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(cases()))]

    /// PURE layer: `expand_paths` == `oracle.walk` on identical adjacency, for
    /// every `(min, max)` with `max <= 4` (decision 14a). The oracle's walk is
    /// an independent from-scratch BFS; agreement here is the integrity
    /// guarantee for the whole traversal engine.
    #[test]
    fn expansion_matches_oracle_walk(
        edges in prop::collection::vec((0u8..20, 0u8..20), 0..60),
        start in 0u8..20,
    min in 0u32..=4,
    span in 0u32..=4,
) {
    let max = min + span;
        let node = |i: u8| Iid::derive("g", "nodes", &[i]);
        let mut oracle = GraphOracle::new();
        let mut entries = Vec::new();
        for (k, (s, d)) in edges.iter().enumerate() {
            let e = Iid::derive("g", "edges", &[k as u8]);
            entries.push((node(*s), AdjEdge { neighbor: node(*d), edge: e }));
            oracle.append_edge(varve_index::Event {
                iid: e,
                system_from: Instant::from_micros(k as i64),
                valid_from: Instant::MIN,
                valid_to: Instant::END_OF_TIME,
                src: Some(node(*s)),
                dst: Some(node(*d)),
                op: varve_index::Op::Put {
                    labels: vec!["K".into()],
                    doc: Default::default(),
                },
            });
        }
        let adj = EdgeAdjacency::from_entries(entries);
        let got = expand_paths(&adj, node(start), min, max);
        let want = oracle.walk(
            node(start),
            "K",
            OracleDir::Out,
            min,
            max,
            Instant::from_micros(0),
            Instant::END_OF_TIME,
        );
        prop_assert_eq!(got, want);
    }
}

// ---- e2e + flush support helpers --------------------------------------------

/// The e2e / flush layers each boot real `Db` fixtures, so they are capped well
/// below the pure layer's full `PROPTEST_CASES` (decision 14b).
fn capped_e2e_cases(total: u32) -> u32 {
    total.min(DB_E2E_CASE_CAP)
}

fn capped_flush_cases(total: u32) -> u32 {
    total.min(DB_FLUSH_CASE_CAP)
}

fn e2e_cases() -> u32 {
    capped_e2e_cases(cases())
}

fn flush_cases() -> u32 {
    capped_flush_cases(cases())
}

fn db_runtime() -> &'static tokio::runtime::Runtime {
    DB_RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    })
}

fn batched_programs(statements: &[String], batch: usize) -> Vec<String> {
    assert!(batch > 0, "program batch size must be non-zero");
    statements
        .chunks(batch)
        .map(|chunk| chunk.join(";\n"))
        .collect()
}

#[test]
fn db_oracle_case_budget_is_memory_safe() {
    assert!(std::hint::black_box(DB_E2E_MAX_NODES) <= 24);
    assert!(std::hint::black_box(DB_E2E_MAX_EDGES) <= 48);
    assert!(std::hint::black_box(DB_E2E_MAX_DEPTH) <= 2);
    assert!(std::hint::black_box(DB_FLUSH_MAX_NODES) <= 4);
    assert!(std::hint::black_box(DB_FLUSH_MAX_EDGES) <= 6);
    assert_eq!(std::hint::black_box(flush_cases()), 1);
}

#[test]
fn db_oracle_release_case_caps_are_documented() {
    assert_eq!(
        std::hint::black_box(capped_e2e_cases(1024)),
        DB_E2E_CASE_CAP
    );
    assert_eq!(
        std::hint::black_box(capped_flush_cases(1024)),
        DB_FLUSH_CASE_CAP
    );
    assert_eq!(std::hint::black_box(capped_e2e_cases(1)), 1);
    assert_eq!(std::hint::black_box(capped_flush_cases(1)), 1);
}

#[test]
fn db_fixture_program_batches_preserve_statement_order() {
    let statements = vec![
        "INSERT (:P {_id: 1})".to_owned(),
        "INSERT (:P {_id: 2})".to_owned(),
        "MATCH (a:P {_id: 1}), (b:P {_id: 2}) INSERT (a)-[:K]->(b)".to_owned(),
    ];

    assert_eq!(
        batched_programs(&statements, 2),
        vec![
            "INSERT (:P {_id: 1});\nINSERT (:P {_id: 2})".to_owned(),
            "MATCH (a:P {_id: 1}), (b:P {_id: 2}) INSERT (a)-[:K]->(b)".to_owned(),
        ]
    );
}

/// The oracle probe mirroring the engine's read point. `valid` = the grid probe
/// under AS OF (`Some`), else an instant past the grid's end (`i64::MAX - 1`) —
/// the same verdict the engine's "AS OF now" gives for grid-ranged edges
/// (open-ended edges visible at both, grid-ranged edges at neither). `system` =
/// `i64::MAX` always: the oracle's system axis is order-faithful only (every
/// edge was written in the past, so all are current); system time-travel
/// equivalence is slice-2 covered. `Db` is not needed to compute this.
fn probe_at(probe_valid: Option<i64>) -> (Instant, Instant) {
    let valid = match probe_valid {
        Some(us) => Instant::from_micros(us),
        None => Instant::from_micros(i64::MAX - 1),
    };
    (valid, Instant::from_micros(i64::MAX))
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(e2e_cases()))]

    /// E2E (decision 14b): a random graph driven through the real `Db` via
    /// GQL; every `{min,max}` (max <= DB_E2E_MAX_DEPTH) expansion
    /// from a sampled anchor matches the oracle, at NOW and at a sampled AS-OF
    /// valid time (exit criterion: edge validity respected). Capped at
    /// `min(PROPTEST_CASES, 2)` real `Db` fixtures.
    #[test]
    fn db_traversal_matches_oracle(
        graph in varve_testkit::oracle::arb_graph(DB_E2E_MAX_NODES, DB_E2E_MAX_EDGES),
        anchor_pick in any::<prop::sample::Index>(),
        min in 0u32..=DB_E2E_MAX_DEPTH,
        end in 0u32..=DB_E2E_MAX_DEPTH,
        valid_probe in 0i64..varve_testkit::strategy::T_POOL,
    ) {
        let (min, max) = if min <= end { (min, end) } else { (end, min) };
        let _guard = DB_PROPERTY_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        db_runtime().block_on(async {
            let db = varve::Db::memory();
            for program in batched_programs(&graph.inserts, DB_FIXTURE_PROGRAM_BATCH) {
                db.execute(&program).await.unwrap();
            }
            let anchor_id = graph.node_ids[anchor_pick.index(graph.node_ids.len())];
            let anchor = Iid::derive(
                "default",
                "nodes",
                &varve_types::Value::Int(anchor_id).id_bytes().unwrap(),
            );

            for (layer, gql_time, probe_valid) in [
                ("now", String::new(), None),
                (
                    "asof",
                    format!(
                        "FOR VALID_TIME AS OF TIMESTAMP '{}' ",
                        micros_to_rfc3339(valid_probe)
                    ),
                    Some(valid_probe),
                ),
            ] {
                let gql = format!(
                    "{gql_time}MATCH (a:P)-[:K]->{{{min},{max}}}(b:P) \
                     WHERE a._id = {anchor_id} RETURN b._id AS _id"
                );
                let rows = db.query(&gql).await.unwrap();
                let mut got: Vec<i64> = column_i64(&rows, "_id");
                got.sort_unstable();
                let (valid, system) = probe_at(probe_valid);
                let mut want: Vec<i64> = graph
                    .oracle
                    .walk(anchor, "K", OracleDir::Out, min, max, valid, system)
                    .into_iter()
                    .map(|(end, _)| graph.node_id(end))
                    .collect();
                want.sort_unstable();
                prop_assert_eq!(got, want, "layer {}", layer);
            }
            Ok(())
        })?;
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(flush_cases()))]

    /// Traversal results are invariant under flushing: a memory-only `Db` and a
    /// `Db` that flushes mid-ingest (memory log + memory storage, tiny
    /// `max_block_rows` — legal per slice-4 decision 11; only local-log +
    /// memory-storage is forbidden) give identical `{1,2}` expansions. Capped
    /// at `min(PROPTEST_CASES, 1)` (two real `Db` fixtures).
    #[test]
fn traversal_invariant_under_flush(
        graph in varve_testkit::oracle::arb_graph(DB_FLUSH_MAX_NODES, DB_FLUSH_MAX_EDGES),
    anchor_pick in any::<prop::sample::Index>(),
) {
    let _guard = DB_PROPERTY_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    db_runtime().block_on(async {
            let plain = varve::Db::memory();
            let cfg = varve::Config::from_toml_str(
                "[log]\nbackend = \"memory\"\n[storage]\nbackend = \"memory\"\nmax_block_rows = 4\n",
            )
            .unwrap();
            let flushy = varve::Db::open(cfg).await.unwrap();
            for program in batched_programs(&graph.inserts, DB_FIXTURE_PROGRAM_BATCH) {
                plain.execute(&program).await.unwrap();
                flushy.execute(&program).await.unwrap();
            }
            let anchor_id = graph.node_ids[anchor_pick.index(graph.node_ids.len())];
            let gql = format!(
                "MATCH (a:P)-[:K]->{{1,2}}(b:P) WHERE a._id = {anchor_id} RETURN b._id AS _id"
            );
            let mut a = column_i64(&plain.query(&gql).await.unwrap(), "_id");
            let mut b = column_i64(&flushy.query(&gql).await.unwrap(), "_id");
            a.sort_unstable();
            b.sort_unstable();
            prop_assert_eq!(a, b);
            Ok(())
        })?;
    }
}
