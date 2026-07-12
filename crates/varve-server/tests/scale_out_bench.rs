//! Read scale-out benchmark (slice 11, task 9): aggregate QPS against 1, 2,
//! and 4 query nodes of ONE cluster (single ingest). Env-gated: set
//! `VARVE_SCALE_BENCH=1` and run `--release` (see `just bench-scale-out`).
//! Prints a markdown table for `docs/benchmarks/v1.md`. Asserts CORRECTNESS
//! (every node agrees at the final basis, and stays correct during the
//! steady-state read window), never throughput — Spec §13's "~linear to 4
//! nodes" target is tracked in the benchmark report, not gated here.
#![cfg(feature = "http")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use varve_testkit::fixture::social_graph;

#[path = "support/process_cluster.rs"]
mod process_cluster;
use process_cluster::ProcessCluster;

/// Bounded traversal every reader issues: row count equals the number of
/// KNOWS edge instances visible at the attached basis (mirrors
/// `process_scale_out.rs`).
const TRAVERSAL: &str = "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN b.name AS name";
/// Per-node-count measurement window.
const MEASURE_WINDOW: Duration = Duration::from_secs(10);

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn read_qps_scales_from_one_to_four_query_nodes() {
    if std::env::var("VARVE_SCALE_BENCH").is_err() {
        eprintln!("skipping: set VARVE_SCALE_BENCH=1 (see `just bench-scale-out`)");
        return;
    }

    let cluster = Arc::new(ProcessCluster::start_with_query_nodes(4).await.unwrap());

    // Ingest the fixture once over the writer; capture the final basis.
    let graph = social_graph(1_000, 5_000, 42);
    let mut final_basis = 0u64;
    for statement in graph.node_statements(50) {
        final_basis = cluster
            .tx(cluster.writer_url(), &statement)
            .await
            .unwrap()
            .basis;
    }
    for program in graph.edge_programs(100) {
        final_basis = cluster
            .tx(cluster.writer_url(), &program)
            .await
            .unwrap()
            .basis;
    }

    // Correctness floor: every query node agrees with the writer at the
    // final basis before any throughput is measured.
    let urls: Vec<String> = cluster
        .query_urls()
        .into_iter()
        .map(str::to_owned)
        .collect();
    let expected = cluster
        .query_row_count(cluster.writer_url(), TRAVERSAL, Some(final_basis))
        .await
        .unwrap();
    assert!(expected > 0, "writer ingested no edges");
    for url in &urls {
        let got = cluster
            .query_row_count(url, TRAVERSAL, Some(final_basis))
            .await
            .unwrap();
        assert_eq!(
            got, expected,
            "{url} diverged from the writer at basis {final_basis}"
        );
    }

    println!("| query nodes | aggregate reads | window | QPS |");
    println!("|---|---|---|---|");
    for n in [1usize, 2, 4] {
        let mut readers = Vec::new();
        let deadline = Instant::now() + MEASURE_WINDOW;
        // `.cloned()` is load-bearing, not redundant: each `url` is moved
        // into a `tokio::spawn`ed task, which requires `'static` — a
        // borrow from `urls` cannot satisfy that, so clippy's
        // `unnecessary_to_owned` suggestion here does not compile.
        #[allow(clippy::unnecessary_to_owned)]
        for url in urls.iter().take(n).cloned() {
            let cluster = Arc::clone(&cluster);
            readers.push(tokio::spawn(async move {
                let mut ok = 0usize;
                while Instant::now() < deadline {
                    // Steady-state reads: no basis attached (ingest already
                    // finished, so followers have long converged); the
                    // per-read equality is the ongoing correctness check.
                    if let Ok(count) = cluster.query_row_count(&url, TRAVERSAL, None).await {
                        assert_eq!(count, expected, "{url} diverged during steady-state reads");
                        ok += 1;
                    }
                }
                assert!(ok > 0, "{url} made no successful reads in the window");
                ok
            }));
        }
        let mut total = 0usize;
        for reader in readers {
            total += reader.await.unwrap();
        }
        let qps = total as f64 / MEASURE_WINDOW.as_secs_f64();
        println!("| {n} | {total} | {MEASURE_WINDOW:?} | {qps:.0} |");
    }
}
