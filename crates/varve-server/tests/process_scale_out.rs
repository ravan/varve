//! Cross-process read scale-out (roadmap slice 9, task 14).
//!
//! Two Query-only processes serve a bounded traversal read continuously while
//! a third task drives the deterministic `social_graph(200, 1_000, 42)`
//! fixture into the writer. Each reader attaches the most recently published
//! atomic basis, and we assert every reader stays monotonically consistent
//! and converges to the writer's own final count.
#![cfg(feature = "http")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use varve_testkit::fixture::social_graph;

#[path = "support/process_cluster.rs"]
mod process_cluster;
use process_cluster::ProcessCluster;

/// The bounded traversal every reader issues. Row count equals the number of
/// KNOWS edge instances visible at the attached basis, so it can only grow as
/// the writer ingests — a clean monotonic signal.
const TRAVERSAL: &str = "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN b.name AS name";

/// Step 4: concurrent read scale-out. Two readers (one per query node) each
/// complete >= 20 successful queries with monotonically nondecreasing row
/// counts while the writer ingests the fixture; both query nodes and the
/// writer node agree on the final count at the final published basis.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_query_processes_scale_out_reads_while_the_writer_ingests() {
    let cluster = Arc::new(ProcessCluster::start().await.unwrap());
    let graph = social_graph(200, 1_000, 42);
    let node_statements = graph.node_statements(50);
    let edge_programs = graph.edge_programs(100);

    let basis = Arc::new(AtomicU64::new(0));
    let done = Arc::new(AtomicBool::new(false));

    let writer = {
        let cluster = Arc::clone(&cluster);
        let basis = Arc::clone(&basis);
        let done = Arc::clone(&done);
        tokio::spawn(async move {
            for statement in node_statements {
                let receipt = cluster.tx(cluster.writer_url(), &statement).await.unwrap();
                basis.store(receipt.basis, Ordering::SeqCst);
            }
            for program in edge_programs {
                let receipt = cluster.tx(cluster.writer_url(), &program).await.unwrap();
                basis.store(receipt.basis, Ordering::SeqCst);
            }
            let final_basis = basis.load(Ordering::SeqCst);
            done.store(true, Ordering::SeqCst);
            final_basis
        })
    };

    let reader = |query_url: String| {
        let cluster = Arc::clone(&cluster);
        let basis = Arc::clone(&basis);
        let done = Arc::clone(&done);
        tokio::spawn(async move {
            tokio::time::timeout(Duration::from_secs(120), async move {
                let mut counts: Vec<usize> = Vec::new();
                let mut successes = 0usize;
                loop {
                    let published = basis.load(Ordering::SeqCst);
                    let attach = (published > 0).then_some(published);
                    if let Ok(count) = cluster.query_row_count(&query_url, TRAVERSAL, attach).await
                    {
                        counts.push(count);
                        successes += 1;
                    }
                    if done.load(Ordering::SeqCst) && successes >= 20 {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
                (successes, counts)
            })
            .await
            .expect(
                "reader loop did not finish within 120s: a persistent read-path error \
                 (e.g. query_row_count failing on every attempt) would otherwise retry \
                 forever without ever reaching `successes >= 20`, hanging this test \
                 indefinitely instead of failing loudly (slice-9 deferred fix)",
            )
        })
    };

    let query_urls: Vec<String> = cluster
        .query_urls()
        .into_iter()
        .map(str::to_owned)
        .collect();
    let reader_a = reader(query_urls[0].clone());
    let reader_b = reader(query_urls[1].clone());

    let final_basis = writer.await.unwrap();
    let (successes_a, counts_a) = reader_a.await.unwrap();
    let (successes_b, counts_b) = reader_b.await.unwrap();

    assert!(successes_a >= 20, "reader A only completed {successes_a}");
    assert!(successes_b >= 20, "reader B only completed {successes_b}");
    assert!(
        is_nondecreasing(&counts_a),
        "reader A row counts regressed: {counts_a:?}"
    );
    assert!(
        is_nondecreasing(&counts_b),
        "reader B row counts regressed: {counts_b:?}"
    );

    // Authoritative final read at the final published basis: both query nodes
    // must agree with the writer node's own view.
    let writer_final = cluster
        .query_row_count(cluster.writer_url(), TRAVERSAL, Some(final_basis))
        .await
        .unwrap();
    let final_a = cluster
        .query_row_count(&query_urls[0], TRAVERSAL, Some(final_basis))
        .await
        .unwrap();
    let final_b = cluster
        .query_row_count(&query_urls[1], TRAVERSAL, Some(final_basis))
        .await
        .unwrap();
    assert!(writer_final > 0, "writer ingested no edges");
    assert_eq!(final_a, writer_final, "query node A diverged from writer");
    assert_eq!(final_b, writer_final, "query node B diverged from writer");
}

fn is_nondecreasing(counts: &[usize]) -> bool {
    counts.windows(2).all(|pair| pair[0] <= pair[1])
}
