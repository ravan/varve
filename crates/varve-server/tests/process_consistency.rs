//! Cross-process consistency contracts (roadmap slice 9, task 14).
//!
//! One Writer+Query+Compactor process and two Query-only processes share a
//! single on-disk local log/store. These tests prove basis-bounded
//! read-your-writes, basis-free eventual consistency, writer redirection, and
//! a real Rust Arrow-stream client decode — all across genuine OS processes.
#![cfg(feature = "http")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use arrow::ipc::reader::StreamReader;
use futures::StreamExt;
use serde_json::json;
use std::io::Cursor;
use std::time::{Duration, Instant};

#[path = "support/process_cluster.rs"]
mod process_cluster;
use process_cluster::ProcessCluster;

/// Step 1: a writer receipt's basis is immediately readable from BOTH query
/// processes. Follower polling is 200 ms (see the harness config) so a
/// basis-bounded read normally has to block until the follower tails the new
/// record; we never assert an unbased read is stale (scheduling may already
/// have applied it).
#[tokio::test(flavor = "multi_thread")]
async fn writer_receipt_is_immediately_readable_from_both_query_processes() {
    let cluster = ProcessCluster::start().await.unwrap();
    let receipt = cluster
        .tx(
            cluster.writer_url(),
            "INSERT (:Person {_id: 1, name: 'Ada'})",
        )
        .await
        .unwrap();

    for query_url in cluster.query_urls() {
        let rows = cluster
            .query_json(
                query_url,
                "MATCH (p:Person) RETURN p.name AS name",
                Some(receipt.basis),
            )
            .await
            .unwrap();
        assert_eq!(rows, json!([{"name":"Ada"}]));
    }
}

/// Step 2: eventual consistency without a basis, plus the 421 writer
/// redirection. After a writer tx, a basis-free read on each query node
/// converges to the row within a bounded 5-second retry; a tx POSTed to each
/// query node returns 421 with the writer's exact advertised address.
#[tokio::test(flavor = "multi_thread")]
async fn basis_free_reads_converge_and_query_node_tx_is_misdirected() {
    let cluster = ProcessCluster::start().await.unwrap();
    cluster
        .tx(
            cluster.writer_url(),
            "INSERT (:Person {_id: 7, name: 'Grace'})",
        )
        .await
        .unwrap();

    for query_url in cluster.query_urls() {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let rows = cluster
                .query_json(
                    query_url,
                    "MATCH (p:Person {name: 'Grace'}) RETURN p.name AS name",
                    None,
                )
                .await
                .unwrap();
            if rows == json!([{"name":"Grace"}]) {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "query node {query_url} never observed the basis-free write"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    for query_url in cluster.query_urls() {
        let response = cluster
            .client()
            .post(format!("{query_url}/v1/tx"))
            .bearer_auth(cluster.token())
            .json(&json!({"gql":"INSERT (:Person {_id: 8})"}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), 421, "query node must redirect");
        let body: serde_json::Value = response.json().await.unwrap();
        assert_eq!(body["writer"], json!(cluster.writer_url()));
    }
}

/// Step 3: a real Rust Arrow-stream client. Insert enough rows for multiple
/// output batches, request the Arrow media type from a query node, consume
/// `bytes_stream` incrementally (requiring non-empty data), then decode the
/// concatenated bytes with the Arrow 58 `StreamReader` and assert every
/// row/schema. Network chunk count is deliberately NOT asserted (HTTP layers
/// may coalesce application chunks; task 9's in-process test pins the
/// multi-chunk producer).
#[tokio::test(flavor = "multi_thread")]
async fn arrow_stream_from_a_query_process_decodes_every_row() {
    let cluster = ProcessCluster::start().await.unwrap();
    let rows = 8_300usize;
    let statement = format!(
        "INSERT {}",
        (0..rows)
            .map(|id| format!("(:Person {{_id: {id}, name: 'Ada'}})"))
            .collect::<Vec<_>>()
            .join(", ")
    );
    let receipt = cluster.tx(cluster.writer_url(), &statement).await.unwrap();

    let query_url = cluster.query_urls()[0];
    let response = cluster
        .client()
        .post(format!("{query_url}/v1/query"))
        .bearer_auth(cluster.token())
        .header(
            reqwest::header::ACCEPT,
            "application/vnd.apache.arrow.stream",
        )
        .json(&json!({
            "gql":"MATCH (p:Person) RETURN p.name AS name",
            "basis": receipt.basis,
            "basis_timeout_ms": 5000,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 200);

    let mut stream = response.bytes_stream();
    let mut bytes = Vec::new();
    let mut saw_data = false;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.unwrap();
        if !chunk.is_empty() {
            saw_data = true;
        }
        bytes.extend_from_slice(&chunk);
    }
    assert!(saw_data, "Arrow stream must yield non-empty data");

    let batches = StreamReader::try_new(Cursor::new(bytes), None)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(batches.len() >= 2, "query output spans record batches");
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        rows
    );
    assert_eq!(batches[0].schema().field(0).name(), "name");
}
