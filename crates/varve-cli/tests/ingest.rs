//! Embedded bulk import (`CommandClient::ingest` on `EmbeddedClient`) and the
//! whole-graph snapshot (`snapshot_all`) that `varve export --format ndjson`
//! builds on. These drive a real local `Db` (no network): the streamed body
//! goes through the SAME incremental framer the `/v1/ingest` handler uses, and
//! the loaded graph is read back through the normal query surface.
#![allow(clippy::unwrap_used)]

use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use std::collections::BTreeMap;
use std::sync::Arc;
use varve_cli::{
    export_ndjson, run_export, run_import, BulkBody, BulkFormat, CommandClient, EmbeddedClient,
    ExportArgs, ExportFormat, ImportArgs, ImportFormat,
};
use varve_server::api::bulk::{decode_ndjson_lines, BulkOp};
use varve_server::api::QueryRequest;

/// A `BulkBody` that yields `frames` in order — exercises the framer's
/// cross-frame line handling on the embedded path.
fn body(frames: Vec<&'static [u8]>) -> BulkBody {
    Box::pin(futures::stream::iter(
        frames
            .into_iter()
            .map(|frame| Ok(Bytes::from_static(frame))),
    ))
}

/// A `BulkBody` from owned bytes (e.g. a previous export's output).
fn body_owned(bytes: Vec<u8>) -> BulkBody {
    Box::pin(futures::stream::once(async move { Ok(Bytes::from(bytes)) }))
}

fn query(gql: &str) -> QueryRequest {
    QueryRequest {
        gql: gql.to_string(),
        params: BTreeMap::new(),
        basis: None,
        basis_timeout_ms: None,
        graph: None,
    }
}

fn rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

async fn open(dir: &std::path::Path) -> EmbeddedClient {
    EmbeddedClient::open(dir)
        .await
        .unwrap_or_else(|error| panic!("embedded client must open: {error}"))
}

#[tokio::test]
async fn embedded_ndjson_ingest_loads_a_queryable_graph() {
    let tmp = tempfile::tempdir().unwrap();
    let client = open(tmp.path()).await;

    // Two nodes and one edge, split across frames so a line spans the boundary.
    let response = client
        .ingest(
            BulkFormat::Ndjson,
            body(vec![
                b"{\"type\":\"node\",\"labels\":[\"Person\"],\"props\":{\"_id\":\"a\",\"name\":\"Ada\"}}\n{\"type\":\"node\",\"labels\":[\"Per",
                b"son\"],\"props\":{\"_id\":\"b\",\"name\":\"Bob\"}}\n{\"type\":\"edge\",\"label\":\"KNOWS\",\"src\":\"a\",\"dst\":\"b\"}\n",
            ]),
        )
        .await
        .unwrap_or_else(|error| panic!("embedded ingest must succeed: {error}"));
    assert_eq!(response.nodes, 2);
    assert_eq!(response.edges, 1);
    assert!(response.transactions >= 1);

    let traversal = client
        .query(query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name AS knower, b.name AS known",
        ))
        .await
        .unwrap();
    assert_eq!(
        rows(&traversal),
        1,
        "the a->b KNOWS edge must be traversable"
    );
}

#[tokio::test]
async fn embedded_csv_ingest_loads_nodes_and_edges() {
    let tmp = tempfile::tempdir().unwrap();
    let client = open(tmp.path()).await;

    client
        .ingest(
            BulkFormat::Csv,
            body(vec![b":ID,name,:LABEL\na,Ada,Person\nb,Bob,Person\n"]),
        )
        .await
        .unwrap_or_else(|error| panic!("csv node ingest must succeed: {error}"));
    let edge_response = client
        .ingest(
            BulkFormat::Csv,
            body(vec![b":START_ID,:END_ID,:TYPE\na,b,KNOWS\n"]),
        )
        .await
        .unwrap_or_else(|error| panic!("csv edge ingest must succeed: {error}"));
    assert_eq!(edge_response.edges, 1);

    let traversal = client
        .query(query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name AS knower",
        ))
        .await
        .unwrap();
    assert_eq!(rows(&traversal), 1);
}

#[tokio::test]
async fn empty_body_is_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let client = open(tmp.path()).await;
    let error = client
        .ingest(BulkFormat::Ndjson, body(vec![b"\n  \n"]))
        .await
        .expect_err("a records-free body must be rejected");
    assert!(
        error.to_string().contains("no records"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn snapshot_all_returns_the_loaded_graph() {
    let tmp = tempfile::tempdir().unwrap();
    let client = open(tmp.path()).await;
    client
        .ingest(
            BulkFormat::Ndjson,
            body(vec![
                b"{\"type\":\"node\",\"labels\":[\"Person\"],\"props\":{\"_id\":\"a\"}}\n{\"type\":\"edge\",\"label\":\"KNOWS\",\"src\":\"a\",\"dst\":\"a\"}\n",
            ]),
        )
        .await
        .unwrap();

    let (nodes, edges) = client.snapshot_all().await.unwrap();
    assert_eq!(nodes.map(|b| b.num_rows()), Some(1));
    assert_eq!(edges.map(|b| b.num_rows()), Some(1));
}

// ---- Round-trip: export | import | export is a fixpoint ------------------

/// A logical, order-independent key for one decoded op, so two exports of the
/// same graph compare equal as multisets regardless of row order (edge iids —
/// hence edge row order — differ between two independent ingests).
fn op_key(op: &BulkOp) -> String {
    match op {
        BulkOp::Node(node) => {
            let mut labels = node.labels.clone();
            labels.sort();
            format!("N labels={labels:?} doc={:?}", node.doc)
        }
        BulkOp::Edge(edge) => {
            format!(
                "E {} {:?}->{:?} doc={:?}",
                edge.label, edge.src, edge.dst, edge.doc
            )
        }
    }
}

fn sorted_op_keys(ndjson: &[u8]) -> Vec<String> {
    let text = std::str::from_utf8(ndjson).unwrap();
    let ops = decode_ndjson_lines(text).unwrap();
    let mut keys: Vec<String> = ops.iter().map(op_key).collect();
    keys.sort();
    keys
}

async fn arc_client(dir: &std::path::Path) -> Arc<dyn CommandClient> {
    Arc::new(open(dir).await)
}

#[tokio::test]
async fn export_then_import_round_trips_nodes_edges_and_props() {
    let src = tempfile::tempdir().unwrap();
    let a = arc_client(src.path()).await;
    // A graph with node + edge properties, to prove props round-trip.
    a.ingest(
        BulkFormat::Ndjson,
        body(vec![
            b"{\"type\":\"node\",\"labels\":[\"Person\"],\"props\":{\"_id\":\"a\",\"name\":\"Ada\",\"age\":36}}\n\
              {\"type\":\"node\",\"labels\":[\"Person\"],\"props\":{\"_id\":\"b\",\"name\":\"Bob\"}}\n\
              {\"type\":\"edge\",\"label\":\"KNOWS\",\"src\":\"a\",\"dst\":\"b\",\"props\":{\"since\":2001}}\n",
        ]),
    )
    .await
    .unwrap();

    let mut export_a = Vec::new();
    let summary = export_ndjson(a.clone(), &mut export_a).await.unwrap();
    assert_eq!(
        (summary.nodes, summary.edges, summary.edges_skipped),
        (2, 1, 0)
    );

    // Import A's export into a fresh DB, then export THAT.
    let dst = tempfile::tempdir().unwrap();
    let b = arc_client(dst.path()).await;
    b.ingest(BulkFormat::Ndjson, body_owned(export_a.clone()))
        .await
        .unwrap();
    let mut export_b = Vec::new();
    export_ndjson(b.clone(), &mut export_b).await.unwrap();

    // The two exports describe the same logical graph (multiset of ops).
    assert_eq!(sorted_op_keys(&export_a), sorted_op_keys(&export_b));
    // And it's the graph we loaded: 2 Person nodes + 1 KNOWS edge with props.
    assert_eq!(sorted_op_keys(&export_b).len(), 3);
}

// ---- run_import / run_export validation ----------------------------------

fn sink() -> Vec<u8> {
    Vec::new()
}

#[tokio::test]
async fn import_rejects_label_with_a_bulk_format() {
    let tmp = tempfile::tempdir().unwrap();
    let client = arc_client(tmp.path()).await;
    let error = run_import(
        client,
        ImportArgs {
            format: ImportFormat::Ndjson,
            label: Some("Person".to_string()),
            graph: None,
            file: "-".to_string(),
        },
        &mut sink(),
    )
    .await
    .expect_err("--label with --format ndjson must be rejected");
    assert!(error.to_string().contains("jsonl-legacy"), "{error}");
}

#[tokio::test]
async fn import_jsonl_legacy_requires_a_label() {
    let tmp = tempfile::tempdir().unwrap();
    let client = arc_client(tmp.path()).await;
    let error = run_import(
        client,
        ImportArgs {
            format: ImportFormat::JsonlLegacy,
            label: None,
            graph: None,
            file: "-".to_string(),
        },
        &mut sink(),
    )
    .await
    .expect_err("jsonl-legacy without --label must be rejected");
    assert!(error.to_string().contains("--label is required"), "{error}");
}

#[tokio::test]
async fn export_ndjson_rejects_a_query() {
    let tmp = tempfile::tempdir().unwrap();
    let client = arc_client(tmp.path()).await;
    let error = run_export(
        client,
        ExportArgs {
            format: ExportFormat::Ndjson,
            query: Some("MATCH (n) RETURN n".to_string()),
            basis: None,
            file: "-".to_string(),
        },
        &mut sink(),
    )
    .await
    .expect_err("--query with --format ndjson must be rejected");
    assert!(error.to_string().contains("whole graph"), "{error}");
}

#[tokio::test]
async fn export_jsonl_requires_a_query() {
    let tmp = tempfile::tempdir().unwrap();
    let client = arc_client(tmp.path()).await;
    let error = run_export(
        client,
        ExportArgs {
            format: ExportFormat::Jsonl,
            query: None,
            basis: None,
            file: "-".to_string(),
        },
        &mut sink(),
    )
    .await
    .expect_err("jsonl export without --query must be rejected");
    assert!(error.to_string().contains("--query is required"), "{error}");
}
