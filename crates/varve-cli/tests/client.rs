use std::collections::BTreeMap;
use std::sync::Arc;

use serde_json::json;
use tempfile::TempDir;
use tokio::net::TcpListener;
use url::Url;
use varve::Db;
use varve_cli::{CommandClient, EmbeddedClient, RemoteClient};
use varve_server::api::{BasisRequest, QueryRequest, TxRequest};
use varve_server::{
    http_router, readiness_channel, static_auth, FrontendContext, HttpContext, PrometheusMetrics,
};

fn rows_in(batches: &[arrow::record_batch::RecordBatch]) -> usize {
    batches.iter().map(|batch| batch.num_rows()).sum()
}

fn insert_ada_request() -> TxRequest {
    let mut params = BTreeMap::new();
    params.insert("name".to_string(), json!("Ada"));
    TxRequest {
        gql: "INSERT (:Person {name: $name})".to_string(),
        params,
    }
}

fn find_ada_request(basis_tx_id: u64) -> QueryRequest {
    QueryRequest {
        gql: "MATCH (p:Person) RETURN p.name AS name".to_string(),
        params: BTreeMap::new(),
        basis: Some(BasisRequest::TxId(basis_tx_id)),
        basis_timeout_ms: Some(5_000),
    }
}

async fn assert_client_parity(client: &dyn CommandClient) {
    let tx = client
        .execute(insert_ada_request())
        .await
        .unwrap_or_else(|error| panic!("tx must succeed: {error}"));
    assert_eq!(tx.side_effects.nodes_created, 1);
    assert_eq!(tx.side_effects.properties_set, 1);

    let batches = client
        .query(find_ada_request(tx.tx_id))
        .await
        .unwrap_or_else(|error| panic!("query must succeed: {error}"));
    assert_eq!(rows_in(&batches), 1);

    let status = client
        .status()
        .await
        .unwrap_or_else(|error| panic!("status must succeed: {error}"));
    assert!(status.applied_tx_id >= tx.tx_id);
    assert!(status.roles.iter().any(|role| role == "writer"));
    assert!(status.roles.iter().any(|role| role == "query"));
    assert!(status.roles.iter().any(|role| role == "compactor"));

    client
        .compact()
        .await
        .unwrap_or_else(|error| panic!("compact must succeed: {error}"));
    client
        .gc()
        .await
        .unwrap_or_else(|error| panic!("gc must succeed: {error}"));
    let verify = client
        .verify()
        .await
        .unwrap_or_else(|error| panic!("verify must succeed: {error}"));
    assert!(verify.log_records_checked >= 1);
}

#[tokio::test]
async fn embedded_client_executes_query_status_and_admin_ops() {
    let dir = TempDir::new().unwrap_or_else(|error| panic!("tempdir must create: {error}"));
    let client = EmbeddedClient::open(dir.path())
        .await
        .unwrap_or_else(|error| panic!("embedded client must open: {error}"));
    assert_client_parity(&client).await;
}

#[tokio::test]
async fn remote_client_executes_query_status_and_admin_ops_over_http() {
    let dir = TempDir::new().unwrap_or_else(|error| panic!("tempdir must create: {error}"));
    let db = Db::local(dir.path())
        .await
        .unwrap_or_else(|error| panic!("db must open: {error}"));
    let probe = db
        .probe_capabilities()
        .await
        .unwrap_or_else(|error| panic!("probe must run: {error}"));
    let context = HttpContext {
        frontend: FrontendContext {
            db,
            authenticator: static_auth(&[("cli", "secret-token")])
                .unwrap_or_else(|error| panic!("auth must build: {error}")),
            metrics: Arc::new(
                PrometheusMetrics::new()
                    .unwrap_or_else(|error| panic!("metrics must build: {error}")),
            ),
            probe,
            readiness: readiness_channel().0,
        },
        max_body_bytes: 8 * 1024 * 1024,
    };
    let router = http_router(context);
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap_or_else(|error| panic!("listener must bind: {error}"));
    let addr = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("addr must resolve: {error}"));
    tokio::spawn(async move {
        axum::serve(listener, router)
            .await
            .unwrap_or_else(|error| panic!("server must serve: {error}"));
    });

    let base = Url::parse(&format!("http://{addr}"))
        .unwrap_or_else(|error| panic!("base url must parse: {error}"));
    let client = RemoteClient::new(base, "secret-token".to_string())
        .unwrap_or_else(|error| panic!("remote client must build: {error}"));
    assert_client_parity(&client).await;
}
