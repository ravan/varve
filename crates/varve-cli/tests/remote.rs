use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use axum::{extract::Request, middleware, middleware::Next, response::Response, Router};
use tempfile::TempDir;
use tokio::net::TcpListener;
use url::Url;
use varve::{Config, Db};
use varve_cli::{CliError, CommandClient, RemoteClient};
use varve_server::api::{BasisRequest, QueryRequest, TxRequest};
use varve_server::{
    http_router, readiness_channel, static_auth, FrontendContext, HttpContext, PrometheusMetrics,
};

const TOKEN: &str = "test-token";

fn config(root: &TempDir, roles: &[&str]) -> Config {
    let roles = roles
        .iter()
        .map(|role| format!("\"{role}\""))
        .collect::<Vec<_>>()
        .join(", ");
    Config::from_toml_str(&format!(
        "[node]\nroles = [{roles}]\ntail_poll_interval_ms = 5\n\
         tail_batch_records = 16\nbasis_timeout_ms = 5000\n\
         [log]\nbackend = \"local\"\ngroup_commit_window_ms = 0\n\
         [log.local]\ndir = {:?}\n\
         [storage]\nbackend = \"local\"\nmax_block_rows = 100000\n\
         [storage.local]\ndir = {:?}\n",
        root.path().join("log").display().to_string(),
        root.path().join("store").display().to_string(),
    ))
    .unwrap_or_else(|error| panic!("test config must parse: {error}"))
}

#[derive(Default)]
struct RouteCounters {
    tx: AtomicUsize,
    query: AtomicUsize,
}

async fn count_routes(counters: Arc<RouteCounters>, request: Request, next: Next) -> Response {
    match request.uri().path() {
        "/v1/tx" => counters.tx.fetch_add(1, Ordering::SeqCst),
        "/v1/query" => counters.query.fetch_add(1, Ordering::SeqCst),
        _ => 0,
    };
    next.run(request).await
}

async fn spawn_node(db: Db, counters: Arc<RouteCounters>) -> Url {
    let probe = db
        .probe_capabilities()
        .await
        .unwrap_or_else(|error| panic!("probe must run: {error}"));
    let context = HttpContext {
        frontend: FrontendContext {
            db,
            authenticator: static_auth(&[("cli", TOKEN)])
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
    let router: Router = http_router(context).layer(middleware::from_fn(move |req, next| {
        let counters = counters.clone();
        count_routes(counters, req, next)
    }));
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
    Url::parse(&format!("http://{addr}")).unwrap_or_else(|error| panic!("url must parse: {error}"))
}

#[tokio::test]
async fn remote_client_reroutes_writer_mutation_once_but_keeps_queries_on_query_node() {
    let root = TempDir::new().unwrap_or_else(|error| panic!("tempdir must create: {error}"));
    let writer_db = Db::open(config(&root, &["writer", "query", "compactor"]))
        .await
        .unwrap_or_else(|error| panic!("writer db must open: {error}"));
    let query_db = Db::open(config(&root, &["query"]))
        .await
        .unwrap_or_else(|error| panic!("query db must open: {error}"));

    let writer_counters = Arc::new(RouteCounters::default());
    let query_counters = Arc::new(RouteCounters::default());
    let writer_url = spawn_node(writer_db.clone(), writer_counters.clone()).await;
    let query_url = spawn_node(query_db, query_counters.clone()).await;

    writer_db
        .publish_writer(writer_url.as_str())
        .await
        .unwrap_or_else(|error| panic!("writer must publish: {error}"));

    let client = RemoteClient::new(query_url, TOKEN.to_string())
        .unwrap_or_else(|error| panic!("remote client must build: {error}"));

    let tx = client
        .execute(TxRequest {
            gql: "INSERT (:X {_id: 1})".to_string(),
            params: BTreeMap::new(),
        })
        .await
        .unwrap_or_else(|error| panic!("tx must succeed via reroute: {error}"));
    assert_eq!(tx.side_effects.nodes_created, 1);

    assert_eq!(query_counters.tx.load(Ordering::SeqCst), 1);
    assert_eq!(writer_counters.tx.load(Ordering::SeqCst), 1);

    let batches = client
        .query(QueryRequest {
            gql: "MATCH (x:X) RETURN x._id AS id".to_string(),
            params: BTreeMap::new(),
            basis: Some(BasisRequest::TxId(tx.tx_id)),
            basis_timeout_ms: Some(5_000),
        })
        .await
        .unwrap_or_else(|error| panic!("query must succeed on query node: {error}"));
    let rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(rows, 1);

    assert_eq!(query_counters.query.load(Ordering::SeqCst), 1);
    assert_eq!(writer_counters.query.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn remote_client_reports_redirect_loop_on_a_second_misdirected_response() {
    let root = TempDir::new().unwrap_or_else(|error| panic!("tempdir must create: {error}"));

    // A writer-capable node exists only to publish a `writer` advertisement
    // into the shared store; it is never exposed over HTTP. Both HTTP nodes
    // below are query-only, so the advertised address always 421s too --
    // the second hop must never be followed.
    let publisher = Db::open(config(&root, &["writer", "query", "compactor"]))
        .await
        .unwrap_or_else(|error| panic!("publisher db must open: {error}"));

    let node_a = Db::open(config(&root, &["query"]))
        .await
        .unwrap_or_else(|error| panic!("node a must open: {error}"));
    let node_b = Db::open(config(&root, &["query"]))
        .await
        .unwrap_or_else(|error| panic!("node b must open: {error}"));

    let counters_a = Arc::new(RouteCounters::default());
    let counters_b = Arc::new(RouteCounters::default());
    let url_a = spawn_node(node_a, counters_a).await;
    let url_b = spawn_node(node_b, counters_b).await;

    publisher
        .publish_writer(url_b.as_str())
        .await
        .unwrap_or_else(|error| panic!("writer advertisement must publish: {error}"));

    let client = RemoteClient::new(url_a, TOKEN.to_string())
        .unwrap_or_else(|error| panic!("remote client must build: {error}"));

    let result = client
        .execute(TxRequest {
            gql: "INSERT (:X {_id: 1})".to_string(),
            params: BTreeMap::new(),
        })
        .await;
    assert!(
        matches!(result, Err(CliError::RedirectLoop)),
        "expected RedirectLoop, got {result:?}"
    );
}
