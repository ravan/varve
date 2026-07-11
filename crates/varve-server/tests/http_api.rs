#![cfg(feature = "http")]
#![allow(clippy::unwrap_used)]

use axum::{
    body::Body,
    http::{header::WWW_AUTHENTICATE, Method, Request, StatusCode},
};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use std::sync::Arc;
use tempfile::TempDir;
use tower::ServiceExt;
use varve::{ProbeReport, ProbeVerdict};
use varve_log::{LocalLog, Log, LogRecord, TableEffects, DEFAULT_SEGMENT_MAX_BYTES};
use varve_server::{
    api::ARROW_STREAM_CONTENT_TYPE, http_router, readiness_channel, static_auth, FrontendContext,
    HttpContext, PrometheusMetrics,
};
use varve_types::LogPosition;

fn router() -> axum::Router {
    let (readiness, _) = readiness_channel();
    http_router(HttpContext {
        frontend: FrontendContext {
            db: varve::Db::memory(),
            authenticator: static_auth(&[("ada", "secret")]).unwrap(),
            metrics: Arc::new(PrometheusMetrics::new().unwrap()),
            probe: ProbeReport {
                verdict: ProbeVerdict::Supported,
                probe_key: "test".into(),
            },
            readiness,
        },
        max_body_bytes: 1024 * 1024,
    })
}

fn router_with_db(db: varve::Db) -> axum::Router {
    let (readiness, _) = readiness_channel();
    http_router(HttpContext {
        frontend: FrontendContext {
            db,
            authenticator: static_auth(&[("ada", "secret")]).unwrap(),
            metrics: Arc::new(PrometheusMetrics::new().unwrap()),
            probe: ProbeReport {
                verdict: ProbeVerdict::Supported,
                probe_key: "test".into(),
            },
            readiness,
        },
        max_body_bytes: 1024 * 1024,
    })
}

fn node_config(root: &TempDir, roles: &[&str]) -> varve_config::Config {
    let roles = roles
        .iter()
        .map(|role| format!("\"{role}\""))
        .collect::<Vec<_>>()
        .join(",");
    varve_config::Config::from_toml_str(&format!("[node]\nroles=[{roles}]\ntail_poll_interval_ms=5\ntail_batch_records=1024\nbasis_timeout_ms=10\n[log]\nbackend=\"local\"\ngroup_commit_window_ms=0\n[log.local]\ndir={:?}\n[storage]\nbackend=\"local\"\nmax_block_rows=100000\nflush_interval_ms=300000\n[storage.local]\ndir={:?}\n", root.path().join("log").display().to_string(), root.path().join("store").display().to_string())).unwrap()
}

async fn call(
    app: axum::Router,
    method: Method,
    uri: &str,
    body: Option<Value>,
    auth: bool,
) -> axum::response::Response {
    let mut builder = Request::builder().method(method).uri(uri);
    if auth {
        builder = builder.header("authorization", "Bearer secret");
    }
    if body.is_some() {
        builder = builder.header("content-type", "application/json");
    }
    app.oneshot(
        builder
            .body(Body::from(body.map_or_else(String::new, |v| v.to_string())))
            .unwrap(),
    )
    .await
    .unwrap()
}

async fn json_body(response: axum::response::Response) -> Value {
    serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap()
}

#[tokio::test]
async fn health_is_public_but_v1_routes_require_bearer_auth() {
    let app = router();
    assert_eq!(
        call(app.clone(), Method::GET, "/healthz", None, false)
            .await
            .status(),
        StatusCode::OK
    );
    let response = call(app.clone(), Method::GET, "/v1/status", None, false).await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(response.headers()[WWW_AUTHENTICATE], "Bearer");
    let metrics = call(app, Method::GET, "/metrics", None, true).await;
    let body = String::from_utf8(
        metrics
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    assert!(
        body.contains("method=\"GET\",route=\"/v1/status\",status=\"401\""),
        "{body}"
    );
}

#[tokio::test]
async fn tx_then_json_query_round_trips() {
    let app = router();
    let tx = call(
        app.clone(),
        Method::POST,
        "/v1/tx",
        Some(json!({"gql":"INSERT (:Person {_id: 1, name: $name})","params":{"name":"Ada"}})),
        true,
    )
    .await;
    assert_eq!(tx.status(), StatusCode::OK, "{}", json_body(tx).await);
    let query = call(
        app,
        Method::POST,
        "/v1/query",
        Some(json!({"gql":"MATCH (p:Person) RETURN p.name AS name","basis":1})),
        true,
    )
    .await;
    assert_eq!(query.status(), StatusCode::OK);
    assert_eq!(json_body(query).await["rows"], json!([{"name":"Ada"}]));
}

#[tokio::test]
async fn invalid_requests_negotiation_timeout_and_internal_errors_are_stable() {
    let malformed = call(
        router(),
        Method::POST,
        "/v1/query",
        Some(json!({"gql":"MATCH ("})),
        true,
    )
    .await;
    assert_eq!(malformed.status(), StatusCode::BAD_REQUEST);
    assert_eq!(json_body(malformed).await["code"], "invalid_request");

    let params = call(
        router(),
        Method::POST,
        "/v1/query",
        Some(json!({"gql":"MATCH (p) RETURN p", "params":{"bad":[1]}})),
        true,
    )
    .await;
    assert_eq!(params.status(), StatusCode::BAD_REQUEST);

    let request = Request::builder()
        .method(Method::POST)
        .uri("/v1/query")
        .header("authorization", "Bearer secret")
        .header("content-type", "application/json")
        .header("accept", "application/jsonish")
        .body(Body::from(r#"{"gql":"MATCH (p) RETURN p"}"#))
        .unwrap();
    assert_eq!(
        router().oneshot(request).await.unwrap().status(),
        StatusCode::NOT_ACCEPTABLE
    );

    let timeout = call(
        router(),
        Method::POST,
        "/v1/query",
        Some(json!({"gql":"MATCH (p) RETURN p", "basis":999, "basis_timeout_ms":1})),
        true,
    )
    .await;
    assert_eq!(timeout.status(), StatusCode::REQUEST_TIMEOUT);
    assert_eq!(json_body(timeout).await["code"], "basis_timeout");

    let internal = call(
        router(),
        Method::POST,
        "/v1/query",
        Some(json!({"gql":"USE secret_storage_credential; MATCH (p) RETURN p"})),
        true,
    )
    .await;
    assert_eq!(internal.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let body = json_body(internal).await;
    assert_eq!(body["code"], "internal");
    assert!(!body.to_string().contains("secret_storage_credential"));
}

#[tokio::test]
async fn query_node_tx_redirects_to_fresh_advertisement_or_503() {
    let root = TempDir::new().unwrap();
    let writer = varve::Db::open(node_config(&root, &["writer", "query", "compactor"]))
        .await
        .unwrap();
    let query = varve::Db::open(node_config(&root, &["query"]))
        .await
        .unwrap();
    let missing = call(
        router_with_db(query.clone()),
        Method::POST,
        "/v1/tx",
        Some(json!({"gql":"INSERT (:X {_id:1})"})),
        true,
    )
    .await;
    assert_eq!(missing.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(json_body(missing).await["code"], "writer_unavailable");
    writer
        .publish_writer("https://writer.example")
        .await
        .unwrap();
    let redirected = call(
        router_with_db(query.clone()),
        Method::POST,
        "/v1/tx",
        Some(json!({"gql":"INSERT (:X {_id:1})"})),
        true,
    )
    .await;
    assert_eq!(redirected.status(), StatusCode::MISDIRECTED_REQUEST);
    assert_eq!(
        json_body(redirected).await["writer"],
        "https://writer.example"
    );
    for path in ["/v1/admin/compact", "/v1/admin/gc"] {
        let response = call(
            router_with_db(query.clone()),
            Method::POST,
            path,
            None,
            true,
        )
        .await;
        assert_eq!(response.status(), StatusCode::MISDIRECTED_REQUEST, "{path}");
    }
    assert_eq!(
        call(
            router_with_db(query),
            Method::POST,
            "/v1/admin/verify",
            None,
            true
        )
        .await
        .status(),
        StatusCode::OK
    );
}

#[tokio::test]
async fn authorization_accept_and_body_limit_are_parsed_exactly() {
    let duplicate = Request::builder()
        .method(Method::GET)
        .uri("/v1/status")
        .header("authorization", "Bearer secret")
        .header("authorization", "Bearer secret")
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        router().oneshot(duplicate).await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );

    let accepted = Request::builder()
        .method(Method::POST)
        .uri("/v1/query")
        .header("authorization", "Bearer secret")
        .header("content-type", "application/json")
        .header("accept", "application/vnd.apache.arrow.stream; q=1.0")
        .body(Body::from(r#"{"gql":"MATCH (p) RETURN p"}"#))
        .unwrap();
    assert_eq!(
        router().oneshot(accepted).await.unwrap().status(),
        StatusCode::OK
    );
    let arrow_after_json = Request::builder()
        .method(Method::POST)
        .uri("/v1/query")
        .header("authorization", "Bearer secret")
        .header("content-type", "application/json")
        .header(
            "accept",
            "application/json, application/vnd.apache.arrow.stream",
        )
        .body(Body::from(r#"{"gql":"MATCH (p) RETURN p"}"#))
        .unwrap();
    let response = router().oneshot(arrow_after_json).await.unwrap();
    assert_eq!(
        response.headers()["content-type"],
        ARROW_STREAM_CONTENT_TYPE
    );

    let (readiness, _) = readiness_channel();
    let small = http_router(HttpContext {
        frontend: FrontendContext {
            db: varve::Db::memory(),
            authenticator: static_auth(&[("ada", "secret")]).unwrap(),
            metrics: Arc::new(PrometheusMetrics::new().unwrap()),
            probe: ProbeReport {
                verdict: ProbeVerdict::Supported,
                probe_key: "test".into(),
            },
            readiness,
        },
        max_body_bytes: 8,
    });
    let oversized = call(
        small,
        Method::POST,
        "/v1/query",
        Some(json!({"gql":"MATCH (p) RETURN p"})),
        true,
    )
    .await;
    assert_eq!(oversized.status(), StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn status_metrics_admin_and_response_headers_follow_contracts() {
    let app = router();
    let health = call(app.clone(), Method::GET, "/healthz", None, false).await;
    assert_eq!(json_body(health).await, json!({"status":"ok"}));
    let response = call(app.clone(), Method::GET, "/metrics", None, true).await;
    let text = String::from_utf8(
        response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    assert!(
        text.contains("method=\"GET\",route=\"/healthz\",status=\"200\""),
        "{text}"
    );
    for path in ["/v1/status", "/metrics"] {
        let response = call(app.clone(), Method::GET, path, None, true).await;
        assert_eq!(response.status(), StatusCode::OK, "{path}");
        assert_eq!(response.headers()["x-content-type-options"], "nosniff");
        if path == "/metrics" {
            assert!(response.headers()["content-type"]
                .to_str()
                .unwrap()
                .starts_with("text/plain"));
        }
    }
    for path in ["/v1/admin/compact", "/v1/admin/gc", "/v1/admin/verify"] {
        assert_eq!(
            call(app.clone(), Method::POST, path, None, true)
                .await
                .status(),
            StatusCode::OK,
            "{path}"
        );
    }
}

#[tokio::test]
async fn authenticated_subject_is_written_to_the_durable_log() {
    let root = TempDir::new().unwrap();
    let db = varve::Db::open(node_config(&root, &["writer", "query", "compactor"]))
        .await
        .unwrap();
    let response = call(
        router_with_db(db.clone()),
        Method::POST,
        "/v1/tx",
        Some(json!({"gql":"INSERT (:X {_id:1})"})),
        true,
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    drop(db);
    let log = LocalLog::open(&root.path().join("log"), DEFAULT_SEGMENT_MAX_BYTES).unwrap();
    let records = log.tail(LogPosition::ZERO).await.unwrap();
    assert_eq!(records.last().unwrap().1.user, "ada");
}

#[tokio::test]
async fn health_reports_terminal_follower_failure_without_internal_detail() {
    let root = TempDir::new().unwrap();
    let query = varve::Db::open(node_config(&root, &["query"]))
        .await
        .unwrap();
    let log = LocalLog::open(&root.path().join("log"), DEFAULT_SEGMENT_MAX_BYTES).unwrap();
    log.append(vec![LogRecord {
        tx_id: 1,
        system_time_us: 1,
        user: String::new(),
        effects: vec![TableEffects {
            table: "secret_internal_table".into(),
            arrow_ipc: Vec::new(),
            graph: String::new(),
        }],
    }])
    .await
    .unwrap();
    for _ in 0..100 {
        if query.status().await.unwrap().follower_error.is_some() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    assert!(query.status().await.unwrap().follower_error.is_some());
    let app = router_with_db(query);
    let response = call(app.clone(), Method::GET, "/healthz", None, false).await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        json_body(response).await,
        json!({"status":"degraded","error":"follower stopped"})
    );
    let response = call(app, Method::GET, "/metrics", None, true).await;
    let text = String::from_utf8(
        response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    assert!(
        text.contains("method=\"GET\",route=\"/healthz\",status=\"503\""),
        "{text}"
    );
}

#[tokio::test]
async fn health_does_not_depend_on_manifest_storage_reads() {
    // Writer-only: no follower is spawned (`assemble` only starts one for
    // non-writer roles), so nothing in this node ever polls the manifest in
    // the background — the only manifest read left is the one `/v1/status`
    // performs explicitly. That isolates the thing under test: liveness must
    // stay `ok` purely from the in-memory progress watch, independent of
    // whether the object store can be read at all.
    let root = TempDir::new().unwrap();
    let db = varve::Db::open(node_config(&root, &["writer"]))
        .await
        .unwrap();
    let app = router_with_db(db.clone());

    // Sanity: a fresh node with no manifest yet is healthy.
    let health = call(app.clone(), Method::GET, "/healthz", None, false).await;
    assert_eq!(health.status(), StatusCode::OK);
    assert_eq!(json_body(health).await, json!({"status":"ok"}));

    // Corrupt the manifest object directly on disk, after the node has
    // already opened successfully, so any *subsequent* manifest read fails.
    let blocks_dir = root.path().join("store").join("v1").join("blocks");
    std::fs::create_dir_all(&blocks_dir).unwrap();
    std::fs::write(blocks_dir.join("00.manifest"), b"\xFF\xFF\xFF").unwrap();

    // Confirm the manifest really is unreadable: the authenticated
    // `/v1/status` route (which still calls `Db::status`/`latest_manifest`)
    // surfaces the storage failure.
    let status = call(app.clone(), Method::GET, "/v1/status", None, true).await;
    assert_eq!(status.status(), StatusCode::INTERNAL_SERVER_ERROR);

    // `/healthz` must still be `ok`: liveness reflects only the in-memory
    // follower-error state, never the object store.
    let health = call(app, Method::GET, "/healthz", None, false).await;
    assert_eq!(health.status(), StatusCode::OK);
    assert_eq!(json_body(health).await, json!({"status":"ok"}));
}
