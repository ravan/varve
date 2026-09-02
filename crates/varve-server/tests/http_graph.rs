//! Silt-0a tasks 4-6: the `graph` request field on `/v1/query` and `/v1/tx`,
//! the `?graph=` query parameter on `/v1/ingest`, the `subject` field on
//! every write answer, and graph isolation under `[security]`.
#![cfg(feature = "http")]
#![allow(clippy::unwrap_used)]

use axum::{
    body::Body,
    http::{Method, Request, StatusCode},
};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use std::sync::Arc;
use tower::ServiceExt;
use varve::{ProbeReport, ProbeVerdict};
use varve_server::{
    http_router, readiness_channel, static_auth, FrontendContext, HttpContext, IngestConfig,
    PrometheusMetrics,
};

fn router_with(db: varve::Db, tokens: &[(&str, &str)]) -> axum::Router {
    let (readiness, _) = readiness_channel();
    http_router(HttpContext {
        frontend: FrontendContext {
            db,
            authenticator: static_auth(tokens).unwrap(),
            metrics: Arc::new(PrometheusMetrics::new().unwrap()),
            probe: ProbeReport {
                verdict: ProbeVerdict::Supported,
                probe_key: "test".into(),
            },
            readiness,
        },
        max_body_bytes: 1024 * 1024,
        ingest: IngestConfig::default(),
    })
}

fn router() -> axum::Router {
    router_with(varve::Db::memory(), &[("demo", "secret")])
}

async fn post_json(
    app: axum::Router,
    token: &str,
    uri: &str,
    body: Value,
) -> axum::response::Response {
    app.oneshot(
        Request::builder()
            .method(Method::POST)
            .uri(uri)
            .header("authorization", format!("Bearer {token}"))
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap(),
    )
    .await
    .unwrap()
}

async fn post_ndjson(
    app: axum::Router,
    token: &str,
    uri: &str,
    body: &str,
) -> axum::response::Response {
    app.oneshot(
        Request::builder()
            .method(Method::POST)
            .uri(uri)
            .header("authorization", format!("Bearer {token}"))
            .header("content-type", "application/x-ndjson")
            .body(Body::from(body.to_string()))
            .unwrap(),
    )
    .await
    .unwrap()
}

async fn json_body(response: axum::response::Response) -> Value {
    serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap()
}

async fn tx(app: &axum::Router, token: &str, body: Value) -> (StatusCode, Value) {
    let response = post_json(app.clone(), token, "/v1/tx", body).await;
    let status = response.status();
    (status, json_body(response).await)
}

async fn query(app: &axum::Router, token: &str, body: Value) -> (StatusCode, Value) {
    let response = post_json(app.clone(), token, "/v1/query", body).await;
    let status = response.status();
    (status, json_body(response).await)
}

fn row_count(body: &Value) -> usize {
    body["rows"]
        .as_array()
        .map(Vec::len)
        .unwrap_or_else(|| panic!("{body}"))
}

/// Task 4: `graph` selects the target on both write and read; the answer
/// names the subject; an unknown graph is a 404 that names the graph; a
/// field plus `USE` is a 400.
#[tokio::test]
async fn graph_field_targets_a_named_graph_on_tx_and_query() {
    let app = router();
    let (status, body) = tx(&app, "secret", json!({"gql": "CREATE GRAPH g"})).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["subject"], "demo");

    let (status, body) = tx(
        &app,
        "secret",
        json!({"gql": "INSERT (:P {_id: 1})", "graph": "g"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["subject"], "demo");
    assert_eq!(body["side_effects"]["nodes_created"], 1);

    let (status, body) = query(
        &app,
        "secret",
        json!({"gql": "MATCH (p:P) RETURN p._id AS id", "graph": "g"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(row_count(&body), 1);

    let (status, body) = query(
        &app,
        "secret",
        json!({"gql": "MATCH (p:P) RETURN p._id AS id"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(row_count(&body), 0);

    // `null` means absent.
    let (status, body) = query(
        &app,
        "secret",
        json!({"gql": "MATCH (p:P) RETURN p._id AS id", "graph": null}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(row_count(&body), 0);

    for uri in ["/v1/query", "/v1/tx"] {
        let gql = if uri == "/v1/tx" {
            "INSERT (:P {_id: 2})"
        } else {
            "MATCH (p:P) RETURN p._id AS id"
        };
        let response = post_json(
            app.clone(),
            "secret",
            uri,
            json!({"gql": gql, "graph": "nope"}),
        )
        .await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{uri}");
        let body = json_body(response).await;
        assert_eq!(body["code"], "unknown_graph", "{uri}: {body}");
        assert!(
            body["message"].as_str().unwrap().contains("'nope'"),
            "{uri}: {body}"
        );

        let response = post_json(
            app.clone(),
            "secret",
            uri,
            json!({"gql": format!("USE h; {gql}"), "graph": "g"}),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
        let body = json_body(response).await;
        assert_eq!(body["code"], "invalid_request", "{uri}: {body}");
        assert!(
            body["message"]
                .as_str()
                .unwrap()
                .contains("graph given twice"),
            "{uri}: {body}"
        );

        let response = post_json(
            app.clone(),
            "secret",
            uri,
            json!({"gql": gql, "graph": "__meta"}),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
        let body = json_body(response).await;
        assert_eq!(body["code"], "invalid_request", "{uri}: {body}");
    }
}

/// Task 5: `?graph=` on `/v1/ingest` selects the target; the answer names
/// the subject; an unknown graph is a 404 before any chunk commits.
#[tokio::test]
async fn ingest_graph_parameter_targets_a_named_graph() {
    let app = router();
    let (status, body) = tx(&app, "secret", json!({"gql": "CREATE GRAPH g"})).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let records = "{\"type\":\"node\",\"labels\":[\"P\"],\"props\":{\"_id\":1}}\n\
                   {\"type\":\"node\",\"labels\":[\"P\"],\"props\":{\"_id\":2}}\n";
    let response = post_ndjson(app.clone(), "secret", "/v1/ingest?graph=g", records).await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = json_body(response).await;
    assert_eq!(body["nodes"], 2, "{body}");
    assert_eq!(body["subject"], "demo", "{body}");

    let (status, body) = query(
        &app,
        "secret",
        json!({"gql": "MATCH (p:P) RETURN p._id AS id", "graph": "g"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(row_count(&body), 2);
    let (_, body) = query(
        &app,
        "secret",
        json!({"gql": "MATCH (p:P) RETURN p._id AS id"}),
    )
    .await;
    assert_eq!(row_count(&body), 0);

    let response = post_ndjson(app.clone(), "secret", "/v1/ingest?graph=nope", records).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let body = json_body(response).await;
    assert!(body["error"].as_str().unwrap().contains("'nope'"), "{body}");
    assert_eq!(
        body["committed"],
        json!({"nodes": 0, "edges": 0, "transactions": 0, "basis": 0}),
        "{body}"
    );

    let response = post_ndjson(app.clone(), "secret", "/v1/ingest?graph=__meta", records).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    // No parameter: the default graph, as before.
    let response = post_ndjson(app.clone(), "secret", "/v1/ingest", records).await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = json_body(response).await;
    assert_eq!(body["subject"], "demo", "{body}");
    let (_, body) = query(
        &app,
        "secret",
        json!({"gql": "MATCH (p:P) RETURN p._id AS id"}),
    )
    .await;
    assert_eq!(row_count(&body), 2);
}

/// Task 6: a reader granted `ON GRAPH a` sees `a`; on `b` the read path
/// keeps the engine's deny-by-default rule (reads are filtered, so zero
/// rows, never a leak) and the write path refuses (403); ingest into `a`
/// without WRITE is refused too.
#[tokio::test]
async fn named_graph_isolation_under_security() {
    let config = varve_config::Config::from_toml_str(
        "[log]\ngroup_commit_window_ms = 0\n\
         [security]\nenabled = true\nadmins = [\"root\"]\n",
    )
    .unwrap();
    let db = varve::Db::open(config).await.unwrap();
    let app = router_with(db, &[("root", "roottok"), ("ada", "adatok")]);

    for gql in [
        "CREATE GRAPH a",
        "CREATE GRAPH b",
        "CREATE ROLE reader",
        "GRANT READ ON GRAPH a NODES * TO ROLE reader",
        "GRANT ROLE reader TO USER 'ada'",
    ] {
        let (status, body) = tx(&app, "roottok", json!({"gql": gql})).await;
        assert_eq!(status, StatusCode::OK, "root: {gql}: {body}");
        assert_eq!(body["subject"], "root");
    }
    for graph in ["a", "b"] {
        let (status, body) = tx(
            &app,
            "roottok",
            json!({"gql": "INSERT (:P {_id: 1})", "graph": graph}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "root insert {graph}: {body}");
    }

    let (status, body) = query(
        &app,
        "adatok",
        json!({"gql": "MATCH (p:P) RETURN p._id AS id", "graph": "a"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(row_count(&body), 1);

    let (status, body) = query(
        &app,
        "adatok",
        json!({"gql": "MATCH (p:P) RETURN p._id AS id", "graph": "b"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(row_count(&body), 0, "{body}");

    let record = "{\"type\":\"node\",\"labels\":[\"P\"],\"props\":{\"_id\":9}}\n";
    for graph in ["a", "b"] {
        let response = post_ndjson(
            app.clone(),
            "adatok",
            &format!("/v1/ingest?graph={graph}"),
            record,
        )
        .await;
        assert_eq!(response.status(), StatusCode::FORBIDDEN, "ingest {graph}");
    }
    // Nothing leaked into either graph.
    for graph in ["a", "b"] {
        let (status, body) = query(
            &app,
            "roottok",
            json!({"gql": "MATCH (p:P) RETURN p._id AS id", "graph": graph}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(row_count(&body), 1, "{graph}: {body}");
    }
}
