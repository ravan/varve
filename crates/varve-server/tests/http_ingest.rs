//! End-to-end contract for `POST /v1/ingest` (roadmap slice BI-1): NDJSON in,
//! `Db::ingest` fast path, GQL read-back out; typed, line-numbered errors;
//! the 415/421/403/422 matrix; and oracle equivalence with the GQL surface.
#![cfg(feature = "http")]
#![allow(clippy::unwrap_used)]

use axum::{
    body::Body,
    http::{Method, Request, StatusCode},
};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use std::sync::Arc;
use tempfile::TempDir;
use tower::ServiceExt;
use varve::{ProbeReport, ProbeVerdict};
use varve_server::{
    http_router, readiness_channel, static_auth, FrontendContext, HttpContext, IngestConfig,
    PrometheusMetrics,
};

fn router_with_db(db: varve::Db) -> axum::Router {
    router_with_ingest(db, IngestConfig::default())
}

fn router_with_ingest(db: varve::Db, ingest: IngestConfig) -> axum::Router {
    router_full(db, 8 * 1024 * 1024, ingest)
}

fn router_full(db: varve::Db, max_body_bytes: usize, ingest: IngestConfig) -> axum::Router {
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
        max_body_bytes,
        ingest,
    })
}

/// POST an NDJSON body delivered as many separate stream frames, exercising
/// the incremental framer across real frame boundaries.
async fn post_ndjson_streamed(app: axum::Router, frames: Vec<Vec<u8>>) -> axum::response::Response {
    let body = Body::from_stream(futures::stream::iter(
        frames.into_iter().map(Ok::<Vec<u8>, std::io::Error>),
    ));
    app.oneshot(
        Request::builder()
            .method(Method::POST)
            .uri("/v1/ingest")
            .header("content-type", "application/x-ndjson")
            .header("authorization", "Bearer secret")
            .body(body)
            .unwrap(),
    )
    .await
    .unwrap()
}

fn router() -> axum::Router {
    router_with_db(varve::Db::memory())
}

fn node_config(root: &TempDir, roles: &[&str]) -> varve_config::Config {
    let roles = roles
        .iter()
        .map(|role| format!("\"{role}\""))
        .collect::<Vec<_>>()
        .join(",");
    varve_config::Config::from_toml_str(&format!(
        "[node]\nroles=[{roles}]\ntail_poll_interval_ms=5\ntail_batch_records=1024\nbasis_timeout_ms=10\n[log]\nbackend=\"local\"\ngroup_commit_window_ms=0\n[log.local]\ndir={:?}\n[storage]\nbackend=\"local\"\nmax_block_rows=100000\nflush_interval_ms=300000\n[storage.local]\ndir={:?}\n",
        root.path().join("log").display().to_string(),
        root.path().join("store").display().to_string()
    ))
    .unwrap()
}

/// POST an NDJSON body with the correct content type.
async fn post_ndjson(app: axum::Router, body: &str, auth: bool) -> axum::response::Response {
    post_typed(app, "application/x-ndjson", body, auth).await
}

/// POST a body under an arbitrary `Content-Type` to `/v1/ingest`.
async fn post_typed(
    app: axum::Router,
    content_type: &str,
    body: &str,
    auth: bool,
) -> axum::response::Response {
    let mut builder = Request::builder()
        .method(Method::POST)
        .uri("/v1/ingest")
        .header("content-type", content_type);
    if auth {
        builder = builder.header("authorization", "Bearer secret");
    }
    app.oneshot(builder.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap()
}

/// POST a JSON body to an arbitrary route (query / tx).
async fn post_json(
    app: axum::Router,
    uri: &str,
    body: Value,
    auth: bool,
) -> axum::response::Response {
    let mut builder = Request::builder()
        .method(Method::POST)
        .uri(uri)
        .header("content-type", "application/json");
    if auth {
        builder = builder.header("authorization", "Bearer secret");
    }
    app.oneshot(builder.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap()
}

async fn json_body(response: axum::response::Response) -> Value {
    serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap()
}

#[tokio::test]
async fn ndjson_nodes_and_edges_round_trip_through_gql_query() {
    let app = router();
    let body = concat!(
        "{\"type\":\"node\",\"labels\":[\"Person\"],\"props\":{\"_id\":\"p1\",\"name\":\"Ada\"}}\n",
        "{\"type\":\"node\",\"labels\":[\"Person\"],\"props\":{\"_id\":\"p2\",\"name\":\"Bob\"}}\n",
        "{\"type\":\"edge\",\"label\":\"KNOWS\",\"src\":\"p1\",\"dst\":\"p2\"}\n",
    );
    let response = post_ndjson(app.clone(), body, true).await;
    assert_eq!(response.status(), StatusCode::OK);
    let summary = json_body(response).await;
    assert_eq!(summary["nodes"], 2);
    assert_eq!(summary["edges"], 1);
    assert_eq!(summary["transactions"], 1);
    let basis = summary["basis"].as_u64().unwrap();
    assert!(basis > 0, "basis must be a committed tx id: {summary}");
    assert!(summary["system_time"].as_str().is_some());

    let query = post_json(
        app,
        "/v1/query",
        json!({
            "gql": "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name AS a, b.name AS b",
            "basis": basis,
        }),
        true,
    )
    .await;
    assert_eq!(query.status(), StatusCode::OK);
    assert_eq!(
        json_body(query).await["rows"],
        json!([{"a": "Ada", "b": "Bob"}])
    );
}

#[tokio::test]
async fn repeated_id_upserts_last_write_wins() {
    let app = router();
    let body = concat!(
        "{\"type\":\"node\",\"labels\":[\"Person\"],\"props\":{\"_id\":7,\"name\":\"first\"}}\n",
        "{\"type\":\"node\",\"labels\":[\"Person\"],\"props\":{\"_id\":7,\"name\":\"second\"}}\n",
    );
    let basis = json_body(post_ndjson(app.clone(), body, true).await).await["basis"]
        .as_u64()
        .unwrap();
    let query = post_json(
        app,
        "/v1/query",
        json!({"gql": "MATCH (p:Person) RETURN p.name AS name", "basis": basis}),
        true,
    )
    .await;
    assert_eq!(json_body(query).await["rows"], json!([{"name": "second"}]));
}

#[tokio::test]
async fn large_stream_commits_in_fixed_chunks() {
    let app = router();
    let mut body = String::new();
    for id in 0..25_000 {
        body.push_str(&format!(
            "{{\"type\":\"node\",\"labels\":[\"N\"],\"props\":{{\"_id\":{id}}}}}\n"
        ));
    }
    let summary = json_body(post_ndjson(app, &body, true).await).await;
    assert_eq!(summary["nodes"], 25_000);
    // 25_000 ops / CHUNK_OPS(10_000) = 3 chunks = 3 transactions.
    assert_eq!(summary["transactions"], 3);
}

#[tokio::test]
async fn ndjson_and_gql_ingestion_are_query_indistinguishable() {
    // The interop invariant: the same small graph loaded via `/v1/ingest`
    // answers a traversal identically to the same graph loaded via GQL over
    // `/v1/tx`. Two fresh in-memory DBs; identical query, identical rows.
    let bulk = router();
    let gql = router();

    let ndjson = concat!(
        "{\"type\":\"node\",\"labels\":[\"Person\"],\"props\":{\"_id\":1,\"name\":\"Ada\"}}\n",
        "{\"type\":\"node\",\"labels\":[\"Person\"],\"props\":{\"_id\":2,\"name\":\"Bob\"}}\n",
        "{\"type\":\"node\",\"labels\":[\"Person\"],\"props\":{\"_id\":3,\"name\":\"Cy\"}}\n",
        "{\"type\":\"edge\",\"label\":\"KNOWS\",\"src\":1,\"dst\":2}\n",
        "{\"type\":\"edge\",\"label\":\"KNOWS\",\"src\":2,\"dst\":3}\n",
    );
    let bulk_basis = json_body(post_ndjson(bulk.clone(), ndjson, true).await).await["basis"]
        .as_u64()
        .unwrap();

    for gql_stmt in [
        "INSERT (:Person {_id: 1, name: 'Ada'}), (:Person {_id: 2, name: 'Bob'}), (:Person {_id: 3, name: 'Cy'})",
        "MATCH (a:Person {_id: 1}), (b:Person {_id: 2}) INSERT (a)-[:KNOWS]->(b)",
        "MATCH (a:Person {_id: 2}), (b:Person {_id: 3}) INSERT (a)-[:KNOWS]->(b)",
    ] {
        let response = post_json(gql.clone(), "/v1/tx", json!({"gql": gql_stmt}), true).await;
        assert_eq!(response.status(), StatusCode::OK, "gql: {gql_stmt}");
    }
    let gql_basis = 3; // three sequential txs on a fresh DB

    let traversal = "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name AS a, b.name AS b";
    let bulk_rows = json_body(
        post_json(
            bulk,
            "/v1/query",
            json!({"gql": traversal, "basis": bulk_basis}),
            true,
        )
        .await,
    )
    .await;
    let gql_rows = json_body(
        post_json(
            gql,
            "/v1/query",
            json!({"gql": traversal, "basis": gql_basis}),
            true,
        )
        .await,
    )
    .await;
    // Sort-independent compare: both must contain Ada→Bob and Bob→Cy.
    let mut bulk_set = bulk_rows["rows"].as_array().unwrap().clone();
    let mut gql_set = gql_rows["rows"].as_array().unwrap().clone();
    bulk_set.sort_by_key(|v| v.to_string());
    gql_set.sort_by_key(|v| v.to_string());
    assert_eq!(bulk_set, gql_set);
    assert_eq!(bulk_set.len(), 2);
}

#[tokio::test]
async fn wrong_content_type_is_415() {
    let response = Request::builder()
        .method(Method::POST)
        .uri("/v1/ingest")
        .header("authorization", "Bearer secret")
        .header("content-type", "application/json")
        .body(Body::from("{\"type\":\"node\",\"props\":{}}"))
        .unwrap();
    let response = router().oneshot(response).await.unwrap();
    assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
}

#[tokio::test]
async fn missing_bearer_is_401() {
    let response = post_ndjson(router(), "{\"type\":\"node\",\"props\":{}}", false).await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn query_node_ingest_redirects_to_writer_or_503() {
    let root = TempDir::new().unwrap();
    let writer = varve::Db::open(node_config(&root, &["writer", "query", "compactor"]))
        .await
        .unwrap();
    let query = varve::Db::open(node_config(&root, &["query"]))
        .await
        .unwrap();

    let body = "{\"type\":\"node\",\"props\":{\"_id\":1}}";
    let missing = post_ndjson(router_with_db(query.clone()), body, true).await;
    assert_eq!(missing.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(json_body(missing).await["code"], "writer_unavailable");

    writer
        .publish_writer("https://writer.example")
        .await
        .unwrap();
    let redirected = post_ndjson(router_with_db(query), body, true).await;
    assert_eq!(redirected.status(), StatusCode::MISDIRECTED_REQUEST);
    assert_eq!(
        json_body(redirected).await["writer"],
        "https://writer.example"
    );
}

#[tokio::test]
async fn ingest_enforces_write_grants_under_security() {
    // BI-4: `/v1/ingest` enforces the SAME write grants as a GQL INSERT.
    // `root` is a bootstrap admin (bypasses); `ada` is a plain principal who
    // must be granted WRITE on a label to ingest it.
    let config = varve_config::Config::from_toml_str(
        "[log]\ngroup_commit_window_ms = 0\n[security]\nenabled = true\nadmins = [\"root\"]\n",
    )
    .unwrap();
    let db = varve::Db::open(config).await.unwrap();
    let (readiness, _) = readiness_channel();
    let app = http_router(HttpContext {
        frontend: FrontendContext {
            db,
            authenticator: static_auth(&[("root", "roottok"), ("ada", "adatok")]).unwrap(),
            metrics: Arc::new(PrometheusMetrics::new().unwrap()),
            probe: ProbeReport {
                verdict: ProbeVerdict::Supported,
                probe_key: "test".into(),
            },
            readiness,
        },
        max_body_bytes: 1024 * 1024,
        ingest: varve_server::IngestConfig::default(),
    });
    // Distinct `_id` per post so no put collides with an existing entity
    // (updating an entity also requires WRITE on its CURRENT labels).
    let ingest_as = |token: &'static str, label: &'static str, id: u32| {
        let app = app.clone();
        async move {
            app.oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/v1/ingest")
                    .header("authorization", format!("Bearer {token}"))
                    .header("content-type", "application/x-ndjson")
                    .body(Body::from(format!(
                        "{{\"type\":\"node\",\"labels\":[\"{label}\"],\"props\":{{\"_id\":{id}}}}}"
                    )))
                    .unwrap(),
            )
            .await
            .unwrap()
        }
    };

    // Admin bypasses enforcement entirely.
    let admin = ingest_as("roottok", "X", 1).await;
    assert_eq!(admin.status(), StatusCode::OK);
    assert_eq!(json_body(admin).await["nodes"], 1);

    // Plain principal with no grants: denied with 403 naming the label.
    let denied = ingest_as("adatok", "Widget", 2).await;
    assert_eq!(denied.status(), StatusCode::FORBIDDEN);
    let body = json_body(denied).await;
    assert!(body["error"].as_str().unwrap().contains("Widget"), "{body}");
    assert_eq!(body["committed"]["transactions"], 0);

    // Grant `ada` WRITE on Widget (as admin, over /v1/tx), then ingest works.
    for gql in [
        "CREATE ROLE writers",
        "GRANT WRITE ON GRAPH * NODES Widget TO ROLE writers",
        "GRANT ROLE writers TO USER 'ada'",
    ] {
        let r = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/v1/tx")
                    .header("authorization", "Bearer roottok")
                    .header("content-type", "application/json")
                    .body(Body::from(json!({"gql": gql}).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::OK, "admin DDL: {gql}");
    }
    let granted = ingest_as("adatok", "Widget", 3).await;
    let status = granted.status();
    assert_eq!(status, StatusCode::OK, "{}", json_body(granted).await);
}

#[tokio::test]
async fn valid_time_loads_history_and_answers_as_of() {
    // Load a node valid only during 2020 (NDJSON valid_from/valid_to), then
    // query FOR VALID_TIME AS OF inside and outside the interval.
    let app = router();
    let body =
        "{\"type\":\"node\",\"labels\":[\"Person\"],\"props\":{\"_id\":\"p1\",\"name\":\"Ada\"},\
                \"valid_from\":\"2020-01-01T00:00:00Z\",\"valid_to\":\"2021-01-01T00:00:00Z\"}\n";
    let basis = json_body(post_ndjson(app.clone(), body, true).await).await["basis"]
        .as_u64()
        .unwrap();

    for (as_of, expected) in [("2020-06-01T00:00:00Z", 1), ("2022-06-01T00:00:00Z", 0)] {
        let rows = json_body(
            post_json(
                app.clone(),
                "/v1/query",
                json!({
                    "gql": format!(
                        "MATCH (p:Person) FOR VALID_TIME AS OF TIMESTAMP '{as_of}' RETURN p._id"
                    ),
                    "basis": basis,
                }),
                true,
            )
            .await,
        )
        .await;
        assert_eq!(
            rows["rows"].as_array().unwrap().len(),
            expected,
            "AS OF {as_of}"
        );
    }
}

#[tokio::test]
async fn csv_valid_time_columns_load_history() {
    // CSV :VALID_FROM/:VALID_TO carry the same interval; AS OF sees it.
    let app = router();
    let csv = ":ID,name,:LABEL,:VALID_FROM,:VALID_TO\n\
               p1,Ada,Person,2020-01-01T00:00:00Z,2021-01-01T00:00:00Z\n";
    let basis = json_body(post_typed(app.clone(), "text/csv", csv, true).await).await["basis"]
        .as_u64()
        .unwrap();
    let inside = json_body(
        post_json(
            app,
            "/v1/query",
            json!({
                "gql": "MATCH (p:Person) FOR VALID_TIME AS OF TIMESTAMP '2020-06-01T00:00:00Z' RETURN p._id",
                "basis": basis,
            }),
            true,
        )
        .await,
    )
    .await;
    assert_eq!(inside["rows"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn bulk_writes_are_visible_on_a_follower_at_basis() {
    // Bulk ingest replicates through the normal log, so a query node sees the
    // rows once it has applied up to the returned basis — same as a GQL write.
    let root = TempDir::new().unwrap();
    let writer = varve::Db::open(node_config(&root, &["writer", "query", "compactor"]))
        .await
        .unwrap();
    let query = varve::Db::open(node_config(&root, &["query"]))
        .await
        .unwrap();

    let body = concat!(
        "{\"type\":\"node\",\"labels\":[\"Person\"],\"props\":{\"_id\":\"p1\",\"name\":\"Ada\"}}\n",
        "{\"type\":\"node\",\"labels\":[\"Person\"],\"props\":{\"_id\":\"p2\",\"name\":\"Bob\"}}\n",
    );
    let ingested = post_ndjson(router_with_db(writer), body, true).await;
    assert_eq!(ingested.status(), StatusCode::OK);
    let basis = json_body(ingested).await["basis"].as_u64().unwrap();

    let rows = post_json(
        router_with_db(query),
        "/v1/query",
        json!({"gql": "MATCH (p:Person) RETURN p.name AS name", "basis": basis}),
        true,
    )
    .await;
    assert_eq!(rows.status(), StatusCode::OK);
    assert_eq!(json_body(rows).await["rows"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn empty_and_blank_only_bodies_are_422() {
    for body in ["", "\n\n   \n"] {
        let response = post_ndjson(router(), body, true).await;
        assert_eq!(
            response.status(),
            StatusCode::UNPROCESSABLE_ENTITY,
            "{body:?}"
        );
        let json = json_body(response).await;
        assert_eq!(json["committed"]["transactions"], 0);
    }
}

#[tokio::test]
async fn malformed_record_reports_line_number_and_zero_committed() {
    let body = concat!(
        "{\"type\":\"node\",\"props\":{\"_id\":1}}\n",
        "{\"type\":\"node\",\"props\":{\"_id\":2}}\n",
        "{\"type\":\"edge\",\"label\":\"KNOWS\",\"src\":\"a\"}\n",
    );
    let response = post_ndjson(router(), body, true).await;
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let json = json_body(response).await;
    assert!(
        json["error"].as_str().unwrap().starts_with("line 3:"),
        "{json}"
    );
    assert!(json["error"].as_str().unwrap().contains("dst"), "{json}");
    // Nothing committed: BI-1 buffers and decodes before any chunk submit.
    assert_eq!(json["committed"]["nodes"], 0);
    assert_eq!(json["committed"]["transactions"], 0);
}

#[tokio::test]
async fn streaming_body_across_many_frames_commits_correctly() {
    // The body arrives as one frame per byte, so records span frame
    // boundaries — the incremental framer must reassemble them.
    let app = router();
    let ndjson = concat!(
        "{\"type\":\"node\",\"labels\":[\"Person\"],\"props\":{\"_id\":\"p1\",\"name\":\"Ada\"}}\n",
        "{\"type\":\"node\",\"labels\":[\"Person\"],\"props\":{\"_id\":\"p2\",\"name\":\"Bob\"}}\n",
        "{\"type\":\"edge\",\"label\":\"KNOWS\",\"src\":\"p1\",\"dst\":\"p2\"}\n",
    );
    let frames: Vec<Vec<u8>> = ndjson.as_bytes().chunks(1).map(<[u8]>::to_vec).collect();
    let response = post_ndjson_streamed(app.clone(), frames).await;
    assert_eq!(response.status(), StatusCode::OK);
    let summary = json_body(response).await;
    assert_eq!(summary["nodes"], 2);
    assert_eq!(summary["edges"], 1);
    let basis = summary["basis"].as_u64().unwrap();
    let query = post_json(
        app,
        "/v1/query",
        json!({
            "gql": "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name AS knower, b.name AS known",
            "basis": basis,
        }),
        true,
    )
    .await;
    assert_eq!(
        json_body(query).await["rows"],
        json!([{"knower": "Ada", "known": "Bob"}])
    );
}

#[tokio::test]
async fn mid_stream_decode_error_reports_earlier_committed_progress() {
    // Committed progress = whole chunks that flushed BEFORE the error. With a
    // tiny chunk size, frame 1's two nodes flush as chunk 1; frame 2's
    // malformed line then fails — so `committed` is non-zero, the streaming
    // per-chunk-atomicity contract the buffered BI-1 path could not show.
    let ingest = IngestConfig {
        chunk_ops: 2,
        ..IngestConfig::default()
    };
    let app = router_with_ingest(varve::Db::memory(), ingest);
    let frames = vec![
        b"{\"type\":\"node\",\"props\":{\"_id\":1}}\n{\"type\":\"node\",\"props\":{\"_id\":2}}\n"
            .to_vec(),
        b"{\"type\":\"edge\",\"label\":\"KNOWS\",\"src\":\"a\"}\n".to_vec(),
    ];
    let response = post_ndjson_streamed(app, frames).await;
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let json = json_body(response).await;
    assert!(
        json["error"].as_str().unwrap().starts_with("line 3:"),
        "{json}"
    );
    assert_eq!(json["committed"]["nodes"], 2);
    assert_eq!(json["committed"]["transactions"], 1);
}

#[tokio::test]
async fn ingest_is_exempt_from_the_request_body_limit() {
    // A body larger than `max_body_bytes` is rejected on `/v1/query` (413) but
    // streams through `/v1/ingest` (200) — the route-level disable at work.
    let max_body_bytes = 32;
    let app = router_full(varve::Db::memory(), max_body_bytes, IngestConfig::default());
    let mut body = String::new();
    for id in 0..10 {
        body.push_str(&format!(
            "{{\"type\":\"node\",\"props\":{{\"_id\":{id}}}}}\n"
        ));
    }
    assert!(body.len() > max_body_bytes);

    let ingested = post_ndjson(app.clone(), &body, true).await;
    assert_eq!(ingested.status(), StatusCode::OK);
    assert_eq!(json_body(ingested).await["nodes"], 10);

    // The same-size body on the capped `/v1/query` route is 413.
    let oversized = post_json(
        app,
        "/v1/query",
        json!({"gql": "MATCH (p) RETURN p /* padding padding padding padding */"}),
        true,
    )
    .await;
    assert_eq!(oversized.status(), StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn gzip_encoded_body_is_transparently_decompressed() {
    use flate2::{write::GzEncoder, Compression};
    use std::io::Write;

    let app = router();
    let ndjson = concat!(
        "{\"type\":\"node\",\"labels\":[\"Person\"],\"props\":{\"_id\":\"p1\",\"name\":\"Ada\"}}\n",
        "{\"type\":\"node\",\"labels\":[\"Person\"],\"props\":{\"_id\":\"p2\",\"name\":\"Bob\"}}\n",
        "{\"type\":\"edge\",\"label\":\"KNOWS\",\"src\":\"p1\",\"dst\":\"p2\"}\n",
    );
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(ndjson.as_bytes()).unwrap();
    let gzipped = encoder.finish().unwrap();

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/v1/ingest")
                .header("content-type", "application/x-ndjson")
                .header("content-encoding", "gzip")
                .header("authorization", "Bearer secret")
                .body(Body::from(gzipped))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let summary = json_body(response).await;
    assert_eq!(summary["nodes"], 2);
    assert_eq!(summary["edges"], 1);
    let basis = summary["basis"].as_u64().unwrap();
    let query = post_json(
        app,
        "/v1/query",
        json!({"gql": "MATCH (p:Person) RETURN p.name AS name", "basis": basis}),
        true,
    )
    .await;
    assert_eq!(json_body(query).await["rows"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn neo4j_csv_nodes_and_edges_load_and_answer_traversal() {
    // The exact `neo4j-admin database import` shape: a nodes file then an
    // edges file, each POSTed as text/csv. Typed columns round-trip and the
    // KNOWS traversal answers just as the NDJSON/GQL paths do.
    let app = router();
    let nodes = "\
:ID,name,age:int,:LABEL
p1,Ada,36,Person
p2,Bob,29,Person
p3,Cy,41,Person
";
    let edges = "\
:START_ID,:END_ID,:TYPE,since:int
p1,p2,KNOWS,2001
p2,p3,KNOWS,2012
";
    // POST each file exactly once: edges here carry no `:ID`, so a re-POST
    // would auto-generate fresh edge ids and duplicate the relationships
    // (same as re-running an edge `INSERT`).
    let n = post_typed(app.clone(), "text/csv", nodes, true).await;
    assert_eq!(n.status(), StatusCode::OK);
    assert_eq!(json_body(n).await["nodes"], 3);

    let e = post_typed(app.clone(), "text/csv", edges, true).await;
    assert_eq!(e.status(), StatusCode::OK);
    let e_body = json_body(e).await;
    assert_eq!(e_body["edges"], 2);
    let basis = e_body["basis"].as_u64().unwrap();

    let rows = json_body(
        post_json(
            app,
            "/v1/query",
            json!({
                "gql": "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name AS knower, b.name AS known, a.age AS age",
                "basis": basis,
            }),
            true,
        )
        .await,
    )
    .await;
    let mut set = rows["rows"].as_array().unwrap().clone();
    set.sort_by_key(|v| v.to_string());
    let mut expected = vec![
        json!({"knower": "Ada", "known": "Bob", "age": 36}),
        json!({"knower": "Bob", "known": "Cy", "age": 29}),
    ];
    expected.sort_by_key(|v| v.to_string());
    assert_eq!(set, expected);
}

#[tokio::test]
async fn csv_and_ndjson_ingestion_are_query_indistinguishable() {
    // Interop: the same graph via CSV vs via NDJSON answers identically.
    let via_csv = router();
    let via_ndjson = router();

    let csv_basis = {
        let _ = post_typed(
            via_csv.clone(),
            "text/csv",
            ":ID,name,:LABEL\np1,Ada,Person\np2,Bob,Person\n",
            true,
        )
        .await;
        json_body(
            post_typed(
                via_csv.clone(),
                "text/csv",
                ":START_ID,:END_ID,:TYPE\np1,p2,KNOWS\n",
                true,
            )
            .await,
        )
        .await["basis"]
            .as_u64()
            .unwrap()
    };
    let ndjson_basis = json_body(
        post_ndjson(
            via_ndjson.clone(),
            concat!(
                "{\"type\":\"node\",\"labels\":[\"Person\"],\"props\":{\"_id\":\"p1\",\"name\":\"Ada\"}}\n",
                "{\"type\":\"node\",\"labels\":[\"Person\"],\"props\":{\"_id\":\"p2\",\"name\":\"Bob\"}}\n",
                "{\"type\":\"edge\",\"label\":\"KNOWS\",\"src\":\"p1\",\"dst\":\"p2\"}\n",
            ),
            true,
        )
        .await,
    )
    .await["basis"]
        .as_u64()
        .unwrap();

    let q = "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name AS knower, b.name AS known";
    let csv_rows = json_body(
        post_json(
            via_csv,
            "/v1/query",
            json!({"gql": q, "basis": csv_basis}),
            true,
        )
        .await,
    )
    .await;
    let ndjson_rows = json_body(
        post_json(
            via_ndjson,
            "/v1/query",
            json!({"gql": q, "basis": ndjson_basis}),
            true,
        )
        .await,
    )
    .await;
    assert_eq!(csv_rows["rows"], ndjson_rows["rows"]);
    assert_eq!(csv_rows["rows"], json!([{"knower": "Ada", "known": "Bob"}]));
}

#[tokio::test]
async fn csv_streamed_across_frames_and_bad_header_is_422() {
    // Frame-split CSV still decodes; an unknown meta-header is a 422 carrying
    // the header context.
    let app = router();
    let csv = ":ID,name,:LABEL\np1,Ada,Person\np2,Bob,Person\n";
    let frames: Vec<Vec<u8>> = csv.as_bytes().chunks(3).map(<[u8]>::to_vec).collect();
    let body = Body::from_stream(futures::stream::iter(
        frames.into_iter().map(Ok::<Vec<u8>, std::io::Error>),
    ));
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/v1/ingest")
                .header("content-type", "text/csv")
                .header("authorization", "Bearer secret")
                .body(body)
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(json_body(response).await["nodes"], 2);

    let bad = post_typed(app, "text/csv", ":ID,:BOGUS\np1,x\n", true).await;
    assert_eq!(bad.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert!(json_body(bad).await["error"]
        .as_str()
        .unwrap()
        .contains(":BOGUS"),);
}

#[tokio::test]
async fn metrics_label_the_ingest_route() {
    // The observe middleware and the metrics registry are shared through the
    // one `HttpContext`, so the exposition must be read from the SAME app
    // instance that served the ingest POST.
    let app = router();
    let ok = post_ndjson(
        app.clone(),
        "{\"type\":\"node\",\"labels\":[\"X\"],\"props\":{\"_id\":1}}",
        true,
    )
    .await;
    assert_eq!(ok.status(), StatusCode::OK);

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/metrics")
                .header("authorization", "Bearer secret")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
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
        text.contains("method=\"POST\",route=\"/v1/ingest\",status=\"200\""),
        "{text}"
    );
}

#[tokio::test]
async fn ingest_records_bytes_and_transactions_are_metered() {
    // Two chunks (chunk_ops = 2, four records) so the transaction counter is
    // unambiguously exercised: 4 records / 2 = 2 transactions.
    let ingest = IngestConfig {
        chunk_ops: 2,
        ..IngestConfig::default()
    };
    let app = router_with_ingest(varve::Db::memory(), ingest);
    let body = concat!(
        "{\"type\":\"node\",\"props\":{\"_id\":1}}\n",
        "{\"type\":\"node\",\"props\":{\"_id\":2}}\n",
        "{\"type\":\"node\",\"props\":{\"_id\":3}}\n",
        "{\"type\":\"edge\",\"label\":\"KNOWS\",\"src\":1,\"dst\":2}\n",
    );
    let ok = post_ndjson(app.clone(), body, true).await;
    assert_eq!(ok.status(), StatusCode::OK);

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/metrics")
                .header("authorization", "Bearer secret")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
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
    assert!(text.contains("varve_ingest_records_total 4"), "{text}");
    assert!(text.contains("varve_ingest_transactions_total 2"), "{text}");
    // Bytes are non-zero (the streamed body length) and the duration
    // histogram is exposed.
    assert!(
        text.lines()
            .any(|line| line.starts_with("varve_ingest_bytes_total ")
                && line != "varve_ingest_bytes_total 0"),
        "{text}"
    );
    assert!(
        text.contains("varve_ingest_duration_seconds_count 1"),
        "{text}"
    );
}
