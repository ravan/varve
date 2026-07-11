#![cfg(feature = "http")]
#![allow(clippy::unwrap_used)]

use arrow::ipc::reader::StreamReader;
use axum::{body::Body, http::Request};
use std::{io::Cursor, sync::Arc};
use tower::ServiceExt;
use varve::{ProbeReport, ProbeVerdict};
use varve_server::{
    http_router, readiness_channel, static_auth, FrontendContext, HttpContext, PrometheusMetrics,
};

#[tokio::test]
async fn arrow_response_is_a_valid_chunked_ipc_stream() {
    let db = varve::Db::memory();
    let rows = 8_300usize;
    let gql = format!(
        "INSERT {}",
        (0..rows)
            .map(|id| format!("(:Person {{_id: {id}, name: 'Ada'}})"))
            .collect::<Vec<_>>()
            .join(", ")
    );
    db.execute(&gql).await.unwrap();
    let (readiness, _) = readiness_channel();
    let app = http_router(HttpContext {
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
    });
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/query")
                .header("authorization", "Bearer secret")
                .header("content-type", "application/json")
                .header("accept", "application/vnd.apache.arrow.stream")
                .body(Body::from(
                    r#"{"gql":"MATCH (p:Person) RETURN p.name AS name"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let chunks =
        futures::StreamExt::collect::<Vec<_>>(response.into_body().into_data_stream()).await;
    assert!(chunks.len() >= 2, "schema and record batch are separate");
    let bytes = chunks
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
        .concat();
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
