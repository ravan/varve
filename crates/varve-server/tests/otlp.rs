#![cfg(all(feature = "http", feature = "otel"))]
#![allow(clippy::unwrap_used)]

use axum::{extract::State, routing::post, Json, Router};
use serde_json::Value;
use std::time::Duration;
use tokio::sync::mpsc;
use varve_server::{MetricsSink, OtlpMetrics, PrometheusMetrics};

async fn capture(State(tx): State<mpsc::UnboundedSender<Value>>, Json(body): Json<Value>) {
    let _ = tx.send(body);
}

/// Push integration test (brief step 2): a real axum server captures every
/// `POST /v1/metrics` body; `OtlpMetrics` (push interval 50ms) is driven by
/// one `observe_request`, and within 2s one of the pushed OTLP/JSON bodies
/// must contain the counter it just recorded.
#[tokio::test]
async fn otlp_metrics_pushes_prometheus_registry_as_otlp_json() {
    let (tx, mut rx) = mpsc::unbounded_channel::<Value>();
    let router = Router::new()
        .route("/v1/metrics", post(capture))
        .with_state(tx);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });

    let metrics = OtlpMetrics::new(
        PrometheusMetrics::new().unwrap(),
        format!("http://{addr}/v1/metrics"),
        Duration::from_millis(50),
    );
    metrics.observe_request("GET", "/v1/query", 200, Duration::from_millis(5));

    let found = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let body = rx
                .recv()
                .await
                .expect("push server must keep receiving bodies");
            if body.to_string().contains("varve_http_requests_total") {
                return;
            }
        }
    })
    .await;
    found.unwrap_or_else(|_| panic!("otlp push must be captured within 2s"));
}
