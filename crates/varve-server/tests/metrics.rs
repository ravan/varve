use std::time::Duration;
use varve::Config;
use varve_server::{MetricsSink, PrometheusMetrics};

#[test]
fn prometheus_metrics_encode_request_labels_and_instances_are_isolated() {
    let metrics = PrometheusMetrics::new().unwrap();
    metrics.observe_request("GET", "/query", 200, Duration::from_millis(10));
    metrics.observe_request("POST", "/tx", 421, Duration::from_millis(20));
    let encoded = metrics.encode().unwrap();

    assert!(encoded
        .contains("varve_http_requests_total{method=\"GET\",route=\"/query\",status=\"200\"} 1"));
    assert!(encoded
        .contains("varve_http_requests_total{method=\"POST\",route=\"/tx\",status=\"421\"} 1"));
    assert!(encoded
        .contains("varve_http_request_duration_seconds_count{method=\"GET\",route=\"/query\"} 1"));
    assert!(encoded.contains("varve_applied_tx_id"));
    assert!(encoded.contains("varve_applied_log_position"));
    assert!(encoded.contains("varve_manifest_watermark"));
    assert!(encoded.contains("varve_follower_healthy"));

    let second = PrometheusMetrics::new().unwrap();
    let second_encoded = second.encode().unwrap();
    assert!(!second_encoded.contains("method=\"GET\""));
}

#[tokio::test]
async fn prometheus_metrics_publish_node_progress() {
    let config = Config::from_toml_str(
        "[node]\nroles=['writer','query']\n[log]\nbackend='memory'\n[storage]\nbackend='memory'",
    )
    .unwrap();
    let db = varve::Db::open(config).await.unwrap();
    let status = db.status().await.unwrap();
    let metrics = PrometheusMetrics::new().unwrap();
    metrics.set_progress(&status);
    let encoded = metrics.encode().unwrap();
    assert!(encoded.contains("varve_applied_tx_id 0"));
    assert!(encoded.contains("varve_applied_log_position 0"));
    assert!(encoded.contains("varve_manifest_watermark 0"));
    assert!(encoded.contains("varve_follower_healthy 1"));
}
