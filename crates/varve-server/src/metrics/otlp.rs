//! `otlp` `MetricsSink` builtin (feature `otel`, decision 11): a hand-rolled
//! OTLP/HTTP JSON push of the [`super::PrometheusMetrics`] registry — no
//! OpenTelemetry SDK crate. [`OtlpMetrics`] delegates every
//! recording/encoding call to an inner `PrometheusMetrics` and, in the
//! background, periodically gathers that same registry, converts it with
//! [`families_to_otlp_json`], and POSTs it to a configured endpoint.

use crate::metrics::PrometheusMetrics;
use crate::{MetricsSink, ServerError};
use prometheus::proto::{LabelPair, Metric, MetricFamily, MetricType};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use varve_config::{BuildContext, ComponentFactory, ConfigSection, RegistryError};
use varve_engine::{EngineMetricsSnapshot, NodeStatus};

/// Pure converter: Prometheus families → one OTLP/HTTP JSON
/// `ExportMetricsServiceRequest`. `counter` families become a `sum` with
/// `isMonotonic: true` and `aggregationTemporality: 2` (CUMULATIVE);
/// `gauge` families become a `gauge`; `histogram` families become a
/// `histogram` (`bucketCounts`/`explicitBounds`/`count`/`sum`). Every label
/// pair becomes an OTLP `attributes` entry, and every data point carries
/// `time_unix_nano`.
///
/// **OTLP/JSON encodes 64-bit integer fields as JSON strings** (`asInt`,
/// `timeUnixNano`, histogram `count`/`bucketCounts`) — this is the OTLP/JSON
/// mapping of protobuf `int64`/`uint64`/`fixed64`, and a collector rejects a
/// bare number there, so every such field below is emitted via
/// `.to_string()`.
pub(crate) fn families_to_otlp_json(families: &[MetricFamily], time_unix_nano: u128) -> Value {
    let metrics: Vec<Value> = families
        .iter()
        .map(|family| family_to_otlp_metric(family, time_unix_nano))
        .collect();
    json!({
        "resourceMetrics": [{
            "resource": {
                "attributes": [
                    {"key": "service.name", "value": {"stringValue": "varve"}},
                ],
            },
            "scopeMetrics": [{
                "scope": {"name": "varve"},
                "metrics": metrics,
            }],
        }],
    })
}

fn family_to_otlp_metric(family: &MetricFamily, time_unix_nano: u128) -> Value {
    let name = family.name();
    match family.get_field_type() {
        MetricType::COUNTER => {
            let data_points: Vec<Value> = family
                .get_metric()
                .iter()
                .map(|metric| {
                    number_data_point(
                        metric.get_label(),
                        metric.get_counter().value(),
                        time_unix_nano,
                    )
                })
                .collect();
            json!({
                "name": name,
                "sum": {
                    "dataPoints": data_points,
                    "aggregationTemporality": 2,
                    "isMonotonic": true,
                },
            })
        }
        MetricType::GAUGE => {
            let data_points: Vec<Value> = family
                .get_metric()
                .iter()
                .map(|metric| {
                    number_data_point(
                        metric.get_label(),
                        metric.get_gauge().value(),
                        time_unix_nano,
                    )
                })
                .collect();
            json!({
                "name": name,
                "gauge": { "dataPoints": data_points },
            })
        }
        MetricType::HISTOGRAM => {
            let data_points: Vec<Value> = family
                .get_metric()
                .iter()
                .map(|metric| histogram_data_point(metric, time_unix_nano))
                .collect();
            json!({
                "name": name,
                "histogram": {
                    "dataPoints": data_points,
                    "aggregationTemporality": 2,
                },
            })
        }
        // Summaries and untyped families have no current producer in
        // `PrometheusMetrics` (spec §12 only registers counters, gauges,
        // and histograms) — emit the bare name rather than guessing a shape.
        MetricType::SUMMARY | MetricType::UNTYPED => json!({ "name": name }),
    }
}

fn attributes(labels: &[LabelPair]) -> Vec<Value> {
    labels
        .iter()
        .map(|label| json!({"key": label.name(), "value": {"stringValue": label.value()}}))
        .collect()
}

fn number_data_point(labels: &[LabelPair], value: f64, time_unix_nano: u128) -> Value {
    let mut point = json!({
        "attributes": attributes(labels),
        "timeUnixNano": time_unix_nano.to_string(),
    });
    if value.is_finite() && value.fract() == 0.0 {
        point["asInt"] = json!((value as i64).to_string());
    } else {
        point["asDouble"] = json!(value);
    }
    point
}

fn histogram_data_point(metric: &Metric, time_unix_nano: u128) -> Value {
    let histogram = metric.get_histogram();
    let buckets = histogram.get_bucket();
    // The `prometheus` crate's `Histogram` always strips a trailing `+Inf`
    // bound before storing `upper_bounds` (see
    // `histogram::check_and_adjust_buckets`: "The +Inf bucket is implicit.
    // Remove it here.") — so `buckets` here holds only the *finite* bounds,
    // each with its Prometheus-style cumulative count, and the `+Inf`
    // overflow bucket's count is never materialized as its own `Bucket`
    // entry. OTLP's `explicitBounds` is exactly those finite bounds, but
    // `bucketCounts` must have one *more* entry than `explicitBounds` — a
    // trailing DELTA count for that implicit overflow bucket — so it is
    // computed here as `sample_count - <last finite cumulative count>`.
    let explicit_bounds: Vec<f64> = buckets.iter().map(|bucket| bucket.upper_bound()).collect();
    let mut bucket_counts = Vec::with_capacity(buckets.len() + 1);
    let mut previous_cumulative = 0u64;
    for bucket in buckets {
        let cumulative = bucket.cumulative_count();
        bucket_counts.push(cumulative.saturating_sub(previous_cumulative).to_string());
        previous_cumulative = cumulative;
    }
    let sample_count = histogram.get_sample_count();
    bucket_counts.push(sample_count.saturating_sub(previous_cumulative).to_string());
    json!({
        "attributes": attributes(metric.get_label()),
        "timeUnixNano": time_unix_nano.to_string(),
        "count": sample_count.to_string(),
        "sum": histogram.get_sample_sum(),
        "bucketCounts": bucket_counts,
        "explicitBounds": explicit_bounds,
    })
}

/// `MetricsSink` builtin that wraps a [`PrometheusMetrics`] for
/// recording/encoding and, in the background, periodically pushes that same
/// registry to an OTLP/HTTP JSON endpoint (decision 11).
pub struct OtlpMetrics {
    inner: Arc<PrometheusMetrics>,
    // Held only to abort the background pusher task on drop.
    _pusher: PusherHandle,
}

struct PusherHandle(tokio::task::JoinHandle<()>);

impl Drop for PusherHandle {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl OtlpMetrics {
    /// Wraps `inner`, spawning a background task that gathers `inner`'s
    /// Prometheus registry and POSTs it to `endpoint` as OTLP/HTTP JSON
    /// every `push_interval`.
    ///
    /// **Requires an ambient tokio runtime** (this calls `tokio::spawn`) —
    /// satisfied inside the `varved` binary and any `#[tokio::test]`, but
    /// NOT by a plain `#[test]`.
    ///
    /// Push failures (connection errors, non-2xx responses are not
    /// inspected) are logged via `tracing::warn!` and never fatal — a
    /// down or slow collector must not affect request handling.
    pub fn new(inner: PrometheusMetrics, endpoint: String, push_interval: Duration) -> Self {
        let inner = Arc::new(inner);
        let pusher = spawn_pusher(Arc::clone(&inner), endpoint, push_interval);
        Self {
            inner,
            _pusher: pusher,
        }
    }
}

impl MetricsSink for OtlpMetrics {
    fn observe_request(
        &self,
        method: &'static str,
        route: &'static str,
        status: u16,
        elapsed: Duration,
    ) {
        self.inner.observe_request(method, route, status, elapsed);
    }

    fn set_progress(&self, status: &NodeStatus) {
        self.inner.set_progress(status);
    }

    fn set_engine(&self, snapshot: &EngineMetricsSnapshot) {
        self.inner.set_engine(snapshot);
    }

    fn encode(&self) -> Result<String, ServerError> {
        self.inner.encode()
    }
}

fn spawn_pusher(
    metrics: Arc<PrometheusMetrics>,
    endpoint: String,
    interval: Duration,
) -> PusherHandle {
    let handle = tokio::spawn(async move {
        // `.no_proxy()` skips system proxy auto-detection, and
        // `.tls_built_in_native_certs(false)` skips loading the OS native
        // root-cert store (on macOS, a Keychain lookup that has been
        // observed to add multi-second latency to a process's first TLS
        // client) — the workspace's own `rustls-tls` feature already
        // bundles the Mozilla webpki roots, which stay enabled.
        //
        // OPERATIONAL IMPLICATION: with this flag, OTLP-over-HTTPS
        // certificate validation checks ONLY the bundled webpki/Mozilla
        // root set — never the OS trust store. A collector whose
        // certificate chains through an enterprise/private CA that is not
        // in that bundle (common for on-prem/internal collectors) will
        // fail TLS verification; the only symptom is the generic
        // `tracing::warn!` below ("otlp metrics push failed"), not a
        // certificate-specific error. If your collector uses such a CA,
        // either terminate its TLS with a publicly-trusted certificate or
        // point `[metrics.otlp] endpoint` at a plaintext HTTP endpoint on
        // a trusted network.
        let client = match reqwest::Client::builder()
            .no_proxy()
            .tls_built_in_native_certs(false)
            .build()
        {
            Ok(client) => client,
            Err(error) => {
                tracing::warn!(
                    %error,
                    "otlp metrics pusher failed to build an HTTP client; metrics push disabled"
                );
                return;
            }
        };
        let mut ticker = tokio::time::interval(interval);
        loop {
            ticker.tick().await;
            let families = metrics.gather();
            let time_unix_nano = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|elapsed| elapsed.as_nanos())
                .unwrap_or(0);
            let body = families_to_otlp_json(&families, time_unix_nano);
            if let Err(error) = client.post(&endpoint).json(&body).send().await {
                tracing::warn!(%error, %endpoint, "otlp metrics push failed");
            }
        }
    });
    PusherHandle(handle)
}

pub(crate) struct OtlpMetricsFactory;

#[derive(Deserialize)]
struct OtlpConfig {
    endpoint: Option<String>,
    #[serde(default = "default_push_interval_ms")]
    push_interval_ms: u64,
}

fn default_push_interval_ms() -> u64 {
    10_000
}

impl ComponentFactory<dyn MetricsSink> for OtlpMetricsFactory {
    fn name(&self) -> &'static str {
        "otlp"
    }

    /// Reads `[metrics.otlp]` (`endpoint` required, `push_interval_ms`
    /// defaults to 10000) and builds an [`OtlpMetrics`]. Building this sink
    /// spawns the background pusher task (see [`OtlpMetrics::new`]), so
    /// `build` must be called from inside a tokio runtime.
    fn build(
        &self,
        cfg: &ConfigSection,
        _ctx: &BuildContext,
    ) -> Result<Arc<dyn MetricsSink>, RegistryError> {
        let result = (|| -> Result<OtlpMetrics, Box<dyn std::error::Error + Send + Sync>> {
            let config: OtlpConfig = cfg
                .child("otlp")
                .unwrap_or_else(ConfigSection::empty)
                .get()?;
            let endpoint = config
                .endpoint
                .ok_or_else(|| std::io::Error::other("[metrics.otlp] endpoint is required"))?;
            let inner = PrometheusMetrics::new()?;
            Ok(OtlpMetrics::new(
                inner,
                endpoint,
                Duration::from_millis(config.push_interval_ms),
            ))
        })();
        result
            .map(|metrics| Arc::new(metrics) as Arc<dyn MetricsSink>)
            .map_err(|source| RegistryError::Build {
                kind: "metrics",
                name: "otlp".into(),
                source,
            })
    }
}

#[cfg(test)]
mod tests {
    use super::families_to_otlp_json;

    #[test]
    fn converts_counters_gauges_and_histograms_to_otlp_shapes() {
        let registry = prometheus::Registry::new();
        let counter = prometheus::IntCounterVec::new(
            prometheus::Opts::new("varve_http_requests_total", "help"),
            &["route"],
        )
        .unwrap();
        registry.register(Box::new(counter.clone())).unwrap();
        counter.with_label_values(&["/v1/query"]).inc_by(3);
        let gauge = prometheus::IntGauge::new("varve_live_rows", "help").unwrap();
        registry.register(Box::new(gauge.clone())).unwrap();
        gauge.set(42);

        let body = families_to_otlp_json(&registry.gather(), 1_000);
        let metrics = &body["resourceMetrics"][0]["scopeMetrics"][0]["metrics"];
        let requests = metrics
            .as_array()
            .unwrap()
            .iter()
            .find(|m| m["name"] == "varve_http_requests_total")
            .unwrap();
        assert_eq!(requests["sum"]["isMonotonic"], true);
        assert_eq!(requests["sum"]["dataPoints"][0]["asInt"], "3"); // OTLP JSON int64 = string
        let attrs = &requests["sum"]["dataPoints"][0]["attributes"];
        assert_eq!(attrs[0]["key"], "route");
        let live = metrics
            .as_array()
            .unwrap()
            .iter()
            .find(|m| m["name"] == "varve_live_rows")
            .unwrap();
        assert_eq!(live["gauge"]["dataPoints"][0]["asInt"], "42");
    }

    /// Prometheus histograms expose cumulative, `+Inf`-terminated bucket
    /// counts; OTLP requires per-bucket (DELTA) `bucketCounts` with one more
    /// entry than `explicitBounds` (the last entry is the `+Inf` overflow
    /// bucket) and excludes `+Inf` from `explicitBounds` itself. This test
    /// pins that cumulative→delta subtraction with per-bucket counts (3, 2,
    /// 1, 4) that are distinct and non-trivial, so an off-by-one or
    /// wrong-order subtraction would flip an asserted value rather than
    /// passing vacuously.
    #[test]
    fn converts_histogram_cumulative_buckets_to_otlp_deltas() {
        let registry = prometheus::Registry::new();
        let histogram = prometheus::Histogram::with_opts(
            prometheus::HistogramOpts::new("varve_query_latency_seconds", "help")
                .buckets(vec![0.1, 0.5, 1.0]),
        )
        .unwrap();
        registry.register(Box::new(histogram.clone())).unwrap();
        // Bucket (-inf, 0.1]: 3 observations. Bucket (0.1, 0.5]: 2. Bucket
        // (0.5, 1.0]: 1. Overflow bucket (1.0, +inf): 4. Total count: 10.
        for value in [0.05, 0.05, 0.05, 0.3, 0.4, 0.7, 2.0, 2.0, 2.0, 2.0] {
            histogram.observe(value);
        }

        let body = families_to_otlp_json(&registry.gather(), 1_000);
        let metrics = &body["resourceMetrics"][0]["scopeMetrics"][0]["metrics"];
        let latency = metrics
            .as_array()
            .unwrap()
            .iter()
            .find(|m| m["name"] == "varve_query_latency_seconds")
            .unwrap();
        let point = &latency["histogram"]["dataPoints"][0];

        assert_eq!(
            point["explicitBounds"],
            serde_json::json!([0.1, 0.5, 1.0]),
            "explicitBounds excludes the +Inf bucket"
        );
        assert_eq!(point["count"], "10"); // OTLP JSON int64 = string
        let sum = point["sum"].as_f64().unwrap();
        assert!((sum - 9.55).abs() < 1e-9, "sum was {sum}");
        let bucket_counts: Vec<&str> = point["bucketCounts"]
            .as_array()
            .unwrap()
            .iter()
            .map(|value| value.as_str().unwrap()) // OTLP JSON int64 = string
            .collect();
        // bounds.len() + 1 (the +Inf overflow bucket), and DELTA (not
        // cumulative) counts: 3, 2, 1, 4 — summing to the total count of 10.
        assert_eq!(bucket_counts, vec!["3", "2", "1", "4"]);
    }
}
