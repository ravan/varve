use crate::ServerError;
use prometheus::{
    Encoder, HistogramOpts, HistogramVec, IntCounterVec, IntGauge, Opts, Registry, TextEncoder,
};
use std::{sync::Arc, time::Duration};
use varve_config::{BuildContext, ComponentFactory, ConfigSection, RegistryError};
use varve_engine::NodeStatus;

pub trait MetricsSink: Send + Sync {
    fn observe_request(
        &self,
        method: &'static str,
        route: &'static str,
        status: u16,
        elapsed: Duration,
    );
    fn set_progress(&self, status: &NodeStatus);
    fn encode(&self) -> Result<String, ServerError>;
}

pub struct PrometheusMetrics {
    registry: Registry,
    requests: IntCounterVec,
    duration: HistogramVec,
    applied_tx: IntGauge,
    applied_log: IntGauge,
    manifest_watermark: IntGauge,
    follower_healthy: IntGauge,
}

impl PrometheusMetrics {
    pub fn new() -> Result<Self, ServerError> {
        let registry = Registry::new();
        let requests = IntCounterVec::new(
            Opts::new("varve_http_requests_total", "HTTP requests"),
            &["method", "route", "status"],
        )
        .map_err(protocol)?;
        let duration = HistogramVec::new(
            HistogramOpts::new(
                "varve_http_request_duration_seconds",
                "HTTP request duration",
            ),
            &["method", "route"],
        )
        .map_err(protocol)?;
        let applied_tx =
            IntGauge::new("varve_applied_tx_id", "Latest applied transaction").map_err(protocol)?;
        let applied_log =
            IntGauge::new("varve_applied_log_position", "Latest applied log position")
                .map_err(protocol)?;
        let manifest_watermark =
            IntGauge::new("varve_manifest_watermark", "Current manifest watermark")
                .map_err(protocol)?;
        let follower_healthy =
            IntGauge::new("varve_follower_healthy", "Follower health").map_err(protocol)?;
        for collector in [
            Box::new(requests.clone()) as Box<dyn prometheus::core::Collector>,
            Box::new(duration.clone()),
            Box::new(applied_tx.clone()),
            Box::new(applied_log.clone()),
            Box::new(manifest_watermark.clone()),
            Box::new(follower_healthy.clone()),
        ] {
            registry.register(collector).map_err(protocol)?;
        }
        Ok(Self {
            registry,
            requests,
            duration,
            applied_tx,
            applied_log,
            manifest_watermark,
            follower_healthy,
        })
    }
}

impl MetricsSink for PrometheusMetrics {
    fn observe_request(
        &self,
        method: &'static str,
        route: &'static str,
        status: u16,
        elapsed: Duration,
    ) {
        let status = status.to_string();
        self.requests
            .with_label_values(&[method, route, &status])
            .inc();
        self.duration
            .with_label_values(&[method, route])
            .observe(elapsed.as_secs_f64());
    }

    fn set_progress(&self, status: &NodeStatus) {
        self.applied_tx.set(saturating_i64(status.applied.tx_id));
        self.applied_log
            .set(saturating_i64(status.applied.log_position.as_u64()));
        self.manifest_watermark
            .set(saturating_i64(status.manifest_watermark.as_u64()));
        self.follower_healthy
            .set(i64::from(status.follower_error.is_none()));
    }

    fn encode(&self) -> Result<String, ServerError> {
        let mut bytes = Vec::new();
        TextEncoder::new()
            .encode(&self.registry.gather(), &mut bytes)
            .map_err(protocol)?;
        String::from_utf8(bytes).map_err(|error| ServerError::Protocol(error.to_string()))
    }
}

fn saturating_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn protocol(error: prometheus::Error) -> ServerError {
    ServerError::Protocol(error.to_string())
}

pub(crate) struct PrometheusMetricsFactory;

impl ComponentFactory<dyn MetricsSink> for PrometheusMetricsFactory {
    fn name(&self) -> &'static str {
        "prometheus"
    }

    fn build(
        &self,
        _cfg: &ConfigSection,
        _ctx: &BuildContext,
    ) -> Result<Arc<dyn MetricsSink>, RegistryError> {
        PrometheusMetrics::new()
            .map(|metrics| Arc::new(metrics) as Arc<dyn MetricsSink>)
            .map_err(|source| RegistryError::Build {
                kind: "metrics",
                name: "prometheus".into(),
                source: Box::new(source),
            })
    }
}
