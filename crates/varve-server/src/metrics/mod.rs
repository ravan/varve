#[cfg(feature = "otel")]
mod otlp;

#[cfg(feature = "otel")]
pub use otlp::OtlpMetrics;
#[cfg(feature = "otel")]
pub(crate) use otlp::OtlpMetricsFactory;

use crate::ServerError;
use prometheus::{
    Encoder, Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge,
    IntGaugeVec, Opts, Registry, TextEncoder,
};
use std::{sync::Arc, time::Duration};
use varve_config::{ComponentFactory, ConfigSection, RegistryError};
use varve_engine::{log_lag_records, EngineMetricsSnapshot, NodeStatus};

pub trait MetricsSink: Send + Sync {
    fn observe_request(
        &self,
        method: &'static str,
        route: &'static str,
        status: u16,
        elapsed: Duration,
    );
    fn set_progress(&self, status: &NodeStatus);
    /// Task 12 (spec §12): sets the engine-counter/cache/inventory gauges
    /// from an I/O-free `Db::metrics()` snapshot.
    fn set_engine(&self, snapshot: &EngineMetricsSnapshot);
    /// BI-2: records one completed `/v1/ingest` request — `records` = total
    /// nodes + edges committed, `bytes` = post-decompression body bytes
    /// streamed, `transactions` = chunks committed, `elapsed` = wall time.
    fn observe_ingest(&self, records: u64, bytes: u64, transactions: u64, elapsed: Duration);
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
    log_head_position: IntGauge,
    log_lag_records: IntGauge,
    txs_committed: IntGauge,
    events_committed: IntGauge,
    commit_failures: IntGauge,
    flush_blocks: IntGauge,
    flush_failures: IntGauge,
    compaction_runs: IntGauge,
    backpressure_rejections: IntGauge,
    live_rows: IntGauge,
    live_bytes: IntGauge,
    persisted_tries: IntGauge,
    compaction_debt_tries: IntGauge,
    block_pages_read: IntGauge,
    block_events_decoded: IntGauge,
    block_pages_cached: IntGauge,
    blocks_skipped: IntGauge,
    cache_hits: IntGaugeVec,
    cache_misses: IntGaugeVec,
    ingest_records: IntCounter,
    ingest_bytes: IntCounter,
    ingest_transactions: IntCounter,
    ingest_duration: Histogram,
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
        let log_head_position =
            IntGauge::new("varve_log_head_position", "Latest known log head").map_err(protocol)?;
        let log_lag_records = IntGauge::new(
            "varve_log_lag_records",
            "Records between applied position and log head",
        )
        .map_err(protocol)?;
        let txs_committed = IntGauge::new("varve_txs_committed_total", "Committed transactions")
            .map_err(protocol)?;
        let events_committed =
            IntGauge::new("varve_events_committed_total", "Committed events").map_err(protocol)?;
        let commit_failures = IntGauge::new(
            "varve_commit_failures_total",
            "Pre-durability append failures",
        )
        .map_err(protocol)?;
        let flush_blocks = IntGauge::new("varve_flush_blocks_total", "Successful block flushes")
            .map_err(protocol)?;
        let flush_failures = IntGauge::new("varve_flush_failures_total", "Failed block flushes")
            .map_err(protocol)?;
        let compaction_runs =
            IntGauge::new("varve_compaction_runs_total", "Compaction runs").map_err(protocol)?;
        let backpressure_rejections = IntGauge::new(
            "varve_backpressure_rejections_total",
            "try_execute_as rejections under backpressure",
        )
        .map_err(protocol)?;
        let live_rows = IntGauge::new("varve_live_rows", "Unflushed rows across all graphs")
            .map_err(protocol)?;
        let live_bytes = IntGauge::new(
            "varve_live_bytes",
            "Unflushed approximate bytes across all graphs",
        )
        .map_err(protocol)?;
        let persisted_tries =
            IntGauge::new("varve_persisted_tries", "Persisted tries across all scopes")
                .map_err(protocol)?;
        let compaction_debt_tries = IntGauge::new(
            "varve_compaction_debt_tries",
            "I/O-free compaction-debt proxy: sum of (tries - 1) per scope",
        )
        .map_err(protocol)?;
        let block_pages_read = IntGauge::new(
            "varve_block_pages_read_total",
            "Block data pages read by the read paths, after page pruning",
        )
        .map_err(protocol)?;
        let block_events_decoded = IntGauge::new(
            "varve_block_events_decoded_total",
            "Events materialized from those pages, after the key filter: read              against varve_block_pages_read_total, an anchored lookup should              decode its own degree, not a whole page",
        )
        .map_err(protocol)?;
        let block_pages_cached = IntGauge::new(
            "varve_block_pages_cached_total",
            "Of varve_block_pages_read_total, pages served from the decoded-page cache without a decode",
        )
        .map_err(protocol)?;
        let blocks_skipped = IntGauge::new(
            "varve_blocks_skipped_total",
            "Blocks a point lookup skipped because the block's key filter ruled the key out",
        )
        .map_err(protocol)?;
        let cache_hits = IntGaugeVec::new(
            Opts::new("varve_cache_hits_total", "Cache-tier hits"),
            &["tier"],
        )
        .map_err(protocol)?;
        let cache_misses = IntGaugeVec::new(
            Opts::new("varve_cache_misses_total", "Cache-tier misses"),
            &["tier"],
        )
        .map_err(protocol)?;
        let ingest_records = IntCounter::new(
            "varve_ingest_records_total",
            "Node + edge records committed through /v1/ingest",
        )
        .map_err(protocol)?;
        let ingest_bytes = IntCounter::new(
            "varve_ingest_bytes_total",
            "Post-decompression request bytes streamed through /v1/ingest",
        )
        .map_err(protocol)?;
        let ingest_transactions = IntCounter::new(
            "varve_ingest_transactions_total",
            "Chunk transactions committed through /v1/ingest",
        )
        .map_err(protocol)?;
        let ingest_duration = Histogram::with_opts(HistogramOpts::new(
            "varve_ingest_duration_seconds",
            "End-to-end /v1/ingest request duration",
        ))
        .map_err(protocol)?;
        for collector in [
            Box::new(requests.clone()) as Box<dyn prometheus::core::Collector>,
            Box::new(duration.clone()),
            Box::new(applied_tx.clone()),
            Box::new(applied_log.clone()),
            Box::new(manifest_watermark.clone()),
            Box::new(follower_healthy.clone()),
            Box::new(log_head_position.clone()),
            Box::new(log_lag_records.clone()),
            Box::new(txs_committed.clone()),
            Box::new(events_committed.clone()),
            Box::new(commit_failures.clone()),
            Box::new(flush_blocks.clone()),
            Box::new(flush_failures.clone()),
            Box::new(compaction_runs.clone()),
            Box::new(backpressure_rejections.clone()),
            Box::new(live_rows.clone()),
            Box::new(live_bytes.clone()),
            Box::new(persisted_tries.clone()),
            Box::new(compaction_debt_tries.clone()),
            Box::new(block_pages_read.clone()),
            Box::new(block_events_decoded.clone()),
            Box::new(block_pages_cached.clone()),
            Box::new(blocks_skipped.clone()),
            Box::new(cache_hits.clone()),
            Box::new(cache_misses.clone()),
            Box::new(ingest_records.clone()),
            Box::new(ingest_bytes.clone()),
            Box::new(ingest_transactions.clone()),
            Box::new(ingest_duration.clone()),
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
            log_head_position,
            log_lag_records,
            txs_committed,
            events_committed,
            commit_failures,
            flush_blocks,
            flush_failures,
            compaction_runs,
            backpressure_rejections,
            live_rows,
            live_bytes,
            persisted_tries,
            compaction_debt_tries,
            block_pages_read,
            block_events_decoded,
            block_pages_cached,
            blocks_skipped,
            cache_hits,
            cache_misses,
            ingest_records,
            ingest_bytes,
            ingest_transactions,
            ingest_duration,
        })
    }

    /// Gathers the current state of every registered collector as
    /// Prometheus proto `MetricFamily` values (spec §12 / decision 11's
    /// `otlp` sink converts these into OTLP/HTTP JSON before pushing).
    #[cfg_attr(not(feature = "otel"), allow(dead_code))]
    pub(crate) fn gather(&self) -> Vec<prometheus::proto::MetricFamily> {
        self.registry.gather()
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
        self.log_head_position
            .set(saturating_i64(status.log_head.as_u64()));
        self.log_lag_records.set(saturating_i64(log_lag_records(
            status.applied.log_position,
            status.log_head,
        )));
    }

    fn set_engine(&self, snapshot: &EngineMetricsSnapshot) {
        self.txs_committed
            .set(saturating_i64(snapshot.txs_committed));
        self.events_committed
            .set(saturating_i64(snapshot.events_committed));
        self.commit_failures
            .set(saturating_i64(snapshot.commit_failures));
        self.flush_blocks.set(saturating_i64(snapshot.flush_blocks));
        self.flush_failures
            .set(saturating_i64(snapshot.flush_failures));
        self.compaction_runs
            .set(saturating_i64(snapshot.compaction_runs));
        self.backpressure_rejections
            .set(saturating_i64(snapshot.backpressure_rejections));
        self.live_rows.set(saturating_i64(snapshot.live_rows));
        self.live_bytes.set(saturating_i64(snapshot.live_bytes));
        self.persisted_tries
            .set(saturating_i64(snapshot.persisted_tries));
        self.compaction_debt_tries
            .set(saturating_i64(snapshot.compaction_debt_tries));
        self.block_pages_read
            .set(saturating_i64(snapshot.block_pages_read));
        self.block_events_decoded
            .set(saturating_i64(snapshot.block_events_decoded));
        self.block_pages_cached
            .set(saturating_i64(snapshot.block_pages_cached));
        self.blocks_skipped
            .set(saturating_i64(snapshot.blocks_skipped));
        for tier in &snapshot.cache_tiers {
            self.cache_hits
                .with_label_values(&[tier.tier.as_str()])
                .set(saturating_i64(tier.hits));
            self.cache_misses
                .with_label_values(&[tier.tier.as_str()])
                .set(saturating_i64(tier.misses));
        }
    }

    fn observe_ingest(&self, records: u64, bytes: u64, transactions: u64, elapsed: Duration) {
        self.ingest_records.inc_by(records);
        self.ingest_bytes.inc_by(bytes);
        self.ingest_transactions.inc_by(transactions);
        self.ingest_duration.observe(elapsed.as_secs_f64());
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
        _ctx: &(),
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
