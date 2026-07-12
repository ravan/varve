# Varve operations reference: metrics, tracing, and failover

Grafana-ready reference for everything Varve exposes at `GET /metrics`
(Prometheus text format, `PrometheusMetrics` in
`crates/varve-server/src/metrics/mod.rs`), the stable `tracing` span names
emitted across the write/read/follower paths, and the coordination objects +
failover runbook for the `[coordinator]` subsystem (spec §12). Every name
below is copied verbatim from source — nothing here is invented.

## 1. Metrics

All metrics are served on the writer/query-node HTTP surface at `/metrics`.
Metric names ending in `_total` that are implemented as a Prometheus
`IntGauge`/`IntGaugeVec` rather than a `Counter` are noted explicitly below —
**they are monotone by construction** (mirrors of engine `AtomicU64`
counters that only ever increase within a process lifetime), so
`rate()`/`increase()` behave correctly, but a Prometheus counter-type
`reset()` detection (e.g. after a process restart, the gauge drops to 0 and
`rate()` briefly reads negative-then-corrects) is a `Gauge`, not `Counter`,
concern operators should be aware of. Only `varve_http_requests_total` is a
true `IntCounterVec`.

| Metric | Type | Labels | Meaning | Suggested Grafana expression |
|---|---|---|---|---|
| `varve_http_requests_total` | counter | `method`, `route`, `status` | HTTP requests served | `rate(varve_http_requests_total[5m])` by `route`/`status` |
| `varve_http_request_duration_seconds` | histogram | `method`, `route` | HTTP request latency | `histogram_quantile(0.99, rate(varve_http_request_duration_seconds_bucket[5m]))` — query-latency p99 |
| `varve_applied_tx_id` | gauge | — | Latest applied transaction id on this node | `varve_applied_tx_id` |
| `varve_applied_log_position` | gauge | — | Latest applied log position on this node | `varve_applied_log_position` |
| `varve_manifest_watermark` | gauge | — | Current manifest watermark | `varve_manifest_watermark` |
| `varve_follower_healthy` | gauge (0/1) | — | 1 while the follower loop has no error; 0 once `follower_error` is set | `min_over_time(varve_follower_healthy[5m]) == 0` — alert on follower stalls |
| `varve_log_head_position` | gauge | — | Latest known log head (writer publishes its own durable position, lag 0 by construction; a follower/query node publishes `max(last-read+1, watermark seen in gap check)`, or the jumped cursor on a fence jump) | `varve_log_head_position` |
| `varve_log_lag_records` | gauge | — | **Follower lag**: records between this node's applied position and the (locally known) log head | `varve_log_lag_records` per node — alert if sustained > 0 on a query node |
| `varve_txs_committed_total` | gauge, monotone-by-construction (mirrors an engine `AtomicU64`, `_total`-named but not a `Counter` type) | — | Committed transactions | **ingest rate**: `rate(varve_txs_committed_total[1m])` |
| `varve_events_committed_total` | gauge, monotone-by-construction | — | Committed events (finer-grained than tx count) | `rate(varve_events_committed_total[1m])` |
| `varve_commit_failures_total` | gauge, monotone-by-construction | — | Pre-durability append failures | `increase(varve_commit_failures_total[15m]) > 0` — alert |
| `varve_flush_blocks_total` | gauge, monotone-by-construction | — | Successful block flushes | `rate(varve_flush_blocks_total[5m])` |
| `varve_flush_failures_total` | gauge, monotone-by-construction | — | Failed block flushes | `increase(varve_flush_failures_total[15m]) > 0` — alert |
| `varve_compaction_runs_total` | gauge, monotone-by-construction | — | Compaction runs | `rate(varve_compaction_runs_total[1h])` |
| `varve_backpressure_rejections_total` | gauge, monotone-by-construction | — | `try_execute_as` rejections under backpressure (429s) | `rate(varve_backpressure_rejections_total[1m])` — sustained > 0 means writers are saturated |
| `varve_live_rows` | gauge | — | Unflushed rows across all graphs | `varve_live_rows` |
| `varve_live_bytes` | gauge | — | Unflushed approximate bytes across all graphs (drives the memory-watermark early flush) | `varve_live_bytes` — compare against the configured watermark |
| `varve_persisted_tries` | gauge | — | Persisted tries across all scopes | `varve_persisted_tries` |
| `varve_compaction_debt_tries` | gauge | — | I/O-free compaction-debt proxy: Σ over scopes of `max(0, tries(scope) − 1)` (each scope with more than one persisted trie has debt equal to all but its newest) | `varve_compaction_debt_tries` — **caveat: this is a count of extra tries, computed without any I/O; it approximates compaction debt but does not measure bytes-to-rewrite or read amplification directly** |
| `varve_cache_hits_total` | gauge vec, monotone-by-construction | `tier` | Cache-tier hits | see cache hit ratio below |
| `varve_cache_misses_total` | gauge vec, monotone-by-construction | `tier` | Cache-tier misses | see cache hit ratio below |

**Cache hit ratio per tier:**

```promql
varve_cache_hits_total / (varve_cache_hits_total + varve_cache_misses_total)
```

(group/sum `by (tier)` if multiple tiers are configured via `[cache] tiers`,
composed outermost-first, default `["memory"]`.)

## 2. Tracing spans and OpenTelemetry export

### Stable span names (Task 13)

Emitted via the `tracing` crate across the write, flush/compaction, follower,
and query paths. All names are stable and safe to build dashboards/alerts on
(e.g. via a span-duration exporter or the OTLP traces pipeline, if wired up
downstream of `tracing-opentelemetry`).

| Span | Where | Fields | Notes |
|---|---|---|---|
| `varve.submit` | `Db` write entrypoint (`db.rs`) | `user` | Wraps a single submitted command from HTTP/CLI entry to enqueue |
| `varve.resolve` | `resolve_program` (`writer.rs`) | `tx_id` (recorded once assigned) | GQL program resolution against the live index |
| `varve.commit` | `flush`/`flush_impl` (`writer.rs`) | `batch` (known up front), `first_position` (recorded once `log.append` succeeds) | Log append of a staged batch |
| `varve.apply` | writer loop (`writer.rs`) | `batch` | Applying an appended batch to the live index; a plain sync-entered span, never held across an `.await` |
| `varve.flush_block` | `flush.rs` | `block_id` | A single block flush to storage |
| `varve.compact` | `compact` (`writer.rs`) | — | Wraps an entire compaction run |
| `varve.follower.apply` | `apply_next_range` (`follower.rs`) | `from` (known at entry), `applied` (recorded before returning) | Follower catch-up application of a log range |
| `varve.query.parse` | query entrypoint (`db.rs`) | — | Synchronous GQL parse (entered span, no async work) |
| `varve.query.plan` | query entrypoint (`db.rs`) | — | Query planning, including each leg of a UNION |
| `varve.query.execute` | query entrypoint (`db.rs`) | `graph` | Query execution against a named graph, including each leg of a UNION |

### `[metrics.otlp]` configuration

The `otlp` `MetricsSink` (feature `otel`) wraps `PrometheusMetrics` and adds a
background task that gathers the same Prometheus registry on an interval and
POSTs it as OTLP/HTTP JSON metrics (hand-rolled converter — no OpenTelemetry
SDK dependency). Counters convert to OTLP `sum` (`isMonotonic: true`,
cumulative temporality); gauges to OTLP `gauge`; histograms to OTLP
`histogram` with per-interval delta bucket counts derived from Prometheus's
cumulative bucket counts.

```toml
[metrics]
backend = "otlp"          # or "prometheus" for scrape-only (default)

[metrics.otlp]
endpoint = "http://otel-collector:4318/v1/metrics"   # required, no default
push_interval_ms = 10000                              # default; how often the registry is gathered and pushed
```

- `endpoint` is required — `PrometheusMetricsFactory`/`OtlpMetricsFactory`
  build error is `"[metrics.otlp] endpoint is required"` if omitted.
- `push_interval_ms` defaults to `10000` (10s).
- Push failures are logged via `tracing::warn!("otlp metrics push failed", ...)`
  and are never fatal — a collector outage degrades observability, not the
  write/read path.

**OTLP-over-HTTPS certificate caveat:** the OTLP pusher's HTTP client is built
with `tls_built_in_native_certs(false)` — TLS certificate validation checks
**only** the bundled webpki/Mozilla root set, **never the OS trust store**.
If your collector's certificate chains through an enterprise/private CA that
is not in that bundle, the push will fail TLS verification silently — the
only symptom is the generic `tracing::warn!("otlp metrics push failed")` log
line, not a certificate-specific error. If your collector uses such a CA,
either terminate its TLS with a publicly-trusted certificate, or point
`[metrics.otlp] endpoint` at a plaintext HTTP endpoint on a trusted network.

### Example OTLP collector snippet (receiver + JSON over HTTP)

```yaml
receivers:
  otlp:
    protocols:
      http:
        endpoint: 0.0.0.0:4318   # matches the endpoint Varve POSTs OTLP/HTTP JSON to

exporters:
  prometheus:
    endpoint: 0.0.0.0:8889

service:
  pipelines:
    metrics:
      receivers: [otlp]
      exporters: [prometheus]
```

Varve's pusher sends `Content-Type: application/json` bodies matching the
OTLP metrics JSON schema (`resourceMetrics` → `scopeMetrics` → `metrics`),
not protobuf — ensure the collector's `http` receiver accepts JSON (the
standard `otlp` receiver does).

## 3. Coordination objects and failover runbook

### Coordination objects (shared object store, spec §12)

| Key | Written by | Contents | Purpose |
|---|---|---|---|
| `v1/writer.json` | Both coordinators' `advertise()` | `{ holder, address, epoch, heartbeat_us }`-shaped advertisement | Best-effort discovery of the current writer's address + heartbeat freshness; also used by `designated-writer`'s `acquire()` guard |
| `v1/lease.json` | `cas-failover`'s `CasFailover` only | `LeaseDoc { holder, address, epoch, heartbeat_us }` | The actual CAS-fenced lease. Staleness is decided purely by **ETag double-observation** (clock-skew-free), not by comparing `heartbeat_us` timestamps: a standby waits a full `takeover_after` window, re-reads the ETag, and only seizes the lease if the ETag is unchanged |
| `v1/epochs/<epoch-4hex>.json` (e.g. `v1/epochs/0000.json`) | `write_fence` on lease seizure | `FenceDoc { epoch, fence_offset, fenced_by, fenced_at_us }` | Marks a dead epoch: appends at or behind `fence_offset` within that epoch are ignored by recovery, the follower cursor, and `admin verify`. Written at the log's **actual** head position at seizure time (not the dead holder's last-known/cached position) |

### `[coordinator]` configuration

```toml
[coordinator]
backend = "designated-writer"   # default; or "cas-failover" (feature-gated, requires [log] backend = "object-store")
heartbeat_interval_ms = 5000    # default; Duration::ZERO disables the heartbeat task entirely
takeover_after_ms = 15000       # default; must be >= 2 * heartbeat_interval_ms when heartbeat_interval_ms > 0
```

- `backend = "cas-failover"` with a `[log]` backend other than
  `"object-store"` fails fast with `CasRequiresSharedLog`
  ("cas-failover requires the shared \"object-store\" log; [log] backend is
  \"{backend}\"").
- `cas-failover` also requires the storage backend's conditional-put
  capability probe (slice 5) to report `Supported`; anything else
  (`Unsupported` or `Inconsistent`) is a hard error at startup — see
  `CasUnsupported` below. Verified live against both MinIO (probe
  `Supported`, gate passes) and Garage (probe `Inconsistent` — Garage's S3
  layer accepts an `If-None-Match` create over an existing object instead of
  rejecting it — gate correctly refuses with an actionable error).

### Runbook: what each failure state means and what to do

| State | Meaning | Operator action |
|---|---|---|
| `EngineError::WriterActive { address, age_ms, takeover_after_ms }` | `designated-writer`'s best-effort startup guard: a fresh advertisement from a *different* node id already exists in `v1/writer.json` (age below `takeover_after_ms`). This is **not** a fencing mechanism — it only refuses a second writer from starting while another looks alive. | Stop the other writer if it's a leftover process, or simply wait — the guard's own message says "wait for its heartbeat to go stale." Once `age_ms` exceeds `takeover_after_ms`, a retried start proceeds. If you need real (non-best-effort) fencing, switch `[coordinator] backend` to `cas-failover` on a backend whose conditional-put probe is `Supported`. |
| `EngineError::CasUnsupported { reason }` | Startup with `backend = "cas-failover"` against a storage backend whose slice-5 capability probe returned `Unsupported` or `Inconsistent` (i.e. its S3-compatible API doesn't honor `If-None-Match`/`If-Match` correctly — e.g. Garage's current server-side create-if-absent behavior). The engine refuses to start rather than run without real fencing. | Switch `[coordinator] backend` to `"designated-writer"` on this backend (accepting best-effort guarding only), or move the shared log/object store to a backend that passes the probe (verified: MinIO passes; Garage does not, as of this slice). |
| `EngineError::WriterFenced(reason)` | **Terminal.** The writer observed a fatal event after already appending durably — either a lease loss (a `cas-failover` heartbeat returned `Lost`) or a post-durability apply failure. The writer loop publishes the failure so `/healthz` degrades, drains every subsequent queued command with `WriterFenced` (mapped to HTTP 503 `writer_fenced`), and stops. There is no recovery path that resumes the same process — the block-flush-after-fatal path is deliberately never taken. | Restart the writer process. On restart it re-runs `acquire()`/recovery from scratch: for `cas-failover`, it will only regain the lease if it can win a fresh seizure (i.e. after the current legitimate holder is itself gone); check `v1/lease.json`'s current holder/epoch before restarting to confirm you're not restarting into an immediate second fencing. |

### Failover demo and chaos numbers (this slice's verification run)

- `cargo run --release --example failover -p varve`: writer A commits 3 txs,
  crashes; writer B takes over in **304 ms** (well under the 10 s exit
  criterion), seizes epoch 1 with fence `0@3`; the zombie's late append at
  `(0,3)` is ignored by writer B, the query node, and `admin verify`; final
  row count is consistent (4) everywhere.
- `VARVE_CHAOS_SECS=60 cargo test -p varve-testkit --release --test chaos`:
  **64 kills survived, 2463 acked transactions all present**, 0 corruption,
  0 acked-tx loss, over a 60 s run (the nightly gate runs this for 30 min per
  the roadmap exit criterion; the 60 s run here is the CI-speed smoke check).
