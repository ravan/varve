# Configuration reference

<!-- GENERATED FILE. Do not hand-edit. Produced by `cargo run -p varve-testkit --bin config_reference` (`just docs-gen`); `crates/varve-testkit/tests/config_reference_doc.rs` pins this file to that output. -->

Every `[section]` key a `varve.toml` file accepts (spec §4/§11), one table per section, generated from the same `#[serde(default = ...)]` code paths the engine, log, storage, and server crates use to parse it. See [Deployment profiles & sizing](profiles.md) for worked topologies and [Metrics & observability](metrics.md) for the `[coordinator]` and `[metrics.otlp]` runbooks. Any key here can also be set via a `VARVE__SECTION__KEY` environment variable at process startup (e.g. `VARVE__LOG__LOCAL__DIR=/data/log` sets `[log.local] dir`); nested sections use an extra `__`, e.g. `VARVE__STORAGE__S3__ENDPOINT`.

## `[node]`

Node role selection and query/follower tuning (spec §4, §12).

| Key | Type | Default | Description |
|---|---|---|---|
| `roles` | array of `writer`/`query`/`compactor` | `["writer", "query", "compactor"]` | Roles this node performs; the `compactor` role requires `writer`. |
| `tail_poll_interval_ms` | integer (ms) | `50` | Query-node follower poll interval for new log records. |
| `tail_batch_records` | integer | `1024` | Max records the follower applies per poll batch. |
| `basis_timeout_ms` | integer (ms) | `5000` | How long a query waits for its requested basis before timing out. |
| `submission_queue_len` | integer | `256` | Bounded capacity of the writer's submission queue; `try_execute_as` returns backpressure immediately once it is full. |

## `[log]`

Write-ahead log backend selection and group-commit tuning (spec §6).

| Key | Type | Default | Description |
|---|---|---|---|
| `backend` | string: `memory`/`local`/`object-store` | `"memory"` | Log backend; `local` requires `[log.local]`. `object-store` shares the `[storage]` backend's object store. |
| `group_commit_window_ms` | integer (ms) | `15` | A batch flushes once this window elapses OR `group_commit_max_bytes` is reached, whichever comes first. |
| `group_commit_max_bytes` | byte size | `"8MiB"` | The other half of the group-commit trigger. |

## `[log.local]`

Tuning for `[log] backend = "local"` (a single-process durable log file).

| Key | Type | Default | Description |
|---|---|---|---|
| `dir` | string | (required) | Directory containing the local log's segment files. |
| `segment_max_bytes` | integer (bytes) | `67108864` | Segment rotation size in bytes (default 64 MiB). |

## `[storage]`

Block-store backend selection and flush tuning (spec §9).

| Key | Type | Default | Description |
|---|---|---|---|
| `backend` | string: `memory`/`local`/`s3` | `"memory"` | Object-store backend for flushed blocks; `local` requires `[storage.local]`, `s3` requires `[storage.s3]`. |
| `max_block_rows` | integer | `100000` | Row count that triggers an early block flush. |
| `flush_interval_ms` | integer (ms) | `300000` | Timer-based flush interval; `0` disables the timer. |
| `max_live_bytes` | byte size | `"512MiB"` | Live-index memory watermark; forces an early block flush independent of `max_block_rows`. |

## `[storage.local]`

Tuning for `[storage] backend = "local"`.

| Key | Type | Default | Description |
|---|---|---|---|
| `dir` | string | (required) | Directory for flushed blocks. |

## `[storage.s3]`

Tuning for `[storage] backend = "s3"` (any S3-API endpoint: AWS, Garage, Ceph RGW, SeaweedFS, MinIO).

| Key | Type | Default | Description |
|---|---|---|---|
| `bucket` | string | (required) | Target bucket name. |
| `endpoint` | string | (none) | e.g. `http://127.0.0.1:3900` (Garage); omitted resolves the AWS endpoint. |
| `region` | string | (none) | Must match the backend's configured region (e.g. Garage's `s3_region`); omitted uses the environment or `us-east-1`. |
| `access_key_id` | string | (none) | Overrides the environment/AWS provider chain. |
| `secret_access_key` | string | (none) | Overrides the environment/AWS provider chain. |
| `path_style` | boolean | `true` | Path-style addressing (`endpoint/bucket/key`); Garage and MinIO need it. `false` selects virtual-hosted style. |
| `allow_http` | boolean | (none) | Permit plain-HTTP endpoints; defaults to whether `endpoint` starts with `http://`. |

## `[cache]`

Named cache tiers composed outermost-first over the raw object store (spec §4/§9).

| Key | Type | Default | Description |
|---|---|---|---|
| `tiers` | array of strings | `["memory"]` | Tier names checked in order before falling through to the backend; an empty list runs uncached. |

## `[cache.memory]`

Tuning for the `memory` cache tier.

| Key | Type | Default | Description |
|---|---|---|---|
| `max_bytes` | byte size | `"512MiB"` | In-memory cache budget. |

## `[cache.disk]`

Tuning for the `disk` cache tier (a self-describing on-disk LRU that survives restarts).

| Key | Type | Default | Description |
|---|---|---|---|
| `dir` | string | (required) | Directory dedicated to this cache tier; must not be shared with any other store. |
| `max_bytes` | byte size | `"50GiB"` | On-disk cache budget. |

## `[query]`

Query planning limits (spec §10).

| Key | Type | Default | Description |
|---|---|---|---|
| `max_path_depth` | integer | `10` | Maximum traversal depth for variable-length path patterns. |
| `path_output_batch_rows` | integer | `8192` | Rows per output batch for path results. |
| `path_row_budget` | integer | `100000` | Row budget for path expansion before it aborts. |
| `path_frontier_budget` | integer | `100000` | Frontier-size budget for path expansion before it aborts. |
| `traversal_node_budget` | integer | `100000` | Node budget for general traversal before it aborts. |
| `traversal_adjacency_budget` | integer | `250000` | Adjacency-edge budget for general traversal before it aborts. |

## `[gc]`

Garbage collection of superseded objects (spec §9).

| Key | Type | Default | Description |
|---|---|---|---|
| `enabled` | boolean | `false` | Enables GC; disabled by default. |
| `blocks_to_keep` | integer | `10` | Flushed blocks retained behind the GC frontier, for lagging followers/basis reads. |
| `garbage_lifetime_hours` | integer (hours) | `24` | Minimum age before a superseded object becomes GC-eligible. |

## `[coordinator]`

Writer coordination backend and heartbeat/lease tuning (spec §12).

| Key | Type | Default | Description |
|---|---|---|---|
| `backend` | string: `designated-writer`/`cas-failover` | `"designated-writer"` | `cas-failover` requires `[log] backend = "object-store"` and a storage backend whose conditional-put probe reports `Supported`. |
| `heartbeat_interval_ms` | integer (ms) | `5000` | Heartbeat publish interval; `0` disables the heartbeat task entirely. |
| `takeover_after_ms` | integer (ms) | `15000` | Staleness deadline before a standby may take over; must be at least 2x `heartbeat_interval_ms` when heartbeats are enabled. |

## `[server]`

Protocol frontend selection (the `varved` binary).

| Key | Type | Default | Description |
|---|---|---|---|
| `backend` | string: `http` | `"http"` | Protocol frontend; `http` requires `[server.http]`. |

## `[server.http]`

Tuning for `[server] backend = "http"`.

| Key | Type | Default | Description |
|---|---|---|---|
| `listen` | string | `"0.0.0.0:8080"` | Socket address the HTTP frontend binds. |
| `advertised_address` | string | (none) | Absolute `http`/`https` URL clients use to reach this node; required when this node has the `writer` role. |
| `max_body_bytes` | byte size | `"8MiB"` | Max accepted request body size. |
| `tls_cert` | path | (none) | PEM certificate path; must be set together with `tls_key`. |
| `tls_key` | path | (none) | PEM private-key path; must be set together with `tls_cert`. |

## `[auth]`

Authentication backend selection.

| Key | Type | Default | Description |
|---|---|---|---|
| `backend` | string: `static` | `"static"` | Authenticator backend. |

## `[auth.static]`

Tuning for `[auth] backend = "static"` (a bearer-token allowlist).

| Key | Type | Default | Description |
|---|---|---|---|
| `tokens` | array of tables (`subject`, `token`) | (required) | Bearer tokens accepted, each with a distinct subject; at least one is required and tokens must be unique. |

## `[metrics]`

Metrics sink backend selection.

| Key | Type | Default | Description |
|---|---|---|---|
| `backend` | string: `prometheus`/`otlp` | `"prometheus"` | `otlp` requires `[metrics.otlp]` and the `otel` build feature. |

## `[metrics.otlp]`

Tuning for `[metrics] backend = "otlp"` (wraps the Prometheus registry and pushes OTLP/HTTP JSON on an interval).

| Key | Type | Default | Description |
|---|---|---|---|
| `endpoint` | string | (required) | OTLP/HTTP metrics endpoint, e.g. `http://otel-collector:4318/v1/metrics`. |
| `push_interval_ms` | integer (ms) | `10000` | How often the registry is gathered and pushed. |
