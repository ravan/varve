# Slice 9: Server, CLI, Query-Node Role Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship a role-aware Varve node, production HTTP server, and `varve` CLI so one designated writer and any number of query nodes can serve authenticated GQL with bounded read-your-writes, JSON/Arrow responses, administration, and a runnable Garage-backed Compose deployment.

**Architecture:** `varve-engine` becomes a cloneable, role-aware deep module. Writer nodes keep the existing serialized writer loop; query-only nodes recover the latest manifest, then consume bounded ranges of resolved log effects into their own live indexes without parsing or executing GQL. A shared progress watch drives basis waits, status, and health. `varve-server` owns the protocol/authentication/metrics interfaces and the axum HTTP adapter; `varve-cli` uses one client interface with embedded and HTTP adapters.

**Tech Stack:** Rust stable 1.93, tokio 1, DataFusion 54.0.0, Arrow/arrow-json 58.3.0, object_store 0.13.2, axum 0.8, axum-server 0.8 with rustls, serde/serde_json, Prometheus 0.14, clap 4.6, reqwest 0.12, rustyline 18.

**Source precedence used:** `docs/plans/STATUS.md` (authoritative current facts and prior decisions), then the Slice 9 entry and Global Constraints in `docs/plans/varve-v1-roadmap.md`, then design spec §3/§11, then the current `crates/` implementation. Slice 9 cites no XTDB porting files, so there are no `refs/xtdb/` inputs for this slice.

## Global Constraints

- All roadmap Global Constraints apply to every task: TDD; registry/composition; plain-S3 sovereignty; append-only bitemporal events with derived effective ranges; deterministic replay; workspace lint policy; `Timestamp(µs, UTC)`; and `xxh3_128(graph, table, _id)` IIDs.
- This repository is in development. Replace superseded interfaces and configuration shapes directly; do not add legacy aliases, compatibility fields, fallback parsing, dual routes, or deprecation shims.
- Resolved transaction effects are the only follower input. A query node must never parse, plan, or re-execute the originating GQL mutation.
- The designated writer remains exactly one process. Slice 10 coordination/failover is not pulled forward; `v1/writer.json` is an advertisement, not a lock or election primitive.
- Query-node log consumption is bounded by `tail_batch_records`; no poll may call unbounded `Log::tail`. Polling remains the v1 transport because the three existing log backends expose range reads, not push streams.
- Basis waits are bounded by configuration and fail explicitly on timeout or follower failure. They never silently serve a snapshot older than the requested token.
- A writer acknowledgement remains durable-and-visible. The writer publishes applied progress only after the existing durable append → live-index apply sequence succeeds.
- All server bodies have configured byte limits. Static bearer tokens are compared in constant time and are never included in logs, errors, status, or metrics labels.
- HTTP Arrow responses use `application/vnd.apache.arrow.stream`; JSON remains the default when `Accept` is absent or explicitly includes `application/json`.
- `GET /healthz` is public. Every `/v1/*` route and `/metrics` requires authentication. Do not add CORS or browser-session state in v1.
- JSONL import uses the normal parameterized transaction path, one input record per transaction. It never writes Arrow blocks, log records, or object-store keys directly.
- The Slice 9 roadmap deliberately narrows design §11 transfer support to JSONL. Do not add CSV flags, parsers, routes, or compatibility behavior in this slice.
- Every task runs `rtk cargo fmt --all --check`, its focused tests, and `rtk cargo clippy --workspace --all-targets -- -D warnings` before commit. Use only the conventional prefixes allowed by `AGENTS.md`; do not add co-author trailers.

### Dependency/API contract

- Keep the existing root pins unified: `datafusion = "54"` resolves 54.0.0, `arrow = "58"` resolves 58.3.0, `object_store = "0.13"` resolves 0.13.2, and `reqwest = "0.12"` deliberately reuses the 0.12.28 already in `Cargo.lock` instead of introducing reqwest 0.13.
- Change the Arrow declaration to `arrow = { version = "58", features = ["prettyprint"] }` and add `arrow-json = "58"`. `RecordBatch` must still resolve through one Arrow 58 crate across engine, server, and CLI.
- Add workspace pins as they first become used: `serde_json = "1"`, `base64 = "0.22"`, `axum = { version = "0.8", features = ["http1", "http2", "json", "tokio"] }`, `axum-server = { version = "0.8", features = ["tls-rustls"] }`, `prometheus = "0.14"`, `subtle = "2.6"`, `clap = { version = "4.6", features = ["derive", "env"] }`, `reqwest = { version = "0.12", default-features = false, features = ["json", "rustls-tls", "http2", "stream"] }`, `rustyline = "18"`, `tokio-stream = "0.1"`, `url = "2"`, `tracing = "0.1"`, and `tracing-subscriber = { version = "0.3", features = ["env-filter", "fmt"] }`.
- Extend workspace tokio features to `['rt-multi-thread', 'macros', 'sync', 'time', 'net', 'signal', 'io-util', 'fs']` when the server binary lands.
- APIs were checked against the pinned/resolved sources: DataFusion 54 has `DataFrame::execute_stream(self) -> Result<SendableRecordBatchStream>` and `EmptyRecordBatchStream::new(SchemaRef)`; Arrow IPC 58 has `StreamWriter::try_new`, `write`, and `finish`; arrow-json 58 has `WriterBuilder::build::<_, JsonArray|LineDelimited>`; axum-server 0.8 has `bind_rustls` and async `RustlsConfig::from_pem_file`; Prometheus 0.14 has per-registry `gather` plus `TextEncoder`; rustyline 18 has `DefaultEditor::new`, `readline`, and `add_history_entry`.
- As in Slice 1, **test code is the contract**. If an import path or builder sketch below differs from the exact resolved patch release at implementation time, adapt the implementation/imports only. Do not weaken assertions, change wire content types, buffer an endpoint that the tests require to stream, or change public types to make a sketch compile.

## File and Module Map

- `crates/varve-config/src/byte_size.rs` — one strict IEC byte-size parser shared by log/cache/server configuration.
- `crates/varve-log/src/{local,object_store}.rs` — bounded range reads stop decoding at the exclusive upper position.
- `crates/varve-engine/src/replay.rs` — decode and atomically apply resolved log effects; shared by startup recovery and followers.
- `crates/varve-engine/src/node.rs` — node roles, applied progress, status, and basis tokens.
- `crates/varve-engine/src/follower.rs` — bounded query-node poll/apply loop and lifecycle handle.
- `crates/varve-engine/src/verify.rs` — read-only manifest/block/log integrity verification.
- `crates/varve-engine/src/db.rs` — cloneable facade, role assembly/gates, basis-aware query builder, writer advertisement, and public reports.
- `crates/varve-plan/src/pattern.rs` — lazy final DataFrame and `SendableRecordBatchStream` execution surface.
- `crates/varve-server/src/api.rs` — shared v1 wire DTOs and JSON/Varve value conversion.
- `crates/varve-server/src/{frontend,auth,metrics}.rs` — protocol, authentication, and metrics interfaces plus registries.
- `crates/varve-server/src/http/{mod,handlers,encoding}.rs` — axum router/handlers, status mapping, JSON encoding, and Arrow IPC body streaming.
- `crates/varve-server/src/bin/varved.rs` — config-driven node/frontend assembly and graceful shutdown.
- `crates/varve-cli/src/{client,embedded,remote}.rs` — one CLI client interface and its two real adapters.
- `crates/varve-cli/src/{output,shell,transfer,admin}.rs` — table/JSONL output, REPL, import/export, and administration.
- `crates/varve-cli/src/main.rs` — clap command tree and exit-code handling.
- `deploy/` plus root `Dockerfile`, `.dockerignore`, and `docker-compose.yml` — Garage bootstrap and the 1-writer/2-query-node demo.

---

### Task 1: Strict human-readable byte sizes across log and cache configuration

**Files:**
- Create: `crates/varve-config/src/byte_size.rs`
- Modify: `crates/varve-config/src/lib.rs`
- Modify: `crates/varve-engine/src/db.rs`
- Modify: `crates/varve-storage/src/cache.rs`
- Modify: `crates/varve-storage/src/disk.rs`
- Test: in-module `#[cfg(test)]` in `crates/varve-config/src/byte_size.rs`
- Test: existing factory tests in `crates/varve-storage/src/cache.rs` and `crates/varve-storage/src/disk.rs`

**Interfaces:**
- Produces: `varve_config::ByteSize` — `#[derive(Clone, Copy, Debug, PartialEq, Eq)] pub struct ByteSize(usize)`.
- Produces: `pub const fn ByteSize::from_bytes(bytes: usize) -> ByteSize` and `pub const fn ByteSize::as_usize(self) -> usize`.
- Produces: strict `Deserialize` for strings matching `^[0-9]+(B|KiB|MiB|GiB)$`; suffixes use powers of 1024. Empty values, whitespace, fractions, SI suffixes (`KB`, `MB`, `GB`), unknown suffixes, and `usize` overflow fail deserialization.
- Changes configuration contracts, with no numeric compatibility form: `[log] group_commit_max_bytes = "8MiB"`, `[cache.memory] max_bytes = "512MiB"`, and `[cache.disk] max_bytes = "50GiB"`.
- Defaults remain exactly 8 MiB, 512 MiB, and 50 GiB.

- [ ] **Step 1: Write parser contract tests**

`crates/varve-config/src/byte_size.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[derive(Deserialize)]
    struct Tuning {
        size: ByteSize,
    }

    fn parse(value: &str) -> Result<usize, toml::de::Error> {
        toml::from_str::<Tuning>(&format!("size = {value}"))
            .map(|tuning| tuning.size.as_usize())
    }

    #[test]
    fn parses_exact_iec_units() {
        assert_eq!(parse("\"0B\"").unwrap(), 0);
        assert_eq!(parse("\"8KiB\"").unwrap(), 8 * 1024);
        assert_eq!(parse("\"8MiB\"").unwrap(), 8 * 1024 * 1024);
        assert_eq!(parse("\"2GiB\"").unwrap(), 2 * 1024 * 1024 * 1024);
    }

    #[test]
    fn rejects_numeric_and_ambiguous_forms() {
        for value in ["8388608", "\"8MB\"", "\"8 MiB\"", "\"1.5GiB\"", "\"MiB\""] {
            assert!(parse(value).is_err(), "{value} must be rejected");
        }
    }

    #[test]
    fn rejects_overflow() {
        assert!(parse("\"18446744073709551615GiB\"").is_err());
    }
}
```

- [ ] **Step 2: Run the focused test and confirm RED**

Run: `rtk cargo test -p varve-config byte_size`

Expected: FAIL because `byte_size` and `ByteSize` do not exist.

- [ ] **Step 3: Implement `ByteSize` and export it**

`crates/varve-config/src/byte_size.rs`:

```rust
use serde::de::{Error as _, Visitor};
use serde::{Deserialize, Deserializer};
use std::fmt;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ByteSize(usize);

impl ByteSize {
    pub const fn from_bytes(bytes: usize) -> ByteSize {
        ByteSize(bytes)
    }

    pub const fn as_usize(self) -> usize {
        self.0
    }
}

fn parse(input: &str) -> Result<ByteSize, String> {
    let (digits, multiplier) = [
        ("GiB", 1024usize.pow(3)),
        ("MiB", 1024usize.pow(2)),
        ("KiB", 1024usize),
        ("B", 1usize),
    ]
    .into_iter()
    .find_map(|(suffix, multiplier)| input.strip_suffix(suffix).map(|d| (d, multiplier)))
    .ok_or_else(|| "byte size must end in B, KiB, MiB, or GiB".to_string())?;
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err("byte size must contain an unsigned integer followed by an IEC unit".into());
    }
    let amount = digits
        .parse::<usize>()
        .map_err(|error| format!("invalid byte-size integer: {error}"))?;
    amount
        .checked_mul(multiplier)
        .map(ByteSize)
        .ok_or_else(|| "byte size overflows usize".to_string())
}

struct ByteSizeVisitor;

impl Visitor<'_> for ByteSizeVisitor {
    type Value = ByteSize;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a quoted IEC byte size such as 8MiB")
    }

    fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Self::Value, E> {
        parse(value).map_err(E::custom)
    }
}

impl<'de> Deserialize<'de> for ByteSize {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_str(ByteSizeVisitor)
    }
}
```

Export it from `varve-config/src/lib.rs` with `pub use byte_size::ByteSize;`.

- [ ] **Step 4: Replace the three integer tuning fields and pin factory behavior**

Use `ByteSize` fields and convert only at the constructor seam:

```rust
#[derive(serde::Deserialize)]
struct LogTuning {
    #[serde(default = "default_window_ms")]
    group_commit_window_ms: u64,
    #[serde(default = "default_group_commit_max_bytes")]
    group_commit_max_bytes: ByteSize,
}

fn default_group_commit_max_bytes() -> ByteSize {
    ByteSize::from_bytes(8 * 1024 * 1024)
}
```

Set `WriterConfig.max_bytes` from `.as_usize()`. Apply the identical type/default pattern to `MemoryCacheConfig` and `DiskCacheConfig`; convert the disk value with `u64::try_from(config.max_bytes.as_usize())` and surface overflow as `RegistryError::Build` before `DiskCache::open`.

Add factory assertions that `"1MiB"` produces a 1 MiB tier and numeric `1048576` returns `ConfigError::Deserialize`.

- [ ] **Step 5: Run focused and full gates**

Run: `rtk cargo test -p varve-config byte_size`

Run: `rtk cargo test -p varve-storage cache`

Run: `rtk cargo fmt --all --check`

Run: `rtk cargo clippy --workspace --all-targets -- -D warnings`

Expected: all green; existing configs that omit these fields keep the same byte defaults.

- [ ] **Step 6: Commit**

```bash
rtk git add crates/varve-config crates/varve-engine/src/db.rs crates/varve-storage
rtk git commit -m "feat: parse human-readable byte sizes"
```

---

### Task 2: Make bounded log reads stop at their exclusive upper position

**Files:**
- Modify: `crates/varve-log/src/local.rs`
- Modify: `crates/varve-log/src/object_store.rs`
- Test: in-module `#[cfg(test)]` in both files
- Test: `crates/varve-log/tests/local_log.rs`
- Test: `crates/varve-log/tests/object_store_log.rs`

**Interfaces:**
- Keeps the public `Log::read_range(&self, from: LogPosition, to: LogPosition) -> Result<Vec<(LogPosition, LogRecord)>, LogError>` signature unchanged.
- Strengthens its performance/error interface: once `position >= to`, a backend stops reading/decoding later frames or objects. Corruption strictly at or beyond `to` is outside the requested range and is not surfaced by that call.
- Keeps range semantics half-open and ordered: `from <= position < to`.

- [ ] **Step 1: Write a local-log regression that corrupts only the excluded suffix**

Add an in-module test that appends two single-record batches, locates the second frame, corrupts its payload CRC after append, and then asserts:

```rust
#[tokio::test]
async fn bounded_read_does_not_decode_a_corrupt_excluded_frame() {
    let dir = tempfile::tempdir().unwrap();
    let log = LocalLog::open(dir.path(), DEFAULT_SEGMENT_MAX_BYTES).unwrap();
    log.append(vec![record(1)]).await.unwrap();
    log.append(vec![record(2)]).await.unwrap();
    corrupt_frame_crc(dir.path(), 1);

    let rows = log
        .read_range(LogPosition::ZERO, LogPosition::ZERO.advance(1).unwrap())
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1.tx_id, 1);
    assert!(matches!(
        log.read_range(LogPosition::ZERO, LogPosition::ZERO.advance(2).unwrap()).await,
        Err(LogError::Corrupt { .. })
    ));
}
```

`corrupt_frame_crc` is test-only and computes frame offsets from the same `FRAME_HEADER`/length grammar; it must not hard-code an Arrow or protobuf payload length.

- [ ] **Step 2: Run the local regression and confirm RED**

Run: `rtk cargo test -p varve-log bounded_read_does_not_decode_a_corrupt_excluded_frame`

Expected: FAIL with `LogError::Corrupt`, because `read_range_sync` currently scans every later frame.

- [ ] **Step 3: Add the early breaks to both durable backends**

In `read_range_sync`, insert the first guard at the top of the segment loop:

```rust
if LogPosition::from_u64(first) >= to {
    break;
}
```

Insert the second guard at the top of the frame loop, immediately before the current frame length/CRC/decode body:

```rust
if position >= to {
    return Ok(out);
}
```

Do not alter the current frame body. In `ObjectStoreLog::read_range`, insert this exact guard as the first statement inside the decoded-record loop:

```rust
if position >= to {
    break;
}
```

- [ ] **Step 4: Add backend contract cases**

For local and object-store logs, append tx ids 1–4 and assert ranges `0..0`, `1..3`, and `4..8` return `[]`, `[2,3]`, and `[]` respectively. These tests are the follower batching contract.

- [ ] **Step 5: Run gates**

Run: `rtk cargo test -p varve-log`

Run: `rtk cargo fmt --all --check`

Run: `rtk cargo clippy --workspace --all-targets -- -D warnings`

Expected: all log tests green, including recovery and trim suites.

- [ ] **Step 6: Commit**

```bash
rtk git add crates/varve-log
rtk git commit -m "perf: stop bounded log reads at upper position"
```

---

### Task 3: Extract one atomic resolved-effect replay module

**Files:**
- Create: `crates/varve-engine/src/replay.rs`
- Modify: `crates/varve-engine/src/lib.rs`
- Modify: `crates/varve-engine/src/state.rs`
- Modify: `crates/varve-engine/src/db.rs`
- Test: in-module `#[cfg(test)]` in `crates/varve-engine/src/replay.rs`
- Test: existing recovery tests in `crates/varve-engine/src/db.rs`

**Interfaces:**
- Produces: `pub(crate) struct DecodedLogRecord { pub tx_id: u64, pub system_time: Instant, pub effects: Vec<DecodedTableEffects> }`.
- Produces: `pub(crate) struct DecodedTableEffects { pub graph: String, pub table: TableKind, pub events: Vec<Event> }`.
- Produces: `pub(crate) fn decode_log_record(record: &LogRecord) -> Result<DecodedLogRecord, EngineError>` — validates every table and decodes every Arrow effect before state is mutated.
- Produces: `pub(crate) fn apply_decoded_log_record(state: &mut GraphsState, record: DecodedLogRecord) -> Result<(), EngineError>` — applies one transaction while the caller holds the state write lock.
- Changes `GraphsState` to own `catalog_graphs: BTreeMap<Iid, String>` so catalog create/drop replay works both during startup and after a follower is already running.
- `recover` uses these functions and deletes its private `catalog_iids` map plus `apply_catalog_replay_event` copy.

- [ ] **Step 1: Write decode-before-mutate and catalog replay tests**

```rust
#[test]
fn malformed_later_effect_cannot_partially_apply_a_record() {
    let good = TableEffects {
        graph: DEFAULT_GRAPH.into(),
        table: NODES_TABLE.into(),
        arrow_ipc: encode_events(&[put_event(1)]).unwrap(),
    };
    let bad = TableEffects {
        graph: DEFAULT_GRAPH.into(),
        table: EDGES_TABLE.into(),
        arrow_ipc: vec![0xff, 0xff],
    };
    let record = LogRecord {
        tx_id: 1,
        system_time_us: 1,
        user: String::new(),
        effects: vec![good, bad],
    };
    let mut state = GraphsState::new();

    assert!(decode_log_record(&record).is_err());
    assert_eq!(state.live_rows(), 0);
}

#[test]
fn catalog_put_and_delete_change_the_graph_map() {
    let mut state = GraphsState::new();
    apply_decoded_log_record(&mut state, decoded_catalog_put("tenant_a")).unwrap();
    assert!(state.graph("tenant_a").is_some());
    apply_decoded_log_record(&mut state, decoded_catalog_delete("tenant_a")).unwrap();
    assert!(state.graph("tenant_a").is_none());
}
```

Test helpers build real `Event`/`LogRecord` values; they do not call the GQL parser or writer resolver.

- [ ] **Step 2: Run and confirm RED**

Run: `rtk cargo test -p varve-engine replay`

Expected: compile failure because the module and interfaces do not exist.

- [ ] **Step 3: Implement full-record decoding**

```rust
pub(crate) fn decode_log_record(record: &LogRecord) -> Result<DecodedLogRecord, EngineError> {
    let mut effects = Vec::with_capacity(record.effects.len());
    for effect in &record.effects {
        let table = match effect.table.as_str() {
            NODES_TABLE => TableKind::Nodes,
            EDGES_TABLE => TableKind::Edges,
            other => return Err(EngineError::UnknownTable(other.to_string())),
        };
        effects.push(DecodedTableEffects {
            graph: if effect.graph.is_empty() {
                DEFAULT_GRAPH.to_string()
            } else {
                effect.graph.clone()
            },
            table,
            events: decode_events(&effect.arrow_ipc)?,
        });
    }
    Ok(DecodedLogRecord {
        tx_id: record.tx_id,
        system_time: Instant::from_micros(record.system_time_us),
        effects,
    })
}
```

- [ ] **Step 4: Implement catalog-aware application**

`apply_decoded_log_record` iterates decoded effects in envelope order. Before appending a `META_GRAPH` node event, it updates `state.catalog_graphs` and `state.graphs` using the same rules as current recovery: `Put` with label `Graph` and string `_id` creates non-reserved graphs; `Delete|Erase` removes the IID mapping and graph. It then appends the event to `state.graphs[graph].core_mut(table).live`. Unknown target graphs/tables are explicit errors; no GQL is parsed.

Move the catalog map into `GraphsState::new`:

```rust
pub(crate) struct GraphsState {
    pub graphs: BTreeMap<String, TableState>,
    pub catalog_graphs: BTreeMap<Iid, String>,
}
```

During persisted catalog recovery, populate `state.catalog_graphs` through the same private `apply_catalog_event` helper used by live replay.

- [ ] **Step 5: Refactor startup recovery onto the module**

Replace the nested effect/event loop in `recover` with:

```rust
for (position, record) in log.tail(watermark).await? {
    let decoded = decode_log_record(&record)?;
    let system_time = decoded.system_time;
    let tx_id = decoded.tx_id;
    apply_decoded_log_record(&mut state, decoded)?;
    next_tx_id = next_tx_id.max(tx_id);
    max_system = Some(max_system.map_or(system_time, |current| current.max(system_time)));
    watermark = watermark.max(position.advance(1)?);
}
```

Keep manifest inventory recovery and clock flooring unchanged.

- [ ] **Step 6: Run recovery and workspace gates**

Run: `rtk cargo test -p varve-engine replay`

Run: `rtk cargo test -p varve-engine recover_`

Run: `rtk cargo test -p varve --test catalog`

Run: `rtk cargo fmt --all --check`

Run: `rtk cargo clippy --workspace --all-targets -- -D warnings`

Expected: all green; existing recovery tests prove result equivalence, and the new atomicity test proves malformed records do not partially update a running state.

- [ ] **Step 7: Commit**

```bash
rtk git add crates/varve-engine
rtk git commit -m "refactor: share resolved effect replay"
```

---

### Task 4: Add cloneable node roles, query-node tailing, and observable progress

**Files:**
- Create: `crates/varve-engine/src/node.rs`
- Create: `crates/varve-engine/src/follower.rs`
- Modify: `crates/varve-engine/src/lib.rs`
- Modify: `crates/varve-engine/src/db.rs`
- Modify: `crates/varve-engine/src/writer.rs`
- Modify: `crates/varve/src/lib.rs`
- Test: `crates/varve-engine/tests/query_node.rs`
- Test: `crates/varve-engine/tests/concurrency.rs`

**Interfaces:**
- Produces: `#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd)] #[serde(rename_all = "kebab-case")] pub enum NodeRole { Writer, Query, Compactor }`.
- Produces: `#[derive(Clone, Debug, Eq, PartialEq)] pub struct NodeRoles(BTreeSet<NodeRole>)` with `pub fn contains(&self, role: NodeRole) -> bool` and `pub fn iter(&self) -> impl Iterator<Item = NodeRole> + '_`.
- Produces: `[node] roles = ["writer", "query", "compactor"]`; omission defaults to all three roles. `tail_poll_interval_ms` defaults to 50, `tail_batch_records` to 1024, and `basis_timeout_ms` to 5000. An empty role list, zero batch size, or `compactor` without `writer` is `EngineError::InvalidNodeConfig(String)`. Writer and Query remain independently composable roles.
- Produces: `#[derive(Clone, Copy, Debug, Eq, PartialEq)] pub struct AppliedProgress { pub tx_id: u64, pub log_position: LogPosition }`, where `log_position` is the exclusive end of the applied prefix.
- Produces: `#[derive(Clone, Debug, Eq, PartialEq)] pub struct NodeStatus { pub roles: NodeRoles, pub applied: AppliedProgress, pub manifest_block_id: Option<u64>, pub manifest_watermark: LogPosition, pub follower_error: Option<String> }`.
- Produces: `pub async fn Db::status(&self) -> Result<NodeStatus, EngineError>` and `pub fn Db::roles(&self) -> &NodeRoles`.
- Changes `Db` to `#[derive(Clone)] pub struct Db { inner: Arc<DbInner> }`; the final clone dropping stops the follower (query node) or closes the writer command channel (writer node).
- Query-only assembly runs `recover`, does not spawn `WriterHandle`, and starts `spawn_follower` at `Recovered.watermark` with `Recovered.next_tx_id` as applied progress.
- Adds role errors: `RoleDisabled(NodeRole)`, `FollowerFailed(String)`, `LogGap { expected: LogPosition, actual: LogPosition }`, and `InvalidNodeConfig(String)`.

- [ ] **Step 1: Write role validation and shared-local-store follower tests**

`crates/varve-engine/tests/query_node.rs`:

```rust
use std::time::Duration;
use tempfile::TempDir;
use varve_engine::{Db, EngineError, NodeRole};

fn config(root: &TempDir, roles: &[&str], poll_ms: u64, batch: usize) -> varve_config::Config {
    let roles = roles
        .iter()
        .map(|role| format!("\"{role}\""))
        .collect::<Vec<_>>()
        .join(", ");
    varve_config::Config::from_toml_str(&format!(
        "[node]\nroles = [{roles}]\ntail_poll_interval_ms = {poll_ms}\n\
         tail_batch_records = {batch}\nbasis_timeout_ms = 1000\n\
         [log]\nbackend = \"local\"\ngroup_commit_window_ms = 0\n\
         [log.local]\ndir = {:?}\n\
         [storage]\nbackend = \"local\"\nmax_block_rows = 100000\n\
         [storage.local]\ndir = {:?}\n",
        root.path().join("log").display().to_string(),
        root.path().join("store").display().to_string(),
    ))
    .unwrap()
}

#[tokio::test]
async fn query_node_applies_resolved_effects_and_has_no_writer() {
    let root = TempDir::new().unwrap();
    let writer = Db::open(config(&root, &["writer", "query", "compactor"], 5, 2))
        .await
        .unwrap();
    let query = Db::open(config(&root, &["query"], 5, 2)).await.unwrap();

    let receipt = writer
        .execute("INSERT (:Person {_id: 1, name: 'Ada'})")
        .await
        .unwrap();
    for _ in 0..200 {
        if query.status().await.unwrap().applied.tx_id >= receipt.tx_id {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(query.status().await.unwrap().applied.tx_id >= receipt.tx_id);

    let batches = query.query("MATCH (p:Person) RETURN p.name AS name").await.unwrap();
    assert_eq!(batches.iter().map(|batch| batch.num_rows()).sum::<usize>(), 1);
    assert!(matches!(
        query.execute("INSERT (:X {_id: 2})").await,
        Err(EngineError::RoleDisabled(NodeRole::Writer))
    ));
    assert!(query.status().await.unwrap().follower_error.is_none());
}

#[tokio::test]
async fn node_roles_reject_invalid_combinations() {
    let root = TempDir::new().unwrap();
    assert!(matches!(
        Db::open(config(&root, &[], 5, 1)).await,
        Err(EngineError::InvalidNodeConfig(_))
    ));
    assert!(matches!(
        Db::open(config(&root, &["query"], 5, 0)).await,
        Err(EngineError::InvalidNodeConfig(_))
    ));
    assert!(matches!(
        Db::open(config(&root, &["query", "compactor"], 5, 1)).await,
        Err(EngineError::InvalidNodeConfig(_))
    ));
}
```

- [ ] **Step 2: Run and confirm RED**

Run: `rtk cargo test -p varve-engine --test query_node`

Expected: compile failure for `NodeRole`, role-aware open, and status.

- [ ] **Step 3: Implement role/progress types and config parsing**

`node.rs` owns the public types and crate-private watch state:

```rust
#[derive(Clone, Debug)]
pub(crate) struct ProgressState {
    pub applied: AppliedProgress,
    pub follower_error: Option<String>,
}

impl ProgressState {
    pub fn running(tx_id: u64, log_position: LogPosition) -> Self {
        Self {
            applied: AppliedProgress { tx_id, log_position },
            follower_error: None,
        }
    }
}
```

Deserialize a private `NodeTuning`, validate it once in `Db::open_with`, and pass a validated `NodeConfig` into assembly. `Db::memory()` and `Db::local()` use `NodeRoles::all()` and therefore preserve the laptop profile without a compatibility branch.

- [ ] **Step 4: Build the bounded follower module**

`follower.rs`:

```rust
pub(crate) struct FollowerState {
    pub state: Arc<RwLock<GraphsState>>,
    pub log: Arc<dyn Log>,
    pub store: Arc<dyn ObjectStore>,
    pub cursor: LogPosition,
    pub config: FollowerConfig,
    pub progress: watch::Sender<ProgressState>,
}

#[derive(Clone, Copy)]
pub(crate) struct FollowerConfig {
    pub poll_interval: Duration,
    pub batch_records: u64,
}

pub(crate) struct FollowerHandle {
    shutdown: watch::Sender<bool>,
}

pub(crate) async fn apply_next_range(state: &mut FollowerState) -> Result<usize, EngineError> {
    let upper = state.cursor.advance(state.config.batch_records)?;
    let records = state.log.read_range(state.cursor, upper).await?;
    if records.is_empty() {
        if let Some(manifest) = latest_manifest(state.store.as_ref()).await? {
            let manifest_watermark = LogPosition::from_u64(manifest.watermark);
            if state.cursor < manifest_watermark {
                return Err(EngineError::LogGap {
                    expected: state.cursor,
                    actual: manifest_watermark,
                });
            }
        }
        return Ok(0);
    }
    if let Some((actual, _)) = records.first() {
        if *actual != state.cursor {
            return Err(EngineError::LogGap {
                expected: state.cursor,
                actual: *actual,
            });
        }
    }
    let mut applied = 0usize;
    for (position, record) in records {
        let decoded = decode_log_record(&record)?;
        let tx_id = decoded.tx_id;
        let next = position.advance(1)?;
        {
            let mut graphs = state.state.write().map_err(|_| EngineError::Poisoned)?;
            apply_decoded_log_record(&mut graphs, decoded)?;
        }
        state.cursor = next;
        state.progress.send_replace(ProgressState::running(tx_id, next));
        applied += 1;
    }
    Ok(applied)
}
```

`spawn_follower` loops on `apply_next_range`; it immediately polls again after a non-empty batch, sleeps `poll_interval` after an empty batch, exits on shutdown, and publishes the first terminal error to `ProgressState.follower_error` before exiting. `FollowerHandle::drop` sends shutdown; no detached task survives the final `Db` clone.

- [ ] **Step 5: Refactor `Db` assembly and make the handle cloneable**

Move current `Db` fields into `DbInner`; retain `log: Arc<dyn Log>` alongside the existing store/clock/state for status/verification; replace `writer: WriterHandle` with `writer: Option<WriterHandle>`; and add `roles`, `progress: watch::Receiver<ProgressState>`, `basis_timeout`, and `follower: Option<FollowerHandle>`. Branch exactly once during assembly:

```rust
let (progress_tx, progress_rx) = watch::channel(ProgressState::running(
    recovered.next_tx_id,
    recovered.watermark,
));
let writer = roles.contains(NodeRole::Writer).then(|| {
    writer_state.progress = progress_tx.clone();
    spawn_writer(writer_state, writer_config)
});
let follower = if roles.contains(NodeRole::Writer) {
    None
} else {
    Some(spawn_follower(FollowerState {
        state: Arc::clone(&state),
        log: Arc::clone(&log),
        store: Arc::clone(&store),
        cursor: recovered.watermark,
        config: follower_config,
        progress: progress_tx,
    }))
};
```

All existing methods access `self.inner`. `execute` requires `Writer`; `query` requires `Query`; `compact_once` and `gc_once` require `Compactor`.

- [ ] **Step 6: Publish writer progress only after apply succeeds**

Add `progress: watch::Sender<ProgressState>` to `WriterState`. In `writer::flush`, after `apply` returns `Ok(())`, publish the last staged receipt's `tx_id` and `first.advance(count)` before sending acknowledgements. On append/apply error, leave progress unchanged.

Extend the existing concurrency test to clone `Db` directly instead of wrapping it in a second `Arc`:

```rust
let db = Db::memory();
let task_db = db.clone();
tokio::spawn(async move { task_db.execute("INSERT (:C {_id: 1})").await });
```

- [ ] **Step 7: Implement async status**

`Db::status` clones the latest progress, calls `latest_manifest(self.inner.store.as_ref()).await`, and returns `manifest_block_id` plus its replay watermark (or `None`/zero without a manifest). It does not run the conditional-PUT probe.

- [ ] **Step 8: Run focused and regression gates**

Run: `rtk cargo test -p varve-engine --test query_node -- --test-threads=1`

Run: `rtk cargo test -p varve-engine --test concurrency`

Run: `rtk cargo test -p varve --test durability -- --test-threads=1`

Run: `rtk cargo fmt --all --check`

Run: `rtk cargo clippy --workspace --all-targets -- -D warnings`

Expected: query node catches up without GQL replay, query-node writes are rejected, existing writer durability/concurrency behavior remains green, and `Db: Clone + Debug`.

- [ ] **Step 9: Commit**

```bash
rtk git add crates/varve-engine crates/varve/src/lib.rs
rtk git commit -m "feat: add query-node role and log follower"
```

---

### Task 5: Add bounded basis waits and a streaming query builder

**Files:**
- Modify: `crates/varve-plan/src/pattern.rs`
- Modify: `crates/varve-plan/src/lib.rs`
- Modify: `crates/varve-engine/src/node.rs`
- Modify: `crates/varve-engine/src/db.rs`
- Modify: `crates/varve-engine/src/lib.rs`
- Modify: `crates/varve/src/lib.rs`
- Modify: current `.query_with` call sites under `crates/`
- Test: `crates/varve-plan/tests/exec_test.rs`
- Test: `crates/varve-engine/tests/query_node.rs`
- Test: `crates/varve/tests/pipeline.rs`

**Interfaces:**
- Produces: `#[derive(Clone, Copy, Debug, Eq, PartialEq)] pub enum BasisToken { TxId(u64), At(LogPosition) }`.
- Produces: `impl From<TxReceipt> for BasisToken`, `impl From<&TxReceipt> for BasisToken`, and `impl From<LogPosition> for BasisToken`.
- Produces: `pub async fn Db::wait_for_basis(&self, basis: BasisToken, timeout: Duration) -> Result<(), EngineError>`.
- Adds `EngineError::BasisTimeout { requested: BasisToken, applied: AppliedProgress }`.
- Replaces the old async method with `pub fn Db::query(&self, gql: impl Into<String>) -> Query`; existing `db.query("MATCH (n:N) RETURN n").await` syntax continues because `Query: IntoFuture`, not through a compatibility wrapper.
- Produces: `pub struct Query { db: Db, gql: String, params: BTreeMap<String, Value>, basis: Option<BasisToken>, timeout: Duration }`.
- Produces: `Query::params(self, BTreeMap<String, Value>) -> Query`, `Query::basis(self, impl Into<BasisToken>) -> Query`, `Query::basis_timeout(self, Duration) -> Query`, and `pub async fn Query::stream(self) -> Result<SendableRecordBatchStream, EngineError>`.
- Removes public `Db::query_with`; update all repository call sites to the builder. This is the sole query interface.
- Produces in `varve-plan`:

```rust
pub async fn execute_body_stream_with_limits(
    body: &QueryBody,
    clause_specs: &[ClauseSpecs],
    inputs: Vec<Vec<ScanInput>>,
    functions: &FunctionRegistry,
    path_expand_limits: PathExpandLimits,
    params: &BTreeMap<String, Value>,
) -> Result<SendableRecordBatchStream, PlanError>;

pub async fn union_query_results_stream(
    first: Vec<RecordBatch>,
    unions: Vec<(UnionKind, Vec<RecordBatch>)>,
    functions: &FunctionRegistry,
) -> Result<SendableRecordBatchStream, PlanError>;
```
- Keeps `execute_body_with_limits` and `union_query_results` as collection helpers for mutation planning/tests; they collect the new stream and contain no second lowering implementation.

- [ ] **Step 1: Write basis timeout/catch-up tests**

Append to `query_node.rs`:

```rust
#[tokio::test]
async fn basis_wait_times_out_then_succeeds_after_writer_commit() {
    let root = TempDir::new().unwrap();
    let writer = Db::open(config(&root, &["writer", "query", "compactor"], 200, 16))
        .await
        .unwrap();
    let query = Db::open(config(&root, &["query"], 200, 16)).await.unwrap();

    assert!(matches!(
        query
            .wait_for_basis(BasisToken::TxId(1), Duration::from_millis(10))
            .await,
        Err(EngineError::BasisTimeout { .. })
    ));

    let receipt = writer.execute("INSERT (:X {_id: 1})").await.unwrap();
    let rows = query
        .query("MATCH (x:X) RETURN x._id AS id")
        .basis(receipt)
        .basis_timeout(Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(rows.iter().map(|batch| batch.num_rows()).sum::<usize>(), 1);
}
```

Add a terminal follower-error test with a test log that returns `LogError::Corrupt`; the wait must return `FollowerFailed` immediately instead of waiting for the timeout.

- [ ] **Step 2: Write stream equivalence and empty-schema tests**

In `varve-plan/tests/exec_test.rs`, execute the same ordered query through collecting and streaming entry points, collect the stream with `TryStreamExt::try_collect`, and assert identical schemas/rows. Add an unknown-label query and assert its stream has a stable (possibly empty) schema and yields zero rows rather than failing to construct Arrow IPC.

- [ ] **Step 3: Run and confirm RED**

Run: `rtk cargo test -p varve-engine --test query_node basis_`

Run: `rtk cargo test -p varve-plan stream_`

Expected: compile failure for basis/query-builder/stream interfaces.

- [ ] **Step 4: Refactor final projection into a lazy DataFrame**

Split `project_return_body` into a synchronous `project_return_body_frame -> Result<DataFrame, PlanError>` containing the existing aggregate/project/distinct/order/limit lowering, and a collecting wrapper. Split `execute_body_with_limits` the same way: one private `build_body_frame_with_limits -> Result<Option<DataFrame>, PlanError>`, where `None` is the existing empty-input outcome.

The private signatures are fixed:

```rust
fn project_return_body_frame(
    df: DataFrame,
    ret: &ReturnClause,
    specs: &[ScanSpec],
    value_vars: &BTreeSet<String>,
    params: &BTreeMap<String, Value>,
    functions: &FunctionRegistry,
) -> Result<DataFrame, PlanError>;

fn build_body_frame_with_limits(
    body: &QueryBody,
    clause_specs: &[ClauseSpecs],
    inputs: Vec<Vec<ScanInput>>,
    functions: &FunctionRegistry,
    path_expand_limits: PathExpandLimits,
    params: &BTreeMap<String, Value>,
) -> Result<Option<DataFrame>, PlanError>;
```

The stream entry point is then exactly:

```rust
pub async fn execute_body_stream_with_limits(
    body: &QueryBody,
    clause_specs: &[ClauseSpecs],
    inputs: Vec<Vec<ScanInput>>,
    functions: &FunctionRegistry,
    path_expand_limits: PathExpandLimits,
    params: &BTreeMap<String, Value>,
) -> Result<SendableRecordBatchStream, PlanError> {
    match build_body_frame_with_limits(
        body,
        clause_specs,
        inputs,
        functions,
        path_expand_limits,
        params,
    )? {
        Some(frame) => Ok(frame.execute_stream().await?),
        None => Ok(Box::pin(EmptyRecordBatchStream::new(Arc::new(
            Schema::empty(),
        )))),
    }
}
```

`execute_body_with_limits` collects this stream. `union_query_results_stream` builds the existing union DataFrame and calls `execute_stream`; distinct union remains a DataFusion operator. Union arms may remain materialized because their schema compatibility check requires each arm's projected schema.

- [ ] **Step 5: Implement progress waits**

`wait_for_basis` clones the watch receiver, checks its current value before awaiting, loops on `changed()`, returns `FollowerFailed` if the state carries an error or the channel closes, and wraps the loop in `tokio::time::timeout`. Satisfaction is `applied.tx_id >= n` for `TxId(n)` and `applied.log_position >= position` for `At(position)`.

- [ ] **Step 6: Implement the owned query builder**

`Query` owns a cloned `Db`, so its `IntoFuture` is `Send + 'static`. `Query::stream` validates the Query role, awaits an optional basis, then calls a private `Db::query_stream_impl(gql, params)`. The current `query_with` body moves into that private function and uses `execute_body_stream_with_limits` for non-UNION queries. `IntoFuture` collects with `TryStreamExt::try_collect` and maps DataFusion errors through `PlanError`.

Exact `IntoFuture` surface:

```rust
impl std::future::IntoFuture for Query {
    type Output = Result<Vec<RecordBatch>, EngineError>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let stream = self.stream().await?;
            stream
                .try_collect::<Vec<_>>()
                .await
                .map_err(|error| EngineError::Plan(PlanError::DataFusion(error)))
        })
    }
}
```

Use the actual public `PlanError` DataFusion variant name from the pinned code if it differs; test expectations and returned error semantics do not change.

- [ ] **Step 7: Update all current call sites without aliases**

Convert `db.query_with(gql, &params).await` to `db.query(gql).params(params.clone()).await`. Existing no-param `.query(gql).await` remains source-compatible by virtue of `IntoFuture`, but it now exercises the builder.

- [ ] **Step 8: Run query, traversal, and full gates**

Run: `rtk cargo test -p varve-plan`

Run: `rtk cargo test -p varve-engine --test query_node -- --test-threads=1`

Run: `rtk cargo test -p varve --test pipeline --test traversal -- --test-threads=1`

Run: `rtk cargo fmt --all --check`

Run: `rtk cargo clippy --workspace --all-targets -- -D warnings`

Expected: stream/collect equivalence, bounded basis behavior, and all existing query semantics green.

- [ ] **Step 9: Commit**

```bash
rtk git add crates/varve-plan crates/varve-engine crates/varve crates/varve-testkit
rtk git commit -m "feat: add basis-aware streaming queries"
```

---

### Task 6: Add writer advertisement, role-gated administration, and integrity verification

**Files:**
- Create: `crates/varve-engine/src/verify.rs`
- Modify: `Cargo.toml` (add workspace `serde_json = "1"`)
- Modify: `crates/varve-engine/Cargo.toml` (add `serde_json`)
- Modify: `crates/varve-engine/src/lib.rs`
- Modify: `crates/varve-engine/src/db.rs`
- Modify: `crates/varve/src/lib.rs`
- Test: `crates/varve-engine/tests/admin.rs`
- Test: existing `crates/varve/tests/compaction.rs` and `crates/varve/tests/gc.rs`

**Interfaces:**
- Produces: constant object key `v1/writer.json` owned by `varve-engine::db`.
- Produces: `#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)] pub struct WriterAdvertisement { pub address: String }`.
- Produces: `pub async fn Db::publish_writer(&self, address: &str) -> Result<(), EngineError>`; requires Writer role and overwrites `v1/writer.json` with canonical serde_json bytes via plain PUT.
- Produces: `pub async fn Db::writer_advertisement(&self) -> Result<Option<WriterAdvertisement>, EngineError>`; a missing object is `Ok(None)`, malformed JSON is an error.
- Produces: `#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)] pub struct VerifyReport { pub manifest_block_id: Option<u64>, pub tries_checked: usize, pub pages_checked: usize, pub events_checked: usize, pub log_records_checked: usize }`.
- Produces: `pub async fn Db::verify(&self) -> Result<VerifyReport, EngineError>`; valid on Writer or Query roles and strictly read-only.
- `compact_once` and `gc_once` require `NodeRole::Compactor`; `execute` and `publish_writer` require Writer. Query-only nodes therefore cannot mutate inventory.

- [ ] **Step 1: Write advertisement and role-gate tests**

```rust
#[tokio::test]
async fn writer_advertisement_round_trips_through_the_store() {
    let root = TempDir::new().unwrap();
    let writer = Db::open(writer_config(&root)).await.unwrap();
    let query = Db::open(query_config(&root)).await.unwrap();

    writer.publish_writer("https://writer.internal:8443").await.unwrap();
    assert_eq!(
        query.writer_advertisement().await.unwrap().unwrap().address,
        "https://writer.internal:8443"
    );
    assert!(matches!(
        query.publish_writer("https://wrong").await,
        Err(EngineError::RoleDisabled(NodeRole::Writer))
    ));
    assert!(matches!(
        query.compact_once().await,
        Err(EngineError::RoleDisabled(NodeRole::Compactor))
    ));
}
```

- [ ] **Step 2: Write verification success and corruption tests**

Create a local database with a forced block flush. `verify` must report one or more tries/pages/events and the log tail count. Then truncate one referenced data object to fewer bytes than its meta page range and assert `verify` returns `EngineError::Storage` or `EngineError::Index`, never a panic and never `Ok`.

- [ ] **Step 3: Run and confirm RED**

Run: `rtk cargo test -p varve-engine --test admin`

Expected: compile failure for advertisement and verify interfaces.

- [ ] **Step 4: Implement the advertisement methods**

Serialize with `serde_json::to_vec`, store via `ObjectStore::put`, and read with `ObjectStore::get`. Detect absence without string-matching errors by first listing the exact `v1/writer.json` prefix and requiring an exact key before GET. Validate addresses with `url::Url` in the server task; the engine stores the supplied opaque address.

- [ ] **Step 5: Implement full latest-snapshot verification**

`verify.rs` performs these deterministic checks in manifest table/trie order:

1. Load the latest manifest; if absent, verify the log from zero and return a zero-manifest report.
2. For every trie entry, derive its data/meta keys from `TableScope` and `TrieKey`, GET both, assert `entry.data_len == data.len()`, and decode meta.
3. Require meta pages to be non-overlapping and ascending; require every checked `offset + len` to fit inside the data bytes; decode every page with `decode_events`; require decoded row count to equal `PageMeta.row_count`; require the trie total to equal `TrieEntry.row_count`.
4. Verify the log tail in batches of 1024 using `read_range(cursor, cursor.advance(1024)?)`; require the first returned position to equal the cursor; run `decode_log_record` for every record without applying it; advance only over returned records and stop on an empty batch.

Expose one entry point:

```rust
pub(crate) async fn verify_database(
    store: &dyn ObjectStore,
    log: &dyn Log,
) -> Result<VerifyReport, EngineError>;
```

No verification step calls `delete`, `put`, `append`, `trim`, or the capability probe.

- [ ] **Step 6: Run admin and regression gates**

Run: `rtk cargo test -p varve-engine --test admin -- --test-threads=1`

Run: `rtk cargo test -p varve --test compaction --test gc -- --test-threads=1`

Run: `rtk cargo fmt --all --check`

Run: `rtk cargo clippy --workspace --all-targets -- -D warnings`

Expected: reports are correct, corruption is loud, query-node inventory mutation is rejected, and compaction/GC behavior is unchanged for the default all-role embedded profile.

- [ ] **Step 7: Commit**

```bash
rtk git add Cargo.toml Cargo.lock crates/varve-engine crates/varve/src/lib.rs
rtk git commit -m "feat: add node status advertisement and verification"
```

---

### Task 7: Scaffold `varve-server` and freeze the v1 JSON/wire contract

**Files:**
- Modify: `Cargo.toml` (add `base64 = "0.22"`, `arrow-json = "58"`; reuse Task 6's `serde_json = "1"`)
- Modify: `crates/varve/Cargo.toml` (add `serde_json`, `arrow-json`)
- Modify: `crates/varve/src/lib.rs`
- Create: `crates/varve/src/rows.rs`
- Create: `crates/varve-server/Cargo.toml`
- Create: `crates/varve-server/src/lib.rs`
- Create: `crates/varve-server/src/api.rs`
- Create: `crates/varve-server/src/error.rs`
- Test: `crates/varve-server/tests/api.rs`
- Test: `crates/varve/tests/rows.rs`

**Interfaces:**
- All request/response DTOs below derive `Clone, Debug, Serialize, Deserialize, PartialEq`; response-only structs may omit `Deserialize` only if no CLI/client test consumes them. Field names use serde's default snake_case exactly as written.
- Produces media constant `pub const ARROW_STREAM_CONTENT_TYPE: &str = "application/vnd.apache.arrow.stream"`.
- Produces: `QueryRequest { gql: String, params: BTreeMap<String, serde_json::Value>, basis: Option<BasisRequest>, basis_timeout_ms: Option<u64> }`.
- Produces: `#[serde(untagged)] enum BasisRequest { TxId(u64), At(String) }`; string form must be exactly `at:<packed-u64>` and converts to `BasisToken::At(LogPosition::from_u64(value))`.
- Produces: `TxRequest { gql: String, params: BTreeMap<String, serde_json::Value> }`.
- Produces: `TxResponse { tx_id: u64, system_time: String, system_time_us: i64, side_effects: SideEffectsResponse, basis: u64 }` where `basis == tx_id`.
- Produces: `SideEffectsResponse { nodes_created: usize, nodes_deleted: usize, relationships_created: usize, relationships_deleted: usize, properties_set: usize, properties_removed: usize, labels_added: usize, labels_removed: usize }`.
- Produces: `QueryJsonResponse { rows: Vec<serde_json::Map<String, serde_json::Value>> }`.
- Produces: `StatusResponse { roles: Vec<String>, applied_tx_id: u64, applied_log_position: u64, manifest_block_id: Option<u64>, manifest_watermark: u64, follower_error: Option<String>, probe: ProbeResponse }`.
- Produces: `ProbeResponse { verdict: String, reason: Option<String>, probe_key: String }`; verdict is exactly `supported`, `unsupported`, or `inconsistent`.
- Produces: `CompactionResponse { jobs: usize, input_tries: usize, output_tries: usize, input_rows: u64, output_rows: u64 }`.
- Produces: `GcResponse { planned_objects: usize, deleted_objects: usize }`.
- Produces: `VerifyResponse { manifest_block_id: Option<u64>, tries_checked: usize, pages_checked: usize, events_checked: usize, log_records_checked: usize }`.
- Produces mapping constructors: `TxResponse::from_receipt(&TxReceipt) -> TxResponse`, `StatusResponse::from_engine(&NodeStatus, &ProbeReport) -> StatusResponse`, `CompactionResponse::from_report(&CompactionReport) -> CompactionResponse`, `GcResponse::from_report(&GcReport) -> GcResponse`, and `VerifyResponse::from_report(&VerifyReport) -> VerifyResponse`.
- Produces: `ErrorResponse { code: String, message: String, writer: Option<String> }`.
- Produces: `pub fn params_from_json(&BTreeMap<String, serde_json::Value>) -> Result<BTreeMap<String, Value>, ServerError>`.
- Produces: `pub fn batches_to_json(&[RecordBatch]) -> Result<QueryJsonResponse, ServerError>` using arrow-json 58 with explicit nulls.
- Produces the design §11 embedded ergonomic iterator in the facade: `pub type JsonRow = serde_json::Map<String, serde_json::Value>`, `pub struct RowIter`, and `pub fn rows(batches: &[RecordBatch]) -> Result<RowIter, RowError>`. `RowIter: Iterator<Item = JsonRow>` and uses the same explicit-null arrow-json conversion as HTTP.
- `batches_to_json` is a thin adapter over `varve::rows`; there is one Arrow-to-serde row implementation.
- JSON scalar conversion accepts null, bool, signed i64, finite f64, UTF-8 string, and bytes encoded only as `{ "$bytes": "<base64-standard>" }`. Arrays, other objects, unsigned integers above `i64::MAX`, NaN, and infinity are errors.

- [ ] **Step 1: Write round-trip and rejection tests first**

`crates/varve-server/tests/api.rs`:

```rust
#[test]
fn query_request_accepts_tx_and_position_basis_forms() {
    let tx: QueryRequest = serde_json::from_value(json!({
        "gql": "MATCH (n:N) RETURN n.x",
        "basis": 42
    }))
    .unwrap();
    assert_eq!(tx.basis.unwrap().try_into().unwrap(), BasisToken::TxId(42));

    let at: QueryRequest = serde_json::from_value(json!({
        "gql": "MATCH (n:N) RETURN n.x",
        "basis": "at:281474976710663"
    }))
    .unwrap();
    assert_eq!(
        at.basis.unwrap().try_into().unwrap(),
        BasisToken::At(LogPosition::from_u64(281474976710663))
    );
}

#[test]
fn params_reject_nested_json_but_decode_tagged_bytes() {
    let ok = BTreeMap::from([("payload".into(), json!({"$bytes": "AAEC"}))]);
    assert_eq!(
        params_from_json(&ok).unwrap()["payload"],
        Value::Bytes(vec![0, 1, 2])
    );
    for value in [json!([1, 2]), json!({"nested": 1}), json!(u64::MAX)] {
        assert!(params_from_json(&BTreeMap::from([("x".into(), value)])).is_err());
    }
}

#[test]
fn arrow_batches_become_explicit_null_json_rows() {
    let batch = sample_batch_with_null();
    let response = batches_to_json(&[batch]).unwrap();
    assert_eq!(response.rows, vec![json!({"name": "Ada", "age": null}).as_object().unwrap().clone()]);
}
```

`crates/varve/tests/rows.rs` constructs the same batch, calls `varve::rows`, and asserts the collected `JsonRow` contains the explicit null. This directly pins the embedded ergonomic surface instead of relying only on the server adapter.

- [ ] **Step 2: Run and confirm RED**

Run: `rtk cargo test -p varve-server --test api`

Run: `rtk cargo test -p varve --test rows`

Expected: package/module compile failure.

- [ ] **Step 3: Create the crate and error type**

`varve-server` is a library now; Task 10 adds the binary. Dependencies are `varve`, `varve-engine`, `varve-types`, `varve-config`, `serde`, `serde_json`, `base64`, `arrow`, `arrow-json`, `thiserror`, and `tokio`. Add `[lints] workspace = true`.

`ServerError` has transparent engine/row/base64/config/registry variants plus `InvalidRequest(String)`, `Unauthorized`, `Forbidden`, `NotAcceptable(String)`, `MissingWriterAdvertisement`, `Protocol(String)`, and `Io(std::io::Error)`. The row variant is `Rows(#[from] varve::RowError)`; `varve::RowError` owns Arrow/JSON failures. Library code returns typed errors; HTTP mapping arrives in Task 9.

- [ ] **Step 4: Implement exact basis and parameter conversion**

Use `base64::engine::general_purpose::STANDARD.decode` only when an object has exactly the `$bytes` key. Use `serde_json::Number::as_i64` before `as_f64` and reject `!value.is_finite()`.

`TryFrom<BasisRequest> for BasisToken` parses with:

```rust
let packed = value
    .strip_prefix("at:")
    .ok_or_else(|| ServerError::InvalidRequest("basis string must be at:<packed-u64>".into()))?
    .parse::<u64>()
    .map_err(|error| ServerError::InvalidRequest(format!("invalid basis position: {error}")))?;
Ok(BasisToken::At(LogPosition::from_u64(packed)))
```

- [ ] **Step 5: Implement JSON row encoding with pinned arrow-json**

In `varve::rows`, build `WriterBuilder::new().with_explicit_nulls(true).build::<_, JsonArray>(&mut bytes)`, call `write_batches`, call `finish`, deserialize the resulting array into `Vec<JsonRow>`, and wrap its `IntoIter`. `batches_to_json` collects that iterator under `rows`. Empty batches return `{"rows":[]}`.

- [ ] **Step 6: Run gates**

Run: `rtk cargo test -p varve-server --test api`

Run: `rtk cargo test -p varve --test rows`

Run: `rtk cargo fmt --all --check`

Run: `rtk cargo clippy --workspace --all-targets -- -D warnings`

Expected: DTO JSON shapes and scalar rules are pinned.

- [ ] **Step 7: Commit**

```bash
rtk git add Cargo.toml Cargo.lock crates/varve crates/varve-server
rtk git commit -m "feat: define server wire contract"
```

---

### Task 8: Add protocol, authentication, and metrics interfaces with registries

**Files:**
- Modify: `Cargo.toml` (add `async-trait = "0.1"` if not already direct, `prometheus = "0.14"`, `subtle = "2.6"`)
- Modify: `crates/varve-server/Cargo.toml`
- Modify: `crates/varve-server/src/lib.rs`
- Create: `crates/varve-server/src/frontend.rs`
- Create: `crates/varve-server/src/auth.rs`
- Create: `crates/varve-server/src/metrics.rs`
- Test: `crates/varve-server/tests/auth.rs`
- Test: `crates/varve-server/tests/metrics.rs`
- Test: in-module registry tests in `crates/varve-server/src/frontend.rs`

**Interfaces:**
- Produces: `#[async_trait] pub trait ProtocolFrontend: Send + Sync { async fn serve(&self, context: FrontendContext, shutdown: Shutdown) -> Result<(), ServerError>; }`.
- Produces: `#[derive(Clone)] pub struct FrontendContext { pub db: Db, pub authenticator: Arc<dyn Authenticator>, pub metrics: Arc<dyn MetricsSink>, pub probe: ProbeReport, pub readiness: ReadinessReporter }`.
- Produces: `pub fn readiness_channel() -> (ReadinessReporter, Readiness)`; `ReadinessReporter::listening(&self, endpoint: String)` publishes exactly once through a watch channel, and `Readiness::wait(&mut self) -> Result<String, ServerError>` returns the bound endpoint or an error if serving exits first.
- Produces: cloneable `Shutdown` backed by `tokio::sync::watch::Receiver<bool>`, with `pub async fn cancelled(&mut self)`.
- Produces: `pub trait Authenticator: Send + Sync { fn authenticate(&self, bearer: Option<&str>) -> Result<Principal, AuthError>; }`, `#[derive(Clone, Debug, Eq, PartialEq)] pub struct Principal { pub subject: String }`, and `#[derive(Debug, Error, Eq, PartialEq)] pub enum AuthError { Missing, Invalid }` with messages that reveal no token material.
- Produces builtin `static` authenticator selected by `[auth] backend = "static"`; `[auth.static] tokens = [{ subject = "demo", token = "demo-secret-token" }]` must contain at least one non-empty unique token and subject.
- Produces: `pub trait MetricsSink: Send + Sync { fn observe_request(&self, method: &'static str, route: &'static str, status: u16, elapsed: Duration); fn set_progress(&self, status: &NodeStatus); fn encode(&self) -> Result<String, ServerError>; }`.
- Produces builtin `prometheus` metrics selected by `[metrics] backend = "prometheus"` with request counter (`method,route,status`), duration histogram (`method,route`), applied tx/log gauges, manifest watermark gauge, and follower health gauge.
- Produces: `pub struct ServerRegistries { pub frontend: Registry<dyn ProtocolFrontend>, pub authenticator: Registry<dyn Authenticator>, pub metrics: Registry<dyn MetricsSink> }` and `ServerRegistries::with_builtins()`. At this task the frontend registry is empty; Task 10 registers `http` behind the `http` Cargo feature.

- [ ] **Step 1: Write static-auth contract tests**

```rust
#[test]
fn static_auth_accepts_exact_tokens_and_rejects_absent_or_near_matches() {
    let auth = static_auth(&[("alice", "correct-horse-battery-staple")]);
    assert_eq!(
        auth.authenticate(Some("correct-horse-battery-staple")).unwrap().subject,
        "alice"
    );
    assert!(matches!(auth.authenticate(None), Err(AuthError::Missing)));
    assert!(matches!(
        auth.authenticate(Some("correct-horse-battery-staplef")),
        Err(AuthError::Invalid)
    ));
}

#[test]
fn static_auth_config_rejects_empty_and_duplicate_tokens() {
    assert!(build_static("tokens = []").is_err());
    assert!(build_static(
        "tokens = [{subject='a',token='same'},{subject='b',token='same'}]"
    )
    .is_err());
}
```

- [ ] **Step 2: Write isolated Prometheus registry tests**

Create one `PrometheusMetrics`, observe a query 200 and tx 421, set a known `NodeStatus`, encode, and assert the exact metric names and labels occur. Create a second instance and prove registration does not collide; never use the global Prometheus registry.

- [ ] **Step 3: Run and confirm RED**

Run: `rtk cargo test -p varve-server --test auth --test metrics`

Expected: compile failure for interfaces/factories.

- [ ] **Step 4: Implement constant-time static authentication**

Store token bytes privately. For each configured token, compare equal-length byte slices with `subtle::ConstantTimeEq::ct_eq`; do not return early on the first candidate. Aggregate the match and return the associated subject only after all entries were compared. `Debug` for the backend prints token count, never token material.

The factory reads only `cfg.child("static")`, validates the vector, and maps build failures to `RegistryError::Build { kind: "authenticator", name: "static", source }`.

- [ ] **Step 5: Implement per-instance Prometheus metrics**

Construct `prometheus::Registry::new()`, register cloned `IntCounterVec`, `HistogramVec`, and `IntGauge` collectors once, update with `with_label_values`, and encode with `TextEncoder::encode(&registry.gather(), &mut Vec<u8>)`. Convert UTF-8/Prometheus errors into `ServerError::Protocol` without panics.

- [ ] **Step 6: Implement the registries and shutdown token**

Use the established `varve_config::Registry`/`ComponentFactory` pattern. `ServerRegistries::with_builtins()` registers static auth and Prometheus metrics; `frontend` starts as `Registry::new("protocol-frontend")`. `Shutdown::channel()` returns `(ShutdownTrigger, Shutdown)` and `readiness_channel()` returns the reporter/waiter pair for the binary and process tests.

- [ ] **Step 7: Run gates**

Run: `rtk cargo test -p varve-server --test auth --test metrics`

Run: `rtk cargo test -p varve-server frontend::tests`

Run: `rtk cargo fmt --all --check`

Run: `rtk cargo clippy --workspace --all-targets -- -D warnings`

Expected: auth/metrics registries are isolated, explicit, and green.

- [ ] **Step 8: Commit**

```bash
rtk git add Cargo.toml Cargo.lock crates/varve-server
rtk git commit -m "feat: add auth and metrics registries"
```

---

### Task 9: Implement the authenticated axum v1 router and Arrow IPC streaming

**Files:**
- Modify: `Cargo.toml` (add axum/tokio-stream pins; add `tower = "0.5"` and `http-body-util = "0.1"` as workspace dev pins if needed)
- Modify: `crates/varve-server/Cargo.toml`
- Modify: `crates/varve-server/src/lib.rs`
- Create: `crates/varve-server/src/http/mod.rs`
- Create: `crates/varve-server/src/http/handlers.rs`
- Create: `crates/varve-server/src/http/encoding.rs`
- Test: `crates/varve-server/tests/http_api.rs`
- Test: `crates/varve-server/tests/arrow_stream.rs`

**Interfaces:**
- Produces: `#[derive(Clone)] pub struct HttpContext { pub frontend: FrontendContext, pub max_body_bytes: usize }`.
- Produces: `pub fn http_router(context: HttpContext) -> axum::Router`.
- Extends `ServerError` with `Http(#[from] axum::http::Error)` at the response-construction seam.
- Routes: `POST /v1/query`, `POST /v1/tx`, `GET /healthz`, `GET /metrics`, `GET /v1/status`, `POST /v1/admin/compact`, `POST /v1/admin/gc`, and `POST /v1/admin/verify`.
- `POST /v1/query`: JSON request; default JSON response; Arrow stream when `Accept` contains `application/vnd.apache.arrow.stream`; unsupported explicit media types return 406.
- `POST /v1/tx`: authenticated principal is passed as the transaction `user`; writer returns 200 `TxResponse`; query-only node returns 421 with `ErrorResponse.writer` loaded fresh from `v1/writer.json`.
- Produces in `varve-engine`: `pub async fn Db::execute_as(&self, gql: &str, params: &BTreeMap<String, Value>, user: &str) -> Result<TxReceipt, EngineError>`. `execute` and `execute_with` delegate with `user = ""`; the server passes `Principal.subject`.
- Compact/GC use the same 421 writer redirect on a node without Compactor. Verify is allowed on query nodes.
- `GET /healthz`: 200 `{ "status": "ok" }` when status has no follower error; 503 `{ "status": "degraded", "error": "follower stopped" }` for that example terminal error. It is the only unauthenticated route.
- `GET /metrics`: Prometheus text with encoder content type.
- Every response includes `X-Content-Type-Options: nosniff`; 401 includes `WWW-Authenticate: Bearer`.

- [ ] **Step 1: Write in-process HTTP route contracts**

Use `tower::ServiceExt::oneshot` with a `Db::memory()` context and static token. Pin these cases:

```rust
#[tokio::test]
async fn health_is_public_but_v1_routes_require_bearer_auth() {
    assert_eq!(request(router(), Method::GET, "/healthz", None, None).await.status(), 200);
    let response = request(router(), Method::GET, "/v1/status", None, None).await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(response.headers()[WWW_AUTHENTICATE], "Bearer");
}

#[tokio::test]
async fn tx_then_json_query_round_trips() {
    let app = router();
    let tx = authorized_json(
        app.clone(),
        "/v1/tx",
        json!({"gql":"INSERT (:Person {_id: 1, name: $name})","params":{"name":"Ada"}}),
    )
    .await;
    assert_eq!(tx.status(), StatusCode::OK);
    let query = authorized_json(
        app,
        "/v1/query",
        json!({"gql":"MATCH (p:Person) RETURN p.name AS name","basis":1}),
    )
    .await;
    assert_eq!(query.status(), StatusCode::OK);
    assert_eq!(json_body(query).await["rows"], json!([{"name":"Ada"}]));
}
```

Add exact status tests for malformed GQL/params (400), unsupported Accept (406), basis timeout (408), query-node tx with advertisement (421), missing query-node advertisement (503), and internal failure (500 without internal secrets).

- [ ] **Step 2: Write Arrow stream validity and chunking tests**

Request Arrow with enough output rows for at least two record batches. Consume `Body::into_data_stream`, assert at least the schema chunk and one data chunk arrive, concatenate them, decode with `arrow::ipc::reader::StreamReader::try_new(Cursor::new(bytes), None)`, and assert schema/row values.

- [ ] **Step 3: Run and confirm RED**

Run: `rtk cargo test -p varve-server --test http_api --test arrow_stream`

Expected: compile failure for router and HTTP modules.

- [ ] **Step 4: Implement auth/metrics middleware and error mapping**

Build a public router containing only `/healthz`, merge a protected router for all other routes, then apply `middleware::from_fn_with_state` to the protected router. The middleware extracts exactly one `Authorization: Bearer <token>` credential, authenticates it, inserts `Principal` into request extensions, times the call, records method/static route/status, and sets `nosniff`.

Map errors centrally to stable codes/statuses: `invalid_request`/400, `unauthorized`/401, `not_acceptable`/406, `basis_timeout`/408, `misdirected_request`/421, `writer_unavailable`/503, `follower_failed`/503, and `internal`/500. Error bodies never include bearer tokens, storage credentials, or Rust debug formatting.

- [ ] **Step 5: Implement handlers against engine interfaces only**

Query handler converts params, applies optional basis/timeout to the builder, and calls `Query::stream`. JSON collects batches then calls `batches_to_json`; Arrow passes the stream to `arrow_ipc_response`. Tx handler calls `db.execute_as(&request.gql, &params, &principal.subject)` and the implementation extends `Submission`/`LogRecord.user`; embedded `execute`/`execute_with` use an empty user.

Status calls `db.status` plus the startup probe from `FrontendContext`. Metrics calls `set_progress` before `encode`. Compact/GC/verify convert report fields to DTOs.

- [ ] **Step 6: Implement batch-backpressured Arrow IPC bodies**

`encoding.rs` defines a private `SharedBuffer(Arc<Mutex<Vec<u8>>>)` implementing `std::io::Write`. Create `StreamWriter::try_new(SharedBuffer::clone(), stream.schema().as_ref())`, drain the schema bytes to a bounded `mpsc::channel<Result<Bytes, std::io::Error>>(2)`, then for each asynchronously received batch call `writer.write(&batch)`, drain/send that batch's bytes, and finally call `finish` and drain/send the continuation marker. Await each channel send before requesting the next batch; this is the HTTP backpressure point.

Return:

```rust
Response::builder()
    .status(StatusCode::OK)
    .header(CONTENT_TYPE, ARROW_STREAM_CONTENT_TYPE)
    .body(Body::from_stream(ReceiverStream::new(receiver)))
    .map_err(ServerError::from)
```

If query planning fails, return an ordinary error before headers. If execution/encoding fails after headers, send an error item so the client observes a truncated/failed Arrow stream rather than a valid partial result.

- [ ] **Step 7: Enforce request size and content negotiation**

Apply `DefaultBodyLimit::max(context.max_body_bytes)` to JSON routes. Accept absent/`*/*`/`application/json` as JSON and the Arrow media type as Arrow; parse comma-separated Accept values and ignore parameters such as `q=1.0`. Do not use substring matching that accepts `application/jsonish`.

- [ ] **Step 8: Run gates**

Run: `rtk cargo test -p varve-server --test http_api --test arrow_stream -- --test-threads=1`

Run: `rtk cargo test -p varve --test durability`

Run: `rtk cargo fmt --all --check`

Run: `rtk cargo clippy --workspace --all-targets -- -D warnings`

Expected: all route, auth, status, 421, JSON, and Arrow stream contracts green; authenticated user is present in the durable log test.

- [ ] **Step 9: Commit**

```bash
rtk git add Cargo.toml Cargo.lock crates/varve-engine crates/varve-server
rtk git commit -m "feat: serve authenticated HTTP and Arrow streams"
```

---

### Task 10: Register the HTTP frontend, add rustls, and ship `varved`

**Files:**
- Modify: `Cargo.toml` (add `axum-server`, `url`, `tracing`, `tracing-subscriber`; extend tokio features)
- Modify: `crates/varve-server/Cargo.toml` (features `default = ["http", "tls"]`, `http`, `tls`; `[[bin]] name = "varved" required-features = ["http"]`)
- Modify: `crates/varve-server/src/frontend.rs`
- Modify: `crates/varve-server/src/http/mod.rs`
- Create: `crates/varve-server/src/bin/varved.rs`
- Create: `crates/varve-server/tests/fixtures/tls-cert.pem`
- Create: `crates/varve-server/tests/fixtures/tls-key.pem`
- Test: `crates/varve-server/tests/frontend.rs`
- Test: `crates/varve-server/tests/tls.rs`

**Interfaces:**
- Produces builtin `http` `ComponentFactory<dyn ProtocolFrontend>` and registers it only when the `http` feature is compiled.
- `HttpFrontendFactory::build` requires a cloned `Db` in `BuildContext` so it can enforce the Writer/`advertised_address` invariant; missing context is `RegistryError::Build`, not a fallback profile.
- Config contract: `[server] backend = "http"`; `[server.http] listen = "0.0.0.0:8080"`, `advertised_address = "https://writer.example:8443"`, `max_body_bytes = "8MiB"`, optional `tls_cert`, optional `tls_key`.
- Exactly one TLS path is invalid. With both paths, load rustls PEM material before binding and serve HTTPS. With neither, serve HTTP. `advertised_address` is required on a Writer node and must parse as an absolute `http` or `https` URL; it is ignored on query-only nodes.
- `varved --config <PATH>` loads `Config`, builds `Db`, runs `probe_capabilities` once, builds auth/metrics/frontend through `ServerRegistries`, and serves until SIGINT/SIGTERM. Before binding, the HTTP frontend publishes `v1/writer.json` when Writer is enabled.
- Startup prints one machine-readable line to stdout after bind: `VARVED_LISTENING <socket-address>`. Process tests use it when `[server.http] listen = "127.0.0.1:0"`.
- Shutdown stops accepting requests, lets in-flight HTTP work finish for up to 10 seconds, then drops the last `Db` and exits nonzero on server failure.

- [ ] **Step 1: Write frontend config/registry tests**

Pin: builtin frontend names are `["http"]` with default features; an invalid socket address fails build; numeric `max_body_bytes` fails; query nodes may omit advertised address; writer nodes may not.

- [ ] **Step 2: Write a real rustls handshake test**

Start `HttpFrontend` on `127.0.0.1:0` with the checked-in self-signed test pair, wait for its bound-address channel, call `/healthz` with a reqwest client configured with the test cert as a root, assert HTTPS 200, trigger shutdown, and assert `serve` returns `Ok(())`.

- [ ] **Step 3: Run and confirm RED**

Run: `rtk cargo test -p varve-server --test frontend --test tls`

Expected: compile failure for frontend factory/binary/TLS serve.

- [ ] **Step 4: Implement `HttpFrontendFactory`**

Deserialize `[server.http]` into `HttpConfig`, validate the TLS pair and URL, and return `Arc<HttpFrontend>`. `HttpFrontend::serve` first calls `context.db.publish_writer` when Writer is enabled, then builds the router and binds with `axum_server::bind` or:

```rust
let tls = RustlsConfig::from_pem_file(cert, key).await?;
axum_server::bind_rustls(listen, tls)
    .handle(handle.clone())
    .serve(router.into_make_service())
    .await?;
```

Drive graceful shutdown with `axum_server::Handle::graceful_shutdown(Some(Duration::from_secs(10)))` when `Shutdown::cancelled()` resolves. Concurrently await `handle.listening()`, publish the actual socket through `context.readiness.listening(format!("{addr}"))`, and let the binary print `VARVED_LISTENING` from its `Readiness` waiter.

- [ ] **Step 5: Complete `ServerRegistries::with_builtins`**

Under `#[cfg(feature = "http")]`, register `HttpFrontendFactory`; no dummy frontend exists when the feature is off. Assert names and build a frontend from a complete `BuildContext`.

- [ ] **Step 6: Implement binary assembly and signals**

Use clap derive for `--config`, `tracing_subscriber::EnvFilter` defaulting to `info,varve=debug`, `tokio::signal::ctrl_c`, and Unix SIGTERM under `cfg(unix)`. Build raw engine registries through `Registries::with_builtins()` and server registries separately; insert the cloned `Db` into the frontend `BuildContext`. Spawn `ProtocolFrontend::serve`, await `Readiness::wait`, print the ready line, then wait for signal/server termination. Query-only HTTP frontends never publish.

- [ ] **Step 7: Run binary/TLS/no-default-feature gates**

Run: `rtk cargo test -p varve-server --test frontend --test tls -- --test-threads=1`

Run: `rtk cargo check -p varve-server --no-default-features`

Run: `rtk cargo run -p varve-server --bin varved -- --help`

Run: `rtk cargo fmt --all --check`

Run: `rtk cargo clippy --workspace --all-targets -- -D warnings`

Expected: TLS handshake green, no-default library compiles without HTTP/TLS, and help documents `--config`.

- [ ] **Step 8: Commit**

```bash
rtk git add Cargo.toml Cargo.lock crates/varve-server
rtk git commit -m "feat: ship configurable varved frontend"
```

---

### Task 11: Create the CLI client interface with embedded and HTTP adapters

**Files:**
- Modify: `Cargo.toml` (add `reqwest = "0.12"`, `url = "2"`)
- Create: `crates/varve-cli/Cargo.toml`
- Create: `crates/varve-cli/src/lib.rs`
- Create: `crates/varve-cli/src/client.rs`
- Create: `crates/varve-cli/src/embedded.rs`
- Create: `crates/varve-cli/src/remote.rs`
- Test: `crates/varve-cli/tests/client.rs`
- Test: `crates/varve-cli/tests/remote.rs`

**Interfaces:**
- Produces: `#[async_trait] pub trait CommandClient: Send + Sync` with exact methods:

```rust
async fn query(&self, request: QueryRequest) -> Result<Vec<RecordBatch>, CliError>;
async fn execute(&self, request: TxRequest) -> Result<TxResponse, CliError>;
async fn status(&self) -> Result<StatusResponse, CliError>;
async fn compact(&self) -> Result<CompactionResponse, CliError>;
async fn gc(&self) -> Result<GcResponse, CliError>;
async fn verify(&self) -> Result<VerifyResponse, CliError>;
```

- Produces: `pub struct EmbeddedClient { db: Db, probe: ProbeReport }` and `pub async fn EmbeddedClient::open(dir: &Path) -> Result<Self, CliError>` using `Db::local`.
- Produces: `pub struct RemoteClient { http: reqwest::Client, base: Url, token: String, max_response_bytes: usize }`, `pub fn RemoteClient::new(base: Url, token: String) -> Result<Self, CliError>`, and `pub fn RemoteClient::with_max_response_bytes(self, bytes: usize) -> RemoteClient`. The default cap is 256 MiB and zero is rejected.
- `varve-cli` depends on `varve-server = { path = "../varve-server", default-features = false }` for shared DTOs/converters; the CLI must not link the axum listener or rustls server adapter.
- Remote queries always request Arrow IPC and decode it with Arrow 58 `StreamReader`; JSON query responses remain a server/browser surface.
- A remote tx/admin mutation receiving 421 must parse `ErrorResponse.writer`, validate its absolute URL, replay the request once to that writer, and never follow a second 421. Queries remain on the originally selected query node.
- Adds `CliError` variants for IO/JSON/Arrow/engine/HTTP/status/API/invalid input/redirect loop. Display messages exclude bearer values and response headers.

- [ ] **Step 1: Write embedded/remote parity tests**

For embedded, execute a parameterized insert, query it, call status/verify, and assert report fields. For remote, start the in-process router from Task 9 and assert the same query batches and tx response.

- [ ] **Step 2: Write one-hop 421 routing tests**

Start one writer router and one query-only router sharing a local store. Point `RemoteClient` at the query router, call `execute`, assert it succeeds through the advertised writer, then issue `query` with the returned basis and assert the query request was served by the original query router (use separate request counters). Add a fake second 421 and assert `CliError::RedirectLoop`.

- [ ] **Step 3: Run and confirm RED**

Run: `rtk cargo test -p varve-cli --test client --test remote -- --test-threads=1`

Expected: package/interface compile failure.

- [ ] **Step 4: Implement the embedded adapter**

Convert wire params through `params_from_json`; use `db.query(request.gql).params(params)` with optional basis/timeout; collect the returned batches. Use `db.execute_as(&request.gql, &params, "cli:embedded")` for tx. Convert status/probe/admin reports with `StatusResponse::from_engine`, `TxResponse::from_receipt`, `CompactionResponse::from_report`, `GcResponse::from_report`, and `VerifyResponse::from_report`, the same constructors used by server handlers.

- [ ] **Step 5: Implement the remote adapter and Arrow decoder**

All requests attach `.bearer_auth(&self.token)` and `.error_for_status` is not used until 421/error bodies have been decoded. Query sets `Accept: application/vnd.apache.arrow.stream`; after a 2xx response, read the byte stream into a bounded growing `Vec<u8>` capped by a CLI `max_response_bytes` default of 256 MiB, then decode:

```rust
let reader = StreamReader::try_new(Cursor::new(bytes), None)?;
reader.collect::<Result<Vec<RecordBatch>, _>>().map_err(CliError::from)
```

The CLI buffers results because table/JSONL rendering needs a complete client-side result; this does not change server backpressure or the embedded streaming interface.

- [ ] **Step 6: Implement exactly-one writer reroute**

Factor one private `send_mutation<T: DeserializeOwned>(&self, path: &str, body: &impl Serialize, allow_redirect: bool)` method. On 421, require `writer`, create a temporary base URL, send once with `allow_redirect = false`, and preserve the same bearer token/body. Do not configure reqwest automatic redirects for 421.

- [ ] **Step 7: Run gates**

Run: `rtk cargo test -p varve-cli --test client --test remote -- --test-threads=1`

Run: `rtk cargo fmt --all --check`

Run: `rtk cargo clippy --workspace --all-targets -- -D warnings`

Expected: embedded/remote parity and one-hop writer routing green.

- [ ] **Step 8: Commit**

```bash
rtk git add Cargo.toml Cargo.lock crates/varve-cli
rtk git commit -m "feat: add embedded and remote CLI clients"
```

---

### Task 12: Add the `varve shell` REPL and table output

**Files:**
- Modify: `Cargo.toml` (enable Arrow `prettyprint`; add `clap = "4.6"`, `rustyline = "18"`)
- Modify: `crates/varve-cli/Cargo.toml`
- Create: `crates/varve-cli/src/output.rs`
- Create: `crates/varve-cli/src/shell.rs`
- Create: `crates/varve-cli/src/main.rs`
- Test: `crates/varve-cli/tests/shell.rs`
- Test: `crates/varve-cli/tests/cli_help.rs`

**Interfaces:**
- Produces binary `varve` from package `varve-cli`.
- Produces connection selector shared by commands: exactly one of `--dir <PATH>` or `--url <URL>`; `--token`/`VARVE_TOKEN` is required with `--url` and forbidden from debug output.
- Produces: `varve shell [connection options]`.
- Produces: `pub enum ShellEvent { Line(String), Interrupted, Eof }`, `pub trait ShellInput { fn read(&mut self, prompt: &str) -> Result<ShellEvent, CliError>; fn add_history(&mut self, line: &str) -> Result<(), CliError>; }`, and `pub async fn run_shell(client: Arc<dyn CommandClient>, input: &mut dyn ShellInput, output: &mut dyn Write) -> Result<(), CliError>`; `RustylineInput` is the production adapter, scripted tests use `VecShellInput`.
- REPL commands: `:quit`/`:exit`, `:status`, and `:help`. Other input is GQL.
- One complete parseable GQL program executes immediately. An incomplete/unparseable line is accumulated until a line ending in `;`; at that point parse errors are displayed and the buffer resets. Primary prompt is `varve> ` and continuation prompt is `cont> `.
- Query results render with Arrow 58 `pretty_format_batches`; tx output is `tx <id> @ <RFC3339-micros>`, followed by nonzero side-effect counts.
- The shell remembers the last successful `TxResponse.basis` and attaches it to every later query in that shell, giving remote read-your-writes by default.

- [ ] **Step 1: Write a scripted shell round-trip**

```rust
#[tokio::test]
async fn shell_executes_tx_then_basis_query_and_prints_table() {
    let client = Arc::new(FakeClient::new());
    let mut input = VecShellInput::new([
        "INSERT (:Person {_id: 1, name: 'Ada'});",
        "MATCH (p:Person) RETURN p.name AS name;",
        ":quit",
    ]);
    let mut output = Vec::new();

    run_shell(client.clone(), &mut input, &mut output).await.unwrap();

    let text = String::from_utf8(output).unwrap();
    assert!(text.contains("tx 1 @"));
    assert!(text.contains("Ada"));
    assert_eq!(client.query_bases(), vec![Some(BasisRequest::TxId(1))]);
}
```

Add multiline accumulation, parse-error reset, `:status`, EOF, and Ctrl-C (`ReadlineError::Interrupted`) tests. Ctrl-C clears an in-progress buffer; Ctrl-D exits cleanly.

- [ ] **Step 2: Write clap help/selector tests**

Use `Cli::try_parse_from` to assert `shell` appears and `--dir` conflicts with `--url`; assert remote without token returns a user-facing configuration error before any network call. Task 13 extends the same test to import/export/admin after those variants exist.

- [ ] **Step 3: Run and confirm RED**

Run: `rtk cargo test -p varve-cli --test shell --test cli_help`

Expected: compile failure for shell/main/output.

- [ ] **Step 4: Implement deterministic table/receipt output**

`format_batches` calls `arrow::util::pretty::pretty_format_batches`. Empty results print `(0 rows)`. Receipt output prints side-effect fields in this fixed order when nonzero: nodes created/deleted, relationships created/deleted, properties set/removed, labels added/removed.

- [ ] **Step 5: Implement statement buffering and dispatch**

Use `varve_gql::parse_program` to classify a program. Exactly one query statement routes to `CommandClient::query`; one or more mutation statements route to `execute`; mixed/empty programs print the parser/shape error and do not issue a client call. Build `QueryRequest` with the remembered basis and default timeout.

Do not infer query-vs-mutation from leading text; `USE`, whitespace, and comments make that unsafe.

- [ ] **Step 6: Implement rustyline and clap entry point**

Use `rustyline::DefaultEditor::new`, add non-command non-empty lines to history, and map Interrupted/Eof as specified. `#[tokio::main]` builds the selected client, runs the subcommand, prints typed errors to stderr, and exits 2 for CLI/input errors or 1 for runtime/server errors.

- [ ] **Step 7: Run shell/help gates**

Run: `rtk cargo test -p varve-cli --test shell --test cli_help`

Run: `rtk cargo run -p varve-cli --bin varve -- --help`

Run: `rtk cargo fmt --all --check`

Run: `rtk cargo clippy --workspace --all-targets -- -D warnings`

Expected: scripted shell round-trip/table output and CLI grammar green.

- [ ] **Step 8: Commit**

```bash
rtk git add Cargo.toml Cargo.lock crates/varve-cli
rtk git commit -m "feat: add varve shell"
```

---

### Task 13: Add JSONL import/export and administration commands

**Files:**
- Modify: `crates/varve-cli/src/lib.rs`
- Modify: `crates/varve-cli/src/main.rs`
- Create: `crates/varve-cli/src/transfer.rs`
- Create: `crates/varve-cli/src/admin.rs`
- Test: `crates/varve-cli/tests/transfer.rs`
- Test: `crates/varve-cli/tests/admin.rs`

**Interfaces:**
- Produces: `varve import [connection] --label <IDENT> [--graph <IDENT>] <FILE|->` for JSONL objects; `-` reads stdin.
- Produces: `varve export [connection] --query <GQL> [--basis <TX_ID|at:POSITION>] <FILE|->`; output is line-delimited JSON with explicit null fields; `-` writes stdout.
- Produces: `varve admin [connection] status|compact|gc|verify`.
- Produces: `pub async fn import_jsonl<R: BufRead>(client: Arc<dyn CommandClient>, input: R, label: &str, graph: Option<&str>) -> Result<ImportReport, CliError>`.
- Produces: `pub async fn export_jsonl<W: Write>(client: Arc<dyn CommandClient>, request: QueryRequest, output: W) -> Result<usize, CliError>`; the returned `usize` is the number of rows written.
- Import maps each JSON object deterministically (BTreeMap key order) to one statement `USE graph; INSERT (:Label {prop: $p0, prop2: $p1})` plus params. Without `--graph`, omit the `USE` prefix. Empty objects and non-object lines are errors with 1-based line numbers.
- Label/graph/property keys must form valid current GQL identifiers and the generated statement must pass `varve_gql::parse_program` before the client is called. No quoting or alternate legacy syntax is introduced.
- Import stops at the first failed transaction and reports committed count plus failing line. It does not claim all-file atomicity.
- Export uses arrow-json 58 `LineDelimited` with explicit nulls and writes through a buffered writer; it does not serialize `RecordBatch` debug output.
- Admin output is stable human-readable key/value text; `--json` emits the exact server DTO.

- [ ] **Step 1: Write import contract tests**

```rust
#[tokio::test]
async fn jsonl_import_uses_one_parameterized_tx_per_line() {
    let input = br#"{"_id":1,"name":"Ada"}
{"_id":2,"name":"Bob"}
"#;
    let client = Arc::new(FakeClient::new());
    let report = import_jsonl(client.clone(), Cursor::new(input), "Person", None)
        .await
        .unwrap();
    assert_eq!(report.committed, 2);
    let requests = client.tx_requests();
    assert_eq!(requests[0].gql, "INSERT (:Person {_id: $p0, name: $p1})");
    assert_eq!(requests[0].params["p0"], json!(1));
    assert_eq!(requests[0].params["p1"], json!("Ada"));
}
```

Add invalid JSON/non-object/empty object/invalid identifier/nested value tests; assert the client saw no request for the failing line.
Extend `cli_help.rs` to assert `import`, `export`, and `admin` now appear and each subcommand rejects simultaneous `--dir`/`--url`.

- [ ] **Step 2: Write export/admin tests**

Export a batch containing null/string/int/bytes and assert two valid JSON lines with explicit nulls and base64 bytes. For every admin subcommand, assert the matching client method is called once and both human and `--json` output include all report fields.

- [ ] **Step 3: Run and confirm RED**

Run: `rtk cargo test -p varve-cli --test transfer --test admin`

Expected: compile failure for transfer/admin modules.

- [ ] **Step 4: Implement deterministic JSONL import**

Read with `BufRead::read_line` so errors carry line numbers and files do not load wholly into memory. Convert each object into a `BTreeMap`, validate identifiers with `is_ascii_alphabetic|_` for the first byte and `is_ascii_alphanumeric|_` thereafter, generate `$pN` names in sorted key order, build/parse the statement, then call `execute`. Return:

```rust
pub struct ImportReport {
    pub committed: usize,
    pub last_basis: Option<u64>,
}
```

If the engine parser rejects a reserved identifier, wrap that error with the input line number; do not maintain a second reserved-word list.

- [ ] **Step 5: Implement Arrow-to-JSONL export**

Call `CommandClient::query`, then:

```rust
let mut writer = WriterBuilder::new()
    .with_explicit_nulls(true)
    .build::<_, LineDelimited>(BufWriter::new(output));
writer.write_batches(&batches.iter().collect::<Vec<_>>())?;
writer.finish()?;
```

Use the exact arrow-json 58 accepted iterator/reference form; keep the emitted line contract unchanged if the generic call needs adaptation.

- [ ] **Step 6: Implement admin dispatch and output**

Map `status`, `compact`, `gc`, and `verify` directly to the interface. Query-node compact/GC is transparently rerouted once by `RemoteClient`; embedded query-only profiles return the role error. Human output uses fixed field order and packed log positions.

- [ ] **Step 7: Run gates**

Run: `rtk cargo test -p varve-cli --test transfer --test admin`

Run: `rtk cargo run -p varve-cli --bin varve -- admin --help`

Run: `rtk cargo fmt --all --check`

Run: `rtk cargo clippy --workspace --all-targets -- -D warnings`

Expected: import/export/admin contracts green; JSONL never bypasses tx/query interfaces.

- [ ] **Step 8: Commit**

```bash
rtk git add crates/varve-cli
rtk git commit -m "feat: add CLI transfer and admin commands"
```

---

### Task 14: Prove read-your-writes, Arrow, eventual consistency, and read scale-out across processes

**Files:**
- Create: `crates/varve-server/tests/support/process_cluster.rs`
- Create: `crates/varve-server/tests/process_consistency.rs`
- Create: `crates/varve-server/tests/process_scale_out.rs`
- Modify: `crates/varve-server/Cargo.toml` dev-dependencies (`reqwest`, `tempfile`, `arrow`, `futures`, `varve-testkit`)
- Test: the two new process suites, serialized

**Interfaces:**
- Produces a test-only `ProcessCluster` that starts one `varved` Writer+Query+Compactor process and two Query-only `varved` processes against one temporary local log/store, all on `127.0.0.1:0`.
- Child readiness is only the stdout line `VARVED_LISTENING <addr>` followed by a successful `/healthz`; fixed sleeps are not readiness.
- Child stderr is captured and included on startup/test failure; `Drop` sends kill and waits so no process survives a test.
- Uses a static test bearer token and `v1/writer.json` written by the writer process.

- [ ] **Step 1: Write the three-node basis-token test**

`process_consistency.rs`:

```rust
#[tokio::test(flavor = "multi_thread")]
async fn writer_receipt_is_immediately_readable_from_both_query_processes() {
    let cluster = ProcessCluster::start().await.unwrap();
    let receipt = cluster
        .tx(cluster.writer_url(), "INSERT (:Person {_id: 1, name: 'Ada'})")
        .await
        .unwrap();

    for query_url in cluster.query_urls() {
        let rows = cluster
            .query_json(query_url, "MATCH (p:Person) RETURN p.name AS name", Some(receipt.basis))
            .await
            .unwrap();
        assert_eq!(rows, json!([{"name":"Ada"}]));
    }
}
```

Set follower polling to 200 ms so the basis request normally has to wait; do not assert that an unbased first read is stale because scheduling may already have applied it.

- [ ] **Step 2: Write eventual-consistency and 421 tests**

After another writer tx, query without basis in a bounded 5-second retry loop until both query nodes return the row. POST the tx to each query node and assert 421 plus the exact writer address advertised by the writer process.

- [ ] **Step 3: Write a Rust Arrow-stream client test**

Insert enough rows for multiple output batches, call `/v1/query` with the Arrow Accept header, consume `reqwest::Response::bytes_stream` incrementally, require non-empty data, decode the concatenated bytes with Arrow 58 `StreamReader`, and assert all rows/schema. Do not assert network chunk count because HTTP implementations may coalesce application chunks; Task 9's in-process body test pins the multi-chunk producer. This is the exit criterion's Rust client verification.

- [ ] **Step 4: Write concurrent read-scale test**

`process_scale_out.rs` starts two tasks, one per query node, continuously issuing a bounded aggregate/traversal read while a third task sends the deterministic `social_graph(200, 1_000, 42)` node statements and edge programs to the writer. Each reader attaches the most recently published atomic basis when present. Assert both readers completed at least 20 successful queries, saw monotonically nondecreasing row counts, and end at the same final count while the writer ingested.

- [ ] **Step 5: Run and confirm RED**

Run: `rtk cargo test -p varve-server --test process_consistency --test process_scale_out -- --test-threads=1`

Expected: process harness/test compile failure or missing behavioral contracts.

- [ ] **Step 6: Implement the process harness**

Use `env!("CARGO_BIN_EXE_varved")`, write three complete TOML files with shared `[log.local]`/`[storage.local]`, distinct roles, authenticated server config, and writer advertised address. Because the writer's actual port is assigned at bind time, reserve a loopback port before writing its config, release it immediately before spawn, and fail loudly if the child cannot bind. Query nodes may use port zero because they do not advertise.

Read readiness with a dedicated thread per child stdout, parse only the `VARVED_LISTENING` prefix, and use a 10-second channel timeout. Store child handles in creation order and kill in reverse order.

- [ ] **Step 7: Run process tests repeatedly and gate**

Run: `rtk cargo test -p varve-server --test process_consistency -- --test-threads=1`

Run: `rtk cargo test -p varve-server --test process_consistency -- --test-threads=1`

Run: `rtk cargo test -p varve-server --test process_scale_out -- --test-threads=1`

Run: `rtk cargo fmt --all --check`

Run: `rtk cargo clippy --workspace --all-targets -- -D warnings`

Expected: two consecutive consistency runs and the concurrent scale-out run are green without orphaned children.

- [ ] **Step 8: Commit**

```bash
rtk git add crates/varve-server
rtk git commit -m "test: prove cross-process query consistency"
```

---

### Task 15: Ship the static-ish image and Garage + writer + two-query-node Compose demo

**Files:**
- Create: `Dockerfile`
- Create: `.dockerignore`
- Create: `docker-compose.yml`
- Create: `deploy/garage.toml`
- Create: `deploy/garage-init.sh`
- Create: `deploy/varve-writer.toml`
- Create: `deploy/varve-query.toml`
- Create: `scripts/compose_demo.sh`
- Create: `crates/varve-testkit/src/bin/http_fixture.rs`
- Modify: `crates/varve-testkit/Cargo.toml` (runtime reqwest/serde_json for the demo binary)
- Modify: `justfile`
- Modify: `.github/workflows/ci.yml`
- Test: `docker compose config`, image build, and runnable Compose smoke

**Interfaces:**
- Produces one runtime image containing only `/usr/local/bin/varved`, CA certificates/runtime C library from distroless, and no Cargo/toolchain/source tree.
- Pins the existing tested Garage image `dxflrs/garage:v1.0.1` in one Compose location.
- Compose services: `garage`, one-shot `garage-init`, `writer`, `query-1`, `query-2`; host ports are writer 8080, queries 8081/8082.
- All Varve nodes share Garage bucket `varve`, object-store log backend, S3 storage backend, and credentials created by `garage-init`; writer roles are all three, query roles are only Query.
- Produces `just compose-demo`: build/up, wait health, load the reduced deterministic Slice 6 fixture over HTTP, verify basis reads/Arrow on both query nodes, pipe a shell query through `varve`, run status/verify, and always tear down volumes.
- CI adds a non-scheduled `container-image` job running `docker build`; the full Compose demo remains explicit/local because the existing backend matrix already exercises live Garage in CI.

- [ ] **Step 1: Write/validate deployment configuration before the image exists**

`deploy/garage.toml` uses the same one-node sqlite layout and fixed test-only RPC secret as `varve-testkit::backends`. `garage-init.sh` must:

1. Poll `/garage status` until it returns the sole node id.
2. Run `/garage layout assign -z dc1 -c 1G <node-id>` and `/garage layout apply --version 1` idempotently.
3. Create bucket `varve`.
4. Import the fixed demo access/secret pair under key name `varve-demo` using the Garage v1.0.1 CLI.
5. Grant read/write/owner on `varve` and write `/shared/ready`.

Keep the literal credentials labeled test/demo-only in the script/config; no production secret claim is made.

The script body is concrete and idempotent at the resource-exists cases:

```sh
#!/bin/sh
set -eu
until status=$(/garage status 2>/dev/null); do sleep 1; done
node_id=$(printf '%s\n' "$status" | awk '$1 ~ /^[0-9a-f]+/ {sub(/…$/, "", $1); print $1; exit}')
test -n "$node_id"
/garage layout assign -z dc1 -c 1G "$node_id" || true
/garage layout apply --version 1 || true
/garage bucket create varve || /garage bucket info varve >/dev/null
/garage key import --yes \
  GK000000000000000000000000 \
  0000000000000000000000000000000000000000000000000000000000000000 \
  --name varve-demo || /garage key info varve-demo >/dev/null
/garage bucket allow --read --write --owner varve --key varve-demo
touch /shared/ready
```

If the pinned Garage v1.0.1 argument order differs, adapt only this implementation command while preserving the fixed key material and the successful Compose test.

Run: `rtk docker compose config`

Expected before Dockerfile/config completion: FAIL on missing files or invalid Compose expansion.

- [ ] **Step 2: Add exact Varve node configs**

Writer config uses:

```toml
[node]
roles = ["writer", "query", "compactor"]

[log]
backend = "object-store"
group_commit_window_ms = 15
group_commit_max_bytes = "8MiB"

[storage]
backend = "s3"
[storage.s3]
endpoint = "http://garage:3900"
bucket = "varve"
region = "garage"
access_key_id = "GK000000000000000000000000"
secret_access_key = "0000000000000000000000000000000000000000000000000000000000000000"
path_style = true

[server]
backend = "http"
[server.http]
listen = "0.0.0.0:8080"
advertised_address = "http://writer:8080"
max_body_bytes = "8MiB"

[auth]
backend = "static"
[auth.static]
tokens = [{ subject = "compose-demo", token = "varve-demo-token" }]

[metrics]
backend = "prometheus"
```

Query config is identical except `roles = ["query"]` and omits `advertised_address`. Each query process has its own in-memory query cache; no disk cache directory is shared between stores/processes.

- [ ] **Step 3: Add the multi-stage image**

Builder: `rust:1.93-bookworm`, copy manifests/source, `cargo build --locked --release -p varve-server --bin varved`. Runtime: `gcr.io/distroless/cc-debian12:nonroot`, copy the binary, `USER nonroot`, expose 8080, and use entrypoint `[/usr/local/bin/varved]`. Compose supplies `--config /etc/varve/varve.toml`.

Run: `rtk docker build -t varve:slice9 .`

Expected: one release `varved` image builds with the committed lockfile.

- [ ] **Step 4: Add the HTTP fixture driver**

`http_fixture` takes `--writer`, repeated `--query`, and `--token`; generates `social_graph(200, 1_000, 42)`; posts `node_statements(100)` and `edge_programs(100)` to the writer; retains the final basis; queries the two-hop and `{1,3}` forms from every query URL with that basis; requests Arrow from at least one query URL and decodes it. Any mismatch exits nonzero.

- [ ] **Step 5: Add the teardown-safe demo script/recipe**

`scripts/compose_demo.sh` uses `set -eu`, installs an EXIT trap `rtk docker compose down -v --remove-orphans`, starts with `rtk docker compose up -d --build`, polls all three `/healthz` endpoints, runs `rtk cargo run -p varve-testkit --bin http_fixture -- --writer http://127.0.0.1:8080 --query http://127.0.0.1:8081 --query http://127.0.0.1:8082 --token varve-demo-token`, pipes `MATCH (p:Person) RETURN p.name LIMIT 3;` and `:quit` to `rtk cargo run -p varve-cli --bin varve -- shell --url http://127.0.0.1:8081 --token varve-demo-token`, then runs remote `admin status` and `admin verify`.

The `just compose-demo` recipe invokes this script through `rtk proxy sh scripts/compose_demo.sh` so the repository RTK rule remains visible.

- [ ] **Step 6: Run the real Compose exit demo**

Run: `rtk docker compose config`

Run: `rtk proxy sh scripts/compose_demo.sh`

Expected: Garage initializes, writer and two query nodes become healthy, fixture queries and Arrow decode pass on both query nodes, shell prints a table, verify succeeds, and teardown removes the stack/volumes.

- [ ] **Step 7: Add and run image CI/local gates**

Add `container-image` job with checkout + `docker build --tag varve-ci .`. Then run:

Run: `rtk cargo test -p varve-testkit --bin http_fixture`

Run: `rtk cargo fmt --all --check`

Run: `rtk cargo clippy --workspace --all-targets -- -D warnings`

Expected: Rust/demo helper and image definition green.

- [ ] **Step 8: Commit**

```bash
rtk git add Dockerfile .dockerignore docker-compose.yml deploy scripts/compose_demo.sh crates/varve-testkit justfile .github/workflows/ci.yml
rtk git commit -m "feat: add Garage scale-out Compose demo"
```

---

### Task 16: Whole-slice verification and documentation closeout

**Files:**
- Modify: `docs/plans/STATUS.md`
- Modify: `docs/plans/varve-v1-roadmap.md`
- Modify: `README.md` (server/CLI quick start and links to Compose demo)
- Modify: crate rustdoc touched by final verification only when a warning identifies an omission
- Test: all workspace/server/process/Compose gates below

**Interfaces:**
- No new runtime interface. This task proves the Slice 9 interfaces and records their final dependency versions, commands, behavior, and any test-contract implementation adaptations.
- `STATUS.md` becomes the next-session source of truth: Slice 9 COMPLETE, Slice 10 planning next, final test counts, demo command, dependency pins, query role/basis semantics, HTTP routes/media/auth/TLS, CLI commands, Compose topology, and deviations.
- Every unchecked Slice 9 roadmap task box is changed to `[x]`; exit-criterion prose remains intact.

- [ ] **Step 1: Run formatting and lint gates**

Run: `rtk cargo fmt --all --check`

Run: `rtk cargo clippy --workspace --all-targets -- -D warnings`

Expected: zero warnings/errors.

- [ ] **Step 2: Run the serial whole workspace and focused role/server suites**

Run: `rtk cargo test --workspace -- --test-threads=1`

Run: `rtk cargo test -p varve-server --test process_consistency --test process_scale_out -- --test-threads=1`

Run: `rtk cargo test -p varve-cli -- --test-threads=1`

Expected: all green; record exact passed test/suite counts in `STATUS.md`.

- [ ] **Step 3: Verify dependency unification and feature surfaces**

Run: `rtk cargo tree -p datafusion | rtk proxy rg ' arrow v|object_store v'`

Run: `rtk cargo tree -d | rtk proxy rg '^(arrow|object_store|reqwest) v'`

Run: `rtk cargo check -p varve-server --no-default-features`

Expected: exactly one Arrow 58 and object_store 0.13; reqwest remains 0.12 for Varve direct use; server core library compiles without HTTP/TLS.

- [ ] **Step 4: Run HTTP/Arrow/TLS/CLI demonstrations**

Run: `rtk cargo test -p varve-server --test arrow_stream --test tls -- --test-threads=1`

Run: `rtk cargo run -p varve-server --bin varved -- --help`

Run: `rtk cargo run -p varve-cli --bin varve -- --help`

Expected: Arrow client and TLS handshake pass; both binaries document their complete surfaces.

- [ ] **Step 5: Run the Compose scale-out exit demo**

Run: `rtk proxy sh scripts/compose_demo.sh`

Expected: reduced Slice 6 fixture loads over HTTP; two query nodes answer basis reads; Arrow stream decodes; CLI shell round-trips; concurrent/read scale proof remains covered by Task 14; status and verify succeed.

- [ ] **Step 6: Update README, STATUS, and roadmap**

README includes embedded vs remote shell examples, tx/query curl examples with bearer auth and basis, Arrow Accept example, TLS config, JSONL import/export, admin commands, and `just compose-demo`.

In `STATUS.md`:

- Set current entry point to Slice 10 planning and Slice 9 COMPLETE.
- Record the final demo command `just compose-demo`.
- Record actual resolved versions for every dependency added in this slice and any implementation-only API adaptation made while keeping tests unchanged.
- Record the exact query follower batch/poll/basis defaults, the `v1/writer.json` non-coordination contract, route/auth matrix, and JSON/Arrow media types.
- Close the Slice 1 `Db` clone/debug open item and Slice 3 bounded `read_range_sync` early-return item.
- Add any genuinely deferred fast-follow with a concrete risk/owner slice; do not hide a failed exit criterion as a fast-follow.
- Update the Slice log row with final test counts and the 1-writer/2-query-node result.

Tick all six Slice 9 roadmap task boxes.

- [ ] **Step 7: Re-run doc-sensitive final gate**

Run: `rtk cargo fmt --all --check`

Run: `rtk cargo clippy --workspace --all-targets -- -D warnings`

Run: `rtk cargo test --workspace -- --test-threads=1`

Run: `rtk git diff --check`

Expected: green; only intentional Slice 9 code/deployment/docs changes remain.

- [ ] **Step 8: Commit closeout**

```bash
rtk git add README.md docs/plans/STATUS.md docs/plans/varve-v1-roadmap.md
rtk git commit -m "docs: close Slice 9 server CLI and query nodes"
```

---

## Slice Exit Checklist

- [ ] Query-only `Db` opens from the latest manifest watermark, consumes bounded log ranges, and applies decoded resolved effects without any GQL re-execution.
- [ ] `Db` is a cloneable role-aware handle; Writer/Query/Compactor methods are explicitly gated and query follower shutdown/terminal errors are observable.
- [ ] Basis by tx id and `at:<packed-log-position>` waits until applied, times out at the configured bound, and makes a writer receipt immediately readable from both query processes.
- [ ] Streaming query tests prove DataFusion `RecordBatchStream` collection equivalence and valid chunked Arrow IPC over HTTP from a Rust client.
- [ ] The `varve` facade exposes the serde-friendly `rows(&[RecordBatch]) -> Result<RowIter, RowError>` iterator and HTTP JSON uses that single conversion path.
- [ ] `varved` serves authenticated `POST /v1/query`, `POST /v1/tx`, `GET /healthz`, `GET /metrics`, `GET /v1/status`, and admin compact/gc/verify; JSON/Arrow negotiation, 421 writer routing, body limits, error mapping, and rustls are tested.
- [ ] `ProtocolFrontend`, `Authenticator`, and `MetricsSink` are interfaces backed by explicit registries; builtin names are `http`, `static`, and `prometheus`.
- [ ] `varve shell` round-trips against embedded and remote nodes with table output and automatic last-tx basis.
- [ ] `varve import`/`export` use JSONL through normal tx/query paths; `varve admin status|compact|gc|verify` works embedded and remote.
- [ ] Multi-stage/distroless image builds; Compose runs pinned Garage + one writer + two query nodes and advertises the writer through `v1/writer.json`.
- [ ] The reduced deterministic Slice 6 fixture runs end-to-end over HTTP; both query nodes serve concurrent reads while the writer ingests, and converge without a basis.
- [ ] Human byte sizes are live for group commit and cache limits; bounded local/object log reads stop at their upper bound.
- [ ] `rtk cargo fmt --all --check`, workspace clippy with warnings denied, serial workspace tests, focused process tests, TLS/Arrow tests, dependency-unification checks, image build, and `just compose-demo` are green.
- [ ] `README.md` documents server/CLI/Compose use.
- [ ] `docs/plans/STATUS.md` records Slice 9 complete, demo/test facts, closed deferred items, dependency pins/adaptations, and Slice 10 as the next entry point.
- [ ] All Slice 9 boxes in `docs/plans/varve-v1-roadmap.md` are ticked and the closeout is committed.
