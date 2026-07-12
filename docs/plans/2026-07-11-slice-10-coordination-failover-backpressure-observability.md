# Slice 10 — Coordination, Failover, Backpressure, Observability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship spec §12 — a `Coordinator` trait with `designated-writer` (heartbeat guard) and `cas-failover` (lease + epoch fencing) builtins, proven failover with zero acked-tx loss and provably-ignored zombie appends, config-driven writer backpressure (429/wait + live-index memory watermark), and complete observability (tracing spans, spec-§12 Prometheus metrics, OTLP export behind `MetricsSink`).

**Architecture:** Coordination state lives in three plain-JSON objects in the shared store: `v1/writer.json` (advertisement + heartbeat, plain PUT, both modes), `v1/lease.json` (CAS-managed, cas mode only), and immutable `v1/epochs/<epoch-4hex>.json` fence objects. A takeover increments the 16-bit epoch in `LogPosition` and writes a fence recording where the dead epoch ends; every log reader (recovery, follower, verify) filters fenced records and jumps its cursor across epoch boundaries. The writer loop gains a lease-validity gate before every ack and becomes fatal on post-durability apply failure. Backpressure and metrics are engine-side atomics surfaced through the existing `MetricsSink` seam.

**Tech Stack:** Existing pins only — tokio 1, object_store 0.13.2, prometheus 0.14, tracing 0.1.44, serde_json 1.0.150, reqwest 0.12.28 (server `otel` feature), axum 0.8.9 (dev/test). **No new external dependencies.** OTLP export is a hand-rolled OTLP/HTTP JSON encoder over `prometheus::Registry::gather()` — no OpenTelemetry SDK crates.

## Global Constraints

(From `docs/plans/varve-v1-roadmap.md` Global Constraints — every task below implicitly includes these.)

- **TDD, no exceptions:** failing test first, minimal implementation, refactor, commit (superpowers:test-driven-development).
- **Interfaces + registry + composition (spec §4):** `Coordinator` is a trait in `varve-engine`; implementations register in the explicit `Registry`; TOML selects by name; engine code never depends on a concrete backend.
- **Sovereignty (spec §1, D7):** nothing may *require* more than plain S3 PUT/GET/LIST. `cas-failover` is opt-in, capability-probed at startup, and refuses with a clear error naming the backend capability when the probe fails. `designated-writer` (the default) uses plain PUTs only.
- **Bitemporal invariant (spec §5.2):** untouched by this slice — storage stays append-only events.
- **Determinism:** effect-replay stays a deterministic function of the log + fence set. Fence filtering is pure. The only randomness introduced (node-id / probe nonce) never enters replayed or compacted output.
- Workspace lints: `cargo clippy --workspace --all-targets -- -D warnings`; `unwrap()`/`expect()` forbidden in library code (allowed in tests, per repo `clippy.toml`).
- Timestamps are always `Timestamp(µs, UTC)`; IIDs are always `xxh3_128(graph, table, _id)`.
- Commit style: `feat:`/`fix:`/`test:`/`refactor:`/`docs:` prefixes. **Do NOT add a `Co-Authored-By` / co-author trailer.**
- Slice ends with: all workspace tests green, clippy clean, `cargo fmt --all --check` clean, STATUS.md updated, roadmap checkboxes ticked, runnable demo command recorded.
- **We are in development. No backward compatibility anywhere — production code only.** Existing wire/JSON shapes may be extended or changed; update every consumer and test in the same task.
- **Test code is the contract.** Implementation sketches below were verified against the pinned versions where possible, but if a sketch does not compile against tokio 1.x / object_store 0.13.2 / prometheus 0.14 / axum 0.8.9 as pinned in `Cargo.lock`, adapt the implementation — never weaken a test's assertion to fit an API.
- Per-task gate (slice-5 process note): `cargo fmt --all` + `cargo clippy --workspace --all-targets -- -D warnings` + the task's named tests, minimum. Whole-workspace test run at least at Tasks 9 and 16.

## Design decisions locked by this plan

1. **Coordination objects.** `v1/writer.json` — the discovery/advertisement document (extended with `node_id`, `epoch`, `heartbeat_us`), plain canonical-JSON PUT, written by the writer's coordinator; query nodes keep reading it to answer 421. `v1/lease.json` — the CAS lease (cas mode only), managed exclusively through `put_if_absent`/`put_if_matches`. `v1/epochs/<epoch-4hex>.json` — one immutable fence per DEAD epoch, plain PUT, written by the seizing writer *before* its first append.

2. **Epoch fencing model.** `LogPosition` is already epoch-major (`epoch u16 | offset u48`), and object-store log keys are `v1/log/<epoch-4hex>/<offset-lexhex>.vlog`. On takeover the winner (a) seizes the lease with `put_if_matches`, bumping `epoch` to `old + 1`; (b) scans the log head (position after the last durable record); (c) PUTs the fence for the old epoch at that head offset; (d) recovers (fence-filtered) and calls `Log::start_epoch(new_epoch)` so its first append lands at `(new_epoch, 0)`. A record at `(e, o)` is **dead** iff a fence exists for epoch `e` with `fence_offset <= o`. *Why zombies can never overwrite a live object:* a zombie's cached next-position is `>=` the head the seizer observed (only one grant exists per epoch, so nobody else appends into the zombie's epoch), so every zombie PUT lands at a fresh key at-or-after the fence — dead by construction, and never a key collision with live data.

3. **Acked-tx safety across takeover.** A record that landed *before* the seizer's head scan is included as live — if the old writer acked it, it survives (zero acked-tx loss); if unacked it MAY surface, which is the standard WAL contract. A record landing *after* the scan is dead — and the lease-deadline ack gate (decision 4) guarantees the old writer never acked it: the writer may only ack while `local_now < last_successful_heartbeat_start + takeover_after`, and a seizer must observe the *same lease ETag for a full local `takeover_after` window* before seizing. Staleness detection is therefore **clock-skew-free** (double-observation of the ETag on the standby's own clock); only the best-effort designated-writer guard compares wall-clock timestamps, and it is documented as best-effort.

4. **Writer fatal states (closes slice-3 T9).** The writer loop checks lease validity before appending and again before acking; a lost/expired lease acks `EngineError::WriterFenced` and stops the writer (drains subsequent submissions with the same error, publishes the reason on the progress watch so `/healthz` degrades). Post-durability apply failure — previously ack-`CommitFailed`-and-continue — is now also fatal, because epoch takeover weakens the global-monotonicity argument that made that path provably unreachable.

5. **Guard & heartbeat activation.** `Db::open_with` builds a coordinator for writer-role nodes always (default `designated-writer`, `[coordinator] heartbeat_interval_ms = 5000`, `takeover_after_ms = 15000`; `heartbeat_interval_ms = 0` disables the heartbeat task). The heartbeat task **idles until the first `publish_writer` call** sets an address — embedded writers that never advertise produce no `writer.json` and are invisible to the guard, exactly as in slice 9, so restart-heavy tests (crash matrix, benches) are unaffected. `Db::memory()` / `Db::local()` build **no** coordinator (`None`); their `publish_writer` falls back to a plain PUT of the extended advertisement. The designated-writer guard refuses startup only when a fresh (`heartbeat_us` within `takeover_after`) advertisement from a **different `node_id`** exists — best-effort, documented as such. Nobody deletes `writer.json` on graceful shutdown; the guard clears when the heartbeat goes stale.

6. **Standby = blocking acquire.** A cas-failover writer node blocks inside `Db::open_with` until it wins the lease. No dynamic role promotion in v1 — a standby process serves nothing until it becomes primary ("reads never stop" is served by separate query nodes). Takeover latency is bounded by `[takeover_after, 2×takeover_after]` plus recovery; tests/demo tune the intervals well under the 10 s exit criterion.

7. **cas-failover requires the shared log.** `[coordinator] backend = "cas-failover"` with any `[log]` backend other than `"object-store"` is a hard config error (name-based guard, same precedent as `VolatileBlockStore`). `LocalLog::start_epoch` returns `LogError::EpochUnsupported("local")`.

8. **Backpressure.** `[node] submission_queue_len` (default 256, must be > 0) sizes the writer's mpsc queue. Embedded `execute*` keeps the *wait* semantics (`send().await`). New `Db::try_execute_as` uses `try_send` and returns `EngineError::Backpressure` on a full queue; the HTTP `/v1/tx` handler uses it and maps `Backpressure` → **429** with `Retry-After: 1`. `[storage] max_live_bytes` (IEC `ByteSize`, default `"512MiB"`) adds a live-index memory watermark that forces an early block flush alongside the existing `max_block_rows` trigger.

9. **Lag metric.** `ProgressState`/`NodeStatus` gain `log_head: LogPosition` — the best-known log head (writer: its durable watermark; follower: max of last-read position + 1 and the manifest watermark). Prometheus exports `varve_log_head_position` and `varve_log_lag_records` (same-epoch offset difference; when epochs differ the transient approximation `head.offset() + 1` is reported and documented). A slow follower never affects the writer — structural (separate task/process), asserted by the lag test.

10. **I/O-free scrape rule (the slice-9 `/healthz` lesson).** Every NEW metric comes from atomics or one in-memory read-lock pass (`Db::metrics()` does no object-store I/O). `compaction_debt_tries` is `Σ_scope max(0, trie_count(scope) − 1)` over the in-memory inventory — a documented I/O-free approximation of pending compaction work.

11. **OTLP export = hand-rolled OTLP/HTTP JSON.** The `otlp` `MetricsSink` builtin wraps `PrometheusMetrics` for recording/encoding and pushes `prometheus::Registry::gather()` converted to OTLP JSON (`resourceMetrics`) to `[metrics.otlp] endpoint` every `push_interval_ms` (default 10000) via reqwest. No OpenTelemetry SDK dependency (dep-budget + unverifiable API pins). Behind varve-server feature `otel` (default ON; `varve-cli` uses `default-features = false` and is unaffected; the core lib must still build with `--no-default-features`).

12. **Chaos harness.** Env-gated (`VARVE_CHAOS_SECS`, skip silently when unset — the `VARVE_S3_BACKENDS` precedent): a child writer process (testkit bin) inserts continuously and prints `ACKED <n>` per ack; the parent `kill -9`s it at pseudo-random intervals and restarts; at the end it reopens and proves every acked id is present and `verify()` is clean. `just chaos` = 60 s locally; CI nightly = 1800 s (the roadmap's 30 min).

13. **Probe-key nonce (closes the slice-5 open item).** `Db::probe_capabilities` keys become `v1/probe/<micros>-<nonce>` where the nonce is process-unique, so a wall-clock regression can never collide with a previously-probed key. The same generator supplies coordinator `node_id`s.

## File structure

```
crates/varve-types/src/position.rs        (unchanged — epoch accessors already exist)
crates/varve-log/src/log.rs               Log trait: + head(), + start_epoch(); LogError: + EpochUnsupported, + EpochRegression
crates/varve-log/src/{memory,local,object_store}.rs   backend impls of head/start_epoch
crates/varve-storage/src/keys.rs          + EPOCH_FENCE_PREFIX, epoch_fence_key, parse_epoch_fence_key
crates/varve-storage/src/store.rs         ConditionalStore: + get_versioned; StorageError: + NoEtag
crates/varve-storage/src/cache.rs         + CacheStats; CachedStore instrumented
crates/varve-storage/src/disk.rs          TOCTOU fix in open()
crates/varve-engine/src/coord/mod.rs      NEW: Coordinator trait, WriterGrant, LeaseState, node-id/nonce gen, heartbeat task
crates/varve-engine/src/coord/fence.rs    NEW: FenceMap, FenceDoc, load_fences, write_fence
crates/varve-engine/src/coord/designated.rs  NEW: DesignatedWriter + factory
crates/varve-engine/src/coord/cas.rs      NEW: CasFailover + factory (feature "cas-failover", default on)
crates/varve-engine/src/metrics.rs        NEW: EngineMetrics atomics + EngineMetricsSnapshot + CacheTierStats
crates/varve-engine/src/db.rs             open_with wiring, publish_writer, try_execute_as, Db::metrics, EngineError variants
crates/varve-engine/src/writer.rs         lease ack-gate, fatal states, queue len, live-bytes trigger, metrics, spans
crates/varve-engine/src/follower.rs       fence-aware apply + epoch jump + log_head progress
crates/varve-engine/src/replay.rs / db.rs recover()  fence-filtered replay
crates/varve-engine/src/verify.rs         fence-aware log walk
crates/varve-engine/src/node.rs           NodeTuning + submission_queue_len; ProgressState/NodeStatus + log_head
crates/varve-engine/src/registries.rs     + coordinator registry
crates/varve-index/src/{event,live}.rs    Event::approx_bytes, LiveTable::approx_bytes
crates/varve-server/src/metrics.rs        MetricsSink + set_engine; Prometheus completion
crates/varve-server/src/metrics/otlp.rs   NEW: OTLP JSON converter + pusher (feature "otel")
crates/varve-server/src/http/handlers.rs  tx → try_execute_as + 429; metrics → set_engine; status + log_head
crates/varve-server/src/api.rs            StatusResponse + log_head_position; advertisement fields
crates/varve-cli/src/remote.rs            drop dead allow_redirect param; 429 surfacing
crates/varve/src/lib.rs                   re-exports (Coordinator, WriterGrant, EngineMetricsSnapshot, …)
crates/varve/examples/failover.rs         NEW: slice demo
crates/varve-testkit/src/bin/chaos_writer.rs  NEW
crates/varve-testkit/tests/{failover_backends,chaos}.rs  NEW (env-gated)
docs/ops/metrics.md                       NEW: Grafana-ready metric reference
justfile / .github/workflows/ci.yml       chaos targets
```

Task order: 1–4 build the epoch/fence substrate bottom-up; 5–8 build coordination onto it; 9 proves failover end-to-end; 10–11 backpressure; 12–14 observability; 15 chaos; 16 closeout. Tasks 1–4 land independently testable pieces (each is meaningful without the coordinator), so review gates hold between them.

---

### Task 1: Process-unique identity + probe-key nonce

Closes the slice-5 open item "probe-key entropy before slice-10 cas-failover trusts the verdict", and provides the `node_id` generator Tasks 5–7 consume.

**Files:**
- Create: `crates/varve-engine/src/coord/mod.rs` (module skeleton: just `identity` for now)
- Modify: `crates/varve-engine/src/lib.rs` (add `mod coord;` — check the actual module list in `lib.rs`; it is 24 lines)
- Modify: `crates/varve-engine/src/db.rs` (`probe_capabilities`)
- Test: unit tests inside `coord/mod.rs`; existing `probe` tests stay green

**Interfaces:**
- Consumes: `xxhash_rust::xxh3::xxh3_128` (already a workspace dep — add `xxhash-rust = { workspace = true }` to `varve-engine/Cargo.toml` if not present).
- Produces: `pub(crate) fn generate_node_id() -> String` — 32 lower-hex chars, unique per call within and across processes. Consumed by Tasks 5, 6, 7 and by `probe_capabilities`.

- [x] **Step 1: Write the failing test**

```rust
// crates/varve-engine/src/coord/mod.rs
//! Coordination (spec §12): writer identity, fences, and the Coordinator
//! trait land across slice-10 tasks 1–7.

pub(crate) mod identity {
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    /// Process-unique 128-bit id as 32 lower-hex chars: xxh3_128 over
    /// (pid, unix-nanos, per-process counter). Not cryptographic — just
    /// collision-resistant enough that two writer instances (or two probe
    /// runs after a wall-clock regression) never share an id.
    pub(crate) fn generate_node_id() -> String {
        let pid = std::process::id() as u128;
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let count = COUNTER.fetch_add(1, Ordering::Relaxed) as u128;
        let mut buf = [0u8; 48];
        buf[..16].copy_from_slice(&pid.to_le_bytes());
        buf[16..32].copy_from_slice(&nanos.to_le_bytes());
        buf[32..].copy_from_slice(&count.to_le_bytes());
        format!("{:032x}", xxhash_rust::xxh3::xxh3_128(&buf))
    }
}

#[cfg(test)]
mod tests {
    use super::identity::generate_node_id;

    #[test]
    fn node_ids_are_32_hex_and_unique_per_call() {
        let a = generate_node_id();
        let b = generate_node_id();
        assert_eq!(a.len(), 32);
        assert!(a.chars().all(|c| c.is_ascii_hexdigit()));
        assert_ne!(a, b, "same-process calls must differ (counter)");
    }
}
```

Write ONLY the test first (module with an unimplemented body or missing fn), run to see it fail, then fill the implementation above.

- [x] **Step 2: Run test to verify it fails**

Run: `cargo test -p varve-engine coord::tests::node_ids_are_32_hex_and_unique_per_call`
Expected: FAIL (unresolved module / unimplemented).

- [x] **Step 3: Implement (code above), then make `probe_capabilities` nonce-keyed**

In `db.rs`, change the probe key (test first — add to the existing probe test file or `db.rs` tests):

```rust
#[tokio::test]
async fn probe_keys_are_unique_even_at_the_same_clock_tick() {
    // Two probes never collide on key even if the clock regressed:
    // the key carries a process-unique nonce.
    let db = Db::memory();
    let a = db.probe_capabilities().await.unwrap();
    let b = db.probe_capabilities().await.unwrap();
    assert_ne!(a.probe_key, b.probe_key);
    assert!(a.probe_key.starts_with("v1/probe/"));
    assert!(a.probe_key.contains('-'), "key must carry the nonce suffix");
}
```

Implementation in `Db::probe_capabilities` (db.rs:1475):

```rust
let key = format!(
    "{}/{}-{}",
    varve_storage::PROBE_PREFIX,
    self.inner.clock.next().as_micros(),
    crate::coord::identity::generate_node_id()
);
```

- [x] **Step 4: Run tests**

Run: `cargo test -p varve-engine coord && cargo test -p varve-engine probe`
Expected: PASS. Also `cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings`.

- [x] **Step 5: Commit**

```bash
git add crates/varve-engine
git commit -m "feat: process-unique node ids and nonce-keyed capability probes"
```

---

### Task 2: `Log::head` and `Log::start_epoch`

The log learns to report the position after its last durable record and to reposition its next append at a new epoch's origin. Needed by the cas coordinator (fence offset) and the takeover writer (first append at `(E+1, 0)`).

**Files:**
- Modify: `crates/varve-log/src/log.rs` (trait + `LogError` variants)
- Modify: `crates/varve-log/src/memory.rs`, `crates/varve-log/src/local.rs`, `crates/varve-log/src/object_store.rs`
- Modify: test doubles implementing `Log` gain the two methods: `crates/varve-engine/src/follower.rs` (`ScriptedLog`, `PendingLog`), `crates/varve-engine/src/writer.rs` (`CountingLog`, `BlockingAppendLog`), and any `impl Log for` in `varve-engine/tests/` / `varve-testkit` (grep `impl Log for` across the workspace and stub each: `head` → delegate or `unreachable!("test double")`, `start_epoch` → same).
- Test: unit tests in each backend file.

**Interfaces:**
- Consumes: `LogPosition::{epoch, offset, new}` (varve-types, exists).
- Produces (Tasks 5–8 rely on these exact signatures):

```rust
// log.rs — additions to trait Log
/// Position the NEXT appended record will receive (== the exclusive end of
/// the durable prefix). For the object-store log the first call scans the
/// store and primes the internal cursor; later calls return the cached value.
async fn head(&self) -> Result<LogPosition, LogError>;
/// Repositions the next append at `(epoch, 0)`. `epoch` must be at or above
/// the current head's epoch; moving INTO an epoch that already holds records
/// is `EpochRegression`. Idempotent when the head is already `(epoch, 0)`.
async fn start_epoch(&self, epoch: u16) -> Result<(), LogError>;
```

```rust
// LogError additions
#[error("log backend '{0}' does not support epochs; cas-failover requires the shared object-store log")]
EpochUnsupported(&'static str),
#[error("cannot start epoch {requested}: log head is already at epoch {head}")]
EpochRegression { requested: u16, head: u16 },
```

- [x] **Step 1: Write the failing tests**

```rust
// memory.rs tests
#[tokio::test]
async fn head_and_start_epoch_reposition_appends() {
    let log = MemoryLog::new();
    assert_eq!(log.head().await.unwrap(), LogPosition::ZERO);
    log.append(vec![record(1), record(2)]).await.unwrap();
    assert_eq!(log.head().await.unwrap(), LogPosition::new(0, 2).unwrap());

    log.start_epoch(1).await.unwrap();
    assert_eq!(log.head().await.unwrap(), LogPosition::new(1, 0).unwrap());
    let first = log.append(vec![record(3)]).await.unwrap();
    assert_eq!(first, LogPosition::new(1, 0).unwrap());

    // regression: back into an occupied epoch
    assert!(matches!(
        log.start_epoch(0).await,
        Err(LogError::EpochRegression { requested: 0, head: 1 })
    ));
    // idempotent at an empty epoch origin is allowed
    log.start_epoch(2).await.unwrap();
    log.start_epoch(2).await.unwrap();
}
```

```rust
// object_store.rs tests (module has `record(tx_id)` helper already)
#[tokio::test]
async fn head_scans_once_and_start_epoch_moves_the_next_append() {
    let store = memory_store();
    let log = ObjectStoreLog::new(Arc::clone(&store));
    log.append(vec![record(1), record(2)]).await.unwrap();

    // A SECOND instance over the same store scans the durable head.
    let other = ObjectStoreLog::new(Arc::clone(&store));
    assert_eq!(other.head().await.unwrap(), LogPosition::new(0, 2).unwrap());

    other.start_epoch(1).await.unwrap();
    let first = other.append(vec![record(3)]).await.unwrap();
    assert_eq!(first, LogPosition::new(1, 0).unwrap());
    // key landed under the new epoch directory
    let keys = store.list("v1/log/0001").await.unwrap();
    assert_eq!(keys.len(), 1);
}

#[tokio::test]
async fn zombie_primer_head_caches_the_stale_cursor() {
    // The failover test (Task 9) relies on this: a handle whose head() was
    // taken BEFORE a takeover keeps appending at its stale cached position.
    let store = memory_store();
    let log = ObjectStoreLog::new(Arc::clone(&store));
    log.append(vec![record(1)]).await.unwrap();

    let zombie = ObjectStoreLog::new(Arc::clone(&store));
    assert_eq!(zombie.head().await.unwrap(), LogPosition::new(0, 1).unwrap());

    // Someone else moves on to epoch 1...
    let successor = ObjectStoreLog::new(Arc::clone(&store));
    successor.start_epoch(1).await.unwrap();
    successor.append(vec![record(10)]).await.unwrap();

    // ...but the zombie still writes at its cached (0, 1).
    let pos = zombie.append(vec![record(99)]).await.unwrap();
    assert_eq!(pos, LogPosition::new(0, 1).unwrap());
}
```

```rust
// local.rs tests
#[tokio::test]
async fn local_log_reports_head_and_refuses_epochs() {
    let dir = tempfile::tempdir().unwrap();
    let log = LocalLog::open(dir.path(), DEFAULT_SEGMENT_MAX_BYTES).unwrap();
    assert_eq!(log.head().await.unwrap(), LogPosition::ZERO);
    assert!(matches!(
        log.start_epoch(1).await,
        Err(LogError::EpochUnsupported("local"))
    ));
}
```

- [x] **Step 2: Run tests to verify they fail**

Run: `cargo test -p varve-log`
Expected: FAIL to compile (trait methods missing).

- [x] **Step 3: Implement**

`MemoryLog` (lock `inner`):

```rust
async fn head(&self) -> Result<LogPosition, LogError> {
    Ok(self.inner.lock().map_err(|_| LogError::Poisoned)?.next)
}

async fn start_epoch(&self, epoch: u16) -> Result<(), LogError> {
    let mut inner = self.inner.lock().map_err(|_| LogError::Poisoned)?;
    validate_epoch_start(epoch, inner.next)?;
    inner.next = LogPosition::new(epoch, 0)?;
    Ok(())
}
```

Shared helper in `log.rs` (used by memory + object-store):

```rust
/// `EpochRegression` unless `(epoch, 0)` is at or beyond `head`.
pub(crate) fn validate_epoch_start(epoch: u16, head: LogPosition) -> Result<(), LogError> {
    if epoch < head.epoch() || (epoch == head.epoch() && head.offset() > 0) {
        return Err(LogError::EpochRegression {
            requested: epoch,
            head: head.epoch(),
        });
    }
    Ok(())
}
```

`ObjectStoreLog`:

```rust
async fn head(&self) -> Result<LogPosition, LogError> {
    let mut next = self.next.lock().await;
    match *next {
        Some(position) => Ok(position),
        None => {
            let position = self.recover_next().await?;
            *next = Some(position);
            Ok(position)
        }
    }
}

async fn start_epoch(&self, epoch: u16) -> Result<(), LogError> {
    let mut next = self.next.lock().await;
    let head = match *next {
        Some(position) => position,
        None => self.recover_next().await?,
    };
    crate::log::validate_epoch_start(epoch, head)?;
    *next = Some(LogPosition::new(epoch, 0)?);
    Ok(())
}
```

`LocalLog`: `head` returns `inner.next` (respect `poisoned` → `LogError::Poisoned`); `start_epoch` returns `Err(LogError::EpochUnsupported("local"))`.

Stub the engine-side test doubles (delegate to inner where one exists, `unreachable!()` where the double never coordinates).

- [x] **Step 4: Run tests**

Run: `cargo test -p varve-log && cargo test -p varve-engine`
Expected: PASS (engine compiles with stubs; no behavior change).

- [x] **Step 5: Commit**

```bash
git add crates/varve-log crates/varve-engine
git commit -m "feat: log head and epoch repositioning for writer fencing"
```

---

### Task 3: Epoch fences — keys, FenceMap, fence-filtered recovery

Fences are the durable "epoch E ends here" markers. This task adds the key helpers, the engine-side `FenceMap`, and makes `recover()` (startup replay) fence-aware. Independent value: even before the cas coordinator exists, recovery correctly ignores dead records if fences are present.

**Files:**
- Modify: `crates/varve-storage/src/keys.rs`
- Create: `crates/varve-engine/src/coord/fence.rs` (+ `pub(crate) mod fence;` in `coord/mod.rs`)
- Modify: `crates/varve-engine/src/db.rs` (`recover`)
- Test: unit tests in `keys.rs`, `fence.rs`; recovery test in `crates/varve-engine/tests/` (find the existing recovery test file — replay tests live in `varve-engine/tests/`; add to the file that already exercises `Db::open` replay, or create `tests/fenced_recovery.rs`)

**Interfaces:**
- Consumes: `varve_storage::ObjectStore` (get/put/list), `varve_types::LogPosition`.
- Produces:

```rust
// varve-storage/src/keys.rs
pub const EPOCH_FENCE_PREFIX: &str = "v1/epochs";
/// `v1/epochs/<epoch-4hex>.json` — fence for a DEAD epoch.
pub fn epoch_fence_key(epoch: u16) -> String;      // format!("{EPOCH_FENCE_PREFIX}/{epoch:04x}.json")
pub fn parse_epoch_fence_key(key: &str) -> Option<u16>;
```

```rust
// varve-engine/src/coord/fence.rs
/// epoch → fence_offset. A record at (e, o) is dead iff fences[e] <= o.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct FenceMap(std::collections::BTreeMap<u16, u64>);

impl FenceMap {
    pub fn is_live(&self, position: LogPosition) -> bool;
    /// If `cursor` sits at/behind a fence in its epoch, the position where a
    /// reader continues: `(cursor.epoch() + 1, 0)`. None when unfenced.
    pub fn jump(&self, cursor: LogPosition) -> Result<Option<LogPosition>, EngineError>;
    #[cfg(test)] pub fn from_pairs(pairs: &[(u16, u64)]) -> FenceMap;
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct FenceDoc {
    pub epoch: u16,
    pub fence_offset: u64,
    pub fenced_by: String,   // node_id of the seizing writer
    pub fenced_at_us: i64,
}

pub(crate) async fn load_fences(store: &dyn ObjectStore) -> Result<FenceMap, EngineError>;
pub(crate) async fn write_fence(store: &dyn ObjectStore, doc: &FenceDoc) -> Result<(), EngineError>;
```

`jump` returns `Result` because `cursor.epoch() == u16::MAX` cannot advance — map to `EngineError::EpochExhausted` (add the variant here):

```rust
// EngineError addition (db.rs)
#[error("log epoch space (u16) is exhausted")]
EpochExhausted,
```

- [x] **Step 1: Write the failing unit tests**

```rust
// keys.rs tests
#[test]
fn epoch_fence_keys_round_trip() {
    assert_eq!(epoch_fence_key(0), "v1/epochs/0000.json");
    assert_eq!(epoch_fence_key(0xBEEF), "v1/epochs/beef.json");
    assert_eq!(parse_epoch_fence_key("v1/epochs/beef.json"), Some(0xBEEF));
    assert_eq!(parse_epoch_fence_key("v1/epochs/nope.txt"), None);
    assert_eq!(parse_epoch_fence_key("v1/blocks/0001.manifest"), None);
}
```

```rust
// fence.rs tests
#[test]
fn liveness_and_jump_follow_the_fence() {
    let fences = FenceMap::from_pairs(&[(0, 5)]);
    let live = LogPosition::new(0, 4).unwrap();
    let dead = LogPosition::new(0, 5).unwrap();
    let later = LogPosition::new(1, 0).unwrap();
    assert!(fences.is_live(live));
    assert!(!fences.is_live(dead));
    assert!(fences.is_live(later));
    assert_eq!(fences.jump(live).unwrap(), None);
    assert_eq!(fences.jump(dead).unwrap(), Some(later));
}

#[tokio::test]
async fn fences_round_trip_through_the_store() {
    let store = varve_storage::memory_store();
    write_fence(
        store.as_ref(),
        &FenceDoc { epoch: 0, fence_offset: 7, fenced_by: "n1".into(), fenced_at_us: 1 },
    )
    .await
    .unwrap();
    let fences = load_fences(store.as_ref()).await.unwrap();
    assert!(!fences.is_live(LogPosition::new(0, 7).unwrap()));
    assert!(fences.is_live(LogPosition::new(0, 6).unwrap()));
}
```

- [x] **Step 2: Run to verify failure**

Run: `cargo test -p varve-storage keys && cargo test -p varve-engine fence`
Expected: FAIL (missing symbols).

- [x] **Step 3: Implement**

`load_fences`: `store.list(EPOCH_FENCE_PREFIX)` → for each key with `parse_epoch_fence_key` → `store.get` → `serde_json::from_slice::<FenceDoc>` → insert `(doc.epoch, doc.fence_offset)`. Foreign keys under the prefix are ignored (the `parse_log_key` policy). `write_fence`: plain `store.put(epoch_fence_key(doc.epoch), serde_json::to_vec(doc))`.

`jump`:

```rust
pub fn jump(&self, cursor: LogPosition) -> Result<Option<LogPosition>, EngineError> {
    match self.0.get(&cursor.epoch()) {
        Some(fence) if cursor.offset() >= *fence => {
            let next_epoch = cursor
                .epoch()
                .checked_add(1)
                .ok_or(EngineError::EpochExhausted)?;
            Ok(Some(LogPosition::new(next_epoch, 0)?))
        }
        _ => Ok(None),
    }
}
```

- [x] **Step 4: Make `recover()` fence-aware — failing e2e test first**

```rust
// varve-engine/tests/fenced_recovery.rs (new file; model the existing
// replay/recovery integration test's Db construction — object-store log
// over a shared memory store is built with Db::open_with + a registered
// shared-store factory, see the SharedStoreFactory pattern in Task 9;
// alternatively drive ObjectStoreLog directly as below)
use std::sync::Arc;
use varve_log::{Log, LogRecord, ObjectStoreLog, TableEffects};

fn put_record(tx_id: u64, id: i64) -> LogRecord {
    // One INSERT-equivalent effect event on the default graph nodes table.
    use varve_types::{Instant, Value};
    let mut doc = varve_types::Doc::new();
    doc.insert("_id".to_string(), Value::Int(id));
    let iid = varve_types::Iid::derive(
        "default",
        "nodes",
        &Value::Int(id).id_bytes().unwrap(),
    );
    let event = varve_index::Event {
        iid,
        system_from: Instant::from_micros(tx_id as i64),
        valid_from: Instant::from_micros(tx_id as i64),
        valid_to: Instant::END_OF_TIME,
        src: None,
        dst: None,
        op: varve_index::Op::Put { labels: vec!["Chaos".into()], doc },
    };
    // (Field names/constructors verified against varve-index and varve-types;
    // if a name drifted, adapt the construction — the assertion is the contract.)
    LogRecord {
        tx_id,
        system_time_us: tx_id as i64,
        user: String::new(),
        effects: vec![TableEffects {
            graph: String::new(),
            table: "nodes".into(),
            arrow_ipc: varve_index::encode_events(&[event]).unwrap(),
        }],
    }
}

#[tokio::test]
async fn recovery_skips_fenced_records_and_replays_across_the_epoch_jump() {
    let store = varve_storage::memory_store();
    let log = ObjectStoreLog::new(Arc::clone(&store));
    log.append(vec![put_record(1, 1), put_record(2, 2)]).await.unwrap();
    // zombie record at (0,2): fence epoch 0 at offset 2, then append it
    store.put(
        &varve_storage::keys::epoch_fence_key(0),
        bytes::Bytes::from(serde_json::json!({
            "epoch": 0, "fence_offset": 2, "fenced_by": "test", "fenced_at_us": 0
        }).to_string()),
    ).await.unwrap();
    log.append(vec![put_record(99, 99)]).await.unwrap(); // lands at (0,2) — DEAD
    // epoch-1 successor record
    let log2 = ObjectStoreLog::new(Arc::clone(&store));
    log2.start_epoch(1).await.unwrap();
    log2.append(vec![put_record(3, 3)]).await.unwrap();

    let db = open_query_writer_db_over(store).await; // Db::open_with + shared-store registries
    let rows = db.query("MATCH (c:Chaos) RETURN c._id").await.unwrap();
    let ids = collect_i64_column(&rows);
    assert_eq!(ids, vec![1, 2, 3], "zombie _id 99 must NOT be visible");
}
```

Implementation in `recover` (db.rs:1552): after loading the manifest, `let fences = crate::coord::fence::load_fences(store.as_ref()).await?;` then filter the replayed tail: skip any `(position, record)` where `!fences.is_live(position)` — the skip happens BEFORE the record contributes to `next_tx_id`, the clock floor, or the state fold. The final watermark still advances past dead records (it is `max(manifest, last position + 1)` — verify against the actual fold in `recover` and keep the watermark monotonic across dead suffixes).

Note: `next_tx_id` deliberately ignores dead records — a zombie's tx ids are reassigned by the new epoch's writer; that is the point of fencing.

- [x] **Step 5: Run tests**

Run: `cargo test -p varve-engine --test fenced_recovery && cargo test -p varve-engine`
Expected: PASS.

- [x] **Step 6: Commit**

```bash
git add crates/varve-storage crates/varve-engine
git commit -m "feat: epoch fence objects and fence-filtered recovery"
```

---

### Task 4: Fence-aware follower and verify

Followers must skip dead records inside a range, jump their cursor across a fenced epoch boundary, and keep the LogGap contract; `verify` walks the same way.

**Files:**
- Modify: `crates/varve-engine/src/follower.rs`
- Modify: `crates/varve-engine/src/verify.rs`
- Test: unit tests in `follower.rs` (existing `ScriptedLog` machinery), `verify` test via the existing verify test location (grep `verify_database` callers in tests; extend there or in `db.rs` tests)

**Interfaces:**
- Consumes: `FenceMap`, `load_fences`, `FenceMap::jump` (Task 3); `LogGap` semantics unchanged for unfenced logs.
- Produces: `FollowerState` gains `pub fences: FenceMap` (refreshed from the store on empty polls); behavior consumed by Task 9's cross-node assertions.

Behavior contract for `apply_next_range`:
1. Non-empty read: first returned position must equal the cursor (LogGap otherwise, unchanged). For each record in order: if dead (`!fences.is_live(position)`) → advance the cursor past it and publish progress with the **position advanced but `tx_id` unchanged**; if live → apply + publish as today. Dead records do not count toward the returned `applied` count.
2. Empty read: refresh `fences = load_fences(store)`. If `fences.jump(cursor)?` yields a new position → set the cursor there and return `Ok(0)` (the loop re-polls; a non-zero sentinel would skip the poll delay — instead have `apply_next_range` immediately retry the read once after a jump so takeover catch-up is not delayed by `poll_interval`). Then the existing manifest-gap check runs against the (possibly jumped) cursor.

- [x] **Step 1: Write the failing follower unit tests**

```rust
// follower.rs tests (reuse ScriptedLog / follower_state helpers)
#[tokio::test]
async fn dead_records_advance_the_cursor_without_applying() {
    let store = varve_storage::memory_store();
    put_fence(&store, 0, 1).await; // fence epoch 0 at offset 1
    // scripted: cursor 0 reads [(0,0) live, (0,1) dead]
    let log = ScriptedLog::new(vec![Ok(vec![
        (LogPosition::ZERO, record(1)),
        (LogPosition::new(0, 1).unwrap(), record(2)),
    ])]);
    let (mut state, progress) = follower_state(log, store);
    state.fences = load_fences_blocking(&state.store).await;

    let applied = apply_next_range(&mut state).await.unwrap();
    assert_eq!(applied, 1, "only the live record applies");
    assert_eq!(state.cursor, LogPosition::new(0, 2).unwrap());
    assert_eq!(progress.borrow().applied.tx_id, 1, "dead tx id 2 never published");
    assert_eq!(progress.borrow().applied.log_position, LogPosition::new(0, 2).unwrap());
}

#[tokio::test]
async fn empty_read_at_a_fence_jumps_to_the_next_epoch_and_reads_it() {
    let store = varve_storage::memory_store();
    put_fence(&store, 0, 0).await; // whole epoch 0 is dead
    let log = ScriptedLog::new(vec![
        Ok(Vec::new()),                                        // epoch-0 read: empty
        Ok(vec![(LogPosition::new(1, 0).unwrap(), record(5))]), // post-jump read
    ]);
    let (mut state, progress) = follower_state(log, store);

    let applied = apply_next_range(&mut state).await.unwrap();
    assert_eq!(applied, 1, "jump retries the read immediately");
    assert_eq!(state.cursor, LogPosition::new(1, 1).unwrap());
    assert_eq!(progress.borrow().applied.tx_id, 5);
}

#[tokio::test]
async fn manifest_watermark_in_a_later_epoch_is_not_a_gap_when_a_jump_is_pending() {
    let store = varve_storage::memory_store();
    put_fence(&store, 0, 0).await;
    put_manifest_with_watermark(&store, LogPosition::new(1, 0).unwrap()).await; // helper mirrors the existing empty_range_behind_manifest_watermark_is_a_gap test setup
    let log = ScriptedLog::new(vec![Ok(Vec::new()), Ok(Vec::new())]);
    let (mut state, _progress) = follower_state(log, store);

    // jump lands the cursor exactly at the watermark — no LogGap
    assert_eq!(apply_next_range(&mut state).await.unwrap(), 0);
    assert_eq!(state.cursor, LogPosition::new(1, 0).unwrap());
}
```

(`put_fence` = the inline JSON PUT from Task 3's test; `load_fences_blocking` is just `load_fences(...).await.unwrap()` — write it inline. `record(n)` exists in the test module; note `ScriptedLog` records carry NO effects, so "applies" here means cursor/progress bookkeeping — the existing tests rely on the same property.)

- [x] **Step 2: Run to verify failure**

Run: `cargo test -p varve-engine follower`
Expected: FAIL (no `fences` field / no jump behavior).

- [x] **Step 3: Implement in `apply_next_range` + `FollowerState`**

Sketch (adapt around the existing gap checks — keep every existing test green):

```rust
pub(crate) struct FollowerState {
    // ... existing fields ...
    pub fences: FenceMap, // starts empty; refreshed on empty polls
}

pub(crate) async fn apply_next_range(state: &mut FollowerState) -> Result<usize, EngineError> {
    let applied = apply_range_once(state).await?;
    if applied == JUMPED {
        // one immediate retry after an epoch jump so catch-up is not
        // poll-interval-delayed
        return apply_range_once(state).await.map(|a| if a == JUMPED { 0 } else { a });
    }
    Ok(applied)
}
```

(Implement with an internal enum or `Option<usize>` rather than a magic constant — e.g. `async fn apply_range_once(&mut state) -> Result<RangeOutcome, EngineError>` with `enum RangeOutcome { Applied(usize), Jumped }`. The empty-read branch: refresh fences, `if let Some(next) = state.fences.jump(state.cursor)? { state.cursor = next; return Ok(RangeOutcome::Jumped); }` then the manifest-gap check as today. The non-empty branch: per-record `if !state.fences.is_live(position) { state.cursor = next; state.progress.send_replace(/* position advanced, tx unchanged */); continue; }`.)

For the dead-record progress publish, read the current `tx_id` from `state.progress.borrow().applied.tx_id` and re-publish with the advanced position.

- [x] **Step 4: Fence-aware `verify` — failing test then implement**

Test (place next to existing verify coverage):

```rust
#[tokio::test]
async fn verify_walks_across_a_fenced_epoch_boundary() {
    // store with: (0,0) live record, fence(0)=1, zombie at (0,1), (1,0) live
    // (construct exactly as in fenced_recovery.rs)
    let report = db.verify().await.unwrap();
    assert_eq!(report.log_records_checked, 2, "zombie checked-but-skipped records are not counted");
}
```

Implementation in `verify_database`: load fences once at the top; in the log loop, per record: dead → `cursor = cursor.next()?` and continue (no decode-count); the empty-read branch performs `fences.jump(cursor)?` once before breaking (loop again if jumped). The strict `position != cursor → LogGap` check stays.

- [x] **Step 5: Run tests**

Run: `cargo test -p varve-engine`
Expected: PASS, including all pre-existing follower/verify tests.

- [x] **Step 6: Commit**

```bash
git add crates/varve-engine
git commit -m "feat: fence-aware follower cursor jumps and verify log walk"
```

---

### Task 5: `Coordinator` trait, registry, and `designated-writer`

The trait + registry (spec §4 pattern) and the default coordinator: advertisement heartbeats and the best-effort second-writer guard.

**Files:**
- Modify: `crates/varve-engine/src/coord/mod.rs` (trait, `WriterGrant`, `LeaseState`, heartbeat task)
- Create: `crates/varve-engine/src/coord/designated.rs`
- Modify: `crates/varve-engine/src/registries.rs` (`coordinator` registry)
- Modify: `crates/varve-engine/src/db.rs` (`WriterAdvertisement` extension, `EngineError` variants, extract `read_writer_advertisement`)
- Modify: `crates/varve-server/src/http/handlers.rs` + `crates/varve-cli` tests if they construct `WriterAdvertisement` literals (grep `WriterAdvertisement {` across the workspace; new fields get real values)
- Test: unit tests in `designated.rs` and `coord/mod.rs`

**Interfaces:**
- Consumes: `generate_node_id` (Task 1), `ObjectStore`, `Clock`, `varve_config::{Registry, ComponentFactory, ConfigSection, BuildContext}`.
- Produces (Tasks 6–9 rely on these exact shapes):

```rust
// coord/mod.rs
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use varve_log::Log;

pub struct WriterGrant {
    /// Epoch this writer must append at (Log::start_epoch); None = continue
    /// the log's recovered epoch.
    pub epoch: Option<u16>,
}

/// Published by the heartbeat task; the writer loop gates acks on it.
#[derive(Clone, Debug)]
pub enum LeaseState {
    /// designated-writer: nothing to lose.
    Unfenced,
    /// cas-failover: acks allowed while `tokio::time::Instant::now() < deadline`.
    ValidUntil(tokio::time::Instant),
    /// Terminal: the lease was seized or could not be renewed in time.
    Lost(String),
}

#[async_trait::async_trait]
pub trait Coordinator: Send + Sync {
    /// Writer-role startup gate. Blocks (standby) until this node may write,
    /// or refuses with a diagnostic error. Runs BEFORE recovery.
    async fn acquire(&self, log: &Arc<dyn Log>) -> Result<WriterGrant, EngineError>;
    /// Records the advertised address and publishes v1/writer.json now.
    async fn advertise(&self, address: &str) -> Result<(), EngineError>;
    /// One heartbeat: refresh the advertisement (and lease, in cas mode).
    async fn heartbeat(&self) -> LeaseState;
    /// Duration::ZERO disables the heartbeat task.
    fn heartbeat_interval(&self) -> Duration;
}

pub(crate) struct HeartbeatHandle { /* watch shutdown + JoinHandle; Drop aborts (FollowerHandle pattern) */ }
pub(crate) fn spawn_heartbeat(
    coordinator: Arc<dyn Coordinator>,
    lease: watch::Sender<LeaseState>,
) -> HeartbeatHandle;
```

```rust
// db.rs — WriterAdvertisement replaces the single-field struct (all fields serialized, declared order = canonical JSON order)
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize, Eq, PartialEq)]
pub struct WriterAdvertisement {
    pub address: String,
    #[serde(default)] pub node_id: String,
    #[serde(default)] pub epoch: u16,
    #[serde(default)] pub heartbeat_us: i64,
}

/// Free fn extracted from Db::writer_advertisement (list-then-get pattern,
/// db.rs:1022): shared by Db and the coordinators.
pub(crate) async fn read_writer_advertisement(
    store: &dyn ObjectStore,
) -> Result<Option<WriterAdvertisement>, EngineError>;
```

```rust
// EngineError additions
#[error("another writer is active at '{address}' (heartbeat {age_ms} ms old; stale after {takeover_after_ms} ms). Stop it, or wait for its heartbeat to go stale. This guard is best-effort (spec §12).")]
WriterActive { address: String, age_ms: u64, takeover_after_ms: u64 },
#[error("invalid [coordinator] configuration: {0}")]
InvalidCoordinatorConfig(String),
```

```rust
// coord/designated.rs
pub(crate) struct DesignatedWriter {
    store: Arc<dyn varve_storage::ObjectStore>,
    clock: Arc<dyn crate::clock::Clock>,
    node_id: String,
    heartbeat_interval: Duration,
    takeover_after: Duration,
    address: std::sync::Mutex<Option<String>>,
}
pub(crate) struct DesignatedWriterFactory;   // name: "designated-writer"
```

Config (parsed by BOTH coordinator factories from their section):

```rust
#[derive(serde::Deserialize)]
pub(crate) struct CoordTuning {
    #[serde(default = "default_heartbeat_interval_ms")] pub heartbeat_interval_ms: u64, // 5000
    #[serde(default = "default_takeover_after_ms")] pub takeover_after_ms: u64,          // 15000
}
// validate(): heartbeat_interval_ms > 0 ⇒ takeover_after_ms >= 2 * heartbeat_interval_ms,
// else Err(String) → EngineError::InvalidCoordinatorConfig
```

Factory registration requires the store AND clock in the `BuildContext`; the factory generates its own `node_id` per built instance.

**Registry plumbing:** `Registries` gains `pub coordinator: Registry<dyn Coordinator>`; `with_builtins()` registers `DesignatedWriterFactory` (and Task 7 adds `cas-failover`). Update `registries.rs::builtins_cover_log_and_clock` to assert `vec!["cas-failover", "designated-writer"]` once Task 7 lands — for THIS task assert `vec!["designated-writer"]` and let Task 7 update it.

- [x] **Step 1: Failing tests for the guard + heartbeat**

```rust
// coord/designated.rs tests
fn designated(store: Arc<dyn ObjectStore>, node_id: &str) -> DesignatedWriter {
    DesignatedWriter {
        store,
        clock: Arc::new(crate::clock::MonotonicClock::new()),
        node_id: node_id.into(),
        heartbeat_interval: Duration::from_millis(5000),
        takeover_after: Duration::from_millis(15000),
        address: std::sync::Mutex::new(None),
    }
}

#[tokio::test]
async fn acquire_on_an_empty_store_grants_without_an_epoch() {
    let store = varve_storage::memory_store();
    let log: Arc<dyn Log> = Arc::new(varve_log::MemoryLog::new());
    let grant = designated(store, "a").acquire(&log).await.unwrap();
    assert!(grant.epoch.is_none());
}

#[tokio::test]
async fn a_fresh_foreign_heartbeat_refuses_startup_with_a_clear_error() {
    let store = varve_storage::memory_store();
    let a = designated(Arc::clone(&store), "a");
    a.advertise("http://a:8080").await.unwrap(); // publishes with heartbeat_us = now
    let log: Arc<dyn Log> = Arc::new(varve_log::MemoryLog::new());
    let err = designated(store, "b").acquire(&log).await.unwrap_err();
    match err {
        EngineError::WriterActive { address, takeover_after_ms, .. } => {
            assert_eq!(address, "http://a:8080");
            assert_eq!(takeover_after_ms, 15000);
        }
        other => panic!("expected WriterActive, got {other}"),
    }
}

#[tokio::test]
async fn a_stale_or_own_heartbeat_does_not_refuse() {
    let store = varve_storage::memory_store();
    // Stale: heartbeat_us far in the past — write the advertisement JSON directly.
    store.put("v1/writer.json", bytes::Bytes::from(
        serde_json::to_vec(&WriterAdvertisement {
            address: "http://old:1".into(), node_id: "old".into(), epoch: 0, heartbeat_us: 1,
        }).unwrap(),
    )).await.unwrap();
    let log: Arc<dyn Log> = Arc::new(varve_log::MemoryLog::new());
    designated(Arc::clone(&store), "b").acquire(&log).await.unwrap();

    // Own node_id (restart of the same instance handle): never refuses.
    let me = designated(store, "old");
    me.acquire(&log).await.unwrap();
}

#[tokio::test]
async fn heartbeat_republishes_the_advertisement_with_a_fresh_timestamp() {
    let store = varve_storage::memory_store();
    let c = designated(Arc::clone(&store), "a");
    assert!(matches!(c.heartbeat().await, LeaseState::Unfenced)); // no address yet: no PUT
    assert!(store.list("v1").await.unwrap().is_empty());

    c.advertise("http://a:8080").await.unwrap();
    let first: WriterAdvertisement =
        serde_json::from_slice(&store.get("v1/writer.json").await.unwrap()).unwrap();
    assert!(matches!(c.heartbeat().await, LeaseState::Unfenced));
    let second: WriterAdvertisement =
        serde_json::from_slice(&store.get("v1/writer.json").await.unwrap()).unwrap();
    assert_eq!(second.node_id, "a");
    assert!(second.heartbeat_us > first.heartbeat_us);
}
```

- [x] **Step 2: Run to verify failure**

Run: `cargo test -p varve-engine coord::designated`
Expected: FAIL (module missing).

- [x] **Step 3: Implement `DesignatedWriter`**

- `advertise`: store the address in the mutex, then `publish(now)` — build `WriterAdvertisement { address, node_id, epoch: 0, heartbeat_us: clock.next().as_micros() }`, canonical `serde_json::to_vec`, plain PUT to `v1/writer.json` (reuse the `WRITER_ADVERTISEMENT_KEY` const — make it `pub(crate)` in db.rs).
- `heartbeat`: if the address mutex is `None` → `LeaseState::Unfenced` (no I/O). Else re-publish with a fresh `heartbeat_us`; a PUT failure is logged (`tracing::warn!` arrives in Task 13 — for now ignore the error) and still returns `Unfenced` (best-effort by design).
- `acquire`: `read_writer_advertisement(store)`; refuse iff `Some(ad)` and `ad.node_id != self.node_id` and `ad.heartbeat_us > 0` and `age < takeover_after` where `age = clock.next().as_micros() - ad.heartbeat_us` (clamp negative to 0 — a FUTURE timestamp is "fresh"). Grant `WriterGrant { epoch: None }` otherwise. `heartbeat_interval == ZERO` skips nothing here — the guard still runs; only the heartbeat task is disabled.
- `heartbeat_interval()`: return the configured interval.

Factory:

```rust
impl ComponentFactory<dyn Coordinator> for DesignatedWriterFactory {
    fn name(&self) -> &'static str { "designated-writer" }
    fn build(&self, cfg: &ConfigSection, ctx: &BuildContext) -> Result<Arc<dyn Coordinator>, RegistryError> {
        let store = ctx.get::<Arc<dyn varve_storage::ObjectStore>>().ok_or_else(|| /* Build err: "open through Db::open" */)?;
        let clock = ctx.get::<Arc<dyn crate::clock::Clock>>().ok_or_else(|| /* same */)?;
        let tuning: CoordTuning = cfg.get().map_err(|e| /* Build err */)?;
        let (hb, takeover) = tuning.validate().map_err(|e| /* Build err */)?;
        Ok(Arc::new(DesignatedWriter { store, clock, node_id: identity::generate_node_id(), heartbeat_interval: hb, takeover_after: takeover, address: Mutex::new(None) }))
    }
}
```

`spawn_heartbeat` (coord/mod.rs): watch-shutdown + abortable task, exactly the `FollowerHandle` pattern (follower.rs:27–37). Loop: if `coordinator.heartbeat_interval().is_zero()` → send `Unfenced`, return. Else `tokio::select!` over `sleep(interval)` and the shutdown watch; on each tick `lease.send_replace(coordinator.heartbeat().await)`; break on `Lost` (leave the Lost state published).

Extend `WriterAdvertisement` and extract `read_writer_advertisement`; update `Db::writer_advertisement` to call it. Fix every literal-constructor and JSON-shape assertion the grep finds (server `api.rs`/tests, CLI tests) — new fields get real values; JSON assertions include the new keys.

- [x] **Step 4: Run tests**

Run: `cargo test -p varve-engine && cargo test -p varve-server && cargo test -p varve-cli -- --test-threads=1`
Expected: PASS.

- [x] **Step 5: Commit**

```bash
git add crates/varve-engine crates/varve-server crates/varve-cli
git commit -m "feat: Coordinator trait, registry, and designated-writer heartbeat guard"
```

---

### Task 6: Engine wiring — coordinator in `open_with`, heartbeat lifecycle, `publish_writer` delegation

**Files:**
- Modify: `crates/varve-engine/src/db.rs` (`open_with`, `assemble`, `DbInner`, `publish_writer`, `Db::memory`, `Db::local`)
- Modify: `crates/varve-engine/src/writer.rs` (`WriterState` gains the lease receiver — plumbing only in this task; gating logic is Task 8)
- Modify: `crates/varve-server/src/http/handlers.rs` (`redirect`: empty-address advertisement → 503, not a 421 pointing nowhere)
- Modify: `crates/varve/src/lib.rs` (re-export `Coordinator`, `WriterGrant`, `LeaseState`)
- Test: `crates/varve-engine/tests/coordination.rs` (new)

**Interfaces:**
- Consumes: Tasks 1–5 (`Coordinator`, `spawn_heartbeat`, `FenceMap`, `Log::start_epoch`, `WriterGrant`).
- Produces:
  - `Db::open_with` sequencing (below) — Tasks 7/9 depend on acquire-before-recover.
  - `DbInner` holds `coordinator: Option<Arc<dyn Coordinator>>`, `node_id: String`, `heartbeat: Option<HeartbeatHandle>` (abort-on-drop), and the writer receives `lease: watch::Receiver<LeaseState>`.
  - `Db::publish_writer(&self, address: &str)` — unchanged signature; delegates to the coordinator when present, plain-PUTs the extended advertisement (`node_id = self.inner.node_id`, `epoch` = current durable epoch is NOT tracked on DbInner — use `0`; the coordinator path is the real deployment path) otherwise.

`open_with` sequencing (replaces db.rs:891–973's tail; storage/log/cache/clock construction unchanged):

```rust
// after clock is built:
ctx.insert::<Arc<dyn Clock>>(Arc::clone(&clock));
// ... existing tuning parses; node_tuning/roles already parsed here ...
let coord_section = config.section("coordinator").unwrap_or_else(ConfigSection::empty);
let coord_backend = coord_section.backend().unwrap_or("designated-writer").to_string();
if coord_backend == "cas-failover" && log_backend != "object-store" {
    return Err(EngineError::CasRequiresSharedLog(log_backend));
}
let coordinator = if roles.contains(NodeRole::Writer) {
    Some(registries.coordinator.build(&coord_backend, &coord_section, &ctx)?)
} else {
    None
};
let grant = match &coordinator {
    Some(c) => c.acquire(&log).await?,     // may BLOCK (cas standby)
    None => WriterGrant { epoch: None },
};
let recovered = recover(log.as_ref(), clock.as_ref(), &store).await?; // fence-aware since Task 3
if let Some(epoch) = grant.epoch {
    log.start_epoch(epoch).await?;
}
Ok(Self::assemble(/* existing args */, coordinator))
```

```rust
// EngineError addition
#[error("cas-failover requires the shared \"object-store\" log; [log] backend is \"{0}\"")]
CasRequiresSharedLog(String),
```

`assemble` additions: create `let (lease_tx, lease_rx) = watch::channel(LeaseState::Unfenced);` — `WriterState` gets `lease: lease_rx`; spawn the heartbeat only for writer-role nodes with a coordinator whose `heartbeat_interval() > ZERO`: `spawn_heartbeat(Arc::clone(coordinator), lease_tx)`. `DbInner` stores the handle so drop cancels it. `node_id: identity::generate_node_id()` on every assemble.

- [x] **Step 1: Failing integration tests**

```rust
// crates/varve-engine/tests/coordination.rs
use std::collections::BTreeMap;
use std::sync::Arc;
use varve_config::{BuildContext, ComponentFactory, Config, ConfigSection, RegistryError};
use varve_engine::{Db, EngineError, Registries};
use varve_storage::ObjectStore;

/// Registers a storage factory that returns ONE shared store for every
/// build — two Db processes "sharing a bucket", in-process. (Task 9 reuses
/// this; keep it in a small `mod shared` here and copy the ~20 lines there,
/// or move it to varve-testkit if preferred — tests are the contract.)
struct SharedStoreFactory(Arc<dyn ObjectStore>);
impl ComponentFactory<dyn ObjectStore> for SharedStoreFactory {
    fn name(&self) -> &'static str { "shared" }
    fn build(&self, _: &ConfigSection, _: &BuildContext) -> Result<Arc<dyn ObjectStore>, RegistryError> {
        Ok(Arc::clone(&self.0))
    }
}

fn shared_registries(store: &Arc<dyn ObjectStore>) -> Registries {
    let mut r = Registries::with_builtins();
    r.storage.register(Box::new(SharedStoreFactory(Arc::clone(store)))).unwrap();
    r
}

fn writer_config(extra: &str) -> Config {
    Config::from_str(&format!(
        r#"
[storage]
backend = "shared"
[log]
backend = "object-store"
{extra}
"#
    )).unwrap()
}
// NOTE: check varve_config::Config's constructor surface — slice 0 shipped
// from_file + env overrides; if there is no from_str, write the TOML to a
// tempfile and use from_file. Tests are the contract; adapt construction.

#[tokio::test]
async fn second_advertised_writer_is_refused_while_the_heartbeat_is_fresh() {
    let store: Arc<dyn ObjectStore> = varve_storage::memory_store();
    let a = Db::open_with(&writer_config("[coordinator]\nheartbeat_interval_ms = 100\ntakeover_after_ms = 300"),
                          &shared_registries(&store)).await.unwrap();
    a.publish_writer("http://a:1").await.unwrap();

    let err = Db::open_with(&writer_config("[coordinator]\nheartbeat_interval_ms = 100\ntakeover_after_ms = 300"),
                            &shared_registries(&store)).await.unwrap_err();
    assert!(matches!(err, EngineError::WriterActive { .. }), "{err}");

    // ...and once the heartbeat goes stale (drop A, wait past takeover_after),
    // a new writer starts fine.
    drop(a);
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    Db::open_with(&writer_config("[coordinator]\nheartbeat_interval_ms = 100\ntakeover_after_ms = 300"),
                  &shared_registries(&store)).await.unwrap();
}

#[tokio::test]
async fn an_unadvertised_writer_restarts_immediately_without_a_guard() {
    let store: Arc<dyn ObjectStore> = varve_storage::memory_store();
    let cfg = writer_config("[coordinator]\nheartbeat_interval_ms = 100\ntakeover_after_ms = 300");
    let a = Db::open_with(&cfg, &shared_registries(&store)).await.unwrap();
    a.execute("INSERT (:P {_id: 1})").await.unwrap();
    drop(a);
    // no publish_writer ⇒ no advertisement ⇒ immediate restart is fine
    let b = Db::open_with(&cfg, &shared_registries(&store)).await.unwrap();
    let rows = b.query("MATCH (p:P) RETURN p._id").await.unwrap();
    assert_eq!(rows.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
}

#[tokio::test]
async fn cas_failover_without_the_object_store_log_is_a_config_error() {
    let cfg = Config::from_str(r#"
[storage]
backend = "memory"
[coordinator]
backend = "cas-failover"
"#).unwrap(); // [log] defaults to "memory"
    let err = Db::open(cfg).await.unwrap_err();
    assert!(matches!(err, EngineError::CasRequiresSharedLog(name) if name == "memory"));
}
```

(The third test compiles before Task 7 because the name check fires before the registry lookup.)

- [x] **Step 2: Run to verify failure**

Run: `cargo test -p varve-engine --test coordination`
Expected: FAIL (no coordinator wiring).

- [x] **Step 3: Implement the wiring**

As specified in Interfaces. Also:
- `publish_writer`: `match &self.inner.coordinator { Some(c) => c.advertise(address).await, None => /* plain PUT of extended advertisement */ }`.
- server `redirect` (handlers.rs:159): `Ok(Some(a)) if !a.address.is_empty() => 421 …; _ => 503 …`.
- `varve/src/lib.rs`: add `Coordinator, WriterGrant, LeaseState` to the `varve_engine` re-export list.

- [x] **Step 4: Run the full engine + server suites**

Run: `cargo test -p varve-engine && cargo test -p varve-server -- --test-threads=1`
Expected: PASS (heartbeat tasks shut down on drop; no test leaks).

- [x] **Step 5: Commit**

```bash
git add crates/varve-engine crates/varve-server crates/varve
git commit -m "feat: coordinator lifecycle wired through Db::open_with"
```

---

### Task 7: `cas-failover` coordinator

Lease acquisition, clock-skew-free staleness (ETag double-observation), seizure with epoch bump + fence write, heartbeat renewal. Feature-gated `cas-failover` (default on) per the roadmap.

**Files:**
- Modify: `crates/varve-storage/src/store.rs` (`ConditionalStore::get_versioned`, `StorageError::NoEtag`)
- Create: `crates/varve-engine/src/coord/cas.rs` (+ `#[cfg(feature = "cas-failover")] pub(crate) mod cas;` in `coord/mod.rs`)
- Modify: `crates/varve-engine/Cargo.toml` (`[features] default = ["cas-failover"]`, `cas-failover = []`) and `crates/varve/Cargo.toml` if `varve` re-exposes engine features (check; the engine dep is default-features by default — nothing to do unless features are curated there)
- Modify: `crates/varve-engine/src/registries.rs` (register under the feature; update the builtin-names test)
- Modify: `crates/varve-engine/src/db.rs` (`EngineError::{CasUnsupported, WriterFenced}` — `WriterFenced` declared here, used in Task 8)
- Test: unit tests in `cas.rs` (memory store = probe-Supported CAS) and `store.rs`

**Interfaces:**
- Consumes: `ConditionalStore` (`put_if_absent`/`put_if_matches`/`CondPut`), `probe_conditional_put` + `ProbeVerdict` (slice 5), `Log::head`, `write_fence`/`FenceDoc` (Task 3), `generate_node_id` (Task 1).
- Produces:

```rust
// varve-storage/src/store.rs — addition to trait ConditionalStore
/// Reads the object together with its version tag (ETag); None if absent.
/// A backend that stores the object but returns no ETag yields `NoEtag`
/// (such a backend cannot pass the probe anyway).
async fn get_versioned(&self, key: &str) -> Result<Option<(Bytes, String)>, StorageError>;

// StorageError addition
#[error("object {0} has no ETag; conditional workflows are inexpressible")]
NoEtag(String),
```

Blanket impl via `object_store::ObjectStore::get` → `GetResult.meta.e_tag` (verify field name against object_store 0.13.2: `ObjectMeta::e_tag: Option<String>`); `Error::NotFound` → `Ok(None)`.

```rust
// coord/cas.rs
pub(crate) const LEASE_KEY: &str = "v1/lease.json";

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub(crate) struct LeaseDoc {
    pub holder: String,      // node_id
    pub address: String,     // last advertised; "" until advertise()
    pub epoch: u16,          // the holder's granted epoch
    pub heartbeat_us: i64,   // informational (ops); staleness uses ETag observation
}

pub(crate) struct CasFailover {
    store: Arc<dyn varve_storage::ObjectStore>,
    clock: Arc<dyn crate::clock::Clock>,
    node_id: String,
    heartbeat_interval: Duration,
    takeover_after: Duration,
    address: std::sync::Mutex<Option<String>>,
    held: tokio::sync::Mutex<Option<HeldLease>>,  // etag + epoch after acquire
}
struct HeldLease { etag: String, epoch: u16, valid_until: tokio::time::Instant }

pub(crate) struct CasFailoverFactory;  // name: "cas-failover"
```

```rust
// EngineError additions (db.rs)
#[error("storage backend cannot support cas-failover: {reason}. Use [coordinator] backend = \"designated-writer\" (spec §12, D5).")]
CasUnsupported { reason: String },
#[error("writer fenced: {0}")]
WriterFenced(String),
```

**`acquire(log)` algorithm (implement exactly; each numbered branch gets a unit test):**

1. Probe: `probe_conditional_put(store, "v1/probe/<clock-µs>-<node_id>")`. Verdict `Unsupported{reason}` / `Inconsistent{reason}` → `Err(CasUnsupported { reason })` (include the verdict kind in the reason string, e.g. `"probe verdict Inconsistent: create-if-absent …"`). The store must expose `conditional()` — `None` is `Unsupported` via the probe already.
2. Loop:
   a. `get_versioned(LEASE_KEY)`:
      - `None` → `head = log.head()`; `put_if_absent(LEASE_KEY, LeaseDoc { holder: me, address: current or "", epoch: head.epoch(), heartbeat_us: now })`:
        `Stored{etag}` → hold `{etag, epoch: head.epoch(), valid_until: acquire_start + takeover_after}`; return `WriterGrant { epoch: None }` (first holder continues the log's epoch — nothing to fence).
        `AlreadyExists` → continue loop (lost the creation race). `Unsupported` → `CasUnsupported` (defense-in-depth; probe should have caught it). `PreconditionFailed` → continue loop.
      - `Some((bytes, etag))` → decode `LeaseDoc` (JSON error → `EngineError::WriterAdvertisementJson` via `?`). If `doc.holder == self.node_id` → treat as ours-from-a-previous-life? NO — node_ids are per-instance; this cannot happen; fall through to standby.
   b. Standby double-observation: `tokio::time::sleep(self.takeover_after).await`, then `get_versioned(LEASE_KEY)` again:
      - `Some((_, etag2)) if etag2 == etag` → the holder made no heartbeat for a full local window: **seize**.
      - anything else (rotated etag, vanished lease) → continue loop (holder alive, or racing).
   c. Seize: `new_epoch = doc.epoch.checked_add(1).ok_or(EngineError::EpochExhausted)?`. `t0 = tokio::time::Instant::now()`. `put_if_matches(LEASE_KEY, LeaseDoc { holder: me, address: …, epoch: new_epoch, heartbeat_us: now }, &etag)`:
      - `Stored{etag: new_etag}` → won. `let head = log.head().await?` (fresh scan — this instance never appended, its cursor is unprimed). `write_fence(store, &FenceDoc { epoch: head.epoch(), fence_offset: head.offset(), fenced_by: me, fenced_at_us: now })`. Hold `{etag: new_etag, epoch: new_epoch, valid_until: t0 + takeover_after}`. Return `WriterGrant { epoch: Some(new_epoch) }`.
      - `PreconditionFailed`/`AlreadyExists` → lost the race → continue loop. `Unsupported` → `CasUnsupported`.

   Note the fence epoch is `head.epoch()`, not `doc.epoch` — if the dead holder never appended, the fence lands on the epoch where a zombie's cached cursor could actually write (decision 2's collision-impossibility argument).

**`heartbeat()`:** take `held` lock; `None` → `LeaseState::Lost("no lease held")`. Else `t0 = Instant::now()`; `put_if_matches(LEASE_KEY, refreshed doc (same holder/epoch/address, new heartbeat_us), &held.etag)`:
- `Stored{etag}` → update `held.etag`, `held.valid_until = t0 + takeover_after` → `LeaseState::ValidUntil(held.valid_until)`; also republish `v1/writer.json` (plain PUT) when an address is set, with `epoch = held.epoch`.
- `PreconditionFailed` → `Lost("lease seized by another writer")` (clear `held`).
- Transport `Err(_)` → do NOT extend: if `Instant::now() < held.valid_until` return `ValidUntil(held.valid_until)` (retry next tick), else `Lost("lease renewal failed past the takeover window: <err>")`.

**`advertise(address)`:** store the address; PUT `v1/writer.json` with `epoch = held.epoch` (0 if somehow unheld); do NOT touch the lease (heartbeat owns it).

- [x] **Step 1: Failing unit tests (memory store is CAS-Supported — slice-5 verified)**

```rust
// coord/cas.rs tests — construct CasFailover directly (fast intervals)
fn cas(store: &Arc<dyn ObjectStore>, node: &str, takeover_ms: u64) -> CasFailover { /* … */ }

#[tokio::test]
async fn first_acquire_creates_the_lease_and_continues_the_epoch() {
    let store = varve_storage::memory_store();
    let log: Arc<dyn Log> = Arc::new(varve_log::MemoryLog::new());
    let grant = cas(&store, "a", 200).acquire(&log).await.unwrap();
    assert!(grant.epoch.is_none());
    let doc: LeaseDoc = serde_json::from_slice(&store.get(LEASE_KEY).await.unwrap()).unwrap();
    assert_eq!(doc.holder, "a");
    assert_eq!(doc.epoch, 0);
}

#[tokio::test]
async fn a_stale_lease_is_seized_with_an_epoch_bump_and_a_fence() {
    let store = varve_storage::memory_store();
    let log: Arc<dyn Log> = Arc::new(varve_log::MemoryLog::new());
    log.append(vec![record(1), record(2)]).await.unwrap(); // head (0,2)
    let a = cas(&store, "a", 200);
    a.acquire(&log).await.unwrap();
    // a never heartbeats again → stale after 200 ms

    let started = std::time::Instant::now();
    let grant = cas(&store, "b", 200).acquire(&log).await.unwrap();
    assert_eq!(grant.epoch, Some(1));
    assert!(started.elapsed() < std::time::Duration::from_secs(5));
    let fences = crate::coord::fence::load_fences(store.as_ref()).await.unwrap();
    assert!(!fences.is_live(LogPosition::new(0, 2).unwrap()));
    assert!(fences.is_live(LogPosition::new(0, 1).unwrap()));
}

#[tokio::test]
async fn a_live_holder_keeps_the_standby_waiting_and_heartbeat_lost_after_seizure() {
    let store = varve_storage::memory_store();
    let log: Arc<dyn Log> = Arc::new(varve_log::MemoryLog::new());
    let a = cas(&store, "a", 300);
    a.acquire(&log).await.unwrap();

    // A heartbeats concurrently while B tries to acquire: B must not win
    // while heartbeats keep rotating the etag.
    let hb = tokio::spawn({ /* clone Arc<CasFailover> or loop a.heartbeat() every 100 ms for ~4 ticks */ });
    let b = cas(&store, "b", 300);
    let b_acquire = tokio::time::timeout(std::time::Duration::from_millis(450), b.acquire(&log));
    assert!(b_acquire.await.is_err(), "B must still be waiting while A heartbeats");
    hb.await.unwrap();

    // A stops: B wins; A's next heartbeat is Lost.
    let grant = b.acquire(&log).await.unwrap();
    assert_eq!(grant.epoch, Some(1));
    assert!(matches!(a.heartbeat().await, LeaseState::Lost(_)));
}

#[tokio::test]
async fn probe_failure_refuses_cas_naming_the_capability() {
    // PlainStore wrapper (probe.rs test pattern) exposes no conditional()
    let store: Arc<dyn ObjectStore> = Arc::new(PlainStore(varve_storage::memory_store()));
    let log: Arc<dyn Log> = Arc::new(varve_log::MemoryLog::new());
    let err = cas(&store, "a", 200).acquire(&log).await.unwrap_err();
    match err {
        EngineError::CasUnsupported { reason } => {
            assert!(reason.contains("conditional"), "{reason}");
        }
        other => panic!("expected CasUnsupported, got {other}"),
    }
}
```

Also a `store.rs` test: `get_versioned` on the memory store returns rotating etags and `Ok(None)` for absent keys.

- [x] **Step 2: Run to verify failure**

Run: `cargo test -p varve-storage store && cargo test -p varve-engine coord::cas`
Expected: FAIL.

- [x] **Step 3: Implement** (per the algorithm above), register `CasFailoverFactory` under `#[cfg(feature = "cas-failover")]`, update `builtins_cover_log_and_clock` to expect `["cas-failover", "designated-writer"]` (registry `names()` sorts — verify against `Registry::names`).

- [x] **Step 4: Run tests + feature matrix**

Run: `cargo test -p varve-engine && cargo check -p varve-engine --no-default-features`
Expected: PASS; the engine still compiles without the feature (registry simply lacks the name; config selecting it then yields the registry's unknown-name error listing available implementations).

- [x] **Step 5: Commit**

```bash
git add crates/varve-storage crates/varve-engine
git commit -m "feat: cas-failover coordinator with lease seizure and epoch fencing"
```

---

### Task 8: Writer ack gate + fatal writer states (closes slice-3 T9)

**Files:**
- Modify: `crates/varve-engine/src/writer.rs` (`WriterState.lease`, `flush` outcome, drain-on-fatal loop)
- Modify: `crates/varve-engine/src/db.rs` (assemble already passes the lease receiver from Task 6)
- Test: unit tests in `writer.rs`

**Interfaces:**
- Consumes: `LeaseState` watch receiver (Task 6), `EngineError::WriterFenced` (Task 7).
- Produces: writer-loop semantics Tasks 9/15 rely on:
  - Every ack is gated: lease `Lost(msg)` or `ValidUntil` in the past → the batch acks `Err(WriterFenced(msg))` and the writer becomes FATAL.
  - Post-durability apply failure → batch acks `Err(CommitFailed(msg))` and the writer becomes FATAL (was: keep serving).
  - FATAL: the loop publishes `ProgressState { follower_error: Some(format!("writer stopped: {reason}")), .. }` (so `/healthz` degrades and `Db::follower_error()` reports it), then drains every subsequent command with `Err(WriterFenced(reason))` until the channel closes. No block flush after fatal.

Implementation shape:

```rust
enum FlushOutcome { Continue, Fatal(String) }

fn lease_block(lease: &watch::Receiver<LeaseState>) -> Option<String> {
    match &*lease.borrow() {
        LeaseState::Unfenced => None,
        LeaseState::ValidUntil(deadline) if tokio::time::Instant::now() < *deadline => None,
        LeaseState::ValidUntil(_) => Some("lease expired before ack".to_string()),
        LeaseState::Lost(reason) => Some(reason.clone()),
    }
}

async fn flush(state: &mut WriterState, mut staged: Vec<Staged>) -> FlushOutcome {
    if let Some(reason) = lease_block(&state.lease) {
        return fence_all(staged, reason); // acks WriterFenced, returns Fatal
    }
    match state.log.append(records).await {
        Ok(first) => {
            if let Some(reason) = lease_block(&state.lease) {
                // durable but possibly beyond the fence — never ack
                return fence_all(staged, reason);
            }
            /* existing watermark/apply/ack path, except: */
            match apply(state, &mut staged) {
                Ok(()) => { /* progress + Ok acks */ FlushOutcome::Continue }
                Err(msg) => { /* CommitFailed acks */ FlushOutcome::Fatal(format!("apply failed after durable append: {msg}")) }
            }
        }
        Err(e) => { /* existing CommitFailed acks */ FlushOutcome::Continue } // pre-durability failure keeps serving (unchanged)
    }
}
```

`run_batch` and the loop propagate the outcome; on `Fatal(reason)` the loop enters `drain(rx, reason)`.

- [x] **Step 1: Failing unit tests**

```rust
#[tokio::test]
async fn a_lost_lease_fences_acks_and_stops_the_writer() {
    let (lease_tx, lease_rx) = watch::channel(LeaseState::Unfenced);
    let (sender, live) = spawn_with_lease(lease_rx); // extend the test-module spawn helper
    submit(&sender, "INSERT (:P {_id: 1})").await.unwrap().unwrap();

    lease_tx.send_replace(LeaseState::Lost("seized in test".into()));
    let fenced = submit(&sender, "INSERT (:P {_id: 2})").await.unwrap();
    assert!(matches!(fenced, Err(EngineError::WriterFenced(reason)) if reason.contains("seized")));

    // the writer drains — later submissions also fence, and nothing applied
    let again = submit(&sender, "INSERT (:P {_id: 3})").await.unwrap();
    assert!(matches!(again, Err(EngineError::WriterFenced(_))));
    assert_eq!(live_event_count(&live), 1);
}

#[tokio::test]
async fn an_expired_lease_deadline_blocks_acks() {
    let (_lease_tx, lease_rx) = watch::channel(LeaseState::ValidUntil(
        tokio::time::Instant::now() - std::time::Duration::from_millis(1),
    ));
    let (sender, _live) = spawn_with_lease(lease_rx);
    let fenced = submit(&sender, "INSERT (:P {_id: 1})").await.unwrap();
    assert!(matches!(fenced, Err(EngineError::WriterFenced(_))));
}

#[tokio::test]
async fn apply_failure_after_durable_append_is_fatal() {
    // Build a WriterState whose staged events violate LiveTable monotonicity
    // (system_from going backwards) by calling flush() directly with a
    // hand-built Staged — the writer.rs test module already constructs
    // WriterState directly for such cases.
    let outcome = flush(&mut state, vec![bad_staged]).await;
    assert!(matches!(outcome, FlushOutcome::Fatal(_)));
    // the ack carried CommitFailed:
    assert!(matches!(ack_rx.await.unwrap(), Err(EngineError::CommitFailed(_))));
}
```

(Existing tests `resolve_errors_are_acked_and_the_loop_survives` and the failed-append rollback tests must stay green — resolve errors and PRE-durability append failures still keep the loop alive.)

- [x] **Step 2: Run to verify failure**

Run: `cargo test -p varve-engine writer`
Expected: FAIL.

- [x] **Step 3: Implement** per the shape above. The default `spawn` used by `Db::memory()`-style tests passes an `Unfenced` watch so all existing behavior is unchanged.

- [x] **Step 4: Run engine suite**

Run: `cargo test -p varve-engine`
Expected: PASS.

- [x] **Step 5: Commit**

```bash
git add crates/varve-engine
git commit -m "feat: lease-gated acks and fatal writer states"
```

---

### Task 9: Failover proven end-to-end — integration test, live-backend gates, demo

The roadmap's failover exit criteria: kill writer → standby takes over < 10 s, zero acked-tx loss, zombie appends provably ignored; Garage refuses cas-failover with an actionable error. Whole-workspace green checkpoint.

**Files:**
- Create: `crates/varve-engine/tests/failover.rs`
- Create: `crates/varve-testkit/tests/cas_failover_backends.rs` (env-gated live Garage/MinIO)
- Create: `crates/varve/examples/failover.rs` (the slice demo)
- Modify: `crates/varve/src/lib.rs` if the demo needs additional re-exports
- Test: this task IS tests + demo

**Interfaces:**
- Consumes: everything from Tasks 1–8; `SharedStoreFactory` pattern (Task 6); `ObjectStoreLog` as the zombie handle (varve-log is already a varve-engine dep; add it as a dev-dependency of `varve-engine` if `[dev-dependencies]` lacks it); `varve-testkit/src/backends.rs` container rig (`VARVE_S3_BACKENDS` gating, garage/minio helpers — reuse the existing start/bucket helpers exactly as `backend_matrix.rs` does).
- Produces: `just`-runnable proof; `cargo run --release --example failover -p varve` demo for STATUS.md.

- [x] **Step 1: The in-process failover integration test (failing until it isn't)**

```rust
// crates/varve-engine/tests/failover.rs
// Shared memory store (probe verdict: Supported — slice-5 verified), two
// sequential writer Dbs with cas-failover, a real query-node Db, and a
// zombie log handle primed before the takeover.
// Helpers repeated here so this task stands alone (they also appear in
// Tasks 3/6): SharedStoreFactory + shared_registries + put_record.

struct SharedStoreFactory(Arc<dyn ObjectStore>);
impl ComponentFactory<dyn ObjectStore> for SharedStoreFactory {
    fn name(&self) -> &'static str { "shared" }
    fn build(&self, _: &ConfigSection, _: &BuildContext) -> Result<Arc<dyn ObjectStore>, RegistryError> {
        Ok(Arc::clone(&self.0))
    }
}

fn shared_registries(store: &Arc<dyn ObjectStore>) -> Registries {
    let mut r = Registries::with_builtins();
    r.storage.register(Box::new(SharedStoreFactory(Arc::clone(store)))).unwrap();
    r
}

fn cas_config() -> Config {
    // Config::from_str if it exists; else write to a tempfile + from_file
    // (same adaptation note as Task 6 — tests are the contract).
    Config::from_str(r#"
[storage]
backend = "shared"
[log]
backend = "object-store"
[coordinator]
backend = "cas-failover"
heartbeat_interval_ms = 100
takeover_after_ms = 300
"#).unwrap()
}

// put_record(tx_id, id) — identical to Task 3's fenced_recovery.rs helper
// (one encoded :Chaos Put event wrapped in a LogRecord); repeat it here.

#[tokio::test]
async fn failover_preserves_acked_txs_and_fences_the_zombie() {
    let store: Arc<dyn ObjectStore> = varve_storage::memory_store();
    let registries = shared_registries(&store);

    // Writer A: commit 3 acked txs.
    let a = Db::open_with(&cas_config(), &registries).await.unwrap();
    for n in 1..=3 { a.execute(&format!("INSERT (:P {{_id: {n}}})")).await.unwrap(); }

    // Prime the zombie: a raw log handle whose cached cursor predates takeover.
    let zombie = varve_log::ObjectStoreLog::new(Arc::clone(&store));
    let stale_head = zombie.head().await.unwrap();
    assert_eq!(stale_head, varve_types::LogPosition::new(0, 3).unwrap());

    // A "crashes": drop stops its heartbeat WITHOUT releasing the lease.
    drop(a);

    // Writer B: standby-acquires, seizes, fences, recovers — measure it.
    let started = std::time::Instant::now();
    let b = Db::open_with(&cas_config(), &registries).await.unwrap();
    let takeover = started.elapsed();
    assert!(takeover < std::time::Duration::from_secs(10), "takeover took {takeover:?}");

    // Zero acked-tx loss.
    let rows = b.query("MATCH (p:P) RETURN p._id").await.unwrap();
    assert_eq!(count_rows(&rows), 3);

    // B commits in the new epoch.
    b.execute("INSERT (:P {_id: 4})").await.unwrap();

    // The zombie's late append lands at its stale position — durable, DEAD.
    zombie.append(vec![put_record(999, 999)]).await.unwrap(); // helper from Task 3's test
    assert!(!store.list("v1/log/0000").await.unwrap().is_empty());

    // B (writer+query) never sees it…
    let rows = b.query("MATCH (p:P) RETURN p._id").await.unwrap();
    let ids = collect_ids(&rows);
    assert_eq!(ids, vec![1, 2, 3, 4], "zombie _id 999 must be invisible");
    // …verify walks clean over it…
    b.verify().await.unwrap();

    // …and a fresh query-only node agrees (follower jumps the fence).
    let q_cfg = /* cas_config() minus [coordinator], plus "[node]\nroles = [\"query\"]" */;
    let q = Db::open_with(&q_cfg, &registries).await.unwrap();
    let receipt_basis = /* basis from b's last receipt */;
    let rows = q.query("MATCH (p:P) RETURN p._id").basis(receipt_basis).await.unwrap();
    assert_eq!(collect_ids(&rows), vec![1, 2, 3, 4]);
}

#[tokio::test]
async fn the_seized_writer_fences_instead_of_acking() {
    // A holds the lease with SLOW heartbeats (interval 5000, takeover 300 is
    // invalid config — use interval 150/takeover 300 and STOP A's heartbeat
    // by aborting it: simplest honest construction is heartbeat_interval_ms
    // = 150, takeover_after_ms = 300, then suspend A's heartbeat by seizing
    // from B and observing A's next execute()):
    let store: Arc<dyn ObjectStore> = varve_storage::memory_store();
    let registries = shared_registries(&store);
    let a = Db::open_with(&cas_config(), &registries).await.unwrap();
    a.execute("INSERT (:P {_id: 1})").await.unwrap();

    // Seize the lease out from under A directly (raw CAS on the store),
    // exactly what a competing standby does:
    seize_lease_directly(&store).await; // get_versioned + put_if_matches with a foreign holder/epoch+1

    // A's heartbeat task discovers the loss within ~2 intervals; then:
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        match a.execute("INSERT (:P {_id: 2})").await {
            Err(EngineError::WriterFenced(_)) => break,
            Ok(_) if std::time::Instant::now() < deadline => {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            other => panic!("expected WriterFenced before the deadline, got {other:?}"),
        }
    }
    assert!(a.follower_error().is_some(), "fatal writer reports on the progress watch");
}
```

Notes for the implementer: `count_rows`/`collect_ids` are 5-line Arrow helpers (Int64Array downcast — copy from any existing engine test). `put_record` builds a `LogRecord` with one encoded `Event` (Task 3's helper). The wait-loop in the second test tolerates acks that squeeze in before the heartbeat notices — the assertion is that fencing HAPPENS, not that it is instant; the acked `_id: 2` rows landing before the fence are legitimate (pre-seizure-window semantics are covered by decision 3's window math with real container latencies, not memory-store microseconds).

- [x] **Step 2: Run the failover test**

Run: `cargo test -p varve-engine --test failover -- --test-threads=1 --nocapture`
Expected: PASS (after fixing whatever it flushes out — this is the integration checkpoint for Tasks 1–8).

- [x] **Step 3: Live-backend gates (Garage refusal + MinIO takeover)**

```rust
// crates/varve-testkit/tests/cas_failover_backends.rs — mirror
// backend_matrix.rs's env gating and container lifecycle exactly.
// For each backend in VARVE_S3_BACKENDS (skip silently if unset):
//   garage   → Db::open_with(cas config over the live bucket) must fail with
//              EngineError::CasUnsupported; assert the message contains the
//              probe reason ("precondition ignored" family) — the roadmap's
//              "actionable error naming the backend capability".
//   minio    → run the full takeover flow from Step 1 (writer A → drop →
//              writer B < 10 s, acked txs intact) over the live bucket.
```

Run: `VARVE_S3_BACKENDS=garage,minio cargo test -p varve-testkit --test cas_failover_backends -- --nocapture --test-threads=1`
Expected: PASS locally with docker; silently skipped in plain `just check`.

- [x] **Step 4: The demo**

`crates/varve/examples/failover.rs` — same shape as the integration test, printing a timeline:

```text
writer A acquired lease (epoch 0), committed 3 txs
writer A crashed (heartbeats stopped)
writer B took over in 412ms: epoch 1, fence 0@3
zombie append landed at (0,3) — IGNORED by B, query node, and verify
final row count everywhere: 4
```

Uses only `varve` public API (`Db`, `Registries`, `Config`, a local `SharedStoreFactory`) plus `varve-log`/`varve-storage`/`varve-types` as example-scoped dev-dependencies of the `varve` crate (check `crates/varve/Cargo.toml` `[dev-dependencies]`; add what is missing). Exit non-zero on any assertion failure so CI can run it.

Run: `cargo run --release --example failover -p varve`
Expected: the timeline above, exit 0.

- [x] **Step 5: Whole-workspace checkpoint**

Run: `cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace -- --test-threads=1`
Expected: ALL PASS.

- [x] **Step 6: Commit**

```bash
git add crates/varve-engine crates/varve-testkit crates/varve
git commit -m "test: end-to-end cas failover with zombie fencing, backend gates, and demo"
```

---

### Task 10: Backpressure — configurable queue, `try_execute_as`, HTTP 429, client cleanup

**Files:**
- Modify: `crates/varve-engine/src/node.rs` (`NodeTuning.submission_queue_len`)
- Modify: `crates/varve-engine/src/writer.rs` (`WriterConfig.queue_len`, real `try_submit`)
- Modify: `crates/varve-engine/src/db.rs` (`try_execute_as`, `EngineError::Backpressure`, open_with plumbs the queue length)
- Modify: `crates/varve-server/src/http/handlers.rs` (`tx` handler), `crates/varve-server/src/error.rs`/`handlers.rs` `mapped()` (429), `crates/varve-server/src/api.rs` if the error DTO needs nothing new (it doesn't — `code: "backpressure"`)
- Modify: `crates/varve-cli/src/remote.rs` (delete the dead `allow_redirect` parameter of `send_mutation`; surface 429 with a retry hint)
- Modify: `crates/varve-server/tests/process_scale_out.rs` (wrap the reader loop in `tokio::time::timeout` — the slice-9 deferred fix)
- Test: `writer.rs`/`db.rs` unit tests; `varve-server/tests/http_api.rs` addition

**Interfaces:**
- Consumes: `WriterHandle` mpsc (writer.rs:60), existing `execute_as` (db.rs:1075).
- Produces:

```rust
// EngineError addition
#[error("writer submission queue is full; retry")]
Backpressure,

// writer.rs — WriterConfig gains
pub queue_len: usize,          // default 256; spawn_writer uses it instead of SUBMISSION_QUEUE_LEN
// WriterHandle — replaces the #[cfg(test)] try_submit
pub fn try_submit(&self, submission: Submission) -> Result<(), EngineError>; // TrySendError::Full → Backpressure, Closed → WriterUnavailable

// db.rs
/// Like execute_as, but returns Backpressure instead of waiting when the
/// writer's submission queue is full. The server's 429 path.
pub async fn try_execute_as(
    &self,
    gql: &str,
    params: &BTreeMap<String, Value>,
    user: &str,
) -> Result<TxReceipt, EngineError>;
```

`[node] submission_queue_len` (default 256; `0` → validation error in `NodeTuning::validate`). Delete the `SUBMISSION_QUEUE_LEN` const (config owns it now); `Db::memory()`/`local()` pass 256 via `WriterConfig::default()`.

HTTP mapping (`mapped()` in handlers.rs): `ServerError::Engine(EngineError::Backpressure)` → `StatusCode::TOO_MANY_REQUESTS`, code `"backpressure"`, and a `Retry-After: 1` header on the response. `EngineError::WriterFenced(_)` → 503, code `"writer_fenced"`. The `tx` handler switches `execute_as` → `try_execute_as`.

- [x] **Step 1: Failing tests**

```rust
// writer.rs test — a full queue rejects instead of waiting
#[tokio::test]
async fn try_submit_on_a_full_queue_is_backpressure() {
    // BlockingAppendLog holds the loop inside append; queue_len = 1
    // (spawn with WriterConfig { queue_len: 1, window: ZERO, .. }).
    // 1st submission: enters append and blocks. 2nd: sits in the queue.
    // 3rd: try_submit must return Err(Backpressure) immediately.
    /* … existing BlockingAppendLog machinery … */
    assert!(matches!(handle.try_submit(third), Err(EngineError::Backpressure)));
    release.notify_waiters(); // let the loop drain; assert 1st+2nd ack Ok
}

// db.rs test
#[tokio::test]
async fn try_execute_as_maps_queue_full_to_backpressure() { /* same rig through Db if reachable, else the writer-level test suffices — then this test just asserts try_execute_as works on an idle Db */ }
```

```rust
// varve-server/tests/http_api.rs addition — follow the file's existing
// router-construction pattern (http_router + tower::ServiceExt oneshot)
#[tokio::test]
async fn tx_returns_429_with_retry_after_when_the_writer_queue_is_full() {
    // Build the HttpContext over a Db whose writer is blocked (testkit
    // BlockingAppendLog is engine-internal — instead, saturate a real Db:
    // open with [node] submission_queue_len = 1 and [log] group_commit_window_ms
    // high, fire N concurrent /v1/tx requests, and assert AT LEAST ONE 429
    // with Retry-After while the rest are 200). Deterministic alternative if
    // flaky: unit-test the mapped() 429 arm directly and keep this as the
    // header-shape assertion on a mocked EngineError::Backpressure response.
    let response = mapped_backpressure_response();
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(response.headers().get("retry-after").unwrap(), "1");
}
```

(Prefer the deterministic `mapped()`-level test as the committed contract; the concurrent saturation variant is allowed but must not flake — if it can't be made deterministic, don't ship it.)

- [x] **Step 2: Run to verify failure** — `cargo test -p varve-engine writer && cargo test -p varve-server -- --test-threads=1`. Expected: FAIL.

- [x] **Step 3: Implement** (per Interfaces). In `varve-cli/src/remote.rs`, delete the `allow_redirect: bool` parameter from `send_mutation` (its `false` branch is unreachable — slice-9 review finding); update the one call site; extend the non-2xx error path so a 429 body surfaces as `"server is applying backpressure (429): retry"` — check the file's existing error enum/message style and match it. In `process_scale_out.rs`, wrap the reader loop future in `tokio::time::timeout(Duration::from_secs(120), …)` with an `expect`-style panic message naming the regression risk.

- [x] **Step 4: Run** — `cargo test -p varve-engine && cargo test -p varve-server -- --test-threads=1 && cargo test -p varve-cli -- --test-threads=1`. Expected: PASS.

- [x] **Step 5: Commit**

```bash
git add crates/varve-engine crates/varve-server crates/varve-cli
git commit -m "feat: configurable submission queue with 429 backpressure"
```

---

### Task 11: Live-index memory watermark forces early flush

**Files:**
- Modify: `crates/varve-index/src/event.rs` (or wherever `Event` lives — grep `pub struct Event` in varve-index) — `Event::approx_bytes`
- Modify: `crates/varve-index/src/live.rs` (`LiveTable::approx_bytes`)
- Modify: `crates/varve-engine/src/state.rs` (`TableState::live_bytes`, `GraphsState::live_bytes`)
- Modify: `crates/varve-engine/src/writer.rs` (`WriterConfig.max_live_bytes`, flush trigger)
- Modify: `crates/varve-engine/src/db.rs` (`StorageTuning.max_live_bytes: ByteSize`, default `"512MiB"`)
- Test: unit tests in `live.rs`; engine e2e in `crates/varve-engine/tests/` (alongside the existing flush-trigger config test — find the test that opens `Db::open(blocks_config(dir, N))`; mirror it)

**Interfaces:**
- Produces:

```rust
// varve-index
impl Event {
    /// Rough in-memory footprint: fixed overhead + label and doc byte
    /// lengths. Used only for the flush watermark — never for correctness.
    pub fn approx_bytes(&self) -> usize;  // 64 + Σ label.len() + Σ (key.len() + value approx: Str/Bytes = len, other = 8)
}
impl LiveTable {
    pub fn approx_bytes(&self) -> usize;  // running sum maintained in append(); reset with the table
}

// engine
impl TableState { pub fn live_bytes(&self) -> usize; }   // nodes + edges
impl GraphsState { pub fn live_bytes(&self) -> usize; }
// WriterConfig gains: pub max_live_bytes: usize (default 512 MiB)
```

Writer-loop trigger (writer.rs:253): `if live_rows(&state) >= cfg.max_block_rows || live_bytes(&state) >= cfg.max_live_bytes { flush_block(...) }`.

- [x] **Step 1: Failing tests**

```rust
// live.rs
#[test]
fn approx_bytes_grows_with_appends_and_reflects_payload_size() {
    let mut table = LiveTable::new();
    assert_eq!(table.approx_bytes(), 0);
    table.append(small_event()).unwrap();
    let small = table.approx_bytes();
    table.append(event_with_a_1kb_string()).unwrap();
    assert!(table.approx_bytes() >= small + 1024);
}
```

```rust
// engine e2e (tests/…): [storage] max_live_bytes = "4KiB", max_block_rows = 1_000_000,
// flush_interval_ms = 0 → insert a handful of ~1 KiB docs → a manifest appears
// (list v1/blocks) even though the row trigger is far away.
#[tokio::test]
async fn live_bytes_watermark_forces_an_early_flush() { /* … */ }
```

- [x] **Step 2: Run to verify failure** — `cargo test -p varve-index live && cargo test -p varve-engine`. Expected: FAIL.

- [x] **Step 3: Implement.** `ByteSize` parses only quoted IEC strings (slice-9 decision) — the default comes from `ByteSize::from_bytes(512 * 1024 * 1024)` in the serde default fn, config overrides with e.g. `"4KiB"`.

- [x] **Step 4: Run** — `cargo test -p varve-index && cargo test -p varve-engine`. Expected: PASS.

- [x] **Step 5: Commit**

```bash
git add crates/varve-index crates/varve-engine
git commit -m "feat: live-index memory watermark triggers early block flush"
```

---

### Task 12: Engine metrics — atomics, cache stats, log lag, Prometheus completion

The spec-§12 metric list: ingest rate, log lag per node, cache hit ratios, compaction debt, query latency histograms (HTTP histograms exist; engine counters land here). I/O-free by design decision 10. Also folds in the slice-5 disk-cache TOCTOU fix.

**Files:**
- Create: `crates/varve-engine/src/metrics.rs` (+ `mod metrics;` + re-exports in `varve-engine/src/lib.rs` and `varve/src/lib.rs`)
- Modify: `crates/varve-storage/src/cache.rs` (`CacheStats`, instrumented `CachedStore`)
- Modify: `crates/varve-storage/src/disk.rs` (`open`: per-file `fs::metadata` race → skip the file, `.ok()` style, instead of aborting the whole open)
- Modify: `crates/varve-engine/src/node.rs` (`ProgressState`/`NodeStatus` + `log_head: LogPosition`)
- Modify: `crates/varve-engine/src/writer.rs` (increment counters; publish `log_head`), `follower.rs` (publish `log_head`), `db.rs` (`Db::metrics`, DbInner holds `Arc<EngineMetrics>` + cache stats vec; `try_execute_as` increments `backpressure_rejections`)
- Modify: `crates/varve-server/src/metrics.rs` (`MetricsSink::set_engine`, Prometheus gauges), `http/handlers.rs` (`metrics` + `status` handlers), `api.rs` (`StatusResponse.log_head_position`)
- Test: unit tests per file; a `/metrics` end-to-end assertion in `varve-server/tests/http_api.rs`

**Interfaces:**

```rust
// varve-storage/src/cache.rs
#[derive(Debug, Default)]
pub struct CacheStats { pub hits: std::sync::atomic::AtomicU64, pub misses: std::sync::atomic::AtomicU64 }
impl CachedStore {
    /// Instrumented constructor; the plain `new` delegates with fresh,
    /// unobserved stats so existing call sites compile unchanged.
    pub fn with_stats(inner: Arc<dyn ObjectStore>, cache: Arc<dyn CacheTier>, stats: Arc<CacheStats>) -> CachedStore;
}
// get/get_range: cache hit → stats.hits += 1; miss (backend fetch) → misses += 1.
```

```rust
// varve-engine/src/metrics.rs
#[derive(Debug, Default)]
pub(crate) struct EngineMetrics {
    pub txs_committed: AtomicU64,
    pub events_committed: AtomicU64,
    pub commit_failures: AtomicU64,      // pre-durability append failures
    pub flush_blocks: AtomicU64,
    pub flush_failures: AtomicU64,
    pub compaction_runs: AtomicU64,
    pub backpressure_rejections: AtomicU64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CacheTierStats { pub tier: String, pub hits: u64, pub misses: u64 }

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EngineMetricsSnapshot {
    pub txs_committed: u64,
    pub events_committed: u64,
    pub commit_failures: u64,
    pub flush_blocks: u64,
    pub flush_failures: u64,
    pub compaction_runs: u64,
    pub backpressure_rejections: u64,
    pub live_rows: u64,
    pub live_bytes: u64,
    pub persisted_tries: u64,
    /// Σ_scope max(0, tries(scope) − 1) — I/O-free compaction-debt proxy.
    pub compaction_debt_tries: u64,
    pub cache_tiers: Vec<CacheTierStats>,
}

impl Db {
    /// I/O-free: atomics + one read-lock pass over the in-memory inventory.
    pub fn metrics(&self) -> EngineMetricsSnapshot;
}
```

```rust
// node.rs
pub struct AppliedProgress { pub tx_id: u64, pub log_position: LogPosition }  // unchanged
pub(crate) struct ProgressState { pub applied: AppliedProgress, pub log_head: LogPosition, pub follower_error: Option<String> }
pub struct NodeStatus { /* existing */ pub log_head: LogPosition, /* … */ }
/// Same-epoch offset difference; cross-epoch transient approximation.
pub fn log_lag_records(applied: LogPosition, head: LogPosition) -> u64;
```

```rust
// varve-server/src/metrics.rs — MetricsSink addition
fn set_engine(&self, snapshot: &varve::EngineMetricsSnapshot);
```

Prometheus additions (all `IntGauge` set from the snapshot each scrape; `_total` names are monotone by construction — document in docs/ops/metrics.md): `varve_txs_committed_total`, `varve_events_committed_total`, `varve_commit_failures_total`, `varve_flush_blocks_total`, `varve_flush_failures_total`, `varve_compaction_runs_total`, `varve_backpressure_rejections_total`, `varve_live_rows`, `varve_live_bytes`, `varve_persisted_tries`, `varve_compaction_debt_tries`, `varve_log_head_position`, `varve_log_lag_records`, and `IntGaugeVec` `varve_cache_hits_total{tier}` / `varve_cache_misses_total{tier}`.

Recording points: `flush()` success → `txs_committed += batch`, `events_committed += Σ events`; pre-durability append failure → `commit_failures += 1`; `flush_block` Ok-with-manifest → `flush_blocks += 1`, Err → `flush_failures += 1`; `compact_once` with jobs → `compaction_runs += 1`; `try_execute_as` Backpressure → `backpressure_rejections += 1`. `WriterState` carries the `Arc<EngineMetrics>`.

`log_head` publication: writer `flush()` publishes `log_head = durable_watermark` (lag 0 by construction); follower publishes `log_head = max(last read position + 1, manifest watermark seen in the gap check)` and, on fence jumps, the jumped cursor.

Handlers: `metrics` adds `c.frontend.metrics.set_engine(&c.frontend.db.metrics());` before `encode()`; `status` DTO gains `log_head_position: u64`.

- [x] **Step 1: Failing tests** — representative set (write all):

```rust
// cache.rs: hits/misses count through CachedStore::with_stats (get twice: 1 miss, 1 hit)
// metrics.rs: Db::memory() → execute 2 txs → metrics().txs_committed == 2, live_rows == 2
//             flush via tiny max_live_bytes config → flush_blocks == 1
// node.rs: log_lag_records(same-epoch (0,3)→(0,7)) == 4; cross-epoch ((0,3)→(1,2)) == 3 (head.offset+1)
// varve-server http_api.rs: GET /metrics body contains "varve_log_lag_records" and "varve_cache_hits_total"
// db.rs: try_execute_as rejection increments backpressure_rejections
```

- [x] **Step 2: Run to verify failure** — `cargo test -p varve-storage && cargo test -p varve-engine metrics`. Expected: FAIL.

- [x] **Step 3: Implement**, including the `disk.rs` TOCTOU one-liner (metadata error → skip file with `continue`, matching the surrounding best-effort sweep). `Db::metrics` computes `live_rows/live_bytes/persisted_tries/compaction_debt_tries` under one read lock (per-scope trie counts: nodes.tries, edges.tries, adj_out, adj_in per graph).

- [x] **Step 4: Run** — `cargo test -p varve-storage && cargo test -p varve-engine && cargo test -p varve-server -- --test-threads=1`. Expected: PASS.

- [x] **Step 5: Commit**

```bash
git add crates/varve-storage crates/varve-engine crates/varve-server crates/varve
git commit -m "feat: engine metrics, cache hit ratios, and log lag through MetricsSink"
```

---

### Task 13: `tracing` spans across the write, follow, and query paths

Roadmap: spans across submit→commit→apply→flush and parse→plan→execute. Also the slice-4 deferred flush-failure observability (error events on the previously-silent retry path).

**Files:**
- Modify: `crates/varve-engine/Cargo.toml` (`tracing = { workspace = true }`)
- Modify: `crates/varve-engine/src/writer.rs`, `flush.rs`, `follower.rs`, `db.rs`
- Test: `crates/varve-engine/tests/tracing_spans.rs` (query path, scoped subscriber) + a unit test in `writer.rs` (writer fns called directly under a scoped subscriber)

**Interfaces:**
- Consumes: `tracing` 0.1.44, `tracing-subscriber` 0.3.23 (dev-dep for the test collector).
- Produces span names (STABLE — docs/ops/metrics.md documents them): `varve.submit` (execute path, field `user`), `varve.resolve` (field `tx_id`), `varve.commit` (fields `batch`, `first_position`), `varve.apply` (field `batch`), `varve.flush_block` (field `block_id`), `varve.compact`, `varve.follower.apply` (fields `from`, `applied`), `varve.query.parse`, `varve.query.plan`, `varve.query.execute` (field `graph`).
- Error events: `tracing::error!` on append failure, apply failure (fatal), flush_block failure (with the retry-at-next-trigger note), lease loss; `tracing::warn!` on heartbeat PUT failure (Task 5's ignored error gets its warn here).

Span mechanics: synchronous sections use `let _g = tracing::info_span!("varve.commit", batch = n).entered();` — but NEVER hold an `EnteredSpan` across an `.await` (clippy/tracing footgun); for async sections wrap the future with `tracing::Instrument::instrument(fut, span)`. `Db::execute_as` instruments the submit+ack future; `query_stream_impl` wraps parse (sync, entered) and the plan/execute futures (instrument).

- [x] **Step 1: Failing test**

```rust
// crates/varve-engine/tests/tracing_spans.rs
use std::sync::{Arc, Mutex};
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::{registry, Layer};

#[derive(Clone, Default)]
struct SpanNames(Arc<Mutex<Vec<&'static str>>>);
impl<S: tracing::Subscriber> Layer<S> for SpanNames {
    fn on_new_span(&self, attrs: &tracing::span::Attributes<'_>, _: &tracing::span::Id, _: Context<'_, S>) {
        self.0.lock().unwrap().push(attrs.metadata().name());
    }
}

#[tokio::test]
async fn query_path_emits_parse_plan_execute_spans() {
    let names = SpanNames::default();
    let subscriber = registry().with(names.clone());
    let _guard = tracing::subscriber::set_default(subscriber);

    let db = varve::Db::memory();
    db.execute("INSERT (:P {_id: 1})").await.unwrap();
    db.query("MATCH (p:P) RETURN p._id").await.unwrap();

    let seen = names.0.lock().unwrap().clone();
    for expected in ["varve.query.parse", "varve.query.plan", "varve.query.execute"] {
        assert!(seen.contains(&expected), "missing span {expected}; saw {seen:?}");
    }
}
```

(`set_default` is thread-local: the query path runs on the calling task, so this is deterministic. Writer-loop spans run on the SPAWNED task — assert them in a `writer.rs` unit test that calls `resolve_program`/`flush` directly under the scoped subscriber; do NOT try to observe spawned-task spans through `set_default`.)

- [x] **Step 2: Run to verify failure** — `cargo test -p varve-engine --test tracing_spans`. Expected: FAIL (no spans).

- [x] **Step 3: Implement spans + error events** per the name list. Keep fields cheap (no GQL text in fields — lengths/ids only).

- [x] **Step 4: Run** — `cargo test -p varve-engine && cargo clippy --workspace --all-targets -- -D warnings` (watch for `await_holding` on entered spans). Expected: PASS.

- [x] **Step 5: Commit**

```bash
git add crates/varve-engine
git commit -m "feat: tracing spans across submit-commit-apply-flush and parse-plan-execute"
```

---

### Task 14: OTLP export behind `MetricsSink` (`otlp` builtin, feature `otel`)

Hand-rolled OTLP/HTTP JSON push of the Prometheus registry — decision 11.

**Files:**
- Modify: `crates/varve-server/Cargo.toml` (`[features] otel = ["dep:reqwest"]`, add `otel` to `default`; `reqwest = { workspace = true, optional = true }`)
- Create: `crates/varve-server/src/metrics/otlp.rs` (move `metrics.rs` to `metrics/mod.rs`; `#[cfg(feature = "otel")] mod otlp;`)
- Modify: `crates/varve-server/src/lib.rs` (`ServerRegistries::with_builtins` registers `"otlp"` under the feature; update the builtin-names test to be feature-conditional)
- Test: converter unit tests; push integration test (axum capture server) gated `#[cfg(all(feature = "http", feature = "otel"))]`

**Interfaces:**
- Consumes: `PrometheusMetrics` (delegation target), `prometheus::proto::MetricFamily` (from `registry.gather()`), `reqwest::Client`, `serde_json`.
- Produces:

```rust
pub struct OtlpMetrics { /* inner: PrometheusMetrics, state: Arc<…>, pusher handle (abort-on-drop) */ }
// impl MetricsSink for OtlpMetrics: observe_request/set_progress/set_engine/encode → delegate to inner

/// Pure converter: Prometheus families → one OTLP/HTTP JSON ExportMetricsServiceRequest.
/// counter → sum { isMonotonic: true, aggregationTemporality: 2 /* CUMULATIVE */ },
/// gauge → gauge, histogram → histogram (bucketCounts/explicitBounds/count/sum).
/// Labels → attributes [{key, value: {stringValue}}]. timeUnixNano on every point.
pub(crate) fn families_to_otlp_json(
    families: &[prometheus::proto::MetricFamily],
    time_unix_nano: u128,
) -> serde_json::Value;
```

Config: `[metrics.otlp] endpoint` (required — factory error names it), `push_interval_ms` (default 10000). Factory name `"otlp"`. The factory spawns the pusher task (`tokio::spawn` — document that building the otlp sink requires an ambient tokio runtime, which `varved` and every `#[tokio::test]` provide); each tick: `gather()` → convert → `client.post(endpoint).json(&body).send()`; failures are `tracing::warn!`, never fatal; the handle aborts on drop.

- [x] **Step 1: Failing converter test**

```rust
#[test]
fn converts_counters_gauges_and_histograms_to_otlp_shapes() {
    let registry = prometheus::Registry::new();
    let counter = prometheus::IntCounterVec::new(
        prometheus::Opts::new("varve_http_requests_total", "help"), &["route"]).unwrap();
    registry.register(Box::new(counter.clone())).unwrap();
    counter.with_label_values(&["/v1/query"]).inc_by(3);
    let gauge = prometheus::IntGauge::new("varve_live_rows", "help").unwrap();
    registry.register(Box::new(gauge.clone())).unwrap();
    gauge.set(42);

    let body = families_to_otlp_json(&registry.gather(), 1_000);
    let metrics = &body["resourceMetrics"][0]["scopeMetrics"][0]["metrics"];
    let requests = metrics.as_array().unwrap().iter()
        .find(|m| m["name"] == "varve_http_requests_total").unwrap();
    assert_eq!(requests["sum"]["isMonotonic"], true);
    assert_eq!(requests["sum"]["dataPoints"][0]["asInt"], "3"); // OTLP JSON int64 = string
    let attrs = &requests["sum"]["dataPoints"][0]["attributes"];
    assert_eq!(attrs[0]["key"], "route");
    let live = metrics.as_array().unwrap().iter()
        .find(|m| m["name"] == "varve_live_rows").unwrap();
    assert_eq!(live["gauge"]["dataPoints"][0]["asInt"], "42");
}
```

(OTLP/JSON encodes 64-bit ints as strings — keep that; a collector rejects bare numbers. `resource` may carry `service.name = "varve"`.)

- [x] **Step 2: Push integration test** — spin an axum router with a `POST /v1/metrics` handler capturing the JSON into a channel; build `OtlpMetrics` with `push_interval_ms = 50` pointed at it; drive one `observe_request`; assert a captured body contains `varve_http_requests_total` within a 2 s timeout.

- [x] **Step 3: Run to verify failure, implement, run**

Run: `cargo test -p varve-server -- --test-threads=1 && cargo check -p varve-server --no-default-features && cargo test -p varve-cli -- --test-threads=1`
Expected: PASS; `--no-default-features` still compiles (no reqwest/axum in the core lib path).

- [x] **Step 4: Commit**

```bash
git add crates/varve-server
git commit -m "feat: otlp metrics sink pushing the prometheus registry as OTLP JSON"
```

---

### Task 15: Chaos harness — random writer kills under load

Roadmap exit: "chaos test (random writer kills under load, 30min) — no corruption, no acked loss". Env-gated so `just check` never pays for it; 60 s locally, 30 min nightly.

**Files:**
- Create: `crates/varve-testkit/src/bin/chaos_writer.rs`
- Create: `crates/varve-testkit/tests/chaos.rs`
- Modify: `crates/varve-testkit/Cargo.toml` if the bin needs explicit `[[bin]]` (bins under `src/bin/` are auto-discovered; check how the existing `src/bin/` helpers are declared and follow that)
- Modify: `justfile` (`chaos` recipe), `.github/workflows/ci.yml` (`chaos-nightly` job)
- Test: this task IS the test

**Interfaces:**
- Consumes: `Db::local` (designated-writer restart chaos — no coordinator section, so restarts are instant per decision 5), `env!("CARGO_BIN_EXE_chaos_writer")` (available to integration tests of the same package — the crash_recovery harness pattern).
- Produces: `just chaos`; CI job.

`chaos_writer` contract (stdout, line-buffered):

```text
CHAOS_WRITER_READY
ACKED 1
ACKED 2
…
```

```rust
// src/bin/chaos_writer.rs — args: <dir> <start_n>
// opens Db::local(dir); loops n = start_n.. executing
//   INSERT (:Chaos {_id: <n>})
// printing "ACKED <n>" (flush stdout) after each Ok ack. Runs until killed.
// An execute error prints "ERR <n> <message>" and exits 1 (the parent fails
// the test on unexpected child exits).
```

```rust
// tests/chaos.rs — skeleton
#[tokio::test]
async fn random_writer_kills_lose_no_acked_transactions() {
    let Some(secs) = std::env::var("VARVE_CHAOS_SECS").ok().and_then(|v| v.parse::<u64>().ok()) else {
        eprintln!("VARVE_CHAOS_SECS unset; skipping chaos run");
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(secs);
    let mut acked: Vec<i64> = Vec::new();
    let mut next_start = 1i64;
    let mut seed = 0x5eed_cafe_u64;             // xorshift; deterministic per run
    while std::time::Instant::now() < deadline {
        let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_chaos_writer"))
            .arg(dir.path()).arg(next_start.to_string())
            .stdout(std::process::Stdio::piped())
            .spawn().unwrap();
        // reader thread accumulates ACKED lines into a channel
        let kill_after = 200 + xorshift(&mut seed) % 1300;   // 200..1500 ms
        std::thread::sleep(std::time::Duration::from_millis(kill_after));
        kill_9(child.id());                                   // libc-free: std::process::Command "kill" -9 <pid>
        let _ = child.wait();
        // drain reader; extend `acked`; next_start = acked.last() + gap headroom
        next_start = acked.last().copied().unwrap_or(0) + 1_000; // ids never collide across lives
    }

    // Verdict: reopen; every acked id visible; verify() clean.
    let db = varve::Db::local(dir.path()).await.unwrap();
    let rows = db.query("MATCH (c:Chaos) RETURN c._id").await.unwrap();
    let present: std::collections::BTreeSet<i64> = collect_ids(&rows);
    for id in &acked {
        assert!(present.contains(id), "acked _id {id} lost after kill -9");
    }
    db.verify().await.unwrap();
    println!("chaos: {} kills survived, {} acked txs all present", kills, acked.len());
}
```

Implementation notes: `kill_9` = `std::process::Command::new("kill").args(["-9", &pid.to_string()]).status()` (Unix; the crash matrix already delivers real `kill -9`s — reuse its helper if one is importable from `varve-testkit`). The stdout reader must be a thread started before the sleep so the pipe never fills. Only ids read from the pipe BEFORE the kill count as acked — an `ACKED` line is only printed after the engine ack, so this is conservative in the right direction (never counts an unacked tx as acked; may treat an acked tx as unknown, which the assertion tolerates by only checking the acked set).

justfile:

```make
chaos secs="60":
    VARVE_CHAOS_SECS={{secs}} cargo test -p varve-testkit --release --test chaos -- --nocapture
```

CI: add a `chaos-nightly` job to `.github/workflows/ci.yml` mirroring `property-nightly`'s schedule gating (its own cron, e.g. `0 5 * * *`, `if: github.event.schedule == '0 5 * * *'` — match the file's existing cron-discrimination pattern EXACTLY; this workflow has a history of fragile YAML, so validate with a YAML parse before committing): `VARVE_CHAOS_SECS=1800 cargo test -p varve-testkit --release --test chaos -- --nocapture`.

- [x] **Step 1: Write the bin + test (they fail/skip until wired), run locally**

Run: `VARVE_CHAOS_SECS=30 cargo test -p varve-testkit --release --test chaos -- --nocapture`
Expected: PASS with a couple of dozen kills; prints the survival line.

- [x] **Step 2: Run un-gated (skip path)**

Run: `cargo test -p varve-testkit --test chaos`
Expected: PASS instantly with the skip message.

- [x] **Step 3: Commit**

```bash
git add crates/varve-testkit justfile .github/workflows/ci.yml
git commit -m "test: env-gated chaos harness for random writer kills under load"
```

---

### Task 16: Metrics documentation, whole-slice verification, docs closeout

**Files:**
- Create: `docs/ops/metrics.md`
- Modify: `docs/plans/STATUS.md`, `docs/plans/varve-v1-roadmap.md` (tick the five Slice-10 boxes)
- Test: the full gate

**Step 1: `docs/ops/metrics.md` (Grafana-ready — the exit criterion).** One table row per metric: name, type (counter-semantics gauge / gauge / histogram), labels, meaning, and a suggested Grafana expression (e.g. `rate(varve_txs_committed_total[1m])` for ingest rate; `varve_log_lag_records` per node for follower lag; `varve_cache_hits_total / (varve_cache_hits_total + varve_cache_misses_total)` per tier; `varve_compaction_debt_tries` with its approximation caveat; `histogram_quantile(0.99, rate(varve_http_request_duration_seconds_bucket[5m]))`). A second section lists the stable tracing span names (Task 13) and the `[metrics.otlp]` configuration with a collector snippet (`otlp` receiver, JSON over HTTP). A third section documents the coordination objects (`v1/writer.json`, `v1/lease.json`, `v1/epochs/*.json`), the `[coordinator]` config keys with defaults, and the failover runbook (what `WriterActive`, `CasUnsupported`, `WriterFenced` mean and what the operator does about each).

- [x] **Step 2: Whole-slice verification (all must pass, in this order)**

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace -- --test-threads=1
cargo test -p varve-engine --test failover -- --test-threads=1
cargo run --release --example failover -p varve
VARVE_CHAOS_SECS=60 cargo test -p varve-testkit --release --test chaos -- --nocapture
VARVE_S3_BACKENDS=garage,minio cargo test -p varve-testkit --test cas_failover_backends -- --test-threads=1 --nocapture   # docker required
cargo check -p varve-server --no-default-features
cargo check -p varve-engine --no-default-features
just compose-demo     # slice-9 regression: server/CLI/compose stack unaffected
git diff --check
```

- [x] **Step 3: Update STATUS.md** — done by the controller at HEAD `6462dbc`: current position → Slice 11 planning; Slice-10 shipped summary; verification numbers; demo command `cargo run --release --example failover -p varve`; final whole-branch review outcome (finding 1 fixed `6462dbc`, finding 2 documented as a v1 known limitation); deferred fast-follows recorded.

- [x] **Step 4: Tick the roadmap** — all five Slice-10 checkboxes in `docs/plans/varve-v1-roadmap.md:383-396`.

- [x] **Step 5: Commit**

```bash
git add docs
git commit -m "docs: slice 10 closeout — metrics reference, STATUS, roadmap"
```

---

## Slice exit checklist (from the roadmap entry)

- [x] `Coordinator` trait + registry; `designated-writer` default heartbeats `v1/writer.json`; second writer starting while the heartbeat is fresh refuses with a clear error (best-effort, documented) — Tasks 5–6.
- [x] `cas-failover` coordinator (feature-gated): lease via `If-None-Match`/`If-Match` through `object_store` PutMode; **epoch increment on takeover fences the old writer** (stale appends land in a dead epoch and are ignored); enabled only when the slice-5 probe verdict is `Supported`, else hard error naming the backend capability — Tasks 2–4, 7–8.
- [x] Failover test (CAS-semantics store): kill writer → standby takes over < 10 s, zero acked-tx loss, zombie writer's late appends provably ignored — Task 9.
- [x] Backpressure: bounded submission queue (429/wait on full); live-index memory watermark forces early flush; slow-query-node lag metric that never affects the writer — Tasks 10–12.
- [x] Observability completion: `tracing` spans across submit→commit→apply→flush and parse→plan→execute; Prometheus metrics per spec §12; OpenTelemetry export behind `MetricsSink` — Tasks 12–14.
- [x] Exit: failover demo green (CAS store) + Garage correctly refuses cas-failover with an actionable error — Task 9; chaos test (random writer kills under load, 30 min nightly) with no corruption and no acked loss — Task 15; Grafana-ready metrics documented — Task 16.
- [x] All workspace tests green, clippy clean, fmt clean; STATUS.md updated; roadmap boxes ticked; demo command recorded — Task 16 + controller STATUS.md closeout (HEAD `6462dbc`).
