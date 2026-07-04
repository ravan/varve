# Varve Implementation Status Ledger

> Update at the end of EVERY session. This is the entry point for the next session —
> read this first, then `varve-v1-roadmap.md`, then the current slice's detailed plan.

## Current position

- **Current slice:** 3 (durability: log, group commit, crash safety) — ✅ COMPLETE
  (2026-07-05, 1 session, SDD, 12 tasks). Writes now flow through a log-serialized writer loop
  with group commit onto a pluggable `Log` (new `varve-log` crate: `Log` trait + prost record
  envelope + `memory`/`local` backends — CRC32C frames, fsync-before-ack, torn-tail recovery).
  The loop resolves DML serially, group-commits to the log, applies to the live index ONLY after
  durability, then acks — so an acked tx is both durable and visible, and concurrent `execute()`
  is now supported (slice-2 single-writer caveat + DELETE `await_holding_lock` dissolved).
  `Db::open(Config)` selects the backend by registry name and replays the log into the live index
  on startup. A `kill -9` crash matrix in `varve-testkit` proves the recovery contract.
  Demo: `cargo run --release --example write_bench -p varve`.
- **Next action:** generate the slice-4 detailed plan (blocks & persisted scan — flush Arrow
  blocks + hash-trie meta to object storage, persisted `BitemporalScan`, restart from manifest —
  spec §9) with the writing-plans skill from the roadmap entry + spec, commit it, then execute.
  Slice 4 depends on slice 3's log (block manifest = commit point; replay-from-watermark trims
  the log). `BuildContext` factory param may finally be needed there (cache tiers) — see open items.
- **Slice 2 (bitemporal core):** ✅ COMPLETE (2026-07-04). Events + XTDB Ceiling/Polygon port +
  temporal GQL. Demo: `cargo run --example time_travel -p varve`.
- **Slice 1 (walking skeleton):** ✅ COMPLETE (2026-07-04). INSERT → MATCH end-to-end in memory.
  Demo: `cargo run --example hello -p varve` (still green under slice 3's writer loop).
- **Detailed plans ready:** slice 0 ✅ (done) · slice 1 ✅ (done) · slice 2 ✅ (done) ·
  slice 3 ✅ (done) · slices 4–11 generated just-in-time from the roadmap (writing-plans skill)
  at each slice's start.

## Environment facts (verify before relying on)

- Repo dir named `timedb` but project is **Varve** (rename pending, user's call).
- `~/.gitignore_global` ignores any path containing `specs` — keep docs in
  `docs/design/` and `docs/plans/`.
- XTDB reference checkout at `refs/xtdb` (gitignored); porting references for
  bitemporal/trie/compaction live in `refs/xtdb/core/src/main/kotlin/xtdb/` and
  `refs/xtdb/dev/doc/*.allium`.
- GQL grammar reference vendored at `resources/gql-grammar/` (committed, Apache-2.0).
- Toolchain: Rust stable 1.93 (`rust-toolchain.toml`). `just` installed via brew.
- **Dependency pins (slice 1):** `datafusion = "54"` (resolved 54.0.0), `arrow = "58"` (58.3.0),
  `tokio = { version = "1", features = ["rt-multi-thread","macros"] }`, `async-trait = "0.1"`.
  DataFusion 54 re-exports arrow 58 — the workspace arrow pin MUST track it so a single `arrow`
  crate unifies `RecordBatch` across `varve-index` and `varve-plan` (verified: `Cargo.lock` has
  exactly one arrow). To re-derive on a bump: `cargo tree -p datafusion | grep ' arrow '`.
  The brief's DataFusion/arrow API sketches compiled VERBATIM on 54/58 — no adaptations needed.
- **⚠️ ENV ISSUE (observed 2026-07-04 during slice 1 Task 7):** an automated `git rebase` (rtk
  hook/tooling) rewrote main's ENTIRE history mid-session — every commit SHA changed, committer
  dates were unified, an initial commit was squashed into the design-spec commit. Content was
  verified 100% intact each time, but this silently orphans any recorded SHAs. If you record
  base commits for review tooling, re-resolve them from `git log` after any long agent run.
- **Dependency pins (slice 3, verified live 2026-07-04):** `prost = "0.14"` (resolved 0.14.4 —
  derive-only, NO protoc/build.rs), `crc32c = "0.6"` (0.6.8), `tempfile = "3"` (3.27.0, dev). tokio
  workspace features grew to `["rt-multi-thread","macros","sync","time"]` (writer-loop channels +
  group-commit window timer). `varve-log` declares a `fault-injection = []` feature (crash hooks);
  it is enabled workspace-wide only via `varve-testkit`'s dep (feature unification), inert unless
  `VARVE_CRASH_TRIGGER` names an armed file — downstream consumers of `varve` never enable it.
- Gate: `just check` = `cargo fmt --all --check` + `cargo clippy --workspace --all-targets
  -- -D warnings` + `cargo test --workspace`. Same three commands run in CI (`.github/workflows/ci.yml`).
  CI gained a `crash-matrix` job (slice 3): `cargo test -p varve-testkit --release --test
  crash_recovery` with `VARVE_CRASH_ITERS=100` (default 3 in the normal `check` run); gated
  `!= 'schedule'`. `just crash` runs it at 10 iterations locally.
- `Cargo.lock` IS committed (workspace ships binaries; `xxhash-rust` defines the on-disk `Iid`
  byte format, so it is pinned). `.superpowers/` is SDD scratch (self-ignored).

## Decisions made during implementation

- **2026-07-05 (slice 3) writer loop = the serialization point (spec §3, D3):** `Db::execute`
  parses + submits to a bounded mpsc queue (`SUBMISSION_QUEUE_LEN = 256`); a dedicated tokio task
  assigns `(tx_id, system_time)`, resolves DML SERIALLY (tx N sees tx N−1), group-commits a batch
  to the `Log` (window OR size trigger OR channel-close), applies events to the `LiveTable` **only
  after** the batch is durable, then acks. So an acked tx is both durable AND visible
  (read-your-writes), and queries never observe un-durable data. A reading statement (v1: `DELETE`)
  flushes the staged batch first so its snapshot includes every earlier tx. This dissolved slice
  2's single-writer clock caveat and DELETE's `#[allow(await_holding_lock)]` — concurrent
  `execute()` is now supported and tested (`varve-engine/tests/concurrency.rs`, 50 concurrent).
- **2026-07-05 (slice 3) failed append ⇒ clean rollback:** nothing was applied, so a failed durable
  append acks `EngineError::CommitFailed` to every tx in the batch and the loop continues with
  consistent state. `LocalLog` restores its file to the pre-batch length (poisons itself if the
  restore fails). The apply-after-durable ordering makes an apply-failure-after-durable path
  provably unreachable today (LiveTable strict-`<` monotonicity + monotonic assign order) — see
  open items for the scheduled defense-in-depth.
- **2026-07-05 (slice 3) positions per-record; batch = durability unit:** `Log::append(Vec<LogRecord>)`
  durably writes all records with ONE fsync (later: one S3 PUT) and returns the first record's
  `LogPosition`. One record = one tx, so tx atomicity holds even if a torn batch leaves a durable
  prefix. `LocalLog` frame = `len u32 LE · crc32c u32 LE · payload`; segment file `{first_pos:016x}.vseg`.
- **2026-07-05 (slice 3) envelope = protobuf (prost derive, no protoc); effects = per-table Arrow IPC:**
  `LogRecord { tx_id, system_time_us, user, effects }` (`user` empty in v1). Docs+labels ride as ONE
  nullable `payload` Binary column via a canonical byte codec owned by us (golden-tested in
  varve-types). The Event↔Arrow-IPC codec lives in `varve-index` (owns `Event`), keeping `varve-log`
  payload-agnostic. Arrow IPC bytes are deliberately NOT golden-pinned (no cross-version guarantee)
  — round-trip + proptest instead. The canonical Value/Doc codec + protobuf wire ARE golden-pinned.
- **2026-07-05 (slice 3) generated ids now durable:** `varve:gen:{tx_id}:{ordinal}` replaces slice-1's
  process-local `varve:gen:{n}` (which reset on restart). `tx_id` is recovered from the log, so
  uniqueness survives restarts (pinned by `replay_recovers_max_tx_id_across_a_burned_id_gap`).
- **2026-07-05 (slice 3) `Clock` is now a pluggable trait + `Registries` aggregate landed:**
  `varve_engine::Clock` (builtin `system` = the existing `MonotonicClock`, gaining `advance_to(floor)`
  for recovery). `Registries { log, clock }` + `with_builtins()` in varve-engine; `Db::open_with(&config,
  &registries)` is the embedder extension point (spec §4). Discharges the slice-2 "pluggable Clock
  arrives with durability config" + slice-0 "Registries aggregate deferred to varve-engine" decisions.
- **2026-07-05 (slice 3) recovery = pure fold over the log:** `Db::open` replays `log.tail(ZERO)` into
  a fresh `LiveTable`; `next_tx_id = max(record.tx_id)` (NOT a count — a failed resolve burns a tx_id,
  so the on-disk sequence can have gaps); `clock.advance_to(max system_time)` floors post-restart txs
  after history. An effect for any table other than `nodes` is a hard `UnknownTable` error (future-format
  guard). Block-manifest replay-from-watermark arrives in slice 4.
- **2026-07-05 (slice 3) DEVIATION — `group_commit_max_bytes` is an integer byte count** (default
  `8388608`). The spec §4 sketch shows the string `"8MiB"`; human-size parsing is config polish
  deferred to the server slice (9). `group_commit_window_ms` default 15; `Db::memory()` uses window = 0
  (no fsync to amortize — keeps embedded in-memory latency at slice-2 levels).
- **2026-07-05 (slice 3) crash contract formalized (design decision 8):** two feature-gated hooks in
  `LocalLog::append` (`pre-append`, `post-append`) armed via a trigger file; the child announces
  `CRASH_POINT <name>` and parks; the parent delivers a real `kill -9`. A tx killed BEFORE durability
  never surfaces; a durable-but-unacked tx (post-append) MAY surface after restart — the standard WAL
  contract (client saw no ack, must treat the tx as unknown). No lock file on the local log dir
  (exactly-one-writer enforced by deployment; slice 10 adds a best-effort `writer.json` guard).
- **2026-07-05 (slice 3) write-throughput smoke bench** (Apple M3 Max, macOS Darwin 25.3.0 arm64,
  `cargo run --release --example write_bench`, 4000 txs / 8 workers): **memory 6226 tx/s** (642 ms);
  **local (fsync) 340 tx/s** (11.78 s) — group-commit-bound (real fsync + 15 ms window). Smoke number,
  not a benchmark (that's slice 11).
- **2026-07-04 (slice 2) deps pinned:** `chrono = "0.4"` resolved 0.4.45 (already in-tree
  transitively; workspace pin unified it), `proptest = "1"` resolved 1.11.0. Both in root
  `[workspace.dependencies]`; existing `datafusion 54.0.0 / arrow 58.3.0` pins carried forward.
- **2026-07-04 (slice 2) `TxReceipt.system_time` pulled forward from slice 3:** temporal e2e
  tests need a handle on assigned tx times, so `TxReceipt` is `{ tx_id, system_time: Instant }`
  now rather than at durability. Additive — slice-1 code reads only `.tx_id`.
- **2026-07-04 (slice 2) DELETE reads current state, rejects temporal clauses:** `MATCH … DELETE`
  resolves matches at (valid=now, system=now) and appends `Op::Delete` events over sorted+deduped
  IIDs; a `FOR` clause on a DELETE is a parse error. Retroactive/as-of deletes deferred (post-v1).
- **2026-07-04 (slice 2) GQL `ERASE` statement deferred to slice 7:** the event-level `Op::Erase`
  is fully implemented and property-tested (erase hides history at EVERY system time — a
  deliberate, tested GDPR choice, not a time-travel bug); only the surface `ERASE` statement and
  end-to-end GDPR object-scan verification (slice 11) are deferred.
- **2026-07-04 (slice 2) 13 new reserved words:** `FOR, FROM, TO, ALL, AND, VALID, DELETE,
  BETWEEN, TIMESTAMP, DATE, VALID_TIME, SYSTEM_TIME, OF` can no longer be property names —
  accepted until slice 7's full literal/identifier grammar.
- **2026-07-04 (slice 2) `MonotonicClock` is crate-internal:** strictly-increasing wall-clock µs
  (AtomicI64 compare_exchange); the pluggable `Clock` registry interface (spec §4) arrives with
  durability config wiring. v0 is SINGLE-WRITER: `clock.next()` is taken before the `live.write()`
  lock, so concurrent `execute()` is unsupported (a lock-race loser hits `OutOfOrderEvent`); full
  write-serialization arrives in slice 3's writer loop (spec D3). Documented in `db.rs`.
- **2026-07-04 (slice 2) CI nightly property job:** `.github/workflows/ci.yml` gained a
  `schedule`-gated `property-nightly` job running `varve-testkit --release` with
  `PROPTEST_CASES=200000` (10k in the normal `check` job). `check` gated `!= 'schedule'`.
- **2026-07-04 (slice 2, whole-branch review) `Statement::Query` boxed** (`bf8e85a`): with the
  final enum shape known (Query ~250B vs Insert ~56B / Delete ~152B), boxing the one oversized
  variant drops the max below clippy's `large_enum_variant` threshold, so the `#[allow]` (added
  in Task 7) was removed rather than kept — cleaner and avoids copying a 250B enum per parse.
- **2026-07-04 (slice 2) `ReferenceStore::append` hardened** (`64f0651`): a `debug_assert!`
  enforces the non-decreasing-per-iid `system_from` invariant the oracle's winner-by-arrival
  logic relies on (remediates a whole-branch-review finding; keeps the oracle self-defending).
- **2026-07-04 (slice 0) clippy.toml:** repo-root `clippy.toml` sets `allow-unwrap-in-tests`
  / `allow-expect-in-tests = true`. Workspace lints deny `unwrap_used`/`expect_used` globally;
  this realizes the Global Constraint's "allowed in tests" so `clippy --all-targets -D warnings`
  passes. Deviation from the literal plan (which omitted the file).
- **2026-07-04 (slice 0) per-crate errors:** `TypeError` / `ConfigError` / `RegistryError`
  (thiserror, one per crate) instead of a single `VarveError` base; `Registries` aggregate
  deferred to `varve-engine`. Matches the detailed plan + "errors via thiserror per crate"
  Global Constraint. The roadmap's slice-0 wording ("VarveError base", "Registries aggregate")
  is looser — this is not a missed requirement.
- **2026-07-04 (slice 0) serial_test:** added `serial_test` dev-dependency + `#[serial]` on the
  config tests — process env is shared mutable state and the env-override tests raced under
  parallel execution. The plan sanctioned this "only if observed"; it was observed.
- **2026-07-04 (slice 0) golden vectors:** known-answer tests pin the `Iid` 16-byte format and
  the `LogPosition` packed-`u64` format. Changing either output is now a conscious breaking change.
- **2026-07-04 (slice 0, post-review) spec §5.2 reconciled** (commit `3e0f539`): §5.2's IID
  comment now reads `xxh3-128 of (graph, table, _id)`, matching §5.3, the roadmap, and the code.
- **2026-07-04 (slice 1) DataFusion/arrow API sketch held:** the slice-1 plan warned its
  DataFrame/builder API sketches might drift from the pinned versions; on datafusion 54 / arrow
  58 they compiled verbatim. No test was altered to fit the API (the plan's "tests are the
  contract" rule was never invoked). Feeds slice-2 planning: the same API shape is safe to reuse.
- **2026-07-04 (slice 1) `_labels` column omitted from LiveTable snapshot (plan-vs-roadmap):**
  the roadmap slice-1 entry lists the snapshot schema as `_iid, _labels, property columns`, but
  the detailed slice-1 plan (Task 5) scoped it to `_iid` + one column per observed property, and
  the code follows the detailed plan. Label filtering happens in `snapshot_for_label(label)`, and
  RETURN only projects properties, so `_labels` is unused in the walking skeleton. Same "roadmap
  wording is looser than the detailed plan" pattern as slice 0's VarveError note — the detailed
  plan governs execution (roadmap says so). `_labels` will be needed for multi-label MATCH /
  `labels(p)` — add it when slice 6 (edges/traversal) or slice 7 (GQL completion) needs it. See open items.
- **2026-07-04 (slice 1) `await_holding_lock` accepted as documented v0 deferral:** `Db::query`
  holds a std `RwLockReadGuard` across `run_query(...).await` (real DataFusion async work),
  suppressed with `#[allow(clippy::await_holding_lock)]` + a `// v0` comment. Both the plan and
  the whole-branch reviewer (opus) triaged this DEFER: no concurrent access exists in the
  single-writer in-memory v0. Remediation is scheduled — see open items.
- **2026-07-04 (slice 1, whole-branch review) atomic multi-node INSERT fix** (commit `ad5b19a`):
  `Db::execute` was appending nodes one at a time, so a later node with an invalid `_id`
  (`Float`/`Null` → `id_bytes()` errors, e.g. `INSERT (:A {_id:1}), (:B {_id:2.5})`) left earlier
  nodes committed — a user-triggerable torn write. Fixed to validate ALL node `(iid,labels,doc)`
  triples before the first `append` (all-or-nothing for the v0 in-memory write). TDD: RED (node A
  leaked) → GREEN. This was a latent bug the plan overlooked, not a plan-mandated behavior.
- **2026-07-04 (slice 1) tokio omitted from varve-engine:** the plan's Task-7 file list included
  `tokio` as a `varve-engine` dep, but the engine's async fns only `.await` a future and use
  `std::sync` — no direct `tokio::` symbol — so it was omitted to keep the crate clean.
- **2026-07-04 (slice 0, post-review) env overrides upgraded** (commit `1b517e8`): now support
  **N-segment nested keys** (`VARVE__LOG__LOCAL__DIR` → `[log.local] dir`, `VARVE__STORAGE__S3__ENDPOINT`
  → `[storage.s3] endpoint`) and **scalar coercion** (bool → i64 → f64 → string; e.g.
  `group_commit_window_ms=20` coerces to int). A path through an existing non-table value is
  skipped (no clobber/panic). Replaces the prior 2-segment/string-only behavior; the slice-3
  decision below is now closed.

## Open items / decisions needed

- **~~SPEC INCONSISTENCY~~ — RESOLVED** 2026-07-04 (`3e0f539`): §5.2 aligned to §5.3.
- **~~ENV-OVERRIDE DESIGN (slice 3)~~ — RESOLVED** 2026-07-04 (`1b517e8`): nesting + scalar
  coercion implemented and tested; no longer a slice-3 decision.
- **~~DEFERRED slice-1 remediations~~ — RESOLVED** 2026-07-04 (slice 2):
  - `await_holding_lock` on `Db::query` DELETED — query path lock-split into sync
    `snapshot_for_query` + async `execute_query` over an owned batch (Task 6, `ebdf4b6`).
  - Deferred tests added: LiveTable all-null-property-column + empty-Doc row, and BOTH
    `PlanError::UnknownColumn` paths (WHERE + RETURN) — Task 6.
  - `id_bytes` catch-all narrowed + same-length collision test pinned (Task 1, `5e31452`).
  - Multi-node INSERT atomicity preserved through the Task-6/8 event-buffer rewrite
    (`multi_node_insert_is_atomic_on_invalid_id` still green).
  - STILL DEFERRED (intentional): the `SnapshotSource` trait seam waits for slice 4 (YAGNI
    before a second scan source); DELETE's documented `#[allow(await_holding_lock)]` on
    `execute_delete` dissolves in slice 3's log-serialized writer loop (spec D3).
- **`_labels` roadmap divergence (user's call):** either add `_labels` to the LiveTable snapshot
  when slice 6/7 needs it, OR reconcile the roadmap slice-1 text to match the detailed plan
  (`_iid` + property columns). Not blocking; flagged so it isn't silently lost.
- **Slice-1 minor follow-ups (non-blocking, triaged DEFER by whole-branch review):** T1 strengthen
  the id collision test to a same-length case + narrow the `id_bytes` `other =>` arm (slice 2);
  T2 lexer `// v0` comment scope + per-ident `to_ascii_uppercase` alloc (slice 7); T3 empty
  `(:Label {})` prop block fails to parse, factor the 3 `GqlError::Parse` reconstructions, add a
  multi-label `(:A:B)` test (slice 7); T4 factor `var.prop` parse duplication (slice 7); T6
  unlabeled `MATCH (p) …` returns empty not error (revisit when patterns expand); WHERE/RETURN on
  an all-absent property errors (`UnknownColumn`) rather than yielding null/0 rows — mild GQL
  deviation, revisit slice 7; `Db` derives neither `Clone` nor `Debug` (slice 9, query-node handles).
- **~~Deferred slice-0 minors (rustdoc + `from_file` test)~~ — RESOLVED** 2026-07-05 (slice 3, Task 10,
  `e5b4519`): full rustdoc sweep on the public `varve-config` API (`Config`/`ConfigSection`/`ConfigError`/
  `Registry`/`ComponentFactory`/`RegistryError`), `cargo doc` warning-free; `Config::from_file` /
  `ConfigError::Io` now directly tested (`config_test.rs::from_file_reads_and_missing_file_is_io_error`).
  STILL DEFERRED: `BuildContext` factory param — config-only factories still suffice (`log/local` gets
  its dir from `[log.local]`). Revisit at slice 4/5 when a factory genuinely needs another *component*
  (e.g. cache tiers) — it is a trait break, cheapest before more backends exist.
- **Slice-3 fast-follows (non-blocking; whole-branch review triaged, verdict READY TO MERGE):**
  - **T9 apply-failure defense-in-depth (RECOMMENDED, schedule for slice 10):** `writer.rs::flush` on an
    apply-failure-after-durable-append acks `CommitFailed` and keeps serving — but that path is PROVABLY
    UNREACHABLE today (LiveTable global strict-`<` monotonicity + the writer's monotonic serial assign
    order ⇒ `OutOfOrderEvent` cannot fire). Make apply-failure FATAL (stop the writer / mark unavailable)
    when slice 10's multi-writer or per-entity monotonicity weakens that invariant — else a durable tx
    could be acked as failed (false-negative ack that only a restart heals).
  - **T5 `read_range_sync` early-return (defer):** dropped the reference's `position >= to` early break, so
    a bounded `read_range` scans every frame in every segment and a corrupt frame beyond `to` surfaces as
    `Corrupt`. Output-equivalent for valid data; full replay uses `tail(ZERO..MAX)` so it's unaffected.
    Restore the early break when a real bounded `read_range` caller lands (slice 9 query-node tailing).
  - **4 GiB `len() as u32` truncation (defer, forward-hardening):** `value.rs::write_len_prefixed` and
    `codec.rs::encode_put_payload` silently truncate a >4 GiB value/label/doc with no `Result` to signal.
    Unreachable in v1 (statements come from parsed GQL). Harden both together when it can matter.
  - **T9 `flush` clones every `LogRecord`** (incl. `arrow_ipc`) to build the append arg — doubles peak IPC
    memory for large batches. Move the record out of `Staged` (apply needs only events/receipt/ack). Efficiency.

## Slice log

| Slice | Status | Sessions | Demo command | Notes |
|---|---|---|---|---|
| 0 foundation | ✅ complete | 1 | `just check` / `cargo test --workspace` (22 tests) | workspace + `varve-types` (Iid, LogPosition) + `varve-config` (Config, Registry, nested/coerced env overrides) + CI |
| 1 walking skeleton | ✅ complete | 1 | `cargo run --example hello -p varve` | INSERT→MATCH e2e in memory; +`varve-gql`(lexer/parser/AST), `varve-index`(LiveTable→Arrow), `varve-plan`(DataFusion), `varve-engine`(Db), `varve` facade; datafusion 54/arrow 58 pinned; 44 workspace tests |
| 2 bitemporal core | ✅ complete | 1 | `cargo run --example time_travel -p varve` | events + XTDB Ceiling/Polygon port + per-entity resolve; `varve-testkit` reference model + proptest equivalence (10k CI / 200k nightly); temporal GQL (`FOR VALID_TIME`/`SYSTEM_TIME`, `INSERT … VALID`, `MATCH … DELETE`, history fns); `MonotonicClock`; `TxReceipt.system_time`; lock-split query; ~125 workspace tests |
| 3 durability (log) | ✅ complete | 1 | `cargo run --release --example write_bench -p varve` | `varve-log` crate: `Log` trait + prost envelope + `memory`/`local` backends (CRC32C frames, fsync-before-ack, torn-tail recovery) + writer loop group commit + `Db::open` replay + pluggable `Clock`/`Registries` + `kill -9` crash matrix; bench memory 6226 / local 340 tx/s (M3 Max); 181 workspace tests |
| 4 blocks & persisted scan | not started | – | – | no detailed plan yet |
| 5 s3 backends & caches | not started | – | – | no detailed plan yet |
| 6 edges & traversal | not started | – | – | no detailed plan yet |
| 7 GQL completion & TCK | not started | – | – | no detailed plan yet |
| 8 compaction & GC | not started | – | – | no detailed plan yet |
| 9 server, CLI, query nodes | not started | – | – | no detailed plan yet |
| 10 coordination | not started | – | – | no detailed plan yet |
| 11 ship | not started | – | – | no detailed plan yet |
