# Varve Implementation Status Ledger

> Update at the end of EVERY session. This is the entry point for the next session —
> read this first, then `varve-v1-roadmap.md`, then the current slice's detailed plan.

## Current position

- **Current slice:** 2 (bitemporal core) — ✅ COMPLETE (2026-07-04, 1 session, SDD).
  Every mutation is now an immutable bitemporal event; queries resolve visibility through
  ported XTDB Ceiling/Polygon and answer `FOR VALID_TIME`/`FOR SYSTEM_TIME` time-travel,
  retroactive corrections, and `DELETE` end-to-end through GQL. A naive `varve-testkit`
  reference model proves the engine correct on 10k randomized histories/CI (200k nightly).
  Demo: `cargo run --example time_travel -p varve`.
- **Next action:** generate the slice-3 detailed plan (durability: pluggable log, group commit,
  crash safety — spec §6) with the writing-plans skill from the roadmap entry + spec, commit
  it, then execute. Slice 3 is where the writer loop log-serializes writes (dissolves the
  documented single-writer clock assumption and DELETE's `await_holding_lock` — see decisions).
- **Slice 1 (walking skeleton):** ✅ COMPLETE (2026-07-04). INSERT → MATCH end-to-end in memory.
  Demo: `cargo run --example hello -p varve` (still green under slice 2's "AS OF now").
- **Detailed plans ready:** slice 0 ✅ (done) · slice 1 ✅ (done) · slice 2 ✅ (done) ·
  slices 3–11 generated just-in-time from the roadmap (writing-plans skill) at each
  slice's start.

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
- Gate: `just check` = `cargo fmt --all --check` + `cargo clippy --workspace --all-targets
  -- -D warnings` + `cargo test --workspace`. Same three commands run in CI (`.github/workflows/ci.yml`).
- `Cargo.lock` IS committed (workspace ships binaries; `xxhash-rust` defines the on-disk `Iid`
  byte format, so it is pinned). `.superpowers/` is SDD scratch (self-ignored).

## Decisions made during implementation

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
- **Deferred slice-0 minors (do before the API grows consumers):** rustdoc on the public
  `varve-config` API (`Config`, `ConfigSection`, `ConfigError`, `Registry`, `ComponentFactory`,
  `RegistryError`); `Config::from_file` / `ConfigError::Io` have no direct test; `BuildContext`
  factory param still deferred (revisit slice 3 — it is a trait break, cheapest before backends exist).

## Slice log

| Slice | Status | Sessions | Demo command | Notes |
|---|---|---|---|---|
| 0 foundation | ✅ complete | 1 | `just check` / `cargo test --workspace` (22 tests) | workspace + `varve-types` (Iid, LogPosition) + `varve-config` (Config, Registry, nested/coerced env overrides) + CI |
| 1 walking skeleton | ✅ complete | 1 | `cargo run --example hello -p varve` | INSERT→MATCH e2e in memory; +`varve-gql`(lexer/parser/AST), `varve-index`(LiveTable→Arrow), `varve-plan`(DataFusion), `varve-engine`(Db), `varve` facade; datafusion 54/arrow 58 pinned; 44 workspace tests |
| 2 bitemporal core | ✅ complete | 1 | `cargo run --example time_travel -p varve` | events + XTDB Ceiling/Polygon port + per-entity resolve; `varve-testkit` reference model + proptest equivalence (10k CI / 200k nightly); temporal GQL (`FOR VALID_TIME`/`SYSTEM_TIME`, `INSERT … VALID`, `MATCH … DELETE`, history fns); `MonotonicClock`; `TxReceipt.system_time`; lock-split query; ~125 workspace tests |
| 3 durability (log) | not started | – | – | no detailed plan yet; env-override decision due here |
| 4 blocks & persisted scan | not started | – | – | no detailed plan yet |
| 5 s3 backends & caches | not started | – | – | no detailed plan yet |
| 6 edges & traversal | not started | – | – | no detailed plan yet |
| 7 GQL completion & TCK | not started | – | – | no detailed plan yet |
| 8 compaction & GC | not started | – | – | no detailed plan yet |
| 9 server, CLI, query nodes | not started | – | – | no detailed plan yet |
| 10 coordination | not started | – | – | no detailed plan yet |
| 11 ship | not started | – | – | no detailed plan yet |
