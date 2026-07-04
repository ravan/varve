# Varve Implementation Status Ledger

> Update at the end of EVERY session. This is the entry point for the next session —
> read this first, then `varve-v1-roadmap.md`, then the current slice's detailed plan.

## Current position

- **Current slice:** 1 (walking skeleton) — ✅ COMPLETE (2026-07-04, 1 session).
  INSERT → MATCH end-to-end in memory: tokenizer → parser → AST → LiveTable (Arrow) →
  DataFusion → Arrow RecordBatches. Demo: `cargo run --example hello -p varve`.
- **Next action:** begin slice 2 (bitemporal core) — FIRST generate its detailed plan with
  the writing-plans skill from the roadmap slice-2 entry + spec §5.2/§7, commit it, THEN
  execute. Slice 2 is the correctness heart (temporal types, event model, XTDB Ceiling/Polygon
  port, reference model + proptest). Do the deferred remediations below early in slice 2.
- **Detailed plans ready:** slice 0 ✅ (done) · slice 1 ✅ (done) · slices 2–11 generated
  just-in-time from the roadmap (writing-plans skill) at each slice's start.

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
- **DEFERRED slice-1 remediations (do EARLY in slice 2, ideally in one refactor):**
  - **`await_holding_lock` in `Db::query`** (`varve-engine/src/db.rs`): when slice 2 adds the
    temporal scan, snapshot under the lock → drop the guard → run DataFusion on the owned batch.
    The reviewer's seam: introduce a `SnapshotSource`/scan trait at `Db.live: Arc<RwLock<LiveTable>>`
    and `run_query(stmt, &LiveTable)` — the only two spots the engine touches a concrete backend
    (satisfies the "no concrete backend in engine code" Global Constraint with minimal churn).
  - Add tests deferred from slice 1: LiveTable all-null-property-column + empty-Doc row; the two
    `PlanError::UnknownColumn` paths (WHERE + RETURN on a property absent from all matched rows).
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
| 2 bitemporal core | not started | – | – | no detailed plan yet |
| 3 durability (log) | not started | – | – | no detailed plan yet; env-override decision due here |
| 4 blocks & persisted scan | not started | – | – | no detailed plan yet |
| 5 s3 backends & caches | not started | – | – | no detailed plan yet |
| 6 edges & traversal | not started | – | – | no detailed plan yet |
| 7 GQL completion & TCK | not started | – | – | no detailed plan yet |
| 8 compaction & GC | not started | – | – | no detailed plan yet |
| 9 server, CLI, query nodes | not started | – | – | no detailed plan yet |
| 10 coordination | not started | – | – | no detailed plan yet |
| 11 ship | not started | – | – | no detailed plan yet |
