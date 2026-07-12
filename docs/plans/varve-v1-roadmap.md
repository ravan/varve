# Varve v1 Implementation Roadmap

> **For agentic workers:** This is the master plan. Each slice below is implemented from a
> detailed per-slice plan in `docs/plans/` (writing-plans format, TDD). Slices 0 and 1 are
> already detailed. At the start of a slice whose detailed plan doesn't exist yet, FIRST
> generate it with the superpowers:writing-plans skill from this roadmap entry + the spec,
> THEN execute it with superpowers:subagent-driven-development or superpowers:executing-plans.

**Goal:** Ship VarveDB v1 — a bitemporal property-graph database in Rust speaking GQL,
embedded-first, storage/compute separated over any S3-API object store.

**Spec:** `docs/design/2026-07-04-varve-design.md` (the contract; section refs below are to it).
**Progress ledger:** `docs/plans/STATUS.md` (update at the end of every session).

**Architecture:** Designated writer resolves GQL DML into effect events, group-commits them to
a pluggable log, and flushes Arrow blocks + hash tries to object storage; stateless query
nodes tail the log and execute GQL through a DataFusion-based engine with a custom bitemporal
scan. Everything behind traits + registry + TOML config.

**Tech stack:** Rust stable · tokio · DataFusion (pin latest at slice 1 start; use its arrow
re-export version for the workspace `arrow` pin) · `object_store` · axum · prost · proptest ·
criterion · cargo-fuzz.

## Global constraints (apply to every task of every slice)

- **TDD, no exceptions:** failing test first, minimal implementation, refactor, commit.
  Use superpowers:test-driven-development during implementation sessions.
- **Interfaces + registry + composition (spec §4):** every subsystem is a trait in a core
  crate; implementations register in the explicit `Registry`; TOML selects by name; optional
  backends are Cargo features. Never let engine code depend on a concrete backend.
- **Sovereignty (spec §1, D7):** nothing may require more than plain S3 semantics
  (PUT/GET/LIST). Anything stronger (conditional PUT) must be optional and capability-probed.
- **Bitemporal invariant (spec §5.2):** `_system_to` and effective valid ranges are never
  stored, always derived. Storage is append-only events.
- **Determinism:** compaction and effect-replay must be deterministic functions of their
  inputs (no wall-clock, no randomness, no map-iteration-order dependence in outputs).
- Workspace lints: `cargo clippy --workspace --all-targets -- -D warnings`; `unwrap()`/
  `expect()` forbidden in library code (allowed in tests); errors via `thiserror` per crate.
- Timestamps are always `Timestamp(µs, UTC)`; IIDs are always `xxh3_128(graph, table, _id)`.
- Commit style: `feat:`/`fix:`/`test:`/`refactor:`/`docs:` conventional prefixes.
  Do NOT add a `Co-Authored-By` / co-author trailer (user preference, 2026-07-04).
- Every slice ends with: all workspace tests green, clippy clean, STATUS.md updated,
  roadmap checkbox ticked, a runnable demo command recorded in STATUS.md.

## Session protocol (multi-session execution)

1. Read `docs/plans/STATUS.md`, this roadmap, and the spec sections the current slice cites.
2. If the current slice has no detailed plan file yet → generate it (writing-plans skill),
   commit it, then execute. If it exists → resume at the first unchecked task.
3. Execute task-by-task with TDD. Commit after every green test cycle.
4. End of session: update STATUS.md (done tasks, decisions made, deviations from plan,
   next entry point), commit everything. Never leave red tests at a session boundary.

## Slice sequencing (dependency order)

```
0 foundation → 1 walking skeleton → 2 bitemporal core → 3 durability (log)
→ 4 blocks & persisted scan → 5 S3 backends & caches → 6 edges & traversal
→ 7 GQL completion & TCK → 8 compaction & GC → 9 server, CLI, query nodes
→ 10 coordination & backpressure → 11 ship (GDPR, fuzz, bench, docs, release)
```

Each slice = 1–3 Claude Code sessions and ends with demonstrably working software.

---

## Slice 0 — Foundation: workspace, types, config, registry, CI

**Detailed plan:** `docs/plans/2026-07-04-slice-00-foundation.md` ✅ written
**Spec:** §4, §15. **Sessions:** 1.

- [x] Cargo workspace + `varve-types` crate: `Iid` (xxh3-128, 16-byte), `LogPosition`
      (epoch u16 | offset u48 packed in u64, sort-correct), `VarveError` base.
      _(Done. Errors shipped per-crate (`TypeError`) not as one `VarveError` base — see STATUS.md decisions.)_
- [x] `varve-config`: TOML config loading with env overrides; `ConfigSection`;
      typed `Registry<T>` + `ComponentFactory<T>` + `Registries` aggregate with
      actionable unknown-name errors.
      _(Done. `Registries` aggregate deferred to `varve-engine`; env overrides are 2-segment/string-only for now — see STATUS.md open items.)_
- [x] CI: GitHub Actions (fmt, clippy -D warnings, test) + `justfile` + `rust-toolchain.toml`.

**Exit criteria:** `just check` green locally and in CI; a toy component registered in a test
resolves from TOML by name; unknown name error lists available implementations.

---

## Slice 1 — Walking skeleton: INSERT → MATCH end-to-end, in memory

**Detailed plan:** `docs/plans/2026-07-04-slice-01-walking-skeleton.md` ✅ written
**Spec:** §3 (writer path), §5.1, §8 (subset), §10 (subset), §11 (embedded). **Sessions:** 2–3.

The whole pipeline with minimal versions of every stage — real parser, real plan, real
DataFusion execution, real live table — no temporal semantics, no durability, nodes only:

- [x] `varve-types`: `Value` enum (Bool/Int/Float/Str/Bytes/Null) ↔ Arrow scalar mapping.
- [x] `varve-gql`: tokenizer; parser for `INSERT (:Label {k: lit, …})`,
      `MATCH (v:Label) [WHERE v.prop = lit] RETURN v.prop [AS name], …`; AST module.
- [x] `varve-index`: `LiveTable` v0 — append node events, snapshot → `RecordBatch`
      (dynamic schema from observed properties); `_iid`, `_labels`, property columns.
- [x] `varve-plan`: AST → `GraphPlan` v0 (`Scan{label}` → `Filter` → `Project`) → DataFusion
      (`MemTable` over snapshot batches, DF expressions for filter/projection).
- [x] `varve-engine` + `varve` facade: `Db::memory()`, `execute()` (parse → event → live
      table, in-memory log v0 assigns `TxId`), `query()` → `Vec<RecordBatch>`.
- [x] Walking-skeleton integration test + `examples/hello.rs`.

**Exit criteria:** integration test: insert 3 people, `MATCH (p:Person) WHERE p.name = 'Ada'
RETURN p.name` returns 1 row through the full pipeline; `cargo run --example hello` prints it.

---

## Slice 2 — Bitemporal core (spec §5.2, §7; the correctness heart)

**Detailed plan:** `docs/plans/2026-07-04-slice-02-bitemporal-core.md` ✅ written
**Sessions:** 2–3. **Depends:** slice 1.

- [x] `varve-types`: temporal types — `Instant` (µs UTC), `END_OF_TIME`, `TemporalBounds`
      / `TemporalDimension` (at/in/between/all).
- [x] Event model in `varve-index`: `Event { iid, system_from, valid_from, valid_to,
      op: Put(doc) | Delete | Erase }`; live table stores events sorted (iid, system_from desc).
- [x] Port XTDB `Ceiling` then `Polygon` (reference: `refs/xtdb/core/src/main/kotlin/xtdb/
      bitemporal/`, spec'd by `refs/xtdb/dev/doc/db.allium`): unit tests from hand-drawn
      rectangle cases, including same-system-time batches and retroactive corrections.
- [x] **Reference model** in `varve-testkit`: naive `BTreeMap`-based bitemporal store with
      the same query API; proptest strategy generating random op histories; equivalence
      property `resolve(events, bounds) == reference(events, bounds)` (10k cases in CI,
      more nightly).
- [x] Wire into scan: resolution during snapshot read; `TemporalBounds` filter parameter.
- [x] GQL surface: parse + plan `FOR VALID_TIME AS OF …` / `FOR SYSTEM_TIME AS OF …`
      (query-level and per-MATCH), `INSERT … VALID FROM/TO`, `DELETE`; defaults
      (valid AS OF now, system AS OF latest); `valid_from(x)`/`valid_to(x)`/`system_from(x)`
      functions on bound elements.
- [x] End-to-end temporal tests: as-of past, retroactive correction visible in old/new
      system time, delete then as-of-before-delete.

**Exit criteria:** property tests green; the four canonical bitemporal scenarios from the
spec pass end-to-end through GQL; slice-1 tests still green (current-time queries are now
just `AS OF now`).

---

## Slice 3 — Durability: pluggable log, group commit, crash safety (spec §6)

**Sessions:** 2. **Depends:** slice 2.

- [x] `varve-log`: `Log` trait (append batch → position; read range; tail from position),
      protobuf record envelope (`prost`): `{tx_id, system_time, effects: per-table Arrow IPC}`;
      registry factories `log/memory`, `log/local`.
- [x] Formalize slice-1 in-memory log onto the trait.
- [x] Local log: segmented append-only files, CRC32C per record, fsync-before-ack,
      torn-tail truncation on open (test: corrupt tail bytes → clean recovery).
- [x] Group commit: bounded submission queue; window (`group_commit_window_ms`, default 15)
      + size (`group_commit_max_bytes`) batching; all txs in a batch acked after one durable
      append.
- [x] Recovery: `Db::open` replays log into live index from position 0 (blocks arrive in
      slice 4); `TxReceipt { tx_id, system_time }` returned from `execute`.
      _(TxReceipt was already delivered in slice 2; replay recovers `next_tx_id = max(tx_id)`
      and floors the clock via `Clock::advance_to`.)_
- [x] Crash harness in `varve-testkit`: spawn child process doing writes, `kill -9` at
      injected fault points (pre-append, post-append-pre-ack, post-ack), restart, assert:
      every acked tx present, no unacked tx visible, log parses cleanly.
      _("no unacked tx visible" formalized per design decision 8: a tx killed BEFORE durability
      never surfaces; a durable-but-unacked tx MAY surface after restart — standard WAL contract.)_

**Exit criteria:** crash matrix green (run 100× in CI without flake); write throughput
smoke bench recorded in STATUS.md; config selects `log = "local"` vs `"memory"` via registry.

---

## Slice 4 — Blocks: flush to object storage, persisted scan, restart (spec §9)

**Sessions:** 2–3. **Depends:** slice 3.

- [x] `varve-storage`: thin wrapper over `object_store` crate behind our trait; key layout
      exactly per spec §9 (`v1/…`, lex-hex trie keys; L0 key `l00-rc-b<lexhex>`, `-p` part
      segment omitted when empty per XTDB `Trie.kt` — decision 15);
      registry factories `storage/memory`, `storage/local`.
- [x] Block manifest (protobuf): log position watermark, per-graph/table trie inventory
      (+ `max_tx_id`/`max_system_time_us` floors for post-trim recovery);
      **manifest write = commit point** (data file without manifest entry is invisible).
- [x] Flush: at `max_block_rows` (default 100k, configurable) or flush timeout — sort live
      events (iid, system_from desc), write L0 `data/<key>.arrow` + `meta/<key>.arrow`
      (per-page min/max for temporal cols), write manifest, trim
      log-replay watermark, reset live table.
- [x] Meta files carry a serialized single-level page index in v1-of-this-slice (the full
      hash-trie branch structure lands with compaction in slice 8; pages of 1024 rows,
      IID-ordered, are the pruning unit now).
- [x] Scan: merge live snapshot + persisted pages by (iid, system_from desc) with bitemporal
      resolution across sources; page pruning by IID range + temporal bounds from meta
      (valid axis deliberately NOT pruned in v1 — decision 4).
- [x] Restart: `Db::open` = latest manifest + log tail replay (extends slice-3 recovery).
- [x] Memory cache tier v1: LRU over (path, byte-range) → Arrow buffers; footer cache.
- [x] Extend property tests: same op history, randomized flush points → identical query
      results as never-flushed reference.

**Exit criteria:** 1M-event ingest → restart → correct temporal queries with < 100ms warm
point lookup; flush-boundary property tests green; crash matrix extended with kill-during-flush
(manifest absent ⇒ clean replay, no corruption).

---

## Slice 5 — S3-API backends, disk cache, capability probe (spec §6, §9, §12; D5/D7)

**Sessions:** 2. **Depends:** slice 4.

- [x] `storage/s3` registry factory: endpoint/bucket/region/path-style config (Garage and
      MinIO need path-style); credentials from config or env. (default-on `s3` feature over
      `object_store/aws`; `path_style` default TRUE; `allow_http` derived from endpoint scheme.)
- [x] `log/object-store` factory: one object per group-commit batch at
      `v1/log/<epoch>/<offset-lexhex>.vlog`; tail = list-after; positions
      assigned locally (designated writer needs no CAS). (`trim` is a documented no-op — sovereign
      store has no delete; GC = slice 8.)
- [x] Disk cache tier: (path, range)-keyed files, survives restart (rebuild index by walking dir),
      `[cache.disk] max_bytes` enforcement. (Deviations: config keys are `[cache.disk] dir`/`max_bytes`
      not `cache.disk_path`/`cache.disk_max`; LRU by **mtime** (touched on hit), not atime; ref-count
      pinning while mapped is vacuously satisfied — reads copy into owned `Bytes`, no mmap path in v1.)
- [x] Capability probe (report-only this slice): 4-step create/create-again/swap/stale-swap on
      `v1/probe`; verdict via `Db::probe_capabilities()` (server `/v1/status` lands in slice 9). Gates
      cas-failover in slice 10. Observed: MinIO=Supported, Garage/SeaweedFS=Inconsistent.
- [x] Integration matrix in `varve-testkit`: **hand-rolled docker-CLI harness** (deviation from
      "testcontainers" — Garage needs multi-step `docker exec` init; zero new deps) for **Garage**,
      **SeaweedFS**, **MinIO** (Ceph demo as weekly CI job); storage+log+Db-e2e+probe suite
      parameterized over backends; probe expected-results table per backend.

**Exit criteria:** [x] entire test suite green with `storage = "s3"` against Garage locally (via
`just s3-matrix`); [x] CI matrix job added (Garage + SeaweedFS + MinIO push/PR; Ceph weekly) — green
in CI pending first push; [x] laptop profile unaffected (local FS default; hello/write_bench/block_bench
unchanged); [x] cold vs warm query latency demonstrates disk cache (`cache_bench`: local 29.7→8.3 ms,
S3/MinIO 167.5→111.1 ms). **✅ SLICE COMPLETE 2026-07-05 (1 session, SDD, 10 tasks).**

---

## Slice 6 — Edges, adjacency, multi-hop traversal, paths (spec §5, §9, §10)

**Sessions:** 2–3. **Depends:** slice 4 (works on local FS; 5 not required).

- [x] Edge events: `_src_iid`/`_dst_iid` columns; `INSERT (a)-[:REL {props}]->(b)` binding
      previously matched or inline nodes; edge `_id` (user or derived).
- [x] Adjacency families: edges flushed under three sort orders (`data/`, `adj-out/`,
      `adj-in/` per spec §9); live table maintains src- and dst-ordered views.
- [x] Pattern lowering: multi-element `MATCH` paths → scans + hash joins on iids
      (DF `HashJoinExec` via logical plan joins); label + property predicates pushed to
      each element's scan; join order by simple size heuristics.
- [x] `PathExpand` custom DataFusion operator: quantified `{m,n}` / bounded `*` expansion,
      breadth-wise iteration with GQL WALK semantics + `max_path_depth` config cap;
      path variables bind lists of elements.
- [x] `DETACH DELETE` (delete node + incident edges as one tx); plain `DELETE` on still-
      connected node errors per GQL.
- [x] Traversal oracle tests: naive in-memory graph walker in `varve-testkit`; property
      tests over random graphs (≤200 nodes) comparing all `{m,n}` expansions; social-graph
      fixture (10k nodes / 60k edges) for integration + perf smoke.

**Exit criteria:** 2-hop friend-of-friend and `-[:KNOWS]->{1,3}` queries correct vs oracle;
bitemporal traversal correct (edge validity respected at AS-OF time — new property tests);
2-hop over the fixture < 50ms warm.
**✅ SLICE COMPLETE 2026-07-06 (1 session, SDD, 12 tasks + 1 perf-opt task).** Exit-criteria evidence:
`{1,3}` friend-of-friend + 2-hop correct vs the independent traversal oracle (pure property at 10k
cases + e2e property + 10k/60k social-graph fixture cross-check); bitemporal AS-OF traversal
property-tested (future-valid edges invisible at NOW, visible AS OF); **warm 2-hop 16.23 ms < 50 ms**
after the anchor-reachable edge-pruning optimization (was 88.92 ms full-scan); `just check` green,
clippy `-D warnings` clean. A mid-slice Critical (props-ignoring `DELETE` over-delete) was caught by
review and fixed. Demo: `cargo run --release --example traversal_bench -p varve`.

---

## Slice 7 — GQL practical-core completion + conformance harness (spec §8; D2)

**Sessions:** 5. **Status:** ✅ complete. **Depends:** slice 6.

- [x] Expression completion: full operator set with 3-valued logic; `CASE`; `EXISTS {…}`
      subqueries; parameters (`$p`); string/numeric/list/temporal function library behind
      `FunctionRegistry` (registered as DF UDFs); `CAST`.
- [x] Statement completion: `OPTIONAL MATCH` (left-join lowering), `FILTER`, `LET`, `FOR`
      (unwind), `ORDER BY`/`SKIP`/`LIMIT`/`OFFSET`, `UNION [ALL]`, `RETURN DISTINCT`,
      aggregation with implicit grouping (`COUNT/SUM/AVG/MIN/MAX/COLLECT`).
  _(Slice 7 Task 6 complete: OPTIONAL MATCH/FILTER/LET/FOR and multi-MATCH/multi-path
  pipeline lowering shipped; ORDER/UNION/DISTINCT/aggregation shipped in Task 8.)_
_(Slice 7 Task 7 complete: `_labels` snapshot column plus multi-label node conjunction
and alternation matching shipped; roadmap boxes are closed at slice exit.)_
_(Slice 7 Task 9 complete: top-level `WHERE`/`FILTER` `EXISTS` and `NOT EXISTS`
subqueries shipped with nested semi/anti joins, temporal inheritance, and quantified-path coverage;
expression-completion box is closed at slice exit.)_
- [x] Mutation completion: `SET` (props/labels), `REMOVE`, multi-statement tx bodies
      (`execute` takes a statement block, all-or-nothing), label ops; writer-side
      read-modify-write planning via the query engine.
_(Slice 7 Task 10 complete: mutation `MATCH` parts now resolve through the query
engine, including hop patterns for `MATCH ... INSERT` and `DELETE`; quantified
hops/path vars fail with engine `Unsupported`. Task 11 complete: `SET`/`REMOVE`
props and labels now use writer-side bitemporal RMW through query-engine
bindings. Task 12 complete: `ERASE`/`DETACH ERASE` now share DELETE
resolution and emit `Op::Erase`. Task 13 complete: multi-statement programs
execute all-or-nothing in one tx with statement-local overlay visibility.)_
- [x] Catalog minimal: `CREATE GRAPH`/`DROP GRAPH`/`USE` (namespace prefixes). _(Slice 7 Task 14 complete: `GraphsState`, `__meta` catalog entries, graph-routed IIDs/storage/flush/recovery, same-window catalog group-commit barriers, flushed catalog restart coverage.)_
- [x] **TCK harness** in `varve-testkit`: openCypher TCK feature-file parser; mechanical
      translation layer (`CREATE`→`INSERT`, etc.); per-scenario allowlist with recorded
      exclusion reasons; pass-rate report artifact in CI; regressions block merge.
_(Slice 7 Task 16 complete: openCypher TCK features vendored at pinned commit with
Apache-2.0 license/provenance; Gherkin subset parser parses all 220 vendored
feature files / 1,615 scenarios; TCK value parser and `RecordBatch` comparator
cover primitives, lists, maps, node/relationship reconstruction, unordered
multiset comparison, nested Arrow values, table escapes, and over-projection
checks. Task 17 shipped Cypher→GQL translation, fresh-Db scenario runner,
feature-qualified exclusions, strict error-class assertions, baseline/core/pass-rate
gate, stable exclusion-reason guards, and `target/tck-report.json` +
`target/tck-outcomes.tsv` CI artifact upload. Current gate: 3,897 expanded
outcomes, 3,386 excluded, 511 adapted, 445 passed, 66 honest non-excluded
failures, pass rate 0.870841 >= 0.85.)_
- [x] **ANTLR differential oracle** (CI-only, Java): generate parser from
      `resources/gql-grammar/`; accept/reject comparison over the corpus + fuzz seeds.
      _(Slice 7 Task 18 complete: committed `resources/gql-corpus/` verdict
      corpus, `varve-testkit` `parse_corpus` bin + local corpus test,
      `scripts/gql_diff` Java/Python harness, `gql-differential` CI job with
      missing-file self-check; local generated-ANTLR differential run green.)_
- [x] Parser fuzz target (`cargo-fuzz`): no panics, parse-print-reparse stability. _(Slice 7 Task 15 complete: `varve-gql` printer API, parse-print-reparse tests/proptest, standalone cargo-fuzz `parse` target, nightly `fuzz-nightly` CI, 10-min Task 25 reclose green on 2026-07-09.)_

**Exit criteria:** curated core TCK list 100% green; overall adapted TCK pass-rate remains above 85% with all exclusions reasoned in-repo; ANTLR differential configured and local corpus/compare checks green; parser fuzz nightly configured. Task 25 reclose verification green: demo `cargo run --release --example gql_tour -p varve`; workspace tests 596 passed; clippy/fmt clean; release traversal oracle `PROPTEST_CASES=1024` 6 passed in 49.17s; TCK 445/511 adapted passed (0.870841; 3,897 total, 3,386 excluded, 66 non-excluded failures); parser fuzz 10-min run 13,903,093 execs with no crashes.

Post-exit Tasks 20-25 complete: configurable traversal/query budgets, streaming `PathExpandExec` batches, reachable-edge BFS budgets, traversal-oracle CPU reduction, TCK side effects/path values, and final fuzz reclose.

---

## Slice 8 — Compaction, tries, GC (spec §9; D-determinism)

**Sessions:** 3. **Depends:** slices 4, 6 (adjacency families compact too).

- [x] Full hash trie (branch factor 4 on IID bits, `LOG_LIMIT` 64 / `PAGE_LIMIT` 1024 —
      adopt XTDB constants) for live index and persisted meta files; scan pruning by trie
      path (`Bucketer::filter_iids_for_path` equivalent).
- [x] Trie catalog: per (table, shard=(level, recency, part)) lists nascent/live/garbage,
      as a pure fold over manifest history (reference: `refs/xtdb/dev/doc/trie-cat.allium`).
- [x] Job calculator: pure `fn(catalog) -> Vec<Job>` (reference: `refs/xtdb/dev/doc/
      compaction.allium`): L0→L1C+L1H recency split (weekly buckets, Monday 0000Z);
      4 same-shard files at Ln → L(n+1) partitioned by next 2 IID bits; ~100MB target.
- [x] Merge: k-way by (iid, system_from desc) with `Polygon` resolution; recency routing;
      `Erase` events physically drop matching rows; page normalization (~1k rows).
- [x] Determinism: **byte-identical output** golden tests — same inputs merged under
      different thread counts/seeds/machines ⇒ identical file bytes; nascent→live→garbage
      lifecycle so queries only see live tries; duplicate-job tolerance test (two compactors
      race the same job ⇒ identical object, last-write-wins harmless).
- [x] GC: delete garbage tries + orphan data files past `gc.retention` (default 7d);
      unreferenced-manifest pinning respected.
- [x] Churn benchmark: sustained update-heavy workload keeps storage bounded (demo:
      192 objects before compaction, 3 after GC; report in STATUS.md).

**Exit criteria:** ✅ complete 2026-07-09. Golden determinism tests green; compaction
equivalence property tests green; raw-object erase proof green; storage plateau demo green.
Final verification: `rtk cargo fmt --all --check`; `rtk cargo clippy --workspace --all-targets -- -D warnings`;
`rtk cargo test --workspace -- --test-threads=1` (630 passed, 71 suites);
`rtk cargo test -p varve-testkit --test compaction_equivalence -- --test-threads=1` (4 passed);
`rtk cargo run --release --example compaction_gc -p varve` (192 objects before compaction, 3 after GC,
62 current rows).

---

## Slice 9 — Server, CLI, query-node role (spec §3, §11)

**Sessions:** 2–3. **Depends:** slices 5, 7.

- [x] Query-node role in `varve-engine`: open in `query` role → tail log from latest
      manifest watermark, apply effect events to own live index (no GQL re-execution);
      basis tokens: query waits (bounded, configurable) until `tx_id` applied.
- [x] `varve-server` (`varved` binary): axum; `POST /v1/query` (JSON body; response JSON or
      Arrow IPC stream by `Accept`), `POST /v1/tx`, `GET /healthz`, `GET /metrics`
      (Prometheus), `GET /v1/status` (role, log position, watermark, probe verdict);
      bearer-token auth behind `Authenticator` trait; rustls TLS option; writes to a
      query node → 421 + writer address from `v1/writer.json`.
- [x] `varve-cli` (`varve` binary): `shell` REPL (embedded dir or remote URL; table output),
      `import`/`export` (JSONL via normal tx path), `admin status|compact|gc|verify`.
- [x] `ProtocolFrontend` trait + registry entry for http (Bolt/pgwire are roadmap slots).
- [x] Docker: multi-stage build, single static-ish binary image; docker-compose demo:
      Garage + 1 writer + 2 query nodes.
- [x] Read-your-writes integration test across processes: write to writer, immediately query
      both query nodes with basis token ⇒ correct; without token ⇒ eventually consistent.

**Exit criteria:** compose demo runs the slice-6 fixture workload end-to-end over HTTP;
Arrow IPC streaming verified from a Rust client; CLI shell round-trips; read scale-out
shown (2 query nodes serve concurrent reads while writer ingests).

---

## Slice 10 — Coordination, failover, backpressure, observability (spec §12)

**Sessions:** 2. **Depends:** slice 9.

- [x] `Coordinator` trait + registry: `designated-writer` (default) — writer heartbeats
      `v1/writer.json` (plain PUT, timestamped); second writer starting while heartbeat is
      fresh refuses with clear error (best-effort guard, documented as such).
- [x] `cas-failover` coordinator (feature-gated): lease object via `If-None-Match`/`If-Match`
      through `object_store` `PutMode`; log **epoch increment on takeover** fences the old
      writer (stale appends land in a dead epoch and are ignored); only enabled when the
      slice-5 probe passes at startup, else hard error naming the backend capability.
- [x] Failover test (MinIO or local CAS-semantics store): kill writer, standby takes over
      < 10s, zero acked-tx loss, zombie writer's late appends provably ignored.
- [x] Backpressure: bounded submission queue (429/wait on full); live-index memory watermark
      forces early flush; slow query node lag metric (never affects writer).
- [x] Observability completion: `tracing` spans across submit→commit→apply→flush and
      parse→plan→execute; Prometheus metrics per spec §12 list; OpenTelemetry export behind
      `MetricsSink`.

**Exit criteria:** failover demo green (CAS store) + Garage correctly refuses cas-failover
mode with actionable error; chaos test (random writer kills under load, 30min) — no
corruption, no acked loss; Grafana-ready metrics documented.

---

## Slice 11 — Ship: GDPR verify, fuzzing, benchmarks, docs, release (spec §1, §13)

**Sessions:** 2–3. **Depends:** all.

- [x] `ERASE` end-to-end verification: erase entity → immediate invisibility at every time
      axis → after compaction, raw object scan proves the property bytes are gone (test
      greps storage objects for sentinel values). *(Slice 11 Tasks 1-3: GC log/probe sweep +
      local-profile whole-disk-byte proof + object-store-log raw-object-scan proof + DETACH ERASE.)*
- [x] Fuzz targets complete: log-record decoder, manifest decoder, Arrow meta reader against
      corrupted/truncated inputs (no panics, clean errors); nightly CI fuzz budget. *(Slice 11
      Tasks 5-6: 5 targets, 5-way nightly matrix; arrow-rs IPC panics guarded by catch_unwind +
      framing-allocation bounded, gates on RSS; deeper record-batch-descriptor over-reservation
      is production-safe/clean-Err + owned post-v1 follow-up.)*
- [ ] Benchmark suite: criterion micro (resolution, trie ops, parse) + end-to-end social
      workload (ingest rate, point read, 2-hop, AS-OF historical, query-node scale-out 1→4);
      compare against spec §13 targets; publish `docs/benchmarks/v1.md` report.
- [ ] Docs site (mdBook under `docs/book/`): getting started (laptop 5-min), GQL reference
      with temporal extensions + deviations list, capability matrix per backend
      (Garage/Ceph/SeaweedFS/MinIO/AWS), ops guide (profiles, config reference generated
      from code, failover modes, sizing), architecture overview.
- [ ] Release engineering: `cargo dist` (or equiv) for macOS arm64 + linux x86_64/arm64
      (musl) binaries; Docker images; `varve` + subcrates published to crates.io;
      CHANGELOG; `v1.0.0` tag; README quickstart.
- [ ] Final acceptance pass: walk spec §1 success criteria 1–8, each with a linked
      passing test/demo/report; fix gaps before tagging.

**Exit criteria:** every spec §1 criterion checked off with evidence; `cargo install varve`
/ `docker run` / `brew`-style tarball all reach a working shell in ≤ 5 minutes.

---

## Spec-coverage map (self-review)

| Spec section | Slices |
|---|---|
| §1 success criteria 1–8 | 1+9 · 7 · 2 · 5 · 9+10 · 8 · 3 · 11 |
| §3 roles/one-log | 1, 3, 9 |
| §4 interfaces/registry/config | 0, every backend slice adds factories |
| §5 data model/events/IIDs | 1 (nodes), 2 (events), 6 (edges) |
| §6 log | 3 (trait/local), 5 (object-store) |
| §7 bitemporal engine | 2 (+4, 8 integration) |
| §8 GQL surface | 1 (subset), 2 (temporal), 6 (paths), 7 (completion) |
| §9 storage/tries/compaction/caching | 4, 5, 8 |
| §10 query engine | 1 (lowering), 4 (BitemporalScan), 6 (PathExpand), 7 (functions) |
| §11 API/embedded/server/CLI | 1, 9 |
| §12 coordination/ops | 5 (probe), 10 |
| §13 testing/perf | distributed: 2, 3, 5, 6, 7, 8, 11 |
| §16 risks | grammar oracle (7), probe (5/10), reference model (2), pins (0/1) |
