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

- [ ] `varve-storage`: thin wrapper over `object_store` crate behind our trait; key layout
      exactly per spec §9 (`v1/…`, lex-hex trie keys `l<level>-r<recency>-p<part>-b<block>`);
      registry factories `storage/memory`, `storage/local`.
- [ ] Block manifest (protobuf): log position watermark, per-graph/table trie inventory;
      **manifest write = commit point** (data file without manifest entry is invisible).
- [ ] Flush: at `max_block_rows` (default 100k, configurable) or flush timeout — sort live
      events (iid, system_from desc), write L0 `data/<key>.arrow` + `meta/<key>.arrow`
      (per-page min/max for temporal cols + per-column min/max), write manifest, trim
      log-replay watermark, reset live table.
- [ ] Meta files carry a serialized single-level page index in v1-of-this-slice (the full
      hash-trie branch structure lands with compaction in slice 8; pages of ~1k rows,
      IID-ordered, are the pruning unit now).
- [ ] Scan: merge live snapshot + persisted pages by (iid, system_from desc) with bitemporal
      resolution across sources; page pruning by IID range + temporal bounds from meta.
- [ ] Restart: `Db::open` = latest manifest + log tail replay (extends slice-3 recovery).
- [ ] Memory cache tier v1: LRU over (path, byte-range) → Arrow buffers; footer cache.
- [ ] Extend property tests: same op history, randomized flush points → identical query
      results as never-flushed reference.

**Exit criteria:** 1M-event ingest → restart → correct temporal queries with < 100ms warm
point lookup; flush-boundary property tests green; crash matrix extended with kill-during-flush
(manifest absent ⇒ clean replay, no corruption).

---

## Slice 5 — S3-API backends, disk cache, capability probe (spec §6, §9, §12; D5/D7)

**Sessions:** 2. **Depends:** slice 4.

- [ ] `storage/s3` registry factory: endpoint/bucket/region/path-style config (Garage and
      MinIO need path-style); credentials from config or env.
- [ ] `log/object-store` factory: one object per group-commit batch at
      `v1/log/<epoch>/<offset-lexhex>.vlog`; tail = list-after + poll interval; positions
      assigned locally (designated writer needs no CAS).
- [ ] Disk cache tier: (path, range)-keyed files under `cache.disk_path`, LRU by atime,
      ref-count pinning while mapped, survives restart (rebuild index by walking dir),
      `cache.disk_max` enforcement.
- [ ] Capability probe (report-only this slice): attempt `If-None-Match: *` create + ETag
      `If-Match` swap on `v1/probe`; record verdict in status output. Used to gate
      cas-failover in slice 10.
- [ ] Integration matrix in `varve-testkit`: testcontainers for **Garage**, **SeaweedFS**,
      **MinIO** (Ceph demo container as optional weekly CI job); full storage+log test suite
      parameterized over backends; probe expected-results table per backend.

**Exit criteria:** entire test suite green with `storage = "s3"` against Garage locally;
CI matrix job green (Garage + SeaweedFS + MinIO); laptop profile unaffected (local FS default);
cold vs warm query latency demonstrates disk cache.

---

## Slice 6 — Edges, adjacency, multi-hop traversal, paths (spec §5, §9, §10)

**Sessions:** 2–3. **Depends:** slice 4 (works on local FS; 5 not required).

- [ ] Edge events: `_src_iid`/`_dst_iid` columns; `INSERT (a)-[:REL {props}]->(b)` binding
      previously matched or inline nodes; edge `_id` (user or derived).
- [ ] Adjacency families: edges flushed under three sort orders (`data/`, `adj-out/`,
      `adj-in/` per spec §9); live table maintains src- and dst-ordered views.
- [ ] Pattern lowering: multi-element `MATCH` paths → scans + hash joins on iids
      (DF `HashJoinExec` via logical plan joins); label + property predicates pushed to
      each element's scan; join order by simple size heuristics.
- [ ] `PathExpand` custom DataFusion operator: quantified `{m,n}` / bounded `*` expansion,
      breadth-wise iteration with GQL WALK semantics + `max_path_depth` config cap;
      path variables bind lists of elements.
- [ ] `DETACH DELETE` (delete node + incident edges as one tx); plain `DELETE` on still-
      connected node errors per GQL.
- [ ] Traversal oracle tests: naive in-memory graph walker in `varve-testkit`; property
      tests over random graphs (≤200 nodes) comparing all `{m,n}` expansions; social-graph
      fixture (10k nodes / 60k edges) for integration + perf smoke.

**Exit criteria:** 2-hop friend-of-friend and `-[:KNOWS]->{1,3}` queries correct vs oracle;
bitemporal traversal correct (edge validity respected at AS-OF time — new property tests);
2-hop over the fixture < 50ms warm.

---

## Slice 7 — GQL practical-core completion + conformance harness (spec §8; D2)

**Sessions:** 3. **Depends:** slice 6.

- [ ] Expression completion: full operator set with 3-valued logic; `CASE`; `EXISTS {…}`
      subqueries; parameters (`$p`); string/numeric/list/temporal function library behind
      `FunctionRegistry` (registered as DF UDFs); `CAST`.
- [ ] Statement completion: `OPTIONAL MATCH` (left-join lowering), `FILTER`, `LET`, `FOR`
      (unwind), `ORDER BY`/`SKIP`/`LIMIT`/`OFFSET`, `UNION [ALL]`, `RETURN DISTINCT`,
      aggregation with implicit grouping (`COUNT/SUM/AVG/MIN/MAX/COLLECT`).
- [ ] Mutation completion: `SET` (props/labels), `REMOVE`, multi-statement tx bodies
      (`execute` takes a statement block, all-or-nothing), label ops; writer-side
      read-modify-write planning via the query engine.
- [ ] Catalog minimal: `CREATE GRAPH`/`DROP GRAPH`/`USE` (namespace prefixes).
- [ ] **TCK harness** in `varve-testkit`: openCypher TCK feature-file parser; mechanical
      translation layer (`CREATE`→`INSERT`, etc.); per-scenario allowlist with recorded
      exclusion reasons; pass-rate report artifact in CI; regressions block merge.
- [ ] **ANTLR differential oracle** (CI-only, Java): generate parser from
      `resources/gql-grammar/`; accept/reject comparison over the corpus + fuzz seeds.
- [ ] Parser fuzz target (`cargo-fuzz`): no panics, parse-print-reparse stability.

**Exit criteria:** curated core TCK list 100% green + overall adapted pass-rate ≥ 85% with
every exclusion reasoned in-repo; differential oracle green; fuzzer runs 10min in CI nightly
without findings.

---

## Slice 8 — Compaction, tries, GC (spec §9; D-determinism)

**Sessions:** 3. **Depends:** slices 4, 6 (adjacency families compact too).

- [ ] Full hash trie (branch factor 4 on IID bits, `LOG_LIMIT` 64 / `PAGE_LIMIT` 1024 —
      adopt XTDB constants) for live index and persisted meta files; scan pruning by trie
      path (`Bucketer::filter_iids_for_path` equivalent).
- [ ] Trie catalog: per (table, shard=(level, recency, part)) lists nascent/live/garbage,
      as a pure fold over manifest history (reference: `refs/xtdb/dev/doc/trie-cat.allium`).
- [ ] Job calculator: pure `fn(catalog) -> Vec<Job>` (reference: `refs/xtdb/dev/doc/
      compaction.allium`): L0→L1C+L1H recency split (weekly buckets, Monday 0000Z);
      4 same-shard files at Ln → L(n+1) partitioned by next 2 IID bits; ~100MB target.
- [ ] Merge: k-way by (iid, system_from desc) with `Polygon` resolution; recency routing;
      `Erase` events physically drop matching rows; page normalization (~1k rows).
- [ ] Determinism: **byte-identical output** golden tests — same inputs merged under
      different thread counts/seeds/machines ⇒ identical file bytes; nascent→live→garbage
      lifecycle so queries only see live tries; duplicate-job tolerance test (two compactors
      race the same job ⇒ identical object, last-write-wins harmless).
- [ ] GC: delete garbage tries + orphan data files past `gc.retention` (default 7d);
      unreferenced-manifest pinning respected.
- [ ] Churn benchmark: sustained update-heavy workload keeps query latency and storage
      bounded (report in STATUS.md).

**Exit criteria:** golden determinism tests green; property tests extended across compaction
(same results pre/post-compact); erase → bytes provably absent from post-compaction objects;
storage plateaus under churn.

---

## Slice 9 — Server, CLI, query-node role (spec §3, §11)

**Sessions:** 2–3. **Depends:** slices 5, 7.

- [ ] Query-node role in `varve-engine`: open in `query` role → tail log from latest
      manifest watermark, apply effect events to own live index (no GQL re-execution);
      basis tokens: query waits (bounded, configurable) until `tx_id` applied.
- [ ] `varve-server` (`varved` binary): axum; `POST /v1/query` (JSON body; response JSON or
      Arrow IPC stream by `Accept`), `POST /v1/tx`, `GET /healthz`, `GET /metrics`
      (Prometheus), `GET /v1/status` (role, log position, watermark, probe verdict);
      bearer-token auth behind `Authenticator` trait; rustls TLS option; writes to a
      query node → 421 + writer address from `v1/writer.json`.
- [ ] `varve-cli` (`varve` binary): `shell` REPL (embedded dir or remote URL; table output),
      `import`/`export` (JSONL via normal tx path), `admin status|compact|gc|verify`.
- [ ] `ProtocolFrontend` trait + registry entry for http (Bolt/pgwire are roadmap slots).
- [ ] Docker: multi-stage build, single static-ish binary image; docker-compose demo:
      Garage + 1 writer + 2 query nodes.
- [ ] Read-your-writes integration test across processes: write to writer, immediately query
      both query nodes with basis token ⇒ correct; without token ⇒ eventually consistent.

**Exit criteria:** compose demo runs the slice-6 fixture workload end-to-end over HTTP;
Arrow IPC streaming verified from a Rust client; CLI shell round-trips; read scale-out
shown (2 query nodes serve concurrent reads while writer ingests).

---

## Slice 10 — Coordination, failover, backpressure, observability (spec §12)

**Sessions:** 2. **Depends:** slice 9.

- [ ] `Coordinator` trait + registry: `designated-writer` (default) — writer heartbeats
      `v1/writer.json` (plain PUT, timestamped); second writer starting while heartbeat is
      fresh refuses with clear error (best-effort guard, documented as such).
- [ ] `cas-failover` coordinator (feature-gated): lease object via `If-None-Match`/`If-Match`
      through `object_store` `PutMode`; log **epoch increment on takeover** fences the old
      writer (stale appends land in a dead epoch and are ignored); only enabled when the
      slice-5 probe passes at startup, else hard error naming the backend capability.
- [ ] Failover test (MinIO or local CAS-semantics store): kill writer, standby takes over
      < 10s, zero acked-tx loss, zombie writer's late appends provably ignored.
- [ ] Backpressure: bounded submission queue (429/wait on full); live-index memory watermark
      forces early flush; slow query node lag metric (never affects writer).
- [ ] Observability completion: `tracing` spans across submit→commit→apply→flush and
      parse→plan→execute; Prometheus metrics per spec §12 list; OpenTelemetry export behind
      `MetricsSink`.

**Exit criteria:** failover demo green (CAS store) + Garage correctly refuses cas-failover
mode with actionable error; chaos test (random writer kills under load, 30min) — no
corruption, no acked loss; Grafana-ready metrics documented.

---

## Slice 11 — Ship: GDPR verify, fuzzing, benchmarks, docs, release (spec §1, §13)

**Sessions:** 2–3. **Depends:** all.

- [ ] `ERASE` end-to-end verification: erase entity → immediate invisibility at every time
      axis → after compaction, raw object scan proves the property bytes are gone (test
      greps storage objects for sentinel values).
- [ ] Fuzz targets complete: log-record decoder, manifest decoder, Arrow meta reader against
      corrupted/truncated inputs (no panics, clean errors); nightly CI fuzz budget.
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
