# Varve Implementation Status Ledger

> Update end EVERY session. entry point next session — read first, then `varve-v1-roadmap.md`, then current slice's detailed plan.

## Current position
- **Current entry point:** Slice 9 planning in `docs/plans/varve-v1-roadmap.md` -- write the Slice 9 detailed plan for server, CLI, and query-node role, then execute it task-by-task.
- **Current slice:** 9 (Server, CLI, query nodes) -- NOT STARTED. Slice 8 is COMPLETE.
- **Slice 8 completed session:** Task 14 whole-slice verification and docs closeout.
- **Slice 8 shipped:** hash-trie key/parser and page path metadata; recovery/query trie-path pruning; manifest-history trie catalog; GC-only object-store delete; deterministic embedded compaction through manifest state; compaction query equivalence coverage; retention-aware embedded `Db::gc_once()`/`GcReport`; raw-object GDPR erase proof; churn plateau smoke/demo.
- **Slice 8 Task 13 deviation RESOLVED (2026-07-10):** compaction manifest commits now run through the single writer and share its manifest-generation allocator; the plateau smoke churns through compact+GC cycles on one `Db` handle.
- **Slice 8 final verification:** `rtk cargo fmt --all --check`; `rtk cargo clippy --workspace --all-targets -- -D warnings`; `rtk cargo test --workspace -- --test-threads=1` (630 passed, 71 suites); `rtk cargo test -p varve-testkit --test compaction_equivalence -- --test-threads=1` (4 passed); `rtk cargo run --release --example compaction_gc -p varve` printed objects before compaction 192, compaction jobs 1, objects after compaction 195, GC deleted 192 planned / 192 deleted, objects after GC 3, current rows 62.
- **Slice 7:** COMPLETE (2026-07-09 reclosed through Task 25). Demo: `cargo run --release --example gql_tour -p varve`.
- **Slice 7 catalog/data program deviation (recorded 2026-07-10):** a single mutation program may contain catalog statements or data statements, but not both. The v1 log groups one effect batch by graph, while catalog effects target `__meta` and data effects target the routed graph; mixed programs therefore return an explicit `Unsupported` error instead of silently splitting the promised one-record transaction across graphs.
- **Slice 7 review remediation (2026-07-10):** fixed binary string predicates, OPTIONAL MATCH WHERE left-row preservation, configured EXISTS expansion budgets, and explicit IID-tuple ordering for SET last-wins; aligned byte parameters, aggregation grouping, UNION errors, and stale unsupported messages with the plan; consolidated expression printing and IID binding decoding. Verification: `cargo fmt --all --check`; workspace clippy with warnings denied; 660 workspace tests across 71 suites.
- **Slice 6:** COMPLETE (2026-07-06). Demo: `cargo run --release --example traversal_bench -p varve`.
- **Slice 5:** COMPLETE (2026-07-05). Demo: `cargo run --release --example cache_bench -p varve`; `just s3-matrix`.
- **Slice 4:** COMPLETE (2026-07-05). Demo: `cargo run --release --example block_bench -p varve`.
- **Slice 3:** COMPLETE (2026-07-05). Demo: `cargo run --release --example write_bench -p varve`.
- **Slice 2:** COMPLETE (2026-07-04). Demo: `cargo run --example time_travel -p varve`.
- **Slice 1:** COMPLETE (2026-07-04). Demo: `cargo run --example hello -p varve`.

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
- **Dependency pins (slice 4, verified live 2026-07-05):** `object_store = "0.13"` (resolved 0.13.2 —
  MUST track datafusion's transitive pin, same rule as arrow; re-derive `cargo tree -p datafusion | grep
  object_store`; `Cargo.lock` has exactly one), `bytes = "1"`, `futures = "0.3"` (both already transitive
  via datafusion). `varve-engine` now ALSO declares a `fault-injection = []` feature (flush crash hooks
  `pre-manifest-put`/`post-manifest-put`, same pattern/caveats as `varve-log`'s); `varve-testkit` enables it
  via a direct `varve-engine { features = ["fault-injection"] }` dep so cargo unifies the single engine
  artifact with the hooks live. `Db::local(dir)` layout is now `dir/log` (log) + `dir/store` (blocks).
- **Dependency/feature facts (slice 5, verified live 2026-07-05):** `varve-storage`'s default-on `s3` feature enables
  `object_store/aws`, pulling reqwest + quick-xml (+ TLS stack) transitively into `Cargo.lock` — `--no-default-features` gives a
  local-only build. `varve-log`'s default-on `object-store` feature adds optional `varve-storage` + `bytes` deps (the
  integration test `tests/object_store_log.rs` is NOT feature-gated, so `cargo test -p varve-log --no-default-features` won't
  compile the test — the LIB does; CI only runs default features). `xxhash-rust` (already a workspace dep) now also names disk-cache
  files. `varve-testkit` moved `tempfile` to `[dependencies]` and added `varve-config` (deps) + `bytes` (dev-dep). Container tests are
  gated by `VARVE_S3_BACKENDS` (comma-list or `all`) and skip silently otherwise; image pins live ONLY in `varve-testkit/src/backends.rs`
  (tags listed in slice-5 decision 7). Live matrix runner: `just s3-matrix` (defaults garage,seaweedfs,minio).
- Gate: `just check` = `cargo fmt --all --check` + `cargo clippy --workspace --all-targets
  -- -D warnings` + `cargo test --workspace`. Same three commands run in CI (`.github/workflows/ci.yml`).
  CI gained a `crash-matrix` job (slice 3): `cargo test -p varve-testkit --release --test
  crash_recovery` with `VARVE_CRASH_ITERS=100` (default 3 in the normal `check` run); gated
  `!= 'schedule'`. `just crash` runs it at 10 iterations locally.
- `Cargo.lock` IS committed (workspace ships binaries; `xxhash-rust` defines the on-disk `Iid`
  byte format, so it is pinned). `.superpowers/` is SDD scratch (self-ignored).

## Decisions made during implementation

- **2026-07-06 (slice 6) edge model + IPC (decisions 1–3):** `Event` gained `src: Option<Iid>`/`dst: Option<Iid>`
  (Some on EVERY edge event incl. Delete/Erase; None on nodes; endpoints immutable per edge `_id`). Event IPC gained
  two NULLABLE `FixedSizeBinary(16)` cols `_src_iid`/`_dst_iid` between `_valid_to` and `op` (op/payload shift to
  6/7); `encode/decode_events` sigs unchanged; IPC bytes still NOT golden-pinned (round-trip only). `LiveTable` keeps
  `out`/`in_` `BTreeMap<Iid,BTreeSet<Iid>>` views (updated in `append` when both endpoints present, incl. Delete —
  visibility resolved at read time); edge snapshots emit non-null endpoint cols; mixed node/edge in one snapshot errors.
- **2026-07-06 (slice 6) `TableState` split + adjacency families (decisions 3–5):** `TableState { nodes: TableCore,
  edges: TableCore, adj_out: Vec<PersistedTrie>, adj_in: Vec<PersistedTrie> }` under ONE RwLock (slice-4 decision 8
  upheld). `encode_block_by(live, rows, SortOrder::{ByIid,BySrc,ByDst})` sorts `(sort_key, iid, system_from desc)` and
  records the SORT KEY range in `PageMeta.min_iid/max_iid`, so `PageMeta::selected(bounds, Some(anchor))` prunes adj
  pages by src/dst with ZERO new prune code; `encode_block` = thin wrapper over `ByIid` (BYTE-IDENTICAL, pinned +
  10k flush_equivalence proptest). One flush = one block_id = up to 4 `TableTries` (nodes primary, edges primary,
  edges adj-out, edges adj-in) in ONE atomic manifest PUT (crash hooks unmoved; crash matrix green). `TableTries.family`
  = prost string tag 4 (`""`=primary; empty encodes to zero bytes ⇒ old golden wire unchanged). Keys
  `v1/graphs/<g>/tables/<t>/<family>/{data,meta}/<trie>.arrow`. `edge_adjacency(anchor)` == full result filtered to
  anchor, across live+flushed+restart (property-verified).
- **2026-07-06 (slice 6) GQL grammar reshape (decisions 6–9,15,16):** path-based AST — `NodePattern{var, labels:Vec,
  props}`, `EdgePattern{var, label (REQUIRED v1 — unlabeled edge = parse error), props, direction, quantifier}`,
  `Quantifier{min, max:Option}`, `PathPattern`, `MatchPart`, reshaped `InsertStmt{match_part, paths, valid_from/to}`,
  `DeleteStmt.detach`, `QueryStmt.paths`+`single_node()`, `ReturnItem::Var`; `InsertNode` deleted. `{n}`⇒(n,n),
  `{m,n}`, `{m,}`⇒(m,max_path_depth), `*`⇒(0,max_path_depth); quantified-edge-with-var = slice-7 error. Negative
  literals parse (`Minus` arm). Multi-label node in AST but MATCH uses ≤1 (>1 ⇒ Unsupported). Comma-multi-path MATCH ⇒
  Unsupported (single linear path v1).
- **2026-07-06 (slice 6) writer effects + bindings (decision 6c):** `Effects{nodes,edges}`; `resolve_insert` async
  (reads match-part), statement-local bindings via Cartesian product of per-variable candidate iids; edge id fallback
  `varve:gen:{tx_id}:{ordinal}`. A resolve failure (Unbound/AlreadyBound) applies NOTHING (atomic). `MATCH…INSERT` and
  DETACH DELETE read at current snapshot (slice-3 pattern).
- **2026-07-06 (slice 6) DETACH DELETE + still-connected (decision 8):** plain `DELETE` on a node with ≥1 visible
  incident edge (live OR flushed) → `EngineError::StillConnected(count)`, whole tx fails, nothing applied. `DETACH
  DELETE` emits node Delete + one edge Delete per distinct incident edge (self-loops deduped via BTreeMap keyed by edge
  iid → deleted once) in ONE tx. Incidence uses label-BLIND `incident_edges` (both directions).
- **2026-07-06 (slice 6) pattern lowering + PathExpand (decisions 9–13):** multi-element MATCH → per-element scans with
  mangled `{var}__{col}` columns, label+inline-prop+WHERE pushdown per element, left-deep hash-join chain oriented by a
  terminal-size heuristic. `PathExpand` = real DataFusion extension (`PathExpandNode` UDLN + `PathExpandExec` +
  `VarveQueryPlanner`); `expand_paths` = WALK semantics (repeats allowed, depth-cap termination, min=0 zero-length,
  interleaved `[n0,e1,n1,…]`); EdgeAdjacency built engine-side AT QUERY BOUNDS (AS-OF-correct); `RETURN p` = List<FSB16>
  (single quantified hop only). `[query] max_path_depth` (default 10) caps `*`/`{m,}`; explicit bound > cap errors.
  Quantified-hop inline props filter EVERY traversed hop (decision 13). Task-8 consolidated the single-node guard into
  `scan_specs` (deleted `require_single_node`/`execute_query`/`snapshot_for_query`).
- **2026-07-06 (slice 6) traversal oracle + property economics (decision 14):** `GraphOracle` is an INDEPENDENT naive
  BFS walker (structurally cannot import `expand_paths` — varve-plan is a dev-dep, oracle.rs is lib code). Pure-layer
  property (`expand_paths` == oracle.walk) at full PROPTEST_CASES; e2e-layer (Db via GQL, ≤200 nodes, n≤3, AS-OF probes)
  capped `min(cases,128)`; flush-invariance capped `min(cases,32)`. Nodes inserted at epoch VALID FROM (load-bearing —
  else grid probes see nothing); edge iids mirror the engine's derivation; SYSTEM probes always `i64::MAX`.
- **2026-07-06 (slice 6) PERF OPT — anchor-reachable edge pruning (added per user decision, exit-criterion fix):**
  when a linear path's start node is point-anchored, a bounded BFS over `edge_adjacency(anchor=Some)` builds the edges
  reachable within the hop bound and feeds that subset to the hash-join edge scans AND PathExpand's adjacency; falls
  back to full scan otherwise. RESULT-IDENTICAL (superset of all edges on qualifying paths; join/filter/RETURN/WALK
  unchanged). **Warm 2-hop 88.92 → 16.23 ms; {1,3} 87.44 → 26.71 ms; row counts unchanged (33/211).**
- **2026-07-06 (slice 6) mid-slice CRITICAL caught by review:** the AST reshape gave MATCH `NodePattern` a `props`
  field; the DELETE path initially ignored it (`MATCH (p:L {prop}) DELETE p` deleted ALL `:L` nodes — data loss). Fixed
  with an engine-side guard, then fully resolved in Task 7 by real props filtering through `iids_from_snapshot`.
- **2026-07-06 (slice 6) deviations:** the plan's per-task `force_flush` test helper can't trigger a flush on a live
  `Db::local` handle, so flush-dependent tests use the config route (`Db::open(blocks_config(dir, N))` + a benign tx →
  size trigger); the plan's Task 5 markdown had an unbalanced code fence (worked around during brief extraction, plan
  file untouched); the social-graph INTEGRATION test ships a reduced 2k/12k fixture (full 10k/60k exceeded 5 min in a
  debug test) while the BENCH example uses the full 10k/60k; ingest is slow (57 tx/s / ~17 min for 60k edges) due to the
  v1 one-tx-per-edge write surface (multi-edge INSERT bodies land in slice 7) + a 100%-full dev disk.

- **2026-07-05 (slice 5) `BuildContext` landed (decision 1):** `ComponentFactory::build(&self, cfg: &ConfigSection,
  ctx: &BuildContext)` — spec §4's full signature. `BuildContext` is a typed component map (`TypeId → Box<dyn Any + Send + Sync>`,
  `insert<C>`/`get<C>` for `C: Clone + Send + Sync + 'static`). `Db::open_with` builds STORAGE first and inserts the RAW
  (uncached) `Arc<dyn ObjectStore>` into the ctx; log/clock/cache factories then build with it. The object-store log factory is the
  first real consumer (shares the block store's bucket). Discharges the slice-4 "revisit BuildContext" checkpoint.
- **2026-07-05 (slice 5) `storage/s3` (decision 2):** wraps `object_store::aws::AmazonS3` behind a default-on `s3` feature
  (`s3 = ["object_store/aws"]`, adds reqwest/quick-xml transitively). `[storage.s3]`: `bucket` required; `endpoint`/`region`/
  `access_key_id`/`secret_access_key` optional (builder starts from `from_env()`, config overrides env); `path_style` default TRUE
  (Garage/MinIO), `allow_http` derived from endpoint scheme. `AmazonS3Builder::build()` does no I/O ⇒ factory unit tests are network-free.
- **2026-07-05 (slice 5) `log/object-store` (decision 3):** one object per group-commit batch at
  `v1/log/<epoch>/<offset-lexhex>.vlog` (epoch = 4-hex u16 so listing sorts by position), body = the exact `LocalLog` frame grammar
  (`len u32 LE · crc32c u32 LE · protobuf`) but decoded STRICTLY (object PUTs are atomic ⇒ any malformed frame = `Corrupt`, no torn tail).
  Positions assigned locally (designated writer, no CAS/D5); `next` recovered lazily on first append by listing+counting the last object.
  `trim` is a documented NO-OP (sovereign store has no delete; superseded objects swept by slice-8 GC; replay reads only `tail(watermark)`).
  Log rides the same bucket and receives the RAW store so its traffic never fills the query cache.
- **2026-07-05 (slice 5) disk cache tier (decision 4):** one self-describing file per `(path, range)` under `[cache.disk] dir`
  (header = full key, body = value) so the index rebuilds by walking the dir on open (restart survival, no separate index). File names =
  `xxh3_128(key)` hex. Recency = in-memory LRU tick at runtime, persisted as file mtime (touched on hit). Reads copy into owned `Bytes`
  (eviction never invalidates a handed-out buffer). Synchronous I/O on the caller's thread + a single Mutex held across I/O = documented v1
  tradeoff (forward note for hot concurrent read paths). Write-temp-then-rename; `.tmpN`/malformed files swept on open.
- **2026-07-05 (slice 5) cache registry-by-name (decision 5):** `Registry<dyn CacheTier>` (kind "cache") with builtin `memory`+`disk`
  factories; `Registries` gained `cache`. Config: `[cache] tiers = [...]` composed OUTERMOST-FIRST (first listed = first checked; engine
  folds `tiers.iter().rev()`), per-tier `[cache.memory] max_bytes` (512 MiB default) / `[cache.disk] dir`+`max_bytes` (50 GiB default).
  **NO back-compat: slice-4 `[cache] memory_max_bytes` REMOVED** (no alias). `Db::memory()`/`Db::local()` keep a memory tier at 512 MiB.
- **2026-07-05 (slice 5) capability probe (decision 6):** optional `ConditionalStore` (`put_if_absent`=If-None-Match:*, `put_if_matches`=If-Match)
  reached via `ObjectStore::conditional() -> Option<&dyn ConditionalStore>` (default `None` — engine NEVER requires it; sovereignty verified by grep:
  only `Db::probe_capabilities` consumes it). Blanket impl over `object_store` provides it via `put_opts`; `CachedStore` delegates. 4-step probe
  (create → create-again-must-refuse → swap-current-etag → swap-STALE-must-refuse) yields `Supported`/`Unsupported{reason}`/`Inconsistent{reason}`.
  **OBSERVED live verdicts (first run): MinIO = Supported; Garage = Inconsistent; SeaweedFS = Inconsistent** (both: "create-if-absent over an
  existing object succeeded (precondition ignored)" — the SeaweedFS-class header-blindness D5 warned of); Ceph = not yet run (weekly cron). Garage &
  SeaweedFS assert `NotSupported` (= `!Supported`, the load-bearing cas-failover gate) rather than pinning the exact `Inconsistent` variant.
- **2026-07-05 (slice 5) docker-CLI backend harness (decision 7, deviation from roadmap "testcontainers"):** hand-rolled `std::process::Command`
  rig in `varve-testkit/src/backends.rs` (Garage needs multi-step `docker exec` init the testcontainers crate models poorly; zero new deps). Gated by
  `VARVE_S3_BACKENDS` (comma-list or `all`); skips silently otherwise (`just check` never needs docker). Two buckets per backend isolate raw-contract
  vs Db-e2e phases. **Image pins (ONE place — `backends.rs` consts):** garage `dxflrs/garage:v1.0.1`, seaweedfs `chrislusf/seaweedfs:3.80`,
  minio `minio/minio:RELEASE.2025-04-22T22-12-26Z` + `minio/mc:RELEASE.2025-04-16T18-13-26Z`, ceph `quay.io/ceph/demo:latest-quincy`.
  **Execution-time adaptation (recorded per plan):** SeaweedFS bucket-create uses `echo '…' | weed shell` (stdin) + poll on `s3.bucket.list` — `weed shell`
  has no `-c` flag in 3.80. Ceph contract-bucket `s3cmd mb` adds `--host`/`--host-bucket=` for loopback RGW path-style.
- **2026-07-05 (slice 5) CI (decision, spec §13.5):** `backend-matrix` (garage/seaweedfs/minio, fail-fast:false) on push/PR; `backend-ceph-weekly`
  on the `0 4 * * 1` cron; `property-nightly` pinned to the `0 3 * * *` cron so the Monday trigger doesn't double-run it.
- **2026-07-05 (slice 5) benches (M3 Max):** `cache_bench` local FS — ingest 100k @ ~4s, cold open+lookup 29.7 ms → warm 8.3 ms (3.6×),
  9 disk-cache entries survived restart. S3-backed (MinIO) — ingest 100k @ 3.83 s, cold 167.5 ms → warm 111.1 ms, 9 entries survived restart.
- **2026-07-05 (slice 5) process note:** the plan's per-task gate omitted `cargo fmt --check`; verbatim brief-transcribed code drifted from rustfmt
  across T1–T4 and was fixed in one `style:` commit (03e2348). `cargo fmt --all` folded into every subsequent task's gate. Roadmap Global Constraints
  (which include `just check` = fmt+clippy+test) govern over the plan's abbreviated per-task gate.

- **2026-07-05 (slice 4) block format + persisted scan (spec §9, design decisions 1–15):** Data file =
  concatenation of self-contained per-page Arrow IPC streams; the meta file is a single-level page index
  (one row/page: byte range, row count, min/max `_iid`/`_system_from`/`_valid_from`/`_valid_to`, `has_erase`) —
  the "footer surrogate" held decoded in memory (full hash-trie meta + property blooms land with slice-8
  compaction). Pages reuse the golden slice-3 `encode_events`/`decode_events` verbatim; `PAGE_ROWS = 1024`
  (XTDB pageLimit). `encode_block` is a pure fn of the live table (BTreeMap iid-asc + fixed chunking + stable
  reversal → `system_from desc` per entity, ties preserved). Arrow IPC block bytes are deliberately NOT
  golden-pinned (no cross-version guarantee) — round-trip/proptest only; the manifest protobuf wire IS golden-pinned.
- **2026-07-05 (slice 4) page prune rules (decision 4):** (a) IID point outside `[min_iid, max_iid]` → skip;
  (b) `min_system_from >= bounds.system.upper && !has_erase` → skip. **The valid axis is deliberately NOT pruned
  in v1** — a valid-disjoint event still clips the reported `_valid_from`/`_valid_to` of visible rectangles inside
  the window (slice-2 history-introspection), so dropping its page would corrupt reported ranges even though
  visibility would survive; a regression test pins the non-pruning. Meta still records valid min/max for slice-8
  rectangle-aware rules. IID point pushdown comes from `WHERE v._id = <literal>` (Float/Null → unpruned scan).
- **2026-07-05 (slice 4) manifest = the atomic commit point (spec §9, decisions 5/6):** a database-wide protobuf
  written as the FULL trie inventory each time; the manifest PUT is the commit (a data/meta object without a
  manifest entry is invisible garbage). Carries `block_id`, `watermark` (= `append_position.advance(batch_len)`,
  the flushed-prefix exclusive end — replay is `log.tail(watermark)`), and **`max_tx_id` + `max_system_time_us`
  floors — REQUIRED because after a `Log::trim` the log alone can no longer provide them**. Key
  `v1/blocks/<lexhex(block_id)>.manifest`; latest = max parsed block id.
- **2026-07-05 (slice 4) one lock, atomic swap, keep-serving (decisions 7/8/10):** live table + persisted-trie
  inventory live under ONE `RwLock<TableState>` — flush swaps atomically, a query snapshots both under one read
  lock (guard dropped before async GETs, no `await_holding_lock`). Flush order: encode → data PUT → meta PUT →
  manifest PUT (commit) → one-write-lock (push trie + reset live) → best-effort `log.trim`. On any pre-manifest
  PUT failure the live table is UNTOUCHED and flush retries next trigger (orphans are invisible; GC = slice 8).
  `Log::trim` = whole-segment-only (active segment never deleted; memory keeps its next-position counter so
  positions never regress). Merge order per entity: `block0 asc ++ … ++ live asc` (stable reversal restores ties).
- **2026-07-05 (slice 4) restart = manifest + log tail (decision, extends slice-3 replay):** `Db::open` reads the
  latest manifest, replays `log.tail(manifest.watermark)`, restores the persisted inventory from the manifest metas,
  and re-derives all four floors as `max(manifest, replayed)`: `next_block_id = block_id + 1`, `next_tx_id`,
  clock floor (`advance_to`), and the writer watermark (monotonic guard). Verified: 1M-event ingest → restart →
  correct temporal queries, warm point lookup 7.58 ms (< 100 ms exit criterion), full-scan 1M rows after restart.
- **2026-07-05 (slice 4) `[log] local` + `[storage] memory` is a hard config error (decision 11):**
  `EngineError::VolatileBlockStore` (flushing would trim the durable log while blocks sit in volatile memory =
  silent data loss on restart). `Db::local(dir)` configures both durably (`dir/log` + `dir/store`). Config surface:
  `[storage] backend`/`max_block_rows` (default 100_000)/`flush_interval_ms` (default 300_000, 0 disables),
  `[storage.local] dir`, and cache config (slice-4 `[cache] memory_max_bytes` was REPLACED in slice 5 by
  `[cache] tiers` + `[cache.<name>]` — see slice-5 decisions) — integer-bytes convention throughout.
- **2026-07-05 (slice 4) `CacheTier` = engine composition, NOT a factory (decision 14):** the memory cache wraps
  the store by plain `CachedStore`/`MemoryCache` composition in `varve-engine`; **`BuildContext` is STILL not
  needed** (discharges the slice-4 revisit). Registry-by-name cache selection waits for the slice-5 disk tier —
  that is the next checkpoint for whether a factory needs another *component*.
- **2026-07-05 (slice 4) block bench (M3 Max, `cargo run --release --example block_bench -p varve`):**
  1,000,000 events ingested @ **39,816 events/s** (25.12 s); **39 blocks flushed**; reopen (manifest + log tail)
  **38.18 ms**; point lookup **cold 22.39 ms / warm 7.58 ms** (< 100 ms exit criterion PASS); full scan after
  restart = 1,000,000 rows. write_bench unaffected (memory 6275 / local-fsync 350 tx/s).

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

- **Slice-6 fast-follows (non-blocking; whole-branch review triaged READY TO MERGE):**
  - **T10 e2e property CI wall-time (user tuning decision):** `db_traversal_matches_oracle` runs `min(cases,128)`
    cases over `arb_graph(200,400)`, each booting a Db + replaying hundreds of `MATCH…INSERT` — ~200 s, on EVERY
    CI `check` (not just nightly), because the cap is 128 regardless of `PROPTEST_CASES`. Plan-mandated. Mitigate by
    shrinking arb_graph bounds or making the e2e cap scale with `PROPTEST_CASES` if CI wall-time matters.
  - **`where_for_var` single-variant `Expr` closure (WATCH in slice 7):** `writer.rs::resolve_insert`'s
    `.filter(|Expr::PropEq{..}| …)` uses an irrefutable pattern that compiles only while `Expr` has one variant; it
    will fail to compile when slice-7 WHERE grows variants — fix then.
  - **`EngineError::Unsupported` is now dead** (zero constructors after the T8 scan_specs consolidation) but kept as
    pub API — remove if slice 7 doesn't need it.
  - **Fixed-path traversal not property-fuzzed / no fixed-path AS-OF test:** the fixed multi-Edge-hop shape shares the
    `reachable_edges`/`edge_adjacency_impl` core with the heavily-fuzzed quantified path (analytically + transitively
    covered), but a random-graph fuzz + an AS-OF regression test for fixed paths would close the gap empirically.
  - **`MixedPropertyTypes` error-mode divergence (pre-existing, doc-note):** a pruned edge/node batch (subset) can
    succeed where a full scan would error on an unrelated same-label entity with a conflicting doc-column type — error
    behavior only, never wrong rows; same asymmetry already exists for node `iid_point` reads.
  - **Trivial (as-is):** codec sibling-append error taxonomy (Codec vs Arrow, unreachable); `PathExpandNode` Debug
    would print the whole adjacency; `StillConnected` message says "node(s)" but counts edges; `pattern.rs` duplicates
    `DEFAULT_GRAPH`/`NODES_TABLE` literals; social_graph test lacks a `!is_empty()` guard.

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
- **~~`_labels` roadmap divergence~~ RESOLVED** 2026-07-06 (slice 7 Task 7): snapshots now
  emit non-null system `_labels: List<Utf8>` for nodes and edges, node MATCH supports label
  conjunction/alternation, and `_labels` is reserved from user property projection to avoid
  duplicate system-column collisions.
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
  STILL DEFERRED: `BuildContext` factory param — config-only factories still suffice. **Slice 4 confirmed
  it is NOT needed** (the memory cache wraps the store by engine composition, not a factory — decision 14).
  Next checkpoint is slice 5's disk cache tier (registry-by-name): if selecting it needs another *component*,
  that is the cheapest moment for the trait break, before more backends exist.
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
- **Slice-5 fast-follows (non-blocking; whole-branch review verdict = READY TO MERGE WITH FIXES — the two
  ship-bar fixes were applied in commit `c4beea6`; the rest are tracked here):**
  - **Disk-cache store-identity namespacing (Important, do before multi-store cache sharing):** `CacheKey` is
    `(path, range)` with no store identity, and every DB uses the same `v1/blocks/…`/`v1/log/…` keyspace. Sharing one
    persistent `[cache.disk] dir` across two backends would silently serve wrong bytes. SHIPPED FIX: loud doc note on
    `DiskCache`/`DiskCacheFactory` ("one dir per store"). DURABLE FIX (fast-follow): a store-identity marker file written
    on `DiskCache::open` (bucket+endpoint or a minted UUID) that refuses/clears the dir on mismatch, and/or prefix the
    encoded key with a store token.
  - **`object_store_log.rs` no-default-features compile (Minor) — RESOLVED** commit `c4beea6`: added
    `#![cfg(feature = "object-store")]` so `cargo test -p varve-log --no-default-features` compiles (the file becomes empty).
  - **Disk-cache `open` TOCTOU (Minor, slice 8/10 hardening):** `disk.rs` `open`'s per-file `fs::metadata?` aborts the
    whole open (and thus `Db::open`) on a race, unlike the best-effort sweep around it. Use `.ok()`/`unwrap_or(UNIX_EPOCH)`.
  - **Probe-key entropy (Minor, before slice-10 cas-failover trusts the verdict):** `Db::probe_capabilities` keys on
    `clock.next().as_micros()` and probe objects are never deleted; a wall-clock regression to a previously-probed µs would
    yield a false `Inconsistent`. Add a per-open nonce, and have slice-8 GC sweep `v1/probe/`.
  - **`backend-matrix` CI unproven (Minor):** the job runs 3 live containers per push/PR with 2-min init polls — validated
    LOCALLY via `just s3-matrix` but not yet green in GitHub Actions. Confirm on first push (or gate behind a label/manual
    dispatch) before treating it as a required check.
  - **`list_objects` re-sort / `#[derive(Default)]` on BuildContext / `docker()` stdout-drop:** reviewer-triaged as
    non-defects or trivial; leave as-is.
- **Slice-4 fast-follows (non-blocking; whole-branch review verdict = READY TO MERGE):**
  - **~~Flush-equivalence property tests the SHIPPED merge, not a copy~~ RESOLVED** 2026-07-05 (post-slice, commit
    `refactor: extract pure merge_sources core`): the decision-9 merge/reversal/concat logic was extracted from
    `varve-engine::scan::merged_snapshot` into a pure, shared `varve_index::merge_sources`; `merged_snapshot` (I/O
    shell) and `varve-testkit/tests/flush_equivalence.rs` now BOTH call it, so the 10k-case property guards the
    SHIPPED merge with no copy to drift. Behavior-preserving (all 6 scan unit tests + proptest assertions unchanged);
    added 2 non-vacuous `merge_sources` unit tests (per-block reversal + block-order + live-last; same-`system_from`
    intra-block tie via stable reversal — also closes the Task-6 tie-coverage note). 247 workspace tests.
  - **Flush-failure observability (slice 10):** on any PUT failure the flush silently keeps serving + retries next
    trigger (decision 10) — no log/metric surface yet. Add when observability lands (slice 10).
  - **GC of orphaned data/meta objects (slice 8):** a crash between data/meta PUTs and the manifest PUT leaves
    invisible orphan objects; `Log::trim` also never GCs S3 objects. Sweep with compaction/GC (slice 8).
  - **`VolatileBlockStore` guard is name-based (later slice):** `log=local && storage=memory` is rejected by backend
    NAME; a custom durable-log + volatile-custom-store pairing would bypass it. Base it on a durability *property* when
    custom backends arrive.
  - **`get_range` offset validation on recovery (defer, from T9 review):** persisted page byte ranges come from trusted
    meta; `decode_events` errors (no panic) on a bad page. When loading externally-mutable real objects becomes a threat
    model, validate `offset..offset+len` against the data length as defense-in-depth.

## Slice log

| Slice | Status | Sessions | Demo command | Notes |
|---|---|---|---|---|
| 0 foundation | ✅ complete | 1 | `just check` / `cargo test --workspace` (22 tests) | workspace + `varve-types` (Iid, LogPosition) + `varve-config` (Config, Registry, nested/coerced env overrides) + CI |
| 1 walking skeleton | ✅ complete | 1 | `cargo run --example hello -p varve` | INSERT→MATCH e2e in memory; +`varve-gql`(lexer/parser/AST), `varve-index`(LiveTable→Arrow), `varve-plan`(DataFusion), `varve-engine`(Db), `varve` facade; datafusion 54/arrow 58 pinned; 44 workspace tests |
| 2 bitemporal core | ✅ complete | 1 | `cargo run --example time_travel -p varve` | events + XTDB Ceiling/Polygon port + per-entity resolve; `varve-testkit` reference model + proptest equivalence (10k CI / 200k nightly); temporal GQL (`FOR VALID_TIME`/`SYSTEM_TIME`, `INSERT … VALID`, `MATCH … DELETE`, history fns); `MonotonicClock`; `TxReceipt.system_time`; lock-split query; ~125 workspace tests |
| 3 durability (log) | ✅ complete | 1 | `cargo run --release --example write_bench -p varve` | `varve-log` crate: `Log` trait + prost envelope + `memory`/`local` backends (CRC32C frames, fsync-before-ack, torn-tail recovery) + writer loop group commit + `Db::open` replay + pluggable `Clock`/`Registries` + `kill -9` crash matrix; bench memory 6226 / local 340 tx/s (M3 Max); 181 workspace tests |
| 4 blocks & persisted scan | ✅ complete | 1 | `cargo run --release --example block_bench -p varve` | `varve-storage` crate (sovereign `ObjectStore` over object_store 0.13, §9 lex-hex keys, `BlockManifest` commit point, memory cache) + paged block codec/prune in `varve-index` + `Log::trim` + one-lock merged live∪persisted scan + writer-loop flush (data/meta→manifest→reset→trim) + `Db::open` manifest+log-tail recovery + kill-during-flush crash matrix + flush-equivalence proptest; bench 1M events @ 39.8k ev/s, reopen 38 ms, warm point lookup 7.6 ms (<100 ms); 247 workspace tests (incl. post-slice `merge_sources` extraction fast-follow) |
| 5 s3 backends & caches | ✅ complete | 1 | `just s3-matrix` / `cargo run --release --example cache_bench -p varve` | `BuildContext` (spec-§4 factory sig); `storage/s3` (`object_store/aws`, default-on) for Garage/SeaweedFS/MinIO/Ceph; `log/object-store` (1 object per group-commit batch, shared bucket); disk `CacheTier` + `[cache] tiers` registry (memory/disk builtins, replaces `[cache] memory_max_bytes`); optional `ConditionalStore` + 4-step capability probe (`Db::probe_capabilities`); docker-CLI backend harness + CI `backend-matrix`/`backend-ceph-weekly`. LIVE trio (garage/seaweedfs/minio) green; probe verdicts minio=Supported, garage/seaweedfs=Inconsistent. 293 workspace tests |
| 6 edges & traversal | ✅ complete | 1 | `cargo run --release --example traversal_bench -p varve` | edge events (`_src_iid`/`_dst_iid`) + `LiveTable` adjacency views; `INSERT (a)-[:REL]->(b)` inline + `MATCH…INSERT`; 3 sort-order edge families (primary/adj-out/adj-in) in one atomic manifest (`TableTries.family`); anchor-pruned `edge_adjacency`; multi-element MATCH → DataFusion hash joins; `PathExpand` UDLN+ExecutionPlan for `{m,n}`/`*` WALK + `RETURN p`; `DETACH DELETE` + still-connected error; independent traversal oracle (pure 10k + e2e + flush-invariance properties) + 10k/60k social fixture; anchor-reachable edge-pruning perf opt → **warm 2-hop 16.23 ms** (<50 ms). Caught+fixed a mid-slice `DELETE` data-loss bug. 372 workspace tests |
| 7 GQL completion & TCK | ✅ complete | 5 | `cargo run --release --example gql_tour -p varve` | Tasks 1-25 complete. Practical-core expressions/statements/mutations/catalog/TCK/differential/fuzz/demo shipped; post-exit hardening closed configurable query budgets, streaming `PathExpandExec`, reachable-edge BFS budgets, traversal-oracle CPU reduction, TCK side effects/path values, and final fuzz reclose. Final verification: 596 workspace tests, clippy clean, fmt clean, release traversal oracle 6 passed in 49.17s, TCK 445/511 adapted passed (0.870841), 10-min parser fuzz 13,903,093 execs no crashes. Open future hardening: million-edge streaming/cursor/backpressure work. |
| 8 compaction & GC | ✅ complete | 4 | `cargo run --release --example compaction_gc -p varve` | Full hash trie keys/meta/recovery pruning; manifest-history trie catalog; deterministic embedded compaction; compaction query equivalence; retention-aware GC; raw-object erase proof; churn plateau demo. Final verification: fmt, clippy, workspace tests 630 passed, compaction equivalence 4 passed. Demo: 192 objects before compaction, 3 after GC, 62 current rows. |
| 9 server, CLI, query nodes | not started | – | – | no detailed plan yet |
| 10 coordination | not started | – | – | no detailed plan yet |
| 11 ship | not started | – | – | no detailed plan yet |
