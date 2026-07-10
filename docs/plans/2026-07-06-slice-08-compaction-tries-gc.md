# Slice 8: Compaction, Hash Tries, and GC

> **For agentic workers:** REQUIRED SUB-SKILL requested by the session prompt: `superpowers:subagent-driven-development` plus strict TDD. That skill is not available in this Codex session, so use the available sub-agent tools for independent exploration/review and execute the same task-by-task workflow directly. Every implementation step below starts with a failing test, then minimal implementation, then refactor, then commit.

**Goal:** Varve storage moves from flat L0 block inventories to deterministic IID hash-trie inventories, coordination-free compaction, and retention-aware garbage collection. Queries continue to merge live plus persisted sources, but persisted meta now carries trie path information for point/range pruning. Compaction produces byte-identical output from the same logical inputs regardless input ordering or duplicate worker attempts. GC deletes only objects that are unreferenced by retained manifest state; `ERASE` data is physically absent after compaction plus GC.

**Scope from roadmap Slice 8:**
- Full hash trie, branch factor 4 on IID bits, `LOG_LIMIT = 64`, `PAGE_LIMIT = 1024`, persisted meta files, and scan pruning by trie path (`Bucketer::filter_iids_for_path` equivalent).
- Trie catalog: per `(graph, table, family, shard=(level, recency, part))`, lists nascent/live/garbage as a pure fold over manifest history.
- Deterministic compaction: same-shard merges, L0/Ln promotion, 2 IID bits per level partitioning, duplicate jobs last-write-wins because output bytes and manifest inventory are deterministic.
- GC: retention-aware unreferenced manifest/object deletion; update-heavy churn plateaus; erased bytes absent from compacted objects.

**Inputs consulted:**
- `docs/plans/STATUS.md`: current position says Slice 8 plan is missing; slice-4/5/6 decisions define existing L0 block inventory, adjacency families, object-store constraints, and GC as the owner of DELETE.
- `docs/plans/varve-v1-roadmap.md`: Slice 8 task list and global constraints.
- `docs/design/2026-07-04-varve-design.md`: section 9 storage layout and compaction, section 13 deterministic-compaction tests, section 14 XTDB adoption.
- XTDB references:
  - `refs/xtdb/core/src/main/kotlin/xtdb/trie/Trie.kt`: trie key format.
  - `refs/xtdb/core/src/main/kotlin/xtdb/trie/Bucketer.kt`: default 2-bit bucket levels, branch factor 4, path start/end IID math.
  - `refs/xtdb/core/src/main/kotlin/xtdb/api/IndexerConfig.kt`: `logLimit = 64`, `pageLimit = 1024`, `rowsPerBlock = 102400`.
  - `refs/xtdb/dev/doc/trie-cat.allium`: shard state live/nascent/garbage, branch factor, target 100 MB.
  - `refs/xtdb/dev/doc/compaction.allium`: coordination-free deterministic job selection/output.

## Global Constraints

- TDD, no exceptions: write the failing test first, prove it fails for the intended reason, implement the minimum, run the focused green command, then commit.
- Storage semantics remain sovereign object store semantics. Plain `PUT`/`GET`/range `GET`/`LIST` are enough for all correctness. `DELETE` is introduced only on `ObjectStore` for GC.
- Bitemporal invariant: `_system_to` is never stored; scans and compaction derive visibility from append-only events.
- Determinism: no wall clock, randomness, hash-map iteration order, or filesystem listing order in output bytes. Sort all inputs by stable keys before writing output.
- Library code has no `unwrap()`/`expect()`; tests may use them per root `clippy.toml`.
- Commands in this repository session are run with `rtk` prefix.
- Commit style uses prefixes only: `feat`, `fix`, `chore`, `docs`, `security`, `test`, `style`, `perf`, `ci`, `refactor`. No co-author trailers.
- Slice ends with all workspace tests green, clippy clean, `STATUS.md` updated, roadmap and this plan checked off, and a demo command recorded.

## Existing Implementation Map

Current code already has a slice-4/6 L0 inventory:
- `crates/varve-storage/src/keys.rs`: `lex_hex`, `l0_trie_key`, data/meta/manifest and adjacency keys.
- `crates/varve-storage/src/manifest.rs`: `BlockManifest`, `TableTries`, `TrieEntry`, `latest_manifest`.
- `crates/varve-index/src/block.rs`: `EncodedBlock`, flat `PageMeta`, `encode_block`, `encode_block_by`, `decode_meta`, `PageMeta::selected`.
- `crates/varve-engine/src/state.rs`: `PersistedTrie { entry, pages }`, `TableCore { live, tries }`, edge adjacency family vectors.
- `crates/varve-engine/src/flush.rs`: `flush_block` writes L0 data/meta/manifest and appends `PersistedTrie`s to state.
- `crates/varve-engine/src/scan.rs`: `merged_snapshot` and `edge_adjacency_impl` read persisted pages with range GETs and merge with live/overlay events.
- Recovery in `crates/varve-engine/src/db.rs` rebuilds state from the latest manifest and decodes meta once.

Slice 8 must evolve that shape without introducing a separate storage path.

## Design Decisions for This Slice

1. **Trie key parser/generator becomes first-class.** Keep existing L0 strings byte-identical (`l00-rc-b00`) and add level/recency/part/block parsing in `varve-storage`.
2. **Part path is the trie path.** Branch factor 4 means each path segment is one bucket `0..=3`, derived from two high-to-low IID bits per level, exactly XTDB `Bucketer`.
3. **Meta file remains Arrow IPC, but `PageMeta` grows `path: Vec<u8>`.** Existing flat pages become trie leaves. A page can be skipped when an IID point does not share its path before byte ranges are fetched.
4. **Manifest remains the commit point.** Compaction writes all output data/meta first, then writes one database-wide manifest containing the full post-compaction inventory. Latest manifest alone is enough for reads; manifest history is used by catalog/GC.
5. **Trie catalog is derived, not authoritative.** `TrieCatalog::from_manifests` folds sorted manifests into state. The latest retained manifest decides live/nascent query inventory; older entries absent from retained live sets become garbage candidates.
6. **Compaction is an embedded/admin operation in slice 8.** Add `Db::compact_once()` and `Db::gc_once()` as the embedded API. Server/CLI wrappers land in slice 9.
7. **L0 compaction groups up to `LOG_LIMIT` L0 tries into L1 current plus L1 historical outputs.** Use existing `varve_index::Polygon::recency()` to route resolved rows: current recency stays `rc`; finite recency becomes a weekly historical bucket named `YYYYMMDD` for the Monday 00:00Z bucket boundary.
8. **Higher-level compaction preserves input recency.** L1 current and Ln current files compact on the current side. L1 historical and Ln historical files compact within one weekly recency bucket. No mixed-recency job is valid.
9. **Erase compaction rule:** for one IID's sorted event history, find the latest `Op::Erase`; drop that erase and all earlier events from compacted output, keep events after it. This preserves GDPR hard-delete semantics and permits future re-inserts of the same `_id`.
10. **GC retention follows XTDB-compatible defaults.** Config keys are `[gc] blocks_to_keep = 10`, `[gc] garbage_lifetime_hours = 24`, and `[gc] enabled = false` by default. Tests may set zero lifetime for deterministic immediate collection. GC never deletes latest live/nascent query inventory.

## New Interfaces

```rust
// varve-storage/src/keys.rs or trie.rs
pub const TRIE_LEVEL_BITS: u8 = 2;
pub const TRIE_BRANCH_FACTOR: u8 = 4;
pub const LOG_LIMIT: usize = 64;
pub const PAGE_LIMIT: usize = 1024;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Recency {
    Current,
    Week { yyyymmdd: u32 },
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TrieKey {
    pub level: u64,
    pub recency: Recency,
    pub part: Vec<u8>,
    pub block: u64,
}

impl TrieKey {
    pub fn l0(block: u64) -> TrieKey;
    pub fn child(&self, bucket: u8, block: u64) -> TrieKey;
    pub fn to_key_string(&self) -> String;
    pub fn parse(s: &str) -> Result<TrieKey, StorageError>;
    pub fn shard(&self) -> TrieShard;
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TrieShard {
    pub level: u64,
    pub recency: Recency,
    pub part: Vec<u8>,
}

pub struct Bucketer;
impl Bucketer {
    pub fn bucket(iid: &varve_types::Iid, level: usize) -> Option<u8>;
    pub fn path(iid: &varve_types::Iid, levels: usize) -> Option<Vec<u8>>;
    pub fn contains(path: &[u8], iid: &varve_types::Iid) -> bool;
}

// varve-index/src/block.rs
pub struct PageMeta {
    pub path: Vec<u8>,
    // existing fields unchanged
}

impl PageMeta {
    pub fn selected(&self, bounds: &TemporalBounds, iid_point: Option<&Iid>) -> bool;
}

// varve-storage/src/store.rs
#[async_trait::async_trait]
pub trait ObjectStore: Send + Sync {
    async fn put(&self, key: &str, bytes: Bytes) -> Result<(), StorageError>;
    async fn get(&self, key: &str) -> Result<Bytes, StorageError>;
    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Bytes, StorageError>;
    async fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError>;
    async fn delete(&self, key: &str) -> Result<(), StorageError>; // new; GC only
}

// varve-storage/src/catalog.rs
pub enum TrieState { Nascent, Live, Garbage }
pub struct TrieCatalog { /* per graph/table/family/shard state */ }
impl TrieCatalog {
    pub fn from_manifests(manifests: &[BlockManifest]) -> Result<TrieCatalog, StorageError>;
    pub fn live_for(&self, graph: &str, table: &str, family: &str) -> Vec<TrieEntry>;
    pub fn garbage_entries(&self) -> Vec<(String, String, String, TrieEntry)>;
}

// varve-engine/src/compact.rs
pub struct CompactionConfig {
    pub log_limit: usize,          // default 64
    pub file_size_target: u64,     // default 104_857_600
}

pub struct CompactionJob { /* graph, table, family, inputs, output key */ }
pub async fn compact_once(state: &mut WriterState) -> Result<CompactionReport, EngineError>;

// varve-engine/src/gc.rs
pub struct GcConfig {
    pub enabled: bool,              // default false
    pub blocks_to_keep: u64,        // default 10
    pub garbage_lifetime_us: i64,   // default 24h
}
pub async fn gc_once(store: &Arc<dyn ObjectStore>, cfg: &GcConfig) -> Result<GcReport, EngineError>;
```

## Task Breakdown

## Task Status

- [x] Task 1: Trie key parser and Bucketer
- [x] Task 2: Page meta carries trie path and prunes by path
- [x] Task 3: Recovery and scans consume path-aware meta
- [x] Task 4: Manifest history reader and derived trie catalog
- [x] Task 5: Add GC-only delete to ObjectStore
- [x] Task 6: Pure compaction job selection
- [x] Task 7: Compaction block writer and deterministic merge
- [x] Task 8: Commit compaction through manifest and in-memory state
- [x] Task 9: Query equivalence before and after compaction
- [x] Task 10: GC planning from block retention and garbage lifetime
- [x] Task 11: Execute GC and expose embedded API
- [x] Task 12: Physical erase proof after compaction plus GC
- [x] Task 13: Churn plateau smoke test and compaction demo
- [x] Task 14: Whole-slice verification and docs closeout

### Task 1: Trie key parser and Bucketer

**Files:**
- Modify: `crates/varve-storage/src/keys.rs`
- Modify: `crates/varve-storage/src/lib.rs`
- Tests: inline tests in `keys.rs`

**Failing tests first:**
- `trie_key_round_trips_l0_l1_l2`
- `trie_key_rejects_invalid_parts_and_recency`
- `bucketer_matches_xtdb_known_bit_patterns`
- `filter_iids_for_path_returns_only_prefix_range`

**Implementation:**
- Add constants `TRIE_LEVEL_BITS`, `TRIE_BRANCH_FACTOR`, `LOG_LIMIT`, `PAGE_LIMIT`.
- Add `Recency`, `TrieKey`, `TrieShard`, `Bucketer`.
- Keep `l0_trie_key(block)` as a compatibility wrapper returning `TrieKey::l0(block).to_key_string()`.
- `TrieKey::parse("l02-rc-p13-b00")` yields level `2`, current recency, part `[1, 3]`, block `0`.
- Part digits are base-4 only; reject uppercase lex-hex and malformed segment order.
- `Bucketer::bucket` uses the same high-to-low bit extraction as XTDB `Bucketer.bucketFor`.

**Commands:**
- Red: `rtk cargo test -p varve-storage keys::tests::trie_key_round_trips_l0_l1_l2`
- Green: `rtk cargo test -p varve-storage keys::tests`
- Commit: `feat: add trie key parser and bucketer`

### Task 2: Page meta carries trie path and prunes by path

**Files:**
- Modify: `crates/varve-index/src/block.rs`
- Modify: `crates/varve-index/src/lib.rs` if exports change
- Tests: inline tests in `block.rs`

**Failing tests first:**
- `encoded_meta_records_leaf_paths`
- `iid_point_outside_page_path_is_pruned_before_range_stats`
- `by_src_adjacency_paths_use_sort_key_not_edge_iid`
- `meta_wire_round_trips_path_column`

**Implementation:**
- Add `path: Vec<u8>` to `PageMeta`.
- Add a `path` Arrow column to meta IPC, stored as `List<UInt8>` or `Binary`. Use the simpler Arrow 58 API that compiles cleanly; tests assert round-trip not physical column type.
- In `encode_block_by`, compute the sort key per row first, then assign page path as the longest shared bucket prefix for the chunk, capped at the trie level implied by the output key. For L0 pages, path is empty. For compacted output, Task 6 passes a target path.
- Extend `PageMeta::selected`: if `iid_point` exists and `!Bucketer::contains(&self.path, iid)`, return false before existing min/max checks.
- Keep old valid-time non-pruning rule unchanged.

**Commands:**
- Red: `rtk cargo test -p varve-index block::tests::encoded_meta_records_leaf_paths`
- Green: `rtk cargo test -p varve-index block::tests`
- Commit: `feat: persist trie paths in page metadata`

### Task 3: Recovery and scans consume path-aware meta

**Files:**
- Modify: `crates/varve-engine/src/db.rs`
- Modify: `crates/varve-engine/src/scan.rs`
- Tests: `crates/varve/tests/blocks.rs`, `crates/varve-engine/src/scan.rs`

**Failing tests first:**
- `point_lookup_skips_pages_outside_trie_path`
- `restart_preserves_path_pruning_metadata`
- `adjacency_anchor_skips_pages_outside_trie_path`

**Implementation:**
- Ensure `decode_meta` is used everywhere recovery builds `PersistedTrie`.
- Add a test-only counting store wrapper in the relevant test module that counts `get_range` calls.
- Seed multiple persisted pages, query by `_id` or adjacency anchor, and assert only matching path pages are fetched.
- No query result behavior changes.

**Commands:**
- Red: `rtk cargo test -p varve-engine scan::tests::point_lookup_skips_pages_outside_trie_path`
- Green: `rtk cargo test -p varve-engine scan::tests`
- Green e2e: `rtk cargo test -p varve --test blocks restart_preserves_path_pruning_metadata`
- Commit: `feat: use trie paths for persisted scan pruning`

### Task 4: Manifest history reader and derived trie catalog

**Files:**
- Modify: `crates/varve-storage/src/manifest.rs`
- Create: `crates/varve-storage/src/catalog.rs`
- Modify: `crates/varve-storage/src/lib.rs`
- Tests: inline tests in `catalog.rs` and `manifest.rs`

**Failing tests first:**
- `manifest_history_is_sorted_by_block_id_and_ignores_strays`
- `catalog_marks_latest_inventory_live`
- `catalog_marks_superseded_entries_garbage`
- `catalog_groups_by_graph_table_family_and_shard`
- `l1_historical_is_nascent_until_matching_l1_current`
- `l2_partition_siblings_become_live_as_a_group`

**Implementation:**
- Add `manifest_history(store) -> Result<Vec<BlockManifest>, StorageError>` sorted ascending by `block_id`.
- Add `TrieCatalog::from_manifests`.
- Catalog fold logic:
  - L0 files are `Live` immediately.
  - L1 historical files are `Nascent` until an L1 current file in the same scope with the identical block exists, then become `Live`; a later block does not activate them.
  - L2+ partition outputs are `Nascent` until all four sibling buckets for that parent path exist, then become `Live` as a group.
  - Later manifest full inventory supersedes earlier full inventory.
  - Entries in older manifests that are absent from the latest inventory and not protected by retained live/nascent state are `Garbage`.
  - Track `garbage_as_of_us` from the manifest's `max_system_time_us`; zero in pure unit tests is acceptable when the test does not exercise time-based cutoff.
- Group keys use parsed `TrieKey::shard()` and include graph/table/family.

**Commands:**
- Red: `rtk cargo test -p varve-storage catalog::tests::catalog_marks_superseded_entries_garbage`
- Green: `rtk cargo test -p varve-storage`
- Commit: `feat: derive trie catalog from manifest history`

### Task 5: Add GC-only delete to ObjectStore

**Files:**
- Modify: `crates/varve-storage/src/store.rs`
- Modify: `crates/varve-storage/src/memory.rs` if custom implementation needs it
- Modify: `crates/varve-storage/src/local.rs` if custom implementation needs it
- Tests: `crates/varve-storage/tests/store_test.rs`

**Failing tests first:**
- `delete_removes_object_and_is_idempotent_for_missing_keys`
- `delete_keeps_prefix_listing_sorted`

**Implementation:**
- Add `delete` to `ObjectStore`.
- Blanket object_store implementation maps missing object to `Ok(())`; GC must be idempotent.
- Memory/local stores inherit from object_store where possible.
- Do not use delete outside GC code.

**Commands:**
- Red: `rtk cargo test -p varve-storage --test store_test delete_removes_object_and_is_idempotent_for_missing_keys`
- Green: `rtk cargo test -p varve-storage`
- Commit: `feat: add gc delete to object store`

### Task 6: Pure compaction job selection

**Files:**
- Create: `crates/varve-engine/src/compact.rs`
- Modify: `crates/varve-engine/src/lib.rs` or `db.rs` module wiring
- Tests: inline tests in `compact.rs`

**Failing tests first:**
- `selects_l0_job_when_log_limit_reached`
- `selects_four_same_shard_level_jobs`
- `selects_l0_recency_split_outputs`
- `job_selection_is_order_independent`
- `duplicate_output_key_for_same_catalog_state`

**Implementation:**
- Define `CompactionConfig`.
- Define `CompactionJob` with graph/table/family, input entries sorted by `TrieKey`, output `TrieKey`, and target `SortOrder`.
- L0 rule: choose up to `LOG_LIMIT` live L0 entries in one graph/table/family ordered by trie key, output one L1 current key and zero or more L1 weekly historical keys for the recencies present after resolution.
- L1C/LnC rule: four live current tries in the same shard output four child current partitions at level `n+1`, appending one 2-bit IID bucket to `part`.
- L1H/LnH rule: four live historical tries in the same shard and same weekly recency output historical child partitions; never mix recencies.
- Never inspect current time or object-store listing order.

**Commands:**
- Red: `rtk cargo test -p varve-engine compact::tests::job_selection_is_order_independent`
- Green: `rtk cargo test -p varve-engine compact::tests`
- Commit: `feat: select deterministic compaction jobs`

### Task 7: Compaction block writer and deterministic merge

**Files:**
- Modify: `crates/varve-engine/src/compact.rs`
- Modify: `crates/varve-index/src/block.rs`
- Tests: inline tests in `compact.rs`; optionally `crates/varve-testkit/tests/compaction_determinism.rs`

**Failing tests first:**
- `compacted_output_is_byte_identical_for_permuted_inputs`
- `compacted_events_are_sorted_by_iid_and_system_desc`
- `l0_compaction_routes_current_and_weekly_historical_outputs`
- `erase_drops_prior_bytes_but_keeps_later_reinsert`

**Implementation:**
- Read input pages in sorted input trie order, decode events, group by IID in a `BTreeMap`.
- For each IID, sort events by `(system_from asc, stable original order)` for merge, apply erase rule, derive bitemporal polygons, and route each retained row to current or weekly historical output via `Polygon::recency()`.
- Write every output in `(_iid asc, _system_from desc)` order.
- Use `encode_block_by` with the job's target `SortOrder` and target trie path.
- Tests compare exact `data` and `meta` bytes for permuted input order.
- Tests include a sentinel string in an erased event and assert it is absent from output bytes.

**Commands:**
- Red: `rtk cargo test -p varve-engine compact::tests::compacted_output_is_byte_identical_for_permuted_inputs`
- Green: `rtk cargo test -p varve-engine compact::tests`
- Commit: `feat: write deterministic compacted blocks`

### Task 8: Commit compaction through manifest and in-memory state

**Files:**
- Modify: `crates/varve-engine/src/compact.rs`
- Modify: `crates/varve-engine/src/writer.rs`
- Modify: `crates/varve-engine/src/db.rs`
- Tests: `crates/varve/tests/compaction.rs` (new)

**Failing tests first:**
- `compact_once_replaces_input_tries_after_manifest_commit`
- `compact_failure_before_manifest_keeps_inputs_live`
- `duplicate_compaction_job_last_write_wins`

**Implementation:**
- Add embedded `Db::compact_once() -> Result<CompactionReport, EngineError>`.
- Write compacted data/meta objects first.
- Build a new full inventory manifest: previous latest inventory minus input tries plus output trie(s).
- PUT manifest as the only commit point.
- After manifest PUT, update `GraphsState` under one write lock, replacing input `PersistedTrie`s with output `PersistedTrie`s.
- If any pre-manifest PUT fails, leave state unchanged and do not write manifest.
- Duplicate job retry writes identical objects and manifest inventory; latest manifest wins.

**Commands:**
- Red: `rtk cargo test -p varve --test compaction compact_once_replaces_input_tries_after_manifest_commit`
- Green: `rtk cargo test -p varve --test compaction`
- Commit: `feat: commit compaction through manifests`

### Task 9: Query equivalence before and after compaction

**Files:**
- Modify: `crates/varve-testkit/Cargo.toml` if needed
- Create: `crates/varve-testkit/tests/compaction_equivalence.rs`
- Tests: property and focused e2e tests

**Failing tests first:**
- `compaction_preserves_node_query_results`
- `compaction_preserves_edge_adjacency_results`
- `random_histories_match_before_after_compaction`

**Implementation:**
- Generate randomized histories with puts/deletes/erases over nodes and edges.
- Force multiple flushes, query a selected set of GQL statements before compaction, run `compact_once` until no job remains, query again.
- Compare normalized batches.
- Include AS OF valid/system queries and adjacency traversal queries.
- Cap CI cases conservatively; release/nightly can raise `PROPTEST_CASES`.

**Commands:**
- Red: `rtk cargo test -p varve-testkit --test compaction_equivalence compaction_preserves_node_query_results`
- Green: `rtk cargo test -p varve-testkit --test compaction_equivalence`
- Commit: `test: add compaction query equivalence coverage`

### Task 10: GC planning from block retention and garbage lifetime

**Files:**
- Create: `crates/varve-engine/src/gc.rs`
- Modify: `crates/varve-engine/src/lib.rs` or module wiring
- Tests: inline tests in `gc.rs`

**Failing tests first:**
- `gc_plan_keeps_objects_referenced_by_retained_manifests`
- `gc_plan_deletes_orphan_data_meta_pairs`
- `gc_plan_deletes_old_unretained_manifests`
- `gc_plan_respects_blocks_to_keep_and_garbage_lifetime`
- `gc_plan_keeps_probe_and_log_objects_out_of_scope`

**Implementation:**
- Add `GcConfig { enabled, blocks_to_keep, garbage_lifetime_us }`, defaults `false`, `10`, `24h`.
- Add pure `plan_gc(manifests, listed_keys, cfg) -> GcPlan`.
- Protected keys:
  - newest manifest and every trie entry live or nascent in its catalog,
  - manifests with `block_id > latest_block_id.saturating_sub(blocks_to_keep)`,
  - garbage entries whose `garbage_as_of_us + garbage_lifetime_us` has not elapsed relative to the latest manifest's `max_system_time_us`,
  - log keys and probe keys for now.
- Delete candidates:
  - data/meta/adjacency objects under graph/table prefixes not protected,
  - manifest objects older than the retained block window after their referenced data/meta are no longer protected.

**Commands:**
- Red: `rtk cargo test -p varve-engine gc::tests::gc_plan_keeps_objects_referenced_by_retained_manifests`
- Green: `rtk cargo test -p varve-engine gc::tests`
- Commit: `feat: plan retention-aware gc`

### Task 11: Execute GC and expose embedded API

**Files:**
- Modify: `crates/varve-engine/src/gc.rs`
- Modify: `crates/varve-engine/src/db.rs`
- Tests: `crates/varve/tests/gc.rs` (new)

**Failing tests first:**
- `gc_once_deletes_unreferenced_objects`
- `gc_once_is_idempotent`
- `gc_once_does_not_break_restart_from_latest_manifest`

**Implementation:**
- Add `Db::gc_once() -> Result<GcReport, EngineError>`.
- Use object-store `delete` for plan candidates.
- Missing objects during delete are success.
- After GC, `Db::open` from the same config must recover latest manifest and query compacted data.

**Commands:**
- Red: `rtk cargo test -p varve --test gc gc_once_deletes_unreferenced_objects`
- Green: `rtk cargo test -p varve --test gc`
- Commit: `feat: execute storage gc`

### Task 12: Physical erase proof after compaction plus GC

**Files:**
- Modify: `crates/varve/tests/erase.rs` or create `crates/varve/tests/gdpr_gc.rs`
- Tests: e2e sentinel-byte object scan

**Failing tests first:**
- `erased_property_bytes_absent_after_compaction_and_gc`
- `post_erase_reinsert_survives_compaction`

**Implementation:**
- Insert sentinel property value, flush.
- Execute `ERASE`, flush.
- Run compaction until idle, then GC.
- List all data/meta objects and read bytes; assert sentinel bytes are absent.
- Reinsert the same `_id` after erase and assert the new value remains queryable after compaction.

**Commands:**
- Red: `rtk cargo test -p varve --test gdpr_gc erased_property_bytes_absent_after_compaction_and_gc`
- Green: `rtk cargo test -p varve --test gdpr_gc`
- Commit: `test: prove erased bytes disappear after gc`

### Task 13: Churn plateau smoke test and compaction demo

**Files:**
- Create: `crates/varve/examples/compaction_gc.rs`
- Modify: `crates/varve-testkit/tests/compaction_equivalence.rs` or create smoke test

**Failing tests first:**
- `storage_object_count_plateaus_under_update_churn`

**Implementation:**
- Test runs bounded update churn with flush, compaction, GC cycles.
- Assert protected object count stays below a documented multiple of retained manifests and active compacted tries.
- Add example that inserts, updates, erases, flushes, compacts, GCs, and prints object counts and query result count.

**Commands:**
- Red: `rtk cargo test -p varve-testkit --test compaction_equivalence storage_object_count_plateaus_under_update_churn`
- Green: `rtk cargo test -p varve-testkit --test compaction_equivalence storage_object_count_plateaus_under_update_churn`
- Demo: `rtk cargo run --release --example compaction_gc -p varve`
- Commit: `feat: add compaction gc demo`

### Task 14: Whole-slice verification and docs closeout

**Files:**
- Modify: `docs/plans/2026-07-06-slice-08-compaction-tries-gc.md` checkboxes
- Modify: `docs/plans/varve-v1-roadmap.md` Slice 8 checkboxes
- Modify: `docs/plans/STATUS.md`

**Checklist:**
- [x] `rtk cargo fmt --all --check`
- [x] `rtk cargo clippy --workspace --all-targets -- -D warnings`
- [x] `rtk cargo test --workspace -- --test-threads=1`
- [x] `rtk cargo test -p varve-testkit --test compaction_equivalence -- --test-threads=1`
- [x] `rtk cargo run --release --example compaction_gc -p varve`
- [x] STATUS.md updated with position, decisions, deviations, demo command, and next entry point.
- [x] Roadmap Slice 8 boxes checked.
- [x] This plan's completed task boxes checked.

**Commit:**
- `docs: close slice 8 compaction gc`

## Session Boundaries

- Session A: Tasks 1-4, trie keys/meta/catalog foundations.
- Session B: Tasks 5-8, delete surface and compaction write/commit.
- Session C: Tasks 9-13, equivalence, GC, erase proof, plateau/demo.
- Session D: Task 14, final verification and documentation closeout.

At any session boundary, never leave red tests. If stopping mid-slice, update `STATUS.md` with the next unchecked task and any deviations, then commit that status update.
