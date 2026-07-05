# Slice 4: Blocks — Flush to Object Storage, Persisted Scan, Restart

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The live index flushes to object storage as Arrow block files with a protobuf manifest (the commit point), queries merge live + persisted events with bitemporal resolution and page pruning, and `Db::open` restarts from the latest manifest + log tail — so a database survives restarts with a bounded log and bounded memory.

**Architecture:** New `varve-storage` crate: an `ObjectStore` trait (PUT/GET/GET_RANGE/LIST — plain S3 semantics only) as a thin blanket-impl wrapper over the `object_store` crate, spec §9 key layout (lex-hex trie keys), the `BlockManifest` protobuf, and a memory `CacheTier`. `varve-index` gains the block codec (paged data + meta files, prune rules) and a source-agnostic `snapshot_entities` scan. `varve-log` gains `Log::trim`. `varve-engine` orchestrates: the writer loop flushes at `max_block_rows`/flush-timeout (data + meta PUTs → manifest PUT = commit → live reset → log trim), the query path scans merged sources with IID-point + temporal pruning, and `Db::open` recovers from manifest + log tail.

**Tech Stack:** Adds `object_store` (0.13, unified with datafusion's transitive pin), `bytes` (1), `futures` (0.3) to the workspace. Reuses `prost` 0.14 (manifest), `arrow` 58 IPC (block pages, meta), `crc32c`-framed `varve-log`.

## Global Constraints

- All roadmap Global Constraints apply (TDD; `cargo clippy --workspace --all-targets -- -D warnings` clean; no `unwrap()`/`expect()` in library code — tests OK per `clippy.toml`; errors via `thiserror` per crate; conventional commits, **no co-author trailers**).
- **Sovereignty (spec §1, D7):** `varve-storage` exposes ONLY `put`/`get`/`get_range`/`list`. No conditional PUT, no delete (GC is slice 8), no multipart. Both v1 backends (`memory`, `local`) satisfy plain S3 semantics.
- **Bitemporal invariant (spec §5.2):** blocks store raw events sorted `(_iid, _system_from desc)`; `_system_to` and effective valid ranges are never persisted — resolution happens at scan time across live + persisted sources.
- **Determinism:** `encode_block` is a pure function of the live table (BTreeMap iteration order + fixed page chunking); manifest bytes are a pure function of its fields. No wall-clock, no randomness, no map-iteration-order dependence.
- **Dependency pinning:** `object_store = "0.13"` — verified: datafusion 54.0.0 already resolves `object_store 0.13.2` in `Cargo.lock`, so the workspace pin MUST track it (same rule as the arrow pin; re-derive with `cargo tree -p datafusion | grep object_store`). `bytes = "1"`, `futures = "0.3"` (both already transitive via datafusion). The **test code in this plan is the contract**; if an API sketch differs from the pinned crates, adapt the implementation, not the test. The `object_store` 0.13 calls used below (`ObjectStoreExt::{put,get,get_range}`, `ObjectStore::list`, `InMemory::new`, `LocalFileSystem::new_with_prefix`, `GetResult::bytes`, `Error::NotFound`, `PutPayload: From<Bytes>`) were verified against the vendored 0.13.2 source before this plan was written.
- Spec references: `docs/design/2026-07-04-varve-design.md` §9 (storage layout, meta, manifest-as-commit), §10 (BitemporalScan pushdowns), §6 (recovery), §4 (registry), §5.2 (event sort order). XTDB porting references (read, adopted): `refs/xtdb/core/src/main/kotlin/xtdb/trie/Trie.kt` (key format), `refs/xtdb/core/src/main/kotlin/xtdb/util/StringUtil.kt` (lex-hex), `refs/xtdb/dev/doc/trie-cat.allium` (key examples), `refs/xtdb/api/src/main/kotlin/xtdb/api/IndexerConfig.kt` (`rowsPerBlock`/`pageLimit` defaults), `allium/live-index.allium` (flush/NextBlock semantics).
- We are in development: NO backward compatibility anywhere (on-disk layouts, trait signatures, and test helpers change freely), but everything written is production code — no placeholders, no stubs.

## Design decisions (record in STATUS.md at slice end)

1. **Data file = concatenation of self-contained per-page Arrow IPC streams; the meta file carries each page's `(offset, len)` byte range.** A page read costs exactly one ranged GET (spec §9 caching) with zero footer parsing, and pages reuse the golden-tested slice-3 event codec (`encode_events`/`decode_events`) verbatim. The meta file IS the footer surrogate — held decoded in memory per trie (the spec's "footer cache"). A single-stream `.arrow` file with the real trie meta arrives with slice 8's compaction rewrite.
2. **Docs stay in the binary `payload` column inside blocks** (same event schema as the log effects). The slice-3 codec comment deferred "columnar doc structs (dense unions)" to slice 4, but the roadmap's slice-4 task list — which governs — needs only IID + temporal pruning; property-level stats/blooms (which would need columnar docs) land with slice 8's full trie meta. The stale codec comment is corrected in Task 6.
3. **Meta v1 = single-level page index** (roadmap: "the full hash-trie branch structure lands with compaction in slice 8"). One meta row per page: byte range, row count, min/max `_iid`, min/max of `_system_from`/`_valid_from`/`_valid_to`, and `has_erase`. Pages are IID-ordered, `PAGE_ROWS = 1024` rows (XTDB `pageLimit`), a parameter at the API level and a const in the engine.
4. **Prune rules (each proven safe in Task 6 tests):** (a) IID point outside `[min_iid, max_iid]` → skip (resolution is per-entity); (b) `min_system_from >= bounds.system.upper && !has_erase` → skip — EXACTLY output-preserving, because `resolve()` skips events at/after the system upper bound BEFORE they touch the ceiling (they neither appear nor supersede anything), while an `Erase` hides history at EVERY system time (slice-2 GDPR decision), hence the guard. **The valid axis is deliberately NOT pruned in v1:** an event whose valid range is disjoint from the query window still clips the reported `_valid_from`/`_valid_to` of visible rectangles inside the window (slice-2 history-introspection semantics — `valid_to(x)`), so dropping its page would corrupt reported ranges even though visibility would survive. The meta still records valid min/max per page for slice 8's rectangle-aware rules; a regression test pins the non-pruning.
5. **Manifest = database-wide protobuf, full inventory every time; the manifest PUT is the atomic commit point** (spec §9). `object_store`'s `put` is documented atomic on both backends (local = temp file + rename). Fields: `block_id`, `watermark` (packed `LogPosition` to replay from), `max_tx_id` + `max_system_time_us` (tx-counter and clock floors — REQUIRED because after a log trim the log alone can no longer provide them), and the trie inventory per (graph, table). Key: `v1/blocks/<lexhex(block_id)>.manifest`; latest = max parsed block id under the prefix.
6. **Watermark = `append_position.advance(batch_len)`** — the exclusive end of the flushed prefix. No `Log` trait change needed: the trait already guarantees "records receive consecutive positions", so the writer derives the end from `append`'s returned first position. Replay is `log.tail(watermark)`.
7. **`Log::trim(up_to)` = physical, whole-unit-only deletion** (new trait method): `LocalLog` deletes a segment iff the NEXT segment starts at or below `up_to` (the active segment is never deleted); `MemoryLog` retains records `>= up_to` and keeps its next-position counter so positions never regress after a trim. Trim runs strictly AFTER the manifest PUT; a crash between manifest and trim just leaves extra segments that the next flush re-trims (replay filters by position, so untrimmed history is harmless — an exact-duplicate re-apply is even invisible to resolution, pinned by test).
8. **Live table + persisted-trie inventory live under ONE `RwLock<TableState>`.** Flush swaps atomically (push trie + reset live under one write lock), queries snapshot atomically (clone live events + inventory under one read lock) — so a query can never observe flushed events in neither or both sources.
9. **Merge order:** per entity, event lists concatenate `block 0 asc ++ block 1 asc ++ … ++ live asc` — correct because system_from is monotonic in log order and flushes happen at batch boundaries, so every event in a later source postdates (or same-tx-ties never span sources) every event in an earlier one. Persisted pages store events `system_from desc` per entity; plain (stable) reversal restores ascending arrival order including same-timestamp ties.
10. **Flush failure keeps serving:** on any PUT failure the live table is untouched and flush retries at the next trigger; already-PUT data/meta objects without a manifest are invisible garbage (GC in slice 8). No log/metrics surface yet — flag in STATUS as a slice-10 observability item.
11. **`[log] backend = "local"` + `[storage] backend = "memory"` is a hard config error** (`EngineError::VolatileBlockStore`): flushing trims the durable log while blocks sit in volatile memory — silent data loss on restart. `Db::local(dir)` configures both durably: log at `dir/log`, store at `dir/store` (dev: no migration of slice-3 dirs).
12. **IID point pushdown from `WHERE v._id = <literal>`** (spec §10 "IID point/range predicates (from `_id` equality)") — required for the exit criterion "< 100 ms warm point lookup" over 1M events. Non-id-able literals (Float/Null) fall back to the unpruned scan; DataFusion still applies the WHERE filter afterwards either way.
13. **`MATCH … DELETE` resolves against the merged scan too** — a delete must find flushed entities, so the writer's read side uses the same merged-snapshot path as queries.
14. **`CacheTier` is a trait in `varve-storage` assembled directly from `[cache]` config** (`memory_max_bytes`, integer — same integer-bytes convention as slice-3's `group_commit_max_bytes`); registry-by-name selection waits for the slice-5 disk tier. **`BuildContext` is STILL not needed** — the cache wraps the store by plain composition in `varve-engine`, not inside a factory (discharges the STATUS "revisit at slice 4/5" note for slice 4).
15. **Trie keys adopt XTDB's exact encoding** (verified in `Trie.kt`/`StringUtil.kt`): lex-hex = one hex digit of (hex-body length − 1) + hex body (`0 → "00"`, `0x34 → "134"`); L0 key = `l00-rc-b<lexhex(block)>` — level 0, recency `c` (current), NO `-p` segment at L0 (part omitted when empty). Recency dates and parts arrive with slice 8.

## File structure

```
crates/varve-storage/            # NEW crate (spec §15)
  Cargo.toml
  src/lib.rs                     # exports + storage_registry()
  src/store.rs                   # StorageError, ObjectStore trait, blanket impl over object_store
  src/memory.rs                  # memory_store() + MemoryStoreFactory ("memory")
  src/local.rs                   # local_store(dir) + LocalStoreFactory ("local", [storage.local] dir)
  src/keys.rs                    # lex_hex, trie keys, data/meta/manifest keys (spec §9 layout)
  src/manifest.rs                # BlockManifest protobuf + latest_manifest()
  src/cache.rs                   # CacheKey, CacheTier, MemoryCache (LRU), CachedStore
  tests/store_test.rs            # trait conformance over both backends + factories
crates/varve-index/
  src/scan.rs                    # NEW: snapshot_entities (extracted from live.rs)
  src/block.rs                   # NEW: PageMeta, EncodedBlock, encode_block, decode_meta, prune rules
  src/live.rs                    # LiveTable::{entities, events_for}; snapshot_for_label delegates
  src/codec.rs                   # comment fix only (decision 2)
crates/varve-log/
  src/log.rs                     # Log::trim added to the trait
  src/memory.rs                  # next-position field + trim
  src/local.rs                   # whole-segment trim
crates/varve-plan/
  src/exec.rs                    # pub effective_bounds; NEW iid_point()
crates/varve-engine/
  Cargo.toml                     # + varve-storage, bytes; [features] fault-injection
  src/state.rs                   # NEW: TableState, PersistedTrie, StoreCtx, shared consts
  src/scan.rs                    # NEW: merged_snapshot (prune → ranged GET → decode → merge → resolve)
  src/flush.rs                   # NEW: flush_block + crash_point hooks
  src/writer.rs                  # WriterState/WriterConfig extended; loop gains flush trigger + timer
  src/db.rs                      # Db holds TableState + StoreCtx; recovery from manifest; config wiring
  src/registries.rs              # + storage registry
crates/varve/
  tests/blocks.rs                # NEW: flush/restart e2e
  tests/durability.rs            # config helper gains [storage] (decision 11)
  examples/block_bench.rs        # NEW: 1M-event ingest → restart → warm point lookup
crates/varve-testkit/
  Cargo.toml                     # + varve-storage, varve-engine (fault-injection)
  tests/flush_equivalence.rs     # NEW: randomized-flush-points property test
  src/bin/crash_child.rs         # config-based open; flush crash points
  tests/crash_recovery.rs        # matrix + pre/post-manifest points
Cargo.toml                       # workspace pins: object_store, bytes, futures
```

---
### Task 1: `varve-storage` crate — ObjectStore trait, memory & local backends, registry

**Files:**
- Modify: `Cargo.toml` (workspace root — add pins)
- Create: `crates/varve-storage/Cargo.toml`
- Create: `crates/varve-storage/src/lib.rs`
- Create: `crates/varve-storage/src/store.rs`
- Create: `crates/varve-storage/src/memory.rs`
- Create: `crates/varve-storage/src/local.rs`
- Test: `crates/varve-storage/tests/store_test.rs`

**Interfaces:**
- Produces: `varve_storage::StorageError` — thiserror enum: `NotFound(String)`, `Backend(object_store::Error)`, `Io(#[from] std::io::Error)` (a `Decode` variant is added in Task 3).
- Produces: `varve_storage::ObjectStore` (trait, `Send + Sync`, async_trait):
  `async fn put(&self, key: &str, bytes: Bytes) -> Result<(), StorageError>` (atomic create/replace);
  `async fn get(&self, key: &str) -> Result<Bytes, StorageError>`;
  `async fn get_range(&self, key: &str, range: std::ops::Range<u64>) -> Result<Bytes, StorageError>` (half-open byte range);
  `async fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError>` (keys under a path-segment prefix, sorted).
  A blanket impl makes every `object_store::ObjectStore` implementation a Varve `ObjectStore`.
- Produces: `varve_storage::memory_store() -> Arc<dyn ObjectStore>`; `varve_storage::local_store(dir: &Path) -> Result<Arc<dyn ObjectStore>, StorageError>` (creates `dir`); factories `MemoryStoreFactory` (`"memory"`), `LocalStoreFactory` (`"local"`, reads `[storage.local] dir`); `varve_storage::storage_registry() -> Registry<dyn ObjectStore>`.

- [x] **Step 1: Workspace + crate scaffolding**

Root `Cargo.toml`, append to `[workspace.dependencies]`:

```toml
object_store = "0.13"
bytes = "1"
futures = "0.3"
```

(`crates/*` globbing picks the new crate up automatically.)

`crates/varve-storage/Cargo.toml`:

```toml
[package]
name = "varve-storage"
version.workspace = true
edition.workspace = true
license.workspace = true

[dependencies]
varve-config = { path = "../varve-config" }
object_store = { workspace = true }
bytes = { workspace = true }
futures = { workspace = true }
async-trait = { workspace = true }
serde = { workspace = true }
thiserror = { workspace = true }

[dev-dependencies]
tokio = { workspace = true }
tempfile = { workspace = true }

[lints]
workspace = true
```

- [x] **Step 2: Write the failing test**

`crates/varve-storage/tests/store_test.rs`:

```rust
use bytes::Bytes;
use std::sync::Arc;
use varve_config::{Config, ConfigSection};
use varve_storage::{local_store, memory_store, storage_registry, ObjectStore, StorageError};

/// Trait conformance, run against every backend: atomic put/replace, whole
/// and ranged gets, NotFound, sorted prefix-scoped listing.
async fn exercise(store: Arc<dyn ObjectStore>) {
    store
        .put("v1/a/one", Bytes::from_static(b"hello"))
        .await
        .unwrap();
    assert_eq!(
        store.get("v1/a/one").await.unwrap(),
        Bytes::from_static(b"hello")
    );

    // put replaces
    store
        .put("v1/a/one", Bytes::from_static(b"world"))
        .await
        .unwrap();
    assert_eq!(
        store.get("v1/a/one").await.unwrap(),
        Bytes::from_static(b"world")
    );

    // half-open byte range
    assert_eq!(
        store.get_range("v1/a/one", 1..4).await.unwrap(),
        Bytes::from_static(b"orl")
    );

    // NotFound carries the key
    assert!(matches!(
        store.get("v1/a/absent").await,
        Err(StorageError::NotFound(k)) if k == "v1/a/absent"
    ));

    // list: prefix-scoped (path segments), sorted, empty prefix ok
    store
        .put("v1/a/two", Bytes::from_static(b"2"))
        .await
        .unwrap();
    store
        .put("v1/b/three", Bytes::from_static(b"3"))
        .await
        .unwrap();
    assert_eq!(
        store.list("v1/a").await.unwrap(),
        vec!["v1/a/one".to_string(), "v1/a/two".to_string()]
    );
    assert_eq!(store.list("v1/absent").await.unwrap(), Vec::<String>::new());
}

#[tokio::test]
async fn memory_backend_conforms() {
    exercise(memory_store()).await;
}

#[tokio::test]
async fn local_backend_conforms() {
    let dir = tempfile::tempdir().unwrap();
    exercise(local_store(dir.path()).unwrap()).await;
}

#[tokio::test]
async fn local_store_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    local_store(dir.path())
        .unwrap()
        .put("v1/x", Bytes::from_static(b"here"))
        .await
        .unwrap();
    let again = local_store(dir.path()).unwrap();
    assert_eq!(again.get("v1/x").await.unwrap(), Bytes::from_static(b"here"));
}

#[tokio::test]
async fn registry_builds_by_name() {
    let reg = storage_registry();
    assert_eq!(reg.names(), vec!["local", "memory"]);
    let store = reg.build("memory", &ConfigSection::empty()).unwrap();
    store.put("k", Bytes::from_static(b"v")).await.unwrap();
    assert_eq!(store.get("k").await.unwrap(), Bytes::from_static(b"v"));
}

#[test]
fn local_factory_requires_dir() {
    let err = storage_registry()
        .build("local", &ConfigSection::empty())
        .unwrap_err();
    assert!(err.to_string().contains("[storage.local]"), "{err}");
}

#[tokio::test]
async fn local_factory_builds_from_config() {
    let dir = tempfile::tempdir().unwrap();
    let toml = format!(
        "[storage]\nbackend = \"local\"\n[storage.local]\ndir = {:?}\n",
        dir.path().display().to_string()
    );
    let cfg = Config::from_toml_str(&toml).unwrap().section("storage").unwrap();
    let store = storage_registry().build("local", &cfg).unwrap();
    store.put("v1/y", Bytes::from_static(b"z")).await.unwrap();
    assert_eq!(store.get("v1/y").await.unwrap(), Bytes::from_static(b"z"));
}
```

- [x] **Step 3: Run test to verify it fails**

Run: `cargo test -p varve-storage`
Expected: FAIL — crate compiles empty / items missing (`memory_store` not found).

- [x] **Step 4: Minimal implementation**

`crates/varve-storage/src/store.rs`:

```rust
use bytes::Bytes;
use std::ops::Range;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("object not found: {0}")]
    NotFound(String),
    #[error("storage backend error: {0}")]
    Backend(#[source] object_store::Error),
    #[error("storage I/O error: {0}")]
    Io(#[from] std::io::Error),
}

/// Maps a backend error, preserving which key was not found. Deliberately not
/// a `From` impl: `NotFound` needs the key.
pub(crate) fn convert(key: &str, e: object_store::Error) -> StorageError {
    match e {
        object_store::Error::NotFound { .. } => StorageError::NotFound(key.to_string()),
        other => StorageError::Backend(other),
    }
}

/// Varve's object-store interface (spec §4, §9). Sovereignty (spec §1, D7):
/// nothing beyond plain S3 semantics — put/get/list only; no conditional
/// PUT, no delete (GC arrives in slice 8). `put` is atomic: readers see the
/// whole object or none of it (the manifest commit point relies on this).
#[async_trait::async_trait]
pub trait ObjectStore: Send + Sync {
    /// Atomically create or replace the object at `key`.
    async fn put(&self, key: &str, bytes: Bytes) -> Result<(), StorageError>;
    /// The whole object.
    async fn get(&self, key: &str) -> Result<Bytes, StorageError>;
    /// Bytes in `[range.start, range.end)`.
    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Bytes, StorageError>;
    /// Keys under the path-segment prefix (e.g. `"v1/blocks"`), sorted
    /// lexicographically. Prefixes match whole path segments, not substrings.
    async fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError>;
}

/// Every `object_store` crate backend IS a Varve `ObjectStore` — the "thin
/// wrapper" the roadmap asks for is this blanket impl. All calls use fully
/// qualified syntax: `self.put(...)` inside this impl would resolve back to
/// THIS trait and recurse forever.
#[async_trait::async_trait]
impl<T: object_store::ObjectStore> ObjectStore for T {
    async fn put(&self, key: &str, bytes: Bytes) -> Result<(), StorageError> {
        let path = object_store::path::Path::from(key);
        object_store::ObjectStoreExt::put(self, &path, bytes.into())
            .await
            .map_err(|e| convert(key, e))?;
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Bytes, StorageError> {
        let path = object_store::path::Path::from(key);
        let result = object_store::ObjectStoreExt::get(self, &path)
            .await
            .map_err(|e| convert(key, e))?;
        result.bytes().await.map_err(|e| convert(key, e))
    }

    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Bytes, StorageError> {
        let path = object_store::path::Path::from(key);
        object_store::ObjectStoreExt::get_range(self, &path, range)
            .await
            .map_err(|e| convert(key, e))
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
        use futures::TryStreamExt as _;
        let path = object_store::path::Path::from(prefix);
        let metas: Vec<object_store::ObjectMeta> =
            object_store::ObjectStore::list(self, Some(&path))
                .try_collect()
                .await
                .map_err(|e| convert(prefix, e))?;
        let mut keys: Vec<String> = metas.into_iter().map(|m| m.location.to_string()).collect();
        keys.sort();
        Ok(keys)
    }
}
```

`crates/varve-storage/src/memory.rs`:

```rust
use crate::store::ObjectStore;
use std::sync::Arc;
use varve_config::{ComponentFactory, ConfigSection, RegistryError};

/// Volatile in-process store (tests, `Db::memory()`).
pub fn memory_store() -> Arc<dyn ObjectStore> {
    Arc::new(object_store::memory::InMemory::new())
}

/// Registry factory: `[storage] backend = "memory"`.
pub struct MemoryStoreFactory;

impl ComponentFactory<dyn ObjectStore> for MemoryStoreFactory {
    fn name(&self) -> &'static str {
        "memory"
    }

    fn build(&self, _cfg: &ConfigSection) -> Result<Arc<dyn ObjectStore>, RegistryError> {
        Ok(memory_store())
    }
}
```

`crates/varve-storage/src/local.rs`:

```rust
use crate::store::{ObjectStore, StorageError};
use std::path::Path;
use std::sync::Arc;
use varve_config::{ComponentFactory, ConfigSection, RegistryError};

/// Local-filesystem store rooted at `dir` (created if missing). Object keys
/// map to file paths under the root; `object_store::LocalFileSystem` writes
/// via temp-file + rename, so `put` is atomic (the manifest relies on this).
pub fn local_store(dir: &Path) -> Result<Arc<dyn ObjectStore>, StorageError> {
    std::fs::create_dir_all(dir)?;
    let fs = object_store::local::LocalFileSystem::new_with_prefix(dir)
        .map_err(StorageError::Backend)?;
    Ok(Arc::new(fs))
}

#[derive(serde::Deserialize)]
struct LocalStoreConfig {
    dir: String,
}

/// Registry factory: `[storage] backend = "local"`, configured via a nested
/// `[storage.local]` table (`dir` required).
pub struct LocalStoreFactory;

impl ComponentFactory<dyn ObjectStore> for LocalStoreFactory {
    fn name(&self) -> &'static str {
        "local"
    }

    fn build(&self, cfg: &ConfigSection) -> Result<Arc<dyn ObjectStore>, RegistryError> {
        let local = cfg.child("local").ok_or_else(|| RegistryError::Build {
            kind: "storage",
            name: "local".into(),
            source: "missing [storage.local] section (requires `dir`)"
                .to_string()
                .into(),
        })?;
        let config: LocalStoreConfig = local.get()?;
        local_store(Path::new(&config.dir)).map_err(|e| RegistryError::Build {
            kind: "storage",
            name: "local".into(),
            source: Box::new(e),
        })
    }
}
```

`crates/varve-storage/src/lib.rs`:

```rust
pub mod local;
pub mod memory;
pub mod store;

pub use local::{local_store, LocalStoreFactory};
pub use memory::{memory_store, MemoryStoreFactory};
pub use store::{ObjectStore, StorageError};

use varve_config::{ComponentFactory, Registry};

/// All built-in storage backends, registered under kind "storage".
pub fn storage_registry() -> Registry<dyn ObjectStore> {
    let mut reg = Registry::new("storage");
    register_builtin(&mut reg, Box::new(MemoryStoreFactory));
    register_builtin(&mut reg, Box::new(LocalStoreFactory));
    reg
}

/// Built-in names are a static, distinct set fixed at compile time — a
/// collision is a programming error in this crate, never a runtime
/// configuration problem (same rationale as `varve_log::log_registry`).
fn register_builtin(
    reg: &mut Registry<dyn ObjectStore>,
    factory: Box<dyn ComponentFactory<dyn ObjectStore>>,
) {
    if let Err(e) = reg.register(factory) {
        unreachable!("built-in storage factory registration must not collide: {e}");
    }
}
```

- [x] **Step 5: Run test to verify it passes**

Run: `cargo test -p varve-storage`
Expected: 6 tests pass.

- [x] **Step 6: Commit**

```bash
git add Cargo.toml Cargo.lock crates/varve-storage/
git commit -m "feat: varve-storage crate with ObjectStore trait over object_store backends"
```

---

### Task 2: Key layout — lex-hex, trie keys, object keys (spec §9)

**Files:**
- Create: `crates/varve-storage/src/keys.rs`
- Modify: `crates/varve-storage/src/lib.rs` (add `pub mod keys;`)
- Test: in-module `#[cfg(test)]` in `keys.rs`

**Interfaces:**
- Produces: `varve_storage::keys::{lex_hex, parse_lex_hex, l0_trie_key, data_key, meta_key, manifest_key, manifest_block_id, MANIFEST_PREFIX}`:
  `lex_hex(n: u64) -> String`; `parse_lex_hex(s: &str) -> Option<u64>`;
  `l0_trie_key(block_id: u64) -> String` (`"l00-rc-b<lexhex>"`);
  `data_key(graph: &str, table: &str, trie_key: &str) -> String` (`"v1/graphs/<g>/tables/<t>/data/<trie>.arrow"`); `meta_key(..)` (same with `meta/`);
  `manifest_key(block_id: u64) -> String` (`"v1/blocks/<lexhex>.manifest"`); `manifest_block_id(key: &str) -> Option<u64>`; `MANIFEST_PREFIX: &str = "v1/blocks"`.

- [x] **Step 1: Write the failing test**

`crates/varve-storage/src/keys.rs` (tests first; module body in Step 3):

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lex_hex_known_answers() {
        // XTDB StringUtil.asLexHex: one hex digit of (body length - 1), then
        // the hex body. trie-cat.allium: "b134 is block 0x34".
        assert_eq!(lex_hex(0), "00");
        assert_eq!(lex_hex(1), "01");
        assert_eq!(lex_hex(0xf), "0f");
        assert_eq!(lex_hex(0x10), "110");
        assert_eq!(lex_hex(0x34), "134");
        assert_eq!(lex_hex(0xff), "1ff");
        assert_eq!(lex_hex(0x100), "2100");
        assert_eq!(lex_hex(u64::MAX), format!("f{:x}", u64::MAX));
    }

    #[test]
    fn lex_hex_round_trips_and_rejects_garbage() {
        for n in [0u64, 1, 15, 16, 0x34, 255, 256, 1 << 47, u64::MAX] {
            assert_eq!(parse_lex_hex(&lex_hex(n)), Some(n), "{n}");
        }
        assert_eq!(parse_lex_hex(""), None);
        assert_eq!(parse_lex_hex("1"), None); // body missing
        assert_eq!(parse_lex_hex("134x"), None); // body length mismatch
        assert_eq!(parse_lex_hex("zz"), None);
    }

    #[test]
    fn lexicographic_order_is_numeric_order() {
        let ns = [0u64, 1, 9, 0xf, 0x10, 0x99, 0xff, 0x100, 0xabc, 1 << 20];
        let mut by_string: Vec<u64> = ns.to_vec();
        by_string.sort_by_key(|n| lex_hex(*n));
        let mut by_value = ns.to_vec();
        by_value.sort_unstable();
        assert_eq!(by_string, by_value);
    }

    #[test]
    fn l0_trie_key_matches_the_xtdb_reference() {
        // trie-cat.allium canonical example: "l00-rc-b00  level 0, current, block 0".
        assert_eq!(l0_trie_key(0), "l00-rc-b00");
        assert_eq!(l0_trie_key(0x34), "l00-rc-b134");
    }

    #[test]
    fn object_keys_follow_the_spec_layout() {
        // Spec §9 key layout, verbatim.
        assert_eq!(
            data_key("default", "nodes", "l00-rc-b00"),
            "v1/graphs/default/tables/nodes/data/l00-rc-b00.arrow"
        );
        assert_eq!(
            meta_key("default", "nodes", "l00-rc-b00"),
            "v1/graphs/default/tables/nodes/meta/l00-rc-b00.arrow"
        );
        assert_eq!(manifest_key(0), "v1/blocks/00.manifest");
        assert_eq!(manifest_key(0x34), "v1/blocks/134.manifest");
    }

    #[test]
    fn manifest_block_id_parses_only_manifest_keys() {
        assert_eq!(manifest_block_id("v1/blocks/00.manifest"), Some(0));
        assert_eq!(manifest_block_id(&manifest_key(0x34)), Some(0x34));
        assert_eq!(manifest_block_id("v1/blocks/00.tmp"), None);
        assert_eq!(manifest_block_id("v1/other/00.manifest"), None);
        assert_eq!(manifest_block_id("v1/blocks/zz.manifest"), None);
    }
}
```

- [x] **Step 2: Run test to verify it fails**

Run: `cargo test -p varve-storage keys`
Expected: FAIL — module/functions not defined (add `pub mod keys;` to `lib.rs` first so the failure is about the functions).

- [x] **Step 3: Minimal implementation**

Prepend to `crates/varve-storage/src/keys.rs`:

```rust
//! Object-key layout (spec §9). Everything lives under the format-version
//! prefix `v1/`. Trie keys adopt XTDB's encoding exactly (Trie.kt +
//! StringUtil.kt): lexicographic listing order == logical order.

/// Lex-hex: one hex digit encoding (hex-body length − 1), then the hex body.
/// `0 → "00"`, `0x34 → "134"`. Sorts lexicographically in numeric order over
/// the whole u64 range (body length 1..=16 ⇒ prefix digit '0'..='f').
pub fn lex_hex(n: u64) -> String {
    let body = format!("{n:x}");
    format!("{:x}{body}", body.len() - 1)
}

pub fn parse_lex_hex(s: &str) -> Option<u64> {
    let mut chars = s.chars();
    let len_digit = chars.next()?.to_digit(16)? as usize;
    let body = chars.as_str();
    if body.len() != len_digit + 1 || body.chars().any(|c| c.is_ascii_uppercase()) {
        return None;
    }
    u64::from_str_radix(body, 16).ok()
}

/// L0 trie key (XTDB `Trie.l0Key`): level 0, recency `c` (current), no part
/// segment. Levels > 0, recency dates, and IID partitions arrive in slice 8.
pub fn l0_trie_key(block_id: u64) -> String {
    format!("l{}-rc-b{}", lex_hex(0), lex_hex(block_id))
}

pub fn data_key(graph: &str, table: &str, trie_key: &str) -> String {
    format!("v1/graphs/{graph}/tables/{table}/data/{trie_key}.arrow")
}

pub fn meta_key(graph: &str, table: &str, trie_key: &str) -> String {
    format!("v1/graphs/{graph}/tables/{table}/meta/{trie_key}.arrow")
}

pub const MANIFEST_PREFIX: &str = "v1/blocks";

pub fn manifest_key(block_id: u64) -> String {
    format!("{MANIFEST_PREFIX}/{}.manifest", lex_hex(block_id))
}

/// Parses the block id out of a manifest key; `None` for anything else
/// (foreign keys under the prefix are ignored, never an error).
pub fn manifest_block_id(key: &str) -> Option<u64> {
    parse_lex_hex(
        key.strip_prefix(MANIFEST_PREFIX)?
            .strip_prefix('/')?
            .strip_suffix(".manifest")?,
    )
}
```

Add to `crates/varve-storage/src/lib.rs`:

```rust
pub mod keys;
```

(No uppercase in lex-hex bodies: `format!("{n:x}")` emits lowercase; `parse_lex_hex` rejects uppercase so parse(lex_hex(n)) is a bijection on canonical keys.)

- [x] **Step 4: Run test to verify it passes**

Run: `cargo test -p varve-storage keys`
Expected: 6 tests pass.

- [x] **Step 5: Commit**

```bash
git add crates/varve-storage/
git commit -m "feat: spec-9 object key layout with XTDB lex-hex trie keys"
```

---

### Task 3: Block manifest — protobuf envelope + latest-manifest lookup

**Files:**
- Create: `crates/varve-storage/src/manifest.rs`
- Modify: `crates/varve-storage/Cargo.toml` (add `prost`)
- Modify: `crates/varve-storage/src/lib.rs` (export)
- Modify: `crates/varve-storage/src/store.rs` (`StorageError::Decode` variant)
- Test: in-module `#[cfg(test)]` in `manifest.rs`

**Interfaces:**
- Produces: `varve_storage::manifest::{TrieEntry, TableTries, BlockManifest, latest_manifest}`:

```rust
pub struct TrieEntry { pub trie_key: String, pub row_count: u64, pub data_len: u64 }
pub struct TableTries { pub graph: String, pub table: String, pub tries: Vec<TrieEntry> }
pub struct BlockManifest {
    pub block_id: u64,
    pub watermark: u64,            // LogPosition::as_u64 — replay the log from here
    pub max_tx_id: u64,            // tx-counter floor after restart
    pub max_system_time_us: i64,   // clock floor after restart
    pub tables: Vec<TableTries>,   // FULL inventory (not a delta)
}
impl BlockManifest {
    pub fn to_wire(&self) -> Vec<u8>;
    pub fn from_wire(bytes: &[u8]) -> Result<BlockManifest, StorageError>;
}
pub async fn latest_manifest(store: &dyn ObjectStore) -> Result<Option<BlockManifest>, StorageError>;
```

- Produces: `StorageError::Decode(#[from] prost::DecodeError)`.

- [x] **Step 1: Write the failing test**

`crates/varve-storage/src/manifest.rs` (tests first; module body in Step 3):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory_store;
    use bytes::Bytes;

    fn sample() -> BlockManifest {
        BlockManifest {
            block_id: 1,
            watermark: 5,
            max_tx_id: 3,
            max_system_time_us: 7,
            tables: vec![TableTries {
                graph: "default".into(),
                table: "nodes".into(),
                tries: vec![TrieEntry {
                    trie_key: "l00-rc-b00".into(),
                    row_count: 2,
                    data_len: 9,
                }],
            }],
        }
    }

    #[test]
    fn wire_round_trips() {
        let m = sample();
        assert_eq!(BlockManifest::from_wire(&m.to_wire()).unwrap(), m);
    }

    #[test]
    fn wire_golden_bytes() {
        // Pins field numbers and wire types (protobuf wire format is stable,
        // so exact bytes are safe to golden-test — slice-3 LogRecord pattern).
        #[rustfmt::skip]
        let expected: Vec<u8> = vec![
            0x08, 0x01,             // 1: block_id = 1
            0x10, 0x05,             // 2: watermark = 5
            0x18, 0x03,             // 3: max_tx_id = 3
            0x20, 0x07,             // 4: max_system_time_us = 7
            0x2A, 0x22,             // 5: tables[0], 34 bytes
            0x0A, 0x07, b'd', b'e', b'f', b'a', b'u', b'l', b't', // graph
            0x12, 0x05, b'n', b'o', b'd', b'e', b's',             // table
            0x1A, 0x10,             // tries[0], 16 bytes
            0x0A, 0x0A, b'l', b'0', b'0', b'-', b'r', b'c', b'-', b'b', b'0', b'0',
            0x10, 0x02,             // row_count = 2
            0x18, 0x09,             // data_len = 9
        ];
        assert_eq!(sample().to_wire(), expected);
    }

    #[test]
    fn from_wire_rejects_garbage() {
        assert!(matches!(
            BlockManifest::from_wire(&[0xFF, 0xFF, 0xFF]),
            Err(StorageError::Decode(_))
        ));
    }

    #[tokio::test]
    async fn latest_manifest_none_when_empty() {
        let store = memory_store();
        assert_eq!(latest_manifest(store.as_ref()).await.unwrap(), None);
    }

    #[tokio::test]
    async fn latest_manifest_picks_the_highest_block_id() {
        let store = memory_store();
        for block_id in [0u64, 1] {
            let m = BlockManifest {
                block_id,
                ..sample()
            };
            store
                .put(&crate::keys::manifest_key(block_id), Bytes::from(m.to_wire()))
                .await
                .unwrap();
        }
        // A foreign key under the prefix is ignored, not an error.
        store
            .put("v1/blocks/stray.tmp", Bytes::from_static(b"x"))
            .await
            .unwrap();
        let latest = latest_manifest(store.as_ref()).await.unwrap().unwrap();
        assert_eq!(latest.block_id, 1);
    }

    #[tokio::test]
    async fn latest_manifest_surfaces_corruption() {
        let store = memory_store();
        store
            .put(&crate::keys::manifest_key(0), Bytes::from_static(b"\xFF\xFF"))
            .await
            .unwrap();
        assert!(matches!(
            latest_manifest(store.as_ref()).await,
            Err(StorageError::Decode(_))
        ));
    }
}
```

- [x] **Step 2: Run test to verify it fails**

Run: `cargo test -p varve-storage manifest`
Expected: FAIL — types not defined.

- [x] **Step 3: Minimal implementation**

`crates/varve-storage/Cargo.toml` — add to `[dependencies]`:

```toml
prost = { workspace = true }
```

`crates/varve-storage/src/store.rs` — add a variant to `StorageError`:

```rust
    #[error("manifest decode failed: {0}")]
    Decode(#[from] prost::DecodeError),
```

Prepend to `crates/varve-storage/src/manifest.rs`:

```rust
//! The block manifest (spec §9): "the manifest write is the atomic commit"
//! — a data file without a manifest entry is invisible garbage. Database-
//! wide; carries the log-replay watermark and the FULL trie inventory, plus
//! the tx-id and clock floors that the log alone can no longer provide once
//! it has been trimmed.

use crate::keys::{manifest_block_id, manifest_key, MANIFEST_PREFIX};
use crate::store::{ObjectStore, StorageError};
use prost::Message;

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TrieEntry {
    #[prost(string, tag = "1")]
    pub trie_key: String,
    #[prost(uint64, tag = "2")]
    pub row_count: u64,
    #[prost(uint64, tag = "3")]
    pub data_len: u64,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TableTries {
    #[prost(string, tag = "1")]
    pub graph: String,
    #[prost(string, tag = "2")]
    pub table: String,
    #[prost(message, repeated, tag = "3")]
    pub tries: Vec<TrieEntry>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BlockManifest {
    #[prost(uint64, tag = "1")]
    pub block_id: u64,
    #[prost(uint64, tag = "2")]
    pub watermark: u64,
    #[prost(uint64, tag = "3")]
    pub max_tx_id: u64,
    #[prost(int64, tag = "4")]
    pub max_system_time_us: i64,
    #[prost(message, repeated, tag = "5")]
    pub tables: Vec<TableTries>,
}

impl BlockManifest {
    pub fn to_wire(&self) -> Vec<u8> {
        self.encode_to_vec()
    }

    pub fn from_wire(bytes: &[u8]) -> Result<BlockManifest, StorageError> {
        Ok(<BlockManifest as Message>::decode(bytes)?)
    }
}

/// The newest committed manifest: max parsed block id under `v1/blocks/`
/// (lex-hex keys sort numerically, but we parse-and-max rather than trust
/// listing order). `None` on a fresh store.
pub async fn latest_manifest(
    store: &dyn ObjectStore,
) -> Result<Option<BlockManifest>, StorageError> {
    let keys = store.list(MANIFEST_PREFIX).await?;
    let Some(latest) = keys.iter().filter_map(|k| manifest_block_id(k)).max() else {
        return Ok(None);
    };
    let bytes = store.get(&manifest_key(latest)).await?;
    Ok(Some(BlockManifest::from_wire(&bytes)?))
}
```

Add to `crates/varve-storage/src/lib.rs`:

```rust
pub mod manifest;
pub use manifest::{latest_manifest, BlockManifest, TableTries, TrieEntry};
```

- [x] **Step 4: Run test to verify it passes**

Run: `cargo test -p varve-storage`
Expected: all pass (store + keys + 6 manifest tests).

- [x] **Step 5: Commit**

```bash
git add crates/varve-storage/ Cargo.lock
git commit -m "feat: block manifest protobuf with watermark and trie inventory"
```

---
### Task 4: Memory cache tier — `CacheTier`, LRU `MemoryCache`, read-through `CachedStore`

**Files:**
- Create: `crates/varve-storage/src/cache.rs`
- Modify: `crates/varve-storage/src/lib.rs` (export)
- Test: in-module `#[cfg(test)]` in `cache.rs`

**Interfaces:**
- Produces: `varve_storage::cache::{CacheKey, CacheTier, MemoryCache, CachedStore}`:

```rust
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct CacheKey { pub path: String, pub range: Option<(u64, u64)> }

pub trait CacheTier: Send + Sync {
    fn get(&self, key: &CacheKey) -> Option<Bytes>;
    fn insert(&self, key: CacheKey, value: Bytes);
    fn invalidate_path(&self, path: &str);
}

pub struct MemoryCache { /* LRU by byte budget */ }
impl MemoryCache { pub fn new(max_bytes: usize) -> MemoryCache }

pub struct CachedStore { /* read-through wrapper */ }
impl CachedStore {
    pub fn new(inner: Arc<dyn ObjectStore>, cache: Arc<dyn CacheTier>) -> CachedStore
}
// CachedStore implements varve_storage::ObjectStore
```

- Consumes: `ObjectStore`, `StorageError` (Task 1).
- Registry-by-name selection for cache tiers is deliberately deferred to slice 5 (disk tier) — decision 14.

- [x] **Step 1: Write the failing test**

`crates/varve-storage/src/cache.rs` (tests first; module body in Step 3):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{memory_store, ObjectStore, StorageError};
    use std::ops::Range;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    /// Counts backend reads so tests can assert cache hits.
    struct CountingStore {
        inner: Arc<dyn ObjectStore>,
        reads: AtomicUsize,
    }

    impl CountingStore {
        fn new() -> Arc<CountingStore> {
            Arc::new(CountingStore {
                inner: memory_store(),
                reads: AtomicUsize::new(0),
            })
        }
        fn reads(&self) -> usize {
            self.reads.load(Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for CountingStore {
        async fn put(&self, key: &str, bytes: Bytes) -> Result<(), StorageError> {
            self.inner.put(key, bytes).await
        }
        async fn get(&self, key: &str) -> Result<Bytes, StorageError> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            self.inner.get(key).await
        }
        async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Bytes, StorageError> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            self.inner.get_range(key, range).await
        }
        async fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
            self.inner.list(prefix).await
        }
    }

    fn cached(counting: &Arc<CountingStore>, budget: usize) -> CachedStore {
        CachedStore::new(
            Arc::clone(counting) as Arc<dyn ObjectStore>,
            Arc::new(MemoryCache::new(budget)),
        )
    }

    #[tokio::test]
    async fn whole_object_reads_hit_the_cache() {
        let counting = CountingStore::new();
        let store = cached(&counting, 1024);
        store.put("k", Bytes::from_static(b"value")).await.unwrap();
        assert_eq!(store.get("k").await.unwrap(), Bytes::from_static(b"value"));
        assert_eq!(store.get("k").await.unwrap(), Bytes::from_static(b"value"));
        assert_eq!(counting.reads(), 1);
    }

    #[tokio::test]
    async fn ranged_reads_are_cached_per_range() {
        let counting = CountingStore::new();
        let store = cached(&counting, 1024);
        store.put("k", Bytes::from_static(b"abcdef")).await.unwrap();
        assert_eq!(store.get_range("k", 0..2).await.unwrap(), Bytes::from_static(b"ab"));
        assert_eq!(store.get_range("k", 0..2).await.unwrap(), Bytes::from_static(b"ab"));
        assert_eq!(store.get_range("k", 2..4).await.unwrap(), Bytes::from_static(b"cd"));
        assert_eq!(counting.reads(), 2); // one per distinct range
    }

    #[tokio::test]
    async fn put_invalidates_cached_entries_for_the_path() {
        let counting = CountingStore::new();
        let store = cached(&counting, 1024);
        store.put("k", Bytes::from_static(b"old")).await.unwrap();
        assert_eq!(store.get("k").await.unwrap(), Bytes::from_static(b"old"));
        store.put("k", Bytes::from_static(b"new")).await.unwrap();
        assert_eq!(store.get("k").await.unwrap(), Bytes::from_static(b"new"));
        assert_eq!(counting.reads(), 2);
    }

    #[test]
    fn lru_evicts_the_least_recently_used_entry() {
        let cache = MemoryCache::new(8);
        let key = |p: &str| CacheKey { path: p.into(), range: None };
        cache.insert(key("a"), Bytes::from_static(b"aaaa"));
        cache.insert(key("b"), Bytes::from_static(b"bbbb"));
        assert!(cache.get(&key("a")).is_some()); // touch a → b is now LRU
        cache.insert(key("c"), Bytes::from_static(b"cccc"));
        assert!(cache.get(&key("a")).is_some());
        assert!(cache.get(&key("b")).is_none(), "b was least recently used");
        assert!(cache.get(&key("c")).is_some());
    }

    #[test]
    fn oversized_values_are_never_cached() {
        let cache = MemoryCache::new(4);
        let key = CacheKey { path: "big".into(), range: None };
        cache.insert(key.clone(), Bytes::from_static(b"too large"));
        assert!(cache.get(&key).is_none());
    }

    #[test]
    fn reinserting_a_key_replaces_its_byte_accounting() {
        let cache = MemoryCache::new(8);
        let key = |p: &str| CacheKey { path: p.into(), range: None };
        cache.insert(key("a"), Bytes::from_static(b"aaaa"));
        cache.insert(key("a"), Bytes::from_static(b"aa")); // shrink in place
        cache.insert(key("b"), Bytes::from_static(b"bbbb"));
        // 2 + 4 = 6 <= 8: both fit only if the old 4-byte "a" was released.
        assert!(cache.get(&key("a")).is_some());
        assert!(cache.get(&key("b")).is_some());
    }

    #[tokio::test]
    async fn list_bypasses_the_cache() {
        let counting = CountingStore::new();
        let store = cached(&counting, 1024);
        store.put("p/x", Bytes::from_static(b"1")).await.unwrap();
        assert_eq!(store.list("p").await.unwrap(), vec!["p/x".to_string()]);
        store.put("p/y", Bytes::from_static(b"2")).await.unwrap();
        // A fresh manifest must be visible immediately — list is never cached.
        assert_eq!(
            store.list("p").await.unwrap(),
            vec!["p/x".to_string(), "p/y".to_string()]
        );
    }
}
```

- [x] **Step 2: Run test to verify it fails**

Run: `cargo test -p varve-storage cache`
Expected: FAIL — types not defined (add `pub mod cache;` to `lib.rs` first).

- [x] **Step 3: Minimal implementation**

Prepend to `crates/varve-storage/src/cache.rs`:

```rust
//! Query-path caching (spec §9): tiers keyed by object path + byte range.
//! v1 ships the in-memory tier; the disk tier arrives in slice 5. Objects
//! are immutable by key discipline (append-only store), but `put` still
//! invalidates the written path as a correctness belt.

use crate::store::{ObjectStore, StorageError};
use bytes::Bytes;
use std::collections::HashMap;
use std::ops::Range;
use std::sync::{Arc, Mutex};

/// `range: None` = the whole object; `Some((start, end))` = a half-open
/// byte range — distinct cache entries.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct CacheKey {
    pub path: String,
    pub range: Option<(u64, u64)>,
}

pub trait CacheTier: Send + Sync {
    fn get(&self, key: &CacheKey) -> Option<Bytes>;
    fn insert(&self, key: CacheKey, value: Bytes);
    fn invalidate_path(&self, path: &str);
}

struct Entry {
    value: Bytes,
    last_used: u64,
}

#[derive(Default)]
struct CacheInner {
    entries: HashMap<CacheKey, Entry>,
    bytes: usize,
    tick: u64,
}

/// LRU over a byte budget. Eviction scans for the minimum tick — O(n) per
/// eviction, fine at v1 entry counts (whole objects and pages, not rows).
/// A poisoned lock degrades to cache-miss behavior, never an error.
pub struct MemoryCache {
    max_bytes: usize,
    inner: Mutex<CacheInner>,
}

impl MemoryCache {
    pub fn new(max_bytes: usize) -> MemoryCache {
        MemoryCache {
            max_bytes,
            inner: Mutex::new(CacheInner::default()),
        }
    }
}

impl CacheTier for MemoryCache {
    fn get(&self, key: &CacheKey) -> Option<Bytes> {
        let Ok(mut inner) = self.inner.lock() else {
            return None;
        };
        inner.tick += 1;
        let tick = inner.tick;
        let entry = inner.entries.get_mut(key)?;
        entry.last_used = tick;
        Some(entry.value.clone())
    }

    fn insert(&self, key: CacheKey, value: Bytes) {
        if value.len() > self.max_bytes {
            return;
        }
        let Ok(mut inner) = self.inner.lock() else {
            return;
        };
        inner.tick += 1;
        let tick = inner.tick;
        let len = value.len();
        if let Some(old) = inner.entries.insert(
            key,
            Entry {
                value,
                last_used: tick,
            },
        ) {
            inner.bytes -= old.value.len();
        }
        inner.bytes += len;
        while inner.bytes > self.max_bytes {
            let Some(oldest) = inner
                .entries
                .iter()
                .min_by_key(|(_, e)| e.last_used)
                .map(|(k, _)| k.clone())
            else {
                break;
            };
            if let Some(e) = inner.entries.remove(&oldest) {
                inner.bytes -= e.value.len();
            }
        }
    }

    fn invalidate_path(&self, path: &str) {
        let Ok(mut inner) = self.inner.lock() else {
            return;
        };
        let stale: Vec<CacheKey> = inner
            .entries
            .keys()
            .filter(|k| k.path == path)
            .cloned()
            .collect();
        for key in stale {
            if let Some(e) = inner.entries.remove(&key) {
                inner.bytes -= e.value.len();
            }
        }
    }
}

/// Read-through cache wrapper: `get`/`get_range` fill the cache, `put`
/// invalidates its path, `list` always hits the backend (a fresh manifest
/// must be visible immediately).
pub struct CachedStore {
    inner: Arc<dyn ObjectStore>,
    cache: Arc<dyn CacheTier>,
}

impl CachedStore {
    pub fn new(inner: Arc<dyn ObjectStore>, cache: Arc<dyn CacheTier>) -> CachedStore {
        CachedStore { inner, cache }
    }
}

#[async_trait::async_trait]
impl ObjectStore for CachedStore {
    async fn put(&self, key: &str, bytes: Bytes) -> Result<(), StorageError> {
        self.cache.invalidate_path(key);
        self.inner.put(key, bytes).await
    }

    async fn get(&self, key: &str) -> Result<Bytes, StorageError> {
        let cache_key = CacheKey {
            path: key.to_string(),
            range: None,
        };
        if let Some(hit) = self.cache.get(&cache_key) {
            return Ok(hit);
        }
        let value = self.inner.get(key).await?;
        self.cache.insert(cache_key, value.clone());
        Ok(value)
    }

    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Bytes, StorageError> {
        let cache_key = CacheKey {
            path: key.to_string(),
            range: Some((range.start, range.end)),
        };
        if let Some(hit) = self.cache.get(&cache_key) {
            return Ok(hit);
        }
        let value = self.inner.get_range(key, range).await?;
        self.cache.insert(cache_key, value.clone());
        Ok(value)
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
        self.inner.list(prefix).await
    }
}
```

Add to `crates/varve-storage/src/lib.rs`:

```rust
pub mod cache;
pub use cache::{CacheKey, CacheTier, CachedStore, MemoryCache};
```

- [x] **Step 4: Run test to verify it passes**

Run: `cargo test -p varve-storage`
Expected: all pass (8 new cache tests included).

- [x] **Step 5: Commit**

```bash
git add crates/varve-storage/
git commit -m "feat: memory cache tier with read-through cached store"
```

---

### Task 5: Source-agnostic scan — extract `snapshot_entities` from `LiveTable`

**Files:**
- Create: `crates/varve-index/src/scan.rs`
- Modify: `crates/varve-index/src/live.rs` (delegate; add accessors)
- Modify: `crates/varve-index/src/lib.rs` (export)
- Test: in-module `#[cfg(test)]` in `scan.rs`; existing `live.rs` tests pin behavior preservation

**Interfaces:**
- Produces: `varve_index::snapshot_entities`:

```rust
pub fn snapshot_entities<'a, I>(
    entities: I,
    label: &str,
    bounds: &TemporalBounds,
) -> Result<Option<RecordBatch>, IndexError>
where
    I: IntoIterator<Item = (Iid, &'a [Event])>;
```

  Entities MUST arrive in ascending `Iid` order; each slice in arrival (log) order — exactly `resolve`'s documented precondition. Output schema and row order identical to today's `LiveTable::snapshot_for_label`.
- Produces: `LiveTable::entities(&self) -> impl Iterator<Item = (&Iid, &[Event])>` (iid ascending, events in arrival order) and `LiveTable::events_for(&self, iid: &Iid) -> Option<&[Event]>`.
- `LiveTable::snapshot_for_label` delegates to `snapshot_entities` — signature unchanged, all existing tests must stay green untouched.

- [x] **Step 1: Write the failing test**

`crates/varve-index/src/scan.rs` (tests first; module body in Step 3):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{Event, Op};
    use crate::live::LiveTable;
    use varve_types::{Doc, Iid, Instant, TemporalDimension, Value};

    fn iid(n: u8) -> Iid {
        Iid::derive("g", "nodes", &[n])
    }

    fn us(n: i64) -> Instant {
        Instant::from_micros(n)
    }

    fn put(entity: u8, sf: i64, name: &str) -> Event {
        let mut doc = Doc::new();
        doc.insert("name".into(), Value::Str(name.into()));
        Event {
            iid: iid(entity),
            system_from: us(sf),
            valid_from: us(sf),
            valid_to: Instant::END_OF_TIME,
            op: Op::Put {
                labels: vec!["P".into()],
                doc,
            },
        }
    }

    fn now_bounds(n: i64) -> TemporalBounds {
        TemporalBounds {
            valid: TemporalDimension::at(us(n)),
            system: TemporalDimension::at(us(n)),
        }
    }

    /// The extracted function over plain slices must produce the exact batch
    /// the LiveTable produces over the same events.
    #[test]
    fn snapshot_entities_matches_live_table_output() {
        let events = [put(1, 1, "Ada"), put(2, 2, "Bob"), put(1, 3, "Adele")];
        let mut live = LiveTable::new();
        for e in &events {
            live.append(e.clone()).unwrap();
        }
        let via_live = live.snapshot_for_label("P", &now_bounds(10)).unwrap();

        // Manual per-entity grouping in ascending iid order, arrival order kept.
        let mut a = Vec::new();
        let mut b = Vec::new();
        for e in &events {
            if e.iid == iid(1) {
                a.push(e.clone());
            } else {
                b.push(e.clone());
            }
        }
        let mut pairs = vec![(iid(1), a.as_slice()), (iid(2), b.as_slice())];
        pairs.sort_by_key(|(iid, _)| *iid);
        let direct = snapshot_entities(pairs, "P", &now_bounds(10)).unwrap();

        assert_eq!(via_live, direct);
        assert_eq!(direct.unwrap().num_rows(), 2);
    }

    #[test]
    fn empty_input_yields_none() {
        let empty: Vec<(Iid, &[Event])> = Vec::new();
        assert!(snapshot_entities(empty, "P", &now_bounds(10))
            .unwrap()
            .is_none());
    }

    #[test]
    fn live_table_accessors_expose_entities_in_iid_order() {
        let mut live = LiveTable::new();
        let events = [put(2, 1, "Bob"), put(1, 2, "Ada"), put(1, 3, "Adele")];
        for e in &events {
            live.append(e.clone()).unwrap();
        }
        let listed: Vec<(Iid, usize)> = live
            .entities()
            .map(|(iid, events)| (*iid, events.len()))
            .collect();
        let mut expected = vec![(iid(1), 2), (iid(2), 1)];
        expected.sort_by_key(|(iid, _)| *iid);
        assert_eq!(listed, expected);

        let ones = live.events_for(&iid(1)).unwrap();
        assert_eq!(ones.len(), 2);
        assert_eq!(ones[0].system_from, us(2)); // arrival order preserved
        assert_eq!(ones[1].system_from, us(3));
        assert!(live.events_for(&iid(9)).is_none());
    }
}
```

- [x] **Step 2: Run test to verify it fails**

Run: `cargo test -p varve-index scan`
Expected: FAIL — `snapshot_entities` / accessors not defined (add `pub mod scan;` to `lib.rs` first).

- [x] **Step 3: Move the snapshot machinery**

Prepend to `crates/varve-index/src/scan.rs` — this is the body of today's `LiveTable::snapshot_for_label` plus its two private helpers (`VisibleRow`, `value_type`, `timestamp_type`), MOVED from `live.rs` and generalized over an entity iterator:

```rust
//! Source-agnostic bitemporal scan: resolve entities against bounds and
//! build the snapshot RecordBatch. Sources: the live table (slice 2) and
//! persisted block pages merged with it (slice 4). Extracted unchanged from
//! `LiveTable::snapshot_for_label` — the slice-2 seam STATUS.md deferred
//! until a second scan source existed.

use crate::bitemporal::resolve;
use crate::event::{Event, Op};
use crate::live::IndexError;
use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, FixedSizeBinaryBuilder, Float64Builder, Int64Builder,
    StringBuilder, TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use std::collections::BTreeMap;
use std::sync::Arc;
use varve_types::{Doc, Iid, Instant, TemporalBounds, Value};

struct VisibleRow<'a> {
    iid: Iid,
    doc: &'a Doc,
    system_from: Instant,
    system_to: Instant,
    valid_from: Instant,
    valid_to: Instant,
}

/// Maps an observed property value to its Arrow column type. `Value::Null`
/// carries no type information (returns `None`, so it doesn't constrain the
/// column); `Value::Bytes` maps to `Binary`, not `MixedPropertyTypes`.
fn value_type(v: &Value) -> Option<DataType> {
    match v {
        Value::Int(_) => Some(DataType::Int64),
        Value::Float(_) => Some(DataType::Float64),
        Value::Str(_) => Some(DataType::Utf8),
        Value::Bool(_) => Some(DataType::Boolean),
        Value::Bytes(_) => Some(DataType::Binary),
        Value::Null => None,
    }
}

fn timestamp_type() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
}

/// Resolve every entity against `bounds` and snapshot the visible versions
/// carrying `label` into one RecordBatch (`None` when nothing is visible).
/// `entities` must be in ascending `Iid` order with each event slice in
/// arrival (log) order — `resolve`'s precondition. Schema: `_iid`
/// FixedSizeBinary(16), `_system_from`/`_system_to`/`_valid_from`/`_valid_to`
/// Timestamp(µs, "UTC") non-null, then one nullable column per property
/// observed across visible docs.
pub fn snapshot_entities<'a, I>(
    entities: I,
    label: &str,
    bounds: &TemporalBounds,
) -> Result<Option<RecordBatch>, IndexError>
where
    I: IntoIterator<Item = (Iid, &'a [Event])>,
{
    let mut visible: Vec<VisibleRow<'_>> = Vec::new();
    for (iid, events) in entities {
        for version in resolve(events, bounds) {
            let Op::Put { labels, doc } = &version.event.op else {
                continue; // resolve only emits Puts; defensive
            };
            if labels.iter().any(|l| l == label) {
                visible.push(VisibleRow {
                    iid,
                    doc,
                    system_from: version.event.system_from,
                    system_to: version.system_to,
                    valid_from: version.valid_from,
                    valid_to: version.valid_to,
                });
            }
        }
    }
    if visible.is_empty() {
        return Ok(None);
    }

    // Column plan over VISIBLE docs: property name → type of first non-null.
    let mut col_types: BTreeMap<&str, DataType> = BTreeMap::new();
    for row in &visible {
        for (k, v) in row.doc {
            if let Some(dt) = value_type(v) {
                match col_types.get(k.as_str()) {
                    None => {
                        col_types.insert(k, dt);
                    }
                    Some(existing) if *existing == dt => {}
                    Some(_) => {
                        return Err(IndexError::MixedPropertyTypes {
                            property: k.clone(),
                        })
                    }
                }
            }
        }
    }

    let mut fields = vec![Field::new("_iid", DataType::FixedSizeBinary(16), false)];
    let mut iid_b = FixedSizeBinaryBuilder::new(16);
    for row in &visible {
        iid_b.append_value(row.iid.as_bytes())?;
    }
    let mut columns: Vec<ArrayRef> = vec![Arc::new(iid_b.finish())];

    for (name, get) in [
        (
            "_system_from",
            (|r: &VisibleRow<'_>| r.system_from) as fn(&VisibleRow<'_>) -> Instant,
        ),
        ("_system_to", |r| r.system_to),
        ("_valid_from", |r| r.valid_from),
        ("_valid_to", |r| r.valid_to),
    ] {
        fields.push(Field::new(name, timestamp_type(), false));
        let mut b = TimestampMicrosecondBuilder::new().with_timezone("UTC");
        for row in &visible {
            b.append_value(get(row).as_micros());
        }
        columns.push(Arc::new(b.finish()));
    }

    for (name, dt) in &col_types {
        fields.push(Field::new(*name, dt.clone(), true));
        let col: ArrayRef = match dt {
            DataType::Int64 => {
                let mut b = Int64Builder::new();
                for row in &visible {
                    match row.doc.get(*name) {
                        Some(Value::Int(i)) => b.append_value(*i),
                        _ => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            DataType::Float64 => {
                let mut b = Float64Builder::new();
                for row in &visible {
                    match row.doc.get(*name) {
                        Some(Value::Float(f)) => b.append_value(*f),
                        _ => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            DataType::Utf8 => {
                let mut b = StringBuilder::new();
                for row in &visible {
                    match row.doc.get(*name) {
                        Some(Value::Str(s)) => b.append_value(s),
                        _ => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            DataType::Boolean => {
                let mut b = BooleanBuilder::new();
                for row in &visible {
                    match row.doc.get(*name) {
                        Some(Value::Bool(v)) => b.append_value(*v),
                        _ => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            _ => {
                let mut b = BinaryBuilder::new();
                for row in &visible {
                    match row.doc.get(*name) {
                        Some(Value::Bytes(v)) => b.append_value(v),
                        _ => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
        };
        columns.push(col);
    }

    Ok(Some(RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns,
    )?))
}
```

`crates/varve-index/src/live.rs` — DELETE the moved items (`VisibleRow`, `value_type`, `timestamp_type`, the whole body of `snapshot_for_label`, and the now-unused `arrow::array`/`arrow::datatypes` builder imports and `use crate::bitemporal::resolve;`). `IndexError` and `LiveTable` stay. Replace `snapshot_for_label` and add the accessors:

```rust
    /// Resolve all entities against `bounds` and snapshot the visible
    /// versions carrying `label` into one RecordBatch (see
    /// [`crate::scan::snapshot_entities`], which this delegates to).
    pub fn snapshot_for_label(
        &self,
        label: &str,
        bounds: &TemporalBounds,
    ) -> Result<Option<RecordBatch>, IndexError> {
        crate::scan::snapshot_entities(
            self.events.iter().map(|(iid, events)| (*iid, events.as_slice())),
            label,
            bounds,
        )
    }

    /// All entities in ascending `Iid` order, each event slice in arrival
    /// (log) order — the shape `snapshot_entities`, block encoding (Task 6),
    /// and the merged scan (Task 9) consume.
    pub fn entities(&self) -> impl Iterator<Item = (&Iid, &[Event])> {
        self.events.iter().map(|(iid, events)| (iid, events.as_slice()))
    }

    /// One entity's events in arrival order (point-lookup fast path).
    pub fn events_for(&self, iid: &Iid) -> Option<&[Event]> {
        self.events.get(iid).map(Vec::as_slice)
    }
```

(`live.rs` keeps `use arrow::record_batch::RecordBatch;` and `use varve_types::{..., TemporalBounds, ...};` for the delegating signature; its `#[cfg(test)]` module is untouched — those tests are the behavior-preservation proof.)

`crates/varve-index/src/lib.rs`:

```rust
pub mod bitemporal;
pub mod codec;
pub mod event;
pub mod live;
pub mod scan;

pub use bitemporal::{resolve, Ceiling, Polygon, ResolvedVersion};
pub use codec::{decode_events, encode_events};
pub use event::{Event, Op};
pub use live::{IndexError, LiveTable};
pub use scan::snapshot_entities;
```

- [x] **Step 4: Run tests to verify they pass — including the untouched live.rs suite**

Run: `cargo test -p varve-index`
Expected: all pass (3 new scan tests + every pre-existing live/bitemporal/codec test, none modified).

- [x] **Step 5: Commit**

```bash
git add crates/varve-index/
git commit -m "refactor: extract source-agnostic snapshot_entities scan from LiveTable"
```

---
### Task 6: Block format — paged data file, meta page index, prune rules

**Files:**
- Create: `crates/varve-index/src/block.rs`
- Modify: `crates/varve-index/src/codec.rs` (make `downcast` `pub(crate)`; fix the stale "slice 4" comment)
- Modify: `crates/varve-index/src/lib.rs` (export)
- Test: in-module `#[cfg(test)]` in `block.rs`

**Interfaces:**
- Produces: `varve_index::block::{DEFAULT_PAGE_ROWS, PageMeta, EncodedBlock, encode_block, decode_meta}`:

```rust
pub const DEFAULT_PAGE_ROWS: usize = 1024; // XTDB pageLimit

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PageMeta {
    pub offset: u64,               // byte offset of this page in the data file
    pub len: u64,                  // byte length (page = self-contained IPC stream)
    pub rows: u64,
    pub min_iid: Iid,
    pub max_iid: Iid,
    pub min_system_from: Instant,
    pub max_system_from: Instant,
    pub min_valid_from: Instant,
    pub max_valid_from: Instant,
    pub min_valid_to: Instant,
    pub max_valid_to: Instant,
    pub has_erase: bool,
}
impl PageMeta {
    pub fn selected(&self, bounds: &TemporalBounds, iid_point: Option<&Iid>) -> bool;
}

pub struct EncodedBlock { pub data: Vec<u8>, pub meta: Vec<u8>, pub pages: Vec<PageMeta> }

pub fn encode_block(live: &LiveTable, page_rows: usize) -> Result<EncodedBlock, IndexError>;
pub fn decode_meta(bytes: &[u8]) -> Result<Vec<PageMeta>, IndexError>;
// A data page decodes with the existing codec: decode_events(&data[offset..offset+len]).
```

- Consumes: `LiveTable::entities()`, `snapshot_entities` (Task 5), `encode_events`/`decode_events` (slice 3 codec).
- File order: `_iid` ascending, `_system_from` DESCENDING per entity (spec §5.2) via stable reversal of arrival order — same-timestamp ties round-trip exactly (decision 9).

- [x] **Step 1: Write the failing test**

`crates/varve-index/src/block.rs` (tests first; module body in Step 3):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::decode_events;
    use crate::event::{Event, Op};
    use crate::live::LiveTable;
    use crate::scan::snapshot_entities;
    use std::collections::BTreeMap;
    use varve_types::{Doc, Iid, Instant, TemporalBounds, TemporalDimension, Value};

    const EOT: Instant = Instant::END_OF_TIME;

    fn iid(n: u8) -> Iid {
        Iid::derive("g", "nodes", &[n])
    }

    fn us(n: i64) -> Instant {
        Instant::from_micros(n)
    }

    fn put(entity: u8, sf: i64, vf: i64, vt: Instant, seq: i64) -> Event {
        let mut doc = Doc::new();
        doc.insert("seq".into(), Value::Int(seq));
        Event {
            iid: iid(entity),
            system_from: us(sf),
            valid_from: us(vf),
            valid_to: vt,
            op: Op::Put {
                labels: vec!["P".into()],
                doc,
            },
        }
    }

    fn erase(entity: u8, sf: i64) -> Event {
        Event {
            iid: iid(entity),
            system_from: us(sf),
            valid_from: Instant::MIN,
            valid_to: EOT,
            op: Op::Erase,
        }
    }

    fn table(events: &[Event]) -> LiveTable {
        let mut t = LiveTable::new();
        for e in events {
            t.append(e.clone()).unwrap();
        }
        t
    }

    fn at(n: i64) -> TemporalBounds {
        TemporalBounds {
            valid: TemporalDimension::at(us(n)),
            system: TemporalDimension::at(us(n)),
        }
    }

    #[test]
    fn encode_decode_round_trips_pages_and_meta() {
        // 3 entities × 3 events each, page_rows = 2 → 5 pages over 9 rows.
        let mut events = Vec::new();
        for entity in [1u8, 2, 3] {
            for sf in [1i64, 2, 3] {
                events.push(put(entity, sf, sf, EOT, sf));
            }
        }
        let live = table(&events);
        let block = encode_block(&live, 2).unwrap();

        assert_eq!(block.pages.len(), 5);
        assert_eq!(block.pages.iter().map(|p| p.rows).sum::<u64>(), 9);
        // The meta file round-trips the page index exactly.
        assert_eq!(decode_meta(&block.meta).unwrap(), block.pages);

        // Every page is a self-contained IPC stream at its recorded range;
        // file order is (_iid asc, _system_from desc per entity).
        let mut all = Vec::new();
        for page in &block.pages {
            let bytes = &block.data[page.offset as usize..(page.offset + page.len) as usize];
            let page_events = decode_events(bytes).unwrap();
            assert_eq!(page_events.len() as u64, page.rows);
            all.extend(page_events);
        }
        for pair in all.windows(2) {
            assert!(
                pair[0].iid < pair[1].iid
                    || (pair[0].iid == pair[1].iid
                        && pair[0].system_from >= pair[1].system_from),
                "file order violated"
            );
        }

        // Reassembling per entity and reversing restores arrival order.
        let mut per_entity: BTreeMap<Iid, Vec<Event>> = BTreeMap::new();
        for e in all {
            per_entity.entry(e.iid).or_default().push(e);
        }
        for (iid, desc) in per_entity {
            let asc: Vec<Event> = desc.into_iter().rev().collect();
            assert_eq!(asc.as_slice(), live.events_for(&iid).unwrap());
        }
    }

    #[test]
    fn page_meta_stats_are_per_page() {
        let events = [put(1, 5, 3, us(30), 0), put(1, 7, 4, EOT, 1)];
        let block = encode_block(&table(&events), 16).unwrap();
        assert_eq!(block.pages.len(), 1);
        let p = &block.pages[0];
        assert_eq!((p.min_iid, p.max_iid), (iid(1), iid(1)));
        assert_eq!((p.min_system_from, p.max_system_from), (us(5), us(7)));
        assert_eq!((p.min_valid_from, p.max_valid_from), (us(3), us(4)));
        assert_eq!((p.min_valid_to, p.max_valid_to), (us(30), EOT));
        assert!(!p.has_erase);

        let with_erase = encode_block(&table(&[put(1, 5, 3, EOT, 0), erase(1, 6)]), 16).unwrap();
        assert!(with_erase.pages[0].has_erase);
    }

    #[test]
    fn empty_table_encodes_no_pages() {
        let block = encode_block(&LiveTable::new(), 4).unwrap();
        assert!(block.pages.is_empty());
        assert!(block.data.is_empty());
        assert_eq!(decode_meta(&block.meta).unwrap(), vec![]);
    }

    #[test]
    fn iid_point_prunes_only_foreign_pages() {
        let block = encode_block(&table(&[put(1, 1, 1, EOT, 0), put(3, 2, 2, EOT, 0)]), 16).unwrap();
        let page = &block.pages[0];
        assert!(page.selected(&at(10), Some(&iid(1))));
        assert!(page.selected(&at(10), Some(&iid(3))));
        assert!(page.selected(&at(10), None));
        // iid(2) may sort inside or outside [min,max] — pick a definitely-outside probe.
        let all_iids = [iid(1), iid(2), iid(3)];
        let outside = *all_iids.iter().max().unwrap();
        if outside != iid(1) && outside != iid(3) {
            assert!(!page.selected(&at(10), Some(&outside)));
        }
        // Deterministic outside probe: anything beyond max_iid.
        let mut beyond = *page.max_iid.as_bytes();
        if beyond != [0xff; 16] {
            for b in beyond.iter_mut().rev() {
                if *b < 0xff {
                    *b += 1;
                    break;
                }
                *b = 0;
            }
            assert!(!page.selected(&at(10), Some(&Iid::from_bytes(beyond))));
        }
    }

    /// The system-axis prune is EXACTLY output-preserving: resolve() ignores
    /// events at/after the system upper bound even when present, so dropping
    /// a page whose every event is at/after the bound changes nothing.
    #[test]
    fn system_upper_prune_is_output_identical() {
        let old = put(1, 1, 0, EOT, 0);
        let newer = put(1, 20, 0, EOT, 1); // supersedes, but only from system 20
        let bounds = TemporalBounds {
            valid: TemporalDimension::at(us(5)),
            system: TemporalDimension::at(us(10)), // upper = 11 < 20
        };

        // Page containing only `newer` is prunable...
        let newer_block = encode_block(&table(std::slice::from_ref(&newer)), 16).unwrap();
        assert!(!newer_block.pages[0].selected(&bounds, None));
        // ...and the page containing `old` is not.
        let old_block = encode_block(&table(std::slice::from_ref(&old)), 16).unwrap();
        assert!(old_block.pages[0].selected(&bounds, None));

        // Output equivalence: [old] alone == [old, newer] under these bounds.
        let pruned_events = [old.clone()];
        let full_events = [old.clone(), newer.clone()];
        let pruned = snapshot_entities(vec![(iid(1), &pruned_events[..])], "P", &bounds).unwrap();
        let full = snapshot_entities(vec![(iid(1), &full_events[..])], "P", &bounds).unwrap();
        assert_eq!(pruned, full);
        assert!(pruned.is_some());
    }

    /// An erase at system 20 hides history even when querying AS OF system 10
    /// (slice-2 GDPR decision) — its page must survive the system-axis prune.
    #[test]
    fn erase_pages_are_never_pruned_on_the_system_axis() {
        let block = encode_block(&table(&[erase(1, 20)]), 16).unwrap();
        let bounds = TemporalBounds {
            valid: TemporalDimension::at(us(5)),
            system: TemporalDimension::at(us(10)),
        };
        assert!(block.pages[0].min_system_from >= bounds.system.upper);
        assert!(block.pages[0].selected(&bounds, None), "erase page must be scanned");
    }

    /// Regression guard for a subtle bug this plan almost shipped: an event
    /// whose valid range is DISJOINT from the query window still clips the
    /// reported _valid_to of visible rows, so the valid axis must not prune.
    #[test]
    fn valid_axis_is_deliberately_not_pruned() {
        let base = put(1, 1, 0, EOT, 0); // valid [0, ∞) from system 1
        let correction = put(1, 2, 20, us(30), 1); // valid [20, 30) from system 2

        // The correction's page has valid range [20, 30) — disjoint from a
        // query at valid 5 — yet selected() must keep it:
        let block = encode_block(&table(std::slice::from_ref(&correction)), 16).unwrap();
        let bounds = TemporalBounds {
            valid: TemporalDimension::at(us(5)),
            system: TemporalDimension::at(us(10)),
        };
        assert!(block.pages[0].selected(&bounds, None));

        // ...because with it, the visible row's _valid_to is clipped to 20:
        use arrow::array::TimestampMicrosecondArray;
        let events = [base, correction];
        let batch = snapshot_entities(vec![(iid(1), &events[..])], "P", &bounds)
            .unwrap()
            .unwrap();
        let vt: &TimestampMicrosecondArray = batch
            .column_by_name("_valid_to")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert_eq!(vt.value(0), 20, "correction outside the window still clips valid_to");
    }
}
```

- [x] **Step 2: Run test to verify it fails**

Run: `cargo test -p varve-index block`
Expected: FAIL — module/types not defined (add `pub mod block;` to `lib.rs` first).

- [x] **Step 3: Minimal implementation**

`crates/varve-index/src/codec.rs` — change the private helper's visibility (decode_meta reuses it) and fix the stale header comment:

```rust
// before: fn downcast<T: 'static>(...)
pub(crate) fn downcast<T: 'static>(batch: &RecordBatch, index: usize) -> Result<&T, IndexError> {
```

and in the module header comment, replace `columnar doc
structs (dense unions) arrive with slice 4's block format.` with `columnar doc
structs (dense unions) arrive with slice 8's compaction meta (slice-4 plan,
decision 2: blocks reuse this payload codec).`

Prepend to `crates/varve-index/src/block.rs`:

```rust
//! Block format v1 (spec §9, roadmap slice 4). The data file is a
//! concatenation of self-contained per-page Arrow IPC streams (the slice-3
//! event codec, verbatim); the meta file is a single-level page index whose
//! per-page byte ranges make a page read one ranged GET. The full hash-trie
//! meta arrives with slice 8's compaction.

use crate::codec::{downcast, encode_events};
use crate::event::{Event, Op};
use crate::live::{IndexError, LiveTable};
use arrow::array::{
    ArrayRef, BooleanArray, BooleanBuilder, FixedSizeBinaryArray, FixedSizeBinaryBuilder,
    TimestampMicrosecondArray, TimestampMicrosecondBuilder, UInt64Array, UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use varve_types::{Iid, Instant, TemporalBounds};

/// Rows per page (XTDB `pageLimit`). A parameter on `encode_block` so tests
/// can force page splits; the engine passes this constant.
pub const DEFAULT_PAGE_ROWS: usize = 1024;

/// One page's entry in the meta file: byte range in the data file plus the
/// stats the scan prunes by.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PageMeta {
    pub offset: u64,
    pub len: u64,
    pub rows: u64,
    pub min_iid: Iid,
    pub max_iid: Iid,
    pub min_system_from: Instant,
    pub max_system_from: Instant,
    pub min_valid_from: Instant,
    pub max_valid_from: Instant,
    pub min_valid_to: Instant,
    pub max_valid_to: Instant,
    pub has_erase: bool,
}

impl PageMeta {
    /// Should the scan read this page? Prune rules (slice-4 plan, decision 4):
    /// - IID point outside `[min_iid, max_iid]` → skip: resolution is
    ///   per-entity, other entities' pages are irrelevant.
    /// - Every event at/after `bounds.system.upper` → skip: `resolve()`
    ///   ignores such events BEFORE they touch the ceiling, so dropping the
    ///   page is exactly output-preserving — UNLESS the page holds an
    ///   `Erase`, which hides history at every system time (slice-2 GDPR
    ///   decision) and must always be scanned.
    /// - The valid axis deliberately does NOT prune: an event valid-disjoint
    ///   from the query window still clips the reported `_valid_from`/
    ///   `_valid_to` of visible rectangles inside it (`valid_to(x)`
    ///   introspection). Valid stats are recorded for slice 8.
    pub fn selected(&self, bounds: &TemporalBounds, iid_point: Option<&Iid>) -> bool {
        if let Some(iid) = iid_point {
            if *iid < self.min_iid || *iid > self.max_iid {
                return false;
            }
        }
        if self.min_system_from >= bounds.system.upper && !self.has_erase {
            return false;
        }
        true
    }
}

pub struct EncodedBlock {
    pub data: Vec<u8>,
    pub meta: Vec<u8>,
    pub pages: Vec<PageMeta>,
}

/// Serializes the live table into one L0 block: rows in `(_iid asc,
/// _system_from desc)` file order (spec §5.2) chunked into pages of
/// `page_rows`. Pure function of the table (determinism constraint):
/// BTreeMap iteration + stable per-entity reversal, no clocks, no maps
/// with random order.
pub fn encode_block(live: &LiveTable, page_rows: usize) -> Result<EncodedBlock, IndexError> {
    let mut rows: Vec<Event> = Vec::with_capacity(live.event_count());
    for (_iid, events) in live.entities() {
        // Stable reversal: arrival order → system_from desc, ties reversed
        // exactly; the scan's reversal restores arrival order (decision 9).
        rows.extend(events.iter().rev().cloned());
    }
    let mut data = Vec::new();
    let mut pages = Vec::new();
    for chunk in rows.chunks(page_rows.max(1)) {
        let offset = data.len() as u64;
        let bytes = encode_events(chunk)?;
        data.extend_from_slice(&bytes);
        pages.push(page_meta(chunk, offset, bytes.len() as u64));
    }
    let meta = encode_meta(&pages)?;
    Ok(EncodedBlock { data, meta, pages })
}

/// Stats over one page's events. `chunks()` never yields an empty slice.
fn page_meta(events: &[Event], offset: u64, len: u64) -> PageMeta {
    let first = &events[0];
    let mut meta = PageMeta {
        offset,
        len,
        rows: events.len() as u64,
        min_iid: first.iid,
        max_iid: first.iid,
        min_system_from: first.system_from,
        max_system_from: first.system_from,
        min_valid_from: first.valid_from,
        max_valid_from: first.valid_from,
        min_valid_to: first.valid_to,
        max_valid_to: first.valid_to,
        has_erase: false,
    };
    for e in events {
        meta.min_iid = meta.min_iid.min(e.iid);
        meta.max_iid = meta.max_iid.max(e.iid);
        meta.min_system_from = meta.min_system_from.min(e.system_from);
        meta.max_system_from = meta.max_system_from.max(e.system_from);
        meta.min_valid_from = meta.min_valid_from.min(e.valid_from);
        meta.max_valid_from = meta.max_valid_from.max(e.valid_from);
        meta.min_valid_to = meta.min_valid_to.min(e.valid_to);
        meta.max_valid_to = meta.max_valid_to.max(e.valid_to);
        meta.has_erase |= matches!(e.op, Op::Erase);
    }
    meta
}

fn meta_schema() -> Arc<Schema> {
    let ts = || DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    Arc::new(Schema::new(vec![
        Field::new("offset", DataType::UInt64, false),
        Field::new("len", DataType::UInt64, false),
        Field::new("rows", DataType::UInt64, false),
        Field::new("min_iid", DataType::FixedSizeBinary(16), false),
        Field::new("max_iid", DataType::FixedSizeBinary(16), false),
        Field::new("min_system_from", ts(), false),
        Field::new("max_system_from", ts(), false),
        Field::new("min_valid_from", ts(), false),
        Field::new("max_valid_from", ts(), false),
        Field::new("min_valid_to", ts(), false),
        Field::new("max_valid_to", ts(), false),
        Field::new("has_erase", DataType::Boolean, false),
    ]))
}

fn encode_meta(pages: &[PageMeta]) -> Result<Vec<u8>, IndexError> {
    let schema = meta_schema();
    let mut buf = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buf, &schema)?;
    if !pages.is_empty() {
        let mut offset_b = UInt64Builder::new();
        let mut len_b = UInt64Builder::new();
        let mut rows_b = UInt64Builder::new();
        let mut min_iid_b = FixedSizeBinaryBuilder::new(16);
        let mut max_iid_b = FixedSizeBinaryBuilder::new(16);
        let ts_builder = || TimestampMicrosecondBuilder::new().with_timezone("UTC");
        let mut min_sf_b = ts_builder();
        let mut max_sf_b = ts_builder();
        let mut min_vf_b = ts_builder();
        let mut max_vf_b = ts_builder();
        let mut min_vt_b = ts_builder();
        let mut max_vt_b = ts_builder();
        let mut erase_b = BooleanBuilder::new();
        for p in pages {
            offset_b.append_value(p.offset);
            len_b.append_value(p.len);
            rows_b.append_value(p.rows);
            min_iid_b.append_value(p.min_iid.as_bytes())?;
            max_iid_b.append_value(p.max_iid.as_bytes())?;
            min_sf_b.append_value(p.min_system_from.as_micros());
            max_sf_b.append_value(p.max_system_from.as_micros());
            min_vf_b.append_value(p.min_valid_from.as_micros());
            max_vf_b.append_value(p.max_valid_from.as_micros());
            min_vt_b.append_value(p.min_valid_to.as_micros());
            max_vt_b.append_value(p.max_valid_to.as_micros());
            erase_b.append_value(p.has_erase);
        }
        let columns: Vec<ArrayRef> = vec![
            Arc::new(offset_b.finish()),
            Arc::new(len_b.finish()),
            Arc::new(rows_b.finish()),
            Arc::new(min_iid_b.finish()),
            Arc::new(max_iid_b.finish()),
            Arc::new(min_sf_b.finish()),
            Arc::new(max_sf_b.finish()),
            Arc::new(min_vf_b.finish()),
            Arc::new(max_vf_b.finish()),
            Arc::new(min_vt_b.finish()),
            Arc::new(max_vt_b.finish()),
            Arc::new(erase_b.finish()),
        ];
        writer.write(&RecordBatch::try_new(schema.clone(), columns)?)?;
    }
    writer.finish()?;
    drop(writer);
    Ok(buf)
}

/// Deserializes a meta file written by `encode_block`.
pub fn decode_meta(bytes: &[u8]) -> Result<Vec<PageMeta>, IndexError> {
    let reader = StreamReader::try_new(std::io::Cursor::new(bytes), None)?;
    if reader.schema() != meta_schema() {
        return Err(IndexError::Codec("meta file schema mismatch".into()));
    }
    let mut pages = Vec::new();
    for batch in reader {
        let batch = batch?;
        let offset = downcast::<UInt64Array>(&batch, 0)?;
        let len = downcast::<UInt64Array>(&batch, 1)?;
        let rows = downcast::<UInt64Array>(&batch, 2)?;
        let min_iid = downcast::<FixedSizeBinaryArray>(&batch, 3)?;
        let max_iid = downcast::<FixedSizeBinaryArray>(&batch, 4)?;
        let min_sf = downcast::<TimestampMicrosecondArray>(&batch, 5)?;
        let max_sf = downcast::<TimestampMicrosecondArray>(&batch, 6)?;
        let min_vf = downcast::<TimestampMicrosecondArray>(&batch, 7)?;
        let max_vf = downcast::<TimestampMicrosecondArray>(&batch, 8)?;
        let min_vt = downcast::<TimestampMicrosecondArray>(&batch, 9)?;
        let max_vt = downcast::<TimestampMicrosecondArray>(&batch, 10)?;
        let has_erase = downcast::<BooleanArray>(&batch, 11)?;
        for row in 0..batch.num_rows() {
            let iid_at = |arr: &FixedSizeBinaryArray, i: usize| -> Result<Iid, IndexError> {
                let bytes: [u8; 16] = arr
                    .value(i)
                    .try_into()
                    .map_err(|_| IndexError::Codec("meta iid is not 16 bytes".into()))?;
                Ok(Iid::from_bytes(bytes))
            };
            pages.push(PageMeta {
                offset: offset.value(row),
                len: len.value(row),
                rows: rows.value(row),
                min_iid: iid_at(min_iid, row)?,
                max_iid: iid_at(max_iid, row)?,
                min_system_from: Instant::from_micros(min_sf.value(row)),
                max_system_from: Instant::from_micros(max_sf.value(row)),
                min_valid_from: Instant::from_micros(min_vf.value(row)),
                max_valid_from: Instant::from_micros(max_vf.value(row)),
                min_valid_to: Instant::from_micros(min_vt.value(row)),
                max_valid_to: Instant::from_micros(max_vt.value(row)),
                has_erase: has_erase.value(row),
            });
        }
    }
    Ok(pages)
}
```

(`Iid` derives `Ord` + `Copy` — `min`/`max` and `PageMeta: Copy` work. `use arrow::array::Array;` may be needed for `.value` on some arrays; add if the compiler asks — tests are the contract.)

Add to `crates/varve-index/src/lib.rs`:

```rust
pub mod block;
pub use block::{decode_meta, encode_block, EncodedBlock, PageMeta, DEFAULT_PAGE_ROWS};
```

- [x] **Step 4: Run test to verify it passes**

Run: `cargo test -p varve-index`
Expected: all pass (8 new block tests; existing suites untouched).

- [x] **Step 5: Commit**

```bash
git add crates/varve-index/
git commit -m "feat: L0 block format with paged data, meta page index, prune rules"
```

---

### Task 7: `Log::trim` — physical log truncation below the watermark

**Files:**
- Modify: `crates/varve-log/src/log.rs` (trait method)
- Modify: `crates/varve-log/src/memory.rs` (next-position field + trim)
- Modify: `crates/varve-log/src/local.rs` (whole-segment trim)
- Modify: `crates/varve-engine/src/writer.rs` (test doubles `CountingLog`/`FailOnceLog` gain delegating `trim`)
- Test: `crates/varve-log/tests/trim.rs`

**Interfaces:**
- Produces: `Log::trim(&self, up_to: LogPosition) -> Result<(), LogError>` (async, REQUIRED method):
  discards records with `position < up_to` where cheap in whole durability units; records at/after `up_to` are never removed, earlier ones MAY be retained; positions are never reused after a trim.
- `MemoryLog` keeps an explicit next-position counter so appends after a full trim continue the sequence.
- `LocalLog` deletes segment `i` iff segment `i+1` exists and starts at or below `up_to` (the active segment is never deleted; no mid-segment truncation).

- [x] **Step 1: Write the failing test**

`crates/varve-log/tests/trim.rs`:

```rust
#![allow(clippy::unwrap_used)]
use varve_log::{LocalLog, Log, LogRecord, MemoryLog};
use varve_types::LogPosition;

fn record(tx_id: u64) -> LogRecord {
    LogRecord {
        tx_id,
        system_time_us: tx_id as i64,
        user: String::new(),
        effects: vec![],
    }
}

fn positions(records: &[(LogPosition, LogRecord)]) -> Vec<u64> {
    records.iter().map(|(p, _)| p.as_u64()).collect()
}

fn segment_count(dir: &std::path::Path) -> usize {
    std::fs::read_dir(dir)
        .unwrap()
        .filter(|e| {
            e.as_ref()
                .unwrap()
                .path()
                .extension()
                .is_some_and(|x| x == "vseg")
        })
        .count()
}

#[tokio::test]
async fn memory_trim_drops_below_and_positions_never_regress() {
    let log = MemoryLog::new();
    log.append(vec![record(1), record(2)]).await.unwrap(); // positions 0, 1
    log.append(vec![record(3)]).await.unwrap(); // position 2
    log.trim(LogPosition::from_u64(2)).await.unwrap();
    let rest = log.tail(LogPosition::ZERO).await.unwrap();
    assert_eq!(positions(&rest), vec![2]);
    assert_eq!(rest[0].1.tx_id, 3);

    // Trimming EVERYTHING must not reset the position sequence.
    log.trim(LogPosition::from_u64(3)).await.unwrap();
    assert!(log.tail(LogPosition::ZERO).await.unwrap().is_empty());
    let first = log.append(vec![record(4)]).await.unwrap();
    assert_eq!(first.as_u64(), 3);
}

#[tokio::test]
async fn local_trim_deletes_only_whole_covered_segments() {
    let dir = tempfile::tempdir().unwrap();
    // 1-byte budget: every append rolls a fresh segment first.
    let log = LocalLog::open(dir.path(), 1).unwrap();
    log.append(vec![record(1)]).await.unwrap(); // segment @0
    log.append(vec![record(2)]).await.unwrap(); // segment @1
    log.append(vec![record(3)]).await.unwrap(); // segment @2 (active)
    assert_eq!(segment_count(dir.path()), 3);

    log.trim(LogPosition::from_u64(2)).await.unwrap();
    assert_eq!(segment_count(dir.path()), 1); // only the active segment left
    assert_eq!(positions(&log.tail(LogPosition::ZERO).await.unwrap()), vec![2]);
}

#[tokio::test]
async fn local_trim_never_touches_the_active_segment() {
    let dir = tempfile::tempdir().unwrap();
    // Huge budget: everything lands in ONE segment — nothing to delete even
    // with the watermark past every record (whole-unit-only rule).
    let log = LocalLog::open(dir.path(), 64 * 1024 * 1024).unwrap();
    log.append(vec![record(1), record(2)]).await.unwrap();
    log.trim(LogPosition::from_u64(2)).await.unwrap();
    assert_eq!(segment_count(dir.path()), 1);
    // Retaining below-watermark records is allowed; replay filters by position.
    assert_eq!(positions(&log.tail(LogPosition::from_u64(2)).await.unwrap()), Vec::<u64>::new());
    assert_eq!(positions(&log.tail(LogPosition::ZERO).await.unwrap()), vec![0, 1]);
}

#[tokio::test]
async fn local_log_reopens_after_trim_with_positions_intact() {
    let dir = tempfile::tempdir().unwrap();
    {
        let log = LocalLog::open(dir.path(), 1).unwrap();
        log.append(vec![record(1)]).await.unwrap();
        log.append(vec![record(2)]).await.unwrap();
        log.append(vec![record(3)]).await.unwrap();
        log.trim(LogPosition::from_u64(2)).await.unwrap();
    }
    // First remaining segment starts at position 2, not 0 — open() already
    // accepts an arbitrary starting position; this pins it.
    let log = LocalLog::open(dir.path(), 1).unwrap();
    let first = log.append(vec![record(4)]).await.unwrap();
    assert_eq!(first.as_u64(), 3);
    assert_eq!(positions(&log.tail(LogPosition::ZERO).await.unwrap()), vec![2, 3]);
}

#[tokio::test]
async fn trim_at_zero_is_a_no_op() {
    let log = MemoryLog::new();
    log.append(vec![record(1)]).await.unwrap();
    log.trim(LogPosition::ZERO).await.unwrap();
    assert_eq!(positions(&log.tail(LogPosition::ZERO).await.unwrap()), vec![0]);
}
```

- [x] **Step 2: Run test to verify it fails**

Run: `cargo test -p varve-log --test trim`
Expected: FAIL — no method `trim` on the trait.

- [x] **Step 3: Minimal implementation**

`crates/varve-log/src/log.rs` — add to the `Log` trait (after `read_range`, before `tail`):

```rust
    /// Physically discards records with `position < up_to` where that is
    /// cheap in whole durability units (whole segments / whole objects).
    /// Records at or after `up_to` are NEVER removed; earlier ones MAY be
    /// retained. Positions are never reused after a trim. Called by the
    /// writer once a block manifest commits (spec §9: the manifest trims
    /// the log-replay watermark).
    async fn trim(&self, up_to: LogPosition) -> Result<(), LogError>;
```

`crates/varve-log/src/memory.rs` — replace the whole file body (the record store gains an explicit next-position; the factory is unchanged):

```rust
use crate::log::{Log, LogError};
use crate::record::LogRecord;
use std::sync::{Arc, Mutex};
use varve_config::{ComponentFactory, ConfigSection, RegistryError};
use varve_types::LogPosition;

struct Inner {
    records: Vec<(LogPosition, LogRecord)>,
    /// Position the next appended record will receive. Explicit (not derived
    /// from `records.last()`) so a trim never resets the sequence.
    next: LogPosition,
}

/// Volatile in-process log (spec §6). Records live only for the process
/// lifetime — restart loses everything. Useful for tests and non-durable
/// deployments.
pub struct MemoryLog {
    inner: Mutex<Inner>,
}

impl Default for MemoryLog {
    fn default() -> Self {
        MemoryLog {
            inner: Mutex::new(Inner {
                records: Vec::new(),
                next: LogPosition::ZERO,
            }),
        }
    }
}

impl MemoryLog {
    pub fn new() -> MemoryLog {
        MemoryLog::default()
    }
}

#[async_trait::async_trait]
impl Log for MemoryLog {
    async fn append(&self, records: Vec<LogRecord>) -> Result<LogPosition, LogError> {
        if records.is_empty() {
            return Err(LogError::EmptyAppend);
        }
        let mut inner = self.inner.lock().map_err(|_| LogError::Poisoned)?;
        let first = inner.next;
        // Pre-compute positions so an overflow fails before any mutation.
        let after_batch = first.advance(records.len() as u64)?;
        let mut positioned = Vec::with_capacity(records.len());
        for (i, record) in records.into_iter().enumerate() {
            positioned.push((first.advance(i as u64)?, record));
        }
        inner.records.extend(positioned);
        inner.next = after_batch;
        Ok(first)
    }

    async fn read_range(
        &self,
        from: LogPosition,
        to: LogPosition,
    ) -> Result<Vec<(LogPosition, LogRecord)>, LogError> {
        let inner = self.inner.lock().map_err(|_| LogError::Poisoned)?;
        Ok(inner
            .records
            .iter()
            .filter(|(p, _)| *p >= from && *p < to)
            .cloned()
            .collect())
    }

    async fn trim(&self, up_to: LogPosition) -> Result<(), LogError> {
        let mut inner = self.inner.lock().map_err(|_| LogError::Poisoned)?;
        inner.records.retain(|(p, _)| *p >= up_to);
        Ok(())
    }
}

/// Registry factory: `[log] backend = "memory"`.
pub struct MemoryLogFactory;

impl ComponentFactory<dyn Log> for MemoryLogFactory {
    fn name(&self) -> &'static str {
        "memory"
    }

    fn build(&self, _cfg: &ConfigSection) -> Result<Arc<dyn Log>, RegistryError> {
        Ok(Arc::new(MemoryLog::new()))
    }
}
```

`crates/varve-log/src/local.rs` — add a sync helper next to `read_range_sync` and the trait method inside `impl Log for LocalLog`:

```rust
/// Whole-segment trim: a segment is deletable iff the NEXT segment exists
/// and starts at or below `up_to` (every record in it is then < up_to). The
/// active (last) segment is never deleted, so `next`/`segment_len` stay
/// valid and positions never regress. No mid-segment truncation.
fn trim_sync(inner: &Inner, up_to: LogPosition) -> Result<(), LogError> {
    if inner.poisoned {
        return Err(LogError::Poisoned);
    }
    let segments = list_segments(&inner.dir)?;
    let mut removed = false;
    for pair in segments.windows(2) {
        let (_, path) = &pair[0];
        let (next_first, _) = &pair[1];
        if *next_first <= up_to.as_u64() {
            fs::remove_file(path)?;
            removed = true;
        }
    }
    if removed {
        fsync_dir(&inner.dir)?;
    }
    Ok(())
}
```

```rust
    async fn trim(&self, up_to: LogPosition) -> Result<(), LogError> {
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || {
            let guard = inner.lock().map_err(|_| LogError::Poisoned)?;
            trim_sync(&guard, up_to)
        })
        .await
        .map_err(|e| LogError::Io(std::io::Error::other(e)))?
    }
```

`crates/varve-engine/src/writer.rs` — the two test doubles gain delegating impls (inside their existing `impl Log for …` blocks):

```rust
        async fn trim(&self, up_to: LogPosition) -> Result<(), LogError> {
            self.inner.trim(up_to).await
        }
```

- [x] **Step 4: Run tests to verify they pass**

Run: `cargo test -p varve-log && cargo test -p varve-engine && cargo test --workspace`
Expected: 6 new trim tests pass; every existing log/recovery/crash test still green (the memory-log rewrite is behavior-identical for append/read).

- [x] **Step 5: Commit**

```bash
git add crates/varve-log/ crates/varve-engine/
git commit -m "feat: Log::trim with whole-segment deletion and stable positions"
```

---
### Task 8: Plan helpers — public `effective_bounds` + IID point extraction

**Files:**
- Modify: `crates/varve-plan/src/exec.rs`
- Modify: `crates/varve-plan/src/lib.rs` (export)
- Test: `crates/varve-plan/tests/exec_test.rs` (append)

**Interfaces:**
- Produces: `varve_plan::effective_bounds(stmt: &QueryStmt, now: Instant) -> TemporalBounds` — today's private function made `pub`, body unchanged.
- Produces: `varve_plan::iid_point(where_clause: &Option<Expr>, graph: &str, table: &str) -> Option<Iid>` — `Some(Iid)` iff the WHERE clause is `v._id = <literal>` with an id-able literal (Int/Str/Bool/Bytes-representable; Float/Null → `None`). Spec §10: "IID point/range predicates (from `_id` equality)".

- [x] **Step 1: Write the failing test**

Append to `crates/varve-plan/tests/exec_test.rs`:

```rust
mod pushdown {
    use varve_gql::ast::{Expr, Literal, Statement};
    use varve_plan::{effective_bounds, iid_point};
    use varve_types::{Iid, Instant, TemporalDimension, Value};

    fn query(gql: &str) -> varve_gql::ast::QueryStmt {
        let Statement::Query(q) = varve_gql::parse(gql).unwrap() else {
            panic!("not a query");
        };
        *q
    }

    #[test]
    fn effective_bounds_default_to_now_on_both_axes() {
        let q = query("MATCH (p:P) RETURN p.x");
        let now = Instant::from_micros(1000);
        let b = effective_bounds(&q, now);
        assert_eq!(b.valid, TemporalDimension::at(now));
        assert_eq!(b.system, TemporalDimension::at(now));
    }

    #[test]
    fn effective_bounds_honor_query_level_clauses() {
        let q = query(
            "FOR VALID_TIME AS OF TIMESTAMP '2020-01-01T00:00:00Z' MATCH (p:P) RETURN p.x",
        );
        let now = Instant::from_micros(2_000_000_000_000_000);
        let b = effective_bounds(&q, now);
        let t2020 = Instant::parse_rfc3339("2020-01-01T00:00:00Z").unwrap();
        assert_eq!(b.valid, TemporalDimension::at(t2020));
        assert_eq!(b.system, TemporalDimension::at(now)); // unstated axis defaults
    }

    fn id_eq(prop: &str, value: Literal) -> Option<Expr> {
        Some(Expr::PropEq {
            var: "p".into(),
            prop: prop.into(),
            value,
        })
    }

    #[test]
    fn iid_point_from_id_equality() {
        let expected = Iid::derive(
            "default",
            "nodes",
            &Value::Int(42).id_bytes().unwrap(),
        );
        assert_eq!(
            iid_point(&id_eq("_id", Literal::Int(42)), "default", "nodes"),
            Some(expected)
        );
    }

    #[test]
    fn iid_point_distinguishes_literal_types() {
        // Int(49) and Str("1") collide as raw bytes; id_bytes type tags differ.
        let a = iid_point(&id_eq("_id", Literal::Int(0x31)), "default", "nodes");
        let b = iid_point(&id_eq("_id", Literal::Str("1".into())), "default", "nodes");
        assert!(a.is_some() && b.is_some());
        assert_ne!(a, b);
    }

    #[test]
    fn iid_point_falls_back_to_none() {
        // non-_id property
        assert_eq!(iid_point(&id_eq("name", Literal::Int(1)), "default", "nodes"), None);
        // literals that cannot be ids (Value::id_bytes errors)
        assert_eq!(iid_point(&id_eq("_id", Literal::Float(2.5)), "default", "nodes"), None);
        assert_eq!(iid_point(&id_eq("_id", Literal::Null), "default", "nodes"), None);
        // no WHERE at all
        assert_eq!(iid_point(&None, "default", "nodes"), None);
    }
}
```

- [x] **Step 2: Run test to verify it fails**

Run: `cargo test -p varve-plan --test exec_test pushdown`
Expected: FAIL — `effective_bounds`/`iid_point` not exported.

- [x] **Step 3: Minimal implementation**

`crates/varve-plan/src/exec.rs`:
- change `fn effective_bounds(` to `pub fn effective_bounds(` (rustdoc already present).
- add after `to_df_literal`:

```rust
fn literal_value(l: &Literal) -> varve_types::Value {
    use varve_types::Value;
    match l {
        Literal::Int(i) => Value::Int(*i),
        Literal::Float(f) => Value::Float(*f),
        Literal::Str(s) => Value::Str(s.clone()),
        Literal::Bool(b) => Value::Bool(*b),
        Literal::Null => Value::Null,
    }
}

/// IID point pushdown (spec §10): `WHERE v._id = <literal>` pins the scan to
/// exactly one entity, letting it prune persisted pages by IID range and
/// read a single live entity. `None` when the filter isn't an `_id`
/// equality or the literal can't be an id (Float/Null) — the scan stays
/// unpruned and DataFusion applies the WHERE afterwards either way, so this
/// is purely an access-path optimization, never a semantics change.
pub fn iid_point(where_clause: &Option<Expr>, graph: &str, table: &str) -> Option<Iid> {
    let Some(Expr::PropEq { prop, value, .. }) = where_clause else {
        return None;
    };
    if prop != "_id" {
        return None;
    }
    let bytes = literal_value(value).id_bytes().ok()?;
    Some(Iid::derive(graph, table, &bytes))
}
```

`crates/varve-plan/src/lib.rs`:

```rust
pub mod exec;

pub use exec::{
    effective_bounds, execute_query, iid_point, iids_from_snapshot, matching_iids,
    matching_snapshot, run_query, snapshot_for_query, PlanError,
};
```

- [x] **Step 4: Run test to verify it passes**

Run: `cargo test -p varve-plan`
Expected: all pass (6 new pushdown tests).

- [x] **Step 5: Commit**

```bash
git add crates/varve-plan/
git commit -m "feat: expose effective_bounds and _id-equality IID point pushdown"
```

---

### Task 9: Engine merged scan — `TableState` under one lock, query + DELETE through live∪persisted

**Files:**
- Create: `crates/varve-engine/src/state.rs`
- Create: `crates/varve-engine/src/scan.rs`
- Modify: `crates/varve-engine/Cargo.toml` (add `varve-storage`, `bytes`)
- Modify: `crates/varve-engine/src/lib.rs` (register modules)
- Modify: `crates/varve-engine/src/db.rs` (Db holds `TableState` + store; query path)
- Modify: `crates/varve-engine/src/writer.rs` (`WriterState` fields; DELETE via merged scan; `NODES_TABLE` moves out)
- Test: in-module `#[cfg(test)]` in `crates/varve-engine/src/scan.rs` (pub(crate) types need unit-test access)

**Interfaces:**
- Produces (crate-internal): `varve_engine::state::{DEFAULT_GRAPH, NODES_TABLE, PersistedTrie, TableState}`:

```rust
pub(crate) const DEFAULT_GRAPH: &str = "default";
pub(crate) const NODES_TABLE: &str = "nodes"; // moved here from writer.rs

#[derive(Clone)]
pub(crate) struct PersistedTrie {
    pub entry: varve_storage::TrieEntry,
    pub pages: std::sync::Arc<Vec<varve_index::block::PageMeta>>,
}

pub(crate) struct TableState {
    pub live: varve_index::LiveTable,
    pub tries: Vec<PersistedTrie>, // ascending block order == time order
}
impl TableState { pub fn new() -> TableState }
```

- Produces (crate-internal): `varve_engine::scan::merged_snapshot`:

```rust
pub(crate) async fn merged_snapshot(
    state: &Arc<RwLock<TableState>>,
    store: &Arc<dyn varve_storage::ObjectStore>,
    label: &str,
    bounds: &TemporalBounds,
    iid_point: Option<Iid>,
) -> Result<Option<RecordBatch>, EngineError>;
```

- Changes: `Db { state: Arc<RwLock<TableState>>, store: Arc<dyn ObjectStore>, clock, submit }`; `WriterState { state: Arc<RwLock<TableState>>, store: Arc<dyn ObjectStore>, clock, log, next_tx_id }`; `EngineError::Storage(#[from] varve_storage::StorageError)`. `Db::query` and the writer's `resolve_delete` both go through `merged_snapshot`.
- Consumes: `snapshot_entities`, `encode_block`/`PageMeta` (Tasks 5–6), `keys`/`ObjectStore`/`CachedStore`/`MemoryCache`/`TrieEntry` (Tasks 1–4), `effective_bounds`/`iid_point` (Task 8).
- Interim wiring (replaced by Task 11): `Db::open_with`/`Db::local` construct a memory store — harmless because nothing writes to storage until Task 10's flush, and Task 11 lands config selection + recovery together.

- [x] **Step 1: Write the failing test**

`crates/varve-engine/src/scan.rs` (tests first; module body in Step 3):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{PersistedTrie, TableState, DEFAULT_GRAPH, NODES_TABLE};
    use std::sync::{Arc, RwLock};
    use varve_index::block::encode_block;
    use varve_index::{Event, LiveTable, Op};
    use varve_storage::{keys, memory_store, ObjectStore, TrieEntry};
    use varve_types::{Doc, Iid, Instant, TemporalBounds, TemporalDimension, Value};

    const EOT: Instant = Instant::END_OF_TIME;

    fn iid(n: u8) -> Iid {
        Iid::derive(DEFAULT_GRAPH, NODES_TABLE, &[n])
    }

    fn us(n: i64) -> Instant {
        Instant::from_micros(n)
    }

    fn put(entity: u8, sf: i64, name: &str) -> Event {
        let mut doc = Doc::new();
        doc.insert("name".into(), Value::Str(name.into()));
        Event {
            iid: iid(entity),
            system_from: us(sf),
            valid_from: us(sf),
            valid_to: EOT,
            op: Op::Put {
                labels: vec!["P".into()],
                doc,
            },
        }
    }

    fn at(n: i64) -> TemporalBounds {
        TemporalBounds {
            valid: TemporalDimension::at(us(n)),
            system: TemporalDimension::at(us(n)),
        }
    }

    /// Flushes `persisted` into block 0 on a memory store and stages
    /// `live_events` in the live table — the exact state a real flush
    /// produces (Task 10 automates this path).
    async fn seeded(
        persisted: &[Event],
        live_events: &[Event],
    ) -> (Arc<RwLock<TableState>>, Arc<dyn ObjectStore>) {
        let store = memory_store();
        let mut state = TableState::new();
        if !persisted.is_empty() {
            let mut table = LiveTable::new();
            for e in persisted {
                table.append(e.clone()).unwrap();
            }
            let block = encode_block(&table, 2).unwrap(); // small pages → splits
            let trie_key = keys::l0_trie_key(0);
            let row_count = block.pages.iter().map(|p| p.rows).sum();
            let data_len = block.data.len() as u64;
            store
                .put(
                    &keys::data_key(DEFAULT_GRAPH, NODES_TABLE, &trie_key),
                    block.data.into(),
                )
                .await
                .unwrap();
            store
                .put(
                    &keys::meta_key(DEFAULT_GRAPH, NODES_TABLE, &trie_key),
                    block.meta.into(),
                )
                .await
                .unwrap();
            state.tries.push(PersistedTrie {
                entry: TrieEntry {
                    trie_key,
                    row_count,
                    data_len,
                },
                pages: Arc::new(block.pages),
            });
        }
        for e in live_events {
            state.live.append(e.clone()).unwrap();
        }
        (Arc::new(RwLock::new(state)), store)
    }

    fn names(batch: &Option<datafusion::arrow::record_batch::RecordBatch>) -> Vec<String> {
        use datafusion::arrow::array::StringArray;
        let Some(batch) = batch else {
            return vec![];
        };
        let col: &StringArray = batch
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        let mut out: Vec<String> = (0..col.len()).map(|i| col.value(i).to_string()).collect();
        out.sort();
        out
    }

    #[tokio::test]
    async fn persisted_only_events_are_visible() {
        let (state, store) = seeded(&[put(1, 1, "Ada"), put(2, 2, "Bob")], &[]).await;
        let batch = merged_snapshot(&state, &store, "P", &at(10), None)
            .await
            .unwrap();
        assert_eq!(names(&batch), vec!["Ada", "Bob"]);
    }

    #[tokio::test]
    async fn live_put_supersedes_persisted_version() {
        // Cross-source resolution: the persisted "Ada" must get system_to = 5.
        let (state, store) = seeded(&[put(1, 1, "Ada")], &[put(1, 5, "Adele")]).await;
        let now = merged_snapshot(&state, &store, "P", &at(10), None)
            .await
            .unwrap();
        assert_eq!(names(&now), vec!["Adele"]);
        // Time travel to before the live correction still sees the old version.
        let before = merged_snapshot(&state, &store, "P", &at(3), None)
            .await
            .unwrap();
        assert_eq!(names(&before), vec!["Ada"]);
    }

    #[tokio::test]
    async fn live_delete_hides_persisted_put() {
        let delete = Event {
            iid: iid(1),
            system_from: us(5),
            valid_from: us(5),
            valid_to: EOT,
            op: Op::Delete,
        };
        let (state, store) = seeded(&[put(1, 1, "Ada")], std::slice::from_ref(&delete)).await;
        let now = merged_snapshot(&state, &store, "P", &at(10), None)
            .await
            .unwrap();
        assert!(now.is_none());
        let before = merged_snapshot(&state, &store, "P", &at(3), None)
            .await
            .unwrap();
        assert_eq!(names(&before), vec!["Ada"]);
    }

    #[tokio::test]
    async fn live_erase_hides_persisted_history_everywhere() {
        let erase = Event {
            iid: iid(1),
            system_from: us(5),
            valid_from: Instant::MIN,
            valid_to: EOT,
            op: Op::Erase,
        };
        let (state, store) = seeded(&[put(1, 1, "Ada")], std::slice::from_ref(&erase)).await;
        // Even time-traveling BEFORE the erase: gone (slice-2 GDPR semantics).
        let before = merged_snapshot(&state, &store, "P", &at(3), None)
            .await
            .unwrap();
        assert!(before.is_none());
    }

    #[tokio::test]
    async fn iid_point_returns_only_that_entity() {
        let (state, store) = seeded(
            &[put(1, 1, "Ada"), put(2, 2, "Bob"), put(3, 3, "Cyd")],
            &[],
        )
        .await;
        let batch = merged_snapshot(&state, &store, "P", &at(10), Some(iid(2)))
            .await
            .unwrap();
        assert_eq!(names(&batch), vec!["Bob"]);
    }

    #[tokio::test]
    async fn merged_scan_equals_never_flushed_reference() {
        // Same 6 events: all live vs split 3 persisted / 3 live — identical batch.
        let events = [
            put(1, 1, "a1"),
            put(2, 2, "b1"),
            put(1, 3, "a2"),
            put(3, 4, "c1"),
            put(2, 5, "b2"),
            put(1, 6, "a3"),
        ];
        let (all_live, store_a) = seeded(&[], &events).await;
        let (split, store_b) = seeded(&events[..3], &events[3..]).await;
        for bounds in [at(10), at(4), at(2)] {
            let reference = merged_snapshot(&all_live, &store_a, "P", &bounds, None)
                .await
                .unwrap();
            let merged = merged_snapshot(&split, &store_b, "P", &bounds, None)
                .await
                .unwrap();
            assert_eq!(reference, merged);
        }
    }
}
```

- [x] **Step 2: Run test to verify it fails**

Run: `cargo test -p varve-engine scan`
Expected: FAIL — modules not defined (wire `mod scan; mod state;` into `lib.rs` first so failures are about the items).

- [x] **Step 3: Minimal implementation**

`crates/varve-engine/Cargo.toml` — add to `[dependencies]`:

```toml
varve-storage = { path = "../varve-storage" }
bytes = { workspace = true }
```

`crates/varve-engine/src/lib.rs`:

```rust
pub mod clock;
pub mod db;
pub mod registries;
mod scan;
mod state;
mod writer;

pub use clock::{Clock, MonotonicClock};
pub use datafusion::arrow::record_batch::RecordBatch;
pub use db::{Db, EngineError, TxReceipt};
pub use registries::Registries;
```

`crates/varve-engine/src/state.rs`:

```rust
use std::sync::Arc;
use varve_index::block::PageMeta;
use varve_index::LiveTable;
use varve_storage::TrieEntry;

/// v1: single default graph (spec §5.1); named graphs land in slice 7.
pub(crate) const DEFAULT_GRAPH: &str = "default";
/// v1: nodes are the only table (edges land in slice 6). Moved from writer.rs.
pub(crate) const NODES_TABLE: &str = "nodes";

/// One persisted L0 trie: its manifest entry plus the decoded page index.
/// Holding the decoded meta here is the spec §9 "footer cache" — meta
/// objects are fetched once (at flush or recovery), never per query.
#[derive(Clone)]
pub(crate) struct PersistedTrie {
    pub entry: TrieEntry,
    pub pages: Arc<Vec<PageMeta>>,
}

/// The queryable state of the nodes table: the live (unflushed) tail plus
/// the persisted-trie inventory, in ascending block order (== time order).
/// ONE lock over both (slice-4 plan, decision 8): flush pushes a trie and
/// resets the live table under a single write lock, queries snapshot both
/// under a single read lock — flushed events can never be observed in
/// neither or both sources.
pub(crate) struct TableState {
    pub live: LiveTable,
    pub tries: Vec<PersistedTrie>,
}

impl TableState {
    pub fn new() -> TableState {
        TableState {
            live: LiveTable::new(),
            tries: Vec::new(),
        }
    }
}
```

`crates/varve-engine/src/scan.rs` (above the tests from Step 1):

```rust
//! The merged bitemporal scan (spec §10 `BitemporalScan`, v1 shape): an
//! atomic (live, inventory) snapshot, page pruning, one ranged GET per
//! surviving page, per-entity merge in time order, then a single resolution
//! pass across sources via `snapshot_entities`.

use crate::db::EngineError;
use crate::state::{TableState, DEFAULT_GRAPH, NODES_TABLE};
use datafusion::arrow::record_batch::RecordBatch;
use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};
use varve_index::{decode_events, snapshot_entities, Event};
use varve_storage::{keys, ObjectStore};
use varve_types::{Iid, TemporalBounds};

pub(crate) async fn merged_snapshot(
    state: &Arc<RwLock<TableState>>,
    store: &Arc<dyn ObjectStore>,
    label: &str,
    bounds: &TemporalBounds,
    iid_point: Option<Iid>,
) -> Result<Option<RecordBatch>, EngineError> {
    // 1. Atomic snapshot under ONE read lock (decision 8). Live events are
    //    cloned — bounded by max_block_rows; a point lookup clones one
    //    entity. The trie inventory is Arc-cheap.
    let (live_events, tries) = {
        let s = state.read().map_err(|_| EngineError::Poisoned)?;
        let live_events: Vec<(Iid, Vec<Event>)> = match &iid_point {
            Some(iid) => s
                .live
                .events_for(iid)
                .map(|events| vec![(*iid, events.to_vec())])
                .unwrap_or_default(),
            None => s
                .live
                .entities()
                .map(|(iid, events)| (*iid, events.to_vec()))
                .collect(),
        };
        (live_events, s.tries.clone())
    };

    // 2. Persisted events, ascending block order (== time order, decision 9).
    let mut merged: BTreeMap<Iid, Vec<Event>> = BTreeMap::new();
    for trie in &tries {
        let data_key = keys::data_key(DEFAULT_GRAPH, NODES_TABLE, &trie.entry.trie_key);
        // An entity's run may span pages within one block: collect the whole
        // block in file order (system_from desc per entity), then reverse
        // per entity to restore arrival order.
        let mut per_block: BTreeMap<Iid, Vec<Event>> = BTreeMap::new();
        for page in trie
            .pages
            .iter()
            .filter(|p| p.selected(bounds, iid_point.as_ref()))
        {
            let bytes = store
                .get_range(&data_key, page.offset..page.offset + page.len)
                .await?;
            for event in decode_events(&bytes)? {
                if iid_point.as_ref().is_none_or(|iid| event.iid == *iid) {
                    per_block.entry(event.iid).or_default().push(event);
                }
            }
        }
        for (iid, desc) in per_block {
            merged.entry(iid).or_default().extend(desc.into_iter().rev());
        }
    }

    // 3. Live events are newest — appended after every persisted source.
    for (iid, events) in live_events {
        merged.entry(iid).or_default().extend(events);
    }

    Ok(snapshot_entities(
        merged.iter().map(|(iid, events)| (*iid, events.as_slice())),
        label,
        bounds,
    )?)
}

```

`crates/varve-engine/src/db.rs` — changes:

1. Imports: add `use crate::scan::merged_snapshot;`, `use crate::state::{TableState, DEFAULT_GRAPH, NODES_TABLE};`, `use varve_storage::{memory_store, CachedStore, MemoryCache, ObjectStore, StorageError};` and remove the writer's `NODES_TABLE` import (now from `state`).
2. `EngineError` — add:

```rust
    #[error(transparent)]
    Storage(#[from] StorageError),
```

3. Struct + constructors (spec §4 example config shows `memory = "512MiB"` — integer form per decision 14, this constant is the unconfigured default until Task 11):

```rust
/// Default in-memory cache budget until `[cache]` wiring lands (Task 11).
const DEFAULT_CACHE_MEMORY_BYTES: usize = 512 * 1024 * 1024;

pub struct Db {
    state: Arc<RwLock<TableState>>,
    store: Arc<dyn ObjectStore>,
    clock: Arc<dyn Clock>,
    submit: mpsc::Sender<Submission>,
}

fn cached(store: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
    Arc::new(CachedStore::new(
        store,
        Arc::new(MemoryCache::new(DEFAULT_CACHE_MEMORY_BYTES)),
    ))
}
```

`Db::memory()` passes `cached(memory_store())`; `assemble` takes the store and threads it into both `Db` and `WriterState`:

```rust
    fn assemble(
        live: LiveTable,
        log: Arc<dyn varve_log::Log>,
        store: Arc<dyn ObjectStore>,
        clock: Arc<dyn Clock>,
        cfg: WriterConfig,
        next_tx_id: u64,
    ) -> Db {
        let state = Arc::new(RwLock::new(TableState {
            live,
            tries: Vec::new(),
        }));
        let writer_state = WriterState {
            state: Arc::clone(&state),
            store: Arc::clone(&store),
            clock: Arc::clone(&clock),
            log,
            next_tx_id,
        };
        let submit = spawn_writer(writer_state, cfg);
        Db {
            state,
            store,
            clock,
            submit,
        }
    }
```

`Db::open_with` and `Db::local` pass `cached(memory_store())` for now, each with the comment `// storage config selection + manifest recovery land in Task 11 — nothing writes to storage before Task 10's flush.`

4. `Db::query` — replace the body after parsing:

```rust
    pub async fn query(&self, gql: &str) -> Result<Vec<RecordBatch>, EngineError> {
        let Statement::Query(q) = varve_gql::parse(gql)? else {
            return Err(EngineError::NotAQuery);
        };
        let now = self.clock.watermark();
        let bounds = varve_plan::effective_bounds(&q, now);
        let label = q.pattern.label.as_deref().unwrap_or("");
        let iid = varve_plan::iid_point(&q.where_clause, DEFAULT_GRAPH, NODES_TABLE);
        let snapshot = merged_snapshot(&self.state, &self.store, label, &bounds, iid).await?;
        Ok(varve_plan::execute_query(&q, snapshot).await?)
    }
```

5. `replay` is untouched (still folds into a `LiveTable`; `assemble` wraps it).

`crates/varve-engine/src/writer.rs` — changes:

1. Delete `pub(crate) const NODES_TABLE…` (moved to `state.rs`); import `use crate::scan::merged_snapshot;` and `use crate::state::{TableState, DEFAULT_GRAPH, NODES_TABLE};`; drop the now-unused `varve_plan` snapshot imports.
2. `WriterState`:

```rust
pub(crate) struct WriterState {
    pub state: Arc<RwLock<TableState>>,
    pub store: Arc<dyn varve_storage::ObjectStore>,
    pub clock: Arc<dyn Clock>,
    pub log: Arc<dyn Log>,
    pub next_tx_id: u64,
}
```

3. `resolve_insert` keeps using `Iid::derive(DEFAULT_GRAPH, NODES_TABLE, …)` (replace the `"default"` literal with the const).
4. `resolve_delete` — the read side goes through the merged scan (decision 13):

```rust
/// MATCH … DELETE (spec §10 DML): resolves the read side against the merged
/// live∪persisted snapshot at (valid=now, system=now) — a delete must find
/// flushed entities too (slice-4 plan, decision 13).
async fn resolve_delete(
    state: &WriterState,
    del: &DeleteStmt,
    system: Instant,
) -> Result<Vec<Event>, EngineError> {
    let bounds = TemporalBounds {
        valid: TemporalDimension::at(system),
        system: TemporalDimension::at(system),
    };
    let label = del.pattern.label.as_deref().unwrap_or("");
    let iid = varve_plan::iid_point(&del.where_clause, DEFAULT_GRAPH, NODES_TABLE);
    let snapshot = merged_snapshot(&state.state, &state.store, label, &bounds, iid).await?;
    let iids = varve_plan::iids_from_snapshot(snapshot, &del.where_clause).await?;
    Ok(iids
        .into_iter()
        .map(|iid| Event {
            iid,
            system_from: system,
            valid_from: system,
            valid_to: Instant::END_OF_TIME,
            op: Op::Delete,
        })
        .collect())
}
```

5. `apply` locks the shared state and appends into `.live`:

```rust
fn apply(state: &WriterState, staged: &mut [Staged]) -> Result<(), String> {
    let mut shared = state
        .state
        .write()
        .map_err(|_| "table state lock poisoned".to_string())?;
    for s in staged.iter_mut() {
        for event in std::mem::take(&mut s.events) {
            // OutOfOrderEvent cannot happen here: the writer loop is the only
            // caller of `append`, and `system` is assigned monotonically by
            // this same loop just before the event was built.
            shared.live.append(event).map_err(|e| e.to_string())?;
        }
    }
    Ok(())
}
```

6. The writer test helpers: `spawn()` builds `TableState` + a memory store; assertions on `live.read().unwrap().event_count()` become `state.read().unwrap().live.event_count()`:

```rust
    fn spawn(
        log: Arc<dyn Log>,
        cfg: WriterConfig,
    ) -> (mpsc::Sender<Submission>, Arc<RwLock<TableState>>) {
        let state = Arc::new(RwLock::new(TableState::new()));
        let writer_state = WriterState {
            state: Arc::clone(&state),
            store: varve_storage::memory_store(),
            clock: Arc::new(MonotonicClock::new()),
            log,
            next_tx_id: 0,
        };
        (spawn_writer(writer_state, cfg), state)
    }
```

- [x] **Step 4: Run the full workspace to verify nothing regressed**

Run: `cargo test --workspace`
Expected: all pass — 7 new engine scan tests; every existing engine/varve e2e test (walking skeleton, temporal, durability, mutations, concurrency) green: with an empty trie inventory the merged scan reduces exactly to the old live-only path.

- [x] **Step 5: Commit**

```bash
git add crates/varve-engine/ Cargo.lock
git commit -m "feat: merged live+persisted bitemporal scan behind one table-state lock"
```

---
### Task 10: Block flush in the writer loop — threshold, timer, manifest commit, trim

**Files:**
- Create: `crates/varve-engine/src/flush.rs`
- Modify: `crates/varve-engine/Cargo.toml` (`[features] fault-injection = []`)
- Modify: `crates/varve-engine/src/lib.rs` (`mod flush;`)
- Modify: `crates/varve-engine/src/writer.rs` (`WriterConfig`/`WriterState` fields; loop gains flush trigger + timer; watermark tracking)
- Modify: `crates/varve-engine/src/db.rs` (`replay` returns the watermark; `assemble` threads new fields)
- Modify: `crates/varve-index/src/live.rs` (`last_system_from()` accessor)
- Test: in-module `#[cfg(test)]` in `crates/varve-engine/src/flush.rs`

**Interfaces:**
- Produces: `varve_engine::flush::flush_block(state: &mut WriterState) -> Result<(), EngineError>` (pub(crate), async) — encode → data/meta PUTs → manifest PUT (commit) → atomic inventory-push + live-reset → best-effort `log.trim(watermark)`. No-op on an empty live table. Also `pub(crate) const PAGE_ROWS: usize = varve_index::block::DEFAULT_PAGE_ROWS;` and the feature-gated `crash_point` hooks `"pre-manifest-put"` / `"post-manifest-put"` (mirroring `varve-log`'s).
- Produces: `LiveTable::last_system_from(&self) -> Option<Instant>` — the max `system_from` ever appended (already tracked privately); stamps `BlockManifest::max_system_time_us` so the clock floor survives a trimmed log even when the flushed events came from replay, not fresh resolves.
- Changes: `WriterConfig { window, max_bytes, max_block_rows: usize, flush_interval: Duration }` (defaults 100_000 / 300 s; `Duration::ZERO` disables the timer); `WriterState` gains `next_block_id: u64` and `durable_watermark: LogPosition`; the log-flush updates `durable_watermark = first.advance(batch_len)` (decision 6); `replay` additionally returns the watermark.

- [x] **Step 1: Write the failing test**

`crates/varve-engine/src/flush.rs` (tests first; module body in Step 3):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::MonotonicClock;
    use crate::scan::merged_snapshot;
    use crate::state::TableState;
    use crate::writer::{spawn_writer, Submission, WriterConfig, WriterState};
    use crate::db::TxReceipt;
    use crate::db::EngineError;
    use bytes::Bytes;
    use std::ops::Range;
    use std::sync::{Arc, RwLock};
    use std::time::Duration;
    use tokio::sync::{mpsc, oneshot};
    use varve_log::{Log, MemoryLog};
    use varve_storage::{keys, latest_manifest, memory_store, BlockManifest, ObjectStore, StorageError};
    use varve_types::{LogPosition, TemporalBounds, TemporalDimension};

    fn spawn_with(
        store: Arc<dyn ObjectStore>,
        max_block_rows: usize,
        flush_interval: Duration,
    ) -> (
        mpsc::Sender<Submission>,
        Arc<RwLock<TableState>>,
        Arc<MemoryLog>,
    ) {
        let log = Arc::new(MemoryLog::new());
        let state = Arc::new(RwLock::new(TableState::new()));
        let writer_state = WriterState {
            state: Arc::clone(&state),
            store,
            clock: Arc::new(MonotonicClock::new()),
            log: Arc::clone(&log) as Arc<dyn Log>,
            next_tx_id: 0,
            next_block_id: 0,
            durable_watermark: LogPosition::ZERO,
        };
        let cfg = WriterConfig {
            window: Duration::ZERO,
            max_bytes: 8 * 1024 * 1024,
            max_block_rows,
            flush_interval,
        };
        (spawn_writer(writer_state, cfg), state, log)
    }

    fn submit(
        sender: &mpsc::Sender<Submission>,
        gql: &str,
    ) -> oneshot::Receiver<Result<TxReceipt, EngineError>> {
        let stmt = varve_gql::parse(gql).unwrap();
        let (ack, rx) = oneshot::channel();
        sender.try_send(Submission { stmt, ack }).unwrap();
        rx
    }

    /// The flush runs after the acks, so tests poll for the manifest.
    async fn wait_for_manifest(store: &Arc<dyn ObjectStore>) -> BlockManifest {
        for _ in 0..200 {
            if let Some(m) = latest_manifest(store.as_ref()).await.unwrap() {
                return m;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("no manifest appeared within 5s");
    }

    fn now_bounds() -> TemporalBounds {
        let now = MonotonicClock::new().next();
        TemporalBounds {
            valid: TemporalDimension::at(now),
            system: TemporalDimension::at(now),
        }
    }

    #[tokio::test]
    async fn size_trigger_flushes_a_block_and_trims_the_log() {
        let store = memory_store();
        let (sender, state, log) = spawn_with(Arc::clone(&store), 3, Duration::ZERO);
        for i in 1..=3 {
            submit(&sender, &format!("INSERT (:P {{_id: {i}, v: {i}}})"))
                .await
                .unwrap()
                .unwrap();
        }
        let manifest = wait_for_manifest(&store).await;
        assert_eq!(manifest.block_id, 0);
        assert_eq!(manifest.watermark, 3); // three 1-tx batches → positions 0..3
        assert_eq!(manifest.max_tx_id, 3);
        assert!(manifest.max_system_time_us > 0);
        let tries = &manifest.tables[0].tries;
        assert_eq!(tries.len(), 1);
        assert_eq!(tries[0].trie_key, "l00-rc-b00");
        assert_eq!(tries[0].row_count, 3);

        // Data + meta objects exist under the spec §9 keys.
        store
            .get(&keys::data_key("default", "nodes", "l00-rc-b00"))
            .await
            .unwrap();
        store
            .get(&keys::meta_key("default", "nodes", "l00-rc-b00"))
            .await
            .unwrap();

        // Live table reset, inventory extended — and the log trimmed.
        {
            let s = state.read().unwrap();
            assert_eq!(s.live.event_count(), 0);
            assert_eq!(s.tries.len(), 1);
        }
        assert!(log.tail(LogPosition::ZERO).await.unwrap().is_empty());

        // Queries still see every flushed row (merged scan).
        let batch = merged_snapshot(&state, &store, "P", &now_bounds(), None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(batch.num_rows(), 3);
    }

    #[tokio::test]
    async fn second_flush_carries_the_full_inventory() {
        let store = memory_store();
        let (sender, state, _log) = spawn_with(Arc::clone(&store), 2, Duration::ZERO);
        for i in 1..=4 {
            submit(&sender, &format!("INSERT (:P {{_id: {i}}})"))
                .await
                .unwrap()
                .unwrap();
        }
        // Poll until the SECOND manifest lands.
        let manifest = loop {
            let m = wait_for_manifest(&store).await;
            if m.block_id == 1 {
                break m;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        };
        assert_eq!(manifest.watermark, 4);
        let tries = &manifest.tables[0].tries;
        assert_eq!(tries.len(), 2, "manifest lists the FULL inventory");
        assert_eq!(tries[0].trie_key, "l00-rc-b00");
        assert_eq!(tries[1].trie_key, "l00-rc-b01");
        assert_eq!(state.read().unwrap().tries.len(), 2);

        let batch = merged_snapshot(&state, &store, "P", &now_bounds(), None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(batch.num_rows(), 4);
    }

    #[tokio::test]
    async fn flush_timer_flushes_below_the_row_threshold() {
        let store = memory_store();
        let (sender, state, _log) =
            spawn_with(Arc::clone(&store), 1000, Duration::from_millis(50));
        submit(&sender, "INSERT (:P {_id: 1})").await.unwrap().unwrap();
        let manifest = wait_for_manifest(&store).await;
        assert_eq!(manifest.tables[0].tries[0].row_count, 1);
        assert_eq!(state.read().unwrap().live.event_count(), 0);
    }

    /// Every PUT fails: acks still succeed, nothing is lost, no manifest
    /// appears, and the live table keeps serving (decision 10).
    struct FailingStore;

    #[async_trait::async_trait]
    impl ObjectStore for FailingStore {
        async fn put(&self, key: &str, _bytes: Bytes) -> Result<(), StorageError> {
            Err(StorageError::NotFound(format!("injected put failure: {key}")))
        }
        async fn get(&self, key: &str) -> Result<Bytes, StorageError> {
            Err(StorageError::NotFound(key.to_string()))
        }
        async fn get_range(&self, key: &str, _range: Range<u64>) -> Result<Bytes, StorageError> {
            Err(StorageError::NotFound(key.to_string()))
        }
        async fn list(&self, _prefix: &str) -> Result<Vec<String>, StorageError> {
            Ok(vec![])
        }
    }

    #[tokio::test]
    async fn failed_flush_keeps_the_live_table_and_the_acks() {
        let store: Arc<dyn ObjectStore> = Arc::new(FailingStore);
        let (sender, state, log) = spawn_with(Arc::clone(&store), 2, Duration::ZERO);
        for i in 1..=3 {
            submit(&sender, &format!("INSERT (:P {{_id: {i}}})"))
                .await
                .unwrap()
                .unwrap();
        }
        // Give the (failing) flush a moment, then verify nothing was lost.
        tokio::time::sleep(Duration::from_millis(100)).await;
        let s = state.read().unwrap();
        assert_eq!(s.live.event_count(), 3, "live table untouched on flush failure");
        assert!(s.tries.is_empty());
        drop(s);
        assert_eq!(log.tail(LogPosition::ZERO).await.unwrap().len(), 3, "log not trimmed");
    }

    #[tokio::test]
    async fn delete_finds_flushed_entities() {
        let store = memory_store();
        let (sender, state, _log) = spawn_with(Arc::clone(&store), 2, Duration::ZERO);
        submit(&sender, "INSERT (:P {_id: 1})").await.unwrap().unwrap();
        submit(&sender, "INSERT (:P {_id: 2})").await.unwrap().unwrap();
        wait_for_manifest(&store).await; // both rows now live ONLY in the block
        submit(&sender, "MATCH (p:P) WHERE p._id = 1 DELETE p")
            .await
            .unwrap()
            .unwrap();
        let batch = merged_snapshot(&state, &store, "P", &now_bounds(), None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(batch.num_rows(), 1, "delete resolved against the flushed block");
    }
}
```

- [x] **Step 2: Run test to verify it fails**

Run: `cargo test -p varve-engine flush`
Expected: FAIL — `WriterState` has no `next_block_id`/`durable_watermark`, `WriterConfig` has no `max_block_rows`/`flush_interval`, `flush_block` missing (add `mod flush;` to `lib.rs` first).

- [x] **Step 3: Minimal implementation**

`crates/varve-index/src/live.rs` — add next to `event_count`:

```rust
    /// Max `system_from` ever appended (None on an empty/new table). Stamps
    /// the manifest's clock floor: flushed events may predate this process's
    /// resolves (replay), so the writer cannot derive it from its own clock.
    pub fn last_system_from(&self) -> Option<Instant> {
        self.last_system_from
    }
```

`crates/varve-engine/Cargo.toml`:

```toml
[features]
# Test-only crash hooks for the varve-testkit kill -9 harness (mirrors
# varve-log's). Inert unless VARVE_CRASH_TRIGGER points at an armed file.
fault-injection = []
```

`crates/varve-engine/src/lib.rs` — add `mod flush;` alongside `mod scan; mod state;`.

`crates/varve-engine/src/writer.rs` — changes:

1. `WriterConfig` (defaults: spec §9 `max_block_rows` 100k; XTDB flush timeout 5 min):

```rust
#[derive(Clone, Copy, Debug)]
pub(crate) struct WriterConfig {
    pub window: Duration,
    pub max_bytes: usize,
    pub max_block_rows: usize,
    pub flush_interval: Duration, // ZERO disables the timer
}

impl Default for WriterConfig {
    fn default() -> Self {
        WriterConfig {
            window: Duration::from_millis(15),
            max_bytes: 8 * 1024 * 1024,
            max_block_rows: 100_000,
            flush_interval: Duration::from_secs(300),
        }
    }
}
```

2. `WriterState` gains the flush bookkeeping:

```rust
pub(crate) struct WriterState {
    pub state: Arc<RwLock<TableState>>,
    pub store: Arc<dyn varve_storage::ObjectStore>,
    pub clock: Arc<dyn Clock>,
    pub log: Arc<dyn Log>,
    pub next_tx_id: u64,
    /// Next L0 block id (recovered from the latest manifest in Task 11).
    pub next_block_id: u64,
    /// Exclusive end of the durably appended prefix — the next manifest's
    /// watermark (decision 6).
    pub durable_watermark: LogPosition,
}
```

3. `spawn_writer` — the loop gains the block-flush trigger and timer:

```rust
enum Received {
    Submission(Option<Submission>),
    FlushTimeout,
}

pub(crate) fn spawn_writer(mut state: WriterState, cfg: WriterConfig) -> mpsc::Sender<Submission> {
    let (sender, mut rx) = mpsc::channel::<Submission>(SUBMISSION_QUEUE_LEN);
    tokio::spawn(async move {
        // Armed while unflushed rows exist and a flush interval is set.
        let mut flush_deadline: Option<tokio::time::Instant> = None;
        loop {
            let received = match flush_deadline {
                Some(deadline) => tokio::select! {
                    sub = rx.recv() => Received::Submission(sub),
                    _ = tokio::time::sleep_until(deadline) => Received::FlushTimeout,
                },
                None => Received::Submission(rx.recv().await),
            };
            match received {
                Received::Submission(Some(first)) => {
                    run_batch(&mut state, &cfg, &mut rx, first).await;
                    if live_rows(&state) >= cfg.max_block_rows {
                        // A failed flush keeps the live table intact and
                        // retries at the next trigger (decision 10).
                        let _ = crate::flush::flush_block(&mut state).await;
                    }
                    flush_deadline = next_deadline(&state, &cfg, flush_deadline);
                }
                // Sender dropped (Db closed) and channel drained: staged txs
                // were already flushed to the LOG by run_batch; a final block
                // flush is unnecessary (the log holds everything).
                Received::Submission(None) => break,
                Received::FlushTimeout => {
                    let _ = crate::flush::flush_block(&mut state).await;
                    flush_deadline = next_deadline(&state, &cfg, None);
                }
            }
        }
    });
    sender
}

fn live_rows(state: &WriterState) -> usize {
    state
        .state
        .read()
        .map(|s| s.live.event_count())
        .unwrap_or(0)
}

/// Arm while there is unflushed data; keep an existing deadline (later
/// batches must not push it out — the OLDEST unflushed row bounds replay
/// time); disarm when empty or disabled.
fn next_deadline(
    state: &WriterState,
    cfg: &WriterConfig,
    current: Option<tokio::time::Instant>,
) -> Option<tokio::time::Instant> {
    if cfg.flush_interval.is_zero() || live_rows(state) == 0 {
        return None;
    }
    current.or_else(|| Some(tokio::time::Instant::now() + cfg.flush_interval))
}
```

4. `flush` (the LOG flush) records the durable watermark:

```rust
/// Durable append → apply → ack, strictly in that order (decision 1).
async fn flush(state: &mut WriterState, mut staged: Vec<Staged>) {
    let records: Vec<LogRecord> = staged.iter().map(|s| s.record.clone()).collect();
    let count = records.len() as u64;
    match state.log.append(records).await {
        Ok(first) => {
            // Exclusive end of the durable prefix — the manifest watermark
            // (decision 6; positions are consecutive per the Log contract).
            // On (unreachable) 48-bit overflow keep the old, conservative value.
            if let Ok(end) = first.advance(count) {
                state.durable_watermark = end;
            }
            let applied = apply(state, &mut staged);
            for s in staged {
                let _ = s.ack.send(match &applied {
                    Ok(()) => Ok(s.receipt),
                    Err(msg) => Err(EngineError::CommitFailed(msg.clone())),
                });
            }
        }
        Err(e) => {
            let msg = e.to_string();
            for s in staged {
                let _ = s.ack.send(Err(EngineError::CommitFailed(msg.clone())));
            }
        }
    }
}
```

5. The writer-test `spawn` helper gains the two new `WriterState` fields (`next_block_id: 0`, `durable_watermark: LogPosition::ZERO`) and existing writer tests use `WriterConfig { flush_interval: Duration::ZERO, .. }` where they construct configs literally (add the two fields; `flush_interval: Duration::ZERO` keeps old tests timer-free).

`crates/varve-engine/src/flush.rs` (above the Step-1 tests):

```rust
//! Block flush (spec §9): serialize the live table to an L0 trie, commit it
//! with the manifest PUT, swap the table state atomically, trim the log.

use crate::db::EngineError;
use crate::state::{PersistedTrie, DEFAULT_GRAPH, NODES_TABLE};
use crate::writer::WriterState;
use bytes::Bytes;
use std::sync::Arc;
use varve_index::block::{encode_block, EncodedBlock};
use varve_index::LiveTable;
use varve_storage::{keys, BlockManifest, TableTries, TrieEntry};

/// Rows per page (decision 3): a const here, a parameter in the codec.
pub(crate) const PAGE_ROWS: usize = varve_index::block::DEFAULT_PAGE_ROWS;

pub(crate) async fn flush_block(state: &mut WriterState) -> Result<(), EngineError> {
    // Encode under a read lock. The writer loop is the only mutator, so the
    // table cannot change between this snapshot and the reset below; queries
    // keep running against the pre-flush state meanwhile.
    let (encoded, prior, max_system_us) = {
        let s = state.state.read().map_err(|_| EngineError::Poisoned)?;
        if s.live.event_count() == 0 {
            return Ok(());
        }
        let max_system_us = s
            .live
            .last_system_from()
            .map(|t| t.as_micros())
            .unwrap_or(0);
        let prior: Vec<TrieEntry> = s.tries.iter().map(|t| t.entry.clone()).collect();
        (encode_block(&s.live, PAGE_ROWS)?, prior, max_system_us)
    };
    let EncodedBlock { data, meta, pages } = encoded;

    let block_id = state.next_block_id;
    let trie_key = keys::l0_trie_key(block_id);
    let entry = TrieEntry {
        trie_key: trie_key.clone(),
        row_count: pages.iter().map(|p| p.rows).sum(),
        data_len: data.len() as u64,
    };

    // Data + meta first: without a manifest entry they are invisible garbage
    // on failure (GC cleans orphans in slice 8) — never corruption.
    state
        .store
        .put(
            &keys::data_key(DEFAULT_GRAPH, NODES_TABLE, &trie_key),
            Bytes::from(data),
        )
        .await?;
    state
        .store
        .put(
            &keys::meta_key(DEFAULT_GRAPH, NODES_TABLE, &trie_key),
            Bytes::from(meta),
        )
        .await?;

    crash_point("pre-manifest-put");

    let mut tries = prior;
    tries.push(entry.clone());
    let manifest = BlockManifest {
        block_id,
        watermark: state.durable_watermark.as_u64(),
        max_tx_id: state.next_tx_id,
        max_system_time_us: max_system_us,
        tables: vec![TableTries {
            graph: DEFAULT_GRAPH.to_string(),
            table: NODES_TABLE.to_string(),
            tries,
        }],
    };
    // THE commit point (spec §9): before this PUT the block does not exist;
    // after it, recovery reads the block and replays from `watermark`.
    state
        .store
        .put(&keys::manifest_key(block_id), Bytes::from(manifest.to_wire()))
        .await?;

    crash_point("post-manifest-put");

    // Atomic swap under ONE write lock (decision 8): a query sees the rows
    // in the live table or in the trie inventory — never neither, never both.
    {
        let mut s = state.state.write().map_err(|_| EngineError::Poisoned)?;
        s.tries.push(PersistedTrie {
            entry,
            pages: Arc::new(pages),
        });
        s.live = LiveTable::new();
    }
    state.next_block_id += 1;

    // Best-effort: a failed trim leaves extra segments that the next flush
    // re-trims; replay filters by position, so they are harmless. No
    // observability surface yet (decision 10 / slice 10).
    let _ = state.log.trim(state.durable_watermark).await;
    Ok(())
}

/// Test-only crash hook for the varve-testkit kill -9 harness, mirroring
/// `varve-log::local::crash_point`. Inert (a no-op) unless built with the
/// `fault-injection` feature, and even then does nothing unless
/// `VARVE_CRASH_TRIGGER` points at a file containing exactly this point's
/// name. When armed, announces the point on stdout and parks until killed.
#[cfg(feature = "fault-injection")]
fn crash_point(point: &str) {
    let Ok(path) = std::env::var("VARVE_CRASH_TRIGGER") else {
        return;
    };
    match std::fs::read_to_string(&path) {
        Ok(armed) if armed.trim() == point => {}
        _ => return,
    }
    println!("CRASH_POINT {point}");
    use std::io::Write as _;
    let _ = std::io::stdout().flush();
    loop {
        std::thread::sleep(std::time::Duration::from_secs(3600));
    }
}

#[cfg(not(feature = "fault-injection"))]
fn crash_point(_point: &str) {}
```

`crates/varve-engine/src/db.rs` — changes:

1. `replay` also returns the watermark (position after the last replayed record):

```rust
async fn replay(
    log: &dyn varve_log::Log,
    clock: &dyn Clock,
) -> Result<(LiveTable, u64, LogPosition), EngineError> {
    let mut live = LiveTable::new();
    let mut next_tx_id = 0u64;
    let mut max_system: Option<Instant> = None;
    let mut watermark = LogPosition::ZERO;
    for (position, record) in log.tail(LogPosition::ZERO).await? {
        for effect in &record.effects {
            if effect.table != NODES_TABLE {
                return Err(EngineError::UnknownTable(effect.table.clone()));
            }
            for event in decode_events(&effect.arrow_ipc)? {
                live.append(event)?;
            }
        }
        next_tx_id = next_tx_id.max(record.tx_id);
        let system = Instant::from_micros(record.system_time_us);
        max_system = Some(max_system.map_or(system, |m| m.max(system)));
        watermark = position.advance(1)?;
    }
    if let Some(floor) = max_system {
        clock.advance_to(floor);
    }
    Ok((live, next_tx_id, watermark))
}
```

2. `assemble` threads the new `WriterState` fields:

```rust
    fn assemble(
        live: LiveTable,
        log: Arc<dyn varve_log::Log>,
        store: Arc<dyn ObjectStore>,
        clock: Arc<dyn Clock>,
        cfg: WriterConfig,
        next_tx_id: u64,
        next_block_id: u64,
        durable_watermark: LogPosition,
    ) -> Db {
```

`Db::memory()` passes `(…, 0, LogPosition::ZERO)`; `Db::open_with`/`Db::local` pass the replay watermark and `next_block_id: 0` (manifest recovery lands in Task 11).

- [x] **Step 4: Run tests to verify they pass**

Run: `cargo test -p varve-engine && cargo test --workspace`
Expected: 6 new flush tests pass; all existing tests green (default 100k threshold + 300 s timer are unreachable in existing tests).

- [x] **Step 5: Commit**

```bash
git add crates/varve-engine/ crates/varve-index/
git commit -m "feat: writer-loop block flush with manifest commit point and log trim"
```

---

### Task 11: Restart — `Db::open` from latest manifest + log tail; storage/cache config

**Files:**
- Modify: `crates/varve-engine/src/db.rs` (`recover` replaces `replay`; config wiring; `Db::local` layout)
- Modify: `crates/varve-engine/src/registries.rs` (storage registry)
- Modify: `crates/varve/tests/durability.rs` (config helper gains `[storage]`)
- Test: `crates/varve/tests/blocks.rs` (NEW, e2e) + in-module tests in `db.rs`

**Interfaces:**
- Produces: `Registries.storage: Registry<dyn ObjectStore>` (builtins `local`, `memory`).
- Produces: recovery — `Db::open`/`Db::open_with`/`Db::local` = latest manifest (trie inventory + floors) + `log.tail(manifest.watermark)` replay; `next_block_id = manifest.block_id + 1`; `next_tx_id = max(manifest.max_tx_id, replayed)`; clock floored by `max(manifest.max_system_time_us, replayed)`; writer watermark = `max(manifest.watermark, last replayed position + 1)` (monotonic guard).
- Produces: config surface — `[storage] backend = "memory"|"local"` (default `memory`), `[storage.local] dir`, `[storage] max_block_rows` (default `100000`), `[storage] flush_interval_ms` (default `300000`, `0` disables), `[cache] memory_max_bytes` (default `536870912`). NEW error `EngineError::VolatileBlockStore` for `log=local` + `storage=memory` (decision 11).
- Changes: `Db::local(dir)` layout — log at `dir/log`, store at `dir/store` (decision 11; dev, no migration).

- [x] **Step 1: Write the failing e2e test**

`crates/varve/tests/blocks.rs`:

```rust
#![allow(clippy::unwrap_used)]
use std::path::Path;
use varve::{Config, Db, EngineError};

/// log + storage both local under `dir`, tiny block threshold so tests
/// actually flush, 1 ms group-commit window.
fn blocks_config(dir: &Path, max_block_rows: usize) -> Config {
    let log_dir = toml_escaped(&dir.join("log"));
    let store_dir = toml_escaped(&dir.join("store"));
    Config::from_toml_str(&format!(
        "[log]\nbackend = \"local\"\ngroup_commit_window_ms = 1\n\
         [log.local]\ndir = {log_dir}\n\
         [storage]\nbackend = \"local\"\nmax_block_rows = {max_block_rows}\n\
         [storage.local]\ndir = {store_dir}\n"
    ))
    .unwrap()
}

fn toml_escaped(dir: &Path) -> String {
    format!("{:?}", dir.display().to_string())
}

fn rows(batches: &[varve::RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

/// Flushes happen asynchronously after acks — wait until the store dir has
/// a manifest (or give up and let assertions fail loudly).
async fn wait_for_flush(dir: &Path) {
    let blocks = dir.join("store").join("v1").join("blocks");
    for _ in 0..200 {
        if blocks.read_dir().map(|mut d| d.next().is_some()).unwrap_or(false) {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    panic!("no manifest appeared under {blocks:?} within 5s");
}

#[tokio::test]
async fn flushed_blocks_survive_restart_with_correct_queries() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Db::open(blocks_config(dir.path(), 4)).await.unwrap();
        for (id, name) in [(1, "Ada"), (2, "Bob"), (3, "Cyd"), (4, "Dee"), (5, "Eve"), (6, "Fay")] {
            db.execute(&format!("INSERT (:Person {{_id: {id}, name: '{name}'}})"))
                .await
                .unwrap();
        }
        wait_for_flush(dir.path()).await; // 4 rows in block 0, 2 still live
    }

    let db = Db::open(blocks_config(dir.path(), 4)).await.unwrap();
    let all = db.query("MATCH (p:Person) RETURN p.name").await.unwrap();
    assert_eq!(rows(&all), 6);

    // Point lookup crosses the block/live split correctly (IID pushdown path).
    let point = db
        .query("MATCH (p:Person) WHERE p._id = 3 RETURN p.name AS name")
        .await
        .unwrap();
    assert_eq!(rows(&point), 1);
    let names: &arrow::array::StringArray = point[0]
        .column_by_name("name")
        .unwrap()
        .as_any()
        .downcast_ref()
        .unwrap();
    assert_eq!(names.value(0), "Cyd");
}

#[tokio::test]
async fn tx_and_clock_floors_survive_restart_with_a_trimmed_log() {
    let dir = tempfile::tempdir().unwrap();
    let last_receipt;
    {
        let db = Db::open(blocks_config(dir.path(), 2)).await.unwrap();
        db.execute("INSERT (:P {_id: 1})").await.unwrap();
        last_receipt = db.execute("INSERT (:P {_id: 2})").await.unwrap();
        wait_for_flush(dir.path()).await; // flush + trim: the log is now empty
    }

    let db = Db::open(blocks_config(dir.path(), 2)).await.unwrap();
    // With an empty log, floors MUST come from the manifest.
    let next = db.execute("INSERT (:P {_id: 3})").await.unwrap();
    assert_eq!(next.tx_id, 3, "tx counter continues past flushed history");
    assert!(next.system_time > last_receipt.system_time, "clock floored above flushed history");
    assert_eq!(
        rows(&db.query("MATCH (p:P) RETURN p._id").await.unwrap()),
        3
    );
}

#[tokio::test]
async fn bitemporal_history_survives_flush_and_restart() {
    let dir = tempfile::tempdir().unwrap();
    let v1;
    {
        let db = Db::open(blocks_config(dir.path(), 2)).await.unwrap();
        v1 = db
            .execute("INSERT (:City {_id: 1, name: 'Oslo'})")
            .await
            .unwrap();
        db.execute("INSERT (:City {_id: 1, name: 'Osloo'})")
            .await
            .unwrap();
        wait_for_flush(dir.path()).await;
    }

    let db = Db::open(blocks_config(dir.path(), 2)).await.unwrap();
    let now = db.query("MATCH (c:City) RETURN c.name AS name").await.unwrap();
    assert_eq!(rows(&now), 1);
    let names: &arrow::array::StringArray = now[0]
        .column_by_name("name")
        .unwrap()
        .as_any()
        .downcast_ref()
        .unwrap();
    assert_eq!(names.value(0), "Osloo");

    // Time travel to before the correction — served from the flushed block.
    let before = db
        .query(&format!(
            "FOR SYSTEM_TIME AS OF TIMESTAMP '{}' MATCH (c:City) RETURN c.name AS name",
            v1.system_time
        ))
        .await
        .unwrap();
    assert_eq!(rows(&before), 1);
    let names: &arrow::array::StringArray = before[0]
        .column_by_name("name")
        .unwrap()
        .as_any()
        .downcast_ref()
        .unwrap();
    assert_eq!(names.value(0), "Oslo");
}

#[tokio::test]
async fn local_log_with_memory_storage_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let log_dir = toml_escaped(&dir.path().join("log"));
    let config = Config::from_toml_str(&format!(
        "[log]\nbackend = \"local\"\n[log.local]\ndir = {log_dir}\n"
    ))
    .unwrap();
    let err = Db::open(config).await.unwrap_err();
    assert!(matches!(err, EngineError::VolatileBlockStore), "{err}");
    assert!(err.to_string().contains("[storage]"), "{err}");
}
```

And an in-module recovery test in `crates/varve-engine/src/db.rs` (append inside the existing `mod tests`):

```rust
    /// Replay starts AT the manifest watermark: records below it (still in
    /// an untrimmed log — the post-manifest-crash shape) are NOT re-applied.
    #[tokio::test]
    async fn recover_skips_records_below_the_manifest_watermark() {
        use crate::state::{DEFAULT_GRAPH, NODES_TABLE};
        use bytes::Bytes;
        use varve_index::block::encode_block;
        use varve_index::{encode_events, Event, Op};
        use varve_log::TableEffects;
        use varve_storage::{keys, memory_store, BlockManifest, TableTries, TrieEntry};
        use varve_types::Doc;

        fn event(n: u8, sf: i64) -> Event {
            Event {
                iid: varve_types::Iid::derive(DEFAULT_GRAPH, NODES_TABLE, &[n]),
                system_from: Instant::from_micros(sf),
                valid_from: Instant::from_micros(sf),
                valid_to: Instant::END_OF_TIME,
                op: Op::Put {
                    labels: vec!["P".into()],
                    doc: Doc::new(),
                },
            }
        }
        fn record(tx_id: u64, e: &Event) -> LogRecord {
            LogRecord {
                tx_id,
                system_time_us: e.system_from.as_micros(),
                user: String::new(),
                effects: vec![TableEffects {
                    table: NODES_TABLE.to_string(),
                    arrow_ipc: encode_events(std::slice::from_ref(e)).unwrap(),
                }],
            }
        }

        let (e1, e2, e3) = (event(1, 1), event(2, 2), event(3, 3));
        // Full, UNTRIMMED log: positions 0, 1, 2.
        let log = MemoryLog::new();
        for (tx, e) in [(1u64, &e1), (2, &e2), (3, &e3)] {
            log.append(vec![record(tx, e)]).await.unwrap();
        }
        // Manifest says: block 0 holds e1+e2, replay from position 2.
        let store = memory_store();
        let mut flushed = LiveTable::new();
        flushed.append(e1).unwrap();
        flushed.append(e2).unwrap();
        let block = encode_block(&flushed, 1024).unwrap();
        let trie_key = keys::l0_trie_key(0);
        store
            .put(&keys::data_key(DEFAULT_GRAPH, NODES_TABLE, &trie_key), Bytes::from(block.data))
            .await
            .unwrap();
        store
            .put(&keys::meta_key(DEFAULT_GRAPH, NODES_TABLE, &trie_key), Bytes::from(block.meta))
            .await
            .unwrap();
        let manifest = BlockManifest {
            block_id: 0,
            watermark: 2,
            max_tx_id: 2,
            max_system_time_us: 2,
            tables: vec![TableTries {
                graph: DEFAULT_GRAPH.to_string(),
                table: NODES_TABLE.to_string(),
                tries: vec![TrieEntry {
                    trie_key,
                    row_count: 2,
                    data_len: 0,
                }],
            }],
        };
        store
            .put(&keys::manifest_key(0), Bytes::from(manifest.to_wire()))
            .await
            .unwrap();

        let clock = MonotonicClock::new();
        let recovered = recover(&log, &clock, &store).await.unwrap();
        assert_eq!(recovered.state.live.event_count(), 1, "only the post-watermark record replays");
        assert_eq!(recovered.state.tries.len(), 1);
        assert_eq!(recovered.next_tx_id, 3);
        assert_eq!(recovered.next_block_id, 1);
        assert_eq!(recovered.watermark.as_u64(), 3);
    }

    #[tokio::test]
    async fn recover_rejects_unknown_manifest_tables() {
        use bytes::Bytes;
        use varve_storage::{keys, memory_store, BlockManifest, TableTries};

        let store = memory_store();
        let manifest = BlockManifest {
            block_id: 0,
            watermark: 0,
            max_tx_id: 0,
            max_system_time_us: 0,
            tables: vec![TableTries {
                graph: "default".to_string(),
                table: "edges".to_string(), // slice 6 format — must hard-fail
                tries: vec![],
            }],
        };
        store
            .put(&keys::manifest_key(0), Bytes::from(manifest.to_wire()))
            .await
            .unwrap();
        let log = MemoryLog::new();
        let clock = MonotonicClock::new();
        match recover(&log, &clock, &store).await {
            Err(EngineError::UnknownTable(t)) => assert!(t.contains("edges"), "{t}"),
            other => panic!("expected UnknownTable, got {other:?}"),
        }
    }
```

- [x] **Step 2: Run tests to verify they fail**

Run: `cargo test -p varve --test blocks && cargo test -p varve-engine db`
Expected: FAIL — `VolatileBlockStore` missing, `[storage]` ignored (flush never configured), `recover` not defined.

- [x] **Step 3: Minimal implementation**

`crates/varve-engine/src/registries.rs`:

```rust
use crate::clock::{Clock, SystemClockFactory};
use varve_config::Registry;
use varve_log::Log;
use varve_storage::ObjectStore;

/// Per-subsystem component registries (spec §4). `with_builtins()` wires up
/// everything compiled in; embedding applications may `register` additional
/// factories before calling `Db::open_with`.
pub struct Registries {
    pub log: Registry<dyn Log>,
    pub clock: Registry<dyn Clock>,
    pub storage: Registry<dyn ObjectStore>,
}

impl Registries {
    pub fn with_builtins() -> Registries {
        let mut clock = Registry::new("clock");
        // Builtin names are a static, distinct set — duplicates are bugs.
        if let Err(e) = clock.register(Box::new(SystemClockFactory)) {
            unreachable!("duplicate builtin clock factory: {e}");
        }
        Registries {
            log: varve_log::log_registry(),
            clock,
            storage: varve_storage::storage_registry(),
        }
    }
}
```

(and extend its `builtins_cover_log_and_clock` test with `assert_eq!(registries.storage.names(), vec!["local", "memory"]);`.)

`crates/varve-engine/src/db.rs`:

1. `EngineError` — new variant:

```rust
    #[error(
        "[log] backend \"local\" with [storage] backend \"memory\" would lose \
         flushed blocks on restart while trimming the durable log; set \
         [storage] backend = \"local\" (with [storage.local] dir) or use a \
         memory log"
    )]
    VolatileBlockStore,
```

2. Tuning structs next to `LogTuning`:

```rust
/// Block-flush tuning read from `[storage]` (spec §9). Unknown keys
/// (`backend`, the `local` subtable) are ignored, as with `LogTuning`.
#[derive(serde::Deserialize)]
struct StorageTuning {
    #[serde(default = "default_max_block_rows")]
    max_block_rows: usize,
    #[serde(default = "default_flush_interval_ms")]
    flush_interval_ms: u64,
}

fn default_max_block_rows() -> usize {
    100_000
}

fn default_flush_interval_ms() -> u64 {
    300_000
}

/// `[cache]` tuning (decision 14: integer bytes, like group_commit_max_bytes).
#[derive(serde::Deserialize)]
struct CacheTuning {
    #[serde(default = "default_cache_memory_max_bytes")]
    memory_max_bytes: usize,
}

fn default_cache_memory_max_bytes() -> usize {
    512 * 1024 * 1024
}
```

3. `recover` REPLACES `replay` (delete `replay`; the manifest-less arm is exactly the old behavior):

```rust
struct Recovered {
    state: TableState,
    next_tx_id: u64,
    next_block_id: u64,
    watermark: LogPosition,
}

/// Spec §6 recovery: latest block manifest (§9) + log tail replay from its
/// watermark. Without a manifest this is exactly slice-3 recovery from
/// position zero. The floors (`max_tx_id`, `max_system_time_us`) come from
/// the manifest because a trimmed log can no longer provide them; replayed
/// records raise them further. The final watermark takes the max of both
/// sources so it never regresses even against a volatile log.
async fn recover(
    log: &dyn varve_log::Log,
    clock: &dyn Clock,
    store: &Arc<dyn ObjectStore>,
) -> Result<Recovered, EngineError> {
    let manifest = varve_storage::latest_manifest(store.as_ref()).await?;
    let mut tries = Vec::new();
    let (mut next_tx_id, next_block_id, mut watermark, mut max_system) = match &manifest {
        Some(m) => {
            for table in &m.tables {
                if table.graph != DEFAULT_GRAPH || table.table != NODES_TABLE {
                    return Err(EngineError::UnknownTable(format!(
                        "{}/{}",
                        table.graph, table.table
                    )));
                }
                for entry in &table.tries {
                    let meta = store
                        .get(&varve_storage::keys::meta_key(
                            &table.graph,
                            &table.table,
                            &entry.trie_key,
                        ))
                        .await?;
                    tries.push(crate::state::PersistedTrie {
                        entry: entry.clone(),
                        pages: Arc::new(varve_index::block::decode_meta(&meta)?),
                    });
                }
            }
            (
                m.max_tx_id,
                m.block_id + 1,
                LogPosition::from_u64(m.watermark),
                Some(Instant::from_micros(m.max_system_time_us)),
            )
        }
        None => (0, 0, LogPosition::ZERO, None),
    };

    let mut live = LiveTable::new();
    for (position, record) in log.tail(watermark).await? {
        for effect in &record.effects {
            if effect.table != NODES_TABLE {
                return Err(EngineError::UnknownTable(effect.table.clone()));
            }
            for event in decode_events(&effect.arrow_ipc)? {
                live.append(event)?;
            }
        }
        next_tx_id = next_tx_id.max(record.tx_id);
        let system = Instant::from_micros(record.system_time_us);
        max_system = Some(max_system.map_or(system, |m| m.max(system)));
        watermark = watermark.max(position.advance(1)?);
    }
    if let Some(floor) = max_system {
        clock.advance_to(floor);
    }
    Ok(Recovered {
        state: TableState { live, tries },
        next_tx_id,
        next_block_id,
        watermark,
    })
}
```

4. `assemble` now takes a `TableState` (constructors wrap accordingly):

```rust
    fn assemble(
        state: TableState,
        log: Arc<dyn varve_log::Log>,
        store: Arc<dyn ObjectStore>,
        clock: Arc<dyn Clock>,
        cfg: WriterConfig,
        next_tx_id: u64,
        next_block_id: u64,
        durable_watermark: LogPosition,
    ) -> Db {
```

5. `Db::open_with` — full replacement:

```rust
    pub async fn open_with(config: &Config, registries: &Registries) -> Result<Db, EngineError> {
        let log_section = config.section("log").unwrap_or_else(ConfigSection::empty);
        let log_backend = log_section.backend().unwrap_or("memory").to_string();
        let log = registries.log.build(&log_backend, &log_section)?;

        let storage_section = config
            .section("storage")
            .unwrap_or_else(ConfigSection::empty);
        let storage_backend = storage_section.backend().unwrap_or("memory").to_string();
        // Decision 11: flushing trims the durable log; blocks must be at
        // least as durable as the log they replace.
        if log_backend == "local" && storage_backend == "memory" {
            return Err(EngineError::VolatileBlockStore);
        }
        let backend = registries.storage.build(&storage_backend, &storage_section)?;
        let cache_tuning: CacheTuning = config
            .section("cache")
            .unwrap_or_else(ConfigSection::empty)
            .get()?;
        let store: Arc<dyn ObjectStore> = Arc::new(CachedStore::new(
            backend,
            Arc::new(MemoryCache::new(cache_tuning.memory_max_bytes)),
        ));

        let clock_section = config.section("clock").unwrap_or_else(ConfigSection::empty);
        let clock = registries
            .clock
            .build(clock_section.backend().unwrap_or("system"), &clock_section)?;

        let log_tuning: LogTuning = log_section.get()?;
        let storage_tuning: StorageTuning = storage_section.get()?;
        let cfg = WriterConfig {
            window: Duration::from_millis(log_tuning.group_commit_window_ms),
            max_bytes: log_tuning.group_commit_max_bytes,
            max_block_rows: storage_tuning.max_block_rows,
            flush_interval: Duration::from_millis(storage_tuning.flush_interval_ms),
        };

        let recovered = recover(log.as_ref(), clock.as_ref(), &store).await?;
        Ok(Self::assemble(
            recovered.state,
            log,
            store,
            clock,
            cfg,
            recovered.next_tx_id,
            recovered.next_block_id,
            recovered.watermark,
        ))
    }
```

6. `Db::local` — durable pair under one dir (decision 11; dev, no migration of slice-3 layouts):

```rust
    pub async fn local(dir: impl AsRef<Path>) -> Result<Db, EngineError> {
        let dir = dir.as_ref();
        let log: Arc<dyn Log> = Arc::new(LocalLog::open(&dir.join("log"), DEFAULT_SEGMENT_MAX_BYTES)?);
        let store = cached(varve_storage::local_store(&dir.join("store"))?);
        let clock: Arc<dyn Clock> = Arc::new(MonotonicClock::new());
        let recovered = recover(log.as_ref(), clock.as_ref(), &store).await?;
        Ok(Self::assemble(
            recovered.state,
            log,
            store,
            clock,
            WriterConfig::default(),
            recovered.next_tx_id,
            recovered.next_block_id,
            recovered.watermark,
        ))
    }
```

7. `Db::memory()` wraps `TableState::new()` (unchanged behavior; the sync constructor never recovers — both backends are volatile together).

`crates/varve/tests/durability.rs` — the `local_config` helper gains the storage pair (decision 11 makes the old config an error, which `local_log_with_memory_storage_is_rejected` now pins):

```rust
fn local_config(dir: &Path) -> Config {
    let log_dir = toml_escaped(&dir.join("log"));
    let store_dir = toml_escaped(&dir.join("store"));
    Config::from_toml_str(&format!(
        "[log]\nbackend = \"local\"\ngroup_commit_window_ms = 1\n\
         [log.local]\ndir = {log_dir}\n\
         [storage]\nbackend = \"local\"\n\
         [storage.local]\ndir = {store_dir}\n"
    ))
    .unwrap()
}
```

(Existing durability tests keep passing: the default 100k threshold + 300 s timer never fire within them.)

- [x] **Step 4: Run the full gate**

Run: `just check`
Expected: fmt clean, clippy clean, all workspace tests pass (4 new e2e blocks tests + 2 new recovery unit tests).

- [x] **Step 5: Commit**

```bash
git add crates/varve-engine/ crates/varve/ Cargo.lock
git commit -m "feat: Db::open recovery from block manifest with storage and cache config"
```

---
### Task 12: Flush-boundary property test — randomized flush points ≡ never-flushed

**Files:**
- Create: `crates/varve-testkit/tests/flush_equivalence.rs`
- Test: the file IS the test

**Interfaces:**
- Consumes: `arb_history(max_events)`, `arb_bounds()` (`varve_testkit::strategy`, slice 2); `encode_block`, `decode_meta` implicitly via `EncodedBlock.pages`, `PageMeta::selected`, `snapshot_entities`, `decode_events`, `LiveTable` (Tasks 5–6).
- Roadmap: "same op history, randomized flush points → identical query results as never-flushed reference." Pure (no storage, no engine) so 10k cases run fast in CI; the nightly job raises `PROPTEST_CASES` (slice-2 pattern). Because the merge loop applies `PageMeta::selected` with each case's bounds, this property also fuzzes the prune rules against erase/delete-laden histories.

- [x] **Step 1: Write the failing test**

`crates/varve-testkit/tests/flush_equivalence.rs`:

```rust
//! Roadmap slice 4: same op history, randomized flush points (and page
//! sizes) must yield IDENTICAL query results to the never-flushed table —
//! across random bounds, including erase/delete histories. The merge loop
//! below mirrors `varve-engine::scan::merged_snapshot` exactly (block order,
//! page pruning, per-entity reversal), minus the object store.
#![allow(clippy::unwrap_used)]

use proptest::prelude::*;
use std::collections::BTreeMap;
use varve_index::block::{encode_block, EncodedBlock};
use varve_index::{decode_events, snapshot_entities, Event, LiveTable};
use varve_testkit::strategy::{arb_bounds, arb_history};
use varve_types::{Iid, TemporalBounds};

fn cases() -> u32 {
    // 10k in CI; the nightly job raises this via PROPTEST_CASES (slice 2).
    std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10_000)
}

fn table(events: &[Event]) -> LiveTable {
    let mut t = LiveTable::new();
    for e in events {
        t.append(e.clone()).unwrap();
    }
    t
}

/// Sorted, deduped cut positions in `0..=len`.
fn split_points(idxs: &[prop::sample::Index], len: usize) -> Vec<usize> {
    let mut pts: Vec<usize> = idxs.iter().map(|i| i.index(len + 1)).collect();
    pts.sort_unstable();
    pts.dedup();
    pts
}

/// The engine's merge (scan.rs), minus the object store: blocks in ascending
/// (time) order, pages pruned by `selected(bounds, None)`, per-entity
/// reversal to arrival order, live events last.
fn merged(
    blocks: &[EncodedBlock],
    live: &LiveTable,
    bounds: &TemporalBounds,
) -> BTreeMap<Iid, Vec<Event>> {
    let mut merged: BTreeMap<Iid, Vec<Event>> = BTreeMap::new();
    for block in blocks {
        let mut per_block: BTreeMap<Iid, Vec<Event>> = BTreeMap::new();
        for page in block.pages.iter().filter(|p| p.selected(bounds, None)) {
            let bytes = &block.data[page.offset as usize..(page.offset + page.len) as usize];
            for event in decode_events(bytes).unwrap() {
                per_block.entry(event.iid).or_default().push(event);
            }
        }
        for (iid, desc) in per_block {
            merged.entry(iid).or_default().extend(desc.into_iter().rev());
        }
    }
    for (iid, events) in live.entities() {
        merged.entry(*iid).or_default().extend(events.iter().cloned());
    }
    merged
}

proptest! {
    #![proptest_config(ProptestConfig { cases: cases(), ..ProptestConfig::default() })]

    #[test]
    fn randomized_flush_points_do_not_change_query_results(
        history in arb_history(24),
        cut_idxs in proptest::collection::vec(any::<prop::sample::Index>(), 0..3),
        page_rows in 1..4usize,
        bounds in arb_bounds(),
    ) {
        // Reference: the whole history in one live table (slice-2 machinery,
        // itself equivalence-tested against the naive ReferenceStore).
        let expected = table(&history).snapshot_for_label("P", &bounds).unwrap();

        // Flushed variant: every segment before the last cut becomes a block.
        let pts = split_points(&cut_idxs, history.len());
        let mut blocks = Vec::new();
        let mut start = 0usize;
        for p in pts {
            let segment = &history[start..p];
            if !segment.is_empty() {
                blocks.push(encode_block(&table(segment), page_rows).unwrap());
            }
            start = p;
        }
        let live = table(&history[start..]);

        let sources = merged(&blocks, &live, &bounds);
        let got = snapshot_entities(
            sources.iter().map(|(iid, events)| (*iid, events.as_slice())),
            "P",
            &bounds,
        )
        .unwrap();
        prop_assert_eq!(got, expected);
    }
}
```

- [x] **Step 2: Run test to verify it runs RED-capable**

Run: `cargo test -p varve-testkit --test flush_equivalence`
Expected: compiles and PASSES if Tasks 5–6 are correct — this is an equivalence property over already-implemented code, so "failing first" means: temporarily break the merge (e.g. swap `desc.into_iter().rev()` to `desc.into_iter()` in the test's `merged`) and confirm proptest finds a counterexample within seconds, then restore. That confirms the property has teeth.

- [x] **Step 3: Run at full case count**

Run: `PROPTEST_CASES=10000 cargo test -p varve-testkit --test flush_equivalence --release`
Expected: PASS within a couple of minutes.

- [x] **Step 4: Commit**

```bash
git add crates/varve-testkit/
git commit -m "test: flush-boundary property — randomized flush points match never-flushed"
```

---

### Task 13: Crash matrix — kill during flush (pre/post manifest PUT)

**Files:**
- Modify: `crates/varve-testkit/Cargo.toml` (enable `varve-engine/fault-injection`; add `varve-storage`)
- Modify: `crates/varve-testkit/src/bin/crash_child.rs` (config-based open; flush-aware workload)
- Modify: `crates/varve-testkit/tests/crash_recovery.rs` (extended matrix)

**Interfaces:**
- Consumes: `varve-engine`'s `fault-injection` hooks `"pre-manifest-put"`/`"post-manifest-put"` (Task 10) — enabled workspace-wide only via `varve-testkit`'s dep (feature unification, the slice-3 `varve-log` pattern); `varve_storage::{local_store, latest_manifest}`.
- The child now opens via config with `max_block_rows = K` and BOTH backends local, so the K-th ack trips a real block flush during every run — all five fault points now exercise the blocks+manifest recovery path. Roadmap exit criterion: "crash matrix extended with kill-during-flush (manifest absent ⇒ clean replay, no corruption)".
- Note on double-apply: a killed `post-manifest-put` leaves manifest + untrimmed log; replay-from-watermark not re-applying those records is pinned DETERMINISTICALLY by `recover_skips_records_below_the_manifest_watermark` (Task 11) — the matrix here pins the end-to-end contract (`survived == acked`, clean reopen).

- [x] **Step 1: Update the harness (child first)**

`crates/varve-testkit/Cargo.toml` — `[dependencies]` additions:

```toml
# fault-injection features unify workspace-wide through these deps (slice-3
# pattern); varve-engine is listed for its feature alone.
varve-engine = { path = "../varve-engine", features = ["fault-injection"] }
varve-storage = { path = "../varve-storage" }
```

`crates/varve-testkit/src/bin/crash_child.rs` — replace the open + workload + point handling (keep `append_acked` and `park_for_kill` as they are):

```rust
use std::path::{Path, PathBuf};

fn blocks_config(work: &Path, k: u64) -> varve::Config {
    let log_dir = format!("{:?}", work.join("log").display().to_string());
    let store_dir = format!("{:?}", work.join("store").display().to_string());
    varve::Config::from_toml_str(&format!(
        "[log]\nbackend = \"local\"\ngroup_commit_window_ms = 1\n\
         [log.local]\ndir = {log_dir}\n\
         [storage]\nbackend = \"local\"\nmax_block_rows = {k}\n\
         [storage.local]\ndir = {store_dir}\n"
    ))
    .expect("child config")
}

/// The flush runs after the K-th ack; "none" runs must not exit under it.
fn wait_for_manifest_file(work: &Path) {
    let blocks = work.join("store").join("v1").join("blocks");
    for _ in 0..200 {
        if blocks
            .read_dir()
            .map(|mut d| d.next().is_some())
            .unwrap_or(false)
        {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(25));
    }
    panic!("child: flush produced no manifest within 5s");
}

#[tokio::main]
async fn main() {
    let mut args = std::env::args().skip(1);
    let work: PathBuf = args.next().expect("usage: <work> <point> <k>").into();
    let point = args.next().expect("crash point");
    let k: u64 = args
        .next()
        .expect("acked count")
        .parse()
        .expect("acked count is a number");

    // Flush points fire on the K-th ack's block flush, so they arm BEFORE
    // any insert; append points arm after the K acked inserts (as before).
    if matches!(point.as_str(), "pre-manifest-put" | "post-manifest-put") {
        std::fs::write(work.join("trigger"), &point).expect("arm trigger");
    }

    // max_block_rows == K: the K-th acked insert trips a block flush, so
    // EVERY matrix point now runs against the blocks+manifest machinery.
    let db = varve::Db::open(blocks_config(&work, k)).await.expect("open db");
    let acked_path = work.join("acked.txt");
    for i in 1..=k {
        db.execute(&format!("INSERT (:Crash {{seq: {i}}})"))
            .await
            .expect("insert acked");
        append_acked(&acked_path, i);
    }

    match point.as_str() {
        "none" => {
            wait_for_manifest_file(&work); // let the flush commit + trim
            std::process::exit(0);
        }
        "post-ack" => park_for_kill("post-ack"),
        // The writer task's crash_point announces and parks; main just waits
        // for the parent's SIGKILL.
        "pre-manifest-put" | "post-manifest-put" => loop {
            std::thread::sleep(std::time::Duration::from_secs(3600));
        },
        "pre-append" | "post-append" => {
            std::fs::write(work.join("trigger"), &point).expect("arm trigger");
            // The (K+1)th insert parks inside LocalLog::append at the armed
            // point; the ack never arrives.
            let _ = db.execute(&format!("INSERT (:Crash {{seq: {}}})", k + 1)).await;
            std::process::exit(2); // unreachable: killed while parked
        }
        other => {
            eprintln!("unknown crash point {other}");
            std::process::exit(2);
        }
    }
}
```

- [x] **Step 2: Extend the matrix (parent)**

`crates/varve-testkit/tests/crash_recovery.rs` — changes:

1. `surviving_seqs` reopens via the SAME config the child used (add the `blocks_config` helper above the tests, identical body to the child's; `varve::Db::open(blocks_config(work, K)).await.unwrap()` replaces `varve::Db::local(work.join("log"))`). `parse_log` is unchanged (`work/log` is still the log dir).
2. A manifest probe:

```rust
async fn manifest_in(work: &Path) -> Option<varve_storage::BlockManifest> {
    let store = varve_storage::local_store(&work.join("store")).unwrap();
    varve_storage::latest_manifest(store.as_ref()).await.unwrap()
}
```

3. `clean_run_sanity` — the clean run now flushes and trims:

```rust
#[tokio::test]
async fn clean_run_sanity() {
    let work = tempfile::tempdir().unwrap();
    let status = spawn_child(work.path(), "none").wait().unwrap();
    assert!(status.success());
    assert_eq!(acked_seqs(work.path()), (1..=K).collect::<Vec<_>>());
    assert_eq!(
        surviving_seqs(work.path()).await,
        (1..=K).collect::<Vec<_>>()
    );
    // The K-th ack tripped a block flush: manifest committed, log trimmed.
    let manifest = manifest_in(work.path()).await.expect("manifest after clean flush");
    assert_eq!(manifest.watermark, K);
    assert_eq!(
        manifest.tables[0].tries.iter().map(|t| t.row_count).sum::<u64>(),
        K
    );
    assert_eq!(parse_log(work.path()).await, 0, "log trimmed after flush");
}
```

4. `crash_matrix` — extend the points array and the per-point expectations (the flush at the K-th ack changes the append points' record counts: the first K records are TRIMMED before the K+1th tx starts, deterministically — the child only submits tx K+1 after receiving ack K, which happens before `flush_block` runs, and the writer processes the K+1 submission only after `flush_block` (incl. trim) completes):

```rust
#[tokio::test]
async fn crash_matrix() {
    for point in [
        "pre-append",
        "post-append",
        "post-ack",
        "pre-manifest-put",
        "post-manifest-put",
    ] {
        for _ in 0..iterations() {
            let work = tempfile::tempdir().unwrap();
            let mut child = spawn_child(work.path(), point);
            wait_for_crash_then_kill(&mut child, point);

            let acked = acked_seqs(work.path());
            let records = parse_log(work.path()).await;
            let survived = surviving_seqs(work.path()).await;
            let manifest = manifest_in(work.path()).await;

            // The fundamental contract, true at every fault point: nothing
            // acked is ever lost.
            for a in &acked {
                assert!(
                    survived.contains(a),
                    "acked seq {a} missing after crash at {point}"
                );
            }
            assert_eq!(acked, (1..=K).collect::<Vec<_>>(), "{point}");

            match point {
                "pre-append" => {
                    // Flush completed (trigger wasn't armed yet), THEN the
                    // in-flight (K+1)th died before any byte hit the log.
                    assert_eq!(survived, acked, "{point}");
                    assert_eq!(records, 0, "{point}: log trimmed by the flush");
                    assert!(manifest.is_some(), "{point}");
                }
                "post-append" => {
                    // Durable but unacked: the K+1th MAY legally surface.
                    assert!(
                        survived.len() == acked.len() || survived.len() == acked.len() + 1,
                        "{point}"
                    );
                    assert!(records == 0 || records == 1, "{point}");
                    assert!(manifest.is_some(), "{point}");
                }
                "post-ack" => {
                    // Killed at an arbitrary flush stage: whatever committed,
                    // recovery serves exactly the acked set.
                    assert_eq!(survived, acked, "{point}");
                    // Trim strictly follows the manifest PUT, so a trimmed
                    // log implies a manifest.
                    if records == 0 {
                        assert!(manifest.is_some(), "{point}: trim without manifest");
                    } else {
                        assert_eq!(records, K as usize, "{point}");
                    }
                }
                "pre-manifest-put" => {
                    // Data/meta orphans exist, but no manifest: the block
                    // does not exist; recovery replays the intact log.
                    assert!(manifest.is_none(), "{point}: manifest must be absent");
                    assert_eq!(records, K as usize, "{point}: log untrimmed");
                    assert_eq!(survived, acked, "{point}");
                }
                "post-manifest-put" => {
                    // Manifest committed, trim never ran: recovery reads the
                    // block AND the full log — replay-from-watermark must
                    // not double-apply (pinned deterministically in Task 11).
                    let m = manifest.expect("manifest present");
                    assert_eq!(m.watermark, K, "{point}");
                    assert_eq!(records, K as usize, "{point}: log untrimmed");
                    assert_eq!(survived, acked, "{point}");
                }
                other => unreachable!("unknown point {other}"),
            }
        }
    }
}
```

- [x] **Step 3: Run the matrix**

Run: `cargo test -p varve-testkit --test crash_recovery` then `just crash` (10 iterations, release)
Expected: PASS for all five points, no flake across iterations. (CI's `crash-matrix` job runs this same test at `VARVE_CRASH_ITERS=100` — no workflow change needed.)

- [x] **Step 4: Commit**

```bash
git add crates/varve-testkit/ Cargo.lock
git commit -m "test: crash matrix covers kill-during-flush at the manifest commit point"
```

---

### Task 14: 1M-event ingest → restart → warm point lookup (exit-criteria bench)

**Files:**
- Create: `crates/varve/examples/block_bench.rs`

**Interfaces:**
- Roadmap exit criterion: "1M-event ingest → restart → correct temporal queries with < 100ms warm point lookup". Like slice 3's `write_bench`, this is a smoke example whose printed numbers go into STATUS.md — not a CI-gated benchmark (that's slice 11). Correctness IS asserted (right row, right value); the latency threshold is printed as PASS/FAIL.
- Consumes: multi-node `INSERT` (slice 1), `Db::open` with `[storage]` (Task 11), IID point pushdown (Tasks 8–9).

- [x] **Step 1: Write the example**

`crates/varve/examples/block_bench.rs`:

```rust
//! Slice-4 exit-criteria smoke: 1M-event ingest → restart → warm point
//! lookup (< 100 ms target; record the printed numbers in STATUS.md).
//! Run: cargo run --release --example block_bench -p varve

use std::path::Path;
use std::time::Instant;
use varve::{Config, Db};

const NODES_PER_INSERT: usize = 1_000;
const INSERTS: usize = 1_000; // 1M nodes total
const PROBE_ID: usize = 999_999;

fn config(dir: &Path) -> Result<Config, Box<dyn std::error::Error>> {
    let log_dir = format!("{:?}", dir.join("log").display().to_string());
    let store_dir = format!("{:?}", dir.join("store").display().to_string());
    Ok(Config::from_toml_str(&format!(
        "[log]\nbackend = \"local\"\n[log.local]\ndir = {log_dir}\n\
         [storage]\nbackend = \"local\"\n[storage.local]\ndir = {store_dir}\n"
    ))?)
}

fn insert_statement(batch: usize) -> String {
    let mut stmt = String::with_capacity(NODES_PER_INSERT * 40);
    stmt.push_str("INSERT ");
    for j in 0..NODES_PER_INSERT {
        let id = batch * NODES_PER_INSERT + j;
        if j > 0 {
            stmt.push_str(", ");
        }
        stmt.push_str(&format!("(:Bench {{_id: {id}, v: {id}}})"));
    }
    stmt
}

async fn point_lookup(db: &Db) -> Result<i64, Box<dyn std::error::Error>> {
    use arrow::array::Int64Array;
    let batches = db
        .query(&format!(
            "MATCH (b:Bench) WHERE b._id = {PROBE_ID} RETURN b.v AS v"
        ))
        .await?;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 1, "point lookup must return exactly one row");
    let col: &Int64Array = batches[0]
        .column_by_name("v")
        .ok_or("missing v column")?
        .as_any()
        .downcast_ref()
        .ok_or("v is not Int64")?;
    Ok(col.value(0))
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;

    // Phase 1: ingest 1M nodes (1000 txs × 1000 nodes; default 100k
    // max_block_rows → ~10 block flushes along the way).
    let started = Instant::now();
    {
        let db = Db::open(config(dir.path())?).await?;
        for batch in 0..INSERTS {
            db.execute(&insert_statement(batch)).await?;
        }
        let ingest = started.elapsed();
        println!(
            "ingest: {} events in {:.2?} ({:.0} events/s)",
            NODES_PER_INSERT * INSERTS,
            ingest,
            (NODES_PER_INSERT * INSERTS) as f64 / ingest.as_secs_f64()
        );
    } // drop: acked txs are durable (log) or flushed (blocks)

    // Phase 2: restart = latest manifest + log tail replay.
    let reopen_started = Instant::now();
    let db = Db::open(config(dir.path())?).await?;
    println!("reopen (manifest + log tail): {:.2?}", reopen_started.elapsed());

    // Phase 3: point lookup — cold, then warm (cache + meta in memory).
    let cold_started = Instant::now();
    assert_eq!(point_lookup(&db).await?, PROBE_ID as i64);
    let cold = cold_started.elapsed();
    let warm_started = Instant::now();
    assert_eq!(point_lookup(&db).await?, PROBE_ID as i64);
    let warm = warm_started.elapsed();
    println!("point lookup: cold {cold:.2?}, warm {warm:.2?}");
    println!(
        "exit criterion (<100ms warm point lookup): {}",
        if warm.as_millis() < 100 { "PASS" } else { "FAIL" }
    );

    // Correctness across the restart: total row count.
    let all = db.query("MATCH (b:Bench) RETURN b._id").await?;
    let rows: usize = all.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, NODES_PER_INSERT * INSERTS, "all rows visible after restart");
    println!("full scan after restart: {rows} rows OK");
    Ok(())
}
```

- [x] **Step 2: Run it**

Run: `cargo run --release --example block_bench -p varve`
Expected output shape (numbers vary by machine):

```
ingest: 1000000 events in ~Ns (… events/s)
reopen (manifest + log tail): …
point lookup: cold …, warm …
exit criterion (<100ms warm point lookup): PASS
full scan after restart: 1000000 rows OK
```

If the warm lookup prints FAIL, treat it as a defect to fix within this slice (the IID pushdown path in Tasks 8–9 exists precisely for this) — do not weaken the criterion.

- [x] **Step 3: Commit**

```bash
git add crates/varve/examples/
git commit -m "feat: block_bench example — 1M-event ingest, restart, warm point lookup"
```

---

## Slice exit checklist

- [x] `just check` green (fmt + clippy `-D warnings` + full workspace tests).
- [x] `just crash` green (5-point matrix × 10 iterations, release).
- [x] `PROPTEST_CASES=10000 cargo test -p varve-testkit --release` green (equivalence + flush-equivalence).
- [x] `cargo run --release --example block_bench -p varve` — record ingest rate, reopen time, cold/warm point-lookup latencies in STATUS.md; warm < 100 ms.
- [x] Existing demos still green: `cargo run --example hello -p varve`, `cargo run --example time_travel -p varve`, `cargo run --release --example write_bench -p varve`.
- [x] Update `docs/plans/STATUS.md`:
  - Current position → slice 4 ✅ COMPLETE, demo command `cargo run --release --example block_bench -p varve`; next action = generate the slice-5 detailed plan (S3 backends, disk cache, capability probe — spec §6/§9/§12, D5/D7) with the writing-plans skill.
  - Environment facts → new dependency pins as resolved (`object_store` 0.13.x — MUST track datafusion's, re-derive with `cargo tree -p datafusion | grep object_store`; `bytes` 1.x; `futures` 0.3.x); `varve-engine` now has a `fault-injection` feature (same pattern/caveats as `varve-log`'s); `Db::local(dir)` layout is now `dir/log` + `dir/store`.
  - Decisions → transcribe this plan's "Design decisions" 1–15 (abbreviated), explicitly including: valid-axis pruning deliberately absent (decision 4 — the reported-`_valid_to` clipping subtlety), manifest floors (`max_tx_id`/`max_system_time_us`) required once the log trims, `VolatileBlockStore` config rule, `BuildContext` STILL not needed (cache is engine composition — discharges the slice-4 revisit; slice 5's disk tier is the next checkpoint), flush-failure observability deferred to slice 10 (add to open items), GC of orphaned data/meta objects deferred to slice 8 (add to open items).
  - Slice log table row for slice 4 (tests count, bench numbers).
- [x] Tick every slice-4 checkbox in `docs/plans/varve-v1-roadmap.md`.
- [x] Commit: `git add docs/ && git commit -m "docs: slice 4 complete — blocks, persisted scan, restart; STATUS and roadmap updated"`.
- [x] Never leave red tests at a session boundary (roadmap session protocol).
