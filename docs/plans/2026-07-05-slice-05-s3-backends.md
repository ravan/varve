# Slice 5: S3-API backends, disk cache, capability probe

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `storage = "s3"` runs the whole storage+log stack against Garage/SeaweedFS/MinIO through the registry; `log = "object-store"` writes one object per group-commit batch into the shared block store; a disk cache tier — selected by name via the deferred cache registry — survives restarts; and a report-only capability probe classifies conditional-PUT support per backend (the gate slice-10 `cas-failover` will consume).

**Architecture:** Spec §6 (object-store log), §9 (key layout, caching), §12 (capability probe), D5/D7 (sovereignty: plain PUT/GET/LIST is always enough; anything stronger is probed, never assumed). This slice also realizes spec §4's full factory signature: `ComponentFactory::build(cfg, ctx)` gains the `BuildContext` parameter, because the `log/object-store` factory must consume the already-built storage component — exactly the checkpoint STATUS.md earmarked for the trait break.

**Tech Stack:** `object_store` 0.13.2 `aws` feature (already the workspace pin — the feature adds reqwest/quick-xml transitively); `xxhash-rust` (already a workspace dep) in varve-storage for disk-cache file names; docker CLI via `std::process::Command` (test-only, env-gated — no new crate).

## Global Constraints

- All roadmap Global Constraints apply: TDD without exception; traits + registry + TOML composition; sovereignty (put/get/get_range/list is the required surface — conditional PUT stays OPTIONAL and probed); `cargo clippy --workspace --all-targets -- -D warnings`; no `unwrap()`/`expect()` in library code (tests exempt via clippy.toml; `varve-testkit/src/backends.rs` carries an explicit file-level `#![allow]` with justification — it is test-harness code where a broken container rig must abort loudly); errors via `thiserror`; conventional commit prefixes; **NO Co-Authored-By trailer**.
- **APIs verified against the pinned crate.** Every `object_store` 0.13.2 claim in this plan was checked against the vendored source (`~/.cargo/registry/src/*/object_store-0.13.2/`): `AmazonS3Builder::{from_env, with_bucket_name, with_endpoint, with_region, with_access_key_id, with_secret_access_key, with_allow_http, with_virtual_hosted_style_request}`; `build()` requires only the bucket and defaults region to `"us-east-1"` with **no network I/O**; `S3ConditionalPut::ETagMatch` is the 0.13 **default** (no builder call needed); `put_opts(&self, &Path, PutPayload, PutOptions)` lives directly on `object_store::ObjectStore`; `PutMode::{Overwrite, Create, Update(UpdateVersion)}` with `impl From<PutMode> for PutOptions`; `PutResult { e_tag: Option<String>, version: Option<String> }`; error variants `AlreadyExists{path,source}`, `Precondition{path,source}`, `NotSupported{source}`, `NotImplemented{operation,implementer}`; `LocalFileSystem` supports `PutMode::Create` but rejects `PutMode::Update` with `NotImplemented`; `InMemory` supports both and returns rotating ETags. **The test code in this plan is the contract** (slice-1 rule): if any implementation sketch still disagrees with the pinned crate, adapt the implementation, never the asserted behavior.
- **No backward compatibility** (we are in development): the slice-4 `[cache] memory_max_bytes` key is REMOVED and replaced by the `[cache] tiers` design in Task 6 — no alias, no fallback.
- Config sizes stay **integer bytes** (slice-3/4 convention).
- **Hermetic default:** container-backed tests skip silently unless `VARVE_S3_BACKENDS` names a backend; `just check` never needs docker.
- **Image pins live in ONE place** (`varve-testkit/src/backends.rs` constants). If a pinned tag is not pullable at execution time, bump to the nearest available tag and record the final pin in STATUS.md — do not scatter tags anywhere else.

## Design decisions (record in STATUS.md at slice end)

1. **`BuildContext` lands — spec §4's factory signature realized.** `ComponentFactory::build(&self, cfg: &ConfigSection, ctx: &BuildContext)`. `BuildContext` is a typed component map (`TypeId → Box<dyn Any + Send + Sync>`, `insert<C>`/`get<C>` for `C: Clone + Send + Sync + 'static`, e.g. `Arc<dyn ObjectStore>`). The engine builds **storage first** and inserts the RAW (uncached) store; log/clock/cache factories build with that context. Trigger: the `log/object-store` factory needs the already-built storage component — the exact checkpoint STATUS.md scheduled for this trait break ("the cheapest moment, before more backends exist"). Cache-tier factories turned out config-only; the store is the only context component in v1.
2. **`storage/s3` = `object_store::aws::AmazonS3` behind cargo feature `s3` (default-on).** Config `[storage.s3]`: `bucket` (required), `endpoint`/`region`/`access_key_id`/`secret_access_key` (optional — the builder starts from `AmazonS3Builder::from_env()`, so standard `AWS_*` env vars are the fallback and config keys override), `path_style` (default **true** — Garage and MinIO need path-style), `allow_http` (default derived from the endpoint scheme: `http://` ⇒ true). Region falls back to the builder default `us-east-1`; Garage deployments set `region = "garage"`.
3. **`log/object-store` (spec §6/§9):** one object per group-commit batch at `v1/log/<epoch>/<offset-lexhex>.vlog`, named by the batch's FIRST position; epoch directory is fixed-width 4-digit hex (u16) so listing sorts numerically. Object body = the exact `LocalLog` frame grammar (`len u32 LE · crc32c u32 LE · protobuf payload` per record) but decoded STRICTLY — object PUTs are atomic, so any malformed frame is `Corrupt`, never a truncatable torn tail. Positions are assigned locally (designated writer — no CAS, D5); the next position is recovered lazily on first `append` by listing the prefix and counting the last object's frames (factories are synchronous). **`trim` is a documented no-op**: the sovereign trait has no delete; superseded objects are swept by slice-8 GC, and replay cost stays bounded because recovery reads only `tail(manifest.watermark)`. Durability = the backing store's; the log rides the same bucket/keyspace as blocks and receives the RAW store so log traffic never fills the query cache.
4. **Disk cache tier:** one self-describing file per `(path, range)` under `[cache.disk] dir` — header carries the full key, body the value — so the index rebuilds by walking the directory on open (restart survival, no separate index file to corrupt). File names are `xxh3_128(key)` hex. Recency = in-memory LRU tick at runtime, persisted as file mtime (touched on hit) so restart ordering approximates LRU. Reads copy into owned `Bytes`: eviction can never invalidate a handed-out buffer, so the roadmap's "ref-count pinning while mapped" is vacuously satisfied until an mmap path exists (post-v1). Synchronous I/O on the caller's thread is the documented v1 tradeoff (entries are page-sized; same spirit as MemoryCache's O(n) eviction scan).
5. **Cache registry-by-name (the deferred slice-4 registry):** `Registry<dyn CacheTier>` (kind `"cache"`) with builtin factories `memory` and `disk`; `Registries` gains `cache`. Config: `[cache] tiers = ["memory"]` (default) composed **outermost-first** (`["memory", "disk"]` ⇒ memory checked first, then disk, then backend; `[]` = uncached); per-tier tuning in `[cache.memory] max_bytes` (default 512 MiB) and `[cache.disk] dir` (required) + `max_bytes` (default 50 GiB). The slice-4 `[cache] memory_max_bytes` key is gone. DEVIATION from the spec-§4 sketch's `[cache] memory/disk_path/disk_max` flat keys: registry-by-name wants one child table per tier (`[cache.<name>]`), mirroring `[log.local]`/`[storage.local]`; human-size strings ("512MiB") stay deferred to slice 9 with `group_commit_max_bytes` (slice-3 decision). `Db::memory()`/`Db::local()` defaults unchanged (memory tier, 512 MiB).
6. **Capability probe (report-only, spec §12):** a new OPTIONAL `ConditionalStore` surface (`put_if_absent` = If-None-Match:*, `put_if_matches` = If-Match ETag) reachable via `ObjectStore::conditional() -> Option<&dyn ConditionalStore>` (default `None` — the engine never requires it; the blanket impl over `object_store` backends provides it via `put_opts`; `CachedStore` delegates). `probe_conditional_put` runs FOUR steps against a fresh key under `v1/probe/`: create, create-again (must refuse), swap-with-current-etag, swap-with-stale-etag (must refuse) — steps 2 and 4 are the semantic teeth that catch backends which ignore the headers (D5's SeaweedFS-class bugs; the versioned-bucket edge case surfaces the same way). Verdict: `Supported` / `Unsupported{reason}` / `Inconsistent{reason}` — Inconsistent is the dangerous one and MUST also refuse cas-failover in slice 10. `Db::probe_capabilities()` is the v1 surface (server `/v1/status` lands in slice 9); each probe leaves ≤ 2 small objects under `v1/probe/` (no delete; GC = slice 8). Builtin verdicts pinned by unit test: memory ⇒ Supported, local ⇒ Unsupported (source-verified `PutMode::Update` rejection).
7. **Backend matrix = hand-rolled docker-CLI harness** in `varve-testkit` (deviation from the roadmap's word "testcontainers", read as "containers for testing": Garage needs a multi-step `docker exec` init — layout assign/apply, key create, bucket grants — which `std::process::Command` drives with zero new dependencies and no third-party API to verify). Gated by `VARVE_S3_BACKENDS` (comma list or `all`). Two buckets per backend keep phases isolated: `varve-contract` (raw store/log/probe contract) and `varve` (Db end-to-end). Probe expectations are a per-backend table: Garage must NOT be Supported (D5), MinIO must be Supported, SeaweedFS/Ceph record-only until the first observed run pins them (then tighten + record in STATUS.md).
8. **CI:** `backend-matrix` job (fail-fast off, matrix garage/seaweedfs/minio) on push/PR; `backend-ceph-weekly` on a new Monday cron `0 4 * * 1`; `property-nightly` pinned to its own cron expression so the weekly trigger doesn't double-run it.

## File structure

```
crates/varve-config/src/registry.rs        # BuildContext; ComponentFactory::build gains ctx (spec §4)
crates/varve-config/src/lib.rs             # export BuildContext
crates/varve-config/tests/registry_test.rs # ctx tests; existing calls migrated
crates/varve-storage/Cargo.toml            # feature s3 (default) → object_store/aws; + varve-types, xxhash-rust
crates/varve-storage/src/s3.rs             # NEW: storage/s3 factory
crates/varve-storage/src/keys.rs           # + v1/log/… layout: LOG_PREFIX, log_key, parse_log_key
crates/varve-storage/src/store.rs          # + ConditionalStore, CondPut, conditional() hook, blanket impl
crates/varve-storage/src/probe.rs          # NEW: 4-step probe, ProbeReport/ProbeVerdict
crates/varve-storage/src/disk.rs           # NEW: DiskCache tier + cache/disk factory
crates/varve-storage/src/cache.rs          # + cache/memory factory; CachedStore delegates conditional()
crates/varve-storage/src/lib.rs            # + s3 registration (cfg-gated), cache_registry(), exports
crates/varve-storage/tests/store_test.rs   # + s3 factory tests; registry-names update
crates/varve-log/Cargo.toml                # feature object-store (default) → varve-storage + bytes deps
crates/varve-log/src/log.rs                # + LogError::Storage (feature-gated)
crates/varve-log/src/object_store.rs       # NEW: ObjectStoreLog + log/object-store factory
crates/varve-log/src/lib.rs                # + object-store registration (cfg-gated)
crates/varve-log/tests/object_store_log.rs # NEW: contract, reopen, corrupt, factory tests
crates/varve-engine/src/clock.rs           # factory signature migration
crates/varve-engine/src/registries.rs      # + cache registry; names tests
crates/varve-engine/src/db.rs              # open_with: storage-first + ctx; [cache] tiers; probe_capabilities
crates/varve-engine/src/lib.rs             # re-export ProbeReport/ProbeVerdict
crates/varve/src/lib.rs                    # facade re-exports
crates/varve/tests/object_log.rs           # NEW: Db e2e over object-store log (+ restart replay)
crates/varve/tests/cache_tiers.rs          # NEW: [cache] tiers e2e (disk survives restart, errors)
crates/varve/tests/walking_skeleton.rs     # + probe_capabilities facade smoke test
crates/varve/examples/cache_bench.rs       # NEW: cold vs warm disk cache (exit criterion)
crates/varve-testkit/Cargo.toml            # + varve-config dep; tempfile → [dependencies]; bytes dev-dep
crates/varve-testkit/src/backends.rs       # NEW: docker harness (garage/seaweedfs/minio/ceph), S3Params
crates/varve-testkit/src/lib.rs            # pub mod backends
crates/varve-testkit/tests/backend_matrix.rs # NEW: per-backend full suite + probe expectations
.github/workflows/ci.yml                   # backend-matrix job; ceph weekly cron; nightly cron pinning
justfile                                    # s3-matrix target
docs/plans/STATUS.md · docs/plans/varve-v1-roadmap.md  # slice exit
```

Task order: 1 (BuildContext) unblocks every factory; 2 (s3) and 3 (object log) are the backends; 4 wires the log into the engine; 5–6 deliver the disk cache + registry; 7 the probe; 8–9 the container matrix + CI; 10 the exit-criterion bench; 11 the exit checklist.

---
### Task 1: `BuildContext` — factories can consume already-built components (spec §4)

The one deliberate trait break of this slice, taken now because exactly one consumer exists (the Task-3 log factory) and every factory that will ever exist multiplies the migration cost later. Purely mechanical for all existing factories: they gain an ignored `_ctx` parameter.

**Files:**
- Modify: `crates/varve-config/src/registry.rs` (BuildContext + `ComponentFactory::build` + `Registry::build`)
- Modify: `crates/varve-config/src/lib.rs` (export)
- Test: `crates/varve-config/tests/registry_test.rs`
- Mechanical migration (signature + call sites — the compiler enumerates these same spots):
  - `crates/varve-log/src/memory.rs` (MemoryLogFactory), `crates/varve-log/src/local.rs` (LocalLogFactory)
  - `crates/varve-storage/src/memory.rs` (MemoryStoreFactory), `crates/varve-storage/src/local.rs` (LocalStoreFactory)
  - `crates/varve-engine/src/clock.rs` (SystemClockFactory + its test at ~line 131)
  - `crates/varve-engine/src/db.rs` (3 `registries.*.build(...)` calls in `open_with`)
  - `crates/varve-engine/src/registries.rs` (2 build calls in tests)
  - `crates/varve-storage/tests/store_test.rs` (3 build calls)
  - `crates/varve-log/tests/local_log.rs` (2 build calls), `crates/varve-log/tests/memory_log.rs` (2 build calls)

**Interfaces:**
- Consumes: existing `ConfigSection`, `RegistryError`.
- Produces (later tasks rely on these exact names):
  ```rust
  // varve-config (re-exported at crate root)
  pub struct BuildContext { /* private */ }
  impl BuildContext {
      pub fn empty() -> BuildContext;
      pub fn insert<C: Clone + Send + Sync + 'static>(&mut self, component: C);
      pub fn get<C: Clone + Send + Sync + 'static>(&self) -> Option<C>;
  }
  pub trait ComponentFactory<T: ?Sized>: Send + Sync {
      fn name(&self) -> &'static str;
      fn build(&self, cfg: &ConfigSection, ctx: &BuildContext) -> Result<Arc<T>, RegistryError>;
  }
  impl<T: ?Sized> Registry<T> {
      pub fn build(&self, name: &str, cfg: &ConfigSection, ctx: &BuildContext) -> Result<Arc<T>, RegistryError>;
  }
  ```

- [ ] **Step 1: Write the failing tests**

In `crates/varve-config/tests/registry_test.rs`, add at the top of the file (after the existing imports):

```rust
use std::sync::atomic::{AtomicU32, Ordering};
use varve_config::BuildContext;
```

Add after the existing `EnglishFactory` block:

```rust
// A factory that needs an already-built component from the BuildContext —
// the shape the slice-5 object-store log uses to reach the storage
// component (spec §4 ctx).
#[derive(Debug)]
struct CountedGreeter {
    count: Arc<AtomicU32>,
}
impl Greeter for CountedGreeter {
    fn greet(&self) -> String {
        format!("greeting #{}", self.count.fetch_add(1, Ordering::SeqCst) + 1)
    }
}

struct CountedFactory;
impl ComponentFactory<dyn Greeter> for CountedFactory {
    fn name(&self) -> &'static str {
        "counted"
    }
    fn build(
        &self,
        _cfg: &ConfigSection,
        ctx: &BuildContext,
    ) -> Result<Arc<dyn Greeter>, RegistryError> {
        let count = ctx
            .get::<Arc<AtomicU32>>()
            .ok_or_else(|| RegistryError::Build {
                kind: "greeter",
                name: "counted".into(),
                source: "requires a counter component in the BuildContext"
                    .to_string()
                    .into(),
            })?;
        Ok(Arc::new(CountedGreeter { count }))
    }
}
```

Add the three new tests at the end of the file:

```rust
#[test]
fn factories_can_consume_context_components() {
    let mut reg: Registry<dyn Greeter> = Registry::new("greeter");
    reg.register(Box::new(CountedFactory)).unwrap();
    let counter = Arc::new(AtomicU32::new(0));
    let mut ctx = BuildContext::empty();
    ctx.insert(Arc::clone(&counter));
    let g = reg.build("counted", &ConfigSection::empty(), &ctx).unwrap();
    assert_eq!(g.greet(), "greeting #1");
    assert_eq!(counter.load(Ordering::SeqCst), 1, "shares the ctx Arc");
}

#[test]
fn missing_context_component_is_a_build_error() {
    let mut reg: Registry<dyn Greeter> = Registry::new("greeter");
    reg.register(Box::new(CountedFactory)).unwrap();
    let err = reg
        .build("counted", &ConfigSection::empty(), &BuildContext::empty())
        .unwrap_err()
        .to_string();
    assert!(err.contains("counter component"), "{err}");
}

#[test]
fn context_get_is_typed() {
    let mut ctx = BuildContext::empty();
    ctx.insert(7u32);
    assert_eq!(ctx.get::<u32>(), Some(7));
    assert_eq!(ctx.get::<u64>(), None, "different type, different slot");
}
```

Migrate the two existing tests in this file: `EnglishFactory::build` gains `_ctx: &BuildContext`, and both existing `reg.build(...)` calls gain a trailing `&BuildContext::empty()` argument.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p varve-config`
Expected: COMPILE FAIL — `no BuildContext in the root` / method signature mismatches.

- [ ] **Step 3: Implement BuildContext and the new signatures**

In `crates/varve-config/src/registry.rs`, add imports and the struct (below the existing `use` block):

```rust
use std::any::{Any, TypeId};
use std::collections::HashMap;
```

```rust
/// Already-built components that later factories may depend on — spec §4's
/// `ctx` parameter. Typed lookup: components are keyed by their FULL type
/// (e.g. `Arc<dyn ObjectStore>`), and `get` clones the stored value out, so
/// components are cheap-to-clone handles (`Arc`s) by convention.
///
/// The engine populates this in dependency order (storage first), so a
/// factory can only see components built before its own subsystem — a
/// factory that needs something absent fails with an actionable
/// [`RegistryError::Build`], never a panic.
#[derive(Default)]
pub struct BuildContext {
    components: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
}

impl BuildContext {
    /// No components — the common case for config-only factories and tests.
    pub fn empty() -> BuildContext {
        BuildContext::default()
    }

    /// Stores `component` under its type; a second insert of the same type
    /// replaces the first.
    pub fn insert<C: Clone + Send + Sync + 'static>(&mut self, component: C) {
        self.components
            .insert(TypeId::of::<C>(), Box::new(component));
    }

    /// Clones the component of type `C` out, if one was inserted.
    pub fn get<C: Clone + Send + Sync + 'static>(&self) -> Option<C> {
        self.components
            .get(&TypeId::of::<C>())
            .and_then(|b| b.downcast_ref::<C>())
            .cloned()
    }
}
```

Change the trait method (keep the existing doc comment, extend it with one line about `ctx`):

```rust
    /// Builds one instance of `T` from `cfg` — the section the instance was
    /// selected under, e.g. `[log]` for a log factory (not a pre-narrowed
    /// child section; a factory reaches into its own nested table itself).
    /// `ctx` carries already-built components the factory may consume
    /// (spec §4) — config-only factories ignore it.
    fn build(&self, cfg: &ConfigSection, ctx: &BuildContext) -> Result<Arc<T>, RegistryError>;
```

Change `Registry::build` to accept and forward `ctx`:

```rust
    pub fn build(
        &self,
        name: &str,
        cfg: &ConfigSection,
        ctx: &BuildContext,
    ) -> Result<Arc<T>, RegistryError> {
        match self.factories.get(name) {
            Some(f) => f.build(cfg, ctx),
            None => Err(RegistryError::Unknown {
                kind: self.kind,
                name: name.to_string(),
                available: self.factories.keys().map(|s| s.to_string()).collect(),
            }),
        }
    }
```

In `crates/varve-config/src/lib.rs`, add `BuildContext` to the `pub use` of registry items (next to `ComponentFactory`, `Registry`, `RegistryError`).

- [ ] **Step 4: Mechanical migration of every factory and call site**

Each factory impl changes only its signature (the body is untouched). Pattern, shown once — apply to `MemoryLogFactory`, `LocalLogFactory`, `MemoryStoreFactory`, `LocalStoreFactory`, `SystemClockFactory`:

```rust
// before
fn build(&self, cfg: &ConfigSection) -> Result<Arc<dyn Log>, RegistryError> {
// after (import BuildContext in the file's varve_config use list)
fn build(&self, cfg: &ConfigSection, _ctx: &BuildContext) -> Result<Arc<dyn Log>, RegistryError> {
```

Call sites gain a trailing `&BuildContext::empty()` argument (import `BuildContext` where missing):

- `crates/varve-engine/src/db.rs` `open_with`: all three `registries.{log,storage,clock}.build(...)` calls (Task 4 rewires this properly; plain `&BuildContext::empty()` keeps it compiling now).
- `crates/varve-engine/src/clock.rs` test: `SystemClockFactory.build(&ConfigSection::empty(), &BuildContext::empty())`.
- `crates/varve-engine/src/registries.rs` `builds_by_name_from_empty_sections` test: both calls.
- `crates/varve-storage/tests/store_test.rs`: the calls at `registry_builds_by_name`, `local_factory_requires_dir`, `local_factory_builds_from_config`.
- `crates/varve-log/tests/local_log.rs` `factory_builds_from_toml_and_requires_dir`: both calls.
- `crates/varve-log/tests/memory_log.rs` `registry_builds_memory_by_name_and_lists_available_on_unknown`: both calls.

- [ ] **Step 5: Run the full gate**

Run: `cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings`
Expected: all green (existing 245 tests + the 3 new ones).

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat: BuildContext — component factories can consume already-built components (spec §4)"
```

---
### Task 2: `storage/s3` registry factory

Any S3-API endpoint — AWS, Garage, Ceph RGW, SeaweedFS, MinIO — through the existing blanket impl (`object_store::aws::AmazonS3` IS a Varve `ObjectStore` already). All unit tests are network-free: `AmazonS3Builder::build()` only validates configuration (verified against the 0.13.2 source — region defaults to `us-east-1`, missing credentials fall back to the lazy provider chain).

**Files:**
- Modify: `crates/varve-storage/Cargo.toml` (feature `s3`, default-on)
- Create: `crates/varve-storage/src/s3.rs`
- Modify: `crates/varve-storage/src/lib.rs` (module + cfg-gated registration + export)
- Test: `crates/varve-storage/tests/store_test.rs` (factory tests + registry-names update)
- Modify: `crates/varve-engine/src/registries.rs` (storage names assertion gains `"s3"`)

**Interfaces:**
- Consumes: `ObjectStore`/`StorageError` (store.rs), `ComponentFactory`/`BuildContext` (Task 1).
- Produces: `varve_storage::S3StoreFactory` (name `"s3"`), registered in `storage_registry()` under `#[cfg(feature = "s3")]`. Config contract used by Tasks 8/9 and the docs: `[storage.s3]` keys `bucket` (required), `endpoint`, `region`, `access_key_id`, `secret_access_key`, `path_style` (default true), `allow_http` (default: endpoint scheme).

- [ ] **Step 1: Write the failing tests**

Append to `crates/varve-storage/tests/store_test.rs`:

```rust
#[cfg(feature = "s3")]
mod s3_factory {
    use varve_config::{BuildContext, Config, ConfigSection};
    use varve_storage::storage_registry;

    fn storage_section(toml: &str) -> ConfigSection {
        Config::from_toml_str(toml)
            .unwrap()
            .section("storage")
            .unwrap()
    }

    /// `AmazonS3Builder::build()` does no I/O — a fully specified factory
    /// build must succeed with nothing listening on the endpoint.
    #[test]
    fn s3_factory_builds_from_full_config() {
        let cfg = storage_section(
            "[storage]\nbackend = \"s3\"\n[storage.s3]\n\
             endpoint = \"http://127.0.0.1:3900\"\nbucket = \"varve\"\n\
             region = \"garage\"\naccess_key_id = \"GK0123456789\"\n\
             secret_access_key = \"secret\"\n",
        );
        assert!(storage_registry()
            .build("s3", &cfg, &BuildContext::empty())
            .is_ok());
    }

    /// Credentials may be omitted entirely: the builder starts from
    /// `from_env()` and defers to the AWS provider chain — building still
    /// succeeds (resolution is lazy, at first request).
    #[test]
    fn s3_factory_builds_without_inline_credentials() {
        let cfg = storage_section(
            "[storage]\nbackend = \"s3\"\n[storage.s3]\n\
             endpoint = \"http://127.0.0.1:3900\"\nbucket = \"varve\"\n",
        );
        assert!(storage_registry()
            .build("s3", &cfg, &BuildContext::empty())
            .is_ok());
    }

    #[test]
    fn s3_factory_requires_the_s3_section() {
        let cfg = storage_section("[storage]\nbackend = \"s3\"\n");
        let err = match storage_registry().build("s3", &cfg, &BuildContext::empty()) {
            Ok(_) => panic!("expected build(\"s3\") with no [storage.s3] to fail"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("[storage.s3]"), "{err}");
    }

    #[test]
    fn s3_factory_requires_a_bucket() {
        let cfg = storage_section(
            "[storage]\nbackend = \"s3\"\n[storage.s3]\nendpoint = \"http://x\"\n",
        );
        let err = match storage_registry().build("s3", &cfg, &BuildContext::empty()) {
            Ok(_) => panic!("expected build(\"s3\") without bucket to fail"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("bucket"), "{err}");
    }
}
```

Update the existing `registry_builds_by_name` assertion:

```rust
    assert_eq!(reg.names(), vec!["local", "memory", "s3"]);
```

And in `crates/varve-engine/src/registries.rs`, `builtins_cover_log_and_clock`:

```rust
        assert_eq!(registries.storage.names(), vec!["local", "memory", "s3"]);
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p varve-storage --test store_test`
Expected: COMPILE FAIL (`feature = "s3"` doesn't exist yet) or, once the feature stub exists, `unknown storage implementation 's3'`.

- [ ] **Step 3: Declare the feature and implement the factory**

`crates/varve-storage/Cargo.toml` — add before `[dependencies]`:

```toml
[features]
# Any S3-API endpoint (AWS, Garage, Ceph RGW, SeaweedFS, MinIO). Default-on:
# it is a v1 success criterion; embedders may disable default features for a
# local-only build.
default = ["s3"]
s3 = ["object_store/aws"]
```

Create `crates/varve-storage/src/s3.rs`:

```rust
//! `storage/s3` — any S3-API endpoint (spec §6/§9, D7): AWS, Garage, Ceph
//! RGW, SeaweedFS, MinIO. Built on `object_store::aws`; the blanket impl in
//! `store.rs` adapts it to Varve's sovereign `ObjectStore` trait, so the
//! engine still sees put/get/get_range/list ONLY. Conditional-PUT support
//! (for slice-10 cas-failover) is probed, never assumed (D5) — `probe.rs`.

use crate::store::{ObjectStore, StorageError};
use object_store::aws::AmazonS3Builder;
use std::sync::Arc;
use varve_config::{BuildContext, ComponentFactory, ConfigSection, RegistryError};

/// `[storage.s3]` settings. Credentials may be omitted: the builder starts
/// from `AmazonS3Builder::from_env()` (standard `AWS_*` variables and the
/// lazy AWS provider chain); explicit config keys override the environment.
#[derive(serde::Deserialize)]
struct S3Config {
    bucket: String,
    /// e.g. `http://127.0.0.1:3900` (Garage). Omitted = AWS endpoint
    /// resolution.
    endpoint: Option<String>,
    /// Garage requires this to match its `s3_region` (conventionally
    /// "garage"); omitted = env or the builder default `us-east-1`.
    region: Option<String>,
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
    /// Path-style addressing (`endpoint/bucket/key`). Garage and MinIO need
    /// it, so it is the DEFAULT; `false` selects virtual-hosted style
    /// (`bucket.endpoint/key`, the AWS default).
    #[serde(default = "default_path_style")]
    path_style: bool,
    /// Permit plain-HTTP endpoints. Default: derived from the endpoint
    /// scheme (`http://…` ⇒ true), so local containers just work while TLS
    /// stays mandatory for https/AWS.
    allow_http: Option<bool>,
}

fn default_path_style() -> bool {
    true
}

fn build_s3(config: &S3Config) -> Result<Arc<dyn ObjectStore>, StorageError> {
    let mut builder = AmazonS3Builder::from_env()
        .with_bucket_name(&config.bucket)
        .with_virtual_hosted_style_request(!config.path_style);
    if let Some(endpoint) = &config.endpoint {
        let allow_http = config
            .allow_http
            .unwrap_or_else(|| endpoint.starts_with("http://"));
        builder = builder.with_endpoint(endpoint).with_allow_http(allow_http);
    }
    if let Some(region) = &config.region {
        builder = builder.with_region(region);
    }
    if let Some(key) = &config.access_key_id {
        builder = builder.with_access_key_id(key);
    }
    if let Some(secret) = &config.secret_access_key {
        builder = builder.with_secret_access_key(secret);
    }
    let s3 = builder.build().map_err(StorageError::Backend)?;
    Ok(Arc::new(s3))
}

/// Registry factory: `[storage] backend = "s3"`, configured via
/// `[storage.s3]` (`bucket` required).
pub struct S3StoreFactory;

impl ComponentFactory<dyn ObjectStore> for S3StoreFactory {
    fn name(&self) -> &'static str {
        "s3"
    }

    fn build(
        &self,
        cfg: &ConfigSection,
        _ctx: &BuildContext,
    ) -> Result<Arc<dyn ObjectStore>, RegistryError> {
        let section = cfg.child("s3").ok_or_else(|| RegistryError::Build {
            kind: "storage",
            name: "s3".into(),
            source: "missing [storage.s3] section (requires `bucket`)"
                .to_string()
                .into(),
        })?;
        let config: S3Config = section.get()?;
        build_s3(&config).map_err(|e| RegistryError::Build {
            kind: "storage",
            name: "s3".into(),
            source: Box::new(e),
        })
    }
}
```

`crates/varve-storage/src/lib.rs` — add the module, export, and registration:

```rust
#[cfg(feature = "s3")]
pub mod s3;
```

```rust
#[cfg(feature = "s3")]
pub use s3::S3StoreFactory;
```

and inside `storage_registry()`:

```rust
    #[cfg(feature = "s3")]
    register_builtin(&mut reg, Box::new(s3::S3StoreFactory));
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p varve-storage && cargo test -p varve-engine registries`
Expected: PASS, including the updated names assertions.

- [ ] **Step 5: Full gate + commit**

Run: `cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings`
Expected: green. (`missing field \`bucket\`` from serde satisfies the `contains("bucket")` assertion.)

```bash
git add -A
git commit -m "feat: storage/s3 registry factory — any S3-API endpoint via object_store/aws"
```

---

### Task 3: `log/object-store` — spec-§9 log keys + `ObjectStoreLog` + factory

One object per group-commit batch, riding the block store's bucket. Keys live with the rest of the layout in `varve-storage::keys`; the log itself lives in `varve-log` behind a default-on `object-store` feature (spec §15: "varve-log-object (feature)").

**Files:**
- Modify: `crates/varve-storage/Cargo.toml` (+ `varve-types` dependency)
- Modify: `crates/varve-storage/src/keys.rs` (LOG_PREFIX, log_key, parse_log_key + tests)
- Modify: `crates/varve-log/Cargo.toml` (feature `object-store` default-on; optional `varve-storage`, `bytes` deps)
- Modify: `crates/varve-log/src/log.rs` (feature-gated `LogError::Storage`)
- Create: `crates/varve-log/src/object_store.rs`
- Modify: `crates/varve-log/src/lib.rs` (module, export, cfg-gated registration)
- Test: `crates/varve-log/tests/object_store_log.rs` (new)
- Modify: `crates/varve-engine/src/registries.rs` (log names assertion gains `"object-store"`)

**Interfaces:**
- Consumes: `ObjectStore`, `StorageError`, `keys::lex_hex`/`parse_lex_hex` (existing), `BuildContext::get::<Arc<dyn ObjectStore>>()` (Task 1), `LogRecord::{to_wire, from_wire}` (existing).
- Produces:
  ```rust
  // varve-storage::keys
  pub const LOG_PREFIX: &str = "v1/log";
  pub fn log_key(first: varve_types::LogPosition) -> String;
  pub fn parse_log_key(key: &str) -> Option<varve_types::LogPosition>;
  // varve-log (feature "object-store", default-on)
  pub struct ObjectStoreLog { /* private */ }
  impl ObjectStoreLog { pub fn new(store: Arc<dyn varve_storage::ObjectStore>) -> ObjectStoreLog; }
  impl Log for ObjectStoreLog { /* append/read_range/tail/trim */ }
  pub struct ObjectStoreLogFactory;   // ComponentFactory<dyn Log>, name "object-store"
  // varve-log::LogError gains (feature-gated):
  //   Storage(#[from] varve_storage::StorageError)
  ```

- [ ] **Step 1: Write the failing key-layout tests**

Append to the `tests` module in `crates/varve-storage/src/keys.rs`:

```rust
    #[test]
    fn log_keys_follow_the_spec_layout() {
        use varve_types::LogPosition;
        // Spec §9: v1/log/<epoch>/<offset-lexhex>.vlog; epoch dir is
        // fixed-width u16 hex so listing sorts numerically.
        let p = |e, o| LogPosition::new(e, o).unwrap();
        assert_eq!(log_key(p(0, 0)), "v1/log/0000/00.vlog");
        assert_eq!(log_key(p(0, 0x34)), "v1/log/0000/134.vlog");
        assert_eq!(log_key(p(3, 2)), "v1/log/0003/02.vlog");
    }

    #[test]
    fn log_keys_round_trip_and_reject_foreign_keys() {
        use varve_types::LogPosition;
        for (e, o) in [(0u16, 0u64), (0, 1), (0, 0xff), (3, 0x34), (u16::MAX, 1 << 40)] {
            let pos = LogPosition::new(e, o).unwrap();
            assert_eq!(parse_log_key(&log_key(pos)), Some(pos), "{e}/{o}");
        }
        assert_eq!(parse_log_key("v1/log/0000/00.manifest"), None); // wrong ext
        assert_eq!(parse_log_key("v1/log/00/00.vlog"), None); // short epoch
        assert_eq!(parse_log_key("v1/log/000A/00.vlog"), None); // uppercase
        assert_eq!(parse_log_key("v1/log/0000/1FF.vlog"), None); // uppercase body
        assert_eq!(parse_log_key("v1/blocks/00.vlog"), None); // wrong prefix
        assert_eq!(parse_log_key("v1/log/0000.vlog"), None); // missing segment
    }

    #[test]
    fn log_key_listing_order_is_position_order() {
        use varve_types::LogPosition;
        let positions = [
            LogPosition::new(0, 0).unwrap(),
            LogPosition::new(0, 9).unwrap(),
            LogPosition::new(0, 0x10).unwrap(),
            LogPosition::new(0, 0x100).unwrap(),
            LogPosition::new(1, 0).unwrap(),
            LogPosition::new(0x10, 5).unwrap(),
        ];
        let mut by_key: Vec<_> = positions.to_vec();
        by_key.sort_by_key(|p| log_key(*p));
        let mut by_pos = positions.to_vec();
        by_pos.sort();
        assert_eq!(by_key, by_pos);
    }
```

- [ ] **Step 2: Run to verify failure, then implement the keys**

Run: `cargo test -p varve-storage keys`
Expected: COMPILE FAIL (`log_key` not found; `varve_types` not a dependency).

Add to `crates/varve-storage/Cargo.toml` `[dependencies]`:

```toml
varve-types = { path = "../varve-types" }
```

Add to `crates/varve-storage/src/keys.rs`:

```rust
/// Log-object keys (spec §9): `v1/log/<epoch>/<offset-lexhex>.vlog`, one
/// object per group-commit batch, named by the batch's FIRST position. The
/// epoch directory is fixed-width hex (u16 ⇒ 4 digits) and the offset is
/// lex-hex, so lexicographic listing order == position order.
pub const LOG_PREFIX: &str = "v1/log";

pub fn log_key(first: varve_types::LogPosition) -> String {
    format!(
        "{LOG_PREFIX}/{:04x}/{}.vlog",
        first.epoch(),
        lex_hex(first.offset())
    )
}

/// Parses a log-object key back to its first position; `None` for anything
/// else (foreign keys under the prefix are ignored, never an error — same
/// policy as `manifest_block_id`).
pub fn parse_log_key(key: &str) -> Option<varve_types::LogPosition> {
    let rest = key.strip_prefix(LOG_PREFIX)?.strip_prefix('/')?;
    let (epoch_hex, offset_part) = rest.split_once('/')?;
    if epoch_hex.len() != 4
        || epoch_hex
            .chars()
            .any(|c| !c.is_ascii_hexdigit() || c.is_ascii_uppercase())
    {
        return None;
    }
    let epoch = u16::from_str_radix(epoch_hex, 16).ok()?;
    let offset = parse_lex_hex(offset_part.strip_suffix(".vlog")?)?;
    varve_types::LogPosition::new(epoch, offset).ok()
}
```

Run: `cargo test -p varve-storage keys`
Expected: PASS.

- [ ] **Step 3: Write the failing `ObjectStoreLog` tests**

Create `crates/varve-log/tests/object_store_log.rs`:

```rust
#![allow(clippy::unwrap_used)]
use bytes::Bytes;
use std::ops::Range;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use varve_config::{BuildContext, ConfigSection};
use varve_log::{log_registry, Log, LogError, LogRecord, ObjectStoreLog};
use varve_storage::{keys, memory_store, ObjectStore, StorageError};
use varve_types::LogPosition;

fn rec(tx_id: u64) -> LogRecord {
    LogRecord {
        tx_id,
        system_time_us: tx_id as i64,
        user: String::new(),
        effects: vec![],
    }
}

fn tx_ids(records: &[(LogPosition, LogRecord)]) -> Vec<u64> {
    records.iter().map(|(_, r)| r.tx_id).collect()
}

#[tokio::test]
async fn append_assigns_consecutive_positions_across_batches() {
    let log = ObjectStoreLog::new(memory_store());
    assert_eq!(
        log.append(vec![rec(1), rec(2)]).await.unwrap(),
        LogPosition::ZERO
    );
    assert_eq!(log.append(vec![rec(3)]).await.unwrap().offset(), 2);
    let all = log.tail(LogPosition::ZERO).await.unwrap();
    assert_eq!(tx_ids(&all), vec![1, 2, 3]);
    assert_eq!(
        all.iter().map(|(p, _)| p.offset()).collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
}

#[tokio::test]
async fn one_object_per_batch_under_the_spec_keys() {
    let store = memory_store();
    let log = ObjectStoreLog::new(Arc::clone(&store));
    log.append(vec![rec(1), rec(2)]).await.unwrap();
    log.append(vec![rec(3)]).await.unwrap();
    assert_eq!(
        store.list(keys::LOG_PREFIX).await.unwrap(),
        vec![
            "v1/log/0000/00.vlog".to_string(),
            "v1/log/0000/02.vlog".to_string()
        ]
    );
}

#[tokio::test]
async fn a_fresh_handle_continues_after_the_last_object() {
    let store = memory_store();
    ObjectStoreLog::new(Arc::clone(&store))
        .append(vec![rec(1), rec(2)])
        .await
        .unwrap();
    // A restart = a new handle over the same store: the lazy open scan
    // (list + count the last object's frames) restores the position.
    let reopened = ObjectStoreLog::new(Arc::clone(&store));
    assert_eq!(reopened.append(vec![rec(3)]).await.unwrap().offset(), 2);
    assert_eq!(
        tx_ids(&reopened.tail(LogPosition::ZERO).await.unwrap()),
        vec![1, 2, 3]
    );
}

#[tokio::test]
async fn survives_reopen_on_a_local_fs_store() {
    // Durability = the backing store's: a local-FS store round-trips the
    // log across a process-restart-equivalent (fresh store + fresh log).
    let dir = tempfile::tempdir().unwrap();
    {
        let store = varve_storage::local_store(dir.path()).unwrap();
        ObjectStoreLog::new(store)
            .append(vec![rec(1), rec(2)])
            .await
            .unwrap();
    }
    let store = varve_storage::local_store(dir.path()).unwrap();
    let log = ObjectStoreLog::new(store);
    assert_eq!(tx_ids(&log.tail(LogPosition::ZERO).await.unwrap()), vec![1, 2]);
    assert_eq!(log.append(vec![rec(3)]).await.unwrap().offset(), 2);
}

/// Counts backend object reads so the range test can pin object skipping.
struct CountingStore {
    inner: Arc<dyn ObjectStore>,
    gets: AtomicUsize,
}

#[async_trait::async_trait]
impl ObjectStore for CountingStore {
    async fn put(&self, key: &str, bytes: Bytes) -> Result<(), StorageError> {
        self.inner.put(key, bytes).await
    }
    async fn get(&self, key: &str) -> Result<Bytes, StorageError> {
        self.gets.fetch_add(1, Ordering::SeqCst);
        self.inner.get(key).await
    }
    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Bytes, StorageError> {
        self.inner.get_range(key, range).await
    }
    async fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
        self.inner.list(prefix).await
    }
}

#[tokio::test]
async fn read_range_filters_and_skips_disjoint_objects() {
    let counting = Arc::new(CountingStore {
        inner: memory_store(),
        gets: AtomicUsize::new(0),
    });
    let log = ObjectStoreLog::new(Arc::clone(&counting) as Arc<dyn ObjectStore>);
    log.append(vec![rec(1), rec(2)]).await.unwrap(); // positions 0,1
    log.append(vec![rec(3), rec(4)]).await.unwrap(); // positions 2,3
    log.append(vec![rec(5)]).await.unwrap(); // position 4
    counting.gets.store(0, Ordering::SeqCst);

    let mid = log
        .read_range(LogPosition::from_u64(3), LogPosition::from_u64(5))
        .await
        .unwrap();
    assert_eq!(tx_ids(&mid), vec![4, 5]);
    // Object 1 (positions 0–1) is provably below the range and is never
    // fetched; objects 2 and 3 are.
    assert_eq!(counting.gets.load(Ordering::SeqCst), 2);

    let empty = log
        .read_range(LogPosition::from_u64(5), LogPosition::from_u64(100))
        .await
        .unwrap();
    assert!(empty.is_empty());
}

#[tokio::test]
async fn trim_is_a_noop_and_positions_never_regress() {
    let log = ObjectStoreLog::new(memory_store());
    log.append(vec![rec(1), rec(2)]).await.unwrap();
    log.trim(LogPosition::from_u64(u64::MAX)).await.unwrap();
    // Nothing removed (no delete in the sovereign trait; GC = slice 8)…
    assert_eq!(tx_ids(&log.tail(LogPosition::ZERO).await.unwrap()), vec![1, 2]);
    // …and the sequence continues where it left off.
    assert_eq!(log.append(vec![rec(3)]).await.unwrap().offset(), 2);
}

#[tokio::test]
async fn corrupt_object_is_a_hard_error() {
    let store = memory_store();
    let log = ObjectStoreLog::new(Arc::clone(&store));
    log.append(vec![rec(1)]).await.unwrap();
    // Object PUTs are atomic, so damage is never a recoverable torn tail —
    // decode is strict.
    store
        .put(
            &keys::log_key(LogPosition::from_u64(1)),
            Bytes::from_static(b"\xFF\xFF\xFF"),
        )
        .await
        .unwrap();
    assert!(matches!(
        log.tail(LogPosition::ZERO).await,
        Err(LogError::Corrupt { .. })
    ));
}

#[tokio::test]
async fn empty_append_is_rejected() {
    let log = ObjectStoreLog::new(memory_store());
    assert!(matches!(
        log.append(vec![]).await,
        Err(LogError::EmptyAppend)
    ));
}

#[tokio::test]
async fn factory_requires_the_storage_component() {
    let reg = log_registry();
    assert_eq!(reg.names(), vec!["local", "memory", "object-store"]);
    let err = match reg.build("object-store", &ConfigSection::empty(), &BuildContext::empty()) {
        Ok(_) => panic!("expected build without a storage component to fail"),
        Err(e) => e.to_string(),
    };
    assert!(err.contains("storage component"), "{err}");
}

#[tokio::test]
async fn factory_builds_with_the_storage_component() {
    let mut ctx = BuildContext::empty();
    ctx.insert(memory_store());
    let log = log_registry()
        .build("object-store", &ConfigSection::empty(), &ctx)
        .unwrap();
    log.append(vec![rec(1)]).await.unwrap();
    assert_eq!(log.tail(LogPosition::ZERO).await.unwrap().len(), 1);
}
```

- [ ] **Step 4: Run to verify failure**

Run: `cargo test -p varve-log --test object_store_log`
Expected: COMPILE FAIL (`ObjectStoreLog` not found; varve-log has no varve-storage dependency).

- [ ] **Step 5: Implement feature, error variant, log, and factory**

`crates/varve-log/Cargo.toml` — extend `[features]` and `[dependencies]`:

```toml
[features]
default = ["object-store"]
# One object per group-commit batch over the shared block store (spec §6).
object-store = ["dep:varve-storage", "dep:bytes"]
# Test-only crash hooks for the varve-testkit kill -9 harness. Inert unless
# the VARVE_CRASH_TRIGGER env var points at an armed trigger file.
fault-injection = []
```

```toml
varve-storage = { path = "../varve-storage", optional = true }
bytes = { workspace = true, optional = true }
```

`crates/varve-log/src/log.rs` — add the variant to `LogError`:

```rust
    #[cfg(feature = "object-store")]
    #[error("log storage backend error: {0}")]
    Storage(#[from] varve_storage::StorageError),
```

Create `crates/varve-log/src/object_store.rs`:

```rust
//! `log/object-store` (spec §6): one object per group-commit batch at
//! `v1/log/<epoch>/<offset-lexhex>.vlog`, sharing the block store (D7:
//! plain PUT/GET/LIST only — the designated writer assigns positions
//! locally, so no CAS is ever needed). Durability is the backing store's: a
//! PUT that returns Ok is exactly as durable as the backend makes it.
//!
//! `trim` is a documented NO-OP: the sovereign `ObjectStore` trait has no
//! delete (slice-4 decision); superseded log objects are swept by slice-8
//! GC. Replay cost stays bounded regardless, because recovery reads only
//! `tail(manifest.watermark)`.

use crate::log::{Log, LogError};
use crate::record::LogRecord;
use bytes::Bytes;
use std::sync::Arc;
use varve_config::{BuildContext, ComponentFactory, ConfigSection, RegistryError};
use varve_storage::{keys, ObjectStore};
use varve_types::LogPosition;

/// Frame header: `len: u32 LE` + `crc: u32 LE` (CRC32C of the payload) —
/// the exact `LocalLog` frame grammar, so both durable backends share one
/// on-disk format. Decoding here is STRICT (any malformed frame is
/// `Corrupt`): object PUTs are atomic, so a torn tail cannot exist.
const FRAME_HEADER: usize = 8;

pub struct ObjectStoreLog {
    store: Arc<dyn ObjectStore>,
    /// Position of the next appended record. `None` until first use:
    /// factories are synchronous, so the open-time store scan happens
    /// lazily on the first `append` (reads never need it). Exactly one
    /// writer exists per database (D5), so nothing can append between
    /// construction and that first scan.
    next: tokio::sync::Mutex<Option<LogPosition>>,
}

impl ObjectStoreLog {
    pub fn new(store: Arc<dyn ObjectStore>) -> ObjectStoreLog {
        ObjectStoreLog {
            store,
            next: tokio::sync::Mutex::new(None),
        }
    }

    /// Sorted `(first-position, key)` pairs for every log object. Foreign
    /// keys under the prefix are ignored (`parse_log_key` policy).
    async fn list_objects(&self) -> Result<Vec<(LogPosition, String)>, LogError> {
        let listed = self.store.list(keys::LOG_PREFIX).await?;
        let mut objects: Vec<(LogPosition, String)> = listed
            .into_iter()
            .filter_map(|k| keys::parse_log_key(&k).map(|p| (p, k)))
            .collect();
        objects.sort_by_key(|(p, _)| *p);
        Ok(objects)
    }

    /// The position after the last stored record (ZERO on a fresh store):
    /// list the prefix, read the LAST object, count its frames.
    async fn recover_next(&self) -> Result<LogPosition, LogError> {
        let objects = self.list_objects().await?;
        match objects.last() {
            None => Ok(LogPosition::ZERO),
            Some((first, key)) => {
                let bytes = self.store.get(key).await?;
                let count = decode_object(key, &bytes)?.len() as u64;
                Ok(first.advance(count)?)
            }
        }
    }
}

/// Strict frame walk: every byte must belong to a complete, CRC-valid frame.
fn decode_object(key: &str, bytes: &[u8]) -> Result<Vec<LogRecord>, LogError> {
    let mut records = Vec::new();
    let mut off = 0usize;
    while off < bytes.len() {
        if bytes.len() - off < FRAME_HEADER {
            return Err(corrupt(key, off, "truncated frame header"));
        }
        let len = u32::from_le_bytes([bytes[off], bytes[off + 1], bytes[off + 2], bytes[off + 3]])
            as usize;
        let crc = u32::from_le_bytes([
            bytes[off + 4],
            bytes[off + 5],
            bytes[off + 6],
            bytes[off + 7],
        ]);
        if bytes.len() - off - FRAME_HEADER < len {
            return Err(corrupt(key, off, "truncated frame payload"));
        }
        let payload = &bytes[off + FRAME_HEADER..off + FRAME_HEADER + len];
        if crc32c::crc32c(payload) != crc {
            return Err(corrupt(key, off, "CRC mismatch"));
        }
        records.push(LogRecord::from_wire(payload)?);
        off += FRAME_HEADER + len;
    }
    if records.is_empty() {
        return Err(corrupt(key, 0, "empty log object"));
    }
    Ok(records)
}

fn corrupt(key: &str, off: usize, reason: &str) -> LogError {
    LogError::Corrupt {
        path: key.to_string(),
        offset: off as u64,
        reason: reason.to_string(),
    }
}

#[async_trait::async_trait]
impl Log for ObjectStoreLog {
    async fn append(&self, records: Vec<LogRecord>) -> Result<LogPosition, LogError> {
        if records.is_empty() {
            return Err(LogError::EmptyAppend);
        }
        let mut next = self.next.lock().await;
        let first = match *next {
            Some(position) => position,
            None => self.recover_next().await?,
        };
        let after_batch = first.advance(records.len() as u64)?; // fail before writing
        let mut buf = Vec::new();
        for record in &records {
            let payload = record.to_wire();
            buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
            buf.extend_from_slice(&crc32c::crc32c(&payload).to_le_bytes());
            buf.extend_from_slice(&payload);
        }
        // ONE PUT per batch (spec §6): commit latency ≈ backend PUT latency,
        // throughput scales with group-commit batching.
        self.store
            .put(&keys::log_key(first), Bytes::from(buf))
            .await?;
        *next = Some(after_batch);
        Ok(first)
    }

    async fn read_range(
        &self,
        from: LogPosition,
        to: LogPosition,
    ) -> Result<Vec<(LogPosition, LogRecord)>, LogError> {
        let objects = self.list_objects().await?;
        let mut out = Vec::new();
        for (i, (first, key)) in objects.iter().enumerate() {
            if *first >= to {
                break;
            }
            // An object's span ends where the next object begins, so one
            // whose successor starts at or below `from` is entirely below
            // the range. The LAST object's span is unknown without reading
            // it, so this bound never skips it.
            if let Some((next_first, _)) = objects.get(i + 1) {
                if *next_first <= from {
                    continue;
                }
            }
            let bytes = self.store.get(key).await?;
            let mut position = *first;
            for record in decode_object(key, &bytes)? {
                if position >= from && position < to {
                    out.push((position, record));
                }
                position = position.advance(1)?;
            }
        }
        Ok(out)
    }

    /// NO-OP (documented): the sovereign store exposes no delete, so
    /// superseded objects stay until slice-8 GC. The `Log::trim` contract
    /// ("earlier records MAY be retained") is satisfied trivially, and
    /// positions never regress because `next` is tracked independently of
    /// what a trim could remove.
    async fn trim(&self, _up_to: LogPosition) -> Result<(), LogError> {
        Ok(())
    }
}

/// Registry factory: `[log] backend = "object-store"`. Consumes the
/// already-built storage component from the `BuildContext` (spec §4 ctx) —
/// the log shares the block store's bucket and keyspace (spec §9).
pub struct ObjectStoreLogFactory;

impl ComponentFactory<dyn Log> for ObjectStoreLogFactory {
    fn name(&self) -> &'static str {
        "object-store"
    }

    fn build(&self, _cfg: &ConfigSection, ctx: &BuildContext) -> Result<Arc<dyn Log>, RegistryError> {
        let store = ctx.get::<Arc<dyn ObjectStore>>().ok_or_else(|| {
            RegistryError::Build {
                kind: "log",
                name: "object-store".into(),
                source: "no storage component in the build context; the \
                         object-store log shares the [storage] backend — open \
                         through Db::open, which builds storage first"
                    .to_string()
                    .into(),
            }
        })?;
        Ok(Arc::new(ObjectStoreLog::new(store)))
    }
}
```

`crates/varve-log/src/lib.rs` — wire it up:

```rust
#[cfg(feature = "object-store")]
pub mod object_store;
```

```rust
#[cfg(feature = "object-store")]
pub use object_store::{ObjectStoreLog, ObjectStoreLogFactory};
```

and inside `log_registry()`:

```rust
    #[cfg(feature = "object-store")]
    register_builtin(&mut reg, Box::new(object_store::ObjectStoreLogFactory));
```

Update `crates/varve-engine/src/registries.rs` `builtins_cover_log_and_clock`:

```rust
        assert_eq!(
            registries.log.names(),
            vec!["local", "memory", "object-store"]
        );
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `cargo test -p varve-log && cargo test -p varve-engine registries`
Expected: PASS — all 11 new tests plus the existing suites. (`memory_log.rs`'s unknown-name test asserts only `contains("kafka")`/`contains("memory")`, so the longer available-list still satisfies it.)

- [ ] **Step 7: Full gate + commit**

Run: `cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings`
Expected: green.

```bash
git add -A
git commit -m "feat: log/object-store — one object per group-commit batch over the shared block store"
```

---
### Task 4: Engine wiring — storage builds first, the log factory gets the raw store

`Db::open_with` currently builds the log before storage. Reorder: storage → `BuildContext` (carrying the RAW, uncached store — log traffic must not fill the query-path cache) → log → cache/clock. The volatile-store guard (decision 11) is unchanged and still fires before the log is built.

**Files:**
- Modify: `crates/varve-engine/src/db.rs` (`open_with`)
- Test: `crates/varve/tests/object_log.rs` (new)

**Interfaces:**
- Consumes: `BuildContext` (Task 1), `log/object-store` factory (Task 3).
- Produces: `[log] backend = "object-store"` works through `Db::open`/`Db::open_with`; the context invariant later tasks rely on — **the `BuildContext` handed to log/clock (and Task-6 cache) factories contains `Arc<dyn ObjectStore>` = the raw store**.

- [ ] **Step 1: Write the failing tests**

Create `crates/varve/tests/object_log.rs`:

```rust
#![allow(clippy::unwrap_used)]
use std::path::Path;
use varve::{Config, Db};

fn rows(batches: &[varve::RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

fn toml_escaped(dir: &Path) -> String {
    format!("{:?}", dir.display().to_string())
}

#[tokio::test]
async fn object_store_log_backend_works_end_to_end() {
    // memory storage + object-store log: everything volatile TOGETHER
    // (shared-fate, like Db::memory) — the decision-11 guard only rejects a
    // DURABLE log over a volatile block store.
    let config = Config::from_toml_str(
        "[log]\nbackend = \"object-store\"\n[storage]\nbackend = \"memory\"\n",
    )
    .unwrap();
    let db = Db::open(config).await.unwrap();
    db.execute("INSERT (:Person {_id: 1, name: 'Ada'})")
        .await
        .unwrap();
    let batches = db
        .query("MATCH (p:Person) WHERE p.name = 'Ada' RETURN p.name")
        .await
        .unwrap();
    assert_eq!(rows(&batches), 1);
}

#[tokio::test]
async fn object_store_log_replays_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let toml = format!(
        "[log]\nbackend = \"object-store\"\n\
         [storage]\nbackend = \"local\"\n[storage.local]\ndir = {}\n",
        toml_escaped(&dir.path().join("store"))
    );
    {
        let db = Db::open(Config::from_toml_str(&toml).unwrap()).await.unwrap();
        // execute() acks only after the durable PUT, so both records are in
        // v1/log/ the moment these return — dropping the Db is safe.
        db.execute("INSERT (:P {_id: 1, name: 'a'})").await.unwrap();
        db.execute("INSERT (:P {_id: 2, name: 'b'})").await.unwrap();
    }
    let db = Db::open(Config::from_toml_str(&toml).unwrap()).await.unwrap();
    let batches = db.query("MATCH (p:P) RETURN p.name").await.unwrap();
    assert_eq!(rows(&batches), 2, "slice-3 replay through the object log");
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p varve --test object_log`
Expected: FAIL — `failed to build log 'object-store': no storage component in the build context` (the factory exists since Task 3, but `open_with` still hands it an empty context).

- [ ] **Step 3: Reorder `open_with`**

Replace the full body of `Db::open_with` in `crates/varve-engine/src/db.rs`. The tail (cache tuning, clock, writer tuning, recovery) is today's code except that the cache wraps `raw_store` and every registry build gains a context argument — Task 6 replaces the `CacheTuning` block again, so keep that block byte-compatible with what the file currently has:

```rust
    pub async fn open_with(config: &Config, registries: &Registries) -> Result<Db, EngineError> {
        // Storage FIRST: later factories may consume the raw store through
        // the BuildContext (spec §4 ctx) — the object-store log shares the
        // block store's bucket and keyspace (spec §9).
        let storage_section = config
            .section("storage")
            .unwrap_or_else(ConfigSection::empty);
        let storage_backend = storage_section.backend().unwrap_or("memory").to_string();
        let raw_store =
            registries
                .storage
                .build(&storage_backend, &storage_section, &BuildContext::empty())?;

        // The RAW store goes into the context: log traffic must not flow
        // through (or fill) the query-path cache wired below.
        let mut ctx = BuildContext::empty();
        ctx.insert(Arc::clone(&raw_store));

        let log_section = config.section("log").unwrap_or_else(ConfigSection::empty);
        let log_backend = log_section.backend().unwrap_or("memory").to_string();
        // Decision 11 (slice 4): a DURABLE log over a volatile block store
        // would trim durable data while blocks evaporate on restart.
        if log_backend == "local" && storage_backend == "memory" {
            return Err(EngineError::VolatileBlockStore);
        }
        let log = registries.log.build(&log_backend, &log_section, &ctx)?;

        // Slice-4 cache wiring, now over raw_store (replaced in Task 6).
        let cache_tuning: CacheTuning = config
            .section("cache")
            .unwrap_or_else(ConfigSection::empty)
            .get()?;
        let store: Arc<dyn ObjectStore> = Arc::new(CachedStore::new(
            Arc::clone(&raw_store),
            Arc::new(MemoryCache::new(cache_tuning.memory_max_bytes)),
        ));

        let clock_section = config.section("clock").unwrap_or_else(ConfigSection::empty);
        let clock = registries.clock.build(
            clock_section.backend().unwrap_or("system"),
            &clock_section,
            &ctx,
        )?;

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

(The `BuildContext` import landed in Task 1. If the tail differs cosmetically from the real file, keep the real code — the only changes this task makes are the storage-first reorder, the context, and wrapping `raw_store`.)

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p varve --test object_log && cargo test -p varve`
Expected: PASS — both new tests plus every existing varve test (`durability.rs` pins the decision-11 guard; it must still pass).

- [ ] **Step 5: Full gate + commit**

Run: `cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings`
Expected: green.

```bash
git add -A
git commit -m "feat: Db::open builds storage first and hands the raw store to log factories via BuildContext"
```

---

### Task 5: Disk cache tier — restartable `(path, range)`-keyed files

A `CacheTier` whose entries are individual self-describing files: header = the cache key, body = the value. The index rebuilds by walking the directory (restart survival) ordered by file mtime (approximate LRU across restarts); at runtime recency is an in-memory tick, persisted by touching mtime on hits.

**Files:**
- Modify: `crates/varve-storage/Cargo.toml` (+ `xxhash-rust` workspace dep)
- Create: `crates/varve-storage/src/disk.rs` (DiskCache + unit tests; the factory arrives in Task 6)
- Modify: `crates/varve-storage/src/lib.rs` (module + export)

**Interfaces:**
- Consumes: `CacheKey`, `CacheTier` (cache.rs), `StorageError`.
- Produces:
  ```rust
  pub struct DiskCache { /* private */ }
  impl DiskCache {
      /// Opens (creating `dir`), rebuilds the index from directory contents.
      pub fn open(dir: &Path, max_bytes: u64) -> Result<DiskCache, StorageError>;
  }
  impl CacheTier for DiskCache { /* get / insert / invalidate_path */ }
  ```

- [ ] **Step 1: Write the failing tests**

Create `crates/varve-storage/src/disk.rs` with the tests first (module skeleton + tests; implementation lands in Step 3):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::{CacheKey, CacheTier};
    use bytes::Bytes;
    use std::time::Duration;

    fn key(path: &str, range: Option<(u64, u64)>) -> CacheKey {
        CacheKey {
            path: path.into(),
            range,
        }
    }

    #[test]
    fn round_trips_values_by_key_and_range() {
        let dir = tempfile::tempdir().unwrap();
        let cache = DiskCache::open(dir.path(), 1024).unwrap();
        cache.insert(key("v1/a", None), Bytes::from_static(b"whole"));
        cache.insert(key("v1/a", Some((0, 2))), Bytes::from_static(b"wh"));
        assert_eq!(
            cache.get(&key("v1/a", None)),
            Some(Bytes::from_static(b"whole"))
        );
        assert_eq!(
            cache.get(&key("v1/a", Some((0, 2)))),
            Some(Bytes::from_static(b"wh")),
            "ranges are distinct entries"
        );
        assert_eq!(cache.get(&key("v1/b", None)), None);
    }

    #[test]
    fn survives_restart() {
        let dir = tempfile::tempdir().unwrap();
        {
            let cache = DiskCache::open(dir.path(), 1024).unwrap();
            cache.insert(key("v1/a", None), Bytes::from_static(b"aaaa"));
            cache.insert(key("v1/b", Some((3, 9))), Bytes::from_static(b"bbbb"));
        }
        let cache = DiskCache::open(dir.path(), 1024).unwrap();
        assert_eq!(
            cache.get(&key("v1/a", None)),
            Some(Bytes::from_static(b"aaaa"))
        );
        assert_eq!(
            cache.get(&key("v1/b", Some((3, 9)))),
            Some(Bytes::from_static(b"bbbb"))
        );
    }

    #[test]
    fn enforces_the_byte_budget_lru() {
        let dir = tempfile::tempdir().unwrap();
        // Entry size = header + value. Header for a 1-char path with no
        // range = 4 (magic) + 4 (len) + 1 (path) + 1 (tag) = 10; values are
        // 100 bytes ⇒ 110 per entry. Budget fits two.
        let cache = DiskCache::open(dir.path(), 250).unwrap();
        let value = Bytes::from(vec![7u8; 100]);
        cache.insert(key("a", None), value.clone());
        cache.insert(key("b", None), value.clone());
        assert!(cache.get(&key("a", None)).is_some()); // touch a → b is LRU
        cache.insert(key("c", None), value.clone());
        assert!(cache.get(&key("a", None)).is_some());
        assert!(cache.get(&key("b", None)).is_none(), "b was evicted");
        assert!(cache.get(&key("c", None)).is_some());
        // Files on disk match the index: exactly 2 .vcache entries remain.
        let entries = std::fs::read_dir(dir.path())
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .unwrap()
                    .path()
                    .extension()
                    .is_some_and(|x| x == "vcache")
            })
            .count();
        assert_eq!(entries, 2);
    }

    #[test]
    fn restart_preserves_lru_order_via_mtime() {
        let dir = tempfile::tempdir().unwrap();
        let value = Bytes::from(vec![7u8; 100]);
        {
            let cache = DiskCache::open(dir.path(), 1024).unwrap();
            cache.insert(key("a", None), value.clone());
            std::thread::sleep(Duration::from_millis(50)); // mtime resolution
            cache.insert(key("b", None), value.clone());
            std::thread::sleep(Duration::from_millis(50));
            cache.get(&key("a", None)); // touch: a is now newer than b
        }
        let cache = DiskCache::open(dir.path(), 250).unwrap();
        cache.insert(key("c", None), value.clone()); // forces one eviction
        assert!(cache.get(&key("a", None)).is_some(), "a was touched last");
        assert!(cache.get(&key("b", None)).is_none(), "b was oldest by mtime");
    }

    #[test]
    fn oversized_values_are_never_cached() {
        let dir = tempfile::tempdir().unwrap();
        let cache = DiskCache::open(dir.path(), 64).unwrap();
        cache.insert(key("big", None), Bytes::from(vec![0u8; 128]));
        assert_eq!(cache.get(&key("big", None)), None);
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    }

    #[test]
    fn invalidate_path_removes_all_ranges_and_files() {
        let dir = tempfile::tempdir().unwrap();
        let cache = DiskCache::open(dir.path(), 1024).unwrap();
        cache.insert(key("v1/a", None), Bytes::from_static(b"1"));
        cache.insert(key("v1/a", Some((0, 1))), Bytes::from_static(b"2"));
        cache.insert(key("v1/b", None), Bytes::from_static(b"3"));
        cache.invalidate_path("v1/a");
        assert_eq!(cache.get(&key("v1/a", None)), None);
        assert_eq!(cache.get(&key("v1/a", Some((0, 1)))), None);
        assert!(cache.get(&key("v1/b", None)).is_some());
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[test]
    fn malformed_and_temp_files_are_swept_at_open_foreign_files_kept() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("junk.vcache"), b"not a header").unwrap();
        std::fs::write(dir.path().join("leftover.tmp3"), b"crashed write").unwrap();
        std::fs::write(dir.path().join("notes.txt"), b"foreign").unwrap();
        let cache = DiskCache::open(dir.path(), 1024).unwrap();
        assert_eq!(cache.get(&key("junk", None)), None);
        let remaining: Vec<String> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(remaining, vec!["notes.txt".to_string()]);
    }

    #[test]
    fn corrupt_entry_self_heals_to_a_miss() {
        let dir = tempfile::tempdir().unwrap();
        let cache = DiskCache::open(dir.path(), 1024).unwrap();
        cache.insert(key("v1/a", None), Bytes::from_static(b"good"));
        // Corrupt the entry file behind the cache's back.
        let file = std::fs::read_dir(dir.path())
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        std::fs::write(&file, b"garbage").unwrap();
        assert_eq!(cache.get(&key("v1/a", None)), None);
        assert_eq!(
            std::fs::read_dir(dir.path()).unwrap().count(),
            0,
            "the broken file was removed"
        );
        assert_eq!(cache.get(&key("v1/a", None)), None, "stays a clean miss");
    }
}
```

Register the module in `crates/varve-storage/src/lib.rs`:

```rust
pub mod disk;
```

```rust
pub use disk::DiskCache;
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p varve-storage disk`
Expected: COMPILE FAIL — `DiskCache` not defined.

- [ ] **Step 3: Implement `DiskCache`**

Add `xxhash-rust` to `crates/varve-storage/Cargo.toml` `[dependencies]`:

```toml
xxhash-rust = { workspace = true }
```

Prepend the implementation to `crates/varve-storage/src/disk.rs` (above the tests):

```rust
//! Disk cache tier (spec §9): `(path, byte-range)`-keyed files that survive
//! restarts. Each entry is ONE self-describing file (header = the cache
//! key, body = the value), so the index rebuilds by walking the directory
//! on open — no separate index file to corrupt. Recency is an in-memory LRU
//! tick at runtime, persisted as file mtime (touched on every hit) so
//! restart ordering approximates LRU.
//!
//! Reads copy into owned `Bytes`, so eviction can never invalidate a
//! handed-out buffer — the roadmap's "ref-count pinning while mapped" is
//! vacuously satisfied until an mmap path exists (post-v1). I/O is
//! synchronous on the caller's thread: entries are page-sized objects, the
//! same v1 tradeoff as `MemoryCache`'s O(n) eviction scan. A poisoned lock
//! degrades to cache-miss behavior, never an error.

use crate::cache::{CacheKey, CacheTier};
use crate::store::StorageError;
use bytes::Bytes;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

const MAGIC: &[u8; 4] = b"VCA1";
const SUFFIX: &str = "vcache";

/// Header: magic · path-len u32 LE · path bytes · range tag u8 (0 = whole
/// object, 1 = range) · [start u64 LE · end u64 LE].
fn encode_key(key: &CacheKey) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + 4 + key.path.len() + 17);
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&(key.path.len() as u32).to_le_bytes());
    out.extend_from_slice(key.path.as_bytes());
    match key.range {
        None => out.push(0),
        Some((start, end)) => {
            out.push(1);
            out.extend_from_slice(&start.to_le_bytes());
            out.extend_from_slice(&end.to_le_bytes());
        }
    }
    out
}

/// Splits a stored file into `(key, value)`; `None` = malformed.
fn decode_entry(bytes: &[u8]) -> Option<(CacheKey, Bytes)> {
    if bytes.len() < 9 || &bytes[..4] != MAGIC {
        return None;
    }
    let path_len = u32::from_le_bytes(bytes[4..8].try_into().ok()?) as usize;
    let mut off = 8;
    let path = String::from_utf8(bytes.get(off..off + path_len)?.to_vec()).ok()?;
    off += path_len;
    let tag = *bytes.get(off)?;
    off += 1;
    let range = match tag {
        0 => None,
        1 => {
            let start = u64::from_le_bytes(bytes.get(off..off + 8)?.try_into().ok()?);
            let end = u64::from_le_bytes(bytes.get(off + 8..off + 16)?.try_into().ok()?);
            off += 16;
            Some((start, end))
        }
        _ => return None,
    };
    Some((CacheKey { path, range }, Bytes::copy_from_slice(bytes.get(off..)?)))
}

/// Entry file name: 128-bit xxh3 of the encoded key. A collision (2⁻¹²⁸) is
/// harmless: `get` verifies the decoded key and self-heals to a miss.
fn file_name(key: &CacheKey) -> String {
    format!(
        "{:032x}.{SUFFIX}",
        xxhash_rust::xxh3::xxh3_128(&encode_key(key))
    )
}

struct DiskEntry {
    file: PathBuf,
    bytes: u64,
    last_used: u64,
}

struct DiskInner {
    entries: HashMap<CacheKey, DiskEntry>,
    bytes: u64,
    tick: u64,
}

/// Removes least-recently-used entries (and their files) until the budget
/// holds. File removal is best-effort: the index entry goes regardless, and
/// a leftover file is re-adopted or swept at the next `open`.
fn evict_over_budget(inner: &mut DiskInner, max_bytes: u64) {
    while inner.bytes > max_bytes {
        let Some(oldest) = inner
            .entries
            .iter()
            .min_by_key(|(_, e)| e.last_used)
            .map(|(k, _)| k.clone())
        else {
            break;
        };
        if let Some(e) = inner.entries.remove(&oldest) {
            inner.bytes -= e.bytes;
            let _ = fs::remove_file(&e.file);
        }
    }
}

pub struct DiskCache {
    dir: PathBuf,
    max_bytes: u64,
    inner: Mutex<DiskInner>,
}

impl DiskCache {
    /// Opens (creating `dir` if needed) and rebuilds the index from the
    /// directory: entries re-rank by file mtime so LRU order approximately
    /// survives restarts. Malformed `.vcache` files and crashed `.tmpN`
    /// leftovers are removed; anything else is left alone. If the walked
    /// total exceeds `max_bytes`, the oldest entries are evicted right away.
    pub fn open(dir: &Path, max_bytes: u64) -> Result<DiskCache, StorageError> {
        fs::create_dir_all(dir)?;
        let mut found: Vec<(std::time::SystemTime, CacheKey, PathBuf, u64)> = Vec::new();
        for entry in fs::read_dir(dir)? {
            let path = entry?.path();
            let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
            if ext.starts_with("tmp") {
                let _ = fs::remove_file(&path); // crashed mid-insert
                continue;
            }
            if ext != SUFFIX {
                continue;
            }
            match fs::read(&path).ok().and_then(|b| decode_entry(&b)) {
                Some((key, _)) => {
                    let meta = fs::metadata(&path)?;
                    let mtime = meta.modified().unwrap_or(std::time::UNIX_EPOCH);
                    found.push((mtime, key, path, meta.len()));
                }
                None => {
                    let _ = fs::remove_file(&path); // malformed: sweep
                }
            }
        }
        found.sort_by(|a, b| a.0.cmp(&b.0)); // oldest first ⇒ lowest tick
        let mut inner = DiskInner {
            entries: HashMap::new(),
            bytes: 0,
            tick: 0,
        };
        for (_, key, file, len) in found {
            inner.tick += 1;
            inner.bytes += len;
            let tick = inner.tick;
            inner.entries.insert(
                key,
                DiskEntry {
                    file,
                    bytes: len,
                    last_used: tick,
                },
            );
        }
        evict_over_budget(&mut inner, max_bytes);
        Ok(DiskCache {
            dir: dir.to_path_buf(),
            max_bytes,
            inner: Mutex::new(inner),
        })
    }
}

impl CacheTier for DiskCache {
    fn get(&self, key: &CacheKey) -> Option<Bytes> {
        let Ok(mut inner) = self.inner.lock() else {
            return None;
        };
        inner.tick += 1;
        let tick = inner.tick;
        let file = {
            let entry = inner.entries.get_mut(key)?;
            entry.last_used = tick;
            entry.file.clone()
        };
        match fs::read(&file).ok().and_then(|b| decode_entry(&b)) {
            Some((stored, value)) if stored == *key => {
                // Touch mtime (best-effort) so LRU order survives a restart.
                let _ = fs::File::options()
                    .append(true)
                    .open(&file)
                    .and_then(|f| f.set_modified(std::time::SystemTime::now()));
                Some(value)
            }
            _ => {
                // Vanished, corrupt, or a hash-collision mismatch:
                // self-heal to a miss; the read-through wrapper refills.
                if let Some(e) = inner.entries.remove(key) {
                    inner.bytes -= e.bytes;
                    let _ = fs::remove_file(&e.file);
                }
                None
            }
        }
    }

    fn insert(&self, key: CacheKey, value: Bytes) {
        let mut body = encode_key(&key);
        body.extend_from_slice(&value);
        let total = body.len() as u64;
        if total > self.max_bytes {
            return;
        }
        let Ok(mut inner) = self.inner.lock() else {
            return;
        };
        inner.tick += 1;
        let tick = inner.tick;
        let file = self.dir.join(file_name(&key));
        // Write-temp-then-rename: a crash mid-write leaves only a .tmpN that
        // the next open sweeps — never a half-readable entry.
        let tmp = file.with_extension(format!("tmp{tick}"));
        let wrote = fs::write(&tmp, &body).and_then(|()| fs::rename(&tmp, &file));
        if wrote.is_err() {
            let _ = fs::remove_file(&tmp);
            return;
        }
        if let Some(old) = inner.entries.insert(
            key,
            DiskEntry {
                file,
                bytes: total,
                last_used: tick,
            },
        ) {
            inner.bytes -= old.bytes;
        }
        inner.bytes += total;
        evict_over_budget(&mut inner, self.max_bytes);
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
                inner.bytes -= e.bytes;
                let _ = fs::remove_file(&e.file);
            }
        }
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p varve-storage disk`
Expected: PASS — all 8 tests. (`File::set_modified` is std, stable since 1.75; toolchain is 1.93.)

- [ ] **Step 5: Full gate + commit**

Run: `cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings`
Expected: green.

```bash
git add -A
git commit -m "feat: disk cache tier — restartable (path, range)-keyed files with an LRU byte budget"
```

---
### Task 6: Cache registry — `[cache] tiers` selected by name

The deferred slice-4 cache registry. `Registry<dyn CacheTier>` with builtin `memory` and `disk` factories; the engine folds the configured tier list into a `CachedStore` chain, outermost-first. The slice-4 `[cache] memory_max_bytes` key is REMOVED (no back-compat): tuning moves to `[cache.memory] max_bytes` / `[cache.disk] dir, max_bytes`.

**Files:**
- Modify: `crates/varve-storage/src/cache.rs` (+ `MemoryCacheFactory`)
- Modify: `crates/varve-storage/src/disk.rs` (+ `DiskCacheFactory`)
- Modify: `crates/varve-storage/src/lib.rs` (`cache_registry()` + exports)
- Modify: `crates/varve-engine/src/registries.rs` (`Registries.cache` + names test)
- Modify: `crates/varve-engine/src/db.rs` (`CacheConfig` replaces `CacheTuning`; tier fold in `open_with`)
- Test: `crates/varve/tests/cache_tiers.rs` (new)

**Interfaces:**
- Consumes: `CacheTier`, `MemoryCache` (existing), `DiskCache` (Task 5), `CachedStore` composition, `BuildContext`.
- Produces:
  ```rust
  // varve-storage
  pub struct MemoryCacheFactory;   // ComponentFactory<dyn CacheTier>, name "memory"
  pub struct DiskCacheFactory;     // ComponentFactory<dyn CacheTier>, name "disk"
  pub fn cache_registry() -> Registry<dyn CacheTier>;
  // varve-engine
  pub struct Registries { pub log, pub clock, pub storage, pub cache: Registry<dyn CacheTier> }
  ```
  Config contract: `[cache] tiers = ["memory"]` (default; outermost-first; `[]` = uncached), `[cache.memory] max_bytes` (default 536870912), `[cache.disk] dir` (required) + `max_bytes` (default 53687091200).

- [ ] **Step 1: Write the failing tests**

Create `crates/varve/tests/cache_tiers.rs`:

```rust
#![allow(clippy::unwrap_used)]
use std::path::Path;
use varve::{Config, Db};

fn rows(batches: &[varve::RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

fn toml_escaped(dir: &Path) -> String {
    format!("{:?}", dir.display().to_string())
}

/// local log + local storage + the named cache tiers under `root`.
fn tiers_config(root: &Path, tiers: &str, max_block_rows: usize) -> Config {
    Config::from_toml_str(&format!(
        "[log]\nbackend = \"local\"\ngroup_commit_window_ms = 1\n\
         [log.local]\ndir = {}\n\
         [storage]\nbackend = \"local\"\nmax_block_rows = {max_block_rows}\n\
         [storage.local]\ndir = {}\n\
         [cache]\ntiers = [{tiers}]\n\
         [cache.disk]\ndir = {}\n",
        toml_escaped(&root.join("log")),
        toml_escaped(&root.join("store")),
        toml_escaped(&root.join("cache")),
    ))
    .unwrap()
}

/// Flushes happen asynchronously after acks (same helper as blocks.rs).
async fn wait_for_flush(root: &Path) {
    let blocks = root.join("store").join("v1").join("blocks");
    for _ in 0..200 {
        if blocks
            .read_dir()
            .map(|mut d| d.next().is_some())
            .unwrap_or(false)
        {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    panic!("no manifest appeared under {blocks:?} within 5s");
}

#[tokio::test]
async fn disk_tier_selected_by_name_populates_and_survives_restart() {
    let root = tempfile::tempdir().unwrap();
    {
        let db = Db::open(tiers_config(root.path(), "\"disk\"", 3)).await.unwrap();
        for i in 1..=3 {
            db.execute(&format!("INSERT (:P {{_id: {i}, name: 'p{i}'}})"))
                .await
                .unwrap();
        }
        wait_for_flush(root.path()).await;
        // Reading persisted pages fills the disk tier.
        let all = db.query("MATCH (p:P) RETURN p.name").await.unwrap();
        assert_eq!(rows(&all), 3);
    }
    let cache_files = std::fs::read_dir(root.path().join("cache")).unwrap().count();
    assert!(cache_files > 0, "query filled the disk cache");

    // Restart: the SAME cache dir is rebuilt and correctness holds.
    let db = Db::open(tiers_config(root.path(), "\"disk\"", 3)).await.unwrap();
    let all = db.query("MATCH (p:P) RETURN p.name").await.unwrap();
    assert_eq!(rows(&all), 3);
}

#[tokio::test]
async fn memory_and_disk_chain_composes() {
    let root = tempfile::tempdir().unwrap();
    let db = Db::open(tiers_config(root.path(), "\"memory\", \"disk\"", 1000))
        .await
        .unwrap();
    db.execute("INSERT (:P {_id: 1, name: 'a'})").await.unwrap();
    assert_eq!(rows(&db.query("MATCH (p:P) RETURN p.name").await.unwrap()), 1);
}

#[tokio::test]
async fn empty_tier_list_runs_uncached() {
    let root = tempfile::tempdir().unwrap();
    let db = Db::open(tiers_config(root.path(), "", 1000)).await.unwrap();
    db.execute("INSERT (:P {_id: 1, name: 'a'})").await.unwrap();
    assert_eq!(rows(&db.query("MATCH (p:P) RETURN p.name").await.unwrap()), 1);
}

#[tokio::test]
async fn unknown_tier_error_lists_available() {
    let root = tempfile::tempdir().unwrap();
    let err = match Db::open(tiers_config(root.path(), "\"l2\"", 1000)).await {
        Ok(_) => panic!("expected unknown cache tier to fail"),
        Err(e) => e.to_string(),
    };
    assert!(err.contains("l2"), "{err}");
    assert!(err.contains("disk"), "{err}");
    assert!(err.contains("memory"), "{err}");
}

#[tokio::test]
async fn disk_tier_requires_its_dir() {
    let root = tempfile::tempdir().unwrap();
    let config = Config::from_toml_str(&format!(
        "[log]\nbackend = \"local\"\n[log.local]\ndir = {}\n\
         [storage]\nbackend = \"local\"\n[storage.local]\ndir = {}\n\
         [cache]\ntiers = [\"disk\"]\n",
        toml_escaped(&root.path().join("log")),
        toml_escaped(&root.path().join("store")),
    ))
    .unwrap();
    // EngineError wraps RegistryError transparently, so the build error's
    // own message is the display text — no variant matching needed.
    let err = match Db::open(config).await {
        Ok(_) => panic!("expected disk tier without [cache.disk] to fail"),
        Err(e) => e.to_string(),
    };
    assert!(err.contains("[cache.disk]"), "{err}");
}
```

Also update `crates/varve-engine/src/registries.rs` `builtins_cover_log_and_clock` to assert the new registry:

```rust
        assert_eq!(registries.cache.names(), vec!["disk", "memory"]);
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p varve --test cache_tiers`
Expected: FAIL — the `[cache] tiers` key is ignored by the current `CacheTuning` deserializer, so `unknown_tier_error_lists_available` and `disk_tier_*` fail (and the registries test fails: no `cache` field).

- [ ] **Step 3: Implement factories, registry, and engine wiring**

Append to `crates/varve-storage/src/cache.rs` (imports: add `varve_config::{BuildContext, ComponentFactory, ConfigSection, RegistryError}`):

```rust
#[derive(serde::Deserialize)]
struct MemoryCacheConfig {
    #[serde(default = "default_memory_max_bytes")]
    max_bytes: usize,
}

fn default_memory_max_bytes() -> usize {
    512 * 1024 * 1024
}

/// Registry factory: listed as `"memory"` in `[cache] tiers`, tuned by the
/// optional `[cache.memory]` table (`max_bytes`, default 512 MiB).
pub struct MemoryCacheFactory;

impl ComponentFactory<dyn CacheTier> for MemoryCacheFactory {
    fn name(&self) -> &'static str {
        "memory"
    }

    fn build(
        &self,
        cfg: &ConfigSection,
        _ctx: &BuildContext,
    ) -> Result<Arc<dyn CacheTier>, RegistryError> {
        let config: MemoryCacheConfig = cfg
            .child("memory")
            .unwrap_or_else(ConfigSection::empty)
            .get()?;
        Ok(Arc::new(MemoryCache::new(config.max_bytes)))
    }
}
```

Append to `crates/varve-storage/src/disk.rs` (above the tests; imports: add `std::sync::Arc`, `varve_config::{BuildContext, ComponentFactory, ConfigSection, RegistryError}`, `crate::cache::CacheTier` is already imported):

```rust
#[derive(serde::Deserialize)]
struct DiskCacheConfig {
    dir: String,
    #[serde(default = "default_disk_max_bytes")]
    max_bytes: u64,
}

fn default_disk_max_bytes() -> u64 {
    50 * 1024 * 1024 * 1024
}

/// Registry factory: listed as `"disk"` in `[cache] tiers`; `[cache.disk]`
/// requires `dir` (`max_bytes` default 50 GiB).
pub struct DiskCacheFactory;

impl ComponentFactory<dyn CacheTier> for DiskCacheFactory {
    fn name(&self) -> &'static str {
        "disk"
    }

    fn build(
        &self,
        cfg: &ConfigSection,
        _ctx: &BuildContext,
    ) -> Result<Arc<dyn CacheTier>, RegistryError> {
        let section = cfg.child("disk").ok_or_else(|| RegistryError::Build {
            kind: "cache",
            name: "disk".into(),
            source: "missing [cache.disk] section (requires `dir`)"
                .to_string()
                .into(),
        })?;
        let config: DiskCacheConfig = section.get()?;
        match DiskCache::open(Path::new(&config.dir), config.max_bytes) {
            Ok(cache) => Ok(Arc::new(cache)),
            Err(e) => Err(RegistryError::Build {
                kind: "cache",
                name: "disk".into(),
                source: Box::new(e),
            }),
        }
    }
}
```

`crates/varve-storage/src/lib.rs` — registry + exports:

```rust
pub use cache::{CacheKey, CacheTier, CachedStore, MemoryCache, MemoryCacheFactory};
pub use disk::{DiskCache, DiskCacheFactory};
```

```rust
/// All built-in cache tiers, registered under kind "cache".
pub fn cache_registry() -> Registry<dyn CacheTier> {
    let mut reg = Registry::new("cache");
    register_cache_builtin(&mut reg, Box::new(MemoryCacheFactory));
    register_cache_builtin(&mut reg, Box::new(DiskCacheFactory));
    reg
}

/// Same rationale as `register_builtin`: builtin names are a static,
/// distinct set — a collision is a programming error in this crate.
fn register_cache_builtin(
    reg: &mut Registry<dyn CacheTier>,
    factory: Box<dyn ComponentFactory<dyn CacheTier>>,
) {
    if let Err(e) = reg.register(factory) {
        unreachable!("built-in cache factory registration must not collide: {e}");
    }
}
```

`crates/varve-engine/src/registries.rs`:

```rust
use varve_storage::{CacheTier, ObjectStore};
```

```rust
pub struct Registries {
    pub log: Registry<dyn Log>,
    pub clock: Registry<dyn Clock>,
    pub storage: Registry<dyn ObjectStore>,
    pub cache: Registry<dyn CacheTier>,
}
```

and in `with_builtins()`:

```rust
            cache: varve_storage::cache_registry(),
```

`crates/varve-engine/src/db.rs` — DELETE `CacheTuning` and `default_cache_memory_max_bytes`, add:

```rust
/// `[cache]` (spec §4/§9): named tiers composed OUTERMOST-FIRST —
/// `tiers = ["memory", "disk"]` checks memory, then disk, then the backend.
/// An empty list runs uncached. Per-tier tuning lives in `[cache.<name>]`.
#[derive(serde::Deserialize)]
struct CacheConfig {
    #[serde(default = "default_cache_tiers")]
    tiers: Vec<String>,
}

fn default_cache_tiers() -> Vec<String> {
    vec!["memory".to_string()]
}
```

Replace the cache block in `open_with` (after the log build from Task 4):

```rust
        let cache_section = config.section("cache").unwrap_or_else(ConfigSection::empty);
        let cache_config: CacheConfig = cache_section.get()?;
        let mut store: Arc<dyn ObjectStore> = Arc::clone(&raw_store);
        // Innermost tier wraps first, so the FIRST listed tier is the first
        // one checked on a read.
        for name in cache_config.tiers.iter().rev() {
            let tier = registries.cache.build(name, &cache_section, &ctx)?;
            store = Arc::new(CachedStore::new(store, tier));
        }
```

(`Db::memory()`/`Db::local()` keep their existing private `cached()` helper — memory tier at the same 512 MiB default; nothing changes for them.)

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p varve --test cache_tiers && cargo test -p varve-engine && cargo test -p varve-storage`
Expected: PASS. Then `cargo test --workspace` — the slice-4 `varve/tests/blocks.rs` suite must stay green (its configs never set `[cache]`, so they get the default memory tier).

- [ ] **Step 5: Full gate + commit**

Run: `cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings`
Expected: green.

```bash
git add -A
git commit -m "feat: cache tiers selected by name — [cache] tiers registry with memory and disk builtins"
```

---

### Task 7: Capability probe — optional conditional-PUT surface + 4-step verdict

Sovereignty-preserving shape: the engine-required `ObjectStore` trait is untouched except for ONE defaulted hook, `conditional() -> Option<&dyn ConditionalStore>` (default `None`). The blanket impl over `object_store` backends provides the hook via `put_opts`; `CachedStore` delegates. The probe classifies semantics, not API presence: a backend that *claims* success while ignoring preconditions is `Inconsistent` — the dangerous verdict (D5).

**Files:**
- Modify: `crates/varve-storage/src/store.rs` (`ConditionalStore`, `CondPut`, `conditional()` hook, blanket impls)
- Create: `crates/varve-storage/src/probe.rs` (probe + verdicts + unit tests)
- Modify: `crates/varve-storage/src/cache.rs` (`CachedStore::conditional` delegation)
- Modify: `crates/varve-storage/src/lib.rs` (module + exports)
- Modify: `crates/varve-engine/src/db.rs` (`Db::probe_capabilities`)
- Modify: `crates/varve-engine/src/lib.rs`, `crates/varve/src/lib.rs` (re-exports)

**Interfaces:**
- Consumes: `ObjectStore`, `StorageError`, `object_store::{PutMode, UpdateVersion, PutResult, Error}` (verified §Global Constraints), `Clock::next()` for the unique probe key.
- Produces:
  ```rust
  // varve-storage
  pub enum CondPut { Stored { etag: Option<String> }, AlreadyExists, PreconditionFailed, Unsupported { reason: String } }
  #[async_trait] pub trait ConditionalStore: Send + Sync {
      async fn put_if_absent(&self, key: &str, bytes: Bytes) -> Result<CondPut, StorageError>;
      async fn put_if_matches(&self, key: &str, bytes: Bytes, etag: &str) -> Result<CondPut, StorageError>;
  }
  // added to trait ObjectStore (defaulted):
  fn conditional(&self) -> Option<&dyn ConditionalStore> { None }
  // probe.rs
  pub const PROBE_PREFIX: &str = "v1/probe";
  #[derive(Debug, Clone, PartialEq, Eq)] pub enum ProbeVerdict { Supported, Unsupported { reason: String }, Inconsistent { reason: String } }
  #[derive(Debug, Clone, PartialEq, Eq)] pub struct ProbeReport { pub verdict: ProbeVerdict, pub probe_key: String }
  pub async fn probe_conditional_put(store: &dyn ObjectStore, probe_key: &str) -> Result<ProbeReport, StorageError>;
  // varve-engine / varve facade
  impl Db { pub async fn probe_capabilities(&self) -> Result<ProbeReport, EngineError>; }
  ```

- [ ] **Step 1: Write the failing tests**

Create `crates/varve-storage/src/probe.rs` starting with the test module (implementation lands in Step 3):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{CondPut, ConditionalStore, ObjectStore, StorageError};
    use crate::{local_store, memory_store};
    use bytes::Bytes;
    use std::ops::Range;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    /// InMemory supports Create AND etag-checked Update with rotating ETags
    /// (verified against object_store 0.13.2 source) ⇒ Supported.
    #[tokio::test]
    async fn memory_store_is_supported() {
        let store = memory_store();
        let report = probe_conditional_put(store.as_ref(), "v1/probe/t1")
            .await
            .unwrap();
        assert_eq!(report.verdict, ProbeVerdict::Supported);
        assert_eq!(report.probe_key, "v1/probe/t1");
    }

    /// LocalFileSystem rejects PutMode::Update (NotImplemented, verified
    /// against source) ⇒ Unsupported, with the operation named.
    #[tokio::test]
    async fn local_store_is_unsupported() {
        let dir = tempfile::tempdir().unwrap();
        let store = local_store(dir.path()).unwrap();
        let report = probe_conditional_put(store.as_ref(), "v1/probe/t1")
            .await
            .unwrap();
        match report.verdict {
            ProbeVerdict::Unsupported { reason } => {
                assert!(reason.contains("PutMode::Update"), "{reason}");
            }
            other => panic!("expected Unsupported, got {other:?}"),
        }
    }

    /// A store whose ObjectStore impl never overrides `conditional()`.
    struct PlainStore(Arc<dyn ObjectStore>);

    #[async_trait::async_trait]
    impl ObjectStore for PlainStore {
        async fn put(&self, key: &str, bytes: Bytes) -> Result<(), StorageError> {
            self.0.put(key, bytes).await
        }
        async fn get(&self, key: &str) -> Result<Bytes, StorageError> {
            self.0.get(key).await
        }
        async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Bytes, StorageError> {
            self.0.get_range(key, range).await
        }
        async fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
            self.0.list(prefix).await
        }
    }

    #[tokio::test]
    async fn stores_without_the_hook_are_unsupported() {
        let store = PlainStore(memory_store());
        let report = probe_conditional_put(&store, "v1/probe/t1").await.unwrap();
        assert!(matches!(report.verdict, ProbeVerdict::Unsupported { .. }));
    }

    /// Claims success on EVERY conditional write (SeaweedFS-class header
    /// blindness, D5): step 2 must expose it.
    struct BlindStore {
        inner: Arc<dyn ObjectStore>,
        writes: AtomicU64,
    }

    #[async_trait::async_trait]
    impl ObjectStore for BlindStore {
        async fn put(&self, key: &str, bytes: Bytes) -> Result<(), StorageError> {
            self.inner.put(key, bytes).await
        }
        async fn get(&self, key: &str) -> Result<Bytes, StorageError> {
            self.inner.get(key).await
        }
        async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Bytes, StorageError> {
            self.inner.get_range(key, range).await
        }
        async fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
            self.inner.list(prefix).await
        }
        fn conditional(&self) -> Option<&dyn ConditionalStore> {
            Some(self)
        }
    }

    #[async_trait::async_trait]
    impl ConditionalStore for BlindStore {
        async fn put_if_absent(&self, key: &str, bytes: Bytes) -> Result<CondPut, StorageError> {
            self.inner.put(key, bytes).await?;
            Ok(CondPut::Stored {
                etag: Some(format!("e{}", self.writes.fetch_add(1, Ordering::SeqCst))),
            })
        }
        async fn put_if_matches(
            &self,
            key: &str,
            bytes: Bytes,
            _etag: &str,
        ) -> Result<CondPut, StorageError> {
            self.put_if_absent(key, bytes).await
        }
    }

    #[tokio::test]
    async fn blind_success_is_inconsistent() {
        let store = BlindStore {
            inner: memory_store(),
            writes: AtomicU64::new(0),
        };
        let report = probe_conditional_put(&store, "v1/probe/t1").await.unwrap();
        match report.verdict {
            ProbeVerdict::Inconsistent { reason } => {
                assert!(reason.contains("create-if-absent"), "{reason}");
            }
            other => panic!("expected Inconsistent, got {other:?}"),
        }
    }

    /// Creates correctly but never checks the etag on update: step 4 (the
    /// stale-etag swap) must expose it.
    struct StaleAcceptor {
        inner: Arc<dyn ObjectStore>,
        writes: AtomicU64,
    }

    impl StaleAcceptor {
        fn next_etag(&self) -> String {
            format!("e{}", self.writes.fetch_add(1, Ordering::SeqCst))
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for StaleAcceptor {
        async fn put(&self, key: &str, bytes: Bytes) -> Result<(), StorageError> {
            self.inner.put(key, bytes).await
        }
        async fn get(&self, key: &str) -> Result<Bytes, StorageError> {
            self.inner.get(key).await
        }
        async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Bytes, StorageError> {
            self.inner.get_range(key, range).await
        }
        async fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
            self.inner.list(prefix).await
        }
        fn conditional(&self) -> Option<&dyn ConditionalStore> {
            Some(self)
        }
    }

    #[async_trait::async_trait]
    impl ConditionalStore for StaleAcceptor {
        async fn put_if_absent(&self, key: &str, bytes: Bytes) -> Result<CondPut, StorageError> {
            if self.inner.get(key).await.is_ok() {
                return Ok(CondPut::AlreadyExists);
            }
            self.inner.put(key, bytes).await?;
            Ok(CondPut::Stored {
                etag: Some(self.next_etag()),
            })
        }
        async fn put_if_matches(
            &self,
            key: &str,
            bytes: Bytes,
            _etag: &str, // never checked — the bug under test
        ) -> Result<CondPut, StorageError> {
            self.inner.put(key, bytes).await?;
            Ok(CondPut::Stored {
                etag: Some(self.next_etag()),
            })
        }
    }

    #[tokio::test]
    async fn accepted_stale_etag_is_inconsistent() {
        let store = StaleAcceptor {
            inner: memory_store(),
            writes: AtomicU64::new(0),
        };
        let report = probe_conditional_put(&store, "v1/probe/t1").await.unwrap();
        match report.verdict {
            ProbeVerdict::Inconsistent { reason } => {
                assert!(reason.contains("STALE"), "{reason}");
            }
            other => panic!("expected Inconsistent, got {other:?}"),
        }
    }

    /// The cache wrapper must pass the hook through (Db's store is wrapped).
    #[tokio::test]
    async fn cached_store_delegates_the_conditional_hook() {
        use crate::cache::{CachedStore, MemoryCache};
        let cached = CachedStore::new(memory_store(), Arc::new(MemoryCache::new(1024)));
        let report = probe_conditional_put(&cached, "v1/probe/t1").await.unwrap();
        assert_eq!(report.verdict, ProbeVerdict::Supported);
    }
}
```

Register the module in `crates/varve-storage/src/lib.rs`:

```rust
pub mod probe;
```

```rust
pub use probe::{probe_conditional_put, ProbeReport, ProbeVerdict, PROBE_PREFIX};
pub use store::{CondPut, ConditionalStore, ObjectStore, StorageError};
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p varve-storage probe`
Expected: COMPILE FAIL — `ConditionalStore`/`CondPut`/`probe_conditional_put` not defined.

- [ ] **Step 3: Implement the surface and the probe**

In `crates/varve-storage/src/store.rs`, add after the `StorageError`/`convert` block:

```rust
/// One conditional write's outcome. `Err(StorageError)` is reserved for
/// transport failures; every SEMANTIC outcome — including "backend cannot
/// do this" — is a variant, so the probe can classify without guessing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CondPut {
    /// The write landed; `etag` identifies the new object version (`None`
    /// means the backend returns no ETag — which alone rules out CAS).
    Stored { etag: Option<String> },
    /// Correctly refused: the object already exists (create path).
    AlreadyExists,
    /// Correctly refused: the ETag no longer matches (swap path).
    PreconditionFailed,
    /// The backend reports it cannot do conditional writes at all.
    Unsupported { reason: String },
}

/// OPTIONAL conditional-write surface (spec §12, D5/D7). Never required by
/// the engine — sovereignty means plain put/get/list always suffices.
/// Backends that can do more expose it through `ObjectStore::conditional`;
/// `probe::probe_conditional_put` classifies whether the claims actually
/// hold, and slice-10's cas-failover coordinator gates on that verdict.
#[async_trait::async_trait]
pub trait ConditionalStore: Send + Sync {
    /// Create-only PUT (`If-None-Match: *`).
    async fn put_if_absent(&self, key: &str, bytes: Bytes) -> Result<CondPut, StorageError>;
    /// ETag-guarded replace (`If-Match`).
    async fn put_if_matches(
        &self,
        key: &str,
        bytes: Bytes,
        etag: &str,
    ) -> Result<CondPut, StorageError>;
}
```

Add the defaulted hook inside `trait ObjectStore` (after `list`):

```rust
    /// The optional conditional-write surface, if this backend has one.
    /// Default: none — custom embedder stores need change nothing.
    fn conditional(&self) -> Option<&dyn ConditionalStore> {
        None
    }
}
```

Extend the blanket impl (`impl<T: object_store::ObjectStore> ObjectStore for T`) with:

```rust
    fn conditional(&self) -> Option<&dyn ConditionalStore> {
        Some(self)
    }
```

and add below it:

```rust
/// Maps a `put_opts` outcome onto the `CondPut` classification.
fn classify_cond_put(
    key: &str,
    result: Result<object_store::PutResult, object_store::Error>,
) -> Result<CondPut, StorageError> {
    match result {
        Ok(r) => Ok(CondPut::Stored { etag: r.e_tag }),
        Err(object_store::Error::AlreadyExists { .. }) => Ok(CondPut::AlreadyExists),
        Err(object_store::Error::Precondition { .. }) => Ok(CondPut::PreconditionFailed),
        Err(e @ object_store::Error::NotImplemented { .. })
        | Err(e @ object_store::Error::NotSupported { .. }) => Ok(CondPut::Unsupported {
            reason: e.to_string(),
        }),
        Err(e) => Err(convert(key, e)),
    }
}

/// Blanket conditional surface for every `object_store` backend, via
/// `put_opts` (S3ConditionalPut::ETagMatch is the 0.13 default, so the AWS
/// impl sends real If-None-Match / If-Match headers).
#[async_trait::async_trait]
impl<T: object_store::ObjectStore> ConditionalStore for T {
    async fn put_if_absent(&self, key: &str, bytes: Bytes) -> Result<CondPut, StorageError> {
        let path = object_store::path::Path::from(key);
        classify_cond_put(
            key,
            object_store::ObjectStore::put_opts(
                self,
                &path,
                bytes.into(),
                object_store::PutMode::Create.into(),
            )
            .await,
        )
    }

    async fn put_if_matches(
        &self,
        key: &str,
        bytes: Bytes,
        etag: &str,
    ) -> Result<CondPut, StorageError> {
        let path = object_store::path::Path::from(key);
        let version = object_store::UpdateVersion {
            e_tag: Some(etag.to_string()),
            version: None,
        };
        classify_cond_put(
            key,
            object_store::ObjectStore::put_opts(
                self,
                &path,
                bytes.into(),
                object_store::PutMode::Update(version).into(),
            )
            .await,
        )
    }
}
```

In `crates/varve-storage/src/cache.rs`, add to `impl ObjectStore for CachedStore` (probe writes bypass the cache by design — they are never read back through it):

```rust
    fn conditional(&self) -> Option<&dyn crate::store::ConditionalStore> {
        self.inner.conditional()
    }
```

Prepend the implementation to `crates/varve-storage/src/probe.rs`:

```rust
//! Startup capability probe (spec §12, D5): does this backend REALLY
//! implement conditional PUT? Four steps against a fresh key:
//!
//! 1. create (`If-None-Match: *`) on the fresh key → must store + yield an ETag
//! 2. create AGAIN on the same key               → must refuse
//! 3. swap with the CURRENT etag (`If-Match`)    → must store + rotate the ETag
//! 4. swap with the now-STALE step-1 etag        → must refuse
//!
//! Steps 2 and 4 are the semantic teeth: a backend that ignores the headers
//! passes 1 and 3 but "succeeds" at 2 or 4 ⇒ `Inconsistent` — the dangerous
//! verdict (working-looking CAS that would lose the failover race). A
//! versioned bucket that hands back an unchanged ETag surfaces the same way.
//!
//! Report-only in slice 5; slice-10 cas-failover refuses to start unless the
//! verdict is `Supported`. Each run leaves ≤ 2 small objects under
//! `v1/probe/` (the sovereign trait has no delete; slice-8 GC sweeps them).

use crate::store::{CondPut, ObjectStore, StorageError};
use bytes::Bytes;

pub const PROBE_PREFIX: &str = "v1/probe";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProbeVerdict {
    /// Create-if-absent AND etag-swap semantics both enforced correctly.
    Supported,
    /// The backend refuses (or cannot express) conditional writes — the
    /// safe answer; designated-writer mode is unaffected.
    Unsupported { reason: String },
    /// The backend CLAIMS success while violating the semantics (e.g. blind
    /// overwrite). MUST be treated as no-CAS; strictly worse than
    /// `Unsupported` because only this probe distinguishes it from working
    /// CAS (D5: SeaweedFS-class bugs).
    Inconsistent { reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProbeReport {
    pub verdict: ProbeVerdict,
    /// Where the probe objects were left (for slice-8 GC and diagnostics).
    pub probe_key: String,
}

/// Runs the 4-step probe against `probe_key`, which MUST be fresh (the
/// caller supplies uniqueness — e.g. `Db` derives it from its `Clock`, so
/// the probe itself introduces no randomness). Transport failures surface
/// as `Err`; every semantic outcome is a verdict.
pub async fn probe_conditional_put(
    store: &dyn ObjectStore,
    probe_key: &str,
) -> Result<ProbeReport, StorageError> {
    let verdict = classify(store, probe_key).await?;
    Ok(ProbeReport {
        verdict,
        probe_key: probe_key.to_string(),
    })
}

fn unsupported(reason: impl Into<String>) -> ProbeVerdict {
    ProbeVerdict::Unsupported {
        reason: reason.into(),
    }
}

fn inconsistent(reason: impl Into<String>) -> ProbeVerdict {
    ProbeVerdict::Inconsistent {
        reason: reason.into(),
    }
}

async fn classify(store: &dyn ObjectStore, key: &str) -> Result<ProbeVerdict, StorageError> {
    let Some(cond) = store.conditional() else {
        return Ok(unsupported("backend exposes no conditional-write API"));
    };

    // 1. create on a fresh key.
    let etag1 = match cond
        .put_if_absent(key, Bytes::from_static(b"varve-probe-1"))
        .await?
    {
        CondPut::Stored { etag: Some(etag) } => etag,
        CondPut::Stored { etag: None } => {
            return Ok(unsupported(
                "PUT returns no ETag; If-Match swaps are inexpressible",
            ));
        }
        CondPut::Unsupported { reason } => return Ok(unsupported(reason)),
        CondPut::AlreadyExists | CondPut::PreconditionFailed => {
            return Ok(inconsistent("fresh probe key was refused as existing"));
        }
    };

    // 2. create over the existing key must be refused.
    match cond
        .put_if_absent(key, Bytes::from_static(b"varve-probe-1"))
        .await?
    {
        CondPut::AlreadyExists | CondPut::PreconditionFailed => {}
        CondPut::Stored { .. } => {
            return Ok(inconsistent(
                "create-if-absent over an existing object succeeded (precondition ignored)",
            ));
        }
        CondPut::Unsupported { reason } => return Ok(unsupported(reason)),
    }

    // 3. swap with the current etag must land and rotate the etag.
    let etag2 = match cond
        .put_if_matches(key, Bytes::from_static(b"varve-probe-2"), &etag1)
        .await?
    {
        CondPut::Stored { etag: Some(etag) } => etag,
        CondPut::Stored { etag: None } => {
            return Ok(unsupported(
                "update returns no ETag; chained swaps are inexpressible",
            ));
        }
        CondPut::Unsupported { reason } => return Ok(unsupported(reason)),
        CondPut::AlreadyExists | CondPut::PreconditionFailed => {
            return Ok(inconsistent("swap with the CURRENT etag was refused"));
        }
    };
    if etag2 == etag1 {
        return Ok(inconsistent(
            "etag did not change across an update (versioned-bucket edge case, spec §12)",
        ));
    }

    // 4. swap with the now-stale first etag must be refused.
    Ok(
        match cond
            .put_if_matches(key, Bytes::from_static(b"varve-probe-3"), &etag1)
            .await?
        {
            CondPut::PreconditionFailed => ProbeVerdict::Supported,
            CondPut::Stored { .. } => {
                inconsistent("swap with a STALE etag succeeded (lost-update hazard)")
            }
            CondPut::AlreadyExists => {
                inconsistent("stale-etag swap refused with the wrong class (AlreadyExists)")
            }
            CondPut::Unsupported { reason } => unsupported(reason),
        },
    )
}
```

Wire `Db` in `crates/varve-engine/src/db.rs` (a new method on `impl Db`, after `query`):

```rust
    /// Report-only capability probe (spec §12, D5): classifies whether the
    /// store's conditional-PUT semantics actually hold, against a fresh key
    /// under `v1/probe/`. Slice-10's cas-failover coordinator gates on this
    /// verdict at startup; nothing in v1 changes behavior based on it.
    /// Burns one clock tick for key uniqueness (harmless: tx times only
    /// ever need to keep increasing).
    pub async fn probe_capabilities(&self) -> Result<ProbeReport, EngineError> {
        let key = format!(
            "{}/{}",
            varve_storage::PROBE_PREFIX,
            self.clock.next().as_micros()
        );
        Ok(varve_storage::probe_conditional_put(self.store.as_ref(), &key).await?)
    }
```

with `use varve_storage::ProbeReport;` added to the imports. Re-export in `crates/varve-engine/src/lib.rs`:

```rust
pub use varve_storage::{ProbeReport, ProbeVerdict};
```

and in `crates/varve/src/lib.rs`:

```rust
pub use varve_engine::{Db, EngineError, ProbeReport, ProbeVerdict, Registries, TxReceipt};
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p varve-storage probe`
Expected: PASS — all 7 tests, including `local_store_is_unsupported` (pins the source-verified LocalFileSystem behavior) and the CachedStore delegation.

- [ ] **Step 5: Add the facade smoke test**

Append to `crates/varve/tests/walking_skeleton.rs`:

```rust
#[tokio::test]
async fn probe_capabilities_reports_through_the_facade() {
    use varve::ProbeVerdict;
    let db = Db::memory();
    // Db::memory wraps an InMemory store in the cache — Supported proves
    // both the blanket conditional impl and the CachedStore delegation.
    let report = db.probe_capabilities().await.unwrap();
    assert_eq!(report.verdict, ProbeVerdict::Supported);
    assert!(report.probe_key.starts_with("v1/probe/"), "{}", report.probe_key);
}
```

(`Db::memory()` is synchronous — same call shape as the existing tests in that file.)

Run: `cargo test -p varve --test walking_skeleton`
Expected: PASS.

- [ ] **Step 6: Full gate + commit**

Run: `cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings`
Expected: green.

```bash
git add -A
git commit -m "feat: conditional-PUT capability probe — report-only verdict gating slice-10 cas-failover"
```

---
### Task 8: Docker backend harness — Garage, SeaweedFS, MinIO, Ceph containers

Test-only rig in `varve-testkit` driving the docker CLI through `std::process::Command` (see design decision 7 for why not the testcontainers crate). Everything about a backend — image pin, init dance, credentials — lives in this ONE file. Containers publish onto a random localhost port and are force-removed on Drop.

The container init sequences (Garage's `layout assign` flow, SeaweedFS's `weed shell`, MinIO's `mc` sidecar, Ceph demo env) follow each project's documented quick-start. **They are exercised only under `VARVE_S3_BACKENDS`**; if a CLI's output wording or an image tag has drifted at execution time, adapt the parsing/pin here and record it in STATUS.md — the assertions in Task 9's matrix are the contract, not the exec plumbing.

**Files:**
- Modify: `crates/varve-testkit/Cargo.toml` (+ `varve-config` dep; move `tempfile` from dev-deps to deps; + `bytes` dev-dep)
- Create: `crates/varve-testkit/src/backends.rs`
- Modify: `crates/varve-testkit/src/lib.rs` (`pub mod backends;`)
- Test: `crates/varve-testkit/tests/backend_matrix.rs` (created here with the gated smoke; Task 9 grows it into the full suite)

**Interfaces:**
- Consumes: `storage_registry()` + `Config`/`BuildContext` (the harness builds stores through the REAL s3 factory path, never by hand).
- Produces (Task 9 relies on):
  ```rust
  // varve_testkit::backends
  pub const DB_BUCKET: &str = "varve";
  pub const CONTRACT_BUCKET: &str = "varve-contract";
  pub struct S3Params { pub endpoint, pub bucket, pub region, pub access_key_id, pub secret_access_key: String }
  impl S3Params {
      pub fn with_bucket(&self, bucket: &str) -> S3Params;
      pub fn storage_toml(&self) -> String;                       // full [storage] + [storage.s3]
      pub fn storage_toml_with(&self, extra_storage_keys: &str) -> String; // extra lines inside [storage]
      pub fn store(&self) -> Arc<dyn ObjectStore>;                // via storage_registry()
  }
  pub struct Backend { pub name: &'static str, pub params: S3Params, /* container held privately */ }
  pub fn enabled(name: &str) -> bool;          // VARVE_S3_BACKENDS gate
  pub async fn start(name: &str) -> Backend;   // "garage" | "seaweedfs" | "minio" | "ceph"
  ```

- [ ] **Step 1: Write the gated smoke test (fails to compile first)**

Create `crates/varve-testkit/tests/backend_matrix.rs`:

```rust
#![allow(clippy::unwrap_used)]
use varve_testkit::backends;

/// Start the backend, build the store THROUGH the registry, round-trip one
/// object. Skips silently unless VARVE_S3_BACKENDS names the backend.
async fn smoke(name: &'static str) {
    if !backends::enabled(name) {
        eprintln!("skipping {name} (set VARVE_S3_BACKENDS={name} to run)");
        return;
    }
    let backend = backends::start(name).await;
    let store = backend.params.store();
    store
        .put("v1/smoke", bytes::Bytes::from_static(b"ok"))
        .await
        .unwrap();
    assert_eq!(
        store.get("v1/smoke").await.unwrap(),
        bytes::Bytes::from_static(b"ok")
    );
}

#[tokio::test]
async fn garage_matrix() {
    smoke("garage").await;
}

#[tokio::test]
async fn seaweedfs_matrix() {
    smoke("seaweedfs").await;
}

#[tokio::test]
async fn minio_matrix() {
    smoke("minio").await;
}

#[tokio::test]
async fn ceph_matrix() {
    smoke("ceph").await;
}
```

Run: `cargo test -p varve-testkit --test backend_matrix`
Expected: COMPILE FAIL — `backends` module missing.

- [ ] **Step 2: Cargo.toml wiring**

In `crates/varve-testkit/Cargo.toml`: add to `[dependencies]`

```toml
varve-config = { path = "../varve-config" }
tempfile = { workspace = true }
```

remove `tempfile` from `[dev-dependencies]`, and add there instead:

```toml
bytes = { workspace = true }
```

- [ ] **Step 3: Implement the harness**

Create `crates/varve-testkit/src/backends.rs`:

```rust
//! Docker-CLI backend harness for the S3 integration matrix (spec §13.5).
//! Deliberately NOT the `testcontainers` crate: Garage needs a multi-step
//! CLI init driven through `docker exec`, and `std::process::Command` keeps
//! the rig dependency-free and fully inspectable. Containers are removed on
//! Drop (best-effort). Nothing here runs unless `VARVE_S3_BACKENDS` opts in.
#![allow(clippy::unwrap_used, clippy::expect_used)]
// ^ test-harness module: a broken container rig must abort the test loudly;
//   these are the crate-wide test allowances applied to harness lib code.

use std::process::Command;
use std::sync::Arc;
use std::time::Duration;
use varve_config::{BuildContext, Config};
use varve_storage::{storage_registry, ObjectStore};

/// Pinned images — bump ONLY here; record any bump in STATUS.md.
pub const GARAGE_IMAGE: &str = "dxflrs/garage:v1.0.1";
pub const SEAWEEDFS_IMAGE: &str = "chrislusf/seaweedfs:3.80";
pub const MINIO_IMAGE: &str = "minio/minio:RELEASE.2025-04-22T22-12-26Z";
pub const MC_IMAGE: &str = "minio/mc:RELEASE.2025-04-16T18-13-26Z";
pub const CEPH_IMAGE: &str = "quay.io/ceph/demo:latest-quincy";

pub const ACCESS_KEY: &str = "varve";
pub const SECRET_KEY: &str = "varvesecret123"; // ≥ 8 chars (MinIO minimum)
pub const DB_BUCKET: &str = "varve";
pub const CONTRACT_BUCKET: &str = "varve-contract";

/// Connection parameters for one running backend. `bucket` is switchable so
/// raw-contract phases and the Db end-to-end use ISOLATED buckets.
#[derive(Clone, Debug)]
pub struct S3Params {
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
}

impl S3Params {
    pub fn with_bucket(&self, bucket: &str) -> S3Params {
        S3Params {
            bucket: bucket.to_string(),
            ..self.clone()
        }
    }

    /// The `[storage]` TOML this backend needs — tests configure the REAL
    /// factory path, never hand-assembled stores.
    pub fn storage_toml(&self) -> String {
        self.storage_toml_with("")
    }

    /// `extra` = additional lines INSIDE the `[storage]` table (e.g.
    /// `"max_block_rows = 2\n"`) — they must precede the nested
    /// `[storage.s3]` table to stay in scope.
    pub fn storage_toml_with(&self, extra: &str) -> String {
        format!(
            "[storage]\nbackend = \"s3\"\n{extra}[storage.s3]\n\
             endpoint = \"{}\"\nbucket = \"{}\"\nregion = \"{}\"\n\
             access_key_id = \"{}\"\nsecret_access_key = \"{}\"\n",
            self.endpoint, self.bucket, self.region, self.access_key_id, self.secret_access_key
        )
    }

    /// Builds the store through the registry from `storage_toml()`.
    pub fn store(&self) -> Arc<dyn ObjectStore> {
        let section = Config::from_toml_str(&self.storage_toml())
            .expect("valid storage toml")
            .section("storage")
            .expect("storage section");
        storage_registry()
            .build("s3", &section, &BuildContext::empty())
            .expect("s3 store builds")
    }
}

fn docker(args: &[&str]) -> Result<String, String> {
    let out = Command::new("docker")
        .args(args)
        .output()
        .map_err(|e| format!("docker not runnable: {e}"))?;
    let stdout = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if out.status.success() {
        Ok(stdout)
    } else {
        Err(format!(
            "docker {args:?} failed: {stdout}\n{}",
            String::from_utf8_lossy(&out.stderr)
        ))
    }
}

/// A running container, force-removed on Drop.
struct Container {
    id: String,
    /// Mounted config files must outlive the container.
    _files: Option<tempfile::TempDir>,
}

impl Drop for Container {
    fn drop(&mut self) {
        let _ = Command::new("docker").args(["rm", "-f", &self.id]).output();
    }
}

fn run_detached(args: &[&str], files: Option<tempfile::TempDir>) -> Container {
    let mut full = vec!["run", "-d"];
    full.extend_from_slice(args);
    let id = docker(&full).expect("container starts");
    Container { id, _files: files }
}

/// Host port docker mapped to `container_port` (we always publish
/// `127.0.0.1:0:<port>` and let the OS pick a free one).
fn host_port(container: &Container, container_port: u16) -> u16 {
    let spec = format!("{container_port}/tcp");
    let out = docker(&["port", &container.id, &spec]).expect("docker port");
    let line = out.lines().next().expect("a port mapping line");
    line.rsplit(':')
        .next()
        .expect("host port")
        .trim()
        .parse()
        .expect("numeric host port")
}

fn exec(container: &Container, cmd: &[&str]) -> Result<String, String> {
    let mut args = vec!["exec", container.id.as_str()];
    args.extend_from_slice(cmd);
    docker(&args)
}

/// Retries `f` (500 ms apart, ≤ 2 min) until it yields — container inits
/// are eventually consistent.
async fn poll<T>(mut f: impl FnMut() -> Option<T>) -> T {
    for _ in 0..240 {
        if let Some(v) = f() {
            return v;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("timed out waiting for container init");
}

/// Polls until the S3 endpoint answers a LIST for `params.bucket`.
async fn wait_ready(params: &S3Params) {
    let store = params.store();
    for _ in 0..240 {
        if store.list("").await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("backend at {} never became ready", params.endpoint);
}

/// `VARVE_S3_BACKENDS` gate: a comma list (`garage,minio`) or `all`.
pub fn enabled(name: &str) -> bool {
    match std::env::var("VARVE_S3_BACKENDS") {
        Ok(v) => {
            let v = v.to_lowercase();
            v.trim() == "all" || v.split(',').any(|b| b.trim() == name)
        }
        Err(_) => false,
    }
}

pub struct Backend {
    pub name: &'static str,
    /// bucket = DB_BUCKET; use `params.with_bucket(CONTRACT_BUCKET)` for
    /// the isolated contract bucket.
    pub params: S3Params,
    _container: Container,
}

pub async fn start(name: &str) -> Backend {
    match name {
        "garage" => start_garage().await,
        "seaweedfs" => start_seaweedfs().await,
        "minio" => start_minio().await,
        "ceph" => start_ceph().await,
        other => panic!("unknown backend '{other}'"),
    }
}

// ---------------------------------------------------------------- garage --

/// One-node Garage config (v1.x quick start). rpc_secret must be 64 hex
/// chars; the value is fixed test-rig material, not a secret.
const GARAGE_TOML: &str = r#"
metadata_dir = "/var/lib/garage/meta"
data_dir = "/var/lib/garage/data"
db_engine = "sqlite"
replication_factor = 1
rpc_bind_addr = "[::]:3901"
rpc_public_addr = "127.0.0.1:3901"
rpc_secret = "1799bccfd7411eddcf9ebd316bc1f5287ad12a68094e1c6ac6abde7e6feae1ec"

[s3_api]
s3_region = "garage"
api_bind_addr = "[::]:3900"
root_domain = ".s3.garage.localhost"
"#;

/// Extracts the value after `label` on the first line carrying it.
fn field(out: &str, label: &str) -> String {
    out.lines()
        .find_map(|l| l.trim().strip_prefix(label))
        .map(|v| v.trim().to_string())
        .unwrap_or_else(|| panic!("'{label}' not found in output:\n{out}"))
}

async fn start_garage() -> Backend {
    let files = tempfile::tempdir().expect("tempdir");
    let cfg = files.path().join("garage.toml");
    std::fs::write(&cfg, GARAGE_TOML).expect("write garage.toml");
    let mount = format!("{}:/etc/garage.toml", cfg.display());
    let container = run_detached(
        &["-p", "127.0.0.1:0:3900", "-v", &mount, GARAGE_IMAGE],
        Some(files),
    );
    let port = host_port(&container, 3900);

    // Quick-start init: status (node id, shown truncated with a trailing
    // '…' — the CLI accepts the prefix) → layout assign/apply → buckets →
    // key → grants.
    let node_id = poll(|| {
        let status = exec(&container, &["/garage", "status"]).ok()?;
        status.lines().find_map(|l| {
            let first = l.split_whitespace().next()?;
            let id = first.trim_end_matches('…');
            (id.len() >= 16 && id.chars().all(|c| c.is_ascii_hexdigit()))
                .then(|| id.to_string())
        })
    })
    .await;
    exec(
        &container,
        &["/garage", "layout", "assign", "-z", "dc1", "-c", "1G", &node_id],
    )
    .expect("garage layout assign");
    exec(&container, &["/garage", "layout", "apply", "--version", "1"])
        .expect("garage layout apply");
    for bucket in [DB_BUCKET, CONTRACT_BUCKET] {
        exec(&container, &["/garage", "bucket", "create", bucket]).expect("garage bucket create");
    }
    let key_out =
        exec(&container, &["/garage", "key", "create", "varve-ci"]).expect("garage key create");
    let access = field(&key_out, "Key ID:");
    let secret = field(&key_out, "Secret key:");
    for bucket in [DB_BUCKET, CONTRACT_BUCKET] {
        exec(
            &container,
            &[
                "/garage", "bucket", "allow", "--read", "--write", "--owner", bucket, "--key",
                "varve-ci",
            ],
        )
        .expect("garage bucket allow");
    }

    let params = S3Params {
        endpoint: format!("http://127.0.0.1:{port}"),
        bucket: DB_BUCKET.to_string(),
        region: "garage".to_string(),
        access_key_id: access,
        secret_access_key: secret,
    };
    wait_ready(&params).await;
    Backend {
        name: "garage",
        params,
        _container: container,
    }
}

// ------------------------------------------------------------- seaweedfs --

const SEAWEEDFS_S3_JSON: &str = r#"{
  "identities": [
    {
      "name": "varve",
      "credentials": [{ "accessKey": "varve", "secretKey": "varvesecret123" }],
      "actions": ["Admin", "Read", "Write", "List", "Tagging"]
    }
  ]
}"#;

async fn start_seaweedfs() -> Backend {
    let files = tempfile::tempdir().expect("tempdir");
    let cfg = files.path().join("s3.json");
    std::fs::write(&cfg, SEAWEEDFS_S3_JSON).expect("write s3.json");
    let mount = format!("{}:/etc/seaweedfs/s3.json", cfg.display());
    let container = run_detached(
        &[
            "-p", "127.0.0.1:0:8333", "-v", &mount, SEAWEEDFS_IMAGE,
            "server", "-s3", "-s3.config=/etc/seaweedfs/s3.json",
        ],
        Some(files),
    );
    let port = host_port(&container, 8333);
    // Buckets via weed shell (retried until the embedded master answers).
    for bucket in [DB_BUCKET, CONTRACT_BUCKET] {
        let cmd = format!("echo 's3.bucket.create -name {bucket}' | weed shell");
        poll(|| exec(&container, &["sh", "-c", &cmd]).ok()).await;
    }
    let params = S3Params {
        endpoint: format!("http://127.0.0.1:{port}"),
        bucket: DB_BUCKET.to_string(),
        region: "us-east-1".to_string(),
        access_key_id: ACCESS_KEY.to_string(),
        secret_access_key: SECRET_KEY.to_string(),
    };
    wait_ready(&params).await;
    Backend {
        name: "seaweedfs",
        params,
        _container: container,
    }
}

// ----------------------------------------------------------------- minio --

async fn start_minio() -> Backend {
    let root_user = format!("MINIO_ROOT_USER={ACCESS_KEY}");
    let root_pass = format!("MINIO_ROOT_PASSWORD={SECRET_KEY}");
    let container = run_detached(
        &[
            "-p", "127.0.0.1:0:9000", "-e", &root_user, "-e", &root_pass,
            MINIO_IMAGE, "server", "/data",
        ],
        None,
    );
    let port = host_port(&container, 9000);
    // Buckets via a one-shot `mc` container sharing minio's network
    // namespace (so 127.0.0.1:9000 resolves to minio, not the host).
    let net = format!("container:{}", container.id);
    let script = format!(
        "mc alias set m http://127.0.0.1:9000 {ACCESS_KEY} {SECRET_KEY} \
         && mc mb m/{DB_BUCKET} && mc mb m/{CONTRACT_BUCKET}"
    );
    poll(|| {
        Command::new("docker")
            .args([
                "run", "--rm", "--network", &net, "--entrypoint", "sh", MC_IMAGE, "-c", &script,
            ])
            .output()
            .ok()
            .filter(|o| o.status.success())
    })
    .await;
    let params = S3Params {
        endpoint: format!("http://127.0.0.1:{port}"),
        bucket: DB_BUCKET.to_string(),
        region: "us-east-1".to_string(),
        access_key_id: ACCESS_KEY.to_string(),
        secret_access_key: SECRET_KEY.to_string(),
    };
    wait_ready(&params).await;
    Backend {
        name: "minio",
        params,
        _container: container,
    }
}

// ------------------------------------------------------------------ ceph --

/// Ceph demo (weekly CI only): heavyweight, host networking, RGW on :8080.
/// The demo entrypoint auto-creates the CEPH_DEMO_* user and bucket; the
/// contract bucket is added with the bundled s3cmd.
async fn start_ceph() -> Backend {
    let demo_bucket = format!("CEPH_DEMO_BUCKET={DB_BUCKET}");
    let demo_access = format!("CEPH_DEMO_ACCESS_KEY={ACCESS_KEY}");
    let demo_secret = format!("CEPH_DEMO_SECRET_KEY={SECRET_KEY}");
    let container = run_detached(
        &[
            "--net", "host",
            "-e", "MON_IP=127.0.0.1",
            "-e", "CEPH_PUBLIC_NETWORK=127.0.0.0/8",
            "-e", "CEPH_DEMO_UID=varve",
            "-e", &demo_access,
            "-e", &demo_secret,
            "-e", &demo_bucket,
            CEPH_IMAGE,
        ],
        None,
    );
    let params = S3Params {
        endpoint: "http://127.0.0.1:8080".to_string(),
        bucket: DB_BUCKET.to_string(),
        region: "us-east-1".to_string(),
        access_key_id: ACCESS_KEY.to_string(),
        secret_access_key: SECRET_KEY.to_string(),
    };
    wait_ready(&params).await;
    let mb = format!("s3://{CONTRACT_BUCKET}");
    poll(|| exec(&container, &["s3cmd", "mb", &mb]).ok()).await;
    Backend {
        name: "ceph",
        params,
        _container: container,
    }
}
```

Add to `crates/varve-testkit/src/lib.rs`:

```rust
pub mod backends;
```

- [ ] **Step 4: Verify hermetic pass, then a live smoke**

Run: `cargo test -p varve-testkit --test backend_matrix`
Expected: PASS in milliseconds — all four tests print `skipping … (set VARVE_S3_BACKENDS=…)`.

Run (docker required): `VARVE_S3_BACKENDS=minio cargo test -p varve-testkit --test backend_matrix minio -- --nocapture`
Expected: PASS — MinIO is the simplest backend and validates the docker plumbing end-to-end. If an image tag fails to pull or a CLI's output wording has drifted, fix the pin/parsing HERE and note it for STATUS.md.

- [ ] **Step 5: Full gate + commit**

Run: `cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings`
Expected: green (harness compiles everywhere; containers only run when opted in).

```bash
git add -A
git commit -m "test: docker backend harness — Garage/SeaweedFS/MinIO/Ceph containers for the S3 matrix"
```

---

### Task 9: Backend integration matrix — contract, object log, Db e2e, probe verdicts + CI

Grow `backend_matrix.rs` into the full per-backend suite. One container per backend test (the four phases share it): storage contract → object-store-log contract (both on the isolated `varve-contract` bucket) → Db end-to-end with `storage = "s3"` + `log = "object-store"` including a restart (on the clean `varve` bucket) → capability probe against the expectation table.

**Files:**
- Modify: `crates/varve-testkit/tests/backend_matrix.rs` (replace the smoke with the full suite)
- Modify: `justfile` (`s3-matrix` target)
- Modify: `.github/workflows/ci.yml` (backend-matrix job, ceph weekly cron, nightly-cron pinning)

**Interfaces:**
- Consumes: everything from Tasks 2–8 (`S3Params::store/storage_toml_with/with_bucket`, `ObjectStoreLog`, `Db::open`, `latest_manifest`, `probe_conditional_put`).
- Produces: the slice's CI surface — `just s3-matrix` locally, `backend-matrix` + `backend-ceph-weekly` jobs in CI.

- [ ] **Step 1: Replace the smoke test with the full suite**

Replace the entire contents of `crates/varve-testkit/tests/backend_matrix.rs` with:

```rust
//! Backend integration matrix (spec §13.5; slice-5 exit criteria): storage
//! contract, object-store-log contract, Db end-to-end with restart, and the
//! capability probe with per-backend expected verdicts — against real S3
//! containers. Skips silently unless VARVE_S3_BACKENDS names the backend.
#![allow(clippy::unwrap_used)]

use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;
use varve::{Config, Db, ProbeVerdict};
use varve_log::{Log, LogRecord, ObjectStoreLog};
use varve_storage::{latest_manifest, ObjectStore, StorageError};
use varve_testkit::backends::{self, Backend, CONTRACT_BUCKET};
use varve_types::LogPosition;

/// What the probe must report per backend (D5). `NotSupported` asserts only
/// the operationally load-bearing fact — slice-10 cas-failover must refuse —
/// without pinning Unsupported vs Inconsistent; `RecordOnly` prints the
/// verdict so the first CI run can pin it.
/// AFTER THE FIRST OBSERVED RUN: tighten every RecordOnly/NotSupported to
/// the exact verdict seen, and record the final table in STATUS.md.
enum Expectation {
    Supported,
    NotSupported,
    RecordOnly,
}

fn expected_probe(name: &str) -> Expectation {
    match name {
        // D5: "Garage: never CAS".
        "garage" => Expectation::NotSupported,
        // D5: "SeaweedFS: unconfirmed/buggy CAS" — pin from the first run.
        // A Supported verdict here deserves skepticism before slice 10
        // trusts it (that is exactly what Inconsistent-detection is for).
        "seaweedfs" => Expectation::RecordOnly,
        // MinIO implements the standard HTTP preconditions.
        "minio" => Expectation::Supported,
        "ceph" => Expectation::RecordOnly,
        other => panic!("unknown backend '{other}'"),
    }
}

/// Mirror of varve-storage/tests/store_test.rs::exercise — duplicated
/// because varve-storage cannot dev-depend on varve-testkit (cycle).
async fn storage_contract(store: Arc<dyn ObjectStore>) {
    store
        .put("v1/a/one", Bytes::from_static(b"hello"))
        .await
        .unwrap();
    assert_eq!(
        store.get("v1/a/one").await.unwrap(),
        Bytes::from_static(b"hello")
    );
    store
        .put("v1/a/one", Bytes::from_static(b"world"))
        .await
        .unwrap();
    assert_eq!(
        store.get("v1/a/one").await.unwrap(),
        Bytes::from_static(b"world")
    );
    assert_eq!(
        store.get_range("v1/a/one", 1..4).await.unwrap(),
        Bytes::from_static(b"orl")
    );
    assert!(matches!(
        store.get("v1/a/absent").await,
        Err(StorageError::NotFound(k)) if k == "v1/a/absent"
    ));
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

async fn log_contract(store: Arc<dyn ObjectStore>) {
    let rec = |tx_id: u64| LogRecord {
        tx_id,
        system_time_us: tx_id as i64,
        user: String::new(),
        effects: vec![],
    };
    let log = ObjectStoreLog::new(Arc::clone(&store));
    assert_eq!(
        log.append(vec![rec(1), rec(2)]).await.unwrap(),
        LogPosition::ZERO
    );
    assert_eq!(log.append(vec![rec(3)]).await.unwrap().offset(), 2);
    // A fresh handle (= restart) continues after the last object.
    let reopened = ObjectStoreLog::new(Arc::clone(&store));
    assert_eq!(reopened.append(vec![rec(4)]).await.unwrap().offset(), 3);
    let all = reopened.tail(LogPosition::ZERO).await.unwrap();
    assert_eq!(
        all.iter().map(|(_, r)| r.tx_id).collect::<Vec<_>>(),
        vec![1, 2, 3, 4]
    );
    // trim is a no-op on object-store logs (GC = slice 8).
    log.trim(LogPosition::from_u64(u64::MAX)).await.unwrap();
    assert_eq!(reopened.tail(LogPosition::ZERO).await.unwrap().len(), 4);
}

/// storage = "s3" + log = "object-store", flush to blocks, restart, query —
/// every durable byte lives on the backend.
async fn db_end_to_end(backend: &Backend) {
    let toml = format!(
        "[log]\nbackend = \"object-store\"\n{}",
        backend.params.storage_toml_with("max_block_rows = 2\n")
    );
    let store = backend.params.store();
    {
        let db = Db::open(Config::from_toml_str(&toml).unwrap()).await.unwrap();
        for i in 1..=4 {
            db.execute(&format!("INSERT (:Person {{_id: {i}, name: 'p{i}'}})"))
                .await
                .unwrap();
        }
        // Flush runs after acks — wait for ≥1 committed block manifest.
        let mut flushed = false;
        for _ in 0..240 {
            if latest_manifest(store.as_ref()).await.unwrap().is_some() {
                flushed = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        assert!(flushed, "no block manifest appeared on {}", backend.name);
        // One more tx that stays in the log tail past the watermark.
        db.execute("INSERT (:Person {_id: 5, name: 'p5'})")
            .await
            .unwrap();
    }
    let db = Db::open(Config::from_toml_str(&toml).unwrap()).await.unwrap();
    let batches = db.query("MATCH (p:Person) RETURN p.name").await.unwrap();
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 5, "blocks + log-tail replay from {}", backend.name);
}

async fn probe_phase(backend: &Backend) {
    let store = backend.params.with_bucket(CONTRACT_BUCKET).store();
    let key = format!(
        "v1/probe/{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros()
    );
    let report = varve_storage::probe_conditional_put(store.as_ref(), &key)
        .await
        .unwrap();
    eprintln!("[{}] capability probe: {:?}", backend.name, report.verdict);
    match expected_probe(backend.name) {
        Expectation::Supported => assert_eq!(report.verdict, ProbeVerdict::Supported),
        Expectation::NotSupported => assert!(
            !matches!(report.verdict, ProbeVerdict::Supported),
            "cas-failover must refuse on {}: got {:?}",
            backend.name,
            report.verdict
        ),
        Expectation::RecordOnly => {}
    }
}

async fn full_suite(name: &'static str) {
    if !backends::enabled(name) {
        eprintln!("skipping {name} (set VARVE_S3_BACKENDS={name} to run)");
        return;
    }
    let backend = backends::start(name).await;
    let contract_store = backend.params.with_bucket(CONTRACT_BUCKET).store();
    storage_contract(Arc::clone(&contract_store)).await;
    log_contract(contract_store).await;
    db_end_to_end(&backend).await;
    probe_phase(&backend).await;
}

#[tokio::test]
async fn garage_matrix() {
    full_suite("garage").await;
}

#[tokio::test]
async fn seaweedfs_matrix() {
    full_suite("seaweedfs").await;
}

#[tokio::test]
async fn minio_matrix() {
    full_suite("minio").await;
}

#[tokio::test]
async fn ceph_matrix() {
    full_suite("ceph").await;
}
```

- [ ] **Step 2: Verify hermetic + live**

Run: `cargo test -p varve-testkit --test backend_matrix`
Expected: PASS instantly (4 skips).

Run (docker required — THE slice exit criterion):
`VARVE_S3_BACKENDS=garage cargo test -p varve-testkit --test backend_matrix garage -- --nocapture`
Expected: PASS — all four phases against Garage; the probe line prints its verdict (expected class: not Supported). Then run `minio` and `seaweedfs` the same way. Record every observed probe verdict; tighten `expected_probe` pins accordingly and note them for STATUS.md.

- [ ] **Step 3: justfile target**

Append to `justfile`:

```make
s3-matrix backends="garage,seaweedfs,minio":
    VARVE_S3_BACKENDS={{backends}} cargo test -p varve-testkit --test backend_matrix -- --nocapture
```

Run: `just s3-matrix minio`
Expected: the MinIO suite passes.

- [ ] **Step 4: CI jobs**

In `.github/workflows/ci.yml`: extend the cron list —

```yaml
  schedule:
    - cron: "0 3 * * *"
    - cron: "0 4 * * 1" # weekly: Ceph demo backend
```

pin `property-nightly` to its own cron (so the Monday trigger doesn't double-run it):

```yaml
  property-nightly:
    if: github.event_name == 'schedule' && github.event.schedule == '0 3 * * *'
```

and append the two jobs:

```yaml
  backend-matrix:
    if: github.event_name != 'schedule'
    runs-on: ubuntu-latest
    strategy:
      fail-fast: false
      matrix:
        backend: [garage, seaweedfs, minio]
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - uses: Swatinem/rust-cache@v2
      - run: cargo test -p varve-testkit --test backend_matrix -- --nocapture
        env:
          VARVE_S3_BACKENDS: ${{ matrix.backend }}

  backend-ceph-weekly:
    if: github.event_name == 'schedule' && github.event.schedule == '0 4 * * 1'
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - uses: Swatinem/rust-cache@v2
      - run: cargo test -p varve-testkit --test backend_matrix ceph -- --nocapture
        env:
          VARVE_S3_BACKENDS: ceph
```

(ubuntu-latest runners ship docker; no service containers needed — the harness manages its own.)

- [ ] **Step 5: Full gate + commit**

Run: `cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings`
Expected: green.

```bash
git add -A
git commit -m "test: S3 backend integration matrix — contract, object log, Db e2e, probe verdicts + CI jobs"
```

---
### Task 10: `cache_bench` example — cold vs warm disk cache (exit criterion)

Ingest → flush to blocks → reopen with a cold disk cache (the point query fills it) → reopen again with the SAME cache dir (entries survived the restart) → the same query served from disk. Defaults to the local-FS backend (laptop profile — modest but visible delta); set the `VARVE_S3_*` env vars to point it at a real backend (e.g. the Garage container from `just s3-matrix`) for the real network delta.

**Files:**
- Create: `crates/varve/examples/cache_bench.rs`

**Interfaces:**
- Consumes: `[cache] tiers = ["disk"]` config (Task 6), `storage/s3` factory (Task 2), `Db::open`.
- Produces: the demo command recorded in STATUS.md.

- [ ] **Step 1: Write the example**

Create `crates/varve/examples/cache_bench.rs`:

```rust
//! Slice-5 exit criterion: cold vs warm query latency demonstrates the disk
//! cache. Flow: ingest → flush to blocks → reopen with a COLD disk cache
//! (the point query fills it) → reopen again with the SAME cache dir (the
//! cache survived the restart) → the same query is served from disk.
//!
//! Default backend: local FS. Set VARVE_S3_ENDPOINT (+ VARVE_S3_BUCKET,
//! VARVE_S3_ACCESS_KEY_ID, VARVE_S3_SECRET_ACCESS_KEY, optional
//! VARVE_S3_REGION, default "garage") to run against a real S3 backend.
//!
//! Run: cargo run --release --example cache_bench -p varve
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::Path;
use std::time::{Duration, Instant};
use varve::{Config, Db};

const EVENTS: usize = 100_000;
const BATCH: usize = 500;
const LOOKUP_ID: usize = 73_123;

fn toml_escaped(dir: &Path) -> String {
    format!("{:?}", dir.display().to_string())
}

fn storage_toml(root: &Path) -> String {
    if let Ok(endpoint) = std::env::var("VARVE_S3_ENDPOINT") {
        let need = |k: &str| {
            std::env::var(k).unwrap_or_else(|_| panic!("{k} is required with VARVE_S3_ENDPOINT"))
        };
        format!(
            "[storage]\nbackend = \"s3\"\nmax_block_rows = 25000\n[storage.s3]\n\
             endpoint = \"{endpoint}\"\nbucket = \"{}\"\nregion = \"{}\"\n\
             access_key_id = \"{}\"\nsecret_access_key = \"{}\"\n",
            need("VARVE_S3_BUCKET"),
            std::env::var("VARVE_S3_REGION").unwrap_or_else(|_| "garage".into()),
            need("VARVE_S3_ACCESS_KEY_ID"),
            need("VARVE_S3_SECRET_ACCESS_KEY"),
        )
    } else {
        format!(
            "[storage]\nbackend = \"local\"\nmax_block_rows = 25000\n\
             [storage.local]\ndir = {}\n",
            toml_escaped(&root.join("store"))
        )
    }
}

fn config(root: &Path, cache_dir: &Path) -> Config {
    Config::from_toml_str(&format!(
        "[log]\nbackend = \"local\"\ngroup_commit_window_ms = 1\n[log.local]\ndir = {}\n\
         {}\
         [cache]\ntiers = [\"disk\"]\n[cache.disk]\ndir = {}\n",
        toml_escaped(&root.join("log")),
        storage_toml(root),
        toml_escaped(cache_dir),
    ))
    .expect("valid bench config")
}

async fn timed_lookup(root: &Path, cache_dir: &Path) -> Duration {
    let start = Instant::now();
    let db = Db::open(config(root, cache_dir)).await.expect("open");
    let batches = db
        .query(&format!(
            "MATCH (p:Person) WHERE p._id = {LOOKUP_ID} RETURN p.name"
        ))
        .await
        .expect("point lookup");
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 1, "the looked-up person must exist");
    start.elapsed()
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let root = tempfile::tempdir().expect("tempdir");
    let cache_dir = root.path().join("cache");

    let start = Instant::now();
    {
        let db = Db::open(config(root.path(), &cache_dir)).await.expect("open");
        for batch in 0..(EVENTS / BATCH) {
            let mut stmt = String::from("INSERT ");
            for i in 0..BATCH {
                let id = batch * BATCH + i;
                if i > 0 {
                    stmt.push_str(", ");
                }
                stmt.push_str(&format!("(:Person {{_id: {id}, name: 'p{id}'}})"));
            }
            db.execute(&stmt).await.expect("insert batch");
        }
        // 100k events at max_block_rows = 25000 ⇒ 4 blocks; the last flush
        // runs just after the final ack — give it a moment to commit.
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    println!(
        "ingest  {EVENTS} events ({} txs): {:?}",
        EVENTS / BATCH,
        start.elapsed()
    );

    // Ingest only wrote; the cache dir starts effectively empty.
    let cold = timed_lookup(root.path(), &cache_dir).await;
    println!("cold    (disk cache empty):     open+lookup {cold:?}");

    let warm = timed_lookup(root.path(), &cache_dir).await;
    println!("warm    (cache survived reopen): open+lookup {warm:?}");

    let files = std::fs::read_dir(&cache_dir).map(|d| d.count()).unwrap_or(0);
    println!("disk cache: {files} entries under {}", cache_dir.display());
}
```

- [ ] **Step 2: Run it**

Run: `cargo run --release --example cache_bench -p varve`
Expected output shape (local FS; times will vary):

```
ingest  100000 events (200 txs): 2.…s
cold    (disk cache empty):     open+lookup …ms
warm    (cache survived reopen): open+lookup …ms   ← smaller than cold
disk cache: N entries under /…/cache
```

Optionally, with a Garage container up (from Task 9's harness), re-run with the `VARVE_S3_*` env vars set and record the (much larger) cold/warm delta. Record the local numbers in STATUS.md either way.

- [ ] **Step 3: Full gate + commit**

Run: `cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings`
Expected: green (examples are clippy targets — the file-level allow covers the example-style expects).

```bash
git add -A
git commit -m "feat: cache_bench example — cold vs warm disk cache demo (slice-5 exit criterion)"
```

---

## Slice exit checklist

- [ ] **All gates green:** `just check` (fmt + clippy -D warnings + full workspace tests) and `just crash` (the slice-3/4 crash matrix must be untouched by this slice).
- [ ] **Exit criterion — Garage locally:** `just s3-matrix garage` green on this machine (all four phases: storage contract, object-store-log contract, Db e2e with restart, probe). Then `just s3-matrix` for the full local trio if docker resources allow.
- [ ] **Exit criterion — CI matrix:** push and confirm the `backend-matrix` job (garage + seaweedfs + minio) is green in GitHub Actions. If an image tag or CLI-output parse needed fixing at execution time, the fix lives in `backends.rs` and the final pins are recorded in STATUS.md.
- [ ] **Exit criterion — laptop profile unaffected:** `cargo run --example hello -p varve`, `cargo run --release --example write_bench -p varve`, and `cargo run --release --example block_bench -p varve` all still work with zero new requirements (no docker, no s3).
- [ ] **Exit criterion — disk cache demonstrated:** `cargo run --release --example cache_bench -p varve` run and its cold/warm numbers recorded in STATUS.md (plus the Garage-backed numbers if measured).
- [ ] **Probe verdict table pinned:** tighten `expected_probe` in `backend_matrix.rs` from `RecordOnly`/class assertions to the exact observed verdicts (Garage, SeaweedFS, MinIO; Ceph after its first weekly run — leave Ceph `RecordOnly` until then) and record the table in STATUS.md decisions.
- [ ] **STATUS.md updated:**
  - Current position → slice 5 ✅ COMPLETE (+ next action: generate the slice-6 detailed plan — edges, adjacency, traversal — noting slice 6 depends only on slice 4).
  - Decisions: this plan's design decisions 1–8 (§Design decisions), the observed probe verdict table, the final image pins, and any deviations made during execution.
  - Environment facts: `object_store`'s `aws` feature is now default-enabled through varve-storage (adds reqwest/quick-xml transitively — note any Cargo.lock growth); `VARVE_S3_BACKENDS` gates all container tests; docker required only for the matrix.
  - Slice log row for slice 5 (status, sessions, demo command `just s3-matrix garage` + `cargo run --release --example cache_bench -p varve`, notes).
- [ ] **Roadmap ticked:** all five slice-5 checkboxes in `docs/plans/varve-v1-roadmap.md` → `[x]`, with a parenthetical note on any deviation (e.g. "testcontainers" implemented as a docker-CLI harness; trim-as-no-op documented).
- [ ] **Final commit:**

```bash
git add -A
git commit -m "docs: slice 5 complete — s3 backends, object-store log, disk cache, capability probe; STATUS and roadmap updated"
```
