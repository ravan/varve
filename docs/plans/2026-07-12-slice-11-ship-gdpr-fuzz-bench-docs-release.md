# Slice 11 — Ship: GDPR verify, fuzzing, benchmarks, docs, release — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development
> (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close out Varve v1 — prove GDPR erase end-to-end down to raw stored bytes, fuzz every
untrusted decoder, measure and publish the benchmark story against spec §13 targets, ship an
mdBook docs site, and stage release engineering (binaries, image, crates.io metadata, CHANGELOG,
acceptance report) up to the user-gated `v1.0.0` tag.

**Architecture:** No new subsystems. This slice (a) extends the existing pure GC planner so the
raw-byte erase proof can hold on the object-store-log profile, (b) hardens latest-manifest
selection (the slice-10 known limitation), (c) adds fuzz targets over the three untrusted decode
boundaries plus criterion/e2e benches over existing public APIs, and (d) is otherwise docs,
metadata, CI, and verification work on top of the complete slice-0..10 codebase.

**Tech stack:** existing workspace pins (see `Cargo.toml`); new dev/tooling deps: `criterion`
(workspace dev-dep), `cargo-fuzz` (already used; local 0.13.2), `mdbook` (external tool, CI +
local install), `cross` (CI only, aarch64-musl). No new library dependencies in shipped crates.

**Branch:** create `slice-11` from `main` (same convention as `slice-9`/`slice-10`).

## Global Constraints (from the roadmap; apply to every task)

- **TDD, no exceptions:** failing test first, minimal implementation, refactor, commit.
- **Interfaces + registry + composition (spec §4);** engine code never depends on a concrete backend.
- **Sovereignty (spec §1, D7):** nothing may require more than plain S3 PUT/GET/LIST. GC uses the
  sovereign `ObjectStore::delete` added in slice 8 — GC-only, never required for reads/writes.
- **Bitemporal invariant (spec §5.2):** `_system_to` and effective valid ranges never stored.
- **Determinism:** GC planning stays a pure function of `(manifests, listed_keys, config)`.
- Workspace lints: `cargo clippy --workspace --all-targets -- -D warnings`; no `unwrap()`/
  `expect()` in library code (allowed in tests); errors via `thiserror` per crate.
- Timestamps `Timestamp(µs, UTC)`; IIDs `xxh3_128(graph, table, _id)`.
- Commit style: `feat:`/`fix:`/`test:`/`refactor:`/`docs:` prefixes. **NO co-author trailers.**
- Per-task gate before every commit: `cargo fmt --all` then
  `cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings` and the
  task's tests. Whole-slice gate at Task 17.
- **We are in development. NO backward compatibility, production code only** — replace/delete
  freely (e.g. Task 1 rewrites the GC log/probe policy, Task 14 deletes a dead error variant).
- **Test code is the contract** (slice-1 rule): implementation sketches in this plan may need
  adaptation to the live APIs; the tests' asserted behavior may not be weakened to fit a sketch.
  External-tool versions (criterion, mdbook, cross, gh) must be re-verified live at execution;
  the plan pins intent, not exact tool versions.

## Inputs verified against the live tree (2026-07-12)

- `crates/varve/tests/gdpr_gc.rs` — slice-8 erase proof scans ONLY `v1/graphs/**.arrow`.
- `crates/varve-engine/src/gc.rs` — `plan_gc(manifests, listed_keys, config) -> GcPlan` is pure;
  `should_delete_key` currently REFUSES `v1/log` and `v1/probe` keys
  (`gc_plan_keeps_probe_and_log_objects_out_of_scope`). The object-store log's `trim` is a
  documented no-op that *promises* "superseded log objects are swept by slice-8 GC"
  (`crates/varve-log/src/object_store.rs` module doc) — that sweep DOES NOT EXIST YET. Task 1
  makes the promise true; without it the slice's raw-byte erase proof cannot hold on the
  object-store-log profile.
- `crates/varve-storage/src/keys.rs` — `LOG_PREFIX = "v1/log"`, `parse_log_key(&str) ->
  Option<LogPosition>`, `PROBE_PREFIX = "v1/probe"` (re-exported from `probe.rs`),
  `EPOCH_FENCE_PREFIX = "v1/epochs"`, `MANIFEST_PREFIX = "v1/blocks"`.
- `varve_types::LogPosition` — `as_u64()/from_u64()` packed form (sort-correct), `ZERO`, `advance`.
- `crates/varve-storage/src/manifest.rs` — `BlockManifest::{from_wire, to_wire, trie_entries}`,
  `latest_manifest` picks max `block_id` (Task 4 hardens to `(watermark, block_id)`),
  `manifest_history` returns all manifests sorted by block id.
- `crates/varve-log/src/record.rs` — `LogRecord::{to_wire, from_wire, wire_len}`; the frame
  grammar (`len u32 LE · crc32c u32 LE · payload`) is implemented privately in `local.rs`
  (`scan_segment`) and `object_store.rs` (`decode_object`/`decode_frame`).
- `crates/varve-index` — `pub fn decode_events(bytes: &[u8]) -> Result<Vec<Event>, IndexError>`
  (`codec.rs`), `pub fn decode_meta(bytes: &[u8]) -> Result<Vec<PageMeta>, IndexError>` (`block.rs`),
  `pub fn resolve<'a>(events: &'a [Event], bounds: &TemporalBounds) -> Vec<ResolvedVersion<'a>>`
  (`bitemporal.rs`).
- `varve_types::trie::Bucketer` — `bucket(iid, level) -> Option<u8>`, `path(iid, levels) ->
  Option<Vec<u8>>`, `contains(path, iid) -> bool`.
- `varve_gql::parse_program(gql: &str) -> Result<Program, GqlError>`; existing fuzz target
  `fuzz/fuzz_targets/parse.rs` (parse-print-reparse), `fuzz/Cargo.toml` deps only `varve-gql`.
- `crates/varve-server/tests/support/process_cluster.rs` — `ProcessCluster::start()` spawns
  1 writer + exactly 2 query `varved` processes; `tx`, `query_row_count`, `writer_url`,
  `query_urls` helpers; `TxReceipt { tx_id, basis }`.
- `varve_testkit::fixture::social_graph(nodes, edges, seed)` with `node_statements(chunk)` /
  `edge_programs(chunk)`; `varve` already dev-deps `varve-testkit` (see `tests/gdpr_gc.rs`).
- `varve_testkit::db_harness` — `local_gc_blocks_config(root, max_block_rows)`, `row_count`,
  `wait_for_manifest_count(root, count)`, `compact_until_idle(db)`, `toml_escaped_path`.
- `[log.local] segment_max_bytes` is a real config key (serde default 64 MiB,
  `DEFAULT_SEGMENT_MAX_BYTES`); the object-store log factory registers as
  `[log] backend = "object-store"` and takes the raw store from `BuildContext` (no own keys).
- CI (`.github/workflows/ci.yml`): jobs `check`, `gql-differential`, `property-nightly` (0 3 cron),
  `chaos-nightly` (0 5 cron), `fuzz-nightly` (0 3 cron, parse only), `crash-matrix`,
  `backend-matrix`, `backend-ceph-weekly`, `container-image`.
- Publish readiness gaps: NO crate has `description`; NO crate sets `publish = false`; internal
  deps are `path`-only (no `version`); NO `LICENSE` file at repo root; workspace version `0.1.0`;
  `repository = "https://github.com/ravan/varve"` while the repo dir is `timedb` (rename pending —
  user's call, surfaced in Task 15).
- Slice-11-owned deferred items (STATUS.md): latest-manifest `(watermark, block_id)` hardening
  (slice-10 Important-2 follow-up); CLI `--label`/`--graph` rejection + `cli_help` literal
  substrings tests; `TxResponse::from_receipt` synthetic-instant decision;
  `Db::publish_writer` doc-comment move; testkit-dev-dep fault-injection note; slice-10 SAFE-TO-DEFER
  items (1) probe-test unwrap diagnostics, (2) `FenceMap` `is_dead` extraction + `EpochExhausted`
  test, (5) `EngineError::InvalidCoordinatorConfig` declared-never-constructed.

## File structure (created/modified by this slice)

```
crates/varve-engine/src/gc.rs                 # T1 log+probe sweep in plan_gc/execute_gc
crates/varve-engine/src/db.rs                 # T14 delete dead variant, move doc comment
crates/varve-engine/src/coord/fence.rs        # T14 is_dead extraction + EpochExhausted test
crates/varve-engine/src/writer.rs             # T4 limitation comment update
crates/varve-engine/src/flush.rs              # T4 limitation comment update
crates/varve-storage/src/manifest.rs          # T4 latest_manifest (watermark, block_id)
crates/varve-log/src/record.rs                # T5 pub decode_frames + seed-writer test
crates/varve-log/src/object_store.rs          # T5 decode_object delegates to decode_frames
crates/varve/tests/gdpr_gc.rs                 # T2/T3 full-byte erase proofs
crates/varve-testkit/src/db_harness.rs        # T2/T3 config helpers
crates/varve-testkit/src/config_reference.rs  # T13 generator module
crates/varve-testkit/src/bin/config_reference.rs  # T13 generator bin
crates/varve-testkit/tests/config_reference_doc.rs # T13 drift test
crates/varve-testkit/tests/cas_failover_backends.rs # T14 diagnostic asserts
crates/varve-index/benches/resolution.rs      # T7 criterion
crates/varve-types/benches/trie.rs            # T7 criterion
crates/varve-gql/benches/parse.rs             # T7 criterion
crates/varve/examples/social_bench.rs         # T8 e2e workload bench
crates/varve-server/tests/support/process_cluster.rs # T9 start_with_query_nodes
crates/varve-server/tests/scale_out_bench.rs  # T9 env-gated 1→2→4 bench
crates/varve-server/src/api.rs                # T14 from_receipt pin
crates/varve-cli/tests/transfer.rs            # T14 invalid identifier tests
crates/varve-cli/tests/cli_help.rs            # T14 literal --help substrings
fuzz/Cargo.toml                               # T5/T6 new targets + deps
fuzz/fuzz_targets/{log_record,manifest,block_meta,events}.rs # T5/T6
fuzz/corpus/{log_record,manifest,block_meta,events}/          # T5/T6 committed seeds
docs/book/                                    # T10–T13 mdBook site
docs/benchmarks/v1.md                         # T10 report
docs/release/v1-acceptance.md                 # T17
docs/ops/metrics.md                           # T12 becomes pointer stub (content moves to book)
CHANGELOG.md, LICENSE, README.md              # T15
.github/workflows/ci.yml                      # T6 fuzz-nightly matrix, T10 docs job
.github/workflows/release.yml                 # T16
scripts/package_release.sh                    # T16
justfile                                      # T6/T7/T9/T10 recipes
Cargo.toml (workspace)                        # T7 criterion dev-dep, T15 version 1.0.0
crates/*/Cargo.toml                           # T15 publish metadata
docs/plans/STATUS.md, docs/plans/varve-v1-roadmap.md # T17 closeout
```

## Session grouping (2–3 sessions)

- **Session A — correctness (Tasks 1–6):** GC log/probe sweep, both GDPR raw-byte proofs,
  latest-manifest hardening, all four fuzz targets + nightly CI.
- **Session B — measurement + docs (Tasks 7–13):** criterion micro benches, social e2e bench,
  scale-out bench, benchmark report, mdBook site, config reference.
- **Session C — ship (Tasks 14–17):** deferred-item sweep, release metadata, release workflow,
  acceptance pass + closeout + user-gated tag/publish.

---

### Task 1: GC sweeps superseded log objects and probe objects

The object-store log documents `trim` as a no-op *because* "superseded log objects are swept by
slice-8 GC" — but `plan_gc` explicitly refuses every `v1/log` and `v1/probe` key. Make the
documented policy real: a log object is deletable when **every record in it** lies strictly below
the minimum watermark of all retained manifests; probe objects (single-call transients) are always
deletable. Epoch-fence objects (`v1/epochs/`) and `v1/writer.json` are never listed by GC and stay
untouched.

**Files:**
- Modify: `crates/varve-engine/src/gc.rs` (`execute_gc`, `plan_gc`, `should_delete_key`, tests)

**Interfaces:**
- Consumes: `varve_storage::keys::{LOG_PREFIX, parse_log_key}`, `varve_storage::PROBE_PREFIX`,
  `varve_types::LogPosition::{as_u64, from_u64}`, `BlockManifest.watermark: u64` (packed
  `LogPosition`).
- Produces: unchanged public surface (`GcConfig`, `GcReport`, `Db::gc_once`). Internal:
  `plan_gc(manifests, listed_keys, config) -> GcPlan` now returns log/probe keys in
  `delete_keys`; new private helper
  `fn deletable_log_keys(log_keys: &[String], min_retained_watermark: Option<u64>) -> Vec<String>`.
  Tasks 2–3 rely on `Db::gc_once()` deleting swept log objects.

**Deletion rule (document as rustdoc on `deletable_log_keys`):** parse every listed key with
`parse_log_key` (unparseable keys under the prefix are foreign — never touched), sort by packed
first-position. A log object spans `[first_i, first_{i+1})`; object *i* is deletable iff
`first_{i+1}.as_u64() <= min_retained_watermark`. The LAST object is never deletable (its span is
open). `min_retained_watermark` = min `watermark` over the retained manifest set (the same
`block_id >= retain_from` set `plan_gc` already protects); `None` (no manifests) ⇒ no log
deletion. Positions are compared in packed form — epoch bumps sort correctly, and a fenced
zombie's stale-position object simply becomes sweepable garbage. Safety note (also rustdoc): a
query follower lagging below the min retained watermark loses its tail and terminates with
`LogGap` (restart recovers from the latest manifest); `blocks_to_keep`/`garbage_lifetime` are the
operator's guard — cross-referenced in the ops guide (Task 12).

- [ ] **Step 1: Write the failing planner tests** (replace
  `gc_plan_keeps_probe_and_log_objects_out_of_scope` — its policy is now wrong; production code,
  no back-compat). Add to `gc.rs` `mod tests` (the existing `manifest(...)` fixture sets
  `watermark: block_id`; add a variant with an explicit packed watermark):

```rust
fn manifest_with_watermark(block_id: u64, watermark: u64, tries: Vec<&str>) -> BlockManifest {
    let mut m = manifest(block_id, 100, tries);
    m.watermark = watermark;
    m
}

#[test]
fn gc_plan_sweeps_log_objects_wholly_below_the_min_retained_watermark() {
    // Retained manifest watermark = position 4 (epoch 0). Log objects start at 0, 2, 4, 6.
    // Object@0 spans [0,2) and object@2 spans [2,4): both wholly < 4 → swept.
    // Object@4 spans [4,6) and object@6 is last: kept.
    let w = LogPosition::new(0, 4).unwrap().as_u64();
    let manifests = vec![manifest_with_watermark(10, w, vec!["l00-rc-b10"])];
    let keys: Vec<String> = [0u64, 2, 4, 6]
        .into_iter()
        .map(|off| log_key(LogPosition::new(0, off).unwrap()))
        .collect();
    let plan = plan(&manifests, keys.clone(), enabled_config());
    assert!(plan.delete_keys.contains(&keys[0]));
    assert!(plan.delete_keys.contains(&keys[1]));
    assert!(!plan.delete_keys.contains(&keys[2]));
    assert!(!plan.delete_keys.contains(&keys[3]));
}

#[test]
fn gc_plan_uses_the_minimum_watermark_across_retained_manifests() {
    // blocks_to_keep = 1 retains blocks 9 and 10; block 9's watermark (2) is the floor,
    // so only the object whose SUCCESSOR starts at <= 2 is swept.
    let w9 = LogPosition::new(0, 2).unwrap().as_u64();
    let w10 = LogPosition::new(0, 6).unwrap().as_u64();
    let manifests = vec![
        manifest_with_watermark(9, w9, vec!["l00-rc-b09"]),
        manifest_with_watermark(10, w10, vec!["l00-rc-b10"]),
    ];
    let mut config = enabled_config();
    config.blocks_to_keep = 1;
    let keys: Vec<String> = [0u64, 2, 4]
        .into_iter()
        .map(|off| log_key(LogPosition::new(0, off).unwrap()))
        .collect();
    let plan = plan(&manifests, keys.clone(), config);
    assert_eq!(
        plan.delete_keys.iter().filter(|k| k.starts_with("v1/log")).collect::<Vec<_>>(),
        vec![&keys[0]]
    );
}

#[test]
fn gc_plan_boundary_spanning_and_last_log_objects_are_kept() {
    // Watermark 3 falls INSIDE object@2's span [2,4): object@2 kept. object@0 swept.
    let w = LogPosition::new(0, 3).unwrap().as_u64();
    let manifests = vec![manifest_with_watermark(10, w, vec!["l00-rc-b10"])];
    let keys: Vec<String> = [0u64, 2]
        .into_iter()
        .map(|off| log_key(LogPosition::new(0, off).unwrap()))
        .collect();
    let plan = plan(&manifests, keys.clone(), enabled_config());
    assert!(plan.delete_keys.contains(&keys[0]));
    assert!(!plan.delete_keys.contains(&keys[1]));
}

#[test]
fn gc_plan_sweeps_across_epoch_bumps_in_packed_order() {
    // Fenced epoch 0 objects at 0 and 3; epoch 1 resumed at offset 3; retained
    // watermark = (1, 5). Epoch-0 object@0 (successor (0,3) <= w) and object@(0,3)
    // (successor (1,3) <= w) are swept; (1,3) is followed by (1,5) <= w → swept too;
    // (1,5) is last → kept.
    let w = LogPosition::new(1, 5).unwrap().as_u64();
    let manifests = vec![manifest_with_watermark(10, w, vec!["l00-rc-b10"])];
    let positions = [
        LogPosition::new(0, 0).unwrap(),
        LogPosition::new(0, 3).unwrap(),
        LogPosition::new(1, 3).unwrap(),
        LogPosition::new(1, 5).unwrap(),
    ];
    let keys: Vec<String> = positions.into_iter().map(log_key).collect();
    let plan = plan(&manifests, keys.clone(), enabled_config());
    assert!(plan.delete_keys.contains(&keys[0]));
    assert!(plan.delete_keys.contains(&keys[1]));
    assert!(plan.delete_keys.contains(&keys[2]));
    assert!(!plan.delete_keys.contains(&keys[3]));
}

#[test]
fn gc_plan_keeps_foreign_keys_under_the_log_prefix() {
    let w = LogPosition::new(0, 9).unwrap().as_u64();
    let manifests = vec![manifest_with_watermark(10, w, vec!["l00-rc-b10"])];
    let keys = vec!["v1/log/0000/notavlog.txt".to_string(), "v1/logish".to_string()];
    let plan = plan(&manifests, keys, enabled_config());
    assert!(plan.delete_keys.iter().all(|k| !k.contains("log")));
}

#[test]
fn gc_plan_without_manifests_never_touches_log_objects() {
    let keys = vec![log_key(LogPosition::new(0, 0).unwrap())];
    let plan = plan(&[], keys, enabled_config());
    assert!(plan.delete_keys.is_empty());
}

#[test]
fn gc_plan_sweeps_probe_objects_and_keeps_fence_and_writer_keys() {
    let manifests = vec![manifest(10, 100, vec!["l00-rc-b10"])];
    let keys = vec![
        format!("{PROBE_PREFIX}/deadbeef"),
        "v1/epochs/0001.json".to_string(),
        "v1/writer.json".to_string(),
    ];
    let plan = plan(&manifests, keys, enabled_config());
    assert_eq!(plan.delete_keys, vec![format!("{PROBE_PREFIX}/deadbeef")]);
}
```

- [ ] **Step 2: Run to verify the new tests fail** (and the old out-of-scope test is deleted):
  `cargo test -p varve-engine gc_plan` → new tests FAIL (log/probe keys not in `delete_keys`).

- [ ] **Step 3: Implement.** In `plan_gc`: compute
  `let min_retained_watermark: Option<u64> = /* min over retained manifests' .watermark */;`
  inside the existing `if let Some(latest)` block (the retained set is the manifests with
  `block_id >= retain_from`). Split key handling:

```rust
let (log_keys, other_keys): (Vec<String>, Vec<String>) = listed
    .into_iter()
    .partition(|key| has_path_prefix(key, keys::LOG_PREFIX));

let mut delete_keys: Vec<String> = other_keys
    .into_iter()
    .filter(|key| should_delete_key(key, &protected, &sorted_manifests, config))
    .collect();
delete_keys.extend(deletable_log_keys(&log_keys, min_retained_watermark));
delete_keys.sort();
GcPlan { delete_keys }
```

  `should_delete_key` drops its `LOG_PREFIX` refusal arm (log keys are pre-partitioned) and its
  `PROBE_PREFIX` arm becomes `return true`. `deletable_log_keys`:

```rust
fn deletable_log_keys(log_keys: &[String], min_retained_watermark: Option<u64>) -> Vec<String> {
    let Some(watermark) = min_retained_watermark else {
        return Vec::new();
    };
    let mut objects: Vec<(u64, &String)> = log_keys
        .iter()
        .filter_map(|key| keys::parse_log_key(key).map(|p| (p.as_u64(), key)))
        .collect();
    objects.sort_by_key(|(pos, _)| *pos);
    objects
        .windows(2)
        .filter(|pair| pair[1].0 <= watermark)
        .map(|pair| pair[0].1.clone())
        .collect()
}
```

  In `execute_gc`, extend the listing:

```rust
listed_keys.extend(store.list(keys::LOG_PREFIX).await?);
listed_keys.extend(store.list(PROBE_PREFIX).await?);
```

  Add the safety rustdoc (lagging-follower `LogGap` + retention knobs + probe-race note: a
  capability probe racing a concurrent GC may lose its probe object mid-probe and should be
  retried; probes run at node startup, GC on the writer, so the window is operationally narrow).

- [ ] **Step 4: Run** `cargo test -p varve-engine` → all gc tests PASS; full crate green.

- [ ] **Step 5: Update the stale `trim` promise comment** in
  `crates/varve-log/src/object_store.rs` module doc from "swept by slice-8 GC" to "swept by GC
  (`Db::gc_once`) once wholly below the minimum retained manifest watermark" — the promise is now
  true; keep it accurate.

- [ ] **Step 6: Gate + commit**

```bash
cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings && cargo test -p varve-engine -p varve-log
git add crates/varve-engine/src/gc.rs crates/varve-log/src/object_store.rs
git commit -m "feat: gc sweeps superseded log objects and probe objects"
```

---

### Task 2: GDPR ERASE end-to-end proof — local profile, every stored byte

Extend the slice-8 proof from "graph data objects" to **every byte Varve persists on the local
profile**: all objects under `dir/store` (all prefixes — data, meta, manifests, anything) AND all
log segment bytes under `dir/log`. Also prove the erase through the GQL surface at every time
axis after compaction AND after restart, and prove it for edge properties via `DETACH ERASE`.

**Files:**
- Modify: `crates/varve/tests/gdpr_gc.rs` (new helpers + 2 tests)
- Modify: `crates/varve-testkit/src/db_harness.rs` (config helper with tiny log segments)

**Interfaces:**
- Consumes: `Db::{open, execute, query, compact_once, gc_once}`, `TxReceipt.system_time`,
  `db_harness::{row_count, wait_for_manifest_count, compact_until_idle, toml_escaped_path}`.
- Produces: `pub fn local_gc_small_segment_config(root: &Path, max_block_rows: usize) -> Config`
  in `db_harness` — the gc config plus `[log.local] segment_max_bytes = 256` (every group-commit
  batch rolls the segment, so `Log::trim` can drop every superseded segment; the ACTIVE segment
  is never deleted, which is why the sentinel must be written early and followed by fillers).
  New test helper `fn all_disk_bytes(root: &Path) -> Vec<u8>` (recursive walk of `root`, i.e.
  BOTH `dir/log` and `dir/store`, concatenating every file's bytes; `std::fs` only).

- [ ] **Step 1: Write the failing tests**

```rust
fn all_disk_bytes(root: &Path) -> Vec<u8> {
    fn visit(dir: &Path, out: &mut Vec<u8>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                visit(&path, out);
            } else {
                out.extend_from_slice(&std::fs::read(&path).unwrap());
            }
        }
    }
    let mut out = Vec::new();
    visit(root, &mut out);
    out
}

fn contains(haystack: &[u8], needle: &str) -> bool {
    haystack.windows(needle.len()).any(|w| w == needle.as_bytes())
}

#[tokio::test]
async fn erased_bytes_absent_from_every_stored_object_and_log_segment() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(small_segment_config(dir.path(), 1)).await.unwrap();
    let secret = "gdpr-fullscan-sentinel-3c1d9a77";

    let inserted = db
        .execute(&format!("INSERT (:P {{_id: 1, token: '{secret}'}})"))
        .await
        .unwrap();
    let erased = db.execute("MATCH (p:P {_id: 1}) ERASE p").await.unwrap();
    for id in 2..=62 {
        db.execute(&format!("INSERT (:P {{_id: {id}, token: 'filler-{id}'}})"))
            .await
            .unwrap();
    }
    wait_for_manifest_count(dir.path(), 63).await;

    // Non-vacuous: the sentinel IS on disk before compaction+GC (in a log
    // segment and/or an L0 block).
    assert!(contains(&all_disk_bytes(dir.path()), secret));

    compact_until_idle(&db).await.unwrap();
    db.gc_once().await.unwrap();

    // THE slice exit assertion: no stored byte anywhere still spells the secret.
    assert!(!contains(&all_disk_bytes(dir.path()), secret));

    // Invisibility on every time axis survives compaction…
    for gql in [
        "MATCH (p:P {_id: 1}) RETURN p.token".to_string(),
        format!(
            "FOR SYSTEM_TIME AS OF TIMESTAMP '{}' MATCH (p:P {{_id: 1}}) RETURN p.token",
            inserted.system_time
        ),
        format!(
            "FOR SYSTEM_TIME AS OF TIMESTAMP '{}' MATCH (p:P {{_id: 1}}) RETURN p.token",
            erased.system_time
        ),
    ] {
        assert_eq!(rows(&db.query(&gql).await.unwrap()), 0, "visible via: {gql}");
    }

    // …and restart.
    drop(db);
    let db = Db::open(small_segment_config(dir.path(), 1)).await.unwrap();
    assert!(!contains(&all_disk_bytes(dir.path()), secret));
    assert_eq!(
        rows(&db.query("MATCH (p:P {_id: 1}) RETURN p.token").await.unwrap()),
        0
    );
    assert_eq!(
        rows(&db.query("MATCH (p:P) RETURN p.token").await.unwrap()),
        61 // the fillers survive untouched
    );
}

#[tokio::test]
async fn detach_erase_scrubs_edge_property_bytes_too() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(small_segment_config(dir.path(), 1)).await.unwrap();
    let node_secret = "gdpr-node-sentinel-91b2e6f0";
    let edge_secret = "gdpr-edge-sentinel-5a44c8d2";

    db.execute(&format!(
        "INSERT (:P {{_id: 1, token: '{node_secret}'}}), (:P {{_id: 2, token: 'keep'}})"
    ))
    .await
    .unwrap();
    db.execute(&format!(
        "MATCH (a:P {{_id: 1}}) MATCH (b:P {{_id: 2}}) \
         INSERT (a)-[:KNOWS {{note: '{edge_secret}'}}]->(b)"
    ))
    .await
    .unwrap();
    db.execute("MATCH (p:P {_id: 1}) DETACH ERASE p").await.unwrap();
    for id in 3..=62 {
        db.execute(&format!("INSERT (:P {{_id: {id}}})")).await.unwrap();
    }
    wait_for_manifest_count(dir.path(), 63).await;
    assert!(contains(&all_disk_bytes(dir.path()), edge_secret));

    compact_until_idle(&db).await.unwrap();
    db.gc_once().await.unwrap();

    let bytes = all_disk_bytes(dir.path());
    assert!(!contains(&bytes, node_secret));
    assert!(!contains(&bytes, edge_secret)); // adjacency families scrubbed too
    assert_eq!(
        rows(&db.query("MATCH (:P)-[k:KNOWS]->(:P) RETURN k.note").await.unwrap()),
        0
    );
}
```

  (`small_segment_config` is a local alias `use varve_testkit::db_harness::
  local_gc_small_segment_config as small_segment_config;`. Multi-statement mutation programs and
  `MATCH … INSERT` edge syntax follow the existing slice-6/7 test corpus — if the exact INSERT
  form differs, mirror `crates/varve/tests/erase.rs`; the ASSERTIONS are the contract.)

- [ ] **Step 2: Add the config helper to `db_harness.rs`** (same shape as
  `local_gc_blocks_config`, plus one line in `[log.local]`):

```rust
pub fn local_gc_small_segment_config(root: &Path, max_block_rows: usize) -> Config {
    let log_dir = toml_escaped_path(&root.join("log"));
    let store_dir = toml_escaped_path(&root.join("store"));
    Config::from_toml_str(&format!(
        "[log]\n\
         backend = \"local\"\n\
         group_commit_window_ms = 1\n\
         [log.local]\n\
         dir = {log_dir}\n\
         segment_max_bytes = 256\n\
         [storage]\n\
         backend = \"local\"\n\
         max_block_rows = {max_block_rows}\n\
         [storage.local]\n\
         dir = {store_dir}\n\
         [gc]\n\
         enabled = true\n\
         blocks_to_keep = 0\n\
         garbage_lifetime_hours = 0\n"
    ))
    .unwrap_or_else(|error| panic!("small segment gc config should parse: {error}"))
}
```

- [ ] **Step 3: Run to verify current behavior** —
  `cargo test -p varve --test gdpr_gc -- --test-threads=1`. Expected: the new tests FAIL at the
  post-GC full-scan assertion IF any sentinel byte survives (e.g. in a log segment the trim did
  not drop, or an adjacency family compaction missed). Investigate any failure with
  superpowers:systematic-debugging — the test is the spec; likely knobs: sentinel must be in a
  ROLLED segment (`segment_max_bytes = 256` guarantees roll-per-batch), and `compact_until_idle`
  must run enough L0→L1 rounds. If the tests instead pass immediately, verify non-vacuity: the
  pre-GC `assert!(contains(...))` must have run (it fails the test if the sentinel never hit disk).

- [ ] **Step 4: Fix until green.** Expected fixes are test-side (config/sequencing), NOT
  engine-side — slice 8 already proved erase-drops-bytes for compaction; the new ground the test
  breaks is (a) the whole-`dir` scan incl. log segments, and (b) edge/adjacency sentinel bytes.
  If an ENGINE gap is found (e.g. adjacency family retains erased-edge payload bytes), fix it in
  `varve-engine`/`varve-index` with its own failing unit test first and record the deviation in
  STATUS.md at closeout.

- [ ] **Step 5: Gate + commit**

```bash
cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings && cargo test -p varve --test gdpr_gc -p varve-testkit --lib -- --test-threads=1
git add crates/varve/tests/gdpr_gc.rs crates/varve-testkit/src/db_harness.rs
git commit -m "test: GDPR erase proof scans every stored byte on the local profile"
```

---

### Task 3: GDPR ERASE proof — object-store-log profile (the raw-object scan)

Same proof with `[log] backend = "object-store"`: log batches live as `v1/log/**.vlog` objects in
the SAME store as blocks, and only Task 1's GC sweep can remove them. This is the roadmap's
literal "raw object scan" test: list every key in the store, GET every object, grep for the
sentinel.

**Files:**
- Modify: `crates/varve/tests/gdpr_gc.rs` (1 test + helper)
- Modify: `crates/varve-testkit/src/db_harness.rs` (config helper)

**Interfaces:**
- Consumes: Task 1 (`gc_once` sweeps log objects), `varve_storage::local_store(dir) ->
  Result<Arc<dyn ObjectStore>, StorageError>` (as used by the existing `graph_object_bytes`),
  `ObjectStore::{list, get}`.
- Produces: `pub fn object_log_gc_config(root: &Path, max_block_rows: usize) -> Config` in
  `db_harness` — local block store at `root/store`, `[log] backend = "object-store"`,
  `group_commit_window_ms = 1`, gc enabled with `blocks_to_keep = 0`,
  `garbage_lifetime_hours = 0`.

- [ ] **Step 1: Write the failing test**

```rust
async fn every_object_byte(dir: &Path) -> Vec<u8> {
    let store = varve_storage::local_store(&dir.join("store")).unwrap();
    let mut bytes = Vec::new();
    for key in store.list("v1").await.unwrap() {
        bytes.extend_from_slice(&store.get(&key).await.unwrap());
    }
    bytes
}

#[tokio::test]
async fn erased_bytes_absent_from_every_raw_object_on_the_object_store_log_profile() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(object_log_gc_config(dir.path(), 1)).await.unwrap();
    let secret = "gdpr-objectlog-sentinel-77aa01ce";

    db.execute(&format!("INSERT (:P {{_id: 1, token: '{secret}'}})"))
        .await
        .unwrap();
    db.execute("MATCH (p:P {_id: 1}) ERASE p").await.unwrap();
    for id in 2..=62 {
        db.execute(&format!("INSERT (:P {{_id: {id}}})")).await.unwrap();
    }
    wait_for_manifest_count(dir.path(), 63).await;

    // Non-vacuous both ways: the sentinel is in at least one raw object, and at
    // least one of those objects is a LOG object (the profile under test).
    let store = varve_storage::local_store(&dir.path().join("store")).unwrap();
    let mut sentinel_log_objects = 0usize;
    for key in store.list("v1/log").await.unwrap() {
        if contains(&store.get(&key).await.unwrap(), secret) {
            sentinel_log_objects += 1;
        }
    }
    assert!(sentinel_log_objects > 0, "sentinel never reached a log object");

    compact_until_idle(&db).await.unwrap();
    db.gc_once().await.unwrap();

    assert!(!contains(&every_object_byte(dir.path()).await, secret));

    // The store still works: fillers intact, erased id gone, restart clean.
    assert_eq!(rows(&db.query("MATCH (p:P) RETURN p._id").await.unwrap()), 61);
    drop(db);
    let db = Db::open(object_log_gc_config(dir.path(), 1)).await.unwrap();
    assert_eq!(rows(&db.query("MATCH (p:P {_id: 1}) RETURN p._id").await.unwrap()), 0);
}
```

- [ ] **Step 2: Add `object_log_gc_config` to `db_harness.rs`**

```rust
pub fn object_log_gc_config(root: &Path, max_block_rows: usize) -> Config {
    let store_dir = toml_escaped_path(&root.join("store"));
    Config::from_toml_str(&format!(
        "[log]\n\
         backend = \"object-store\"\n\
         group_commit_window_ms = 1\n\
         [storage]\n\
         backend = \"local\"\n\
         max_block_rows = {max_block_rows}\n\
         [storage.local]\n\
         dir = {store_dir}\n\
         [gc]\n\
         enabled = true\n\
         blocks_to_keep = 0\n\
         garbage_lifetime_hours = 0\n"
    ))
    .unwrap_or_else(|error| panic!("object log gc config should parse: {error}"))
}
```

- [ ] **Step 3: Run** `cargo test -p varve --test gdpr_gc -- --test-threads=1`. Without Task 1
  the post-GC assertion fails on surviving `v1/log/**.vlog` bytes; with Task 1 it must pass.
  Note: with `blocks_to_keep = 0` only the latest manifest is retained, so the min retained
  watermark is the latest flush watermark — every fully-superseded log object qualifies. The last
  log object is never deleted; the test's 61 filler txs after the erase guarantee the sentinel
  object has successors at or below the final watermark.

- [ ] **Step 4: Gate + commit**

```bash
cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings && cargo test -p varve --test gdpr_gc -- --test-threads=1
git add crates/varve/tests/gdpr_gc.rs crates/varve-testkit/src/db_harness.rs
git commit -m "test: GDPR raw-object erase proof on the object-store log profile"
```

---

### Task 4: Latest-manifest selection hardened to `(watermark, block_id)`

Closes the slice-10 KNOWN LIMITATION follow-up: the manifest PUT has no epoch-fence equivalent,
so a narrow alive-but-fenced race could leave a STRAY manifest with a higher `block_id` but a
STALE watermark. Selecting the latest manifest by `(watermark, block_id)` (max watermark first,
block id as tiebreak) makes such a stray manifest permanently unable to win recovery/verify/
follower reads.

**Files:**
- Modify: `crates/varve-storage/src/manifest.rs` (`latest_manifest` + tests)
- Modify: `crates/varve-engine/src/writer.rs` (limitation comment at `gated_compact`, ~line 1877)
- Modify: `crates/varve-engine/src/flush.rs` (limitation comment in the module/flush doc)

**Interfaces:**
- Consumes: existing `manifest_history(store) -> Result<Vec<BlockManifest>, StorageError>`.
- Produces: `latest_manifest(store) -> Result<Option<BlockManifest>, StorageError>` — SAME
  signature, new selection rule. Every existing caller (`Db::open` recovery, `verify_database`,
  follower manifest reads, `/v1/status`) inherits the hardening with no call-site change.

- [ ] **Step 1: Write the failing tests** (in `manifest.rs` `mod tests`, alongside
  `latest_manifest_picks_the_highest_block_id` — REPLACE that test, its rule is superseded):

```rust
#[tokio::test]
async fn latest_manifest_picks_the_highest_watermark_not_block_id() {
    let store = memory_store();
    let good = BlockManifest { block_id: 10, watermark: 500, ..sample() };
    // A fenced writer's stray manifest: newer block id, STALE watermark.
    let stray = BlockManifest { block_id: 11, watermark: 400, ..sample() };
    store.put(&manifest_key(10), Bytes::from(good.to_wire())).await.unwrap();
    store.put(&manifest_key(11), Bytes::from(stray.to_wire())).await.unwrap();

    let latest = latest_manifest(store.as_ref()).await.unwrap().unwrap();
    assert_eq!((latest.watermark, latest.block_id), (500, 10));
}

#[tokio::test]
async fn latest_manifest_breaks_watermark_ties_by_block_id() {
    // Compaction manifests legitimately share the flush watermark; the newer
    // block id (the compaction result) must win, preserving today's behavior.
    let store = memory_store();
    let flush = BlockManifest { block_id: 10, watermark: 500, ..sample() };
    let compaction = BlockManifest { block_id: 11, watermark: 500, ..sample() };
    store.put(&manifest_key(10), Bytes::from(flush.to_wire())).await.unwrap();
    store.put(&manifest_key(11), Bytes::from(compaction.to_wire())).await.unwrap();

    let latest = latest_manifest(store.as_ref()).await.unwrap().unwrap();
    assert_eq!(latest.block_id, 11);
}
```

  (Adapt the struct-update `..sample()` spelling to the existing `sample()` fixture — if its
  fields differ, set `block_id`/`watermark` explicitly after cloning. Keep
  `latest_manifest_none_when_empty` and `latest_manifest_surfaces_corruption` — corruption in ANY
  listed manifest now surfaces, which is strictly stricter and correct.)

- [ ] **Step 2: Run** `cargo test -p varve-storage latest_manifest` → new tests FAIL
  (stray block 11 wins under the old rule).

- [ ] **Step 3: Implement**

```rust
/// Finds the newest COMMITTED manifest: max by `(watermark, block_id)`.
/// Watermark-first makes a fenced writer's stray manifest (higher block id,
/// stale watermark — the slice-10 known limitation) permanently unable to
/// win; block id breaks the flush-vs-compaction tie at equal watermarks.
/// Reads every listed manifest (bounded: GC retains `blocks_to_keep + 1`).
pub async fn latest_manifest(
    store: &dyn ObjectStore,
) -> Result<Option<BlockManifest>, StorageError> {
    let manifests = manifest_history(store).await?;
    Ok(manifests
        .into_iter()
        .max_by_key(|manifest| (manifest.watermark, manifest.block_id)))
}
```

- [ ] **Step 4: Run** `cargo test -p varve-storage && cargo test -p varve-engine` → PASS. The
  engine's recovery/follower/verify suites must stay green (equal-watermark tiebreak preserves
  every legitimate ordering they rely on).

- [ ] **Step 5: Update the two limitation comments.** In `writer.rs` (the block around line
  1877 discussing the in-flight manifest PUT) and `flush.rs` (manifest-PUT commit-point doc):
  replace "robust hardening ... is a Slice-11 follow-up" wording with a statement that
  `latest_manifest` now selects by `(watermark, block_id)`, so a stray stale-watermark manifest
  can never be selected; the before+after lease ack-gate remains the liveness guard.

- [ ] **Step 6: Gate + commit**

```bash
cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings && cargo test -p varve-storage -p varve-engine
git add crates/varve-storage/src/manifest.rs crates/varve-engine/src/writer.rs crates/varve-engine/src/flush.rs
git commit -m "fix: latest manifest selected by (watermark, block_id)"
```

---

### Task 5: Public strict frame decoder + `log_record` fuzz target

The log frame grammar (`len u32 LE · crc32c u32 LE · protobuf payload`) is decoded privately in
two places. Extract ONE public strict decoder in `varve-log::record`, delegate the object-store
path to it, and fuzz it: arbitrary bytes must never panic — only `Ok(records)` or a clean
`LogError`.

**Files:**
- Modify: `crates/varve-log/src/record.rs` (new `decode_frames` + unit tests + seed writer)
- Modify: `crates/varve-log/src/object_store.rs` (`decode_object` delegates)
- Modify: `fuzz/Cargo.toml` (dep + `[[bin]]`)
- Create: `fuzz/fuzz_targets/log_record.rs`
- Create: `fuzz/corpus/log_record/valid-two-frames.bin` (committed seed)

**Interfaces:**
- Consumes: `LogRecord::{to_wire, from_wire, wire_len}`, `crc32c::crc32c`, `LogError`.
- Produces: `pub fn decode_frames(context: &str, bytes: &[u8]) -> Result<Vec<LogRecord>, LogError>`
  in `varve_log::record` — STRICT whole-buffer decode: zero-length input → `Ok(vec![])`; any
  truncated header, truncated payload, CRC mismatch, protobuf decode failure, or trailing bytes
  that don't form a complete valid frame → the same `Corrupt`-class `LogError` the object-store
  decoder returns today, with `context` in the message. Re-export as `varve_log::decode_frames`
  from `lib.rs`. Task 6's CI step and the fuzz target consume it.

- [ ] **Step 1: Write the failing unit tests** (in `record.rs` `mod tests`; frame-building
  helper mirrors the object-store log's encode side):

```rust
fn frame(payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(&crc32c::crc32c(payload).to_le_bytes());
    out.extend_from_slice(payload);
    out
}

fn sample_record(tx_id: u64) -> LogRecord {
    LogRecord {
        tx_id,
        system_time_us: 1_700_000_000_000_000 + tx_id as i64,
        user: String::new(),
        effects: vec![],
    }
}

#[test]
fn decode_frames_round_trips_a_multi_frame_stream() {
    let records = vec![sample_record(1), sample_record(2)];
    let mut bytes = Vec::new();
    for record in &records {
        bytes.extend_from_slice(&frame(&record.to_wire()));
    }
    assert_eq!(decode_frames("test", &bytes).unwrap(), records);
    assert_eq!(decode_frames("test", &[]).unwrap(), Vec::<LogRecord>::new());
}

#[test]
fn decode_frames_rejects_truncation_crc_and_trailing_garbage() {
    let good = frame(&sample_record(1).to_wire());
    // Truncated header, truncated payload, flipped CRC byte, trailing garbage.
    assert!(decode_frames("t", &good[..3]).is_err());
    assert!(decode_frames("t", &good[..good.len() - 1]).is_err());
    let mut bad_crc = good.clone();
    bad_crc[4] ^= 0xFF;
    assert!(decode_frames("t", &bad_crc).is_err());
    let mut trailing = good.clone();
    trailing.push(0x00);
    assert!(decode_frames("t", &trailing).is_err());
}

#[test]
fn decode_frames_rejects_an_absurd_length_prefix_without_allocating() {
    // len = u32::MAX with a tiny buffer must be a clean error, not an OOM/panic.
    let mut bytes = u32::MAX.to_le_bytes().to_vec();
    bytes.extend_from_slice(&[0u8; 8]);
    assert!(decode_frames("t", &bytes).is_err());
}
```

  (Adapt `sample_record`/`LogRecord` field spelling to `record.rs` — `effects` is
  `Vec<TableEffects>`; the existing golden-wire tests in the file show the exact construction.
  If `LogError`'s corrupt variant needs a specific payload, mirror `object_store.rs::decode_frame`'s
  construction — that function is about to delegate here.)

- [ ] **Step 2: Run** `cargo test -p varve-log decode_frames` → FAIL (function does not exist).

- [ ] **Step 3: Implement `decode_frames`** by MOVING the loop logic of
  `object_store.rs::{decode_object, decode_frame}` into `record.rs` (length-bounds check BEFORE
  slicing — the absurd-length test — then CRC, then `LogRecord::from_wire`), and reduce
  `decode_object(key, bytes)` to `decode_frames(key, bytes)`. Keep `decode_frame` deleted or as a
  private helper inside `record.rs` — one grammar, one decoder. `local.rs::scan_segment` is NOT
  changed (its torn-TAIL tolerance is a different, lenient contract; note this in `decode_frames`
  rustdoc: "strict — for atomic whole objects; the local segment scanner tolerates a torn tail").

- [ ] **Step 4: Run** `cargo test -p varve-log` (default features include `object-store`) →
  ALL PASS including the existing object-store corruption tests, which now exercise the shared
  decoder.

- [ ] **Step 5: Add the fuzz target and seed.** `fuzz/Cargo.toml` additions:

```toml
[dependencies]
varve-log = { path = "../crates/varve-log", default-features = false }

[[bin]]
name = "log_record"
path = "fuzz_targets/log_record.rs"
test = false
doc = false
bench = false
```

  `fuzz/fuzz_targets/log_record.rs`:

```rust
#![no_main]

use libfuzzer_sys::fuzz_target;
use varve_log::decode_frames;

fuzz_target!(|data: &[u8]| {
    if let Ok(records) = decode_frames("fuzz", data) {
        // Anything the strict decoder accepts must round-trip its envelope.
        for record in records {
            let reparsed = varve_log::LogRecord::from_wire(&record.to_wire())
                .expect("decoded record must re-encode/decode");
            assert_eq!(reparsed, record);
        }
    }
});
```

  Seed: add an `#[ignore]`d regeneration test in `record.rs` tests (run once now, keep for later
  regeneration; commit the binary file):

```rust
#[test]
#[ignore = "regenerates the committed fuzz seed corpus"]
fn write_log_record_fuzz_seed() {
    let mut bytes = Vec::new();
    for record in [sample_record(1), sample_record(2)] {
        bytes.extend_from_slice(&frame(&record.to_wire()));
    }
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../fuzz/corpus/log_record");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("valid-two-frames.bin"), bytes).unwrap();
}
```

  Run: `cargo test -p varve-log write_log_record_fuzz_seed -- --ignored` then `git add` the seed.

- [ ] **Step 6: Smoke-fuzz locally**

```bash
cargo +nightly fuzz run log_record -- -max_total_time=60 -rss_limit_mb=4096
```

  Expected: no crashes. ANY crash = a real bug: minimize (`cargo fuzz tmin`), add the crash bytes
  as a `decode_frames_regression_*` unit test in `record.rs`, fix, re-run.

- [ ] **Step 7: Gate + commit**

```bash
cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings && cargo test -p varve-log
git add crates/varve-log fuzz/Cargo.toml fuzz/fuzz_targets/log_record.rs fuzz/corpus/log_record
git commit -m "feat: shared strict log frame decoder with fuzz target"
```

---

### Task 6: Fuzz targets for manifest, block meta, and event decoders + nightly CI budget

Three more untrusted-input boundaries: `BlockManifest::from_wire` (+ the trie-key parsing that
consumes it), `varve_index::decode_meta`, `varve_index::decode_events` (Arrow IPC readers).
Arbitrary/truncated bytes must produce clean errors, never panics. Extend `fuzz-nightly` to run
all five targets.

**Files:**
- Modify: `fuzz/Cargo.toml` (deps `varve-storage` — with `default-features = false` so the fuzz
  build never pulls the s3/reqwest stack — and `varve-index`; three `[[bin]]`s)
- Create: `fuzz/fuzz_targets/manifest.rs`, `fuzz/fuzz_targets/block_meta.rs`,
  `fuzz/fuzz_targets/events.rs`
- Create: committed seeds `fuzz/corpus/{manifest,block_meta,events}/valid.bin`
- Modify: `crates/varve-storage/src/manifest.rs`, `crates/varve-index/src/block.rs`,
  `crates/varve-index/src/codec.rs` (ONLY if fuzzing finds panics: harden the decode entry with a
  regression test per finding)
- Modify: `.github/workflows/ci.yml` (`fuzz-nightly` target matrix), `justfile` (`fuzz` recipe)

**Interfaces:**
- Consumes: `BlockManifest::{from_wire, to_wire, trie_entries}`,
  `ManifestTrieEntry::scoped_trie_key`, `ScopedTrieKey::parse_trie_key`,
  `varve_storage::TrieCatalog::from_manifests`, `varve_index::{decode_meta, decode_events}`.
- Produces: no API changes (hardening stays behind the existing signatures).

- [ ] **Step 1: Add the three fuzz targets.** `fuzz/Cargo.toml`:

```toml
varve-storage = { path = "../crates/varve-storage", default-features = false }
varve-index = { path = "../crates/varve-index" }

[[bin]]
name = "manifest"
path = "fuzz_targets/manifest.rs"
test = false
doc = false
bench = false

[[bin]]
name = "block_meta"
path = "fuzz_targets/block_meta.rs"
test = false
doc = false
bench = false

[[bin]]
name = "events"
path = "fuzz_targets/events.rs"
test = false
doc = false
bench = false
```

  `fuzz/fuzz_targets/manifest.rs` — decode, then exercise EVERYTHING recovery would do with a
  decoded manifest (trie-key parsing and catalog folding are the panic-prone consumers):

```rust
#![no_main]

use libfuzzer_sys::fuzz_target;
use varve_storage::{BlockManifest, TrieCatalog};

fuzz_target!(|data: &[u8]| {
    let Ok(manifest) = BlockManifest::from_wire(data) else {
        return;
    };
    // prost decode is not byte-canonical; semantic round-trip must hold.
    let reparsed = BlockManifest::from_wire(&manifest.to_wire()).expect("re-decode");
    assert_eq!(reparsed, manifest);
    for entry in manifest.trie_entries() {
        let _ = entry.scoped_trie_key().parse_trie_key(); // Result, never panic
    }
    let _ = TrieCatalog::from_manifests(std::slice::from_ref(&manifest));
});
```

  `fuzz/fuzz_targets/block_meta.rs` and `events.rs`:

```rust
#![no_main]
use libfuzzer_sys::fuzz_target;
fuzz_target!(|data: &[u8]| {
    let _ = varve_index::decode_meta(data);
});
```

```rust
#![no_main]
use libfuzzer_sys::fuzz_target;
fuzz_target!(|data: &[u8]| {
    let _ = varve_index::decode_events(data);
});
```

  (Adjust the `use` paths to the crates' actual re-exports — `varve_storage::TrieCatalog` is
  re-exported per `catalog.rs`; if `TrieCatalog::from_manifests` takes `&[BlockManifest]`, the
  `from_ref` call matches.)

- [ ] **Step 2: Verify all targets build:** `cargo +nightly fuzz build` → compiles all five.

- [ ] **Step 3: Generate seeds** with `#[ignore]`d writer tests, same pattern as Task 5:
  in `manifest.rs` tests, write `sample().to_wire()` to `fuzz/corpus/manifest/valid.bin`; in
  `varve-index` tests, write one `encode_meta`/`encode_block`-produced meta buffer and one
  `encode_events(&[...])` buffer (reuse the existing round-trip test fixtures) to
  `fuzz/corpus/block_meta/valid.bin` and `fuzz/corpus/events/valid.bin`. Run each with
  `-- --ignored`, commit the seeds.

- [ ] **Step 4: Smoke-fuzz each target 120 s:**

```bash
for t in manifest block_meta events; do
  cargo +nightly fuzz run $t -- -max_total_time=120 -rss_limit_mb=4096
done
```

  Expected findings policy: `decode_meta`/`decode_events` wrap arrow-rs IPC readers, which MAY
  panic on adversarial buffers. Every crash: `cargo fuzz tmin <target> <artifact>`, add the
  minimized bytes as a `#[test]` regression (crash bytes inline as a byte-string literal) in the
  owning crate asserting `is_err()`, then harden OUR decode entry (validate lengths/offsets, or
  `arrow`'s `StreamReader` options) until the input errors cleanly. Do NOT catch_unwind — fix the
  boundary. If a panic is deep inside arrow-rs and not preventable by input validation, record it
  in STATUS.md as a known upstream issue with the artifact committed under
  `fuzz/regressions/` and a `#[should_panic]`-free skip note — but exhaust validation options
  first.

- [ ] **Step 5: Extend CI + justfile.** `fuzz-nightly` job becomes a matrix over all five
  targets (parse keeps its 600 s budget; decoders get 300 s each):

```yaml
  fuzz-nightly:
    if: github.event_name == 'schedule' && github.event.schedule == '0 3 * * *'
    runs-on: ubuntu-latest
    strategy:
      fail-fast: false
      matrix:
        include:
          - target: parse
            secs: 600
          - target: log_record
            secs: 300
          - target: manifest
            secs: 300
          - target: block_meta
            secs: 300
          - target: events
            secs: 300
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@nightly
      - uses: Swatinem/rust-cache@v2
      - run: cargo install cargo-fuzz --locked
      - run: cargo +nightly fuzz run ${{ matrix.target }} -- -max_total_time=${{ matrix.secs }} -rss_limit_mb=4096
      - uses: actions/upload-artifact@v4
        if: failure()
        with:
          name: fuzz-artifacts-${{ matrix.target }}
          path: fuzz/artifacts
```

  Validate the edited workflow parses: `python3 -c "import yaml,sys; yaml.safe_load(open('.github/workflows/ci.yml'))"`.
  justfile:

```make
fuzz target="parse" secs="60":
    cargo +nightly fuzz run {{target}} -- -max_total_time={{secs}} -rss_limit_mb=4096
```

- [ ] **Step 6: Gate + commit**

```bash
cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings && cargo test -p varve-storage -p varve-index
git add fuzz .github/workflows/ci.yml justfile crates/varve-storage crates/varve-index
git commit -m "feat: fuzz manifest, block meta, and event decoders nightly"
```

---

### Task 7: Criterion micro-benchmarks — resolution, trie ops, parse

Spec §13.7: criterion micro-benches. Three bench targets over the pure hot paths, runnable
locally, compiled (not run) by CI via `clippy --all-targets`.

**Files:**
- Modify: `Cargo.toml` (workspace) — add `criterion = "0.7"` to `[workspace.dependencies]`
  (**verify live**: `cargo add criterion@0.7 --dev -p varve-index --dry-run`; if 0.7 does not
  exist on the registry, pin the latest 0.x — the `criterion_group!`/`criterion_main!`/
  `bench_function` API used below is stable across 0.5–0.7)
- Modify: `crates/varve-index/Cargo.toml`, `crates/varve-types/Cargo.toml`,
  `crates/varve-gql/Cargo.toml` — `[dev-dependencies] criterion = { workspace = true }` +
  `[[bench]]` sections
- Create: `crates/varve-index/benches/resolution.rs`, `crates/varve-types/benches/trie.rs`,
  `crates/varve-gql/benches/parse.rs`
- Modify: `justfile` (`bench-micro` recipe)

**Interfaces:**
- Consumes: `varve_index::{resolve, Event, Op}`, `varve_types::{Iid, Instant, TemporalBounds,
  trie::Bucketer}`, `varve_gql::parse_program`. Event/bounds CONSTRUCTION MUST MIRROR the unit
  tests in `varve-index/src/bitemporal.rs` (`mod tests` builds events via small helpers like
  `erase(sf)` — copy that idiom; the exact `Event`/`TemporalBounds` field spelling there is the
  contract).
- Produces: `cargo bench -p varve-index -p varve-types -p varve-gql` runs; Task 10's report
  quotes the medians.

Each crate's `Cargo.toml` gains (name per bench):

```toml
[dev-dependencies]
criterion = { workspace = true }

[[bench]]
name = "resolution"
harness = false
```

- [ ] **Step 1: Write `crates/varve-index/benches/resolution.rs`**

```rust
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use varve_index::resolve;
// Event construction: mirror varve-index/src/bitemporal.rs tests exactly
// (Put with a small doc payload; system_from strictly increasing per entity,
// events ordered (iid, system_from desc) as `resolve` requires).

fn bench_resolve(c: &mut Criterion) {
    let mut group = c.benchmark_group("resolve");
    for n in [16usize, 256, 4096] {
        let events = alternating_put_delete_history(n); // helper in this file
        let current = current_time_bounds(); // valid AT now, system AT latest
        let as_of = as_of_middle_bounds(&events); // system AS OF the median system_from
        group.bench_with_input(BenchmarkId::new("current", n), &events, |b, ev| {
            b.iter(|| resolve(ev, &current))
        });
        group.bench_with_input(BenchmarkId::new("as_of_past", n), &events, |b, ev| {
            b.iter(|| resolve(ev, &as_of))
        });
    }
    group.finish();
}

criterion_group!(benches, bench_resolve);
criterion_main!(benches);
```

- [ ] **Step 2: Write `crates/varve-types/benches/trie.rs`**

```rust
use criterion::{criterion_group, criterion_main, Criterion};
use varve_types::trie::Bucketer;
use varve_types::Iid;

fn bench_trie(c: &mut Criterion) {
    let iids: Vec<Iid> = (0..1024u64)
        .map(|i| Iid::derive("default", "nodes", &i.to_le_bytes()))
        .collect();
    let path = Bucketer::path(&iids[0], 4).unwrap();

    c.bench_function("bucketer/bucket_level3", |b| {
        b.iter(|| iids.iter().map(|iid| Bucketer::bucket(iid, 3)).count())
    });
    c.bench_function("bucketer/path_4_levels", |b| {
        b.iter(|| iids.iter().map(|iid| Bucketer::path(iid, 4)).count())
    });
    c.bench_function("bucketer/contains", |b| {
        b.iter(|| iids.iter().filter(|iid| Bucketer::contains(&path, iid)).count())
    });
    c.bench_function("iid/derive", |b| {
        b.iter(|| Iid::derive("default", "nodes", b"benchmark-id"))
    });
}

criterion_group!(benches, bench_trie);
criterion_main!(benches);
```

  (`Iid::derive`'s exact signature: mirror `cas_failover_backends.rs:41` —
  `Iid::derive("default", "nodes", &Value::Int(id).id_bytes().unwrap())`; adapt the byte-arg
  spelling accordingly.)

- [ ] **Step 3: Write `crates/varve-gql/benches/parse.rs`**

```rust
use criterion::{criterion_group, criterion_main, Criterion};
use varve_gql::parse_program;

const POINT: &str = "MATCH (p:Person {_id: 42}) RETURN p.name";
const TRAVERSAL: &str = "FOR SYSTEM_TIME AS OF TIMESTAMP '2020-01-01T00:00:00Z' \
    MATCH (a:Person {_id: 0})-[:KNOWS]->{1,3}(b:Person) \
    WHERE b.age > 21 AND b.name <> 'x' \
    RETURN DISTINCT b.name AS name ORDER BY name LIMIT 100";
const PROGRAM: &str = "INSERT (:Person {_id: 1, name: 'Ada'}); \
    MATCH (p:Person {_id: 1}) SET p.name = 'Lovelace'; \
    MATCH (p:Person {_id: 1}) RETURN p.name";

fn bench_parse(c: &mut Criterion) {
    for (name, src) in [("point", POINT), ("traversal", TRAVERSAL), ("program", PROGRAM)] {
        c.bench_function(&format!("parse/{name}"), |b| {
            b.iter(|| parse_program(src).unwrap())
        });
    }
}

criterion_group!(benches, bench_parse);
criterion_main!(benches);
```

  (Every source string MUST parse — verify with a plain `cargo test -p varve-gql` doc-free run of
  `parse_program` first; if a construct differs from the shipped grammar, take a passing statement
  from `resources/gql-corpus/` instead. The `.unwrap()` in a bench binary is allowed —
  `clippy.toml` allows unwrap in tests, and benches are non-library targets; if clippy still
  flags it, add `#![allow(clippy::unwrap_used)]` at the top of the bench file.)

- [ ] **Step 4: Run** `cargo bench -p varve-types -p varve-gql -p varve-index -- --quick` →
  all benches execute and report. Then `cargo clippy --workspace --all-targets -- -D warnings`
  (benches are now compiled by the standard gate).

- [ ] **Step 5: justfile recipe + commit**

```make
bench-micro:
    cargo bench -p varve-index -p varve-types -p varve-gql
```

```bash
cargo fmt --all && cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace
git add Cargo.toml Cargo.lock crates/varve-index crates/varve-types crates/varve-gql justfile
git commit -m "feat: criterion micro benches for resolution, trie ops, and parse"
```

---

### Task 8: End-to-end social workload bench — `examples/social_bench.rs`

One example that produces the report's core table: batched ingest rate (events/s — the spec §13
"write ops/s" unit), warm point read, warm 2-hop, and the AS-OF-historical/current-time ratio,
over the deterministic 10k-node/60k-edge social fixture on the durable local profile.

**Files:**
- Create: `crates/varve/examples/social_bench.rs`
- Modify: `crates/varve/Cargo.toml` ONLY if the example needs a dev-dep not already present
  (`varve-testkit` already is one)

**Interfaces:**
- Consumes: `varve_testkit::fixture::{social_graph, EDGE_PROGRAM_BATCH}`,
  `Db::{open, execute, query}`, `TxReceipt.system_time`, config idiom from
  `examples/traversal_bench.rs` (same crate — copy its `config(dir)` builder, flush trigger, and
  warm-timing helpers `warm_timings`/`p50`/`avg` VERBATIM where they fit).
- Produces: stdout markdown table consumed by Task 10's report:

```
| metric | value |
|---|---|
| ingest (batched) | NNN events/s (M txs, T.T s) |
| warm point read (p50/avg of 100) | X.XX ms / Y.YY ms |
| warm 2-hop (p50/avg of 100) | X.XX ms / Y.YY ms |
| AS-OF historical 2-hop (p50) | X.XX ms (N.NNx of current) |
```

- [ ] **Step 1: Write the example.** Structure (mirroring `traversal_bench.rs`):
  1. Temp dir; open with the traversal-bench config (durable local log + store, flush via
     `max_block_rows`).
  2. Ingest `social_graph(10_000, 60_000, 42)` via `node_statements(...)` +
     `edge_programs(EDGE_PROGRAM_BATCH)`; time the whole ingest; compute events/s =
     (nodes + edges) / elapsed and print both events/s and tx/s. Record `mid_receipt` =
     the receipt of the LAST node statement (its `system_time` is the AS-OF anchor — at that
     instant zero edges exist).
  3. Reopen the Db (manifest + tail recovery, same as traversal_bench) so reads hit the
     persisted path.
  4. Point read: `MATCH (p:Person {_id: 4242}) RETURN p.name` — 1 cold + 100 warm, p50/avg.
  5. 2-hop: the exact `two_hop` query from `traversal_bench.rs` — 1 cold + 100 warm.
  6. AS-OF: the same 2-hop wrapped in `FOR SYSTEM_TIME AS OF TIMESTAMP '<mid_receipt
     system_time>'` — 100 warm; print the p50 and the ratio to step 5's p50 (spec target: ≤ 2×;
     note the AS-OF query returns 0 rows at that anchor — also time a LATE anchor taken after
     half the edge programs for a non-empty historical read, and report that as the headline
     AS-OF number).
  7. Print the markdown table; exit nonzero if any query errors (no perf asserts — targets are
     tracked, not gated).

- [ ] **Step 2: Run** `cargo run --release --example social_bench -p varve` → table prints,
  numbers plausible vs. STATUS.md slice-4/6 records (39.8k events/s ingest, 7.6 ms warm point,
  16.2 ms warm 2-hop on this machine).

- [ ] **Step 3: Gate + commit**

```bash
cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings && cargo test -p varve
git add crates/varve/examples/social_bench.rs
git commit -m "feat: end-to-end social workload bench example"
```

---

### Task 9: Query-node read scale-out bench — 1 → 2 → 4 nodes

Spec §13 target: "query throughput scales ~linearly to 4 nodes on the read benchmark".
Generalize the slice-9 process harness to N query nodes and add an env-gated release-mode bench
that measures aggregate read QPS against 1, 2, and 4 query nodes of ONE cluster (single ingest).

**Files:**
- Modify: `crates/varve-server/tests/support/process_cluster.rs`
- Create: `crates/varve-server/tests/scale_out_bench.rs`
- Modify: `justfile` (`bench-scale-out` recipe)

**Interfaces:**
- Consumes: everything `process_cluster.rs` already provides.
- Produces: `pub async fn start_with_query_nodes(query_nodes: usize) -> Result<ProcessCluster>`;
  `start()` becomes `Self::start_with_query_nodes(2).await` (existing tests unchanged).
  `query_urls()` keeps returning all query-node URLs in creation order.

- [ ] **Step 1: Generalize the harness.** In `process_cluster.rs`, replace the hardcoded
  `for index in 1..=2` loop with `for index in 1..=query_nodes`, behind:

```rust
pub async fn start() -> Result<ProcessCluster> {
    ProcessCluster::start_with_query_nodes(2).await
}

pub async fn start_with_query_nodes(query_nodes: usize) -> Result<ProcessCluster> {
    // existing body, loop bound = query_nodes
}
```

  Run the existing suites to prove no regression:
  `cargo test -p varve-server --test process_consistency --test process_scale_out -- --test-threads=1` → 4 passed.

- [ ] **Step 2: Write the env-gated bench test** (`tests/scale_out_bench.rs`; skips by default
  so `just check` stays fast):

```rust
//! Read scale-out benchmark (slice 11): aggregate QPS against 1, 2, and 4
//! query nodes of one cluster. Env-gated: set VARVE_SCALE_BENCH=1 and run
//! --release (see `just bench-scale-out`). Prints a markdown table for
//! docs/benchmarks/v1.md. Asserts CORRECTNESS (all nodes agree at the final
//! basis), never throughput (targets are tracked, not gated).
#![cfg(feature = "http")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::{Duration, Instant};
use varve_testkit::fixture::social_graph;

#[path = "support/process_cluster.rs"]
mod process_cluster;
use process_cluster::ProcessCluster;

const TRAVERSAL: &str = "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN b.name AS name";
const MEASURE_WINDOW: Duration = Duration::from_secs(10);

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn read_qps_scales_from_one_to_four_query_nodes() {
    if std::env::var("VARVE_SCALE_BENCH").is_err() {
        eprintln!("skipping: set VARVE_SCALE_BENCH=1 (see `just bench-scale-out`)");
        return;
    }
    let cluster = Arc::new(ProcessCluster::start_with_query_nodes(4).await.unwrap());

    // Ingest once; capture the final basis.
    let graph = social_graph(1_000, 5_000, 42);
    let mut final_basis = 0u64;
    for statement in graph.node_statements(50) {
        final_basis = cluster.tx(cluster.writer_url(), &statement).await.unwrap().basis;
    }
    for program in graph.edge_programs(100) {
        final_basis = cluster.tx(cluster.writer_url(), &program).await.unwrap().basis;
    }

    // Correctness floor: every node agrees at the final basis.
    let urls: Vec<String> = cluster.query_urls().into_iter().map(str::to_owned).collect();
    let expected = cluster
        .query_row_count(cluster.writer_url(), TRAVERSAL, Some(final_basis))
        .await
        .unwrap();
    assert!(expected > 0);
    for url in &urls {
        let got = cluster.query_row_count(url, TRAVERSAL, Some(final_basis)).await.unwrap();
        assert_eq!(got, expected, "{url} diverged");
    }

    println!("| query nodes | aggregate reads | window | QPS |");
    println!("|---|---|---|---|");
    for n in [1usize, 2, 4] {
        let mut readers = Vec::new();
        let deadline = Instant::now() + MEASURE_WINDOW;
        for url in urls.iter().take(n).cloned() {
            let cluster = Arc::clone(&cluster);
            readers.push(tokio::spawn(async move {
                let mut ok = 0usize;
                while Instant::now() < deadline {
                    if let Ok(count) = cluster.query_row_count(&url, TRAVERSAL, None).await {
                        assert_eq!(count, expected);
                        ok += 1;
                    }
                }
                ok
            }));
        }
        let mut total = 0usize;
        for reader in readers {
            total += reader.await.unwrap();
        }
        let qps = total as f64 / MEASURE_WINDOW.as_secs_f64();
        println!("| {n} | {total} | {:?} | {qps:.0} |", MEASURE_WINDOW);
    }
}
```

  Design notes baked into the test: ONE reader task per node (client concurrency scales with
  node count — the scale-out claim), reads WITHOUT basis (steady-state reads; ingest is finished
  so `assert_eq!(count, expected)` still must hold — followers have converged, enforced by the
  basis-pinned pre-check).

- [ ] **Step 3: Run it**

```bash
VARVE_SCALE_BENCH=1 cargo test -p varve-server --release --test scale_out_bench -- --nocapture --test-threads=1
```

  Expected: table with QPS growing with n (record actual numbers for Task 10; no assert on the
  slope). Also `cargo test -p varve-server --test scale_out_bench` (no env) → skips in <1 s.

- [ ] **Step 4: justfile + commit**

```make
bench-scale-out:
    VARVE_SCALE_BENCH=1 cargo test -p varve-server --release --test scale_out_bench -- --nocapture --test-threads=1
```

```bash
cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings && cargo test -p varve-server -- --test-threads=1
git add crates/varve-server/tests justfile
git commit -m "feat: env-gated read scale-out bench over 1/2/4 query nodes"
```

---

### Task 10: Benchmark report — `docs/benchmarks/v1.md`

Run every bench on this machine and publish the report the roadmap and spec §13 demand: measured
numbers side-by-side with the spec targets, honest gaps called out.

**Files:**
- Create: `docs/benchmarks/v1.md`

**Interfaces:**
- Consumes: Task 7 (`just bench-micro`), Task 8 (`social_bench`), Task 9 (`just
  bench-scale-out`), existing examples `write_bench`, `block_bench`, `cache_bench`,
  `traversal_bench`, and (optional S3 numbers) `just s3-matrix` / MinIO via docker.
- Produces: the report Task 12's book links and Task 17's acceptance pass cites.

- [ ] **Step 1: Run the suite and capture outputs** (release mode, quiet machine):

```bash
just bench-micro
cargo run --release --example write_bench -p varve
cargo run --release --example block_bench -p varve
cargo run --release --example cache_bench -p varve
cargo run --release --example traversal_bench -p varve
cargo run --release --example social_bench -p varve
just bench-scale-out
```

- [ ] **Step 2: Write the report** with this exact structure (fill measured values):

```markdown
# Varve v1 benchmark report

Machine: Apple M3 Max, 16-core, macOS <ver> (Darwin 25.3.0), Rust 1.93, release builds.
Date: 2026-07-XX. Commit: <sha>. Reproduce: commands listed per section.

## Spec §13 targets vs measured

| Target (spec §13, laptop) | Measured | Verdict | Source |
|---|---|---|---|
| ≥10k write ops/s sustained (batched) | N events/s | ... | social_bench |
| warm point lookup < 1 ms | N ms | ... | block_bench / social_bench |
| warm 2-hop over 1M-node graph < 50 ms | N ms (10k-node fixture; 1M-node note below) | ... | traversal_bench |
| AS-OF historical within 2× of current | N.Nx | ... | social_bench |
| server ≥5k tx/s on object-store log | N tx/s (group-commit note below) | ... | cache_bench (MinIO) / write_bench |
| read throughput ~linear to 4 nodes | 1→N: QPS table | ... | scale_out_bench |

## Micro-benchmarks (criterion medians) …
## Write path …
## Read path …
## Scale-out …
## Honest notes
- Fixture is 10k nodes/60k edges, not 1M nodes: <extrapolation or explicit non-claim>.
- tx/s vs events/s: one tx = one GQL statement batch; the ≥10k target is met via batching
  (multi-row INSERT), single-statement fsync-bound tx/s is N (write_bench local).
- <any target missed: state it plainly + why + what would close it>.
```

  Every number in the report MUST come from a command run in Step 1 (no reuse of stale STATUS.md
  numbers — re-measure). Where a spec target cannot be honestly claimed (e.g. 1M-node graph not
  ingested), the report says so explicitly rather than extrapolating silently.

- [ ] **Step 3: Commit**

```bash
git add docs/benchmarks/v1.md
git commit -m "docs: v1 benchmark report vs spec section 13 targets"
```

---

### Task 11: mdBook scaffold — getting started, architecture, CI docs job

Stand up `docs/book/` with the full chapter skeleton (so `SUMMARY.md` is final from day one),
write the two chapters that need no further inputs (getting started, architecture), and make CI
build the book.

**Files:**
- Create: `docs/book/book.toml`, `docs/book/src/SUMMARY.md`, `docs/book/src/introduction.md`,
  `docs/book/src/getting-started.md`, `docs/book/src/architecture.md`, plus STUB files (one
  heading + "TODO(slice-11 Task N)" line) for every remaining SUMMARY entry — stubs are
  eliminated by Tasks 12–13; Task 17 greps them gone
- Modify: `.github/workflows/ci.yml` (new `docs` job), `justfile` (`docs`, `docs-serve`),
  `.gitignore` (add `docs/book/book/` build output)

**Interfaces:**
- Consumes: mdBook (install locally: `cargo install mdbook --locked`; record the resolved
  version in the commit message and PIN it in CI with `--version <resolved>`).
- Produces: `mdbook build docs/book` green; chapter files Tasks 12–13 fill in.

- [ ] **Step 1: Install mdbook + scaffold.** `book.toml`:

```toml
[book]
title = "VarveDB"
description = "Bitemporal property-graph database speaking GQL, embedded-first, over any S3-API object store"
authors = ["Varve contributors"]
language = "en"
src = "src"

[build]
create-missing = false
```

  `src/SUMMARY.md` (final chapter tree):

```markdown
# Summary

[Introduction](introduction.md)

- [Getting started (laptop, 5 minutes)](getting-started.md)
- [Architecture overview](architecture.md)
- [GQL reference](gql/reference.md)
  - [Temporal extensions](gql/temporal.md)
  - [Deviations & conformance](gql/deviations.md)
- [Backends & capability matrix](backends.md)
- [Operations guide](ops/README.md)
  - [Deployment profiles & sizing](ops/profiles.md)
  - [Configuration reference](ops/configuration.md)
  - [Failover](ops/failover.md)
  - [Metrics & observability](ops/metrics.md)
- [HTTP API](reference/http-api.md)
- [CLI](reference/cli.md)
```

  `create-missing = false` forces every listed file to exist — create the stubs now.

- [ ] **Step 2: Write `introduction.md`** (one page: what Varve is, sovereignty stance, the
  varve metaphor, links to spec/roadmap in-repo) **and `getting-started.md`** with BOTH paths,
  each verified by actually running it:
  - **From source (works today):** `git clone … && cargo run --release -p varve-cli -- shell
    --dir ./mydb`, then a copy-pasteable 6-statement GQL session: INSERT two people + edge, MATCH,
    retro-dated `INSERT … VALID FROM`, `FOR SYSTEM_TIME AS OF` time travel, `ERASE`. State
    expected output for each (run the session in `varve shell` and paste real output).
  - **From release artifacts (activates at v1.0.0):** `cargo install varve-cli`, `docker run`,
    tarball — written now, marked "available from v1.0.0", exact commands matching Task 15/16
    artifacts.
  - **Server + CLI in 3 commands:** minimal `varve.toml` (writer node, local dirs, static token),
    `varved --config varve.toml`, `varve shell --url http://127.0.0.1:8080 --token …` — run it
    once to verify the TOML is exact.

- [ ] **Step 3: Write `architecture.md`:** condensed spec §3/§5/§9/§12 — roles diagram
  (writer/query/compactor over one log + object store), the event model and derived `_system_to`,
  blocks/tries/manifest-as-commit-point, group commit, epoch fencing, determinism. Target ~2
  pages; link the full design spec for depth. No stale numbers.

- [ ] **Step 4: CI job + justfile.** In `ci.yml`:

```yaml
  docs:
    if: github.event_name != 'schedule'
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - uses: Swatinem/rust-cache@v2
      - run: cargo install mdbook --locked --version <pin resolved version>
      - run: mdbook build docs/book
```

  justfile:

```make
docs:
    mdbook build docs/book

docs-serve:
    mdbook serve docs/book --open
```

  Validate: `python3 -c "import yaml; yaml.safe_load(open('.github/workflows/ci.yml'))"` and
  `just docs` → build succeeds with stubs.

- [ ] **Step 5: Commit**

```bash
git add docs/book .github/workflows/ci.yml justfile .gitignore
git commit -m "docs: mdBook scaffold with getting started and architecture"
```

---

### Task 12: Book content — GQL reference, backend matrix, ops guide

Fill every Task-11 stub except `ops/configuration.md` (Task 13). Content sources are in-repo
truth: the parser/TCK for GQL, `backends.rs` pins + probe verdicts for the matrix, STATUS.md
slice-9/10 contracts for ops.

**Files:**
- Modify (from stub): `docs/book/src/gql/reference.md`, `gql/temporal.md`, `gql/deviations.md`,
  `backends.md`, `ops/README.md`, `ops/profiles.md`, `ops/failover.md`, `ops/metrics.md`,
  `reference/http-api.md`, `reference/cli.md`
- Modify: `docs/ops/metrics.md` → replace body with a one-line pointer to the book page (single
  source of truth moves; STATUS.md references stay valid via the pointer)

**Interfaces:**
- Consumes: `crates/varve-gql/src/parser.rs` (supported grammar), TCK gate data
  (`crates/varve-testkit/tests/tck.rs` + exclusion reasons), `crates/varve-testkit/src/backends.rs`
  (image pins), STATUS.md slice-5/9/10 decision blocks (probe verdicts, route/auth matrix, CLI
  contract, failover semantics), `docs/ops/metrics.md` (moves), Task 1 (GC retention guidance),
  Task 4 (failover hardening wording).
- Produces: complete book minus configuration page.

- [ ] **Step 1: `gql/reference.md`** — the practical core per spec §8 as shipped: statement list
  (INSERT / MATCH / OPTIONAL MATCH / WHERE / FILTER / LET / FOR / ORDER BY / SKIP / LIMIT /
  OFFSET / UNION [ALL] / RETURN [DISTINCT] / aggregation / SET / REMOVE / DELETE [DETACH] /
  ERASE [DETACH] / CREATE·DROP GRAPH / USE / parameters / CASE / EXISTS / CAST), each with one
  runnable example. Source of truth = parser tests + `examples/gql_tour.rs`; every example must
  parse (spot-check any uncertain one against `parse_program` in a scratch test, then delete the
  scratch).

- [ ] **Step 2: `gql/temporal.md`** — `FOR VALID_TIME/SYSTEM_TIME AS OF | FROM/TO | BETWEEN |
  ALL` (whichever subset shipped — the slice-2/7 tests are the truth), `INSERT … VALID FROM/TO`,
  defaults (valid AS OF now, system AS OF latest), `valid_from()/valid_to()/system_from()`
  functions, ERASE vs DELETE semantics (delete = bitemporal tombstone, erase = history gone at
  every axis + physical bytes after compaction+GC — link the Task 2/3 proofs by test name).

- [ ] **Step 3: `gql/deviations.md`** — honest conformance page: v1 deviations recorded in
  STATUS decisions (edge label REQUIRED; one linear path per MATCH; quantified-edge-with-var
  unsupported; catalog+data statements cannot mix in one program; reserved-word list; multi-label
  MATCH limits; `max_path_depth` cap) + TCK standing: adapted openCypher TCK, 445/511 adapted
  scenarios passing (87.1%), all exclusions reasoned in-repo (link `varve-testkit` data), plus
  the ANTLR differential oracle. Copy the CURRENT numbers from the tck gate output, not from
  this plan.

- [ ] **Step 4: `backends.md`** — matrix table:

| Backend | Version tested (CI pin) | Probe verdict | `cas-failover` | CI cadence |
|---|---|---|---|---|
| Garage | `dxflrs/garage:v1.0.1` | Inconsistent (precondition ignored) | refused (by design) | every push/PR |
| SeaweedFS | `chrislusf/seaweedfs:3.80` | Inconsistent | refused | every push/PR |
| MinIO | `minio/minio:RELEASE.2025-04-22T22-12-26Z` | Supported | available | every push/PR (legacy note: repo archived 2026-04) |
| Ceph RGW | `quay.io/ceph/demo:latest-quincy` | (record live verdict from weekly job) | per probe | weekly |
| AWS S3 | n/a | expected Supported | per probe | **not CI-verified** (documented gap; config-compatible via `[storage.s3]`) |
| Local FS | n/a | Supported (blanket impl) | n/a (single node) | every push/PR |

  Plus per-backend config snippets (`[storage.s3]` endpoint/path-style examples) and the
  sovereignty paragraph (plain PUT/GET/LIST always sufficient; CAS strictly optional).

- [ ] **Step 5: ops pages.**
  - `ops/README.md`: one-paragraph orientation + links.
  - `ops/profiles.md`: laptop (memory/local), durable single node (local log+store), sovereign
    scale-out (object-store log + s3 store, 1 writer + N query nodes, Compose demo pointer),
    sizing knobs (cache tiers, `max_live_bytes`, group-commit window/bytes, `max_block_rows`,
    `blocks_to_keep`) with defaults and when to turn each.
  - `ops/failover.md`: designated-writer (default, works everywhere, deployment-enforced) vs
    `cas-failover` (probe-gated, epoch fence, < 10 s takeover, zombie proof — cite the failover
    example), the Task-4 hardened manifest selection, and the GC/follower `LogGap` retention note
    from Task 1.
  - `ops/metrics.md`: MOVE the existing `docs/ops/metrics.md` content here verbatim (it is
    already Grafana-ready); fix any relative links; leave the pointer stub behind.

- [ ] **Step 6: `reference/http-api.md` + `reference/cli.md`** — the frozen slice-9 wire
  contract: route/auth matrix (`/healthz` public; bearer for `/metrics`, `/v1/status`,
  `/v1/query`, `/v1/tx`, `/v1/admin/*`), 421 writer redirect, basis forms (`tx_id` /
  `at:<packed>` / `basis_timeout_ms`), JSON vs `application/vnd.apache.arrow.stream` content
  negotiation, tagged-bytes JSON convention, request/response examples (copy real bodies from
  `varve-server/tests/http_api.rs`); CLI: `--dir` XOR `--url`+`--token` (env `VARVE_TOKEN`),
  `shell`, `import --label [--graph]`, `export --query [--basis]`, `admin
  status|compact|gc|verify [--json]`, JSONL format incl. `{"$bytes": base64}`.

- [ ] **Step 7: Build + commit**

```bash
just docs   # zero warnings, zero missing files
git add docs/book docs/ops/metrics.md
git commit -m "docs: GQL reference, backend matrix, and ops guide"
```

---

### Task 13: Configuration reference generated from code

The ops guide's config page is GENERATED, not hand-maintained: a testkit binary renders the full
`[section] key = default  # description` reference, a drift test pins the committed page to the
generator's output, and load-bearing defaults are asserted against the live code so the page
cannot silently rot.

**Files:**
- Create: `crates/varve-testkit/src/config_reference.rs` (module, `pub fn render() -> String`)
- Create: `crates/varve-testkit/src/bin/config_reference.rs`
- Create: `crates/varve-testkit/tests/config_reference_doc.rs` (drift test)
- Modify: `crates/varve-testkit/src/lib.rs` (`pub mod config_reference;`)
- Create (from stub): `docs/book/src/ops/configuration.md` (generated output, committed)
- Modify: `justfile` (`docs-gen` recipe)

**Interfaces:**
- Consumes: exported default constants where they exist (e.g.
  `varve_log::DEFAULT_SEGMENT_MAX_BYTES`); for defaults that live only in private serde
  `#[serde(default = …)]` fns or factory code, EXPORT a `pub const` next to the owning component
  (small `refactor:` edits in the owning crates — e.g. `pub const DEFAULT_TAIL_POLL_INTERVAL_MS:
  u64 = 50;` in `varve-engine`) and use it in BOTH the serde default and the generator. That
  cross-use is what makes the reference "generated from code".
- Produces: `render() -> String` (full markdown page); bin prints it;
  `just docs-gen` = `cargo run -p varve-testkit --bin config_reference >
  docs/book/src/ops/configuration.md`.

- [ ] **Step 1: Write the failing drift test**

```rust
use std::path::Path;

#[test]
fn committed_configuration_page_matches_the_generator() {
    let page = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../docs/book/src/ops/configuration.md");
    let committed = std::fs::read_to_string(&page)
        .expect("docs/book/src/ops/configuration.md must exist — run `just docs-gen`");
    assert_eq!(
        committed,
        varve_testkit::config_reference::render(),
        "configuration.md drifted — run `just docs-gen` and commit the result"
    );
}
```

- [ ] **Step 2: Implement `render()`.** Data model + full section list (this enumeration IS the
  content contract; every row carries section, key, type, default, one-line description):

```rust
struct Entry {
    key: &'static str,
    r#type: &'static str,
    default: String,
    description: &'static str,
}

struct Section {
    name: &'static str,
    intro: &'static str,
    entries: Vec<Entry>,
}

pub fn render() -> String { /* header + per-section markdown tables */ }
```

  Sections/keys to cover (defaults from the owning code, formatted exactly as TOML accepts them):
  `[node]` roles, tail_poll_interval_ms (50), tail_batch_records (1024), basis_timeout_ms (5000);
  `[log]` backend (memory|local|object-store), group_commit_window_ms (15),
  group_commit_max_bytes ("8MiB"); `[log.local]` dir, segment_max_bytes
  (`DEFAULT_SEGMENT_MAX_BYTES`); `[storage]` backend (memory|local|s3), max_block_rows (100000),
  flush_interval_ms (300000), max_live_bytes; `[storage.local]` dir; `[storage.s3]` bucket,
  endpoint, region, access_key_id, secret_access_key, path_style (true);
  `[cache]` tiers; `[cache.memory]` max_bytes ("512MiB"); `[cache.disk]` dir, max_bytes
  ("50GiB"); `[query]` max_path_depth (10) + the slice-7 budget keys; `[gc]` enabled (false),
  blocks_to_keep (10), garbage_lifetime_hours; `[writer]` submission-queue size key (slice-10);
  `[coordinator]` backend (designated-writer|cas-failover) + heartbeat/lease keys;
  `[server]` backend (http); `[server.http]` listen, advertised_address, max_body_bytes
  ("8MiB"), tls_cert, tls_key; `[auth]` backend (static); `[auth.static]` tokens;
  `[metrics]` backend (prometheus|otlp) + otlp keys.
  **Verification step during implementation:** for each section, open the owning config struct
  (`rg 'serde\(default' crates/<crate>/src` per component) and confirm key spelling + default —
  the generator's rows must name real keys; a key in this plan that doesn't exist in code is
  dropped, a key in code missing here is added (code wins).
  For at least the LOAD-BEARING defaults (group_commit_window_ms, tail_poll_interval_ms,
  max_block_rows, blocks_to_keep, segment_max_bytes, max_body_bytes, basis_timeout_ms), reference
  the exported const in the generator (`default: varve_log::DEFAULT_SEGMENT_MAX_BYTES.to_string()`)
  rather than a literal.

- [ ] **Step 3: Generate + run**

```bash
cargo run -p varve-testkit --bin config_reference > docs/book/src/ops/configuration.md
cargo test -p varve-testkit --test config_reference_doc
just docs
```

  All three green. Add the `docs-gen` justfile recipe.

- [ ] **Step 4: Gate + commit**

```bash
cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings && cargo test -p varve-testkit
git add crates/varve-testkit docs/book/src/ops/configuration.md justfile crates/*/src
git commit -m "docs: configuration reference generated from code with drift test"
```

---

### Task 14: Deferred fast-follow sweep (slice-9/10 items owned by this slice)

Eight small, independent items — each gets its own red/green cycle and commit. v1 must not ship
dead API, misleading docs, or untested reject paths that STATUS.md already flagged.

**Files:** per item below.

**Interfaces:** no new public API; item (g) DELETES one public enum variant.

- [ ] **(a) CLI rejects invalid `--label`/`--graph` identifiers — durable tests.**
  Files: `crates/varve-cli/tests/transfer.rs`. Tests first: drive the import path with label
  `"bad-ident!"` and (separately) graph `"1nope"`; assert the error message names the invalid
  identifier and NO client call is made (mirror the existing invalid-property-KEY test in the same
  file for harness/mock shape — `validate_identifier` at `src/transfer.rs:157` is the unit under
  test). Run → both must FAIL RED only if the behavior is missing; if they pass immediately, the
  reject path already works and the tests simply pin it (commit as `test:`).
  Commit: `test: pin import label/graph identifier rejection`.

- [ ] **(b) `cli_help.rs` asserts literal `--help` output.**
  Files: `crates/varve-cli/tests/cli_help.rs`. Render help via the clap `Command` from
  `varve-cli/src/cli.rs` (`Command::render_long_help()`), assert it contains the literal
  substrings `"import"`, `"export"`, `"admin"`, `"shell"`, `"--dir"`, `"--url"`, `"--token"`.
  Commit: `test: assert varve --help lists every subcommand and selector`.

- [ ] **(c) `TxResponse::from_receipt` synthetic-instant decision — keep + pin.**
  Files: `crates/varve-server/src/api.rs` (line ~71 + tests). DECISION (record in STATUS): keep
  the `<micros>us` fallback for out-of-chrono-range instants (unreachable from real receipts;
  public API stays total). Add a unit test constructing an extreme synthetic receipt and asserting
  the exact fallback string, plus a rustdoc line on `from_receipt` documenting it.
  Commit: `test: pin synthetic-instant formatting on TxResponse::from_receipt`.

- [ ] **(d) Move the strayed mutation doc comment.**
  Files: `crates/varve-engine/src/db.rs`. The doc comment on `publish_writer` (~line 1178)
  describes mutation execution — move it to `Db::execute`; write `publish_writer` its own
  accurate doc (canonical-JSON plain PUT of the advertisement, NOT coordination).
  Commit: `docs: put the mutation doc comment back on Db::execute`.

- [ ] **(e) Probe-path diagnostic asserts.**
  Files: `crates/varve-testkit/tests/cas_failover_backends.rs` (~line 73 onward). Replace bare
  `.unwrap()`/`.unwrap_err()` on the refusal-path assertions with `expect`/`assert!` messages
  that include the probe verdict/report, so a live-backend failure names WHAT the probe saw.
  Commit: `test: diagnostic asserts on the cas-failover probe path`.

- [ ] **(f) `FenceMap`: extract `is_dead` + test `EpochExhausted`.**
  Files: `crates/varve-engine/src/coord/fence.rs`. Test first: a `jump` at epoch `u16::MAX`
  returns the `EpochExhausted` error (currently untested). Then extract the duplicated
  dead-condition shared by `is_live`/`jump` into a private `fn is_dead(...) -> bool` (pure
  refactor; existing tests stay green).
  Commit: `refactor: shared FenceMap dead-condition with EpochExhausted coverage`.

- [ ] **(g) Delete `EngineError::InvalidCoordinatorConfig`.**
  Files: `crates/varve-engine/src/db.rs` (line ~123) + any exhaustive matches. The variant is
  declared, never constructed; bad `[coordinator]` config already surfaces via
  `RegistryError::Build` carrying `validate()`'s message. Delete it (no back-compat); fix
  matches; `cargo test -p varve-engine` green.
  Commit: `refactor: drop the never-constructed InvalidCoordinatorConfig variant`.

- [ ] **(h) Note the inert `fault-injection` unification.**
  Files: `crates/varve-server/Cargo.toml`. One comment above the `varve-testkit` dev-dep: it
  transitively enables `varve-log`/`varve-engine` `fault-injection` in the TEST build only;
  inert unless `VARVE_CRASH_TRIGGER` is set; isolate the fixture if that ever changes.
  Commit: `docs: note the inert fault-injection feature unification in server tests`.

- [ ] **Final step: whole-sweep gate**

```bash
cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace -- --test-threads=1
```

---

### Task 15: Release metadata — LICENSE, crate descriptions, v1.0.0 versions, CHANGELOG, README

Make the workspace publishable: every shipping crate carries full crates.io metadata at version
1.0.0, the repo carries its license text, CHANGELOG and README tell the v1 story. Publishing
itself is Task 17 (user-gated).

**Files:**
- Create: `LICENSE` (Apache-2.0 full text, copyright "Varve contributors")
- Create: `CHANGELOG.md`
- Modify: `Cargo.toml` (workspace version → `1.0.0`)
- Modify: every `crates/*/Cargo.toml` (description; internal path deps gain `version = "1.0.0"`;
  `varve-testkit` gains `publish = false`)
- Modify: `README.md` (quickstart rewrite + license section)

**Interfaces:**
- Consumes: Task 11's getting-started page (README quickstart mirrors it, shorter).
- Produces: `cargo package`-clean leaf crates; Task 16 tarballs include LICENSE/README/CHANGELOG;
  Task 17 publishes in the dependency order recorded here.

- [ ] **Step 1: LICENSE + workspace version.** Apache-2.0 text at repo root;
  `[workspace.package] version = "1.0.0"`. Run `cargo build --workspace` (lockfile version
  bumps).

- [ ] **Step 2: Per-crate metadata.** For each shipping crate add `description` (one line, own
  words per crate — e.g. varve: "Bitemporal property-graph database speaking GQL — embedded
  facade"; varve-server: "HTTP server (varved) for VarveDB"; varve-cli: "CLI (varve shell,
  import/export, admin) for VarveDB"; etc. for types/config/gql/log/storage/index/plan/engine),
  and on EVERY internal dependency add the version:
  `varve-engine = { path = "../varve-engine", version = "1.0.0" }` (required by crates.io;
  path is stripped on publish). `varve-testkit`: `publish = false` (test rig; also keeps its
  docker/fixture surface off crates.io). `fuzz/` is already `publish = false` + workspace-excluded.

- [ ] **Step 3: Verify packaging.**

```bash
cargo package -p varve-types --list   # sane file set, no stray artifacts
cargo publish --dry-run -p varve-types
```

  Leaf crate dry-run must PASS. Dependent crates cannot fully dry-run before their deps exist on
  crates.io — record the publish order in CHANGELOG.md's release checklist instead:
  `varve-types → varve-config → varve-gql → varve-log → varve-storage → varve-index → varve-plan
  → varve-engine → varve → varve-server → varve-cli`
  (derive the true order from `cargo tree` if it disagrees — code wins).

- [ ] **Step 4: CHANGELOG.md** — one `## 1.0.0 (2026-07-XX)` section, feature summary grouped by
  subsystem (bitemporal engine, GQL surface + TCK standing, durability + crash matrix, object
  storage + backends, compaction/GC + GDPR erase, server/CLI, coordination/failover,
  observability), known limitations (from `gql/deviations.md` + AWS-not-CI-verified), and the
  release checklist (publish order, tag, artifacts).

- [ ] **Step 5: README quickstart** — rewrite the top half: badges-free, 30-second pitch,
  install matrix (cargo install / docker / tarball — marked available from v1.0.0; from-source
  path that works today), the 6-statement GQL session from getting-started, links to the book,
  benchmark report, design spec. Add License section (Apache-2.0). Keep the workspace/gates
  section.

- [ ] **Step 6: USER DECISIONS — STOP and ask Ravan** (do not guess; record answers in
  STATUS.md):
  1. Repo rename `timedb` → `varve` and the `repository` URL (`https://github.com/ravan/varve`
     currently points at a name the repo does not have). Rename now, or ship with the URL fixed
     to the real repo?
  2. crates.io: publish under which account? Are all 11 names free/reserved?
  3. Container registry: ghcr.io under which owner/name? (Task 16 defaults to
     `ghcr.io/<github-owner>/varve`.)

- [ ] **Step 7: Gate + commit**

```bash
cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace -- --test-threads=1
git add LICENSE CHANGELOG.md README.md Cargo.toml Cargo.lock crates/*/Cargo.toml
git commit -m "feat: v1.0.0 release metadata, LICENSE, CHANGELOG, README quickstart"
```

---

### Task 16: Release workflow — binaries for 3 targets + container image

Tag-triggered `release.yml`: macOS arm64 native, Linux x86_64-musl native, Linux aarch64-musl via
`cross`; tarballs (`varve` + `varved` + LICENSE + README + CHANGELOG + sha256) uploaded to the
GitHub release; container image pushed to ghcr with the tag. This is "cargo dist (or equiv)" —
equiv chosen: hand-rolled workflow, matching this repo's zero-extra-deps harness precedent
(slice-5 docker rig) and avoiding an unverifiable-offline tool; revisit cargo-dist post-v1 if
release cadence grows.

**Files:**
- Create: `.github/workflows/release.yml`
- Create: `scripts/package_release.sh`
- Modify: `justfile` (`package` recipe for local verification)

**Interfaces:**
- Consumes: Task 15 metadata (version, LICENSE, CHANGELOG), existing `Dockerfile` (distroless,
  CI-built already).
- Produces: on tag `v*`: GitHub release assets `varve-<tag>-<target>.tar.gz` (+ `.sha256`) for
  `aarch64-apple-darwin`, `x86_64-unknown-linux-musl`, `aarch64-unknown-linux-musl`; image
  `ghcr.io/<owner>/<repo>:<tag>` and `:latest`.

- [ ] **Step 1: `scripts/package_release.sh`** (used by CI AND locally):

```bash
#!/usr/bin/env sh
# Usage: package_release.sh <target-triple> <version> [cargo-cmd]
# Builds varve (CLI) + varved (server) for <target> and produces
# dist/varve-<version>-<target>.tar.gz + .sha256.
set -eu
TARGET="$1"
VERSION="$2"
CARGO="${3:-cargo}"

$CARGO build --release --locked --target "$TARGET" -p varve-cli -p varve-server
STAGE="$(mktemp -d)"
trap 'rm -rf "$STAGE"' EXIT
mkdir -p "$STAGE/varve-$VERSION" dist
cp "target/$TARGET/release/varve" "target/$TARGET/release/varved" \
   LICENSE README.md CHANGELOG.md "$STAGE/varve-$VERSION/"
TARBALL="dist/varve-$VERSION-$TARGET.tar.gz"
tar -C "$STAGE" -czf "$TARBALL" "varve-$VERSION"
if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$TARBALL" > "$TARBALL.sha256"
else
    shasum -a 256 "$TARBALL" > "$TARBALL.sha256"
fi
echo "packaged $TARBALL"
```

  Verify LOCALLY for the host triple:

```bash
rustup target add aarch64-apple-darwin 2>/dev/null || true
sh scripts/package_release.sh aarch64-apple-darwin 1.0.0
tar -tzf dist/varve-1.0.0-aarch64-apple-darwin.tar.gz   # lists varve, varved, LICENSE, README.md, CHANGELOG.md
```

  justfile: `package target version: sh scripts/package_release.sh {{target}} {{version}}` (and
  add `dist/` to `.gitignore`).

- [ ] **Step 2: `.github/workflows/release.yml`**

```yaml
name: Release
on:
  push:
    tags: ["v*"]

permissions:
  contents: write
  packages: write

jobs:
  binaries:
    strategy:
      fail-fast: false
      matrix:
        include:
          - target: aarch64-apple-darwin
            os: macos-14
            cross: false
          - target: x86_64-unknown-linux-musl
            os: ubuntu-latest
            cross: false
          - target: aarch64-unknown-linux-musl
            os: ubuntu-latest
            cross: true
    runs-on: ${{ matrix.os }}
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
        with:
          targets: ${{ matrix.target }}
      - if: matrix.target == 'x86_64-unknown-linux-musl'
        run: sudo apt-get update && sudo apt-get install -y musl-tools
      - if: matrix.cross
        run: cargo install cross --locked
      - name: Package
        run: |
          VERSION="${GITHUB_REF_NAME#v}"
          CARGO=cargo
          if [ "${{ matrix.cross }}" = "true" ]; then CARGO=cross; fi
          sh scripts/package_release.sh "${{ matrix.target }}" "$VERSION" "$CARGO"
      - name: Upload to release
        env:
          GH_TOKEN: ${{ github.token }}
        run: |
          gh release create "$GITHUB_REF_NAME" --verify-tag --draft --title "$GITHUB_REF_NAME" 2>/dev/null || true
          gh release upload "$GITHUB_REF_NAME" dist/* --clobber

  image:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: docker/login-action@v3
        with:
          registry: ghcr.io
          username: ${{ github.actor }}
          password: ${{ github.token }}
      - name: Build and push
        run: |
          IMAGE="ghcr.io/${{ github.repository }}"
          docker build --tag "$IMAGE:$GITHUB_REF_NAME" --tag "$IMAGE:latest" .
          docker push "$IMAGE:$GITHUB_REF_NAME"
          docker push "$IMAGE:latest"
```

  Notes to verify at execution: `gh` is preinstalled on GitHub runners; the release is created
  as DRAFT so the user publishes it after inspection; the image name follows
  `github.repository`, which the Task 15 rename decision may change — align with the recorded
  answer. `ring` (rustls) is known to build on musl (x86_64 with musl-tools; aarch64 under
  cross's containerized toolchain) — if the aarch64 leg fails on first tag, that leg is fixed
  forward on the draft release, not re-planned.

- [ ] **Step 3: Validate + commit**

```bash
python3 -c "import yaml; yaml.safe_load(open('.github/workflows/release.yml'))"
git add .github/workflows/release.yml scripts/package_release.sh justfile .gitignore
git commit -m "feat: tag-triggered release workflow for binaries and container image"
```

---

### Task 17: Final acceptance pass, closeout, and user-gated ship

Walk spec §1's success criteria 1–8 with concrete evidence, run the whole-slice gate, close the
ledger, and hand the user the two irreversible buttons (tag, publish) — explicitly NOT pressed by
the agent.

**Files:**
- Create: `docs/release/v1-acceptance.md`
- Modify: `docs/plans/STATUS.md`, `docs/plans/varve-v1-roadmap.md` (tick slice-11 boxes + slice
  log row), `README.md` (only if the gate reveals stale text)

**Interfaces:** consumes everything above.

- [ ] **Step 1: Whole-slice verification gate** (each command, record actual output):

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace -- --test-threads=1
cargo test -p varve --test gdpr_gc -- --test-threads=1
cargo +nightly fuzz build && for t in parse log_record manifest block_meta events; do \
  cargo +nightly fuzz run $t -- -max_total_time=60 -rss_limit_mb=4096; done
cargo bench -p varve-index -p varve-types -p varve-gql -- --quick
cargo run --release --example social_bench -p varve
VARVE_CRASH_ITERS=10 cargo test -p varve-testkit --release --test crash_recovery
just docs && cargo test -p varve-testkit --test config_reference_doc
just compose-demo
sh scripts/package_release.sh aarch64-apple-darwin 1.0.0
git diff --check
git log --format='%an %ae %(trailers)' <branch-base>..HEAD | grep -ci co-authored || true  # MUST be 0
```

- [ ] **Step 2: Write `docs/release/v1-acceptance.md`** — one row per spec §1 criterion, each
  with named evidence (test file :: test name, CI job, demo command, or report section):

| # | Criterion | Evidence |
|---|---|---|
| 1 | embeds + serves HTTP | `examples/hello.rs`; `varve-server/tests/http_api.rs`; `just compose-demo` |
| 2 | GQL core passes adapted TCK + temporal suite | `varve-testkit/tests/tck.rs` gate (current pass rate ≥ 0.85 with reasoned exclusions); `varve/tests/` temporal suites; honesty note: adapted TCK, not full-standard conformance |
| 3 | full bitemporality + GDPR erase | slice-2 property tests; `varve/tests/erase.rs`; `varve/tests/gdpr_gc.rs` full-byte + raw-object proofs (Tasks 2–3) |
| 4 | local FS + Garage/Ceph/SeaweedFS/MinIO/AWS | `backend-matrix` + `backend-ceph-weekly` CI; **AWS documented as config-compatible, NOT CI-verified — explicit gap** |
| 5 | 1 writer + N query nodes, read scale-out | compose demo; `process_scale_out.rs`; `scale_out_bench.rs` table (Task 9) |
| 6 | deterministic compaction, bounded storage | golden determinism tests; `compaction_gc` example plateau; `compaction_equivalence.rs` |
| 7 | crash-safe kill -9 | `crash-matrix` CI (100×); `chaos-nightly` (30 min) |
| 8 | shippable artifacts | release workflow + image (Task 16); mdBook (Tasks 11–13); benchmark report (Task 10) |

  Any criterion that cannot be evidenced → fix the gap now (that is this task's purpose) or
  record it as an explicit, user-acknowledged exception in the report.

- [ ] **Step 3: Closeout.** STATUS.md: slice 11 complete (tasks, decisions — Task 1 GC policy,
  Task 4 selection rule, Task 14c formatting decision, Task 15 user answers, deviations),
  final verification transcript summary, demo commands (`social_bench`, `bench-scale-out`,
  `docs-serve`), and a "v1 READY — awaiting tag/publish" entry point. Roadmap: tick all slice-11
  boxes, fill the exit-criteria line, slice-log row 11. Commit
  `docs: slice 11 closeout — v1 acceptance and ledger`.

- [ ] **Step 4: USER-GATED ship steps — present to Ravan, do NOT execute unprompted:**

```bash
# 1. Tag + release (draft created by the workflow; publish the draft after inspecting assets)
git tag v1.0.0 && git push origin main v1.0.0
# 2. crates.io, in dependency order (each waits for the index):
cargo publish -p varve-types    # then config, gql, log, storage, index, plan, engine, varve, server, cli
# 3. Post-publish smoke (the roadmap's ≤5-minute exit criterion, only measurable now):
cargo install varve-cli && varve shell --dir /tmp/varve-smoke
docker run ghcr.io/<owner>/<repo>:v1.0.0 --help
```

  The plan is DONE when Step 3's closeout commit lands; Step 4 is the user's call.

---

## Slice exit checklist

- [ ] Roadmap slice-11 boxes ticked: ERASE end-to-end verification; fuzz targets complete +
      nightly budget; benchmark suite + `docs/benchmarks/v1.md`; docs site under `docs/book/`;
      release engineering; final acceptance pass.
- [ ] `cargo fmt --all --check` clean; `cargo clippy --workspace --all-targets -- -D warnings`
      clean; `cargo test --workspace -- --test-threads=1` green.
- [ ] Both GDPR proofs green (`cargo test -p varve --test gdpr_gc -- --test-threads=1`).
- [ ] All five fuzz targets build and survive a 60 s local run; `fuzz-nightly` matrix committed.
- [ ] `just docs` builds with zero stubs (`grep -r "TODO(slice-11" docs/book/src` → empty);
      config-reference drift test green.
- [ ] `docs/benchmarks/v1.md` published with re-measured numbers vs spec §13 targets.
- [ ] `docs/release/v1-acceptance.md` — all 8 criteria evidenced or explicitly excepted.
- [ ] Release workflow + package script committed; local host-triple tarball verified.
- [ ] No co-author trailers anywhere on the branch.
- [ ] STATUS.md updated (position, decisions, deviations, demo commands, next entry point =
      user-gated tag/publish); roadmap slice log row filled.
- [ ] User decisions recorded (repo rename, crates.io, registry); tag/publish left to Ravan.
