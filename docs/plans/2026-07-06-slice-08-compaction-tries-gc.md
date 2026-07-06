# Slice 8: Compaction, Tries, GC — deterministic leveled compaction, hash-trie meta, garbage collection

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Flushed L0 blocks are compacted — deterministically and coordination-free — into a leveled trie hierarchy (L0 → L1-current + L1-historical weekly buckets → Ln 4-way IID-partitioned), meta files become real hash tries pruned by IID path, `Erase` events physically drop matching rows, and a garbage collector deletes superseded tries, orphan objects, old manifests, superseded log objects, and probe leftovers past a retention window. Storage plateaus under sustained update churn.

**Architecture:** Ports XTDB's trie catalog / job calculator / segment merge (references: `refs/xtdb/dev/doc/trie-cat.allium`, `compaction.allium`, `gc.allium`, `block-gc.allium`, `refs/xtdb/core/src/main/kotlin/xtdb/{trie,compactor,garbage_collector}/`) onto Varve's manifest-committed storage model. New crate `varve-compact` (spec §12 names it) holds the pure core: `FamilyCatalog` (nascent/live/garbage state machine), `available_jobs` (pure `fn(catalog) -> Vec<CompactionJob>`), `run_merge` (k-way merge + bitemporal resolution + recency routing + page normalization), `plan_gc` (pure deletion planner). `varve-index` gains the IID `Bucketer`, an XTDB-style `MemoryTrie` live index (LOG_LIMIT 64 / PAGE_LIMIT 1024), trie-structured block meta, and a streaming `CompactionResolver` reusing the slice-2 `Ceiling`/`Polygon` port (`Polygon::recency()` already exists — `bitemporal.rs:195`). `varve-engine` wires a compactor task that commits results **through the writer loop** as new manifests (one-lock discipline upheld), and a GC pass. Trie states are **persisted in the manifest**, so the catalog is a pure fold over manifest history and restart-safe.

**Tech Stack:** No new external dependencies. `chrono` (workspace-pinned `0.4`, `default-features=false, features=["std"]`) is added to `varve-storage` and `varve-compact` for week-bucket dates. New workspace member `crates/varve-compact` (deps: `varve-types`, `varve-index`, `varve-storage`, `thiserror`, `bytes`).

## Global Constraints

- All roadmap Global Constraints apply: TDD (superpowers:test-driven-development), `cargo clippy --workspace --all-targets -- -D warnings`, no `unwrap()`/`expect()` in library code (tests may via `clippy.toml`), errors via `thiserror` per crate, conventional commit prefixes, **no `Co-Authored-By` trailer**.
- **We are in development. No backward compatibility, ever.** Old golden vectors that a format change invalidates are regenerated, not aliased. Production code only — no throwaway scaffolding.
- **The test code in this plan is the contract.** Implementation sketches were written against the pinned deps (`arrow = "58"`, `datafusion = "54"`, `prost = "0.14"`, `object_store = "0.13"`, `chrono = "0.4"`, `tokio = "1"`); if a sketch's API doesn't compile against the pinned version, adapt the implementation, never the test's asserted behavior.
- **Determinism is a hard requirement (spec §9, D-determinism):** every function on the compaction output path uses `BTreeMap`/`BTreeSet`/sorted `Vec` (never `HashMap`), no wall-clock reads, no randomness, no thread-count dependence. Job identity = the output trie key; two nodes given the same catalog MUST produce byte-identical output files.
- **XTDB constants adopted verbatim:** branch factor 4 (2 IID bits per level, MSB-first), `LOG_LIMIT = 64`, `PAGE_LIMIT = 1024` (== existing `DEFAULT_PAGE_ROWS`), file-size target 104_857_600 bytes (100 MiB), weekly recency buckets rolling over Monday 0000Z, max trie depth 64.
- **Parallel-session hazard:** slice 7 may be executing in another session and its plan touches `varve-engine/src/{db.rs,writer.rs,state.rs}`, `varve-index/src/scan.rs`, and `varve-plan`. Per the session protocol: **re-read STATUS.md and `git log` at session start; rebase on current main before starting and re-read any file this plan says to modify before editing it.** Signatures quoted from those files are as of commit `e896c58` — if slice 7 moved them, the *semantic* instruction governs.
- Arrow IPC bytes are never golden-pinned across versions (slice-4 decision); determinism tests compare bytes **between runs within one build**, and manifest protobuf wire IS golden-pinned.

## Design decisions (rationale distilled from the XTDB references — binding for this slice)

- **D1 — Trie key grammar (Trie.kt):** `l<lexhex level>-r<recency>[-p<part>]-b<lexhex block>`. Lex-hex = `hex(len(body)-1)` prefix + unsigned-hex body (`0 → "00"`, `52 → "134"`). Recency = literal `c` (current/∞) or `yyyyMMdd` week date. Part = base-4 digit characters, one per level, **segment omitted entirely when empty** (matches existing L0 keys — `l0_trie_key(0) == "l00-rc-b00"` stays byte-identical).
- **D2 — Bucketing (Bucketer.kt):** bucket at `level` = 2-bit crumb of the 16-byte key, MSB-first: `(byte[level/4] >> (6 - 2*(level%4))) & 3`. A part path owns the half-open IID range `[start_iid(path), start_iid(increment_path(path)))`; `increment_path` carries right-to-left in base 4 and returns `None` on overflow (range unbounded above).
- **D3 — Trie catalog is a pure fold over the manifest (trie-cat.allium adapted):** Varve manifests carry the FULL inventory each write (slice-4 decision), so `TrieEntry` gains persisted `state` (Live=0/Nascent=1/Garbage=2) + `garbage_as_of_us`; restart = parse latest manifest, no replay of insertion rules needed. In-memory transitions (`add_trie`) implement trie-cat.allium's insertion rules exactly: L0 → live immediately; L1H → nascent until a covering L1C arrives; L1C → levelled (supersedes partial (<100 MiB) L1C, marks pending L1H live, supersedes L0); L2H → levelled + supersedes L1H; L2+C/L3+H → nascent until all 4 part-group siblings exist, then group live + parent shard superseded. Stale guard: a trie with `block <= shard.max_block` is dropped (idempotent re-delivery).
- **D4 — Deviation from XTDB, recorded:** XTDB *drops* superseded L0 entries from its catalog and never GCs the files (kept as compactor-reset substrate). Varve instead marks superseded L0 **Garbage** like everything else, because our recovery reads only the latest manifest — an unlisted file is unreachable, and determinism makes re-derivation unnecessary (re-running the same jobs reproduces identical bytes). Roadmap's "delete garbage tries past retention" governs.
- **D5 — Job identity = output trie key (Compactor.kt):** `(family, out_trie_key)` is the dedup key; the out key's `block` = the newest input's block index, so it is a pure function of the catalog. Duplicate execution produces byte-identical objects; last-write-wins is harmless.
- **D6 — Three job generators (compaction.allium / job_calculator.clj):** (a) L0→L1: the **single oldest** live L0 (+ the partial (<target) L1C if one exists) → recency-partitioned outputs; one job per round, so quiescence needs a loop. (b) L1H→L2H per `[1, week, []]` shard: oldest-first accumulate (partial L2H first if present) until total ≥ target or 4 inputs (3 + partial) → single output, recency preserved. (c) Tiering `Ln→L(n+1)`: any shard (level ≥ 2, or level 1 current counting only **full** (≥ target) files) with ≥ 4 live files not already covered by the child shard's `max_block` → 4 jobs, one per next 2-bit part `p ∈ 0..=3`.
- **D7 — Merge semantics (SegmentMerge.kt):** k-way cursor merge in file order `(sort_key asc, iid asc, system_from desc)`, ties broken by input order (inputs listed oldest-first, lower index first — preserving within-tie arrival order exactly as a single flush file would). Per-IID streaming resolution via `Ceiling`/`Polygon`: zero-range rows are dropped; an `Erase` row is emitted **once** per IID (then everything older for that IID is dropped) — matching XTDB's `seenErase` guard; surviving rows carry `polygon.recency()`. The reference-equivalence property suite is the tie-semantics contract.
- **D8 — Recency routing (OutWriter.kt):** only L0→L1 jobs partition by recency: `recency == END_OF_TIME` → current output (`r` = `c`); finite → weekly bucket = the **first Monday 0000Z ≥ recency** (XTDB `minusNanos(1).roundToNextPartition(WEEK)`; a recency exactly on Monday 0000Z buckets to *that* Monday). All other jobs preserve the output key's recency. **An empty L1C output is legal and required** — its arrival is what marks sibling L1H files live and supersedes the L0.
- **D9 — Compaction commits through the writer loop:** the compactor task PUTs output data-then-meta objects, then sends a `CompactCommit` submission; the writer (already the only manifest author) applies catalog transitions and writes the next manifest (`block_id` increments on EVERY manifest write, flush or compaction — L0 trie keys therefore have gaps, which is fine: keys only need uniqueness + ordering). Crash between PUT and commit leaves orphans that the same deterministic job regenerates byte-identically on restart (and GC eventually sweeps).
- **D10 — Scan reads live tries only** (nascent excluded per trie-cat.allium's ScanOperatorAccess); `merge_sources` upgrades from per-block concatenation to a per-entity k-way merge by `(system_from asc, source index asc)` because the recency split distributes one entity's events across files that no longer nest by time.
- **D11 — Adjacency families compact independently:** the catalog/jobs/merge machinery is instantiated per `(graph, table, family)` with family `""` (primary, sort key = `_iid`), `adj-out` (sort key = `_src_iid`), `adj-in` (`_dst_iid`). Bucketing/partitioning applies to the family's **sort key**. Same-IID rows stay contiguous inside a sort-key run because edge endpoints are immutable.
- **D12 — GC (gc.allium / block-gc.allium adapted):** pure `plan_gc` computes: (1) garbage tries with `garbage_as_of_us <= now - retention` → delete **data before meta** (meta is the completion marker), then drop from catalog via a writer-committed manifest; (2) orphan data/meta objects (parseable trie key, absent from every catalog, block ≤ latest manifest block, aged past retention via the manifest-history system-time proxy); (3) manifests older than retention except the latest (retention window = snapshot pinning); (4) `.vlog` objects strictly below the **minimum watermark over retained manifests** (discharges slice-5's "trim is a no-op, GC sweeps"); (5) everything under `v1/probe/`. Deletes are idempotent (`NotFound` ⇒ `Ok`); delete-before-commit ordering makes a crash mid-GC self-healing on re-run.
- **D13 — Live index becomes a hash trie (roadmap bullet 1, spec §9):** `LiveTable` swaps its `BTreeMap<Iid, Vec<Event>>` for an event arena + `MemoryTrie` (row-index leaves, log/page split at 64/1024). Public behavior is preserved and property-tested against the old semantics; `entities()`/`events_for` return owned `Vec<Event>` (callers cloned anyway). Deviation from XTDB recorded: leaf ordering is `(iid asc, row asc)` (arrival order — our resolve contract) instead of XTDB's newest-first.
- **D14 — Shared encoder:** page-tree building + encoding is extracted to a pure `encode_rows(rows, page_rows, order)` used by both flush (`encode_block_by`) and compaction outputs, so flush and merge bytes come from ONE code path (mirrors the slice-4 `merge_sources` extraction lesson).

## Task index

| # | Task | Crate(s) |
|---|---|---|
| 1 | Full `TrieKey` type (parse/format, recency, part) | varve-storage |
| 2 | IID `Bucketer` (bucket_for, start_iid, increment_path, path compare) | varve-index |
| 3 | `MemoryTrie` (log/page hash trie over row indices) | varve-index |
| 4 | `LiveTable` adopts arena + `MemoryTrie` (API-preserving) | varve-index, varve-engine |
| 5 | Trie-structured block meta v2 + pure `encode_rows` + path pruning | varve-index, varve-engine |
| 6 | Manifest v2: persisted `TrieState` + `garbage_as_of_us` | varve-storage, varve-engine |
| 7 | `varve-compact` crate + `FamilyCatalog` state machine | varve-compact (new) |
| 8 | Job calculator (`available_jobs`, three generators) | varve-compact |
| 9 | `CompactionResolver` + `week_bucket` | varve-index, varve-compact |
| 10 | Merge executor (`run_merge`: k-way, routing, normalization) + determinism tests | varve-compact |
| 11 | `merge_sources` k-way upgrade (recency-split-safe scan merge) | varve-index |
| 12 | Engine state rework: `FamilyState` catalogs, recovery by state, live-only scan | varve-engine |
| 13 | Compactor task + `CompactCommit` via writer + `Db::compact_to_quiescence` + config | varve-engine |
| 14 | Compaction equivalence property + erase-bytes-gone + duplicate-job + crash matrix | varve-testkit, varve |
| 15 | GC: `ObjectStore::delete`, `plan_gc`, `Db::run_gc`, `[gc]` config, sweeps | varve-storage, varve-compact, varve-engine |
| 16 | Churn benchmark (`churn_bench` example) | varve |
| 17 | Slice exit checklist (STATUS.md, roadmap, demo command) | docs |

---

### Task 1: Full `TrieKey` type in varve-storage

**Files:**
- Modify: `crates/varve-storage/src/keys.rs` (replaces `l0_trie_key`, keeps `lex_hex`/`parse_lex_hex`)
- Modify: `crates/varve-storage/src/lib.rs` (re-export `TrieKey`, `Recency`)
- Modify: `crates/varve-storage/Cargo.toml` (add `chrono = { workspace = true }`)
- Modify: `crates/varve-engine/src/flush.rs` (call site of deleted `l0_trie_key`)
- Test: in-module `#[cfg(test)]` in `keys.rs`

**Interfaces:**
- Consumes: existing `pub fn lex_hex(n: u64) -> String`, `pub fn parse_lex_hex(s: &str) -> Option<u64>` (`keys.rs:8,13`).
- Produces:
  ```rust
  #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
  pub enum Recency { Current, Week(chrono::NaiveDate) }   // derive order: Current < Week(_)

  #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
  pub struct TrieKey { pub level: u64, pub recency: Recency, pub part: Vec<u8>, pub block: u64 }

  impl TrieKey {
      pub fn l0(block: u64) -> TrieKey;          // level 0, Current, empty part
      pub fn parse(s: &str) -> Option<TrieKey>;  // strict inverse of Display
  }
  impl std::fmt::Display for TrieKey { /* D1 grammar */ }
  ```
- `l0_trie_key(block_id)` is **deleted**; `flush.rs` uses `TrieKey::l0(block_id).to_string()` (byte-identical output — pinned by test).
- Later tasks rely on: `TrieKey::{l0,parse}`, `Display`, field access, `Recency`.

- [ ] **Step 1: Write the failing tests**

Append to `crates/varve-storage/src/keys.rs` tests module:

```rust
#[test]
fn trie_key_l0_matches_slice4_format() {
    assert_eq!(TrieKey::l0(0).to_string(), "l00-rc-b00");
    assert_eq!(TrieKey::l0(0x34).to_string(), "l00-rc-b134");
}

#[test]
fn trie_key_formats_xtdb_reference_examples() {
    // examples straight from refs/xtdb/dev/doc/trie-cat.allium
    let l1h = TrieKey {
        level: 1,
        recency: Recency::Week(chrono::NaiveDate::from_ymd_opt(2020, 1, 6).unwrap()),
        part: vec![],
        block: 0,
    };
    assert_eq!(l1h.to_string(), "l01-r20200106-b00");
    let l2c = TrieKey { level: 2, recency: Recency::Current, part: vec![0], block: 0 };
    assert_eq!(l2c.to_string(), "l02-rc-p0-b00");
    let deep = TrieKey { level: 3, recency: Recency::Current, part: vec![1, 3], block: 52 };
    assert_eq!(deep.to_string(), "l03-rc-p13-b134");
}

#[test]
fn trie_key_parse_round_trips() {
    for s in ["l00-rc-b00", "l01-r20200106-b00", "l02-rc-p0-b00", "l03-rc-p13-b134"] {
        let k = TrieKey::parse(s).unwrap();
        assert_eq!(k.to_string(), s);
    }
    let k = TrieKey::parse("l01-r20200106-b00").unwrap();
    assert_eq!(k.level, 1);
    assert_eq!(k.recency, Recency::Week(chrono::NaiveDate::from_ymd_opt(2020, 1, 6).unwrap()));
    assert!(k.part.is_empty());
    assert_eq!(k.block, 0);
}

#[test]
fn trie_key_parse_rejects_garbage() {
    for s in ["", "l00", "l00-rc", "x00-rc-b00", "l00-rc-p4-b00", "l00-rq-b00", "l00-rc-b"] {
        assert!(TrieKey::parse(s).is_none(), "accepted {s:?}");
    }
}

#[test]
fn trie_keys_sort_lexicographically_in_logical_order() {
    // lex-hex means string order == numeric order for level and block
    let a = TrieKey::l0(9).to_string();
    let b = TrieKey::l0(16).to_string(); // "l00-rc-b110" vs "l00-rc-b09"
    assert!(a < b);
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p varve-storage trie_key`
Expected: compile error — `TrieKey` not defined.

- [ ] **Step 3: Write the implementation**

In `crates/varve-storage/src/keys.rs` (replacing `l0_trie_key` and its doc comment):

```rust
use chrono::NaiveDate;
use std::fmt;

/// Recency side of a trie: `Current` (∞, encoded `c`) or a historical weekly
/// bucket, identified by the Monday-0000Z date the week rolls over on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Recency {
    Current,
    Week(NaiveDate),
}

/// Parsed trie key: `l<level>-r<recency>[-p<part>]-b<block>` (spec §9, XTDB Trie.kt).
/// `part` is base-4 digits (2 IID bits per level); the `-p` segment is omitted
/// when empty, so L0/L1 keys are unchanged from slice 4.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TrieKey {
    pub level: u64,
    pub recency: Recency,
    pub part: Vec<u8>,
    pub block: u64,
}

impl TrieKey {
    pub fn l0(block: u64) -> TrieKey {
        TrieKey { level: 0, recency: Recency::Current, part: Vec::new(), block }
    }

    pub fn parse(s: &str) -> Option<TrieKey> {
        let (mut level, mut recency, mut block) = (None, None, None);
        let mut part = Vec::new();
        for seg in s.split('-') {
            let (tag, arg) = (seg.chars().next()?, seg.get(1..)?);
            match tag {
                'l' => level = Some(parse_lex_hex(arg)?),
                'r' if arg == "c" => recency = Some(Recency::Current),
                'r' => {
                    if arg.len() != 8 || !arg.bytes().all(|b| b.is_ascii_digit()) {
                        return None;
                    }
                    let (y, m, d) = (arg[..4].parse().ok()?, arg[4..6].parse().ok()?, arg[6..8].parse().ok()?);
                    recency = Some(Recency::Week(NaiveDate::from_ymd_opt(y, m, d)?));
                }
                'p' => {
                    if arg.is_empty() {
                        return None;
                    }
                    for c in arg.chars() {
                        let digit = c.to_digit(4)?;
                        part.push(digit as u8);
                    }
                }
                'b' => block = Some(parse_lex_hex(arg)?),
                _ => return None,
            }
        }
        Some(TrieKey { level: level?, recency: recency?, part, block: block? })
    }
}

impl fmt::Display for TrieKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "l{}-r", lex_hex(self.level))?;
        match self.recency {
            Recency::Current => write!(f, "c")?,
            Recency::Week(d) => write!(f, "{}", d.format("%Y%m%d"))?,
        }
        if !self.part.is_empty() {
            write!(f, "-p")?;
            for digit in &self.part {
                write!(f, "{digit}")?;
            }
        }
        write!(f, "-b{}", lex_hex(self.block))
    }
}
```

Note: `parse_lex_hex` must reject empty strings (it already returns `Option`; verify — if it panics or accepts `""`, harden it now with a test).

`crates/varve-storage/Cargo.toml`: add `chrono = { workspace = true }` under `[dependencies]`. Root `Cargo.toml` already pins `chrono = { version = "0.4", default-features = false, features = ["std"] }`.

`crates/varve-storage/src/lib.rs`: extend the `keys` re-export list with `Recency, TrieKey`.

`crates/varve-engine/src/flush.rs`: replace `keys::l0_trie_key(block_id)` with `varve_storage::TrieKey::l0(block_id).to_string()` (exact call site: `flush.rs` builds `trie_key` once before the PUT loop). Run `cargo check -p varve-engine` to catch any other caller.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p varve-storage trie_key && cargo test -p varve-engine`
Expected: PASS (including the existing flush tests — the L0 key string is unchanged).

- [ ] **Step 5: Commit**

```bash
git add crates/varve-storage crates/varve-engine Cargo.lock
git commit -m "feat: full TrieKey type (level/recency/part/block) replacing l0_trie_key"
```

---

### Task 2: IID Bucketer in varve-index

**Files:**
- Create: `crates/varve-index/src/trie.rs`
- Modify: `crates/varve-index/src/lib.rs` (add `pub mod trie;` + re-exports)
- Test: in-module `#[cfg(test)]`

**Interfaces:**
- Consumes: `varve_types::Iid` (`Iid::as_bytes(&self) -> &[u8; 16]`, `Iid::from_bytes([u8; 16])`).
- Produces (all in `varve_index::trie`):
  ```rust
  pub const LEVEL_BITS: u32 = 2;
  pub const BRANCH_FACTOR: usize = 4;   // 1 << LEVEL_BITS
  pub const MAX_DEPTH: usize = 64;      // 16 bytes * 8 bits / 2
  pub const LOG_LIMIT: usize = 64;      // used by Task 3
  pub const PAGE_LIMIT: usize = 1024;   // == DEFAULT_PAGE_ROWS; used by Tasks 3/5

  pub fn bucket_for(iid: &Iid, level: usize) -> u8;                      // 0..=3
  pub fn start_iid(path: &[u8]) -> Iid;                                  // smallest IID under path
  pub fn increment_path(path: &[u8]) -> Option<Vec<u8>>;                 // base-4 +1, None on overflow
  pub fn compare_iid_to_path(iid: &Iid, path: &[u8]) -> std::cmp::Ordering;
  pub fn iid_in_path(iid: &Iid, path: &[u8]) -> bool;                    // == Ordering::Equal
  ```
- Later tasks rely on every one of these names plus the five constants.

- [ ] **Step 1: Write the failing tests**

`crates/varve-index/src/trie.rs` (tests module):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use varve_types::Iid;

    fn iid(first: u8) -> Iid {
        let mut b = [0u8; 16];
        b[0] = first;
        Iid::from_bytes(b)
    }

    #[test]
    fn bucket_for_takes_two_bit_crumbs_msb_first() {
        // 0b11_10_01_00 = 0xE4: buckets 3,2,1,0 at levels 0..4
        let i = iid(0xE4);
        assert_eq!(bucket_for(&i, 0), 3);
        assert_eq!(bucket_for(&i, 1), 2);
        assert_eq!(bucket_for(&i, 2), 1);
        assert_eq!(bucket_for(&i, 3), 0);
        // level 4 reads byte 1
        let mut b = [0u8; 16];
        b[1] = 0b01_00_00_00;
        assert_eq!(bucket_for(&Iid::from_bytes(b), 4), 1);
    }

    #[test]
    fn start_iid_reconstructs_prefix() {
        // path [3,2] -> first byte 0b11_10_00_00 = 0xE0, rest zero
        assert_eq!(start_iid(&[3, 2]), iid(0xE0));
        assert_eq!(start_iid(&[]), Iid::from_bytes([0u8; 16]));
    }

    #[test]
    fn increment_path_carries_and_overflows() {
        assert_eq!(increment_path(&[0, 1]), Some(vec![0, 2]));
        assert_eq!(increment_path(&[0, 3]), Some(vec![1, 0]));
        assert_eq!(increment_path(&[3, 3]), None);
        assert_eq!(increment_path(&[]), None); // empty path covers everything
    }

    #[test]
    fn compare_and_membership_agree_with_half_open_range() {
        let path = [1u8, 3];
        let lo = start_iid(&path);
        let hi = start_iid(&increment_path(&path).unwrap());
        // lo is in the path, hi is the first IID past it
        assert!(iid_in_path(&lo, &path));
        assert!(!iid_in_path(&hi, &path));
        assert_eq!(compare_iid_to_path(&lo, &path), std::cmp::Ordering::Equal);
        assert_eq!(compare_iid_to_path(&hi, &path), std::cmp::Ordering::Greater);
        assert_eq!(compare_iid_to_path(&Iid::from_bytes([0u8; 16]), &path), std::cmp::Ordering::Less);
    }

    #[test]
    fn every_iid_lands_in_exactly_one_sibling() {
        for first in 0u8..=255 {
            let i = iid(first);
            let hits = (0u8..4).filter(|p| iid_in_path(&i, &[*p])).count();
            assert_eq!(hits, 1);
        }
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p varve-index trie::`
Expected: compile error — module `trie` not found.

- [ ] **Step 3: Write the implementation**

`crates/varve-index/src/trie.rs`:

```rust
//! IID hash-trie primitives (port of XTDB `Bucketer.kt`): branch factor 4 —
//! consecutive 2-bit crumbs of the 16-byte key, MSB-first.

use std::cmp::Ordering;
use varve_types::Iid;

pub const LEVEL_BITS: u32 = 2;
pub const BRANCH_FACTOR: usize = 1 << LEVEL_BITS;
pub const MAX_DEPTH: usize = 16 * 8 / LEVEL_BITS as usize;
pub const LOG_LIMIT: usize = 64;
pub const PAGE_LIMIT: usize = 1024;

const LEVEL_MASK: u8 = (BRANCH_FACTOR - 1) as u8;

pub fn bucket_for(iid: &Iid, level: usize) -> u8 {
    debug_assert!(level < MAX_DEPTH);
    let bit_idx = level * LEVEL_BITS as usize;
    let byte = iid.as_bytes()[bit_idx / 8];
    (byte >> (6 - (bit_idx % 8))) & LEVEL_MASK
}

pub fn start_iid(path: &[u8]) -> Iid {
    let mut bytes = [0u8; 16];
    for (level, digit) in path.iter().enumerate() {
        let bit_idx = level * LEVEL_BITS as usize;
        bytes[bit_idx / 8] |= (digit & LEVEL_MASK) << (6 - (bit_idx % 8));
    }
    Iid::from_bytes(bytes)
}

pub fn increment_path(path: &[u8]) -> Option<Vec<u8>> {
    let mut next = path.to_vec();
    for digit in next.iter_mut().rev() {
        if *digit < LEVEL_MASK {
            *digit += 1;
            return Some(next);
        }
        *digit = 0;
    }
    None
}

pub fn compare_iid_to_path(iid: &Iid, path: &[u8]) -> Ordering {
    for (level, digit) in path.iter().enumerate() {
        match bucket_for(iid, level).cmp(digit) {
            Ordering::Equal => continue,
            other => return other,
        }
    }
    Ordering::Equal
}

pub fn iid_in_path(iid: &Iid, path: &[u8]) -> bool {
    compare_iid_to_path(iid, path) == Ordering::Equal
}
```

`crates/varve-index/src/lib.rs`: add `pub mod trie;`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p varve-index trie::`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add crates/varve-index
git commit -m "feat: IID bucketer (branch-4 hash-trie primitives, XTDB Bucketer port)"
```

---

### Task 3: `MemoryTrie` — log/page hash trie over row indices

**Files:**
- Modify: `crates/varve-index/src/trie.rs` (append below the Task-2 primitives)
- Test: in-module `#[cfg(test)]` + proptest in the same module

**Interfaces:**
- Consumes: Task 2's `bucket_for`, constants.
- Produces:
  ```rust
  pub struct MemoryTrie { /* private */ }

  impl MemoryTrie {
      pub fn new() -> Self;                                   // LOG_LIMIT 64 / PAGE_LIMIT 1024
      pub fn with_limits(log_limit: usize, page_limit: usize) -> Self;
      pub fn add(&mut self, key_of: &dyn Fn(u32) -> Iid, row: u32);
      pub fn sorted_rows(&self, key_of: &dyn Fn(u32) -> Iid) -> Vec<u32>;      // (iid asc, row asc)
      pub fn rows_for(&self, key_of: &dyn Fn(u32) -> Iid, iid: &Iid) -> Vec<u32>; // row asc
      pub fn len(&self) -> usize;
      pub fn is_empty(&self) -> bool;
  }
  impl Default for MemoryTrie { /* new() */ }
  ```
- Rows are indices into a caller-owned arena; `key_of` maps a row index to its IID (XTDB's `iidReader` shape). Leaf ordering is `(iid asc, row asc)` — **arrival order per entity**, our resolve contract (D13 deviation note from XTDB's newest-first).
- Task 4 relies on: `new`, `add`, `sorted_rows`, `rows_for`, `len`.

- [ ] **Step 1: Write the failing tests**

Append to `crates/varve-index/src/trie.rs` tests module:

```rust
use proptest::prelude::*;

fn key_table(keys: &[Iid]) -> impl Fn(u32) -> Iid + '_ {
    move |row| keys[row as usize]
}

#[test]
fn sorted_rows_orders_by_iid_then_row() {
    let keys = vec![iid(0x80), iid(0x00), iid(0x80), iid(0x40)];
    let k = key_table(&keys);
    let mut t = MemoryTrie::new();
    for row in 0..keys.len() as u32 {
        t.add(&k, row);
    }
    assert_eq!(t.sorted_rows(&k), vec![1, 3, 0, 2]);
    assert_eq!(t.len(), 4);
}

#[test]
fn rows_for_returns_only_matching_iid_in_arrival_order() {
    let keys = vec![iid(0x80), iid(0x00), iid(0x80)];
    let k = key_table(&keys);
    let mut t = MemoryTrie::new();
    for row in 0..3u32 {
        t.add(&k, row);
    }
    assert_eq!(t.rows_for(&k, &iid(0x80)), vec![0, 2]);
    assert_eq!(t.rows_for(&k, &iid(0x00)), vec![1]);
    assert_eq!(t.rows_for(&k, &iid(0x01)), Vec::<u32>::new());
}

#[test]
fn splits_past_page_limit_and_stays_correct() {
    // tiny limits force real branch structure
    let mut keys = Vec::new();
    for i in 0..64u32 {
        let mut b = [0u8; 16];
        b[0] = (i % 8) as u8 * 32; // spread over buckets
        b[15] = i as u8;
        keys.push(Iid::from_bytes(b));
    }
    let k = key_table(&keys);
    let mut t = MemoryTrie::with_limits(4, 8);
    for row in 0..keys.len() as u32 {
        t.add(&k, row);
    }
    let got = t.sorted_rows(&k);
    let mut want: Vec<u32> = (0..keys.len() as u32).collect();
    want.sort_by_key(|r| (*keys[*r as usize].as_bytes(), *r));
    assert_eq!(got, want);
}

#[test]
fn same_iid_flood_does_not_hang_or_lose_rows() {
    // all rows share one IID: splitting can never separate them; MAX_DEPTH stops it
    let keys = vec![iid(0xAA); 5000];
    let k = key_table(&keys);
    let mut t = MemoryTrie::with_limits(4, 8);
    for row in 0..5000u32 {
        t.add(&k, row);
    }
    assert_eq!(t.sorted_rows(&k), (0..5000u32).collect::<Vec<_>>());
}

proptest! {
    #[test]
    fn trie_agrees_with_btreemap_oracle(firsts in proptest::collection::vec(0u8..=255, 0..300)) {
        let keys: Vec<Iid> = firsts.iter().map(|f| iid(*f)).collect();
        let k = key_table(&keys);
        let mut t = MemoryTrie::with_limits(4, 8);
        for row in 0..keys.len() as u32 {
            t.add(&k, row);
        }
        let mut want: Vec<u32> = (0..keys.len() as u32).collect();
        want.sort_by_key(|r| (*keys[*r as usize].as_bytes(), *r));
        prop_assert_eq!(t.sorted_rows(&k), want);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p varve-index trie::`
Expected: compile error — `MemoryTrie` not defined.

- [ ] **Step 3: Write the implementation**

Append to `crates/varve-index/src/trie.rs`:

```rust
/// In-memory log/page hash trie over row indices (port of XTDB `MemoryHashTrie`).
/// Leaves buffer up to `log_limit` unsorted rows; on overflow the log is merged
/// into sorted `data` (by `(iid, row)`), and a leaf whose data exceeds
/// `page_limit` splits into a 4-way branch — unless the path is already
/// MAX_DEPTH deep (same-IID floods stop splitting there, as in XTDB).
pub struct MemoryTrie {
    root: TrieNode,
    log_limit: usize,
    page_limit: usize,
    len: usize,
}

enum TrieNode {
    Branch { path: Vec<u8>, children: [Option<Box<TrieNode>>; BRANCH_FACTOR] },
    Leaf { path: Vec<u8>, data: Vec<u32>, log: Vec<u32> },
}

impl Default for MemoryTrie {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryTrie {
    pub fn new() -> Self {
        Self::with_limits(LOG_LIMIT, PAGE_LIMIT)
    }

    pub fn with_limits(log_limit: usize, page_limit: usize) -> Self {
        MemoryTrie {
            root: TrieNode::Leaf { path: Vec::new(), data: Vec::new(), log: Vec::new() },
            log_limit: log_limit.max(1),
            page_limit: page_limit.max(1),
            len: 0,
        }
    }

    pub fn add(&mut self, key_of: &dyn Fn(u32) -> Iid, row: u32) {
        self.len += 1;
        self.root.add(key_of, row, self.log_limit, self.page_limit);
    }

    pub fn sorted_rows(&self, key_of: &dyn Fn(u32) -> Iid) -> Vec<u32> {
        let mut out = Vec::with_capacity(self.len);
        self.root.collect(key_of, &mut out);
        out
    }

    pub fn rows_for(&self, key_of: &dyn Fn(u32) -> Iid, iid: &Iid) -> Vec<u32> {
        let mut node = &self.root;
        loop {
            match node {
                TrieNode::Branch { path, children } => {
                    match &children[bucket_for(iid, path.len()) as usize] {
                        Some(child) => node = child,
                        None => return Vec::new(),
                    }
                }
                TrieNode::Leaf { .. } => break,
            }
        }
        let mut rows = node.sorted(key_of);
        rows.retain(|r| key_of(*r) == *iid);
        rows
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl TrieNode {
    fn add(&mut self, key_of: &dyn Fn(u32) -> Iid, row: u32, log_limit: usize, page_limit: usize) {
        match self {
            TrieNode::Branch { path, children } => {
                let bucket = bucket_for(&key_of(row), path.len()) as usize;
                let child = children[bucket].get_or_insert_with(|| {
                    let mut child_path = path.clone();
                    child_path.push(bucket as u8);
                    Box::new(TrieNode::Leaf { path: child_path, data: Vec::new(), log: Vec::new() })
                });
                child.add(key_of, row, log_limit, page_limit);
            }
            TrieNode::Leaf { path, data, log } => {
                log.push(row);
                if log.len() < log_limit {
                    return;
                }
                // compact the log into data
                data.extend(log.drain(..));
                data.sort_by_key(|r| (*key_of(*r).as_bytes(), *r));
                if data.len() > page_limit && path.len() < MAX_DEPTH {
                    let mut children: [Option<Box<TrieNode>>; BRANCH_FACTOR] = Default::default();
                    for r in data.drain(..) {
                        let bucket = bucket_for(&key_of(r), path.len()) as usize;
                        let mut child_path = path.clone();
                        child_path.push(bucket as u8);
                        let child = children[bucket].get_or_insert_with(|| {
                            Box::new(TrieNode::Leaf { path: child_path, data: Vec::new(), log: Vec::new() })
                        });
                        if let TrieNode::Leaf { data, .. } = child.as_mut() {
                            data.push(r); // already in sorted order — bucketing preserves it
                        }
                    }
                    *self = TrieNode::Branch { path: std::mem::take(path), children };
                }
            }
        }
    }

    fn sorted(&self, key_of: &dyn Fn(u32) -> Iid) -> Vec<u32> {
        match self {
            TrieNode::Leaf { data, log, .. } => {
                let mut rows: Vec<u32> = data.iter().chain(log.iter()).copied().collect();
                rows.sort_by_key(|r| (*key_of(*r).as_bytes(), *r));
                rows
            }
            TrieNode::Branch { .. } => unreachable!("sorted() is only called on leaves"),
        }
    }

    fn collect(&self, key_of: &dyn Fn(u32) -> Iid, out: &mut Vec<u32>) {
        match self {
            TrieNode::Branch { children, .. } => {
                for child in children.iter().flatten() {
                    child.collect(key_of, out);
                }
            }
            TrieNode::Leaf { .. } => out.extend(self.sorted(key_of)),
        }
    }
}
```

Note the `unreachable!` in `sorted()` — it is defended by the match in `collect`/`rows_for`; if clippy objects under the workspace lint set, restructure `sorted` to take `(data, log)` slices instead. A child split can leave a child leaf's `data` above `page_limit` only for same-bucket floods; it re-splits on that leaf's next log compaction, which is exactly XTDB's behavior without the bulk `addAll` path (v1: fine — flushes cap the arena at `max_block_rows`).

`crates/varve-index/Cargo.toml`: `proptest` is already a dev-dependency (used by `bitemporal` tests) — verify, add if missing.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p varve-index trie::`
Expected: PASS (Task-2 tests + 4 unit + 1 property).

- [ ] **Step 5: Commit**

```bash
git add crates/varve-index
git commit -m "feat: MemoryTrie log/page hash trie (LOG_LIMIT 64, PAGE_LIMIT 1024)"
```

---

### Task 4: `LiveTable` adopts arena + `MemoryTrie`

**Files:**
- Modify: `crates/varve-index/src/live.rs` (internal rework, API shape change to owned returns)
- Modify: `crates/varve-index/src/scan.rs` (`snapshot_entities` bound relaxes to `AsRef<[Event]>`)
- Modify: `crates/varve-index/src/block.rs` (`encode_block_by` consumes owned entity vecs)
- Modify: `crates/varve-engine/src/scan.rs`, `crates/varve-engine/src/writer.rs` (call sites — let `cargo check` enumerate; typical change: drop a `.to_vec()`/`as_slice()` adaptation)
- Test: existing suites are the contract + one new equivalence property in `live.rs`

**Interfaces:**
- Consumes: Task 3 `MemoryTrie`.
- Produces (changed `LiveTable` API — all other methods keep their exact signatures):
  ```rust
  pub fn entities(&self) -> Vec<(Iid, Vec<Event>)>;        // was impl Iterator<Item=(&Iid, &[Event])>
  pub fn events_for(&self, iid: &Iid) -> Option<Vec<Event>>; // was Option<&[Event]>
  ```
  Internals become:
  ```rust
  pub struct LiveTable {
      rows: Vec<Event>,          // arena, arrival order
      index: MemoryTrie,         // row indices keyed by event.iid
      out: BTreeMap<Iid, BTreeSet<Iid>>,
      in_: BTreeMap<Iid, BTreeSet<Iid>>,
      last_system_from: Option<Instant>,
  }
  ```
- `scan::snapshot_entities` becomes generic over owned or borrowed event lists:
  ```rust
  pub fn snapshot_entities<I, E>(entities: I, label: &str, bounds: &TemporalBounds)
      -> Result<Option<RecordBatch>, IndexError>
  where I: IntoIterator<Item = (Iid, E)>, E: AsRef<[Event]>;
  ```
- Semantics unchanged and binding: `append` still rejects `system_from` below `last_system_from` with `IndexError::OutOfOrderEvent` (ties allowed); `entities()` yields ascending IID, arrival order per entity; adjacency views update exactly as before.

- [ ] **Step 1: Write the failing test (new contract pin)**

Append to `crates/varve-index/src/live.rs` tests:

```rust
proptest::proptest! {
    #[test]
    fn arena_trie_livetable_matches_btreemap_semantics(
        firsts in proptest::collection::vec(0u8..8, 1..200)
    ) {
        use std::collections::BTreeMap;
        let mut live = LiveTable::new();
        let mut oracle: BTreeMap<Iid, Vec<Event>> = BTreeMap::new();
        for (i, f) in firsts.iter().enumerate() {
            let mut b = [0u8; 16];
            b[0] = *f;
            let e = Event {
                iid: Iid::from_bytes(b),
                system_from: Instant::from_micros(i as i64), // strictly increasing
                valid_from: Instant::MIN,
                valid_to: Instant::END_OF_TIME,
                src: None,
                dst: None,
                op: Op::Delete,
            };
            oracle.entry(e.iid).or_default().push(e.clone());
            live.append(e).unwrap();
        }
        let got: Vec<(Iid, Vec<Event>)> = live.entities();
        let want: Vec<(Iid, Vec<Event>)> = oracle.into_iter().collect();
        proptest::prop_assert_eq!(got, want);
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p varve-index live`
Expected: compile error — `entities()` returns an iterator of borrows today.

- [ ] **Step 3: Rework `LiveTable`**

Replace the struct + affected methods in `crates/varve-index/src/live.rs`:

```rust
use crate::trie::MemoryTrie;

impl LiveTable {
    pub fn append(&mut self, event: Event) -> Result<(), IndexError> {
        if let Some(last) = self.last_system_from {
            if event.system_from < last {
                return Err(IndexError::OutOfOrderEvent { last, got: event.system_from });
            }
        }
        self.last_system_from = Some(event.system_from);
        if let (Some(src), Some(dst)) = (event.src, event.dst) {
            self.out.entry(src).or_default().insert(event.iid);
            self.in_.entry(dst).or_default().insert(event.iid);
        }
        let row = self.rows.len() as u32;
        self.rows.push(event);
        let rows = &self.rows;
        self.index.add(&|r| rows[r as usize].iid, row);
        Ok(())
    }

    pub fn entities(&self) -> Vec<(Iid, Vec<Event>)> {
        let rows = &self.rows;
        let key_of = |r: u32| rows[r as usize].iid;
        let mut out: Vec<(Iid, Vec<Event>)> = Vec::new();
        for r in self.index.sorted_rows(&key_of) {
            let e = self.rows[r as usize].clone();
            match out.last_mut() {
                Some((iid, evs)) if *iid == e.iid => evs.push(e),
                _ => out.push((e.iid, vec![e])),
            }
        }
        out
    }

    pub fn events_for(&self, iid: &Iid) -> Option<Vec<Event>> {
        let rows = &self.rows;
        let key_of = |r: u32| rows[r as usize].iid;
        let found = self.index.rows_for(&key_of, iid);
        if found.is_empty() {
            return None;
        }
        Some(found.into_iter().map(|r| self.rows[r as usize].clone()).collect())
    }

    pub fn event_count(&self) -> usize {
        self.rows.len()
    }
}
```

(`new`, `last_system_from`, `out_edges`, `in_edges`, `snapshot_for_label` keep their existing bodies/signatures; the borrow-closure dance around `self.index.add` may need `let iid = event.iid;` captured by value — adapt to what borrowck accepts, e.g. compute `row`/`iid` before pushing and use a keys-vector local. The test suite is the contract.)

Adapt consumers until `cargo check --workspace` is clean:
- `scan::snapshot_entities` bound per the Interfaces block; internal uses become `events.as_ref()`.
- `block::encode_block_by` iterates `live.entities()` (owned) — its per-entity logic is unchanged.
- `varve-engine/src/scan.rs` `merged_snapshot`: the live-clone step becomes a direct `entities()` call (it cloned anyway); the point-lookup path uses `events_for` (owned).
- `varve-engine/src/writer.rs` resolve paths using `events_for` drop their `.to_vec()`.

- [ ] **Step 4: Run the full workspace suite**

Run: `cargo test --workspace`
Expected: PASS — every existing LiveTable/scan/flush/traversal test still green, plus the new property. This is the API-preserving proof.

- [ ] **Step 5: Commit**

```bash
git add crates/varve-index crates/varve-engine
git commit -m "feat: LiveTable live index backed by event arena + MemoryTrie"
```

---

### Task 5: Trie-structured block meta v2 + pure `encode_rows` + path pruning

**Files:**
- Modify: `crates/varve-index/src/block.rs` (page-tree partitioning, meta schema v2, `encode_rows` extraction, `decode_meta` → `BlockMeta`)
- Modify: `crates/varve-index/src/lib.rs` (re-export `BlockMeta`, `MetaNode`, `encode_rows`)
- Modify: `crates/varve-engine/src/state.rs` (`PersistedTrie.pages: Arc<Vec<PageMeta>>` → `meta: Arc<BlockMeta>`)
- Modify: `crates/varve-engine/src/scan.rs` (page selection goes through `BlockMeta`; IID points descend the trie)
- Modify: `crates/varve-engine/src/db.rs` (recovery decodes `BlockMeta`)
- Test: in-module in `block.rs`; `varve-testkit/tests/flush_equivalence.rs` must stay green untouched

**Interfaces:**
- Consumes: Task 2 (`bucket_for`, `iid_in_path`, `MAX_DEPTH`), existing `PageMeta`, `SortOrder`, `codec::{encode_events, decode_events}`, `LiveTable::entities()` (Task 4 shape).
- Produces:
  ```rust
  #[derive(Debug, Clone, Copy, PartialEq, Eq)]
  pub enum MetaNode {
      Branch { children: [Option<u32>; 4] },  // node indices; None = empty bucket
      Leaf { page: u32 },                     // index into pages
  }

  #[derive(Debug, Clone, PartialEq)]
  pub struct BlockMeta { pub nodes: Vec<MetaNode>, pub pages: Vec<PageMeta> }

  impl BlockMeta {
      pub fn root(&self) -> Option<u32>;                      // last node (post-order write)
      pub fn pages_for_iid(&self, iid: &Iid) -> Vec<u32>;     // 0 or 1 page; trie descent
      pub fn pages_for_part(&self, part: &[u8]) -> Vec<u32>;  // pages overlapping the part's key range
  }

  pub fn encode_rows(rows: Vec<Event>, page_rows: usize, order: SortOrder)
      -> Result<EncodedBlock, IndexError>;                     // pure; empty rows => empty data, 0 nodes/pages

  pub struct EncodedBlock {          // gains `nodes`
      pub data: Vec<u8>,
      pub meta: Vec<u8>,
      pub pages: Vec<PageMeta>,
      pub nodes: Vec<MetaNode>,
  }

  pub fn decode_meta(bytes: &[u8]) -> Result<BlockMeta, IndexError>;   // CHANGED return type
  ```
- `encode_block_by(live, page_rows, order)` becomes: collect events from `live.entities()` → `encode_rows` (behavior superset: same rows, new page boundaries). `encode_block` stays the `ByIid` wrapper.
- Sort contract (unchanged from slice 6): rows sorted stably by `(sort_key bytes, iid bytes, system_from DESC)`; sort key per `SortOrder` = iid/src/dst (missing endpoint under `BySrc`/`ByDst` still errors `IndexError::Codec`).
- **Page-tree partitioning** (replaces fixed sequential chunking; XTDB `PageTrieWriter`): recursively, a sorted row-range becomes a `Leaf` (one data page) if `len <= page_rows` **or** `depth == MAX_DEPTH` **or** all rows share one sort key; otherwise split into ≤4 child ranges by `bucket_for(sort_key, depth)` (empty buckets → `None` child) and emit a `Branch`. Pages are written to the data file in DFS bucket order; nodes are written **post-order, root last**.
- Meta file v2 Arrow schema (single record batch, one row per node): `kind` UInt8 (0=branch, 1=leaf); `child0..child3` UInt32 nullable (branch rows); `page` UInt32 nullable (leaf rows); then the existing 12 page-stat columns (`offset,len,rows` UInt64, `min_iid,max_iid` FixedSizeBinary(16), six `Timestamp(µs,"UTC")`, `has_erase` Boolean) — **nullable, populated on leaf rows only**. `pages` is reconstructed from leaf rows in node order; leaf `page` indices are assigned in that same order, so decode is self-consistent. Flat columns instead of XTDB's union/list: denser in arrow-rs and one fewer nested reader.
- Later tasks rely on: `encode_rows` (Task 10 outputs), `BlockMeta::{pages_for_iid, pages_for_part, pages}` (Tasks 10/12), `EncodedBlock.nodes`.

- [ ] **Step 1: Write the failing tests**

Append to `crates/varve-index/src/block.rs` tests (helpers `event(...)`/fixtures exist in that module — reuse them; sketch assumes a `put_event(iid, system_from)` helper making a minimal node Put):

```rust
#[test]
fn encode_rows_builds_a_page_tree_and_round_trips() {
    // 12 events across 3 top-level buckets, page_rows=4 forces one split
    let mut rows = Vec::new();
    for bucket in [0u8, 1, 3] {
        for n in 0..4u8 {
            let mut b = [0u8; 16];
            b[0] = bucket << 6;
            b[15] = n;
            rows.push(put_event(Iid::from_bytes(b), n as i64));
        }
    }
    let enc = encode_rows(rows.clone(), 4, SortOrder::ByIid).unwrap();
    let meta = decode_meta(&enc.meta).unwrap();
    assert_eq!(meta, BlockMeta { nodes: enc.nodes.clone(), pages: enc.pages.clone() });
    // root is a branch with children at buckets 0,1,3
    let MetaNode::Branch { children } = meta.nodes[meta.root().unwrap() as usize] else {
        panic!("root must be a branch")
    };
    assert!(children[0].is_some() && children[1].is_some() && children[3].is_some());
    assert!(children[2].is_none());
    assert_eq!(meta.pages.len(), 3);
    // every page decodes back to its 4 events
    let total: usize = meta
        .pages
        .iter()
        .map(|p| decode_events(&enc.data[p.offset as usize..(p.offset + p.len) as usize]).unwrap().len())
        .sum();
    assert_eq!(total, 12);
}

#[test]
fn single_page_block_is_a_lone_leaf() {
    let rows = vec![put_event(Iid::from_bytes([9u8; 16]), 1)];
    let enc = encode_rows(rows, 1024, SortOrder::ByIid).unwrap();
    assert_eq!(enc.nodes, vec![MetaNode::Leaf { page: 0 }]);
}

#[test]
fn empty_rows_encode_to_empty_block() {
    let enc = encode_rows(Vec::new(), 1024, SortOrder::ByIid).unwrap();
    assert!(enc.pages.is_empty() && enc.nodes.is_empty() && enc.data.is_empty());
    let meta = decode_meta(&enc.meta).unwrap();
    assert_eq!(meta.root(), None);
}

#[test]
fn pages_for_iid_descends_to_exactly_the_covering_page() {
    let mut rows = Vec::new();
    for first in 0u8..8 {
        for n in 0..4u8 {
            let mut b = [0u8; 16];
            b[0] = first << 5; // buckets 0..=3 hit twice each
            b[15] = n;
            rows.push(put_event(Iid::from_bytes(b), n as i64));
        }
    }
    let enc = encode_rows(rows.clone(), 4, SortOrder::ByIid).unwrap();
    let meta = decode_meta(&enc.meta).unwrap();
    for e in &rows {
        let hit = meta.pages_for_iid(&e.iid);
        assert_eq!(hit.len(), 1, "one covering page per iid");
        let p = &meta.pages[hit[0] as usize];
        let evs = decode_events(&enc.data[p.offset as usize..(p.offset + p.len) as usize]).unwrap();
        assert!(evs.iter().any(|x| x.iid == e.iid));
    }
    // an IID in an empty bucket range finds nothing only if no page covers it
    // (a lone root leaf covers everything, so use the split tree from above)
}

#[test]
fn pages_for_part_covers_shallow_leaves_and_deep_parts() {
    let rows = vec![put_event(Iid::from_bytes([0u8; 16]), 1)];
    let enc = encode_rows(rows, 1024, SortOrder::ByIid).unwrap();
    let meta = decode_meta(&enc.meta).unwrap();
    // tree is one leaf at the root; any part is covered by it
    assert_eq!(meta.pages_for_part(&[2, 1]), vec![0]);
    assert_eq!(meta.pages_for_part(&[]), vec![0]);
}

#[test]
fn encode_rows_is_byte_deterministic() {
    let rows: Vec<Event> =
        (0..100u8).map(|n| put_event(Iid::from_bytes([n; 16]), n as i64)).collect();
    let a = encode_rows(rows.clone(), 8, SortOrder::ByIid).unwrap();
    let b = encode_rows(rows, 8, SortOrder::ByIid).unwrap();
    assert_eq!(a.data, b.data);
    assert_eq!(a.meta, b.meta);
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p varve-index block::`
Expected: compile errors — `encode_rows`, `MetaNode`, `BlockMeta` not defined.

- [ ] **Step 3: Implement**

In `crates/varve-index/src/block.rs`:

1. Add `MetaNode`, `BlockMeta` + methods:

```rust
impl BlockMeta {
    pub fn root(&self) -> Option<u32> {
        self.nodes.len().checked_sub(1).map(|i| i as u32)
    }

    pub fn pages_for_iid(&self, iid: &Iid) -> Vec<u32> {
        let Some(mut at) = self.root() else { return Vec::new() };
        let mut depth = 0;
        loop {
            match self.nodes[at as usize] {
                MetaNode::Leaf { page } => return vec![page],
                MetaNode::Branch { children } => {
                    match children[crate::trie::bucket_for(iid, depth) as usize] {
                        Some(child) => {
                            at = child;
                            depth += 1;
                        }
                        None => return Vec::new(),
                    }
                }
            }
        }
    }

    pub fn pages_for_part(&self, part: &[u8]) -> Vec<u32> {
        fn collect(nodes: &[MetaNode], at: u32, out: &mut Vec<u32>) {
            match nodes[at as usize] {
                MetaNode::Leaf { page } => out.push(page),
                MetaNode::Branch { children } => {
                    for child in children.iter().flatten() {
                        collect(nodes, *child, out);
                    }
                }
            }
        }
        let Some(mut at) = self.root() else { return Vec::new() };
        for digit in part {
            match self.nodes[at as usize] {
                MetaNode::Leaf { page } => return vec![page], // shallow leaf covers the part
                MetaNode::Branch { children } => match children[*digit as usize] {
                    Some(child) => at = child,
                    None => return Vec::new(),
                },
            }
        }
        let mut out = Vec::new();
        collect(&self.nodes, at, &mut out);
        out
    }
}
```

2. `encode_rows`: take the existing `encode_block_by` body from the point where it has the full sorted row vec (keep the sort + endpoint validation), then replace sequential chunking with the recursive partition:

```rust
fn build_tree(
    rows: &[Event],
    sort_key: &dyn Fn(&Event) -> Iid,
    range: std::ops::Range<usize>,
    depth: usize,
    page_rows: usize,
    pages_out: &mut Vec<std::ops::Range<usize>>,
    nodes_out: &mut Vec<MetaNode>,
) -> u32 {
    let slice = &rows[range.clone()];
    let solo = slice.iter().all(|e| sort_key(e) == sort_key(&slice[0]));
    if slice.len() <= page_rows || depth >= crate::trie::MAX_DEPTH || solo {
        pages_out.push(range);
        nodes_out.push(MetaNode::Leaf { page: (pages_out.len() - 1) as u32 });
        return (nodes_out.len() - 1) as u32;
    }
    let mut children: [Option<u32>; 4] = [None; 4];
    let mut start = range.start;
    for bucket in 0u8..4 {
        let end = start
            + rows[start..range.end]
                .iter()
                .take_while(|e| crate::trie::bucket_for(&sort_key(e), depth) == bucket)
                .count();
        if end > start {
            children[bucket as usize] =
                Some(build_tree(rows, sort_key, start..end, depth + 1, page_rows, pages_out, nodes_out));
        }
        start = end;
    }
    nodes_out.push(MetaNode::Branch { children });
    (nodes_out.len() - 1) as u32
}
```

Then per page range: `encode_events(&rows[range])` appended to `data`, `PageMeta` computed exactly as today (offset/len/rows/min-max stats — reuse the existing per-page stats code). Root call only when `!rows.is_empty()`.

3. Meta v2 writer/reader: extend the existing meta schema builders with the three node columns (`kind` UInt8 non-null, `child0..child3` UInt32 nullable, `page` UInt32 nullable) and make the 12 stat columns nullable (null on branch rows). `decode_meta` reads nodes + reassembles `pages` from leaf rows (leaf row order == page order — assert monotonic `page` indices, else `IndexError::Codec`).

4. `encode_block_by(live, page_rows, order)` → collect `Vec<Event>` from `live.entities()` (flatten, arrival order) and delegate to `encode_rows`. Delete the now-dead chunking code.

5. Engine: `PersistedTrie { pub entry: TrieEntry, pub meta: Arc<BlockMeta> }`; `merged_snapshot` page loop becomes:

```rust
let candidate: Vec<u32> = match iid_point {
    Some(ref iid) => trie.meta.pages_for_iid(iid),
    None => (0..trie.meta.pages.len() as u32).collect(),
};
for idx in candidate {
    let page = &trie.meta.pages[idx as usize];
    if !page.selected(bounds, iid_point.as_ref()) { continue; }
    /* existing ranged GET + decode */
}
```
Adjacency scans (`edge_adjacency_impl`, `reachable_edges`) get the same treatment with the anchor as the point key. Recovery (`db.rs`) stores `Arc::new(decode_meta(&bytes)?)`.

- [ ] **Step 4: Run the workspace suite**

Run: `cargo test --workspace && PROPTEST_CASES=2000 cargo test -p varve-testkit --release --test flush_equivalence`
Expected: PASS — flush-equivalence is the no-behavior-change proof for the new page boundaries.

- [ ] **Step 5: Commit**

```bash
git add crates/varve-index crates/varve-engine
git commit -m "feat: trie-structured block meta (page tree, path pruning) + pure encode_rows"
```

---

### Task 6: Manifest v2 — persisted `TrieState` + `garbage_as_of_us`

**Files:**
- Modify: `crates/varve-storage/src/manifest.rs`
- Modify: `crates/varve-storage/src/lib.rs` (re-export `TrieState`)
- Modify: `crates/varve-engine/src/db.rs` (recovery: non-Live entries don't join the scan inventory)
- Modify: `crates/varve-engine/src/flush.rs` (new `TrieEntry` construction sites gain the two fields)
- Test: in-module in `manifest.rs`

**Interfaces:**
- Produces:
  ```rust
  #[derive(Clone, Copy, Debug, PartialEq, Eq, ::prost::Enumeration)]
  #[repr(i32)]
  pub enum TrieState { Live = 0, Nascent = 1, Garbage = 2 }

  #[derive(Clone, PartialEq, ::prost::Message)]
  pub struct TrieEntry {
      #[prost(string, tag = "1")] pub trie_key: String,
      #[prost(uint64, tag = "2")] pub row_count: u64,
      #[prost(uint64, tag = "3")] pub data_len: u64,
      #[prost(enumeration = "TrieState", tag = "4")] pub state: i32,
      #[prost(int64, tag = "5")] pub garbage_as_of_us: i64,   // meaningful only when state == Garbage
  }
  ```
  (prost's `#[prost(enumeration)]` generates `pub fn state(&self) -> TrieState` returning `Live` for out-of-range values, and `pub fn set_state(&mut self, TrieState)` — use the accessor everywhere.)
- `BlockManifest`/`TableTries`/`latest_manifest` unchanged. Proto3 semantics: `state = 0` (Live) and `garbage_as_of_us = 0` encode to zero bytes, so **every pre-slice-8 manifest still decodes as all-Live** — and the slice-4 golden wire test still passes unchanged. Add new golden coverage for non-default state.
- Later tasks rely on: `TrieState`, the two new fields, `entry.state()`.

- [ ] **Step 1: Write the failing tests**

Append to `crates/varve-storage/src/manifest.rs` tests:

```rust
#[test]
fn trie_entry_state_round_trips_and_defaults_live() {
    let mut e = TrieEntry {
        trie_key: "l01-rc-b00".into(),
        row_count: 5,
        data_len: 100,
        state: 0,
        garbage_as_of_us: 0,
    };
    assert_eq!(e.state(), TrieState::Live);
    e.set_state(TrieState::Garbage);
    e.garbage_as_of_us = 1_234;
    let m = BlockManifest {
        block_id: 1,
        watermark: 0,
        max_tx_id: 0,
        max_system_time_us: 0,
        tables: vec![TableTries { graph: "g".into(), table: "t".into(), tries: vec![e], family: String::new() }],
    };
    let back = BlockManifest::from_wire(&m.to_wire()).unwrap();
    assert_eq!(back.tables[0].tries[0].state(), TrieState::Garbage);
    assert_eq!(back.tables[0].tries[0].garbage_as_of_us, 1_234);
}

#[test]
fn default_state_encodes_to_zero_extra_bytes() {
    // proto3: default-valued fields are absent on the wire — slice-4 goldens hold
    let e = TrieEntry { trie_key: "k".into(), row_count: 1, data_len: 2, state: 0, garbage_as_of_us: 0 };
    let bytes = prost::Message::encode_to_vec(&e);
    // string field: tag(1) + len(1) + "k"(1); two varint fields: tag(1) + value(1) each.
    // Tags 4/5 at their defaults must contribute nothing.
    assert_eq!(bytes.len(), 3 + 2 + 2);
}
```

(The second test's length arithmetic pins "no bytes added"; the pre-existing `wire_golden_bytes` test passing untouched is the real guard — do NOT regenerate it.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p varve-storage manifest`
Expected: compile error — `TrieEntry` has no field `state`.

- [ ] **Step 3: Implement**

Add the enum + two fields per the Interfaces block. Fix construction sites: `flush.rs` builds `TrieEntry { .., state: TrieState::Live as i32, garbage_as_of_us: 0 }` for L0 flushes (L0 is born live, D3). Recovery in `db.rs`: when routing `TableTries` entries into the four inventories, skip entries where `entry.state() != TrieState::Live` for the **scan** inventory (they still exist in the manifest; Task 12 gives them a catalog home). Metas for `Nascent` entries are not fetched yet — Task 12 owns that.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p varve-storage && cargo test -p varve-engine && cargo test -p varve`
Expected: PASS, including the untouched `wire_golden_bytes`.

- [ ] **Step 5: Commit**

```bash
git add crates/varve-storage crates/varve-engine
git commit -m "feat: manifest TrieEntry carries TrieState + garbage_as_of_us"
```

---

### Task 7: `varve-compact` crate + `FamilyCatalog` state machine

**Files:**
- Create: `crates/varve-compact/Cargo.toml`, `crates/varve-compact/src/lib.rs`, `crates/varve-compact/src/catalog.rs`
- Modify: root `Cargo.toml` only if `crates/*` globbing doesn't pick the new member up automatically (it does — verify with `cargo metadata`)
- Test: in-module `#[cfg(test)]` in `catalog.rs`

**Interfaces:**
- Consumes: `varve_storage::{TrieKey, Recency, TrieEntry, TrieState}`.
- Produces (crate root re-exports everything below):
  ```rust
  // lib.rs
  pub mod catalog;
  pub mod jobs;      // Task 8
  pub mod recency;   // Task 9
  pub mod merge;     // Task 10
  pub mod gc;        // Task 15

  #[derive(thiserror::Error, Debug)]
  pub enum CompactError {
      #[error("bad trie key: {0}")] BadKey(String),
      #[error("index: {0}")] Index(#[from] varve_index::IndexError),
  }

  // catalog.rs
  pub const FILE_SIZE_TARGET: u64 = 104_857_600; // 100 MiB, XTDB *file-size-target*

  #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
  pub struct Shard { pub level: u64, pub recency: Recency, pub part: Vec<u8> }

  #[derive(Debug, Clone, PartialEq)]
  pub struct TrieHandle {
      pub key: TrieKey,
      pub row_count: u64,
      pub data_len: u64,
      pub state: TrieState,
      pub garbage_as_of_us: i64,
  }

  #[derive(Debug, Clone, Default, PartialEq)]
  pub struct ShardState {
      pub live: Vec<TrieHandle>,     // sorted block DESC (newest first), like XTDB
      pub nascent: Vec<TrieHandle>,  // sorted block DESC
      pub garbage: Vec<TrieHandle>,  // sorted block DESC
      pub max_block: Option<u64>,
  }

  #[derive(Debug, Clone, Default, PartialEq)]
  pub struct FamilyCatalog { /* shards: BTreeMap<Shard, ShardState>, l1h_recencies: BTreeMap<u64, BTreeSet<NaiveDate>> */ }

  impl FamilyCatalog {
      pub fn from_entries<'a>(entries: impl IntoIterator<Item = &'a TrieEntry>)
          -> Result<FamilyCatalog, CompactError>;                    // pure fold; states taken from entries
      pub fn add_trie(&mut self, key: TrieKey, row_count: u64, data_len: u64,
                      as_of_us: i64, file_size_target: u64);         // trie-cat.allium insertion rules
      pub fn shard(&self, shard: &Shard) -> Option<&ShardState>;
      pub fn shards(&self) -> impl Iterator<Item = (&Shard, &ShardState)>;
      pub fn live_tries(&self) -> Vec<TrieHandle>;                   // (block asc, level desc, recency, part)
      pub fn garbage_before(&self, cutoff_us: i64) -> Vec<TrieHandle>;
      pub fn drop_garbage(&mut self, keys: &[TrieKey]);              // after GC physical delete
      pub fn to_entries(&self) -> Vec<TrieEntry>;                    // deterministic; inverse of from_entries
  }
  ```
- `crates/varve-compact/Cargo.toml`:
  ```toml
  [package]
  name = "varve-compact"
  version.workspace = true
  edition.workspace = true
  license.workspace = true

  [dependencies]
  varve-types = { path = "../varve-types" }
  varve-index = { path = "../varve-index" }
  varve-storage = { path = "../varve-storage", default-features = false }
  thiserror = { workspace = true }
  chrono = { workspace = true }
  bytes = { workspace = true }

  [dev-dependencies]
  proptest = { workspace = true }

  [lints]
  workspace = true
  ```
  (`default-features = false` keeps the s3 stack out of the pure crate; cargo unifies features workspace-wide anyway.)
- **Insertion rules implemented by `add_trie` (D3, exact):**
  1. Stale guard: if `key.block <= shard(key).max_block` → return unchanged (idempotent re-delivery).
  2. `level == 0` → push Live.
  3. `level == 1 && recency == Week(d)` (L1H) → Live iff the `[1, Current, []]` shard has a live trie with `block >= key.block`, else Nascent; record `l1h_recencies[key.block] += d`.
  4. `level == 1 && recency == Current` (L1C) → (a) for every `d` in `l1h_recencies[key.block]`: promote the Nascent L1H with that block in `[1, Week(d), []]` to Live, then clear the record; (b) supersede-partial in `[1, Current, []]`: every Live trie with `data_len < file_size_target && block <= key.block` → Garbage (stamp `garbage_as_of_us = as_of_us`); (c) push self Live; (d) supersede-by-block in `[0, Current, []]`: every Live L0 with `block <= key.block` → Garbage (D4 deviation: XTDB drops these; we mark Garbage).
  5. `level == 2 && recency == Week(_)` (L2H) → supersede-partial in own shard, push Live, supersede-by-block in `[1, recency, []]`.
  6. Everything else (L≥2 current, L≥3 historical) → push Nascent; if all `BRANCH_FACTOR` sibling shards `[level, recency, parent_part ++ [p]]` have `max_block >= key.block`, promote the block-matching Nascent trie in **each** sibling to Live and supersede-by-block the parent shard `[level-1, recency, parent_part]` (where `parent_part = part[..part.len()-1]`).
  7. Every push updates `max_block`; Vec ordering (block DESC) is maintained by inserting at the front (the stale guard guarantees monotonicity).
- `from_entries` parses each `TrieEntry` (`TrieKey::parse` else `CompactError::BadKey`) and places it directly in its shard's state list **without** running insertion rules (states are persisted truth); rebuilds `l1h_recencies` from Nascent L1H entries; recomputes `max_block` per shard over all three lists.
- `to_entries` walks shards in `BTreeMap` order, within a shard emits garbage, then nascent, then live, each block ASC (fully deterministic; exact order also pinned by round-trip test).
- Later tasks rely on: every name above, especially `live_tries` ordering and `garbage_before`.

- [ ] **Step 1: Write the failing tests**

`crates/varve-compact/src/catalog.rs` tests module (helper `h(key_str, len)` builds a `TrieHandle`-shaped `add_trie` call):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use varve_storage::{Recency, TrieKey, TrieState};

    const TGT: u64 = 100;

    fn add(cat: &mut FamilyCatalog, key: &str, data_len: u64, as_of: i64) {
        cat.add_trie(TrieKey::parse(key).unwrap(), 1, data_len, as_of, TGT);
    }

    fn states(cat: &FamilyCatalog, shard_key: &str) -> (Vec<String>, Vec<String>, Vec<String>) {
        let k = TrieKey::parse(shard_key).unwrap();
        let s = cat
            .shard(&Shard { level: k.level, recency: k.recency, part: k.part.clone() })
            .cloned()
            .unwrap_or_default();
        let names = |v: &[TrieHandle]| v.iter().map(|t| t.key.to_string()).collect();
        (names(&s.live), names(&s.nascent), names(&s.garbage))
    }

    #[test]
    fn l0_is_live_immediately_and_stale_messages_are_dropped() {
        let mut cat = FamilyCatalog::default();
        add(&mut cat, "l00-rc-b00", 10, 1);
        add(&mut cat, "l00-rc-b01", 10, 2);
        add(&mut cat, "l00-rc-b00", 10, 3); // stale: block <= max_block
        let (live, nascent, garbage) = states(&cat, "l00-rc-b00");
        assert_eq!(live, vec!["l00-rc-b01", "l00-rc-b00"]); // block desc
        assert!(nascent.is_empty() && garbage.is_empty());
    }

    #[test]
    fn l1c_supersedes_l0_and_partial_l1c_and_promotes_l1h() {
        let mut cat = FamilyCatalog::default();
        add(&mut cat, "l00-rc-b00", 10, 1);
        add(&mut cat, "l00-rc-b01", 10, 2);
        // L1H arrives first: nascent until its L1C lands
        add(&mut cat, "l01-r20260706-b01", 10, 3);
        let (_, nascent, _) = states(&cat, "l01-r20260706-b01");
        assert_eq!(nascent, vec!["l01-r20260706-b01"]);
        // partial L1C at b00, then L1C at b01 supersedes it
        add(&mut cat, "l01-rc-b00", 50, 4); // 50 < TGT => partial
        add(&mut cat, "l01-rc-b01", 60, 5);
        let (live, _, garbage) = states(&cat, "l01-rc-b00");
        assert_eq!(live, vec!["l01-rc-b01"]);
        assert_eq!(garbage, vec!["l01-rc-b00"]);
        // both L0s superseded
        let (l0_live, _, l0_garbage) = states(&cat, "l00-rc-b00");
        assert!(l0_live.is_empty());
        assert_eq!(l0_garbage, vec!["l00-rc-b01", "l00-rc-b00"]);
        // L1H promoted live by its covering L1C
        let (l1h_live, l1h_nascent, _) = states(&cat, "l01-r20260706-b01");
        assert_eq!(l1h_live, vec!["l01-r20260706-b01"]);
        assert!(l1h_nascent.is_empty());
    }

    #[test]
    fn full_l1c_survives_levelled_supersession() {
        let mut cat = FamilyCatalog::default();
        add(&mut cat, "l01-rc-b00", 200, 1); // >= TGT => full
        add(&mut cat, "l01-rc-b01", 60, 2);
        let (live, _, garbage) = states(&cat, "l01-rc-b00");
        assert_eq!(live, vec!["l01-rc-b01", "l01-rc-b00"]);
        assert!(garbage.is_empty());
    }

    #[test]
    fn part_group_goes_live_only_when_all_four_siblings_exist() {
        let mut cat = FamilyCatalog::default();
        for b in ["l01-rc-b00", "l01-rc-b01", "l01-rc-b02", "l01-rc-b03"] {
            add(&mut cat, b, 200, 1); // four full L1Cs
        }
        for p in 0..3 {
            add(&mut cat, &format!("l02-rc-p{p}-b03"), 80, 2);
            let (live, nascent, _) = states(&cat, &format!("l02-rc-p{p}-b03"));
            assert!(live.is_empty(), "p{p} must stay nascent");
            assert_eq!(nascent.len(), 1);
        }
        add(&mut cat, "l02-rc-p3-b03", 80, 3); // completes the group
        for p in 0..4 {
            let (live, nascent, _) = states(&cat, &format!("l02-rc-p{p}-b03"));
            assert_eq!(live, vec![format!("l02-rc-p{p}-b03")]);
            assert!(nascent.is_empty());
        }
        // parent L1C shard fully superseded
        let (l1c_live, _, l1c_garbage) = states(&cat, "l01-rc-b00");
        assert!(l1c_live.is_empty());
        assert_eq!(l1c_garbage.len(), 4);
    }

    #[test]
    fn l2h_supersedes_l1h_inputs() {
        let mut cat = FamilyCatalog::default();
        add(&mut cat, "l01-r20260706-b00", 40, 1);
        add(&mut cat, "l01-rc-b00", 10, 2); // promotes the L1H
        add(&mut cat, "l02-r20260706-b00", 90, 3);
        let (live, _, _) = states(&cat, "l02-r20260706-b00");
        assert_eq!(live, vec!["l02-r20260706-b00"]);
        let (l1h_live, _, l1h_garbage) = states(&cat, "l01-r20260706-b00");
        assert!(l1h_live.is_empty());
        assert_eq!(l1h_garbage, vec!["l01-r20260706-b00"]);
    }

    #[test]
    fn garbage_before_respects_cutoff_and_drop_garbage_removes() {
        let mut cat = FamilyCatalog::default();
        add(&mut cat, "l00-rc-b00", 10, 1);
        add(&mut cat, "l01-rc-b00", 10, 500); // supersedes the L0 at as_of 500
        assert!(cat.garbage_before(499).is_empty());
        let g = cat.garbage_before(500);
        assert_eq!(g.len(), 1);
        assert_eq!(g[0].key.to_string(), "l00-rc-b00");
        cat.drop_garbage(&[g[0].key.clone()]);
        assert!(cat.garbage_before(i64::MAX).is_empty());
    }

    #[test]
    fn entries_round_trip_through_manifest_form() {
        let mut cat = FamilyCatalog::default();
        add(&mut cat, "l00-rc-b00", 10, 1);
        add(&mut cat, "l00-rc-b01", 10, 2);
        add(&mut cat, "l01-r20260706-b01", 10, 3); // nascent
        add(&mut cat, "l01-rc-b00", 50, 4);
        let entries = cat.to_entries();
        let back = FamilyCatalog::from_entries(entries.iter()).unwrap();
        assert_eq!(back, cat);
        // and every persisted state matches the in-memory one
        assert!(entries.iter().any(|e| e.state() == TrieState::Nascent));
        assert!(entries.iter().any(|e| e.state() == TrieState::Garbage));
    }

    #[test]
    fn live_tries_order_is_block_asc_then_level_desc() {
        let mut cat = FamilyCatalog::default();
        add(&mut cat, "l00-rc-b00", 10, 1);
        add(&mut cat, "l01-rc-b00", 10, 2); // supersedes the L0, same block
        add(&mut cat, "l00-rc-b01", 10, 3);
        let order: Vec<String> = cat.live_tries().iter().map(|t| t.key.to_string()).collect();
        assert_eq!(order, vec!["l01-rc-b00", "l00-rc-b01"]);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p varve-compact`
Expected: crate doesn't exist / compile errors.

- [ ] **Step 3: Implement**

Create the crate per the Interfaces block. Implementation notes beyond the rules spelled out above:

- `Shard::for_key(key: &TrieKey) -> Shard` private helper (`level`, `recency`, `part` copied).
- Supersession helpers as private methods mirroring trie-cat.allium:
  ```rust
  fn supersede_partial(&mut self, shard: &Shard, block: u64, as_of_us: i64, target: u64)
  fn supersede_by_block(&mut self, shard: &Shard, block: u64, as_of_us: i64)
  fn promote_nascent(&mut self, shard: &Shard, block: u64)   // mark_block_index_live
  ```
  Both supersessions move matching handles from `live` to `garbage` (set `state = TrieState::Garbage`, stamp `garbage_as_of_us`), preserving block-DESC order in `garbage`.
- Part-group completion (rule 6): `parent_part = &key.part[..key.part.len()-1]` (a general-case trie always has a non-empty part — a defensive early-return if empty, it cannot be produced by the job calculator).
- `live_tries`: collect all live handles, sort by `(key.block, std::cmp::Reverse(key.level), key.recency, key.part.clone())`.
- `garbage_before(cutoff)`: all garbage handles with `garbage_as_of_us <= cutoff`, sorted by key string for determinism.
- `to_entries`: `TrieEntry { trie_key: handle.key.to_string(), row_count, data_len, state: handle.state as i32, garbage_as_of_us }`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p varve-compact && cargo clippy -p varve-compact --all-targets -- -D warnings`
Expected: PASS (8 tests).

- [ ] **Step 5: Commit**

```bash
git add crates/varve-compact Cargo.lock
git commit -m "feat: varve-compact crate with FamilyCatalog trie state machine"
```

---

### Task 8: Job calculator

**Files:**
- Create: `crates/varve-compact/src/jobs.rs`
- Modify: `crates/varve-compact/src/lib.rs` (`pub mod jobs;`)
- Test: in-module `#[cfg(test)]`

**Interfaces:**
- Consumes: Task 7 catalog types.
- Produces:
  ```rust
  #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
  pub struct FamilyRef { pub graph: String, pub table: String, pub family: String } // "" = primary

  #[derive(Debug, Clone, PartialEq)]
  pub struct CompactionJob {
      pub family: FamilyRef,
      pub inputs: Vec<TrieKey>,          // oldest-first; a partial higher-level input comes FIRST
      pub out: TrieKey,                  // job identity (D5)
      pub partitioned_by_recency: bool,  // true only for L0→L1
  }

  #[derive(Debug, Clone, Copy)]
  pub struct CompactionConfig { pub file_size_target: u64, pub page_rows: usize }
  impl Default for CompactionConfig {
      // file_size_target: FILE_SIZE_TARGET (100 MiB), page_rows: 1024
  }

  pub fn available_jobs(family: &FamilyRef, cat: &FamilyCatalog, cfg: &CompactionConfig)
      -> Vec<CompactionJob>;             // deterministic order: generator (a), then (b) by shard, then (c) by shard/part
  ```
- **Generator rules (D6, exact):**
  - **(a) L0→L1:** if `[0, Current, []]` has live tries: take the OLDEST (smallest block) live L0; optionally prepend the newest live L1C in `[1, Current, []]` with `data_len < file_size_target`; `out = TrieKey { level: 1, recency: Current, part: [], block: l0.block }`, `partitioned_by_recency: true`. At most ONE such job per family per round.
  - **(b) L1H→L2H:** per `[1, Week(d), []]` shard with live tries: candidates = live L1H oldest-first, prepended by the newest partial (`< target`) live L2H in `[2, Week(d), []]` if any; accumulate until `sum(data_len) >= target` or `len == 4`; emit only if `>= 2` inputs or size trigger hit (a single L1H with nothing to merge into is not a job); `out = TrieKey { level: 2, recency: Week(d), part: [], block: last_input.block }`, `partitioned_by_recency: false`.
  - **(c) Tiering Ln→L(n+1):** for every shard `[level, recency, part]` with `level >= 1`, skipping `[0,..]` and `[1, Week(_), []]` (owned by (a)/(b)): candidate live files = (for `level == 1 && recency == Current`: only FULL files, `data_len >= target`; else all live) sorted block ASC, dropping files with `block <= child_max_block` where `child_max_block = max over p of shard [level+1, recency, part ++ [p]].max_block`; if ≥ 4 remain, take the first 4 and emit 4 jobs, one per `p ∈ 0..=3`: `out = TrieKey { level: level+1, recency, part: part ++ [p], block: inputs[3].block }`, `partitioned_by_recency: false`.
- Later tasks rely on: `CompactionJob`, `FamilyRef`, `CompactionConfig`, `available_jobs`.

- [ ] **Step 1: Write the failing tests**

`crates/varve-compact/src/jobs.rs` tests:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::FamilyCatalog;
    use varve_storage::TrieKey;

    const TGT: u64 = 100;

    fn cfg() -> CompactionConfig {
        CompactionConfig { file_size_target: TGT, page_rows: 4 }
    }

    fn fam() -> FamilyRef {
        FamilyRef { graph: "g".into(), table: "t".into(), family: String::new() }
    }

    fn add(cat: &mut FamilyCatalog, key: &str, len: u64) {
        cat.add_trie(TrieKey::parse(key).unwrap(), 1, len, 0, TGT);
    }

    fn keys(job: &CompactionJob) -> (Vec<String>, String) {
        (job.inputs.iter().map(|k| k.to_string()).collect(), job.out.to_string())
    }

    #[test]
    fn oldest_l0_plus_partial_l1c_makes_the_l0_job() {
        let mut cat = FamilyCatalog::default();
        add(&mut cat, "l00-rc-b00", 10);
        add(&mut cat, "l00-rc-b01", 10);
        let jobs = available_jobs(&fam(), &cat, &cfg());
        assert_eq!(jobs.len(), 1);
        let (inputs, out) = keys(&jobs[0]);
        assert_eq!(inputs, vec!["l00-rc-b00"]); // oldest only
        assert_eq!(out, "l01-rc-b00");
        assert!(jobs[0].partitioned_by_recency);

        // a partial L1C joins the next round's job
        add(&mut cat, "l01-rc-b00", 50);
        let jobs = available_jobs(&fam(), &cat, &cfg());
        let (inputs, out) = keys(&jobs[0]);
        assert_eq!(inputs, vec!["l01-rc-b00", "l00-rc-b01"]); // partial L1C first, then oldest L0
        assert_eq!(out, "l01-rc-b01");
    }

    #[test]
    fn no_l0_jobs_without_live_l0() {
        let mut cat = FamilyCatalog::default();
        add(&mut cat, "l01-rc-b00", 200);
        assert!(available_jobs(&fam(), &cat, &cfg()).is_empty());
    }

    #[test]
    fn l1h_shard_accumulates_to_l2h() {
        let mut cat = FamilyCatalog::default();
        // four live L1H in one week shard (promoted by L1Cs)
        for b in 0..4 {
            add(&mut cat, &format!("l01-r20260706-b0{b}"), 10);
            add(&mut cat, &format!("l01-rc-b0{b}"), 0); // empty L1C promotes it
        }
        let jobs = available_jobs(&fam(), &cat, &cfg());
        let l2h: Vec<_> = jobs.iter().filter(|j| j.out.level == 2).collect();
        assert_eq!(l2h.len(), 1);
        let (inputs, out) = keys(l2h[0]);
        assert_eq!(inputs, vec!["l01-r20260706-b00", "l01-r20260706-b01",
                                "l01-r20260706-b02", "l01-r20260706-b03"]);
        assert_eq!(out, "l02-r20260706-b03");
        assert!(!l2h[0].partitioned_by_recency);
    }

    #[test]
    fn single_small_l1h_is_not_a_job() {
        let mut cat = FamilyCatalog::default();
        add(&mut cat, "l01-r20260706-b00", 10);
        add(&mut cat, "l01-rc-b00", 0);
        let jobs = available_jobs(&fam(), &cat, &cfg());
        assert!(jobs.iter().all(|j| j.out.level != 2));
    }

    #[test]
    fn four_full_l1c_tier_into_four_partition_jobs() {
        let mut cat = FamilyCatalog::default();
        for b in 0..4 {
            add(&mut cat, &format!("l01-rc-b0{b}"), 200); // full
        }
        let jobs = available_jobs(&fam(), &cat, &cfg());
        let tier: Vec<_> = jobs.iter().filter(|j| j.out.level == 2).collect();
        assert_eq!(tier.len(), 4);
        for (p, job) in tier.iter().enumerate() {
            let (inputs, out) = keys(job);
            assert_eq!(inputs.len(), 4);
            assert_eq!(out, format!("l02-rc-p{p}-b03"));
        }
    }

    #[test]
    fn partial_l1c_does_not_tier_and_covered_inputs_are_skipped() {
        let mut cat = FamilyCatalog::default();
        for b in 0..4 {
            add(&mut cat, &format!("l01-rc-b0{b}"), 200);
        }
        add(&mut cat, "l01-rc-b04", 50); // partial — must not count toward tiering
        // child shard already covering b03 (simulate completed group)
        for p in 0..4 {
            add(&mut cat, &format!("l02-rc-p{p}-b03"), 80);
        }
        let jobs = available_jobs(&fam(), &cat, &cfg());
        assert!(jobs.iter().all(|j| !(j.out.level == 2 && j.out.recency == varve_storage::Recency::Current)),
            "everything at L1C is either partial or already covered: {jobs:?}");
    }

    #[test]
    fn job_selection_is_deterministic() {
        let build = || {
            let mut cat = FamilyCatalog::default();
            add(&mut cat, "l00-rc-b02", 10);
            add(&mut cat, "l00-rc-b03", 10);
            add(&mut cat, "l01-rc-b01", 30);
            for b in 0..2 {
                add(&mut cat, &format!("l01-r20260629-b0{b}"), 60);
                add(&mut cat, &format!("l01-r20260706-b0{b}"), 60);
            }
            cat
        };
        assert_eq!(available_jobs(&fam(), &build(), &cfg()),
                   available_jobs(&fam(), &build(), &cfg()));
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p varve-compact jobs`
Expected: compile error — module `jobs` missing.

- [ ] **Step 3: Implement**

`available_jobs` iterates `cat.shards()` (BTreeMap order = deterministic) applying the three generators exactly as specified in the Interfaces block. Keep each generator a private function (`l0_job`, `l1h_jobs`, `tiering_jobs`) returning `Option<CompactionJob>`/`Vec<CompactionJob>`; concatenate `(a) ++ (b) ++ (c)`.

Implementation details worth pinning:
- Oldest-first = iterate a shard's `live` Vec in reverse (it's stored block DESC).
- (b)'s "size trigger or 4 inputs": collect until either condition; if a partial L2H was prepended, cap the L1H count at 3 (4 total inputs max).
- (c)'s `child_max_block`: `(0..4).filter_map(|p| cat.shard(&child(p)).and_then(|s| s.max_block)).max()`.
- Never emit a job whose `inputs` are not all currently Live (structurally guaranteed — generators read only `live`).

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p varve-compact`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/varve-compact
git commit -m "feat: deterministic compaction job calculator (L0 recency split, L1H accumulate, 4-way tiering)"
```

---

### Task 9: `CompactionResolver` + `week_bucket`

**Files:**
- Modify: `crates/varve-index/src/bitemporal.rs` (streaming resolver next to `Ceiling`/`Polygon`)
- Modify: `crates/varve-index/src/lib.rs` (re-export `CompactionResolver`, `RowFate`)
- Create: `crates/varve-compact/src/recency.rs`
- Test: in-module in both files

**Interfaces:**
- Consumes: existing `Ceiling` (`new/reset/apply_log`), `Polygon` (`calculate_for/range_count/system_to/recency` — `Polygon::recency()` at `bitemporal.rs:195` is already the XTDB port: max over ranges of `min(system_to, valid_to)`), `Event`/`Op`, `varve_storage::Recency`.
- Produces:
  ```rust
  // varve-index bitemporal.rs
  #[derive(Debug, Clone, Copy, PartialEq, Eq)]
  pub enum RowFate { Keep { recency: Instant }, Drop }

  #[derive(Default)]
  pub struct CompactionResolver { /* ceiling: Ceiling, polygon: Polygon, current: Option<Iid>, seen_erase: bool */ }

  impl CompactionResolver {
      pub fn new() -> Self;
      /// MUST be fed events in `(iid asc, system_from desc)` order.
      pub fn on_event(&mut self, e: &Event) -> RowFate;
  }

  // varve-compact recency.rs
  pub fn week_bucket(recency: Instant) -> varve_storage::Recency;
  ```
- **`on_event` rules (D7):** on IID change → reset ceiling, clear `seen_erase`. Then:
  1. `seen_erase` → `Drop` (everything older than an erase dies, later erases too).
  2. `Op::Erase` → set `seen_erase`; `Keep { recency: Instant::END_OF_TIME }` (the tombstone survives, payload-free, routed current).
  3. Else: `polygon.calculate_for(&ceiling, e.valid_from, e.valid_to)`, then `ceiling.apply_log(e.system_from, e.valid_from, e.valid_to)`. The row is **system-visible** iff any polygon range has `system_to > e.system_from`; invisible rows (zero valid extent, or fully overwritten at their own system time — same-tx supersession) → `Drop`; visible → `Keep { recency: polygon.recency() }`. Put and Delete rows are treated identically (a Delete row is history too).
- **`week_bucket` (D8):** `END_OF_TIME` → `Recency::Current`; else the first Monday-0000Z **≥** recency (XTDB `minusNanos(1).roundToNextPartition(WEEK)`, transposed to µs: compute the UTC date of `recency_us - 1`, then the next Monday strictly after that date).
- Later tasks rely on: `CompactionResolver::on_event`, `RowFate`, `week_bucket`.

- [ ] **Step 1: Write the failing tests**

`crates/varve-index/src/bitemporal.rs` tests (reuse the module's existing `event`-building helpers; sketch uses explicit construction):

```rust
fn ev(iid: u8, sysfrom: i64, valid: (i64, i64), op: Op) -> Event {
    Event {
        iid: Iid::from_bytes([iid; 16]),
        system_from: Instant::from_micros(sysfrom),
        valid_from: Instant::from_micros(valid.0),
        valid_to: if valid.1 == i64::MAX { Instant::END_OF_TIME } else { Instant::from_micros(valid.1) },
        src: None,
        dst: None,
        op,
    }
}

fn put(iid: u8, sysfrom: i64, valid: (i64, i64)) -> Event {
    ev(iid, sysfrom, valid, Op::Put { labels: vec!["T".into()], doc: Default::default() })
}

#[test]
fn superseded_put_gets_finite_recency_current_put_gets_infinity() {
    let mut r = CompactionResolver::new();
    // feed newest-first
    let newest = r.on_event(&put(1, 20, (0, i64::MAX)));
    let older = r.on_event(&put(1, 10, (0, i64::MAX)));
    assert_eq!(newest, RowFate::Keep { recency: Instant::END_OF_TIME });
    assert_eq!(older, RowFate::Keep { recency: Instant::from_micros(20) }); // superseded at sys 20
}

#[test]
fn past_valid_window_routes_historical_even_when_never_superseded() {
    let mut r = CompactionResolver::new();
    let fate = r.on_event(&put(1, 10, (0, 5)));
    assert_eq!(fate, RowFate::Keep { recency: Instant::from_micros(5) }); // min(sys ∞, valid_to 5)
}

#[test]
fn erase_keeps_one_tombstone_and_drops_all_older_rows() {
    let mut r = CompactionResolver::new();
    assert_eq!(
        r.on_event(&ev(1, 30, (i64::MIN, i64::MAX), Op::Erase)),
        RowFate::Keep { recency: Instant::END_OF_TIME }
    );
    assert_eq!(r.on_event(&put(1, 20, (0, i64::MAX))), RowFate::Drop);
    assert_eq!(r.on_event(&ev(1, 10, (i64::MIN, i64::MAX), Op::Erase)), RowFate::Drop);
    // next entity starts clean
    assert!(matches!(r.on_event(&put(2, 5, (0, i64::MAX))), RowFate::Keep { .. }));
}

#[test]
fn same_system_time_full_overwrite_drops_the_loser() {
    let mut r = CompactionResolver::new();
    // arrival order a then b at the same system time; fed newest-arrival-first
    let winner = r.on_event(&put(1, 10, (0, i64::MAX)));
    let loser = r.on_event(&put(1, 10, (0, i64::MAX)));
    assert!(matches!(winner, RowFate::Keep { .. }));
    assert_eq!(loser, RowFate::Drop); // zero system extent: system_to == system_from
}

#[test]
fn delete_rows_are_kept_as_history() {
    let mut r = CompactionResolver::new();
    let del = r.on_event(&ev(1, 20, (0, i64::MAX), Op::Delete));
    assert_eq!(del, RowFate::Keep { recency: Instant::END_OF_TIME });
    let put_below = r.on_event(&put(1, 10, (0, i64::MAX)));
    assert_eq!(put_below, RowFate::Keep { recency: Instant::from_micros(20) });
}
```

`crates/varve-compact/src/recency.rs` tests:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use varve_storage::Recency;
    use varve_types::Instant;

    fn date(y: i32, m: u32, d: u32) -> chrono::NaiveDate {
        chrono::NaiveDate::from_ymd_opt(y, m, d).unwrap()
    }

    // 2026-07-06 is a Monday. Monday 0000Z in µs since epoch:
    const MONDAY_US: i64 = 1_783_296_000_000_000; // 2026-07-06T00:00:00Z

    #[test]
    fn end_of_time_is_current() {
        assert_eq!(week_bucket(Instant::END_OF_TIME), Recency::Current);
    }

    #[test]
    fn exactly_monday_midnight_buckets_to_that_monday() {
        assert_eq!(week_bucket(Instant::from_micros(MONDAY_US)), Recency::Week(date(2026, 7, 6)));
    }

    #[test]
    fn one_micro_past_monday_rolls_to_next_week() {
        assert_eq!(week_bucket(Instant::from_micros(MONDAY_US + 1)), Recency::Week(date(2026, 7, 13)));
    }

    #[test]
    fn epoch_buckets_to_first_monday_of_1970() {
        // 1970-01-01 was a Thursday; the covering week ends Monday 1970-01-05
        assert_eq!(week_bucket(Instant::from_micros(1)), Recency::Week(date(1970, 1, 5)));
    }

    #[test]
    fn pre_epoch_recency_is_total() {
        assert_eq!(week_bucket(Instant::from_micros(-1)), Recency::Week(date(1970, 1, 5)));
    }
}
```

(Verify `MONDAY_US` at implementation time: `chrono::NaiveDate::from_ymd_opt(2026,7,6).unwrap().and_hms_opt(0,0,0).unwrap().and_utc().timestamp_micros()` — if the constant is wrong, fix the constant, not the semantics.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p varve-index bitemporal && cargo test -p varve-compact recency`
Expected: compile errors — `CompactionResolver`, `week_bucket` not defined.

- [ ] **Step 3: Implement**

`bitemporal.rs`:

```rust
impl CompactionResolver {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn on_event(&mut self, e: &Event) -> RowFate {
        if self.current != Some(e.iid) {
            self.current = Some(e.iid);
            self.ceiling.reset();
            self.seen_erase = false;
        }
        if self.seen_erase {
            return RowFate::Drop;
        }
        if matches!(e.op, Op::Erase) {
            self.seen_erase = true;
            return RowFate::Keep { recency: Instant::END_OF_TIME };
        }
        self.polygon.calculate_for(&self.ceiling, e.valid_from, e.valid_to);
        self.ceiling.apply_log(e.system_from, e.valid_from, e.valid_to);
        let visible = (0..self.polygon.range_count()).any(|i| self.polygon.system_to(i) > e.system_from);
        if !visible {
            return RowFate::Drop;
        }
        RowFate::Keep { recency: self.polygon.recency() }
    }
}
```

(`Ceiling` needs `Default` or construct-in-`new`; add `#[derive(Default)]` if missing — `Ceiling::new()` exists.)

`recency.rs`:

```rust
use chrono::{Datelike, NaiveDate};
use varve_storage::Recency;
use varve_types::Instant;

const US_PER_DAY: i64 = 86_400_000_000;
/// Days from 0001-01-01 (CE) to 1970-01-01.
const EPOCH_CE_DAYS: i64 = 719_163;

pub fn week_bucket(recency: Instant) -> Recency {
    if recency == Instant::END_OF_TIME {
        return Recency::Current;
    }
    let days = (recency.as_micros() - 1).div_euclid(US_PER_DAY);
    let date = NaiveDate::from_num_days_from_ce_opt((days + EPOCH_CE_DAYS) as i32)
        .unwrap_or(NaiveDate::MIN); // saturate for absurdly ancient recencies
    let days_ahead = 7 - i64::from(date.weekday().num_days_from_monday()); // Mon→7 … Sun→1
    Recency::Week(date + chrono::Days::new(days_ahead as u64))
}
```

Wire `pub mod recency;` into `varve-compact/src/lib.rs`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p varve-index bitemporal && cargo test -p varve-compact`
Expected: PASS. Existing `resolve`/property tests untouched and green.

- [ ] **Step 5: Commit**

```bash
git add crates/varve-index crates/varve-compact
git commit -m "feat: streaming CompactionResolver (erase-aware, recency) + Monday-0000Z week buckets"
```

---

### Task 10: Merge executor (`run_merge`)

**Files:**
- Create: `crates/varve-compact/src/merge.rs`
- Modify: `crates/varve-compact/src/lib.rs` (`pub mod merge;`, add `CompactError::MissingEndpoint`)
- Test: in-module `#[cfg(test)]`

**Interfaces:**
- Consumes: `varve_index::{encode_rows, EncodedBlock, BlockMeta, Event, Op, SortOrder, CompactionResolver, RowFate}`, `varve_index::trie::iid_in_path`, Task 8 `CompactionJob`/`CompactionConfig`, Task 9 `week_bucket`.
- Produces:
  ```rust
  pub struct SegmentInput {
      pub key: TrieKey,
      pub pages: Vec<Vec<Event>>,   // decoded candidate pages, file order
  }

  #[derive(Debug)]
  pub struct OutputSegment {
      pub key: TrieKey,
      pub data: Vec<u8>,
      pub meta: Vec<u8>,
      pub pages: Vec<varve_index::PageMeta>,
      pub nodes: Vec<varve_index::MetaNode>,
      pub row_count: u64,
  }

  pub fn run_merge(
      job: &CompactionJob,
      inputs: Vec<SegmentInput>,     // MUST be in job.inputs order (oldest-first)
      order: SortOrder,              // family sort key: ByIid / BySrc / ByDst
      cfg: &CompactionConfig,
  ) -> Result<Vec<OutputSegment>, CompactError>;
  ```
  New error variant: `#[error("edge event missing {0} endpoint")] MissingEndpoint(&'static str)`.
- **Algorithm (D7/D8/D11):**
  1. Flatten each input's pages into one cursor (file order is already `(sort_key, iid, system_from desc)`); rows outside `job.out.part` (compare against the **sort key**) are skipped — tiering jobs receive pages that can straddle the part boundary.
  2. K-way merge by `(sort_key bytes, iid bytes, system_from DESC)`; ties pick the **lowest input index** (inputs oldest-first ⇒ within-tie arrival order preserved). With ≤ 5 inputs a linear-scan min over peeked cursors is the implementation — obviously deterministic, no heap needed.
  3. Feed each merged row to `CompactionResolver` (per-IID reset is internal). `Drop` → skip. `Keep { recency }` →
     - `partitioned_by_recency == true` (L0→L1): route to `week_bucket(recency)` buffer; the `Recency::Current` buffer **always exists** even if it stays empty (D8 — empty L1C required).
     - else: single buffer for `job.out` (drop/erase rules still apply; computed recency is not used for routing).
  4. Encode every buffer with `encode_rows(rows, cfg.page_rows, order)` — page normalization for free, byte-identical to flush encoding. Output keys: Partition → `TrieKey { level: 1, recency: <bucket>, part: [], block: job.out.block }`; Preserve → `job.out`.
  5. Output order: `Recency::Current` first, then weeks ascending (BTreeMap order); `row_count = rows.len()`.
- Later tasks rely on: `run_merge`, `SegmentInput`, `OutputSegment` exactly as above.

- [ ] **Step 1: Write the failing tests**

`crates/varve-compact/src/merge.rs` tests:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobs::{CompactionConfig, CompactionJob, FamilyRef};
    use varve_index::{decode_events, encode_rows, Event, Op, SortOrder};
    use varve_storage::{Recency, TrieKey};
    use varve_types::{Iid, Instant};

    fn put(iid: u8, sysfrom: i64, valid_to: i64, val: &str) -> Event {
        let mut doc = varve_types::Doc::new();
        doc.insert("v".into(), varve_types::Value::Str(val.into()));
        Event {
            iid: Iid::from_bytes([iid; 16]),
            system_from: Instant::from_micros(sysfrom),
            valid_from: Instant::from_micros(0),
            valid_to: if valid_to == i64::MAX { Instant::END_OF_TIME } else { Instant::from_micros(valid_to) },
            src: None,
            dst: None,
            op: Op::Put { labels: vec!["T".into()], doc },
        }
    }

    fn erase(iid: u8, sysfrom: i64) -> Event {
        Event {
            iid: Iid::from_bytes([iid; 16]),
            system_from: Instant::from_micros(sysfrom),
            valid_from: Instant::MIN,
            valid_to: Instant::END_OF_TIME,
            src: None,
            dst: None,
            op: Op::Erase,
        }
    }

    fn seg(key: &str, rows: Vec<Event>) -> SegmentInput {
        let enc = encode_rows(rows, 4, SortOrder::ByIid).unwrap();
        let pages = enc
            .pages
            .iter()
            .map(|p| decode_events(&enc.data[p.offset as usize..(p.offset + p.len) as usize]).unwrap())
            .collect();
        SegmentInput { key: TrieKey::parse(key).unwrap(), pages }
    }

    fn l0_job(block: u64) -> CompactionJob {
        CompactionJob {
            family: FamilyRef { graph: "g".into(), table: "t".into(), family: String::new() },
            inputs: vec![TrieKey::l0(block)],
            out: TrieKey { level: 1, recency: Recency::Current, part: vec![], block },
            partitioned_by_recency: true,
        }
    }

    fn cfg() -> CompactionConfig {
        CompactionConfig { file_size_target: 1_000_000, page_rows: 4 }
    }

    #[test]
    fn l0_to_l1_splits_current_from_historical() {
        // entity 1: superseded then current; entity 2: expired valid window
        let rows = vec![put(1, 10, i64::MAX, "old"), put(1, 20, i64::MAX, "new"), put(2, 10, 5, "expired")];
        let outs = run_merge(&l0_job(0), vec![seg("l00-rc-b00", rows)], SortOrder::ByIid, &cfg()).unwrap();
        let current = outs.iter().find(|o| o.key.recency == Recency::Current).unwrap();
        assert_eq!(current.key.to_string(), "l01-rc-b00");
        assert_eq!(current.row_count, 1); // only the live put
        let historical: Vec<_> = outs.iter().filter(|o| o.key.recency != Recency::Current).collect();
        let hist_rows: u64 = historical.iter().map(|o| o.row_count).sum();
        assert_eq!(hist_rows, 2);
        for h in &historical {
            assert_eq!(h.key.level, 1);
            assert_eq!(h.key.block, 0);
        }
    }

    #[test]
    fn all_historical_still_emits_an_empty_l1c() {
        let rows = vec![put(2, 10, 5, "expired")];
        let outs = run_merge(&l0_job(0), vec![seg("l00-rc-b00", rows)], SortOrder::ByIid, &cfg()).unwrap();
        let current = outs.iter().find(|o| o.key.recency == Recency::Current).unwrap();
        assert_eq!(current.row_count, 0);
        assert!(current.pages.is_empty());
    }

    #[test]
    fn erase_scrubs_victim_bytes_from_every_output() {
        let rows = vec![put(1, 10, i64::MAX, "SENTINEL-DEAD"), erase(1, 20)];
        let outs = run_merge(&l0_job(0), vec![seg("l00-rc-b00", rows)], SortOrder::ByIid, &cfg()).unwrap();
        let total_rows: u64 = outs.iter().map(|o| o.row_count).sum();
        assert_eq!(total_rows, 1); // the tombstone only
        for o in &outs {
            assert!(!o.data.windows(13).any(|w| w == b"SENTINEL-DEAD"), "victim bytes leaked");
        }
        let current = outs.iter().find(|o| o.key.recency == Recency::Current).unwrap();
        assert_eq!(current.row_count, 1);
    }

    #[test]
    fn same_system_time_tie_across_inputs_is_deterministic() {
        // same entity, same system_from split across a partial L1C and an L0
        // (an L0→L1 job). The merged stream must reproduce single-file order:
        // within a tie, the older input's row comes first (arrival order).
        let older = seg("l01-rc-b00", vec![put(1, 10, i64::MAX, "first")]);
        let newer = seg("l00-rc-b01", vec![put(1, 10, i64::MAX, "second")]);
        let job = CompactionJob {
            family: FamilyRef { graph: "g".into(), table: "t".into(), family: String::new() },
            inputs: vec![older.key.clone(), newer.key.clone()],
            out: TrieKey { level: 1, recency: Recency::Current, part: vec![], block: 1 },
            partitioned_by_recency: true,
        };
        let outs = run_merge(&job, vec![older, newer], SortOrder::ByIid, &cfg()).unwrap();
        let a: Vec<Vec<u8>> = outs.iter().map(|o| o.data.clone()).collect();
        let outs2 = {
            let older = seg("l01-rc-b00", vec![put(1, 10, i64::MAX, "first")]);
            let newer = seg("l00-rc-b01", vec![put(1, 10, i64::MAX, "second")]);
            run_merge(&job, vec![older, newer], SortOrder::ByIid, &cfg()).unwrap()
        };
        let b: Vec<Vec<u8>> = outs2.iter().map(|o| o.data.clone()).collect();
        assert_eq!(a, b);
    }

    #[test]
    fn tiering_job_filters_rows_outside_its_part() {
        // iids in buckets 0 and 3; part [0] job must keep only bucket-0 rows
        let mut lo = [0u8; 16];
        lo[0] = 0x00;
        let mut hi = [0u8; 16];
        hi[0] = 0xC0;
        let mk = |b: [u8; 16], sys: i64| {
            let mut e = put(0, sys, i64::MAX, "x");
            e.iid = Iid::from_bytes(b);
            e
        };
        let rows = vec![mk(lo, 10), mk(hi, 11)];
        let input = seg("l01-rc-b00", rows);
        let job = CompactionJob {
            family: FamilyRef { graph: "g".into(), table: "t".into(), family: String::new() },
            inputs: vec![input.key.clone(); 1],
            out: TrieKey { level: 2, recency: Recency::Current, part: vec![0], block: 0 },
            partitioned_by_recency: false,
        };
        let outs = run_merge(&job, vec![input], SortOrder::ByIid, &cfg()).unwrap();
        assert_eq!(outs.len(), 1);
        assert_eq!(outs[0].row_count, 1);
        assert_eq!(outs[0].key.to_string(), "l02-rc-p0-b00");
    }

    #[test]
    fn merge_is_byte_deterministic_across_repeat_runs() {
        let rows: Vec<Event> = (0..50u8).map(|n| put(n % 7, i64::from(n), i64::MAX, "v")).collect();
        let run = || {
            run_merge(&l0_job(0), vec![seg("l00-rc-b00", rows.clone())], SortOrder::ByIid, &cfg()).unwrap()
        };
        let (a, b) = (run(), run());
        assert_eq!(a.len(), b.len());
        for (x, y) in a.iter().zip(&b) {
            assert_eq!(x.key, y.key);
            assert_eq!(x.data, y.data);
            assert_eq!(x.meta, y.meta);
        }
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p varve-compact merge`
Expected: compile error — module `merge` missing.

- [ ] **Step 3: Implement**

Core loop sketch (linear-scan k-way merge — deterministic by construction):

```rust
struct Cursor {
    rows: std::iter::Peekable<std::vec::IntoIter<Event>>,
}

fn sort_key(e: &Event, order: SortOrder) -> Result<Iid, CompactError> {
    match order {
        SortOrder::ByIid => Ok(e.iid),
        SortOrder::BySrc => e.src.ok_or(CompactError::MissingEndpoint("src")),
        SortOrder::ByDst => e.dst.ok_or(CompactError::MissingEndpoint("dst")),
    }
}

pub fn run_merge(job, inputs, order, cfg) -> Result<Vec<OutputSegment>, CompactError> {
    let part = &job.out.part;
    let mut cursors: Vec<Cursor> = inputs
        .into_iter()
        .map(|s| Cursor { rows: s.pages.into_iter().flatten().collect::<Vec<_>>().into_iter().peekable() })
        .collect();
    let mut resolver = varve_index::CompactionResolver::new();
    let mut buffers: BTreeMap<Recency, Vec<Event>> = BTreeMap::new();
    if job.partitioned_by_recency {
        buffers.insert(Recency::Current, Vec::new()); // empty L1C is a real output
    } else {
        buffers.insert(job.out.recency, Vec::new());
    }
    loop {
        // pick the cursor whose head is smallest by (sort_key, iid, Reverse(system_from));
        // ties -> lowest index. Skip heads outside `part` (advance and re-peek).
        /* … linear scan over cursors, using sort_key()? and iid_in_path(&key, part) … */
        let Some(e) = next_row else { break };
        match resolver.on_event(&e) {
            RowFate::Drop => {}
            RowFate::Keep { recency } => {
                let bucket = if job.partitioned_by_recency {
                    crate::recency::week_bucket(recency)
                } else {
                    job.out.recency
                };
                buffers.entry(bucket).or_default().push(e);
            }
        }
    }
    buffers
        .into_iter()
        .map(|(bucket, rows)| {
            let key = if job.partitioned_by_recency {
                TrieKey { level: 1, recency: bucket, part: vec![], block: job.out.block }
            } else {
                job.out.clone()
            };
            let row_count = rows.len() as u64;
            let enc = varve_index::encode_rows(rows, cfg.page_rows, order)?;
            Ok(OutputSegment { key, data: enc.data, meta: enc.meta, pages: enc.pages, nodes: enc.nodes, row_count })
        })
        .collect()
}
```

The part-filter must test the **sort key** (`iid_in_path(&sort_key(e, order)?, part)`), and empty `part` accepts everything. Write the linear-scan selection as a helper `fn next_min(cursors: &mut [Cursor], order: SortOrder, part: &[u8]) -> Result<Option<Event>, CompactError>` so it is unit-testable on its own.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p varve-compact && cargo clippy -p varve-compact --all-targets -- -D warnings`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/varve-compact
git commit -m "feat: k-way segment merge with bitemporal resolution, recency routing, erase scrub"
```

---

### Task 11: `merge_sources` k-way upgrade (recency-split-safe scan merge)

**Files:**
- Modify: `crates/varve-index/src/scan.rs` (`merge_sources` internals; signature unchanged)
- Test: in-module + `varve-testkit/tests/flush_equivalence.rs` (unchanged, re-run)

**Interfaces:**
- Signature is **unchanged** (the flush-equivalence suite calls it directly):
  ```rust
  pub fn merge_sources<B, L>(blocks: B, live: L) -> BTreeMap<Iid, Vec<Event>>
  where
      B: IntoIterator<Item = Vec<Event>>,
      L: IntoIterator<Item = (Iid, Vec<Event>)>;
  ```
- **Semantics change (D10):** today the per-entity lists from consecutive blocks are concatenated (valid while blocks nest by time). After the recency split, one entity's history is distributed across L1C/L1H/newer-L0 files with **interleaved** system times, so concatenation breaks. New rule: per entity, the per-source lists (each internally `system_from` ascending — file order reversed, exactly as today) are k-way merged by `(system_from asc, source index asc)`, live last. For non-overlapping sources this degenerates to concatenation, so it is strictly compatible; ties across sources keep source order (deterministic).
- Later tasks rely on: nothing new — Task 12's scan passes compacted tries as `blocks` in `live_tries()` order.

- [ ] **Step 1: Write the failing test**

Append to `crates/varve-index/src/scan.rs` tests:

```rust
#[test]
fn merge_sources_interleaves_split_histories_by_system_time() {
    let iid = Iid::from_bytes([7u8; 16]);
    let at = |sys: i64| test_event(iid, sys); // module's existing event helper shape
    // L1H file: the superseded old version (sys 10); L1C file: the live one (sys 30);
    // newer L0: a correction between them (sys 20). File order = system_from DESC.
    let l1h = vec![at(10)];
    let l1c = vec![at(30)];
    let l0 = vec![at(20)];
    let merged = merge_sources([l1h, l1c, l0], std::iter::empty());
    let sys: Vec<i64> = merged[&iid].iter().map(|e| e.system_from.as_micros()).collect();
    assert_eq!(sys, vec![10, 20, 30]); // globally ascending, not [10, 30, 20]
}

#[test]
fn merge_sources_tie_keeps_source_order() {
    let iid = Iid::from_bytes([7u8; 16]);
    let mk = |sys: i64, marker: &str| {
        let mut e = test_event(iid, sys);
        if let Op::Put { doc, .. } = &mut e.op {
            doc.insert("m".into(), varve_types::Value::Str(marker.into()));
        }
        e
    };
    let a = vec![mk(10, "a")];
    let b = vec![mk(10, "b")];
    let merged = merge_sources([a, b], std::iter::empty());
    let markers: Vec<String> = merged[&iid]
        .iter()
        .map(|e| match &e.op {
            Op::Put { doc, .. } => match doc.get("m") {
                Some(varve_types::Value::Str(s)) => s.clone(),
                _ => panic!(),
            },
            _ => panic!(),
        })
        .collect();
    assert_eq!(markers, vec!["a", "b"]);
}
```

(Adapt `test_event` to whatever per-module helper exists — the module has event builders from the slice-4 tests; single-event-per-source lists sidestep the per-block reversal, which stays as-is.)

- [ ] **Step 2: Run to verify the first test fails**

Run: `cargo test -p varve-index merge_sources`
Expected: `merge_sources_interleaves_split_histories_by_system_time` FAILS with `[10, 30, 20]` (concat order). The tie test may pass already — keep it as a pin.

- [ ] **Step 3: Implement**

Inside `merge_sources`, replace the per-entity `extend` accumulation with per-entity source lists and a final stable k-way merge:

```rust
// per entity: Vec<Vec<Event>> in source order (blocks then live), each ascending
let mut merged: Vec<Event> = Vec::with_capacity(total);
let mut cursors: Vec<std::iter::Peekable<std::vec::IntoIter<Event>>> =
    source_lists.into_iter().map(|v| v.into_iter().peekable()).collect();
loop {
    let mut best: Option<usize> = None;
    for (i, c) in cursors.iter_mut().enumerate() {
        if let Some(e) = c.peek() {
            let better = match best {
                None => true,
                Some(b) => e.system_from < cursors_peek_system_from(b), // strictly less: ties keep lowest i
            };
            if better { best = Some(i); }
        }
    }
    match best {
        Some(i) => merged.push(cursors[i].next().unwrap_or_else(|| unreachable!("peeked"))),
        None => break,
    }
}
```

(The double-borrow in the sketch needs restructuring — e.g. peek keys into a `Vec<Option<Instant>>` per iteration, or index-based two-phase select. Keep it allocation-light; per-entity source counts are small. The tests are the contract.)

- [ ] **Step 4: Run the proof suites**

Run: `cargo test -p varve-index && PROPTEST_CASES=2000 cargo test -p varve-testkit --release --test flush_equivalence && cargo test --workspace`
Expected: PASS — flush-equivalence green proves strict compatibility for the non-split case.

- [ ] **Step 5: Commit**

```bash
git add crates/varve-index
git commit -m "feat: merge_sources k-way per-entity merge (recency-split-safe)"
```

---

### Task 12: Engine state rework — `FamilyState` catalogs, recovery by state, live-only scan

**Files:**
- Modify: `crates/varve-engine/src/state.rs` (delete `PersistedTrie`; add `FamilyState`)
- Modify: `crates/varve-engine/src/flush.rs` (commit path goes through the catalog)
- Modify: `crates/varve-engine/src/scan.rs` (`merged_snapshot` + adjacency scans read `live_tries()`)
- Modify: `crates/varve-engine/src/db.rs` (recovery builds catalogs; `EngineError::Compact`)
- Modify: `crates/varve-engine/Cargo.toml` (dep `varve-compact = { path = "../varve-compact" }`)
- Test: existing `crates/varve/tests/blocks.rs` + new in-module tests in `state.rs`

**Interfaces:**
- Consumes: Tasks 5–7 (`BlockMeta`, manifest v2, `FamilyCatalog`, `TrieHandle`).
- Produces (`pub(crate)`, in `state.rs`):
  ```rust
  pub(crate) struct FamilyState {
      pub catalog: varve_compact::catalog::FamilyCatalog,
      pub metas: BTreeMap<String, Arc<BlockMeta>>,   // trie_key string → meta; Live + Nascent only
  }

  impl FamilyState {
      pub fn new() -> FamilyState;
      /// Live tries paired with their decoded meta, in catalog.live_tries() order
      /// (block asc, level desc, recency, part) — the order scan feeds merge_sources.
      pub fn live_tries(&self) -> Vec<(varve_compact::catalog::TrieHandle, Arc<BlockMeta>)>;
      /// Register a committed trie: catalog insertion rules + meta registration +
      /// dropping metas of tries the insertion just turned garbage.
      pub fn add_committed(&mut self, key: &TrieKey, row_count: u64, data_len: u64,
                           meta: Arc<BlockMeta>, as_of_us: i64, file_size_target: u64);
  }

  pub(crate) struct TableCore { pub live: LiveTable, pub family: FamilyState }
  pub(crate) struct TableState {
      pub nodes: TableCore,
      pub edges: TableCore,
      pub adj_out: FamilyState,
      pub adj_in: FamilyState,
  }
  ```
  `TableState::{new, core, core_mut, live_rows}` keep their signatures. `PersistedTrie` is deleted (D4 world: state lives in the catalog).
- `EngineError` gains `#[error("compact: {0}")] Compact(#[from] varve_compact::CompactError)`.
- **Flush path change (`flush_block`):** unchanged through data/meta PUTs. Manifest build becomes: clone each family's catalog, `add_trie(TrieKey::l0(block_id), rows, data_len, max_system_us, file_size_target)` on the clone for families that flushed, `manifest.tables = [nodes, edges, adj_out, adj_in].map(to_entries)` (same fixed order as today, full inventory incl. garbage/nascent). After the manifest PUT (crash hooks unmoved), the write-lock swap installs the cloned catalogs + new metas and resets live tails. **Scan-visibility invariant:** catalog transitions become visible only at the write-lock swap after a successful manifest PUT — a query sees pre-commit or post-commit, never both, so an event is never readable from two live tries.
- **Recovery (`db.rs::recover`):** per `TableTries` → `FamilyCatalog::from_entries(&tries)?`; fetch + decode meta objects for Live **and** Nascent entries (nascent metas feed the compactor's dedup; garbage metas are never fetched); route into the four `FamilyState`s by `(graph, table, family)` exactly as today.
- Later tasks rely on: `FamilyState::{live_tries, add_committed, catalog, metas}`.

- [ ] **Step 1: Write the failing test**

New `#[cfg(test)]` in `crates/varve-engine/src/state.rs`:

```rust
#[test]
fn add_committed_l1c_supersedes_l0_and_purges_its_meta() {
    let mut fam = FamilyState::new();
    let meta = Arc::new(BlockMeta { nodes: vec![], pages: vec![] });
    fam.add_committed(&TrieKey::l0(0), 10, 100, meta.clone(), 1, 1_000_000);
    fam.add_committed(&TrieKey::l0(1), 10, 100, meta.clone(), 2, 1_000_000);
    assert_eq!(fam.live_tries().len(), 2);
    let l1c = TrieKey { level: 1, recency: varve_storage::Recency::Current, part: vec![], block: 1 };
    fam.add_committed(&l1c, 20, 200, meta, 3, 1_000_000);
    let live: Vec<String> = fam.live_tries().iter().map(|(h, _)| h.key.to_string()).collect();
    assert_eq!(live, vec!["l01-rc-b01"]);
    // superseded L0 metas are gone from the meta map
    assert!(fam.metas.keys().all(|k| k == "l01-rc-b01"));
    // but the catalog still lists them as garbage (for GC)
    assert_eq!(fam.catalog.garbage_before(i64::MAX).len(), 2);
}
```

And an end-to-end pin in `crates/varve/tests/blocks.rs` (append):

```rust
#[tokio::test]
async fn manifest_carries_full_inventory_with_states_across_restart() {
    // reuse the file's existing blocks_config(dir, max_rows) helper + insert loop
    let dir = tempfile::tempdir().unwrap();
    {
        let db = varve::Db::open(blocks_config(dir.path(), 2)).unwrap();
        for n in 0..6 {
            db.execute(&format!("INSERT (:Blk {{_id: {n}, v: {n}}})")).await.unwrap();
        }
        // three L0 flushes happened (size trigger); queries correct
        let rows = db.query("MATCH (b:Blk) RETURN b.v").await.unwrap();
        assert_eq!(varve_testkit::column_i64(&rows, "b.v").len(), 6);
    }
    // restart: recovery folds catalogs from the manifest, scan stays correct
    let db = varve::Db::open(blocks_config(dir.path(), 2)).unwrap();
    let rows = db.query("MATCH (b:Blk) RETURN b.v").await.unwrap();
    assert_eq!(varve_testkit::column_i64(&rows, "b.v").len(), 6);
}
```

(Adapt helper names to the file's actual ones — `blocks.rs` already has this fixture pattern; if `column_i64` lives elsewhere, use the file's existing row-extraction idiom.)

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p varve-engine state && cargo test -p varve --test blocks`
Expected: compile errors (`FamilyState` missing).

- [ ] **Step 3: Implement**

Mechanical per the Interfaces block. `add_committed`:

```rust
pub fn add_committed(&mut self, key: &TrieKey, row_count: u64, data_len: u64,
                     meta: Arc<BlockMeta>, as_of_us: i64, file_size_target: u64) {
    self.catalog.add_trie(key.clone(), row_count, data_len, as_of_us, file_size_target);
    self.metas.insert(key.to_string(), meta);
    // metas map holds live+nascent only: drop anything the insertion superseded
    let garbage: std::collections::BTreeSet<String> =
        self.catalog.garbage_before(i64::MAX).iter().map(|h| h.key.to_string()).collect();
    self.metas.retain(|k, _| !garbage.contains(k));
}
```

`flush.rs`: the pre-PUT read-lock section additionally clones the four `FamilyState` catalogs; `TrieEntry` construction is replaced by clone-side `add_trie` + `to_entries()`; the post-PUT write-lock section installs clones + metas via `add_committed` on the real state (or swaps the pre-built clones — pick ONE mechanism; the clone-swap is simpler and keeps PUT-failure rollback trivial: on failure nothing was mutated). `as_of_us` for supersessions = the manifest's `max_system_time_us` (deterministic, monotonic — D12's retention clock).

`scan.rs`: replace every `core.tries` / `state.adj_out` iteration with `family.live_tries()`; the per-trie key string is `handle.key.to_string()` passed to `keys::data_key`/`adj_data_key`.

`db.rs::recover`: per the Interfaces block; keep the four-way `(graph, table, family)` routing and `EngineError::UnknownTable` for strangers.

- [ ] **Step 4: Run the workspace suite**

Run: `cargo test --workspace && just crash`
Expected: PASS — all slice-4/5/6 block, restart, and crash tests green (the flush commit contract is unchanged: manifest PUT is still the only commit point).

- [ ] **Step 5: Commit**

```bash
git add crates/varve-engine crates/varve Cargo.lock
git commit -m "feat: engine trie inventory becomes per-family catalogs (live-only scan)"
```

---

### Task 13: Compactor task + `CompactCommit` via writer + `Db::compact_to_quiescence` + config

**Files:**
- Create: `crates/varve-engine/src/compact.rs`
- Modify: `crates/varve-engine/src/writer.rs` (control channel + commit handler; re-read first — slice 7 may have touched it)
- Modify: `crates/varve-engine/src/flush.rs` (post-flush `Kick`)
- Modify: `crates/varve-engine/src/db.rs` (`[compaction]` config, spawn wiring, `Db::compact_to_quiescence`)
- Modify: `crates/varve-engine/src/lib.rs` (`mod compact;`)
- Create: `crates/varve/tests/compaction.rs`
- Test: e2e in `compaction.rs` + in-module writer test

**Interfaces:**
- Consumes: Tasks 8/10/12 (`available_jobs`, `run_merge`, `FamilyState`), existing `WriterState`, `spawn_writer`, `keys::{data_key, meta_key, adj_data_key, adj_meta_key}`, `flush.rs::crash_point`.
- Produces (`pub(crate)` unless noted):
  ```rust
  // compact.rs
  pub(crate) enum CompactMsg {
      Kick,                                                     // fire-and-forget, post-flush
      Quiesce(tokio::sync::oneshot::Sender<Result<(), EngineError>>), // run until no jobs remain
  }

  pub(crate) struct CompactorDeps {
      pub state: Arc<RwLock<TableState>>,
      pub store: Arc<dyn ObjectStore>,
      pub control: mpsc::Sender<WriterControl>,
      pub cfg: varve_compact::jobs::CompactionConfig,
      pub enabled: bool,
  }

  pub(crate) fn spawn_compactor(deps: CompactorDeps) -> mpsc::Sender<CompactMsg>;

  // writer.rs — second channel alongside the DML submission queue
  pub(crate) enum WriterControl {
      CompactCommit {
          family: varve_compact::jobs::FamilyRef,
          outputs: Vec<CommittedSegment>,
          ack: tokio::sync::oneshot::Sender<Result<(), EngineError>>,
      },
      // Task 15 adds GcCommit here
  }

  pub(crate) struct CommittedSegment {
      pub key: TrieKey,
      pub row_count: u64,
      pub data_len: u64,
      pub meta: Arc<BlockMeta>,
  }
  ```
  `spawn_writer` now returns `(mpsc::Sender<Submission>, mpsc::Sender<WriterControl>)`; its loop `select!`s over both receivers plus the existing flush timer. (If slice 7 reshaped `Submission`, only the *new* channel matters — DML submission is untouched.)
- **Family map (fixed order everywhere):** `("default","nodes","")→(nodes, ByIid)`, `("default","edges","")→(edges, ByIid)`, `("default","edges","adj-out")→(adj_out, BySrc)`, `("default","edges","adj-in")→(adj_in, ByDst)`. Key builders: family `""` → `data_key`/`meta_key`, else `adj_data_key`/`adj_meta_key`.
- **Compactor loop (one message at a time):** on `Kick`/`Quiesce`, run **rounds** until a full pass yields zero jobs across all four families: snapshot catalogs + metas under one read lock (clones are cheap: `Arc` metas, small catalogs); for each family, `available_jobs`; for each job, in order:
  1. Fetch inputs: for each input key, meta = snapshot `metas[key]` (a missing meta is a bug — error out, don't skip); candidate pages = `meta.pages_for_part(&job.out.part)`; ranged `store.get_range` per page (via the block's data key), `decode_events` → `SegmentInput`.
  2. `run_merge` (CPU-bound: wrap in `tokio::task::spawn_blocking`, moving inputs in).
  3. PUT outputs: per `OutputSegment`, data first, then meta (D9/D12 ordering).
  4. Send `CompactCommit`, await ack; on error, surface it (Quiesce) or log-and-stop the round (Kick).
- **Writer `CompactCommit` handler** (serial with flushes — one loop, no interleaved manifests):
  1. Clone the target family's catalog; `add_trie` each output (`as_of_us` = the manifest's `max_system_time_us`, carried from writer state like flush does).
  2. Build the manifest exactly like `flush_block` does (`block_id = state.next_block_id`, full inventory from all four families using the ONE mutated clone + three current catalogs, same `watermark`/`max_tx_id`/`max_system_time_us` floors).
  3. `crash_point("pre-compact-manifest-put")` → manifest PUT → `crash_point("post-compact-manifest-put")`.
  4. Write-lock swap: `add_committed` each output on the real `FamilyState`; `state.next_block_id += 1`; ack `Ok(())`. On PUT failure: nothing was mutated → ack the error (compactor retries next round; the orphaned output objects are re-PUT byte-identically — D9).
- **Config (`db.rs`):**
  ```rust
  #[derive(serde::Deserialize)]
  #[serde(default)]
  pub(crate) struct CompactionTuning {
      pub enabled: bool,                 // default true
      pub file_size_target_bytes: u64,   // default 104_857_600
  }
  ```
  read from `[compaction]`; `page_rows` stays `flush.rs::PAGE_ROWS`. `Db::assemble` spawns the compactor after the writer and stores the `mpsc::Sender<CompactMsg>` on `Db`.
- Public API (documented, used by tests/CLI/embedders):
  ```rust
  impl Db {
      /// Run compaction rounds until no jobs remain. No-op when [compaction] enabled = false.
      pub async fn compact_to_quiescence(&self) -> Result<(), EngineError>;
  }
  ```
- `flush_block` tail: after the write-lock swap, `let _ = compact_tx.try_send(CompactMsg::Kick);` (never blocks, never fails the flush; the sender rides in `WriterState`).
- Later tasks rely on: `Db::compact_to_quiescence`, `WriterControl`, `CommittedSegment`, `CompactorDeps` (Task 15 reuses the task + control channel for GC).

- [ ] **Step 1: Write the failing e2e tests**

`crates/varve/tests/compaction.rs`:

```rust
#![allow(clippy::unwrap_used, clippy::expect_used)]
use varve::Db;

fn compact_config(dir: &std::path::Path, max_rows: usize) -> varve::Config {
    // same shape as blocks.rs::blocks_config + [compaction] defaults on
    varve::Config::from_str_toml(&format!(
        r#"
        [log]
        backend = "local"
        [log.local]
        dir = {log_dir:?}
        [storage]
        backend = "local"
        max_block_rows = {max_rows}
        flush_interval_ms = 0
        [storage.local]
        dir = {store_dir:?}
        "#,
        log_dir = dir.join("log"),
        store_dir = dir.join("store"),
        max_rows = max_rows,
    ))
    .unwrap()
}

#[tokio::test]
async fn compaction_preserves_query_results_and_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let snapshot_before;
    {
        let db = Db::open(compact_config(dir.path(), 2)).unwrap();
        for n in 0..10 {
            db.execute(&format!("INSERT (:C {{_id: {n}, v: {n}}})")).await.unwrap();
        }
        snapshot_before = db.query("MATCH (c:C) RETURN c.v").await.unwrap();
        db.compact_to_quiescence().await.unwrap();
        let after = db.query("MATCH (c:C) RETURN c.v").await.unwrap();
        assert_eq!(varve_testkit::column_i64(&snapshot_before, "c.v"),
                   varve_testkit::column_i64(&after, "c.v"));
    }
    let db = Db::open(compact_config(dir.path(), 2)).unwrap();
    let after_restart = db.query("MATCH (c:C) RETURN c.v").await.unwrap();
    assert_eq!(varve_testkit::column_i64(&snapshot_before, "c.v"),
               varve_testkit::column_i64(&after_restart, "c.v"));
}

#[tokio::test]
async fn compaction_supersedes_l0s_in_the_manifest() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(compact_config(dir.path(), 2)).unwrap();
    for n in 0..6 {
        db.execute(&format!("INSERT (:C {{_id: {n}}})")).await.unwrap();
    }
    db.compact_to_quiescence().await.unwrap();
    drop(db);
    // read the latest manifest straight off the store
    let store = varve_storage::local_store(&dir.path().join("store")).unwrap();
    let m = varve_storage::latest_manifest(&store).await.unwrap().unwrap();
    let nodes = m.tables.iter().find(|t| t.table == "nodes" && t.family.is_empty()).unwrap();
    let live: Vec<&str> = nodes.tries.iter()
        .filter(|e| e.state() == varve_storage::TrieState::Live)
        .map(|e| e.trie_key.as_str()).collect();
    let garbage = nodes.tries.iter()
        .filter(|e| e.state() == varve_storage::TrieState::Garbage).count();
    assert!(live.iter().all(|k| k.starts_with("l01-")), "live after quiescence must be L1: {live:?}");
    assert!(garbage >= 3, "the L0s must be garbage, got {garbage}");
}

#[tokio::test]
async fn duplicate_compaction_run_is_harmless() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(compact_config(dir.path(), 2)).unwrap();
    for n in 0..6 {
        db.execute(&format!("INSERT (:C {{_id: {n}}})")).await.unwrap();
    }
    db.compact_to_quiescence().await.unwrap();
    let before = db.query("MATCH (c:C) RETURN c.v").await.unwrap();
    db.compact_to_quiescence().await.unwrap(); // second run: zero jobs, no state change
    let after = db.query("MATCH (c:C) RETURN c.v").await.unwrap();
    assert_eq!(format!("{before:?}"), format!("{after:?}"));
}

#[tokio::test]
async fn edges_and_adjacency_families_compact_too() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(compact_config(dir.path(), 2)).unwrap();
    for n in 0..4 {
        db.execute(&format!("INSERT (:N {{_id: {n}}})")).await.unwrap();
    }
    for n in 0..3 {
        db.execute(&format!(
            "MATCH (a:N), (b:N) WHERE a._id = {n} AND b._id = {m} INSERT (a)-[:R {{_id: {id}}}]->(b)",
            n = n, m = n + 1, id = 100 + n
        )).await.unwrap();
    }
    let hops_before = db.query("MATCH (a:N)-[:R]->{1,3}(b:N) WHERE a._id = 0 RETURN b._id").await.unwrap();
    db.compact_to_quiescence().await.unwrap();
    let hops_after = db.query("MATCH (a:N)-[:R]->{1,3}(b:N) WHERE a._id = 0 RETURN b._id").await.unwrap();
    assert_eq!(varve_testkit::column_i64(&hops_before, "b._id"),
               varve_testkit::column_i64(&hops_after, "b._id"));
}
```

(Adapt the config builder and edge-INSERT syntax to the working forms in `blocks.rs`/`traversal.rs` — the *assertions* are the contract. `varve::Config::from_str_toml` = whatever the existing tests use to build a `Config` from TOML text; `varve_storage::local_store` = the existing local factory helper, check `lib.rs` re-exports.)

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p varve --test compaction`
Expected: compile error — `compact_to_quiescence` doesn't exist.

- [ ] **Step 3: Implement**

Per the Interfaces block, in this order: `WriterControl` + writer handler; `compact.rs` task; `db.rs` config/wiring/`compact_to_quiescence`; `flush.rs` kick. Multi-runtime determinism note: `run_merge` runs inside `spawn_blocking` on one thread per job — thread count cannot affect bytes (jobs are serial in v1; parallel job execution is a recorded future option, safe because jobs are independent by construction).

- [ ] **Step 4: Run the suites**

Run: `cargo test -p varve --test compaction && cargo test --workspace`
Expected: PASS.

- [ ] **Step 5: Add crash points + crash-matrix rows**

Add `"pre-compact-manifest-put"` / `"post-compact-manifest-put"` to the matrix in `crates/varve-testkit/tests/crash_recovery.rs` (and make `crash_child.rs` trigger compaction: after its K inserts, call `compact_to_quiescence` when the trigger names a compact point — mirror how flush points are exercised). Post-restart assertions are the existing ones: every acked tx present, manifest parses, queries correct — plus: a `pre-compact-manifest-put` kill leaves L0s live (compaction simply re-runs later); a `post-compact-manifest-put` kill recovers with the compacted inventory.

Run: `just crash`
Expected: green across all 7 fault points.

- [ ] **Step 6: Commit**

```bash
git add crates/varve-engine crates/varve crates/varve-testkit
git commit -m "feat: compactor task with writer-serialized manifest commits + Db::compact_to_quiescence"
```

---

### Task 14: Compaction equivalence property + erase-bytes-gone proof

**Files:**
- Create: `crates/varve-testkit/tests/compaction_equivalence.rs`
- Modify: `crates/varve-testkit/Cargo.toml` (dep `varve-compact = { path = "../varve-compact" }`)
- Modify: `crates/varve/tests/compaction.rs` (append the erase e2e)
- Test: this task IS tests

**Interfaces:**
- Consumes: `varve_testkit::{ReferenceStore, strategy::{arb_history, arb_bounds}}`, `varve_index::{encode_rows, decode_events, merge_sources, snapshot_entities, resolve}`, `varve_compact::{catalog::FamilyCatalog, jobs::{available_jobs, CompactionConfig, FamilyRef}, merge::{run_merge, SegmentInput}}`.
- Produces: two property tests + one e2e erase proof. Pattern follows `flush_equivalence.rs` exactly: exercise the SHIPPED cores, never copies; `cases()` reads `PROPTEST_CASES` (default 10_000, nightly raises it).
- **Pure-level harness (the workhorse):** given a random history and random cut points, build L0 `EncodedBlock`s per cut (via `encode_rows`), register them in a `FamilyCatalog`, then loop `available_jobs` → decode inputs → `run_merge` → `add_trie` outputs (tiny `file_size_target` so multi-level tiering actually happens) until quiescent — an in-memory replica of the driver with NO engine involvement. Query check: decode the live tries' pages, `merge_sources`, `resolve` under random bounds, compare with the never-compacted reference — for **every** random `TemporalBounds`, so system-time travel through compacted files is covered. `T_POOL = 12` µs collisions make same-system-time ties common — this suite is the D7 tie-semantics contract.

- [ ] **Step 1: Write the property tests**

`crates/varve-testkit/tests/compaction_equivalence.rs`:

```rust
#![allow(clippy::unwrap_used, clippy::expect_used)]
use proptest::prelude::*;
use std::collections::BTreeMap;
use varve_compact::catalog::FamilyCatalog;
use varve_compact::jobs::{available_jobs, CompactionConfig, FamilyRef};
use varve_compact::merge::{run_merge, SegmentInput};
use varve_index::{decode_events, encode_rows, merge_sources, resolve, Event, SortOrder};
use varve_storage::TrieKey;
use varve_testkit::strategy::{arb_bounds, arb_history};
use varve_types::{Iid, Instant};

fn cases() -> u32 {
    std::env::var("PROPTEST_CASES").ok().and_then(|v| v.parse().ok()).unwrap_or(10_000)
}

struct MiniStore {
    catalog: FamilyCatalog,
    blobs: BTreeMap<String, (Vec<u8>, varve_index::BlockMeta)>, // trie_key -> (data, meta)
}

impl MiniStore {
    fn commit(&mut self, key: &TrieKey, data: Vec<u8>, meta: varve_index::BlockMeta,
              rows: u64, as_of: i64, target: u64) {
        self.catalog.add_trie(key.clone(), rows, data.len() as u64, as_of, target);
        self.blobs.insert(key.to_string(), (data, meta));
    }

    fn input_for(&self, key: &TrieKey, part: &[u8]) -> SegmentInput {
        let (data, meta) = &self.blobs[&key.to_string()];
        let pages = meta.pages_for_part(part).into_iter()
            .map(|i| {
                let p = &meta.pages[i as usize];
                decode_events(&data[p.offset as usize..(p.offset + p.len) as usize]).unwrap()
            })
            .collect();
        SegmentInput { key: key.clone(), pages }
    }
}

/// Drive jobs to quiescence, exactly as the engine driver does.
fn compact_to_quiescence(store: &mut MiniStore, cfg: &CompactionConfig) -> usize {
    let fam = FamilyRef { graph: "g".into(), table: "t".into(), family: String::new() };
    let mut ran = 0;
    loop {
        let jobs = available_jobs(&fam, &store.catalog, cfg);
        if jobs.is_empty() {
            return ran;
        }
        for job in jobs {
            let inputs = job.inputs.iter().map(|k| store.input_for(k, &job.out.part)).collect();
            let outs = run_merge(&job, inputs, SortOrder::ByIid, cfg).unwrap();
            for o in outs {
                let meta = varve_index::decode_meta(&o.meta).unwrap();
                store.commit(&o.key, o.data, meta, o.row_count, ran as i64 + 1, cfg.file_size_target);
            }
            ran += 1;
        }
    }
}

fn live_query(store: &MiniStore) -> BTreeMap<Iid, Vec<Event>> {
    let blocks: Vec<Vec<Event>> = store.catalog.live_tries().iter()
        .map(|h| {
            let (data, meta) = &store.blobs[&h.key.to_string()];
            meta.pages.iter()
                .flat_map(|p| decode_events(&data[p.offset as usize..(p.offset + p.len) as usize]).unwrap())
                .collect()
        })
        .collect();
    merge_sources(blocks, std::iter::empty())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(cases()))]

    #[test]
    fn compaction_does_not_change_visibility(
        history in arb_history(24),
        cuts in proptest::collection::vec(1usize..8, 1..4),
        bounds in arb_bounds(),
    ) {
        // slice the history into L0 blocks at the cut points
        let mut store = MiniStore { catalog: FamilyCatalog::default(), blobs: BTreeMap::new() };
        let cfg = CompactionConfig { file_size_target: 512, page_rows: 4 }; // tiny => real tiering
        let mut block = 0u64;
        let mut rest = history.as_slice();
        for cut in &cuts {
            let take = (*cut).min(rest.len());
            let (head, tail) = rest.split_at(take);
            rest = tail;
            if head.is_empty() { continue; }
            let enc = encode_rows(head.to_vec(), cfg.page_rows, SortOrder::ByIid).unwrap();
            let meta = varve_index::decode_meta(&enc.meta).unwrap();
            store.commit(&TrieKey::l0(block), enc.data, meta, head.len() as u64, 0, cfg.file_size_target);
            block += 1;
        }
        if !rest.is_empty() {
            let enc = encode_rows(rest.to_vec(), cfg.page_rows, SortOrder::ByIid).unwrap();
            let meta = varve_index::decode_meta(&enc.meta).unwrap();
            store.commit(&TrieKey::l0(block), enc.data, meta, rest.len() as u64, 0, cfg.file_size_target);
        }

        let before = live_query(&store);
        compact_to_quiescence(&mut store, &cfg);
        let after = live_query(&store);

        // visibility must be identical entity-by-entity under the random bounds
        let iids: std::collections::BTreeSet<Iid> =
            before.keys().chain(after.keys()).copied().collect();
        for iid in iids {
            let empty = Vec::new();
            let b = resolve(before.get(&iid).unwrap_or(&empty), &bounds);
            let a = resolve(after.get(&iid).unwrap_or(&empty), &bounds);
            let strip = |vs: Vec<varve_index::ResolvedVersion<'_>>| -> Vec<_> {
                vs.into_iter().map(|v| (v.event.clone(), v.valid_from, v.valid_to, v.system_to)).collect::<Vec<_>>()
            };
            prop_assert_eq!(strip(b), strip(a), "iid {:?} diverged", iid);
        }
    }

    #[test]
    fn compaction_output_is_deterministic_across_reruns(
        history in arb_history(16),
    ) {
        let cfg = CompactionConfig { file_size_target: 512, page_rows: 4 };
        let build = || {
            let mut store = MiniStore { catalog: FamilyCatalog::default(), blobs: BTreeMap::new() };
            let enc = encode_rows(history.clone(), cfg.page_rows, SortOrder::ByIid).unwrap();
            let meta = varve_index::decode_meta(&enc.meta).unwrap();
            store.commit(&TrieKey::l0(0), enc.data, meta, history.len() as u64, 0, cfg.file_size_target);
            compact_to_quiescence(&mut store, &cfg);
            store.blobs
        };
        let (a, b) = (build(), build());
        prop_assert_eq!(
            a.iter().map(|(k, (d, _))| (k.clone(), d.clone())).collect::<Vec<_>>(),
            b.iter().map(|(k, (d, _))| (k.clone(), d.clone())).collect::<Vec<_>>()
        );
    }
}
```

Add one non-proptest test in the same file discharging the roadmap's "different thread counts" wording literally (the merge core is single-threaded by construction, so this is a pin, not a probe):

```rust
fn fixed_history() -> Vec<Event> {
    // 30 events over 5 entities, mixed ops, non-decreasing system_from
    (0..30i64)
        .map(|sys| {
            let mut b = [0u8; 16];
            b[0] = (sys % 5) as u8;
            let op = match sys % 6 {
                4 => varve_index::Op::Delete,
                5 => varve_index::Op::Erase,
                _ => {
                    let mut doc = varve_types::Doc::new();
                    doc.insert("v".into(), varve_types::Value::Int(sys));
                    varve_index::Op::Put { labels: vec!["T".into()], doc }
                }
            };
            Event {
                iid: Iid::from_bytes(b),
                system_from: Instant::from_micros(sys),
                valid_from: Instant::from_micros(sys % 3),
                valid_to: if sys % 4 == 0 { Instant::from_micros(sys + 10) } else { Instant::END_OF_TIME },
                src: None,
                dst: None,
                op,
            }
        })
        .collect()
}

#[test]
fn compaction_bytes_identical_across_runtime_flavors() {
    let history: Vec<Event> = fixed_history();
    let cfg = CompactionConfig { file_size_target: 512, page_rows: 4 };
    let run_in = |rt: tokio::runtime::Runtime| {
        rt.block_on(async {
            tokio::task::spawn_blocking({
                let history = history.clone();
                move || {
                    let mut store = MiniStore { catalog: FamilyCatalog::default(), blobs: BTreeMap::new() };
                    let enc = encode_rows(history.clone(), cfg.page_rows, SortOrder::ByIid).unwrap();
                    let meta = varve_index::decode_meta(&enc.meta).unwrap();
                    store.commit(&TrieKey::l0(0), enc.data, meta, history.len() as u64, 0, cfg.file_size_target);
                    compact_to_quiescence(&mut store, &cfg);
                    store.blobs.into_iter().map(|(k, (d, _))| (k, d)).collect::<Vec<_>>()
                }
            })
            .await
            .unwrap()
        })
    };
    let multi = run_in(tokio::runtime::Builder::new_multi_thread().worker_threads(8).enable_all().build().unwrap());
    let single = run_in(tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap());
    assert_eq!(multi, single);
}
```

(Requires `tokio` as a testkit dev-dependency with `rt-multi-thread` — already in the workspace feature set.)

(`resolve` comparison via a `strip` tuple because `ResolvedVersion` borrows; if `Event: Ord` is missing for the `BTreeSet`, key on `iid` bytes. `arb_history` events are pre-sorted per its non-decreasing `system_from` contract — `encode_rows` handles the rest. If `arb_history`'s generated `Erase` rows make `before` differ from `after` in a way `resolve` can't see, that's a REAL finding — investigate against `refs/xtdb` same-system-time handling before touching the assertion.)

- [ ] **Step 2: Run them (they must fail only until wired, then pass)**

Run: `PROPTEST_CASES=2000 cargo test -p varve-testkit --release --test compaction_equivalence`
Expected: compiles once deps are added; PASSES. If `compaction_does_not_change_visibility` finds a counterexample, STOP and fix the merge/resolver (likely same-system-time tie handling — port `resolveSameSystemTimeEvents` semantics from `refs/xtdb/core/src/main/kotlin/xtdb/compactor/SegmentMerge.kt` into `CompactionResolver`), never weaken the property.

- [ ] **Step 3: Erase e2e — bytes provably absent (roadmap exit criterion)**

Append to `crates/varve/tests/compaction.rs`:

```rust
#[tokio::test]
async fn erase_bytes_are_absent_from_all_post_compaction_objects() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(compact_config(dir.path(), 2)).unwrap();
    db.execute(r#"INSERT (:P {_id: 1, secret: 'ERASE-ME-SENTINEL'})"#).await.unwrap();
    for n in 2..8 {
        db.execute(&format!("INSERT (:P {{_id: {n}}})")).await.unwrap(); // force flushes
    }
    // ERASE surface: slice 7 lands `ERASE` GQL; if this session runs before it,
    // fall back to the erase execution path that slice-2 tests use (Op::Erase is
    // fully implemented event-level). Preferred:
    db.execute("MATCH (p:P) WHERE p._id = 1 ERASE p").await.unwrap();
    for n in 8..12 {
        db.execute(&format!("INSERT (:P {{_id: {n}}})")).await.unwrap();
    }
    db.compact_to_quiescence().await.unwrap();
    drop(db);
    // grep EVERY object under the store dir for the sentinel
    let mut hits = Vec::new();
    for entry in walk(dir.path().join("store")) {
        let bytes = std::fs::read(&entry).unwrap();
        if bytes.windows(b"ERASE-ME-SENTINEL".len()).any(|w| w == b"ERASE-ME-SENTINEL") {
            hits.push(entry);
        }
    }
    assert!(hits.is_empty(), "sentinel bytes survived in {hits:?}");
}

fn walk(root: std::path::PathBuf) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![root];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).unwrap() {
            let p = e.unwrap().path();
            if p.is_dir() { stack.push(p) } else { out.push(p) }
        }
    }
    out
}
```

Two adaptations recorded, not optional:
1. **ERASE statement:** if slice 7 hasn't landed `ERASE` when this executes, drive the erase through whatever event-level path slice 2's tests use (see `varve-index` erase property tests) — the assertion (bytes gone from the `store/` tree) is the contract, GQL is just the trigger. The sentinel lives in *flushed* L0 data before compaction — assert it IS present pre-compaction (add that intermediate assert) so the test can't pass vacuously.
2. **Scope honesty (D7):** this proves victims + erase co-merging in the L1 chain scrub bytes. Rows already routed to a *historical* L1H before the erase arrives are scrubbed only when an erase-carrying merge reaches that partition — matching XTDB. Record in STATUS.md open items: slice 11's GDPR pass adds targeted re-compaction if the TCK/GDPR test demands it.

Run: `cargo test -p varve --test compaction`
Expected: PASS (with the pre-compaction presence assert proving non-vacuity).

- [ ] **Step 4: Commit**

```bash
git add crates/varve-testkit crates/varve
git commit -m "test: compaction equivalence properties + erase-bytes-gone proof"
```

---

### Task 15: GC — `ObjectStore::delete`, `plan_gc`, `Db::run_gc`, `[gc]` config

**Files:**
- Modify: `crates/varve-storage/src/store.rs` (trait method + blanket impl), `src/memory.rs`, `src/local.rs`, `src/cache.rs` (CachedStore forwards + invalidates), `src/disk.rs` if it wraps stores (it doesn't — cache tier only, skip)
- Create: `crates/varve-compact/src/gc.rs`
- Modify: `crates/varve-engine/src/compact.rs` (GC executor on the compactor task), `src/writer.rs` (`WriterControl::GcCommit`), `src/db.rs` (`[gc]` config + `Db::run_gc`)
- Modify: `crates/varve-testkit/tests/crash_recovery.rs` (GC crash point)
- Test: in-module in `gc.rs`; e2e in `crates/varve/tests/compaction.rs`

**Interfaces:**
- `ObjectStore` trait gains (breaking, all impls updated — memory, local, blanket `object_store` bridge, `CachedStore`):
  ```rust
  /// Idempotent delete: missing key is Ok(()). Sovereignty holds — S3 DeleteObject
  /// is universal; only conditional PUT is capability-probed.
  async fn delete(&self, key: &str) -> Result<(), StorageError>;
  ```
  `CachedStore::delete` = `cache.invalidate_path(key)` then inner delete. The blanket impl maps `object_store::Error::NotFound` to `Ok(())`.
- `varve-compact/src/gc.rs`:
  ```rust
  #[derive(Debug, Clone, Copy)]
  pub struct GcConfig { pub enabled: bool, pub retention_us: i64 }

  #[derive(Debug, Default, PartialEq)]
  pub struct GcPlan {
      /// (data_key, meta_key) pairs for garbage tries — delete data FIRST, meta second (D12)
      pub tries: Vec<(String, String)>,
      /// catalog entries to drop once the pair above is deleted, keyed by family
      pub dropped: Vec<(FamilyRef, TrieKey)>,
      pub orphans: Vec<String>,          // unreferenced data/meta objects, aged out
      pub manifests: Vec<String>,        // manifest keys older than retention (never the latest)
      pub log_objects: Vec<String>,      // .vlog strictly below min retained watermark
      pub probe_objects: Vec<String>,    // everything under v1/probe/
  }

  pub struct GcInputs<'a> {
      pub families: &'a [(FamilyRef, &'a FamilyCatalog)],
      pub manifests: &'a [BlockManifest],     // retained history, block_id ASC (latest = last)
      pub graph_keys: &'a [String],           // listing of v1/graphs/
      pub manifest_keys: &'a [String],        // listing of v1/blocks/
      pub log_keys: &'a [String],             // listing of v1/log/
      pub probe_keys: &'a [String],           // listing of v1/probe/
      pub now_us: i64,
  }

  pub fn plan_gc(inputs: &GcInputs<'_>, cfg: &GcConfig) -> GcPlan;   // pure
  ```
- **`plan_gc` rules (D12, exact):** let `cutoff = now_us - retention_us`.
  1. *Garbage tries:* per family, `catalog.garbage_before(cutoff)` → build `(data, meta)` key pairs with the family-appropriate builders; also push `(family, key)` into `dropped`. **Level-0 entries are included** (D4 deviation — Varve GCs L0).
  2. *Manifests:* parse `manifest_block_id` per key; keep the max always; delete others whose manifest `max_system_time_us <= cutoff` (look the value up in `manifests`; a listed manifest absent from the retained history is older than the oldest retained one → delete).
  3. *Orphans:* every `graph_keys` entry that parses as `<...>/data|meta/<trie_key>.arrow` but whose trie key is absent from its family's catalog (any state), with `key.block <= latest_manifest.block_id`, and whose age proxy is past cutoff. Age proxy: the `max_system_time_us` of the **earliest retained manifest with `block_id >= key.block`** (an orphan can't be newer than the manifest that follows its block); if no retained manifest covers it, it predates retention → delete. Unparseable keys under `v1/graphs/` are left alone (never delete what we don't understand).
  4. *Log objects:* `parse_log_key` each; delete keys whose position `< min(manifest.watermark for manifest in manifests)` — every retained (pinnable) manifest can still replay. Unparseable keys skipped.
  5. *Probe objects:* all of `probe_keys` (one-shot diagnostics; discharges the slice-5 probe-entropy fast-follow).
- Engine executor (compactor task, after quiescence when `gc.enabled`, and via explicit message):
  1. Read lock: snapshot family catalogs. Store: `list` the four prefixes; `get` + parse retained manifests (those still listed).
  2. `plan_gc`; execute deletes in plan order: per trie pair data→meta, then orphans, then log objects, then probe objects, then manifests (manifests LAST — they are the pinning roots).
  3. Send `WriterControl::GcCommit { dropped, ack }`: writer `drop_garbage`s each family catalog clone, writes the next manifest (same mechanics as `CompactCommit`), swaps, acks. Crash point `"post-gc-delete"` fires between step 2 and 3.
  ```rust
  // compact.rs additions
  pub(crate) enum CompactMsg { Kick, Quiesce(..), Gc(tokio::sync::oneshot::Sender<Result<GcReport, EngineError>>) }

  pub struct GcReport { pub tries: usize, pub orphans: usize, pub manifests: usize,
                        pub log_objects: usize, pub probe_objects: usize }   // pub: Db::run_gc returns it

  impl Db {
      /// Plan + execute one GC pass. No-op (zero report) when [gc] enabled = false.
      pub async fn run_gc(&self) -> Result<GcReport, EngineError>;
  }
  ```
- Config: `[gc] enabled` (default **true**), `retention_days` (u32, default **7**; `retention_us = days as i64 * 86_400_000_000`) — `GcTuning` struct next to `CompactionTuning`. Automatic trigger: after each `Kick`-initiated round that reached quiescence, run a GC pass (best-effort, log-and-continue on error).
- Later tasks rely on: `Db::run_gc`, `GcReport`.

- [ ] **Step 1: Write the failing pure tests**

`crates/varve-compact/src/gc.rs` tests:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::FamilyCatalog;
    use crate::jobs::FamilyRef;
    use varve_storage::{BlockManifest, TrieKey};

    const DAY: i64 = 86_400_000_000;

    fn fam() -> FamilyRef {
        FamilyRef { graph: "default".into(), table: "nodes".into(), family: String::new() }
    }

    fn manifest(block_id: u64, watermark: u64, sys: i64) -> BlockManifest {
        BlockManifest { block_id, watermark, max_tx_id: 0, max_system_time_us: sys, tables: vec![] }
    }

    fn catalog_with_garbage(garbage_as_of: i64) -> FamilyCatalog {
        let mut cat = FamilyCatalog::default();
        cat.add_trie(TrieKey::l0(0), 1, 10, 0, 100);
        cat.add_trie(TrieKey::parse("l01-rc-b00").unwrap(), 1, 10, garbage_as_of, 100);
        cat
    }

    #[test]
    fn garbage_tries_past_retention_are_planned_data_before_meta() {
        let cat = catalog_with_garbage(1 * DAY);
        let fams = [(fam(), &cat)];
        let manifests = [manifest(1, 0, 1 * DAY)];
        let inputs = GcInputs {
            families: &fams, manifests: &manifests,
            graph_keys: &[], manifest_keys: &[], log_keys: &[], probe_keys: &[],
            now_us: 9 * DAY,
        };
        let plan = plan_gc(&inputs, &GcConfig { enabled: true, retention_us: 7 * DAY });
        assert_eq!(plan.tries.len(), 1);
        let (data, meta) = &plan.tries[0];
        assert_eq!(data, "v1/graphs/default/tables/nodes/data/l00-rc-b00.arrow");
        assert_eq!(meta, "v1/graphs/default/tables/nodes/meta/l00-rc-b00.arrow");
        assert_eq!(plan.dropped, vec![(fam(), TrieKey::l0(0))]);

        // inside retention: nothing
        let inputs2 = GcInputs { now_us: 7 * DAY, ..inputs };
        assert!(plan_gc(&inputs2, &GcConfig { enabled: true, retention_us: 7 * DAY }).tries.is_empty());
    }

    #[test]
    fn latest_manifest_is_never_deleted_and_old_ones_age_out() {
        let cat = FamilyCatalog::default();
        let fams = [(fam(), &cat)];
        let manifests = [manifest(0, 0, 1), manifest(1, 0, 2), manifest(2, 0, 100 * DAY)];
        let keys: Vec<String> = (0..3).map(varve_storage::manifest_key).collect();
        let inputs = GcInputs {
            families: &fams, manifests: &manifests,
            graph_keys: &[], manifest_keys: &keys, log_keys: &[], probe_keys: &[],
            now_us: 100 * DAY,
        };
        let plan = plan_gc(&inputs, &GcConfig { enabled: true, retention_us: 7 * DAY });
        assert_eq!(plan.manifests, vec![varve_storage::manifest_key(0), varve_storage::manifest_key(1)]);
    }

    #[test]
    fn orphans_are_detected_by_absence_and_age() {
        let cat = FamilyCatalog::default(); // knows nothing
        let fams = [(fam(), &cat)];
        let manifests = [manifest(5, 0, 1), manifest(6, 0, 100 * DAY)];
        let orphan = "v1/graphs/default/tables/nodes/data/l00-rc-b03.arrow".to_string();
        let fresh = "v1/graphs/default/tables/nodes/data/l00-rc-b06.arrow".to_string();
        let junk = "v1/graphs/default/tables/nodes/data/README.txt".to_string();
        let graph_keys = [orphan.clone(), fresh.clone(), junk];
        let inputs = GcInputs {
            families: &fams, manifests: &manifests,
            graph_keys: &graph_keys, manifest_keys: &[], log_keys: &[], probe_keys: &[],
            now_us: 100 * DAY,
        };
        let plan = plan_gc(&inputs, &GcConfig { enabled: true, retention_us: 7 * DAY });
        // b03 covered by manifest 5 (sys=1, ancient) -> orphaned + aged; b06 covered by
        // manifest 6 (sys=now) -> too fresh; README unparseable -> untouched
        assert_eq!(plan.orphans, vec![orphan]);
    }

    #[test]
    fn log_objects_below_min_retained_watermark_are_swept() {
        let cat = FamilyCatalog::default();
        let fams = [(fam(), &cat)];
        // two retained manifests with watermarks 100 and 300: min = 100 pins replay
        let manifests = [manifest(0, 100, 1), manifest(1, 300, 2)];
        let below = varve_storage::log_key(varve_types::LogPosition::from_u64(50));
        let at = varve_storage::log_key(varve_types::LogPosition::from_u64(100));
        let log_keys = [below.clone(), at.clone()];
        let inputs = GcInputs {
            families: &fams, manifests: &manifests,
            graph_keys: &[], manifest_keys: &[], log_keys: &log_keys, probe_keys: &[],
            now_us: i64::MAX,
        };
        let plan = plan_gc(&inputs, &GcConfig { enabled: true, retention_us: 0 });
        assert_eq!(plan.log_objects, vec![below]); // strictly below only
    }

    #[test]
    fn probe_objects_always_go() {
        let cat = FamilyCatalog::default();
        let fams = [(fam(), &cat)];
        let manifests = [manifest(0, 0, 1)];
        let probes = ["v1/probe/abc".to_string()];
        let inputs = GcInputs {
            families: &fams, manifests: &manifests,
            graph_keys: &[], manifest_keys: &[], log_keys: &[], probe_keys: &probes,
            now_us: 0,
        };
        assert_eq!(plan_gc(&inputs, &GcConfig { enabled: true, retention_us: 7 * DAY }).probe_objects, probes);
    }
}
```

(Adjust `LogPosition::from_u64` / `manifest_key(u64)` argument shapes to the real APIs — both exist per the slice-4/5 surfaces; adjacency families use `adj_data_key`/`adj_meta_key` in `plan_gc` when `family` ≠ `""` — add a sixth test mirroring the first with `family: "adj-out"` and `table: "edges"`.)

- [ ] **Step 2: Run to verify failure, then implement `plan_gc` + `delete`**

Run: `cargo test -p varve-compact gc` → compile errors → implement per rules 1–5 (pure, `BTreeMap`-ordered outputs) and the trait method + four impls. `varve-storage` needs no new deps.

Run: `cargo test -p varve-compact gc && cargo test -p varve-storage`
Expected: PASS.

- [ ] **Step 3: Engine executor + e2e**

Append to `crates/varve/tests/compaction.rs`:

```rust
#[tokio::test]
async fn gc_deletes_superseded_objects_and_keeps_queries_correct() {
    let dir = tempfile::tempdir().unwrap();
    // retention 0 => everything superseded is collectable immediately
    let db = Db::open(compact_config_with(dir.path(), 2, "[gc]\nenabled = true\nretention_days = 0\n")).unwrap();
    for n in 0..10 {
        db.execute(&format!("INSERT (:C {{_id: {n}, v: {n}}})")).await.unwrap();
    }
    let before = db.query("MATCH (c:C) RETURN c.v").await.unwrap();
    db.compact_to_quiescence().await.unwrap();
    let report = db.run_gc().await.unwrap();
    assert!(report.tries > 0, "L0s must have been collected: {report:?}");
    // queries unchanged after physical deletion
    let after = db.query("MATCH (c:C) RETURN c.v").await.unwrap();
    assert_eq!(varve_testkit::column_i64(&before, "c.v"), varve_testkit::column_i64(&after, "c.v"));
    // no L0 data files remain on disk
    let l0_files: Vec<_> = walk(dir.path().join("store"))
        .into_iter()
        .filter(|p| p.to_string_lossy().contains("/data/l00-"))
        .collect();
    assert!(l0_files.is_empty(), "L0 data files survived GC: {l0_files:?}");
    // restart still healthy (catalog drop was committed via manifest)
    drop(db);
    let db = Db::open(compact_config_with(dir.path(), 2, "[gc]\nenabled = true\nretention_days = 0\n")).unwrap();
    let restarted = db.query("MATCH (c:C) RETURN c.v").await.unwrap();
    assert_eq!(varve_testkit::column_i64(&before, "c.v"), varve_testkit::column_i64(&restarted, "c.v"));
}

#[tokio::test]
async fn gc_is_idempotent_after_partial_failure() {
    // run_gc twice back-to-back: the second pass deletes nothing and succeeds
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(compact_config_with(dir.path(), 2, "[gc]\nenabled = true\nretention_days = 0\n")).unwrap();
    for n in 0..6 {
        db.execute(&format!("INSERT (:C {{_id: {n}}})")).await.unwrap();
    }
    db.compact_to_quiescence().await.unwrap();
    let first = db.run_gc().await.unwrap();
    let second = db.run_gc().await.unwrap();
    assert!(first.tries > 0);
    assert_eq!(second.tries, 0);
}
```

(`compact_config_with(dir, max_rows, extra_toml)` = the Task-13 helper with an appended TOML section — factor it.) Add crash point `"post-gc-delete"` (fires after physical deletes, before `GcCommit`) to `crash_child.rs`/the matrix; post-restart assertion: `Db::open` succeeds, queries correct, and a follow-up `run_gc` converges (re-delete is a no-op, catalog drop lands).

Run: `cargo test -p varve --test compaction && just crash && cargo test --workspace`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add crates/varve-storage crates/varve-compact crates/varve-engine crates/varve crates/varve-testkit
git commit -m "feat: GC — ObjectStore::delete, pure plan_gc, Db::run_gc with manifest-committed catalog drops"
```

---

### Task 16: Churn benchmark

**Files:**
- Create: `crates/varve/examples/churn_bench.rs`
- Test: it's a bench example (same convention as `block_bench.rs`/`traversal_bench.rs` — plain example, no criterion; verify it runs, record numbers in STATUS.md)

**Interfaces:**
- Consumes: `Db::open`, `Db::compact_to_quiescence`, `Db::run_gc` (Tasks 13/15).
- Produces: `cargo run --release --example churn_bench -p varve` — the slice demo command.
- **Workload (roadmap: "sustained update-heavy workload keeps query latency and storage bounded"):** a fixed set of 500 entities; 8 rounds; each round re-INSERTs every entity with a new `v` (a Put version per update — update-heavy by construction), then `compact_to_quiescence` + `run_gc`, then measures:
  - storage bytes = recursive file-size sum of the local `store/` dir (`fs::metadata().len()` walk — the example owns its tempdir, so direct FS access is fine),
  - warm point-lookup latency (`MATCH (c:C) WHERE c._id = 250 RETURN c.v`, median of 20),
  - live/garbage trie counts from `varve_storage::latest_manifest`.
- Config: local log+store in a tempdir, `max_block_rows = 2_000`, `flush_interval_ms = 0`, `[gc] retention_days = 0` (immediate collection — the plateau is the point), compaction on.

- [ ] **Step 1: Write the example**

```rust
//! Churn benchmark: update-heavy workload; storage must plateau, latency stay flat.
//! Run: cargo run --release --example churn_bench -p varve
```

Body: per the Interfaces block; print one table row per round:

```text
round  updates  store_bytes  live_tries  garbage_tries  point_lookup_ms
    1     500       412_331           4              0             1.2
  ...
```

End with a plateau check printed as PASS/FAIL (not a hard assert — it's a bench): `store_bytes(final) < 2 × store_bytes(round 2)` and `point_lookup(final) < 3 × point_lookup(round 2)`.

- [ ] **Step 2: Run it**

Run: `cargo run --release --example churn_bench -p varve`
Expected: completes; plateau check PASS. Without compaction+GC the bytes line grows linearly — sanity-check once with `[compaction] enabled = false` to see the contrast (do not commit that toggle).

- [ ] **Step 3: Commit (numbers go into STATUS.md in Task 17)**

```bash
git add crates/varve/examples/churn_bench.rs
git commit -m "feat: churn_bench example — storage/latency plateau under update churn"
```

---

### Task 17: Slice exit checklist

**Files:**
- Modify: `docs/plans/STATUS.md`, `docs/plans/varve-v1-roadmap.md`

- [ ] **Step 1: Full verification gates**

```bash
just check                          # fmt + clippy -D warnings + workspace tests
just crash                          # 7-point crash matrix incl. compact/gc points
PROPTEST_CASES=20000 cargo test -p varve-testkit --release --test compaction_equivalence
PROPTEST_CASES=20000 cargo test -p varve-testkit --release --test flush_equivalence
cargo run --release --example churn_bench -p varve
cargo run --release --example block_bench -p varve      # regression: slice-4 numbers still hold
```
All green / numbers recorded. Optional (docker available): `VARVE_S3_BACKENDS=minio just s3-matrix` — exercises `ObjectStore::delete` against a real S3 API.

- [ ] **Step 2: Verify roadmap exit criteria explicitly**

- Golden determinism tests green (Task 10 byte-determinism + Task 14 rerun property + duplicate-job test).
- Property tests extended across compaction (Task 14, same results pre/post-compact).
- Erase → bytes provably absent from post-compaction objects (Task 14 e2e, non-vacuous).
- Storage plateaus under churn (Task 16 output, recorded).

- [ ] **Step 3: Update STATUS.md**

Prepend the slice-8 block to **Current position** (follow the slice-6 entry's format): new surfaces (`varve-compact`; `TrieKey`/`Recency`; `MemoryTrie` live index; trie-structured `BlockMeta`; manifest `TrieState`; `FamilyCatalog`/jobs/merge/GC; `WriterControl` commits; `Db::{compact_to_quiescence, run_gc}`; `ObjectStore::delete`; `[compaction]`/`[gc]` config). Record decisions D1–D14 (verbatim numbers), the deviations that materialized during execution, churn-bench numbers, and these open items:
- erase-in-historical-partitions scope note (Task 14 adaptation 2 → slice 11),
- compaction runs jobs serially on one task; parallel job execution is safe-by-construction future work (slice 10 backpressure will want it),
- in-memory merge buffers (XTDB's two-stage disk-bounded merge deferred until file-size targets are real 100 MiB),
- discharge the slice-5 fast-follow lines for probe-object GC and object-store-log sweeping (both now done).
Update the **Slice log** table row 8 with the demo command `cargo run --release --example churn_bench -p varve` and the workspace test count.

- [ ] **Step 4: Tick the roadmap**

In `docs/plans/varve-v1-roadmap.md`, tick all seven slice-8 checkboxes and append the "✅ SLICE COMPLETE" line with date/sessions/exit-criteria evidence, matching the slice-6 entry's format. **Re-read both docs files immediately before editing** — a parallel slice-7 session may have modified them (memory: slice prompts may carry stale ids; STATUS.md + git are the truth).

- [ ] **Step 5: Final commit**

```bash
git add docs/plans/STATUS.md docs/plans/varve-v1-roadmap.md
git commit -m "docs: slice 8 complete — compaction, tries, GC"
```

---

## Execution notes for the implementing sessions

- **Session split suggestion (3 sessions):** ① Tasks 1–6 (primitives + formats), ② Tasks 7–11 (pure compaction core), ③ Tasks 12–17 (engine integration, GC, benches, exit). Each boundary leaves `just check` green.
- **When a sketch fights the borrow checker or a pinned-dep API** (arrow-58 builders, chrono-0.4 date math, prost-0.14 enumeration accessors): the test assertions are the contract; reshape the implementation freely.
- **When the equivalence property finds a counterexample:** that is the plan working as designed — minimize it, check `refs/xtdb/core/src/main/kotlin/xtdb/compactor/SegmentMerge.kt` (`resolveSameSystemTimeEvents`) and `refs/xtdb/core/src/main/kotlin/xtdb/bitemporal/` for the semantics, fix the resolver/merge, never the property.
- **Determinism discipline checklist before every commit in Tasks 7–15:** no `HashMap`/`HashSet` on any output path, no `Instant::now`/clock reads inside `varve-compact`, all iteration over `BTreeMap`/sorted `Vec`s.









