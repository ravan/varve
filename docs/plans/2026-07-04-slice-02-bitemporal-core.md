# Slice 2: Bitemporal Core — events, Ceiling/Polygon resolution, temporal GQL

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Every mutation becomes an immutable bitemporal event; queries resolve visibility through ported XTDB Ceiling/Polygon algorithms and answer `FOR VALID_TIME` / `FOR SYSTEM_TIME` time travel, retroactive corrections, and `DELETE` end-to-end through GQL — with a naive reference model proving the engine correct on 10k randomized histories per CI run.

**Architecture:** `varve-types` gains temporal primitives (`Instant`, `TemporalDimension`, `TemporalBounds`). `varve-index` gains the event model (`Event`, `Op::{Put, Delete, Erase}`), the `Ceiling`/`Polygon` port (from `refs/xtdb/core/src/main/kotlin/xtdb/bitemporal/`), and per-entity `resolve()`; `LiveTable` v1 stores events per IID and snapshots through resolution with a `TemporalBounds` filter (spec §5.2, §7). A new `varve-testkit` crate holds the `BTreeMap`-based reference model and proptest strategies; equivalence properties compare engine vs. reference on a boundary-derived probe grid. `varve-gql`/`varve-plan`/`varve-engine` grow the temporal surface: `FOR` clauses (query-level and per-MATCH), `INSERT … VALID FROM/TO`, `MATCH … DELETE`, history functions `valid_from(x)`/`valid_to(x)`/`system_from(x)`, and a monotonic writer clock with `TxReceipt.system_time`.

**Tech Stack:** Adds `chrono` (timestamp parsing/formatting, already in-tree transitively at 0.4.45) and `proptest` to the workspace. Existing pins: `datafusion` 54.0.0 / `arrow` 58.3.0 (DataFusion's re-exported arrow).

## Global Constraints

- All roadmap Global Constraints apply: TDD (failing test first, minimal implementation, commit per green cycle); `cargo clippy --workspace --all-targets -- -D warnings` clean; `unwrap()`/`expect()` forbidden in library code (allowed in tests via repo-root `clippy.toml`); errors via `thiserror` per crate; conventional-commit prefixes; **no `Co-Authored-By` trailer**.
- **Bitemporal invariant (spec §5.2):** `_system_to` and effective valid ranges are NEVER stored — always derived at read time by resolution. Storage stays append-only events.
- **Determinism:** resolution output order is a pure function of the event set and bounds (BTreeMap iteration by IID; per entity newest-system-first, valid ascending). `DELETE` emits events for sorted, deduplicated IIDs.
- **Timestamps** are always `Timestamp(µs, UTC)` — `Instant` is µs since Unix epoch; Arrow columns are `Timestamp(Microsecond, Some("UTC"))`.
- **Dependency pinning:** add to root `Cargo.toml` `[workspace.dependencies]`: `chrono = { version = "0.4", default-features = false, features = ["std"] }` and `proptest = "1"` (pin latest stable at implementation time). **The test code in this plan is the contract** — if an API sketch differs from the pinned chrono/proptest/arrow API, adapt the implementation, not the test.
- **Scope guards:** `Op::Erase` is fully implemented and property-tested at the event level, but the GQL `ERASE` statement lands with mutation completion (slice 7; end-to-end GDPR verification slice 11). Edges, blocks, durability: later slices. New reserved words (`FOR`, `FROM`, `TO`, `ALL`, `AND`, `VALID`, `DELETE`, `BETWEEN`, `TIMESTAMP`, `DATE`, `VALID_TIME`, `SYSTEM_TIME`, `OF`) can no longer be used as property names — acceptable until slice 7's full literal/identifier grammar.
- **Slice-1 review remediations folded in (STATUS.md "do EARLY in slice 2"):** Task 1 narrows `Value::id_bytes`'s catch-all rejection arm and adds a same-length collision test (T1). Task 6 splits the query path into a sync snapshot phase + async execution phase so `Db::query` no longer holds the `RwLock` across an await (the `#[allow(clippy::await_holding_lock)]` on `query` is DELETED), and adds the deferred tests (LiveTable all-null property / empty doc; both `UnknownColumn` paths). Multi-node INSERT atomicity (validate everything, then append — slice-1 post-review fix) is preserved through Tasks 6 and 8. Deliberately NOT done here: the reviewer's `SnapshotSource` trait seam waits for slice 4 (first second scan source — YAGNI before two impls); DELETE's write-lock-across-await is documented and dissolves in slice 3's writer loop (writes are log-serialized by design, spec D3).

## File structure

```
Cargo.toml                                  # + chrono, proptest workspace deps
.github/workflows/ci.yml                    # + nightly property-test job
crates/
  varve-types/src/temporal.rs               # NEW: Instant, TemporalDimension, TemporalBounds
  varve-types/src/position.rs               # + TypeError::InvalidTimestamp
  varve-types/src/lib.rs                    # + pub mod temporal, re-exports
  varve-types/Cargo.toml                    # + chrono
  varve-index/src/bitemporal.rs             # NEW: Ceiling, Polygon, resolve, ResolvedVersion
  varve-index/src/event.rs                  # NEW: Event, Op
  varve-index/src/live.rs                   # REWRITE: event storage + resolved snapshots
  varve-index/src/lib.rs                    # + modules, re-exports
  varve-testkit/                            # NEW CRATE: reference model, strategies, equivalence
    Cargo.toml
    src/lib.rs
    src/reference.rs
    src/strategy.rs
    tests/equivalence.rs
  varve-gql/src/token.rs                    # + 13 temporal keywords
  varve-gql/src/ast.rs                      # v1: TemporalClauses, ReturnItem enum, DeleteStmt, INSERT valid
  varve-gql/src/parser.rs                   # + FOR clauses, datetime literals, DELETE, VALID clause
  varve-gql/Cargo.toml                      # + varve-types
  varve-plan/src/exec.rs                    # run_query(…, now) + bounds + temporal fns + matching_iids
  varve-plan/Cargo.toml                     # + varve-types as regular dep
  varve-engine/src/clock.rs                 # NEW: MonotonicClock
  varve-engine/src/db.rs                    # events, VALID clause, DELETE, TxReceipt.system_time
  varve-engine/Cargo.toml                   # + tokio dev-dep
  varve/src/lib.rs                          # + temporal re-exports
  varve/Cargo.toml                          # + varve-types
  varve/tests/temporal.rs                   # NEW: end-to-end acceptance scenarios
  varve/examples/time_travel.rs             # NEW: demo (STATUS.md demo command)
```

XTDB porting references (read before Tasks 2–4): `refs/xtdb/core/src/main/kotlin/xtdb/bitemporal/{Ceiling,Polygon,PolygonCalculator}.kt`, `refs/xtdb/core/src/test/kotlin/xtdb/bitemporal/{CeilingTest,PolygonTest}.kt`, `refs/xtdb/core/src/main/kotlin/xtdb/util/TemporalBounds.kt`. The Kotlin unit tests are ported as our known-answer tests — behavior is pinned to XTDB's proven algorithms.

---

### Task 1: Temporal types in varve-types

**Files:**
- Create: `crates/varve-types/src/temporal.rs`
- Modify: `crates/varve-types/src/position.rs` (add `TypeError::InvalidTimestamp`)
- Modify: `crates/varve-types/src/lib.rs`
- Modify: `crates/varve-types/Cargo.toml`, root `Cargo.toml`
- Test: in-module `#[cfg(test)]`

**Interfaces:**
- Produces `varve_types::Instant` — `Copy + Ord + Hash + Display`; µs since Unix epoch UTC:
  - `const MIN: Instant` (i64::MIN sentinel), `const END_OF_TIME: Instant` (i64::MAX sentinel, spec §5.2 "∞")
  - `const fn from_micros(i64) -> Instant`, `const fn as_micros(self) -> i64`
  - `fn parse_rfc3339(&str) -> Result<Instant, TypeError>` (offsets normalized to UTC)
  - `fn parse_date(&str) -> Result<Instant, TypeError>` (`YYYY-MM-DD` → midnight UTC)
  - `Display`: RFC 3339 with µs precision (`2020-01-01T00:00:00.000000Z`); out-of-chrono-range values (sentinels) render as `<µs>us`
- Produces `varve_types::TemporalDimension` — half-open `[lower, upper)`, `pub lower/upper: Instant`; constructors `at(t)` (=[t,t+1µs)), `in_range(from,to)` (=[from,to)), `between(from,to)` (=[from,to+1µs), closed upper), `all()`; `fn intersects(&self, lower, upper) -> bool` = `self.lower < upper && lower < self.upper`; `Default` = `all()`. (Port of XTDB `TemporalBounds.kt` semantics.)
- Produces `varve_types::TemporalBounds` — `pub valid/system: TemporalDimension`, `fn intersects(&self, valid_from, valid_to, system_from, system_to) -> bool`; `Default` = both `all()`.
- Modify `TypeError`: add `#[error("invalid timestamp: {0}")] InvalidTimestamp(String)`.

- [ ] **Step 1: Write the failing test**

`crates/varve-types/src/temporal.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn us(n: i64) -> Instant {
        Instant::from_micros(n)
    }

    #[test]
    fn parse_rfc3339_known_answer() {
        assert_eq!(
            Instant::parse_rfc3339("2020-01-01T00:00:00Z").unwrap().as_micros(),
            1_577_836_800_000_000
        );
    }

    #[test]
    fn parse_normalizes_offsets_to_utc() {
        assert_eq!(
            Instant::parse_rfc3339("2020-01-01T02:00:00+02:00").unwrap(),
            Instant::parse_rfc3339("2020-01-01T00:00:00Z").unwrap()
        );
    }

    #[test]
    fn parse_date_is_midnight_utc() {
        assert_eq!(
            Instant::parse_date("2020-01-01").unwrap(),
            Instant::parse_rfc3339("2020-01-01T00:00:00Z").unwrap()
        );
    }

    #[test]
    fn parse_errors_are_reported() {
        assert!(Instant::parse_rfc3339("not a time").is_err());
        assert!(Instant::parse_date("2020-13-01").is_err());
    }

    #[test]
    fn display_round_trips() {
        let t = Instant::parse_rfc3339("2024-06-01T12:34:56.789012Z").unwrap();
        assert_eq!(Instant::parse_rfc3339(&t.to_string()).unwrap(), t);
    }

    #[test]
    fn sentinels_display_without_panicking() {
        assert!(!Instant::END_OF_TIME.to_string().is_empty());
        assert!(!Instant::MIN.to_string().is_empty());
    }

    #[test]
    fn ordering_and_sentinels() {
        assert!(Instant::MIN < us(0));
        assert!(us(0) < Instant::END_OF_TIME);
    }

    #[test]
    fn dimension_at_is_a_single_instant() {
        let d = TemporalDimension::at(us(5));
        assert!(d.intersects(us(5), us(6)));
        assert!(d.intersects(us(0), us(6)));
        assert!(!d.intersects(us(6), us(10))); // starts after the point
        assert!(!d.intersects(us(0), us(5))); // half-open: ends exactly at the point
    }

    #[test]
    fn dimension_in_range_is_half_open() {
        let d = TemporalDimension::in_range(us(3), us(7));
        assert!(d.intersects(us(6), us(9)));
        assert!(!d.intersects(us(7), us(9))); // adjacency is not overlap
    }

    #[test]
    fn dimension_between_is_closed_at_the_top() {
        let d = TemporalDimension::between(us(3), us(7));
        assert!(d.intersects(us(7), us(9)));
        assert_eq!(
            TemporalDimension::between(us(3), Instant::END_OF_TIME).upper,
            Instant::END_OF_TIME // saturating +1
        );
    }

    #[test]
    fn dimension_all_and_default() {
        assert_eq!(TemporalDimension::default(), TemporalDimension::all());
        assert!(TemporalDimension::all().intersects(Instant::MIN, us(0)));
    }

    #[test]
    fn bounds_require_both_axes_to_intersect() {
        let b = TemporalBounds {
            valid: TemporalDimension::at(us(5)),
            system: TemporalDimension::at(us(10)),
        };
        assert!(b.intersects(us(0), us(9), us(8), us(12)));
        assert!(!b.intersects(us(6), us(9), us(8), us(12))); // valid misses
        assert!(!b.intersects(us(0), us(9), us(11), us(12))); // system misses
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p varve-types temporal`
Expected: compile error — `temporal` module not defined.

- [ ] **Step 3: Write minimal implementation**

Root `Cargo.toml` — add to `[workspace.dependencies]`:
```toml
chrono = { version = "0.4", default-features = false, features = ["std"] }
proptest = "1"
```

`crates/varve-types/Cargo.toml` — add to `[dependencies]`:
```toml
chrono = { workspace = true }
```

`crates/varve-types/src/position.rs` — add variant to `TypeError`:
```rust
    #[error("invalid timestamp: {0}")]
    InvalidTimestamp(String),
```

Prepend to `crates/varve-types/src/temporal.rs`:
```rust
use crate::position::TypeError;
use std::fmt;

/// Microseconds since the Unix epoch, UTC — the only timestamp representation
/// in Varve (Global Constraint: Timestamp(µs, UTC)).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Instant(i64);

impl Instant {
    /// "Beginning of time" sentinel.
    pub const MIN: Instant = Instant(i64::MIN);
    /// "Forever" sentinel — unset `_valid_to`, unsuperseded `_system_to` (spec §5.2).
    pub const END_OF_TIME: Instant = Instant(i64::MAX);

    pub const fn from_micros(us: i64) -> Self {
        Instant(us)
    }

    pub const fn as_micros(self) -> i64 {
        self.0
    }

    /// RFC 3339 timestamp, e.g. `2020-01-01T00:00:00Z`; offsets normalized to UTC.
    pub fn parse_rfc3339(s: &str) -> Result<Self, TypeError> {
        chrono::DateTime::parse_from_rfc3339(s)
            .map(|dt| Instant(dt.with_timezone(&chrono::Utc).timestamp_micros()))
            .map_err(|e| TypeError::InvalidTimestamp(format!("{s}: {e}")))
    }

    /// Calendar date `YYYY-MM-DD` as midnight UTC.
    pub fn parse_date(s: &str) -> Result<Self, TypeError> {
        let date = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
            .map_err(|e| TypeError::InvalidTimestamp(format!("{s}: {e}")))?;
        let midnight = date
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| TypeError::InvalidTimestamp(s.to_string()))?;
        Ok(Instant(midnight.and_utc().timestamp_micros()))
    }
}

impl fmt::Display for Instant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Sentinels and instants beyond chrono's range render as raw µs.
        match chrono::DateTime::<chrono::Utc>::from_timestamp_micros(self.0) {
            Some(dt) => write!(f, "{}", dt.to_rfc3339_opts(chrono::SecondsFormat::Micros, true)),
            None => write!(f, "{}us", self.0),
        }
    }
}

/// Half-open range `[lower, upper)` on one temporal axis.
/// Semantics ported from XTDB's `TemporalBounds.kt`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct TemporalDimension {
    pub lower: Instant,
    pub upper: Instant,
}

impl TemporalDimension {
    /// `AS OF t` — the single instant `t`.
    pub fn at(t: Instant) -> Self {
        Self { lower: t, upper: Instant(t.0.saturating_add(1)) }
    }

    /// `FROM a TO b` — `[a, b)`.
    pub fn in_range(from: Instant, to: Instant) -> Self {
        Self { lower: from, upper: to }
    }

    /// `BETWEEN a AND b` — `[a, b]` (closed upper, SQL:2011 style).
    pub fn between(from: Instant, to: Instant) -> Self {
        Self { lower: from, upper: Instant(to.0.saturating_add(1)) }
    }

    pub fn all() -> Self {
        Self { lower: Instant::MIN, upper: Instant::END_OF_TIME }
    }

    pub fn intersects(&self, lower: Instant, upper: Instant) -> bool {
        self.lower < upper && lower < self.upper
    }
}

impl Default for TemporalDimension {
    fn default() -> Self {
        Self::all()
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct TemporalBounds {
    pub valid: TemporalDimension,
    pub system: TemporalDimension,
}

impl TemporalBounds {
    pub fn intersects(
        &self,
        valid_from: Instant,
        valid_to: Instant,
        system_from: Instant,
        system_to: Instant,
    ) -> bool {
        self.valid.intersects(valid_from, valid_to)
            && self.system.intersects(system_from, system_to)
    }
}
```

Update `crates/varve-types/src/lib.rs`:
```rust
pub mod iid;
pub mod position;
pub mod temporal;
pub mod value;
pub use iid::Iid;
pub use position::{LogPosition, TypeError};
pub use temporal::{Instant, TemporalBounds, TemporalDimension};
pub use value::{Doc, Value};
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p varve-types`
Expected: all pass (12 new + 15 existing).

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml Cargo.lock crates/varve-types/
git commit -m "feat: temporal types — Instant, TemporalDimension, TemporalBounds"
```

- [ ] **Step 6: Slice-1 remediation (STATUS.md T1) — write the failing test**

Append to the tests module in `crates/varve-types/src/value.rs`:
```rust
    #[test]
    fn same_length_encodings_do_not_collide() {
        // Int encodes to 9 bytes (tag + BE i64); an 8-byte string also encodes
        // to 9 bytes with an IDENTICAL payload — only the tag disambiguates.
        let i = Value::Int(0x3132_3334_3536_3738).id_bytes().unwrap(); // BE bytes == b"12345678"
        let s = Value::Str("12345678".into()).id_bytes().unwrap();
        assert_eq!(i.len(), s.len());
        assert_ne!(i, s);
    }
```

- [ ] **Step 7: Run, then narrow the rejection arm**

Run: `cargo test -p varve-types value` — the new test already passes (tags exist); it pins the property. Then narrow `id_bytes`'s catch-all so a future `Value` variant forces an explicit decision — replace:
```rust
            other => Err(TypeError::InvalidId(format!("{other:?}"))),
```
with:
```rust
            other @ (Value::Float(_) | Value::Null) => {
                Err(TypeError::InvalidId(format!("{other:?}")))
            }
```

Run: `cargo test -p varve-types` — Expected: all pass (the match is now exhaustive without a wildcard).

- [ ] **Step 8: Commit the remediation**

```bash
git add crates/varve-types/
git commit -m "fix: narrow id_bytes rejection arm; pin same-length id collision test"
```

---

### Task 2: Port XTDB Ceiling

**Files:**
- Create: `crates/varve-index/src/bitemporal.rs`
- Modify: `crates/varve-index/src/lib.rs`
- Test: in-module `#[cfg(test)]`

**Interfaces:**
- Produces `varve_index::Ceiling` — the descending staircase of "system time above which this valid range is superseded", maintained while scanning one entity's events newest-system-time-first (spec §7). Faithful port of `refs/xtdb/core/src/main/kotlin/xtdb/bitemporal/Ceiling.kt`; behavior pinned by porting `CeilingTest.kt`.
  - `Ceiling::new() -> Ceiling` — reset state: `valid_times = [END_OF_TIME, MIN]` (descending), `sys_time_ceilings = [END_OF_TIME]`
  - `fn reset(&mut self)`
  - `fn apply_log(&mut self, system_from: Instant, valid_from: Instant, valid_to: Instant)` — record that `[valid_from, valid_to)` is superseded above `system_from`; no-op when `valid_from >= valid_to`
  - Range accessors used by `Polygon` (range indices count from the oldest valid time upward): `fn valid_to(&self, range_idx: usize) -> Instant`, `fn system_time(&self, range_idx: usize) -> Instant`, `fn ceiling_index(&self, valid_time: Instant) -> usize`
  - Internal fields `valid_times: Vec<Instant>` (descending, sentinel-bounded) and `sys_time_ceilings: Vec<Instant>` (one per interval) stay private; in-module tests inspect them directly.
- Produces (crate-private) `fn binary_search_desc(xs: &[Instant], needle: Instant) -> Result<usize, usize>` — search in a **descending** slice; `Ok(idx)` on match, `Err(insertion_point)` otherwise. (Replaces Kotlin's `-left - 1` encoding with `Result`.)

- [ ] **Step 1: Write the failing test**

`crates/varve-index/src/bitemporal.rs` (test module; the Kotlin known answers translated 1:1 — `Long.MAX_VALUE` → `Instant::END_OF_TIME`, `Long.MIN_VALUE` → `Instant::MIN`):
```rust
#[cfg(test)]
mod tests {
    use super::*;

    pub(super) const EOT: Instant = Instant::END_OF_TIME;
    pub(super) const TMIN: Instant = Instant::MIN;

    pub(super) fn us(n: i64) -> Instant {
        Instant::from_micros(n)
    }

    fn ts(ns: &[i64]) -> Vec<Instant> {
        ns.iter().copied().map(Instant::from_micros).collect()
    }

    #[test]
    fn binary_search_descending() {
        let list = ts(&[10, 8, 6, 4, 2]);
        assert_eq!(binary_search_desc(&list, us(10)), Ok(0));
        assert_eq!(binary_search_desc(&list, us(6)), Ok(2));
        assert_eq!(binary_search_desc(&list, us(2)), Ok(4));
        assert_eq!(binary_search_desc(&list, us(9)), Err(1));
        assert_eq!(binary_search_desc(&list, us(11)), Err(0));
        assert_eq!(binary_search_desc(&list, us(3)), Err(4));
        assert_eq!(binary_search_desc(&list, us(1)), Err(5));
    }

    #[test]
    fn ceiling_index_selects_the_covering_range() {
        // XTDB CeilingTest.testGetCeilingIndex: only valid_times matters here.
        let ceiling = Ceiling { valid_times: ts(&[10, 8, 6, 4, 2]), sys_time_ceilings: vec![] };
        assert_eq!(ceiling.ceiling_index(us(1)), 0);
        assert_eq!(ceiling.ceiling_index(us(2)), 0);
        assert_eq!(ceiling.ceiling_index(us(10)), 4);
        assert_eq!(ceiling.ceiling_index(us(11)), 4);
        assert_eq!(ceiling.ceiling_index(us(5)), 1);
    }

    #[test]
    fn applies_logs() {
        // XTDB CeilingTest.testAppliesLogs, step by step.
        let mut ceiling = Ceiling::new();
        assert_eq!(ceiling.valid_times, vec![EOT, TMIN]);
        assert_eq!(ceiling.sys_time_ceilings, vec![EOT]);

        ceiling.apply_log(us(4), us(4), EOT);
        assert_eq!(ceiling.valid_times, vec![EOT, us(4), TMIN]);
        assert_eq!(ceiling.sys_time_ceilings, vec![us(4), EOT]);

        // lower the whole ceiling
        ceiling.apply_log(us(3), us(2), EOT);
        assert_eq!(ceiling.valid_times, vec![EOT, us(2), TMIN]);
        assert_eq!(ceiling.sys_time_ceilings, vec![us(3), EOT]);

        // lower part of the ceiling
        ceiling.apply_log(us(2), us(1), us(4));
        assert_eq!(ceiling.valid_times, vec![EOT, us(4), us(1), TMIN]);
        assert_eq!(ceiling.sys_time_ceilings, vec![us(3), us(2), EOT]);

        // replace a range exactly
        ceiling.apply_log(us(1), us(1), us(4));
        assert_eq!(ceiling.valid_times, vec![EOT, us(4), us(1), TMIN]);
        assert_eq!(ceiling.sys_time_ceilings, vec![us(3), us(1), EOT]);

        // replace the whole middle section
        ceiling.apply_log(us(0), us(0), us(6));
        assert_eq!(ceiling.valid_times, vec![EOT, us(6), us(0), TMIN]);
        assert_eq!(ceiling.sys_time_ceilings, vec![us(3), us(0), EOT]);
    }

    #[test]
    fn replace_within_a_range() {
        // XTDB CeilingTest."test replace within a range"
        let mut ceiling = Ceiling::new();
        ceiling.apply_log(us(4), us(4), us(6));
        assert_eq!(ceiling.valid_times, vec![EOT, us(6), us(4), TMIN]);
        assert_eq!(ceiling.sys_time_ceilings, vec![EOT, us(4), EOT]);
    }

    #[test]
    fn empty_valid_range_is_a_no_op() {
        let mut ceiling = Ceiling::new();
        ceiling.apply_log(us(4), us(5), us(5));
        assert_eq!(ceiling.valid_times, vec![EOT, TMIN]);
        assert_eq!(ceiling.sys_time_ceilings, vec![EOT]);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p varve-index bitemporal`
Expected: compile error — module not defined.

- [ ] **Step 3: Write minimal implementation**

Prepend to `crates/varve-index/src/bitemporal.rs`:
```rust
use varve_types::Instant;

/// Binary search in a DESCENDING slice. `Ok(idx)` on match, `Err(insertion)`
/// otherwise. Port of XTDB `Ceiling.kt` `binarySearch` (its `-left - 1`
/// not-found encoding becomes `Err`).
fn binary_search_desc(xs: &[Instant], needle: Instant) -> Result<usize, usize> {
    let (mut left, mut right) = (0usize, xs.len());
    while left < right {
        let mid = (left + right) / 2;
        match xs[mid].cmp(&needle) {
            std::cmp::Ordering::Equal => return Ok(mid),
            std::cmp::Ordering::Greater => left = mid + 1,
            std::cmp::Ordering::Less => right = mid,
        }
    }
    Err(left)
}

/// The descending staircase of "system time above which this valid range is
/// superseded", maintained while scanning one entity's events newest-first
/// (spec §7). Port of XTDB `Ceiling.kt`.
///
/// `valid_times` is a descending boundary list bounded by the sentinels
/// `END_OF_TIME … MIN`; `sys_time_ceilings[i]` is the ceiling of the interval
/// `[valid_times[i + 1], valid_times[i])`. Range indices used by the public
/// accessors count from the OLDEST valid time upward (Kotlin `reverseIdx`).
pub struct Ceiling {
    valid_times: Vec<Instant>,
    sys_time_ceilings: Vec<Instant>,
}

impl Default for Ceiling {
    fn default() -> Self {
        Self::new()
    }
}

impl Ceiling {
    pub fn new() -> Self {
        let mut ceiling = Ceiling { valid_times: Vec::new(), sys_time_ceilings: Vec::new() };
        ceiling.reset();
        ceiling
    }

    pub fn reset(&mut self) {
        self.valid_times.clear();
        self.valid_times.extend([Instant::END_OF_TIME, Instant::MIN]);
        self.sys_time_ceilings.clear();
        self.sys_time_ceilings.push(Instant::END_OF_TIME);
    }

    fn reverse_idx(&self, idx: usize) -> usize {
        self.valid_times.len() - 1 - idx
    }

    pub fn valid_to(&self, range_idx: usize) -> Instant {
        self.valid_times[self.reverse_idx(range_idx + 1)]
    }

    pub fn system_time(&self, range_idx: usize) -> Instant {
        self.sys_time_ceilings[self.reverse_idx(range_idx) - 1]
    }

    /// Index of the range containing `valid_time` (in oldest-upward order).
    pub fn ceiling_index(&self, valid_time: Instant) -> usize {
        let mut idx = match binary_search_desc(&self.valid_times, valid_time) {
            Ok(i) | Err(i) => i,
        };
        if idx < self.valid_times.len() - 1 && valid_time < self.valid_times[idx] {
            idx += 1;
        }
        if idx == self.valid_times.len() {
            idx -= 1;
        }
        self.reverse_idx(idx)
    }

    /// Record that `[valid_from, valid_to)` is superseded above `system_from`.
    /// Port of `Ceiling.applyLog` — same case analysis, same order of operations.
    pub fn apply_log(&mut self, system_from: Instant, valid_from: Instant, valid_to: Instant) {
        if valid_from >= valid_to {
            return;
        }

        let (end, inserted_end) = match binary_search_desc(&self.valid_times, valid_to) {
            Ok(i) => (i, false),
            Err(i) => (i, true),
        };
        let (mut start, inserted_start) = match binary_search_desc(&self.valid_times, valid_from) {
            Ok(i) => (i, false),
            Err(i) => (i, true),
        };

        match (inserted_end, inserted_start) {
            (false, false) => {
                self.sys_time_ceilings[end] = system_from;
            }
            (false, true) => {
                self.valid_times.insert(start, valid_from);
                self.sys_time_ceilings.insert(end, system_from);
            }
            (true, false) => {
                self.valid_times.insert(end, valid_to);
                self.sys_time_ceilings.insert(end, system_from);
                start += 1;
            }
            (true, true) if end == start => {
                self.valid_times.insert(end, valid_to);
                self.sys_time_ceilings.insert(end, system_from);
                start += 1;
                self.valid_times.insert(start, valid_from);
                // end >= 1 always: valid_to can never insert above the
                // END_OF_TIME sentinel at index 0.
                let above = self.sys_time_ceilings[end - 1];
                self.sys_time_ceilings.insert(start, above);
            }
            (true, true) => {
                self.valid_times.insert(end, valid_to);
                self.sys_time_ceilings.insert(end, system_from);
                self.valid_times[start] = valid_from;
            }
        }

        // Collapse boundaries swallowed by [valid_from, valid_to).
        self.valid_times.drain(end + 1..start);
        self.sys_time_ceilings.drain(end + 1..start);
    }
}
```

Update `crates/varve-index/src/lib.rs`:
```rust
pub mod bitemporal;
pub mod live;

pub use bitemporal::Ceiling;
pub use live::{IndexError, LiveTable};
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p varve-index`
Expected: 5 new tests pass (plus the 3 existing `live` tests).

- [ ] **Step 5: Commit**

```bash
git add crates/varve-index/
git commit -m "feat: port XTDB Ceiling — valid-time staircase of system-time ceilings"
```

---

### Task 3: Port XTDB Polygon (with recency)

**Files:**
- Modify: `crates/varve-index/src/bitemporal.rs`
- Modify: `crates/varve-index/src/lib.rs`
- Test: in-module `#[cfg(test)]`

**Interfaces:**
- Consumes: `Ceiling` from Task 2 (`ceiling_index`, `valid_to`, `system_time`, `apply_log`).
- Produces `varve_index::Polygon` — one event's effective bitemporal rectangle set, computed against the ceiling (spec §7). Port of `Polygon.kt`; behavior pinned by porting `PolygonTest.kt`.
  - `Polygon::default()` — empty
  - `fn calculate_for(&mut self, ceiling: &Ceiling, valid_from: Instant, valid_to: Instant)` — requires `valid_from < valid_to` (callers guard; Task 4's `resolve` skips empty-valid events)
  - `fn range_count(&self) -> usize`; per range `i` (valid-time ascending): `fn valid_from(&self, i) -> Instant`, `fn valid_to(&self, i) -> Instant`, `fn system_to(&self, i) -> Instant` — rectangle `i` is `[valid_from(i), valid_to(i)) × [event.system_from, system_to(i))`
  - `fn recency(&self) -> Instant` — youngest instant at which the event still matters; drives current/historical file routing in slices 4/8 (spec §9). Requires `range_count() >= 1`.
  - Internal fields `valid_times: Vec<Instant>` (ascending here) and `sys_time_ceilings: Vec<Instant>` stay private; in-module tests construct instances directly.

- [ ] **Step 1: Write the failing test**

Append to the `tests` module in `crates/varve-index/src/bitemporal.rs` (translated 1:1 from XTDB `PolygonTest.kt`):
```rust
    fn apply_event(
        polygon: &mut Polygon,
        ceiling: &mut Ceiling,
        sys_from: Instant,
        valid_from: Instant,
        valid_to: Instant,
    ) {
        polygon.calculate_for(ceiling, valid_from, valid_to);
        ceiling.apply_log(sys_from, valid_from, valid_to);
    }

    fn polygon_of(valid_times: &[Instant], sys_time_ceilings: &[Instant]) -> Polygon {
        Polygon {
            valid_times: valid_times.to_vec(),
            sys_time_ceilings: sys_time_ceilings.to_vec(),
        }
    }

    #[test]
    fn calculate_for_empty_ceiling() {
        let (mut polygon, mut ceiling) = (Polygon::default(), Ceiling::new());
        apply_event(&mut polygon, &mut ceiling, us(0), us(2), us(3));
        assert_eq!(polygon.valid_times, vec![us(2), us(3)]);
        assert_eq!(polygon.sys_time_ceilings, vec![EOT]);
    }

    #[test]
    fn starts_before_no_overlap() {
        let (mut polygon, mut ceiling) = (Polygon::default(), Ceiling::new());
        apply_event(&mut polygon, &mut ceiling, us(1), us(2005), us(2009));
        assert_eq!(polygon.valid_times, vec![us(2005), us(2009)]);
        assert_eq!(polygon.sys_time_ceilings, vec![EOT]);

        apply_event(&mut polygon, &mut ceiling, us(0), us(2010), us(2020));
        assert_eq!(polygon.valid_times, vec![us(2010), us(2020)]);
        assert_eq!(polygon.sys_time_ceilings, vec![EOT]);
    }

    #[test]
    fn starts_before_and_overlaps() {
        let (mut polygon, mut ceiling) = (Polygon::default(), Ceiling::new());
        apply_event(&mut polygon, &mut ceiling, us(1), us(2010), us(2020));
        apply_event(&mut polygon, &mut ceiling, us(0), us(2015), us(2025));
        assert_eq!(polygon.valid_times, vec![us(2015), us(2020), us(2025)]);
        assert_eq!(polygon.sys_time_ceilings, vec![us(1), EOT]);
    }

    #[test]
    fn starts_equally_and_overlaps() {
        let (mut polygon, mut ceiling) = (Polygon::default(), Ceiling::new());
        apply_event(&mut polygon, &mut ceiling, us(1), us(2010), us(2020));
        apply_event(&mut polygon, &mut ceiling, us(0), us(2010), us(2025));
        assert_eq!(polygon.valid_times, vec![us(2010), us(2020), us(2025)]);
        assert_eq!(polygon.sys_time_ceilings, vec![us(1), EOT]);
    }

    #[test]
    fn newer_period_completely_covered() {
        let (mut polygon, mut ceiling) = (Polygon::default(), Ceiling::new());
        apply_event(&mut polygon, &mut ceiling, us(1), us(2015), us(2020));
        apply_event(&mut polygon, &mut ceiling, us(0), us(2010), us(2025));
        assert_eq!(polygon.valid_times, vec![us(2010), us(2015), us(2020), us(2025)]);
        assert_eq!(polygon.sys_time_ceilings, vec![EOT, us(1), EOT]);
    }

    #[test]
    fn older_period_completely_covered() {
        let (mut polygon, mut ceiling) = (Polygon::default(), Ceiling::new());
        apply_event(&mut polygon, &mut ceiling, us(1), us(2010), us(2025));
        apply_event(&mut polygon, &mut ceiling, us(0), us(2010), us(2020));
        assert_eq!(polygon.valid_times, vec![us(2010), us(2020)]);
        assert_eq!(polygon.sys_time_ceilings, vec![us(1)]);
    }

    #[test]
    fn period_ends_equally_and_overlaps() {
        let (mut polygon, mut ceiling) = (Polygon::default(), Ceiling::new());
        apply_event(&mut polygon, &mut ceiling, us(1), us(2015), us(2025));
        apply_event(&mut polygon, &mut ceiling, us(0), us(2010), us(2025));
        assert_eq!(polygon.valid_times, vec![us(2010), us(2015), us(2025)]);
        assert_eq!(polygon.sys_time_ceilings, vec![EOT, us(1)]);
    }

    #[test]
    fn period_ends_after_and_overlaps() {
        let (mut polygon, mut ceiling) = (Polygon::default(), Ceiling::new());
        apply_event(&mut polygon, &mut ceiling, us(1), us(2015), us(2025));
        apply_event(&mut polygon, &mut ceiling, us(0), us(2010), us(2020));
        assert_eq!(polygon.valid_times, vec![us(2010), us(2015), us(2020)]);
        assert_eq!(polygon.sys_time_ceilings, vec![EOT, us(1)]);
    }

    #[test]
    fn period_starts_before_and_touches() {
        let (mut polygon, mut ceiling) = (Polygon::default(), Ceiling::new());
        apply_event(&mut polygon, &mut ceiling, us(1), us(2005), us(2010));
        apply_event(&mut polygon, &mut ceiling, us(0), us(2010), us(2020));
        assert_eq!(polygon.valid_times, vec![us(2010), us(2020)]);
        assert_eq!(polygon.sys_time_ceilings, vec![EOT]);
    }

    #[test]
    fn period_starts_after_and_touches() {
        let (mut polygon, mut ceiling) = (Polygon::default(), Ceiling::new());
        apply_event(&mut polygon, &mut ceiling, us(1), us(2010), us(2020));
        apply_event(&mut polygon, &mut ceiling, us(0), us(2005), us(2010));
        assert_eq!(polygon.valid_times, vec![us(2005), us(2010)]);
        assert_eq!(polygon.sys_time_ceilings, vec![EOT]);
    }

    #[test]
    fn period_starts_after_and_does_not_overlap() {
        let (mut polygon, mut ceiling) = (Polygon::default(), Ceiling::new());
        apply_event(&mut polygon, &mut ceiling, us(1), us(2010), us(2020));
        apply_event(&mut polygon, &mut ceiling, us(0), us(2005), us(2009));
        assert_eq!(polygon.valid_times, vec![us(2005), us(2009)]);
        assert_eq!(polygon.sys_time_ceilings, vec![EOT]);
    }

    #[test]
    fn time_series_prefix_stays_visible() {
        // XTDB PolygonTest.testTimeSeries
        let (mut polygon, mut ceiling) = (Polygon::default(), Ceiling::new());
        ceiling.apply_log(us(10), us(10), us(12));
        ceiling.apply_log(us(8), us(8), us(10));
        ceiling.apply_log(us(6), us(6), us(8));
        assert_eq!(ceiling.valid_times, vec![EOT, us(12), us(10), us(8), us(6), TMIN]);
        assert_eq!(ceiling.sys_time_ceilings, vec![EOT, us(10), us(8), us(6), EOT]);

        apply_event(&mut polygon, &mut ceiling, us(4), us(4), us(6));
        assert_eq!(polygon.valid_times, vec![us(4), us(6)]);
        assert_eq!(polygon.sys_time_ceilings, vec![EOT]);
    }

    #[test]
    fn single_rectangle_recency() {
        assert_eq!(polygon_of(&[us(3), EOT], &[EOT]).recency(), EOT, "current");
        assert_eq!(polygon_of(&[us(4), us(10)], &[EOT]).recency(), us(10), "put for range");
        assert_eq!(polygon_of(&[us(6), us(10)], &[us(4)]).recency(), us(4), "vt=tt passes above");
        assert_eq!(polygon_of(&[us(6), us(10)], &[us(6)]).recency(), us(6), "touches top-left");
        assert_eq!(polygon_of(&[us(6), us(10)], &[us(8)]).recency(), us(8), "hits the top");
        assert_eq!(polygon_of(&[us(6), us(10)], &[us(10)]).recency(), us(10), "touches top-right");
        assert_eq!(polygon_of(&[us(6), us(10)], &[us(12)]).recency(), us(10), "hits the RHS");
    }

    #[test]
    fn multi_rectangle_recency() {
        assert_eq!(polygon_of(&[us(3), us(5), EOT], &[EOT, us(5)]).recency(), us(5));
        assert_eq!(polygon_of(&[us(3), us(5), EOT], &[EOT, us(6)]).recency(), us(6));
        assert_eq!(polygon_of(&[us(3), us(7), EOT], &[EOT, us(6)]).recency(), us(7));
        assert_eq!(polygon_of(&[us(1), us(4)], &[us(5)]).recency(), us(4));
        assert_eq!(polygon_of(&[us(10), us(12), us(15), us(18)], &[us(8), us(6), us(3)]).recency(), us(8));
        assert_eq!(polygon_of(&[us(10), us(12), us(15), us(18)], &[us(6), us(8), us(3)]).recency(), us(8));
        assert_eq!(polygon_of(&[us(0), us(2), us(5), us(8)], &[us(7), us(4), us(2)]).recency(), us(4));
        assert_eq!(polygon_of(&[us(100), us(100), us(5), us(8)], &[us(100), us(9), us(6)]).recency(), us(6));
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p varve-index bitemporal`
Expected: compile error — `Polygon` not defined.

- [ ] **Step 3: Write minimal implementation**

Add to `crates/varve-index/src/bitemporal.rs` (below `Ceiling`):
```rust
/// One event's effective bitemporal rectangle set, computed against the
/// ceiling. `valid_times` is ASCENDING here (unlike `Ceiling`); rectangle `i`
/// spans `[valid_times[i], valid_times[i + 1])` in valid time and ends at
/// `sys_time_ceilings[i]` in system time. Port of XTDB `Polygon.kt`.
#[derive(Default)]
pub struct Polygon {
    valid_times: Vec<Instant>,
    sys_time_ceilings: Vec<Instant>,
}

impl Polygon {
    pub fn range_count(&self) -> usize {
        self.sys_time_ceilings.len()
    }

    pub fn valid_from(&self, range_idx: usize) -> Instant {
        self.valid_times[range_idx]
    }

    pub fn valid_to(&self, range_idx: usize) -> Instant {
        self.valid_times[range_idx + 1]
    }

    pub fn system_to(&self, range_idx: usize) -> Instant {
        self.sys_time_ceilings[range_idx]
    }

    /// Split `[valid_from, valid_to)` by the ceiling's boundaries; each
    /// sub-range's system ceiling becomes this event's derived `_system_to`.
    /// Requires `valid_from < valid_to`.
    pub fn calculate_for(&mut self, ceiling: &Ceiling, valid_from: Instant, valid_to: Instant) {
        debug_assert!(valid_from < valid_to);
        self.valid_times.clear();
        self.sys_time_ceilings.clear();

        let mut valid_time = valid_from;
        let mut ceil_idx = ceiling.ceiling_index(valid_from);

        loop {
            let mut ceil_valid_to = ceiling.valid_to(ceil_idx);
            while ceil_valid_to <= valid_time {
                ceil_idx += 1;
                ceil_valid_to = ceiling.valid_to(ceil_idx);
            }

            self.valid_times.push(valid_time);
            self.sys_time_ceilings.push(ceiling.system_time(ceil_idx));

            valid_time = ceil_valid_to.min(valid_to);
            if valid_time == valid_to {
                break;
            }
        }
        self.valid_times.push(valid_time);
    }

    /// Youngest instant at which this event still matters — the maximum T
    /// where the event is visible somewhere with both valid-time >= T and
    /// system-time >= T. Drives current/historical routing (spec §9).
    /// Requires `range_count() >= 1`.
    pub fn recency(&self) -> Instant {
        let n = self.range_count();
        let mut recency = Instant::MIN;
        let mut valid_to = self.valid_to(n - 1);

        // Start from the RHS; stop early once recency can't grow.
        for i in (0..n).rev() {
            recency = recency.max(self.system_to(i).min(valid_to));
            let valid_from = self.valid_from(i);
            if recency >= valid_from {
                return recency;
            }
            valid_to = valid_from;
        }
        recency
    }
}
```

Update the re-export line in `crates/varve-index/src/lib.rs`:
```rust
pub use bitemporal::{Ceiling, Polygon};
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p varve-index`
Expected: all pass (14 new since Task 2 started, plus existing).

- [ ] **Step 5: Commit**

```bash
git add crates/varve-index/
git commit -m "feat: port XTDB Polygon with recency"
```

---

### Task 4: Event model and per-entity resolution

**Files:**
- Create: `crates/varve-index/src/event.rs`
- Modify: `crates/varve-index/src/bitemporal.rs` (add `ResolvedVersion`, `resolve`)
- Modify: `crates/varve-index/src/lib.rs`
- Test: in-module `#[cfg(test)]`

**Interfaces:**
- Produces `varve_index::{Event, Op}` (spec §5.2 — the unit of storage):

```rust
#[derive(Debug, Clone, PartialEq)]
pub enum Op {
    Put { labels: Vec<String>, doc: Doc },
    Delete,
    Erase,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Event {
    pub iid: Iid,
    pub system_from: Instant,
    pub valid_from: Instant,
    pub valid_to: Instant,
    pub op: Op,
}
```

By convention `Op::Erase` events carry `valid_from: Instant::MIN, valid_to: Instant::END_OF_TIME` (an erase removes the whole entity).

- Produces `varve_index::{resolve, ResolvedVersion}`:

```rust
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedVersion<'a> {
    pub event: &'a Event, // always an Op::Put
    pub valid_from: Instant,
    pub valid_to: Instant,
    pub system_to: Instant, // derived, never stored (spec §5.2)
}

/// Resolve one entity's events against `bounds`. `events` must be in arrival
/// (log) order: ascending system_from, ties broken by arrival. Returns visible
/// Put versions (rectangles intersecting `bounds`), newest-system-first, valid
/// ascending within an event. Port of XTDB `PolygonCalculator.calculate` for a
/// single entity.
pub fn resolve<'a>(events: &'a [Event], bounds: &TemporalBounds) -> Vec<ResolvedVersion<'a>>;
```

Semantics (each pinned by a test below):
1. Events are processed newest-first (reverse arrival). Within the same `system_from`, the later arrival is newer — last write in a batch wins.
2. An `Erase` stops processing: the erase and everything at-or-before it is invisible **regardless of the query's system bounds** (GDPR: you cannot time-travel to before an erase). Puts after the erase are visible again.
3. Non-erase events with `system_from >= bounds.system.upper` are skipped entirely — invisible to this snapshot AND not applied to the ceiling (they must not supersede in a time-traveling query).
4. `Delete` events supersede (apply to the ceiling) but emit no rows.
5. Rectangles with `system_to <= system_from` are dropped (fully superseded, e.g. same-system-time batches).
6. Events with `valid_from >= valid_to` are skipped defensively (engine validates upstream).

- [ ] **Step 1: Write the failing test**

Append to the `tests` module in `crates/varve-index/src/bitemporal.rs`:
```rust
    use crate::event::{Event, Op};
    use varve_types::{Doc, Iid, TemporalBounds, TemporalDimension, Value};

    fn iid1() -> Iid {
        Iid::derive("g", "nodes", &[1])
    }

    fn put(sf: i64, vf: Instant, vt: Instant, seq: i64) -> Event {
        let mut doc = Doc::new();
        doc.insert("seq".into(), Value::Int(seq));
        Event {
            iid: iid1(),
            system_from: us(sf),
            valid_from: vf,
            valid_to: vt,
            op: Op::Put { labels: vec!["P".into()], doc },
        }
    }

    fn delete(sf: i64, vf: Instant, vt: Instant) -> Event {
        Event { iid: iid1(), system_from: us(sf), valid_from: vf, valid_to: vt, op: Op::Delete }
    }

    fn erase(sf: i64) -> Event {
        Event { iid: iid1(), system_from: us(sf), valid_from: TMIN, valid_to: EOT, op: Op::Erase }
    }

    fn all_bounds() -> TemporalBounds {
        TemporalBounds::default()
    }

    fn point(valid: i64, system: i64) -> TemporalBounds {
        TemporalBounds {
            valid: TemporalDimension::at(us(valid)),
            system: TemporalDimension::at(us(system)),
        }
    }

    fn rects(versions: &[ResolvedVersion<'_>]) -> Vec<(i64, i64, i64, i64)> {
        versions
            .iter()
            .map(|v| {
                (
                    v.valid_from.as_micros(),
                    v.valid_to.as_micros(),
                    v.event.system_from.as_micros(),
                    v.system_to.as_micros(),
                )
            })
            .collect()
    }

    #[test]
    fn newer_event_splits_older_events_rectangles() {
        // Older put covers [0, 20); newer put supersedes [10, ∞) from system 100.
        let events = vec![put(50, us(0), us(20), 0), put(100, us(10), EOT, 1)];
        let versions = resolve(&events, &all_bounds());
        assert_eq!(
            rects(&versions),
            vec![
                (10, EOT.as_micros(), 100, EOT.as_micros()), // newer: untouched
                (0, 10, 50, EOT.as_micros()),                // older: still current below 10
                (10, 20, 50, 100),                           // older: superseded at system 100
            ]
        );
    }

    #[test]
    fn system_time_travel_ignores_newer_events() {
        let events = vec![put(50, us(0), us(20), 0), put(100, us(10), EOT, 1)];
        // A snapshot at system 60 never saw the newer event — and must not let
        // it supersede the older one.
        let versions = resolve(
            &events,
            &TemporalBounds {
                valid: TemporalDimension::all(),
                system: TemporalDimension::at(us(60)),
            },
        );
        assert_eq!(rects(&versions), vec![(0, 20, 50, EOT.as_micros())]);
    }

    #[test]
    fn same_system_time_batch_last_write_wins() {
        let events = vec![put(5, us(0), EOT, 0), put(5, us(0), EOT, 1)];
        let versions = resolve(&events, &all_bounds());
        // seq 1 is visible; seq 0's rectangle has system_to == system_from and is dropped.
        assert_eq!(versions.len(), 1);
        assert_eq!(rects(&versions), vec![(0, EOT.as_micros(), 5, EOT.as_micros())]);
        let Op::Put { doc, .. } = &versions[0].event.op else { panic!() };
        assert_eq!(doc.get("seq"), Some(&Value::Int(1)));
    }

    #[test]
    fn delete_truncates_visibility() {
        let events = vec![put(5, us(0), EOT, 0), delete(10, us(0), EOT)];
        assert_eq!(rects(&resolve(&events, &all_bounds())), vec![(0, EOT.as_micros(), 5, 10)]);
        assert!(resolve(&events, &point(1, 12)).is_empty()); // after the delete
        assert_eq!(resolve(&events, &point(1, 7)).len(), 1); // before the delete
    }

    #[test]
    fn range_delete_splits_the_put() {
        // Delete only valid range [3, 6) at system 10.
        let events = vec![put(5, us(0), EOT, 0), delete(10, us(3), us(6))];
        assert_eq!(
            rects(&resolve(&events, &all_bounds())),
            vec![
                (0, 3, 5, EOT.as_micros()),
                (3, 6, 5, 10),
                (6, EOT.as_micros(), 5, EOT.as_micros()),
            ]
        );
    }

    #[test]
    fn erase_hides_the_entity_even_from_time_travel() {
        let events = vec![put(5, us(0), EOT, 0), erase(10), put(15, us(0), EOT, 2)];
        // Only the post-erase put survives — at every system time.
        let versions = resolve(&events, &all_bounds());
        assert_eq!(versions.len(), 1);
        let Op::Put { doc, .. } = &versions[0].event.op else { panic!() };
        assert_eq!(doc.get("seq"), Some(&Value::Int(2)));
        // Time travel to before the erase still sees nothing of the erased history.
        assert!(resolve(&events[..2], &point(1, 7)).is_empty());
    }

    #[test]
    fn bounds_filter_rectangles() {
        let events = vec![put(5, us(10), us(20), 0)];
        assert!(resolve(&events, &point(25, 7)).is_empty()); // valid axis misses
        assert_eq!(resolve(&events, &point(15, 7)).len(), 1);
        assert!(resolve(&events, &point(15, 3)).is_empty()); // before it existed
    }

    #[test]
    fn empty_valid_range_events_are_ignored() {
        let events = vec![put(5, us(3), us(3), 0)];
        assert!(resolve(&events, &all_bounds()).is_empty());
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p varve-index bitemporal`
Expected: compile error — `event` module / `resolve` not defined.

- [ ] **Step 3: Write minimal implementation**

`crates/varve-index/src/event.rs`:
```rust
use varve_types::{Doc, Iid, Instant};

/// The operation carried by an event (spec §5.2). Node labels ride in the Put
/// alongside the document (spec §5.1 label set).
#[derive(Debug, Clone, PartialEq)]
pub enum Op {
    Put { labels: Vec<String>, doc: Doc },
    Delete,
    Erase,
}

/// Every mutation becomes an immutable event; `_system_to` and effective
/// valid ranges are never stored — always derived at read time (spec §5.2).
/// `Op::Erase` events carry `valid_from: Instant::MIN, valid_to:
/// Instant::END_OF_TIME` by convention (an erase removes the whole entity).
#[derive(Debug, Clone, PartialEq)]
pub struct Event {
    pub iid: Iid,
    pub system_from: Instant,
    pub valid_from: Instant,
    pub valid_to: Instant,
    pub op: Op,
}
```

Add to `crates/varve-index/src/bitemporal.rs`:
```rust
use crate::event::{Event, Op};
use varve_types::TemporalBounds;

/// A visible version of an entity: one Put event's rectangle that intersects
/// the query bounds. `system_to` is derived by resolution, never stored.
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedVersion<'a> {
    pub event: &'a Event,
    pub valid_from: Instant,
    pub valid_to: Instant,
    pub system_to: Instant,
}

/// Resolve one entity's events against `bounds`. `events` must be in arrival
/// (log) order: ascending `system_from`, ties broken by arrival order.
///
/// Iterates newest-system-first (reverse arrival, so a batch's last write is
/// newest). Single-entity port of XTDB `PolygonCalculator.calculate`.
pub fn resolve<'a>(events: &'a [Event], bounds: &TemporalBounds) -> Vec<ResolvedVersion<'a>> {
    let mut out = Vec::new();
    let mut ceiling = Ceiling::new();
    let mut polygon = Polygon::default();

    for event in events.iter().rev() {
        // An erase kills itself and everything older — deliberately BEFORE the
        // system-bounds check: erased history is gone at every system time.
        if matches!(event.op, Op::Erase) {
            break;
        }
        // Events after the snapshot's system upper bound don't exist for this
        // query — and must not supersede older events either.
        if event.system_from >= bounds.system.upper {
            continue;
        }
        // Defensive: empty valid ranges affect nothing (engine validates upstream).
        if event.valid_from >= event.valid_to {
            continue;
        }

        polygon.calculate_for(&ceiling, event.valid_from, event.valid_to);
        ceiling.apply_log(event.system_from, event.valid_from, event.valid_to);

        if let Op::Put { .. } = event.op {
            for i in 0..polygon.range_count() {
                let (valid_from, valid_to, system_to) =
                    (polygon.valid_from(i), polygon.valid_to(i), polygon.system_to(i));
                // Fully superseded (e.g. an earlier write in a same-system-time batch).
                if system_to <= event.system_from {
                    continue;
                }
                if bounds.intersects(valid_from, valid_to, event.system_from, system_to) {
                    out.push(ResolvedVersion { event, valid_from, valid_to, system_to });
                }
            }
        }
    }
    out
}
```

Update `crates/varve-index/src/lib.rs`:
```rust
pub mod bitemporal;
pub mod event;
pub mod live;

pub use bitemporal::{resolve, Ceiling, Polygon, ResolvedVersion};
pub use event::{Event, Op};
pub use live::{IndexError, LiveTable};
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p varve-index`
Expected: all pass (8 new resolution tests).

- [ ] **Step 5: Commit**

```bash
git add crates/varve-index/
git commit -m "feat: bitemporal event model and per-entity resolution"
```

---

### Task 5: varve-testkit — reference model + proptest equivalence harness

**Files:**
- Create: `crates/varve-testkit/Cargo.toml`
- Create: `crates/varve-testkit/src/lib.rs`
- Create: `crates/varve-testkit/src/reference.rs`
- Create: `crates/varve-testkit/src/strategy.rs`
- Create: `crates/varve-testkit/tests/equivalence.rs`
- Modify: `.github/workflows/ci.yml` (nightly property job)
- Test: in-module `#[cfg(test)]` + `tests/equivalence.rs`

**Interfaces:**
- Consumes: `varve_index::{resolve, Event, Op}`, `varve_types::{Doc, Iid, Instant, TemporalBounds, TemporalDimension, Value}`.
- Produces `varve_testkit::reference::ReferenceStore` — the naive `BTreeMap`-based bitemporal store (spec §7 correctness harness). Visibility **by definition**, no shared code with the engine:

```rust
pub struct ReferenceStore { /* BTreeMap<Iid, Vec<Event>> in arrival order */ }

impl ReferenceStore {
    pub fn new() -> Self;
    pub fn append(&mut self, event: Event);
    /// The Put event visible for `iid` at bitemporal point (valid, system), or
    /// None. Definition: ignore everything at-or-before the last Erase; among
    /// remaining events with system_from <= system whose valid range contains
    /// `valid`, the winner is the latest by (system_from, arrival); a Delete
    /// winner means "not visible".
    pub fn visible_at(&self, iid: Iid, valid: Instant, system: Instant) -> Option<&Event>;
}
```

- Produces `varve_testkit::strategy`:

```rust
pub fn entity_iid(n: u8) -> Iid;                                        // Iid::derive("g", "nodes", &[n])
pub fn arb_history(max_events: usize) -> impl Strategy<Value = Vec<Event>>;
pub fn arb_bounds() -> impl Strategy<Value = TemporalBounds>;
```

`arb_history` generates log-ordered histories over ≤3 entities with instants from a small pool (0..12 µs): weighted Put/Delete/Erase ops, valid ranges `[a, b)` or `[a, ∞)`, and **non-decreasing** `system_from` with 0-deltas producing same-system-time batches. Every Put doc carries a unique `seq: Int` property identifying the event. Retroactive corrections arise naturally (valid times independent of system times).

- CI: the equivalence tests read `PROPTEST_CASES` (default **10_000**); a scheduled nightly job runs them with `PROPTEST_CASES=200000 --release`.

- [ ] **Step 1: Write the failing unit tests for the reference model**

`crates/varve-testkit/src/reference.rs` (test module first):
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use varve_types::Value;

    const EOT: Instant = Instant::END_OF_TIME;

    fn us(n: i64) -> Instant {
        Instant::from_micros(n)
    }

    fn iid1() -> Iid {
        Iid::derive("g", "nodes", &[1])
    }

    fn put(sf: i64, vf: Instant, vt: Instant, seq: i64) -> Event {
        let mut doc = Doc::new();
        doc.insert("seq".into(), Value::Int(seq));
        Event {
            iid: iid1(),
            system_from: us(sf),
            valid_from: vf,
            valid_to: vt,
            op: Op::Put { labels: vec!["P".into()], doc },
        }
    }

    fn seq_of(event: &Event) -> i64 {
        match &event.op {
            Op::Put { doc, .. } => match doc.get("seq") {
                Some(Value::Int(i)) => *i,
                _ => -1,
            },
            _ => -1,
        }
    }

    #[test]
    fn latest_system_time_wins() {
        let mut store = ReferenceStore::new();
        store.append(put(5, us(0), EOT, 0));
        store.append(put(10, us(0), EOT, 1));
        assert_eq!(store.visible_at(iid1(), us(1), us(12)).map(seq_of), Some(1));
        assert_eq!(store.visible_at(iid1(), us(1), us(7)).map(seq_of), Some(0));
        assert_eq!(store.visible_at(iid1(), us(1), us(3)), None);
    }

    #[test]
    fn arrival_order_breaks_system_time_ties() {
        let mut store = ReferenceStore::new();
        store.append(put(5, us(0), EOT, 0));
        store.append(put(5, us(0), EOT, 1));
        assert_eq!(store.visible_at(iid1(), us(1), us(5)).map(seq_of), Some(1));
    }

    #[test]
    fn delete_winner_hides() {
        let mut store = ReferenceStore::new();
        store.append(put(5, us(0), EOT, 0));
        store.append(Event {
            iid: iid1(),
            system_from: us(10),
            valid_from: us(0),
            valid_to: EOT,
            op: Op::Delete,
        });
        assert_eq!(store.visible_at(iid1(), us(1), us(12)), None);
        assert_eq!(store.visible_at(iid1(), us(1), us(7)).map(seq_of), Some(0));
    }

    #[test]
    fn erase_kills_history_at_every_system_time() {
        let mut store = ReferenceStore::new();
        store.append(put(5, us(0), EOT, 0));
        store.append(Event {
            iid: iid1(),
            system_from: us(10),
            valid_from: Instant::MIN,
            valid_to: EOT,
            op: Op::Erase,
        });
        store.append(put(15, us(0), EOT, 2));
        assert_eq!(store.visible_at(iid1(), us(1), us(7)), None); // pre-erase time travel
        assert_eq!(store.visible_at(iid1(), us(1), us(20)).map(seq_of), Some(2));
    }

    #[test]
    fn valid_range_must_contain_the_point() {
        let mut store = ReferenceStore::new();
        store.append(put(5, us(10), us(20), 0));
        assert_eq!(store.visible_at(iid1(), us(9), us(7)), None);
        assert_eq!(store.visible_at(iid1(), us(10), us(7)).map(seq_of), Some(0));
        assert_eq!(store.visible_at(iid1(), us(20), us(7)), None); // half-open
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p varve-testkit`
Expected: compile error — crate does not exist yet.

- [ ] **Step 3: Write the crate and minimal implementation**

`crates/varve-testkit/Cargo.toml`:
```toml
[package]
name = "varve-testkit"
version.workspace = true
edition.workspace = true
license.workspace = true

[dependencies]
proptest = { workspace = true }
varve-index = { path = "../varve-index" }
varve-types = { path = "../varve-types" }

[lints]
workspace = true
```

`crates/varve-testkit/src/lib.rs`:
```rust
pub mod reference;
pub mod strategy;

pub use reference::ReferenceStore;
```

Prepend to `crates/varve-testkit/src/reference.rs`:
```rust
use std::collections::BTreeMap;
use varve_index::{Event, Op};
use varve_types::{Doc, Iid, Instant};

/// Naive bitemporal store: visibility computed from first principles on every
/// query. The correctness oracle for the vectorized engine (spec §7) — keep it
/// obvious, never optimize it.
#[derive(Default)]
pub struct ReferenceStore {
    events: BTreeMap<Iid, Vec<Event>>, // arrival (log) order per entity
}

impl ReferenceStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn append(&mut self, event: Event) {
        self.events.entry(event.iid).or_default().push(event);
    }

    /// The Put event visible for `iid` at (valid, system), or None.
    pub fn visible_at(&self, iid: Iid, valid: Instant, system: Instant) -> Option<&Event> {
        let events = self.events.get(&iid)?;
        // Everything at-or-before the last Erase is gone — at every system time.
        let alive = match events.iter().rposition(|e| matches!(e.op, Op::Erase)) {
            Some(i) => &events[i + 1..],
            None => &events[..],
        };
        // Arrival order is ascending (system_from, arrival), so the last
        // candidate is the winner by (system_from, arrival).
        let winner = alive
            .iter()
            .filter(|e| e.system_from <= system)
            .filter(|e| e.valid_from <= valid && valid < e.valid_to)
            .next_back()?;
        match winner.op {
            Op::Put { .. } => Some(winner),
            Op::Delete | Op::Erase => None,
        }
    }
}
```

(`Doc` is used by the test module; keep the import.)

- [ ] **Step 4: Run reference tests to verify they pass**

Run: `cargo test -p varve-testkit`
Expected: 5 passed.

- [ ] **Step 5: Write the strategies and the failing equivalence properties**

`crates/varve-testkit/src/strategy.rs`:
```rust
use proptest::prelude::*;
use varve_index::{Event, Op};
use varve_types::{Doc, Iid, Instant, TemporalBounds, TemporalDimension, Value};

/// Instants are drawn from 0..T_POOL µs so histories collide heavily.
pub const T_POOL: i64 = 12;

pub fn entity_iid(n: u8) -> Iid {
    Iid::derive("g", "nodes", &[n])
}

fn arb_instant() -> impl Strategy<Value = Instant> {
    (0..T_POOL).prop_map(Instant::from_micros)
}

fn ordered_pair() -> impl Strategy<Value = (Instant, Instant)> {
    (0..T_POOL - 1).prop_flat_map(|a| {
        ((a + 1)..T_POOL)
            .prop_map(move |b| (Instant::from_micros(a), Instant::from_micros(b)))
    })
}

fn arb_valid_range() -> impl Strategy<Value = (Instant, Instant)> {
    prop_oneof![
        3 => ordered_pair(),
        2 => (0..T_POOL).prop_map(|a| (Instant::from_micros(a), Instant::END_OF_TIME)),
    ]
}

#[derive(Debug, Clone)]
enum OpKind {
    Put,
    Delete,
    Erase,
}

fn arb_op_kind() -> impl Strategy<Value = OpKind> {
    prop_oneof![
        8 => Just(OpKind::Put),
        3 => Just(OpKind::Delete),
        1 => Just(OpKind::Erase),
    ]
}

/// A log-ordered history: ≤3 entities, non-decreasing system_from (0-deltas =
/// same-system-time batches), valid ranges independent of system times (so
/// retroactive corrections arise naturally). Each Put doc carries a unique
/// `seq` identifying the event.
pub fn arb_history(max_events: usize) -> impl Strategy<Value = Vec<Event>> {
    prop::collection::vec((0..3u8, arb_op_kind(), arb_valid_range(), 0i64..=2), 1..=max_events)
        .prop_map(|specs| {
            let mut system = 0i64;
            specs
                .into_iter()
                .enumerate()
                .map(|(seq, (entity, kind, (valid_from, valid_to), delta))| {
                    system += delta;
                    let (valid_from, valid_to, op) = match kind {
                        OpKind::Put => {
                            let mut doc = Doc::new();
                            doc.insert("seq".into(), Value::Int(seq as i64));
                            (valid_from, valid_to, Op::Put { labels: vec!["P".into()], doc })
                        }
                        OpKind::Delete => (valid_from, valid_to, Op::Delete),
                        OpKind::Erase => (Instant::MIN, Instant::END_OF_TIME, Op::Erase),
                    };
                    Event {
                        iid: entity_iid(entity),
                        system_from: Instant::from_micros(system),
                        valid_from,
                        valid_to,
                        op,
                    }
                })
                .collect()
        })
}

fn arb_dimension() -> impl Strategy<Value = TemporalDimension> {
    prop_oneof![
        arb_instant().prop_map(TemporalDimension::at),
        ordered_pair().prop_map(|(a, b)| TemporalDimension::in_range(a, b)),
        ordered_pair().prop_map(|(a, b)| TemporalDimension::between(a, b)),
        Just(TemporalDimension::all()),
    ]
}

pub fn arb_bounds() -> impl Strategy<Value = TemporalBounds> {
    (arb_dimension(), arb_dimension())
        .prop_map(|(valid, system)| TemporalBounds { valid, system })
}
```

`crates/varve-testkit/tests/equivalence.rs`:
```rust
use proptest::prelude::*;
use std::collections::BTreeMap;
use varve_index::{resolve, Event, Op};
use varve_testkit::strategy::{arb_bounds, arb_history};
use varve_testkit::ReferenceStore;
use varve_types::{Iid, Instant, TemporalBounds, TemporalDimension, Value};

fn cases() -> u32 {
    // 10k in CI; the nightly job raises this via PROPTEST_CASES (roadmap slice 2).
    std::env::var("PROPTEST_CASES").ok().and_then(|s| s.parse().ok()).unwrap_or(10_000)
}

fn point(valid: Instant, system: Instant) -> TemporalBounds {
    TemporalBounds {
        valid: TemporalDimension::at(valid),
        system: TemporalDimension::at(system),
    }
}

fn seq_of(event: &Event) -> i64 {
    match &event.op {
        Op::Put { doc, .. } => match doc.get("seq") {
            Some(Value::Int(i)) => *i,
            _ => -1,
        },
        _ => -1,
    }
}

/// Probe values per axis: every event boundary and its +1 neighbour, plus 0
/// and a far-future instant. Rectangle corners can only sit on event
/// boundaries, so agreement on this grid implies agreement everywhere.
fn probes(events: &[Event]) -> (Vec<Instant>, Vec<Instant>) {
    let far = Instant::from_micros(1_000_000);
    let mut valid = vec![Instant::from_micros(0), far];
    let mut system = vec![Instant::from_micros(0), far];
    for e in events {
        for t in [e.valid_from, e.valid_to] {
            if t > Instant::MIN && t < Instant::END_OF_TIME {
                valid.push(t);
                valid.push(Instant::from_micros(t.as_micros() + 1));
            }
        }
        system.push(e.system_from);
        system.push(Instant::from_micros(e.system_from.as_micros() + 1));
    }
    valid.sort();
    valid.dedup();
    system.sort();
    system.dedup();
    (valid, system)
}

fn by_iid(history: &[Event]) -> BTreeMap<Iid, Vec<Event>> {
    let mut map: BTreeMap<Iid, Vec<Event>> = BTreeMap::new();
    for e in history {
        map.entry(e.iid).or_default().push(e.clone());
    }
    map
}

proptest! {
    #![proptest_config(ProptestConfig { cases: cases(), ..ProptestConfig::default() })]

    #[test]
    fn engine_matches_reference_on_the_full_grid(history in arb_history(16)) {
        let mut reference = ReferenceStore::new();
        for e in &history {
            reference.append(e.clone());
        }
        for (iid, events) in by_iid(&history) {
            let (valids, systems) = probes(&events);
            for &v in &valids {
                for &s in &systems {
                    let versions = resolve(&events, &point(v, s));
                    prop_assert!(
                        versions.len() <= 1,
                        "point query returned {} versions at v={v} s={s}",
                        versions.len()
                    );
                    let engine = versions.first().map(|r| seq_of(r.event));
                    let oracle = reference.visible_at(iid, v, s).map(seq_of);
                    prop_assert_eq!(engine, oracle, "iid={:?} v={} s={}", iid, v, s);
                }
            }
        }
    }

    #[test]
    fn emitted_versions_are_disjoint_and_intersect_bounds(
        history in arb_history(16),
        bounds in arb_bounds(),
    ) {
        for (_iid, events) in by_iid(&history) {
            let versions = resolve(&events, &bounds);
            for v in &versions {
                prop_assert!(v.valid_from < v.valid_to);
                prop_assert!(v.event.system_from < v.system_to);
                prop_assert!(bounds.intersects(
                    v.valid_from, v.valid_to, v.event.system_from, v.system_to
                ));
            }
            for (i, a) in versions.iter().enumerate() {
                for b in &versions[i + 1..] {
                    let overlap = a.valid_from < b.valid_to
                        && b.valid_from < a.valid_to
                        && a.event.system_from < b.system_to
                        && b.event.system_from < a.system_to;
                    prop_assert!(!overlap, "overlapping visible versions");
                }
            }
        }
    }
}
```

- [ ] **Step 6: Run the equivalence properties**

Run: `cargo test -p varve-testkit`
Expected: all pass — 5 reference tests + 2 properties × 10_000 cases (expect tens of seconds in debug; if a case fails, proptest prints the minimal counterexample: FIX THE ENGINE, never the oracle, unless the oracle provably contradicts the semantics in Task 4's Interfaces block).

- [ ] **Step 7: Add the nightly CI property job**

Modify `.github/workflows/ci.yml` — extend `on:` and add a job (final file):
```yaml
name: CI
on:
  push:
    branches: [main]
  pull_request:
  schedule:
    - cron: "0 3 * * *"

env:
  CARGO_TERM_COLOR: always

jobs:
  check:
    if: github.event_name != 'schedule'
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
        with:
          components: rustfmt, clippy
      - uses: Swatinem/rust-cache@v2
      - run: cargo fmt --all --check
      - run: cargo clippy --workspace --all-targets -- -D warnings
      - run: cargo test --workspace

  property-nightly:
    if: github.event_name == 'schedule'
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - uses: Swatinem/rust-cache@v2
      - run: cargo test -p varve-testkit --release
        env:
          PROPTEST_CASES: "200000"
```

- [ ] **Step 8: Run the full gate**

Run: `just check`
Expected: green (fmt, clippy, all workspace tests including the new crate).

- [ ] **Step 9: Commit**

```bash
git add crates/varve-testkit/ .github/workflows/ci.yml Cargo.toml Cargo.lock
git commit -m "feat: varve-testkit reference model with proptest equivalence harness"
```

---

### Task 6: Temporal LiveTable — resolved snapshots with a TemporalBounds filter

**Files:**
- Rewrite: `crates/varve-index/src/live.rs`
- Modify: `crates/varve-plan/src/exec.rs` + `crates/varve-plan/Cargo.toml` (adapt to the new API; `now` parameter)
- Modify: `crates/varve-plan/tests/exec_test.rs`
- Modify: `crates/varve-engine/src/db.rs` (adapt; interim counter-clock)
- Test: in-module `#[cfg(test)]` + `exec_test.rs`

**Interfaces:**
- Consumes: `Event`, `Op`, `resolve` (Task 4); `Instant`, `TemporalBounds` (Task 1).
- Produces the v1 `varve_index::LiveTable` (REPLACES the v0 API — we are in development, no backward compatibility):

```rust
impl LiveTable {
    pub fn new() -> Self;
    /// Append an event. Events must arrive in log order: `system_from` must be
    /// >= every previously appended event's (ties allowed — same-tx batches).
    pub fn append(&mut self, event: Event) -> Result<(), IndexError>;
    pub fn event_count(&self) -> usize;
    /// Resolve all entities against `bounds` and snapshot the visible versions
    /// carrying `label` into one RecordBatch. Returns None when nothing is visible.
    /// Schema: _iid FixedSizeBinary(16), then _system_from/_system_to/_valid_from/
    /// _valid_to as Timestamp(µs, "UTC") (all non-null), then one nullable column
    /// per property observed across visible docs (same type rules as v0).
    pub fn snapshot_for_label(
        &self,
        label: &str,
        bounds: &TemporalBounds,
    ) -> Result<Option<RecordBatch>, IndexError>;
}
```

- `IndexError` gains `#[error("event appended out of order: system_from {got} precedes {last}")] OutOfOrderEvent { last: Instant, got: Instant }`. The `MixedPropertyTypes` message now says "(lifted with dense-union columns in slice 4)".
- Storage: `BTreeMap<Iid, Vec<Event>>` in arrival order — reverse iteration per entity yields the `(iid, system_from desc)` scan order the roadmap requires; BTreeMap gives deterministic whole-table iteration by IID.
- Produces the split query API in `varve-plan` (slice-1 review remediation: `Db::query` must not hold its lock across an await):

```rust
/// Sync phase: resolve + snapshot under the caller's lock. Bounds are the
/// spec §7 defaults (at(now) on both axes) until Task 7 wires FOR clauses.
pub fn snapshot_for_query(stmt: &QueryStmt, live: &LiveTable, now: Instant)
    -> Result<Option<RecordBatch>, PlanError>;
/// Async phase: DataFusion filter/projection over an OWNED snapshot —
/// callers drop their live-table lock before awaiting this.
pub async fn execute_query(stmt: &QueryStmt, snapshot: Option<RecordBatch>)
    -> Result<Vec<RecordBatch>, PlanError>;
/// One-shot convenience for tests and non-locking callers.
pub async fn run_query(stmt: &QueryStmt, live: &LiveTable, now: Instant)
    -> Result<Vec<RecordBatch>, PlanError>;
```

- `varve_engine::Db::query` snapshots under the read guard in an inner block, drops it, then awaits `execute_query` — its `#[allow(clippy::await_holding_lock)]` is DELETED. `Db::execute` keeps the slice-1 atomicity fix: build + validate every event BEFORE the first append. Interim system-time source: `system_from = Instant::from_micros(tx_id)`, query `now` = latest tx counter value; replaced by the real `MonotonicClock` in Task 8 (keeps Task 6 reviewable on its own; slice-1 tests stay green throughout).

- [ ] **Step 1: Write the failing tests**

Replace the test module in `crates/varve-index/src/live.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{Event, Op};
    use arrow::array::{Array, Int64Array, StringArray, TimestampMicrosecondArray};
    use arrow::datatypes::{DataType, TimeUnit};
    use varve_types::{Doc, Iid, Instant, TemporalBounds, TemporalDimension, Value};

    const EOT: Instant = Instant::END_OF_TIME;

    fn us(n: i64) -> Instant {
        Instant::from_micros(n)
    }

    fn iid(n: u8) -> Iid {
        Iid::derive("g", "nodes", &[n])
    }

    fn doc(pairs: &[(&str, Value)]) -> Doc {
        pairs.iter().map(|(k, v)| (k.to_string(), v.clone())).collect()
    }

    fn put(entity: u8, sf: i64, vf: i64, label: &str, d: Doc) -> Event {
        Event {
            iid: iid(entity),
            system_from: us(sf),
            valid_from: us(vf),
            valid_to: EOT,
            op: Op::Put { labels: vec![label.into()], doc: d },
        }
    }

    fn now_bounds(n: i64) -> TemporalBounds {
        TemporalBounds {
            valid: TemporalDimension::at(us(n)),
            system: TemporalDimension::at(us(n)),
        }
    }

    fn ada_and_bob() -> LiveTable {
        let mut t = LiveTable::new();
        t.append(put(1, 1, 1, "Person", doc(&[("name", Value::Str("Ada".into())), ("age", Value::Int(36))])))
            .unwrap();
        t.append(put(2, 2, 2, "Person", doc(&[("name", Value::Str("Bob".into()))])))
            .unwrap();
        t
    }

    #[test]
    fn current_snapshot_shows_one_version_per_entity() {
        let mut t = ada_and_bob();
        // Ada renamed at time 10: only the new version is current.
        t.append(put(1, 10, 10, "Person", doc(&[("name", Value::Str("Adele".into()))])))
            .unwrap();
        let batch = t.snapshot_for_label("Person", &now_bounds(50)).unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);
        let names: &StringArray =
            batch.column_by_name("name").unwrap().as_any().downcast_ref().unwrap();
        let mut got: Vec<String> = (0..2).map(|i| names.value(i).to_string()).collect();
        got.sort();
        assert_eq!(got, vec!["Adele", "Bob"]);
    }

    #[test]
    fn all_bounds_expose_history_with_derived_system_to() {
        let mut t = LiveTable::new();
        t.append(put(1, 1, 0, "P", doc(&[("v", Value::Int(1))]))).unwrap();
        t.append(put(1, 5, 0, "P", doc(&[("v", Value::Int(2))]))).unwrap();
        let batch = t.snapshot_for_label("P", &TemporalBounds::default()).unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);
        let v: &Int64Array = batch.column_by_name("v").unwrap().as_any().downcast_ref().unwrap();
        let st: &TimestampMicrosecondArray =
            batch.column_by_name("_system_to").unwrap().as_any().downcast_ref().unwrap();
        // Output is deterministic: newest version first per entity.
        assert_eq!(v.value(0), 2);
        assert_eq!(st.value(0), EOT.as_micros());
        assert_eq!(v.value(1), 1);
        assert_eq!(st.value(1), 5); // superseded at system time 5 — derived, never stored
    }

    #[test]
    fn temporal_columns_have_utc_microsecond_type() {
        let t = ada_and_bob();
        let batch = t.snapshot_for_label("Person", &now_bounds(50)).unwrap().unwrap();
        for col in ["_system_from", "_system_to", "_valid_from", "_valid_to"] {
            let field = batch.schema().field_with_name(col).unwrap().clone();
            assert_eq!(
                field.data_type(),
                &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                "{col}"
            );
            assert!(!field.is_nullable(), "{col}");
        }
    }

    #[test]
    fn deleted_entities_disappear_at_the_right_system_time() {
        let mut t = ada_and_bob();
        t.append(Event {
            iid: iid(1),
            system_from: us(10),
            valid_from: us(10),
            valid_to: EOT,
            op: Op::Delete,
        })
        .unwrap();
        let batch = t.snapshot_for_label("Person", &now_bounds(50)).unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1); // only Bob
        let before = t.snapshot_for_label("Person", &now_bounds(5)).unwrap().unwrap();
        assert_eq!(before.num_rows(), 2); // time travel to before the delete
    }

    #[test]
    fn label_filter_applies_to_the_visible_version() {
        let mut t = ada_and_bob();
        t.append(put(3, 3, 3, "City", doc(&[("name", Value::Str("Oslo".into()))]))).unwrap();
        assert_eq!(t.snapshot_for_label("City", &now_bounds(50)).unwrap().unwrap().num_rows(), 1);
        assert!(t.snapshot_for_label("Robot", &now_bounds(50)).unwrap().is_none());
    }

    #[test]
    fn out_of_order_append_rejected() {
        let mut t = ada_and_bob(); // last system_from == 2
        let err = t.append(put(3, 1, 1, "P", Doc::new())).unwrap_err();
        assert!(matches!(err, IndexError::OutOfOrderEvent { .. }));
        // Equal system_from is fine (same-tx batches).
        t.append(put(3, 2, 2, "P", Doc::new())).unwrap();
    }

    #[test]
    fn mixed_property_types_still_rejected() {
        let mut t = LiveTable::new();
        t.append(put(1, 1, 1, "P", doc(&[("x", Value::Int(1))]))).unwrap();
        t.append(put(2, 2, 2, "P", doc(&[("x", Value::Str("one".into()))]))).unwrap();
        assert!(matches!(
            t.snapshot_for_label("P", &now_bounds(50)),
            Err(IndexError::MixedPropertyTypes { .. })
        ));
    }

    // Deferred from slice 1 (STATUS.md remediation list).
    #[test]
    fn all_null_property_and_empty_doc_rows() {
        let mut t = LiveTable::new();
        t.append(put(1, 1, 1, "P", doc(&[("ghost", Value::Null)]))).unwrap();
        t.append(put(2, 2, 2, "P", Doc::new())).unwrap();
        let batch = t.snapshot_for_label("P", &now_bounds(50)).unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);
        // A property observed only as Null constrains no column type — no column.
        assert!(batch.column_by_name("ghost").is_none());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p varve-index live`
Expected: compile errors — v0 `append(iid, labels, doc)` signature gone.

- [ ] **Step 3: Rewrite the implementation**

Replace the implementation part of `crates/varve-index/src/live.rs`:
```rust
use crate::bitemporal::resolve;
use crate::event::{Event, Op};
use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, FixedSizeBinaryBuilder, Float64Builder, Int64Builder,
    StringBuilder, TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use std::collections::BTreeMap;
use std::sync::Arc;
use thiserror::Error;
use varve_types::{Doc, Iid, Instant, TemporalBounds, Value};

#[derive(Debug, Error)]
pub enum IndexError {
    #[error("property '{property}' has mixed types across rows (lifted with dense-union columns in slice 4)")]
    MixedPropertyTypes { property: String },
    #[error("event appended out of order: system_from {got} precedes {last}")]
    OutOfOrderEvent { last: Instant, got: Instant },
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
}

/// Live, in-memory event index. Events are stored per entity in arrival (log)
/// order — reverse iteration yields the (iid, system_from desc) scan order
/// resolution needs (spec §5.2); BTreeMap keeps whole-table iteration
/// deterministic by IID.
#[derive(Default)]
pub struct LiveTable {
    events: BTreeMap<Iid, Vec<Event>>,
    last_system_from: Option<Instant>,
    event_count: usize,
}

struct VisibleRow<'a> {
    iid: Iid,
    doc: &'a Doc,
    system_from: Instant,
    system_to: Instant,
    valid_from: Instant,
    valid_to: Instant,
}

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

impl LiveTable {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn append(&mut self, event: Event) -> Result<(), IndexError> {
        if let Some(last) = self.last_system_from {
            if event.system_from < last {
                return Err(IndexError::OutOfOrderEvent { last, got: event.system_from });
            }
        }
        self.last_system_from = Some(event.system_from);
        self.event_count += 1;
        self.events.entry(event.iid).or_default().push(event);
        Ok(())
    }

    pub fn event_count(&self) -> usize {
        self.event_count
    }

    pub fn snapshot_for_label(
        &self,
        label: &str,
        bounds: &TemporalBounds,
    ) -> Result<Option<RecordBatch>, IndexError> {
        let mut visible: Vec<VisibleRow<'_>> = Vec::new();
        for (iid, events) in &self.events {
            for version in resolve(events, bounds) {
                let Op::Put { labels, doc } = &version.event.op else {
                    continue; // resolve only emits Puts; defensive
                };
                if labels.iter().any(|l| l == label) {
                    visible.push(VisibleRow {
                        iid: *iid,
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
                            return Err(IndexError::MixedPropertyTypes { property: k.clone() })
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
            ("_system_from", (|r: &VisibleRow<'_>| r.system_from) as fn(&VisibleRow<'_>) -> Instant),
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

        Ok(Some(RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?))
    }
}
```

(API caveat: if the pinned arrow 58.3.0 spells the builder timezone helper differently — e.g. `with_timezone_opt` — adapt the implementation; the schema/type assertions in the tests are the contract.)

- [ ] **Step 4: Run the index tests**

Run: `cargo test -p varve-index`
Expected: live tests pass; the workspace does NOT compile yet (`varve-plan`/`varve-engine` still call the v0 API) — fixed in the next steps before this task commits.

- [ ] **Step 5: Adapt varve-plan (failing tests first)**

`crates/varve-plan/Cargo.toml` — move `varve-types` from dev-dependencies to `[dependencies]` (keep the dev-deps `tokio`, `arrow`).

Replace `crates/varve-plan/tests/exec_test.rs`:
```rust
#![allow(clippy::unwrap_used)] // tests may use unwrap; crate-level allow covers helper fns
use arrow::array::{Array, StringArray};
use varve_gql::ast::Statement;
use varve_index::{Event, LiveTable, Op};
use varve_plan::run_query;
use varve_types::{Doc, Iid, Instant, Value};

const NOW: Instant = Instant::from_micros(100);

fn person(n: u8, sf: i64, name: &str, age: i64) -> Event {
    let mut doc = Doc::new();
    doc.insert("name".into(), Value::Str(name.into()));
    doc.insert("age".into(), Value::Int(age));
    Event {
        iid: Iid::derive("g", "nodes", &[n]),
        system_from: Instant::from_micros(sf),
        valid_from: Instant::from_micros(sf),
        valid_to: Instant::END_OF_TIME,
        op: Op::Put { labels: vec!["Person".into()], doc },
    }
}

fn setup() -> LiveTable {
    let mut t = LiveTable::new();
    for (n, sf, name, age) in [(1u8, 1, "Ada", 36i64), (2, 2, "Bob", 41), (3, 3, "Cyd", 36)] {
        t.append(person(n, sf, name, age)).unwrap();
    }
    t
}

fn query_stmt(src: &str) -> varve_gql::ast::QueryStmt {
    match varve_gql::parse(src).unwrap() {
        Statement::Query(q) => q,
        _ => panic!("not a query"),
    }
}

fn names(batches: &[arrow::record_batch::RecordBatch]) -> Vec<String> {
    let mut out: Vec<String> = batches
        .iter()
        .flat_map(|b| {
            let col: &StringArray =
                b.column_by_name("name").unwrap().as_any().downcast_ref().unwrap();
            (0..col.len()).map(|i| col.value(i).to_string()).collect::<Vec<_>>()
        })
        .collect();
    out.sort();
    out
}

#[tokio::test]
async fn match_where_return_filters_rows() {
    let live = setup();
    let q = query_stmt("MATCH (p:Person) WHERE p.age = 36 RETURN p.name AS name");
    let batches = run_query(&q, &live, NOW).await.unwrap();
    assert_eq!(names(&batches), vec!["Ada", "Cyd"]);
}

#[tokio::test]
async fn unknown_label_returns_empty() {
    let live = setup();
    let q = query_stmt("MATCH (r:Robot) RETURN r.name");
    assert!(run_query(&q, &live, NOW).await.unwrap().is_empty());
}

#[tokio::test]
async fn current_query_sees_only_the_latest_version() {
    let mut live = setup();
    let mut doc = Doc::new();
    doc.insert("name".into(), Value::Str("Adele".into()));
    doc.insert("age".into(), Value::Int(36));
    live.append(Event {
        iid: Iid::derive("g", "nodes", &[1u8]),
        system_from: Instant::from_micros(10),
        valid_from: Instant::from_micros(10),
        valid_to: Instant::END_OF_TIME,
        op: Op::Put { labels: vec!["Person".into()], doc },
    })
    .unwrap();
    let q = query_stmt("MATCH (p:Person) WHERE p.age = 36 RETURN p.name AS name");
    let batches = run_query(&q, &live, NOW).await.unwrap();
    assert_eq!(names(&batches), vec!["Adele", "Cyd"]);
}

// Deferred from slice 1 (STATUS.md remediation list): both UnknownColumn paths.
#[tokio::test]
async fn where_and_return_on_absent_property_are_unknown_column() {
    let live = setup();
    let q = query_stmt("MATCH (p:Person) WHERE p.ghost = 1 RETURN p.name");
    assert!(matches!(
        run_query(&q, &live, NOW).await,
        Err(PlanError::UnknownColumn(c)) if c == "ghost"
    ));
    let q = query_stmt("MATCH (p:Person) RETURN p.ghost");
    assert!(matches!(
        run_query(&q, &live, NOW).await,
        Err(PlanError::UnknownColumn(c)) if c == "ghost"
    ));
}
```
(Add `use varve_plan::PlanError;` to the imports.)

Replace `run_query` in `crates/varve-plan/src/exec.rs` with the split API (the slice-1 review remediation — `to_df_literal` and `PlanError` stay as they are):
```rust
use varve_types::{Instant, TemporalBounds, TemporalDimension};

/// Sync phase: resolve + snapshot under the caller's lock. Bounds are the
/// spec §7 defaults — valid AS OF now, system AS OF now (the writer clock is
/// monotonic, so at(now) sees exactly the current versions). Task 7 derives
/// bounds from the statement's FOR clauses.
pub fn snapshot_for_query(
    stmt: &QueryStmt,
    live: &LiveTable,
    now: Instant,
) -> Result<Option<RecordBatch>, PlanError> {
    let bounds = TemporalBounds {
        valid: TemporalDimension::at(now),
        system: TemporalDimension::at(now),
    };
    let label = stmt.pattern.label.as_deref().unwrap_or("");
    Ok(live.snapshot_for_label(label, &bounds)?)
}

/// Async phase: DataFusion filter/projection over an OWNED snapshot — callers
/// drop their live-table lock before awaiting this.
pub async fn execute_query(
    stmt: &QueryStmt,
    snapshot: Option<RecordBatch>,
) -> Result<Vec<RecordBatch>, PlanError> {
    let Some(batch) = snapshot else {
        return Ok(vec![]);
    };
    let schema = batch.schema();
    let has_col = |name: &str| schema.column_with_name(name).is_some();

    let ctx = SessionContext::new();
    let table = MemTable::try_new(schema.clone(), vec![vec![batch]])?;
    let mut df = ctx.read_table(Arc::new(table))?;

    if let Some(Expr::PropEq { prop, value, .. }) = &stmt.where_clause {
        if !has_col(prop) {
            return Err(PlanError::UnknownColumn(prop.clone()));
        }
        df = df.filter(col(prop.as_str()).eq(to_df_literal(value)))?;
    }

    let mut projection = Vec::new();
    for item in &stmt.return_items {
        if !has_col(&item.prop) {
            return Err(PlanError::UnknownColumn(item.prop.clone()));
        }
        let out_name = item.alias.clone().unwrap_or_else(|| item.prop.clone());
        projection.push(col(item.prop.as_str()).alias(out_name));
    }
    let df = df.select(projection)?;

    Ok(df.collect().await?)
}

/// One-shot convenience for tests and non-locking callers.
pub async fn run_query(
    stmt: &QueryStmt,
    live: &LiveTable,
    now: Instant,
) -> Result<Vec<RecordBatch>, PlanError> {
    execute_query(stmt, snapshot_for_query(stmt, live, now)?).await
}
```

Update `crates/varve-plan/src/lib.rs`:
```rust
pub mod exec;

pub use exec::{execute_query, run_query, snapshot_for_query, PlanError};
```

- [ ] **Step 6: Adapt varve-engine (interim counter clock; lock split; atomicity preserved)**

In `crates/varve-engine/src/db.rs`:

Add imports:
```rust
use varve_index::{Event, Op};
use varve_types::Instant;
```

In `execute()`, KEEP the slice-1 two-phase shape (build + validate everything, then append — the post-review atomicity fix, pinned by `multi_node_insert_is_atomic_on_invalid_id`). Insert after the `tx_id` binding:
```rust
        // Interim system time = tx counter as µs; the real monotonic wall
        // clock lands with temporal mutations (Task 8 of the slice-2 plan).
        let system = Instant::from_micros(i64::try_from(tx_id).unwrap_or(i64::MAX));
```
change the phase-1 buffer to hold events — `rows.push((iid, node.labels.clone(), doc));` becomes:
```rust
            rows.push(Event {
                iid,
                system_from: system,
                valid_from: system,
                valid_to: Instant::END_OF_TIME,
                op: Op::Put { labels: node.labels.clone(), doc },
            });
```
and the phase-2 append loop becomes:
```rust
        let mut live = self.live.write().map_err(|_| EngineError::Poisoned)?;
        for event in rows {
            live.append(event)?;
        }
```

Replace `query()` entirely — the guard is dropped BEFORE the await, and the `#[allow(clippy::await_holding_lock)]` and its `// v0` comment are DELETED (slice-1 review remediation):
```rust
    /// Execute a read query, returning Arrow batches.
    pub async fn query(&self, gql: &str) -> Result<Vec<RecordBatch>, EngineError> {
        let Statement::Query(q) = varve_gql::parse(gql)? else {
            return Err(EngineError::NotAQuery);
        };
        let now = Instant::from_micros(
            i64::try_from(self.tx_counter.load(Ordering::SeqCst)).unwrap_or(i64::MAX),
        );
        // Snapshot under the read lock, drop the guard, then run DataFusion
        // on the owned batch — no await while holding the lock.
        let snapshot = {
            let live = self.live.read().map_err(|_| EngineError::Poisoned)?;
            varve_plan::snapshot_for_query(&q, &live, now)?
        };
        Ok(varve_plan::execute_query(&q, snapshot).await?)
    }
```

- [ ] **Step 7: Run the full gate**

Run: `just check`
Expected: green — index, plan, engine, walking-skeleton, testkit all pass (slice-1 tests prove current-time queries are just "AS OF now").

- [ ] **Step 8: Commit**

```bash
git add crates/
git commit -m "feat: temporal LiveTable — resolved snapshots behind a TemporalBounds filter"
```

---

### Task 7: GQL temporal query surface — FOR clauses and history functions

**Files:**
- Modify: `crates/varve-gql/Cargo.toml` (add `varve-types`)
- Modify: `crates/varve-gql/src/token.rs` (13 new keywords)
- Modify: `crates/varve-gql/src/ast.rs`
- Modify: `crates/varve-gql/src/parser.rs`
- Modify: `crates/varve-plan/src/exec.rs` (+ `crates/varve-plan/tests/exec_test.rs`)
- Test: in-module `#[cfg(test)]` + `exec_test.rs`

**Interfaces:**
- New `Keyword` variants: `For, ValidTime, SystemTime, Of, All, From, To, Between, And, Valid, Delete, Timestamp, Date` (mapped from `FOR, VALID_TIME, SYSTEM_TIME, OF, ALL, FROM, TO, BETWEEN, AND, VALID, DELETE, TIMESTAMP, DATE`; `VALID_TIME`/`SYSTEM_TIME` lex as single words — `_` is already an identifier char).
- AST v1 (`varve-gql` now depends on `varve-types`; parsed datetimes become `Instant`, temporal specs become `TemporalDimension` — the parser resolves syntax to semantics):

```rust
use varve_types::{Instant, TemporalDimension};

#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct TemporalClauses {
    pub valid: Option<TemporalDimension>,  // FOR VALID_TIME …
    pub system: Option<TemporalDimension>, // FOR SYSTEM_TIME …
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TemporalFnKind { ValidFrom, ValidTo, SystemFrom }

#[derive(Debug, Clone, PartialEq)]
pub enum ReturnItem {
    Prop { var: String, prop: String, alias: Option<String> },
    /// valid_from(x) / valid_to(x) / system_from(x) on a bound element.
    TemporalFn { func: TemporalFnKind, var: String, alias: Option<String> },
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueryStmt {
    pub temporal: TemporalClauses,       // query-level, before MATCH
    pub pattern: NodePattern,
    pub match_temporal: TemporalClauses, // per-MATCH, after the pattern; overrides per axis
    pub where_clause: Option<Expr>,
    pub return_items: Vec<ReturnItem>,
}
```

(`InsertStmt`/`DeleteStmt` changes land in Task 8 — this task is the read side only.)
- Grammar added (both clause positions, all four specs — spec §8 temporal extensions):

```
query        := for_clause* MATCH node_pattern for_clause* [WHERE prop_eq] RETURN return_items
for_clause   := FOR (VALID_TIME | SYSTEM_TIME) temporal_spec
temporal_spec:= AS OF datetime | FROM datetime TO datetime | BETWEEN datetime AND datetime | ALL
datetime     := TIMESTAMP '<rfc3339>' | DATE '<yyyy-mm-dd>'
return_item  := ident '.' ident [AS ident] | (valid_from|valid_to|system_from) '(' ident ')' [AS ident]
```

Duplicate `FOR VALID_TIME`/`FOR SYSTEM_TIME` in one position is an error; `FROM a TO b` requires `a < b`, `BETWEEN a AND b` requires `a <= b`.
- Produces in `varve-plan`: `run_query` now derives bounds per axis: per-MATCH clause, else query-level clause, else `at(now)` (defaults: valid AS OF now, system AS OF latest-visible — with a monotonic writer clock `at(now)` is exactly "latest"). `ReturnItem::TemporalFn` projects the hidden temporal columns: `valid_from → _valid_from`, `valid_to → _valid_to`, `system_from → _system_from`; default output names are the function names.

- [ ] **Step 1: Write the failing tokenizer test**

Append to the tests module in `crates/varve-gql/src/token.rs`:
```rust
    #[test]
    fn temporal_keywords_tokenize() {
        use Keyword::*;
        use TokenKind::*;
        assert_eq!(
            kinds("FOR VALID_TIME AS OF TIMESTAMP '2024-01-01T00:00:00Z'"),
            vec![Kw(For), Kw(ValidTime), Kw(As), Kw(Of), Kw(Timestamp), Str("2024-01-01T00:00:00Z".into()), Eof]
        );
        assert_eq!(
            kinds("for system_time from date '2020-01-01' to all between and valid delete"),
            vec![
                Kw(For), Kw(SystemTime), Kw(From), Kw(Date), Str("2020-01-01".into()),
                Kw(To), Kw(All), Kw(Between), Kw(And), Kw(Valid), Kw(Delete), Eof
            ]
        );
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p varve-gql token`
Expected: compile error — `Keyword::For` etc. not defined.

- [ ] **Step 3: Add the keywords**

In `crates/varve-gql/src/token.rs`, extend the `Keyword` enum:
```rust
    For,
    ValidTime,
    SystemTime,
    Of,
    All,
    From,
    To,
    Between,
    And,
    Valid,
    Delete,
    Timestamp,
    Date,
```
and the `keyword()` match:
```rust
        "FOR" => Some(Keyword::For),
        "VALID_TIME" => Some(Keyword::ValidTime),
        "SYSTEM_TIME" => Some(Keyword::SystemTime),
        "OF" => Some(Keyword::Of),
        "ALL" => Some(Keyword::All),
        "FROM" => Some(Keyword::From),
        "TO" => Some(Keyword::To),
        "BETWEEN" => Some(Keyword::Between),
        "AND" => Some(Keyword::And),
        "VALID" => Some(Keyword::Valid),
        "DELETE" => Some(Keyword::Delete),
        "TIMESTAMP" => Some(Keyword::Timestamp),
        "DATE" => Some(Keyword::Date),
```

Run: `cargo test -p varve-gql token` — Expected: PASS.

- [ ] **Step 4: Write the failing parser tests**

In `crates/varve-gql/src/parser.rs` tests — first UPDATE the two existing literals for the new `QueryStmt`/`ReturnItem` shapes:

In `parses_match_where_return`, the expected value becomes:
```rust
            Statement::Query(QueryStmt {
                temporal: TemporalClauses::default(),
                pattern: NodePattern { var: "p".into(), label: Some("Person".into()) },
                match_temporal: TemporalClauses::default(),
                where_clause: Some(Expr::PropEq {
                    var: "p".into(),
                    prop: "name".into(),
                    value: Literal::Str("Ada".into()),
                }),
                return_items: vec![
                    ReturnItem::Prop { var: "p".into(), prop: "name".into(), alias: Some("n".into()) },
                    ReturnItem::Prop { var: "p".into(), prop: "age".into(), alias: None },
                ],
            })
```

Then append the new tests (add `use varve_types::{Instant, TemporalDimension};` to the test module):
```rust
    fn ts(s: &str) -> Instant {
        Instant::parse_rfc3339(s).unwrap()
    }

    fn query(src: &str) -> QueryStmt {
        match parse(src).unwrap() {
            Statement::Query(q) => q,
            other => panic!("not a query: {other:?}"),
        }
    }

    #[test]
    fn parses_query_level_for_clauses() {
        let q = query(
            "FOR VALID_TIME AS OF TIMESTAMP '2024-01-01T00:00:00Z' \
             FOR SYSTEM_TIME AS OF TIMESTAMP '2025-01-01T00:00:00Z' \
             MATCH (p:Person) RETURN p.name",
        );
        assert_eq!(q.temporal.valid, Some(TemporalDimension::at(ts("2024-01-01T00:00:00Z"))));
        assert_eq!(q.temporal.system, Some(TemporalDimension::at(ts("2025-01-01T00:00:00Z"))));
        assert_eq!(q.match_temporal, TemporalClauses::default());
    }

    #[test]
    fn parses_per_match_for_clause() {
        let q = query("MATCH (p:Person) FOR VALID_TIME AS OF DATE '2024-01-01' RETURN p.name");
        assert_eq!(q.temporal, TemporalClauses::default());
        assert_eq!(q.match_temporal.valid, Some(TemporalDimension::at(ts("2024-01-01T00:00:00Z"))));
    }

    #[test]
    fn parses_range_and_all_specs() {
        let q = query(
            "FOR VALID_TIME FROM TIMESTAMP '2020-01-01T00:00:00Z' TO TIMESTAMP '2021-01-01T00:00:00Z' \
             FOR SYSTEM_TIME ALL MATCH (p:Person) RETURN p.name",
        );
        assert_eq!(
            q.temporal.valid,
            Some(TemporalDimension::in_range(ts("2020-01-01T00:00:00Z"), ts("2021-01-01T00:00:00Z")))
        );
        assert_eq!(q.temporal.system, Some(TemporalDimension::all()));

        let q = query(
            "FOR VALID_TIME BETWEEN DATE '2020-01-01' AND DATE '2021-01-01' \
             MATCH (p:Person) RETURN p.name",
        );
        assert_eq!(
            q.temporal.valid,
            Some(TemporalDimension::between(ts("2020-01-01T00:00:00Z"), ts("2021-01-01T00:00:00Z")))
        );
    }

    #[test]
    fn parses_temporal_functions_in_return() {
        let q = query("MATCH (p:Person) RETURN valid_from(p) AS since, valid_to(p), system_from(p)");
        assert_eq!(
            q.return_items,
            vec![
                ReturnItem::TemporalFn {
                    func: TemporalFnKind::ValidFrom,
                    var: "p".into(),
                    alias: Some("since".into())
                },
                ReturnItem::TemporalFn { func: TemporalFnKind::ValidTo, var: "p".into(), alias: None },
                ReturnItem::TemporalFn { func: TemporalFnKind::SystemFrom, var: "p".into(), alias: None },
            ]
        );
    }

    #[test]
    fn temporal_clause_errors() {
        // duplicate axis
        let err = parse(
            "FOR VALID_TIME ALL FOR VALID_TIME ALL MATCH (p:P) RETURN p.x",
        )
        .unwrap_err();
        assert!(err.to_string().contains("duplicate"), "{err}");
        // inverted range
        let err = parse(
            "FOR VALID_TIME FROM TIMESTAMP '2021-01-01T00:00:00Z' TO TIMESTAMP '2020-01-01T00:00:00Z' \
             MATCH (p:P) RETURN p.x",
        )
        .unwrap_err();
        assert!(err.to_string().contains("earlier"), "{err}");
        // bad timestamp literal
        let err = parse("FOR VALID_TIME AS OF TIMESTAMP 'nope' MATCH (p:P) RETURN p.x").unwrap_err();
        assert!(err.to_string().contains("invalid timestamp"), "{err}");
        // unknown function in RETURN
        let err = parse("MATCH (p:P) RETURN nonsense(p)").unwrap_err();
        assert!(err.to_string().contains("valid_from"), "{err}");
    }
```

- [ ] **Step 5: Run test to verify it fails**

Run: `cargo test -p varve-gql parser`
Expected: compile errors (`TemporalClauses` etc. undefined).

- [ ] **Step 6: Implement AST + parser**

`crates/varve-gql/Cargo.toml` — add:
```toml
varve-types = { path = "../varve-types" }
```

`crates/varve-gql/src/ast.rs` — add the types from the Interfaces block above (`TemporalClauses`, `TemporalFnKind`, replace `ReturnItem` struct with the enum, extend `QueryStmt` with `temporal`/`match_temporal`), keeping `Literal`, `InsertNode`, `InsertStmt`, `NodePattern`, `Expr`, `Statement` unchanged.

`crates/varve-gql/src/parser.rs` — add `use varve_types::{Instant, TemporalDimension};` and these methods to `impl Parser`; replace `query_stmt` and `return_item`:
```rust
    fn for_clauses(&mut self) -> Result<TemporalClauses, GqlError> {
        let mut clauses = TemporalClauses::default();
        while *self.peek() == TokenKind::Kw(Keyword::For) {
            self.pos += 1;
            let offset = self.offset();
            match self.bump() {
                TokenKind::Kw(Keyword::ValidTime) => {
                    let dim = self.temporal_spec()?;
                    if clauses.valid.replace(dim).is_some() {
                        return Err(GqlError::Parse {
                            offset,
                            msg: "duplicate FOR VALID_TIME clause".into(),
                        });
                    }
                }
                TokenKind::Kw(Keyword::SystemTime) => {
                    let dim = self.temporal_spec()?;
                    if clauses.system.replace(dim).is_some() {
                        return Err(GqlError::Parse {
                            offset,
                            msg: "duplicate FOR SYSTEM_TIME clause".into(),
                        });
                    }
                }
                other => {
                    return Err(GqlError::Parse {
                        offset,
                        msg: format!("expected VALID_TIME or SYSTEM_TIME after FOR, found {other:?}"),
                    })
                }
            }
        }
        Ok(clauses)
    }

    fn temporal_spec(&mut self) -> Result<TemporalDimension, GqlError> {
        let offset = self.offset();
        match self.peek().clone() {
            TokenKind::Kw(Keyword::As) => {
                self.pos += 1;
                self.expect(&TokenKind::Kw(Keyword::Of), "OF")?;
                Ok(TemporalDimension::at(self.datetime()?))
            }
            TokenKind::Kw(Keyword::From) => {
                self.pos += 1;
                let from = self.datetime()?;
                self.expect(&TokenKind::Kw(Keyword::To), "TO")?;
                let to = self.datetime()?;
                if from >= to {
                    return Err(GqlError::Parse {
                        offset,
                        msg: "FROM must be earlier than TO".into(),
                    });
                }
                Ok(TemporalDimension::in_range(from, to))
            }
            TokenKind::Kw(Keyword::Between) => {
                self.pos += 1;
                let from = self.datetime()?;
                self.expect(&TokenKind::Kw(Keyword::And), "AND")?;
                let to = self.datetime()?;
                if from > to {
                    return Err(GqlError::Parse {
                        offset,
                        msg: "BETWEEN start must be earlier than or equal to AND end".into(),
                    });
                }
                Ok(TemporalDimension::between(from, to))
            }
            TokenKind::Kw(Keyword::All) => {
                self.pos += 1;
                Ok(TemporalDimension::all())
            }
            other => Err(GqlError::Parse {
                offset,
                msg: format!("expected AS OF, FROM, BETWEEN, or ALL, found {other:?}"),
            }),
        }
    }

    fn datetime(&mut self) -> Result<Instant, GqlError> {
        let offset = self.offset();
        let parse_str = |parser: fn(&str) -> Result<Instant, varve_types::TypeError>,
                         token: TokenKind|
         -> Result<Instant, GqlError> {
            match token {
                TokenKind::Str(s) => {
                    parser(&s).map_err(|e| GqlError::Parse { offset, msg: e.to_string() })
                }
                other => Err(GqlError::Parse {
                    offset,
                    msg: format!("expected a quoted datetime literal, found {other:?}"),
                }),
            }
        };
        match self.bump() {
            TokenKind::Kw(Keyword::Timestamp) => parse_str(Instant::parse_rfc3339, self.bump()),
            TokenKind::Kw(Keyword::Date) => parse_str(Instant::parse_date, self.bump()),
            other => Err(GqlError::Parse {
                offset,
                msg: format!("expected TIMESTAMP '…' or DATE '…', found {other:?}"),
            }),
        }
    }

    fn query_stmt(&mut self, temporal: TemporalClauses) -> Result<QueryStmt, GqlError> {
        self.expect(&TokenKind::LParen, "'('")?;
        let var = self.ident("pattern variable")?;
        let label = if *self.peek() == TokenKind::Colon {
            self.pos += 1;
            Some(self.ident("label name")?)
        } else {
            None
        };
        self.expect(&TokenKind::RParen, "')'")?;

        let match_temporal = self.for_clauses()?;

        let where_clause = if *self.peek() == TokenKind::Kw(Keyword::Where) {
            self.pos += 1;
            Some(self.prop_eq_expr()?)
        } else {
            None
        };

        self.expect(&TokenKind::Kw(Keyword::Return), "RETURN")?;
        let mut return_items = vec![self.return_item()?];
        while *self.peek() == TokenKind::Comma {
            self.pos += 1;
            return_items.push(self.return_item()?);
        }
        self.expect(&TokenKind::Eof, "end of statement")?;
        Ok(QueryStmt {
            temporal,
            pattern: NodePattern { var, label },
            match_temporal,
            where_clause,
            return_items,
        })
    }

    fn return_item(&mut self) -> Result<ReturnItem, GqlError> {
        let offset = self.offset();
        let name = self.ident("variable or temporal function")?;
        if *self.peek() == TokenKind::LParen {
            let func = match name.as_str() {
                "valid_from" => TemporalFnKind::ValidFrom,
                "valid_to" => TemporalFnKind::ValidTo,
                "system_from" => TemporalFnKind::SystemFrom,
                other => {
                    return Err(GqlError::Parse {
                        offset,
                        msg: format!(
                            "unknown function '{other}' — expected valid_from, valid_to, or system_from"
                        ),
                    })
                }
            };
            self.pos += 1; // '('
            let var = self.ident("variable")?;
            self.expect(&TokenKind::RParen, "')'")?;
            let alias = self.alias()?;
            return Ok(ReturnItem::TemporalFn { func, var, alias });
        }
        self.expect(&TokenKind::Dot, "'.'")?;
        let prop = self.ident("property name")?;
        let alias = self.alias()?;
        Ok(ReturnItem::Prop { var: name, prop, alias })
    }

    fn alias(&mut self) -> Result<Option<String>, GqlError> {
        if *self.peek() == TokenKind::Kw(Keyword::As) {
            self.pos += 1;
            Ok(Some(self.ident("alias")?))
        } else {
            Ok(None)
        }
    }
```

And rewire `statement()` so a query may start with FOR:
```rust
    fn statement(&mut self) -> Result<Statement, GqlError> {
        match self.peek() {
            TokenKind::Kw(Keyword::Insert) => {
                self.pos += 1;
                self.insert_stmt().map(Statement::Insert)
            }
            TokenKind::Kw(Keyword::Match) | TokenKind::Kw(Keyword::For) => {
                let temporal = self.for_clauses()?;
                self.expect(&TokenKind::Kw(Keyword::Match), "MATCH")?;
                self.query_stmt(temporal).map(Statement::Query)
            }
            _ => Err(self.err("expected INSERT, MATCH, or FOR")),
        }
    }
```

Run: `cargo test -p varve-gql` — Expected: all parser + tokenizer tests pass. (`varve-plan`/`varve-engine` don't compile yet — next step.)

- [ ] **Step 7: Write the failing planner tests, then wire the planner**

Append to `crates/varve-plan/tests/exec_test.rs` (µs-scale instants exercise the literal path: `1970-01-01T00:00:00.000005Z` is 5 µs):
```rust
use arrow::array::TimestampMicrosecondArray;

#[tokio::test]
async fn for_system_time_as_of_travels_back() {
    let mut live = setup();
    let mut doc = Doc::new();
    doc.insert("name".into(), Value::Str("Adele".into()));
    doc.insert("age".into(), Value::Int(36));
    live.append(Event {
        iid: Iid::derive("g", "nodes", &[1u8]),
        system_from: Instant::from_micros(10),
        valid_from: Instant::from_micros(10),
        valid_to: Instant::END_OF_TIME,
        op: Op::Put { labels: vec!["Person".into()], doc },
    })
    .unwrap();

    // Snapshot as of system time 5µs: the rename at 10µs hasn't happened.
    let q = query_stmt(
        "FOR SYSTEM_TIME AS OF TIMESTAMP '1970-01-01T00:00:00.000005Z' \
         MATCH (p:Person) WHERE p.age = 36 RETURN p.name AS name",
    );
    let batches = run_query(&q, &live, NOW).await.unwrap();
    assert_eq!(names(&batches), vec!["Ada", "Cyd"]);

    // Per-MATCH placement behaves identically.
    let q = query_stmt(
        "MATCH (p:Person) FOR SYSTEM_TIME AS OF TIMESTAMP '1970-01-01T00:00:00.000005Z' \
         WHERE p.age = 36 RETURN p.name AS name",
    );
    let batches = run_query(&q, &live, NOW).await.unwrap();
    assert_eq!(names(&batches), vec!["Ada", "Cyd"]);
}

#[tokio::test]
async fn temporal_functions_project_hidden_columns() {
    let live = setup();
    let q = query_stmt(
        "MATCH (p:Person) WHERE p.name = 'Ada' \
         RETURN p.name AS name, valid_from(p) AS vf, valid_to(p), system_from(p)",
    );
    let batches = run_query(&q, &live, NOW).await.unwrap();
    let batch = &batches[0];
    // Default output names are the function names.
    let vf: &TimestampMicrosecondArray =
        batch.column_by_name("vf").unwrap().as_any().downcast_ref().unwrap();
    let vt: &TimestampMicrosecondArray =
        batch.column_by_name("valid_to").unwrap().as_any().downcast_ref().unwrap();
    let sf: &TimestampMicrosecondArray =
        batch.column_by_name("system_from").unwrap().as_any().downcast_ref().unwrap();
    assert_eq!(vf.value(0), 1); // Ada inserted at 1µs
    assert_eq!(vt.value(0), Instant::END_OF_TIME.as_micros());
    assert_eq!(sf.value(0), 1);
}
```

Run: `cargo test -p varve-plan` — Expected: FAIL (compile: `ReturnItem` is now an enum; FOR clauses ignored).

In `crates/varve-plan/src/exec.rs`, add the import `use varve_gql::ast::{Expr, Literal, QueryStmt, ReturnItem, TemporalFnKind};` and replace the hardcoded bounds and the projection loop:
```rust
/// Per axis: per-MATCH clause, else query-level clause, else AS OF now
/// (spec §7 defaults — the monotonic writer clock makes at(now) "latest").
fn effective_bounds(stmt: &QueryStmt, now: Instant) -> TemporalBounds {
    TemporalBounds {
        valid: stmt
            .match_temporal
            .valid
            .or(stmt.temporal.valid)
            .unwrap_or_else(|| TemporalDimension::at(now)),
        system: stmt
            .match_temporal
            .system
            .or(stmt.temporal.system)
            .unwrap_or_else(|| TemporalDimension::at(now)),
    }
}

/// (hidden column, default output name) for a temporal function.
fn temporal_fn_columns(func: TemporalFnKind) -> (&'static str, &'static str) {
    match func {
        TemporalFnKind::ValidFrom => ("_valid_from", "valid_from"),
        TemporalFnKind::ValidTo => ("_valid_to", "valid_to"),
        TemporalFnKind::SystemFrom => ("_system_from", "system_from"),
    }
}
```

In `snapshot_for_query`, replace `let bounds = TemporalBounds { … at(now) … };` (and its defaults comment) with:
```rust
    let bounds = effective_bounds(stmt, now);
```
and in `execute_query`, replace the projection loop with:
```rust
    let mut projection = Vec::new();
    for item in &stmt.return_items {
        let (source, out_name) = match item {
            ReturnItem::Prop { prop, alias, .. } => {
                if !has_col(prop) {
                    return Err(PlanError::UnknownColumn(prop.clone()));
                }
                (prop.as_str(), alias.clone().unwrap_or_else(|| prop.clone()))
            }
            ReturnItem::TemporalFn { func, alias, .. } => {
                let (hidden, default_name) = temporal_fn_columns(*func);
                (hidden, alias.clone().unwrap_or_else(|| default_name.to_string()))
            }
        };
        projection.push(col(source).alias(out_name));
    }
    let df = df.select(projection)?;
```

- [ ] **Step 8: Run the full gate**

Run: `just check`
Expected: green. (`varve-engine` compiles unchanged: it reads `ins.nodes` and constructs no `QueryStmt`/`ReturnItem` literals.)

- [ ] **Step 9: Commit**

```bash
git add crates/varve-gql/ crates/varve-plan/ Cargo.lock
git commit -m "feat: GQL temporal query surface — FOR clauses and history functions"
```

---

### Task 8: Temporal mutations — INSERT … VALID FROM/TO, DELETE, monotonic tx time

**Files:**
- Modify: `crates/varve-gql/src/ast.rs` + `crates/varve-gql/src/parser.rs`
- Modify: `crates/varve-plan/src/exec.rs` (add `matching_iids`)
- Create: `crates/varve-engine/src/clock.rs`
- Modify: `crates/varve-engine/src/db.rs`, `crates/varve-engine/src/lib.rs`, `crates/varve-engine/Cargo.toml`
- Test: in-module `#[cfg(test)]` + `crates/varve-engine/tests/mutations.rs`

**Interfaces:**
- AST additions (`InsertStmt` REPLACED — new fields; new `DeleteStmt`; new `Statement` variant):

```rust
#[derive(Debug, Clone, PartialEq)]
pub struct InsertStmt {
    pub nodes: Vec<InsertNode>,
    pub valid_from: Option<Instant>, // INSERT … VALID FROM a — applies to every node in the statement
    pub valid_to: Option<Instant>,   // INSERT … VALID FROM a TO b | VALID TO b
}

#[derive(Debug, Clone, PartialEq)]
pub struct DeleteStmt {
    pub pattern: NodePattern,
    pub where_clause: Option<Expr>,
    pub target: String, // must equal pattern.var
}

#[derive(Debug, Clone, PartialEq)]
pub enum Statement { Insert(InsertStmt), Query(QueryStmt), Delete(DeleteStmt) }
```

Grammar: `insert_stmt := INSERT insert_node (',' insert_node)* [VALID (FROM datetime [TO datetime] | TO datetime)]`; `delete := for_clause* MATCH node_pattern for_clause* [WHERE prop_eq] DELETE ident`. FOR clauses on a DELETE are rejected ("DELETE reads current state"); `VALID FROM a TO b` requires `a < b`; `DELETE x` requires `x` = the pattern variable.
- Produces `varve_plan::matching_iids`:

```rust
/// IIDs of entities visible at `bounds` that match the pattern + WHERE —
/// the reading part of writer-side DML (spec §10). Sorted and deduplicated
/// (determinism constraint).
pub async fn matching_iids(
    pattern: &NodePattern,
    where_clause: &Option<Expr>,
    live: &LiveTable,
    bounds: &TemporalBounds,
) -> Result<Vec<Iid>, PlanError>;
```
`PlanError` gains `#[error("internal: _iid column malformed")] MalformedIid`.
- Produces `varve_engine::clock::MonotonicClock` (crate-internal; the pluggable `Clock` registry interface from spec §4 arrives with durability, when config wiring reaches the engine):

```rust
pub struct MonotonicClock { /* AtomicI64 */ }
impl MonotonicClock {
    pub fn new() -> Self;
    /// Strictly increasing: max(wall-clock µs, last + 1). One call per tx.
    pub fn next(&self) -> Instant;
    /// max(wall, last) without advancing — the query-time "now"; >= every
    /// assigned tx time, so at(watermark) sees all applied events.
    pub fn watermark(&self) -> Instant;
}
```
- `TxReceipt` becomes `pub struct TxReceipt { pub tx_id: u64, pub system_time: Instant }` (pulled forward from the roadmap's slice-3 wording — the temporal e2e tests need a handle on assigned tx times; record in STATUS.md).
- `EngineError` gains `#[error("VALID FROM {from} must be earlier than VALID TO {to}")] InvalidValidRange { from: Instant, to: Instant }`.
- `Db::execute` semantics: one tx = one `clock.next()` `system_from` shared by all its events; every event is built and validated BEFORE the first append (slice-1 atomicity fix preserved). INSERT: `valid_from` defaults to the tx time, `valid_to` to `END_OF_TIME`; the computed pair must satisfy `from < to`. DELETE: run `matching_iids` at `valid: at(tx_time), system: at(tx_time)`, emit one `Op::Delete` event per IID with valid `[tx_time, END_OF_TIME)`; zero matches is a successful empty tx — DELETE keeps a documented `#[allow(clippy::await_holding_lock)]` (writes are log-serialized by design, spec D3; dissolves in slice 3's writer loop). `Db::query` passes `clock.watermark()` as `now` and stays lock-split (Task 6).

- [ ] **Step 1: Write the failing parser tests**

Update the existing `parses_insert_node` expected literal: `InsertStmt { nodes: vec![…], valid_from: None, valid_to: None }`. Then append to `crates/varve-gql/src/parser.rs` tests:
```rust
    #[test]
    fn parses_insert_valid_clause() {
        let Statement::Insert(ins) =
            parse("INSERT (:P {_id: 1}) VALID FROM DATE '2020-01-01' TO DATE '2021-01-01'").unwrap()
        else {
            panic!()
        };
        assert_eq!(ins.valid_from, Some(ts("2020-01-01T00:00:00Z")));
        assert_eq!(ins.valid_to, Some(ts("2021-01-01T00:00:00Z")));

        let Statement::Insert(ins) = parse("INSERT (:P {_id: 1}) VALID TO DATE '2021-01-01'").unwrap()
        else {
            panic!()
        };
        assert_eq!(ins.valid_from, None);
        assert_eq!(ins.valid_to, Some(ts("2021-01-01T00:00:00Z")));
    }

    #[test]
    fn insert_valid_range_must_be_ordered() {
        let err = parse("INSERT (:P {_id: 1}) VALID FROM DATE '2021-01-01' TO DATE '2020-01-01'")
            .unwrap_err();
        assert!(err.to_string().contains("earlier"), "{err}");
    }

    #[test]
    fn parses_match_delete() {
        let stmt = parse("MATCH (p:Person) WHERE p.name = 'Ada' DELETE p").unwrap();
        assert_eq!(
            stmt,
            Statement::Delete(DeleteStmt {
                pattern: NodePattern { var: "p".into(), label: Some("Person".into()) },
                where_clause: Some(Expr::PropEq {
                    var: "p".into(),
                    prop: "name".into(),
                    value: Literal::Str("Ada".into()),
                }),
                target: "p".into(),
            })
        );
    }

    #[test]
    fn delete_target_must_be_bound() {
        let err = parse("MATCH (p:Person) DELETE q").unwrap_err();
        assert!(err.to_string().contains("not bound"), "{err}");
    }

    #[test]
    fn delete_rejects_temporal_clauses() {
        let err = parse("FOR VALID_TIME ALL MATCH (p:Person) DELETE p").unwrap_err();
        assert!(err.to_string().contains("DELETE"), "{err}");
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p varve-gql parser`
Expected: compile errors (`DeleteStmt` undefined, `InsertStmt` field mismatch).

- [ ] **Step 3: Implement AST + parser changes**

`crates/varve-gql/src/ast.rs`: apply the `InsertStmt`/`DeleteStmt`/`Statement` shapes from the Interfaces block.

`crates/varve-gql/src/parser.rs`:

In `insert_stmt`, replace the trailing `expect(Eof)` with the VALID clause:
```rust
        let (mut valid_from, mut valid_to) = (None, None);
        if *self.peek() == TokenKind::Kw(Keyword::Valid) {
            self.pos += 1;
            let offset = self.offset();
            match self.bump() {
                TokenKind::Kw(Keyword::From) => {
                    valid_from = Some(self.datetime()?);
                    if *self.peek() == TokenKind::Kw(Keyword::To) {
                        self.pos += 1;
                        valid_to = Some(self.datetime()?);
                    }
                }
                TokenKind::Kw(Keyword::To) => {
                    valid_to = Some(self.datetime()?);
                }
                other => {
                    return Err(GqlError::Parse {
                        offset,
                        msg: format!("expected FROM or TO after VALID, found {other:?}"),
                    })
                }
            }
            if let (Some(from), Some(to)) = (valid_from, valid_to) {
                if from >= to {
                    return Err(GqlError::Parse {
                        offset,
                        msg: "VALID FROM must be earlier than VALID TO".into(),
                    });
                }
            }
        }
        self.expect(&TokenKind::Eof, "end of statement")?;
        Ok(InsertStmt { nodes, valid_from, valid_to })
```

In `statement()`'s MATCH/FOR arm, dispatch on what follows the pattern — replace the arm body with:
```rust
                let temporal = self.for_clauses()?;
                self.expect(&TokenKind::Kw(Keyword::Match), "MATCH")?;
                self.match_tail(temporal)
```
and add to `impl Parser` (`query_stmt` keeps its Task-7 shape but now receives the parsed pattern/clauses; refactor accordingly):
```rust
    /// Everything after MATCH: pattern, per-MATCH FOR clauses, WHERE, then
    /// RETURN … (query) or DELETE <var> (mutation).
    fn match_tail(&mut self, temporal: TemporalClauses) -> Result<Statement, GqlError> {
        self.expect(&TokenKind::LParen, "'('")?;
        let var = self.ident("pattern variable")?;
        let label = if *self.peek() == TokenKind::Colon {
            self.pos += 1;
            Some(self.ident("label name")?)
        } else {
            None
        };
        self.expect(&TokenKind::RParen, "')'")?;

        let match_temporal = self.for_clauses()?;

        let where_clause = if *self.peek() == TokenKind::Kw(Keyword::Where) {
            self.pos += 1;
            Some(self.prop_eq_expr()?)
        } else {
            None
        };

        let offset = self.offset();
        match self.bump() {
            TokenKind::Kw(Keyword::Return) => {
                let mut return_items = vec![self.return_item()?];
                while *self.peek() == TokenKind::Comma {
                    self.pos += 1;
                    return_items.push(self.return_item()?);
                }
                self.expect(&TokenKind::Eof, "end of statement")?;
                Ok(Statement::Query(QueryStmt {
                    temporal,
                    pattern: NodePattern { var, label },
                    match_temporal,
                    where_clause,
                    return_items,
                }))
            }
            TokenKind::Kw(Keyword::Delete) => {
                if temporal != TemporalClauses::default()
                    || match_temporal != TemporalClauses::default()
                {
                    return Err(GqlError::Parse {
                        offset,
                        msg: "DELETE reads current state — temporal clauses are not supported".into(),
                    });
                }
                let target = self.ident("variable to delete")?;
                if target != var {
                    return Err(GqlError::Parse {
                        offset,
                        msg: format!("DELETE target '{target}' is not bound (pattern variable is '{var}')"),
                    });
                }
                self.expect(&TokenKind::Eof, "end of statement")?;
                Ok(Statement::Delete(DeleteStmt {
                    pattern: NodePattern { var, label },
                    where_clause,
                    target,
                }))
            }
            other => Err(GqlError::Parse {
                offset,
                msg: format!("expected RETURN or DELETE, found {other:?}"),
            }),
        }
    }
```
Delete the now-unused `query_stmt` — `match_tail` fully replaces it.

Run: `cargo test -p varve-gql` — Expected: PASS.

- [ ] **Step 4: Write the failing engine tests**

`crates/varve-engine/Cargo.toml` — add:
```toml
[dev-dependencies]
tokio = { workspace = true }
```

`crates/varve-engine/tests/mutations.rs`:
```rust
use varve_engine::Db;
use varve_types::Instant;

#[tokio::test]
async fn receipts_carry_strictly_increasing_system_time() {
    let db = Db::memory();
    let a = db.execute("INSERT (:X {_id: 1})").await.unwrap();
    let b = db.execute("INSERT (:X {_id: 2})").await.unwrap();
    assert!(b.system_time > a.system_time);
    assert!(a.system_time > Instant::from_micros(0));
}

#[tokio::test]
async fn insert_valid_from_is_visible_only_in_its_valid_range() {
    let db = Db::memory();
    db.execute("INSERT (:P {_id: 1, name: 'Eve'}) VALID FROM DATE '2020-06-01'")
        .await
        .unwrap();
    let rows = |batches: Vec<varve_engine::RecordBatch>| -> usize {
        batches.iter().map(|b| b.num_rows()).sum()
    };
    // Default query (valid AS OF now, 2026+): visible.
    assert_eq!(rows(db.query("MATCH (p:P) RETURN p.name").await.unwrap()), 1);
    // Before its valid range: invisible.
    assert_eq!(
        rows(db
            .query("FOR VALID_TIME AS OF DATE '2019-01-01' MATCH (p:P) RETURN p.name")
            .await
            .unwrap()),
        0
    );
    // Inside it: visible.
    assert_eq!(
        rows(db
            .query("FOR VALID_TIME AS OF DATE '2021-01-01' MATCH (p:P) RETURN p.name")
            .await
            .unwrap()),
        1
    );
}

#[tokio::test]
async fn insert_with_inverted_computed_range_errors() {
    let db = Db::memory();
    // valid_from defaults to the tx time (2026+) which is AFTER the given TO.
    let err = db.execute("INSERT (:P {_id: 1}) VALID TO DATE '2020-01-01'").await.unwrap_err();
    assert!(err.to_string().contains("VALID FROM"), "{err}");
}

#[tokio::test]
async fn delete_hides_now_but_not_in_the_past() {
    let db = Db::memory();
    db.execute("INSERT (:Person {_id: 1, name: 'Zoe'})").await.unwrap();
    let amy = db.execute("INSERT (:Person {_id: 2, name: 'Amy'})").await.unwrap();
    db.execute("MATCH (p:Person) WHERE p.name = 'Zoe' DELETE p").await.unwrap();

    let rows = |batches: Vec<varve_engine::RecordBatch>| -> usize {
        batches.iter().map(|b| b.num_rows()).sum()
    };
    // Only the delete's target disappears.
    assert_eq!(rows(db.query("MATCH (p:Person) RETURN p.name").await.unwrap()), 1);
    // Time travel to just before the delete (Amy's tx): both are visible.
    let time_travel = format!(
        "FOR SYSTEM_TIME AS OF TIMESTAMP '{}' MATCH (p:Person) RETURN p.name",
        amy.system_time
    );
    assert_eq!(rows(db.query(&time_travel).await.unwrap()), 2);
}

#[tokio::test]
async fn delete_with_no_matches_is_an_empty_tx() {
    let db = Db::memory();
    let receipt = db.execute("MATCH (p:Nobody) DELETE p").await.unwrap();
    assert!(receipt.tx_id > 0);
}
```

- [ ] **Step 5: Run test to verify it fails**

Run: `cargo test -p varve-engine`
Expected: compile errors — `TxReceipt` has no `system_time`, engine has no `Statement::Delete` arm, `varve_engine::RecordBatch` not exported.

- [ ] **Step 6: Implement clock + engine**

`crates/varve-engine/src/clock.rs`:
```rust
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use varve_types::Instant;

/// Strictly increasing wall-clock µs source — the writer's transaction time
/// authority (spec §5.2: system_from is "assigned by the writer, monotonic
/// per log"). The pluggable Clock interface (spec §4) arrives with durability.
#[derive(Default)]
pub struct MonotonicClock {
    last_us: AtomicI64,
}

fn wall_us() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_micros()).unwrap_or(i64::MAX))
        .unwrap_or(0) // pre-1970 clock: fall back to the monotonic counter
}

impl MonotonicClock {
    pub fn new() -> Self {
        Self::default()
    }

    /// The next transaction time: max(wall, last + 1). One call per tx.
    pub fn next(&self) -> Instant {
        let wall = wall_us();
        let mut last = self.last_us.load(Ordering::SeqCst);
        loop {
            let candidate = wall.max(last + 1);
            match self.last_us.compare_exchange(last, candidate, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => return Instant::from_micros(candidate),
                Err(actual) => last = actual,
            }
        }
    }

    /// max(wall, last) WITHOUT advancing — the query-time "now". It is >=
    /// every assigned tx time, so at(watermark) sees all applied events.
    pub fn watermark(&self) -> Instant {
        Instant::from_micros(wall_us().max(self.last_us.load(Ordering::SeqCst)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn next_is_strictly_increasing_under_bursts() {
        let clock = MonotonicClock::new();
        let mut prev = clock.next();
        for _ in 0..10_000 {
            let t = clock.next();
            assert!(t > prev);
            prev = t;
        }
    }

    #[test]
    fn watermark_never_precedes_assigned_times() {
        let clock = MonotonicClock::new();
        let t = clock.next();
        assert!(clock.watermark() >= t);
        let w1 = clock.watermark();
        let w2 = clock.watermark();
        assert!(w2 >= w1); // reading the watermark never advances the clock
    }
}
```

`crates/varve-engine/src/db.rs` — final shape (replaces the interim Task-6 code):
```rust
use crate::clock::MonotonicClock;
use datafusion::arrow::record_batch::RecordBatch;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use thiserror::Error;
use varve_gql::ast::{DeleteStmt, InsertStmt, Literal, Statement};
use varve_gql::token::GqlError;
use varve_index::{Event, IndexError, LiveTable, Op};
use varve_plan::PlanError;
use varve_types::{Doc, Iid, Instant, TemporalBounds, TemporalDimension, TypeError, Value};

#[derive(Debug, Error)]
pub enum EngineError {
    #[error(transparent)]
    Gql(#[from] GqlError),
    #[error(transparent)]
    Plan(#[from] PlanError),
    #[error(transparent)]
    Index(#[from] IndexError),
    #[error(transparent)]
    Type(#[from] TypeError),
    #[error("VALID FROM {from} must be earlier than VALID TO {to}")]
    InvalidValidRange { from: Instant, to: Instant },
    #[error("statement is a query; use query()")]
    NotAMutation,
    #[error("statement is a mutation; use execute()")]
    NotAQuery,
    #[error("internal lock poisoned")]
    Poisoned,
}

#[derive(Debug, Clone, Copy)]
pub struct TxReceipt {
    pub tx_id: u64,
    pub system_time: Instant,
}

/// Embedded, in-process database handle. Single in-memory LiveTable; the
/// writer assigns monotonic system time per tx (durability arrives slice 3).
pub struct Db {
    live: Arc<RwLock<LiveTable>>,
    clock: MonotonicClock,
    tx_counter: AtomicU64,
    id_counter: AtomicU64,
}

fn literal_to_value(l: &Literal) -> Value {
    match l {
        Literal::Int(i) => Value::Int(*i),
        Literal::Float(f) => Value::Float(*f),
        Literal::Str(s) => Value::Str(s.clone()),
        Literal::Bool(b) => Value::Bool(*b),
        Literal::Null => Value::Null,
    }
}

impl Db {
    pub fn memory() -> Db {
        Db {
            live: Arc::new(RwLock::new(LiveTable::new())),
            clock: MonotonicClock::new(),
            tx_counter: AtomicU64::new(0),
            id_counter: AtomicU64::new(0),
        }
    }

    /// Execute a mutation statement (INSERT, MATCH … DELETE).
    pub async fn execute(&self, gql: &str) -> Result<TxReceipt, EngineError> {
        match varve_gql::parse(gql)? {
            Statement::Insert(ins) => self.execute_insert(&ins),
            Statement::Delete(del) => self.execute_delete(&del).await,
            Statement::Query(_) => Err(EngineError::NotAMutation),
        }
    }

    fn execute_insert(&self, ins: &InsertStmt) -> Result<TxReceipt, EngineError> {
        let tx_id = self.tx_counter.fetch_add(1, Ordering::SeqCst) + 1;
        let system = self.clock.next();
        let valid_from = ins.valid_from.unwrap_or(system);
        let valid_to = ins.valid_to.unwrap_or(Instant::END_OF_TIME);
        if valid_from >= valid_to {
            return Err(EngineError::InvalidValidRange { from: valid_from, to: valid_to });
        }
        // Build and validate EVERY event — including the fallible `id_bytes()?`
        // — before the first append, so a later node's invalid `_id` can't
        // leave earlier nodes partially committed (slice-1 review fix, pinned
        // by `multi_node_insert_is_atomic_on_invalid_id`).
        let mut events = Vec::with_capacity(ins.nodes.len());
        for node in &ins.nodes {
            let mut doc: Doc =
                node.props.iter().map(|(k, v)| (k.clone(), literal_to_value(v))).collect();
            let id = match doc.get("_id") {
                Some(v) => v.clone(),
                None => {
                    // process-local generated id; user-durable ids arrive with slice 3
                    let n = self.id_counter.fetch_add(1, Ordering::SeqCst);
                    let v = Value::Str(format!("varve:gen:{n}"));
                    doc.insert("_id".into(), v.clone());
                    v
                }
            };
            let iid = Iid::derive("default", "nodes", &id.id_bytes()?);
            events.push(Event {
                iid,
                system_from: system,
                valid_from,
                valid_to,
                op: Op::Put { labels: node.labels.clone(), doc },
            });
        }
        let mut live = self.live.write().map_err(|_| EngineError::Poisoned)?;
        for event in events {
            live.append(event)?;
        }
        Ok(TxReceipt { tx_id, system_time: system })
    }

    // DELETE plans its reading part with the query engine at the tx's own
    // snapshot (spec §10 DML), holding the write lock across the internal
    // await — acceptable for the single-writer embedded engine (no concurrent
    // access model until slice 3's writer loop).
    #[allow(clippy::await_holding_lock)]
    async fn execute_delete(&self, del: &DeleteStmt) -> Result<TxReceipt, EngineError> {
        let tx_id = self.tx_counter.fetch_add(1, Ordering::SeqCst) + 1;
        let system = self.clock.next();
        let bounds = TemporalBounds {
            valid: TemporalDimension::at(system),
            system: TemporalDimension::at(system),
        };
        let mut live = self.live.write().map_err(|_| EngineError::Poisoned)?;
        let iids = varve_plan::matching_iids(&del.pattern, &del.where_clause, &live, &bounds).await?;
        for iid in iids {
            live.append(Event {
                iid,
                system_from: system,
                valid_from: system,
                valid_to: Instant::END_OF_TIME,
                op: Op::Delete,
            })?;
        }
        Ok(TxReceipt { tx_id, system_time: system })
    }

    /// Execute a read query, returning Arrow batches.
    pub async fn query(&self, gql: &str) -> Result<Vec<RecordBatch>, EngineError> {
        let Statement::Query(q) = varve_gql::parse(gql)? else {
            return Err(EngineError::NotAQuery);
        };
        let now = self.clock.watermark();
        // Snapshot under the read lock, drop the guard, then run DataFusion
        // on the owned batch — no await while holding the lock.
        let snapshot = {
            let live = self.live.read().map_err(|_| EngineError::Poisoned)?;
            varve_plan::snapshot_for_query(&q, &live, now)?
        };
        Ok(varve_plan::execute_query(&q, snapshot).await?)
    }
}
```

`crates/varve-engine/src/lib.rs`:
```rust
mod clock;
pub mod db;

pub use datafusion::arrow::record_batch::RecordBatch;
pub use db::{Db, EngineError, TxReceipt};
```

Add `matching_iids` to `crates/varve-plan/src/exec.rs`:
```rust
use datafusion::arrow::array::FixedSizeBinaryArray;
use varve_gql::ast::NodePattern;
use varve_types::Iid;

/// IIDs of entities visible at `bounds` matching pattern + WHERE — the
/// reading part of writer-side DML (spec §10). Sorted + deduplicated.
pub async fn matching_iids(
    pattern: &NodePattern,
    where_clause: &Option<Expr>,
    live: &LiveTable,
    bounds: &TemporalBounds,
) -> Result<Vec<Iid>, PlanError> {
    let label = pattern.label.as_deref().unwrap_or("");
    let Some(batch) = live.snapshot_for_label(label, bounds)? else {
        return Ok(vec![]);
    };
    let schema = batch.schema();
    let has_col = |name: &str| schema.column_with_name(name).is_some();

    let ctx = SessionContext::new();
    let table = MemTable::try_new(schema.clone(), vec![vec![batch]])?;
    let mut df = ctx.read_table(Arc::new(table))?;
    if let Some(Expr::PropEq { prop, value, .. }) = where_clause {
        if !has_col(prop) {
            return Err(PlanError::UnknownColumn(prop.clone()));
        }
        df = df.filter(col(prop.as_str()).eq(to_df_literal(value)))?;
    }
    let df = df.select(vec![col("_iid")])?;

    let mut iids = Vec::new();
    for batch in df.collect().await? {
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .ok_or(PlanError::MalformedIid)?;
        for i in 0..col.len() {
            let bytes: [u8; 16] =
                col.value(i).try_into().map_err(|_| PlanError::MalformedIid)?;
            iids.push(Iid::from_bytes(bytes));
        }
    }
    iids.sort();
    iids.dedup();
    Ok(iids)
}
```
and the `PlanError` variant:
```rust
    #[error("internal: _iid column malformed")]
    MalformedIid,
```
`run_query`'s label handling is shared — extract nothing yet (two call sites, both trivial; DRY review at slice 7 when the planner grows a real GraphPlan).

Update `crates/varve/src/lib.rs` (the facade re-exports the engine's `RecordBatch` unchanged — no edit needed; verify `pub use varve_engine::{Db, EngineError, TxReceipt};` still compiles).

- [ ] **Step 7: Run the engine tests**

Run: `cargo test -p varve-engine`
Expected: 5 mutation tests + 2 clock tests pass.

- [ ] **Step 8: Run the full gate**

Run: `just check`
Expected: green — including slice-1 walking-skeleton tests (`tx_ids_are_monotonic` etc.) and the property suite.

- [ ] **Step 9: Commit**

```bash
git add crates/
git commit -m "feat: temporal mutations — VALID clause on INSERT, DELETE, monotonic tx time"
```

---

### Task 9: End-to-end bitemporal acceptance suite + time-travel demo

**Files:**
- Create: `crates/varve/tests/temporal.rs`
- Create: `crates/varve/examples/time_travel.rs`
- Modify: `crates/varve/src/lib.rs` + `crates/varve/Cargo.toml` (re-export temporal types)
- Test: `temporal.rs`

**Interfaces:**
- Consumes the whole slice through the public facade only: `varve::{Db, Instant, RecordBatch, TxReceipt}`.
- Produces facade re-exports (spec §11 users need the temporal vocabulary):

```rust
pub use datafusion::arrow::record_batch::RecordBatch;
pub use varve_engine::{Db, EngineError, TxReceipt};
pub use varve_types::{Instant, TemporalBounds, TemporalDimension};
```

- The four canonical bitemporal scenarios (roadmap slice-2 exit criteria): (1) as-of past valid time; (2) retroactive correction invisible at the old system time; (3) retroactive correction visible at the new system time; (4) delete, then as-of-before-the-delete.

- [ ] **Step 1: Write the failing acceptance tests**

`crates/varve/tests/temporal.rs`:
```rust
#![allow(clippy::unwrap_used)] // tests may use unwrap; crate-level allow covers helper fns
use arrow::array::{Int64Array, StringArray, TimestampMicrosecondArray};
use varve::{Db, Instant, RecordBatch};

fn rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

fn strings(batches: &[RecordBatch], col: &str) -> Vec<String> {
    let mut out: Vec<String> = batches
        .iter()
        .flat_map(|b| {
            let a: &StringArray = b.column_by_name(col).unwrap().as_any().downcast_ref().unwrap();
            (0..a.len()).map(|i| a.value(i).to_string()).collect::<Vec<_>>()
        })
        .collect();
    out.sort();
    out
}

fn ints(batches: &[RecordBatch], col: &str) -> Vec<i64> {
    let mut out: Vec<i64> = batches
        .iter()
        .flat_map(|b| {
            let a: &Int64Array = b.column_by_name(col).unwrap().as_any().downcast_ref().unwrap();
            (0..a.len()).map(|i| a.value(i)).collect::<Vec<_>>()
        })
        .collect();
    out.sort();
    out
}

// Scenario 1 — as-of past valid time: Ada moves city in 2024; a 2022 query
// still finds her in London.
#[tokio::test]
async fn valid_time_travel_sees_the_old_version() {
    let db = Db::memory();
    db.execute("INSERT (:Person {_id: 1, name: 'Ada', city: 'London'}) VALID FROM DATE '2020-01-01'")
        .await
        .unwrap();
    db.execute("INSERT (:Person {_id: 1, name: 'Ada', city: 'Oslo'}) VALID FROM DATE '2024-01-01'")
        .await
        .unwrap();

    let current = db.query("MATCH (p:Person) RETURN p.city AS city").await.unwrap();
    assert_eq!(rows(&current), 1);
    assert_eq!(strings(&current, "city"), vec!["Oslo"]);

    let past = db
        .query("FOR VALID_TIME AS OF DATE '2022-06-01' MATCH (p:Person) RETURN p.city AS city")
        .await
        .unwrap();
    assert_eq!(strings(&past, "city"), vec!["London"]);
}

// Scenarios 2 + 3 — a retroactive correction changes the past, but the old
// belief remains reachable at the old system time.
#[tokio::test]
async fn retroactive_correction_is_system_time_dependent() {
    let db = Db::memory();
    let before = db.execute("INSERT (:Employee {_id: 7, salary: 50000})").await.unwrap();
    // Correction backdated to Jan 2026 — before the original insert's valid_from.
    db.execute("INSERT (:Employee {_id: 7, salary: 55000}) VALID FROM DATE '2026-01-01'")
        .await
        .unwrap();

    // New system time (default): the correction won.
    let now = db.query("MATCH (e:Employee) RETURN e.salary AS salary").await.unwrap();
    assert_eq!(ints(&now, "salary"), vec![55000]);

    // Old system time: we still see what we believed then.
    let then = db
        .query(&format!(
            "FOR SYSTEM_TIME AS OF TIMESTAMP '{}' MATCH (e:Employee) RETURN e.salary AS salary",
            before.system_time
        ))
        .await
        .unwrap();
    assert_eq!(ints(&then, "salary"), vec![50000]);

    // And at the old system time, February 2026 had no known salary at all.
    let feb_then = db
        .query(&format!(
            "FOR VALID_TIME AS OF DATE '2026-02-01' FOR SYSTEM_TIME AS OF TIMESTAMP '{}' \
             MATCH (e:Employee) RETURN e.salary AS salary",
            before.system_time
        ))
        .await
        .unwrap();
    assert_eq!(rows(&feb_then), 0);
}

// Scenario 4 — delete, then time travel to before the delete.
#[tokio::test]
async fn delete_then_as_of_before_the_delete() {
    let db = Db::memory();
    let ins = db.execute("INSERT (:Person {_id: 9, name: 'Zoe'})").await.unwrap();
    db.execute("MATCH (p:Person) WHERE p.name = 'Zoe' DELETE p").await.unwrap();

    assert_eq!(rows(&db.query("MATCH (p:Person) RETURN p.name").await.unwrap()), 0);
    let back = db
        .query(&format!(
            "FOR SYSTEM_TIME AS OF TIMESTAMP '{}' MATCH (p:Person) RETURN p.name AS name",
            ins.system_time
        ))
        .await
        .unwrap();
    assert_eq!(strings(&back, "name"), vec!["Zoe"]);
}

#[tokio::test]
async fn same_tx_batch_on_one_entity_is_last_write_wins() {
    let db = Db::memory();
    db.execute("INSERT (:P {_id: 5, v: 1}), (:P {_id: 5, v: 2})").await.unwrap();
    let batches = db.query("MATCH (p:P) RETURN p.v AS v").await.unwrap();
    assert_eq!(rows(&batches), 1);
    assert_eq!(ints(&batches, "v"), vec![2]);
}

#[tokio::test]
async fn temporal_functions_expose_version_metadata() {
    let db = Db::memory();
    db.execute("INSERT (:P {_id: 3, name: 'Eve'}) VALID FROM TIMESTAMP '2021-03-04T05:06:07Z'")
        .await
        .unwrap();
    let batches = db
        .query("MATCH (p:P) RETURN p.name AS name, valid_from(p) AS vf, valid_to(p) AS vt, system_from(p) AS sf")
        .await
        .unwrap();
    let batch = &batches[0];
    let vf: &TimestampMicrosecondArray =
        batch.column_by_name("vf").unwrap().as_any().downcast_ref().unwrap();
    let vt: &TimestampMicrosecondArray =
        batch.column_by_name("vt").unwrap().as_any().downcast_ref().unwrap();
    let sf: &TimestampMicrosecondArray =
        batch.column_by_name("sf").unwrap().as_any().downcast_ref().unwrap();
    assert_eq!(
        vf.value(0),
        Instant::parse_rfc3339("2021-03-04T05:06:07Z").unwrap().as_micros()
    );
    assert_eq!(vt.value(0), Instant::END_OF_TIME.as_micros());
    assert!(sf.value(0) > 0);
}

#[tokio::test]
async fn for_valid_time_all_returns_every_version() {
    let db = Db::memory();
    db.execute("INSERT (:Person {_id: 1, city: 'London'}) VALID FROM DATE '2020-01-01'")
        .await
        .unwrap();
    db.execute("INSERT (:Person {_id: 1, city: 'Oslo'}) VALID FROM DATE '2024-01-01'")
        .await
        .unwrap();
    let all = db
        .query("FOR VALID_TIME ALL MATCH (p:Person) RETURN p.city AS city")
        .await
        .unwrap();
    // At the current system time the valid axis holds London [2020, 2024) then Oslo [2024, ∞).
    assert_eq!(strings(&all, "city"), vec!["London", "Oslo"]);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p varve --test temporal`
Expected: compile error — `varve::Instant` not exported.

- [ ] **Step 3: Facade re-exports + example**

`crates/varve/Cargo.toml` — add `varve-types = { path = "../varve-types" }` to `[dependencies]`.

`crates/varve/src/lib.rs`:
```rust
pub use datafusion::arrow::record_batch::RecordBatch;
pub use varve_engine::{Db, EngineError, TxReceipt};
pub use varve_types::{Instant, TemporalBounds, TemporalDimension};
```

`crates/varve/examples/time_travel.rs`:
```rust
use varve::Db;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Db::memory();

    let before = db.execute("INSERT (:Product {_id: 'wrench', price: 10})").await?;
    // Retroactive correction: the price was actually 12 since January.
    db.execute("INSERT (:Product {_id: 'wrench', price: 12}) VALID FROM DATE '2026-01-01'")
        .await?;

    println!("what we believe now:");
    show(&db.query("MATCH (p:Product) RETURN p.price AS price, valid_from(p) AS since").await?)?;

    println!("what we believed at tx {} ({}):", before.tx_id, before.system_time);
    show(
        &db.query(&format!(
            "FOR SYSTEM_TIME AS OF TIMESTAMP '{}' MATCH (p:Product) RETURN p.price AS price",
            before.system_time
        ))
        .await?,
    )?;
    Ok(())
}

fn show(batches: &[varve::RecordBatch]) -> Result<(), Box<dyn std::error::Error>> {
    println!("{}", datafusion::arrow::util::pretty::pretty_format_batches(batches)?);
    Ok(())
}
```

- [ ] **Step 4: Run tests and the demo**

Run: `cargo test -p varve && cargo run --example time_travel -p varve`
Expected: 9 tests pass (3 walking-skeleton + 6 temporal); the demo prints price 12 "now" and price 10 at the old system time.

- [ ] **Step 5: Run the full gate**

Run: `just check`
Expected: green.

- [ ] **Step 6: Commit**

```bash
git add crates/varve/
git commit -m "test: end-to-end bitemporal scenarios and time-travel example"
```

---

## Slice exit checklist

- [ ] `just check` green: fmt, clippy `-D warnings`, all workspace tests (including 2 × 10k property cases).
- [ ] Roadmap slice-2 exit criteria verified: property tests green; the four canonical bitemporal scenarios pass end-to-end through GQL (`crates/varve/tests/temporal.rs`); slice-1 tests still green untouched (`crates/varve/tests/walking_skeleton.rs` including `multi_node_insert_is_atomic_on_invalid_id`, `examples/hello.rs`).
- [ ] Slice-1 deferred remediations from STATUS.md verified done: `Db::query` carries NO `await_holding_lock` allow (lock-split via `snapshot_for_query`/`execute_query`); LiveTable all-null/empty-doc test and both `UnknownColumn` tests exist; `id_bytes` rejection arm narrowed + same-length collision test. Update the STATUS.md "DEFERRED slice-1 remediations" open item to RESOLVED (note: `SnapshotSource` trait seam deliberately deferred to slice 4; DELETE's documented write-lock allow dissolves in slice 3's writer loop).
- [ ] Update `docs/plans/STATUS.md`:
  - Current position: slice 2 ✅ complete; next action = plan slice 3 (durability) via writing-plans.
  - Slice log row: `2 bitemporal core | ✅ complete | <sessions> | cargo run --example time_travel -p varve | events + Ceiling/Polygon + reference-model proptest + temporal GQL`.
  - Decisions to record: `TxReceipt.system_time` pulled forward from slice 3 (e2e system-time tests need it); DELETE rejects temporal clauses (reads current state; retroactive deletes deferred); GQL `ERASE` statement deferred to slice 7 (event-level `Op::Erase` fully implemented + property-tested); 13 new reserved keywords can no longer be property names until slice 7's full grammar; interim Task-6 counter clock replaced by `MonotonicClock` in Task 8 (note only if tasks were split across sessions).
  - Environment facts: `proptest` pinned (record the resolved version); nightly CI property job added (`PROPTEST_CASES=200000`).
- [ ] Tick the slice-2 checkboxes in `docs/plans/varve-v1-roadmap.md` (with parenthetical notes for the deviations above) and tick all checkboxes in this plan.
- [ ] Commit:

```bash
git add docs/plans/
git commit -m "docs: slice 2 complete — bitemporal core, STATUS and roadmap updated"
```
