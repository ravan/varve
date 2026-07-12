//! Spec §13.7 micro-bench: `resolve` (bitemporal resolution), the hot path
//! walked once per entity on every scan (`varve-index/src/scan.rs`).
//!
//! Benches are non-library targets (not under `#[cfg(test)]`), so
//! `clippy.toml`'s `allow-unwrap-in-tests` doesn't cover them even though the
//! workspace denies `clippy::unwrap_used`; allow it here explicitly.
#![allow(clippy::unwrap_used)]

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use varve_index::{resolve, Event, Op};
use varve_types::{Doc, Iid, Instant, TemporalBounds, TemporalDimension, Value};

fn us(n: i64) -> Instant {
    Instant::from_micros(n)
}

fn bench_iid() -> Iid {
    // Mirrors the real `_id` -> `Iid` derivation path (see
    // `varve-engine/src/writer.rs`, `varve-testkit/src/oracle.rs`), not a
    // raw byte slice: `Value::id_bytes()` is the canonical, type-tagged
    // encoding actually fed to `Iid::derive`.
    Iid::derive("bench", "nodes", &Value::Int(1).id_bytes().unwrap())
}

/// One entity's event history in arrival (log) order: `system_from` strictly
/// increasing, exactly as `LiveTable::append` enforces (spec §5.2; see
/// `varve-index/src/live.rs`) and as `resolve`'s doc comment requires
/// ("events must be in arrival (log) order: ascending system_from"). Every
/// 4th event is a `Delete`; the very last event is always a `Put` so the
/// "current" bench has a non-trivial resolved row to compute.
///
/// # Benchmark scope caveat
///
/// Every event has an **identical eternal valid range** (`Instant::MIN` to
/// `Instant::END_OF_TIME`). This deliberately exercises `resolve`'s
/// system-time Ceiling/Polygon bookkeeping (the reverse-arrival scan) but
/// **does not exercise** valid-time range splitting (insert/drain/binary-search
/// under distinct valid-time boundaries). Task 10's report must not generalize
/// these medians to mixed-valid-time workloads.
fn alternating_put_delete_history(n: usize) -> Vec<Event> {
    let iid = bench_iid();
    (0..n)
        .map(|i| {
            let system_from = us(i as i64 * 10);
            if i % 4 == 3 && i != n - 1 {
                Event {
                    iid,
                    system_from,
                    valid_from: Instant::MIN,
                    valid_to: Instant::END_OF_TIME,
                    src: None,
                    dst: None,
                    op: Op::Delete,
                }
            } else {
                let mut doc = Doc::new();
                doc.insert("seq".into(), Value::Int(i as i64));
                Event {
                    iid,
                    system_from,
                    valid_from: Instant::MIN,
                    valid_to: Instant::END_OF_TIME,
                    src: None,
                    dst: None,
                    op: Op::Put {
                        labels: vec!["Bench".into()],
                        doc,
                    },
                }
            }
        })
        .collect()
}

/// `AS OF now` on both axes, with `now` strictly after the last event's
/// `system_from` — the shipped "current state" default when a query gives
/// no temporal clause (spec §7; mirrors `varve_plan::exec::effective_bounds`'s
/// `unwrap_or_else(|| TemporalDimension::at(now))` fallback).
fn current_time_bounds(events: &[Event]) -> TemporalBounds {
    let last_sf = events.last().unwrap().system_from.as_micros();
    let now = us(last_sf + 1);
    TemporalBounds {
        valid: TemporalDimension::at(now),
        system: TemporalDimension::at(now),
    }
}

/// `system AS OF` the median event's `system_from`; valid axis unconstrained
/// — a historical (system-time-travel) query over the same history, forcing
/// `resolve` to skip every event newer than the median without an early exit.
fn as_of_middle_bounds(events: &[Event]) -> TemporalBounds {
    let mid = events[events.len() / 2].system_from;
    TemporalBounds {
        valid: TemporalDimension::all(),
        system: TemporalDimension::at(mid),
    }
}

fn bench_resolve(c: &mut Criterion) {
    let mut group = c.benchmark_group("resolve");
    for n in [16usize, 256, 4096] {
        let events = alternating_put_delete_history(n);
        let current = current_time_bounds(&events);
        let as_of = as_of_middle_bounds(&events);
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
