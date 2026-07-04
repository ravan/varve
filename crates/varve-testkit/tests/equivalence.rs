use proptest::prelude::*;
use std::collections::BTreeMap;
use varve_index::{resolve, Event, Op};
use varve_testkit::strategy::{arb_bounds, arb_history};
use varve_testkit::ReferenceStore;
use varve_types::{Iid, Instant, TemporalBounds, TemporalDimension, Value};

fn cases() -> u32 {
    // 10k in CI; the nightly job raises this via PROPTEST_CASES (roadmap slice 2).
    std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10_000)
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
