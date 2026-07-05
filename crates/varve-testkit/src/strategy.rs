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
        ((a + 1)..T_POOL).prop_map(move |b| (Instant::from_micros(a), Instant::from_micros(b)))
    })
}

pub(crate) fn arb_valid_range() -> impl Strategy<Value = (Instant, Instant)> {
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
    prop::collection::vec(
        (0..3u8, arb_op_kind(), arb_valid_range(), 0i64..=2),
        1..=max_events,
    )
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
                        (
                            valid_from,
                            valid_to,
                            Op::Put {
                                labels: vec!["P".into()],
                                doc,
                            },
                        )
                    }
                    OpKind::Delete => (valid_from, valid_to, Op::Delete),
                    OpKind::Erase => (Instant::MIN, Instant::END_OF_TIME, Op::Erase),
                };
                Event {
                    iid: entity_iid(entity),
                    system_from: Instant::from_micros(system),
                    valid_from,
                    valid_to,
                    src: None,
                    dst: None,
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
    (arb_dimension(), arb_dimension()).prop_map(|(valid, system)| TemporalBounds { valid, system })
}
