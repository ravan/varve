use std::collections::BTreeMap;
use varve_index::{Event, Op};
#[cfg(test)]
use varve_types::Doc;
use varve_types::{Iid, Instant};

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

    /// Append an event in arrival (log) order. Callers MUST supply events with
    /// non-decreasing `system_from` per `iid` — the winner-selection in
    /// `visible_at` relies on this (it matches `resolve`'s documented precondition).
    pub fn append(&mut self, event: Event) {
        if let Some(last) = self.events.get(&event.iid).and_then(|v| v.last()) {
            debug_assert!(
                event.system_from >= last.system_from,
                "ReferenceStore::append requires non-decreasing system_from per iid"
            );
        }
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
            .rfind(|e| e.valid_from <= valid && valid < e.valid_to)?;
        match winner.op {
            Op::Put { .. } => Some(winner),
            Op::Delete | Op::Erase => None,
        }
    }
}

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
            src: None,
            dst: None,
            op: Op::Put {
                labels: vec!["P".into()],
                doc,
            },
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
            src: None,
            dst: None,
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
            src: None,
            dst: None,
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
