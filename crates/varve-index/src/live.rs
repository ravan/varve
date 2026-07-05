use crate::event::Event;
use arrow::record_batch::RecordBatch;
use std::collections::BTreeMap;
use thiserror::Error;
use varve_types::{Iid, Instant, TemporalBounds};

#[derive(Debug, Error)]
pub enum IndexError {
    #[error(
        "property '{property}' has mixed types across rows (lifted with dense-union columns in slice 4)"
    )]
    MixedPropertyTypes { property: String },
    #[error("event appended out of order: system_from {got} precedes {last}")]
    OutOfOrderEvent { last: Instant, got: Instant },
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("event codec: {0}")]
    Codec(String),
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

impl LiveTable {
    pub fn new() -> Self {
        Self::default()
    }

    /// Append an event. Events must arrive in log order: `system_from` must
    /// be >= every previously appended event's (ties allowed — same-tx
    /// batches).
    pub fn append(&mut self, event: Event) -> Result<(), IndexError> {
        if let Some(last) = self.last_system_from {
            if event.system_from < last {
                return Err(IndexError::OutOfOrderEvent {
                    last,
                    got: event.system_from,
                });
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

    /// Max `system_from` ever appended (`None` on an empty/new table).
    /// Stamps the manifest's clock floor (`max_system_time_us`): flushed
    /// events may predate the process's own clock after a restart replays
    /// them, so the writer cannot derive this from its own clock.
    pub fn last_system_from(&self) -> Option<Instant> {
        self.last_system_from
    }

    /// Resolve all entities against `bounds` and snapshot the visible
    /// versions carrying `label` into one RecordBatch. Returns `None` when
    /// nothing is visible. See [`crate::scan::snapshot_entities`] (delegates
    /// to it) for the schema and precondition details.
    pub fn snapshot_for_label(
        &self,
        label: &str,
        bounds: &TemporalBounds,
    ) -> Result<Option<RecordBatch>, IndexError> {
        crate::scan::snapshot_entities(
            self.events
                .iter()
                .map(|(iid, events)| (*iid, events.as_slice())),
            label,
            bounds,
        )
    }

    /// All entities in ascending `Iid` order, each event slice in arrival
    /// (log) order — the shape [`crate::scan::snapshot_entities`] consumes,
    /// and what block encoding (Task 6) and the merged scan (Task 9) will
    /// consume too.
    pub fn entities(&self) -> impl Iterator<Item = (&Iid, &[Event])> {
        self.events
            .iter()
            .map(|(iid, events)| (iid, events.as_slice()))
    }

    /// One entity's events in arrival order (point-lookup fast path).
    pub fn events_for(&self, iid: &Iid) -> Option<&[Event]> {
        self.events.get(iid).map(Vec::as_slice)
    }
}

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
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    fn put(entity: u8, sf: i64, vf: i64, label: &str, d: Doc) -> Event {
        Event {
            iid: iid(entity),
            system_from: us(sf),
            valid_from: us(vf),
            valid_to: EOT,
            src: None,
            dst: None,
            op: Op::Put {
                labels: vec![label.into()],
                doc: d,
            },
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
        t.append(put(
            1,
            1,
            1,
            "Person",
            doc(&[("name", Value::Str("Ada".into())), ("age", Value::Int(36))]),
        ))
        .unwrap();
        t.append(put(
            2,
            2,
            2,
            "Person",
            doc(&[("name", Value::Str("Bob".into()))]),
        ))
        .unwrap();
        t
    }

    #[test]
    fn current_snapshot_shows_one_version_per_entity() {
        let mut t = ada_and_bob();
        // Ada renamed at time 10: only the new version is current.
        t.append(put(
            1,
            10,
            10,
            "Person",
            doc(&[("name", Value::Str("Adele".into()))]),
        ))
        .unwrap();
        let batch = t
            .snapshot_for_label("Person", &now_bounds(50))
            .unwrap()
            .unwrap();
        assert_eq!(batch.num_rows(), 2);
        let names: &StringArray = batch
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        let mut got: Vec<String> = (0..2).map(|i| names.value(i).to_string()).collect();
        got.sort();
        assert_eq!(got, vec!["Adele", "Bob"]);
    }

    #[test]
    fn all_bounds_expose_history_with_derived_system_to() {
        let mut t = LiveTable::new();
        t.append(put(1, 1, 0, "P", doc(&[("v", Value::Int(1))])))
            .unwrap();
        t.append(put(1, 5, 0, "P", doc(&[("v", Value::Int(2))])))
            .unwrap();
        let batch = t
            .snapshot_for_label("P", &TemporalBounds::default())
            .unwrap()
            .unwrap();
        assert_eq!(batch.num_rows(), 2);
        let v: &Int64Array = batch
            .column_by_name("v")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        let st: &TimestampMicrosecondArray = batch
            .column_by_name("_system_to")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        // Output is deterministic: newest version first per entity.
        assert_eq!(v.value(0), 2);
        assert_eq!(st.value(0), EOT.as_micros());
        assert_eq!(v.value(1), 1);
        assert_eq!(st.value(1), 5); // superseded at system time 5 — derived, never stored
    }

    #[test]
    fn temporal_columns_have_utc_microsecond_type() {
        let t = ada_and_bob();
        let batch = t
            .snapshot_for_label("Person", &now_bounds(50))
            .unwrap()
            .unwrap();
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
            src: None,
            dst: None,
            op: Op::Delete,
        })
        .unwrap();
        let batch = t
            .snapshot_for_label("Person", &now_bounds(50))
            .unwrap()
            .unwrap();
        assert_eq!(batch.num_rows(), 1); // only Bob
        let before = t
            .snapshot_for_label("Person", &now_bounds(5))
            .unwrap()
            .unwrap();
        assert_eq!(before.num_rows(), 2); // time travel to before the delete
    }

    #[test]
    fn label_filter_applies_to_the_visible_version() {
        let mut t = ada_and_bob();
        t.append(put(
            3,
            3,
            3,
            "City",
            doc(&[("name", Value::Str("Oslo".into()))]),
        ))
        .unwrap();
        assert_eq!(
            t.snapshot_for_label("City", &now_bounds(50))
                .unwrap()
                .unwrap()
                .num_rows(),
            1
        );
        assert!(t
            .snapshot_for_label("Robot", &now_bounds(50))
            .unwrap()
            .is_none());
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
        t.append(put(1, 1, 1, "P", doc(&[("x", Value::Int(1))])))
            .unwrap();
        t.append(put(2, 2, 2, "P", doc(&[("x", Value::Str("one".into()))])))
            .unwrap();
        assert!(matches!(
            t.snapshot_for_label("P", &now_bounds(50)),
            Err(IndexError::MixedPropertyTypes { .. })
        ));
    }

    // Deferred from slice 1 (STATUS.md remediation list).
    #[test]
    fn all_null_property_and_empty_doc_rows() {
        let mut t = LiveTable::new();
        t.append(put(1, 1, 1, "P", doc(&[("ghost", Value::Null)])))
            .unwrap();
        t.append(put(2, 2, 2, "P", Doc::new())).unwrap();
        let batch = t.snapshot_for_label("P", &now_bounds(50)).unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);
        // A property observed only as Null constrains no column type — no column.
        assert!(batch.column_by_name("ghost").is_none());
    }
}
