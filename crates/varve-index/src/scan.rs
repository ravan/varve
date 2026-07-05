//! Source-agnostic bitemporal scan: resolve entities against bounds and
//! build the snapshot `RecordBatch`. Sources: the live table today, and
//! (from Task 9) persisted block pages merged with it. Extracted unchanged
//! from `LiveTable::snapshot_for_label` so a second scan source can share
//! this logic without duplicating it.

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

/// Resolve each entity in `entities` against `bounds` and snapshot the
/// visible versions carrying `label` into one RecordBatch (`None` when
/// nothing is visible). This is the source-agnostic core of
/// [`crate::live::LiveTable::snapshot_for_label`]: `entities` must arrive in
/// ascending `Iid` order, each event slice in arrival (log) order — exactly
/// `resolve`'s precondition — so it can be fed by live events, persisted
/// block events, or (later) a merge of both.
///
/// Schema: `_iid` FixedSizeBinary(16), then `_system_from`/`_system_to`/
/// `_valid_from`/`_valid_to` as Timestamp(µs, "UTC") (all non-null), then
/// one nullable column per property observed across visible docs (same type
/// rules as v0).
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
