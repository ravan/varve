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
    src: Option<Iid>,
    dst: Option<Iid>,
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
                    src: version.event.src,
                    dst: version.event.dst,
                });
            }
        }
    }
    if visible.is_empty() {
        return Ok(None);
    }

    // Edge-ness: every visible row's event must carry both endpoints, or
    // none of them — a mix means the caller handed us rows from more than
    // one table (spec §5.2 endpoints are immutable per edge, all-or-nothing
    // per table).
    let with_endpoints = visible.iter().filter(|r| r.src.is_some()).count();
    if with_endpoints != 0 && with_endpoints != visible.len() {
        return Err(IndexError::Codec(
            "mixed node and edge events in one snapshot".into(),
        ));
    }
    let is_edges = with_endpoints == visible.len() && !visible.is_empty();

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

    if is_edges {
        let mut src_b = FixedSizeBinaryBuilder::new(16);
        let mut dst_b = FixedSizeBinaryBuilder::new(16);
        for row in &visible {
            let Some(src) = row.src else {
                // Unreachable: `is_edges` established every row.src.is_some().
                return Err(IndexError::Codec("edge event missing src endpoint".into()));
            };
            let Some(dst) = row.dst else {
                // `src` presence doesn't guarantee `dst` — both are stamped
                // together by the writer, but this guards against a
                // malformed event slipping through.
                return Err(IndexError::Codec("edge event missing dst endpoint".into()));
            };
            src_b.append_value(src.as_bytes())?;
            dst_b.append_value(dst.as_bytes())?;
        }
        fields.push(Field::new("_src_iid", DataType::FixedSizeBinary(16), false));
        columns.push(Arc::new(src_b.finish()));
        fields.push(Field::new("_dst_iid", DataType::FixedSizeBinary(16), false));
        columns.push(Arc::new(dst_b.finish()));
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

/// Merge decoded events from persisted blocks and a live tail into
/// per-entity arrival order — the source-agnostic core of the bitemporal
/// scan (spec §10, decision 9). `blocks` must arrive in ascending (time)
/// order; each block's events are in FILE order per entity (system_from
/// DESC, decision 9). `live` holds each entity's events already in arrival
/// (log) order.
///
/// Per block, events are grouped by entity (preserving file order), then
/// each entity's group is reversed to restore arrival order; blocks are
/// concatenated in block order; live events are appended last since they
/// are always newest. This is pure and infallible — the async-free core
/// the flush-equivalence property test exercises directly.
pub fn merge_sources<B, L>(blocks: B, live: L) -> BTreeMap<Iid, Vec<Event>>
where
    B: IntoIterator<Item = Vec<Event>>,
    L: IntoIterator<Item = (Iid, Vec<Event>)>,
{
    let mut merged: BTreeMap<Iid, Vec<Event>> = BTreeMap::new();
    for block_events in blocks {
        // Group the block's events by entity (preserving file order =
        // system_from DESC per entity), then reverse per entity to restore
        // arrival order.
        let mut per_block: BTreeMap<Iid, Vec<Event>> = BTreeMap::new();
        for event in block_events {
            per_block.entry(event.iid).or_default().push(event);
        }
        for (iid, desc) in per_block {
            merged
                .entry(iid)
                .or_default()
                .extend(desc.into_iter().rev());
        }
    }
    for (iid, events) in live {
        merged.entry(iid).or_default().extend(events);
    }
    merged
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
            src: None,
            dst: None,
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

    /// One entity's run spans two persisted blocks plus a live tail. Each
    /// block stores its slice of the run in file order (system_from DESC,
    /// decision 9); `merge_sources` must reverse per block to arrival order,
    /// concatenate blocks in block (time) order, then append live last.
    #[test]
    fn merge_sources_merges_blocks_then_live_with_per_block_reversal() {
        let entity = iid(1);
        let block_a = vec![put(1, 3, "c"), put(1, 2, "b")]; // file order DESC
        let block_b = vec![put(1, 5, "e"), put(1, 4, "d")]; // file order DESC
        let live: Vec<(Iid, Vec<Event>)> = vec![(entity, vec![put(1, 6, "f"), put(1, 7, "g")])];

        let merged = merge_sources([block_a, block_b], live);

        let got: Vec<i64> = merged[&entity]
            .iter()
            .map(|e| e.system_from.as_micros())
            .collect();
        assert_eq!(got, vec![2, 3, 4, 5, 6, 7]);
    }

    /// Two events in one block share the same `system_from` (a legitimate
    /// intra-block tie). `merge_sources` must restore arrival order by
    /// reversing file order, NOT by sorting on a key that can't
    /// discriminate ties.
    #[test]
    fn merge_sources_reverses_same_system_from_ties_by_position_not_sort() {
        let entity = iid(1);
        let x = put(1, 5, "x");
        let y = put(1, 5, "y");
        let block = vec![x, y]; // file order: x arrived after y (DESC)

        let merged = merge_sources([block], std::iter::empty::<(Iid, Vec<Event>)>());

        let names: Vec<&str> = merged[&entity]
            .iter()
            .map(|e| match &e.op {
                Op::Put { doc, .. } => match doc.get("name") {
                    Some(Value::Str(s)) => s.as_str(),
                    _ => "?",
                },
                _ => "?",
            })
            .collect();
        assert_eq!(names, vec!["y", "x"]); // reversed file order, not re-sorted
    }

    /// A node event (no endpoints) and an edge event (both endpoints), both
    /// visible under the same bounds and sharing a label, must not be
    /// snapshotted together — endpoints are all-or-nothing per table
    /// (spec §5.2).
    #[test]
    fn mixed_node_and_edge_events_error() {
        let node_event = put(1, 1, "Ada"); // label "P", src/dst None
        let edge_event = Event {
            iid: iid(2),
            system_from: us(1),
            valid_from: us(1),
            valid_to: Instant::END_OF_TIME,
            src: Some(iid(10)),
            dst: Some(iid(20)),
            op: Op::Put {
                labels: vec!["P".into()],
                doc: Doc::new(),
            },
        };
        let mut pairs = vec![
            (iid(1), std::slice::from_ref(&node_event)),
            (iid(2), std::slice::from_ref(&edge_event)),
        ];
        pairs.sort_by_key(|(iid, _)| *iid);
        let err = snapshot_entities(pairs, "P", &now_bounds(10)).unwrap_err();
        match err {
            IndexError::Codec(msg) => {
                assert!(msg.contains("mixed node and edge events in one snapshot"))
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    /// Every visible row's event carries `src`, but one is missing `dst` —
    /// the defensive guard against a malformed edge event slipping through
    /// (src/dst are stamped together by the writer, but this branch exists
    /// in case that invariant is ever violated).
    #[test]
    fn edge_event_missing_dst_endpoint_error() {
        let bad_edge = Event {
            iid: iid(1),
            system_from: us(1),
            valid_from: us(1),
            valid_to: Instant::END_OF_TIME,
            src: Some(iid(10)),
            dst: None,
            op: Op::Put {
                labels: vec!["KNOWS".into()],
                doc: Doc::new(),
            },
        };
        let pairs = vec![(bad_edge.iid, std::slice::from_ref(&bad_edge))];
        let err = snapshot_entities(pairs, "KNOWS", &now_bounds(10)).unwrap_err();
        match err {
            IndexError::Codec(msg) => assert!(msg.contains("edge event missing dst endpoint")),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn edge_snapshot_carries_endpoint_columns() {
        // Inline edge helper (same shape as live.rs's test helper).
        let e = Event {
            iid: Iid::derive("g", "edges", &[1]),
            system_from: Instant::from_micros(1),
            valid_from: Instant::from_micros(1),
            valid_to: Instant::END_OF_TIME,
            src: Some(Iid::derive("g", "nodes", &[10])),
            dst: Some(Iid::derive("g", "nodes", &[20])),
            op: Op::Put {
                labels: vec!["KNOWS".into()],
                doc: BTreeMap::new(),
            },
        };
        let batch = snapshot_entities(
            [(e.iid, std::slice::from_ref(&e))],
            "KNOWS",
            &TemporalBounds {
                valid: TemporalDimension::at(Instant::from_micros(5)),
                system: TemporalDimension::at(Instant::from_micros(5)),
            },
        )
        .unwrap()
        .unwrap();
        let schema = batch.schema();
        let src_idx = schema.column_with_name("_src_iid").unwrap().0;
        let dst_idx = schema.column_with_name("_dst_iid").unwrap().0;
        let src = batch
            .column(src_idx)
            .as_any()
            .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(src.value(0), e.src.unwrap().as_bytes());
        assert!(dst_idx > src_idx);
    }
}
