//! Events ↔ Arrow IPC — the wire format for the log envelope's per-table
//! `arrow_ipc` effect bytes (spec §6). Docs and labels ride in a single
//! Binary `payload` column via the canonical varve-types codec; columnar doc
//! structs (dense unions) arrive with slice 8's compaction meta (slice-4 plan,
//! decision 2: blocks reuse this payload codec).

use crate::event::{Event, Op};
use crate::live::IndexError;
use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryBuilder, FixedSizeBinaryArray, FixedSizeBinaryBuilder,
    TimestampMicrosecondArray, TimestampMicrosecondBuilder, UInt8Array, UInt8Builder,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use varve_types::{decode_doc, encode_doc, Doc, Iid, Instant};

const OP_PUT: u8 = 0;
const OP_DELETE: u8 = 1;
const OP_ERASE: u8 = 2;

fn codec_err(msg: impl Into<String>) -> IndexError {
    IndexError::Codec(msg.into())
}

fn event_schema() -> Arc<Schema> {
    let ts = || DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    Arc::new(Schema::new(vec![
        Field::new("_iid", DataType::FixedSizeBinary(16), false),
        Field::new("_system_from", ts(), false),
        Field::new("_valid_from", ts(), false),
        Field::new("_valid_to", ts(), false),
        Field::new("_src_iid", DataType::FixedSizeBinary(16), true),
        Field::new("_dst_iid", DataType::FixedSizeBinary(16), true),
        Field::new("op", DataType::UInt8, false),
        Field::new("payload", DataType::Binary, true),
    ]))
}

/// Encodes a Put's labels and doc into the `payload` column's bytes: u32 LE
/// label count, then per label a u32 LE length + UTF-8 bytes, then the
/// canonical `encode_doc` bytes.
fn encode_put_payload(labels: &[String], doc: &Doc) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&(labels.len() as u32).to_le_bytes());
    for label in labels {
        out.extend_from_slice(&(label.len() as u32).to_le_bytes());
        out.extend_from_slice(label.as_bytes());
    }
    out.extend_from_slice(&encode_doc(doc));
    out
}

fn decode_put_payload(mut input: &[u8]) -> Result<(Vec<String>, Doc), IndexError> {
    fn take<'a>(input: &mut &'a [u8], n: usize) -> Result<&'a [u8], IndexError> {
        if input.len() < n {
            return Err(codec_err(format!(
                "payload: need {n} bytes, have {}",
                input.len()
            )));
        }
        let (head, rest) = input.split_at(n);
        *input = rest;
        Ok(head)
    }
    fn read_u32(input: &mut &[u8]) -> Result<u32, IndexError> {
        let b = take(input, 4)?;
        let arr: [u8; 4] = b.try_into().map_err(|_| codec_err("payload u32"))?;
        Ok(u32::from_le_bytes(arr))
    }

    let label_count = read_u32(&mut input)?;
    let mut labels = Vec::with_capacity(label_count as usize);
    for _ in 0..label_count {
        let len = read_u32(&mut input)? as usize;
        let label = std::str::from_utf8(take(&mut input, len)?)
            .map_err(|e| codec_err(format!("label not UTF-8: {e}")))?;
        labels.push(label.to_string());
    }
    let doc = decode_doc(&mut input).map_err(|e| codec_err(e.to_string()))?;
    if !input.is_empty() {
        return Err(codec_err(format!("{} trailing payload bytes", input.len())));
    }
    Ok((labels, doc))
}

/// Serializes events into one Arrow IPC stream (one RecordBatch; zero
/// batches for an empty slice).
pub fn encode_events(events: &[Event]) -> Result<Vec<u8>, IndexError> {
    let schema = event_schema();
    let mut buf = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buf, &schema)?;
    if !events.is_empty() {
        let mut iid_b = FixedSizeBinaryBuilder::new(16);
        let mut system_from_b = TimestampMicrosecondBuilder::new().with_timezone("UTC");
        let mut valid_from_b = TimestampMicrosecondBuilder::new().with_timezone("UTC");
        let mut valid_to_b = TimestampMicrosecondBuilder::new().with_timezone("UTC");
        let mut src_b = FixedSizeBinaryBuilder::new(16);
        let mut dst_b = FixedSizeBinaryBuilder::new(16);
        let mut op_b = UInt8Builder::new();
        let mut payload_b = BinaryBuilder::new();
        for event in events {
            iid_b.append_value(event.iid.as_bytes())?;
            system_from_b.append_value(event.system_from.as_micros());
            valid_from_b.append_value(event.valid_from.as_micros());
            valid_to_b.append_value(event.valid_to.as_micros());
            match &event.src {
                Some(iid) => src_b
                    .append_value(iid.as_bytes())
                    .map_err(|e| IndexError::Codec(e.to_string()))?,
                None => src_b.append_null(),
            }
            match &event.dst {
                Some(iid) => dst_b
                    .append_value(iid.as_bytes())
                    .map_err(|e| IndexError::Codec(e.to_string()))?,
                None => dst_b.append_null(),
            }
            match &event.op {
                Op::Put { labels, doc } => {
                    op_b.append_value(OP_PUT);
                    payload_b.append_value(encode_put_payload(labels, doc));
                }
                Op::Delete => {
                    op_b.append_value(OP_DELETE);
                    payload_b.append_null();
                }
                Op::Erase => {
                    op_b.append_value(OP_ERASE);
                    payload_b.append_null();
                }
            }
        }
        let columns: Vec<ArrayRef> = vec![
            Arc::new(iid_b.finish()),
            Arc::new(system_from_b.finish()),
            Arc::new(valid_from_b.finish()),
            Arc::new(valid_to_b.finish()),
            Arc::new(src_b.finish()),
            Arc::new(dst_b.finish()),
            Arc::new(op_b.finish()),
            Arc::new(payload_b.finish()),
        ];
        writer.write(&RecordBatch::try_new(schema.clone(), columns)?)?;
    }
    writer.finish()?;
    drop(writer);
    Ok(buf)
}

/// Deserializes events from one Arrow IPC stream produced by `encode_events`.
pub fn decode_events(bytes: &[u8]) -> Result<Vec<Event>, IndexError> {
    let reader = StreamReader::try_new(std::io::Cursor::new(bytes), None)?;
    if reader.schema() != event_schema() {
        return Err(codec_err("event batch schema mismatch"));
    }
    let mut events = Vec::new();
    for batch in reader {
        let batch = batch?;
        let iids = downcast::<FixedSizeBinaryArray>(&batch, 0)?;
        let system_from = downcast::<TimestampMicrosecondArray>(&batch, 1)?;
        let valid_from = downcast::<TimestampMicrosecondArray>(&batch, 2)?;
        let valid_to = downcast::<TimestampMicrosecondArray>(&batch, 3)?;
        let src_col = downcast::<FixedSizeBinaryArray>(&batch, 4)?;
        let dst_col = downcast::<FixedSizeBinaryArray>(&batch, 5)?;
        let ops = downcast::<UInt8Array>(&batch, 6)?;
        let payloads = downcast::<BinaryArray>(&batch, 7)?;
        for row in 0..batch.num_rows() {
            let iid_bytes: [u8; 16] = iids
                .value(row)
                .try_into()
                .map_err(|_| codec_err("_iid column value is not 16 bytes"))?;
            let src = if src_col.is_null(row) {
                None
            } else {
                let mut b = [0u8; 16];
                b.copy_from_slice(src_col.value(row));
                Some(Iid::from_bytes(b))
            };
            let dst = if dst_col.is_null(row) {
                None
            } else {
                let mut b = [0u8; 16];
                b.copy_from_slice(dst_col.value(row));
                Some(Iid::from_bytes(b))
            };
            let op = match ops.value(row) {
                OP_PUT => {
                    if payloads.is_null(row) {
                        return Err(codec_err("Put event with null payload"));
                    }
                    let (labels, doc) = decode_put_payload(payloads.value(row))?;
                    Op::Put { labels, doc }
                }
                OP_DELETE => Op::Delete,
                OP_ERASE => Op::Erase,
                other => return Err(codec_err(format!("unknown op tag {other}"))),
            };
            events.push(Event {
                iid: Iid::from_bytes(iid_bytes),
                system_from: Instant::from_micros(system_from.value(row)),
                valid_from: Instant::from_micros(valid_from.value(row)),
                valid_to: Instant::from_micros(valid_to.value(row)),
                src,
                dst,
                op,
            });
        }
    }
    Ok(events)
}

pub(crate) fn downcast<T: 'static>(batch: &RecordBatch, index: usize) -> Result<&T, IndexError> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| codec_err(format!("column {index} has unexpected array type")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{Event, Op};
    use proptest::prelude::*;
    use std::collections::BTreeMap;
    use varve_types::{Doc, Iid, Instant, Value};

    fn iid(n: u8) -> Iid {
        Iid::derive("g", "nodes", &[n])
    }

    fn us(n: i64) -> Instant {
        Instant::from_micros(n)
    }

    #[test]
    fn round_trips_put_delete_erase() {
        let mut doc = Doc::new();
        doc.insert("name".into(), Value::Str("Ada".into()));
        doc.insert("age".into(), Value::Int(36));
        doc.insert("ghost".into(), Value::Null);
        doc.insert("raw".into(), Value::Bytes(vec![1, 2]));
        let events = vec![
            Event {
                iid: iid(1),
                system_from: us(10),
                valid_from: us(5),
                valid_to: Instant::END_OF_TIME,
                src: None,
                dst: None,
                op: Op::Put {
                    labels: vec!["Person".into(), "Admin".into()],
                    doc,
                },
            },
            Event {
                iid: iid(2),
                system_from: us(10),
                valid_from: us(10),
                valid_to: Instant::END_OF_TIME,
                src: None,
                dst: None,
                op: Op::Delete,
            },
            Event {
                iid: iid(3),
                system_from: us(10),
                valid_from: Instant::MIN,
                valid_to: Instant::END_OF_TIME,
                src: None,
                dst: None,
                op: Op::Erase,
            },
        ];
        let bytes = encode_events(&events).unwrap();
        assert_eq!(decode_events(&bytes).unwrap(), events);
    }

    #[test]
    #[ignore = "regenerates the committed fuzz seed corpus"]
    fn write_events_fuzz_seed() {
        let mut doc = Doc::new();
        doc.insert("name".into(), Value::Str("Ada".into()));
        let events = vec![Event {
            iid: iid(1),
            system_from: us(10),
            valid_from: us(5),
            valid_to: Instant::END_OF_TIME,
            src: None,
            dst: None,
            op: Op::Put {
                labels: vec!["Person".into()],
                doc,
            },
        }];
        let bytes = encode_events(&events).unwrap();
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fuzz/corpus/events");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("valid.bin"), &bytes).unwrap();
    }

    #[test]
    fn round_trips_empty_labels_and_empty_doc() {
        let events = vec![Event {
            iid: iid(1),
            system_from: us(1),
            valid_from: us(1),
            valid_to: us(2),
            src: None,
            dst: None,
            op: Op::Put {
                labels: vec![],
                doc: Doc::new(),
            },
        }];
        let bytes = encode_events(&events).unwrap();
        assert_eq!(decode_events(&bytes).unwrap(), events);
    }

    #[test]
    fn empty_event_slice_round_trips() {
        let bytes = encode_events(&[]).unwrap();
        assert_eq!(decode_events(&bytes).unwrap(), vec![]);
    }

    #[test]
    fn unknown_op_tag_is_rejected() {
        // Build a schema-conformant batch by hand with op = 7.
        use arrow::array::{
            ArrayRef, BinaryBuilder, FixedSizeBinaryBuilder, TimestampMicrosecondBuilder,
            UInt8Builder,
        };
        use arrow::ipc::writer::StreamWriter;
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let schema = event_schema();
        let mut iid_b = FixedSizeBinaryBuilder::new(16);
        iid_b.append_value([0u8; 16]).unwrap();
        let ts = || {
            let mut b = TimestampMicrosecondBuilder::new().with_timezone("UTC");
            b.append_value(1);
            Arc::new(b.finish()) as ArrayRef
        };
        let null_fsb = || {
            let mut b = FixedSizeBinaryBuilder::new(16);
            b.append_null();
            Arc::new(b.finish()) as ArrayRef
        };
        let mut op_b = UInt8Builder::new();
        op_b.append_value(7);
        let mut payload_b = BinaryBuilder::new();
        payload_b.append_null();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(iid_b.finish()),
                ts(),
                ts(),
                ts(),
                null_fsb(),
                null_fsb(),
                Arc::new(op_b.finish()),
                Arc::new(payload_b.finish()),
            ],
        )
        .unwrap();
        let mut buf = Vec::new();
        {
            let mut w = StreamWriter::try_new(&mut buf, &schema).unwrap();
            w.write(&batch).unwrap();
            w.finish().unwrap();
        }
        assert!(matches!(
            decode_events(&buf),
            Err(IndexError::Codec(msg)) if msg.contains("op tag")
        ));
    }

    #[test]
    fn put_without_payload_is_rejected() {
        // op = 0 (Put) but payload null.
        use arrow::array::{
            ArrayRef, BinaryBuilder, FixedSizeBinaryBuilder, TimestampMicrosecondBuilder,
            UInt8Builder,
        };
        use arrow::ipc::writer::StreamWriter;
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let schema = event_schema();
        let mut iid_b = FixedSizeBinaryBuilder::new(16);
        iid_b.append_value([0u8; 16]).unwrap();
        let ts = || {
            let mut b = TimestampMicrosecondBuilder::new().with_timezone("UTC");
            b.append_value(1);
            Arc::new(b.finish()) as ArrayRef
        };
        let null_fsb = || {
            let mut b = FixedSizeBinaryBuilder::new(16);
            b.append_null();
            Arc::new(b.finish()) as ArrayRef
        };
        let mut op_b = UInt8Builder::new();
        op_b.append_value(0);
        let mut payload_b = BinaryBuilder::new();
        payload_b.append_null();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(iid_b.finish()),
                ts(),
                ts(),
                ts(),
                null_fsb(),
                null_fsb(),
                Arc::new(op_b.finish()),
                Arc::new(payload_b.finish()),
            ],
        )
        .unwrap();
        let mut buf = Vec::new();
        {
            let mut w = StreamWriter::try_new(&mut buf, &schema).unwrap();
            w.write(&batch).unwrap();
            w.finish().unwrap();
        }
        assert!(matches!(decode_events(&buf), Err(IndexError::Codec(_))));
    }

    #[test]
    fn wrong_schema_is_rejected() {
        assert!(matches!(
            decode_events(b"not an ipc stream"),
            Err(IndexError::Arrow(_))
        ));
    }

    // Bounded strategies: no NaN (Event's PartialEq would fail); NaN
    // round-tripping is covered bit-exactly in varve-types Task 1.
    fn value_strategy() -> impl Strategy<Value = Value> {
        prop_oneof![
            Just(Value::Null),
            any::<bool>().prop_map(Value::Bool),
            any::<i64>().prop_map(Value::Int),
            (-1.0e12_f64..1.0e12).prop_map(Value::Float),
            "[a-zA-Z0-9 ]{0,12}".prop_map(Value::Str),
            proptest::collection::vec(any::<u8>(), 0..16).prop_map(Value::Bytes),
        ]
    }

    fn event_strategy() -> impl Strategy<Value = Event> {
        let doc = proptest::collection::btree_map("[a-z]{1,8}", value_strategy(), 0..5);
        let labels = proptest::collection::vec("[A-Z][a-z]{0,6}", 0..3);
        let op = prop_oneof![
            (labels, doc).prop_map(|(labels, doc)| Op::Put { labels, doc }),
            Just(Op::Delete),
            Just(Op::Erase),
        ];
        (any::<u8>(), 0..1000i64, 0..1000i64, 1000..2000i64, op).prop_map(
            |(entity, sf, vf, vt, op)| Event {
                iid: Iid::derive("g", "nodes", &[entity]),
                system_from: Instant::from_micros(sf),
                valid_from: Instant::from_micros(vf),
                valid_to: Instant::from_micros(vt),
                src: None,
                dst: None,
                op,
            },
        )
    }

    proptest! {
        #[test]
        fn codec_round_trips_random_events(events in proptest::collection::vec(event_strategy(), 0..20)) {
            let bytes = encode_events(&events).unwrap();
            prop_assert_eq!(decode_events(&bytes).unwrap(), events);
        }
    }

    fn edge_event(n: u8) -> Event {
        Event {
            iid: Iid::derive("g", "edges", &[n]),
            system_from: Instant::from_micros(10),
            valid_from: Instant::from_micros(10),
            valid_to: Instant::END_OF_TIME,
            src: Some(Iid::derive("g", "nodes", &[1])),
            dst: Some(Iid::derive("g", "nodes", &[2])),
            op: Op::Put {
                labels: vec!["KNOWS".into()],
                doc: BTreeMap::from([("_id".into(), Value::Int(n as i64))]),
            },
        }
    }

    #[test]
    fn edge_events_round_trip_with_endpoints() {
        let events = vec![
            edge_event(1),
            Event {
                op: Op::Delete,
                ..edge_event(1)
            },
        ];
        let bytes = encode_events(&events).unwrap();
        assert_eq!(decode_events(&bytes).unwrap(), events);
    }

    #[test]
    fn node_events_round_trip_with_null_endpoints() {
        let events = vec![Event {
            src: None,
            dst: None,
            ..edge_event(3)
        }];
        let bytes = encode_events(&events).unwrap();
        let decoded = decode_events(&bytes).unwrap();
        assert_eq!(decoded[0].src, None);
        assert_eq!(decoded[0].dst, None);
    }
}
