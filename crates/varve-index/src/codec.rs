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

/// Continuation marker prefixing every message length in the modern
/// (>= Arrow v0.15.0 / metadata V5) IPC stream framing. Mirrors arrow-ipc's
/// private `CONTINUATION_MARKER` (`arrow-ipc-58.3.0/src/lib.rs:73`, `[0xff; 4]`).
const IPC_CONTINUATION_MARKER: [u8; 4] = [0xff; 4];

/// Bounds the allocation arrow-ipc's `StreamReader` performs from a message's
/// declared `metadata_size`/`bodyLength` BEFORE any bytes reach arrow.
///
/// `arrow_ipc::reader::MessageReader::maybe_next`
/// (`arrow-ipc-58.3.0/src/reader.rs:1822`) does
/// `self.buf.resize(meta_len, 0)` and
/// `MutableBuffer::from_len_zeroed(message.bodyLength() as usize)` with NO
/// check that those attacker-controlled lengths fit the actual input. A
/// corrupt / bit-rotted / tampered object-store block declaring a huge
/// `bodyLength` (e.g. `i64::MAX`) makes arrow request terabytes and the
/// allocator ABORTS the process — no unwinding happens, so the `catch_unwind`
/// guard on the decoders categorically cannot intercept it. Fuzzing
/// `decode_meta`/`decode_events` surfaced exactly this class once the three
/// unwinding panic classes were guarded.
///
/// This walks the Arrow "encapsulated message" stream framing EXACTLY as
/// arrow's `MessageReader::maybe_next` + `read_meta_len` walk it (verified
/// against that source): optional `0xFFFFFFFF` continuation marker, then
/// `metadata_size: i32 LE` (`== 0` marks end-of-stream), then `metadata_size`
/// bytes of `Message` flatbuffer, then `bodyLength` bytes of body — where the
/// writer folds both the metadata and body 8-byte padding INTO those two
/// counts (`writer.rs:566-598`), so there is no separate padding to skip. It
/// rejects — with a clean [`IndexError::Codec`] — any length prefix,
/// `metadata_size`, or `bodyLength` that is negative or exceeds the remaining
/// input, before arrow can over-allocate.
///
/// It is conservative ONLY in the safe direction: each message's metadata is
/// parsed with arrow's own verifier-backed, panic-free
/// [`arrow::ipc::root_as_message`] — the identical call `maybe_next` makes — so
/// it accepts every stream arrow itself accepts and can never false-reject a
/// genuine block (a false rejection would break recovery/query reads of real
/// data). Called at the very top of both decoders, AHEAD of the `catch_unwind`
/// guard, because the abort it prevents never unwinds and its own body is
/// panic-free (no `unwrap`/`expect`; `root_as_message` returns `Result`).
pub(crate) fn validate_ipc_framing(bytes: &[u8]) -> Result<(), IndexError> {
    let mut pos: usize = 0;
    loop {
        // `read_meta_len` reads a 4-byte length prefix; arrow treats EOF here
        // (fewer than 4 bytes left at a message boundary) as a clean
        // end-of-stream (`Ok(None)`), so we stop WITHOUT rejecting.
        if bytes.len() - pos < 4 {
            return Ok(());
        }
        let mut prefix: [u8; 4] = bytes[pos..pos + 4]
            .try_into()
            .map_err(|_| codec_err("IPC framing: short length prefix"))?;
        pos += 4;
        // A continuation marker means the real `metadata_size` is in the NEXT
        // four bytes (modern format); its absence is the legacy format where
        // the first four bytes ARE the size. After a continuation marker arrow
        // REQUIRES the size (it uses `?`, not the EOF-tolerant path).
        if prefix == IPC_CONTINUATION_MARKER {
            if bytes.len() - pos < 4 {
                return Err(codec_err(
                    "IPC framing: truncated metadata length after continuation marker",
                ));
            }
            prefix = bytes[pos..pos + 4]
                .try_into()
                .map_err(|_| codec_err("IPC framing: short metadata length"))?;
            pos += 4;
        }
        let meta_len = i32::from_le_bytes(prefix);
        // `metadata_size == 0` marks end-of-stream (EOS).
        if meta_len == 0 {
            return Ok(());
        }
        // arrow rejects a negative `metadata_size` (`usize::try_from`).
        let meta_len = usize::try_from(meta_len)
            .map_err(|_| codec_err(format!("IPC framing: negative metadata length {meta_len}")))?;
        // arrow `resize`s a buffer to (and `read_exact`s) `meta_len` bytes —
        // bound it against the remaining input so a huge `metadata_size` can't
        // drive a multi-gigabyte allocation.
        if meta_len > bytes.len() - pos {
            return Err(codec_err(format!(
                "IPC framing: metadata length {meta_len} exceeds {} remaining bytes",
                bytes.len() - pos
            )));
        }
        let metadata = &bytes[pos..pos + meta_len];
        pos += meta_len;
        // Parse with arrow's OWN verifier-backed, panic-free parser — the
        // identical call `maybe_next` makes — to read the body length. Any
        // structurally-invalid flatbuffer is rejected here exactly as arrow
        // would reject it, never more strictly.
        let message = arrow::ipc::root_as_message(metadata)
            .map_err(|e| codec_err(format!("IPC framing: invalid message metadata: {e:?}")))?;
        let body_length = message.bodyLength();
        if body_length < 0 {
            return Err(codec_err(format!(
                "IPC framing: negative message body length {body_length}"
            )));
        }
        let body_length = usize::try_from(body_length).map_err(|_| {
            codec_err(format!(
                "IPC framing: message body length {body_length} does not fit in usize"
            ))
        })?;
        // The critical bound: arrow allocates `bodyLength` bytes via
        // `MutableBuffer::from_len_zeroed` BEFORE reading them. Reject a body
        // that cannot fit in the remaining input rather than let the allocator
        // abort.
        if body_length > bytes.len() - pos {
            return Err(codec_err(format!(
                "IPC framing: message body length {body_length} exceeds {} remaining bytes",
                bytes.len() - pos
            )));
        }
        pos += body_length;
    }
}

/// Absurd `bodyLength` used by the allocation-bound regression tests: ~1.15 EB.
/// Chosen so arrow's `MutableBuffer::from_len_zeroed` PASSES `Layout`
/// construction (it is well under `isize::MAX`) but then FAILS the real
/// allocation — `handle_alloc_error` → process ABORT, with no unwinding. That
/// makes it the genuinely uncatchable class the framing validator must stop,
/// as opposed to `i64::MAX`, which trips a catchable `LayoutError` panic that
/// the `catch_unwind` guard alone would already handle.
#[cfg(test)]
pub(crate) const HUGE_BODY_LEN: i64 = 1 << 60;

/// Test helper: rewrite a valid IPC stream's first record-batch message so its
/// declared `bodyLength` is absurd ([`HUGE_BODY_LEN`]), reproducing the
/// corrupt-object class that made arrow's reader allocate ~1 EB and abort.
/// Walks the framing exactly like [`validate_ipc_framing`], locates the
/// `bodyLength` scalar by re-parsing each candidate offset (so a colliding
/// buffer offset/length can't be patched by mistake), and overwrites it. Shared
/// by both decoders' allocation-bound regression tests.
#[cfg(test)]
pub(crate) fn corrupt_body_length_to_huge(stream: &[u8]) -> Vec<u8> {
    let mut pos: usize = 0;
    loop {
        assert!(
            stream.len() - pos >= 4,
            "no record-batch message to corrupt"
        );
        let mut prefix: [u8; 4] = stream[pos..pos + 4].try_into().unwrap();
        pos += 4;
        if prefix == IPC_CONTINUATION_MARKER {
            prefix = stream[pos..pos + 4].try_into().unwrap();
            pos += 4;
        }
        let meta_len = i32::from_le_bytes(prefix);
        assert!(meta_len > 0, "reached end-of-stream before a record batch");
        let meta_len = meta_len as usize;
        let meta_start = pos;
        let metadata = &stream[meta_start..meta_start + meta_len];
        let message = arrow::ipc::root_as_message(metadata).expect("valid message metadata");
        let body_length = message.bodyLength();
        pos += meta_len;
        if body_length > 0 {
            let needle = body_length.to_le_bytes();
            for (rel, window) in metadata.windows(8).enumerate() {
                if window == needle {
                    let mut out = stream.to_vec();
                    let at = meta_start + rel;
                    out[at..at + 8].copy_from_slice(&HUGE_BODY_LEN.to_le_bytes());
                    let patched =
                        arrow::ipc::root_as_message(&out[meta_start..meta_start + meta_len])
                            .expect("still-valid metadata after patch");
                    if patched.bodyLength() == HUGE_BODY_LEN {
                        return out;
                    }
                }
            }
            panic!("could not locate bodyLength field in record-batch metadata");
        }
        pos += body_length as usize;
    }
}

/// Deserializes events from one Arrow IPC stream produced by `encode_events`.
///
/// Task-6 fuzzing found that arrow-rs 58.3.0's IPC `StreamReader` PANICS
/// (rather than returning an `Err`) on several classes of adversarial
/// flatbuffer/record-batch bytes — see `fuzz/regressions/events/*.bin` and
/// `fuzz/regressions/block_meta/*.bin` (the panics are in shared
/// `arrow_ipc`/`arrow_buffer` internals used by both decoders). No public
/// arrow API avoids these panics (confirmed by reading arrow-ipc's
/// `convert`/`reader` source directly). `decode_events` is a pure function
/// of `bytes` with no shared mutable state, so catching the unwind here and
/// converting it to a clean `IndexError` at this pure-function trust
/// boundary is sound (owner-approved, narrow override of the plan's general
/// no-`catch_unwind` rule for this specific case). The `catch_unwind` and the
/// fuzz-only panic-hook handling live in [`catch_arrow_panic`] — production
/// and `cargo test` builds do NOT touch the process-global panic hook (that
/// swap raced across concurrent decodes); only `cargo fuzz` swaps it, to
/// defeat `libfuzzer-sys`'s abort-before-unwind hook.
///
/// The `catch_unwind` guard handles arrow's UNWINDING panic classes only.
/// Fuzzing surfaced a fourth, distinct class it cannot reach: arrow's
/// `MessageReader::maybe_next` allocates a buffer sized from an unvalidated,
/// attacker-controlled `bodyLength`, so a malformed huge value ABORTS the
/// allocator with no unwind. [`validate_ipc_framing`] runs first (outside the
/// guard) to bound that allocation against the actual input size before arrow
/// sees the bytes.
/// Runs an arrow-IPC decode under `catch_unwind`, turning arrow-rs's
/// adversarial-input panics into a caught unwind the decoders convert to a
/// clean `IndexError`.
///
/// Under `cargo fuzz` (`--cfg fuzzing`) ONLY, `libfuzzer-sys` installs an
/// abort-before-unwind panic hook that defeats `catch_unwind`, so there we swap
/// a no-op hook around the call so the unwind is actually caught. In every
/// other build (production, `cargo test`) we DELIBERATELY do NOT touch the
/// process-global panic hook: `set_hook`/`take_hook` are process-global, so
/// swapping them around a decode that runs concurrently with other decodes (as
/// it does on query-node reads and recovery) races — two interleaved swaps can
/// leave a no-op hook permanently installed, silencing all future panic
/// diagnostics. A bare `catch_unwind` catches arrow's unwinding panics fine;
/// the only cost is arrow printing its panic message to stderr, which is
/// harmless.
pub(crate) fn catch_arrow_panic<T>(decode: impl FnOnce() -> T) -> std::thread::Result<T> {
    #[cfg(fuzzing)]
    let prev_hook = {
        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        prev
    };
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(decode));
    #[cfg(fuzzing)]
    std::panic::set_hook(prev_hook);
    result
}

pub fn decode_events(bytes: &[u8]) -> Result<Vec<Event>, IndexError> {
    validate_ipc_framing(bytes)?;
    match catch_arrow_panic(|| decode_events_uncaught(bytes)) {
        Ok(result) => result,
        Err(_) => Err(IndexError::Codec(
            "arrow IPC decode panicked (corrupt input)".into(),
        )),
    }
}

fn decode_events_uncaught(bytes: &[u8]) -> Result<Vec<Event>, IndexError> {
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

    /// Regression for Task 6's fuzzing: `panic!("Interval type with unit of
    /// {z:?} unsupported")` in `arrow_ipc::convert::get_data_type` for an
    /// out-of-range `IntervalUnit` flatbuffer enum discriminant. Pre-fix,
    /// this input made `decode_events` unwind the process; post-fix, the
    /// `catch_unwind` guard converts it to a clean `Err`.
    #[test]
    fn decode_events_rejects_interval_unit_panic_bytes() {
        let bytes: &[u8] =
            include_bytes!("../../../fuzz/regressions/events/interval-unit-panic.bin");
        assert!(decode_events(bytes).is_err());
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
        // Not an Arrow IPC stream at all. The framing validator now runs
        // first (to bound arrow's allocation): the bogus 4-byte prefix decodes
        // to a `metadata_size` far larger than the input, so it is rejected
        // with a clean `Codec` error before `StreamReader` is ever built.
        // (Pre-framing-validation this surfaced as `IndexError::Arrow`.)
        assert!(matches!(
            decode_events(b"not an ipc stream"),
            Err(IndexError::Codec(_))
        ));
    }

    /// Round-trip over a stream carrying TWO record-batch messages (not just
    /// the schema + single batch + EOS that every other test exercises). This
    /// forces `validate_ipc_framing` to step over two consecutive NON-zero
    /// `bodyLength` messages and still land on the EOS marker — the multi-body
    /// walk a single `encode_events` output can never produce (it always emits
    /// exactly one record batch). If the body-advance were off, the walk would
    /// mis-parse the second message and reject a stream arrow accepts.
    #[test]
    fn decode_events_walks_multi_batch_stream() {
        let events = vec![
            edge_event(1),
            Event {
                op: Op::Delete,
                ..edge_event(1)
            },
        ];
        // Pull a real RecordBatch back out of a valid single-batch stream, then
        // write it TWICE into one stream → two record-batch messages, schema
        // guaranteed to match `event_schema`.
        let single = encode_events(&events).unwrap();
        let mut reader = StreamReader::try_new(std::io::Cursor::new(&single), None).unwrap();
        let batch = reader.next().unwrap().unwrap();

        let schema = event_schema();
        let mut buf = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut buf, &schema).unwrap();
            writer.write(&batch).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }

        let decoded = decode_events(&buf).unwrap();
        let mut expected = events.clone();
        expected.extend(events);
        assert_eq!(decoded, expected);
    }

    /// Allocation-bound regression: a stream whose record-batch message
    /// declares a `bodyLength` of ~1.15 EB ([`HUGE_BODY_LEN`]). Pre-fix, arrow's
    /// `MessageReader::maybe_next` would `MutableBuffer::from_len_zeroed(..)`
    /// that many bytes; the allocation fails and `handle_alloc_error` ABORTS the
    /// process — no unwind, so `catch_unwind` could not intercept it (this test
    /// would kill the runner). Post-fix, `validate_ipc_framing` bounds the body
    /// length against the input and returns a clean `Err`.
    #[test]
    fn decode_events_rejects_unbounded_body_length() {
        let seed = encode_events(&[edge_event(1)]).unwrap();
        let corrupt = corrupt_body_length_to_huge(&seed);
        assert!(matches!(decode_events(&corrupt), Err(IndexError::Codec(_))));
    }

    /// Production-safety regression for the record-batch-descriptor
    /// over-reservation class that `validate_ipc_framing` deliberately does NOT
    /// police (it bounds the stream framing's `metadata_size`/`bodyLength`, not
    /// the record-batch-INTERNAL `FieldNode`/`Buffer` length descriptors one
    /// layer deeper). The committed artifact is the minimized `events` fuzz
    /// input that made arrow's `RecordBatchDecoder::create_primitive_array` ->
    /// `ArrayDataBuilder::build` request ~103 GB (`24 * u32::MAX`) from a
    /// corrupted array-length descriptor. In a normal build that ~103 GB is a
    /// lazily-backed `alloc_zeroed` (OS shared zero page, never touched → no
    /// RSS) and our payload decoder rejects the truncated data first, so
    /// `decode_events` returns a clean `Err` in ~0 ms — which this test asserts
    /// and which passes under an ordinary `cargo test` build. The "crash" the
    /// fuzzer reported was only libFuzzer's `-malloc_limit_mb` allocation-REQUEST
    /// hook firing; the fuzz targets now gate on RSS (`-malloc_limit_mb=0`), and
    /// a conservative descriptor-vs-`bodyLength` bound (or an arrow-rs upgrade)
    /// for strict-no-overcommit deployments is an owned post-v1 follow-up.
    #[test]
    fn decode_events_returns_clean_err_on_oversized_record_batch_length() {
        let bytes: &[u8] =
            include_bytes!("../../../fuzz/regressions/events/record-batch-length-oom.bin");
        assert!(decode_events(bytes).is_err());
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
