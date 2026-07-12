use crate::log::LogError;
use prost::Message;

/// Resolved effects for one table: Arrow IPC bytes of the event batch
/// (spec §6 "Arrow for the payload").
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TableEffects {
    #[prost(string, tag = "1")]
    pub table: String,
    #[prost(bytes = "vec", tag = "2")]
    pub arrow_ipc: Vec<u8>,
    #[prost(string, tag = "3")]
    pub graph: String,
}

/// One transaction's log record — the spec §6 protobuf envelope
/// `{tx_id, system_time, user, effects}`. `user` is carried empty in v1.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LogRecord {
    #[prost(uint64, tag = "1")]
    pub tx_id: u64,
    #[prost(int64, tag = "2")]
    pub system_time_us: i64,
    #[prost(string, tag = "3")]
    pub user: String,
    #[prost(message, repeated, tag = "4")]
    pub effects: Vec<TableEffects>,
}

impl LogRecord {
    /// Protobuf wire bytes (the payload framed by each log backend).
    pub fn to_wire(&self) -> Vec<u8> {
        self.encode_to_vec()
    }

    pub fn from_wire(bytes: &[u8]) -> Result<LogRecord, LogError> {
        Ok(<LogRecord as Message>::decode(bytes)?)
    }

    /// Encoded size without allocating (group-commit size accounting).
    pub fn wire_len(&self) -> usize {
        self.encoded_len()
    }
}

/// Frame header: `len: u32 LE` (payload length) + `crc: u32 LE` (CRC32C of
/// the payload), immediately followed by the payload itself — the one log
/// frame grammar shared by every durable backend.
pub(crate) const FRAME_HEADER: usize = 8;

/// Decodes a buffer that is expected to hold zero or more complete,
/// back-to-back log frames (`len u32 LE · crc32c u32 LE · protobuf payload`).
///
/// STRICT, whole-buffer contract: every byte in `bytes` must belong to a
/// complete, CRC-valid frame whose payload decodes as a `LogRecord`. A
/// truncated header, a truncated payload, a CRC mismatch, a protobuf decode
/// failure, or trailing bytes that don't form another complete frame all
/// produce a `LogError::Corrupt` naming `context`. Zero-length input decodes
/// to `Ok(vec![])`. The length prefix is bounds-checked against the
/// remaining buffer BEFORE any slicing, so an adversarial length (e.g.
/// `u32::MAX`) can never be used to slice past the buffer or allocate
/// unboundedly.
///
/// This is the STRICT sibling of `local.rs::scan_segment`: this function is
/// for atomic whole objects (an object-store PUT, or a fuzz input) where a
/// torn tail can never legitimately occur. `scan_segment` is a DIFFERENT,
/// LENIENT contract used only for the local segmented log, where a torn tail
/// on the last, still-being-written segment is expected after a crash and is
/// tolerated (truncated away) rather than treated as corruption.
pub fn decode_frames(context: &str, bytes: &[u8]) -> Result<Vec<LogRecord>, LogError> {
    let mut records = Vec::new();
    let mut off = 0usize;
    while let Some(record) = decode_one_frame(context, bytes, &mut off)? {
        records.push(record);
    }
    Ok(records)
}

/// Decodes a single frame at `*off`, advancing it past the frame on
/// success. Returns `Ok(None)` once `*off` reaches the end of `bytes`
/// exactly (the strict end-of-buffer condition); any other short read is a
/// `Corrupt` error.
pub(crate) fn decode_one_frame(
    context: &str,
    bytes: &[u8],
    off: &mut usize,
) -> Result<Option<LogRecord>, LogError> {
    if *off == bytes.len() {
        return Ok(None);
    }
    if bytes.len() - *off < FRAME_HEADER {
        return Err(corrupt(context, *off, "truncated frame header"));
    }
    let len = u32::from_le_bytes([
        bytes[*off],
        bytes[*off + 1],
        bytes[*off + 2],
        bytes[*off + 3],
    ]) as usize;
    let crc = u32::from_le_bytes([
        bytes[*off + 4],
        bytes[*off + 5],
        bytes[*off + 6],
        bytes[*off + 7],
    ]);
    // Bounds-check the claimed length against what remains BEFORE slicing:
    // an adversarial len (e.g. u32::MAX) must be a clean error here, never a
    // slice-index panic or an unbounded allocation.
    if bytes.len() - *off - FRAME_HEADER < len {
        return Err(corrupt(context, *off, "truncated frame payload"));
    }
    let payload = &bytes[*off + FRAME_HEADER..*off + FRAME_HEADER + len];
    if crc32c::crc32c(payload) != crc {
        return Err(corrupt(context, *off, "CRC mismatch"));
    }
    *off += FRAME_HEADER + len;
    Ok(Some(LogRecord::from_wire(payload)?))
}

fn corrupt(context: &str, offset: usize, reason: &str) -> LogError {
    LogError::Corrupt {
        path: context.to_string(),
        offset: offset as u64,
        reason: reason.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> LogRecord {
        LogRecord {
            tx_id: 1,
            system_time_us: 2,
            user: String::new(),
            effects: vec![TableEffects {
                table: "nodes".into(),
                arrow_ipc: vec![0xAA],
                graph: String::new(),
            }],
        }
    }

    #[test]
    fn wire_round_trips() {
        let rec = sample();
        let bytes = rec.to_wire();
        assert_eq!(rec.wire_len(), bytes.len());
        assert_eq!(LogRecord::from_wire(&bytes).unwrap(), rec);
    }

    #[test]
    fn wire_golden_bytes() {
        // Pins field numbers and wire types (protobuf wire format is stable,
        // so exact bytes are safe to golden-test — unlike Arrow IPC).
        assert_eq!(
            sample().to_wire(),
            vec![
                0x08, 0x01, // field 1 varint: tx_id = 1
                0x10, 0x02, // field 2 varint: system_time_us = 2
                // field 3 (user) omitted: proto3 default (empty string)
                0x22, 0x0A, // field 4, length-delimited, 10 bytes
                0x0A, 0x05, b'n', b'o', b'd', b'e', b's', // effects.table
                0x12, 0x01, 0xAA, // effects.arrow_ipc
            ]
        );
    }

    #[test]
    fn from_wire_rejects_garbage() {
        assert!(matches!(
            LogRecord::from_wire(&[0xFF, 0xFF, 0xFF]),
            Err(LogError::Decode(_))
        ));
    }

    fn frame(payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        out.extend_from_slice(&crc32c::crc32c(payload).to_le_bytes());
        out.extend_from_slice(payload);
        out
    }

    fn sample_record(tx_id: u64) -> LogRecord {
        LogRecord {
            tx_id,
            system_time_us: 1_700_000_000_000_000 + tx_id as i64,
            user: String::new(),
            effects: vec![],
        }
    }

    #[test]
    fn decode_frames_round_trips_a_multi_frame_stream() {
        let records = vec![sample_record(1), sample_record(2)];
        let mut bytes = Vec::new();
        for record in &records {
            bytes.extend_from_slice(&frame(&record.to_wire()));
        }
        assert_eq!(decode_frames("test", &bytes).unwrap(), records);
        assert_eq!(decode_frames("test", &[]).unwrap(), Vec::<LogRecord>::new());
    }

    #[test]
    fn decode_frames_rejects_truncation_crc_and_trailing_garbage() {
        let good = frame(&sample_record(1).to_wire());
        // Truncated header, truncated payload, flipped CRC byte, trailing garbage.
        assert!(decode_frames("t", &good[..3]).is_err());
        assert!(decode_frames("t", &good[..good.len() - 1]).is_err());
        let mut bad_crc = good.clone();
        bad_crc[4] ^= 0xFF;
        assert!(decode_frames("t", &bad_crc).is_err());
        let mut trailing = good.clone();
        trailing.push(0x00);
        assert!(decode_frames("t", &trailing).is_err());
    }

    #[test]
    fn decode_frames_rejects_an_absurd_length_prefix_without_allocating() {
        // len = u32::MAX with a tiny buffer must be a clean error, not an OOM/panic.
        let mut bytes = u32::MAX.to_le_bytes().to_vec();
        bytes.extend_from_slice(&[0u8; 8]);
        assert!(decode_frames("t", &bytes).is_err());
    }

    #[test]
    #[ignore = "regenerates the committed fuzz seed corpus"]
    fn write_log_record_fuzz_seed() {
        let mut bytes = Vec::new();
        for record in [sample_record(1), sample_record(2)] {
            bytes.extend_from_slice(&frame(&record.to_wire()));
        }
        let dir =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fuzz/corpus/log_record");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("valid-two-frames.bin"), bytes).unwrap();
    }

    #[test]
    fn table_effects_graph_roundtrip_and_empty_is_default() {
        let old_wire = sample().to_wire();
        assert_eq!(
            old_wire,
            vec![
                0x08, 0x01, // field 1 varint: tx_id = 1
                0x10, 0x02, // field 2 varint: system_time_us = 2
                // field 3 (user) omitted: proto3 default (empty string)
                0x22, 0x0A, // field 4, length-delimited, 10 bytes
                0x0A, 0x05, b'n', b'o', b'd', b'e', b's', // effects.table
                0x12, 0x01, 0xAA, // effects.arrow_ipc
            ],
            "adding graph tag 3 must not change existing default wire bytes"
        );
        let decoded = LogRecord::from_wire(&old_wire).unwrap();
        assert_eq!(decoded.effects[0].graph, "");

        let rec = LogRecord {
            tx_id: 7,
            system_time_us: 8,
            user: String::new(),
            effects: vec![TableEffects {
                table: "nodes".into(),
                arrow_ipc: vec![0xBB],
                graph: "tenant_a".into(),
            }],
        };
        let decoded = LogRecord::from_wire(&rec.to_wire()).unwrap();
        assert_eq!(decoded.effects[0].graph, "tenant_a");
    }
}
