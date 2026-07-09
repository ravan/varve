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
