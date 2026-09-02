//! Bulk-ingest wire types for `POST /v1/ingest` (roadmap slice BI-1).
//!
//! The NDJSON contract: one JSON object per `\n`-terminated line, tagged by
//! `type` (`node` / `edge`). Records decode straight to the engine's
//! [`NodePut`]/[`EdgePut`] data ops — no GQL parse or plan anywhere on this
//! path. Every decode error carries the 1-based input line number so a
//! client can fix its exporter without guessing.

#[cfg(feature = "bulk")]
pub mod csv;

use std::collections::HashMap;

use arrow::array::{
    Array, BinaryArray, BooleanArray, FixedSizeBinaryArray, Float64Array, Int64Array, ListArray,
    StringArray,
};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as JsonValue};
use varve_config::ByteSize;
use varve_engine::{EdgePut, NodePut, TxReceipt};
use varve_types::{Doc, Instant, Value};

use crate::ServerError;

/// Default `[ingest] chunk_ops`: decoded ops per atomic `Db::ingest`
/// transaction (mirrors the embedded bulk-ingest bench chunking). The bulk
/// contract's home for this value — the HTTP handler, the CLI's embedded bulk
/// import, and the generated configuration reference all read it here so the
/// docs cannot drift.
pub const DEFAULT_CHUNK_OPS: usize = 10_000;

/// Default `[ingest] max_line_bytes`: the largest a single NDJSON line (the
/// un-newlined pending buffer) or CSV record may grow before the stream is
/// rejected, so a missing newline can never buffer unbounded.
pub const DEFAULT_MAX_LINE_BYTES: ByteSize = ByteSize::from_bytes(1024 * 1024);

/// The record framer the `/v1/ingest` handler drives, selected by
/// `Content-Type`. Both variants stream frames to `push` and flush with
/// `finish`, returning ordered [`BulkOp`]s, so the handler's chunked-commit
/// loop is format-agnostic. Also drives the CLI's embedded bulk import, which
/// is why it is gated on `bulk` (the CSV decoder) rather than the heavier
/// `http` feature.
#[cfg(feature = "bulk")]
pub enum Framer {
    Ndjson(NdjsonFramer),
    // Boxed: `CsvFramer` carries several scratch buffers and is far larger
    // than the NDJSON variant.
    Csv(Box<csv::CsvFramer>),
}

#[cfg(feature = "bulk")]
impl Framer {
    pub fn push(&mut self, frame: &[u8]) -> Result<Vec<BulkOp>, BulkDecodeError> {
        match self {
            Framer::Ndjson(framer) => framer.push(frame),
            Framer::Csv(framer) => framer.push(frame),
        }
    }

    pub fn finish(self) -> Result<Vec<BulkOp>, BulkDecodeError> {
        match self {
            Framer::Ndjson(framer) => framer.finish(),
            Framer::Csv(framer) => framer.finish(),
        }
    }
}

/// One decoded record, in input order. Order is preserved end-to-end so the
/// handler's chunk boundaries correspond to input-line prefixes (the
/// committed-progress contract when a later chunk fails).
#[derive(Clone, Debug)]
pub enum BulkOp {
    Node(NodePut),
    Edge(EdgePut),
}

/// The NDJSON record shapes, discriminated by `type`. Unknown fields are
/// rejected: silently dropping a field a client believed was honored would
/// corrupt the caller's intent. `valid_from`/`valid_to` (BI-4) are optional
/// top-level fields — RFC3339 string or integer microseconds — matching GQL
/// `INSERT … VALID`; absent means "valid from now".
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase", deny_unknown_fields)]
pub enum BulkRecord {
    Node {
        #[serde(default)]
        labels: Vec<String>,
        #[serde(default)]
        props: Map<String, JsonValue>,
        #[serde(default)]
        valid_from: Option<JsonValue>,
        #[serde(default)]
        valid_to: Option<JsonValue>,
    },
    Edge {
        label: String,
        src: JsonValue,
        dst: JsonValue,
        #[serde(default)]
        props: Map<String, JsonValue>,
        #[serde(default)]
        valid_from: Option<JsonValue>,
        #[serde(default)]
        valid_to: Option<JsonValue>,
    },
}

/// A decode failure pinned to its 1-based input line.
#[derive(Debug, PartialEq, Eq)]
pub struct BulkDecodeError {
    pub line: usize,
    pub message: String,
}

impl std::fmt::Display for BulkDecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "line {}: {}", self.line, self.message)
    }
}

impl std::error::Error for BulkDecodeError {}

/// Decodes a buffered NDJSON stream into ordered bulk ops. Blank (or
/// whitespace-only) lines are skipped but still count toward line numbers; a
/// trailing newline on the last record is not required.
pub fn decode_ndjson_lines(input: &str) -> Result<Vec<BulkOp>, BulkDecodeError> {
    let mut ops = Vec::new();
    for (index, line) in input.lines().enumerate() {
        if let Some(op) = decode_line(line).map_err(|message| BulkDecodeError {
            line: index + 1,
            message,
        })? {
            ops.push(op);
        }
    }
    Ok(ops)
}

/// Decodes one physical line into an op, or `Ok(None)` for a blank or
/// whitespace-only line. The line-number context is supplied by the caller
/// (`decode_ndjson_lines` / [`NdjsonFramer`]) since it differs by path.
fn decode_line(line: &str) -> Result<Option<BulkOp>, String> {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    let record: BulkRecord = serde_json::from_str(trimmed).map_err(|error| error.to_string())?;
    Ok(Some(record_to_op(record)?))
}

fn record_to_op(record: BulkRecord) -> Result<BulkOp, String> {
    match record {
        BulkRecord::Node {
            labels,
            props,
            valid_from,
            valid_to,
        } => Ok(BulkOp::Node(NodePut {
            labels,
            doc: doc_from_props(&props)?,
            valid_from: parse_valid("valid_from", valid_from)?,
            valid_to: parse_valid("valid_to", valid_to)?,
        })),
        BulkRecord::Edge {
            label,
            src,
            dst,
            props,
            valid_from,
            valid_to,
        } => Ok(BulkOp::Edge(EdgePut {
            label,
            src: id_value("src", &src)?,
            dst: id_value("dst", &dst)?,
            doc: doc_from_props(&props)?,
            valid_from: parse_valid("valid_from", valid_from)?,
            valid_to: parse_valid("valid_to", valid_to)?,
        })),
    }
}

/// Parses a valid-time field: RFC3339 string (exactly GQL's TIMESTAMP parse)
/// or integer microseconds. Absent or `null` ⇒ `None` (engine default).
fn parse_valid(field: &str, value: Option<JsonValue>) -> Result<Option<Instant>, String> {
    match value {
        None | Some(JsonValue::Null) => Ok(None),
        Some(JsonValue::String(text)) => Instant::parse_rfc3339(&text)
            .map(Some)
            .map_err(|error| format!("`{field}` is not a valid RFC3339 timestamp: {error}")),
        Some(JsonValue::Number(number)) => number
            .as_i64()
            .map(|micros| Some(Instant::from_micros(micros)))
            .ok_or_else(|| format!("`{field}` must be an RFC3339 string or integer microseconds")),
        Some(_) => Err(format!(
            "`{field}` must be an RFC3339 string or integer microseconds"
        )),
    }
}

/// Property conversion: the `/v1/tx` params rules ([`scalar_from_json`]),
/// except `null` is rejected outright (the bulk contract has no use for a
/// null property — absence expresses it) and `_id`, when present, must be a
/// valid entity id (string or integer) so a bad id fails here with a line
/// number instead of surfacing as an un-numbered engine error at resolve.
fn doc_from_props(props: &Map<String, JsonValue>) -> Result<Doc, String> {
    let mut doc = Doc::new();
    for (key, value) in props {
        if value.is_null() {
            return Err(format!(
                "property `{key}` is null; omit absent properties instead"
            ));
        }
        if key == "_id" {
            doc.insert(key.clone(), id_value("_id", value)?);
            continue;
        }
        let scalar = crate::api::scalar_from_json(value)
            .map_err(|error| format!("property `{key}`: {error}"))?;
        doc.insert(key.clone(), scalar);
    }
    Ok(doc)
}

/// `_id` / `src` / `dst` per the wire spec: a JSON string or integer.
fn id_value(field: &str, value: &JsonValue) -> Result<Value, String> {
    match value {
        JsonValue::String(id) => Ok(Value::Str(id.clone())),
        JsonValue::Number(id) => id.as_i64().map(Value::Int).ok_or_else(|| {
            format!("`{field}` must be a JSON string or integer representable as i64")
        }),
        other => Err(format!(
            "`{field}` must be a JSON string or integer, got {other}"
        )),
    }
}

/// Incremental NDJSON framer for a streamed request body (BI-2). Bytes are
/// buffered across frames and split on `\n`; each complete line decodes via
/// [`decode_line`]. Splitting on `\n` (0x0A) is UTF-8-safe — that byte never
/// appears inside a multibyte sequence — so a line's bytes are always whole
/// before UTF-8 validation. Physical line numbers advance across frames
/// (blank lines included), matching `decode_ndjson_lines`. The pending
/// (un-newlined) buffer is capped at `max_line_bytes` so a stream with no
/// newline cannot buffer unbounded.
pub struct NdjsonFramer {
    buf: Vec<u8>,
    line: usize,
    max_line_bytes: usize,
}

impl NdjsonFramer {
    pub fn new(max_line_bytes: usize) -> Self {
        Self {
            buf: Vec::new(),
            line: 0,
            max_line_bytes,
        }
    }

    /// Feeds one body frame, returning the ops for every line the frame (or
    /// earlier buffered bytes) completed.
    pub fn push(&mut self, frame: &[u8]) -> Result<Vec<BulkOp>, BulkDecodeError> {
        self.buf.extend_from_slice(frame);
        let mut ops = Vec::new();
        // Drain every complete (newline-terminated) line from the buffer.
        while let Some(newline) = self.buf.iter().position(|&byte| byte == b'\n') {
            self.line += 1;
            // A complete line whose content exceeds the cap is rejected too,
            // so no single line — terminated or not — can buffer unbounded.
            if newline > self.max_line_bytes {
                return Err(self.overflow_error());
            }
            let line: Vec<u8> = self.buf.drain(..=newline).collect();
            // The `\n` is at the end; decode the bytes before it.
            if let Some(op) = self.decode_bytes(&line[..line.len() - 1])? {
                ops.push(op);
            }
        }
        // The remaining, un-newlined tail must stay within the cap.
        if self.buf.len() > self.max_line_bytes {
            self.line += 1;
            return Err(self.overflow_error());
        }
        Ok(ops)
    }

    /// Flushes a trailing line that had no terminating newline.
    pub fn finish(mut self) -> Result<Vec<BulkOp>, BulkDecodeError> {
        if self.buf.is_empty() {
            return Ok(Vec::new());
        }
        self.line += 1;
        let tail = std::mem::take(&mut self.buf);
        Ok(self.decode_bytes(&tail)?.into_iter().collect())
    }

    fn decode_bytes(&self, bytes: &[u8]) -> Result<Option<BulkOp>, BulkDecodeError> {
        let fail = |message: String| BulkDecodeError {
            line: self.line,
            message,
        };
        let text = std::str::from_utf8(bytes).map_err(|error| fail(error.to_string()))?;
        decode_line(text).map_err(fail)
    }

    fn overflow_error(&self) -> BulkDecodeError {
        BulkDecodeError {
            line: self.line,
            message: format!(
                "line exceeds max_line_bytes ({} bytes) with no terminating newline",
                self.max_line_bytes
            ),
        }
    }
}

/// The `200` summary: chunk receipts folded together. `basis` is the last
/// committed tx id, usable as a `basis` on a subsequent read exactly like
/// `TxResponse.basis`; `system_time`/`system_time_us` come from the last
/// chunk's receipt (same rendering as `TxResponse`).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct IngestResponse {
    pub nodes: u64,
    pub edges: u64,
    pub transactions: u64,
    pub basis: u64,
    pub system_time: String,
    pub system_time_us: i64,
}

impl IngestResponse {
    /// Builds the success body from the folded progress and the LAST chunk
    /// receipt (which `progress` has already absorbed).
    pub fn from_committed(progress: &IngestProgress, last: &TxReceipt) -> Self {
        Self {
            nodes: progress.nodes,
            edges: progress.edges,
            transactions: progress.transactions,
            basis: progress.basis,
            system_time: last.system_time.to_string(),
            system_time_us: last.system_time.as_micros(),
        }
    }
}

/// Committed progress: what is durably in the store when a later chunk (or
/// the decode) fails — earlier chunks stay committed, per the roadmap's
/// per-chunk atomicity contract.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct IngestProgress {
    pub nodes: u64,
    pub edges: u64,
    pub transactions: u64,
    pub basis: u64,
}

impl IngestProgress {
    /// Folds one chunk receipt into the running totals.
    pub fn absorb(&mut self, receipt: &TxReceipt) {
        self.nodes += receipt.side_effects.nodes_created as u64;
        self.edges += receipt.side_effects.relationships_created as u64;
        self.transactions += 1;
        self.basis = receipt.tx_id;
    }
}

/// Mid-stream failure body: the error plus what already committed.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct IngestErrorResponse {
    pub error: String,
    pub committed: IngestProgress,
}

/// Columns the engine snapshot ([`varve::Db::snapshot_all_nodes`]) carries
/// that are NOT bulk-record properties: identity, the two temporal rectangles,
/// labels, and edge endpoints. Every other column is a property — including
/// the upsert key `_id`.
const SYSTEM_COLUMNS: &[&str] = &[
    "_iid",
    "_system_from",
    "_system_to",
    "_valid_from",
    "_valid_to",
    "_labels",
    "_src_iid",
    "_dst_iid",
];

/// Outcome of a whole-graph NDJSON export ([`write_ndjson`]).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ExportSummary {
    pub nodes: u64,
    pub edges: u64,
    /// Edges dropped because they cannot be expressed as a bulk edge record:
    /// an endpoint iid with no `_id` in the node snapshot (a dangling edge, or
    /// an endpoint node that carries no `_id`), or an edge whose label set is
    /// not exactly one label (the wire record has a single `label`).
    pub edges_skipped: u64,
}

/// Serializes a whole-graph snapshot (from [`varve::Db::snapshot_all_nodes`] /
/// `snapshot_all_edges`) as bulk NDJSON — node records first, then edge
/// records — so that `varve export --format ndjson | varve import` copies a
/// graph Varve→Varve (BI-5). This is the exact reverse of
/// [`decode_ndjson_lines`]: emitted records carry only the fields the decoder
/// accepts (no `valid_from`/`valid_to` — export captures the CURRENT state,
/// one version per entity, and import re-stamps valid-from-now), so the output
/// re-imports without transformation.
///
/// Edge endpoints are stored as one-way-hashed iids, so `src`/`dst` are
/// recovered by mapping `_src_iid`/`_dst_iid` through the `_iid`→`_id` map
/// built from the node snapshot. An edge whose endpoint is not in that map (or
/// which carries anything other than exactly one label) is skipped and counted
/// in [`ExportSummary::edges_skipped`].
pub fn write_ndjson<W: std::io::Write>(
    nodes: Option<&RecordBatch>,
    edges: Option<&RecordBatch>,
    out: &mut W,
) -> Result<ExportSummary, ServerError> {
    let mut summary = ExportSummary::default();
    let mut iid_to_id: HashMap<[u8; 16], JsonValue> = HashMap::new();

    if let Some(batch) = nodes {
        let iids = fixed_binary_column(batch, "_iid")?;
        let labels = list_column(batch, "_labels")?;
        for row in 0..batch.num_rows() {
            let props = props_at(batch, row)?;
            if let (Some(id), Ok(bytes)) = (props.get("_id"), <[u8; 16]>::try_from(iids.value(row)))
            {
                iid_to_id.insert(bytes, id.clone());
            }
            let record = serde_json::json!({
                "type": "node",
                "labels": labels_at(labels, row),
                "props": props,
            });
            write_record(out, &record)?;
            summary.nodes += 1;
        }
    }

    if let Some(batch) = edges {
        let labels = list_column(batch, "_labels")?;
        let src = fixed_binary_column(batch, "_src_iid")?;
        let dst = fixed_binary_column(batch, "_dst_iid")?;
        for row in 0..batch.num_rows() {
            let label = match labels_at(labels, row).as_slice() {
                [single] => single.clone(),
                _ => {
                    summary.edges_skipped += 1;
                    continue;
                }
            };
            let (Ok(src_bytes), Ok(dst_bytes)) = (
                <[u8; 16]>::try_from(src.value(row)),
                <[u8; 16]>::try_from(dst.value(row)),
            ) else {
                summary.edges_skipped += 1;
                continue;
            };
            let (Some(src_id), Some(dst_id)) =
                (iid_to_id.get(&src_bytes), iid_to_id.get(&dst_bytes))
            else {
                summary.edges_skipped += 1;
                continue;
            };
            let record = serde_json::json!({
                "type": "edge",
                "label": label,
                "src": src_id,
                "dst": dst_id,
                "props": props_at(batch, row)?,
            });
            write_record(out, &record)?;
            summary.edges += 1;
        }
    }

    Ok(summary)
}

/// Writes one record as a `\n`-terminated NDJSON line. Serialization of a
/// `serde_json::Value` built here cannot actually fail (finite numbers, valid
/// strings), but the error is mapped rather than unwrapped in library code.
fn write_record<W: std::io::Write>(out: &mut W, record: &JsonValue) -> Result<(), ServerError> {
    let mut bytes = serde_json::to_vec(record).map_err(|error| {
        ServerError::Protocol(format!("failed to serialize export record: {error}"))
    })?;
    bytes.push(b'\n');
    out.write_all(&bytes)?;
    Ok(())
}

/// The non-system columns of `row` as a bulk `props` object. Null cells are
/// omitted (absence expresses them, matching the decode contract, which
/// rejects null props).
fn props_at(batch: &RecordBatch, row: usize) -> Result<Map<String, JsonValue>, ServerError> {
    let mut props = Map::new();
    for (index, field) in batch.schema().fields().iter().enumerate() {
        if SYSTEM_COLUMNS.contains(&field.name().as_str()) {
            continue;
        }
        if let Some(value) = column_value(batch.column(index).as_ref(), row)? {
            props.insert(field.name().clone(), value);
        }
    }
    Ok(props)
}

/// One cell as a JSON value matching the wire contract (the inverse of
/// [`crate::api::scalar_from_json`]): integers/floats as numbers, strings and
/// booleans directly, binary as `{"$bytes": "<base64>"}`. A null cell is
/// `None` (the caller omits the property).
fn column_value(array: &dyn Array, row: usize) -> Result<Option<JsonValue>, ServerError> {
    if array.is_null(row) {
        return Ok(None);
    }
    let value = match array.data_type() {
        DataType::Int64 => JsonValue::from(downcast::<Int64Array>(array)?.value(row)),
        DataType::Float64 => JsonValue::from(downcast::<Float64Array>(array)?.value(row)),
        DataType::Utf8 => JsonValue::String(downcast::<StringArray>(array)?.value(row).to_string()),
        DataType::Boolean => JsonValue::Bool(downcast::<BooleanArray>(array)?.value(row)),
        DataType::Binary => serde_json::json!({
            "$bytes": BASE64.encode(downcast::<BinaryArray>(array)?.value(row)),
        }),
        other => {
            return Err(ServerError::Protocol(format!(
                "snapshot column has unexportable type {other:?}"
            )))
        }
    };
    Ok(Some(value))
}

fn downcast<T: 'static>(array: &dyn Array) -> Result<&T, ServerError> {
    array
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| ServerError::Protocol("snapshot column had an unexpected array type".into()))
}

fn fixed_binary_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a FixedSizeBinaryArray, ServerError> {
    batch
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref::<FixedSizeBinaryArray>())
        .ok_or_else(|| ServerError::Protocol(format!("snapshot missing `{name}` column")))
}

fn list_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a ListArray, ServerError> {
    batch
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref::<ListArray>())
        .ok_or_else(|| ServerError::Protocol(format!("snapshot missing `{name}` column")))
}

/// The labels of one row of a `_labels` `List<Utf8>` column.
fn labels_at(list: &ListArray, row: usize) -> Vec<String> {
    if list.is_null(row) {
        return Vec::new();
    }
    let values = list.value(row);
    match values.as_any().downcast_ref::<StringArray>() {
        Some(strings) => (0..strings.len())
            .filter(|&i| !strings.is_null(i))
            .map(|i| strings.value(i).to_string())
            .collect(),
        None => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode(input: &str) -> Vec<BulkOp> {
        decode_ndjson_lines(input).expect("stream must decode")
    }

    fn decode_err(input: &str) -> BulkDecodeError {
        decode_ndjson_lines(input).expect_err("stream must be rejected")
    }

    #[test]
    fn nodes_and_edges_decode_in_input_order() {
        let ops = decode(concat!(
            "{\"type\":\"node\",\"labels\":[\"Person\"],\"props\":{\"_id\":\"p1\",\"name\":\"Ada\",\"age\":36}}\n",
            "{\"type\":\"edge\",\"label\":\"KNOWS\",\"src\":\"p1\",\"dst\":2,\"props\":{\"since\":2001}}\n",
        ));
        assert_eq!(ops.len(), 2);
        let BulkOp::Node(node) = &ops[0] else {
            panic!("first op must be the node");
        };
        assert_eq!(node.labels, vec!["Person".to_string()]);
        assert_eq!(node.doc.get("_id"), Some(&Value::Str("p1".into())));
        assert_eq!(node.doc.get("name"), Some(&Value::Str("Ada".into())));
        assert_eq!(node.doc.get("age"), Some(&Value::Int(36)));
        let BulkOp::Edge(edge) = &ops[1] else {
            panic!("second op must be the edge");
        };
        assert_eq!(edge.label, "KNOWS");
        assert_eq!(edge.src, Value::Str("p1".into()));
        assert_eq!(edge.dst, Value::Int(2));
        assert_eq!(edge.doc.get("since"), Some(&Value::Int(2001)));
    }

    #[test]
    fn value_mapping_matches_tx_params_rules() {
        let ops = decode(
            "{\"type\":\"node\",\"props\":{\"_id\":7,\"ok\":true,\"score\":1.5,\
             \"blob\":{\"$bytes\":\"AQI=\"}}}",
        );
        let BulkOp::Node(node) = &ops[0] else {
            panic!("must be a node");
        };
        assert_eq!(node.doc.get("_id"), Some(&Value::Int(7)));
        assert_eq!(node.doc.get("ok"), Some(&Value::Bool(true)));
        assert_eq!(node.doc.get("score"), Some(&Value::Float(1.5)));
        assert_eq!(node.doc.get("blob"), Some(&Value::Bytes(vec![1, 2])));
    }

    #[test]
    fn id_absent_and_empty_labels_are_valid() {
        let ops = decode("{\"type\":\"node\",\"props\":{\"name\":\"anon\"}}");
        let BulkOp::Node(node) = &ops[0] else {
            panic!("must be a node");
        };
        assert!(node.labels.is_empty());
        assert!(!node.doc.contains_key("_id"));
    }

    #[test]
    fn blank_lines_skip_but_count_and_trailing_newline_is_optional() {
        let ops = decode(concat!(
            "\n",
            "{\"type\":\"node\",\"props\":{\"_id\":\"a\"}}\n",
            "   \n",
            "{\"type\":\"node\",\"props\":{\"_id\":\"b\"}}",
        ));
        assert_eq!(ops.len(), 2);
        // Blank lines still advance the counter: an error on the last line
        // reports the physical line number 5.
        let error = decode_err(concat!(
            "\n",
            "{\"type\":\"node\",\"props\":{\"_id\":\"a\"}}\n",
            "   \n",
            "{\"type\":\"node\",\"props\":{\"_id\":\"b\"}}\n",
            "not json",
        ));
        assert_eq!(error.line, 5);
    }

    #[test]
    fn non_object_line_reports_its_line_number() {
        let error = decode_err("{\"type\":\"node\",\"props\":{}}\n42\n");
        assert_eq!(error.line, 2);
    }

    #[test]
    fn unknown_type_tag_is_rejected() {
        let error = decode_err("{\"type\":\"blob\",\"props\":{}}");
        assert_eq!(error.line, 1);
        assert!(error.message.contains("blob"), "{error}");
    }

    #[test]
    fn missing_type_tag_is_rejected() {
        let error = decode_err("{\"props\":{\"_id\":1}}");
        assert_eq!(error.line, 1);
        assert!(error.message.contains("type"), "{error}");
    }

    #[test]
    fn nested_property_values_are_rejected_with_key_context() {
        let error = decode_err("{\"type\":\"node\",\"props\":{\"tags\":[1,2]}}");
        assert_eq!(error.line, 1);
        assert!(error.message.contains("tags"), "{error}");
        let error = decode_err("{\"type\":\"node\",\"props\":{\"meta\":{\"a\":1}}}");
        assert!(error.message.contains("meta"), "{error}");
    }

    #[test]
    fn null_property_values_are_rejected() {
        let error = decode_err("{\"type\":\"node\",\"props\":{\"name\":null}}");
        assert_eq!(error.line, 1);
        assert!(error.message.contains("name"), "{error}");
        assert!(error.message.contains("null"), "{error}");
    }

    #[test]
    fn unsigned_overflow_property_is_rejected() {
        let error = decode_err("{\"type\":\"node\",\"props\":{\"n\":18446744073709551615}}");
        assert_eq!(error.line, 1);
        assert!(error.message.contains('n'), "{error}");
    }

    #[test]
    fn node_id_must_be_string_or_integer() {
        let error = decode_err("{\"type\":\"node\",\"props\":{\"_id\":1.5}}");
        assert_eq!(error.line, 1);
        assert!(error.message.contains("_id"), "{error}");
        let error = decode_err("{\"type\":\"node\",\"props\":{\"_id\":true}}");
        assert!(error.message.contains("_id"), "{error}");
    }

    #[test]
    fn edge_missing_endpoints_or_label_is_rejected() {
        let error = decode_err("{\"type\":\"edge\",\"label\":\"KNOWS\",\"src\":\"a\"}");
        assert_eq!(error.line, 1);
        assert!(error.message.contains("dst"), "{error}");
        let error = decode_err("{\"type\":\"edge\",\"label\":\"KNOWS\",\"dst\":\"b\"}");
        assert!(error.message.contains("src"), "{error}");
        let error = decode_err("{\"type\":\"edge\",\"src\":\"a\",\"dst\":\"b\"}");
        assert!(error.message.contains("label"), "{error}");
    }

    #[test]
    fn edge_endpoints_must_be_string_or_integer() {
        let error =
            decode_err("{\"type\":\"edge\",\"label\":\"KNOWS\",\"src\":true,\"dst\":\"b\"}");
        assert_eq!(error.line, 1);
        assert!(error.message.contains("src"), "{error}");
        let error =
            decode_err("{\"type\":\"edge\",\"label\":\"KNOWS\",\"src\":\"a\",\"dst\":null}");
        assert!(error.message.contains("dst"), "{error}");
    }

    #[test]
    fn unknown_record_fields_are_rejected_not_dropped() {
        // A field the format does not define is rejected, not silently
        // dropped — dropping one a client believed was honored would corrupt
        // its intent.
        let error = decode_err("{\"type\":\"node\",\"props\":{\"_id\":1},\"bogus_field\":\"x\"}");
        assert_eq!(error.line, 1);
        assert!(error.message.contains("bogus_field"), "{error}");
    }

    #[test]
    fn valid_time_fields_parse_rfc3339_and_micros() {
        let ops = decode(concat!(
            "{\"type\":\"node\",\"props\":{\"_id\":1},\"valid_from\":\"2020-01-01T00:00:00Z\"}\n",
            "{\"type\":\"node\",\"props\":{\"_id\":2},\"valid_from\":1577836800000000}\n",
        ));
        let BulkOp::Node(a) = &ops[0] else {
            panic!("node");
        };
        let BulkOp::Node(b) = &ops[1] else {
            panic!("node");
        };
        // RFC3339 midnight 2020-01-01 UTC == 1_577_836_800_000_000 µs.
        assert_eq!(
            a.valid_from,
            Some(Instant::from_micros(1_577_836_800_000_000))
        );
        assert_eq!(a.valid_from, b.valid_from);
        assert!(a.valid_to.is_none());
    }

    #[test]
    fn malformed_valid_time_is_rejected_with_line() {
        let error = decode_err("{\"type\":\"node\",\"props\":{\"_id\":1},\"valid_from\":\"nope\"}");
        assert_eq!(error.line, 1);
        assert!(error.message.contains("valid_from"), "{error}");
    }

    #[test]
    fn empty_stream_decodes_to_zero_ops() {
        assert!(decode("").is_empty());
        assert!(decode("\n\n  \n").is_empty());
    }

    #[test]
    fn error_display_carries_the_line_prefix() {
        let error = decode_err("nope");
        assert_eq!(error.to_string(), format!("line 1: {}", error.message));
    }

    fn receipt(tx_id: u64, nodes: usize, edges: usize) -> TxReceipt {
        let side_effects = varve_engine::SideEffects {
            nodes_created: nodes,
            relationships_created: edges,
            ..Default::default()
        };
        TxReceipt {
            tx_id,
            system_time: varve_types::Instant::from_micros(1_700_000_000_000_000),
            side_effects,
            user: "demo".into(),
        }
    }

    #[test]
    fn progress_folds_receipts_keeping_last_tx_as_basis() {
        let mut progress = IngestProgress::default();
        progress.absorb(&receipt(41, 3, 5));
        progress.absorb(&receipt(42, 2, 1));
        assert_eq!(
            progress,
            IngestProgress {
                nodes: 5,
                edges: 6,
                transactions: 2,
                basis: 42,
            }
        );
    }

    #[test]
    fn ingest_response_carries_folded_totals_and_last_receipt_time() {
        let mut progress = IngestProgress::default();
        let last = receipt(7, 10, 20);
        progress.absorb(&last);
        let response = IngestResponse::from_committed(&progress, &last);
        assert_eq!(response.nodes, 10);
        assert_eq!(response.edges, 20);
        assert_eq!(response.transactions, 1);
        assert_eq!(response.basis, 7);
        assert_eq!(response.system_time_us, 1_700_000_000_000_000);
        assert_eq!(response.system_time, last.system_time.to_string());
    }

    /// Drives a framer with one frame per element of `frames` and returns the
    /// concatenated ops (or the first error).
    fn frame_all(frames: &[&[u8]], max_line_bytes: usize) -> Result<Vec<BulkOp>, BulkDecodeError> {
        let mut framer = NdjsonFramer::new(max_line_bytes);
        let mut ops = Vec::new();
        for frame in frames {
            ops.extend(framer.push(frame)?);
        }
        ops.extend(framer.finish()?);
        Ok(ops)
    }

    fn op_keys(ops: &[BulkOp]) -> Vec<String> {
        ops.iter()
            .map(|op| match op {
                BulkOp::Node(node) => format!("node:{:?}", node.doc.get("_id")),
                BulkOp::Edge(edge) => format!("edge:{}:{:?}->{:?}", edge.label, edge.src, edge.dst),
            })
            .collect()
    }

    const SAMPLE_STREAM: &str = concat!(
        "{\"type\":\"node\",\"labels\":[\"Person\"],\"props\":{\"_id\":\"p1\",\"name\":\"Adaé\"}}\n",
        "\n",
        "{\"type\":\"node\",\"props\":{\"_id\":2}}\n",
        "{\"type\":\"edge\",\"label\":\"KNOWS\",\"src\":\"p1\",\"dst\":2}\n",
    );

    #[test]
    fn framer_over_one_frame_matches_buffered_decode() {
        let ops = frame_all(&[SAMPLE_STREAM.as_bytes()], 1 << 20).unwrap();
        let buffered = decode_ndjson_lines(SAMPLE_STREAM).unwrap();
        assert_eq!(op_keys(&ops), op_keys(&buffered));
        assert_eq!(ops.len(), 3);
    }

    #[test]
    fn framer_is_frame_boundary_independent() {
        // The core streaming property: chopping a valid stream at EVERY byte
        // offset must decode identically to feeding it whole. Deterministic —
        // no randomness — so it is a stable regression pin.
        let bytes = SAMPLE_STREAM.as_bytes();
        let whole = op_keys(&frame_all(&[bytes], 1 << 20).unwrap());
        for split in 0..=bytes.len() {
            let (a, b) = bytes.split_at(split);
            let chopped = op_keys(&frame_all(&[a, b], 1 << 20).unwrap());
            assert_eq!(chopped, whole, "split at {split}");
        }
        // Also byte-at-a-time.
        let single: Vec<&[u8]> = bytes.chunks(1).collect();
        assert_eq!(op_keys(&frame_all(&single, 1 << 20).unwrap()), whole);
    }

    #[test]
    fn framer_trailing_line_without_newline_decodes() {
        let ops = frame_all(&[b"{\"type\":\"node\",\"props\":{\"_id\":1}}"], 1 << 20).unwrap();
        assert_eq!(ops.len(), 1);
    }

    #[test]
    fn framer_reports_physical_line_number_across_frames() {
        // Frame 1 ends mid-third-line; the bad record is physical line 3.
        let error = frame_all(
            &[
                b"{\"type\":\"node\",\"props\":{\"_id\":1}}\n\n{\"type\":\"edge\",\"lab",
                b"el\":\"KNOWS\",\"src\":\"a\"}\n",
            ],
            1 << 20,
        )
        .unwrap_err();
        assert_eq!(error.line, 3);
        assert!(error.message.contains("dst"), "{error}");
    }

    #[test]
    fn framer_caps_the_pending_buffer_at_max_line_bytes() {
        // A line longer than the cap, arriving with no newline, must be
        // rejected rather than buffered unbounded.
        let mut framer = NdjsonFramer::new(16);
        let result = framer.push(b"{\"type\":\"node\",\"props\":{\"_id\":\"aaaaaaaaaaaaaa\"}}");
        let error = result.expect_err("oversized pending line must be rejected");
        assert_eq!(error.line, 1);
        assert!(error.message.to_lowercase().contains("line"), "{error}");
    }

    #[test]
    fn framer_rejects_complete_line_exceeding_cap() {
        // Even a newline-terminated line is rejected if its content exceeds
        // the cap — the memory bound holds for terminated lines too.
        let mut framer = NdjsonFramer::new(16);
        let error = framer
            .push(b"{\"type\":\"node\",\"props\":{\"_id\":\"aaaaaaaaaaaaaa\"}}\n")
            .expect_err("oversized complete line must be rejected");
        assert_eq!(error.line, 1);
    }

    #[test]
    fn framer_allows_lines_up_to_the_cap() {
        // A complete line whose bytes fit under the cap decodes fine even when
        // the cap is modest.
        let line = b"{\"type\":\"node\",\"props\":{\"_id\":1}}\n";
        let ops = frame_all(&[line], line.len()).unwrap();
        assert_eq!(ops.len(), 1);
    }

    #[test]
    fn error_response_matches_the_wire_shape() {
        let body = serde_json::to_value(IngestErrorResponse {
            error: "line 5041: edge record missing `dst`".into(),
            committed: IngestProgress {
                nodes: 40000,
                edges: 0,
                transactions: 4,
                basis: 12290,
            },
        })
        .unwrap();
        assert_eq!(
            body,
            serde_json::json!({
                "error": "line 5041: edge record missing `dst`",
                "committed": {"nodes": 40000, "edges": 0, "transactions": 4, "basis": 12290},
            })
        );
    }

    // ---- Export serializer (`write_ndjson`) -------------------------------

    use arrow::array::{
        BinaryBuilder, FixedSizeBinaryBuilder, Int64Builder, ListBuilder, StringBuilder,
    };
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    fn iid(byte: u8) -> [u8; 16] {
        [byte; 16]
    }

    /// A node snapshot batch matching the engine's schema: `_iid`, `_labels`,
    /// then property columns. `_system_*`/`_valid_*` are omitted — the
    /// serializer keys off `SYSTEM_COLUMNS` by name, so their presence is
    /// irrelevant to the export shape.
    fn node_batch() -> RecordBatch {
        let mut iid_b = FixedSizeBinaryBuilder::new(16);
        iid_b.append_value(iid(1)).unwrap();
        iid_b.append_value(iid(2)).unwrap();

        let mut labels_b = ListBuilder::new(StringBuilder::new());
        labels_b.values().append_value("Person");
        labels_b.append(true);
        labels_b.values().append_value("Person");
        labels_b.append(true);

        let mut id_b = StringBuilder::new();
        id_b.append_value("a");
        id_b.append_value("b");

        let mut name_b = StringBuilder::new();
        name_b.append_value("Ada");
        name_b.append_null(); // b has no name → omitted from props

        let mut blob_b = BinaryBuilder::new();
        blob_b.append_value([0x00, 0x01]);
        blob_b.append_null();

        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("_iid", DataType::FixedSizeBinary(16), false),
                Field::new(
                    "_labels",
                    DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                    false,
                ),
                Field::new("_id", DataType::Utf8, true),
                Field::new("name", DataType::Utf8, true),
                Field::new("blob", DataType::Binary, true),
            ])),
            vec![
                Arc::new(iid_b.finish()),
                Arc::new(labels_b.finish()),
                Arc::new(id_b.finish()),
                Arc::new(name_b.finish()),
                Arc::new(blob_b.finish()),
            ],
        )
        .unwrap()
    }

    /// Three edges: a→b resolvable, a→(unknown) unresolvable, and a multi-label
    /// edge that cannot be a single-`label` bulk record.
    fn edge_batch() -> RecordBatch {
        let mut iid_b = FixedSizeBinaryBuilder::new(16);
        let mut src_b = FixedSizeBinaryBuilder::new(16);
        let mut dst_b = FixedSizeBinaryBuilder::new(16);
        for (e, s, d) in [(10, 1, 2), (11, 1, 9), (12, 1, 2)] {
            iid_b.append_value(iid(e)).unwrap();
            src_b.append_value(iid(s)).unwrap();
            dst_b.append_value(iid(d)).unwrap();
        }

        let mut labels_b = ListBuilder::new(StringBuilder::new());
        labels_b.values().append_value("KNOWS");
        labels_b.append(true);
        labels_b.values().append_value("KNOWS");
        labels_b.append(true);
        labels_b.values().append_value("A");
        labels_b.values().append_value("B");
        labels_b.append(true);

        let mut since_b = Int64Builder::new();
        since_b.append_value(2001);
        since_b.append_value(2002);
        since_b.append_value(2003);

        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("_iid", DataType::FixedSizeBinary(16), false),
                Field::new("_src_iid", DataType::FixedSizeBinary(16), false),
                Field::new("_dst_iid", DataType::FixedSizeBinary(16), false),
                Field::new(
                    "_labels",
                    DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                    false,
                ),
                Field::new("since", DataType::Int64, true),
            ])),
            vec![
                Arc::new(iid_b.finish()),
                Arc::new(src_b.finish()),
                Arc::new(dst_b.finish()),
                Arc::new(labels_b.finish()),
                Arc::new(since_b.finish()),
            ],
        )
        .unwrap()
    }

    fn export(
        nodes: Option<&RecordBatch>,
        edges: Option<&RecordBatch>,
    ) -> (Vec<JsonValue>, ExportSummary) {
        let mut out = Vec::new();
        let summary = write_ndjson(nodes, edges, &mut out).expect("export must succeed");
        let text = String::from_utf8(out).expect("export must be utf8");
        let lines = text
            .lines()
            .map(|line| serde_json::from_str(line).expect("each line is json"))
            .collect();
        (lines, summary)
    }

    #[test]
    fn write_ndjson_emits_node_records_with_labels_props_and_tagged_bytes() {
        let nodes = node_batch();
        let (lines, summary) = export(Some(&nodes), None);
        assert_eq!(
            summary,
            ExportSummary {
                nodes: 2,
                edges: 0,
                edges_skipped: 0
            }
        );
        assert_eq!(lines.len(), 2);
        assert_eq!(
            lines[0],
            serde_json::json!({
                "type": "node",
                "labels": ["Person"],
                "props": {"_id": "a", "name": "Ada", "blob": {"$bytes": "AAE="}},
            })
        );
        // b's null `name` is omitted, not emitted as null (decode rejects null).
        assert_eq!(
            lines[1],
            serde_json::json!({"type": "node", "labels": ["Person"], "props": {"_id": "b"}})
        );
    }

    #[test]
    fn write_ndjson_maps_edge_endpoints_and_skips_unresolvable_and_multilabel() {
        let nodes = node_batch();
        let edges = edge_batch();
        let (lines, summary) = export(Some(&nodes), Some(&edges));
        // 2 nodes + only the a→b single-label edge; the a→unknown and the
        // multi-label edge are skipped.
        assert_eq!(
            summary,
            ExportSummary {
                nodes: 2,
                edges: 1,
                edges_skipped: 2
            }
        );
        assert_eq!(lines.len(), 3);
        assert_eq!(
            lines[2],
            serde_json::json!({
                "type": "edge",
                "label": "KNOWS",
                "src": "a",
                "dst": "b",
                "props": {"since": 2001},
            })
        );
    }

    #[test]
    fn write_ndjson_output_re_decodes_through_the_import_path() {
        // The round-trip contract at the serializer level: everything
        // write_ndjson emits is accepted by decode_ndjson_lines (no field the
        // decoder's deny_unknown_fields would reject, no null prop).
        let nodes = node_batch();
        let edges = edge_batch();
        let mut out = Vec::new();
        write_ndjson(Some(&nodes), Some(&edges), &mut out).unwrap();
        let text = String::from_utf8(out).unwrap();
        let ops = decode_ndjson_lines(&text).expect("exported NDJSON must re-import");
        let node_ops = ops
            .iter()
            .filter(|op| matches!(op, BulkOp::Node(_)))
            .count();
        let edge_ops = ops
            .iter()
            .filter(|op| matches!(op, BulkOp::Edge(_)))
            .count();
        assert_eq!((node_ops, edge_ops), (2, 1));
    }

    #[test]
    fn write_ndjson_on_empty_graph_writes_nothing() {
        let (lines, summary) = export(None, None);
        assert!(lines.is_empty());
        assert_eq!(summary, ExportSummary::default());
    }
}
