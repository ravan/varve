//! Neo4j-`admin`-dialect CSV decoding for `POST /v1/ingest` (roadmap slice
//! BI-3). Parsing is incremental (`csv_core`), so a CSV upload streams through
//! the same chunked-commit pipeline as NDJSON — quoting, escaping, and quoted
//! embedded newlines/commas are all handled, which a naive line-split cannot.
//!
//! Header conventions (the de-facto property-graph CSV dialect):
//! `:ID`, `:LABEL` (`;`-separated multi-label), `:START_ID`, `:END_ID`,
//! `:TYPE`; property columns `name` / `name:string` / `name:int` /
//! `name:float` / `name:boolean`. A file is edges iff it has BOTH `:START_ID`
//! and `:END_ID`; otherwise nodes. Ids (`:ID`/`:START_ID`/`:END_ID`) map to
//! `Value::Str` (Neo4j ids are strings — use NDJSON for integer ids). An empty
//! cell means the property is absent.

use super::{BulkDecodeError, BulkOp};
use csv_core::{ReadRecordResult, Reader};
use varve_engine::{EdgePut, NodePut};
use varve_types::{Doc, Instant, Value};

/// The property value type declared by a `name:type` header suffix.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PropType {
    Str,
    Int,
    Float,
    Bool,
}

/// One parsed header column.
#[derive(Clone, Debug, PartialEq, Eq)]
enum ColumnKind {
    Id,
    Label,
    StartId,
    EndId,
    Type,
    ValidFrom,
    ValidTo,
    Prop { name: String, ty: PropType },
}

/// A parsed CSV header: the per-column kinds and whether this file carries
/// edges (both `:START_ID` and `:END_ID` present) or nodes.
#[derive(Clone, Debug, PartialEq, Eq)]
struct CsvHeader {
    columns: Vec<ColumnKind>,
    edges: bool,
}

impl CsvHeader {
    /// Parses the header row. `row` is its 1-based line for error context.
    fn parse(fields: &[String], row: usize) -> Result<CsvHeader, BulkDecodeError> {
        let fail = |message: String| BulkDecodeError { line: row, message };
        let mut columns = Vec::with_capacity(fields.len());
        for field in fields {
            columns.push(parse_column(field).map_err(&fail)?);
        }
        let has_start = columns.contains(&ColumnKind::StartId);
        let has_end = columns.contains(&ColumnKind::EndId);
        let edges = has_start && has_end;
        if edges && !columns.contains(&ColumnKind::Type) {
            return Err(fail(
                "edge CSV (has :START_ID and :END_ID) requires a :TYPE column".into(),
            ));
        }
        if !edges && (has_start || has_end) {
            return Err(fail(
                ":START_ID and :END_ID must both be present (edge file) or both absent (node file)"
                    .into(),
            ));
        }
        Ok(CsvHeader { columns, edges })
    }
}

fn parse_column(field: &str) -> Result<ColumnKind, String> {
    match field {
        ":ID" => Ok(ColumnKind::Id),
        ":LABEL" => Ok(ColumnKind::Label),
        ":START_ID" => Ok(ColumnKind::StartId),
        ":END_ID" => Ok(ColumnKind::EndId),
        ":TYPE" => Ok(ColumnKind::Type),
        ":VALID_FROM" => Ok(ColumnKind::ValidFrom),
        ":VALID_TO" => Ok(ColumnKind::ValidTo),
        other if other.starts_with(':') => Err(format!("unknown CSV meta-header `{other}`")),
        other => {
            let (name, ty) = match other.split_once(':') {
                None => (other, PropType::Str),
                Some((name, suffix)) => {
                    let ty = match suffix {
                        "string" => PropType::Str,
                        "int" => PropType::Int,
                        "float" => PropType::Float,
                        "boolean" => PropType::Bool,
                        _ => {
                            return Err(format!(
                                "unknown property type `{suffix}` in header `{other}`"
                            ))
                        }
                    };
                    (name, ty)
                }
            };
            if name.is_empty() {
                return Err(format!("property header `{other}` has an empty name"));
            }
            Ok(ColumnKind::Prop {
                name: name.to_string(),
                ty,
            })
        }
    }
}

/// Incremental Neo4j-dialect CSV framer. Feeds body frames to a `csv_core`
/// state machine, emitting one `BulkOp` per data row (the first row is the
/// header). Bounds the bytes buffered for a single in-progress record at
/// `max_line_bytes`.
pub struct CsvFramer {
    reader: Reader,
    input: Vec<u8>,
    consumed: usize,
    output: Vec<u8>,
    out_len: usize,
    ends: Vec<usize>,
    ends_len: usize,
    header: Option<CsvHeader>,
    row: usize,
    max_line_bytes: usize,
}

impl CsvFramer {
    pub fn new(max_line_bytes: usize) -> Self {
        Self {
            reader: Reader::new(),
            input: Vec::new(),
            consumed: 0,
            output: vec![0; 1024],
            out_len: 0,
            ends: vec![0; 32],
            ends_len: 0,
            header: None,
            row: 0,
            max_line_bytes,
        }
    }

    pub fn push(&mut self, frame: &[u8]) -> Result<Vec<BulkOp>, BulkDecodeError> {
        self.input.extend_from_slice(frame);
        self.drain(false)
    }

    pub fn finish(mut self) -> Result<Vec<BulkOp>, BulkDecodeError> {
        self.drain(true)
    }

    fn drain(&mut self, eof: bool) -> Result<Vec<BulkOp>, BulkDecodeError> {
        let mut ops = Vec::new();
        loop {
            // Grow scratch buffers so a record bounded by `max_line_bytes`
            // never trips OutputFull/OutputEndsFull mid-record.
            if self.out_len == self.output.len() {
                let next = (self.output.len() * 2).max(1024);
                self.output.resize(next, 0);
            }
            if self.ends_len == self.ends.len() {
                let next = (self.ends.len() * 2).max(32);
                self.ends.resize(next, 0);
            }
            let remaining = &self.input[self.consumed..];
            // Bound the bytes buffered for one not-yet-complete record.
            if remaining.len() > self.max_line_bytes {
                return Err(self.cap_error());
            }
            let (result, nin, nout, nend) = self.reader.read_record(
                remaining,
                &mut self.output[self.out_len..],
                &mut self.ends[self.ends_len..],
            );
            self.consumed += nin;
            self.out_len += nout;
            self.ends_len += nend;
            match result {
                ReadRecordResult::InputEmpty => {
                    if eof {
                        // Feed the reader empty input to flush a trailing
                        // record with no terminating newline.
                        continue;
                    }
                    break;
                }
                ReadRecordResult::OutputFull | ReadRecordResult::OutputEndsFull => continue,
                ReadRecordResult::End => break,
                ReadRecordResult::Record => {
                    if let Some(op) = self.finish_record()? {
                        ops.push(op);
                    }
                    self.out_len = 0;
                    self.ends_len = 0;
                }
            }
        }
        // Drop the consumed prefix so `input` only holds the in-progress record.
        self.input.drain(..self.consumed);
        self.consumed = 0;
        Ok(ops)
    }

    /// Turns the just-parsed record (in `output`/`ends`) into a `BulkOp`, or
    /// consumes it as the header (returning `None`).
    fn finish_record(&mut self) -> Result<Option<BulkOp>, BulkDecodeError> {
        let fields = self.record_fields()?;
        self.row += 1;
        let row = self.row;
        let fail = move |message: String| BulkDecodeError { line: row, message };
        match self.header.clone() {
            None => {
                self.header = Some(CsvHeader::parse(&fields, row)?);
                Ok(None)
            }
            Some(header) => Ok(Some(build_op(&header, &fields, &fail)?)),
        }
    }

    /// Decodes the current record's fields as UTF-8 strings.
    fn record_fields(&self) -> Result<Vec<String>, BulkDecodeError> {
        let mut fields = Vec::with_capacity(self.ends_len);
        let mut start = 0;
        for &end in &self.ends[..self.ends_len] {
            let bytes = &self.output[start..end];
            let text = std::str::from_utf8(bytes).map_err(|error| BulkDecodeError {
                line: self.row + 1,
                message: error.to_string(),
            })?;
            fields.push(text.to_string());
            start = end;
        }
        Ok(fields)
    }

    fn cap_error(&self) -> BulkDecodeError {
        BulkDecodeError {
            line: self.row + 1,
            message: format!(
                "CSV record exceeds max_line_bytes ({} bytes)",
                self.max_line_bytes
            ),
        }
    }
}

fn build_op(
    header: &CsvHeader,
    fields: &[String],
    fail: &impl Fn(String) -> BulkDecodeError,
) -> Result<BulkOp, BulkDecodeError> {
    if fields.len() != header.columns.len() {
        return Err(fail(format!(
            "row has {} fields but the header declares {}",
            fields.len(),
            header.columns.len()
        )));
    }
    let mut doc = Doc::new();
    let mut labels: Vec<String> = Vec::new();
    let mut label: Option<String> = None;
    let mut src: Option<Value> = None;
    let mut dst: Option<Value> = None;
    let mut valid_from: Option<Instant> = None;
    let mut valid_to: Option<Instant> = None;
    for (column, cell) in header.columns.iter().zip(fields) {
        match column {
            ColumnKind::Id => {
                if !cell.is_empty() {
                    doc.insert("_id".into(), Value::Str(cell.clone()));
                }
            }
            ColumnKind::Label => {
                labels.extend(
                    cell.split(';')
                        .filter(|s| !s.is_empty())
                        .map(str::to_string),
                );
            }
            ColumnKind::StartId => src = Some(Value::Str(cell.clone())),
            ColumnKind::EndId => dst = Some(Value::Str(cell.clone())),
            ColumnKind::Type => {
                if !cell.is_empty() {
                    label = Some(cell.clone());
                }
            }
            ColumnKind::ValidFrom => valid_from = parse_valid(":VALID_FROM", cell, fail)?,
            ColumnKind::ValidTo => valid_to = parse_valid(":VALID_TO", cell, fail)?,
            ColumnKind::Prop { name, ty } => {
                if cell.is_empty() {
                    continue; // empty cell = property absent
                }
                doc.insert(name.clone(), convert(name, *ty, cell, fail)?);
            }
        }
    }
    if header.edges {
        let src = src.filter(|v| !matches!(v, Value::Str(s) if s.is_empty()));
        let dst = dst.filter(|v| !matches!(v, Value::Str(s) if s.is_empty()));
        Ok(BulkOp::Edge(EdgePut {
            label: label.ok_or_else(|| fail("edge row is missing its :TYPE value".into()))?,
            src: src.ok_or_else(|| fail("edge row is missing its :START_ID value".into()))?,
            dst: dst.ok_or_else(|| fail("edge row is missing its :END_ID value".into()))?,
            doc,
            valid_from,
            valid_to,
        }))
    } else {
        Ok(BulkOp::Node(NodePut {
            labels,
            doc,
            valid_from,
            valid_to,
        }))
    }
}

/// Parses a `:VALID_FROM`/`:VALID_TO` cell: empty ⇒ `None` (default);
/// an all-integer cell ⇒ microseconds; otherwise RFC3339 (GQL's TIMESTAMP
/// parse). Same value grammar as NDJSON, applied to the stringly-typed cell.
fn parse_valid(
    field: &str,
    cell: &str,
    fail: &impl Fn(String) -> BulkDecodeError,
) -> Result<Option<Instant>, BulkDecodeError> {
    if cell.is_empty() {
        return Ok(None);
    }
    if let Ok(micros) = cell.parse::<i64>() {
        return Ok(Some(Instant::from_micros(micros)));
    }
    Instant::parse_rfc3339(cell).map(Some).map_err(|error| {
        fail(format!(
            "{field}: `{cell}` is not RFC3339 or integer µs: {error}"
        ))
    })
}

fn convert(
    name: &str,
    ty: PropType,
    cell: &str,
    fail: &impl Fn(String) -> BulkDecodeError,
) -> Result<Value, BulkDecodeError> {
    match ty {
        PropType::Str => Ok(Value::Str(cell.to_string())),
        PropType::Int => cell
            .parse::<i64>()
            .map(Value::Int)
            .map_err(|_| fail(format!("column `{name}`: `{cell}` is not an integer"))),
        PropType::Float => cell
            .parse::<f64>()
            .ok()
            .filter(|v| v.is_finite())
            .map(Value::Float)
            .ok_or_else(|| fail(format!("column `{name}`: `{cell}` is not a finite float"))),
        PropType::Bool => match cell {
            "true" => Ok(Value::Bool(true)),
            "false" => Ok(Value::Bool(false)),
            _ => Err(fail(format!(
                "column `{name}`: `{cell}` is not a boolean (true/false)"
            ))),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame_all(frames: &[&[u8]], max_line_bytes: usize) -> Result<Vec<BulkOp>, BulkDecodeError> {
        let mut framer = CsvFramer::new(max_line_bytes);
        let mut ops = Vec::new();
        for frame in frames {
            ops.extend(framer.push(frame)?);
        }
        ops.extend(framer.finish()?);
        Ok(ops)
    }

    fn one(input: &str) -> Result<Vec<BulkOp>, BulkDecodeError> {
        frame_all(&[input.as_bytes()], 1 << 20)
    }

    const NODES: &str = "\
:ID,name,age:int,active:boolean,score:float,:LABEL
p1,Ada,36,true,1.5,Person;Employee
p2,Bob,,false,,Person
";

    #[test]
    fn node_rows_decode_with_typed_columns_and_multilabel() {
        let ops = one(NODES).unwrap();
        assert_eq!(ops.len(), 2);
        let BulkOp::Node(p1) = &ops[0] else {
            panic!("expected node");
        };
        assert_eq!(
            p1.labels,
            vec!["Person".to_string(), "Employee".to_string()]
        );
        assert_eq!(p1.doc.get("_id"), Some(&Value::Str("p1".into())));
        assert_eq!(p1.doc.get("name"), Some(&Value::Str("Ada".into())));
        assert_eq!(p1.doc.get("age"), Some(&Value::Int(36)));
        assert_eq!(p1.doc.get("active"), Some(&Value::Bool(true)));
        assert_eq!(p1.doc.get("score"), Some(&Value::Float(1.5)));
        // p2: empty age and score cells => properties absent.
        let BulkOp::Node(p2) = &ops[1] else {
            panic!("expected node");
        };
        assert!(!p2.doc.contains_key("age"));
        assert!(!p2.doc.contains_key("score"));
        assert_eq!(p2.doc.get("active"), Some(&Value::Bool(false)));
        assert_eq!(p2.labels, vec!["Person".to_string()]);
    }

    #[test]
    fn edge_rows_decode_from_start_end_type_headers() {
        let ops = one("\
:START_ID,:END_ID,:TYPE,since:int
p1,p2,KNOWS,2001
")
        .unwrap();
        assert_eq!(ops.len(), 1);
        let BulkOp::Edge(edge) = &ops[0] else {
            panic!("expected edge");
        };
        assert_eq!(edge.label, "KNOWS");
        assert_eq!(edge.src, Value::Str("p1".into()));
        assert_eq!(edge.dst, Value::Str("p2".into()));
        assert_eq!(edge.doc.get("since"), Some(&Value::Int(2001)));
    }

    #[test]
    fn quoted_fields_with_commas_and_newlines_are_handled() {
        let ops = one("\
:ID,name,:LABEL
p1,\"Ada, the \"\"First\"\"\",Person
p2,\"line one\nline two\",Person
")
        .unwrap();
        let BulkOp::Node(p1) = &ops[0] else {
            panic!("node");
        };
        assert_eq!(
            p1.doc.get("name"),
            Some(&Value::Str("Ada, the \"First\"".into()))
        );
        let BulkOp::Node(p2) = &ops[1] else {
            panic!("node");
        };
        assert_eq!(
            p2.doc.get("name"),
            Some(&Value::Str("line one\nline two".into()))
        );
    }

    #[test]
    fn frame_boundary_independent() {
        let bytes = NODES.as_bytes();
        let whole = format!("{:?}", one(NODES).unwrap());
        for split in 0..=bytes.len() {
            let (a, b) = bytes.split_at(split);
            let chopped = format!("{:?}", frame_all(&[a, b], 1 << 20).unwrap());
            assert_eq!(chopped, whole, "split at {split}");
        }
        let single: Vec<&[u8]> = bytes.chunks(1).collect();
        assert_eq!(format!("{:?}", frame_all(&single, 1 << 20).unwrap()), whole);
    }

    #[test]
    fn trailing_row_without_newline_decodes() {
        let ops = one(":ID,name\np1,Ada").unwrap();
        assert_eq!(ops.len(), 1);
    }

    #[test]
    fn unknown_meta_header_is_rejected_with_context() {
        let error = one(":ID,:BOGUS\np1,x\n").unwrap_err();
        assert_eq!(error.line, 1);
        assert!(error.message.contains(":BOGUS"), "{error}");
    }

    #[test]
    fn unknown_property_type_is_rejected() {
        let error = one(":ID,when:date\np1,2020\n").unwrap_err();
        assert_eq!(error.line, 1);
        assert!(error.message.contains("date"), "{error}");
    }

    #[test]
    fn edge_header_without_type_is_rejected() {
        let error = one(":START_ID,:END_ID\np1,p2\n").unwrap_err();
        assert_eq!(error.line, 1);
        assert!(error.message.contains(":TYPE"), "{error}");
    }

    #[test]
    fn malformed_typed_cell_reports_row_and_column() {
        let error = one(":ID,age:int\np1,notanumber\n").unwrap_err();
        assert_eq!(error.line, 2);
        assert!(error.message.contains("age"), "{error}");
    }

    #[test]
    fn valid_time_columns_parse_rfc3339_and_micros() {
        let ops =
            one(":ID,:VALID_FROM,:VALID_TO\np1,2020-01-01T00:00:00Z,1609459200000000\n").unwrap();
        let BulkOp::Node(node) = &ops[0] else {
            panic!("node");
        };
        assert_eq!(
            node.valid_from,
            Some(Instant::from_micros(1_577_836_800_000_000))
        );
        assert_eq!(
            node.valid_to,
            Some(Instant::from_micros(1_609_459_200_000_000))
        );
    }

    #[test]
    fn malformed_valid_time_cell_is_rejected() {
        let error = one(":ID,:VALID_FROM\np1,nope\n").unwrap_err();
        assert_eq!(error.line, 2);
        assert!(error.message.contains(":VALID_FROM"), "{error}");
    }

    #[test]
    fn record_exceeding_cap_is_rejected() {
        // A record longer than the cap with no row terminator must be rejected.
        let mut framer = CsvFramer::new(16);
        let error = framer
            .push(b":ID,name\np1,aaaaaaaaaaaaaaaaaaaaaaaaaaaa")
            .expect_err("oversized record must be rejected");
        assert!(
            error.message.to_lowercase().contains("max_line_bytes"),
            "{error}"
        );
    }
}
