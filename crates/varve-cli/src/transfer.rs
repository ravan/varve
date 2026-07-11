//! JSONL import/export: one parameterized transaction per input line for
//! `import`, and Arrow-to-line-delimited-JSON for `export`. Both funnel
//! through the same [`CommandClient`] surface the shell uses -- import never
//! writes Arrow blocks, log records, or object-store keys directly, and
//! export never serializes `RecordBatch` debug output.

use std::collections::BTreeMap;
use std::io::{BufRead, BufWriter, Write};
use std::sync::Arc;

use arrow::array::{Array, ArrayAccessor, AsArray};
use arrow::datatypes::{DataType, FieldRef};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use arrow_json::writer::{LineDelimited, NullableEncoder, WriterBuilder};
use arrow_json::{Encoder, EncoderFactory, EncoderOptions};
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use serde_json::Value as JsonValue;
use varve::BasisToken;
use varve_server::api::{params_from_json, BasisRequest, QueryRequest, TxRequest};

use crate::client::{CliError, CommandClient};

/// Outcome of a JSONL import run: how many lines committed as transactions
/// before either finishing cleanly or hitting the first failure, and the
/// basis of the last successful transaction (for chaining into a
/// subsequent `--basis` read).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ImportReport {
    pub committed: usize,
    pub last_basis: Option<u64>,
}

/// Imports newline-delimited JSON objects as one parameterized `INSERT`
/// transaction per line. Reads with [`BufRead::read_line`] so errors carry
/// 1-based line numbers and the whole file never has to fit in memory.
///
/// Each object's keys are sorted (`BTreeMap`) to generate deterministic
/// `$pN` parameter names; the resulting statement is validated with
/// [`varve_gql::parse_program`] before `execute` is ever called, so a
/// rejected reserved identifier surfaces as a parse error wrapped with the
/// line number rather than a second, CLI-side reserved-word list.
///
/// Stops at the first failed line: the error message reports how many
/// transactions committed and which line failed. This is NOT whole-file
/// atomicity -- earlier lines remain committed.
pub async fn import_jsonl<R: BufRead>(
    client: Arc<dyn CommandClient>,
    mut input: R,
    label: &str,
    graph: Option<&str>,
) -> Result<ImportReport, CliError> {
    validate_identifier(label)
        .map_err(|reason| CliError::InvalidInput(format!("invalid --label: {reason}")))?;
    if let Some(graph) = graph {
        validate_identifier(graph)
            .map_err(|reason| CliError::InvalidInput(format!("invalid --graph: {reason}")))?;
    }

    let mut committed = 0usize;
    let mut last_basis = None;
    let mut line_no = 0usize;
    let mut line = String::new();

    loop {
        line.clear();
        let bytes_read = input.read_line(&mut line)?;
        if bytes_read == 0 {
            break;
        }
        line_no += 1;
        let trimmed = line.trim_end_matches(['\n', '\r']);

        let request = build_tx_request(trimmed, label, graph)
            .map_err(|error| line_error(line_no, committed, error))?;
        let response = client
            .execute(request)
            .await
            .map_err(|error| line_error(line_no, committed, error))?;
        committed += 1;
        last_basis = Some(response.basis);
    }

    Ok(ImportReport {
        committed,
        last_basis,
    })
}

/// Wraps an error that occurred processing a given input line with the
/// 1-based line number and how many transactions had already committed.
fn line_error(line_no: usize, committed: usize, error: impl std::fmt::Display) -> CliError {
    CliError::InvalidInput(format!(
        "line {line_no}: {error} (committed {committed} row(s) so far)"
    ))
}

/// Builds one `TxRequest` from a single JSONL line: parses it as a JSON
/// object, validates every key as a GQL identifier and every value as a
/// scalar (or exact `$bytes`-tagged) parameter, then renders and parses the
/// generated `INSERT` statement.
fn build_tx_request(line: &str, label: &str, graph: Option<&str>) -> Result<TxRequest, CliError> {
    let value: JsonValue = serde_json::from_str(line)?;
    let object = match value {
        JsonValue::Object(object) => object,
        _ => return Err(CliError::InvalidInput("expected a JSON object".to_string())),
    };
    if object.is_empty() {
        return Err(CliError::InvalidInput(
            "object must have at least one property".to_string(),
        ));
    }

    // Deterministic key order, independent of serde_json's own map
    // implementation/feature flags.
    let doc: BTreeMap<String, JsonValue> = object.into_iter().collect();

    for key in doc.keys() {
        validate_identifier(key)
            .map_err(|reason| CliError::InvalidInput(format!("invalid property key: {reason}")))?;
    }

    // Reuses the server's own scalar/`$bytes` validation instead of
    // maintaining a second copy of it client-side.
    params_from_json(&doc)?;

    let mut params = BTreeMap::new();
    let mut properties = Vec::with_capacity(doc.len());
    for (index, (key, value)) in doc.into_iter().enumerate() {
        let param_name = format!("p{index}");
        properties.push(format!("{key}: ${param_name}"));
        params.insert(param_name, value);
    }

    let mut gql = String::new();
    if let Some(graph) = graph {
        gql.push_str("USE ");
        gql.push_str(graph);
        gql.push_str("; ");
    }
    gql.push_str("INSERT (:");
    gql.push_str(label);
    gql.push_str(" {");
    gql.push_str(&properties.join(", "));
    gql.push_str("})");

    varve_gql::parse_program(&gql).map_err(|error| CliError::InvalidInput(error.to_string()))?;

    Ok(TxRequest { gql, params })
}

/// Validates a candidate GQL identifier's ASCII shape only (first byte
/// alphabetic or `_`, remaining bytes alphanumeric or `_`). Reserved
/// keywords that are otherwise shaped like identifiers (e.g. `match`) are
/// deliberately NOT rejected here -- `varve_gql::parse_program` is the sole
/// authority on reserved words.
fn validate_identifier(candidate: &str) -> Result<(), String> {
    let mut bytes = candidate.bytes();
    let shape_ok = match bytes.next() {
        Some(first) if first.is_ascii_alphabetic() || first == b'_' => {
            bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        }
        _ => false,
    };
    if shape_ok {
        Ok(())
    } else {
        Err(format!("{candidate:?} is not a valid identifier"))
    }
}

/// Parses a `--basis` CLI argument: a bare integer is a transaction id, and
/// `at:<packed-u64>` is a packed log position. Reuses
/// `BasisToken::try_from(BasisRequest)` to validate the `at:` form instead
/// of re-implementing its prefix/parse logic.
pub fn parse_basis(raw: &str) -> Result<BasisRequest, CliError> {
    if let Ok(tx_id) = raw.parse::<u64>() {
        return Ok(BasisRequest::TxId(tx_id));
    }
    let request = BasisRequest::At(raw.to_string());
    BasisToken::try_from(request.clone())?;
    Ok(request)
}

/// Runs `request` through [`CommandClient::query`] and writes the resulting
/// batches as line-delimited JSON with explicit nulls: one JSON object per
/// row, `null` fields kept rather than elided, and `Binary` columns encoded
/// as `{"$bytes": "<base64>"}` -- the same tagged-bytes convention
/// `import_jsonl`/the server's param decoder already use, so exported files
/// round-trip losslessly back through `import_jsonl`. Returns the number of
/// rows written.
pub async fn export_jsonl<W: Write>(
    client: Arc<dyn CommandClient>,
    request: QueryRequest,
    output: W,
) -> Result<usize, CliError> {
    let batches = client.query(request).await?;
    let row_count = batches.iter().map(RecordBatch::num_rows).sum();

    let mut writer = WriterBuilder::new()
        .with_explicit_nulls(true)
        .with_encoder_factory(Arc::new(TaggedBytesEncoderFactory))
        .build::<_, LineDelimited>(BufWriter::new(output));
    writer.write_batches(&batches.iter().collect::<Vec<_>>())?;
    writer.finish()?;

    Ok(row_count)
}

/// Encodes a `Binary` array's values as `{"$bytes": "<base64-standard>"}`
/// objects rather than arrow-json's default hex-string encoding, matching
/// the wire tag `varve_server::api::params_from_json` already decodes.
struct TaggedBytesEncoder<B> {
    array: B,
}

impl<'a, B> Encoder for TaggedBytesEncoder<B>
where
    B: ArrayAccessor<Item = &'a [u8]>,
{
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        out.extend_from_slice(b"{\"$bytes\":\"");
        out.extend_from_slice(BASE64.encode(self.array.value(idx)).as_bytes());
        out.extend_from_slice(b"\"}");
    }
}

#[derive(Debug)]
struct TaggedBytesEncoderFactory;

impl EncoderFactory for TaggedBytesEncoderFactory {
    fn make_default_encoder<'a>(
        &self,
        _field: &'a FieldRef,
        array: &'a dyn Array,
        _options: &'a EncoderOptions,
    ) -> Result<Option<NullableEncoder<'a>>, ArrowError> {
        match array.data_type() {
            DataType::Binary => {
                let array = array.as_binary::<i32>();
                let encoder = TaggedBytesEncoder { array };
                let boxed = Box::new(encoder) as Box<dyn Encoder + 'a>;
                let nulls = array.nulls().cloned();
                Ok(Some(NullableEncoder::new(boxed, nulls)))
            }
            _ => Ok(None),
        }
    }
}
