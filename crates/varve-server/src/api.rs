use std::collections::BTreeMap;

use arrow::record_batch::RecordBatch;
use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as JsonValue};
use varve_engine::{
    BasisToken, CompactionReport, GcReport, NodeRole, NodeStatus, ProbeReport, ProbeVerdict,
    TxReceipt, VerifyReport,
};
use varve_types::{LogPosition, Value};

use crate::ServerError;

pub const ARROW_STREAM_CONTENT_TYPE: &str = "application/vnd.apache.arrow.stream";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct QueryRequest {
    pub gql: String,
    #[serde(default)]
    pub params: BTreeMap<String, JsonValue>,
    pub basis: Option<BasisRequest>,
    pub basis_timeout_ms: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum BasisRequest {
    TxId(u64),
    At(String),
}

impl TryFrom<BasisRequest> for BasisToken {
    type Error = ServerError;

    fn try_from(value: BasisRequest) -> Result<Self, Self::Error> {
        match value {
            BasisRequest::TxId(tx_id) => Ok(Self::TxId(tx_id)),
            BasisRequest::At(value) => {
                let packed = value
                    .strip_prefix("at:")
                    .ok_or_else(|| {
                        ServerError::InvalidRequest("basis string must be at:<packed-u64>".into())
                    })?
                    .parse::<u64>()
                    .map_err(|error| {
                        ServerError::InvalidRequest(format!("invalid basis position: {error}"))
                    })?;
                Ok(Self::At(LogPosition::from_u64(packed)))
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct TxRequest {
    pub gql: String,
    #[serde(default)]
    pub params: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct TxResponse {
    pub tx_id: u64,
    pub system_time: String,
    pub system_time_us: i64,
    pub side_effects: SideEffectsResponse,
    pub basis: u64,
}

impl TxResponse {
    /// Builds the wire response from an engine [`TxReceipt`].
    ///
    /// `system_time` is the receipt instant rendered through its `Display`,
    /// which is RFC 3339 with microsecond precision for every real receipt.
    /// Instants outside chrono's representable range — sentinels or synthetic
    /// values the writer never emits — fall back to a raw `<µs>us` string
    /// (e.g. `9223372036854775807us`), keeping this conversion total so a
    /// public caller can never trip a panic on an extreme instant.
    pub fn from_receipt(receipt: &TxReceipt) -> Self {
        Self {
            tx_id: receipt.tx_id,
            system_time: receipt.system_time.to_string(),
            system_time_us: receipt.system_time.as_micros(),
            side_effects: SideEffectsResponse {
                nodes_created: receipt.side_effects.nodes_created,
                nodes_deleted: receipt.side_effects.nodes_deleted,
                relationships_created: receipt.side_effects.relationships_created,
                relationships_deleted: receipt.side_effects.relationships_deleted,
                properties_set: receipt.side_effects.properties_set,
                properties_removed: receipt.side_effects.properties_removed,
                labels_added: receipt.side_effects.labels_added,
                labels_removed: receipt.side_effects.labels_removed,
            },
            basis: receipt.tx_id,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SideEffectsResponse {
    pub nodes_created: usize,
    pub nodes_deleted: usize,
    pub relationships_created: usize,
    pub relationships_deleted: usize,
    pub properties_set: usize,
    pub properties_removed: usize,
    pub labels_added: usize,
    pub labels_removed: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct QueryJsonResponse {
    pub rows: Vec<Map<String, JsonValue>>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct StatusResponse {
    pub roles: Vec<String>,
    pub applied_tx_id: u64,
    pub applied_log_position: u64,
    pub manifest_block_id: Option<u64>,
    pub manifest_watermark: u64,
    /// Latest known log head (Task 12, spec §12): see `NodeStatus::log_head`.
    pub log_head_position: u64,
    pub follower_error: Option<String>,
    pub probe: ProbeResponse,
}

impl StatusResponse {
    pub fn from_engine(status: &NodeStatus, report: &ProbeReport) -> Self {
        Self {
            roles: status
                .roles
                .iter()
                .map(role_name)
                .map(str::to_owned)
                .collect(),
            applied_tx_id: status.applied.tx_id,
            applied_log_position: status.applied.log_position.as_u64(),
            manifest_block_id: status.manifest_block_id,
            manifest_watermark: status.manifest_watermark.as_u64(),
            log_head_position: status.log_head.as_u64(),
            follower_error: status.follower_error.clone(),
            probe: ProbeResponse::from_report(report),
        }
    }
}

fn role_name(role: NodeRole) -> &'static str {
    match role {
        NodeRole::Writer => "writer",
        NodeRole::Query => "query",
        NodeRole::Compactor => "compactor",
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ProbeResponse {
    pub verdict: String,
    pub reason: Option<String>,
    pub probe_key: String,
}

impl ProbeResponse {
    fn from_report(report: &ProbeReport) -> Self {
        let (verdict, reason) = match &report.verdict {
            ProbeVerdict::Supported => ("supported", None),
            ProbeVerdict::Unsupported { reason } => ("unsupported", Some(reason.clone())),
            ProbeVerdict::Inconsistent { reason } => ("inconsistent", Some(reason.clone())),
        };
        Self {
            verdict: verdict.into(),
            reason,
            probe_key: report.probe_key.clone(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct CompactionResponse {
    pub jobs: usize,
    pub input_tries: usize,
    pub output_tries: usize,
    pub input_rows: u64,
    pub output_rows: u64,
}

impl CompactionResponse {
    pub fn from_report(report: &CompactionReport) -> Self {
        Self {
            jobs: report.jobs,
            input_tries: report.input_tries,
            output_tries: report.output_tries,
            input_rows: report.input_rows,
            output_rows: report.output_rows,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct GcResponse {
    pub planned_objects: usize,
    pub deleted_objects: usize,
}

impl GcResponse {
    pub fn from_report(report: &GcReport) -> Self {
        Self {
            planned_objects: report.planned_objects,
            deleted_objects: report.deleted_objects,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct VerifyResponse {
    pub manifest_block_id: Option<u64>,
    pub tries_checked: usize,
    pub pages_checked: usize,
    pub events_checked: usize,
    pub log_records_checked: usize,
}

impl VerifyResponse {
    pub fn from_report(report: &VerifyReport) -> Self {
        Self {
            manifest_block_id: report.manifest_block_id,
            tries_checked: report.tries_checked,
            pages_checked: report.pages_checked,
            events_checked: report.events_checked,
            log_records_checked: report.log_records_checked,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ErrorResponse {
    pub code: String,
    pub message: String,
    pub writer: Option<String>,
}

pub fn params_from_json(
    params: &BTreeMap<String, JsonValue>,
) -> Result<BTreeMap<String, Value>, ServerError> {
    params
        .iter()
        .map(|(key, value)| Ok((key.clone(), scalar_from_json(value)?)))
        .collect()
}

fn scalar_from_json(value: &JsonValue) -> Result<Value, ServerError> {
    match value {
        JsonValue::Null => Ok(Value::Null),
        JsonValue::Bool(value) => Ok(Value::Bool(*value)),
        JsonValue::Number(value) => {
            if let Some(value) = value.as_i64() {
                return Ok(Value::Int(value));
            }
            if value.as_u64().is_some() {
                return Err(ServerError::InvalidRequest(
                    "unsigned integer exceeds i64::MAX".into(),
                ));
            }
            let value = value.as_f64().ok_or_else(|| {
                ServerError::InvalidRequest("JSON number cannot be represented as f64".into())
            })?;
            if !value.is_finite() {
                return Err(ServerError::InvalidRequest(
                    "floating-point parameter must be finite".into(),
                ));
            }
            Ok(Value::Float(value))
        }
        JsonValue::String(value) => Ok(Value::Str(value.clone())),
        JsonValue::Object(object) if object.len() == 1 && object.contains_key("$bytes") => {
            let encoded = object["$bytes"].as_str().ok_or_else(|| {
                ServerError::InvalidRequest("$bytes value must be a string".into())
            })?;
            Ok(Value::Bytes(
                base64::engine::general_purpose::STANDARD.decode(encoded)?,
            ))
        }
        JsonValue::Array(_) | JsonValue::Object(_) => Err(ServerError::InvalidRequest(
            "parameter values must be JSON scalars or exact $bytes objects".into(),
        )),
    }
}

pub fn batches_to_json(batches: &[RecordBatch]) -> Result<QueryJsonResponse, ServerError> {
    Ok(QueryJsonResponse {
        rows: varve::rows(batches)?.collect(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use varve_engine::SideEffects;
    use varve_types::Instant;

    #[test]
    fn from_receipt_renders_out_of_range_instants_as_raw_micros() {
        // `Instant::END_OF_TIME` (i64::MAX µs) is outside chrono's range, so
        // its Display falls back to `<µs>us`. Real receipts never carry such
        // an instant; this pins that `from_receipt` stays total either way.
        let receipt = TxReceipt {
            tx_id: 7,
            system_time: Instant::END_OF_TIME,
            side_effects: SideEffects::default(),
        };
        let response = TxResponse::from_receipt(&receipt);
        assert_eq!(response.system_time, "9223372036854775807us");
        assert_eq!(response.system_time_us, i64::MAX);
    }
}
