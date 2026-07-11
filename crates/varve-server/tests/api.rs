use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use serde_json::json;
use std::collections::BTreeMap;
use std::sync::Arc;
use varve::{
    BasisToken, CompactionReport, GcReport, ProbeReport, ProbeVerdict, SideEffects, TxReceipt,
    VerifyReport,
};
use varve_server::api::{
    batches_to_json, params_from_json, BasisRequest, CompactionResponse, GcResponse, QueryRequest,
    StatusResponse, TxResponse, VerifyResponse, ARROW_STREAM_CONTENT_TYPE,
};
use varve_types::{Instant, LogPosition, Value};

fn sample_batch_with_null() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int64, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["Ada"])),
            Arc::new(Int64Array::from(vec![None])),
        ],
    )
    .unwrap_or_else(|error| panic!("sample batch must be valid: {error}"))
}

#[test]
fn query_request_accepts_tx_and_position_basis_forms() {
    let tx: QueryRequest = serde_json::from_value(json!({
        "gql": "MATCH (n:N) RETURN n.x",
        "basis": 42
    }))
    .unwrap();
    assert_eq!(
        BasisToken::try_from(tx.basis.unwrap()).unwrap(),
        BasisToken::TxId(42)
    );

    let at: QueryRequest = serde_json::from_value(json!({
        "gql": "MATCH (n:N) RETURN n.x",
        "basis": "at:281474976710663"
    }))
    .unwrap();
    assert_eq!(
        BasisToken::try_from(at.basis.unwrap()).unwrap(),
        BasisToken::At(LogPosition::from_u64(281474976710663))
    );
}

#[test]
fn basis_strings_require_exact_at_prefix_and_packed_u64() {
    for basis in ["42", "at:", "at:-1", "at:18446744073709551616"] {
        assert!(BasisToken::try_from(BasisRequest::At(basis.into())).is_err());
    }
}

#[test]
fn query_request_defaults_optional_fields_and_params() {
    let request: QueryRequest = serde_json::from_value(json!({ "gql": "RETURN 1" })).unwrap();
    assert_eq!(request.params, BTreeMap::new());
    assert_eq!(request.basis, None);
    assert_eq!(request.basis_timeout_ms, None);
}

#[test]
fn params_reject_nested_json_but_decode_tagged_bytes() {
    let ok = BTreeMap::from([("payload".into(), json!({"$bytes": "AAEC"}))]);
    assert_eq!(
        params_from_json(&ok).unwrap()["payload"],
        Value::Bytes(vec![0, 1, 2])
    );
    for value in [json!([1, 2]), json!({"nested": 1}), json!(u64::MAX)] {
        assert!(params_from_json(&BTreeMap::from([("x".into(), value)])).is_err());
    }
}

#[test]
fn params_accept_only_exact_standard_base64_tag() {
    for value in [
        json!({"$bytes": "AAEC", "extra": true}),
        json!({"$bytes": 7}),
        json!({"$bytes": "-_8="}),
    ] {
        assert!(params_from_json(&BTreeMap::from([("x".into(), value)])).is_err());
    }
}

#[test]
fn params_convert_all_supported_json_scalars() {
    let params = BTreeMap::from([
        ("null".into(), json!(null)),
        ("bool".into(), json!(true)),
        ("int".into(), json!(-7)),
        ("float".into(), json!(2.5)),
        ("str".into(), json!("Ada")),
    ]);
    let converted = params_from_json(&params).unwrap();
    assert_eq!(converted["null"], Value::Null);
    assert_eq!(converted["bool"], Value::Bool(true));
    assert_eq!(converted["int"], Value::Int(-7));
    assert_eq!(converted["float"], Value::Float(2.5));
    assert_eq!(converted["str"], Value::Str("Ada".into()));
}

#[test]
fn arrow_batches_become_explicit_null_json_rows() {
    let response = batches_to_json(&[sample_batch_with_null()]).unwrap();
    assert_eq!(
        response.rows,
        vec![json!({"name": "Ada", "age": null})
            .as_object()
            .unwrap()
            .clone()]
    );
}

#[test]
fn empty_batches_become_empty_rows() {
    assert!(batches_to_json(&[]).unwrap().rows.is_empty());
}

#[test]
fn arrow_stream_media_type_is_exact() {
    assert_eq!(
        ARROW_STREAM_CONTENT_TYPE,
        "application/vnd.apache.arrow.stream"
    );
}

#[test]
fn tx_receipt_maps_every_side_effect_and_repeats_tx_id_as_basis() {
    let receipt = TxReceipt {
        tx_id: 42,
        system_time: Instant::from_micros(1_234_567),
        side_effects: SideEffects {
            nodes_created: 1,
            nodes_deleted: 2,
            relationships_created: 3,
            relationships_deleted: 4,
            properties_set: 5,
            properties_removed: 6,
            labels_added: 7,
            labels_removed: 8,
        },
    };

    assert_eq!(
        serde_json::to_value(TxResponse::from_receipt(&receipt)).unwrap(),
        json!({
            "tx_id": 42,
            "system_time": "1970-01-01T00:00:01.234567Z",
            "system_time_us": 1234567,
            "side_effects": {
                "nodes_created": 1,
                "nodes_deleted": 2,
                "relationships_created": 3,
                "relationships_deleted": 4,
                "properties_set": 5,
                "properties_removed": 6,
                "labels_added": 7,
                "labels_removed": 8
            },
            "basis": 42
        })
    );
}

#[test]
fn admin_reports_map_to_exact_snake_case_shapes() {
    let compaction = CompactionReport {
        jobs: 1,
        input_tries: 2,
        output_tries: 3,
        input_rows: 4,
        output_rows: 5,
    };
    assert_eq!(
        serde_json::to_value(CompactionResponse::from_report(&compaction)).unwrap(),
        json!({"jobs": 1, "input_tries": 2, "output_tries": 3, "input_rows": 4, "output_rows": 5})
    );

    let gc = GcReport {
        planned_objects: 6,
        deleted_objects: 7,
    };
    assert_eq!(
        serde_json::to_value(GcResponse::from_report(&gc)).unwrap(),
        json!({"planned_objects": 6, "deleted_objects": 7})
    );

    let verify = VerifyReport {
        manifest_block_id: Some(8),
        tries_checked: 9,
        pages_checked: 10,
        events_checked: 11,
        log_records_checked: 12,
    };
    assert_eq!(
        serde_json::to_value(VerifyResponse::from_report(&verify)).unwrap(),
        json!({
            "manifest_block_id": 8,
            "tries_checked": 9,
            "pages_checked": 10,
            "events_checked": 11,
            "log_records_checked": 12
        })
    );
}

#[tokio::test]
async fn engine_status_and_probe_map_to_wire_values() {
    let db = varve::Db::open(varve_config::Config::from_toml_str("").unwrap())
        .await
        .unwrap();
    let status = db.status().await.unwrap();
    let probe = ProbeReport {
        verdict: ProbeVerdict::Unsupported {
            reason: "conditional put unavailable".into(),
        },
        probe_key: "v1/probe/test".into(),
    };

    let response = StatusResponse::from_engine(&status, &probe);
    assert_eq!(response.roles, vec!["writer", "query", "compactor"]);
    assert_eq!(response.applied_tx_id, status.applied.tx_id);
    assert_eq!(
        response.applied_log_position,
        status.applied.log_position.as_u64()
    );
    assert_eq!(response.manifest_block_id, status.manifest_block_id);
    assert_eq!(
        response.manifest_watermark,
        status.manifest_watermark.as_u64()
    );
    assert_eq!(response.follower_error, status.follower_error);
    assert_eq!(response.probe.verdict, "unsupported");
    assert_eq!(
        response.probe.reason.as_deref(),
        Some("conditional put unavailable")
    );
    assert_eq!(response.probe.probe_key, "v1/probe/test");
}
