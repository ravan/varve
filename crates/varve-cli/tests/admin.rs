use std::sync::atomic::{AtomicU64, Ordering};

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use serde_json::json;
use varve_cli::{
    run_admin_compact, run_admin_gc, run_admin_status, run_admin_verify, CliError, CommandClient,
};
use varve_server::api::{
    CompactionResponse, GcResponse, ProbeResponse, QueryRequest, StatusResponse, TxRequest,
    TxResponse, VerifyResponse,
};

/// A scripted [`CommandClient`] that counts calls per admin method and
/// returns fixed, fully-populated responses so tests can assert both "called
/// exactly once" and "every report field is present in the rendering".
struct FakeClient {
    status_calls: AtomicU64,
    compact_calls: AtomicU64,
    gc_calls: AtomicU64,
    verify_calls: AtomicU64,
}

impl FakeClient {
    fn new() -> Self {
        Self {
            status_calls: AtomicU64::new(0),
            compact_calls: AtomicU64::new(0),
            gc_calls: AtomicU64::new(0),
            verify_calls: AtomicU64::new(0),
        }
    }
}

#[async_trait]
impl CommandClient for FakeClient {
    async fn query(&self, _request: QueryRequest) -> Result<Vec<RecordBatch>, CliError> {
        unreachable!("not exercised by admin tests")
    }

    async fn execute(&self, _request: TxRequest) -> Result<TxResponse, CliError> {
        unreachable!("not exercised by admin tests")
    }

    async fn status(&self) -> Result<StatusResponse, CliError> {
        self.status_calls.fetch_add(1, Ordering::SeqCst);
        Ok(StatusResponse {
            roles: vec!["writer".to_string(), "query".to_string()],
            applied_tx_id: 7,
            applied_log_position: 42,
            manifest_block_id: Some(3),
            manifest_watermark: 9,
            log_head_position: 42,
            follower_error: Some("lagging".to_string()),
            probe: ProbeResponse {
                verdict: "supported".to_string(),
                reason: None,
                probe_key: "k1".to_string(),
            },
        })
    }

    async fn compact(&self) -> Result<CompactionResponse, CliError> {
        self.compact_calls.fetch_add(1, Ordering::SeqCst);
        Ok(CompactionResponse {
            jobs: 1,
            input_tries: 2,
            output_tries: 1,
            input_rows: 100,
            output_rows: 80,
        })
    }

    async fn gc(&self) -> Result<GcResponse, CliError> {
        self.gc_calls.fetch_add(1, Ordering::SeqCst);
        Ok(GcResponse {
            planned_objects: 5,
            deleted_objects: 3,
        })
    }

    async fn verify(&self) -> Result<VerifyResponse, CliError> {
        self.verify_calls.fetch_add(1, Ordering::SeqCst);
        Ok(VerifyResponse {
            manifest_block_id: Some(3),
            tries_checked: 4,
            pages_checked: 5,
            events_checked: 6,
            log_records_checked: 7,
        })
    }
}

fn text_of(output: Vec<u8>) -> String {
    String::from_utf8(output).unwrap_or_else(|error| panic!("output must be valid utf8: {error}"))
}

fn json_of(output: Vec<u8>) -> serde_json::Value {
    serde_json::from_str(text_of(output).trim())
        .unwrap_or_else(|error| panic!("output must be valid json: {error}"))
}

#[tokio::test]
async fn admin_status_calls_client_once_and_human_output_has_every_field() {
    let client = FakeClient::new();
    let mut output = Vec::new();
    run_admin_status(&client, false, &mut output)
        .await
        .unwrap_or_else(|error| panic!("admin status must succeed: {error}"));
    assert_eq!(client.status_calls.load(Ordering::SeqCst), 1);
    let text = text_of(output);
    for field in [
        "roles",
        "applied_tx_id: 7",
        "applied_log_position: 42",
        "manifest_block_id: 3",
        "manifest_watermark: 9",
        "log_head_position: 42",
        "follower_error: lagging",
        "probe: supported",
    ] {
        assert!(text.contains(field), "missing {field:?} in {text:?}");
    }
}

#[tokio::test]
async fn admin_status_json_emits_exact_server_dto() {
    let client = FakeClient::new();
    let mut output = Vec::new();
    run_admin_status(&client, true, &mut output)
        .await
        .unwrap_or_else(|error| panic!("admin status must succeed: {error}"));
    assert_eq!(client.status_calls.load(Ordering::SeqCst), 1);
    let value = json_of(output);
    assert_eq!(value["applied_tx_id"], json!(7));
    assert_eq!(value["applied_log_position"], json!(42));
    assert_eq!(value["manifest_block_id"], json!(3));
    assert_eq!(value["manifest_watermark"], json!(9));
    assert_eq!(value["log_head_position"], json!(42));
    assert_eq!(value["follower_error"], json!("lagging"));
    assert_eq!(value["probe"]["verdict"], json!("supported"));
}

#[tokio::test]
async fn admin_compact_calls_client_once_and_human_output_has_every_field() {
    let client = FakeClient::new();
    let mut output = Vec::new();
    run_admin_compact(&client, false, &mut output)
        .await
        .unwrap_or_else(|error| panic!("admin compact must succeed: {error}"));
    assert_eq!(client.compact_calls.load(Ordering::SeqCst), 1);
    let text = text_of(output);
    for field in [
        "jobs: 1",
        "input_tries: 2",
        "output_tries: 1",
        "input_rows: 100",
        "output_rows: 80",
    ] {
        assert!(text.contains(field), "missing {field:?} in {text:?}");
    }
}

#[tokio::test]
async fn admin_compact_json_emits_exact_server_dto() {
    let client = FakeClient::new();
    let mut output = Vec::new();
    run_admin_compact(&client, true, &mut output)
        .await
        .unwrap_or_else(|error| panic!("admin compact must succeed: {error}"));
    assert_eq!(client.compact_calls.load(Ordering::SeqCst), 1);
    let value = json_of(output);
    assert_eq!(value["jobs"], json!(1));
    assert_eq!(value["input_tries"], json!(2));
    assert_eq!(value["output_tries"], json!(1));
    assert_eq!(value["input_rows"], json!(100));
    assert_eq!(value["output_rows"], json!(80));
}

#[tokio::test]
async fn admin_gc_calls_client_once_and_human_output_has_every_field() {
    let client = FakeClient::new();
    let mut output = Vec::new();
    run_admin_gc(&client, false, &mut output)
        .await
        .unwrap_or_else(|error| panic!("admin gc must succeed: {error}"));
    assert_eq!(client.gc_calls.load(Ordering::SeqCst), 1);
    let text = text_of(output);
    for field in ["planned_objects: 5", "deleted_objects: 3"] {
        assert!(text.contains(field), "missing {field:?} in {text:?}");
    }
}

#[tokio::test]
async fn admin_gc_json_emits_exact_server_dto() {
    let client = FakeClient::new();
    let mut output = Vec::new();
    run_admin_gc(&client, true, &mut output)
        .await
        .unwrap_or_else(|error| panic!("admin gc must succeed: {error}"));
    assert_eq!(client.gc_calls.load(Ordering::SeqCst), 1);
    let value = json_of(output);
    assert_eq!(value["planned_objects"], json!(5));
    assert_eq!(value["deleted_objects"], json!(3));
}

#[tokio::test]
async fn admin_verify_calls_client_once_and_human_output_has_every_field() {
    let client = FakeClient::new();
    let mut output = Vec::new();
    run_admin_verify(&client, false, &mut output)
        .await
        .unwrap_or_else(|error| panic!("admin verify must succeed: {error}"));
    assert_eq!(client.verify_calls.load(Ordering::SeqCst), 1);
    let text = text_of(output);
    for field in [
        "manifest_block_id: 3",
        "tries_checked: 4",
        "pages_checked: 5",
        "events_checked: 6",
        "log_records_checked: 7",
    ] {
        assert!(text.contains(field), "missing {field:?} in {text:?}");
    }
}

#[tokio::test]
async fn admin_verify_json_emits_exact_server_dto() {
    let client = FakeClient::new();
    let mut output = Vec::new();
    run_admin_verify(&client, true, &mut output)
        .await
        .unwrap_or_else(|error| panic!("admin verify must succeed: {error}"));
    assert_eq!(client.verify_calls.load(Ordering::SeqCst), 1);
    let value = json_of(output);
    assert_eq!(value["manifest_block_id"], json!(3));
    assert_eq!(value["tries_checked"], json!(4));
    assert_eq!(value["pages_checked"], json!(5));
    assert_eq!(value["events_checked"], json!(6));
    assert_eq!(value["log_records_checked"], json!(7));
}
