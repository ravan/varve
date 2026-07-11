use std::collections::BTreeMap;
use std::io::Cursor;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use arrow::array::{BinaryArray, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use serde_json::json;
use varve_cli::{export_jsonl, import_jsonl, CliError, CommandClient};
use varve_server::api::{
    CompactionResponse, GcResponse, QueryRequest, SideEffectsResponse, StatusResponse, TxRequest,
    TxResponse, VerifyResponse,
};

/// A scripted [`CommandClient`] that records every `execute` (tx) request it
/// sees and returns a fixed batch list from `query`, so import/export tests
/// can assert on exactly what reached the client without a real engine.
struct FakeClient {
    tx_requests: Mutex<Vec<TxRequest>>,
    next_tx_id: AtomicU64,
    query_batches: Vec<RecordBatch>,
}

impl FakeClient {
    fn new() -> Self {
        Self {
            tx_requests: Mutex::new(Vec::new()),
            next_tx_id: AtomicU64::new(1),
            query_batches: Vec::new(),
        }
    }

    fn with_query_batches(batches: Vec<RecordBatch>) -> Self {
        Self {
            tx_requests: Mutex::new(Vec::new()),
            next_tx_id: AtomicU64::new(1),
            query_batches: batches,
        }
    }

    fn tx_requests(&self) -> Vec<TxRequest> {
        self.tx_requests
            .lock()
            .unwrap_or_else(|error| panic!("tx_requests lock poisoned: {error}"))
            .clone()
    }
}

#[async_trait]
impl CommandClient for FakeClient {
    async fn query(&self, _request: QueryRequest) -> Result<Vec<RecordBatch>, CliError> {
        Ok(self.query_batches.clone())
    }

    async fn execute(&self, request: TxRequest) -> Result<TxResponse, CliError> {
        let tx_id = self.next_tx_id.fetch_add(1, Ordering::SeqCst);
        self.tx_requests
            .lock()
            .unwrap_or_else(|error| panic!("tx_requests lock poisoned: {error}"))
            .push(request);
        Ok(TxResponse {
            tx_id,
            system_time: "2024-01-01T00:00:00.000000Z".to_string(),
            system_time_us: 0,
            side_effects: SideEffectsResponse {
                nodes_created: 1,
                nodes_deleted: 0,
                relationships_created: 0,
                relationships_deleted: 0,
                properties_set: 0,
                properties_removed: 0,
                labels_added: 0,
                labels_removed: 0,
            },
            basis: tx_id,
        })
    }

    async fn status(&self) -> Result<StatusResponse, CliError> {
        unreachable!("not exercised by transfer tests")
    }

    async fn compact(&self) -> Result<CompactionResponse, CliError> {
        unreachable!("not exercised by transfer tests")
    }

    async fn gc(&self) -> Result<GcResponse, CliError> {
        unreachable!("not exercised by transfer tests")
    }

    async fn verify(&self) -> Result<VerifyResponse, CliError> {
        unreachable!("not exercised by transfer tests")
    }
}

fn expect_err<T: std::fmt::Debug>(result: Result<T, CliError>) -> CliError {
    match result {
        Err(error) => error,
        Ok(value) => panic!("expected an error, got Ok({value:?})"),
    }
}

#[tokio::test]
async fn jsonl_import_uses_one_parameterized_tx_per_line() {
    let input = br#"{"_id":1,"name":"Ada"}
{"_id":2,"name":"Bob"}
"#;
    let client = Arc::new(FakeClient::new());
    let report = import_jsonl(client.clone(), Cursor::new(input), "Person", None)
        .await
        .unwrap_or_else(|error| panic!("import must succeed: {error}"));
    assert_eq!(report.committed, 2);
    let requests = client.tx_requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].gql, "INSERT (:Person {_id: $p0, name: $p1})");
    assert_eq!(requests[0].params["p0"], json!(1));
    assert_eq!(requests[0].params["p1"], json!("Ada"));
    assert_eq!(requests[1].gql, "INSERT (:Person {_id: $p0, name: $p1})");
    assert_eq!(requests[1].params["p0"], json!(2));
    assert_eq!(requests[1].params["p1"], json!("Bob"));
    assert_eq!(report.last_basis, Some(2));
}

#[tokio::test]
async fn jsonl_import_with_graph_prefixes_use_clause() {
    let input = b"{\"_id\":1}\n";
    let client = Arc::new(FakeClient::new());
    let report = import_jsonl(client.clone(), Cursor::new(input), "Person", Some("g1"))
        .await
        .unwrap_or_else(|error| panic!("import must succeed: {error}"));
    assert_eq!(report.committed, 1);
    let requests = client.tx_requests();
    assert_eq!(requests[0].gql, "USE g1; INSERT (:Person {_id: $p0})");
}

#[tokio::test]
async fn invalid_json_line_reports_line_number_and_stops_before_client_call() {
    let input = b"{\"_id\":1,\"name\":\"Ada\"}\nnot json\n";
    let client = Arc::new(FakeClient::new());
    let error = expect_err(import_jsonl(client.clone(), Cursor::new(input), "Person", None).await);
    assert!(error.to_string().contains("line 2"), "error was: {error}");
    assert_eq!(
        client.tx_requests().len(),
        1,
        "only the first, valid line should have reached the client"
    );
}

#[tokio::test]
async fn non_object_line_is_rejected_without_a_client_call() {
    let input = b"[1,2,3]\n";
    let client = Arc::new(FakeClient::new());
    let error = expect_err(import_jsonl(client.clone(), Cursor::new(input), "Person", None).await);
    assert!(error.to_string().contains("line 1"), "error was: {error}");
    assert!(client.tx_requests().is_empty());
}

#[tokio::test]
async fn empty_object_line_is_rejected_without_a_client_call() {
    let input = b"{}\n";
    let client = Arc::new(FakeClient::new());
    let error = expect_err(import_jsonl(client.clone(), Cursor::new(input), "Person", None).await);
    assert!(error.to_string().contains("line 1"), "error was: {error}");
    assert!(client.tx_requests().is_empty());
}

#[tokio::test]
async fn invalid_property_identifier_is_rejected_without_a_client_call() {
    let input = b"{\"_id\":1,\"name\":\"Ada\"}\n{\"1bad\":1}\n";
    let client = Arc::new(FakeClient::new());
    let error = expect_err(import_jsonl(client.clone(), Cursor::new(input), "Person", None).await);
    assert!(error.to_string().contains("line 2"), "error was: {error}");
    assert_eq!(client.tx_requests().len(), 1);
}

#[tokio::test]
async fn keyword_shaped_property_key_is_accepted_because_the_engine_grammar_permits_it() {
    // "match" is a GQL keyword, but varve-gql's `property_name()` explicitly
    // accepts keywords as property names (only node/relationship *labels*
    // require a non-keyword identifier); the generated statement is simply
    // validated by `parse_program` like any other, so there is no need (and
    // no second reserved-word list) to reject it client-side.
    let input = b"{\"match\":1}\n";
    let client = Arc::new(FakeClient::new());
    let report = import_jsonl(client.clone(), Cursor::new(input), "Person", None)
        .await
        .unwrap_or_else(|error| panic!("keyword-shaped property keys must be accepted: {error}"));
    assert_eq!(report.committed, 1);
    assert_eq!(client.tx_requests()[0].gql, "INSERT (:Person {match: $p0})");
}

#[tokio::test]
async fn nested_object_value_is_rejected_without_a_client_call() {
    let input = b"{\"_id\":1,\"name\":\"Ada\"}\n{\"meta\":{\"nested\":true}}\n";
    let client = Arc::new(FakeClient::new());
    let error = expect_err(import_jsonl(client.clone(), Cursor::new(input), "Person", None).await);
    assert!(error.to_string().contains("line 2"), "error was: {error}");
    assert_eq!(client.tx_requests().len(), 1);
}

#[tokio::test]
async fn tagged_bytes_value_is_accepted_as_a_scalar_param() {
    let input = b"{\"blob\":{\"$bytes\":\"AAE=\"}}\n";
    let client = Arc::new(FakeClient::new());
    let report = import_jsonl(client.clone(), Cursor::new(input), "Person", None)
        .await
        .unwrap_or_else(|error| panic!("tagged bytes must be accepted: {error}"));
    assert_eq!(report.committed, 1);
    assert_eq!(
        client.tx_requests()[0].params["p0"],
        json!({"$bytes": "AAE="})
    );
}

fn sample_export_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("age", DataType::Int64, true),
        Field::new("blob", DataType::Binary, true),
    ]));
    let names = Arc::new(StringArray::from(vec![Some("Ada"), None]));
    let ages = Arc::new(Int64Array::from(vec![None, Some(30)]));
    let blobs = Arc::new(BinaryArray::from(vec![Some(b"\x00\x01".as_slice()), None]));
    RecordBatch::try_new(schema, vec![names, ages, blobs])
        .unwrap_or_else(|error| panic!("record batch must build: {error}"))
}

#[tokio::test]
async fn export_writes_line_delimited_json_with_explicit_nulls_and_tagged_bytes() {
    let client = Arc::new(FakeClient::with_query_batches(vec![sample_export_batch()]));
    let request = QueryRequest {
        gql: "MATCH (p:Person) RETURN p.name AS name, p.age AS age, p.blob AS blob".to_string(),
        params: BTreeMap::new(),
        basis: None,
        basis_timeout_ms: None,
    };
    let mut output = Vec::new();
    let rows = export_jsonl(client, request, &mut output)
        .await
        .unwrap_or_else(|error| panic!("export must succeed: {error}"));
    assert_eq!(rows, 2);

    let text = String::from_utf8(output)
        .unwrap_or_else(|error| panic!("export output must be valid utf8: {error}"));
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines.len(), 2, "output was: {text}");

    let row0: serde_json::Value = serde_json::from_str(lines[0])
        .unwrap_or_else(|error| panic!("row 0 must be valid json: {error}"));
    let expected_blob = BASE64.encode([0x00u8, 0x01]);
    assert_eq!(
        row0,
        json!({"name": "Ada", "age": null, "blob": {"$bytes": expected_blob}})
    );

    let row1: serde_json::Value = serde_json::from_str(lines[1])
        .unwrap_or_else(|error| panic!("row 1 must be valid json: {error}"));
    assert_eq!(row1, json!({"name": null, "age": 30, "blob": null}));
}
