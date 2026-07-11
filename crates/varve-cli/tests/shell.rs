use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use varve_cli::{run_shell, CliError, CommandClient, ShellEvent, ShellInput};
use varve_server::api::{
    BasisRequest, CompactionResponse, GcResponse, ProbeResponse, QueryRequest, SideEffectsResponse,
    StatusResponse, TxRequest, TxResponse, VerifyResponse,
};

/// A scripted [`CommandClient`] that never touches a real engine or the
/// network: `execute` hands out sequential tx ids and `query` always
/// returns a single-row `name` batch, so tests can assert on shell
/// orchestration (basis threading, dispatch, rendering) in isolation.
struct FakeClient {
    query_bases: Mutex<Vec<Option<BasisRequest>>>,
    next_tx_id: AtomicU64,
    execute_count: AtomicU64,
}

impl FakeClient {
    fn new() -> Self {
        Self {
            query_bases: Mutex::new(Vec::new()),
            next_tx_id: AtomicU64::new(1),
            execute_count: AtomicU64::new(0),
        }
    }

    fn query_bases(&self) -> Vec<Option<BasisRequest>> {
        self.query_bases
            .lock()
            .unwrap_or_else(|error| panic!("query_bases lock poisoned: {error}"))
            .clone()
    }

    /// Number of `execute` (mutation) calls the client has recorded, so
    /// tests can prove a shape-classified program made NO mutation call.
    fn execute_count(&self) -> u64 {
        self.execute_count.load(Ordering::SeqCst)
    }
}

fn ada_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
    let names = Arc::new(StringArray::from(vec!["Ada"]));
    RecordBatch::try_new(schema, vec![names])
        .unwrap_or_else(|error| panic!("record batch must build: {error}"))
}

#[async_trait]
impl CommandClient for FakeClient {
    async fn query(&self, request: QueryRequest) -> Result<Vec<RecordBatch>, CliError> {
        self.query_bases
            .lock()
            .unwrap_or_else(|error| panic!("query_bases lock poisoned: {error}"))
            .push(request.basis);
        Ok(vec![ada_batch()])
    }

    async fn execute(&self, _request: TxRequest) -> Result<TxResponse, CliError> {
        self.execute_count.fetch_add(1, Ordering::SeqCst);
        let tx_id = self.next_tx_id.fetch_add(1, Ordering::SeqCst);
        Ok(TxResponse {
            tx_id,
            system_time: "2024-01-01T00:00:00.000000Z".to_string(),
            system_time_us: 0,
            side_effects: SideEffectsResponse {
                nodes_created: 1,
                nodes_deleted: 0,
                relationships_created: 0,
                relationships_deleted: 0,
                properties_set: 1,
                properties_removed: 0,
                labels_added: 0,
                labels_removed: 0,
            },
            basis: tx_id,
        })
    }

    async fn status(&self) -> Result<StatusResponse, CliError> {
        Ok(StatusResponse {
            roles: vec!["writer".to_string(), "query".to_string()],
            applied_tx_id: 1,
            applied_log_position: 1,
            manifest_block_id: None,
            manifest_watermark: 0,
            follower_error: None,
            probe: ProbeResponse {
                verdict: "supported".to_string(),
                reason: None,
                probe_key: "test".to_string(),
            },
        })
    }

    async fn compact(&self) -> Result<CompactionResponse, CliError> {
        Ok(CompactionResponse {
            jobs: 0,
            input_tries: 0,
            output_tries: 0,
            input_rows: 0,
            output_rows: 0,
        })
    }

    async fn gc(&self) -> Result<GcResponse, CliError> {
        Ok(GcResponse {
            planned_objects: 0,
            deleted_objects: 0,
        })
    }

    async fn verify(&self) -> Result<VerifyResponse, CliError> {
        Ok(VerifyResponse {
            manifest_block_id: None,
            tries_checked: 0,
            pages_checked: 0,
            events_checked: 0,
            log_records_checked: 0,
        })
    }
}

/// One scripted input event. String literals convert to [`ScriptedInput::Line`]
/// so callers can write plain string arrays for line-only scripts, while
/// `Interrupted`/`Eof` are spelled out explicitly for Ctrl-C/Ctrl-D tests.
enum ScriptedInput {
    Line(String),
    Interrupted,
    Eof,
}

impl From<&str> for ScriptedInput {
    fn from(value: &str) -> Self {
        ScriptedInput::Line(value.to_string())
    }
}

/// A scripted [`ShellInput`]: replays a fixed sequence of events, then
/// yields EOF forever. Records every line handed to `add_history`, and
/// every prompt string it was asked to read with, so tests can assert the
/// shell asks for `varve> ` at statement boundaries and `cont> ` mid-buffer.
struct VecShellInput {
    events: VecDeque<ScriptedInput>,
    history: Vec<String>,
    prompts: Vec<String>,
}

impl VecShellInput {
    fn new<I, S>(events: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<ScriptedInput>,
    {
        Self {
            events: events.into_iter().map(Into::into).collect(),
            history: Vec::new(),
            prompts: Vec::new(),
        }
    }
}

impl ShellInput for VecShellInput {
    fn read(&mut self, prompt: &str) -> Result<ShellEvent, CliError> {
        self.prompts.push(prompt.to_string());
        match self.events.pop_front() {
            Some(ScriptedInput::Line(line)) => Ok(ShellEvent::Line(line)),
            Some(ScriptedInput::Interrupted) => Ok(ShellEvent::Interrupted),
            Some(ScriptedInput::Eof) | None => Ok(ShellEvent::Eof),
        }
    }

    fn add_history(&mut self, line: &str) -> Result<(), CliError> {
        self.history.push(line.to_string());
        Ok(())
    }
}

#[tokio::test]
async fn shell_executes_tx_then_basis_query_and_prints_table() {
    let client = Arc::new(FakeClient::new());
    let mut input = VecShellInput::new([
        "INSERT (:Person {_id: 1, name: 'Ada'});",
        "MATCH (p:Person) RETURN p.name AS name;",
        ":quit",
    ]);
    let mut output = Vec::new();

    run_shell(client.clone(), &mut input, &mut output)
        .await
        .unwrap_or_else(|error| panic!("shell must run cleanly: {error}"));

    let text = String::from_utf8(output)
        .unwrap_or_else(|error| panic!("shell output must be valid utf8: {error}"));
    assert!(text.contains("tx 1 @"), "output was: {text}");
    assert!(text.contains("Ada"), "output was: {text}");
    assert_eq!(client.query_bases(), vec![Some(BasisRequest::TxId(1))]);
}

#[tokio::test]
async fn shell_accumulates_multiline_statement_until_semicolon() {
    let client = Arc::new(FakeClient::new());
    let mut input = VecShellInput::new(["MATCH (p:Person)", "RETURN p.name AS name;", ":quit"]);
    let mut output = Vec::new();

    run_shell(client.clone(), &mut input, &mut output)
        .await
        .unwrap_or_else(|error| panic!("shell must run cleanly: {error}"));

    let text = String::from_utf8(output)
        .unwrap_or_else(|error| panic!("shell output must be valid utf8: {error}"));
    assert!(text.contains("Ada"), "output was: {text}");
    assert_eq!(client.query_bases(), vec![None]);
    // The first line starts a fresh statement (primary prompt); the second
    // continues an unterminated buffer (continuation prompt); the third is
    // a fresh statement again after the previous one flushed.
    assert_eq!(
        input.prompts,
        vec![
            "varve> ".to_string(),
            "cont> ".to_string(),
            "varve> ".to_string()
        ]
    );
}

#[tokio::test]
async fn shell_prints_parse_error_and_resets_buffer() {
    let client = Arc::new(FakeClient::new());
    let mut input = VecShellInput::new([
        "THIS IS NOT VALID GQL;",
        "MATCH (p:Person) RETURN p.name AS name;",
        ":quit",
    ]);
    let mut output = Vec::new();

    run_shell(client.clone(), &mut input, &mut output)
        .await
        .unwrap_or_else(|error| panic!("shell must run cleanly: {error}"));

    let text = String::from_utf8(output)
        .unwrap_or_else(|error| panic!("shell output must be valid utf8: {error}"));
    assert!(text.contains("parse error"), "output was: {text}");
    assert!(text.contains("Ada"), "output was: {text}");
    // Only the valid second statement ever reaches the client.
    assert_eq!(client.query_bases(), vec![None]);
}

#[tokio::test]
async fn shell_status_command_prints_status_without_a_client_query() {
    let client = Arc::new(FakeClient::new());
    let mut input = VecShellInput::new([":status", ":quit"]);
    let mut output = Vec::new();

    run_shell(client.clone(), &mut input, &mut output)
        .await
        .unwrap_or_else(|error| panic!("shell must run cleanly: {error}"));

    let text = String::from_utf8(output)
        .unwrap_or_else(|error| panic!("shell output must be valid utf8: {error}"));
    assert!(text.contains("writer"), "output was: {text}");
    assert!(client.query_bases().is_empty());
}

#[tokio::test]
async fn shell_help_command_prints_command_summary() {
    let client = Arc::new(FakeClient::new());
    let mut input = VecShellInput::new([":help", ":quit"]);
    let mut output = Vec::new();

    run_shell(client, &mut input, &mut output)
        .await
        .unwrap_or_else(|error| panic!("shell must run cleanly: {error}"));

    let text = String::from_utf8(output)
        .unwrap_or_else(|error| panic!("shell output must be valid utf8: {error}"));
    assert!(text.contains(":status"), "output was: {text}");
}

#[tokio::test]
async fn shell_exits_cleanly_on_eof() {
    let client = Arc::new(FakeClient::new());
    let mut input = VecShellInput::new(Vec::<ScriptedInput>::from([ScriptedInput::Eof]));
    let mut output = Vec::new();

    run_shell(client, &mut input, &mut output)
        .await
        .unwrap_or_else(|error| panic!("shell must exit cleanly on eof: {error}"));

    assert!(String::from_utf8(output)
        .unwrap_or_else(|error| panic!("shell output must be valid utf8: {error}"))
        .is_empty());
}

#[tokio::test]
async fn shell_interrupt_clears_in_progress_buffer_without_exiting() {
    let client = Arc::new(FakeClient::new());
    let mut input = VecShellInput::new(Vec::from([
        ScriptedInput::Line("MATCH (p:Person)".to_string()),
        ScriptedInput::Interrupted,
        ScriptedInput::Line("MATCH (p:Person) RETURN p.name AS name;".to_string()),
        ScriptedInput::Line(":quit".to_string()),
    ]));
    let mut output = Vec::new();

    run_shell(client.clone(), &mut input, &mut output)
        .await
        .unwrap_or_else(|error| panic!("shell must run cleanly: {error}"));

    let text = String::from_utf8(output)
        .unwrap_or_else(|error| panic!("shell output must be valid utf8: {error}"));
    assert!(!text.contains("parse error"), "output was: {text}");
    assert!(text.contains("Ada"), "output was: {text}");
    assert_eq!(client.query_bases(), vec![None]);
}

#[tokio::test]
async fn shell_rejects_query_mixed_with_mutation_without_a_client_call() {
    let client = Arc::new(FakeClient::new());
    let mut input = VecShellInput::new([
        "MATCH (p:Person) RETURN p.name; INSERT (:X {_id: 2});",
        ":quit",
    ]);
    let mut output = Vec::new();

    run_shell(client.clone(), &mut input, &mut output)
        .await
        .unwrap_or_else(|error| panic!("shell must run cleanly: {error}"));

    let text = String::from_utf8(output)
        .unwrap_or_else(|error| panic!("shell output must be valid utf8: {error}"));
    assert!(
        text.contains("error: a query cannot be combined with other statements"),
        "output was: {text}"
    );
    // This is a shape error, not a parser failure: it must not be
    // mislabelled as one.
    assert!(!text.contains("parse error"), "output was: {text}");
    assert!(client.query_bases().is_empty(), "output was: {text}");
    assert_eq!(client.execute_count(), 0, "output was: {text}");
}

#[tokio::test]
async fn shell_rejects_empty_program_without_a_client_call() {
    let client = Arc::new(FakeClient::new());
    // "USE g;" is a syntactically valid program with zero statements
    // (the USE clause is not itself a statement), so it exercises the
    // shape-classification "empty program" branch rather than the
    // parser's error path.
    let mut input = VecShellInput::new(["USE g;", ":quit"]);
    let mut output = Vec::new();

    run_shell(client.clone(), &mut input, &mut output)
        .await
        .unwrap_or_else(|error| panic!("shell must run cleanly: {error}"));

    let text = String::from_utf8(output)
        .unwrap_or_else(|error| panic!("shell output must be valid utf8: {error}"));
    assert!(text.contains("error: empty program"), "output was: {text}");
    assert!(!text.contains("parse error"), "output was: {text}");
    assert!(client.query_bases().is_empty(), "output was: {text}");
    assert_eq!(client.execute_count(), 0, "output was: {text}");
}
