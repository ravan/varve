//! The interactive `varve shell` REPL: line-oriented statement buffering,
//! `:status`/`:help`/`:quit`/`:exit` commands, and dispatch of complete GQL
//! programs to a [`CommandClient`]. [`run_shell`] is generic over
//! [`ShellInput`] so tests can script exact input sequences without a
//! terminal; [`RustylineInput`] is the production adapter.

use std::collections::BTreeMap;
use std::io::Write;
use std::sync::Arc;

use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use varve_gql::ast::Statement;
use varve_server::api::{BasisRequest, QueryRequest, TxRequest};

use crate::client::{CliError, CommandClient};
use crate::output::{format_batches, format_receipt, format_status};

const PRIMARY_PROMPT: &str = "varve> ";
const CONTINUATION_PROMPT: &str = "cont> ";

const HELP_TEXT: &str = "Commands:\n  \
:status   show node status\n  \
:help     show this message\n  \
:quit     exit the shell\n  \
:exit     exit the shell\n\
Anything else is treated as GQL and executed once a statement ends with ';'.";

/// One line-oriented input event, abstracting over a blocking line editor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShellEvent {
    /// A line of input, without its trailing newline.
    Line(String),
    /// The user pressed Ctrl-C.
    Interrupted,
    /// The user pressed Ctrl-D, or input has otherwise been exhausted.
    Eof,
}

/// Everything [`run_shell`] needs from a line editor: read one event for a
/// given prompt, and record a line in history. [`RustylineInput`] is the
/// production adapter; tests substitute a scripted double.
pub trait ShellInput {
    fn read(&mut self, prompt: &str) -> Result<ShellEvent, CliError>;
    fn add_history(&mut self, line: &str) -> Result<(), CliError>;
}

/// Production [`ShellInput`] backed by `rustyline::DefaultEditor`.
pub struct RustylineInput {
    editor: DefaultEditor,
}

impl RustylineInput {
    pub fn new() -> Result<Self, CliError> {
        let editor = DefaultEditor::new().map_err(|error| {
            CliError::InvalidInput(format!("failed to start line editor: {error}"))
        })?;
        Ok(Self { editor })
    }
}

impl ShellInput for RustylineInput {
    fn read(&mut self, prompt: &str) -> Result<ShellEvent, CliError> {
        match self.editor.readline(prompt) {
            Ok(line) => Ok(ShellEvent::Line(line)),
            Err(ReadlineError::Interrupted) => Ok(ShellEvent::Interrupted),
            Err(ReadlineError::Eof) => Ok(ShellEvent::Eof),
            Err(error) => Err(CliError::InvalidInput(format!(
                "line editor error: {error}"
            ))),
        }
    }

    fn add_history(&mut self, line: &str) -> Result<(), CliError> {
        self.editor.add_history_entry(line).map_err(|error| {
            CliError::InvalidInput(format!("failed to record history: {error}"))
        })?;
        Ok(())
    }
}

/// Runs the interactive shell loop until `:quit`/`:exit`/EOF. Buffers
/// non-command input until it ends with `;`, then classifies and dispatches
/// the complete program: exactly one query statement goes to
/// [`CommandClient::query`], one or more mutation statements go to
/// [`CommandClient::execute`], and mixed/empty programs print an error
/// without issuing a client call. Successful transactions update a
/// remembered basis that is attached to every later query, giving remote
/// read-your-writes by default.
pub async fn run_shell(
    client: Arc<dyn CommandClient>,
    input: &mut dyn ShellInput,
    output: &mut dyn Write,
) -> Result<(), CliError> {
    let mut buffer = String::new();
    let mut basis: Option<BasisRequest> = None;

    loop {
        let prompt = if buffer.is_empty() {
            PRIMARY_PROMPT
        } else {
            CONTINUATION_PROMPT
        };
        match input.read(prompt)? {
            ShellEvent::Eof => break,
            ShellEvent::Interrupted => {
                buffer.clear();
            }
            ShellEvent::Line(line) => {
                if buffer.is_empty() {
                    match line.trim() {
                        ":quit" | ":exit" => break,
                        ":status" => {
                            run_status(client.as_ref(), output).await?;
                            continue;
                        }
                        ":help" => {
                            writeln!(output, "{HELP_TEXT}")?;
                            continue;
                        }
                        "" => continue,
                        _ => {}
                    }
                }
                input.add_history(&line)?;
                if !buffer.is_empty() {
                    buffer.push('\n');
                }
                buffer.push_str(&line);
                if buffer.trim_end().ends_with(';') {
                    let program_text = std::mem::take(&mut buffer);
                    run_program(client.as_ref(), &program_text, &mut basis, output).await?;
                }
            }
        }
    }
    Ok(())
}

async fn run_status(client: &dyn CommandClient, output: &mut dyn Write) -> Result<(), CliError> {
    let status = client.status().await?;
    writeln!(output, "{}", format_status(&status))?;
    Ok(())
}

/// Parses and classifies one complete GQL program (as buffered by
/// [`run_shell`]) and dispatches it. Parse errors and shape errors (mixed
/// query/mutation statements, or an empty program) are printed to `output`
/// and never reach the client.
async fn run_program(
    client: &dyn CommandClient,
    text: &str,
    basis: &mut Option<BasisRequest>,
    output: &mut dyn Write,
) -> Result<(), CliError> {
    let program = match varve_gql::parse_program(text) {
        Ok(program) => program,
        Err(error) => {
            writeln!(output, "parse error: {error}")?;
            return Ok(());
        }
    };

    if program.statements.is_empty() {
        writeln!(output, "error: empty program")?;
        return Ok(());
    }

    let has_query = program
        .statements
        .iter()
        .any(|statement| matches!(statement, Statement::Query(_)));

    if has_query {
        if program.statements.len() != 1 {
            writeln!(
                output,
                "error: a query cannot be combined with other statements"
            )?;
            return Ok(());
        }
        let request = QueryRequest {
            gql: text.to_string(),
            params: BTreeMap::new(),
            basis: basis.clone(),
            basis_timeout_ms: None,
        };
        let batches = client.query(request).await?;
        writeln!(output, "{}", format_batches(&batches)?)?;
    } else {
        let request = TxRequest {
            gql: text.to_string(),
            params: BTreeMap::new(),
        };
        let response = client.execute(request).await?;
        *basis = Some(BasisRequest::TxId(response.basis));
        writeln!(output, "{}", format_receipt(&response))?;
    }
    Ok(())
}
