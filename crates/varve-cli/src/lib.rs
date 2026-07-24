//! The `varve-cli` crate: a `CommandClient` trait with embedded
//! (in-process `Db`) and remote (HTTP) adapters, the `varve` clap grammar
//! and connection selector, deterministic table/receipt/status rendering,
//! the interactive `varve shell` REPL, JSONL import/export, and node
//! administration commands.

mod admin;
mod cli;
mod client;
mod command;
mod embedded;
mod output;
mod remote;
mod shell;
mod transfer;

pub use admin::{run_admin_compact, run_admin_gc, run_admin_status, run_admin_verify};
pub use cli::{
    AdminArgs, AdminCommand, Cli, Command, ExportArgs, ExportFormat, ImportArgs, ImportFormat,
};
pub use client::{BulkBody, BulkFormat, CliError, CommandClient};
pub use command::{run_export, run_import};
pub use embedded::EmbeddedClient;
pub use remote::RemoteClient;
pub use shell::{run_shell, RustylineInput, ShellEvent, ShellInput};
pub use transfer::{export_jsonl, export_ndjson, import_jsonl, parse_basis, ImportReport};

// Re-exported for convenience: the trait methods above are defined in terms
// of these shared wire DTOs (`varve_server::api`), so callers building
// requests/reading responses do not need a second `varve-server` import.
pub use varve_server::api::{
    CompactionResponse, GcResponse, QueryRequest, StatusResponse, TxRequest, TxResponse,
    VerifyResponse,
};
