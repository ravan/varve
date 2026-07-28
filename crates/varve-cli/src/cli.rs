//! The `varve` command-line grammar: a connection selector shared by every
//! subcommand (`--dir` xor `--url`/`--token`) plus the subcommand itself.
//!
//! [`Cli`] intentionally does not derive `Debug`/`Display`: it owns the raw
//! bearer token, which must never be printed or logged.

use std::path::PathBuf;
use std::sync::Arc;

use clap::{Parser, Subcommand, ValueEnum};
use url::Url;

use crate::client::{CliError, CommandClient};
use crate::embedded::EmbeddedClient;
use crate::remote::RemoteClient;

/// `varve` command-line entry point.
#[derive(Parser)]
#[command(name = "varve", about = "Varve bitemporal graph database client")]
pub struct Cli {
    /// Path to a local database directory. Mutually exclusive with `--url`.
    #[arg(long, conflicts_with = "url")]
    pub dir: Option<PathBuf>,

    /// Base URL of a remote `varved` HTTP frontend. Mutually exclusive
    /// with `--dir`.
    #[arg(long, conflicts_with = "dir")]
    pub url: Option<Url>,

    /// Bearer token sent with every request to `--url`. Falls back to the
    /// `VARVE_TOKEN` environment variable; never echoed back by `--help`
    /// or any diagnostic output.
    #[arg(long, env = "VARVE_TOKEN", hide_env_values = true)]
    pub token: Option<String>,

    #[command(subcommand)]
    pub command: Command,
}

/// Top-level subcommands: the interactive shell, JSONL import/export, and
/// node administration. All share the same `--dir`/`--url` connection
/// selector defined on [`Cli`].
#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum Command {
    /// Start an interactive REPL against the selected connection.
    Shell,
    /// Bulk-load a file or stdin through the engine's fast path
    /// (`/v1/ingest` / `Db::ingest`); `--format jsonl-legacy` keeps the old
    /// one-`INSERT`-per-line mode.
    Import(ImportArgs),
    /// Write the whole graph as bulk NDJSON, or a GQL query's rows as
    /// line-delimited JSON (`--format`).
    Export(ExportArgs),
    /// Node administration: status, compaction, garbage collection, and
    /// integrity verification.
    Admin(AdminArgs),
}

/// Bulk-import wire format. `ndjson`/`csv` take the engine fast path
/// (`Db::ingest` embedded, `/v1/ingest` remote); `jsonl-legacy` is the
/// original one-parameterized-`INSERT`-per-line mode (the only mode with
/// per-line GQL validation and the only one that uses `--label`/`--graph`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum, Default)]
pub enum ImportFormat {
    #[default]
    Ndjson,
    Csv,
    JsonlLegacy,
}

/// Export wire format. `jsonl` runs a GQL query and writes its rows as
/// line-delimited JSON (the original behavior); `ndjson` writes the WHOLE
/// graph as bulk NDJSON (nodes then edges) so `export | import` copies a graph
/// Varve→Varve.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum, Default)]
pub enum ExportFormat {
    #[default]
    Jsonl,
    Ndjson,
}

/// Arguments for `varve import`.
#[derive(Debug, clap::Args, PartialEq, Eq)]
pub struct ImportArgs {
    /// Wire format of the input.
    #[arg(long, value_enum, default_value_t = ImportFormat::Ndjson)]
    pub format: ImportFormat,
    /// Label applied to every inserted node. Only valid with
    /// `--format jsonl-legacy` (bulk formats carry labels per record).
    #[arg(long)]
    pub label: Option<String>,
    /// Graph to `USE` before each insert. Only valid with
    /// `--format jsonl-legacy` (bulk formats load the default graph).
    #[arg(long)]
    pub graph: Option<String>,
    /// Path to the input file, or `-` to read from stdin.
    pub file: String,
}

/// Arguments for `varve export`.
#[derive(Debug, clap::Args, PartialEq, Eq)]
pub struct ExportArgs {
    /// Output format.
    #[arg(long, value_enum, default_value_t = ExportFormat::Jsonl)]
    pub format: ExportFormat,
    /// The GQL query to run. Required for `--format jsonl`; rejected for
    /// `--format ndjson` (which exports the whole graph).
    #[arg(long)]
    pub query: Option<String>,
    /// Read basis: a bare transaction id, or `at:<packed-u64>`. `--format
    /// jsonl` only.
    #[arg(long)]
    pub basis: Option<String>,
    /// Path to write to, or `-` to write to stdout.
    pub file: String,
}

/// Arguments for `varve admin`.
#[derive(Debug, clap::Args, PartialEq, Eq)]
pub struct AdminArgs {
    #[command(subcommand)]
    pub command: AdminCommand,
    /// Emit the exact server response as JSON instead of human-readable
    /// key/value text.
    #[arg(long)]
    pub json: bool,
}

/// `varve admin` actions: each maps to exactly one `CommandClient` call.
#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum AdminCommand {
    /// Report node role(s), applied progress, and probe verdict.
    Status,
    /// Run compaction.
    Compact {
        /// Run full-sweep compaction, repeating until no jobs remain. Also
        /// drains L0 groups below the `log_limit` gate, leaving no
        /// full-iid-space L0 trie for anchored point/set lookups to scan --
        /// the shape a bulk load should be compacted into before serving.
        /// Slower and more memory-hungry than the default incremental pass.
        #[arg(long)]
        full: bool,
    },
    /// Run garbage collection.
    Gc,
    /// Verify manifest/trie/log integrity.
    Verify,
}

impl Cli {
    /// Builds the `CommandClient` selected by `--dir`/`--url`. Returns a
    /// `CliError::InvalidInput` configuration error -- before any network
    /// call -- if the selector is missing, ambiguous, or (for `--url`)
    /// missing its token.
    pub async fn build_client(&self) -> Result<Arc<dyn CommandClient>, CliError> {
        match (&self.dir, &self.url) {
            (Some(dir), None) => {
                let client = EmbeddedClient::open(dir).await?;
                Ok(Arc::new(client))
            }
            (None, Some(url)) => {
                let token = self.token.clone().ok_or_else(|| {
                    CliError::InvalidInput(
                        "--token (or VARVE_TOKEN) is required when using --url".to_string(),
                    )
                })?;
                let client = RemoteClient::new(url.clone(), token)?;
                Ok(Arc::new(client))
            }
            (Some(_), Some(_)) => Err(CliError::InvalidInput(
                "--dir and --url are mutually exclusive".to_string(),
            )),
            (None, None) => Err(CliError::InvalidInput(
                "one of --dir or --url is required".to_string(),
            )),
        }
    }
}
