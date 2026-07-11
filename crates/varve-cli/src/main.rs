//! Thin binary entry point for `varve`: parses the command line, builds
//! the selected `CommandClient`, and dispatches to the chosen subcommand.
//! All grammar, client-selection, and shell logic lives in the library
//! crate (see `lib.rs`) so integration tests can exercise it directly.

use std::fs::File;
use std::io::{self, BufRead, BufReader, Write};
use std::process::ExitCode;

use clap::Parser;
use varve_cli::{AdminCommand, Cli, CliError, Command, RustylineInput};
use varve_server::api::QueryRequest;

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(cli).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error}");
            match error {
                CliError::InvalidInput(_) => ExitCode::from(2),
                _ => ExitCode::from(1),
            }
        }
    }
}

async fn run(cli: Cli) -> Result<(), CliError> {
    let client = cli.build_client().await?;
    match cli.command {
        Command::Shell => {
            let mut input = RustylineInput::new()?;
            let stdout = io::stdout();
            let mut lock = stdout.lock();
            varve_cli::run_shell(client, &mut input, &mut lock).await
        }
        Command::Import(args) => {
            let input = open_input(&args.file)?;
            let report =
                varve_cli::import_jsonl(client, input, &args.label, args.graph.as_deref()).await?;
            eprintln!("committed {} row(s)", report.committed);
            Ok(())
        }
        Command::Export(args) => {
            let basis = args
                .basis
                .as_deref()
                .map(varve_cli::parse_basis)
                .transpose()?;
            let request = QueryRequest {
                gql: args.query,
                params: std::collections::BTreeMap::new(),
                basis,
                basis_timeout_ms: None,
            };
            let output = open_output(&args.file)?;
            let rows = varve_cli::export_jsonl(client, request, output).await?;
            eprintln!("exported {rows} row(s)");
            Ok(())
        }
        Command::Admin(args) => {
            let stdout = io::stdout();
            let mut lock = stdout.lock();
            match args.command {
                AdminCommand::Status => {
                    varve_cli::run_admin_status(client.as_ref(), args.json, &mut lock).await
                }
                AdminCommand::Compact => {
                    varve_cli::run_admin_compact(client.as_ref(), args.json, &mut lock).await
                }
                AdminCommand::Gc => {
                    varve_cli::run_admin_gc(client.as_ref(), args.json, &mut lock).await
                }
                AdminCommand::Verify => {
                    varve_cli::run_admin_verify(client.as_ref(), args.json, &mut lock).await
                }
            }
        }
    }
}

/// Opens `path` for buffered reading, or stdin when `path` is `-`.
fn open_input(path: &str) -> Result<Box<dyn BufRead>, CliError> {
    if path == "-" {
        Ok(Box::new(BufReader::new(io::stdin())))
    } else {
        Ok(Box::new(BufReader::new(File::open(path)?)))
    }
}

/// Opens `path` for writing, or stdout when `path` is `-`.
fn open_output(path: &str) -> Result<Box<dyn Write>, CliError> {
    if path == "-" {
        Ok(Box::new(io::stdout()))
    } else {
        Ok(Box::new(File::create(path)?))
    }
}
