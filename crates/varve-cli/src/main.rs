//! Thin binary entry point for `varve`: parses the command line, builds
//! the selected `CommandClient`, and dispatches to the chosen subcommand.
//! All grammar, client-selection, and shell logic lives in the library
//! crate (see `lib.rs`) so integration tests can exercise it directly.

use std::io::{self};
use std::process::ExitCode;

use clap::Parser;
use varve_cli::{AdminCommand, Cli, CliError, Command, RustylineInput};

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
            let mut stderr = io::stderr();
            varve_cli::run_import(client, args, &mut stderr).await
        }
        Command::Export(args) => {
            let mut stderr = io::stderr();
            varve_cli::run_export(client, args, &mut stderr).await
        }
        Command::Admin(args) => {
            let stdout = io::stdout();
            let mut lock = stdout.lock();
            match args.command {
                AdminCommand::Status => {
                    varve_cli::run_admin_status(client.as_ref(), args.json, &mut lock).await
                }
                AdminCommand::Compact { full } => {
                    varve_cli::run_admin_compact(client.as_ref(), args.json, full, &mut lock).await
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
