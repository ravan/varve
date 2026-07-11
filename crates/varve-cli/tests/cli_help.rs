use clap::Parser;
use varve_cli::{AdminCommand, Cli, CliError, Command};

#[test]
fn shell_subcommand_is_recognized() {
    let cli = Cli::try_parse_from(["varve", "--dir", "/tmp/db", "shell"])
        .unwrap_or_else(|error| panic!("parse must succeed: {error}"));
    assert_eq!(cli.command, Command::Shell);
}

#[test]
fn dir_and_url_conflict() {
    let result = Cli::try_parse_from([
        "varve",
        "--dir",
        "/tmp/db",
        "--url",
        "http://127.0.0.1:9999",
        "shell",
    ]);
    assert!(
        result.is_err(),
        "--dir and --url must be mutually exclusive"
    );
}

#[test]
fn import_subcommand_is_recognized() {
    let cli = Cli::try_parse_from([
        "varve",
        "--dir",
        "/tmp/db",
        "import",
        "--label",
        "Person",
        "data.jsonl",
    ])
    .unwrap_or_else(|error| panic!("parse must succeed: {error}"));
    match cli.command {
        Command::Import(args) => {
            assert_eq!(args.label, "Person");
            assert_eq!(args.graph, None);
            assert_eq!(args.file, "data.jsonl");
        }
        other => panic!("expected Command::Import, got {other:?}"),
    }
}

#[test]
fn import_subcommand_rejects_dir_and_url_together() {
    let result = Cli::try_parse_from([
        "varve",
        "--dir",
        "/tmp/db",
        "--url",
        "http://127.0.0.1:9999",
        "import",
        "--label",
        "Person",
        "data.jsonl",
    ]);
    assert!(
        result.is_err(),
        "--dir and --url must be mutually exclusive for import"
    );
}

#[test]
fn export_subcommand_is_recognized() {
    let cli = Cli::try_parse_from([
        "varve",
        "--dir",
        "/tmp/db",
        "export",
        "--query",
        "MATCH (p:Person) RETURN p.name AS name",
        "-",
    ])
    .unwrap_or_else(|error| panic!("parse must succeed: {error}"));
    match cli.command {
        Command::Export(args) => {
            assert_eq!(args.query, "MATCH (p:Person) RETURN p.name AS name");
            assert_eq!(args.basis, None);
            assert_eq!(args.file, "-");
        }
        other => panic!("expected Command::Export, got {other:?}"),
    }
}

#[test]
fn export_subcommand_rejects_dir_and_url_together() {
    let result = Cli::try_parse_from([
        "varve",
        "--dir",
        "/tmp/db",
        "--url",
        "http://127.0.0.1:9999",
        "export",
        "--query",
        "MATCH (p:Person) RETURN p.name AS name",
        "-",
    ]);
    assert!(
        result.is_err(),
        "--dir and --url must be mutually exclusive for export"
    );
}

#[test]
fn admin_subcommand_is_recognized() {
    let cli = Cli::try_parse_from(["varve", "--dir", "/tmp/db", "admin", "status"])
        .unwrap_or_else(|error| panic!("parse must succeed: {error}"));
    match cli.command {
        Command::Admin(args) => {
            assert_eq!(args.command, AdminCommand::Status);
            assert!(!args.json);
        }
        other => panic!("expected Command::Admin, got {other:?}"),
    }
}

#[test]
fn admin_subcommand_accepts_json_flag_for_every_action() {
    for (action, expected) in [
        ("status", AdminCommand::Status),
        ("compact", AdminCommand::Compact),
        ("gc", AdminCommand::Gc),
        ("verify", AdminCommand::Verify),
    ] {
        let cli = Cli::try_parse_from(["varve", "--dir", "/tmp/db", "admin", "--json", action])
            .unwrap_or_else(|error| panic!("parse must succeed for {action}: {error}"));
        match cli.command {
            Command::Admin(args) => {
                assert_eq!(args.command, expected);
                assert!(args.json);
            }
            other => panic!("expected Command::Admin, got {other:?}"),
        }
    }
}

#[test]
fn admin_subcommand_rejects_dir_and_url_together() {
    let result = Cli::try_parse_from([
        "varve",
        "--dir",
        "/tmp/db",
        "--url",
        "http://127.0.0.1:9999",
        "admin",
        "status",
    ]);
    assert!(
        result.is_err(),
        "--dir and --url must be mutually exclusive for admin"
    );
}

fn assert_configuration_error<T>(result: Result<T, CliError>) {
    match result {
        Err(CliError::InvalidInput(_)) => {}
        Err(other) => panic!("expected a configuration error, got {other}"),
        Ok(_) => panic!("expected a configuration error, got Ok(_)"),
    }
}

#[tokio::test]
async fn remote_without_token_errors_before_any_network_call() {
    let cli = Cli::try_parse_from(["varve", "--url", "http://127.0.0.1:1", "shell"])
        .unwrap_or_else(|error| panic!("parse must succeed: {error}"));

    assert_configuration_error(cli.build_client().await);
}

#[tokio::test]
async fn neither_dir_nor_url_is_a_configuration_error() {
    let cli = Cli::try_parse_from(["varve", "shell"])
        .unwrap_or_else(|error| panic!("parse must succeed: {error}"));

    assert_configuration_error(cli.build_client().await);
}
