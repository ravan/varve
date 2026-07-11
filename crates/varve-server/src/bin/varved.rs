use clap::Parser;
use std::path::PathBuf;
use tracing_subscriber::EnvFilter;
use varve::{Config, Db, Registries};
use varve_config::{BuildContext, ConfigSection};
use varve_server::{readiness_channel, FrontendContext, ServerError, ServerRegistries, Shutdown};

#[derive(Parser)]
#[command(name = "varved", about = "Run the Varve database server")]
struct Args {
    /// Path to the TOML configuration file.
    #[arg(long)]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), ServerError> {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,varve=debug"));
    // Log output must never share stdout with the VARVED_LISTENING contract line below,
    // so route the subscriber to stderr.
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .init();

    let args = Args::parse();
    let config = Config::from_file(&args.config)?;
    let db = Db::open_with(&config, &Registries::with_builtins()).await?;
    let probe = db.probe_capabilities().await?;
    let registries = ServerRegistries::with_builtins()?;
    let auth_section = config.section("auth").unwrap_or_else(ConfigSection::empty);
    let authenticator = registries.authenticator.build(
        auth_section.backend().unwrap_or("static"),
        &auth_section,
        &BuildContext::empty(),
    )?;
    let metrics_section = config
        .section("metrics")
        .unwrap_or_else(ConfigSection::empty);
    let metrics = registries.metrics.build(
        metrics_section.backend().unwrap_or("prometheus"),
        &metrics_section,
        &BuildContext::empty(),
    )?;
    let server_section = config
        .section("server")
        .unwrap_or_else(ConfigSection::empty);
    let mut frontend_context = BuildContext::empty();
    frontend_context.insert(db.clone());
    let frontend = registries.frontend.build(
        server_section.backend().unwrap_or("http"),
        &server_section,
        &frontend_context,
    )?;
    let (reporter, mut readiness) = readiness_channel();
    let (trigger, shutdown) = Shutdown::channel();
    let context = FrontendContext {
        db,
        authenticator,
        metrics,
        probe,
        readiness: reporter,
    };
    let mut server = tokio::spawn(async move { frontend.serve(context, shutdown).await });

    let address = tokio::select! {
        ready = readiness.wait() => ready?,
        result = &mut server => return flatten_server(result),
    };
    println!("VARVED_LISTENING {address}");

    tokio::select! {
        signal = shutdown_signal() => signal?,
        result = &mut server => return flatten_server(result),
    }
    trigger.shutdown();
    flatten_server(server.await)
}

fn flatten_server(
    result: Result<Result<(), ServerError>, tokio::task::JoinError>,
) -> Result<(), ServerError> {
    result.map_err(|error| ServerError::Protocol(format!("server task failed: {error}")))?
}

async fn shutdown_signal() -> Result<(), ServerError> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result.map_err(ServerError::Io),
            _ = terminate.recv() => Ok(()),
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await.map_err(ServerError::Io)
}
