//! `varve admin status|compact|gc|verify`: each subcommand maps directly to
//! one [`CommandClient`] method call and renders either stable
//! human-readable key/value text (fixed field order, packed log positions)
//! or, with `--json`, the exact server DTO.

use std::io::Write;

use crate::client::{CliError, CommandClient};
use crate::output::{format_compaction, format_gc, format_status, format_verify};

/// Runs `varve admin status`.
pub async fn run_admin_status(
    client: &dyn CommandClient,
    json: bool,
    output: &mut dyn Write,
) -> Result<(), CliError> {
    let response = client.status().await?;
    if json {
        writeln!(output, "{}", serde_json::to_string(&response)?)?;
    } else {
        writeln!(output, "{}", format_status(&response))?;
    }
    Ok(())
}

/// Runs `varve admin compact`.
pub async fn run_admin_compact(
    client: &dyn CommandClient,
    json: bool,
    output: &mut dyn Write,
) -> Result<(), CliError> {
    let response = client.compact().await?;
    if json {
        writeln!(output, "{}", serde_json::to_string(&response)?)?;
    } else {
        writeln!(output, "{}", format_compaction(&response))?;
    }
    Ok(())
}

/// Runs `varve admin gc`.
pub async fn run_admin_gc(
    client: &dyn CommandClient,
    json: bool,
    output: &mut dyn Write,
) -> Result<(), CliError> {
    let response = client.gc().await?;
    if json {
        writeln!(output, "{}", serde_json::to_string(&response)?)?;
    } else {
        writeln!(output, "{}", format_gc(&response))?;
    }
    Ok(())
}

/// Runs `varve admin verify`.
pub async fn run_admin_verify(
    client: &dyn CommandClient,
    json: bool,
    output: &mut dyn Write,
) -> Result<(), CliError> {
    let response = client.verify().await?;
    if json {
        writeln!(output, "{}", serde_json::to_string(&response)?)?;
    } else {
        writeln!(output, "{}", format_verify(&response))?;
    }
    Ok(())
}
