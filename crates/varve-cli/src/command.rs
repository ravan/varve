//! `varve import` / `varve export` dispatch. Kept in the library (not
//! `main.rs`) so integration tests exercise the format routing, the
//! `--label`/`--graph`/`--query` validation, and the streamed body opening
//! directly.

use std::fs::File;
use std::io::{self, BufRead, BufReader, Write};
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use tokio::io::{AsyncRead, AsyncReadExt, BufReader as TokioBufReader};
use varve_server::api::bulk::IngestResponse;
use varve_server::api::QueryRequest;

use crate::cli::{ExportArgs, ExportFormat, ImportArgs, ImportFormat};
use crate::client::{BulkBody, BulkFormat, CliError, CommandClient};
use crate::transfer::{export_jsonl, export_ndjson, import_jsonl, parse_basis};

/// `varve import`: `ndjson`/`csv` stream the input through the engine fast
/// path (`/v1/ingest` remote, `Db::ingest` embedded); `jsonl-legacy` keeps the
/// one-parameterized-`INSERT`-per-line mode. `--label`/`--graph` are
/// jsonl-legacy-only — the bulk formats carry labels per record and load the
/// default graph — so supplying them with a bulk format is a usage error, not
/// a silent no-op.
pub async fn run_import(
    client: Arc<dyn CommandClient>,
    args: ImportArgs,
    progress: &mut dyn Write,
) -> Result<(), CliError> {
    match args.format {
        ImportFormat::JsonlLegacy => {
            let label = args.label.as_deref().ok_or_else(|| {
                CliError::InvalidInput("--label is required with --format jsonl-legacy".to_string())
            })?;
            let input = open_input(&args.file)?;
            let report = import_jsonl(client, input, label, args.graph.as_deref()).await?;
            writeln!(progress, "committed {} row(s)", report.committed)?;
            Ok(())
        }
        ImportFormat::Ndjson | ImportFormat::Csv => {
            if args.label.is_some() || args.graph.is_some() {
                return Err(CliError::InvalidInput(
                    "--label/--graph are only valid with --format jsonl-legacy".to_string(),
                ));
            }
            let format = match args.format {
                ImportFormat::Csv => BulkFormat::Csv,
                _ => BulkFormat::Ndjson,
            };
            let body = open_bulk_body(&args.file).await?;
            let started = Instant::now();
            let response = client.ingest(format, body).await?;
            report_ingest(&response, started.elapsed(), progress)?;
            Ok(())
        }
    }
}

/// `varve export`: `ndjson` writes the WHOLE graph as bulk NDJSON (embedded
/// only — there is no HTTP export endpoint); `jsonl` runs a GQL query and
/// writes its rows as line-delimited JSON. `--query`/`--basis` belong to
/// `jsonl` and are rejected for `ndjson`.
pub async fn run_export(
    client: Arc<dyn CommandClient>,
    args: ExportArgs,
    progress: &mut dyn Write,
) -> Result<(), CliError> {
    match args.format {
        ExportFormat::Ndjson => {
            if args.query.is_some() || args.basis.is_some() {
                return Err(CliError::InvalidInput(
                    "--query/--basis are not used with --format ndjson (it exports the whole graph)"
                        .to_string(),
                ));
            }
            let output = open_output(&args.file)?;
            let summary = export_ndjson(client, output).await?;
            write!(
                progress,
                "exported {} node(s), {} edge(s)",
                summary.nodes, summary.edges
            )?;
            if summary.edges_skipped > 0 {
                write!(
                    progress,
                    " ({} edge(s) skipped: unresolvable endpoint or non-single label)",
                    summary.edges_skipped
                )?;
            }
            writeln!(progress)?;
            Ok(())
        }
        ExportFormat::Jsonl => {
            let gql = args.query.ok_or_else(|| {
                CliError::InvalidInput("--query is required with --format jsonl".to_string())
            })?;
            let basis = args.basis.as_deref().map(parse_basis).transpose()?;
            let request = QueryRequest {
                gql,
                params: std::collections::BTreeMap::new(),
                basis,
                basis_timeout_ms: None,
            };
            let output = open_output(&args.file)?;
            let rows = export_jsonl(client, request, output).await?;
            writeln!(progress, "exported {rows} row(s)")?;
            Ok(())
        }
    }
}

/// Writes the bulk-import progress line (records, records/s).
fn report_ingest(
    response: &IngestResponse,
    elapsed: std::time::Duration,
    progress: &mut dyn Write,
) -> Result<(), CliError> {
    let records = response.nodes + response.edges;
    let secs = elapsed.as_secs_f64();
    if secs > 0.0 {
        writeln!(
            progress,
            "imported {records} record(s) ({} node(s), {} edge(s)) in {secs:.2}s ({:.0} records/s)",
            response.nodes,
            response.edges,
            records as f64 / secs
        )?;
    } else {
        writeln!(
            progress,
            "imported {records} record(s) ({} node(s), {} edge(s))",
            response.nodes, response.edges
        )?;
    }
    Ok(())
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

/// Opens `path` as a streamed bulk-ingest request body (64 KiB frames), or
/// stdin when `path` is `-`. Nothing is buffered whole — the frames feed the
/// framer (embedded) or reqwest's streamed body (remote).
async fn open_bulk_body(path: &str) -> Result<BulkBody, CliError> {
    if path == "-" {
        Ok(reader_stream(tokio::io::stdin()))
    } else {
        Ok(reader_stream(tokio::fs::File::open(path).await?))
    }
}

/// Wraps an async reader as a `BulkBody`, reading fixed-size chunks with
/// `try_unfold` (so no extra dependency such as tokio-util is needed).
fn reader_stream<R: AsyncRead + Unpin + Send + 'static>(reader: R) -> BulkBody {
    const CHUNK: usize = 64 * 1024;
    let reader = TokioBufReader::new(reader);
    Box::pin(futures::stream::try_unfold(
        reader,
        |mut reader| async move {
            let mut buf = vec![0u8; CHUNK];
            let read = reader.read(&mut buf).await?;
            if read == 0 {
                Ok(None)
            } else {
                buf.truncate(read);
                Ok(Some((Bytes::from(buf), reader)))
            }
        },
    ))
}
