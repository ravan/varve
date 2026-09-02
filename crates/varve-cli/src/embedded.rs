use std::path::Path;
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::StreamExt;
use varve::{BasisToken, Db, EdgePut, NodePut, ProbeReport, TxReceipt};
use varve_server::api::bulk::csv::CsvFramer;
use varve_server::api::bulk::{
    BulkOp, Framer, IngestProgress, IngestResponse, NdjsonFramer, DEFAULT_CHUNK_OPS,
    DEFAULT_MAX_LINE_BYTES,
};
use varve_server::api::{
    params_from_json, CompactionResponse, GcResponse, QueryRequest, StatusResponse, TxRequest,
    TxResponse, VerifyResponse,
};

use crate::client::{BulkBody, BulkFormat, CliError, CommandClient};

/// Subject recorded against transactions issued from the embedded CLI
/// adapter (mirrors the authenticated subject an HTTP tx would carry).
const EMBEDDED_USER: &str = "cli:embedded";

/// Wraps a local, embedded [`Db`] behind [`CommandClient`]. There is no
/// network hop: every method is a direct call into the engine.
pub struct EmbeddedClient {
    db: Db,
    probe: ProbeReport,
}

impl EmbeddedClient {
    /// Opens (or creates) a local-filesystem database at `dir`, matching
    /// `Db::local`'s durable log+store layout, and probes storage
    /// capabilities once up front for `status()` reporting.
    pub async fn open(dir: &Path) -> Result<Self, CliError> {
        let db = Db::local(dir).await?;
        let probe = db.probe_capabilities().await?;
        Ok(Self { db, probe })
    }
}

#[async_trait]
impl CommandClient for EmbeddedClient {
    async fn query(&self, request: QueryRequest) -> Result<Vec<RecordBatch>, CliError> {
        let QueryRequest {
            gql,
            params,
            basis,
            basis_timeout_ms,
            graph,
        } = request;
        let params = params_from_json(&params)?;
        let mut query = self.db.query(gql).params(params);
        if let Some(graph) = graph {
            query = query.graph(graph);
        }
        if let Some(basis) = basis {
            query = query.basis(BasisToken::try_from(basis)?);
        }
        if let Some(timeout_ms) = basis_timeout_ms {
            query = query.basis_timeout(Duration::from_millis(timeout_ms));
        }
        Ok(query.await?)
    }

    async fn execute(&self, request: TxRequest) -> Result<TxResponse, CliError> {
        let params = params_from_json(&request.params)?;
        let receipt = self
            .db
            .execute_as_in(
                request.graph.as_deref(),
                &request.gql,
                &params,
                EMBEDDED_USER,
            )
            .await?;
        Ok(TxResponse::from_receipt(&receipt))
    }

    async fn status(&self) -> Result<StatusResponse, CliError> {
        let status = self.db.status().await?;
        Ok(StatusResponse::from_engine(&status, &self.probe))
    }

    async fn compact(&self, full: bool) -> Result<CompactionResponse, CliError> {
        let report = if full {
            self.db.compact_full_once().await?
        } else {
            self.db.compact_once().await?
        };
        Ok(CompactionResponse::from_report(&report))
    }

    async fn gc(&self) -> Result<GcResponse, CliError> {
        let report = self.db.gc_once().await?;
        Ok(GcResponse::from_report(&report))
    }

    async fn verify(&self) -> Result<VerifyResponse, CliError> {
        let report = self.db.verify().await?;
        Ok(VerifyResponse::from_report(&report))
    }

    /// Embedded bulk import: drive `body` through the SAME incremental framer
    /// the `/v1/ingest` handler uses, committing `chunk_ops`-sized chunks as
    /// one atomic `Db::ingest_as` each — no GQL parse or plan, constant memory
    /// (one line + one chunk). Earlier chunks stay committed on a later
    /// failure; the error carries the committed counts.
    async fn ingest(
        &self,
        format: BulkFormat,
        mut body: BulkBody,
    ) -> Result<IngestResponse, CliError> {
        let max_line_bytes = DEFAULT_MAX_LINE_BYTES.as_usize();
        let mut framer = match format {
            BulkFormat::Ndjson => Framer::Ndjson(NdjsonFramer::new(max_line_bytes)),
            BulkFormat::Csv => Framer::Csv(Box::new(CsvFramer::new(max_line_bytes))),
        };
        let mut pending: Vec<BulkOp> = Vec::new();
        let mut progress = IngestProgress::default();
        let mut last: Option<TxReceipt> = None;

        while let Some(frame) = body.next().await {
            let frame = frame
                .map_err(|error| ingest_error(&progress, format!("body stream error: {error}")))?;
            let ops = framer
                .push(&frame)
                .map_err(|error| ingest_error(&progress, error.to_string()))?;
            pending.extend(ops);
            while pending.len() >= DEFAULT_CHUNK_OPS {
                let chunk: Vec<BulkOp> = pending.drain(..DEFAULT_CHUNK_OPS).collect();
                self.commit_chunk(chunk, &mut progress, &mut last).await?;
            }
        }
        let tail = framer
            .finish()
            .map_err(|error| ingest_error(&progress, error.to_string()))?;
        pending.extend(tail);
        while !pending.is_empty() {
            let take = pending.len().min(DEFAULT_CHUNK_OPS);
            let chunk: Vec<BulkOp> = pending.drain(..take).collect();
            self.commit_chunk(chunk, &mut progress, &mut last).await?;
        }

        match last {
            Some(last) => Ok(IngestResponse::from_committed(&progress, &last)),
            None => Err(CliError::InvalidInput(
                "input contained no records".to_string(),
            )),
        }
    }

    async fn snapshot_all(&self) -> Result<(Option<RecordBatch>, Option<RecordBatch>), CliError> {
        let nodes = self.db.snapshot_all_nodes().await?;
        let edges = self.db.snapshot_all_edges().await?;
        Ok((nodes, edges))
    }
}

impl EmbeddedClient {
    /// Commits one non-empty chunk as an atomic `Db::ingest_as` transaction
    /// attributed to the embedded subject, folding the receipt into
    /// `progress`/`last`. An engine failure surfaces as a committed-progress
    /// [`CliError::Ingest`].
    async fn commit_chunk(
        &self,
        chunk: Vec<BulkOp>,
        progress: &mut IngestProgress,
        last: &mut Option<TxReceipt>,
    ) -> Result<(), CliError> {
        let mut nodes: Vec<NodePut> = Vec::new();
        let mut edges: Vec<EdgePut> = Vec::new();
        for op in chunk {
            match op {
                BulkOp::Node(node) => nodes.push(node),
                BulkOp::Edge(edge) => edges.push(edge),
            }
        }
        match self.db.ingest_as(EMBEDDED_USER, nodes, edges).await {
            Ok(receipt) => {
                progress.absorb(&receipt);
                *last = Some(receipt);
                Ok(())
            }
            Err(error) => Err(ingest_error(progress, error.to_string())),
        }
    }
}

/// A committed-progress bulk-ingest error (mirrors the HTTP route's body).
fn ingest_error(progress: &IngestProgress, message: String) -> CliError {
    CliError::Ingest {
        message,
        committed: progress.clone(),
    }
}
