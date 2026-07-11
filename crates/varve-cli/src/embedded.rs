use std::path::Path;
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use varve::{BasisToken, Db, ProbeReport};
use varve_server::api::{
    params_from_json, CompactionResponse, GcResponse, QueryRequest, StatusResponse, TxRequest,
    TxResponse, VerifyResponse,
};

use crate::client::{CliError, CommandClient};

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
        } = request;
        let params = params_from_json(&params)?;
        let mut query = self.db.query(gql).params(params);
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
            .execute_as(&request.gql, &params, EMBEDDED_USER)
            .await?;
        Ok(TxResponse::from_receipt(&receipt))
    }

    async fn status(&self) -> Result<StatusResponse, CliError> {
        let status = self.db.status().await?;
        Ok(StatusResponse::from_engine(&status, &self.probe))
    }

    async fn compact(&self) -> Result<CompactionResponse, CliError> {
        let report = self.db.compact_once().await?;
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
}
