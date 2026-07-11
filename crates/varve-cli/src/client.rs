use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use varve_server::api::{
    CompactionResponse, GcResponse, QueryRequest, StatusResponse, TxRequest, TxResponse,
    VerifyResponse,
};
use varve_server::ServerError;

/// Errors that can arise from either CLI client adapter. Display never
/// includes bearer tokens or response headers -- only structured status
/// codes, decoded server error codes/messages, and locally-observed cause
/// chains (IO/JSON/Arrow/engine errors).
#[derive(Debug, thiserror::Error)]
pub enum CliError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
    #[error(transparent)]
    Engine(#[from] varve::EngineError),
    #[error(transparent)]
    Http(#[from] reqwest::Error),
    #[error("server responded with unexpected HTTP status {status}")]
    Status { status: u16 },
    #[error("{code}: {message}")]
    Api { code: String, message: String },
    #[error("invalid input: {0}")]
    InvalidInput(String),
    #[error("server issued a second writer redirect; refusing to follow it")]
    RedirectLoop,
}

impl From<ServerError> for CliError {
    fn from(error: ServerError) -> Self {
        match error {
            ServerError::Engine(inner) => CliError::Engine(inner),
            other => CliError::InvalidInput(other.to_string()),
        }
    }
}

/// The single client surface both CLI adapters implement: an
/// [`crate::EmbeddedClient`] talking straight to a local `Db`, and a
/// [`crate::RemoteClient`] talking to a `varved` HTTP frontend. Callers
/// (the shell, in later tasks) code against this trait and never need to
/// know which adapter is behind it.
#[async_trait]
pub trait CommandClient: Send + Sync {
    async fn query(&self, request: QueryRequest) -> Result<Vec<RecordBatch>, CliError>;
    async fn execute(&self, request: TxRequest) -> Result<TxResponse, CliError>;
    async fn status(&self) -> Result<StatusResponse, CliError>;
    async fn compact(&self) -> Result<CompactionResponse, CliError>;
    async fn gc(&self) -> Result<GcResponse, CliError>;
    async fn verify(&self) -> Result<VerifyResponse, CliError>;
}
