use std::pin::Pin;

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use varve_server::api::bulk::{IngestProgress, IngestResponse};
use varve_server::api::{
    CompactionResponse, GcResponse, QueryRequest, StatusResponse, TxRequest, TxResponse,
    VerifyResponse,
};
use varve_server::ServerError;

/// The bulk wire format a [`CommandClient::ingest`] body is in — selects the
/// `Content-Type` on the remote path and the framer on the embedded path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BulkFormat {
    Ndjson,
    Csv,
}

impl BulkFormat {
    /// The `Content-Type` essence the server negotiates on `/v1/ingest`.
    pub fn content_type(self) -> &'static str {
        match self {
            BulkFormat::Ndjson => "application/x-ndjson",
            BulkFormat::Csv => "text/csv",
        }
    }
}

/// A streamed bulk-ingest request body: byte frames read from a file or stdin.
/// The remote adapter forwards it verbatim to `/v1/ingest` (reqwest streamed
/// body); the embedded adapter drives it through the same incremental framer
/// the server uses. Neither buffers the whole payload.
pub type BulkBody = Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>>;

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
    #[error("server is applying backpressure (429): retry")]
    Backpressure,
    /// A bulk ingest failed mid-stream. Earlier chunks stay committed (the
    /// stream is not atomic); `committed` reports what is durable, mirroring
    /// the HTTP route's committed-progress error body, so the retry story is
    /// idempotent replay from the start.
    #[error("{message} (committed {} node(s), {} edge(s), {} transaction(s); basis {})",
        .committed.nodes, .committed.edges, .committed.transactions, .committed.basis)]
    Ingest {
        message: String,
        committed: IngestProgress,
    },
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
    /// One compaction job. `full` selects the full-sweep variant, which also
    /// drains L0 groups below the `log_limit` gate; callers loop until the
    /// report's `jobs` is 0.
    async fn compact(&self, full: bool) -> Result<CompactionResponse, CliError>;
    async fn gc(&self) -> Result<GcResponse, CliError>;
    async fn verify(&self) -> Result<VerifyResponse, CliError>;

    /// Bulk-load `body` (NDJSON/CSV) through the engine's fast path: the
    /// remote adapter streams it to `POST /v1/ingest`; the embedded adapter
    /// decodes it and commits `Db::ingest_as` chunks. Not atomic across the
    /// whole stream — earlier chunks stay committed if a later one fails, and
    /// the error carries the committed counts (same contract as the HTTP
    /// route). See [`BulkFormat`]/[`BulkBody`].
    async fn ingest(&self, format: BulkFormat, body: BulkBody) -> Result<IngestResponse, CliError>;

    /// Snapshot the whole data graph (nodes, then edges) at the current time,
    /// for `varve export --format ndjson`. Embedded only: the remote adapter
    /// returns an error (there is no HTTP export endpoint).
    async fn snapshot_all(&self) -> Result<(Option<RecordBatch>, Option<RecordBatch>), CliError>;
}
