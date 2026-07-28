use std::io::Cursor;

use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use reqwest::{header, Client, Response, StatusCode};
use serde::{de::DeserializeOwned, Serialize};
use url::Url;
use varve_server::api::bulk::{IngestErrorResponse, IngestResponse};
use varve_server::api::{
    CompactRequest, CompactionResponse, ErrorResponse, GcResponse, QueryRequest, StatusResponse,
    TxRequest, TxResponse, VerifyResponse, ARROW_STREAM_CONTENT_TYPE,
};

use crate::client::{BulkBody, BulkFormat, CliError, CommandClient};

/// Default cap on a single buffered HTTP response body. The CLI always
/// buffers query results client-side (table/JSONL rendering need a
/// complete result), so this bounds worst-case memory rather than
/// disabling buffering; it does not change server-side backpressure or the
/// embedded streaming interface.
pub const DEFAULT_MAX_RESPONSE_BYTES: usize = 256 * 1024 * 1024;

/// Talks to a `varved` HTTP frontend. Mutations (tx/admin) that land on a
/// non-writer node are rerouted exactly once to the advertised writer;
/// queries always stay on the node this client was built against.
pub struct RemoteClient {
    http: Client,
    base: Url,
    token: String,
    max_response_bytes: usize,
}

impl RemoteClient {
    pub fn new(base: Url, token: String) -> Result<Self, CliError> {
        let http = Client::builder().build()?;
        Ok(Self {
            http,
            base,
            token,
            max_response_bytes: DEFAULT_MAX_RESPONSE_BYTES,
        })
    }

    /// Overrides the response-buffering cap. A `bytes` of zero is rejected
    /// (the cap is left unchanged) since a client that can never buffer a
    /// response cannot function.
    pub fn with_max_response_bytes(mut self, bytes: usize) -> RemoteClient {
        if bytes > 0 {
            self.max_response_bytes = bytes;
        }
        self
    }

    fn join(&self, path: &str) -> Result<Url, CliError> {
        self.base
            .join(path)
            .map_err(|error| CliError::InvalidInput(format!("invalid request path: {error}")))
    }

    async fn post(&self, url: Url, body: &[u8]) -> Result<Response, CliError> {
        Ok(self
            .http
            .post(url)
            .bearer_auth(&self.token)
            .header(header::CONTENT_TYPE, "application/json")
            .body(body.to_vec())
            .send()
            .await?)
    }

    /// Reads a response body into memory, refusing to grow the buffer past
    /// `max_response_bytes`. Used for both JSON and Arrow IPC bodies so a
    /// misbehaving or hostile server can never force unbounded buffering.
    async fn read_bounded(&self, mut response: Response) -> Result<Vec<u8>, CliError> {
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await? {
            if bytes.len() + chunk.len() > self.max_response_bytes {
                return Err(CliError::Io(std::io::Error::other(
                    "response body exceeded max_response_bytes",
                )));
            }
            bytes.extend_from_slice(&chunk);
        }
        Ok(bytes)
    }

    async fn decode_json<T: DeserializeOwned>(&self, response: Response) -> Result<T, CliError> {
        let bytes = self.read_bounded(response).await?;
        Ok(serde_json::from_slice(&bytes)?)
    }

    /// Turns a non-2xx, non-421 response into a `CliError`: a 429 (writer
    /// backpressure, slice 10) becomes `CliError::Backpressure`, a
    /// structured `ErrorResponse` becomes `CliError::Api`, anything else
    /// becomes `CliError::Status`. Never surfaces response headers.
    async fn error_for(&self, response: Response) -> CliError {
        if response.status() == StatusCode::TOO_MANY_REQUESTS {
            return CliError::Backpressure;
        }
        let status = response.status().as_u16();
        match self.read_bounded(response).await {
            Ok(bytes) => match serde_json::from_slice::<ErrorResponse>(&bytes) {
                Ok(error) => CliError::Api {
                    code: error.code,
                    message: error.message,
                },
                Err(_) => CliError::Status { status },
            },
            Err(error) => error,
        }
    }

    /// Turns a non-2xx `/v1/ingest` response into a `CliError`, preferring the
    /// committed-progress `IngestErrorResponse` (a mid-stream 422/5xx) so the
    /// caller learns what durably committed; falls back to the plain
    /// `ErrorResponse`/status mapping otherwise. A 429 is `Backpressure`.
    async fn ingest_error_for(&self, response: Response) -> CliError {
        if response.status() == StatusCode::TOO_MANY_REQUESTS {
            return CliError::Backpressure;
        }
        let status = response.status().as_u16();
        match self.read_bounded(response).await {
            Ok(bytes) => {
                // `IngestErrorResponse` requires `committed`; a gate-level
                // `ErrorResponse` (415/401/…) lacks it and falls through.
                if let Ok(error) = serde_json::from_slice::<IngestErrorResponse>(&bytes) {
                    return CliError::Ingest {
                        message: error.error,
                        committed: error.committed,
                    };
                }
                match serde_json::from_slice::<ErrorResponse>(&bytes) {
                    Ok(error) => CliError::Api {
                        code: error.code,
                        message: error.message,
                    },
                    Err(_) => CliError::Status { status },
                }
            }
            Err(error) => error,
        }
    }

    /// Extracts and validates the writer redirect target advertised by a
    /// 421 response body.
    fn redirect_target(error: ErrorResponse) -> Result<Url, CliError> {
        let writer = error.writer.ok_or_else(|| {
            CliError::InvalidInput("misdirected response is missing a writer address".into())
        })?;
        let url = Url::parse(&writer)
            .map_err(|error| CliError::InvalidInput(format!("invalid writer address: {error}")))?;
        if !url.has_host() || !matches!(url.scheme(), "http" | "https") {
            return Err(CliError::InvalidInput(
                "writer address must be an absolute http or https URL".into(),
            ));
        }
        Ok(url)
    }

    /// Sends a tx/admin mutation, replaying it exactly once against the
    /// advertised writer on a 421 (misdirected request). A second 421 --
    /// from either hop -- is a `CliError::RedirectLoop`; it is never
    /// followed.
    async fn send_mutation<T: DeserializeOwned>(
        &self,
        path: &str,
        body: &(impl Serialize + Sync),
    ) -> Result<T, CliError> {
        let bytes = serde_json::to_vec(body)?;
        let response = self.post(self.join(path)?, &bytes).await?;
        if response.status() != StatusCode::MISDIRECTED_REQUEST {
            return if response.status().is_success() {
                self.decode_json(response).await
            } else {
                Err(self.error_for(response).await)
            };
        }
        let bytes_for_error = self.read_bounded(response).await?;
        let error: ErrorResponse = serde_json::from_slice(&bytes_for_error)?;
        let writer_base = Self::redirect_target(error)?;
        let writer_url = writer_base
            .join(path)
            .map_err(|error| CliError::InvalidInput(format!("invalid writer path: {error}")))?;
        let retried = self.post(writer_url, &bytes).await?;
        if retried.status() == StatusCode::MISDIRECTED_REQUEST {
            return Err(CliError::RedirectLoop);
        }
        if retried.status().is_success() {
            self.decode_json(retried).await
        } else {
            Err(self.error_for(retried).await)
        }
    }
}

#[async_trait]
impl CommandClient for RemoteClient {
    async fn query(&self, request: QueryRequest) -> Result<Vec<RecordBatch>, CliError> {
        let bytes = serde_json::to_vec(&request)?;
        let response = self
            .http
            .post(self.join("/v1/query")?)
            .bearer_auth(&self.token)
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::ACCEPT, ARROW_STREAM_CONTENT_TYPE)
            .body(bytes)
            .send()
            .await?;
        if !response.status().is_success() {
            return Err(self.error_for(response).await);
        }
        let bytes = self.read_bounded(response).await?;
        let reader = StreamReader::try_new(Cursor::new(bytes), None)?;
        reader
            .collect::<Result<Vec<RecordBatch>, _>>()
            .map_err(CliError::from)
    }

    async fn execute(&self, request: TxRequest) -> Result<TxResponse, CliError> {
        self.send_mutation("/v1/tx", &request).await
    }

    async fn status(&self) -> Result<StatusResponse, CliError> {
        let response = self
            .http
            .get(self.join("/v1/status")?)
            .bearer_auth(&self.token)
            .send()
            .await?;
        if response.status().is_success() {
            self.decode_json(response).await
        } else {
            Err(self.error_for(response).await)
        }
    }

    async fn compact(&self, full: bool) -> Result<CompactionResponse, CliError> {
        self.send_mutation("/v1/admin/compact", &CompactRequest { full })
            .await
    }

    async fn gc(&self) -> Result<GcResponse, CliError> {
        self.send_mutation("/v1/admin/gc", &()).await
    }

    async fn verify(&self) -> Result<VerifyResponse, CliError> {
        self.send_mutation("/v1/admin/verify", &()).await
    }

    /// Streams `body` to `POST /v1/ingest` as one request (reqwest streamed
    /// body — never buffered). Unlike `send_mutation`, a 421 is NOT followed:
    /// the request body is a one-shot stream that cannot be replayed against
    /// the advertised writer, so the caller is told to re-run with `--url`
    /// pointed at the writer (idempotent replay is the retry story).
    async fn ingest(&self, format: BulkFormat, body: BulkBody) -> Result<IngestResponse, CliError> {
        let response = self
            .http
            .post(self.join("/v1/ingest")?)
            .bearer_auth(&self.token)
            .header(header::CONTENT_TYPE, format.content_type())
            .body(reqwest::Body::wrap_stream(body))
            .send()
            .await?;
        if response.status().is_success() {
            return self.decode_json(response).await;
        }
        if response.status() == StatusCode::MISDIRECTED_REQUEST {
            let bytes = self.read_bounded(response).await?;
            let advertised = serde_json::from_slice::<ErrorResponse>(&bytes)
                .ok()
                .and_then(|error| error.writer)
                .map(|writer| format!(" ({writer})"))
                .unwrap_or_default();
            return Err(CliError::InvalidInput(format!(
                "ingest reached a non-writer node; re-run with --url pointing at the writer{advertised}"
            )));
        }
        Err(self.ingest_error_for(response).await)
    }

    async fn snapshot_all(&self) -> Result<(Option<RecordBatch>, Option<RecordBatch>), CliError> {
        Err(CliError::InvalidInput(
            "bulk NDJSON export requires --dir; there is no HTTP export endpoint".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn client() -> RemoteClient {
        RemoteClient::new(
            Url::parse("http://127.0.0.1:1").unwrap_or_else(|error| {
                panic!("test base url must parse: {error}");
            }),
            "token".to_string(),
        )
        .unwrap_or_else(|error| panic!("client must build: {error}"))
    }

    #[test]
    fn zero_max_response_bytes_is_rejected() {
        let client = client().with_max_response_bytes(0);
        assert_eq!(client.max_response_bytes, DEFAULT_MAX_RESPONSE_BYTES);
    }

    #[test]
    fn nonzero_max_response_bytes_is_applied() {
        let client = client().with_max_response_bytes(64);
        assert_eq!(client.max_response_bytes, 64);
    }

    /// Slice 10: a bare 429 (no JSON body needed -- the server's own
    /// `/v1/tx` 429 always carries `Retry-After`, but `error_for` must map
    /// on status alone) becomes `CliError::Backpressure`, with the exact
    /// Display message the brief specifies.
    #[tokio::test]
    async fn a_429_response_maps_to_backpressure() {
        let router = axum::Router::new().route(
            "/always-429",
            axum::routing::get(|| async { StatusCode::TOO_MANY_REQUESTS }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap_or_else(|error| panic!("listener must bind: {error}"));
        let addr = listener
            .local_addr()
            .unwrap_or_else(|error| panic!("addr must resolve: {error}"));
        tokio::spawn(async move {
            axum::serve(listener, router)
                .await
                .unwrap_or_else(|error| panic!("server must serve: {error}"));
        });

        let response = reqwest::get(format!("http://{addr}/always-429"))
            .await
            .unwrap_or_else(|error| panic!("request must succeed: {error}"));
        let error = client().error_for(response).await;
        assert!(
            matches!(error, CliError::Backpressure),
            "expected Backpressure, got {error:?}"
        );
        assert_eq!(
            error.to_string(),
            "server is applying backpressure (429): retry"
        );
    }

    /// Spawns `router` on an ephemeral port and returns a client pointed at it.
    async fn serve(router: axum::Router) -> RemoteClient {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap_or_else(|error| panic!("listener must bind: {error}"));
        let addr = listener
            .local_addr()
            .unwrap_or_else(|error| panic!("addr must resolve: {error}"));
        tokio::spawn(async move {
            axum::serve(listener, router)
                .await
                .unwrap_or_else(|error| panic!("server must serve: {error}"));
        });
        RemoteClient::new(
            Url::parse(&format!("http://{addr}"))
                .unwrap_or_else(|error| panic!("base url must parse: {error}")),
            "token".to_string(),
        )
        .unwrap_or_else(|error| panic!("client must build: {error}"))
    }

    fn body_from(bytes: &'static [u8]) -> BulkBody {
        Box::pin(futures::stream::once(async move {
            Ok(bytes::Bytes::from_static(bytes))
        }))
    }

    /// The streamed body reaches `/v1/ingest` and the `IngestResponse` is
    /// parsed back. The mock counts the NDJSON lines it received, proving the
    /// body was actually transmitted (not dropped).
    #[tokio::test]
    async fn ingest_streams_the_body_and_parses_the_response() {
        use varve_server::api::bulk::IngestResponse;
        let router = axum::Router::new().route(
            "/v1/ingest",
            axum::routing::post(|body: axum::body::Bytes| async move {
                let nodes = body
                    .split(|&b| b == b'\n')
                    .filter(|line| !line.is_empty())
                    .count() as u64;
                axum::Json(IngestResponse {
                    nodes,
                    edges: 0,
                    transactions: 1,
                    basis: 5,
                    system_time: "2024-01-01T00:00:00.000000Z".to_string(),
                    system_time_us: 0,
                })
            }),
        );
        let client = serve(router).await;
        let body = body_from(
            b"{\"type\":\"node\",\"props\":{\"_id\":1}}\n{\"type\":\"node\",\"props\":{\"_id\":2}}\n",
        );
        let response = client
            .ingest(BulkFormat::Ndjson, body)
            .await
            .unwrap_or_else(|error| panic!("ingest must succeed: {error}"));
        assert_eq!(response.nodes, 2);
        assert_eq!(response.basis, 5);
    }

    /// A mid-stream 422 carrying `IngestErrorResponse` maps to
    /// `CliError::Ingest` with the committed counts preserved.
    #[tokio::test]
    async fn ingest_maps_a_committed_progress_error() {
        use varve_server::api::bulk::{IngestErrorResponse, IngestProgress};
        let router = axum::Router::new().route(
            "/v1/ingest",
            axum::routing::post(|_body: axum::body::Bytes| async move {
                (
                    StatusCode::UNPROCESSABLE_ENTITY,
                    axum::Json(IngestErrorResponse {
                        error: "line 3: edge record missing `dst`".to_string(),
                        committed: IngestProgress {
                            nodes: 2,
                            edges: 0,
                            transactions: 1,
                            basis: 9,
                        },
                    }),
                )
            }),
        );
        let client = serve(router).await;
        let error = client
            .ingest(BulkFormat::Ndjson, body_from(b"whatever"))
            .await
            .expect_err("a 422 must surface as an error");
        match error {
            CliError::Ingest { message, committed } => {
                assert!(message.contains("line 3"), "{message}");
                assert_eq!(committed.nodes, 2);
                assert_eq!(committed.basis, 9);
            }
            other => panic!("expected CliError::Ingest, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn snapshot_all_is_rejected_on_the_remote_adapter() {
        let error = client()
            .snapshot_all()
            .await
            .expect_err("remote export must be rejected");
        assert!(
            matches!(error, CliError::InvalidInput(message) if message.contains("--dir")),
            "expected an InvalidInput naming --dir"
        );
    }
}
