use std::io::Cursor;

use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use reqwest::{header, Client, Response, StatusCode};
use serde::{de::DeserializeOwned, Serialize};
use url::Url;
use varve_server::api::{
    CompactionResponse, ErrorResponse, GcResponse, QueryRequest, StatusResponse, TxRequest,
    TxResponse, VerifyResponse, ARROW_STREAM_CONTENT_TYPE,
};

use crate::client::{CliError, CommandClient};

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

    /// Turns a non-2xx, non-421 response into a `CliError`: a structured
    /// `ErrorResponse` becomes `CliError::Api`, anything else becomes
    /// `CliError::Status`. Never surfaces response headers.
    async fn error_for(&self, response: Response) -> CliError {
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

    async fn compact(&self) -> Result<CompactionResponse, CliError> {
        self.send_mutation("/v1/admin/compact", &()).await
    }

    async fn gc(&self) -> Result<GcResponse, CliError> {
        self.send_mutation("/v1/admin/gc", &()).await
    }

    async fn verify(&self) -> Result<VerifyResponse, CliError> {
        self.send_mutation("/v1/admin/verify", &()).await
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
}
