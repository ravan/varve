use axum::{
    body::Body,
    http::{header::CONTENT_TYPE, Response, StatusCode},
};
use bytes::Bytes;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::StreamExt;
use std::{
    io::{self, Write},
    sync::{Arc, Mutex},
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::{api::ARROW_STREAM_CONTENT_TYPE, ServerError};

#[derive(Clone)]
struct SharedBuffer(Arc<Mutex<Vec<u8>>>);

impl Write for SharedBuffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0
            .lock()
            .map_err(|_| io::Error::other("Arrow buffer lock poisoned"))?
            .extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn drain(buffer: &SharedBuffer) -> io::Result<Bytes> {
    let mut bytes = buffer
        .0
        .lock()
        .map_err(|_| io::Error::other("Arrow buffer lock poisoned"))?;
    Ok(Bytes::from(std::mem::take(&mut *bytes)))
}

pub(super) fn arrow_ipc_response(
    mut stream: SendableRecordBatchStream,
) -> Result<Response<Body>, ServerError> {
    let buffer = SharedBuffer(Arc::new(Mutex::new(Vec::new())));
    let mut writer =
        arrow::ipc::writer::StreamWriter::try_new(buffer.clone(), stream.schema().as_ref())
            .map_err(|error| ServerError::Protocol(error.to_string()))?;
    let schema = drain(&buffer)?;
    let (sender, receiver) = mpsc::channel::<Result<Bytes, io::Error>>(2);
    tokio::spawn(async move {
        if sender.send(Ok(schema)).await.is_err() {
            return;
        }
        while let Some(result) = stream.next().await {
            let batch = match result {
                Ok(batch) => batch,
                Err(_) => {
                    let _ = sender
                        .send(Err(io::Error::other("Arrow stream execution failed")))
                        .await;
                    return;
                }
            };
            if writer.write(&batch).is_err() {
                let _ = sender
                    .send(Err(io::Error::other("Arrow stream encoding failed")))
                    .await;
                return;
            }
            match drain(&buffer) {
                Ok(bytes) => {
                    if sender.send(Ok(bytes)).await.is_err() {
                        return;
                    }
                }
                Err(error) => {
                    let _ = sender.send(Err(error)).await;
                    return;
                }
            }
        }
        if writer.finish().is_err() {
            let _ = sender
                .send(Err(io::Error::other("Arrow stream encoding failed")))
                .await;
            return;
        }
        match drain(&buffer) {
            Ok(bytes) => {
                let _ = sender.send(Ok(bytes)).await;
            }
            Err(error) => {
                let _ = sender.send(Err(error)).await;
            }
        }
    });
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, ARROW_STREAM_CONTENT_TYPE)
        .body(Body::from_stream(ReceiverStream::new(receiver)))
        .map_err(ServerError::from)
}

#[cfg(test)]
mod tests {
    use super::arrow_ipc_response;
    use arrow::datatypes::Schema;
    use datafusion::{error::DataFusionError, physical_plan::stream::RecordBatchStreamAdapter};
    use http_body_util::BodyExt;
    use std::sync::Arc;

    #[tokio::test]
    async fn post_header_execution_errors_do_not_expose_secrets() {
        let stream = futures::stream::once(async {
            Err(DataFusionError::Execution(
                "secret-storage-credential".into(),
            ))
        });
        let response = arrow_ipc_response(Box::pin(RecordBatchStreamAdapter::new(
            Arc::new(Schema::empty()),
            stream,
        )))
        .unwrap();
        let error = response
            .into_body()
            .collect()
            .await
            .unwrap_err()
            .to_string();
        assert!(!error.contains("secret-storage-credential"), "{error}");
        assert!(error.contains("Arrow stream execution failed"), "{error}");
    }
}
