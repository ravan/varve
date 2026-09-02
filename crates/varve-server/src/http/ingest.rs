//! `POST /v1/ingest` — the bulk-ingest fast path over HTTP (roadmap slices
//! BI-1/BI-2). NDJSON in the request body decodes straight to engine data ops
//! and commits through [`varve::Db::ingest`] — no GQL parse or plan anywhere.
//!
//! BI-2 streams the body: frames feed an incremental [`NdjsonFramer`] and
//! decoded ops commit in `[ingest] chunk_ops`-sized chunks as they arrive, so
//! the server holds only bounded per-line + one-chunk memory regardless of
//! stream size. Each chunk is one atomic `Db::ingest` transaction; the stream
//! as a whole is NOT atomic — earlier chunks stay committed if a later chunk
//! (or a decode error) fails, and the error body reports that committed
//! progress. Replaying an interrupted stream from the start is safe (puts are
//! id/endpoint-keyed upserts).

use super::{handlers, HttpContext};
use crate::{
    api::bulk::{
        csv::CsvFramer, BulkOp, Framer, IngestErrorResponse, IngestProgress, IngestResponse,
        NdjsonFramer,
    },
    Principal,
};
use axum::{
    body::Body,
    extract::{Extension, Query, State},
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use futures::StreamExt;
use serde::Deserialize;
use std::time::Instant;
use tracing::Instrument;
use varve_engine::{EdgePut, EngineError, NodePut, NodeRole, TxReceipt};

/// NDJSON media type (roadmap wire spec).
const NDJSON_CONTENT_TYPE: &str = "application/x-ndjson";
/// CSV media type (Neo4j-dialect, BI-3).
const CSV_CONTENT_TYPE: &str = "text/csv";

/// `?graph=<name>` selects the target graph (Silt-0a). The body is a
/// stream of NDJSON or CSV, so it has no JSON envelope to hold a field; a
/// query parameter keeps the URL the single address of the target and works
/// with `curl --data-binary @file`. Absent ⇒ the default graph.
#[derive(Debug, Default, Deserialize)]
pub(super) struct IngestQuery {
    #[serde(default)]
    graph: Option<String>,
}

pub(super) async fn ingest(
    State(c): State<HttpContext>,
    Extension(p): Extension<Principal>,
    Query(target): Query<IngestQuery>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    async move {
        // 1. Content negotiation selects the framer: NDJSON or Neo4j-CSV.
        let max_line_bytes = c.ingest.max_line_bytes.as_usize();
        let Some(mut framer) = select_framer(headers.get(header::CONTENT_TYPE), max_line_bytes)
        else {
            return handlers::error(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "unsupported_media_type",
                "Content-Type must be application/x-ndjson or text/csv",
                None,
            );
        };
        // 2. Writer-only, exactly like `/v1/tx`: redirect a query node to the
        //    advertised writer (421) or report the writer unavailable (503).
        if !c.frontend.db.roles().contains(NodeRole::Writer) {
            return handlers::redirect(&c).await;
        }
        // 3. Resolve the target graph before reading a byte of the body: a
        //    reserved name is a 400, an unknown one a 404 with nothing
        //    committed.
        let graph = target
            .graph
            .unwrap_or_else(|| varve_engine::DEFAULT_GRAPH.to_string());
        if let Some(rejected) = handlers::reserved_graph(Some(&graph)) {
            return rejected;
        }
        match c.frontend.db.graph_exists(&graph) {
            Ok(true) => {}
            Ok(false) => {
                return committed_error(
                    StatusCode::NOT_FOUND,
                    EngineError::UnknownGraph(graph).to_string(),
                    IngestProgress::default(),
                )
            }
            Err(error) => {
                let (status, message) = classify_chunk_error(error);
                return committed_error(status, message, IngestProgress::default());
            }
        }
        // 4. Stream the body: frames feed the incremental framer; decoded ops
        //    commit in `chunk_ops`-sized chunks (via `ingest_in_as`, so the
        //    submitter's write grants on the target graph are enforced under
        //    `[security]` exactly as a GQL INSERT would be — a denied
        //    label/type → 403). Constant server memory (one line + one
        //    chunk), never the whole payload.
        let started = Instant::now();
        let chunk_ops = c.ingest.chunk_ops;
        let mut pending: Vec<BulkOp> = Vec::new();
        let mut progress = IngestProgress::default();
        let mut bytes: u64 = 0;
        let mut last: Option<TxReceipt> = None;
        let mut stream = body.into_data_stream();

        while let Some(frame) = stream.next().await {
            let frame = match frame {
                Ok(frame) => frame,
                // Client disconnect / transport error mid-stream: committed
                // chunks stay committed; report progress. The client is
                // typically gone, but the contract is idempotent retry.
                Err(_) => {
                    return committed_error(
                        StatusCode::BAD_REQUEST,
                        "request body stream ended before completion".into(),
                        progress,
                    )
                }
            };
            bytes += frame.len() as u64;
            let ops = match framer.push(&frame) {
                Ok(ops) => ops,
                Err(error) => {
                    return committed_error(
                        StatusCode::UNPROCESSABLE_ENTITY,
                        error.to_string(),
                        progress,
                    )
                }
            };
            pending.extend(ops);
            while pending.len() >= chunk_ops {
                let chunk: Vec<BulkOp> = pending.drain(..chunk_ops).collect();
                if let Err(response) =
                    commit(&c, &graph, &p.subject, chunk, &mut progress, &mut last).await
                {
                    return response;
                }
            }
        }
        // Flush the trailing (un-newlined) line, then any partial final chunk.
        match framer.finish() {
            Ok(ops) => pending.extend(ops),
            Err(error) => {
                return committed_error(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    error.to_string(),
                    progress,
                )
            }
        }
        while !pending.is_empty() {
            let take = pending.len().min(chunk_ops);
            let chunk: Vec<BulkOp> = pending.drain(..take).collect();
            if let Err(response) =
                commit(&c, &graph, &p.subject, chunk, &mut progress, &mut last).await
            {
                return response;
            }
        }

        match last {
            Some(last) => {
                c.frontend.metrics.observe_ingest(
                    progress.nodes + progress.edges,
                    bytes,
                    progress.transactions,
                    started.elapsed(),
                );
                Json(IngestResponse::from_committed(&progress, &last)).into_response()
            }
            // No record ever decoded: an empty (or blank-only) stream. Reject
            // it — `Db::ingest` rejects empty batches too.
            None => committed_error(
                StatusCode::UNPROCESSABLE_ENTITY,
                "request contained no records".into(),
                progress,
            ),
        }
    }
    .instrument(tracing::info_span!("varve.http.ingest"))
    .await
}

/// Commits one non-empty chunk as an atomic `Db::ingest_in_as` transaction
/// on `graph`, attributed to `user` (write grants enforced under
/// `[security]`), folding the receipt into `progress`/`last`. On engine
/// failure returns the committed-progress error `Response` for the caller
/// to return immediately.
async fn commit(
    c: &HttpContext,
    graph: &str,
    user: &str,
    chunk: Vec<BulkOp>,
    progress: &mut IngestProgress,
    last: &mut Option<TxReceipt>,
) -> Result<(), Response> {
    let mut nodes: Vec<NodePut> = Vec::new();
    let mut edges: Vec<EdgePut> = Vec::new();
    for op in chunk {
        match op {
            BulkOp::Node(node) => nodes.push(node),
            BulkOp::Edge(edge) => edges.push(edge),
        }
    }
    match c.frontend.db.ingest_in_as(graph, user, nodes, edges).await {
        Ok(receipt) => {
            progress.absorb(&receipt);
            *last = Some(receipt);
            Ok(())
        }
        Err(error) => {
            let (status, message) = classify_chunk_error(error);
            Err(committed_error(status, message, progress.clone()))
        }
    }
}

/// Selects the framer from the `Content-Type` essence (case-insensitive,
/// media-type parameters ignored): `application/x-ndjson` → NDJSON,
/// `text/csv` → Neo4j-dialect CSV, anything else → `None` (415).
fn select_framer(value: Option<&HeaderValue>, max_line_bytes: usize) -> Option<Framer> {
    let essence = value
        .and_then(|value| value.to_str().ok())
        .map(|value| value.split(';').next().unwrap_or("").trim())?;
    if essence.eq_ignore_ascii_case(NDJSON_CONTENT_TYPE) {
        Some(Framer::Ndjson(NdjsonFramer::new(max_line_bytes)))
    } else if essence.eq_ignore_ascii_case(CSV_CONTENT_TYPE) {
        Some(Framer::Csv(Box::new(CsvFramer::new(max_line_bytes))))
    } else {
        None
    }
}

fn committed_error(status: StatusCode, message: String, committed: IngestProgress) -> Response {
    (
        status,
        Json(IngestErrorResponse {
            error: message,
            committed,
        }),
    )
        .into_response()
}

/// Classifies a mid-stream chunk failure into a status + client-safe message,
/// mirroring the `/v1/tx` mapping. A statement-level engine error (safe to
/// echo) becomes a 422; a fenced writer a 503; anything else an opaque 500
/// with the real cause logged for operators — the message may embed storage
/// credentials or user-supplied identifiers and must not reach the client.
fn classify_chunk_error(error: EngineError) -> (StatusCode, String) {
    if let Some(message) = error.client_query_error() {
        return (StatusCode::UNPROCESSABLE_ENTITY, message);
    }
    match error {
        // A denied write grant (BI-4): the label/edge-type the submitter
        // named is safe to echo — it is their own request.
        EngineError::AccessDenied(message) => (StatusCode::FORBIDDEN, message),
        // The target graph vanished mid-stream (or never existed): the name
        // is the caller's own request input.
        unknown @ EngineError::UnknownGraph(_) => (StatusCode::NOT_FOUND, unknown.to_string()),
        EngineError::WriterFenced(_) => (StatusCode::SERVICE_UNAVAILABLE, "writer fenced".into()),
        EngineError::Backpressure => (
            StatusCode::TOO_MANY_REQUESTS,
            "writer submission queue is full; retry".into(),
        ),
        other => {
            tracing::error!(error = %other, "ingest chunk failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal server error".into(),
            )
        }
    }
}
