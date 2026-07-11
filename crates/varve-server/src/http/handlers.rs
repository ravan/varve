use super::{encoding::arrow_ipc_response, HttpContext};
use crate::{api::*, Principal, ServerError};
use axum::{
    extract::{Extension, State},
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use futures::TryStreamExt;
use std::time::Duration;
use varve_engine::{BasisToken, EngineError, NodeRole};

pub(super) async fn health(State(c): State<HttpContext>) -> Response {
    // I/O-free: `Db::follower_error` reads only the in-memory progress
    // watch, so this public/unauthenticated route never performs
    // object-store I/O (unlike the authenticated `/v1/status`, which still
    // calls `Db::status`).
    match c.frontend.db.follower_error() {
        None => Json(serde_json::json!({"status":"ok"})).into_response(),
        Some(_) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"status":"degraded","error":"follower stopped"})),
        )
            .into_response(),
    }
}
pub(super) fn unauthorized() -> Response {
    let mut r = error(
        StatusCode::UNAUTHORIZED,
        "unauthorized",
        "authentication required",
        None,
    );
    r.headers_mut()
        .insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
    r
}
pub(super) async fn query(
    State(c): State<HttpContext>,
    headers: HeaderMap,
    Json(r): Json<QueryRequest>,
) -> Response {
    let arrow = match accept(headers.get(header::ACCEPT).and_then(|v| v.to_str().ok())) {
        Ok(v) => v,
        Err(e) => return mapped(e),
    };
    let params = match params_from_json(&r.params) {
        Ok(v) => v,
        Err(e) => return mapped(e),
    };
    let mut q = c.frontend.db.query(r.gql).params(params);
    if let Some(b) = r.basis {
        match BasisToken::try_from(b) {
            Ok(v) => q = q.basis(v),
            Err(e) => return mapped(e),
        }
    }
    if let Some(ms) = r.basis_timeout_ms {
        q = q.basis_timeout(Duration::from_millis(ms))
    }
    let stream = match q.stream().await {
        Ok(v) => v,
        Err(e) => return mapped(e.into()),
    };
    if arrow {
        return match arrow_ipc_response(stream) {
            Ok(v) => v,
            Err(e) => mapped(e),
        };
    }
    match stream.try_collect::<Vec<_>>().await {
        Ok(b) => match batches_to_json(&b) {
            Ok(v) => Json(v).into_response(),
            Err(e) => mapped(e),
        },
        Err(_) => mapped(ServerError::Protocol("query execution failed".into())),
    }
}
pub(super) async fn tx(
    State(c): State<HttpContext>,
    Extension(p): Extension<Principal>,
    Json(r): Json<TxRequest>,
) -> Response {
    if !c.frontend.db.roles().contains(NodeRole::Writer) {
        return redirect(&c).await;
    }
    let params = match params_from_json(&r.params) {
        Ok(v) => v,
        Err(e) => return mapped(e),
    };
    match c.frontend.db.execute_as(&r.gql, &params, &p.subject).await {
        Ok(v) => Json(TxResponse::from_receipt(&v)).into_response(),
        Err(e) => mapped(e.into()),
    }
}
pub(super) async fn status(State(c): State<HttpContext>) -> Response {
    match c.frontend.db.status().await {
        Ok(v) => Json(StatusResponse::from_engine(&v, &c.frontend.probe)).into_response(),
        Err(e) => mapped(e.into()),
    }
}
pub(super) async fn metrics(State(c): State<HttpContext>) -> Response {
    match c.frontend.db.status().await {
        Ok(v) => {
            c.frontend.metrics.set_progress(&v);
            match c.frontend.metrics.encode() {
                Ok(v) => ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], v).into_response(),
                Err(e) => mapped(e),
            }
        }
        Err(e) => mapped(e.into()),
    }
}
pub(super) async fn compact(State(c): State<HttpContext>) -> Response {
    if !c.frontend.db.roles().contains(NodeRole::Compactor) {
        return redirect(&c).await;
    }
    match c.frontend.db.compact_once().await {
        Ok(v) => Json(CompactionResponse::from_report(&v)).into_response(),
        Err(e) => mapped(e.into()),
    }
}
pub(super) async fn gc(State(c): State<HttpContext>) -> Response {
    if !c.frontend.db.roles().contains(NodeRole::Compactor) {
        return redirect(&c).await;
    }
    match c.frontend.db.gc_once().await {
        Ok(v) => Json(GcResponse::from_report(&v)).into_response(),
        Err(e) => mapped(e.into()),
    }
}
pub(super) async fn verify(State(c): State<HttpContext>) -> Response {
    match c.frontend.db.verify().await {
        Ok(v) => Json(VerifyResponse::from_report(&v)).into_response(),
        Err(e) => mapped(e.into()),
    }
}
fn accept(v: Option<&str>) -> Result<bool, ServerError> {
    let Some(value) = v else { return Ok(false) };
    let mut json = false;
    for media_type in value
        .split(',')
        .map(|item| item.split(';').next().unwrap_or("").trim())
    {
        match media_type {
            ARROW_STREAM_CONTENT_TYPE => return Ok(true),
            "*/*" | "application/json" => json = true,
            _ => {}
        }
    }
    if json {
        Ok(false)
    } else {
        Err(ServerError::NotAcceptable(
            "unsupported Accept media type".into(),
        ))
    }
}
async fn redirect(c: &HttpContext) -> Response {
    match c.frontend.db.writer_advertisement().await {
        Ok(Some(a)) => error(
            StatusCode::MISDIRECTED_REQUEST,
            "misdirected_request",
            "request must be sent to writer",
            Some(a.address),
        ),
        _ => error(
            StatusCode::SERVICE_UNAVAILABLE,
            "writer_unavailable",
            "writer is unavailable",
            None,
        ),
    }
}
fn mapped(e: ServerError) -> Response {
    match e {
        ServerError::InvalidRequest(_)
        | ServerError::Base64(_)
        | ServerError::Engine(
            EngineError::Gql(_)
            | EngineError::Type(_)
            | EngineError::NotAMutation
            | EngineError::NotAQuery,
        ) => error(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "invalid request",
            None,
        ),
        ServerError::NotAcceptable(_) => error(
            StatusCode::NOT_ACCEPTABLE,
            "not_acceptable",
            "requested representation is unavailable",
            None,
        ),
        ServerError::Engine(EngineError::BasisTimeout { .. }) => error(
            StatusCode::REQUEST_TIMEOUT,
            "basis_timeout",
            "basis was not reached before timeout",
            None,
        ),
        ServerError::Engine(EngineError::FollowerFailed(_)) => error(
            StatusCode::SERVICE_UNAVAILABLE,
            "follower_failed",
            "follower stopped",
            None,
        ),
        _ => error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
            "internal server error",
            None,
        ),
    }
}
fn error(status: StatusCode, code: &str, message: &str, writer: Option<String>) -> Response {
    (
        status,
        Json(ErrorResponse {
            code: code.into(),
            message: message.into(),
            writer,
        }),
    )
        .into_response()
}
