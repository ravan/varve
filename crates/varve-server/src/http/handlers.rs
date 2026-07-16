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
    Extension(p): Extension<Principal>,
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
    // The authenticated principal always rides along; with `[security]`
    // disabled it is inert (attribution only), with it enabled the engine
    // enforces the subject's grants (deny-by-default).
    let mut q = c
        .frontend
        .db
        .query(r.gql)
        .params(params)
        .as_principal(p.subject);
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
    match c
        .frontend
        .db
        .try_execute_as(&r.gql, &params, &p.subject)
        .await
    {
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
            // Task 12 (spec §12, decision 10): I/O-free — atomics plus one
            // in-memory read-lock pass, no additional object-store I/O
            // beyond the pre-existing `db.status()` call above.
            c.frontend.metrics.set_engine(&c.frontend.db.metrics());
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
        Ok(Some(a)) if !a.address.is_empty() => error(
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
        ServerError::Engine(EngineError::Backpressure) => {
            let mut r = error(
                StatusCode::TOO_MANY_REQUESTS,
                "backpressure",
                "writer submission queue is full; retry",
                None,
            );
            r.headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
            r
        }
        ServerError::Engine(EngineError::WriterFenced(_)) => error(
            StatusCode::SERVICE_UNAVAILABLE,
            "writer_fenced",
            "writer fenced",
            None,
        ),
        ServerError::Engine(EngineError::AccessDenied(_)) => error(
            StatusCode::FORBIDDEN,
            "forbidden",
            "access denied",
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Slice 10's committed contract for the `/v1/tx` 429 path: this is a
    /// direct, deterministic test of `mapped()` — no writer saturation, no
    /// concurrency, no flake risk — asserting the exact status code and
    /// `Retry-After` header the brief requires.
    #[test]
    fn backpressure_maps_to_429_with_retry_after_one_second() {
        let response = mapped(ServerError::Engine(EngineError::Backpressure));
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            response
                .headers()
                .get(header::RETRY_AFTER)
                .expect("Retry-After header must be present"),
            "1"
        );
    }

    /// Slice 10: `WriterFenced` reports 503 with a distinct `writer_fenced`
    /// code — distinguishing "the fence moved on" from a full queue.
    #[test]
    fn writer_fenced_maps_to_503() {
        let response = mapped(ServerError::Engine(EngineError::WriterFenced(
            "stale epoch".into(),
        )));
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    /// ReBAC enforcement: a denied read or write maps to 403 `forbidden`,
    /// clearly distinct from 401 (authentication) and 400 (malformed).
    #[test]
    fn access_denied_maps_to_403_forbidden() {
        let response = mapped(ServerError::Engine(EngineError::AccessDenied(
            "user 'ada' lacks READ".into(),
        )));
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }
}
