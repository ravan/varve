mod encoding;
mod handlers;

use crate::{FrontendContext, Principal, ProtocolFrontend, ServerError, Shutdown};
use async_trait::async_trait;
use axum::{
    extract::{DefaultBodyLimit, Request, State},
    http::{header, HeaderValue},
    middleware::{self, Next},
    response::Response,
    routing::{get, post},
    Router,
};
use serde::Deserialize;
use std::{
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
use url::Url;
use varve::NodeRole;
use varve_config::{BuildContext, ByteSize, ComponentFactory, ConfigSection, RegistryError};

#[derive(Deserialize)]
struct HttpConfig {
    #[serde(default = "default_listen")]
    listen: String,
    advertised_address: Option<String>,
    #[serde(default = "default_max_body_bytes")]
    max_body_bytes: ByteSize,
    tls_cert: Option<PathBuf>,
    tls_key: Option<PathBuf>,
}

fn default_listen() -> String {
    "0.0.0.0:8080".into()
}
fn default_max_body_bytes() -> ByteSize {
    ByteSize::from_bytes(8 * 1024 * 1024)
}

pub(crate) struct HttpFrontendFactory;

impl ComponentFactory<dyn ProtocolFrontend> for HttpFrontendFactory {
    fn name(&self) -> &'static str {
        "http"
    }

    fn build(
        &self,
        cfg: &ConfigSection,
        ctx: &BuildContext,
    ) -> Result<Arc<dyn ProtocolFrontend>, RegistryError> {
        let result = (|| -> Result<HttpFrontend, Box<dyn std::error::Error + Send + Sync>> {
            let db = ctx
                .get::<varve::Db>()
                .ok_or_else(|| std::io::Error::other("HttpFrontend requires Db in BuildContext"))?;
            let config: HttpConfig = cfg
                .child("http")
                .unwrap_or_else(ConfigSection::empty)
                .get()?;
            let listen = config.listen.parse::<SocketAddr>()?;
            if config.tls_cert.is_some() != config.tls_key.is_some() {
                return Err(std::io::Error::other(
                    "tls_cert and tls_key must be configured together",
                )
                .into());
            }
            let advertised_address = if db.roles().contains(NodeRole::Writer) {
                let raw = config.advertised_address.ok_or_else(|| {
                    std::io::Error::other("advertised_address is required on Writer nodes")
                })?;
                let parsed = Url::parse(&raw)?;
                if !matches!(parsed.scheme(), "http" | "https") || !parsed.has_host() {
                    return Err(std::io::Error::other(
                        "advertised_address must be an absolute http or https URL",
                    )
                    .into());
                }
                Some(raw)
            } else {
                None
            };
            Ok(HttpFrontend {
                listen,
                advertised_address,
                max_body_bytes: config.max_body_bytes.as_usize(),
                tls_cert: config.tls_cert,
                tls_key: config.tls_key,
            })
        })();
        result
            .map(|frontend| Arc::new(frontend) as Arc<dyn ProtocolFrontend>)
            .map_err(|source| RegistryError::Build {
                kind: "protocol-frontend",
                name: "http".into(),
                source,
            })
    }
}

pub struct HttpFrontend {
    listen: SocketAddr,
    advertised_address: Option<String>,
    max_body_bytes: usize,
    tls_cert: Option<PathBuf>,
    tls_key: Option<PathBuf>,
}

#[async_trait]
impl ProtocolFrontend for HttpFrontend {
    async fn serve(
        &self,
        context: FrontendContext,
        mut shutdown: Shutdown,
    ) -> Result<(), ServerError> {
        if let Some(address) = &self.advertised_address {
            context.db.publish_writer(address).await?;
        }
        let router = http_router(HttpContext {
            frontend: context.clone(),
            max_body_bytes: self.max_body_bytes,
        });
        let handle = axum_server::Handle::<SocketAddr>::new();
        let listening = handle.clone();
        let readiness = context.readiness.clone();
        let ready_task = tokio::spawn(async move {
            if let Some(address) = listening.listening().await {
                readiness.listening(address.to_string());
            }
        });
        let shutdown_handle = handle.clone();
        let shutdown_task = tokio::spawn(async move {
            shutdown.cancelled().await;
            shutdown_handle.graceful_shutdown(Some(Duration::from_secs(10)));
        });
        #[cfg(feature = "tls")]
        let result = if let (Some(cert), Some(key)) = (&self.tls_cert, &self.tls_key) {
            // axum-server is built with `tls-rustls-no-provider`, so rustls has no process
            // default crypto provider installed automatically. Install `ring` explicitly
            // (rather than relying on it being reachable transitively through some other
            // dependency's feature selection) so the TLS handshake below is deterministic
            // regardless of the rest of the dependency graph. `install_default` errors only
            // if a provider is already installed, which is fine to ignore here.
            let _ = rustls::crypto::ring::default_provider().install_default();
            let tls = axum_server::tls_rustls::RustlsConfig::from_pem_file(cert, key)
                .await
                .map_err(ServerError::Io)?;
            axum_server::bind_rustls(self.listen, tls)
                .handle(handle)
                .serve(router.into_make_service())
                .await
        } else {
            axum_server::bind(self.listen)
                .handle(handle)
                .serve(router.into_make_service())
                .await
        };
        #[cfg(not(feature = "tls"))]
        let result = {
            if self.tls_cert.is_some() {
                return Err(ServerError::Protocol(
                    "TLS support is not compiled in".into(),
                ));
            }
            axum_server::bind(self.listen)
                .handle(handle)
                .serve(router.into_make_service())
                .await
        };
        ready_task.abort();
        shutdown_task.abort();
        result.map_err(ServerError::Io)
    }
}

#[derive(Clone)]
pub struct HttpContext {
    pub frontend: FrontendContext,
    pub max_body_bytes: usize,
}

pub fn http_router(context: HttpContext) -> Router {
    let public = Router::new()
        .route("/healthz", get(handlers::health))
        .route_layer(middleware::from_fn_with_state(
            context.clone(),
            observe_health,
        ));
    let protected = Router::new()
        .route("/v1/query", post(handlers::query))
        .route("/v1/tx", post(handlers::tx))
        .route("/v1/status", get(handlers::status))
        .route("/metrics", get(handlers::metrics))
        .route("/v1/admin/compact", post(handlers::compact))
        .route("/v1/admin/gc", post(handlers::gc))
        .route("/v1/admin/verify", post(handlers::verify))
        .layer(DefaultBodyLimit::max(context.max_body_bytes))
        .route_layer(middleware::from_fn_with_state(
            context.clone(),
            authenticate,
        ));
    Router::new()
        .merge(public)
        .merge(protected)
        .layer(middleware::from_fn(nosniff))
        .with_state(context)
}

async fn observe_health(
    State(context): State<HttpContext>,
    request: Request,
    next: Next,
) -> Response {
    let started = Instant::now();
    let response = next.run(request).await;
    context.frontend.metrics.observe_request(
        "GET",
        "/healthz",
        response.status().as_u16(),
        started.elapsed(),
    );
    response
}

async fn authenticate(
    State(context): State<HttpContext>,
    mut request: Request,
    next: Next,
) -> Response {
    let started = Instant::now();
    let method = static_method(request.method());
    let route = static_route(request.uri().path());
    let values = request
        .headers()
        .get_all(header::AUTHORIZATION)
        .iter()
        .collect::<Vec<_>>();
    let bearer = if values.len() == 1 {
        values[0]
            .to_str()
            .ok()
            .and_then(|v| v.strip_prefix("Bearer "))
    } else {
        None
    };
    let principal = match context.frontend.authenticator.authenticate(bearer) {
        Ok(v) => v,
        Err(_) => {
            let response = handlers::unauthorized();
            context.frontend.metrics.observe_request(
                method,
                route,
                response.status().as_u16(),
                started.elapsed(),
            );
            return response;
        }
    };
    request.extensions_mut().insert::<Principal>(principal);
    let response = next.run(request).await;
    context.frontend.metrics.observe_request(
        method,
        route,
        response.status().as_u16(),
        started.elapsed(),
    );
    response
}
fn static_method(method: &axum::http::Method) -> &'static str {
    match *method {
        axum::http::Method::GET => "GET",
        axum::http::Method::POST => "POST",
        _ => "OTHER",
    }
}
fn static_route(path: &str) -> &'static str {
    match path {
        "/v1/query" => "/v1/query",
        "/v1/tx" => "/v1/tx",
        "/v1/status" => "/v1/status",
        "/metrics" => "/metrics",
        "/v1/admin/compact" => "/v1/admin/compact",
        "/v1/admin/gc" => "/v1/admin/gc",
        "/v1/admin/verify" => "/v1/admin/verify",
        _ => "unknown",
    }
}
async fn nosniff(request: Request, next: Next) -> Response {
    let mut r = next.run(request).await;
    r.headers_mut().insert(
        "x-content-type-options",
        HeaderValue::from_static("nosniff"),
    );
    r
}
