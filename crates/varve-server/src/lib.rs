pub mod api;
pub mod auth;
pub mod error;
pub mod frontend;
#[cfg(feature = "http")]
pub mod http;
pub mod metrics;

pub use auth::{static_auth, AuthError, Authenticator, Principal};
pub use error::ServerError;
pub use frontend::{
    readiness_channel, FrontendContext, ProtocolFrontend, Readiness, ReadinessReporter, Shutdown,
    ShutdownTrigger,
};
// The bulk-ingest defaults live in `api::bulk` (ungated) so the CLI's embedded
// import can read them without `http`; re-exported here for existing callers
// (e.g. the generated config reference's `varve_server::DEFAULT_CHUNK_OPS`).
pub use api::bulk::{DEFAULT_CHUNK_OPS, DEFAULT_MAX_LINE_BYTES};
#[cfg(feature = "http")]
pub use http::{http_router, HttpContext, HttpFrontend, IngestConfig, DEFAULT_MAX_BODY_BYTES};
#[cfg(feature = "otel")]
pub use metrics::OtlpMetrics;
pub use metrics::{MetricsSink, PrometheusMetrics};

use varve_config::{Registry, RegistryError};

pub struct ServerRegistries {
    pub frontend: Registry<dyn ProtocolFrontend>,
    pub authenticator: Registry<dyn Authenticator>,
    pub metrics: Registry<dyn MetricsSink>,
}

impl ServerRegistries {
    pub fn with_builtins() -> Result<Self, RegistryError> {
        #[cfg_attr(not(feature = "http"), allow(unused_mut))]
        let mut frontend = Registry::new("protocol-frontend");
        #[cfg(feature = "http")]
        frontend.register(Box::new(http::HttpFrontendFactory))?;
        let mut authenticator = Registry::new("authenticator");
        authenticator.register(Box::new(auth::StaticAuthFactory))?;
        #[cfg(feature = "oidc")]
        authenticator.register(Box::new(auth::oidc::OidcAuthFactory))?;
        let mut metrics = Registry::new("metrics");
        metrics.register(Box::new(metrics::PrometheusMetricsFactory))?;
        #[cfg(feature = "otel")]
        metrics.register(Box::new(metrics::OtlpMetricsFactory))?;
        Ok(Self {
            frontend,
            authenticator,
            metrics,
        })
    }
}
