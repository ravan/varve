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
#[cfg(feature = "http")]
pub use http::{http_router, HttpContext, HttpFrontend};
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
