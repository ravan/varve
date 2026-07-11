use crate::{Authenticator, MetricsSink, ServerError};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::watch;
use varve::{Db, ProbeReport};

#[async_trait]
pub trait ProtocolFrontend: Send + Sync {
    async fn serve(&self, context: FrontendContext, shutdown: Shutdown) -> Result<(), ServerError>;
}

#[derive(Clone)]
pub struct FrontendContext {
    pub db: Db,
    pub authenticator: Arc<dyn Authenticator>,
    pub metrics: Arc<dyn MetricsSink>,
    pub probe: ProbeReport,
    pub readiness: ReadinessReporter,
}

#[derive(Clone)]
pub struct ReadinessReporter(watch::Sender<Option<String>>);

pub struct Readiness(watch::Receiver<Option<String>>);

pub fn readiness_channel() -> (ReadinessReporter, Readiness) {
    let (sender, receiver) = watch::channel(None);
    (ReadinessReporter(sender), Readiness(receiver))
}

impl ReadinessReporter {
    pub fn listening(&self, endpoint: String) {
        self.0.send_if_modified(|reported| {
            if reported.is_some() {
                return false;
            }
            *reported = Some(endpoint);
            true
        });
    }
}

impl Readiness {
    pub async fn wait(&mut self) -> Result<String, ServerError> {
        loop {
            if let Some(endpoint) = self.0.borrow().clone() {
                return Ok(endpoint);
            }
            self.0.changed().await.map_err(|_| {
                ServerError::Protocol("serving exited before reporting readiness".into())
            })?;
        }
    }
}

#[derive(Clone)]
pub struct Shutdown(watch::Receiver<bool>);

#[derive(Clone)]
pub struct ShutdownTrigger(watch::Sender<bool>);

impl Shutdown {
    pub fn channel() -> (ShutdownTrigger, Self) {
        let (sender, receiver) = watch::channel(false);
        (ShutdownTrigger(sender), Self(receiver))
    }

    pub async fn cancelled(&mut self) {
        while !*self.0.borrow() {
            if self.0.changed().await.is_err() {
                break;
            }
        }
    }
}

impl ShutdownTrigger {
    pub fn shutdown(&self) {
        self.0.send_replace(true);
    }
}

#[cfg(test)]
mod tests {
    use super::{readiness_channel, Shutdown};
    use crate::ServerRegistries;

    #[test]
    #[cfg(feature = "http")]
    fn builtin_frontend_registry_includes_http() {
        let registries = ServerRegistries::with_builtins()
            .unwrap_or_else(|error| panic!("builtin registries must construct: {error}"));
        assert_eq!(registries.frontend.names(), vec!["http"]);
        assert_eq!(registries.authenticator.names(), vec!["static"]);
        assert_eq!(registries.metrics.names(), vec!["prometheus"]);
    }

    #[test]
    #[cfg(not(feature = "http"))]
    fn builtin_frontend_registry_is_empty_without_http_feature() {
        let registries = ServerRegistries::with_builtins()
            .unwrap_or_else(|error| panic!("builtin registries must construct: {error}"));
        assert!(registries.frontend.names().is_empty());
    }

    #[tokio::test]
    async fn readiness_reports_the_first_endpoint_exactly_once() {
        let (reporter, mut readiness) = readiness_channel();
        reporter.listening("127.0.0.1:1000".into());
        reporter.listening("127.0.0.1:2000".into());
        assert_eq!(
            readiness
                .wait()
                .await
                .unwrap_or_else(|error| panic!("readiness must be reported: {error}")),
            "127.0.0.1:1000"
        );
    }

    #[tokio::test]
    async fn shutdown_wakes_every_clone() {
        let (trigger, mut first) = Shutdown::channel();
        let mut second = first.clone();
        trigger.shutdown();
        tokio::join!(first.cancelled(), second.cancelled());
    }

    #[tokio::test]
    async fn readiness_errors_if_reporter_exits_first() {
        let (reporter, mut readiness) = readiness_channel();
        drop(reporter);
        assert!(readiness.wait().await.is_err());
    }
}
