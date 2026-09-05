#![allow(clippy::unwrap_used)]
//! Assembly validates declared capabilities, independently of factory names.
use std::sync::Arc;
use varve_config::{ComponentFactory, Config, ConfigSection, RegistryError};
use varve_engine::{Db, EngineError, Registries};
use varve_log::{Log, LogDependencies};
use varve_storage::ObjectStore;

struct NamedLog(Arc<dyn Log>);
impl ComponentFactory<dyn Log, LogDependencies> for NamedLog {
    fn name(&self) -> &'static str {
        "custom-log"
    }
    fn build(&self, _: &ConfigSection, _: &LogDependencies) -> Result<Arc<dyn Log>, RegistryError> {
        Ok(Arc::clone(&self.0))
    }
}
struct NamedStore(Arc<dyn ObjectStore>);
impl ComponentFactory<dyn ObjectStore> for NamedStore {
    fn name(&self) -> &'static str {
        "custom-store"
    }
    fn build(&self, _: &ConfigSection, _: &()) -> Result<Arc<dyn ObjectStore>, RegistryError> {
        Ok(Arc::clone(&self.0))
    }
}

#[tokio::test]
async fn renamed_durable_log_requires_durable_blocks() {
    let dir = tempfile::tempdir().unwrap();
    let log: Arc<dyn Log> =
        Arc::new(varve_log::LocalLog::open(&dir.path().join("log"), 1024).unwrap());
    let config =
        Config::from_toml_str("[log]\nbackend='custom-log'\n[storage]\nbackend='custom-store'")
            .unwrap();
    for durable in [false, true] {
        let mut registries = Registries::with_builtins();
        registries
            .log
            .register(Box::new(NamedLog(Arc::clone(&log))))
            .unwrap();
        let store = if durable {
            varve_storage::local_store(&dir.path().join("blocks")).unwrap()
        } else {
            varve_storage::memory_store()
        };
        registries
            .storage
            .register(Box::new(NamedStore(store)))
            .unwrap();
        let result = Db::open_with(&config, &registries).await;
        if durable {
            assert!(result.is_ok());
        } else {
            assert!(matches!(result, Err(EngineError::VolatileBlockStore)));
        }
    }
}

#[tokio::test]
async fn malformed_configuration_cannot_start_with_defaults() {
    for toml in [
        "storage=123",
        "log=false",
        "[storage]\nbackend=123",
        "[log]\nbackend=false",
        "[cache]\ndisk=123\ntiers=['disk']",
        "[clock]\nbackend=[]",
    ] {
        let config = Config::from_toml_str(toml).unwrap();
        assert!(Db::open(config).await.is_err(), "accepted {toml}");
    }
}

#[tokio::test]
async fn mixed_numeric_query_reports_precision_error() {
    let db = Db::memory();
    db.execute("INSERT (:P {_id: 1, score: 9007199254740993}), (:P {_id: 2, score: 0.5})")
        .await
        .unwrap();
    let err = db.query("MATCH (n:P) RETURN n.score").await.unwrap_err();
    assert!(
        err.client_query_error().is_some(),
        "precision error must be actionable: {err}"
    );
    assert!(err.to_string().contains("9007199254740993"));
}

/// No durability override: existing custom stores start as Unknown.
struct UndeclaredStore(Arc<dyn ObjectStore>);
#[async_trait::async_trait]
impl ObjectStore for UndeclaredStore {
    async fn put(&self, key: &str, bytes: bytes::Bytes) -> Result<(), varve_storage::StorageError> {
        self.0.put(key, bytes).await
    }
    async fn get(&self, key: &str) -> Result<bytes::Bytes, varve_storage::StorageError> {
        self.0.get(key).await
    }
    async fn get_range(
        &self,
        key: &str,
        range: std::ops::Range<u64>,
    ) -> Result<bytes::Bytes, varve_storage::StorageError> {
        self.0.get_range(key, range).await
    }
    async fn list(&self, prefix: &str) -> Result<Vec<String>, varve_storage::StorageError> {
        self.0.list(prefix).await
    }
    async fn delete(&self, key: &str) -> Result<(), varve_storage::StorageError> {
        self.0.delete(key).await
    }
}

#[tokio::test]
async fn undeclared_storage_cannot_replace_a_durable_log() {
    let dir = tempfile::tempdir().unwrap();
    let mut registries = Registries::with_builtins();
    registries
        .log
        .register(Box::new(NamedLog(Arc::new(
            varve_log::LocalLog::open(&dir.path().join("log"), 1024).unwrap(),
        ))))
        .unwrap();
    registries
        .storage
        .register(Box::new(NamedStore(Arc::new(UndeclaredStore(
            varve_storage::memory_store(),
        )))))
        .unwrap();
    let config =
        Config::from_toml_str("[log]\nbackend='custom-log'\n[storage]\nbackend='custom-store'")
            .unwrap();
    assert!(matches!(
        Db::open_with(&config, &registries).await,
        Err(EngineError::VolatileBlockStore)
    ));
}
