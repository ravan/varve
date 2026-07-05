use bytes::Bytes;
use std::sync::Arc;
use varve_config::{BuildContext, Config, ConfigSection};
use varve_storage::{local_store, memory_store, storage_registry, ObjectStore, StorageError};

#[cfg(feature = "s3")]
mod s3_factory {
    use varve_config::{BuildContext, Config, ConfigSection};
    use varve_storage::storage_registry;

    #[allow(clippy::unwrap_used)]
    fn storage_section(toml: &str) -> ConfigSection {
        Config::from_toml_str(toml)
            .unwrap()
            .section("storage")
            .unwrap()
    }

    /// `AmazonS3Builder::build()` does no I/O — a fully specified factory
    /// build must succeed with nothing listening on the endpoint.
    #[test]
    fn s3_factory_builds_from_full_config() {
        let cfg = storage_section(
            "[storage]\nbackend = \"s3\"\n[storage.s3]\n\
             endpoint = \"http://127.0.0.1:3900\"\nbucket = \"varve\"\n\
             region = \"garage\"\naccess_key_id = \"GK0123456789\"\n\
             secret_access_key = \"secret\"\n",
        );
        assert!(storage_registry()
            .build("s3", &cfg, &BuildContext::empty())
            .is_ok());
    }

    /// Credentials may be omitted entirely: the builder starts from
    /// `from_env()` and defers to the AWS provider chain — building still
    /// succeeds (resolution is lazy, at first request).
    #[test]
    fn s3_factory_builds_without_inline_credentials() {
        let cfg = storage_section(
            "[storage]\nbackend = \"s3\"\n[storage.s3]\n\
             endpoint = \"http://127.0.0.1:3900\"\nbucket = \"varve\"\n",
        );
        assert!(storage_registry()
            .build("s3", &cfg, &BuildContext::empty())
            .is_ok());
    }

    #[test]
    fn s3_factory_requires_the_s3_section() {
        let cfg = storage_section("[storage]\nbackend = \"s3\"\n");
        let err = match storage_registry().build("s3", &cfg, &BuildContext::empty()) {
            Ok(_) => panic!("expected build(\"s3\") with no [storage.s3] to fail"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("[storage.s3]"), "{err}");
    }

    #[test]
    fn s3_factory_requires_a_bucket() {
        let cfg = storage_section(
            "[storage]\nbackend = \"s3\"\n[storage.s3]\nendpoint = \"http://x\"\n",
        );
        let err = match storage_registry().build("s3", &cfg, &BuildContext::empty()) {
            Ok(_) => panic!("expected build(\"s3\") without bucket to fail"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("bucket"), "{err}");
    }
}

/// Trait conformance, run against every backend: atomic put/replace, whole
/// and ranged gets, NotFound, sorted prefix-scoped listing.
#[allow(clippy::unwrap_used)]
async fn exercise(store: Arc<dyn ObjectStore>) {
    store
        .put("v1/a/one", Bytes::from_static(b"hello"))
        .await
        .unwrap();
    assert_eq!(
        store.get("v1/a/one").await.unwrap(),
        Bytes::from_static(b"hello")
    );

    // put replaces
    store
        .put("v1/a/one", Bytes::from_static(b"world"))
        .await
        .unwrap();
    assert_eq!(
        store.get("v1/a/one").await.unwrap(),
        Bytes::from_static(b"world")
    );

    // half-open byte range
    assert_eq!(
        store.get_range("v1/a/one", 1..4).await.unwrap(),
        Bytes::from_static(b"orl")
    );

    // NotFound carries the key
    assert!(matches!(
        store.get("v1/a/absent").await,
        Err(StorageError::NotFound(k)) if k == "v1/a/absent"
    ));

    // list: prefix-scoped (path segments), sorted, empty prefix ok
    store
        .put("v1/a/two", Bytes::from_static(b"2"))
        .await
        .unwrap();
    store
        .put("v1/b/three", Bytes::from_static(b"3"))
        .await
        .unwrap();
    assert_eq!(
        store.list("v1/a").await.unwrap(),
        vec!["v1/a/one".to_string(), "v1/a/two".to_string()]
    );
    assert_eq!(store.list("v1/absent").await.unwrap(), Vec::<String>::new());
}

#[tokio::test]
async fn memory_backend_conforms() {
    exercise(memory_store()).await;
}

#[tokio::test]
async fn local_backend_conforms() {
    let dir = tempfile::tempdir().unwrap();
    exercise(local_store(dir.path()).unwrap()).await;
}

#[tokio::test]
async fn local_store_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    local_store(dir.path())
        .unwrap()
        .put("v1/x", Bytes::from_static(b"here"))
        .await
        .unwrap();
    let again = local_store(dir.path()).unwrap();
    assert_eq!(
        again.get("v1/x").await.unwrap(),
        Bytes::from_static(b"here")
    );
}

#[tokio::test]
async fn registry_builds_by_name() {
    let reg = storage_registry();
    assert_eq!(reg.names(), vec!["local", "memory", "s3"]);
    let store = reg.build("memory", &ConfigSection::empty(), &BuildContext::empty()).unwrap();
    store.put("k", Bytes::from_static(b"v")).await.unwrap();
    assert_eq!(store.get("k").await.unwrap(), Bytes::from_static(b"v"));
}

#[test]
fn local_factory_requires_dir() {
    // `.unwrap_err()` needs `Arc<dyn ObjectStore>: Debug`, which `ObjectStore`
    // does not require, so extract the error via `match` instead (see
    // varve-log's local_log.rs test for the same pattern).
    let err = match storage_registry().build("local", &ConfigSection::empty(), &BuildContext::empty()) {
        Ok(_) => panic!("expected build(\"local\") with no [storage.local] to fail"),
        Err(e) => e.to_string(),
    };
    assert!(err.contains("[storage.local]"), "{err}");
}

#[tokio::test]
async fn local_factory_builds_from_config() {
    let dir = tempfile::tempdir().unwrap();
    let toml = format!(
        "[storage]\nbackend = \"local\"\n[storage.local]\ndir = {:?}\n",
        dir.path().display().to_string()
    );
    let cfg = Config::from_toml_str(&toml)
        .unwrap()
        .section("storage")
        .unwrap();
    let store = storage_registry().build("local", &cfg, &BuildContext::empty()).unwrap();
    store.put("v1/y", Bytes::from_static(b"z")).await.unwrap();
    assert_eq!(store.get("v1/y").await.unwrap(), Bytes::from_static(b"z"));
}
