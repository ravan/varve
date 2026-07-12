use bytes::Bytes;
use std::ops::Range;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("object not found: {0}")]
    NotFound(String),
    #[error("storage backend error: {0}")]
    Backend(#[source] object_store::Error),
    #[error("storage I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("manifest decode failed: {0}")]
    Decode(#[from] prost::DecodeError),
    #[error("invalid storage key: {0}")]
    InvalidKey(String),
    #[error("object {0} has no ETag; conditional workflows are inexpressible")]
    NoEtag(String),
}

/// Maps a backend error, preserving the key when it was not found.
/// Deliberately not a `From` impl: `NotFound` needs the key, which the
/// backend error alone does not carry.
pub(crate) fn convert(key: &str, e: object_store::Error) -> StorageError {
    match e {
        object_store::Error::NotFound { .. } => StorageError::NotFound(key.to_string()),
        other => StorageError::Backend(other),
    }
}

/// One conditional write's outcome. `Err(StorageError)` is reserved for
/// transport failures; every SEMANTIC outcome — including "backend cannot
/// do this" — is a variant, so the probe can classify without guessing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CondPut {
    /// The write landed; `etag` identifies the new object version (`None`
    /// means the backend returns no ETag — which alone rules out CAS).
    Stored { etag: Option<String> },
    /// Correctly refused: the object already exists (create path).
    AlreadyExists,
    /// Correctly refused: the ETag no longer matches (swap path).
    PreconditionFailed,
    /// The backend reports it cannot do conditional writes at all.
    Unsupported { reason: String },
}

/// OPTIONAL conditional-write surface (spec §12, D5/D7). Never required by
/// the engine — sovereignty means plain put/get/list always suffices.
/// Backends that can do more expose it through `ObjectStore::conditional`;
/// `probe::probe_conditional_put` classifies whether the claims actually
/// hold, and slice-10's cas-failover coordinator gates on that verdict.
#[async_trait::async_trait]
pub trait ConditionalStore: Send + Sync {
    /// Create-only PUT (`If-None-Match: *`).
    async fn put_if_absent(&self, key: &str, bytes: Bytes) -> Result<CondPut, StorageError>;
    /// ETag-guarded replace (`If-Match`).
    async fn put_if_matches(
        &self,
        key: &str,
        bytes: Bytes,
        etag: &str,
    ) -> Result<CondPut, StorageError>;
    /// Reads the object together with its version tag (ETag); `None` if
    /// absent. A backend that stores the object but returns no ETag yields
    /// `NoEtag` (such a backend cannot pass the probe anyway, so this is only
    /// reachable via a hand-rolled `ConditionalStore`, never the blanket
    /// `object_store` impl once the probe has verified `Supported`).
    async fn get_versioned(&self, key: &str) -> Result<Option<(Bytes, String)>, StorageError>;
}

/// Varve's object-store interface (spec §4, §9). Sovereignty (spec §1, D7):
/// nothing beyond plain S3 semantics — put/get/list/delete only; no conditional
/// PUT. `put` is atomic: readers see the
/// whole object or none (the manifest commit point relies on this).
#[async_trait::async_trait]
pub trait ObjectStore: Send + Sync {
    /// Atomically create/replace the object at `key`.
    async fn put(&self, key: &str, bytes: Bytes) -> Result<(), StorageError>;
    /// Reads the whole object.
    async fn get(&self, key: &str) -> Result<Bytes, StorageError>;
    /// Reads bytes in `[range.start, range.end)`.
    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Bytes, StorageError>;
    /// Keys under a path-segment prefix (e.g. `"v1/blocks"`), sorted
    /// lexicographically. Prefixes match whole path segments only, so
    /// `"v1/a"` matches `"v1/a/one"` but not `"v1/ab/one"`.
    async fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError>;
    /// Deletes an object. Missing keys are success so GC can retry safely.
    async fn delete(&self, key: &str) -> Result<(), StorageError>;
    /// The optional conditional-write surface, if this backend has one.
    /// Default: none — custom embedder stores need change nothing.
    fn conditional(&self) -> Option<&dyn ConditionalStore> {
        None
    }
}

/// Blanket impl: every `object_store::ObjectStore` IS a Varve `ObjectStore`.
///
/// Fully-qualified call syntax (`object_store::ObjectStoreExt::put(self, ...)`)
/// is required throughout this impl — a bare `self.put(...)` would resolve
/// back to OUR trait (of the same name) and recurse forever instead of
/// reaching the underlying `object_store` crate's implementation.
#[async_trait::async_trait]
impl<T: object_store::ObjectStore> ObjectStore for T {
    async fn put(&self, key: &str, bytes: Bytes) -> Result<(), StorageError> {
        let path = object_store::path::Path::from(key);
        object_store::ObjectStoreExt::put(self, &path, bytes.into())
            .await
            .map_err(|e| convert(key, e))?;
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Bytes, StorageError> {
        let path = object_store::path::Path::from(key);
        let result = object_store::ObjectStoreExt::get(self, &path)
            .await
            .map_err(|e| convert(key, e))?;
        result.bytes().await.map_err(|e| convert(key, e))
    }

    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Bytes, StorageError> {
        let path = object_store::path::Path::from(key);
        object_store::ObjectStoreExt::get_range(self, &path, range)
            .await
            .map_err(|e| convert(key, e))
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
        use futures::TryStreamExt as _;
        let path = object_store::path::Path::from(prefix);
        let metas: Vec<object_store::ObjectMeta> =
            object_store::ObjectStore::list(self, Some(&path))
                .try_collect()
                .await
                .map_err(|e| convert(prefix, e))?;
        let mut keys: Vec<String> = metas.into_iter().map(|m| m.location.to_string()).collect();
        keys.sort();
        Ok(keys)
    }

    async fn delete(&self, key: &str) -> Result<(), StorageError> {
        let path = object_store::path::Path::from(key);
        match object_store::ObjectStoreExt::delete(self, &path).await {
            Ok(()) => Ok(()),
            Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(convert(key, e)),
        }
    }

    fn conditional(&self) -> Option<&dyn ConditionalStore> {
        Some(self)
    }
}

/// Maps a `put_opts` outcome onto the `CondPut` classification.
fn classify_cond_put(
    key: &str,
    result: Result<object_store::PutResult, object_store::Error>,
) -> Result<CondPut, StorageError> {
    match result {
        Ok(r) => Ok(CondPut::Stored { etag: r.e_tag }),
        Err(object_store::Error::AlreadyExists { .. }) => Ok(CondPut::AlreadyExists),
        Err(object_store::Error::Precondition { .. }) => Ok(CondPut::PreconditionFailed),
        Err(e @ object_store::Error::NotImplemented { .. })
        | Err(e @ object_store::Error::NotSupported { .. }) => Ok(CondPut::Unsupported {
            reason: e.to_string(),
        }),
        Err(e) => Err(convert(key, e)),
    }
}

/// Blanket conditional surface for every `object_store` backend, via
/// `put_opts` (S3ConditionalPut::ETagMatch is the 0.13 default, so the AWS
/// impl sends real If-None-Match / If-Match headers).
#[async_trait::async_trait]
impl<T: object_store::ObjectStore> ConditionalStore for T {
    async fn put_if_absent(&self, key: &str, bytes: Bytes) -> Result<CondPut, StorageError> {
        let path = object_store::path::Path::from(key);
        classify_cond_put(
            key,
            object_store::ObjectStore::put_opts(
                self,
                &path,
                bytes.into(),
                object_store::PutMode::Create.into(),
            )
            .await,
        )
    }

    async fn put_if_matches(
        &self,
        key: &str,
        bytes: Bytes,
        etag: &str,
    ) -> Result<CondPut, StorageError> {
        let path = object_store::path::Path::from(key);
        let version = object_store::UpdateVersion {
            e_tag: Some(etag.to_string()),
            version: None,
        };
        classify_cond_put(
            key,
            object_store::ObjectStore::put_opts(
                self,
                &path,
                bytes.into(),
                object_store::PutMode::Update(version).into(),
            )
            .await,
        )
    }

    async fn get_versioned(&self, key: &str) -> Result<Option<(Bytes, String)>, StorageError> {
        let path = object_store::path::Path::from(key);
        let result = match object_store::ObjectStoreExt::get(self, &path).await {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(e) => return Err(convert(key, e)),
        };
        let etag = result.meta.e_tag.clone();
        let bytes = result.bytes().await.map_err(|e| convert(key, e))?;
        match etag {
            Some(etag) => Ok(Some((bytes, etag))),
            None => Err(StorageError::NoEtag(key.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory_store;

    #[tokio::test]
    async fn get_versioned_rotates_etags_and_is_none_when_absent() {
        let store = memory_store();
        let cond = store.conditional().unwrap();
        assert_eq!(cond.get_versioned("v1/x").await.unwrap(), None);

        let CondPut::Stored { etag: Some(etag1) } = cond
            .put_if_absent("v1/x", Bytes::from_static(b"one"))
            .await
            .unwrap()
        else {
            panic!("expected Stored with an etag");
        };
        let (bytes1, observed1) = cond.get_versioned("v1/x").await.unwrap().unwrap();
        assert_eq!(bytes1, Bytes::from_static(b"one"));
        assert_eq!(observed1, etag1);

        let CondPut::Stored { etag: Some(etag2) } = cond
            .put_if_matches("v1/x", Bytes::from_static(b"two"), &etag1)
            .await
            .unwrap()
        else {
            panic!("expected Stored with an etag");
        };
        assert_ne!(etag1, etag2, "an update must rotate the etag");
        let (bytes2, observed2) = cond.get_versioned("v1/x").await.unwrap().unwrap();
        assert_eq!(bytes2, Bytes::from_static(b"two"));
        assert_eq!(observed2, etag2);
    }
}
