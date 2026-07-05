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

/// Varve's object-store interface (spec §4, §9). Sovereignty (spec §1, D7):
/// nothing beyond plain S3 semantics — put/get/list only; no conditional
/// PUT, no delete (GC arrives in slice 8). `put` is atomic: readers see the
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
}
