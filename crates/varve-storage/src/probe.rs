//! Startup capability probe (spec §12, D5): does this backend REALLY
//! implement conditional PUT? Four steps against a fresh key:
//!
//! 1. create (`If-None-Match: *`) on the fresh key → must store + yield an ETag
//! 2. create AGAIN on the same key               → must refuse
//! 3. swap with the CURRENT etag (`If-Match`)    → must store + rotate the ETag
//! 4. swap with the now-STALE step-1 etag        → must refuse
//!
//! Steps 2 and 4 are the semantic teeth: a backend that ignores the headers
//! passes 1 and 3 but "succeeds" at 2 or 4 ⇒ `Inconsistent` — the dangerous
//! verdict (working-looking CAS that would lose the failover race). A
//! versioned bucket that hands back an unchanged ETag surfaces the same way.
//!
//! Report-only in slice 5; slice-10 cas-failover refuses to start unless the
//! verdict is `Supported`. Each run leaves ≤ 2 small objects under
//! `v1/probe/` (the sovereign trait has no delete; slice-8 GC sweeps them).

use crate::store::{CondPut, ObjectStore, StorageError};
use bytes::Bytes;

pub const PROBE_PREFIX: &str = "v1/probe";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProbeVerdict {
    /// Create-if-absent AND etag-swap semantics both enforced correctly.
    Supported,
    /// The backend refuses (or cannot express) conditional writes — the
    /// safe answer; designated-writer mode is unaffected.
    Unsupported { reason: String },
    /// The backend CLAIMS success while violating the semantics (e.g. blind
    /// overwrite). MUST be treated as no-CAS; strictly worse than
    /// `Unsupported` because only this probe distinguishes it from working
    /// CAS (D5: SeaweedFS-class bugs).
    Inconsistent { reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProbeReport {
    pub verdict: ProbeVerdict,
    /// Where the probe objects were left (for slice-8 GC and diagnostics).
    pub probe_key: String,
}

/// Runs the 4-step probe against `probe_key`, which MUST be fresh (the
/// caller supplies uniqueness — e.g. `Db` derives it from its `Clock`, so
/// the probe itself introduces no randomness). Transport failures surface
/// as `Err`; every semantic outcome is a verdict.
pub async fn probe_conditional_put(
    store: &dyn ObjectStore,
    probe_key: &str,
) -> Result<ProbeReport, StorageError> {
    let verdict = classify(store, probe_key).await?;
    Ok(ProbeReport {
        verdict,
        probe_key: probe_key.to_string(),
    })
}

fn unsupported(reason: impl Into<String>) -> ProbeVerdict {
    ProbeVerdict::Unsupported {
        reason: reason.into(),
    }
}

fn inconsistent(reason: impl Into<String>) -> ProbeVerdict {
    ProbeVerdict::Inconsistent {
        reason: reason.into(),
    }
}

async fn classify(store: &dyn ObjectStore, key: &str) -> Result<ProbeVerdict, StorageError> {
    let Some(cond) = store.conditional() else {
        return Ok(unsupported("backend exposes no conditional-write API"));
    };

    // 1. create on a fresh key.
    let etag1 = match cond
        .put_if_absent(key, Bytes::from_static(b"varve-probe-1"))
        .await?
    {
        CondPut::Stored { etag: Some(etag) } => etag,
        CondPut::Stored { etag: None } => {
            return Ok(unsupported(
                "PUT returns no ETag; If-Match swaps are inexpressible",
            ));
        }
        CondPut::Unsupported { reason } => return Ok(unsupported(reason)),
        CondPut::AlreadyExists | CondPut::PreconditionFailed => {
            return Ok(inconsistent("fresh probe key was refused as existing"));
        }
    };

    // 2. create over the existing key must be refused.
    match cond
        .put_if_absent(key, Bytes::from_static(b"varve-probe-1"))
        .await?
    {
        CondPut::AlreadyExists | CondPut::PreconditionFailed => {}
        CondPut::Stored { .. } => {
            return Ok(inconsistent(
                "create-if-absent over an existing object succeeded (precondition ignored)",
            ));
        }
        CondPut::Unsupported { reason } => return Ok(unsupported(reason)),
    }

    // 3. swap with the current etag must land and rotate the etag.
    let etag2 = match cond
        .put_if_matches(key, Bytes::from_static(b"varve-probe-2"), &etag1)
        .await?
    {
        CondPut::Stored { etag: Some(etag) } => etag,
        CondPut::Stored { etag: None } => {
            return Ok(unsupported(
                "update returns no ETag; chained swaps are inexpressible",
            ));
        }
        CondPut::Unsupported { reason } => return Ok(unsupported(reason)),
        CondPut::AlreadyExists | CondPut::PreconditionFailed => {
            return Ok(inconsistent("swap with the CURRENT etag was refused"));
        }
    };
    if etag2 == etag1 {
        return Ok(inconsistent(
            "etag did not change across an update (versioned-bucket edge case, spec §12)",
        ));
    }

    // 4. swap with the now-stale first etag must be refused.
    Ok(
        match cond
            .put_if_matches(key, Bytes::from_static(b"varve-probe-3"), &etag1)
            .await?
        {
            CondPut::PreconditionFailed => ProbeVerdict::Supported,
            CondPut::Stored { .. } => {
                inconsistent("swap with a STALE etag succeeded (lost-update hazard)")
            }
            CondPut::AlreadyExists => {
                inconsistent("stale-etag swap refused with the wrong class (AlreadyExists)")
            }
            CondPut::Unsupported { reason } => unsupported(reason),
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{CondPut, ConditionalStore, ObjectStore, StorageError};
    use crate::{local_store, memory_store};
    use bytes::Bytes;
    use std::ops::Range;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    /// InMemory supports Create AND etag-checked Update with rotating ETags
    /// (verified against object_store 0.13.2 source) ⇒ Supported.
    #[tokio::test]
    async fn memory_store_is_supported() {
        let store = memory_store();
        let report = probe_conditional_put(store.as_ref(), "v1/probe/t1")
            .await
            .unwrap();
        assert_eq!(report.verdict, ProbeVerdict::Supported);
        assert_eq!(report.probe_key, "v1/probe/t1");
    }

    /// LocalFileSystem rejects PutMode::Update (NotImplemented, verified
    /// against source) ⇒ Unsupported, with the operation named.
    #[tokio::test]
    async fn local_store_is_unsupported() {
        let dir = tempfile::tempdir().unwrap();
        let store = local_store(dir.path()).unwrap();
        let report = probe_conditional_put(store.as_ref(), "v1/probe/t1")
            .await
            .unwrap();
        match report.verdict {
            ProbeVerdict::Unsupported { reason } => {
                assert!(reason.contains("PutMode::Update"), "{reason}");
            }
            other => panic!("expected Unsupported, got {other:?}"),
        }
    }

    /// A store whose ObjectStore impl never overrides `conditional()`.
    struct PlainStore(Arc<dyn ObjectStore>);

    #[async_trait::async_trait]
    impl ObjectStore for PlainStore {
        async fn put(&self, key: &str, bytes: Bytes) -> Result<(), StorageError> {
            self.0.put(key, bytes).await
        }
        async fn get(&self, key: &str) -> Result<Bytes, StorageError> {
            self.0.get(key).await
        }
        async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Bytes, StorageError> {
            self.0.get_range(key, range).await
        }
        async fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
            self.0.list(prefix).await
        }
    }

    #[tokio::test]
    async fn stores_without_the_hook_are_unsupported() {
        let store = PlainStore(memory_store());
        let report = probe_conditional_put(&store, "v1/probe/t1").await.unwrap();
        assert!(matches!(report.verdict, ProbeVerdict::Unsupported { .. }));
    }

    /// Claims success on EVERY conditional write (SeaweedFS-class header
    /// blindness, D5): step 2 must expose it.
    struct BlindStore {
        inner: Arc<dyn ObjectStore>,
        writes: AtomicU64,
    }

    #[async_trait::async_trait]
    impl ObjectStore for BlindStore {
        async fn put(&self, key: &str, bytes: Bytes) -> Result<(), StorageError> {
            self.inner.put(key, bytes).await
        }
        async fn get(&self, key: &str) -> Result<Bytes, StorageError> {
            self.inner.get(key).await
        }
        async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Bytes, StorageError> {
            self.inner.get_range(key, range).await
        }
        async fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
            self.inner.list(prefix).await
        }
        fn conditional(&self) -> Option<&dyn ConditionalStore> {
            Some(self)
        }
    }

    #[async_trait::async_trait]
    impl ConditionalStore for BlindStore {
        async fn put_if_absent(&self, key: &str, bytes: Bytes) -> Result<CondPut, StorageError> {
            self.inner.put(key, bytes).await?;
            Ok(CondPut::Stored {
                etag: Some(format!("e{}", self.writes.fetch_add(1, Ordering::SeqCst))),
            })
        }
        async fn put_if_matches(
            &self,
            key: &str,
            bytes: Bytes,
            _etag: &str,
        ) -> Result<CondPut, StorageError> {
            self.put_if_absent(key, bytes).await
        }
    }

    #[tokio::test]
    async fn blind_success_is_inconsistent() {
        let store = BlindStore {
            inner: memory_store(),
            writes: AtomicU64::new(0),
        };
        let report = probe_conditional_put(&store, "v1/probe/t1").await.unwrap();
        match report.verdict {
            ProbeVerdict::Inconsistent { reason } => {
                assert!(reason.contains("create-if-absent"), "{reason}");
            }
            other => panic!("expected Inconsistent, got {other:?}"),
        }
    }

    /// Creates correctly but never checks the etag on update: step 4 (the
    /// stale-etag swap) must expose it.
    struct StaleAcceptor {
        inner: Arc<dyn ObjectStore>,
        writes: AtomicU64,
    }

    impl StaleAcceptor {
        fn next_etag(&self) -> String {
            format!("e{}", self.writes.fetch_add(1, Ordering::SeqCst))
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for StaleAcceptor {
        async fn put(&self, key: &str, bytes: Bytes) -> Result<(), StorageError> {
            self.inner.put(key, bytes).await
        }
        async fn get(&self, key: &str) -> Result<Bytes, StorageError> {
            self.inner.get(key).await
        }
        async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Bytes, StorageError> {
            self.inner.get_range(key, range).await
        }
        async fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
            self.inner.list(prefix).await
        }
        fn conditional(&self) -> Option<&dyn ConditionalStore> {
            Some(self)
        }
    }

    #[async_trait::async_trait]
    impl ConditionalStore for StaleAcceptor {
        async fn put_if_absent(&self, key: &str, bytes: Bytes) -> Result<CondPut, StorageError> {
            if self.inner.get(key).await.is_ok() {
                return Ok(CondPut::AlreadyExists);
            }
            self.inner.put(key, bytes).await?;
            Ok(CondPut::Stored {
                etag: Some(self.next_etag()),
            })
        }
        async fn put_if_matches(
            &self,
            key: &str,
            bytes: Bytes,
            _etag: &str, // never checked — the bug under test
        ) -> Result<CondPut, StorageError> {
            self.inner.put(key, bytes).await?;
            Ok(CondPut::Stored {
                etag: Some(self.next_etag()),
            })
        }
    }

    #[tokio::test]
    async fn accepted_stale_etag_is_inconsistent() {
        let store = StaleAcceptor {
            inner: memory_store(),
            writes: AtomicU64::new(0),
        };
        let report = probe_conditional_put(&store, "v1/probe/t1").await.unwrap();
        match report.verdict {
            ProbeVerdict::Inconsistent { reason } => {
                assert!(reason.contains("STALE"), "{reason}");
            }
            other => panic!("expected Inconsistent, got {other:?}"),
        }
    }

    /// The cache wrapper must pass the hook through (Db's store is wrapped).
    #[tokio::test]
    async fn cached_store_delegates_the_conditional_hook() {
        use crate::cache::{CachedStore, MemoryCache};
        let cached = CachedStore::new(memory_store(), Arc::new(MemoryCache::new(1024)));
        let report = probe_conditional_put(&cached, "v1/probe/t1").await.unwrap();
        assert_eq!(report.verdict, ProbeVerdict::Supported);
    }
}
