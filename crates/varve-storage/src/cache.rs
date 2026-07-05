//! Query-path caching (spec §9): tiers keyed by object path + byte range.
//! v1 ships the in-memory tier; the disk tier arrives in slice 5. Objects
//! are immutable by key discipline (append-only store), but `put` still
//! invalidates the written path as a correctness belt.

use crate::store::{ObjectStore, StorageError};
use bytes::Bytes;
use std::collections::HashMap;
use std::ops::Range;
use std::sync::{Arc, Mutex};
use varve_config::{BuildContext, ComponentFactory, ConfigSection, RegistryError};

/// `range: None` = the whole object; `Some((start, end))` = a half-open
/// byte range — distinct cache entries.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct CacheKey {
    pub path: String,
    pub range: Option<(u64, u64)>,
}

pub trait CacheTier: Send + Sync {
    fn get(&self, key: &CacheKey) -> Option<Bytes>;
    fn insert(&self, key: CacheKey, value: Bytes);
    fn invalidate_path(&self, path: &str);
}

struct Entry {
    value: Bytes,
    last_used: u64,
}

#[derive(Default)]
struct CacheInner {
    entries: HashMap<CacheKey, Entry>,
    bytes: usize,
    tick: u64,
}

/// LRU over a byte budget. Eviction scans for the minimum tick — O(n) per
/// eviction, fine at v1 entry counts (whole objects and pages, not rows).
/// A poisoned lock degrades to cache-miss behavior, never an error.
pub struct MemoryCache {
    max_bytes: usize,
    inner: Mutex<CacheInner>,
}

impl MemoryCache {
    pub fn new(max_bytes: usize) -> MemoryCache {
        MemoryCache {
            max_bytes,
            inner: Mutex::new(CacheInner::default()),
        }
    }
}

impl CacheTier for MemoryCache {
    fn get(&self, key: &CacheKey) -> Option<Bytes> {
        let Ok(mut inner) = self.inner.lock() else {
            return None;
        };
        inner.tick += 1;
        let tick = inner.tick;
        let entry = inner.entries.get_mut(key)?;
        entry.last_used = tick;
        Some(entry.value.clone())
    }

    fn insert(&self, key: CacheKey, value: Bytes) {
        if value.len() > self.max_bytes {
            return;
        }
        let Ok(mut inner) = self.inner.lock() else {
            return;
        };
        inner.tick += 1;
        let tick = inner.tick;
        let len = value.len();
        if let Some(old) = inner.entries.insert(
            key,
            Entry {
                value,
                last_used: tick,
            },
        ) {
            inner.bytes -= old.value.len();
        }
        inner.bytes += len;
        while inner.bytes > self.max_bytes {
            let Some(oldest) = inner
                .entries
                .iter()
                .min_by_key(|(_, e)| e.last_used)
                .map(|(k, _)| k.clone())
            else {
                break;
            };
            if let Some(e) = inner.entries.remove(&oldest) {
                inner.bytes -= e.value.len();
            }
        }
    }

    fn invalidate_path(&self, path: &str) {
        let Ok(mut inner) = self.inner.lock() else {
            return;
        };
        let stale: Vec<CacheKey> = inner
            .entries
            .keys()
            .filter(|k| k.path == path)
            .cloned()
            .collect();
        for key in stale {
            if let Some(e) = inner.entries.remove(&key) {
                inner.bytes -= e.value.len();
            }
        }
    }
}

/// Read-through cache wrapper: `get`/`get_range` fill the cache, `put`
/// invalidates its path, `list` always hits the backend (a fresh manifest
/// must be visible immediately).
pub struct CachedStore {
    inner: Arc<dyn ObjectStore>,
    cache: Arc<dyn CacheTier>,
}

impl CachedStore {
    pub fn new(inner: Arc<dyn ObjectStore>, cache: Arc<dyn CacheTier>) -> CachedStore {
        CachedStore { inner, cache }
    }
}

#[async_trait::async_trait]
impl ObjectStore for CachedStore {
    async fn put(&self, key: &str, bytes: Bytes) -> Result<(), StorageError> {
        self.cache.invalidate_path(key);
        self.inner.put(key, bytes).await
    }

    async fn get(&self, key: &str) -> Result<Bytes, StorageError> {
        let cache_key = CacheKey {
            path: key.to_string(),
            range: None,
        };
        if let Some(hit) = self.cache.get(&cache_key) {
            return Ok(hit);
        }
        let value = self.inner.get(key).await?;
        self.cache.insert(cache_key, value.clone());
        Ok(value)
    }

    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Bytes, StorageError> {
        let cache_key = CacheKey {
            path: key.to_string(),
            range: Some((range.start, range.end)),
        };
        if let Some(hit) = self.cache.get(&cache_key) {
            return Ok(hit);
        }
        let value = self.inner.get_range(key, range).await?;
        self.cache.insert(cache_key, value.clone());
        Ok(value)
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
        self.inner.list(prefix).await
    }
}

#[derive(serde::Deserialize)]
struct MemoryCacheConfig {
    #[serde(default = "default_memory_max_bytes")]
    max_bytes: usize,
}

fn default_memory_max_bytes() -> usize {
    512 * 1024 * 1024
}

/// Registry factory: listed as `"memory"` in `[cache] tiers`, tuned by the
/// optional `[cache.memory]` table (`max_bytes`, default 512 MiB).
pub struct MemoryCacheFactory;

impl ComponentFactory<dyn CacheTier> for MemoryCacheFactory {
    fn name(&self) -> &'static str {
        "memory"
    }

    fn build(
        &self,
        cfg: &ConfigSection,
        _ctx: &BuildContext,
    ) -> Result<Arc<dyn CacheTier>, RegistryError> {
        let config: MemoryCacheConfig = cfg
            .child("memory")
            .unwrap_or_else(ConfigSection::empty)
            .get()?;
        Ok(Arc::new(MemoryCache::new(config.max_bytes)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{memory_store, ObjectStore, StorageError};
    use std::ops::Range;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    /// Counts backend reads so tests can assert cache hits.
    struct CountingStore {
        inner: Arc<dyn ObjectStore>,
        reads: AtomicUsize,
    }

    impl CountingStore {
        fn new() -> Arc<CountingStore> {
            Arc::new(CountingStore {
                inner: memory_store(),
                reads: AtomicUsize::new(0),
            })
        }
        fn reads(&self) -> usize {
            self.reads.load(Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for CountingStore {
        async fn put(&self, key: &str, bytes: Bytes) -> Result<(), StorageError> {
            self.inner.put(key, bytes).await
        }
        async fn get(&self, key: &str) -> Result<Bytes, StorageError> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            self.inner.get(key).await
        }
        async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Bytes, StorageError> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            self.inner.get_range(key, range).await
        }
        async fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
            self.inner.list(prefix).await
        }
    }

    fn cached(counting: &Arc<CountingStore>, budget: usize) -> CachedStore {
        CachedStore::new(
            Arc::clone(counting) as Arc<dyn ObjectStore>,
            Arc::new(MemoryCache::new(budget)),
        )
    }

    #[tokio::test]
    async fn whole_object_reads_hit_the_cache() {
        let counting = CountingStore::new();
        let store = cached(&counting, 1024);
        store.put("k", Bytes::from_static(b"value")).await.unwrap();
        assert_eq!(store.get("k").await.unwrap(), Bytes::from_static(b"value"));
        assert_eq!(store.get("k").await.unwrap(), Bytes::from_static(b"value"));
        assert_eq!(counting.reads(), 1);
    }

    #[tokio::test]
    async fn ranged_reads_are_cached_per_range() {
        let counting = CountingStore::new();
        let store = cached(&counting, 1024);
        store.put("k", Bytes::from_static(b"abcdef")).await.unwrap();
        assert_eq!(
            store.get_range("k", 0..2).await.unwrap(),
            Bytes::from_static(b"ab")
        );
        assert_eq!(
            store.get_range("k", 0..2).await.unwrap(),
            Bytes::from_static(b"ab")
        );
        assert_eq!(
            store.get_range("k", 2..4).await.unwrap(),
            Bytes::from_static(b"cd")
        );
        assert_eq!(counting.reads(), 2); // one per distinct range
    }

    #[tokio::test]
    async fn put_invalidates_cached_entries_for_the_path() {
        let counting = CountingStore::new();
        let store = cached(&counting, 1024);
        store.put("k", Bytes::from_static(b"old")).await.unwrap();
        assert_eq!(store.get("k").await.unwrap(), Bytes::from_static(b"old"));
        store.put("k", Bytes::from_static(b"new")).await.unwrap();
        assert_eq!(store.get("k").await.unwrap(), Bytes::from_static(b"new"));
        assert_eq!(counting.reads(), 2);
    }

    #[test]
    fn lru_evicts_the_least_recently_used_entry() {
        let cache = MemoryCache::new(8);
        let key = |p: &str| CacheKey {
            path: p.into(),
            range: None,
        };
        cache.insert(key("a"), Bytes::from_static(b"aaaa"));
        cache.insert(key("b"), Bytes::from_static(b"bbbb"));
        assert!(cache.get(&key("a")).is_some()); // touch a → b is now LRU
        cache.insert(key("c"), Bytes::from_static(b"cccc"));
        assert!(cache.get(&key("a")).is_some());
        assert!(cache.get(&key("b")).is_none(), "b was least recently used");
        assert!(cache.get(&key("c")).is_some());
    }

    #[test]
    fn oversized_values_are_never_cached() {
        let cache = MemoryCache::new(4);
        let key = CacheKey {
            path: "big".into(),
            range: None,
        };
        cache.insert(key.clone(), Bytes::from_static(b"too large"));
        assert!(cache.get(&key).is_none());
    }

    #[test]
    fn reinserting_a_key_replaces_its_byte_accounting() {
        let cache = MemoryCache::new(8);
        let key = |p: &str| CacheKey {
            path: p.into(),
            range: None,
        };
        cache.insert(key("a"), Bytes::from_static(b"aaaa"));
        cache.insert(key("a"), Bytes::from_static(b"aa")); // shrink in place
        cache.insert(key("b"), Bytes::from_static(b"bbbb"));
        // 2 + 4 = 6 <= 8: both fit only if the old 4-byte "a" was released.
        assert!(cache.get(&key("a")).is_some());
        assert!(cache.get(&key("b")).is_some());
    }

    #[tokio::test]
    async fn list_bypasses_the_cache() {
        let counting = CountingStore::new();
        let store = cached(&counting, 1024);
        store.put("p/x", Bytes::from_static(b"1")).await.unwrap();
        assert_eq!(store.list("p").await.unwrap(), vec!["p/x".to_string()]);
        store.put("p/y", Bytes::from_static(b"2")).await.unwrap();
        // A fresh manifest must be visible immediately — list is never cached.
        assert_eq!(
            store.list("p").await.unwrap(),
            vec!["p/x".to_string(), "p/y".to_string()]
        );
    }
}
