//! Decoded-page cache: the block read paths' second cache tier, keyed by
//! `(object key, page offset)` and holding the page's DECODED events rather
//! than its bytes. `varve_storage::CachedStore` already keeps page bytes in
//! memory, but every scan still paid the Arrow IPC decode (~2 µs/event) to
//! turn those bytes back into `Event`s — on a polled deployment that decode
//! was the whole CPU budget. Block objects are immutable per trie key
//! (append-only store; compaction and GC mint NEW keys), so an entry never
//! goes stale and no invalidation hook is needed: superseded entries age out
//! through the LRU.
//!
//! Only WHOLE pages are cached. A narrowed decode (`decode_events_keyed`
//! under a point/set selector or an adjacency anchor) materializes a subset
//! and is never inserted; a hit under such a selector filters the cached
//! whole page by the selector instead, which is a 16-byte compare per row.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use varve_index::Event;

/// Default `[query] decoded_page_cache_bytes`.
pub(crate) const DEFAULT_PAGE_CACHE_BYTES: usize = 256 * 1024 * 1024;

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct PageKey {
    object: String,
    offset: u64,
}

struct Entry {
    events: Arc<Vec<Event>>,
    bytes: usize,
    last_used: u64,
}

#[derive(Default)]
struct Inner {
    entries: HashMap<PageKey, Entry>,
    bytes: usize,
    tick: u64,
}

/// LRU over an approximate byte budget (`Event::approx_bytes`, the same
/// estimator the flush watermark uses). Eviction scans for the minimum tick —
/// O(n) per eviction, fine at page granularity. A poisoned lock degrades to
/// miss/no-op, never an error. `max_bytes == 0` disables the cache.
pub(crate) struct PageCache {
    max_bytes: usize,
    inner: Mutex<Inner>,
}

impl PageCache {
    pub fn new(max_bytes: usize) -> PageCache {
        PageCache {
            max_bytes,
            inner: Mutex::new(Inner::default()),
        }
    }

    pub fn get(&self, object: &str, offset: u64) -> Option<Arc<Vec<Event>>> {
        if self.max_bytes == 0 {
            return None;
        }
        let Ok(mut inner) = self.inner.lock() else {
            return None;
        };
        inner.tick += 1;
        let tick = inner.tick;
        let key = PageKey {
            object: object.to_string(),
            offset,
        };
        let entry = inner.entries.get_mut(&key)?;
        entry.last_used = tick;
        Some(Arc::clone(&entry.events))
    }

    /// Inserts a WHOLE decoded page. Oversized pages (larger than the whole
    /// budget) are skipped rather than evicting everything else.
    pub fn insert(&self, object: &str, offset: u64, events: Arc<Vec<Event>>) {
        if self.max_bytes == 0 {
            return;
        }
        let bytes = events.iter().map(Event::approx_bytes).sum::<usize>();
        if bytes > self.max_bytes {
            return;
        }
        let Ok(mut inner) = self.inner.lock() else {
            return;
        };
        inner.tick += 1;
        let tick = inner.tick;
        let key = PageKey {
            object: object.to_string(),
            offset,
        };
        if let Some(old) = inner.entries.insert(
            key,
            Entry {
                events,
                bytes,
                last_used: tick,
            },
        ) {
            inner.bytes -= old.bytes;
        }
        inner.bytes += bytes;
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
                inner.bytes -= e.bytes;
            }
        }
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.inner.lock().map(|i| i.entries.len()).unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use varve_index::Op;
    use varve_types::{Doc, Iid, Instant, Value};

    fn page(n: u8, payload: usize) -> Arc<Vec<Event>> {
        let mut doc = Doc::new();
        doc.insert("blob".into(), Value::Str("x".repeat(payload)));
        Arc::new(vec![Event {
            iid: Iid::derive("g", "nodes", &[n]),
            system_from: Instant::from_micros(1),
            valid_from: Instant::from_micros(1),
            valid_to: Instant::END_OF_TIME,
            src: None,
            dst: None,
            op: Op::Put {
                labels: vec!["P".into()],
                doc,
            },
        }])
    }

    #[test]
    fn hit_returns_the_same_arc_and_miss_is_none() {
        let cache = PageCache::new(1 << 20);
        assert!(cache.get("k", 0).is_none());
        let p = page(1, 10);
        cache.insert("k", 0, Arc::clone(&p));
        let hit = cache.get("k", 0).unwrap();
        assert!(Arc::ptr_eq(&hit, &p));
        assert!(cache.get("k", 1).is_none());
        assert!(cache.get("other", 0).is_none());
    }

    #[test]
    fn evicts_least_recently_used_within_budget() {
        // Each page ≈ 64 + 4 + 100 bytes; budget fits two, not three.
        let cache = PageCache::new(400);
        cache.insert("k", 0, page(1, 100));
        cache.insert("k", 1, page(2, 100));
        cache.get("k", 0); // touch page 0 so page 1 is the LRU victim
        cache.insert("k", 2, page(3, 100));
        assert_eq!(cache.len(), 2);
        assert!(cache.get("k", 0).is_some());
        assert!(cache.get("k", 1).is_none());
        assert!(cache.get("k", 2).is_some());
    }

    #[test]
    fn zero_budget_disables_the_cache() {
        let cache = PageCache::new(0);
        cache.insert("k", 0, page(1, 1));
        assert!(cache.get("k", 0).is_none());
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn oversized_page_is_skipped_not_cached() {
        let cache = PageCache::new(100);
        cache.insert("k", 0, page(1, 1000));
        assert!(cache.get("k", 0).is_none());
    }
}
