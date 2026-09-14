//! I/O-free engine metrics (Task 12, spec §12, decision 10): plain atomics
//! incremented at the writer's recording points, plus a snapshot type built
//! from those atomics and one in-memory read-lock pass over the queryable
//! inventory (`GraphsState`). Never touches the object store.

use std::sync::atomic::AtomicU64;

/// Writer-owned counters (Task 12). Held behind an `Arc` shared between
/// `WriterState` (which increments them) and `DbInner` (which reads them via
/// `Db::metrics`).
#[derive(Debug, Default)]
pub(crate) struct EngineMetrics {
    pub txs_committed: AtomicU64,
    pub events_committed: AtomicU64,
    /// Pre-durability append failures.
    pub commit_failures: AtomicU64,
    pub flush_blocks: AtomicU64,
    pub flush_failures: AtomicU64,
    pub compaction_runs: AtomicU64,
    pub backpressure_rejections: AtomicU64,
}

/// One cache tier's hit/miss counters, named for the Prometheus `tier`
/// label.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CacheTierStats {
    pub tier: String,
    pub hits: u64,
    pub misses: u64,
}

/// An I/O-free snapshot of the engine's counters and in-memory inventory
/// sizes, returned by `Db::metrics()`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EngineMetricsSnapshot {
    pub txs_committed: u64,
    pub events_committed: u64,
    pub commit_failures: u64,
    pub flush_blocks: u64,
    pub flush_failures: u64,
    pub compaction_runs: u64,
    pub backpressure_rejections: u64,
    pub live_rows: u64,
    pub live_bytes: u64,
    pub persisted_tries: u64,
    /// Σ_scope max(0, tries(scope) − 1) — an I/O-free compaction-debt proxy:
    /// each scope (a table's primary tries, or an adjacency family) with
    /// more than one persisted trie has debt equal to all but its newest.
    pub compaction_debt_tries: u64,
    /// Block data pages read since start, after page pruning. Counts pages the
    /// read paths actually fetched, hit or miss in the block cache.
    pub block_pages_read: u64,
    /// Events materialized from those pages: an anchored decode filters on the
    /// key column before building a row, so rows it rejects are not counted.
    /// Together with `block_pages_read` this is the degree-bound signal — an
    /// anchored lookup averaging ~`PAGE_LIMIT` events per page is scanning
    /// whole pages to answer a point question, which is what makes a
    /// block-resident traversal cost many times a live-resident one. Pair it
    /// with `live_bytes` when reading traversal latency.
    pub block_events_decoded: u64,
    /// Of `block_pages_read`, pages served from the decoded-page cache.
    pub block_pages_cached: u64,
    pub cache_tiers: Vec<CacheTierStats>,
}
