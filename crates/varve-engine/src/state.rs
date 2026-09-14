use crate::page_cache::{PageCache, DEFAULT_PAGE_CACHE_BYTES};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use varve_index::block::{LabelIndex, PageMeta};
use varve_index::LiveTable;
use varve_storage::TrieEntry;
use varve_types::Iid;

pub const DEFAULT_GRAPH: &str = "default";
pub(crate) const META_GRAPH: &str = "__meta";
/// The two v1 tables (spec §5.1). Nodes carry entities; edges carry
/// relationships with `src`/`dst` endpoints (slice 6).
pub(crate) const NODES_TABLE: &str = "nodes";
pub(crate) const EDGES_TABLE: &str = "edges";

/// Which of the two v1 tables a scan/flush/effect targets. Kept as an enum
/// (rather than a bare `&str`) so the writer, scan, and flush paths route by
/// the same closed set and derive object keys via [`TableKind::name`].
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum TableKind {
    Nodes,
    Edges,
}

impl TableKind {
    pub fn name(self) -> &'static str {
        match self {
            TableKind::Nodes => NODES_TABLE,
            TableKind::Edges => EDGES_TABLE,
        }
    }
}

/// One persisted L0 trie: its manifest entry plus the decoded page index.
/// Holding the decoded meta here is the spec §9 "footer cache" — meta
/// objects are fetched once (at flush or recovery), never per query.
#[derive(Clone)]
pub(crate) struct PersistedTrie {
    pub entry: TrieEntry,
    pub pages: Arc<Vec<PageMeta>>,
    /// `None` for adjacency families and for blocks written before the
    /// index existed (a labelled scan then falls back to a full scan).
    pub labels: Option<Arc<LabelIndex>>,
}

/// One table's queryable state: the live (unflushed) tail plus the
/// persisted-trie inventory, in ascending block order (== time order).
pub(crate) struct TableCore {
    pub live: LiveTable,
    pub tries: Vec<PersistedTrie>,
}

impl TableCore {
    pub fn new() -> TableCore {
        TableCore {
            live: LiveTable::new(),
            tries: Vec::new(),
        }
    }
}

/// The queryable state of the whole database: one [`TableCore`] per v1 table
/// plus the persisted adjacency families (edges only). ONE lock over all of
/// it (slice-4 plan, decision 8): flush pushes tries and resets the live
/// tails under a single write lock, queries snapshot under a single read
/// lock — flushed events can never be observed in neither or both sources.
pub(crate) struct TableState {
    pub nodes: TableCore,
    pub edges: TableCore,
    /// Persisted adjacency families (edges only; populated from Task 6). Held
    /// alongside the primary edge tries so a single write lock swaps both.
    pub adj_out: Vec<PersistedTrie>,
    pub adj_in: Vec<PersistedTrie>,
}

impl TableState {
    pub fn new() -> TableState {
        TableState {
            nodes: TableCore::new(),
            edges: TableCore::new(),
            adj_out: Vec::new(),
            adj_in: Vec::new(),
        }
    }

    pub fn core(&self, kind: TableKind) -> &TableCore {
        match kind {
            TableKind::Nodes => &self.nodes,
            TableKind::Edges => &self.edges,
        }
    }

    pub fn core_mut(&mut self, kind: TableKind) -> &mut TableCore {
        match kind {
            TableKind::Nodes => &mut self.nodes,
            TableKind::Edges => &mut self.edges,
        }
    }

    /// Total unflushed rows across both tables — the writer's block-flush
    /// size trigger.
    pub fn live_rows(&self) -> usize {
        self.nodes.live.event_count() + self.edges.live.event_count()
    }

    /// Total unflushed approximate bytes across both tables — the writer's
    /// memory-watermark flush trigger (Task 11). Never used for correctness.
    pub fn live_bytes(&self) -> usize {
        self.nodes.live.approx_bytes() + self.edges.live.approx_bytes()
    }
}

/// Block read-path counters: how much a query actually pulled out of flushed
/// blocks. `block_pages_read` counts data pages that survived page pruning;
/// `block_events_decoded` counts the events those pages MATERIALIZED — an
/// anchored decode applies its key filter inside the decode, so a rejected row
/// is never built and never counted. The ratio between the two is therefore
/// the degree-bound invariant: an anchored lookup materializes its own degree,
/// not [`varve_types::PAGE_LIMIT`] rows per page
/// (`docs/plans/2026-07-28-degree-bound-lookups.md`). Plain `Relaxed` atomics,
/// read under the same lock queries already take, so they cost nothing and
/// never touch the object store.
#[derive(Debug, Default)]
pub(crate) struct ScanStats {
    pub block_pages_read: AtomicU64,
    pub block_events_decoded: AtomicU64,
    /// Pages served from the decoded-page cache ([`crate::page_cache`]):
    /// counted in `block_pages_read` too (the page WAS read), but their
    /// events are not decoded and so never reach `block_events_decoded`.
    pub block_pages_cached: AtomicU64,
}

impl ScanStats {
    /// Records one page read that yielded `events` materialized events.
    pub fn record_page(&self, events: usize) {
        self.block_pages_read.fetch_add(1, Ordering::Relaxed);
        self.block_events_decoded
            .fetch_add(events as u64, Ordering::Relaxed);
    }

    /// Records one page served from the decoded-page cache: no decode.
    pub fn record_cached_page(&self) {
        self.block_pages_read.fetch_add(1, Ordering::Relaxed);
        self.block_pages_cached.fetch_add(1, Ordering::Relaxed);
    }
}

pub(crate) struct GraphsState {
    pub graphs: BTreeMap<String, TableState>,
    pub catalog_graphs: BTreeMap<Iid, String>,
    /// Bumped on every effect applied to the reserved `__security` graph
    /// (writer apply and follower replay alike) — the exact invalidation key
    /// for the per-subject [`crate::security::SecurityContext`] cache.
    pub security_epoch: u64,
    /// Shared out of the read lock (never reset) so every read path can count
    /// its block work without a new parameter on the scan signatures.
    pub scan_stats: Arc<ScanStats>,
    /// Decoded block pages, shared the same way as `scan_stats` so the read
    /// paths reach it through the lock they already take. Sized by
    /// `[query] decoded_page_cache_bytes`; `Db` open paths replace the
    /// default-sized instance before the state is shared.
    pub page_cache: Arc<PageCache>,
}

impl GraphsState {
    pub fn new() -> GraphsState {
        let mut graphs = BTreeMap::new();
        graphs.insert(DEFAULT_GRAPH.to_string(), TableState::new());
        graphs.insert(META_GRAPH.to_string(), TableState::new());
        graphs.insert(
            crate::security::SECURITY_GRAPH.to_string(),
            TableState::new(),
        );
        GraphsState {
            graphs,
            catalog_graphs: BTreeMap::new(),
            security_epoch: 0,
            scan_stats: Arc::new(ScanStats::default()),
            page_cache: Arc::new(PageCache::new(DEFAULT_PAGE_CACHE_BYTES)),
        }
    }

    pub fn graph(&self, graph: &str) -> Option<&TableState> {
        self.graphs.get(graph)
    }

    pub fn graph_mut(&mut self, graph: &str) -> Option<&mut TableState> {
        self.graphs.get_mut(graph)
    }

    pub fn insert_graph(&mut self, graph: String) -> bool {
        self.graphs.insert(graph, TableState::new()).is_none()
    }

    pub fn remove_graph(&mut self, graph: &str) -> Option<TableState> {
        self.graphs.remove(graph)
    }

    pub fn live_rows(&self) -> usize {
        self.graphs.values().map(TableState::live_rows).sum()
    }

    /// Total unflushed approximate bytes across every graph — the writer's
    /// memory-watermark flush trigger (Task 11).
    pub fn live_bytes(&self) -> usize {
        self.graphs.values().map(TableState::live_bytes).sum()
    }
}
