use std::collections::BTreeMap;
use std::sync::Arc;
use varve_index::block::PageMeta;
use varve_index::LiveTable;
use varve_storage::TrieEntry;

pub(crate) const DEFAULT_GRAPH: &str = "default";
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
}

pub(crate) struct GraphsState {
    pub graphs: BTreeMap<String, TableState>,
}

impl GraphsState {
    pub fn new() -> GraphsState {
        let mut graphs = BTreeMap::new();
        graphs.insert(DEFAULT_GRAPH.to_string(), TableState::new());
        graphs.insert(META_GRAPH.to_string(), TableState::new());
        GraphsState { graphs }
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
}
