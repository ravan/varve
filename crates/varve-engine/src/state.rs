use std::sync::Arc;
use varve_index::block::PageMeta;
use varve_index::LiveTable;
use varve_storage::TrieEntry;

/// v1: single default graph (spec §5.1); named graphs land in slice 7.
pub(crate) const DEFAULT_GRAPH: &str = "default";
/// v1: nodes are the only table (edges land in slice 6). Moved from writer.rs.
pub(crate) const NODES_TABLE: &str = "nodes";

/// One persisted L0 trie: its manifest entry plus the decoded page index.
/// Holding the decoded meta here is the spec §9 "footer cache" — meta
/// objects are fetched once (at flush or recovery), never per query.
#[derive(Clone)]
pub(crate) struct PersistedTrie {
    pub entry: TrieEntry,
    pub pages: Arc<Vec<PageMeta>>,
}

/// The queryable state of the nodes table: the live (unflushed) tail plus
/// the persisted-trie inventory, in ascending block order (== time order).
/// ONE lock over both (slice-4 plan, decision 8): flush pushes a trie and
/// resets the live table under a single write lock, queries snapshot both
/// under a single read lock — flushed events can never be observed in
/// neither or both sources.
pub(crate) struct TableState {
    pub live: LiveTable,
    pub tries: Vec<PersistedTrie>,
}

impl TableState {
    pub fn new() -> TableState {
        TableState {
            live: LiveTable::new(),
            tries: Vec::new(),
        }
    }
}
