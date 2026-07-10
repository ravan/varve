//! The merged bitemporal scan (spec §10 `BitemporalScan`, v1 shape): an
//! atomic (live, inventory) snapshot, page pruning, one ranged GET per
//! surviving page, per-entity merge in time order, then a single resolution
//! pass across sources via `snapshot_entities`.

use crate::db::{EngineError, Overlay};
use crate::state::{GraphsState, TableKind, EDGES_TABLE};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, RwLock};
use varve_index::{decode_events, snapshot_entities, Event, LabelFilter, Op};
use varve_storage::{keys, ObjectStore};
use varve_types::{Iid, TemporalBounds, Value};

// Public scan primitive keeps graph/table/filter/time/overlay dimensions explicit.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn merged_snapshot(
    state: &Arc<RwLock<GraphsState>>,
    store: &Arc<dyn ObjectStore>,
    graph: &str,
    kind: TableKind,
    label: LabelFilter<'_>,
    bounds: &TemporalBounds,
    iid_point: Option<Iid>,
    overlay: Option<&Overlay>,
) -> Result<Option<RecordBatch>, EngineError> {
    // 1. Atomic snapshot under ONE read lock (decision 8). Live events are
    //    cloned — bounded by max_block_rows; a point lookup clones one
    //    entity. The trie inventory is Arc-cheap.
    let (live_events, tries) = {
        let s = state.read().map_err(|_| EngineError::Poisoned)?;
        let table = s
            .graph(graph)
            .ok_or_else(|| EngineError::UnknownGraph(graph.to_string()))?;
        let core = table.core(kind);
        let live_events: Vec<(Iid, Vec<Event>)> = match &iid_point {
            Some(iid) => core
                .live
                .events_for(iid)
                .map(|events| vec![(*iid, events.to_vec())])
                .unwrap_or_default(),
            None => core
                .live
                .entities()
                .map(|(iid, events)| (*iid, events.to_vec()))
                .collect(),
        };
        (live_events, core.tries.clone())
    };
    let overlay_events: Vec<(Iid, Vec<Event>)> = overlay
        .map(|overlay| {
            let table = overlay.table(kind);
            match &iid_point {
                Some(iid) => table
                    .events_for(iid)
                    .map(|events| vec![(*iid, events.to_vec())])
                    .unwrap_or_default(),
                None => table
                    .entities()
                    .map(|(iid, events)| (*iid, events.to_vec()))
                    .collect(),
            }
        })
        .unwrap_or_default();

    // 2. Persisted events, ascending block order (== time order, decision 9).
    //    An entity's run may span pages within one block, so each trie's
    //    selected/decoded events are collected in file order as one block;
    //    per-entity grouping/reversal/concat with the live tail is
    //    `varve_index::merge_sources`'s job (decision 9), shared with the
    //    flush-equivalence property test.
    let mut blocks: Vec<Vec<Event>> = Vec::new();
    for trie in &tries {
        let data_key = keys::data_key(graph, kind.name(), &trie.entry.trie_key);
        let mut block_events: Vec<Event> = Vec::new();
        for page in trie
            .pages
            .iter()
            .filter(|p| p.selected(bounds, iid_point.as_ref()))
        {
            let bytes = store
                .get_range(&data_key, page.offset..page.offset + page.len)
                .await?;
            for event in decode_events(&bytes)? {
                if iid_point.as_ref().is_none_or(|iid| event.iid == *iid) {
                    block_events.push(event);
                }
            }
        }
        blocks.push(block_events);
    }

    // 3. Merge persisted blocks with the committed live tail and then the
    //    statement overlay, which is newest within a program.
    let merged = varve_index::merge_sources(blocks, live_events.into_iter().chain(overlay_events));

    Ok(snapshot_entities(
        merged.iter().map(|(iid, events)| (*iid, events.as_slice())),
        label,
        bounds,
    )?)
}

/// Which adjacency family a lookup traverses: `Out` follows `src → dst` (the
/// src-sorted `adj-out` family), `In` follows `dst → src` (the dst-sorted
/// `adj-in` family). `resolve_delete`'s still-connected check (task 7) drives
/// both variants via `incident_edges`; `PathExpand` (task 8) is
/// `edge_adjacency`'s first production consumer.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum AdjDirection {
    Out,
    In,
}

/// One traversable edge at the query bounds: `node` is the anchor-side
/// endpoint (src for `Out`, dst for `In`), `neighbor` the other endpoint,
/// `edge` the edge's own iid.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) struct AdjacencyEntry {
    pub node: Iid,
    pub neighbor: Iid,
    pub edge: Iid,
}

fn traversal_budget_exhausted(kind: &str, budget: usize) -> EngineError {
    EngineError::Plan(varve_plan::PlanError::DataFusion(
        DataFusionError::ResourcesExhausted(format!(
            "traversal {kind} budget {budget} exceeded; make the query more selective"
        )),
    ))
}

/// Visible-edge adjacency at `bounds` (slice 6, decision 11): the live edge
/// tail (narrowed by the live adjacency views when `anchor` is `Some`) merged
/// with the persisted adjacency family, whose pages are pruned by the anchor
/// via `PageMeta::selected` (the anchor is the family's sort-key point) and
/// then filtered exactly. Each surviving edge is resolved at `bounds`; edges
/// whose visible version is a `Put` become one entry when `label` is `None`
/// (any label matches — the label-blind variant used by `incident_edges`) or
/// when `label` is `Some` and one of the edge's labels matches it exactly.
/// Output is sorted by `(node, neighbor, edge)` and — because `merge_sources`
/// keys by edge iid — carries exactly one entry per edge. Deterministic.
///
/// Correctness contract: for any anchor, the anchored result equals the full
/// (`anchor == None`) result filtered to that node.
///
/// When `collect_events` is set, the surviving (matched) edges' merged event
/// lists are returned alongside the entries — the raw rows the task-12
/// reachable-edge batch is built from (`snapshot_entities`). The two thin
/// public wrappers pass `false` so the full-scan callers never pay the clone.
#[allow(clippy::too_many_arguments)]
async fn edge_adjacency_impl(
    state: &Arc<RwLock<GraphsState>>,
    store: &Arc<dyn ObjectStore>,
    graph: &str,
    label: Option<&str>,
    props: &[(String, Value)],
    direction: AdjDirection,
    anchor: Option<Iid>,
    bounds: &TemporalBounds,
    collect_events: bool,
    adjacency_budget: Option<usize>,
    overlay: Option<&Overlay>,
) -> Result<(Vec<AdjacencyEntry>, Vec<(Iid, Vec<Event>)>), EngineError> {
    // 1. One read lock: live edge events (narrowed by the live adjacency
    //    views when anchored) + the persisted family's trie list.
    let (live_events, tries) = {
        let s = state.read().map_err(|_| EngineError::Poisoned)?;
        let table = s
            .graph(graph)
            .ok_or_else(|| EngineError::UnknownGraph(graph.to_string()))?;
        let live = &table.edges.live;
        let live_events: Vec<(Iid, Vec<Event>)> = match anchor {
            Some(node) => {
                let edge_iids: Vec<Iid> = match direction {
                    AdjDirection::Out => live.out_edges(&node).cloned().collect(),
                    AdjDirection::In => live.in_edges(&node).cloned().collect(),
                };
                edge_iids
                    .into_iter()
                    .filter_map(|e| live.events_for(&e).map(|ev| (e, ev.to_vec())))
                    .collect()
            }
            None => live
                .entities()
                .map(|(iid, ev)| (*iid, ev.to_vec()))
                .collect(),
        };
        let tries = match direction {
            AdjDirection::Out => table.adj_out.clone(),
            AdjDirection::In => table.adj_in.clone(),
        };
        (live_events, tries)
    };
    let overlay_events: Vec<(Iid, Vec<Event>)> = overlay
        .map(|overlay| {
            let live = &overlay.edges;
            match anchor {
                Some(node) => {
                    let edge_iids: Vec<Iid> = match direction {
                        AdjDirection::Out => live.out_edges(&node).cloned().collect(),
                        AdjDirection::In => live.in_edges(&node).cloned().collect(),
                    };
                    edge_iids
                        .into_iter()
                        .filter_map(|e| live.events_for(&e).map(|ev| (e, ev.to_vec())))
                        .collect()
                }
                None => live
                    .entities()
                    .map(|(iid, ev)| (*iid, ev.to_vec()))
                    .collect(),
            }
        })
        .unwrap_or_default();

    // 2. Persisted family pages, pruned by the anchor as the sort-key point
    //    (decision 4 — min_iid/max_iid on adj pages record src/dst), then
    //    filtered exactly to the anchor endpoint.
    let family = match direction {
        AdjDirection::Out => varve_storage::ADJ_OUT,
        AdjDirection::In => varve_storage::ADJ_IN,
    };
    let mut blocks: Vec<Vec<Event>> = Vec::new();
    for trie in &tries {
        let key = keys::adj_data_key(graph, EDGES_TABLE, family, &trie.entry.trie_key);
        let mut block_events = Vec::new();
        for page in trie
            .pages
            .iter()
            .filter(|p| p.selected(bounds, anchor.as_ref()))
        {
            let bytes = store
                .get_range(&key, page.offset..page.offset + page.len)
                .await?;
            for event in decode_events(&bytes)? {
                let key_iid = match direction {
                    AdjDirection::Out => event.src,
                    AdjDirection::In => event.dst,
                };
                if anchor.is_none() || key_iid == anchor {
                    block_events.push(event);
                }
            }
        }
        blocks.push(block_events);
    }

    // 3. Merge (one Vec per edge iid), resolve per edge, keep edges whose
    //    visible version is a Put carrying `label`, emit sorted entries.
    let merged = varve_index::merge_sources(blocks, live_events.into_iter().chain(overlay_events));
    let mut entries = Vec::new();
    let mut edge_events = Vec::new();
    for (edge, events) in &merged {
        let visible = varve_index::resolve(events, bounds);
        // A hop matches when some visible version is a `Put` carrying `label`
        // (or any label when `None`) AND every requested inline prop equals a
        // present key in that version's doc (decision 13: a missing key is a
        // non-match, never a wildcard).
        let matched = visible.iter().any(|v| match &v.event.op {
            Op::Put { labels, doc } => {
                label.is_none_or(|l| labels.iter().any(|x| x == l))
                    && props
                        .iter()
                        .all(|(k, want)| doc.get(k).is_some_and(|got| got == want))
            }
            _ => false,
        });
        if !matched {
            continue;
        }
        let (Some(src), Some(dst)) = (events[0].src, events[0].dst) else {
            return Err(EngineError::Index(varve_index::IndexError::Codec(
                "edge event missing endpoints".into(),
            )));
        };
        let (node, neighbor) = match direction {
            AdjDirection::Out => (src, dst),
            AdjDirection::In => (dst, src),
        };
        if let Some(budget) = adjacency_budget {
            if entries.len() >= budget {
                return Err(traversal_budget_exhausted("adjacency", budget));
            }
        }
        entries.push(AdjacencyEntry {
            node,
            neighbor,
            edge: *edge,
        });
        if collect_events {
            edge_events.push((*edge, events.clone()));
        }
    }
    entries.sort_by_key(|e| (e.node, e.neighbor, e.edge));
    Ok((entries, edge_events))
}

/// Label-filtered adjacency lookup: kept alongside `incident_edges` so T8/T9
/// call sites read clearly. `PathExpand` (task 8) is its first production
/// consumer; slice 6 ships and tests the lookup itself.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn edge_adjacency(
    state: &Arc<RwLock<GraphsState>>,
    store: &Arc<dyn ObjectStore>,
    graph: &str,
    label: &str,
    props: &[(String, Value)],
    direction: AdjDirection,
    anchor: Option<Iid>,
    bounds: &TemporalBounds,
    adjacency_budget: Option<usize>,
    overlay: Option<&Overlay>,
) -> Result<Vec<AdjacencyEntry>, EngineError> {
    Ok(edge_adjacency_impl(
        state,
        store,
        graph,
        Some(label),
        props,
        direction,
        anchor,
        bounds,
        false,
        adjacency_budget,
        overlay,
    )
    .await?
    .0)
}

/// Label-blind variant of `edge_adjacency`: every visible `Put` edge
/// incident to `node` counts, regardless of its label. `resolve_delete`'s
/// still-connected check and DETACH DELETE's cascade (task 7) must catch
/// edges of ANY label, so they scan through this instead of
/// `edge_adjacency`'s label filter.
pub(crate) async fn incident_edges(
    state: &Arc<RwLock<GraphsState>>,
    store: &Arc<dyn ObjectStore>,
    graph: &str,
    direction: AdjDirection,
    node: Iid,
    bounds: &TemporalBounds,
    overlay: Option<&Overlay>,
) -> Result<Vec<AdjacencyEntry>, EngineError> {
    Ok(edge_adjacency_impl(
        state,
        store,
        graph,
        None,
        &[],
        direction,
        Some(node),
        bounds,
        false,
        None,
        overlay,
    )
    .await?
    .0)
}

/// One BFS level's traversal (task 12): the edge family (`label` + `direction`)
/// and inline `props` to match at that level. For a fixed k-hop path this is
/// that level's hop; for a quantified `{_,max}` hop it is the single hop,
/// repeated `max` times.
#[derive(Clone, Copy)]
pub(crate) struct HopSpec<'a> {
    pub label: &'a str,
    pub props: &'a [(String, Value)],
    pub direction: AdjDirection,
}

/// The reachable-edge set from a bounded BFS: adjacency entries (deduped by
/// edge iid, for building an `EdgeAdjacency`) and, when a batch label was
/// requested, the reachable-edge snapshot batch (identical schema to
/// `merged_snapshot(TableKind::Edges, label, …)`, since both resolve the same
/// per-edge event lists through `snapshot_entities`).
pub(crate) struct ReachableEdges {
    pub entries: Vec<AdjacencyEntry>,
    pub batch: Option<RecordBatch>,
}

/// Anchor-reachable edge pruning (task 12): a bounded BFS from `anchor`
/// collecting the SUPERSET of every edge that can lie on a qualifying path
/// within the hop bound. `hops[i]` drives level `i`; the family MUST be
/// homogeneous across levels (same label + direction + props — the only shape
/// `Db::query`'s fast path selects), so a node is expanded at most once
/// (`expanded`) and the collected edge set is independent of visit order.
///
/// Superset proof (homogeneous hops, `hops.len()` levels): any qualifying path
/// `anchor = n0 -e0-> n1 … -e_{k-1}-> nk` reaches each `ni` (i < k) at shortest
/// distance `di ≤ i ≤ hops.len()-1`, so `ni` enters the frontier at level
/// `di ≤ hops.len()-1` and is expanded (first time), collecting ALL its
/// matching edges — including `ei`. Thus every edge on a qualifying path is
/// collected; extra edges are harmless (the join keys + per-element predicates
/// select the right rows). Each per-node lookup reuses the anchored
/// `edge_adjacency_impl`, so visibility is resolved at `bounds` exactly as the
/// full scan resolves it — bitemporal correctness is preserved.
///
/// `batch_label` selects the single label the batch is built under (all fixed
/// `Edge` specs share it); pass `None` to skip the batch (a quantified hop
/// needs only `entries`, and skipping avoids cloning every surviving edge's
/// event list).
// Fast-path BFS needs graph, hop, bounds, and optional overlay context together.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn reachable_edges(
    state: &Arc<RwLock<GraphsState>>,
    store: &Arc<dyn ObjectStore>,
    graph: &str,
    anchor: Iid,
    hops: &[HopSpec<'_>],
    batch_label: Option<&str>,
    bounds: &TemporalBounds,
    node_budget: usize,
    adjacency_budget: usize,
    overlay: Option<&Overlay>,
) -> Result<ReachableEdges, EngineError> {
    let mut frontier: Vec<Iid> = vec![anchor];
    let mut expanded: HashSet<Iid> = HashSet::new();
    let mut entries: Vec<AdjacencyEntry> = Vec::new();
    // One event list per surviving edge, deduped by iid (a homogeneous walk
    // can re-encounter the same edge). `BTreeMap` keeps batch rows in a stable
    // iid order (deterministic output).
    let mut edge_events: BTreeMap<Iid, Vec<Event>> = BTreeMap::new();
    let collect = batch_label.is_some();
    for hop in hops {
        if frontier.is_empty() {
            break;
        }
        let mut next_frontier: Vec<Iid> = Vec::new();
        let mut next_seen: HashSet<Iid> = HashSet::new();
        for node in std::mem::take(&mut frontier) {
            if !expanded.insert(node) {
                continue;
            }
            if expanded.len() > node_budget {
                return Err(traversal_budget_exhausted("node", node_budget));
            }
            let (node_entries, node_edges) = edge_adjacency_impl(
                state,
                store,
                graph,
                Some(hop.label),
                hop.props,
                hop.direction,
                Some(node),
                bounds,
                collect,
                Some(adjacency_budget),
                overlay,
            )
            .await?;
            if entries.len().saturating_add(node_entries.len()) > adjacency_budget {
                return Err(traversal_budget_exhausted("adjacency", adjacency_budget));
            }
            for e in &node_entries {
                if next_seen.insert(e.neighbor) {
                    next_frontier.push(e.neighbor);
                }
            }
            for (edge, events) in node_edges {
                edge_events.entry(edge).or_insert(events);
            }
            entries.extend(node_entries);
        }
        frontier = next_frontier;
    }
    // A homogeneous walk expands each node once, so entries are already unique
    // per edge; sort+dedup keeps the invariant explicit and deterministic.
    entries.sort_by_key(|e| (e.node, e.neighbor, e.edge));
    entries.dedup();

    let batch = match batch_label {
        Some(label) => snapshot_entities(
            edge_events
                .iter()
                .map(|(iid, events)| (*iid, events.as_slice())),
            LabelFilter::Single(label),
            bounds,
        )?,
        None => None,
    };
    Ok(ReachableEdges { entries, batch })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{PersistedTrie, TableKind, TableState, DEFAULT_GRAPH, NODES_TABLE};
    use bytes::Bytes;
    use std::ops::Range;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, RwLock};
    use varve_index::block::encode_block;
    use varve_index::{encode_sorted_events_by, Event, LiveTable, Op, SortOrder};
    use varve_storage::{
        keys, memory_store, ConditionalStore, ObjectStore, StorageError, TrieEntry,
    };
    use varve_types::{Doc, Iid, Instant, TemporalBounds, TemporalDimension, Value};

    const EOT: Instant = Instant::END_OF_TIME;

    struct CountingStore {
        inner: Arc<dyn ObjectStore>,
        range_reads: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl ObjectStore for CountingStore {
        async fn put(&self, key: &str, bytes: Bytes) -> Result<(), StorageError> {
            self.inner.put(key, bytes).await
        }

        async fn get(&self, key: &str) -> Result<Bytes, StorageError> {
            self.inner.get(key).await
        }

        async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Bytes, StorageError> {
            self.range_reads.fetch_add(1, Ordering::SeqCst);
            self.inner.get_range(key, range).await
        }

        async fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
            self.inner.list(prefix).await
        }

        async fn delete(&self, key: &str) -> Result<(), StorageError> {
            self.inner.delete(key).await
        }

        fn conditional(&self) -> Option<&dyn ConditionalStore> {
            self.inner.conditional()
        }
    }

    fn iid(n: u8) -> Iid {
        Iid::derive(DEFAULT_GRAPH, NODES_TABLE, &[n])
    }

    fn raw_iid(first: u8) -> Iid {
        Iid::from_bytes([first, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
    }

    fn us(n: i64) -> Instant {
        Instant::from_micros(n)
    }

    fn put(entity: u8, sf: i64, name: &str) -> Event {
        let mut doc = Doc::new();
        doc.insert("name".into(), Value::Str(name.into()));
        Event {
            iid: iid(entity),
            system_from: us(sf),
            valid_from: us(sf),
            valid_to: EOT,
            src: None,
            dst: None,
            op: Op::Put {
                labels: vec!["P".into()],
                doc,
            },
        }
    }

    fn at(n: i64) -> TemporalBounds {
        TemporalBounds {
            valid: TemporalDimension::at(us(n)),
            system: TemporalDimension::at(us(n)),
        }
    }

    /// Flushes `persisted` into block 0 on a memory store and stages
    /// `live_events` in the live table — the exact state a real flush
    /// produces (Task 10 automates this path).
    async fn seeded(
        persisted: &[Event],
        live_events: &[Event],
    ) -> (Arc<RwLock<GraphsState>>, Arc<dyn ObjectStore>) {
        let store = memory_store();
        let mut state = TableState::new();
        if !persisted.is_empty() {
            let mut table = LiveTable::new();
            for e in persisted {
                table.append(e.clone()).unwrap();
            }
            let block = encode_block(&table, 2).unwrap(); // small pages → splits
            let trie_key = keys::l0_trie_key(0);
            let row_count = block.pages.iter().map(|p| p.rows).sum();
            let data_len = block.data.len() as u64;
            store
                .put(
                    &keys::data_key(DEFAULT_GRAPH, NODES_TABLE, &trie_key),
                    block.data.into(),
                )
                .await
                .unwrap();
            store
                .put(
                    &keys::meta_key(DEFAULT_GRAPH, NODES_TABLE, &trie_key),
                    block.meta.into(),
                )
                .await
                .unwrap();
            state.nodes.tries.push(PersistedTrie {
                entry: TrieEntry {
                    trie_key,
                    row_count,
                    data_len,
                },
                pages: Arc::new(block.pages),
            });
        }
        for e in live_events {
            state.nodes.live.append(e.clone()).unwrap();
        }
        {
            let mut graphs = GraphsState::new();
            graphs.graphs.insert(DEFAULT_GRAPH.to_string(), state);
            (Arc::new(RwLock::new(graphs)), store)
        }
    }

    fn names(batch: &Option<datafusion::arrow::record_batch::RecordBatch>) -> Vec<String> {
        use datafusion::arrow::array::{Array, StringArray};
        let Some(batch) = batch else {
            return vec![];
        };
        let col: &StringArray = batch
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        let mut out: Vec<String> = (0..col.len()).map(|i| col.value(i).to_string()).collect();
        out.sort();
        out
    }

    #[tokio::test]
    async fn persisted_only_events_are_visible() {
        let (state, store) = seeded(&[put(1, 1, "Ada"), put(2, 2, "Bob")], &[]).await;
        let batch = merged_snapshot(
            &state,
            &store,
            DEFAULT_GRAPH,
            TableKind::Nodes,
            LabelFilter::Single("P"),
            &at(10),
            None,
            None,
        )
        .await
        .unwrap();
        assert_eq!(names(&batch), vec!["Ada", "Bob"]);
    }

    #[tokio::test]
    async fn point_lookup_skips_pages_outside_trie_path() {
        fn raw_put(first: u8, sf: i64, name: &str) -> Event {
            let mut doc = Doc::new();
            doc.insert("name".into(), Value::Str(name.into()));
            Event {
                iid: raw_iid(first),
                system_from: us(sf),
                valid_from: us(sf),
                valid_to: EOT,
                src: None,
                dst: None,
                op: Op::Put {
                    labels: vec!["P".into()],
                    doc,
                },
            }
        }

        let rows = vec![raw_put(0x00, 1, "Ada"), raw_put(0x40, 2, "Bob")];
        let target = rows[1].iid;
        let block = encode_sorted_events_by(&rows, 1, SortOrder::ByIid, 1).unwrap();
        assert_eq!(block.pages.len(), 2);
        assert_eq!(block.pages[0].path, vec![0]);
        assert_eq!(block.pages[1].path, vec![1]);

        let mut pages = block.pages.clone();
        pages[0].min_iid = raw_iid(0x00);
        pages[0].max_iid = raw_iid(0xff);

        let inner = memory_store();
        let trie_key = "l01-rc-b00".to_string();
        inner
            .put(
                &keys::data_key(DEFAULT_GRAPH, NODES_TABLE, &trie_key),
                block.data.into(),
            )
            .await
            .unwrap();
        let range_reads = Arc::new(AtomicUsize::new(0));
        let store: Arc<dyn ObjectStore> = Arc::new(CountingStore {
            inner,
            range_reads: range_reads.clone(),
        });

        let mut table = TableState::new();
        table.nodes.tries.push(PersistedTrie {
            entry: TrieEntry {
                trie_key,
                row_count: rows.len() as u64,
                data_len: pages.iter().map(|page| page.len).sum(),
            },
            pages: Arc::new(pages),
        });
        let mut graphs = GraphsState::new();
        graphs.graphs.insert(DEFAULT_GRAPH.to_string(), table);
        let state = Arc::new(RwLock::new(graphs));

        let batch = merged_snapshot(
            &state,
            &store,
            DEFAULT_GRAPH,
            TableKind::Nodes,
            LabelFilter::Single("P"),
            &at(10),
            Some(target),
            None,
        )
        .await
        .unwrap();

        assert_eq!(names(&batch), vec!["Bob"]);
        assert_eq!(range_reads.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn live_put_supersedes_persisted_version() {
        // Cross-source resolution: the persisted "Ada" must get system_to = 5.
        let (state, store) = seeded(&[put(1, 1, "Ada")], &[put(1, 5, "Adele")]).await;
        let now = merged_snapshot(
            &state,
            &store,
            DEFAULT_GRAPH,
            TableKind::Nodes,
            LabelFilter::Single("P"),
            &at(10),
            None,
            None,
        )
        .await
        .unwrap();
        assert_eq!(names(&now), vec!["Adele"]);
        // Time travel to before the live correction still sees the old version.
        let before = merged_snapshot(
            &state,
            &store,
            DEFAULT_GRAPH,
            TableKind::Nodes,
            LabelFilter::Single("P"),
            &at(3),
            None,
            None,
        )
        .await
        .unwrap();
        assert_eq!(names(&before), vec!["Ada"]);
    }

    #[tokio::test]
    async fn live_delete_hides_persisted_put() {
        let delete = Event {
            iid: iid(1),
            system_from: us(5),
            valid_from: us(5),
            valid_to: EOT,
            src: None,
            dst: None,
            op: Op::Delete,
        };
        let (state, store) = seeded(&[put(1, 1, "Ada")], std::slice::from_ref(&delete)).await;
        let now = merged_snapshot(
            &state,
            &store,
            DEFAULT_GRAPH,
            TableKind::Nodes,
            LabelFilter::Single("P"),
            &at(10),
            None,
            None,
        )
        .await
        .unwrap();
        assert!(now.is_none());
        let before = merged_snapshot(
            &state,
            &store,
            DEFAULT_GRAPH,
            TableKind::Nodes,
            LabelFilter::Single("P"),
            &at(3),
            None,
            None,
        )
        .await
        .unwrap();
        assert_eq!(names(&before), vec!["Ada"]);
    }

    #[tokio::test]
    async fn live_erase_hides_persisted_history_everywhere() {
        let erase = Event {
            iid: iid(1),
            system_from: us(5),
            valid_from: Instant::MIN,
            valid_to: EOT,
            src: None,
            dst: None,
            op: Op::Erase,
        };
        let (state, store) = seeded(&[put(1, 1, "Ada")], std::slice::from_ref(&erase)).await;
        // Even time-traveling BEFORE the erase: gone (slice-2 GDPR semantics).
        let before = merged_snapshot(
            &state,
            &store,
            DEFAULT_GRAPH,
            TableKind::Nodes,
            LabelFilter::Single("P"),
            &at(3),
            None,
            None,
        )
        .await
        .unwrap();
        assert!(before.is_none());
    }

    #[tokio::test]
    async fn iid_point_returns_only_that_entity() {
        let (state, store) =
            seeded(&[put(1, 1, "Ada"), put(2, 2, "Bob"), put(3, 3, "Cyd")], &[]).await;
        let batch = merged_snapshot(
            &state,
            &store,
            DEFAULT_GRAPH,
            TableKind::Nodes,
            LabelFilter::Single("P"),
            &at(10),
            Some(iid(2)),
            None,
        )
        .await
        .unwrap();
        assert_eq!(names(&batch), vec!["Bob"]);
    }

    #[tokio::test]
    async fn merged_scan_equals_never_flushed_reference() {
        // Same 6 events: all live vs split 3 persisted / 3 live — identical batch.
        let events = [
            put(1, 1, "a1"),
            put(2, 2, "b1"),
            put(1, 3, "a2"),
            put(3, 4, "c1"),
            put(2, 5, "b2"),
            put(1, 6, "a3"),
        ];
        let (all_live, store_a) = seeded(&[], &events).await;
        let (split, store_b) = seeded(&events[..3], &events[3..]).await;
        for bounds in [at(10), at(4), at(2)] {
            let reference = merged_snapshot(
                &all_live,
                &store_a,
                DEFAULT_GRAPH,
                TableKind::Nodes,
                LabelFilter::Single("P"),
                &bounds,
                None,
                None,
            )
            .await
            .unwrap();
            let merged = merged_snapshot(
                &split,
                &store_b,
                DEFAULT_GRAPH,
                TableKind::Nodes,
                LabelFilter::Single("P"),
                &bounds,
                None,
                None,
            )
            .await
            .unwrap();
            assert_eq!(reference, merged);
        }
    }
}
