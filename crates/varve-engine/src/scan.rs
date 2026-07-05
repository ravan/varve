//! The merged bitemporal scan (spec §10 `BitemporalScan`, v1 shape): an
//! atomic (live, inventory) snapshot, page pruning, one ranged GET per
//! surviving page, per-entity merge in time order, then a single resolution
//! pass across sources via `snapshot_entities`.

use crate::db::EngineError;
use crate::state::{TableKind, TableState, DEFAULT_GRAPH, EDGES_TABLE};
use datafusion::arrow::record_batch::RecordBatch;
use std::sync::{Arc, RwLock};
use varve_index::{decode_events, snapshot_entities, Event, Op};
use varve_storage::{keys, ObjectStore};
use varve_types::{Iid, TemporalBounds};

pub(crate) async fn merged_snapshot(
    state: &Arc<RwLock<TableState>>,
    store: &Arc<dyn ObjectStore>,
    kind: TableKind,
    label: &str,
    bounds: &TemporalBounds,
    iid_point: Option<Iid>,
) -> Result<Option<RecordBatch>, EngineError> {
    // 1. Atomic snapshot under ONE read lock (decision 8). Live events are
    //    cloned — bounded by max_block_rows; a point lookup clones one
    //    entity. The trie inventory is Arc-cheap.
    let (live_events, tries) = {
        let s = state.read().map_err(|_| EngineError::Poisoned)?;
        let core = s.core(kind);
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

    // 2. Persisted events, ascending block order (== time order, decision 9).
    //    An entity's run may span pages within one block, so each trie's
    //    selected/decoded events are collected in file order as one block;
    //    per-entity grouping/reversal/concat with the live tail is
    //    `varve_index::merge_sources`'s job (decision 9), shared with the
    //    flush-equivalence property test.
    let mut blocks: Vec<Vec<Event>> = Vec::new();
    for trie in &tries {
        let data_key = keys::data_key(DEFAULT_GRAPH, kind.name(), &trie.entry.trie_key);
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

    // 3. Merge persisted blocks with the live tail (live is newest —
    //    appended after every persisted source).
    let merged = varve_index::merge_sources(blocks, live_events);

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
async fn edge_adjacency_impl(
    state: &Arc<RwLock<TableState>>,
    store: &Arc<dyn ObjectStore>,
    label: Option<&str>,
    direction: AdjDirection,
    anchor: Option<Iid>,
    bounds: &TemporalBounds,
) -> Result<Vec<AdjacencyEntry>, EngineError> {
    // 1. One read lock: live edge events (narrowed by the live adjacency
    //    views when anchored) + the persisted family's trie list.
    let (live_events, tries) = {
        let s = state.read().map_err(|_| EngineError::Poisoned)?;
        let live = &s.edges.live;
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
            AdjDirection::Out => s.adj_out.clone(),
            AdjDirection::In => s.adj_in.clone(),
        };
        (live_events, tries)
    };

    // 2. Persisted family pages, pruned by the anchor as the sort-key point
    //    (decision 4 — min_iid/max_iid on adj pages record src/dst), then
    //    filtered exactly to the anchor endpoint.
    let family = match direction {
        AdjDirection::Out => varve_storage::ADJ_OUT,
        AdjDirection::In => varve_storage::ADJ_IN,
    };
    let mut blocks: Vec<Vec<Event>> = Vec::new();
    for trie in &tries {
        let key = keys::adj_data_key(DEFAULT_GRAPH, EDGES_TABLE, family, &trie.entry.trie_key);
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
    let merged = varve_index::merge_sources(blocks, live_events);
    let mut entries = Vec::new();
    for (edge, events) in &merged {
        let visible = varve_index::resolve(events, bounds);
        let labeled = visible.iter().any(|v| match &v.event.op {
            Op::Put { labels, .. } => label.is_none_or(|l| labels.iter().any(|x| x == l)),
            _ => false,
        });
        if !labeled {
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
        entries.push(AdjacencyEntry {
            node,
            neighbor,
            edge: *edge,
        });
    }
    entries.sort_by_key(|e| (e.node, e.neighbor, e.edge));
    Ok(entries)
}

/// Label-filtered adjacency lookup: kept alongside `incident_edges` so T8/T9
/// call sites read clearly. `PathExpand` (task 8) is its first production
/// consumer; slice 6 ships and tests the lookup itself.
#[allow(dead_code)]
pub(crate) async fn edge_adjacency(
    state: &Arc<RwLock<TableState>>,
    store: &Arc<dyn ObjectStore>,
    label: &str,
    direction: AdjDirection,
    anchor: Option<Iid>,
    bounds: &TemporalBounds,
) -> Result<Vec<AdjacencyEntry>, EngineError> {
    edge_adjacency_impl(state, store, Some(label), direction, anchor, bounds).await
}

/// Label-blind variant of `edge_adjacency`: every visible `Put` edge
/// incident to `node` counts, regardless of its label. `resolve_delete`'s
/// still-connected check and DETACH DELETE's cascade (task 7) must catch
/// edges of ANY label, so they scan through this instead of
/// `edge_adjacency`'s label filter.
pub(crate) async fn incident_edges(
    state: &Arc<RwLock<TableState>>,
    store: &Arc<dyn ObjectStore>,
    direction: AdjDirection,
    node: Iid,
    bounds: &TemporalBounds,
) -> Result<Vec<AdjacencyEntry>, EngineError> {
    edge_adjacency_impl(state, store, None, direction, Some(node), bounds).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{PersistedTrie, TableKind, TableState, DEFAULT_GRAPH, NODES_TABLE};
    use std::sync::{Arc, RwLock};
    use varve_index::block::encode_block;
    use varve_index::{Event, LiveTable, Op};
    use varve_storage::{keys, memory_store, ObjectStore, TrieEntry};
    use varve_types::{Doc, Iid, Instant, TemporalBounds, TemporalDimension, Value};

    const EOT: Instant = Instant::END_OF_TIME;

    fn iid(n: u8) -> Iid {
        Iid::derive(DEFAULT_GRAPH, NODES_TABLE, &[n])
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
    ) -> (Arc<RwLock<TableState>>, Arc<dyn ObjectStore>) {
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
        (Arc::new(RwLock::new(state)), store)
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
        let batch = merged_snapshot(&state, &store, TableKind::Nodes, "P", &at(10), None)
            .await
            .unwrap();
        assert_eq!(names(&batch), vec!["Ada", "Bob"]);
    }

    #[tokio::test]
    async fn live_put_supersedes_persisted_version() {
        // Cross-source resolution: the persisted "Ada" must get system_to = 5.
        let (state, store) = seeded(&[put(1, 1, "Ada")], &[put(1, 5, "Adele")]).await;
        let now = merged_snapshot(&state, &store, TableKind::Nodes, "P", &at(10), None)
            .await
            .unwrap();
        assert_eq!(names(&now), vec!["Adele"]);
        // Time travel to before the live correction still sees the old version.
        let before = merged_snapshot(&state, &store, TableKind::Nodes, "P", &at(3), None)
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
        let now = merged_snapshot(&state, &store, TableKind::Nodes, "P", &at(10), None)
            .await
            .unwrap();
        assert!(now.is_none());
        let before = merged_snapshot(&state, &store, TableKind::Nodes, "P", &at(3), None)
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
        let before = merged_snapshot(&state, &store, TableKind::Nodes, "P", &at(3), None)
            .await
            .unwrap();
        assert!(before.is_none());
    }

    #[tokio::test]
    async fn iid_point_returns_only_that_entity() {
        let (state, store) =
            seeded(&[put(1, 1, "Ada"), put(2, 2, "Bob"), put(3, 3, "Cyd")], &[]).await;
        let batch = merged_snapshot(&state, &store, TableKind::Nodes, "P", &at(10), Some(iid(2)))
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
            let reference =
                merged_snapshot(&all_live, &store_a, TableKind::Nodes, "P", &bounds, None)
                    .await
                    .unwrap();
            let merged = merged_snapshot(&split, &store_b, TableKind::Nodes, "P", &bounds, None)
                .await
                .unwrap();
            assert_eq!(reference, merged);
        }
    }
}
