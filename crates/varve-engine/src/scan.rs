//! The merged bitemporal scan (spec §10 `BitemporalScan`, v1 shape): an
//! atomic (live, inventory) snapshot, page pruning, one ranged GET per
//! surviving page, per-entity merge in time order, then a single resolution
//! pass across sources via `snapshot_entities`.

use crate::db::EngineError;
use crate::state::{TableState, DEFAULT_GRAPH, NODES_TABLE};
use datafusion::arrow::record_batch::RecordBatch;
use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};
use varve_index::{decode_events, snapshot_entities, Event};
use varve_storage::{keys, ObjectStore};
use varve_types::{Iid, TemporalBounds};

pub(crate) async fn merged_snapshot(
    state: &Arc<RwLock<TableState>>,
    store: &Arc<dyn ObjectStore>,
    label: &str,
    bounds: &TemporalBounds,
    iid_point: Option<Iid>,
) -> Result<Option<RecordBatch>, EngineError> {
    // 1. Atomic snapshot under ONE read lock (decision 8). Live events are
    //    cloned — bounded by max_block_rows; a point lookup clones one
    //    entity. The trie inventory is Arc-cheap.
    let (live_events, tries) = {
        let s = state.read().map_err(|_| EngineError::Poisoned)?;
        let live_events: Vec<(Iid, Vec<Event>)> = match &iid_point {
            Some(iid) => s
                .live
                .events_for(iid)
                .map(|events| vec![(*iid, events.to_vec())])
                .unwrap_or_default(),
            None => s
                .live
                .entities()
                .map(|(iid, events)| (*iid, events.to_vec()))
                .collect(),
        };
        (live_events, s.tries.clone())
    };

    // 2. Persisted events, ascending block order (== time order, decision 9).
    let mut merged: BTreeMap<Iid, Vec<Event>> = BTreeMap::new();
    for trie in &tries {
        let data_key = keys::data_key(DEFAULT_GRAPH, NODES_TABLE, &trie.entry.trie_key);
        // An entity's run may span pages within one block: collect the whole
        // block in file order (system_from desc per entity), then reverse
        // per entity to restore arrival order.
        let mut per_block: BTreeMap<Iid, Vec<Event>> = BTreeMap::new();
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
                    per_block.entry(event.iid).or_default().push(event);
                }
            }
        }
        for (iid, desc) in per_block {
            merged
                .entry(iid)
                .or_default()
                .extend(desc.into_iter().rev());
        }
    }

    // 3. Live events are newest — appended after every persisted source.
    for (iid, events) in live_events {
        merged.entry(iid).or_default().extend(events);
    }

    Ok(snapshot_entities(
        merged.iter().map(|(iid, events)| (*iid, events.as_slice())),
        label,
        bounds,
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{PersistedTrie, TableState, DEFAULT_GRAPH, NODES_TABLE};
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
            state.tries.push(PersistedTrie {
                entry: TrieEntry {
                    trie_key,
                    row_count,
                    data_len,
                },
                pages: Arc::new(block.pages),
            });
        }
        for e in live_events {
            state.live.append(e.clone()).unwrap();
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
        let batch = merged_snapshot(&state, &store, "P", &at(10), None)
            .await
            .unwrap();
        assert_eq!(names(&batch), vec!["Ada", "Bob"]);
    }

    #[tokio::test]
    async fn live_put_supersedes_persisted_version() {
        // Cross-source resolution: the persisted "Ada" must get system_to = 5.
        let (state, store) = seeded(&[put(1, 1, "Ada")], &[put(1, 5, "Adele")]).await;
        let now = merged_snapshot(&state, &store, "P", &at(10), None)
            .await
            .unwrap();
        assert_eq!(names(&now), vec!["Adele"]);
        // Time travel to before the live correction still sees the old version.
        let before = merged_snapshot(&state, &store, "P", &at(3), None)
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
            op: Op::Delete,
        };
        let (state, store) = seeded(&[put(1, 1, "Ada")], std::slice::from_ref(&delete)).await;
        let now = merged_snapshot(&state, &store, "P", &at(10), None)
            .await
            .unwrap();
        assert!(now.is_none());
        let before = merged_snapshot(&state, &store, "P", &at(3), None)
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
            op: Op::Erase,
        };
        let (state, store) = seeded(&[put(1, 1, "Ada")], std::slice::from_ref(&erase)).await;
        // Even time-traveling BEFORE the erase: gone (slice-2 GDPR semantics).
        let before = merged_snapshot(&state, &store, "P", &at(3), None)
            .await
            .unwrap();
        assert!(before.is_none());
    }

    #[tokio::test]
    async fn iid_point_returns_only_that_entity() {
        let (state, store) =
            seeded(&[put(1, 1, "Ada"), put(2, 2, "Bob"), put(3, 3, "Cyd")], &[]).await;
        let batch = merged_snapshot(&state, &store, "P", &at(10), Some(iid(2)))
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
            let reference = merged_snapshot(&all_live, &store_a, "P", &bounds, None)
                .await
                .unwrap();
            let merged = merged_snapshot(&split, &store_b, "P", &bounds, None)
                .await
                .unwrap();
            assert_eq!(reference, merged);
        }
    }
}
