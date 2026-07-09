//! Block flush: encode the live table, PUT data + meta, PUT the manifest
//! (spec §9, the ATOMIC COMMIT POINT), then atomically swap the trie
//! inventory + reset the live table, then best-effort trim the log
//! (slice-4 plan, decisions 5, 6, 7, 8, 10).

use crate::db::EngineError;
use crate::state::{PersistedTrie, TableKind, EDGES_TABLE};
use crate::writer::WriterState;
use bytes::Bytes;
use std::sync::Arc;
use varve_index::block::{encode_block, encode_block_by, EncodedBlock, PageMeta, SortOrder};
use varve_index::LiveTable;
use varve_storage::{keys, BlockManifest, TableTries, TrieEntry};

/// Rows per page (spec §9's XTDB `pageLimit`) — Task 6's block-encoding
/// default, reused verbatim for every flush.
pub(crate) const PAGE_ROWS: usize = varve_index::block::DEFAULT_PAGE_ROWS;

/// Encodes both tables' live tails into ONE L0 block and commits it under a
/// SINGLE manifest PUT (THE atomic commit point, spec §9) — only once that
/// succeeds does this atomically push each flushed family's new trie into its
/// inventory and reset the flushed live tails, then best-effort trim the log.
/// One flush = one `block_id` = one manifest PUT with up to FOUR `TableTries`
/// entries: nodes primary, edges primary, and (slice 6) the edges out/in
/// adjacency families. All families share the block's `trie_key`; object keys
/// are namespaced by table and, for adjacency, by family.
///
/// No-op when both live tails are empty: never writes an empty block/manifest.
///
/// Failure keeps serving (decision 10): if any PUT before the manifest
/// fails, the live tables are untouched and the flush simply retries at the
/// next trigger. Already-PUT data/meta without a manifest entry are
/// invisible garbage (GC arrives in slice 8), never corruption.
pub(crate) async fn flush_block(state: &mut WriterState) -> Result<(), EngineError> {
    struct GraphFlushSnapshot {
        graph: String,
        nodes_enc: Option<EncodedBlock>,
        edges_enc: Option<EncodedBlock>,
        adj_out_enc: Option<EncodedBlock>,
        adj_in_enc: Option<EncodedBlock>,
        prior_nodes: Vec<TrieEntry>,
        prior_edges: Vec<TrieEntry>,
        prior_adj_out: Vec<TrieEntry>,
        prior_adj_in: Vec<TrieEntry>,
    }

    struct PrimaryFlush {
        graph: String,
        kind: TableKind,
        entry: TrieEntry,
        pages: Vec<PageMeta>,
    }

    struct AdjFlush {
        graph: String,
        family: &'static str,
        entry: TrieEntry,
        pages: Vec<PageMeta>,
    }

    let (snapshots, max_system_us) = {
        let s = state.state.read().map_err(|_| EngineError::Poisoned)?;
        let mut snapshots = Vec::new();
        let mut max_system_us = 0;

        for (graph, table) in &s.graphs {
            let nodes_enc = if table.nodes.live.event_count() > 0 {
                max_system_us = max_system_us.max(
                    table
                        .nodes
                        .live
                        .last_system_from()
                        .map(|t| t.as_micros())
                        .unwrap_or(0),
                );
                Some(encode_block(&table.nodes.live, PAGE_ROWS)?)
            } else {
                None
            };

            let (edges_enc, adj_out_enc, adj_in_enc) = if table.edges.live.event_count() > 0 {
                max_system_us = max_system_us.max(
                    table
                        .edges
                        .live
                        .last_system_from()
                        .map(|t| t.as_micros())
                        .unwrap_or(0),
                );
                (
                    Some(encode_block(&table.edges.live, PAGE_ROWS)?),
                    Some(encode_block_by(
                        &table.edges.live,
                        PAGE_ROWS,
                        SortOrder::BySrc,
                    )?),
                    Some(encode_block_by(
                        &table.edges.live,
                        PAGE_ROWS,
                        SortOrder::ByDst,
                    )?),
                )
            } else {
                (None, None, None)
            };

            snapshots.push(GraphFlushSnapshot {
                graph: graph.clone(),
                nodes_enc,
                edges_enc,
                adj_out_enc,
                adj_in_enc,
                prior_nodes: table.nodes.tries.iter().map(|t| t.entry.clone()).collect(),
                prior_edges: table.edges.tries.iter().map(|t| t.entry.clone()).collect(),
                prior_adj_out: table.adj_out.iter().map(|t| t.entry.clone()).collect(),
                prior_adj_in: table.adj_in.iter().map(|t| t.entry.clone()).collect(),
            });
        }

        (snapshots, max_system_us)
    };

    let has_dirty_graph = snapshots
        .iter()
        .any(|s| s.nodes_enc.is_some() || s.edges_enc.is_some());
    if !has_dirty_graph {
        return Ok(());
    }

    let block_id = state.next_block_id;
    let trie_key = keys::l0_trie_key(block_id);
    let mut flushed = Vec::new();
    let mut flushed_adj = Vec::new();

    for snapshot in &snapshots {
        for (kind, enc) in [
            (TableKind::Nodes, &snapshot.nodes_enc),
            (TableKind::Edges, &snapshot.edges_enc),
        ] {
            let Some(EncodedBlock { data, meta, pages }) = enc else {
                continue;
            };
            let entry = TrieEntry {
                trie_key: trie_key.clone(),
                row_count: pages.iter().map(|p| p.rows).sum(),
                data_len: data.len() as u64,
            };
            state
                .store
                .put(
                    &keys::data_key(&snapshot.graph, kind.name(), &trie_key),
                    Bytes::from(data.clone()),
                )
                .await?;
            state
                .store
                .put(
                    &keys::meta_key(&snapshot.graph, kind.name(), &trie_key),
                    Bytes::from(meta.clone()),
                )
                .await?;
            flushed.push(PrimaryFlush {
                graph: snapshot.graph.clone(),
                kind,
                entry,
                pages: pages.clone(),
            });
        }

        for (family, enc) in [
            (varve_storage::ADJ_OUT, &snapshot.adj_out_enc),
            (varve_storage::ADJ_IN, &snapshot.adj_in_enc),
        ] {
            let Some(EncodedBlock { data, meta, pages }) = enc else {
                continue;
            };
            let entry = TrieEntry {
                trie_key: trie_key.clone(),
                row_count: pages.iter().map(|p| p.rows).sum(),
                data_len: data.len() as u64,
            };
            state
                .store
                .put(
                    &keys::adj_data_key(&snapshot.graph, EDGES_TABLE, family, &trie_key),
                    Bytes::from(data.clone()),
                )
                .await?;
            state
                .store
                .put(
                    &keys::adj_meta_key(&snapshot.graph, EDGES_TABLE, family, &trie_key),
                    Bytes::from(meta.clone()),
                )
                .await?;
            flushed_adj.push(AdjFlush {
                graph: snapshot.graph.clone(),
                family,
                entry,
                pages: pages.clone(),
            });
        }
    }

    crash_point("pre-manifest-put");

    let mut tables = Vec::new();
    for snapshot in &snapshots {
        for (kind, prior) in [
            (TableKind::Nodes, &snapshot.prior_nodes),
            (TableKind::Edges, &snapshot.prior_edges),
        ] {
            let mut tries = prior.clone();
            if let Some(flush) = flushed
                .iter()
                .find(|flush| flush.graph == snapshot.graph && flush.kind == kind)
            {
                tries.push(flush.entry.clone());
            }
            if !tries.is_empty() {
                tables.push(TableTries {
                    graph: snapshot.graph.clone(),
                    table: kind.name().to_string(),
                    family: String::new(),
                    tries,
                });
            }
        }

        for (family, prior) in [
            (varve_storage::ADJ_OUT, &snapshot.prior_adj_out),
            (varve_storage::ADJ_IN, &snapshot.prior_adj_in),
        ] {
            let mut tries = prior.clone();
            if let Some(flush) = flushed_adj
                .iter()
                .find(|flush| flush.graph == snapshot.graph && flush.family == family)
            {
                tries.push(flush.entry.clone());
            }
            if !tries.is_empty() {
                tables.push(TableTries {
                    graph: snapshot.graph.clone(),
                    table: EDGES_TABLE.to_string(),
                    family: family.to_string(),
                    tries,
                });
            }
        }
    }

    let manifest = BlockManifest {
        block_id,
        watermark: state.durable_watermark.as_u64(),
        max_tx_id: state.next_tx_id,
        max_system_time_us: max_system_us,
        tables,
    };

    state
        .store
        .put(
            &keys::manifest_key(block_id),
            Bytes::from(manifest.to_wire()),
        )
        .await?;

    crash_point("post-manifest-put");

    {
        let mut s = state.state.write().map_err(|_| EngineError::Poisoned)?;
        for flush in flushed {
            let Some(table) = s.graph_mut(&flush.graph) else {
                continue;
            };
            let core = table.core_mut(flush.kind);
            core.tries.push(PersistedTrie {
                entry: flush.entry,
                pages: Arc::new(flush.pages),
            });
            core.live = LiveTable::new();
        }
        for flush in flushed_adj {
            let Some(table) = s.graph_mut(&flush.graph) else {
                continue;
            };
            let trie = PersistedTrie {
                entry: flush.entry,
                pages: Arc::new(flush.pages),
            };
            if flush.family == varve_storage::ADJ_OUT {
                table.adj_out.push(trie);
            } else {
                table.adj_in.push(trie);
            }
        }
    }

    state.next_block_id += 1;
    let _ = state.log.trim(state.durable_watermark).await;
    Ok(())
}

/// Test-only crash hook for the `varve-testkit` `kill -9` harness, mirroring
/// `varve-log::local::crash_point`. Inert (a no-op) unless built with the
/// `fault-injection` feature, and even then does nothing unless
/// `VARVE_CRASH_TRIGGER` points at a file containing exactly this point's
/// name. When armed, announces the point on stdout and parks the thread
/// until the harness delivers `kill -9`.
#[cfg(feature = "fault-injection")]
fn crash_point(point: &str) {
    let Ok(path) = std::env::var("VARVE_CRASH_TRIGGER") else {
        return;
    };
    match std::fs::read_to_string(&path) {
        Ok(armed) if armed.trim() == point => {}
        _ => return,
    }
    println!("CRASH_POINT {point}");
    use std::io::Write as _;
    let _ = std::io::stdout().flush();
    loop {
        std::thread::sleep(std::time::Duration::from_secs(3600));
    }
}

#[cfg(not(feature = "fault-injection"))]
fn crash_point(_point: &str) {}

#[cfg(test)]
mod tests {
    use crate::clock::{Clock, MonotonicClock};
    use crate::db::{EngineError, TxReceipt};
    use crate::scan::merged_snapshot;
    use crate::state::{GraphsState, TableKind, DEFAULT_GRAPH};
    use crate::writer::{spawn_writer, Submission, WriterConfig, WriterState};
    use bytes::Bytes;
    use std::collections::BTreeMap;
    use std::sync::{Arc, RwLock};
    use std::time::Duration;
    use tokio::sync::{mpsc, oneshot};
    use varve_index::LabelFilter;
    use varve_log::{Log, MemoryLog};
    use varve_storage::{
        keys, latest_manifest, memory_store, BlockManifest, ObjectStore, StorageError,
    };
    use varve_types::{LogPosition, TemporalBounds, TemporalDimension};

    fn spawn_with(
        store: Arc<dyn ObjectStore>,
        max_block_rows: usize,
        flush_interval: Duration,
    ) -> (
        mpsc::Sender<Submission>,
        Arc<RwLock<GraphsState>>,
        Arc<MemoryLog>,
    ) {
        let log = Arc::new(MemoryLog::new());
        let state = Arc::new(RwLock::new(GraphsState::new()));
        let writer_state = WriterState {
            state: Arc::clone(&state),
            store,
            clock: Arc::new(MonotonicClock::new()),
            functions: Arc::new(varve_plan::FunctionRegistry::with_builtins()),
            max_path_depth: 10,
            query_limits: varve_plan::QueryLimits::default(),
            log: Arc::clone(&log) as Arc<dyn Log>,
            next_tx_id: 0,
            next_block_id: 0,
            durable_watermark: LogPosition::ZERO,
        };
        let cfg = WriterConfig {
            window: Duration::ZERO,
            max_bytes: 8 * 1024 * 1024,
            max_block_rows,
            flush_interval,
        };
        (spawn_writer(writer_state, cfg), state, log)
    }

    fn submit(
        sender: &mpsc::Sender<Submission>,
        gql: &str,
    ) -> oneshot::Receiver<Result<TxReceipt, EngineError>> {
        let program = varve_gql::parse_program(gql).unwrap();
        let graph = program
            .use_graph
            .unwrap_or_else(|| DEFAULT_GRAPH.to_string());
        let (ack, rx) = oneshot::channel();
        sender
            .try_send(Submission {
                statements: program.statements,
                params: BTreeMap::new(),
                graph,
                ack,
            })
            .unwrap();
        rx
    }

    /// flush runs after acks, so tests poll for the manifest.
    async fn wait_for_manifest(store: &Arc<dyn ObjectStore>) -> BlockManifest {
        for _ in 0..200 {
            if let Some(m) = latest_manifest(store.as_ref()).await.unwrap() {
                return m;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("no manifest appeared within 5s");
    }

    fn now_bounds() -> TemporalBounds {
        let now = MonotonicClock::new().next();
        TemporalBounds {
            valid: TemporalDimension::at(now),
            system: TemporalDimension::at(now),
        }
    }

    #[tokio::test]
    async fn size_trigger_flushes_a_block_and_trims_the_log() {
        let store = memory_store();
        let (sender, state, log) = spawn_with(Arc::clone(&store), 3, Duration::ZERO);
        for i in 1..=3 {
            submit(&sender, &format!("INSERT (:P {{_id: {i}, v: {i}}})"))
                .await
                .unwrap()
                .unwrap();
        }
        let manifest = wait_for_manifest(&store).await;
        assert_eq!(manifest.block_id, 0);
        assert_eq!(manifest.watermark, 3); // exclusive end of the 3-tx prefix
        assert_eq!(manifest.max_tx_id, 3);
        assert!(manifest.max_system_time_us > 0);
        let tries = &manifest.tables[0].tries;
        assert_eq!(tries.len(), 1);
        assert_eq!(tries[0].trie_key, "l00-rc-b00");
        assert_eq!(tries[0].row_count, 3);

        // Data + meta objects exist under the spec §9 keys.
        store
            .get(&keys::data_key("default", "nodes", "l00-rc-b00"))
            .await
            .unwrap();
        store
            .get(&keys::meta_key("default", "nodes", "l00-rc-b00"))
            .await
            .unwrap();

        {
            let s = state.read().unwrap();
            assert_eq!(s.graph(DEFAULT_GRAPH).unwrap().nodes.live.event_count(), 0);
            assert_eq!(s.graph(DEFAULT_GRAPH).unwrap().nodes.tries.len(), 1);
        }

        assert!(log.tail(LogPosition::ZERO).await.unwrap().is_empty());

        let batch = merged_snapshot(
            &state,
            &store,
            DEFAULT_GRAPH,
            TableKind::Nodes,
            LabelFilter::Single("P"),
            &now_bounds(),
            None,
            None,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(batch.num_rows(), 3);
    }

    #[tokio::test]
    async fn a_second_block_carries_the_full_inventory() {
        let store = memory_store();
        let (sender, state, _log) = spawn_with(Arc::clone(&store), 2, Duration::ZERO);
        for i in 1..=4 {
            submit(&sender, &format!("INSERT (:P {{_id: {i}}})"))
                .await
                .unwrap()
                .unwrap();
        }
        // Poll until the SECOND manifest lands.
        let manifest = loop {
            let m = wait_for_manifest(&store).await;
            if m.block_id == 1 {
                break m;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        };
        assert_eq!(manifest.watermark, 4);
        let tries = &manifest.tables[0].tries;
        assert_eq!(tries.len(), 2, "manifest lists FULL inventory");
        assert_eq!(tries[0].trie_key, "l00-rc-b00");
        assert_eq!(tries[1].trie_key, "l00-rc-b01");
        assert_eq!(
            state
                .read()
                .unwrap()
                .graph(DEFAULT_GRAPH)
                .unwrap()
                .nodes
                .tries
                .len(),
            2
        );

        let batch = merged_snapshot(
            &state,
            &store,
            DEFAULT_GRAPH,
            TableKind::Nodes,
            LabelFilter::Single("P"),
            &now_bounds(),
            None,
            None,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(batch.num_rows(), 4);
    }

    #[tokio::test]
    async fn flush_timer_flushes_below_the_row_threshold() {
        let store = memory_store();
        let (sender, state, _log) = spawn_with(Arc::clone(&store), 1000, Duration::from_millis(50));
        submit(&sender, "INSERT (:P {_id: 1})")
            .await
            .unwrap()
            .unwrap();
        let manifest = wait_for_manifest(&store).await;
        assert_eq!(manifest.tables[0].tries[0].row_count, 1);
        assert_eq!(
            state
                .read()
                .unwrap()
                .graph(DEFAULT_GRAPH)
                .unwrap()
                .nodes
                .live
                .event_count(),
            0
        );
    }

    #[tokio::test]
    async fn empty_live_table_never_flushes() {
        // Timer armed but nothing ever staged: no manifest should appear.
        let store = memory_store();
        let (_sender, state, _log) =
            spawn_with(Arc::clone(&store), 1000, Duration::from_millis(30));
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(latest_manifest(store.as_ref()).await.unwrap().is_none());
        assert_eq!(
            state
                .read()
                .unwrap()
                .graph(DEFAULT_GRAPH)
                .unwrap()
                .nodes
                .live
                .event_count(),
            0
        );
    }

    /// Every PUT fails: acks still succeed, nothing lost, no manifest
    /// appears, live table keeps serving (decision 10).
    struct FailingStore;

    #[async_trait::async_trait]
    impl ObjectStore for FailingStore {
        async fn put(&self, key: &str, _bytes: Bytes) -> Result<(), StorageError> {
            Err(StorageError::NotFound(format!(
                "injected failure for {key}"
            )))
        }
        async fn get(&self, key: &str) -> Result<Bytes, StorageError> {
            Err(StorageError::NotFound(key.to_string()))
        }
        async fn get_range(
            &self,
            key: &str,
            _range: std::ops::Range<u64>,
        ) -> Result<Bytes, StorageError> {
            Err(StorageError::NotFound(key.to_string()))
        }
        async fn list(&self, _prefix: &str) -> Result<Vec<String>, StorageError> {
            Ok(Vec::new())
        }
    }

    #[tokio::test]
    async fn failed_puts_keep_the_live_table_serving() {
        let store: Arc<dyn ObjectStore> = Arc::new(FailingStore);
        let (sender, state, log) = spawn_with(Arc::clone(&store), 2, Duration::ZERO);
        for i in 1..=3 {
            submit(&sender, &format!("INSERT (:P {{_id: {i}}})"))
                .await
                .unwrap()
                .unwrap();
        }
        // No manifest ever appears; give the flush attempt time to run.
        tokio::time::sleep(Duration::from_millis(100)).await;
        {
            let s = state.read().unwrap();
            assert_eq!(
                s.graph(DEFAULT_GRAPH).unwrap().nodes.live.event_count(),
                3,
                "a failed flush must not touch the live table"
            );
            assert!(s.graph(DEFAULT_GRAPH).unwrap().nodes.tries.is_empty());
        }
        assert_eq!(
            log.tail(LogPosition::ZERO).await.unwrap().len(),
            3,
            "a failed flush must not trim the log"
        );
    }

    #[tokio::test]
    async fn delete_resolves_against_a_flushed_block() {
        let store = memory_store();
        let (sender, state, _log) = spawn_with(Arc::clone(&store), 2, Duration::ZERO);
        submit(&sender, "INSERT (:P {_id: 1})")
            .await
            .unwrap()
            .unwrap();
        submit(&sender, "INSERT (:P {_id: 2})")
            .await
            .unwrap()
            .unwrap();
        wait_for_manifest(&store).await; // both rows now live ONLY in the block
        submit(&sender, "MATCH (p:P) WHERE p._id = 1 DELETE p")
            .await
            .unwrap()
            .unwrap();
        let batch = merged_snapshot(
            &state,
            &store,
            DEFAULT_GRAPH,
            TableKind::Nodes,
            LabelFilter::Single("P"),
            &now_bounds(),
            None,
            None,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(batch.num_rows(), 1, "delete resolved against flushed block");
    }
}
