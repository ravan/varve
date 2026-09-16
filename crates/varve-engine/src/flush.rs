//! Block flush: seal the live tails, then — off the writer loop — encode
//! them, PUT data + meta, PUT the manifest (spec §9, the ATOMIC COMMIT
//! POINT), atomically swap the trie inventory + drop the sealed tails, then
//! best-effort trim the log (slice-4 plan, decisions 5, 6, 7, 8, 10).
//!
//! Sealing is the only step on the writer's critical path: under one write
//! lock every non-empty live tail becomes that table's `sealed` tail and a
//! fresh live tail takes its place, so commits keep landing while the
//! encode and the PUTs run on their own task. Reads see live + sealed + tries
//! under the same lock they always took. One flush is in flight at a time
//! (the writer loop enforces it); compaction and shutdown wait for it.

use crate::db::EngineError;
use crate::state::{GraphsState, PersistedTrie, TableKind, EDGES_TABLE};
use crate::writer::WriterState;
use bytes::Bytes;
use std::sync::{Arc, RwLock};
use tokio::task::JoinHandle;
use tracing::Instrument;
use varve_index::block::{
    encode_block, encode_block_by, EncodedBlock, LabelIndex, PageMeta, SortOrder,
};
use varve_index::KeyFilter;
use varve_index::LiveTable;
use varve_log::Log;
use varve_storage::{keys, BlockManifest, ObjectStore, TableTries, TrieEntry};
use varve_types::LogPosition;

/// Rows per page (spec §9's XTDB `pageLimit`) — Task 6's block-encoding
/// default, reused verbatim for every flush.
pub(crate) const PAGE_ROWS: usize = varve_storage::keys::PAGE_LIMIT;

/// One graph's share of a flush: its sealed tails and the trie inventory
/// the manifest must carry forward.
struct GraphSeal {
    graph: String,
    nodes: Option<Arc<LiveTable>>,
    edges: Option<Arc<LiveTable>>,
    prior_nodes: Vec<TrieEntry>,
    prior_edges: Vec<TrieEntry>,
    prior_adj_out: Vec<TrieEntry>,
    prior_adj_in: Vec<TrieEntry>,
}

/// Everything a flush needs once the tails are sealed. Built under the
/// write lock by [`seal`], consumed off the writer loop by [`spawn_flush`].
pub(crate) struct FlushJob {
    block_id: u64,
    /// The durable log prefix the sealed tails cover — the manifest's
    /// watermark and the trim point once it lands.
    watermark: LogPosition,
    max_tx_id: u64,
    max_system_us: i64,
    graphs: Vec<GraphSeal>,
}

struct GraphEncoded {
    graph: String,
    nodes_enc: Option<EncodedBlock>,
    edges_enc: Option<EncodedBlock>,
    adj_out_enc: Option<EncodedBlock>,
    adj_in_enc: Option<EncodedBlock>,
}

struct PrimaryFlush {
    graph: String,
    kind: TableKind,
    entry: TrieEntry,
    pages: Vec<PageMeta>,
    labels: LabelIndex,
    keys: KeyFilter,
    props: varve_index::PropSchema,
}

struct AdjFlush {
    graph: String,
    family: &'static str,
    entry: TrieEntry,
    pages: Vec<PageMeta>,
    keys: KeyFilter,
}

/// Seals every non-empty live tail (a table whose earlier flush failed keeps
/// its sealed tail and is retried as is) and reserves the block id. `None`
/// when nothing is unflushed: never writes an empty block/manifest.
pub(crate) fn seal(state: &mut WriterState) -> Result<Option<FlushJob>, EngineError> {
    let mut s = state.state.write().map_err(|_| EngineError::Poisoned)?;
    let mut graphs = Vec::new();
    let mut max_system_us = 0;
    let mut dirty = false;
    for (graph, table) in s.graphs.iter_mut() {
        let mut seal_core = |core: &mut crate::state::TableCore| -> Option<Arc<LiveTable>> {
            if core.sealed.is_none() && core.live.event_count() > 0 {
                core.sealed = Some(Arc::new(std::mem::replace(
                    &mut core.live,
                    LiveTable::new(),
                )));
            }
            let sealed = core.sealed.clone()?;
            max_system_us = max_system_us.max(
                sealed
                    .last_system_from()
                    .map(|t| t.as_micros())
                    .unwrap_or(0),
            );
            dirty = true;
            Some(sealed)
        };
        let nodes = seal_core(&mut table.nodes);
        let edges = seal_core(&mut table.edges);
        graphs.push(GraphSeal {
            graph: graph.clone(),
            nodes,
            edges,
            prior_nodes: table.nodes.tries.iter().map(|t| t.entry.clone()).collect(),
            prior_edges: table.edges.tries.iter().map(|t| t.entry.clone()).collect(),
            prior_adj_out: table.adj_out.iter().map(|t| t.entry.clone()).collect(),
            prior_adj_in: table.adj_in.iter().map(|t| t.entry.clone()).collect(),
        });
    }
    drop(s);
    if !dirty {
        return Ok(None);
    }
    let block_id = state.next_block_id;
    state.next_block_id += 1;
    Ok(Some(FlushJob {
        block_id,
        watermark: state.durable_watermark,
        max_tx_id: state.next_tx_id,
        max_system_us,
        graphs,
    }))
}

/// Runs a sealed flush to completion on its own task: encode, PUT, manifest,
/// swap, trim. The writer loop keeps committing meanwhile.
pub(crate) fn spawn_flush(
    state: &WriterState,
    job: FlushJob,
) -> JoinHandle<Result<(), EngineError>> {
    let shared = Arc::clone(&state.state);
    let store = Arc::clone(&state.store);
    let log = Arc::clone(&state.log);
    let metrics = Arc::clone(&state.metrics);
    let block_id = job.block_id;
    tokio::spawn(
        async move {
            let result = run_flush(&shared, &store, &log, job).await;
            match &result {
                Ok(()) => {
                    metrics
                        .flush_blocks
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                Err(error) => {
                    tracing::error!(
                        error = %error,
                        "flush_block failed; sealed tail retained, will retry at the next flush trigger"
                    );
                    metrics
                        .flush_failures
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
            result
        }
        .instrument(tracing::info_span!("varve.flush_block", block_id)),
    )
}

/// Seal + flush, awaited inline: the synchronous shape the tests use.
#[cfg(test)]
pub(crate) async fn flush_block(state: &mut WriterState) -> Result<(), EngineError> {
    let Some(job) = seal(state)? else {
        return Ok(());
    };
    match spawn_flush(state, job).await {
        Ok(result) => result,
        Err(join) => Err(EngineError::CommitFailed(format!(
            "flush task failed: {join}"
        ))),
    }
}

fn encode_all(job: &FlushJob) -> Result<Vec<GraphEncoded>, EngineError> {
    let mut out = Vec::new();
    for g in &job.graphs {
        let nodes_enc = match &g.nodes {
            Some(live) => Some(encode_block(live, PAGE_ROWS)?),
            None => None,
        };
        let (edges_enc, adj_out_enc, adj_in_enc) = match &g.edges {
            Some(live) => (
                Some(encode_block(live, PAGE_ROWS)?),
                Some(encode_block_by(live, PAGE_ROWS, SortOrder::BySrc)?),
                Some(encode_block_by(live, PAGE_ROWS, SortOrder::ByDst)?),
            ),
            None => (None, None, None),
        };
        out.push(GraphEncoded {
            graph: g.graph.clone(),
            nodes_enc,
            edges_enc,
            adj_out_enc,
            adj_in_enc,
        });
    }
    Ok(out)
}

/// Failure keeps serving (decision 10): if any PUT before the manifest
/// fails, the sealed tails stay sealed (still readable) and the flush simply
/// retries at the next trigger. Already-PUT data/meta without a manifest
/// entry are invisible garbage (GC, slice 8), never corruption.
async fn run_flush(
    shared: &Arc<RwLock<GraphsState>>,
    store: &Arc<dyn ObjectStore>,
    log: &Arc<dyn Log>,
    job: FlushJob,
) -> Result<(), EngineError> {
    let block_id = job.block_id;
    let trie_key = keys::l0_trie_key(block_id);
    // The encode is pure CPU over the sealed tails: keep it off the async
    // workers.
    let (job, encoded) = tokio::task::spawn_blocking(move || {
        let encoded = encode_all(&job);
        (job, encoded)
    })
    .await
    .map_err(|join| EngineError::CommitFailed(format!("flush encode failed: {join}")))?;
    let encoded = encoded?;

    let mut flushed = Vec::new();
    let mut flushed_adj = Vec::new();
    for enc in &encoded {
        for (kind, block) in [
            (TableKind::Nodes, &enc.nodes_enc),
            (TableKind::Edges, &enc.edges_enc),
        ] {
            let Some(EncodedBlock {
                data,
                meta,
                pages,
                labels,
                keys: key_filter,
                props,
            }) = block
            else {
                continue;
            };
            let entry = TrieEntry {
                trie_key: trie_key.clone(),
                row_count: pages.iter().map(|p| p.rows).sum(),
                data_len: data.len() as u64,
            };
            store
                .put(
                    &keys::data_key(&enc.graph, kind.name(), &trie_key),
                    Bytes::from(data.clone()),
                )
                .await?;
            store
                .put(
                    &keys::meta_key(&enc.graph, kind.name(), &trie_key),
                    Bytes::from(meta.clone()),
                )
                .await?;
            store
                .put(
                    &keys::labels_key(&enc.graph, kind.name(), &trie_key),
                    Bytes::from(labels.encode()?),
                )
                .await?;
            store
                .put(
                    &keys::keys_key(&enc.graph, kind.name(), "", &trie_key),
                    Bytes::from(key_filter.encode()),
                )
                .await?;
            store
                .put(
                    &keys::props_key(&enc.graph, kind.name(), &trie_key),
                    Bytes::from(props.encode()?),
                )
                .await?;
            flushed.push(PrimaryFlush {
                graph: enc.graph.clone(),
                kind,
                entry,
                pages: pages.clone(),
                labels: labels.clone(),
                keys: key_filter.clone(),
                props: props.clone(),
            });
        }

        for (family, block) in [
            (varve_storage::ADJ_OUT, &enc.adj_out_enc),
            (varve_storage::ADJ_IN, &enc.adj_in_enc),
        ] {
            let Some(EncodedBlock {
                data,
                meta,
                pages,
                keys: key_filter,
                ..
            }) = block
            else {
                continue;
            };
            let entry = TrieEntry {
                trie_key: trie_key.clone(),
                row_count: pages.iter().map(|p| p.rows).sum(),
                data_len: data.len() as u64,
            };
            store
                .put(
                    &keys::adj_data_key(&enc.graph, EDGES_TABLE, family, &trie_key),
                    Bytes::from(data.clone()),
                )
                .await?;
            store
                .put(
                    &keys::adj_meta_key(&enc.graph, EDGES_TABLE, family, &trie_key),
                    Bytes::from(meta.clone()),
                )
                .await?;
            store
                .put(
                    &keys::keys_key(&enc.graph, EDGES_TABLE, family, &trie_key),
                    Bytes::from(key_filter.encode()),
                )
                .await?;
            flushed_adj.push(AdjFlush {
                graph: enc.graph.clone(),
                family,
                entry,
                pages: pages.clone(),
                keys: key_filter.clone(),
            });
        }
    }

    crash_point("pre-manifest-put");

    let mut tables = Vec::new();
    for g in &job.graphs {
        for (kind, prior) in [
            (TableKind::Nodes, &g.prior_nodes),
            (TableKind::Edges, &g.prior_edges),
        ] {
            let mut tries = prior.clone();
            if let Some(flush) = flushed
                .iter()
                .find(|flush| flush.graph == g.graph && flush.kind == kind)
            {
                tries.push(flush.entry.clone());
            }
            if !tries.is_empty() {
                tables.push(TableTries {
                    graph: g.graph.clone(),
                    table: kind.name().to_string(),
                    family: String::new(),
                    tries,
                });
            }
        }

        for (family, prior) in [
            (varve_storage::ADJ_OUT, &g.prior_adj_out),
            (varve_storage::ADJ_IN, &g.prior_adj_in),
        ] {
            let mut tries = prior.clone();
            if let Some(flush) = flushed_adj
                .iter()
                .find(|flush| flush.graph == g.graph && flush.family == family)
            {
                tries.push(flush.entry.clone());
            }
            if !tries.is_empty() {
                tables.push(TableTries {
                    graph: g.graph.clone(),
                    table: EDGES_TABLE.to_string(),
                    family: family.to_string(),
                    tries,
                });
            }
        }
    }

    let manifest = BlockManifest {
        block_id,
        watermark: job.watermark.as_u64(),
        max_tx_id: job.max_tx_id,
        max_system_time_us: job.max_system_us,
        tables,
    };

    // This manifest PUT is not itself epoch-fenced (only the log is) — a
    // fenced-but-alive writer's in-flight flush could still land this PUT
    // before the Task-8 post-check lease gate fires. The before+after lease
    // ack-gate remains the liveness guard: it makes such a writer fatal and
    // never-acking. Should the stray PUT still land, `latest_manifest`
    // (slice 11) selects the newest manifest by `(watermark, block_id)`
    // rather than max `block_id` alone, so a stray manifest with a newer
    // block id but a stale watermark can never be selected during
    // recovery/verify/follower reads.
    store
        .put(
            &keys::manifest_key(block_id),
            Bytes::from(manifest.to_wire()),
        )
        .await?;

    crash_point("post-manifest-put");

    {
        let mut s = shared.write().map_err(|_| EngineError::Poisoned)?;
        for flush in flushed {
            let Some(table) = s.graph_mut(&flush.graph) else {
                continue;
            };
            let core = table.core_mut(flush.kind);
            core.tries.push(PersistedTrie {
                entry: flush.entry,
                pages: Arc::new(flush.pages),
                labels: Some(Arc::new(flush.labels)),
                keys: Some(Arc::new(flush.keys)),
                props: Some(Arc::new(flush.props)),
            });
            core.sealed = None;
        }
        for flush in flushed_adj {
            let Some(table) = s.graph_mut(&flush.graph) else {
                continue;
            };
            let trie = PersistedTrie {
                entry: flush.entry,
                pages: Arc::new(flush.pages),
                labels: None,
                keys: Some(Arc::new(flush.keys)),
                props: None,
            };
            if flush.family == varve_storage::ADJ_OUT {
                table.adj_out.push(trie);
            } else {
                table.adj_in.push(trie);
            }
        }
    }

    let _ = log.trim(job.watermark).await;
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
    use crate::coord::LeaseState;
    use crate::db::{EngineError, TxReceipt};
    use crate::metrics::EngineMetrics;
    use crate::node::ProgressState;
    use crate::scan::{merged_snapshot, IidSel};
    use crate::security::SecurityEnforcer;
    use crate::state::{GraphsState, TableKind, DEFAULT_GRAPH};
    use crate::writer::{spawn_writer, Submission, WriterConfig, WriterHandle, WriterState};
    use bytes::Bytes;
    use std::collections::BTreeMap;
    use std::sync::{Arc, RwLock};
    use std::time::Duration;
    use tokio::sync::{oneshot, watch};
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
    ) -> (WriterHandle, Arc<RwLock<GraphsState>>, Arc<MemoryLog>) {
        let log = Arc::new(MemoryLog::new());
        let state = Arc::new(RwLock::new(GraphsState::new()));
        let (progress, _progress_rx) = watch::channel(ProgressState::running(
            0,
            LogPosition::ZERO,
            LogPosition::ZERO,
        ));
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
            progress,
            lease: watch::channel(LeaseState::Unfenced).1,
            metrics: Arc::new(EngineMetrics::default()),
            security: SecurityEnforcer::new(crate::security::SecurityTuning::default()),
        };
        let cfg = WriterConfig {
            window: Duration::ZERO,
            max_bytes: 8 * 1024 * 1024,
            max_block_rows,
            // Row-count trigger is what these tests exercise; the byte
            // watermark (Task 11) stays out of reach so it never fires here.
            max_live_bytes: usize::MAX,
            flush_interval,
            queue_len: 256,
        };
        (spawn_writer(writer_state, cfg), state, log)
    }

    fn submit(
        sender: &WriterHandle,
        gql: &str,
    ) -> oneshot::Receiver<Result<TxReceipt, EngineError>> {
        let program = varve_gql::parse_program(gql).unwrap();
        let graph = program
            .use_graph
            .unwrap_or_else(|| DEFAULT_GRAPH.to_string());
        let (ack, rx) = oneshot::channel();
        sender
            .try_submit(Submission {
                payload: crate::writer::Payload::Program {
                    statements: program.statements,
                    params: BTreeMap::new(),
                },
                graph,
                user: String::new(),
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
            &IidSel::All,
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
            &IidSel::All,
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

        async fn delete(&self, key: &str) -> Result<(), StorageError> {
            Err(StorageError::NotFound(key.to_string()))
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
                s.graph(DEFAULT_GRAPH).unwrap().nodes.unflushed_rows(),
                3,
                "a failed flush must keep every row unflushed (sealed or live)"
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
            &IidSel::All,
            None,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(batch.num_rows(), 1, "delete resolved against flushed block");
    }
}
