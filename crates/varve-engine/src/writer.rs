//! The writer loop — Varve's single serialization point (spec §3, D3). Every
//! mutating statement is resolved HERE, serially, so tx N always sees tx
//! N−1. Events are applied to the live index only AFTER their batch is
//! durable, and acks fire after apply: once a tx is acked its effects are
//! both durable and visible; queries never observe un-durable data.

use crate::clock::Clock;
use crate::compact::{
    self, select_compaction_jobs, write_compacted_blocks, CompactionConfig, CompactionInputBlock,
    CompactionReport,
};
use crate::const_eval::const_value;
use crate::coord::LeaseState;
use crate::db::{
    bounds_per_clause, scan_inputs_for, validate_user_graph_name, EngineError, Overlay,
    SideEffects, TxReceipt,
};
use crate::metrics::EngineMetrics;
use crate::node::ProgressState;
use crate::scan::{incident_edges, AdjDirection};
use crate::security::{self, GraphGrants, SecurityEnforcer, SECURITY_GRAPH};
use crate::state::{
    GraphsState, PersistedTrie, TableKind, DEFAULT_GRAPH, EDGES_TABLE, META_GRAPH, NODES_TABLE,
};
use bytes::Bytes;
use datafusion::arrow::array::{
    Array, BinaryArray, BooleanArray, Float64Array, Int64Array, StringArray,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::record_batch::RecordBatch;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, watch};
use tracing::Instrument;
use varve_gql::ast::{
    Clause, Direction, Expr, GraphStmt, InsertStmt, LabelSpec, MatchPart, MutKind, MutateStmt,
    NodePattern, PrivilegeAction, PrivilegeKind, QueryBody, RemoveItem, RemoveStmt, ReturnClause,
    RoleTarget, SecurityStmt, SetItem, SetStmt, SortItem, Statement, TemporalClauses,
};
use varve_index::{decode_events, encode_events, visible_events, Event, Op};
use varve_log::{Log, LogRecord, TableEffects};
use varve_storage::{keys, manifest_history, TrieCatalog, TrieEntry};
use varve_types::{Doc, Iid, Instant, LogPosition, TemporalBounds, TemporalDimension, Value};

/// One node in a bulk-ingest batch (`Db::ingest`) — an xtdb-style put:
/// upsert keyed by the doc's `_id` (auto-generated when absent, same scheme
/// as GQL INSERT). The iid derivation is identical to GQL INSERT's, so
/// bulk-ingested and GQL-ingested data interoperate.
#[derive(Clone, Debug)]
pub struct NodePut {
    pub labels: Vec<String>,
    pub doc: Doc,
}

/// One edge in a bulk-ingest batch: endpoints are referenced by their node
/// `_id` values and existence is deliberately NOT verified (xtdb put-docs
/// semantics — the whole point of the bulk path is skipping the per-edge
/// endpoint MATCH). A dangling edge is durable but never matches a
/// traversal, exactly like an edge whose endpoints were later deleted.
#[derive(Clone, Debug)]
pub struct EdgePut {
    pub label: String,
    pub src: Value,
    pub dst: Value,
    pub doc: Doc,
}

/// What a submission asks the writer to resolve: a parsed GQL program, or a
/// bulk-ingest batch of prebuilt data ops (no parse, no plan, no reads).
pub(crate) enum Payload {
    Program {
        statements: Vec<Statement>,
        params: BTreeMap<String, Value>,
    },
    Ingest {
        nodes: Vec<NodePut>,
        edges: Vec<EdgePut>,
    },
}

pub(crate) struct Submission {
    pub payload: Payload,
    pub graph: String,
    pub user: String,
    pub ack: oneshot::Sender<Result<TxReceipt, EngineError>>,
}

enum Command {
    Submit(Submission),
    Compact {
        ack: oneshot::Sender<Result<CompactionReport, EngineError>>,
        /// Full-sweep policy (`CompactionConfig::full`): drain undersized L0
        /// groups too. Standard steady-state compaction passes `false`.
        full: bool,
    },
}

#[derive(Clone)]
pub(crate) struct WriterHandle {
    sender: mpsc::Sender<Command>,
}

impl WriterHandle {
    pub async fn submit(&self, submission: Submission) -> Result<(), EngineError> {
        self.sender
            .send(Command::Submit(submission))
            .await
            .map_err(|_| EngineError::WriterUnavailable)
    }

    pub async fn compact_once(&self) -> Result<CompactionReport, EngineError> {
        self.compact(false).await
    }

    /// One full-sweep job (`CompactionConfig::full`): also drains L0 groups
    /// below the standard `log_limit` gate — the post-bulk-load shape.
    pub async fn compact_full_once(&self) -> Result<CompactionReport, EngineError> {
        self.compact(true).await
    }

    async fn compact(&self, full: bool) -> Result<CompactionReport, EngineError> {
        let (ack, rx) = oneshot::channel();
        self.sender
            .send(Command::Compact { ack, full })
            .await
            .map_err(|_| EngineError::WriterUnavailable)?;
        rx.await.map_err(|_| EngineError::WriterUnavailable)?
    }

    /// Non-blocking submit (slice 10): the server's 429 path. A full queue
    /// rejects immediately with `Backpressure` instead of waiting for room;
    /// a closed channel (writer task gone) reports `WriterUnavailable`, same
    /// as [`Self::submit`].
    pub fn try_submit(&self, submission: Submission) -> Result<(), EngineError> {
        self.sender
            .try_send(Command::Submit(submission))
            .map_err(|err| match err {
                mpsc::error::TrySendError::Full(_) => EngineError::Backpressure,
                mpsc::error::TrySendError::Closed(_) => EngineError::WriterUnavailable,
            })
    }
}

/// Group-commit tuning (spec §6): a batch flushes when its window elapses OR
/// its encoded size reaches `max_bytes`, whichever comes first. Block-flush
/// tuning (spec §9, slice-4 plan): the live table flushes to a block once it
/// reaches `max_block_rows`, once its approximate in-memory footprint
/// reaches `max_live_bytes` (Task 11 — bounds writer memory when rows are
/// large), or once `flush_interval` elapses since the first unflushed row
/// landed — whichever comes first. `Duration::ZERO` disables the timer
/// (size-only flushing).
#[derive(Clone, Copy, Debug)]
pub(crate) struct WriterConfig {
    pub window: Duration,
    pub max_bytes: usize,
    pub max_block_rows: usize,
    /// Approximate live-index memory watermark (Task 11): once
    /// `GraphsState::live_bytes` reaches this, the writer flushes early even
    /// if `max_block_rows` is far away. Heuristic only — see
    /// `Event::approx_bytes` — never affects correctness.
    pub max_live_bytes: usize,
    pub flush_interval: Duration,
    /// Bounded capacity of the submission channel (`[node]
    /// submission_queue_len`, slice 10; was the fixed `SUBMISSION_QUEUE_LEN`
    /// const through slice 9). `Db::try_execute_as` rejects with
    /// `EngineError::Backpressure` once this many submissions are queued.
    pub queue_len: usize,
}

impl Default for WriterConfig {
    fn default() -> Self {
        WriterConfig {
            window: Duration::from_millis(15),
            max_bytes: 8 * 1024 * 1024,
            max_block_rows: 100_000,
            max_live_bytes: 512 * 1024 * 1024,
            flush_interval: Duration::from_secs(300),
            queue_len: 256,
        }
    }
}

pub(crate) struct WriterState {
    pub state: Arc<RwLock<GraphsState>>,
    pub store: Arc<dyn varve_storage::ObjectStore>,
    pub clock: Arc<dyn Clock>,
    pub functions: Arc<varve_plan::FunctionRegistry>,
    pub max_path_depth: u32,
    pub query_limits: varve_plan::QueryLimits,
    pub log: Arc<dyn Log>,
    pub next_tx_id: u64,
    /// Next manifest generation, shared by flush and compaction commits.
    pub next_block_id: u64,
    /// Exclusive end of the durably-appended log prefix — becomes the next
    /// flushed block's manifest watermark (decision 6).
    pub durable_watermark: LogPosition,
    pub progress: watch::Sender<ProgressState>,
    /// Published by the coordinator's heartbeat task (Task 6 plumbing): every
    /// ack is gated on this via `lease_block` (Task 8) — `Lost`/expired
    /// `ValidUntil` fences the whole staged batch and stops the writer.
    pub lease: watch::Receiver<LeaseState>,
    /// I/O-free engine counters (Task 12, spec §12), shared with `DbInner`.
    pub metrics: Arc<EngineMetrics>,
    /// `[security]` enforcement state, shared with `DbInner` — the writer
    /// gates DDL on admin-ship and checks resolved effects against write
    /// grants before anything is staged.
    pub security: Arc<SecurityEnforcer>,
}

/// One staged-but-not-yet-durable transaction: its log record, the per-table
/// effect events it will apply to the live index once durable, its receipt,
/// and the caller's ack channel.
struct Staged {
    graph: String,
    record: LogRecord,
    events: Effects,
    receipt: TxReceipt,
    ack: oneshot::Sender<Result<TxReceipt, EngineError>>,
}

/// A resolved mutation's effect events, split by target table. Applied to the
/// live tables (nodes then edges) only after the batch is durable.
#[derive(Default)]
pub(crate) struct Effects {
    pub nodes: Vec<Event>,
    pub edges: Vec<Event>,
    pub catalog_ops: Vec<CatalogOp>,
    pub side_effects: SideEffects,
    labels_added: BTreeSet<String>,
    labels_removed: BTreeSet<String>,
}

impl Effects {
    fn merge(&mut self, other: Effects) {
        self.nodes.extend(other.nodes);
        self.edges.extend(other.edges);
        self.catalog_ops.extend(other.catalog_ops);
        self.side_effects.nodes_created += other.side_effects.nodes_created;
        self.side_effects.nodes_deleted += other.side_effects.nodes_deleted;
        self.side_effects.relationships_created += other.side_effects.relationships_created;
        self.side_effects.relationships_deleted += other.side_effects.relationships_deleted;
        self.side_effects.properties_set += other.side_effects.properties_set;
        self.side_effects.properties_removed += other.side_effects.properties_removed;
        self.labels_added.extend(other.labels_added);
        self.labels_removed.extend(other.labels_removed);
        self.side_effects.labels_added = self.labels_added.len();
        self.side_effects.labels_removed = self.labels_removed.len();
    }

    fn record_node_create(&mut self, labels: &[String], doc: &Doc) {
        self.side_effects.nodes_created += 1;
        self.side_effects.properties_set += side_effect_property_count(doc);
        self.record_labels_added(labels);
    }

    fn record_edge_create(&mut self, doc: &Doc) {
        self.side_effects.relationships_created += 1;
        self.side_effects.properties_set += side_effect_property_count(doc);
    }

    fn record_node_delete(&mut self, labels: &[String], doc: &Doc) {
        self.side_effects.nodes_deleted += 1;
        self.side_effects.properties_removed += side_effect_property_count(doc);
        self.record_labels_removed(labels);
    }

    fn record_edge_delete(&mut self, doc: &Doc) {
        self.side_effects.relationships_deleted += 1;
        self.side_effects.properties_removed += side_effect_property_count(doc);
    }

    fn record_labels_added(&mut self, labels: &[String]) {
        self.labels_added.extend(labels.iter().cloned());
        self.side_effects.labels_added = self.labels_added.len();
    }

    fn record_labels_removed(&mut self, labels: &[String]) {
        self.labels_removed.extend(labels.iter().cloned());
        self.side_effects.labels_removed = self.labels_removed.len();
    }
}

fn side_effect_property_count(doc: &Doc) -> usize {
    doc.iter()
        .filter(|(key, value)| key.as_str() != "_id" && **value != Value::Null)
        .count()
}

pub(crate) enum CatalogOp {
    CreateGraph(String),
    DropGraph(String),
}

fn stored_labels(labels: &LabelSpec) -> Result<Vec<String>, EngineError> {
    match labels {
        LabelSpec::All(labels) => Ok(labels.clone()),
        LabelSpec::Any(_) => Err(EngineError::Unsupported(
            "label alternation is not supported when writing labels".into(),
        )),
    }
}

/// Distinguishes a real submission from a flush-timer wakeup in the writer
/// loop's `select!` below.
enum Received {
    Command(Option<Command>),
    FlushTimeout,
}

/// Spawns the writer loop on a dedicated task and returns the command handle
/// `Db` uses for transactions and inventory mutations.
pub(crate) fn spawn_writer(mut state: WriterState, cfg: WriterConfig) -> WriterHandle {
    let (sender, mut rx) = mpsc::channel::<Command>(cfg.queue_len);
    tokio::spawn(async move {
        // Armed (Some) while unflushed rows exist and a flush interval is
        // configured; disarmed after every flush and whenever the live
        // table is empty.
        let mut flush_deadline: Option<tokio::time::Instant> = None;
        let mut pending = None;
        loop {
            let received = match pending.take() {
                Some(command) => Received::Command(Some(command)),
                None => match flush_deadline {
                    Some(deadline) => tokio::select! {
                        command = rx.recv() => Received::Command(command),
                        _ = tokio::time::sleep_until(deadline) => Received::FlushTimeout,
                    },
                    None => Received::Command(rx.recv().await),
                },
            };
            match received {
                Received::Command(Some(Command::Submit(first))) => {
                    let (next, outcome) = run_batch(&mut state, &cfg, &mut rx, first).await;
                    if let FlushOutcome::Fatal(reason) = outcome {
                        // Lease loss / post-durability apply failure: publish
                        // the failure so /healthz degrades, drain every
                        // subsequent command with WriterFenced, and stop —
                        // no block flush after fatal.
                        publish_fatal(&state, &reason);
                        drain(&mut rx, reason).await;
                        break;
                    }
                    pending = next;
                    if live_rows(&state) >= cfg.max_block_rows
                        || live_bytes(&state) >= cfg.max_live_bytes
                    {
                        // A failed flush leaves the live table intact and
                        // retries at the next trigger (decision 10).
                        if let Err(error) = crate::flush::flush_block(&mut state).await {
                            tracing::error!(
                                error = %error,
                                "flush_block failed; live table retained, will retry at the next flush trigger"
                            );
                            state
                                .metrics
                                .flush_failures
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                    }
                    flush_deadline = next_deadline(&state, &cfg, flush_deadline);
                }
                Received::Command(Some(Command::Compact { ack, full })) => {
                    // The mid-batch Compact arm in `run_batch` defers to this
                    // very call site whenever `staged` is empty — it returns
                    // the command as `pending` instead of running
                    // compaction itself — so gating here closes the fencing
                    // hole for both the bare-Compact path and that deferred
                    // case (Task 8 review finding 1: every ack must be
                    // lease-gated, and `compact_once` performs real durable
                    // writes).
                    if let Some(reason) = gated_compact(&mut state, ack, full).await {
                        publish_fatal(&state, &reason);
                        drain(&mut rx, reason).await;
                        break;
                    }
                    flush_deadline = next_deadline(&state, &cfg, flush_deadline);
                }
                Received::Command(None) => {
                    // Sender dropped (Db closed) and channel drained:
                    // nothing left to do.
                    break;
                }
                Received::FlushTimeout => {
                    if let Err(error) = crate::flush::flush_block(&mut state).await {
                        tracing::error!(
                            error = %error,
                            "flush_block failed; live table retained, will retry at the next flush trigger"
                        );
                        state
                            .metrics
                            .flush_failures
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    flush_deadline = next_deadline(&state, &cfg, None);
                }
            }
        }
    });
    WriterHandle { sender }
}

fn live_rows(state: &WriterState) -> usize {
    state.state.read().map(|s| s.live_rows()).unwrap_or(0)
}

/// Approximate unflushed live-index bytes (Task 11 memory watermark). Same
/// lock-poisoned-is-empty fallback as `live_rows` — a poisoned lock never
/// forces a spurious flush attempt here either.
fn live_bytes(state: &WriterState) -> usize {
    state.state.read().map(|s| s.live_bytes()).unwrap_or(0)
}

/// The flush timer is armed once (on the OLDEST unflushed row) and left
/// alone until the next flush disarms it — a steady trickle of inserts
/// below `max_block_rows` still flushes within `flush_interval`.
fn next_deadline(
    state: &WriterState,
    cfg: &WriterConfig,
    current: Option<tokio::time::Instant>,
) -> Option<tokio::time::Instant> {
    if cfg.flush_interval.is_zero() || live_rows(state) == 0 {
        return None;
    }
    current.or_else(|| Some(tokio::time::Instant::now() + cfg.flush_interval))
}

/// One group-commit batch: stage `first`, then keep staging until the window
/// elapses, the size threshold trips, or the channel closes — then flush.
/// Returns the next already-received command (if any) alongside the flush
/// outcome — `Fatal` short-circuits the batch immediately (Task 8): no
/// further staging or block flush happens once the writer is fenced.
async fn run_batch(
    state: &mut WriterState,
    cfg: &WriterConfig,
    rx: &mut mpsc::Receiver<Command>,
    first: Submission,
) -> (Option<Command>, FlushOutcome) {
    let deadline = tokio::time::Instant::now() + cfg.window;
    let mut staged: Vec<Staged> = Vec::new();
    let mut staged_bytes = 0usize;
    let mut pending = Some(first);
    loop {
        if let Some(sub) = pending.take() {
            // A reading statement must observe every earlier tx, and events
            // apply only after durability — so flush any staged batch first.
            if !staged.is_empty()
                && (payload_reads(&sub.payload) || staged_touches_catalog(&staged))
            {
                if let FlushOutcome::Fatal(reason) = flush(state, std::mem::take(&mut staged)).await
                {
                    let _ = sub.ack.send(Err(EngineError::WriterFenced(reason.clone())));
                    return (None, FlushOutcome::Fatal(reason));
                }
                staged_bytes = 0;
            }
            match resolve_submission(state, sub.payload, &sub.graph, &sub.user).await {
                Ok((record, events, receipt, graph)) => {
                    staged_bytes += record.wire_len();
                    staged.push(Staged {
                        graph,
                        record,
                        events,
                        receipt,
                        ack: sub.ack,
                    });
                }
                Err(e) => {
                    let _ = sub.ack.send(Err(e));
                }
            }
            if staged_bytes >= cfg.max_bytes {
                break;
            }
        }
        match tokio::time::timeout_at(deadline, rx.recv()).await {
            Ok(Some(Command::Submit(sub))) => pending = Some(sub),
            Ok(Some(Command::Compact { ack, full })) => {
                if !staged.is_empty() {
                    if let FlushOutcome::Fatal(reason) =
                        flush(state, std::mem::take(&mut staged)).await
                    {
                        let _ = ack.send(Err(EngineError::WriterFenced(reason.clone())));
                        return (None, FlushOutcome::Fatal(reason));
                    }
                }
                return (Some(Command::Compact { ack, full }), FlushOutcome::Continue);
            }
            Ok(None) | Err(_) => break, // channel closed or window elapsed
        }
    }
    if !staged.is_empty() {
        return (None, flush(state, staged).await);
    }
    (None, FlushOutcome::Continue)
}

/// `varve.compact` (Task 13): no fields — the span wraps the whole compaction
/// pass, created inside the function itself so it stays observable no matter
/// how `compact_once` is called (currently only ever via `gated_compact`).
async fn compact_once(
    state: &mut WriterState,
    full: bool,
) -> Result<CompactionReport, EngineError> {
    compact_once_impl(state, full)
        .instrument(tracing::info_span!("varve.compact"))
        .await
}

async fn compact_once_impl(
    state: &mut WriterState,
    full: bool,
) -> Result<CompactionReport, EngineError> {
    let history = manifest_history(state.store.as_ref()).await?;
    let Some(latest) = history.iter().max_by_key(|manifest| manifest.block_id) else {
        return Ok(CompactionReport::default());
    };
    let catalog = TrieCatalog::from_manifests(&history)?;
    let config = CompactionConfig {
        full,
        ..CompactionConfig::default()
    };
    let mut jobs = select_compaction_jobs(&catalog, &config)?;
    let Some(job) = jobs.drain(..).next() else {
        return Ok(CompactionReport::default());
    };
    let order = job.target_sort_order().ok_or_else(|| {
        EngineError::UnknownTable(format!("{}/{}", job.scope().table, job.scope().family))
    })?;

    let mut inputs = Vec::with_capacity(job.input_trie_keys.len());
    for trie_key in &job.input_trie_keys {
        let data = state.store.get(&job.data_key(trie_key)).await?;
        let meta = state.store.get(&job.meta_key(trie_key)).await?;
        inputs.push(CompactionInputBlock {
            trie_key: trie_key.clone(),
            data: data.to_vec(),
            pages: varve_index::decode_meta(&meta)?,
        });
    }
    let input_rows = inputs
        .iter()
        .flat_map(|input| &input.pages)
        .map(|page| page.rows)
        .sum();

    let compacted = write_compacted_blocks(&job, &inputs, order, crate::flush::PAGE_ROWS)?;
    let mut output_entries = Vec::with_capacity(compacted.len());
    let mut persisted = Vec::with_capacity(compacted.len());
    for output in compacted {
        let trie_key = output.trie_key.clone();
        let entry = TrieEntry {
            trie_key: trie_key.to_key_string(),
            row_count: output.encoded.pages.iter().map(|page| page.rows).sum(),
            data_len: output.encoded.data.len() as u64,
        };
        state
            .store
            .put(&job.data_key(&trie_key), Bytes::from(output.encoded.data))
            .await?;
        state
            .store
            .put(&job.meta_key(&trie_key), Bytes::from(output.encoded.meta))
            .await?;
        persisted.push(PersistedTrie {
            entry: entry.clone(),
            pages: Arc::new(output.encoded.pages),
        });
        output_entries.push(entry);
    }

    let output_rows = output_entries.iter().map(|entry| entry.row_count).sum();
    let generation = state.next_block_id;
    let manifest = compact::compacted_manifest(latest, generation, &job, output_entries.clone());
    state
        .store
        .put(
            &keys::manifest_key(generation),
            Bytes::from(manifest.to_wire()),
        )
        .await?;
    state.next_block_id += 1;

    {
        let mut graphs = state.state.write().map_err(|_| EngineError::Poisoned)?;
        let scope = job.scope();
        let table = graphs
            .graph_mut(&scope.graph)
            .ok_or_else(|| EngineError::UnknownGraph(scope.graph.clone()))?;
        let tries = match (scope.table.as_str(), scope.family.as_str()) {
            (NODES_TABLE, "") => &mut table.nodes.tries,
            (EDGES_TABLE, "") => &mut table.edges.tries,
            (EDGES_TABLE, varve_storage::ADJ_OUT) => &mut table.adj_out,
            (EDGES_TABLE, varve_storage::ADJ_IN) => &mut table.adj_in,
            _ => {
                return Err(EngineError::UnknownTable(format!(
                    "{}/{}/{}",
                    scope.graph, scope.table, scope.family
                )))
            }
        };
        job.replace_inputs_with_outputs(tries, persisted, |trie| trie.entry.trie_key.as_str());
    }

    state
        .metrics
        .compaction_runs
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    Ok(CompactionReport {
        jobs: 1,
        input_tries: job.input_trie_keys.len(),
        output_tries: output_entries.len(),
        input_rows,
        output_rows,
    })
}

/// A statement "reads" if resolving it must observe every earlier tx — so
/// the writer flushes any staged (not-yet-applied) batch before resolving it.
/// `MATCH … DELETE`/`ERASE` and `MATCH … INSERT` both read the
/// live∪persisted snapshot; a plain `INSERT` (no MATCH) does not.
fn statement_reads(stmt: &Statement) -> bool {
    match stmt {
        Statement::Mutate(m) => matches!(m.kind, MutKind::Delete | MutKind::Erase),
        Statement::Insert(ins) => ins.match_part.is_some(),
        Statement::Set(_) | Statement::Remove(_) => true,
        Statement::Query(_) => false,
        Statement::Graph(_) => false,
        // Security DDL reads the current policy graph (role existence,
        // incident grants), so it must observe every earlier staged tx.
        Statement::Security(_) => true,
    }
}

fn program_reads(statements: &[Statement]) -> bool {
    statements.iter().any(statement_reads)
}

/// Bulk ingest never reads — it's prebuilt data ops (xtdb put semantics), so
/// it stages behind anything already in the batch without forcing a flush.
fn payload_reads(payload: &Payload) -> bool {
    match payload {
        Payload::Program { statements, .. } => program_reads(statements),
        Payload::Ingest { .. } => false,
    }
}

fn staged_touches_catalog(staged: &[Staged]) -> bool {
    staged
        .iter()
        .any(|staged| !staged.events.catalog_ops.is_empty())
}

fn append_effects_to_overlay(overlay: &mut Overlay, effects: &Effects) -> Result<(), EngineError> {
    for event in &effects.nodes {
        overlay.nodes.append(event.clone())?;
    }
    for event in &effects.edges {
        overlay.edges.append(event.clone())?;
    }
    Ok(())
}

/// Assigns one `(tx_id, system_time)` and resolves a full mutation program into
/// one per-table effect batch and log record.
///
/// `varve.resolve` (Task 13, field `tx_id`): the span is created HERE, inside
/// the function itself, rather than at its `run_batch` call site — the
/// writer.rs unit test calls `resolve_program` directly (bypassing
/// `run_batch`/`spawn_writer` entirely), so the span must be self-contained
/// to be observable there. `tx_id` isn't known until partway through the
/// body, so the field starts `Empty` and is filled in via
/// `Span::current().record` once assigned.
/// Dispatches a submission's payload to the right resolver: parsed GQL
/// programs to [`resolve_program`], bulk-ingest batches to
/// [`resolve_ingest`].
async fn resolve_submission(
    state: &mut WriterState,
    payload: Payload,
    graph: &str,
    user: &str,
) -> Result<(LogRecord, Effects, TxReceipt, String), EngineError> {
    match payload {
        Payload::Program { statements, params } => {
            resolve_program(state, statements, &params, graph, user).await
        }
        Payload::Ingest { nodes, edges } => resolve_ingest(state, nodes, edges, graph, user),
    }
}

async fn resolve_program(
    state: &mut WriterState,
    statements: Vec<Statement>,
    params: &BTreeMap<String, Value>,
    graph: &str,
    user: &str,
) -> Result<(LogRecord, Effects, TxReceipt, String), EngineError> {
    let span = tracing::info_span!("varve.resolve", tx_id = tracing::field::Empty);
    resolve_program_impl(state, statements, params, graph, user)
        .instrument(span)
        .await
}

/// Resolves a bulk-ingest batch (spec: xtdb-style `put-docs`) into one
/// per-table effect batch and log record — no parse, no plan, no reads.
/// Iids derive from `_id` exactly as GQL INSERT derives them
/// ([`resolve_insert_node`]); edge endpoints are referenced by node `_id`
/// and NOT verified to exist (a dangling edge never matches a traversal).
fn resolve_ingest(
    state: &mut WriterState,
    nodes: Vec<NodePut>,
    edges: Vec<EdgePut>,
    graph: &str,
    user: &str,
) -> Result<(LogRecord, Effects, TxReceipt, String), EngineError> {
    if nodes.is_empty() && edges.is_empty() {
        return Err(EngineError::NotAMutation);
    }
    let exists = state
        .state
        .read()
        .map_err(|_| EngineError::Poisoned)?
        .graphs
        .contains_key(graph);
    if !exists {
        return Err(EngineError::UnknownGraph(graph.to_string()));
    }

    state.next_tx_id += 1;
    let tx_id = state.next_tx_id;
    let system = state.clock.next();
    let span = tracing::info_span!("varve.resolve", tx_id);
    let _entered = span.enter();
    let valid_from = system;
    let valid_to = Instant::END_OF_TIME;

    let mut effects = Effects::default();
    let mut generated_ordinal: usize = 0;
    for NodePut { labels, mut doc } in nodes {
        let id = put_id(&mut doc, tx_id, &mut generated_ordinal);
        let iid = Iid::derive(graph, NODES_TABLE, &id.id_bytes()?);
        effects.record_node_create(&labels, &doc);
        effects.nodes.push(Event {
            iid,
            system_from: system,
            valid_from,
            valid_to,
            src: None,
            dst: None,
            op: Op::Put { labels, doc },
        });
    }
    for EdgePut {
        label,
        src,
        dst,
        mut doc,
    } in edges
    {
        let id = put_id(&mut doc, tx_id, &mut generated_ordinal);
        let iid = Iid::derive(graph, EDGES_TABLE, &id.id_bytes()?);
        let src = Iid::derive(graph, NODES_TABLE, &src.id_bytes()?);
        let dst = Iid::derive(graph, NODES_TABLE, &dst.id_bytes()?);
        effects.record_edge_create(&doc);
        effects.edges.push(Event {
            iid,
            system_from: system,
            valid_from,
            valid_to,
            src: Some(src),
            dst: Some(dst),
            op: Op::Put {
                labels: vec![label],
                doc,
            },
        });
    }
    seal_effects(tx_id, system, user, graph, effects)
}

/// The doc's `_id`, or a generated one (same `varve:gen:{tx}:{ordinal}`
/// scheme as GQL INSERT) inserted into the doc. Advances the ordinal either
/// way, mirroring `resolve_insert`/`resolve_insert_node`.
fn put_id(doc: &mut Doc, tx_id: u64, generated_ordinal: &mut usize) -> Value {
    let id = match doc.get("_id") {
        Some(v) => v.clone(),
        None => {
            let v = Value::Str(format!("varve:gen:{tx_id}:{generated_ordinal}"));
            doc.insert("_id".into(), v.clone());
            v
        }
    };
    *generated_ordinal += 1;
    id
}

/// Shared tail of every resolver: encodes the per-table effects into the
/// wire log record and builds the receipt.
fn seal_effects(
    tx_id: u64,
    system: Instant,
    user: &str,
    effect_graph: &str,
    effects: Effects,
) -> Result<(LogRecord, Effects, TxReceipt, String), EngineError> {
    let wire_graph = if effect_graph == DEFAULT_GRAPH {
        String::new()
    } else {
        effect_graph.to_string()
    };
    let mut table_effects = Vec::new();
    if !effects.nodes.is_empty() {
        table_effects.push(TableEffects {
            table: NODES_TABLE.to_string(),
            arrow_ipc: encode_events(&effects.nodes)?,
            graph: wire_graph.clone(),
        });
    }
    if !effects.edges.is_empty() {
        table_effects.push(TableEffects {
            table: EDGES_TABLE.to_string(),
            arrow_ipc: encode_events(&effects.edges)?,
            graph: wire_graph,
        });
    }
    let record = LogRecord {
        tx_id,
        system_time_us: system.as_micros(),
        user: user.to_string(),
        effects: table_effects,
    };
    let receipt = TxReceipt {
        tx_id,
        system_time: system,
        side_effects: effects.side_effects,
    };
    Ok((record, effects, receipt, effect_graph.to_string()))
}

async fn resolve_program_impl(
    state: &mut WriterState,
    statements: Vec<Statement>,
    params: &BTreeMap<String, Value>,
    graph: &str,
    user: &str,
) -> Result<(LogRecord, Effects, TxReceipt, String), EngineError> {
    if statements.is_empty() {
        return Err(EngineError::NotAMutation);
    }

    let has_catalog = statements
        .iter()
        .any(|stmt| matches!(stmt, Statement::Graph(_)));
    let has_security = statements
        .iter()
        .any(|stmt| matches!(stmt, Statement::Security(_)));
    if statements
        .iter()
        .any(|stmt| matches!(stmt, Statement::Security(s) if s.is_show()))
    {
        return Err(EngineError::NotAMutation); // SHOW is a read; use query()
    }
    let has_data = statements
        .iter()
        .any(|stmt| !matches!(stmt, Statement::Graph(_) | Statement::Security(_)));
    if (has_catalog || has_security) && has_data || (has_catalog && has_security) {
        return Err(EngineError::Unsupported(
            "mixing catalog/security and data statements in one transaction".into(),
        ));
    }

    if has_data {
        let exists = state
            .state
            .read()
            .map_err(|_| EngineError::Poisoned)?
            .graphs
            .contains_key(graph);
        if !exists {
            return Err(EngineError::UnknownGraph(graph.to_string()));
        }
    }

    // Security gates run BEFORE the tx id is assigned, so a denied
    // submission burns nothing. Catalog and security DDL are admin-only
    // under enforcement; data statements resolve the submitter's write
    // grants once and check every statement's effects against them below.
    let gate_now = state.clock.watermark();
    if (has_catalog || has_security)
        && !state
            .security
            .is_admin(&state.state, &state.store, user, gate_now)
            .await?
    {
        return Err(EngineError::AccessDenied(format!(
            "user '{user}' is not an administrator (catalog and security DDL require ADMIN)"
        )));
    }
    let enforcement: Option<GraphGrants> = if has_data {
        state
            .security
            .enforcement_for(&state.state, &state.store, user, graph, gate_now)
            .await?
    } else {
        None
    };

    state.next_tx_id += 1;
    let tx_id = state.next_tx_id;
    tracing::Span::current().record("tx_id", tx_id);
    let system = state.clock.next();

    let mut overlay = Overlay::default();
    let mut effects = Effects::default();
    let mut generated_ordinal: usize = 0;
    let effect_graph = if has_catalog {
        META_GRAPH
    } else if has_security {
        SECURITY_GRAPH
    } else {
        graph
    };
    let security = enforcement.as_ref();

    for stmt in statements {
        let statement_effects = match &stmt {
            Statement::Insert(ins) => {
                resolve_insert(
                    state,
                    graph,
                    ins,
                    params,
                    tx_id,
                    system,
                    &mut generated_ordinal,
                    Some(&overlay),
                    security,
                )
                .await?
            }
            Statement::Mutate(del) => {
                resolve_delete(state, graph, del, params, system, Some(&overlay), security).await?
            }
            Statement::Set(set) => {
                resolve_set(state, graph, set, params, system, Some(&overlay), security).await?
            }
            Statement::Remove(remove) => {
                resolve_remove(state, graph, remove, params, system, Some(&overlay), security)
                    .await?
            }
            Statement::Graph(graph_stmt) => resolve_graph_stmt(state, graph_stmt, system)?,
            Statement::Security(stmt) => {
                resolve_security_stmt(state, stmt, system, Some(&overlay)).await?
            }
            Statement::Query(_) => return Err(EngineError::NotAMutation),
        };
        if let Some(sec) = security {
            // Any denied effect rejects the WHOLE transaction: the error
            // propagates before anything is staged, so no partial effects
            // ever reach the log.
            enforce_write_effects(
                state,
                graph,
                &statement_effects,
                sec,
                Some(&overlay),
                system,
                user,
            )
            .await?;
        }
        append_effects_to_overlay(&mut overlay, &statement_effects)?;
        effects.merge(statement_effects);
    }

    seal_effects(tx_id, system, user, effect_graph, effects)
}

/// Effect-level write enforcement: every label on every affected node (its
/// new labels for a `Put`, plus its currently visible labels for updates and
/// deletes) must be write-granted, and every affected edge's type likewise.
async fn enforce_write_effects(
    state: &WriterState,
    graph: &str,
    effects: &Effects,
    sec: &GraphGrants,
    overlay: Option<&Overlay>,
    system: Instant,
    user: &str,
) -> Result<(), EngineError> {
    let bounds = TemporalBounds {
        valid: TemporalDimension::at(system),
        system: TemporalDimension::at(system),
    };
    for (kind, events, grants, what) in [
        (
            BindingKind::Node,
            &effects.nodes,
            &sec.write_nodes,
            "node labels",
        ),
        (
            BindingKind::Edge,
            &effects.edges,
            &sec.write_edges,
            "edge types",
        ),
    ] {
        if grants.wildcard {
            continue;
        }
        for event in events {
            let mut labels: BTreeSet<String> = BTreeSet::new();
            if let Op::Put { labels: new, .. } = &event.op {
                labels.extend(new.iter().cloned());
            }
            // Updates and deletes are governed by the entity's CURRENT
            // labels too (removing a `:Secret` label needs WRITE on Secret).
            if let Some((existing, _)) =
                visible_payload(state, graph, kind, event.iid, &bounds, overlay).await?
            {
                labels.extend(existing);
            }
            let denied: Vec<&String> = labels
                .iter()
                .filter(|label| !grants.names.contains(*label))
                .collect();
            if labels.is_empty() || !denied.is_empty() {
                return Err(EngineError::AccessDenied(format!(
                    "user '{user}' lacks WRITE on {what} {denied:?} in graph '{graph}'"
                )));
            }
        }
    }
    Ok(())
}

fn graph_catalog_iid(name: &str) -> Result<Iid, EngineError> {
    Ok(Iid::derive(
        META_GRAPH,
        NODES_TABLE,
        &Value::Str(name.to_string()).id_bytes()?,
    ))
}

fn graph_catalog_doc(name: &str) -> Doc {
    let mut doc = Doc::new();
    doc.insert("_id".to_string(), Value::Str(name.to_string()));
    doc
}

fn resolve_graph_stmt(
    state: &WriterState,
    stmt: &GraphStmt,
    system: Instant,
) -> Result<Effects, EngineError> {
    let mut effects = Effects::default();
    match stmt {
        GraphStmt::Create(name) => {
            validate_user_graph_name(name)?;
            if state
                .state
                .read()
                .map_err(|_| EngineError::Poisoned)?
                .graphs
                .contains_key(name)
            {
                return Err(EngineError::GraphExists(name.clone()));
            }
            effects.nodes.push(Event {
                iid: graph_catalog_iid(name)?,
                system_from: system,
                valid_from: system,
                valid_to: Instant::END_OF_TIME,
                src: None,
                dst: None,
                op: Op::Put {
                    labels: vec!["Graph".to_string()],
                    doc: graph_catalog_doc(name),
                },
            });
            effects
                .catalog_ops
                .push(CatalogOp::CreateGraph(name.clone()));
        }
        GraphStmt::Drop(name) => {
            if name == DEFAULT_GRAPH {
                return Err(EngineError::Unsupported(
                    "cannot drop default graph".to_string(),
                ));
            }
            validate_user_graph_name(name)?;
            if !state
                .state
                .read()
                .map_err(|_| EngineError::Poisoned)?
                .graphs
                .contains_key(name)
            {
                return Err(EngineError::UnknownGraph(name.clone()));
            }
            effects.nodes.push(Event {
                iid: graph_catalog_iid(name)?,
                system_from: system,
                valid_from: system,
                valid_to: Instant::END_OF_TIME,
                src: None,
                dst: None,
                op: Op::Delete,
            });
            effects.catalog_ops.push(CatalogOp::DropGraph(name.clone()));
        }
    }
    Ok(effects)
}

/// Lowers one security DDL statement into effects on the reserved
/// `__security` graph — the same shape as [`resolve_graph_stmt`] (validate
/// against current state, emit events), but the entities are policy nodes and
/// edges with deterministic `_id`s: a re-grant resolves to the same iid
/// (idempotent) and a revoke deletes exactly the edge it means.
async fn resolve_security_stmt(
    state: &WriterState,
    stmt: &SecurityStmt,
    system: Instant,
    overlay: Option<&Overlay>,
) -> Result<Effects, EngineError> {
    let bounds = TemporalBounds {
        valid: TemporalDimension::at(system),
        system: TemporalDimension::at(system),
    };
    let node_iid = |id: &str| security::security_iid(TableKind::Nodes, id);
    let edge_iid = |id: &str| security::security_iid(TableKind::Edges, id);

    let mut effects = Effects::default();
    match stmt {
        SecurityStmt::CreateRole(name) => {
            let iid = node_iid(&security::role_node_id(name))?;
            if visible_payload(state, SECURITY_GRAPH, BindingKind::Node, iid, &bounds, overlay).await?.is_some() {
                return Err(EngineError::Security(format!("role '{name}' already exists")));
            }
            put_security_node(
                &mut effects,
                iid,
                security::ROLE_LABEL,
                security::role_node_doc(name),
                system,
            );
        }
        SecurityStmt::DropRole(name) => {
            let iid = node_iid(&security::role_node_id(name))?;
            let Some((labels, doc)) = visible_payload(state, SECURITY_GRAPH, BindingKind::Node, iid, &bounds, overlay).await? else {
                return Err(EngineError::Security(format!("unknown role '{name}'")));
            };
            // Cascade: every MEMBER_OF/GRANTED edge incident to the role
            // (memberships into it, its own memberships, its grants) dies
            // with it in this one tx.
            let mut incident: BTreeMap<Iid, (Iid, Iid)> = BTreeMap::new();
            for dir in [AdjDirection::Out, AdjDirection::In] {
                for entry in incident_edges(
                    &state.state,
                    &state.store,
                    SECURITY_GRAPH,
                    dir,
                    iid,
                    &bounds,
                    overlay,
                )
                .await?
                {
                    let (src, dst) = match dir {
                        AdjDirection::Out => (entry.node, entry.neighbor),
                        AdjDirection::In => (entry.neighbor, entry.node),
                    };
                    incident.insert(entry.edge, (src, dst));
                }
            }
            for (edge, (src, dst)) in incident {
                if let Some((_, doc)) = visible_payload(state, SECURITY_GRAPH, BindingKind::Edge, edge, &bounds, overlay).await? {
                    effects.record_edge_delete(&doc);
                }
                delete_security_edge(&mut effects, edge, src, dst, system);
            }
            effects.record_node_delete(&labels, &doc);
            effects.nodes.push(Event {
                iid,
                system_from: system,
                valid_from: system,
                valid_to: Instant::END_OF_TIME,
                src: None,
                dst: None,
                op: Op::Delete,
            });
        }
        SecurityStmt::GrantRole { role, to } => {
            let role_iid = require_role(state, role, &bounds, overlay).await?;
            let (from_id, from_iid) = match to {
                RoleTarget::User(subject) => {
                    let id = security::user_node_id(subject);
                    let iid = node_iid(&id)?;
                    if visible_payload(state, SECURITY_GRAPH, BindingKind::Node, iid, &bounds, overlay).await?.is_none() {
                        // Users are auto-created on first mention; subjects
                        // come from the authenticator.
                        put_security_node(
                            &mut effects,
                            iid,
                            security::USER_LABEL,
                            security::user_node_doc(subject),
                            system,
                        );
                    }
                    (id, iid)
                }
                RoleTarget::Role(member) => {
                    if member == role {
                        return Err(EngineError::Security(format!(
                            "cannot grant role '{role}' to itself"
                        )));
                    }
                    (security::role_node_id(member), require_role(state, member, &bounds, overlay).await?)
                }
            };
            let edge_id = security::member_edge_id(&from_id, &security::role_node_id(role));
            let iid = edge_iid(&edge_id)?;
            if visible_payload(state, SECURITY_GRAPH, BindingKind::Edge, iid, &bounds, overlay).await?.is_none() {
                put_security_edge(
                    &mut effects,
                    iid,
                    security::MEMBER_OF_EDGE,
                    security::edge_only_doc(edge_id),
                    from_iid,
                    role_iid,
                    system,
                );
            }
        }
        SecurityStmt::RevokeRole { role, from } => {
            let role_iid = require_role(state, role, &bounds, overlay).await?;
            let from_id = match from {
                RoleTarget::User(subject) => security::user_node_id(subject),
                RoleTarget::Role(member) => security::role_node_id(member),
            };
            let from_iid = node_iid(&from_id)?;
            let edge_id = security::member_edge_id(&from_id, &security::role_node_id(role));
            let iid = edge_iid(&edge_id)?;
            if let Some((_, doc)) = visible_payload(state, SECURITY_GRAPH, BindingKind::Edge, iid, &bounds, overlay).await? {
                effects.record_edge_delete(&doc);
                delete_security_edge(&mut effects, iid, from_iid, role_iid, system);
            }
        }
        SecurityStmt::GrantPrivilege { privilege, role } => {
            let role_iid = require_role(state, role, &bounds, overlay).await?;
            for (action, graph, kind, name) in privilege_scopes(privilege)? {
                let priv_id = security::privilege_node_id(action, &graph, kind, &name);
                let priv_iid = node_iid(&priv_id)?;
                if visible_payload(state, SECURITY_GRAPH, BindingKind::Node, priv_iid, &bounds, overlay).await?.is_none() {
                    put_security_node(
                        &mut effects,
                        priv_iid,
                        security::PRIVILEGE_LABEL,
                        security::privilege_node_doc(action, &graph, kind, &name),
                        system,
                    );
                }
                grant_privilege_edge(&mut effects, state, role, role_iid, &priv_id, priv_iid, system, &bounds, overlay)
                    .await?;
            }
        }
        SecurityStmt::RevokePrivilege { privilege, role } => {
            let role_iid = require_role(state, role, &bounds, overlay).await?;
            for (action, graph, kind, name) in privilege_scopes(privilege)? {
                let priv_id = security::privilege_node_id(action, &graph, kind, &name);
                let priv_iid = node_iid(&priv_id)?;
                let edge_id = security::grant_edge_id(&security::role_node_id(role), &priv_id);
                let iid = edge_iid(&edge_id)?;
                if let Some((_, doc)) = visible_payload(state, SECURITY_GRAPH, BindingKind::Edge, iid, &bounds, overlay).await? {
                    effects.record_edge_delete(&doc);
                    delete_security_edge(&mut effects, iid, role_iid, priv_iid, system);
                }
            }
        }
        SecurityStmt::GrantAdmin { role } => {
            let role_iid = require_role(state, role, &bounds, overlay).await?;
            let priv_id = security::privilege_node_id("admin", security::WILDCARD, security::WILDCARD, security::WILDCARD);
            let priv_iid = node_iid(&priv_id)?;
            if visible_payload(state, SECURITY_GRAPH, BindingKind::Node, priv_iid, &bounds, overlay).await?.is_none() {
                put_security_node(
                    &mut effects,
                    priv_iid,
                    security::PRIVILEGE_LABEL,
                    security::privilege_node_doc("admin", security::WILDCARD, security::WILDCARD, security::WILDCARD),
                    system,
                );
            }
            grant_privilege_edge(&mut effects, state, role, role_iid, &priv_id, priv_iid, system, &bounds, overlay)
                .await?;
        }
        SecurityStmt::RevokeAdmin { role } => {
            let role_iid = require_role(state, role, &bounds, overlay).await?;
            let priv_id = security::privilege_node_id("admin", security::WILDCARD, security::WILDCARD, security::WILDCARD);
            let priv_iid = node_iid(&priv_id)?;
            let edge_id = security::grant_edge_id(&security::role_node_id(role), &priv_id);
            let iid = edge_iid(&edge_id)?;
            if let Some((_, doc)) = visible_payload(state, SECURITY_GRAPH, BindingKind::Edge, iid, &bounds, overlay).await? {
                effects.record_edge_delete(&doc);
                delete_security_edge(&mut effects, iid, role_iid, priv_iid, system);
            }
        }
        SecurityStmt::ShowRoles | SecurityStmt::ShowGrants { .. } => {
            return Err(EngineError::NotAMutation);
        }
    }
    Ok(effects)
}

/// The role node's iid, or `Security("unknown role …")` when it does not
/// exist at `bounds` (including earlier statements of the same tx via the
/// overlay).
async fn require_role(
    state: &WriterState,
    name: &str,
    bounds: &TemporalBounds,
    overlay: Option<&Overlay>,
) -> Result<Iid, EngineError> {
    let iid = security::security_iid(TableKind::Nodes, &security::role_node_id(name))?;
    if visible_payload(state, SECURITY_GRAPH, BindingKind::Node, iid, bounds, overlay)
        .await?
        .is_none()
    {
        return Err(EngineError::Security(format!("unknown role '{name}'")));
    }
    Ok(iid)
}

/// Expands one surface privilege spec into `(action, graph, kind, name)`
/// scopes: `ALL` → read + write, `*` lists → the literal wildcard name.
fn privilege_scopes(
    privilege: &varve_gql::ast::PrivilegeSpec,
) -> Result<Vec<(&'static str, String, &'static str, String)>, EngineError> {
    let actions: &[&'static str] = match privilege.action {
        PrivilegeAction::Read => &["read"],
        PrivilegeAction::Write => &["write"],
        PrivilegeAction::All => &["read", "write"],
    };
    let graph = match &privilege.graph {
        None => security::WILDCARD.to_string(),
        Some(graph) => {
            validate_user_graph_name(graph)?;
            graph.clone()
        }
    };
    let kind = match privilege.kind {
        PrivilegeKind::Nodes => "nodes",
        PrivilegeKind::Edges => "edges",
    };
    let names: Vec<String> = match &privilege.names {
        None => vec![security::WILDCARD.to_string()],
        Some(names) => names.clone(),
    };
    let mut scopes = Vec::with_capacity(actions.len() * names.len());
    for action in actions {
        for name in &names {
            scopes.push((*action, graph.clone(), kind, name.clone()));
        }
    }
    Ok(scopes)
}

#[allow(clippy::too_many_arguments)]
async fn grant_privilege_edge(
    effects: &mut Effects,
    state: &WriterState,
    _role: &str,
    role_iid: Iid,
    priv_id: &str,
    priv_iid: Iid,
    system: Instant,
    bounds: &TemporalBounds,
    overlay: Option<&Overlay>,
) -> Result<(), EngineError> {
    let edge_id = security::grant_edge_id(&security::role_node_id(_role), priv_id);
    let iid = security::security_iid(TableKind::Edges, &edge_id)?;
    if visible_payload(state, SECURITY_GRAPH, BindingKind::Edge, iid, bounds, overlay)
        .await?
        .is_none()
    {
        put_security_edge(
            effects,
            iid,
            security::GRANTED_EDGE,
            security::edge_only_doc(edge_id),
            role_iid,
            priv_iid,
            system,
        );
    }
    Ok(())
}

fn put_security_node(effects: &mut Effects, iid: Iid, label: &str, doc: Doc, system: Instant) {
    effects.record_node_create(&[label.to_string()], &doc);
    effects.nodes.push(Event {
        iid,
        system_from: system,
        valid_from: system,
        valid_to: Instant::END_OF_TIME,
        src: None,
        dst: None,
        op: Op::Put {
            labels: vec![label.to_string()],
            doc,
        },
    });
}

fn put_security_edge(
    effects: &mut Effects,
    iid: Iid,
    label: &str,
    doc: Doc,
    src: Iid,
    dst: Iid,
    system: Instant,
) {
    effects.record_edge_create(&doc);
    effects.edges.push(Event {
        iid,
        system_from: system,
        valid_from: system,
        valid_to: Instant::END_OF_TIME,
        src: Some(src),
        dst: Some(dst),
        op: Op::Put {
            labels: vec![label.to_string()],
            doc,
        },
    });
}

fn delete_security_edge(effects: &mut Effects, iid: Iid, src: Iid, dst: Iid, system: Instant) {
    effects.edges.push(Event {
        iid,
        system_from: system,
        valid_from: system,
        valid_to: Instant::END_OF_TIME,
        src: Some(src),
        dst: Some(dst),
        op: Op::Delete,
    });
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum BindingKind {
    Node,
    Edge,
}

fn add_binding_var(
    vars: &mut Vec<String>,
    kinds: &mut BTreeMap<String, BindingKind>,
    var: &Option<String>,
    kind: BindingKind,
) -> Result<(), EngineError> {
    let Some(var) = var else {
        return Ok(());
    };
    match kinds.get(var) {
        Some(existing) if *existing != kind => Err(EngineError::Unsupported(format!(
            "mutation MATCH variable {var} bound to multiple element kinds"
        ))),
        Some(_) => Ok(()),
        None => {
            kinds.insert(var.clone(), kind);
            vars.push(var.clone());
            Ok(())
        }
    }
}

fn match_binding_vars(
    match_part: &MatchPart,
) -> Result<(Vec<String>, BTreeMap<String, BindingKind>), EngineError> {
    let mut vars = Vec::new();
    let mut kinds = BTreeMap::new();
    for path in &match_part.paths {
        if path.var.is_some() {
            return Err(EngineError::Unsupported(
                "path variable in mutation MATCH part".into(),
            ));
        }
        add_binding_var(&mut vars, &mut kinds, &path.start.var, BindingKind::Node)?;
        for (edge, node) in &path.hops {
            if edge.quantifier.is_some() {
                return Err(EngineError::Unsupported(
                    "quantified hop in mutation MATCH part".into(),
                ));
            }
            add_binding_var(&mut vars, &mut kinds, &edge.var, BindingKind::Edge)?;
            add_binding_var(&mut vars, &mut kinds, &node.var, BindingKind::Node)?;
        }
    }
    Ok((vars, kinds))
}

fn match_part_body(match_part: &MatchPart, vars: &[String]) -> QueryBody {
    QueryBody {
        temporal: TemporalClauses::default(),
        clauses: vec![Clause::Match {
            optional: false,
            paths: match_part.paths.clone(),
            temporal: TemporalClauses::default(),
            where_clause: match_part.where_clause.clone(),
        }],
        ret: ReturnClause {
            distinct: true,
            items: vars
                .iter()
                .map(|var| {
                    (
                        Expr::Prop {
                            var: var.clone(),
                            prop: "_iid".into(),
                        },
                        Some(var.clone()),
                    )
                })
                .collect(),
            order_by: Vec::new(),
            skip: None,
            limit: None,
        },
    }
}

fn edge_endpoints_from_match_row(
    match_part: &MatchPart,
    edge_var: &str,
    row: &HashMap<String, Iid>,
) -> Result<(Iid, Iid), EngineError> {
    for path in &match_part.paths {
        let mut prev_var = path.start.var.as_deref();
        for (edge, node) in &path.hops {
            if edge.var.as_deref() == Some(edge_var) {
                let left = prev_var.and_then(|var| row.get(var)).copied();
                let right = node.var.as_deref().and_then(|var| row.get(var)).copied();
                let (Some(left), Some(right)) = (left, right) else {
                    return Err(EngineError::Unsupported(format!(
                        "DELETE edge variable '{edge_var}' requires bound endpoint variables"
                    )));
                };
                return Ok(match edge.direction {
                    Direction::Out => (left, right),
                    Direction::In => (right, left),
                });
            }
            prev_var = node.var.as_deref();
        }
    }
    Err(EngineError::UnboundVariable(edge_var.to_string()))
}

async fn resolve_match_part(
    state: &WriterState,
    graph: &str,
    match_part: &MatchPart,
    params: &BTreeMap<String, Value>,
    system: Instant,
    overlay: Option<&Overlay>,
    security: Option<&GraphGrants>,
) -> Result<(Vec<HashMap<String, Iid>>, BTreeMap<String, BindingKind>), EngineError> {
    let (vars, kinds) = match_binding_vars(match_part)?;
    let body = match_part_body(match_part, &vars);
    let clause_specs =
        varve_plan::scan_specs_with_params(&body, graph, state.max_path_depth, params)?;
    let bounds = bounds_per_clause(&body, system);
    let inputs = scan_inputs_for(
        &state.state,
        &state.store,
        graph,
        &clause_specs,
        &bounds,
        params,
        state.query_limits,
        None,
        overlay,
        security,
    )
    .await?;
    let rows = varve_plan::binding_rows_with_limits(
        &body,
        &clause_specs,
        inputs,
        state.functions.as_ref(),
        state.query_limits.path_expand,
        params,
        &vars,
    )
    .await?;
    Ok((
        rows.into_iter()
            .map(|row| row.into_iter().collect::<HashMap<_, _>>())
            .collect(),
        kinds,
    ))
}

#[derive(Clone)]
struct PendingEntity {
    kind: BindingKind,
    original_labels: Vec<String>,
    original_doc: Doc,
    labels: Vec<String>,
    doc: Doc,
    src: Option<Iid>,
    dst: Option<Iid>,
}

fn table_for_binding(kind: BindingKind) -> TableKind {
    match kind {
        BindingKind::Node => TableKind::Nodes,
        BindingKind::Edge => TableKind::Edges,
    }
}

fn set_item_var(item: &SetItem) -> &str {
    match item {
        SetItem::Prop { var, .. } | SetItem::Label { var, .. } => var,
    }
}

fn remove_item_var(item: &RemoveItem) -> &str {
    match item {
        RemoveItem::Prop { var, .. } | RemoveItem::Label { var, .. } => var,
    }
}

fn set_value_alias(idx: usize) -> String {
    format!("__set_value_{idx}")
}

fn match_projection_body(
    match_part: &MatchPart,
    vars: &[String],
    items: Vec<(Expr, Option<String>)>,
) -> QueryBody {
    QueryBody {
        temporal: TemporalClauses::default(),
        clauses: vec![Clause::Match {
            optional: false,
            paths: match_part.paths.clone(),
            temporal: TemporalClauses::default(),
            where_clause: match_part.where_clause.clone(),
        }],
        ret: ReturnClause {
            distinct: false,
            items,
            order_by: vars
                .iter()
                .map(|var| SortItem {
                    expr: Expr::Prop {
                        var: var.clone(),
                        prop: "_iid".into(),
                    },
                    asc: true,
                })
                .collect(),
            skip: None,
            limit: None,
        },
    }
}

#[allow(clippy::too_many_arguments)]
async fn resolve_match_projection(
    state: &WriterState,
    graph: &str,
    match_part: &MatchPart,
    params: &BTreeMap<String, Value>,
    system: Instant,
    projected: Vec<(Expr, Option<String>)>,
    overlay: Option<&Overlay>,
    security: Option<&GraphGrants>,
) -> Result<(Vec<RecordBatch>, Vec<String>, BTreeMap<String, BindingKind>), EngineError> {
    let (vars, kinds) = match_binding_vars(match_part)?;
    let mut items = vars
        .iter()
        .map(|var| {
            (
                Expr::Prop {
                    var: var.clone(),
                    prop: "_iid".into(),
                },
                Some(var.clone()),
            )
        })
        .collect::<Vec<_>>();
    items.extend(projected);
    let body = match_projection_body(match_part, &vars, items);
    let clause_specs =
        varve_plan::scan_specs_with_params(&body, graph, state.max_path_depth, params)?;
    let bounds = bounds_per_clause(&body, system);
    let inputs = scan_inputs_for(
        &state.state,
        &state.store,
        graph,
        &clause_specs,
        &bounds,
        params,
        state.query_limits,
        None,
        overlay,
        security,
    )
    .await?;
    let batches = varve_plan::execute_body_with_limits(
        &body,
        &clause_specs,
        inputs,
        state.functions.as_ref(),
        state.query_limits.path_expand,
        params,
    )
    .await?;
    Ok((batches, vars, kinds))
}

fn row_bindings(
    batch: &RecordBatch,
    vars: &[String],
    row_idx: usize,
) -> Result<HashMap<String, Iid>, EngineError> {
    let mut row = HashMap::new();
    for var in vars {
        if let Some(iid) = varve_plan::binding_iid(batch, var, row_idx)? {
            row.insert(var.clone(), iid);
        }
    }
    Ok(row)
}

fn downcast_value_array<'a, T: 'static>(
    array: &'a dyn Array,
    data_type: &DataType,
) -> Result<&'a T, EngineError> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        EngineError::Unsupported(format!(
            "SET expression array type mismatch for {data_type:?}"
        ))
    })
}

fn value_from_batch(batch: &RecordBatch, col: &str, row_idx: usize) -> Result<Value, EngineError> {
    let (idx, _) = batch
        .schema()
        .column_with_name(col)
        .ok_or_else(|| EngineError::Unsupported(format!("SET expression column {col} missing")))?;
    let array = batch.column(idx);
    if array.is_null(row_idx) {
        return Ok(Value::Null);
    }
    let data_type = array.data_type();
    match data_type {
        DataType::Int64 => Ok(Value::Int(
            downcast_value_array::<Int64Array>(array.as_ref(), data_type)?.value(row_idx),
        )),
        DataType::Float64 => Ok(Value::Float(
            downcast_value_array::<Float64Array>(array.as_ref(), data_type)?.value(row_idx),
        )),
        DataType::Utf8 => Ok(Value::Str(
            downcast_value_array::<StringArray>(array.as_ref(), data_type)?
                .value(row_idx)
                .to_string(),
        )),
        DataType::Boolean => Ok(Value::Bool(
            downcast_value_array::<BooleanArray>(array.as_ref(), data_type)?.value(row_idx),
        )),
        DataType::Binary => Ok(Value::Bytes(
            downcast_value_array::<BinaryArray>(array.as_ref(), data_type)?
                .value(row_idx)
                .to_vec(),
        )),
        other => Err(EngineError::Unsupported(format!(
            "SET expression type {other:?}"
        ))),
    }
}

async fn visible_payload(
    state: &WriterState,
    graph: &str,
    kind: BindingKind,
    iid: Iid,
    bounds: &TemporalBounds,
    overlay: Option<&Overlay>,
) -> Result<Option<(Vec<String>, Doc)>, EngineError> {
    let table = table_for_binding(kind);
    let (live_events, tries) = {
        let shared = state.state.read().map_err(|_| EngineError::Poisoned)?;
        let graph_state = shared
            .graph(graph)
            .ok_or_else(|| EngineError::UnknownGraph(graph.to_string()))?;
        let core = graph_state.core(table);
        let live_events = core
            .live
            .events_for(&iid)
            .map(|events| vec![(iid, events.to_vec())])
            .unwrap_or_default();
        (live_events, core.tries.clone())
    };
    let overlay_events: Vec<(Iid, Vec<Event>)> = overlay
        .and_then(|overlay| {
            overlay
                .table(table)
                .events_for(&iid)
                .map(|events| vec![(iid, events.to_vec())])
        })
        .unwrap_or_default();

    let mut blocks = Vec::new();
    for trie in tries {
        let data_key = keys::data_key(graph, table.name(), &trie.entry.trie_key);
        let mut block_events = Vec::new();
        for page in trie
            .pages
            .iter()
            .filter(|page| page.selected(bounds, Some(&iid)))
        {
            let bytes = state
                .store
                .get_range(&data_key, page.offset..page.offset + page.len)
                .await?;
            block_events.extend(decode_events(bytes.as_ref())?);
        }
        if !block_events.is_empty() {
            blocks.push(block_events);
        }
    }

    let merged = varve_index::merge_sources(blocks, live_events.into_iter().chain(overlay_events));
    Ok(visible_events(
        merged.iter().map(|(iid, events)| (*iid, events.as_slice())),
        bounds,
    )
    .into_iter()
    .find(|(visible_iid, _, _)| *visible_iid == iid)
    .map(|(_, labels, doc)| (labels.to_vec(), doc.clone())))
}

#[allow(clippy::too_many_arguments)]
async fn ensure_pending_entity(
    pending: &mut BTreeMap<Iid, PendingEntity>,
    state: &WriterState,
    graph: &str,
    kind: BindingKind,
    iid: Iid,
    src: Option<Iid>,
    dst: Option<Iid>,
    bounds: &TemporalBounds,
    overlay: Option<&Overlay>,
) -> Result<bool, EngineError> {
    if pending.get(&iid).is_none() {
        let Some((labels, doc)) = visible_payload(state, graph, kind, iid, bounds, overlay).await?
        else {
            return Ok(false);
        };
        pending.entry(iid).or_insert(PendingEntity {
            kind,
            original_labels: labels.clone(),
            original_doc: doc.clone(),
            labels,
            doc,
            src,
            dst,
        });
    }
    let Some(entity) = pending.get(&iid) else {
        return Err(EngineError::Unsupported(
            "pending entity missing after load".into(),
        ));
    };
    if entity.kind != kind || entity.src != src || entity.dst != dst {
        return Err(EngineError::Unsupported(
            "mutation target matched conflicting entity shape".into(),
        ));
    }
    Ok(true)
}

fn add_label(labels: &mut Vec<String>, label: &str) {
    if !labels.iter().any(|candidate| candidate == label) {
        labels.push(label.to_string());
    }
}

fn remove_label(labels: &mut Vec<String>, label: &str) {
    labels.retain(|candidate| candidate != label);
}

fn side_effect_property_delta(original: &Doc, updated: &Doc) -> (usize, usize) {
    let keys = original
        .keys()
        .chain(updated.keys())
        .filter(|key| key.as_str() != "_id")
        .collect::<BTreeSet<_>>();
    let mut set = 0;
    let mut removed = 0;
    for key in keys {
        let before = original.get(key).filter(|value| **value != Value::Null);
        let after = updated.get(key).filter(|value| **value != Value::Null);
        match (before, after) {
            (None, Some(_)) => set += 1,
            (Some(_), None) => removed += 1,
            (Some(before), Some(after)) if before != after => {
                set += 1;
                removed += 1;
            }
            _ => {}
        }
    }
    (set, removed)
}

fn label_delta(original: &[String], updated: &[String]) -> (Vec<String>, Vec<String>) {
    let original = original.iter().collect::<BTreeSet<_>>();
    let updated = updated.iter().collect::<BTreeSet<_>>();
    let added = updated
        .difference(&original)
        .map(|label| (*label).clone())
        .collect();
    let removed = original
        .difference(&updated)
        .map(|label| (*label).clone())
        .collect();
    (added, removed)
}

fn push_pending_effects(
    pending: BTreeMap<Iid, PendingEntity>,
    effects: &mut Effects,
    system: Instant,
) {
    for (iid, entity) in pending {
        if entity.labels == entity.original_labels && entity.doc == entity.original_doc {
            continue;
        }
        let (properties_set, properties_removed) =
            side_effect_property_delta(&entity.original_doc, &entity.doc);
        effects.side_effects.properties_set += properties_set;
        effects.side_effects.properties_removed += properties_removed;
        if entity.kind == BindingKind::Node {
            let (labels_added, labels_removed) =
                label_delta(&entity.original_labels, &entity.labels);
            effects.record_labels_added(&labels_added);
            effects.record_labels_removed(&labels_removed);
        }
        let event = Event {
            iid,
            system_from: system,
            valid_from: system,
            valid_to: Instant::END_OF_TIME,
            src: entity.src,
            dst: entity.dst,
            op: Op::Put {
                labels: entity.labels,
                doc: entity.doc,
            },
        };
        match entity.kind {
            BindingKind::Node => effects.nodes.push(event),
            BindingKind::Edge => effects.edges.push(event),
        }
    }
}

// INSERT resolution carries graph, tx/time, generated-id state, and overlay.
#[allow(clippy::too_many_arguments)]
async fn resolve_insert(
    state: &WriterState,
    graph: &str,
    ins: &InsertStmt,
    params: &BTreeMap<String, Value>,
    tx_id: u64,
    system: Instant,
    generated_ordinal: &mut usize,
    overlay: Option<&Overlay>,
    security: Option<&GraphGrants>,
) -> Result<Effects, EngineError> {
    let valid_from = ins.valid_from.unwrap_or(system);
    let valid_to = ins.valid_to.unwrap_or(Instant::END_OF_TIME);
    if valid_from >= valid_to {
        return Err(EngineError::InvalidValidRange {
            from: valid_from,
            to: valid_to,
        });
    }

    let (binding_rows, binding_kinds) = match &ins.match_part {
        None => (vec![HashMap::new()], BTreeMap::new()),
        Some(mp) => resolve_match_part(state, graph, mp, params, system, overlay, security).await?,
    };

    // 2. Per binding row, walk the INSERT paths creating events.
    let mut effects = Effects::default();
    for row in binding_rows {
        let mut bound = row; // statement-local bindings extend the row
        let mut bound_kinds = binding_kinds.clone();
        for path in &ins.paths {
            let mut prev = resolve_insert_node(
                graph,
                &path.start,
                &mut bound,
                &mut bound_kinds,
                &mut effects,
                generated_ordinal,
                params,
                tx_id,
                system,
                valid_from,
                valid_to,
            )?;
            for (edge, end) in &path.hops {
                let next = resolve_insert_node(
                    graph,
                    end,
                    &mut bound,
                    &mut bound_kinds,
                    &mut effects,
                    generated_ordinal,
                    params,
                    tx_id,
                    system,
                    valid_from,
                    valid_to,
                )?;
                let (src, dst) = match edge.direction {
                    Direction::Out => (prev, next),
                    Direction::In => (next, prev),
                };
                let mut doc: Doc = edge
                    .props
                    .iter()
                    .map(|(k, v)| const_value(v, params).map(|value| (k.clone(), value)))
                    .collect::<Result<_, _>>()?;
                let id = put_id(&mut doc, tx_id, generated_ordinal);
                let iid = Iid::derive(graph, EDGES_TABLE, &id.id_bytes()?);
                effects.record_edge_create(&doc);
                effects.edges.push(Event {
                    iid,
                    system_from: system,
                    valid_from,
                    valid_to,
                    src: Some(src),
                    dst: Some(dst),
                    op: Op::Put {
                        labels: vec![edge.label.clone()],
                        doc,
                    },
                });
                prev = next;
            }
        }
    }
    Ok(effects)
}

/// A node element inside INSERT: a bound reference `(a)` (must be bare — no
/// labels/props) or a new node. Building the whole event before returning —
/// including the fallible `id_bytes()?` — keeps a later node's invalid `_id`
/// from leaving earlier events committed (atomicity, pinned by
/// `multi_node_insert_is_atomic_on_invalid_id`).
#[allow(clippy::too_many_arguments)]
fn resolve_insert_node(
    graph: &str,
    node: &NodePattern,
    bound: &mut HashMap<String, Iid>,
    bound_kinds: &mut BTreeMap<String, BindingKind>,
    effects: &mut Effects,
    generated_ordinal: &mut usize,
    params: &BTreeMap<String, Value>,
    tx_id: u64,
    system: Instant,
    valid_from: Instant,
    valid_to: Instant,
) -> Result<Iid, EngineError> {
    if let Some(var) = &node.var {
        if let Some(iid) = bound.get(var) {
            match bound_kinds.get(var) {
                Some(BindingKind::Node) => {}
                Some(BindingKind::Edge) => {
                    return Err(EngineError::Unsupported(format!(
                        "INSERT node position bound to edge variable '{var}'"
                    )));
                }
                None => {
                    return Err(EngineError::Unsupported(format!(
                        "INSERT node variable '{var}' has no binding kind"
                    )));
                }
            }
            if !node.labels.is_empty() || !node.props.is_empty() {
                return Err(EngineError::AlreadyBoundVariable(var.clone()));
            }
            return Ok(*iid);
        }
        if node.labels.is_empty() && node.props.is_empty() {
            return Err(EngineError::UnboundVariable(var.clone()));
        }
    }
    let mut doc: Doc = node
        .props
        .iter()
        .map(|(k, v)| const_value(v, params).map(|value| (k.clone(), value)))
        .collect::<Result<_, _>>()?;
    let id = put_id(&mut doc, tx_id, generated_ordinal);
    let iid = Iid::derive(graph, NODES_TABLE, &id.id_bytes()?);
    let labels = stored_labels(&node.labels)?;
    effects.record_node_create(&labels, &doc);
    effects.nodes.push(Event {
        iid,
        system_from: system,
        valid_from,
        valid_to,
        src: None,
        dst: None,
        op: Op::Put { labels, doc },
    });
    if let Some(var) = &node.var {
        bound.insert(var.clone(), iid);
        bound_kinds.insert(var.clone(), BindingKind::Node);
    }
    Ok(iid)
}

/// MATCH … DELETE (spec §10 DML): resolves the read side against the merged
/// live∪persisted snapshot at (valid=now, system=now) — a delete must find
/// flushed entities too (slice-4 plan, decision 13).
///
/// Cascade semantics (task 7 of slice 6): for each matched node, every
/// incident edge (either direction, ANY label) is collected via the
/// label-blind `incident_edges`, deduped by edge iid — a self-loop is
/// incident via both `Out` and `In` but must be deleted exactly once. A
/// plain `DELETE` on a still-connected node fails the whole tx atomically
/// (`Err` returned before anything is staged); `DETACH DELETE` emits the
/// node delete(s) plus every distinct incident-edge delete in this one
/// `Effects` batch, so the cascade commits in a single tx.
async fn resolve_set(
    state: &WriterState,
    graph: &str,
    set: &SetStmt,
    params: &BTreeMap<String, Value>,
    system: Instant,
    overlay: Option<&Overlay>,
    security: Option<&GraphGrants>,
) -> Result<Effects, EngineError> {
    let (_, target_kinds) = match_binding_vars(&set.match_part)?;
    for item in &set.items {
        target_kinds
            .get(set_item_var(item))
            .ok_or_else(|| EngineError::UnboundVariable(set_item_var(item).to_string()))?;
    }

    let projected = set
        .items
        .iter()
        .enumerate()
        .filter_map(|(idx, item)| match item {
            SetItem::Prop { value, .. } => Some((value.clone(), Some(set_value_alias(idx)))),
            SetItem::Label { .. } => None,
        })
        .collect::<Vec<_>>();
    let (batches, vars, kinds) = resolve_match_projection(
        state,
        graph,
        &set.match_part,
        params,
        system,
        projected,
        overlay,
        security,
    )
    .await?;
    let bounds = TemporalBounds {
        valid: TemporalDimension::at(system),
        system: TemporalDimension::at(system),
    };
    let mut pending = BTreeMap::new();
    let mut ordered_rows = Vec::new();

    for (batch_idx, batch) in batches.iter().enumerate() {
        for row_idx in 0..batch.num_rows() {
            let mut iid_tuple = Vec::with_capacity(vars.len());
            for var in &vars {
                let iid = varve_plan::binding_iid(batch, var, row_idx)?.ok_or_else(|| {
                    EngineError::Unsupported("SET match produced a null binding iid".into())
                })?;
                iid_tuple.push(iid);
            }
            ordered_rows.push((iid_tuple, batch_idx, row_idx));
        }
    }
    ordered_rows.sort_by(|left, right| left.0.cmp(&right.0));

    for (_, batch_idx, row_idx) in ordered_rows {
        let batch = &batches[batch_idx];
        let row = row_bindings(batch, &vars, row_idx)?;
        for (idx, item) in set.items.iter().enumerate() {
            let var = set_item_var(item);
            let kind = *kinds
                .get(var)
                .ok_or_else(|| EngineError::UnboundVariable(var.to_string()))?;
            let Some(iid) = row.get(var).copied() else {
                continue;
            };
            let (src, dst) = if kind == BindingKind::Edge {
                let (src, dst) = edge_endpoints_from_match_row(&set.match_part, var, &row)?;
                (Some(src), Some(dst))
            } else {
                (None, None)
            };
            if !ensure_pending_entity(
                &mut pending,
                state,
                graph,
                kind,
                iid,
                src,
                dst,
                &bounds,
                overlay,
            )
            .await?
            {
                continue;
            }
            let Some(entity) = pending.get_mut(&iid) else {
                return Err(EngineError::Unsupported(
                    "pending entity missing after load".into(),
                ));
            };
            match item {
                SetItem::Prop { prop, .. } => {
                    let value = value_from_batch(batch, &set_value_alias(idx), row_idx)?;
                    entity.doc.insert(prop.clone(), value);
                }
                SetItem::Label { label, .. } => add_label(&mut entity.labels, label),
            }
        }
    }

    let mut effects = Effects::default();
    push_pending_effects(pending, &mut effects, system);
    Ok(effects)
}

async fn resolve_remove(
    state: &WriterState,
    graph: &str,
    remove: &RemoveStmt,
    params: &BTreeMap<String, Value>,
    system: Instant,
    overlay: Option<&Overlay>,
    security: Option<&GraphGrants>,
) -> Result<Effects, EngineError> {
    let (binding_rows, kinds) =
        resolve_match_part(state, graph, &remove.match_part, params, system, overlay, security)
            .await?;
    for item in &remove.items {
        kinds
            .get(remove_item_var(item))
            .ok_or_else(|| EngineError::UnboundVariable(remove_item_var(item).to_string()))?;
    }

    let bounds = TemporalBounds {
        valid: TemporalDimension::at(system),
        system: TemporalDimension::at(system),
    };
    let mut pending = BTreeMap::new();

    for row in &binding_rows {
        for item in &remove.items {
            let var = remove_item_var(item);
            let kind = *kinds
                .get(var)
                .ok_or_else(|| EngineError::UnboundVariable(var.to_string()))?;
            let Some(iid) = row.get(var).copied() else {
                continue;
            };
            let (src, dst) = if kind == BindingKind::Edge {
                let (src, dst) = edge_endpoints_from_match_row(&remove.match_part, var, row)?;
                (Some(src), Some(dst))
            } else {
                (None, None)
            };
            if !ensure_pending_entity(
                &mut pending,
                state,
                graph,
                kind,
                iid,
                src,
                dst,
                &bounds,
                overlay,
            )
            .await?
            {
                continue;
            }
            let Some(entity) = pending.get_mut(&iid) else {
                return Err(EngineError::Unsupported(
                    "pending entity missing after load".into(),
                ));
            };
            match item {
                RemoveItem::Prop { prop, .. } => {
                    entity.doc.remove(prop);
                }
                RemoveItem::Label { label, .. } => remove_label(&mut entity.labels, label),
            }
        }
    }

    let mut effects = Effects::default();
    push_pending_effects(pending, &mut effects, system);
    Ok(effects)
}

fn mutation_op(kind: &MutKind) -> Op {
    match kind {
        MutKind::Delete => Op::Delete,
        MutKind::Erase => Op::Erase,
    }
}

fn mutation_valid_from(kind: &MutKind, system: Instant) -> Instant {
    match kind {
        MutKind::Delete => system,
        MutKind::Erase => Instant::MIN,
    }
}

fn mutation_name(kind: &MutKind) -> &'static str {
    match kind {
        MutKind::Delete => "DELETE",
        MutKind::Erase => "ERASE",
    }
}

async fn resolve_delete(
    state: &WriterState,
    graph: &str,
    del: &MutateStmt,
    params: &BTreeMap<String, Value>,
    system: Instant,
    overlay: Option<&Overlay>,
    security: Option<&GraphGrants>,
) -> Result<Effects, EngineError> {
    let (binding_rows, kinds) =
        resolve_match_part(state, graph, &del.match_part, params, system, overlay, security)
            .await?;
    let target_kind = kinds
        .get(&del.target)
        .ok_or_else(|| EngineError::UnboundVariable(del.target.clone()))?;
    let mut iids = binding_rows
        .iter()
        .filter_map(|row| row.get(&del.target).copied())
        .collect::<Vec<_>>();
    iids.sort();
    iids.dedup();
    let op = mutation_op(&del.kind);
    let valid_from = mutation_valid_from(&del.kind, system);
    let name = mutation_name(&del.kind);

    let bounds = TemporalBounds {
        valid: TemporalDimension::at(system),
        system: TemporalDimension::at(system),
    };

    if *target_kind == BindingKind::Edge {
        let mut edges = BTreeMap::new();
        for row in &binding_rows {
            let Some(iid) = row.get(&del.target).copied() else {
                continue;
            };
            let endpoints = edge_endpoints_from_match_row(&del.match_part, &del.target, row)?;
            if let Some(existing) = edges.insert(iid, endpoints) {
                if existing != endpoints {
                    return Err(EngineError::Unsupported(format!(
                        "{name} edge variable '{}' matched conflicting endpoints",
                        del.target
                    )));
                }
            }
        }
        let mut effects = Effects::default();
        for (iid, (src, dst)) in edges {
            if let Some((_, doc)) =
                visible_payload(state, graph, BindingKind::Edge, iid, &bounds, overlay).await?
            {
                effects.record_edge_delete(&doc);
            }
            effects.edges.push(Event {
                iid,
                system_from: system,
                valid_from,
                valid_to: Instant::END_OF_TIME,
                src: Some(src),
                dst: Some(dst),
                op: op.clone(),
            });
        }
        return Ok(effects);
    }
    // Incident edges per matched node, both directions, deduped by edge iid.
    // Label "" would match nothing through edge_adjacency's label filter, so
    // incident lookup scans ALL labels: collect via a label-blind variant.
    let mut incident: BTreeMap<Iid, (Iid, Iid)> = BTreeMap::new(); // edge → (src, dst)
    for node in &iids {
        for dir in [AdjDirection::Out, AdjDirection::In] {
            for entry in incident_edges(
                &state.state,
                &state.store,
                graph,
                dir,
                *node,
                &bounds,
                overlay,
            )
            .await?
            {
                let (src, dst) = match dir {
                    AdjDirection::Out => (entry.node, entry.neighbor),
                    AdjDirection::In => (entry.neighbor, entry.node),
                };
                incident.insert(entry.edge, (src, dst));
            }
        }
    }

    if !del.detach && !incident.is_empty() {
        return Err(EngineError::StillConnected(incident.len()));
    }

    let mut effects = Effects::default();
    for (edge, (src, dst)) in incident {
        if let Some((_, doc)) =
            visible_payload(state, graph, BindingKind::Edge, edge, &bounds, overlay).await?
        {
            effects.record_edge_delete(&doc);
        }
        effects.edges.push(Event {
            iid: edge,
            system_from: system,
            valid_from,
            valid_to: Instant::END_OF_TIME,
            src: Some(src),
            dst: Some(dst),
            op: op.clone(),
        });
    }
    for iid in iids {
        if let Some((labels, doc)) =
            visible_payload(state, graph, BindingKind::Node, iid, &bounds, overlay).await?
        {
            effects.record_node_delete(&labels, &doc);
        }
        effects.nodes.push(Event {
            iid,
            system_from: system,
            valid_from,
            valid_to: Instant::END_OF_TIME,
            src: None,
            dst: None,
            op: op.clone(),
        });
    }
    Ok(effects)
}

/// Result of one flush attempt (Task 8): `Fatal` means the writer must stop
/// serving — a lost/expired lease or a post-durability apply failure both
/// break the invariants the loop otherwise relies on (epoch takeover means a
/// stale writer can no longer assume its acks are safe; an apply failure
/// after durability means the live index and the durable log have diverged).
pub(crate) enum FlushOutcome {
    Continue,
    Fatal(String),
}

/// `None` if acks may proceed under the current lease; `Some(reason)` if the
/// lease is lost or its deadline has passed — the caller must fence instead.
fn lease_block(lease: &watch::Receiver<LeaseState>) -> Option<String> {
    match &*lease.borrow() {
        LeaseState::Unfenced => None,
        LeaseState::ValidUntil(deadline) if tokio::time::Instant::now() < *deadline => None,
        LeaseState::ValidUntil(_) => Some("lease expired before ack".to_string()),
        LeaseState::Lost(reason) => Some(reason.clone()),
    }
}

/// Acks every staged tx `Err(WriterFenced(reason))` and reports `Fatal` —
/// used both before append (never even durable) and after a durable append
/// discovers the fence too late to ack (never visible either way).
fn fence_all(staged: Vec<Staged>, reason: String) -> FlushOutcome {
    for s in staged {
        let _ = s.ack.send(Err(EngineError::WriterFenced(reason.clone())));
    }
    FlushOutcome::Fatal(reason)
}

/// Lease-gates a Compact ack exactly like `flush` gates a staged batch (Task
/// 8 review finding 1) — with the SAME two checkpoints `flush` uses (Task 8
/// review pass 2 finding): `compact_once` is a long multi-`.await` durable
/// operation (manifest history fetch, per-trie get/put, final manifest put),
/// so the lease checked once BEFORE it runs can no longer speak for the
/// lease's state once it returns. Acks `Err(WriterFenced(reason))` and
/// returns `Some(reason)` instead of compacting when the PRE-check finds the
/// lease invalid; if `compact_once` succeeds, the lease is re-checked once
/// more — a lease lost mid-flight must still fence the ack (the compaction's
/// durable writes may already be persisted, but a fenced writer must never
/// ack `Ok`) — only a lease still valid after compaction acks the real
/// result. A `compact_once` error is acked as-is, unchanged from before this
/// fix: its failure is not itself evidence of a lease problem.
///
/// A fenced-but-alive writer's in-flight manifest PUT (inside
/// `compact_once`/`flush_block`) is not itself epoch-fenced — only the log
/// is; this gate (never acking `Ok`, going fatal instead) is what contains
/// that narrow alive-zombie race before-and-after the manifest PUT. Should
/// such a writer's manifest PUT still land, `latest_manifest` (slice 11)
/// now selects the newest manifest by `(watermark, block_id)` rather than
/// max `block_id` alone, so a stray manifest with a newer block id but a
/// stale watermark can never be selected during recovery/verify/follower
/// reads — the before+after lease ack-gate here remains the liveness guard
/// that keeps the writer itself from acking `Ok` once fenced.
async fn gated_compact(
    state: &mut WriterState,
    ack: oneshot::Sender<Result<CompactionReport, EngineError>>,
    full: bool,
) -> Option<String> {
    if let Some(reason) = lease_block(&state.lease) {
        let _ = ack.send(Err(EngineError::WriterFenced(reason.clone())));
        return Some(reason);
    }
    match compact_once(state, full).await {
        Ok(report) => {
            if let Some(reason) = lease_block(&state.lease) {
                let _ = ack.send(Err(EngineError::WriterFenced(reason.clone())));
                return Some(reason);
            }
            let _ = ack.send(Ok(report));
            None
        }
        Err(e) => {
            let _ = ack.send(Err(e));
            None
        }
    }
}

/// Durable append → apply → ack, strictly in that order (decision 1), gated
/// by the lease both before append (never durable) and after (durable but
/// possibly beyond the fence — never ack). A post-durability apply failure
/// is FATAL (Task 8): epoch takeover breaks the monotonicity argument that
/// once made "keep serving" safe, since the live index and the durable log
/// have now diverged.
///
/// `varve.commit` (Task 13, fields `batch`, `first_position`): created HERE,
/// inside `flush` itself, rather than at its `run_batch` call sites — the
/// writer.rs unit test calls `flush` directly, so the span must be
/// self-contained to be observable there. `batch` (`staged.len()`) is known
/// up front; `first_position` isn't known until `log.append` returns, so
/// that field starts `Empty` and is recorded once the append succeeds.
async fn flush(state: &mut WriterState, staged: Vec<Staged>) -> FlushOutcome {
    let batch = staged.len() as u64;
    let span = tracing::info_span!(
        "varve.commit",
        batch,
        first_position = tracing::field::Empty
    );
    flush_impl(state, staged).instrument(span).await
}

async fn flush_impl(state: &mut WriterState, mut staged: Vec<Staged>) -> FlushOutcome {
    if let Some(reason) = lease_block(&state.lease) {
        return fence_all(staged, reason);
    }
    let records: Vec<LogRecord> = staged.iter().map(|s| s.record.clone()).collect();
    let count = records.len() as u64;
    // Captured before `apply` (which `mem::take`s each staged tx's event
    // vecs) so the Task 12 `events_committed` counter still sees them.
    let batch_len = staged.len() as u64;
    let event_count: u64 = staged
        .iter()
        .map(|s| (s.events.nodes.len() + s.events.edges.len()) as u64)
        .sum();
    match state.log.append(records).await {
        Ok(first) => {
            tracing::Span::current().record("first_position", first.as_u64());
            if let Some(reason) = lease_block(&state.lease) {
                return fence_all(staged, reason);
            }
            // Exclusive end of the durable prefix — becomes the next
            // manifest's watermark (decision 6; positions are consecutive
            // per the `Log` contract). On (unreachable) 48-bit offset
            // overflow, keep the old, still-conservative watermark.
            if let Ok(end) = first.advance(count) {
                state.durable_watermark = end;
            }
            // `varve.apply` (field `batch`): `apply` is a plain sync
            // function with no `.await` inside it, so this is a plain
            // `entered()` guard — never held across an await.
            let applied = {
                let _g = tracing::info_span!("varve.apply", batch = batch_len).entered();
                apply(state, &mut staged)
            };
            match applied {
                Ok(()) => {
                    if let (Some(last), Ok(end)) = (staged.last(), first.advance(count)) {
                        // `log_head = durable_watermark`: lag 0 by
                        // construction (Task 12) — the writer always
                        // publishes its own just-flushed watermark.
                        state.progress.send_replace(ProgressState::running(
                            last.receipt.tx_id,
                            end,
                            end,
                        ));
                    }
                    state
                        .metrics
                        .txs_committed
                        .fetch_add(batch_len, std::sync::atomic::Ordering::Relaxed);
                    state
                        .metrics
                        .events_committed
                        .fetch_add(event_count, std::sync::atomic::Ordering::Relaxed);
                    for s in staged {
                        let _ = s.ack.send(Ok(s.receipt));
                    }
                    FlushOutcome::Continue
                }
                Err(msg) => {
                    tracing::error!(
                        error = %msg,
                        "apply failed after durable append; writer stopping (fatal): the live \
                         index and durable log have diverged"
                    );
                    for s in staged {
                        let _ = s.ack.send(Err(EngineError::CommitFailed(msg.clone())));
                    }
                    FlushOutcome::Fatal(format!("apply failed after durable append: {msg}"))
                }
            }
        }
        Err(e) => {
            // Nothing applied, so live-index state is untouched: fail the
            // whole batch and keep serving (the log itself, not this
            // in-memory index, is what may need operator attention) — a
            // PRE-durability append failure is not fatal (unchanged).
            tracing::error!(error = %e, "log append failed; batch not durable, acked as failed");
            state
                .metrics
                .commit_failures
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let msg = e.to_string();
            for s in staged {
                let _ = s.ack.send(Err(EngineError::CommitFailed(msg.clone())));
            }
            FlushOutcome::Continue
        }
    }
}

/// Marks the writer FATAL on the shared progress channel — `/healthz` and
/// `Db::follower_error()` both read `follower_error` off this state, so a
/// fatal writer degrades health exactly like a stalled follower does.
fn publish_fatal(state: &WriterState, reason: &str) {
    let mut next = state.progress.borrow().clone();
    next.follower_error = Some(format!("writer stopped: {reason}"));
    state.progress.send_replace(next);
}

/// Acks every subsequent command `Err(WriterFenced(reason))` until the
/// channel closes — entered once and only once, after `flush` reports
/// `Fatal`; no block flush and no further staging happens past this point.
async fn drain(rx: &mut mpsc::Receiver<Command>, reason: String) {
    while let Some(command) = rx.recv().await {
        match command {
            Command::Submit(sub) => {
                let _ = sub.ack.send(Err(EngineError::WriterFenced(reason.clone())));
            }
            Command::Compact { ack, .. } => {
                let _ = ack.send(Err(EngineError::WriterFenced(reason.clone())));
            }
        }
    }
}

fn apply(state: &WriterState, staged: &mut [Staged]) -> Result<(), String> {
    let mut shared = state
        .state
        .write()
        .map_err(|_| "table state lock poisoned".to_string())?;
    for s in staged.iter_mut() {
        for op in std::mem::take(&mut s.events.catalog_ops) {
            match op {
                CatalogOp::CreateGraph(name) => {
                    shared.insert_graph(name);
                }
                CatalogOp::DropGraph(name) => {
                    shared.remove_graph(&name);
                }
            }
        }

        let touches_security =
            s.graph == SECURITY_GRAPH && (!s.events.nodes.is_empty() || !s.events.edges.is_empty());
        let table = shared
            .graph_mut(&s.graph)
            .ok_or_else(|| format!("unknown graph '{}'", s.graph))?;
        for event in std::mem::take(&mut s.events.nodes) {
            table.nodes.live.append(event).map_err(|e| e.to_string())?;
        }
        for event in std::mem::take(&mut s.events.edges) {
            table.edges.live.append(event).map_err(|e| e.to_string())?;
        }
        if touches_security {
            // Exact invalidation for the per-subject SecurityContext cache.
            shared.security_epoch += 1;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::MonotonicClock;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering};
    use std::sync::Mutex;
    use std::task::{Context, Poll, Wake, Waker};
    use tokio::sync::Notify;
    use varve_log::{LogError, MemoryLog};
    use varve_types::LogPosition;

    struct CountingLog {
        inner: MemoryLog,
        appends: AtomicUsize,
    }

    struct BlockingAppendLog {
        inner: MemoryLog,
        appends: AtomicUsize,
        entered: Mutex<Option<oneshot::Sender<()>>>,
        release: Notify,
    }

    #[async_trait::async_trait]
    impl Log for BlockingAppendLog {
        async fn append(&self, records: Vec<LogRecord>) -> Result<LogPosition, LogError> {
            self.appends.fetch_add(1, AtomicOrdering::SeqCst);
            if let Some(entered) = self.entered.lock().unwrap().take() {
                let _ = entered.send(());
            }
            self.release.notified().await;
            self.inner.append(records).await
        }

        async fn read_range(
            &self,
            from: LogPosition,
            to: LogPosition,
        ) -> Result<Vec<(LogPosition, LogRecord)>, LogError> {
            self.inner.read_range(from, to).await
        }

        async fn trim(&self, up_to: LogPosition) -> Result<(), LogError> {
            self.inner.trim(up_to).await
        }

        async fn head(&self) -> Result<LogPosition, LogError> {
            self.inner.head().await
        }

        async fn start_epoch(&self, epoch: u16) -> Result<(), LogError> {
            self.inner.start_epoch(epoch).await
        }
    }

    struct WakeOrder {
        order: AtomicUsize,
        next: Arc<AtomicUsize>,
        notified: Arc<Notify>,
    }

    impl WakeOrder {
        fn new(next: Arc<AtomicUsize>, notified: Arc<Notify>) -> Arc<Self> {
            Arc::new(Self {
                order: AtomicUsize::new(0),
                next,
                notified,
            })
        }

        fn order(&self) -> usize {
            self.order.load(AtomicOrdering::SeqCst)
        }

        fn record(&self) {
            if self.order() == 0 {
                let order = self.next.fetch_add(1, AtomicOrdering::SeqCst) + 1;
                let _ = self.order.compare_exchange(
                    0,
                    order,
                    AtomicOrdering::SeqCst,
                    AtomicOrdering::SeqCst,
                );
            }
            self.notified.notify_one();
        }
    }

    impl Wake for WakeOrder {
        fn wake(self: Arc<Self>) {
            self.record();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.record();
        }
    }

    impl CountingLog {
        fn new() -> Arc<CountingLog> {
            Arc::new(CountingLog {
                inner: MemoryLog::new(),
                appends: AtomicUsize::new(0),
            })
        }
    }

    #[async_trait::async_trait]
    impl Log for CountingLog {
        async fn append(&self, records: Vec<LogRecord>) -> Result<LogPosition, LogError> {
            self.appends.fetch_add(1, AtomicOrdering::SeqCst);
            self.inner.append(records).await
        }
        async fn read_range(
            &self,
            from: LogPosition,
            to: LogPosition,
        ) -> Result<Vec<(LogPosition, LogRecord)>, LogError> {
            self.inner.read_range(from, to).await
        }
        async fn trim(&self, up_to: LogPosition) -> Result<(), LogError> {
            self.inner.trim(up_to).await
        }

        async fn head(&self) -> Result<LogPosition, LogError> {
            self.inner.head().await
        }

        async fn start_epoch(&self, epoch: u16) -> Result<(), LogError> {
            self.inner.start_epoch(epoch).await
        }
    }

    /// Fails the first append with an I/O error, then delegates.
    struct FailOnceLog {
        inner: MemoryLog,
        failed: AtomicBool,
    }

    #[async_trait::async_trait]
    impl Log for FailOnceLog {
        async fn append(&self, records: Vec<LogRecord>) -> Result<LogPosition, LogError> {
            if !self.failed.swap(true, AtomicOrdering::SeqCst) {
                return Err(LogError::Io(std::io::Error::other(
                    "injected append failure",
                )));
            }
            self.inner.append(records).await
        }
        async fn read_range(
            &self,
            from: LogPosition,
            to: LogPosition,
        ) -> Result<Vec<(LogPosition, LogRecord)>, LogError> {
            self.inner.read_range(from, to).await
        }
        async fn trim(&self, up_to: LogPosition) -> Result<(), LogError> {
            self.inner.trim(up_to).await
        }

        async fn head(&self) -> Result<LogPosition, LogError> {
            self.inner.head().await
        }

        async fn start_epoch(&self, epoch: u16) -> Result<(), LogError> {
            self.inner.start_epoch(epoch).await
        }
    }

    fn spawn_with_progress(
        log: Arc<dyn Log>,
        cfg: WriterConfig,
    ) -> (
        WriterHandle,
        Arc<RwLock<GraphsState>>,
        watch::Receiver<ProgressState>,
    ) {
        let state = Arc::new(RwLock::new(GraphsState::new()));
        let (progress, progress_rx) = watch::channel(ProgressState::running(
            0,
            LogPosition::ZERO,
            LogPosition::ZERO,
        ));
        let writer_state = WriterState {
            state: Arc::clone(&state),
            store: varve_storage::memory_store(),
            clock: Arc::new(MonotonicClock::new()),
            functions: Arc::new(varve_plan::FunctionRegistry::with_builtins()),
            max_path_depth: 10,
            query_limits: varve_plan::QueryLimits::default(),
            log,
            next_tx_id: 0,
            next_block_id: 0,
            durable_watermark: LogPosition::ZERO,
            progress,
            lease: watch::channel(LeaseState::Unfenced).1,
            metrics: Arc::new(EngineMetrics::default()),
            security: SecurityEnforcer::new(crate::security::SecurityTuning::default()),
        };
        (spawn_writer(writer_state, cfg), state, progress_rx)
    }

    fn spawn(log: Arc<dyn Log>, cfg: WriterConfig) -> (WriterHandle, Arc<RwLock<GraphsState>>) {
        let (writer, state, _progress) = spawn_with_progress(log, cfg);
        (writer, state)
    }

    /// try_send keeps submission order deterministic (mpsc is FIFO).
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
                payload: Payload::Program {
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

    /// Like `spawn`, but with an explicit lease receiver (Task 8) instead of
    /// the always-`Unfenced` default, and the progress receiver exposed so
    /// tests can assert on `follower_error` (review finding 2). Callers pick
    /// the `WriterConfig` — most want `window: Duration::ZERO` so every
    /// submission flushes without waiting for the group-commit window, but a
    /// non-zero window lets a test keep a write staged (review finding 3).
    fn spawn_with_lease(
        lease: watch::Receiver<LeaseState>,
        cfg: WriterConfig,
    ) -> (
        WriterHandle,
        Arc<RwLock<GraphsState>>,
        watch::Receiver<ProgressState>,
    ) {
        let state = Arc::new(RwLock::new(GraphsState::new()));
        let (progress, progress_rx) = watch::channel(ProgressState::running(
            0,
            LogPosition::ZERO,
            LogPosition::ZERO,
        ));
        let writer_state = WriterState {
            state: Arc::clone(&state),
            store: varve_storage::memory_store(),
            clock: Arc::new(MonotonicClock::new()),
            functions: Arc::new(varve_plan::FunctionRegistry::with_builtins()),
            max_path_depth: 10,
            query_limits: varve_plan::QueryLimits::default(),
            log: Arc::new(MemoryLog::new()),
            next_tx_id: 0,
            next_block_id: 0,
            durable_watermark: LogPosition::ZERO,
            progress,
            lease,
            metrics: Arc::new(EngineMetrics::default()),
            security: SecurityEnforcer::new(crate::security::SecurityTuning::default()),
        };
        (spawn_writer(writer_state, cfg), state, progress_rx)
    }

    /// `spawn_with_lease` with the common zero-window config (immediate
    /// per-submission flush).
    fn spawn_with_lease_zero_window(
        lease: watch::Receiver<LeaseState>,
    ) -> (
        WriterHandle,
        Arc<RwLock<GraphsState>>,
        watch::Receiver<ProgressState>,
    ) {
        spawn_with_lease(
            lease,
            WriterConfig {
                window: Duration::ZERO,
                max_bytes: 8 * 1024 * 1024,
                ..WriterConfig::default()
            },
        )
    }

    /// Live (unflushed) node-event count in the default graph.
    fn live_event_count(live: &Arc<RwLock<GraphsState>>) -> usize {
        live.read()
            .unwrap()
            .graph(DEFAULT_GRAPH)
            .unwrap()
            .nodes
            .live
            .event_count()
    }

    #[tokio::test]
    async fn a_lost_lease_fences_acks_and_stops_the_writer() {
        let (lease_tx, lease_rx) = watch::channel(LeaseState::Unfenced);
        let (sender, live, progress_rx) = spawn_with_lease_zero_window(lease_rx);
        submit(&sender, "INSERT (:P {_id: 1})")
            .await
            .unwrap()
            .unwrap();

        lease_tx.send_replace(LeaseState::Lost("seized in test".into()));
        let fenced = submit(&sender, "INSERT (:P {_id: 2})").await.unwrap();
        assert!(
            matches!(&fenced, Err(EngineError::WriterFenced(reason)) if reason.contains("seized")),
            "expected WriterFenced(\"seized...\"), got {fenced:?}"
        );

        // The writer has stopped and drains — later submissions also fence,
        // and nothing beyond the first tx was ever applied.
        let again = submit(&sender, "INSERT (:P {_id: 3})").await.unwrap();
        assert!(matches!(again, Err(EngineError::WriterFenced(_))));
        assert_eq!(live_event_count(&live), 1);

        // Review finding 2: Fatal must publish `follower_error` so /healthz
        // degrades. By now `again`'s ack (sent from inside `drain`) has
        // resolved, so `publish_fatal` — called before `drain` starts — has
        // already run.
        let follower_error = progress_rx.borrow().follower_error.clone();
        assert!(
            matches!(&follower_error, Some(msg) if msg.contains("writer stopped") && msg.contains("seized")),
            "expected follower_error to report the writer-stopped/lease reason, got {follower_error:?}"
        );
    }

    #[tokio::test]
    async fn an_expired_lease_deadline_blocks_acks() {
        let (_lease_tx, lease_rx) = watch::channel(LeaseState::ValidUntil(
            tokio::time::Instant::now() - Duration::from_millis(1),
        ));
        let (sender, _live, _progress_rx) = spawn_with_lease_zero_window(lease_rx);
        let fenced = submit(&sender, "INSERT (:P {_id: 1})").await.unwrap();
        assert!(matches!(fenced, Err(EngineError::WriterFenced(_))));
    }

    #[tokio::test]
    async fn a_lost_lease_fences_a_bare_compact() {
        // Review finding 1: `Command::Compact` must be lease-gated exactly
        // like a submission batch — `compact_once` performs real durable
        // writes (new blocks + a manifest entry) and must never run (nor
        // ack `Ok`) once the lease is lost.
        let (lease_tx, lease_rx) = watch::channel(LeaseState::Unfenced);
        let (sender, _live, _progress_rx) = spawn_with_lease_zero_window(lease_rx);

        lease_tx.send_replace(LeaseState::Lost("seized in test".into()));
        let compacted = sender.compact_once().await;
        assert!(
            matches!(&compacted, Err(EngineError::WriterFenced(reason)) if reason.contains("seized")),
            "expected WriterFenced(\"seized...\"), got {compacted:?}"
        );

        // The writer has stopped and drains: a subsequent submission fences too.
        let fenced = submit(&sender, "INSERT (:P {_id: 1})").await.unwrap();
        assert!(matches!(fenced, Err(EngineError::WriterFenced(_))));
    }

    /// A `varve_storage::ObjectStore` wrapper that flips a lease to `Lost`
    /// the moment it sees a PUT to a manifest key — `compact_once`'s LAST
    /// durable write — but only once armed. This lands the lease loss
    /// deterministically between "compaction's durable work is done" and
    /// "`compact_once` returns", with no sleep or timing dependency: the
    /// trap fires exactly once, on exactly the call that ends `compact_once`.
    struct LeaseDropOnManifestPut {
        inner: Arc<dyn varve_storage::ObjectStore>,
        lease_tx: watch::Sender<LeaseState>,
        armed: Arc<AtomicBool>,
        reason: String,
    }

    #[async_trait::async_trait]
    impl varve_storage::ObjectStore for LeaseDropOnManifestPut {
        async fn put(&self, key: &str, bytes: Bytes) -> Result<(), varve_storage::StorageError> {
            let result = self.inner.put(key, bytes).await;
            if self.armed.load(AtomicOrdering::SeqCst) && keys::manifest_block_id(key).is_some() {
                self.lease_tx
                    .send_replace(LeaseState::Lost(self.reason.clone()));
            }
            result
        }

        async fn get(&self, key: &str) -> Result<Bytes, varve_storage::StorageError> {
            self.inner.get(key).await
        }

        async fn get_range(
            &self,
            key: &str,
            range: std::ops::Range<u64>,
        ) -> Result<Bytes, varve_storage::StorageError> {
            self.inner.get_range(key, range).await
        }

        async fn list(&self, prefix: &str) -> Result<Vec<String>, varve_storage::StorageError> {
            self.inner.list(prefix).await
        }

        async fn delete(&self, key: &str) -> Result<(), varve_storage::StorageError> {
            self.inner.delete(key).await
        }
    }

    #[tokio::test]
    async fn a_lease_lost_during_compact_once_fences_the_ack_not_ok() {
        // Task 8 review pass 2: `compact_once` performs several durable
        // `.await`s (manifest history, per-trie get/put, final manifest
        // put) after `gated_compact`'s pre-check already found the lease
        // valid. If the lease is lost during that window, `compact_once`
        // still returns `Ok` — the fix is a SECOND lease check after it
        // returns, mirroring `flush`'s before+after checkpoints. Without
        // that post-check, this test's ack would be `Ok(_)` instead of
        // `Err(WriterFenced(_))`.
        let (lease_tx, lease_rx) = watch::channel(LeaseState::Unfenced);
        let armed = Arc::new(AtomicBool::new(false));
        let store: Arc<dyn varve_storage::ObjectStore> = Arc::new(LeaseDropOnManifestPut {
            inner: varve_storage::memory_store(),
            lease_tx,
            armed: Arc::clone(&armed),
            reason: "seized mid-compact".to_string(),
        });

        let graphs = Arc::new(RwLock::new(GraphsState::new()));
        let (progress, progress_rx) = watch::channel(ProgressState::running(
            0,
            LogPosition::ZERO,
            LogPosition::ZERO,
        ));
        let mut state = WriterState {
            state: Arc::clone(&graphs),
            store,
            clock: Arc::new(MonotonicClock::new()),
            functions: Arc::new(varve_plan::FunctionRegistry::with_builtins()),
            max_path_depth: 10,
            query_limits: varve_plan::QueryLimits::default(),
            log: Arc::new(MemoryLog::new()),
            next_tx_id: 0,
            next_block_id: 0,
            durable_watermark: LogPosition::ZERO,
            progress,
            lease: lease_rx,
            metrics: Arc::new(EngineMetrics::default()),
            security: SecurityEnforcer::new(crate::security::SecurityTuning::default()),
        };

        // `select_compaction_jobs`'s L0 job needs `CompactionConfig::default()
        // .log_limit` (64) live L0 tries before it selects anything at all
        // (see `crates/varve-engine/src/compact.rs`), so build 64 one-row L0
        // blocks by flushing directly, bypassing the writer queue and its
        // batching thresholds entirely.
        for i in 0..64u64 {
            let event = Event {
                iid: Iid::derive(DEFAULT_GRAPH, NODES_TABLE, &i.to_be_bytes()),
                system_from: Instant::from_micros(i as i64 + 1),
                valid_from: Instant::from_micros(i as i64 + 1),
                valid_to: Instant::END_OF_TIME,
                src: None,
                dst: None,
                op: Op::Put {
                    labels: vec!["P".to_string()],
                    doc: Doc::new(),
                },
            };
            graphs
                .write()
                .unwrap()
                .graph_mut(DEFAULT_GRAPH)
                .unwrap()
                .nodes
                .live
                .append(event)
                .unwrap();
            crate::flush::flush_block(&mut state).await.unwrap();
        }

        // Arm the trap only now: the 64 setup flushes above must not trip it.
        armed.store(true, AtomicOrdering::SeqCst);

        let sender = spawn_writer(state, WriterConfig::default());
        let compacted = sender.compact_once().await;
        assert!(
            matches!(&compacted, Err(EngineError::WriterFenced(reason)) if reason.contains("seized")),
            "expected a mid-compaction lease loss to fence the ack as WriterFenced, got {compacted:?}"
        );

        // The writer must have gone fatal exactly like the pre-check path:
        // a subsequent submission fences too, and follower_error is set.
        let fenced = submit(&sender, "INSERT (:P {_id: 999})").await.unwrap();
        assert!(matches!(fenced, Err(EngineError::WriterFenced(_))));
        let follower_error = progress_rx.borrow().follower_error.clone();
        assert!(
            matches!(&follower_error, Some(msg) if msg.contains("writer stopped") && msg.contains("seized")),
            "expected follower_error to report the writer-stopped/lease reason, got {follower_error:?}"
        );
    }

    #[tokio::test]
    async fn a_mid_batch_reading_statement_fences_its_trigger_and_the_staged_write() {
        // Review finding 3: with a non-zero window, a staged (not-yet-durable)
        // write must be fenced when a later reading statement forces a
        // mid-batch flush under a lost lease — AND the trigger statement's
        // own ack must be fenced too. mpsc is FIFO and both submissions
        // below happen without any intervening `.await`, so the writer task
        // cannot observe them out of order (see `submit`'s doc comment) —
        // no sleeps or timing needed.
        let (lease_tx, lease_rx) = watch::channel(LeaseState::Unfenced);
        let cfg = WriterConfig {
            window: Duration::from_secs(3600),
            max_bytes: 8 * 1024 * 1024,
            ..WriterConfig::default()
        };
        let (sender, live, _progress_rx) = spawn_with_lease(lease_rx, cfg);

        let staged_write = submit(&sender, "INSERT (:P {_id: 1})");
        lease_tx.send_replace(LeaseState::Lost("seized in test".into()));
        let trigger = submit(&sender, "MATCH (p:P) DELETE p");

        let staged_result = staged_write.await.unwrap();
        assert!(
            matches!(&staged_result, Err(EngineError::WriterFenced(reason)) if reason.contains("seized")),
            "expected the staged write to be fenced by the forced flush, got {staged_result:?}"
        );
        let trigger_result = trigger.await.unwrap();
        assert!(
            matches!(&trigger_result, Err(EngineError::WriterFenced(reason)) if reason.contains("seized")),
            "expected the trigger statement itself to be fenced, got {trigger_result:?}"
        );
        assert_eq!(
            live_event_count(&live),
            0,
            "the staged write must never be applied once fenced"
        );
    }

    #[tokio::test]
    async fn apply_failure_after_durable_append_is_fatal() {
        // Pre-seed the live table with an event whose system_from is LATER
        // than the "bad" staged event below, so `apply()` trips
        // `LiveTable::append`'s monotonicity check (out-of-order rejection).
        let graphs = Arc::new(RwLock::new(GraphsState::new()));
        let seeded = Event {
            iid: Iid::derive(DEFAULT_GRAPH, NODES_TABLE, b"seeded"),
            system_from: Instant::from_micros(200),
            valid_from: Instant::from_micros(200),
            valid_to: Instant::END_OF_TIME,
            src: None,
            dst: None,
            op: Op::Put {
                labels: vec!["P".to_string()],
                doc: Doc::new(),
            },
        };
        graphs
            .write()
            .unwrap()
            .graph_mut(DEFAULT_GRAPH)
            .unwrap()
            .nodes
            .live
            .append(seeded)
            .unwrap();

        let bad_event = Event {
            iid: Iid::derive(DEFAULT_GRAPH, NODES_TABLE, b"bad"),
            system_from: Instant::from_micros(100), // precedes the seeded event
            valid_from: Instant::from_micros(100),
            valid_to: Instant::END_OF_TIME,
            src: None,
            dst: None,
            op: Op::Put {
                labels: vec!["P".to_string()],
                doc: Doc::new(),
            },
        };
        let (ack, ack_rx) = oneshot::channel();
        let bad_staged = Staged {
            graph: DEFAULT_GRAPH.to_string(),
            record: LogRecord {
                tx_id: 1,
                system_time_us: 100,
                user: String::new(),
                effects: Vec::new(),
            },
            events: Effects {
                nodes: vec![bad_event],
                ..Effects::default()
            },
            receipt: TxReceipt {
                tx_id: 1,
                system_time: Instant::from_micros(100),
                side_effects: SideEffects::default(),
            },
            ack,
        };

        let (progress, _progress_rx) = watch::channel(ProgressState::running(
            0,
            LogPosition::ZERO,
            LogPosition::ZERO,
        ));
        let mut state = WriterState {
            state: graphs,
            store: varve_storage::memory_store(),
            clock: Arc::new(MonotonicClock::new()),
            functions: Arc::new(varve_plan::FunctionRegistry::with_builtins()),
            max_path_depth: 10,
            query_limits: varve_plan::QueryLimits::default(),
            log: Arc::new(MemoryLog::new()),
            next_tx_id: 1,
            next_block_id: 0,
            durable_watermark: LogPosition::ZERO,
            progress,
            lease: watch::channel(LeaseState::Unfenced).1,
            metrics: Arc::new(EngineMetrics::default()),
            security: SecurityEnforcer::new(crate::security::SecurityTuning::default()),
        };

        let outcome = flush(&mut state, vec![bad_staged]).await;
        assert!(matches!(outcome, FlushOutcome::Fatal(_)));
        // the ack carried CommitFailed (apply failed after durable append):
        assert!(matches!(
            ack_rx.await.unwrap(),
            Err(EngineError::CommitFailed(_))
        ));
    }

    #[tokio::test]
    async fn concurrent_submissions_share_one_durable_append() {
        let log = CountingLog::new();
        let (sender, _live) = spawn(
            Arc::clone(&log) as Arc<dyn Log>,
            WriterConfig {
                window: Duration::from_secs(1),
                max_bytes: 8 * 1024 * 1024,
                ..WriterConfig::default()
            },
        );
        let acks: Vec<_> = (1..=5)
            .map(|i| submit(&sender, &format!("INSERT (:P {{_id: {i}}})")))
            .collect();
        for ack in acks {
            ack.await.unwrap().unwrap();
        }
        assert_eq!(log.appends.load(AtomicOrdering::SeqCst), 1);
        let records = log.inner.tail(LogPosition::ZERO).await.unwrap();
        assert_eq!(
            records.iter().map(|(_, r)| r.tx_id).collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 5]
        );
    }

    /// Slice 10: with `queue_len = 1`, a third submission must be rejected
    /// immediately (`Backpressure`) rather than waiting for room. `max_bytes:
    /// 1` forces every single-record batch to flush on its own (as in
    /// `size_threshold_flushes_without_waiting_for_the_window`), so the FIRST
    /// submission alone drives the writer into `BlockingAppendLog::append`
    /// and blocks it there — at that point the channel is empty (capacity 1,
    /// 0 queued), so the SECOND submission fills the one free slot and the
    /// THIRD must bounce off a full queue.
    #[tokio::test]
    async fn try_submit_on_a_full_queue_is_backpressure() {
        let (entered_tx, entered_rx) = oneshot::channel();
        let log = Arc::new(BlockingAppendLog {
            inner: MemoryLog::new(),
            appends: AtomicUsize::new(0),
            entered: Mutex::new(Some(entered_tx)),
            release: Notify::new(),
        });
        let (sender, _live) = spawn(
            Arc::clone(&log) as Arc<dyn Log>,
            WriterConfig {
                max_bytes: 1, // every record trips the threshold on its own
                queue_len: 1,
                ..WriterConfig::default()
            },
        );

        let first = submit(&sender, "INSERT (:P {_id: 1})");
        // Blocks until the writer loop has dequeued the first submission and
        // entered `append` — only then is the queue guaranteed empty again.
        entered_rx.await.unwrap();

        let second = submit(&sender, "INSERT (:P {_id: 2})"); // occupies the one free slot

        let program = varve_gql::parse_program("INSERT (:P {_id: 3})").unwrap();
        let (third_ack, third_rx) = oneshot::channel();
        let third = sender.try_submit(Submission {
            payload: Payload::Program {
                statements: program.statements,
                params: BTreeMap::new(),
            },
            graph: DEFAULT_GRAPH.to_string(),
            user: String::new(),
            ack: third_ack,
        });
        assert!(
            matches!(third, Err(EngineError::Backpressure)),
            "expected Backpressure on a full queue, got {third:?}"
        );
        drop(third_rx); // never submitted: no ack will ever arrive for it

        // Release the first blocked append; by the time its ack resolves the
        // writer loop — never having genuinely yielded in between on this
        // single-threaded runtime — has already dequeued the second
        // submission and is blocked on the SECOND `append` call.
        log.release.notify_waiters();
        first.await.unwrap().unwrap();
        log.release.notify_waiters();
        second.await.unwrap().unwrap();
        assert_eq!(log.appends.load(AtomicOrdering::SeqCst), 2);
    }

    #[tokio::test]
    async fn group_progress_is_published_before_the_first_acknowledgement() {
        let (entered_tx, entered_rx) = oneshot::channel();
        let log = Arc::new(BlockingAppendLog {
            inner: MemoryLog::new(),
            appends: AtomicUsize::new(0),
            entered: Mutex::new(Some(entered_tx)),
            release: Notify::new(),
        });
        let (sender, _live, mut progress) = spawn_with_progress(
            Arc::clone(&log) as Arc<dyn Log>,
            WriterConfig {
                window: Duration::from_secs(1),
                max_bytes: 8 * 1024 * 1024,
                ..WriterConfig::default()
            },
        );
        let mut acks = (1..=5)
            .map(|i| submit(&sender, &format!("INSERT (:P {{_id: {i}}})")))
            .collect::<Vec<_>>();
        entered_rx.await.unwrap();

        let next_order = Arc::new(AtomicUsize::new(0));
        let notified = Arc::new(Notify::new());
        let progress_wake = WakeOrder::new(Arc::clone(&next_order), Arc::clone(&notified));
        let progress_waker = Waker::from(Arc::clone(&progress_wake));
        let mut progress_context = Context::from_waker(&progress_waker);
        let mut changed = Box::pin(progress.changed());
        assert!(matches!(
            changed.as_mut().poll(&mut progress_context),
            Poll::Pending
        ));

        let ack_wakes = acks
            .iter_mut()
            .map(|ack| {
                let wake = WakeOrder::new(Arc::clone(&next_order), Arc::clone(&notified));
                let waker = Waker::from(Arc::clone(&wake));
                let mut context = Context::from_waker(&waker);
                assert!(matches!(Pin::new(ack).poll(&mut context), Poll::Pending));
                wake
            })
            .collect::<Vec<_>>();

        log.release.notify_one();
        while progress_wake.order() == 0 || ack_wakes.iter().any(|wake| wake.order() == 0) {
            notified.notified().await;
        }

        assert!(
            ack_wakes
                .iter()
                .all(|ack_wake| progress_wake.order() < ack_wake.order()),
            "progress wake {} must precede acknowledgement wakes {:?}",
            progress_wake.order(),
            ack_wakes
                .iter()
                .map(|wake| wake.order())
                .collect::<Vec<_>>()
        );

        assert!(matches!(
            changed.as_mut().poll(&mut progress_context),
            Poll::Ready(Ok(()))
        ));
        drop(changed);
        assert_eq!(progress.borrow().applied.tx_id, 5);
        assert_eq!(
            progress.borrow().applied.log_position,
            LogPosition::from_u64(5)
        );
        for (ack, wake) in acks.iter_mut().zip(&ack_wakes) {
            let waker = Waker::from(Arc::clone(wake));
            let mut context = Context::from_waker(&waker);
            assert!(matches!(
                Pin::new(ack).poll(&mut context),
                Poll::Ready(Ok(Ok(_)))
            ));
        }
        assert_eq!(log.appends.load(AtomicOrdering::SeqCst), 1);
    }

    #[tokio::test]
    async fn size_threshold_flushes_without_waiting_for_the_window() {
        let log = CountingLog::new();
        let (sender, _live) = spawn(
            Arc::clone(&log) as Arc<dyn Log>,
            WriterConfig {
                window: Duration::from_secs(3600),
                max_bytes: 1, // every record trips the threshold
                ..WriterConfig::default()
            },
        );
        for i in 0..2 {
            let ack = submit(&sender, &format!("INSERT (:P {{_id: {i}}})"));
            // A broken size trigger would park until the 1h window: time out fast.
            tokio::time::timeout(Duration::from_secs(5), ack)
                .await
                .expect("size-triggered flush")
                .unwrap()
                .unwrap();
        }
        assert_eq!(log.appends.load(AtomicOrdering::SeqCst), 2);
    }

    #[tokio::test]
    async fn a_reading_statement_flushes_the_staged_batch_first() {
        let log = CountingLog::new();
        let (sender, _live) = spawn(
            Arc::clone(&log) as Arc<dyn Log>,
            WriterConfig {
                window: Duration::from_secs(1),
                max_bytes: 8 * 1024 * 1024,
                ..WriterConfig::default()
            },
        );
        let insert = submit(&sender, "INSERT (:P {_id: 1})");
        let delete = submit(&sender, "MATCH (p:P) DELETE p");
        insert.await.unwrap().unwrap();
        delete.await.unwrap().unwrap();
        // Two appends: the DELETE forced the staged INSERT out first…
        assert_eq!(log.appends.load(AtomicOrdering::SeqCst), 2);
        // …and therefore SAW the insert: its record carries one delete event.
        let records = log.inner.tail(LogPosition::ZERO).await.unwrap();
        assert_eq!(records.len(), 2);
        assert!(
            !records[1].1.effects.is_empty(),
            "delete resolved against the flushed insert"
        );
    }

    #[tokio::test]
    async fn resolve_errors_are_acked_and_the_loop_survives() {
        let log = CountingLog::new();
        let (sender, _live) = spawn(
            Arc::clone(&log) as Arc<dyn Log>,
            WriterConfig {
                window: Duration::ZERO,
                max_bytes: 8 * 1024 * 1024,
                ..WriterConfig::default()
            },
        );
        // valid_from defaults to tx time (2026+) which lands AFTER VALID TO.
        let bad = submit(&sender, "INSERT (:P {_id: 1}) VALID TO DATE '2020-01-01'");
        assert!(matches!(
            bad.await.unwrap(),
            Err(EngineError::InvalidValidRange { .. })
        ));
        let good = submit(&sender, "INSERT (:P {_id: 2})");
        good.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn failed_append_acks_commit_failed_and_applies_nothing() {
        let log = Arc::new(FailOnceLog {
            inner: MemoryLog::new(),
            failed: AtomicBool::new(false),
        });
        let (sender, state) = spawn(
            Arc::clone(&log) as Arc<dyn Log>,
            WriterConfig {
                window: Duration::ZERO,
                max_bytes: 8 * 1024 * 1024,
                ..WriterConfig::default()
            },
        );
        let first = submit(&sender, "INSERT (:P {_id: 1})");
        assert!(matches!(
            first.await.unwrap(),
            Err(EngineError::CommitFailed(_))
        ));
        // Apply-after-durable: the failed batch never touched the live index.
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

        let second = submit(&sender, "INSERT (:P {_id: 2})");
        second.await.unwrap().unwrap();
        assert_eq!(
            state
                .read()
                .unwrap()
                .graph(DEFAULT_GRAPH)
                .unwrap()
                .nodes
                .live
                .event_count(),
            1
        );
        assert_eq!(log.inner.tail(LogPosition::ZERO).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn closing_the_channel_flushes_the_staged_batch() {
        let log = CountingLog::new();
        let (sender, _live) = spawn(
            Arc::clone(&log) as Arc<dyn Log>,
            WriterConfig {
                window: Duration::from_secs(3600),
                max_bytes: 8 * 1024 * 1024,
                ..WriterConfig::default()
            },
        );
        let ack = submit(&sender, "INSERT (:P {_id: 1})");
        drop(sender); // Db dropped mid-window
        tokio::time::timeout(Duration::from_secs(5), ack)
            .await
            .expect("close-triggered flush")
            .unwrap()
            .unwrap();
        assert_eq!(log.appends.load(AtomicOrdering::SeqCst), 1);
    }

    /// Task 13 TDD (writer-path spans): `resolve_program` and `flush` create
    /// `varve.resolve`, `varve.commit`, and `varve.apply` internally (see
    /// their doc comments) precisely so those spans stay observable when
    /// called directly here, bypassing `run_batch`/`spawn_writer` entirely —
    /// a spawned writer task's spans aren't reliably observable through a
    /// thread-local `set_default` subscriber (see `tests/tracing_spans.rs`).
    #[tokio::test]
    async fn resolve_flush_apply_emit_expected_spans() {
        use tracing_subscriber::layer::{Context as LayerContext, SubscriberExt};
        use tracing_subscriber::{registry, Layer};

        #[derive(Clone, Default)]
        struct SpanNames(Arc<Mutex<Vec<&'static str>>>);

        impl<S: tracing::Subscriber> Layer<S> for SpanNames {
            fn on_new_span(
                &self,
                attrs: &tracing::span::Attributes<'_>,
                _: &tracing::span::Id,
                _: LayerContext<'_, S>,
            ) {
                self.0.lock().unwrap().push(attrs.metadata().name());
            }
        }

        // `tracing` caches each span callsite's interest process-globally, keyed
        // by whichever thread hits it FIRST. The ~120 other writer-path unit
        // tests in this binary call `resolve_program`/`flush` under
        // `NoSubscriber`, so a thread-local `set_default` here loses the race:
        // if a no-subscriber thread registers `varve.resolve`/`varve.commit`/
        // `varve.apply` before we do, it sticks as `Interest::never()` and our
        // spans are compiled out (we'd intermittently see `[]` or a partial
        // set). A GLOBAL default removes the `NoSubscriber` case entirely — every
        // thread routes through our capturing subscriber, so those callsites
        // always register as interested regardless of ordering — and
        // `set_global_default` rebuilds the interest cache, repairing any
        // callsite a prior test already poisoned. Concurrent tests also push
        // their span names into `names`, but we only assert `.contains()`, so
        // the extra entries are harmless.
        let names = SpanNames::default();
        let subscriber = registry().with(names.clone());
        // Only this test installs a subscriber in the engine lib binary, so this
        // is the sole `set_global_default` caller and cannot lose the one-shot.
        tracing::subscriber::set_global_default(subscriber)
            .expect("no other test installs a global subscriber");

        let graphs = Arc::new(RwLock::new(GraphsState::new()));
        let (progress, _progress_rx) = watch::channel(ProgressState::running(
            0,
            LogPosition::ZERO,
            LogPosition::ZERO,
        ));
        let mut state = WriterState {
            state: Arc::clone(&graphs),
            store: varve_storage::memory_store(),
            clock: Arc::new(MonotonicClock::new()),
            functions: Arc::new(varve_plan::FunctionRegistry::with_builtins()),
            max_path_depth: 10,
            query_limits: varve_plan::QueryLimits::default(),
            log: Arc::new(MemoryLog::new()),
            next_tx_id: 0,
            next_block_id: 0,
            durable_watermark: LogPosition::ZERO,
            progress,
            lease: watch::channel(LeaseState::Unfenced).1,
            metrics: Arc::new(EngineMetrics::default()),
            security: SecurityEnforcer::new(crate::security::SecurityTuning::default()),
        };

        let program = varve_gql::parse_program("INSERT (:P {_id: 1})").unwrap();
        let (record, events, receipt, graph) = resolve_program(
            &mut state,
            program.statements,
            &BTreeMap::new(),
            DEFAULT_GRAPH,
            "test-user",
        )
        .await
        .unwrap();

        let (ack, _rx) = oneshot::channel();
        let staged = Staged {
            graph,
            record,
            events,
            receipt,
            ack,
        };
        match flush(&mut state, vec![staged]).await {
            FlushOutcome::Continue => {}
            FlushOutcome::Fatal(reason) => panic!("unexpected fatal flush outcome: {reason}"),
        }

        let seen = names.0.lock().unwrap().clone();
        for expected in ["varve.resolve", "varve.commit", "varve.apply"] {
            assert!(
                seen.contains(&expected),
                "missing span {expected}; saw {seen:?}"
            );
        }
    }
}
