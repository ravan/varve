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
use crate::db::{
    bounds_per_clause, scan_inputs_for, validate_user_graph_name, EngineError, Overlay,
    SideEffects, TxReceipt,
};
use crate::scan::{incident_edges, AdjDirection};
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
use tokio::sync::{mpsc, oneshot};
use varve_gql::ast::{
    Clause, Direction, Expr, GraphStmt, InsertStmt, LabelSpec, MatchPart, MutKind, MutateStmt,
    NodePattern, QueryBody, RemoveItem, RemoveStmt, ReturnClause, SetItem, SetStmt, SortItem,
    Statement, TemporalClauses,
};
use varve_index::{decode_events, encode_events, visible_events, Event, Op};
use varve_log::{Log, LogRecord, TableEffects};
use varve_storage::{keys, manifest_history, TrieCatalog, TrieEntry};
use varve_types::{Doc, Iid, Instant, LogPosition, TemporalBounds, TemporalDimension, Value};

/// Bounded submission queue (roadmap slice 3). Config-driven backpressure
/// semantics arrive in slice 10.
pub(crate) const SUBMISSION_QUEUE_LEN: usize = 256;

pub(crate) struct Submission {
    pub statements: Vec<Statement>,
    pub params: BTreeMap<String, Value>,
    pub graph: String,
    pub ack: oneshot::Sender<Result<TxReceipt, EngineError>>,
}

enum Command {
    Submit(Submission),
    Compact(oneshot::Sender<Result<CompactionReport, EngineError>>),
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
        let (ack, rx) = oneshot::channel();
        self.sender
            .send(Command::Compact(ack))
            .await
            .map_err(|_| EngineError::WriterUnavailable)?;
        rx.await.map_err(|_| EngineError::WriterUnavailable)?
    }

    #[cfg(test)]
    pub(crate) fn try_submit(&self, submission: Submission) {
        self.sender.try_send(Command::Submit(submission)).unwrap();
    }
}

/// Group-commit tuning (spec §6): a batch flushes when its window elapses OR
/// its encoded size reaches `max_bytes`, whichever comes first. Block-flush
/// tuning (spec §9, slice-4 plan): the live table flushes to a block once it
/// reaches `max_block_rows`, or once `flush_interval` elapses since the
/// first unflushed row landed — whichever comes first. `Duration::ZERO`
/// disables the timer (size-only flushing).
#[derive(Clone, Copy, Debug)]
pub(crate) struct WriterConfig {
    pub window: Duration,
    pub max_bytes: usize,
    pub max_block_rows: usize,
    pub flush_interval: Duration,
}

impl Default for WriterConfig {
    fn default() -> Self {
        WriterConfig {
            window: Duration::from_millis(15),
            max_bytes: 8 * 1024 * 1024,
            max_block_rows: 100_000,
            flush_interval: Duration::from_secs(300),
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
    let (sender, mut rx) = mpsc::channel::<Command>(SUBMISSION_QUEUE_LEN);
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
                    pending = run_batch(&mut state, &cfg, &mut rx, first).await;
                    if live_rows(&state) >= cfg.max_block_rows {
                        // A failed flush leaves the live table intact and
                        // retries at the next trigger (decision 10).
                        let _ = crate::flush::flush_block(&mut state).await;
                    }
                    flush_deadline = next_deadline(&state, &cfg, flush_deadline);
                }
                Received::Command(Some(Command::Compact(ack))) => {
                    let _ = ack.send(compact_once(&mut state).await);
                    flush_deadline = next_deadline(&state, &cfg, flush_deadline);
                }
                Received::Command(None) => {
                    // Sender dropped (Db closed) and channel drained:
                    // nothing left to do.
                    break;
                }
                Received::FlushTimeout => {
                    let _ = crate::flush::flush_block(&mut state).await;
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
async fn run_batch(
    state: &mut WriterState,
    cfg: &WriterConfig,
    rx: &mut mpsc::Receiver<Command>,
    first: Submission,
) -> Option<Command> {
    let deadline = tokio::time::Instant::now() + cfg.window;
    let mut staged: Vec<Staged> = Vec::new();
    let mut staged_bytes = 0usize;
    let mut pending = Some(first);
    loop {
        if let Some(sub) = pending.take() {
            // A reading statement must observe every earlier tx, and events
            // apply only after durability — so flush any staged batch first.
            if !staged.is_empty()
                && (program_reads(&sub.statements) || staged_touches_catalog(&staged))
            {
                flush(state, std::mem::take(&mut staged)).await;
                staged_bytes = 0;
            }
            match resolve_program(state, sub.statements, &sub.params, &sub.graph).await {
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
            Ok(Some(command @ Command::Compact(_))) => {
                if !staged.is_empty() {
                    flush(state, std::mem::take(&mut staged)).await;
                }
                return Some(command);
            }
            Ok(None) | Err(_) => break, // channel closed or window elapsed
        }
    }
    if !staged.is_empty() {
        flush(state, staged).await;
    }
    None
}

async fn compact_once(state: &mut WriterState) -> Result<CompactionReport, EngineError> {
    let history = manifest_history(state.store.as_ref()).await?;
    let Some(latest) = history.iter().max_by_key(|manifest| manifest.block_id) else {
        return Ok(CompactionReport::default());
    };
    let catalog = TrieCatalog::from_manifests(&history)?;
    let mut jobs = select_compaction_jobs(&catalog, &CompactionConfig::default())?;
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
    }
}

fn program_reads(statements: &[Statement]) -> bool {
    statements.iter().any(statement_reads)
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
async fn resolve_program(
    state: &mut WriterState,
    statements: Vec<Statement>,
    params: &BTreeMap<String, Value>,
    graph: &str,
) -> Result<(LogRecord, Effects, TxReceipt, String), EngineError> {
    if statements.is_empty() {
        return Err(EngineError::NotAMutation);
    }

    let has_catalog = statements
        .iter()
        .any(|stmt| matches!(stmt, Statement::Graph(_)));
    let has_non_catalog = statements
        .iter()
        .any(|stmt| !matches!(stmt, Statement::Graph(_)));
    if has_catalog && has_non_catalog {
        return Err(EngineError::Unsupported(
            "mixing catalog and data statements in one transaction".into(),
        ));
    }

    if has_non_catalog {
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

    state.next_tx_id += 1;
    let tx_id = state.next_tx_id;
    let system = state.clock.next();

    let mut overlay = Overlay::default();
    let mut effects = Effects::default();
    let mut generated_ordinal: usize = 0;
    let effect_graph = if has_catalog { META_GRAPH } else { graph };

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
                )
                .await?
            }
            Statement::Mutate(del) => {
                resolve_delete(state, graph, del, params, system, Some(&overlay)).await?
            }
            Statement::Set(set) => {
                resolve_set(state, graph, set, params, system, Some(&overlay)).await?
            }
            Statement::Remove(remove) => {
                resolve_remove(state, graph, remove, params, system, Some(&overlay)).await?
            }
            Statement::Graph(graph_stmt) => resolve_graph_stmt(state, graph_stmt, system)?,
            Statement::Query(_) => return Err(EngineError::NotAMutation),
        };
        append_effects_to_overlay(&mut overlay, &statement_effects)?;
        effects.merge(statement_effects);
    }

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
        user: String::new(),
        effects: table_effects,
    };
    let receipt = TxReceipt {
        tx_id,
        system_time: system,
        side_effects: effects.side_effects,
    };
    Ok((record, effects, receipt, effect_graph.to_string()))
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

async fn resolve_match_projection(
    state: &WriterState,
    graph: &str,
    match_part: &MatchPart,
    params: &BTreeMap<String, Value>,
    system: Instant,
    projected: Vec<(Expr, Option<String>)>,
    overlay: Option<&Overlay>,
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
        Some(mp) => resolve_match_part(state, graph, mp, params, system, overlay).await?,
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
                let id = match doc.get("_id") {
                    Some(v) => v.clone(),
                    None => {
                        let v = Value::Str(format!("varve:gen:{tx_id}:{generated_ordinal}"));
                        doc.insert("_id".into(), v.clone());
                        v
                    }
                };
                *generated_ordinal += 1;
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
    let id = match doc.get("_id") {
        Some(v) => v.clone(),
        None => {
            let v = Value::Str(format!("varve:gen:{tx_id}:{generated_ordinal}"));
            doc.insert("_id".into(), v.clone());
            v
        }
    };
    *generated_ordinal += 1;
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
) -> Result<Effects, EngineError> {
    let (binding_rows, kinds) =
        resolve_match_part(state, graph, &remove.match_part, params, system, overlay).await?;
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
) -> Result<Effects, EngineError> {
    let (binding_rows, kinds) =
        resolve_match_part(state, graph, &del.match_part, params, system, overlay).await?;
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

/// Durable append → apply → ack, strictly in that order (decision 1).
async fn flush(state: &mut WriterState, mut staged: Vec<Staged>) {
    let records: Vec<LogRecord> = staged.iter().map(|s| s.record.clone()).collect();
    let count = records.len() as u64;
    match state.log.append(records).await {
        Ok(first) => {
            // Exclusive end of the durable prefix — becomes the next
            // manifest's watermark (decision 6; positions are consecutive
            // per the `Log` contract). On (unreachable) 48-bit offset
            // overflow, keep the old, still-conservative watermark.
            if let Ok(end) = first.advance(count) {
                state.durable_watermark = end;
            }
            let applied = apply(state, &mut staged);
            for s in staged {
                let _ = s.ack.send(match &applied {
                    Ok(()) => Ok(s.receipt),
                    Err(msg) => Err(EngineError::CommitFailed(msg.clone())),
                });
            }
        }
        Err(e) => {
            // Nothing applied, so live-index state is untouched: fail the
            // whole batch and keep serving (the log itself, not this
            // in-memory index, is what may need operator attention).
            let msg = e.to_string();
            for s in staged {
                let _ = s.ack.send(Err(EngineError::CommitFailed(msg.clone())));
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

        let table = shared
            .graph_mut(&s.graph)
            .ok_or_else(|| format!("unknown graph '{}'", s.graph))?;
        for event in std::mem::take(&mut s.events.nodes) {
            table.nodes.live.append(event).map_err(|e| e.to_string())?;
        }
        for event in std::mem::take(&mut s.events.edges) {
            table.edges.live.append(event).map_err(|e| e.to_string())?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::MonotonicClock;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering};
    use varve_log::{LogError, MemoryLog};
    use varve_types::LogPosition;

    struct CountingLog {
        inner: MemoryLog,
        appends: AtomicUsize,
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
    }

    fn spawn(log: Arc<dyn Log>, cfg: WriterConfig) -> (WriterHandle, Arc<RwLock<GraphsState>>) {
        let state = Arc::new(RwLock::new(GraphsState::new()));
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
        };
        (spawn_writer(writer_state, cfg), state)
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
        sender.try_submit(Submission {
            statements: program.statements,
            params: BTreeMap::new(),
            graph,
            ack,
        });
        rx
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
}
