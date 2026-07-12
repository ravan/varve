use crate::clock::{Clock, MonotonicClock};
use crate::compact::CompactionReport;
use crate::const_eval::const_value;
use crate::coord::{spawn_heartbeat, Coordinator, HeartbeatHandle, LeaseState, WriterGrant};
use crate::follower::{spawn_follower, FollowerConfig, FollowerHandle, FollowerState};
use crate::gc::{execute_gc, GcConfig, GcReport};
use crate::metrics::{CacheTierStats, EngineMetrics, EngineMetricsSnapshot};
use crate::node::{
    AppliedProgress, BasisToken, NodeRole, NodeRoles, NodeStatus, NodeTuning, ProgressState,
};
use crate::registries::Registries;
use crate::replay::{apply_catalog_event, apply_decoded_log_record, decode_log_record};
use crate::scan::merged_snapshot;
use crate::state::{
    GraphsState, PersistedTrie, TableKind, TableState, DEFAULT_GRAPH, EDGES_TABLE, META_GRAPH,
    NODES_TABLE,
};
use crate::verify::{verify_database, VerifyReport};
use crate::writer::{spawn_writer, Submission, WriterConfig, WriterHandle, WriterState};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::TryStreamExt;
use std::collections::BTreeMap;
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use thiserror::Error;
use tokio::sync::{oneshot, watch};
use tracing::Instrument;
use varve_config::{BuildContext, ByteSize, Config, ConfigError, ConfigSection, RegistryError};
use varve_gql::ast::{Expr, LabelSpec, QueryBody, Statement};
use varve_gql::token::GqlError;
use varve_index::{decode_events, IndexError, LabelFilter, LiveTable};
use varve_log::{LocalLog, Log, LogError, MemoryLog, DEFAULT_SEGMENT_MAX_BYTES};
use varve_plan::PlanError;
use varve_storage::{
    memory_store, CacheStats, CachedStore, MemoryCache, ObjectStore, ProbeReport, StorageError,
};
use varve_types::{Instant, LogPosition, TemporalBounds, TypeError, Value};

#[derive(Debug, Error)]
pub enum EngineError {
    #[error(transparent)]
    Gql(#[from] GqlError),
    #[error(transparent)]
    Plan(#[from] PlanError),
    #[error(transparent)]
    Index(#[from] IndexError),
    #[error(transparent)]
    Type(#[from] TypeError),
    #[error(transparent)]
    Log(#[from] LogError),
    #[error("VALID FROM {from} must be earlier than VALID TO {to}")]
    InvalidValidRange { from: Instant, to: Instant },
    #[error("transaction failed to commit: {0}")]
    CommitFailed(String),
    #[error("writer is not running (database closed)")]
    WriterUnavailable,
    #[error("statement is a query; use query()")]
    NotAMutation,
    #[error("statement is a mutation; use execute()")]
    NotAQuery,
    #[error("internal lock poisoned")]
    Poisoned,
    #[error(transparent)]
    Registry(#[from] RegistryError),
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error("log record references unknown table '{0}'")]
    UnknownTable(String),
    #[error("unknown graph '{0}'")]
    UnknownGraph(String),
    #[error("graph already exists '{0}'")]
    GraphExists(String),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("writer advertisement JSON: {0}")]
    WriterAdvertisementJson(#[from] serde_json::Error),
    #[error(
        "[log] backend \"local\" with [storage] backend \"memory\" would lose \
         flushed blocks on restart while trimming the durable log; set \
         [storage] backend = \"local\" (with [storage.local] dir) or use a \
         memory log"
    )]
    VolatileBlockStore,
    #[error("unsupported in v1: {0}")]
    Unsupported(String),
    #[error("INSERT references unbound variable '{0}' (a bare `(x)` must be bound earlier in the same statement)")]
    UnboundVariable(String),
    #[error("INSERT re-binds already-bound variable '{0}' (a reference must be a bare `(x)` — no labels or properties)")]
    AlreadyBoundVariable(String),
    #[error(
        "cannot DELETE/ERASE node(s) with {0} still-connected edge(s); use DETACH DELETE/ERASE"
    )]
    StillConnected(usize),
    #[error("node role {0:?} is disabled")]
    RoleDisabled(NodeRole),
    #[error("query-node follower failed: {0}")]
    FollowerFailed(String),
    #[error("timed out waiting for basis {requested:?}; latest applied progress is {applied:?}")]
    BasisTimeout {
        requested: BasisToken,
        applied: AppliedProgress,
    },
    #[error("log gap: expected {expected:?}, found {actual:?}")]
    LogGap {
        expected: LogPosition,
        actual: LogPosition,
    },
    #[error("invalid node configuration: {0}")]
    InvalidNodeConfig(String),
    #[error("log epoch space (u16) is exhausted")]
    EpochExhausted,
    #[error("another writer is active at '{address}' (heartbeat {age_ms} ms old; stale after {takeover_after_ms} ms). Stop it, or wait for its heartbeat to go stale. This guard is best-effort (spec §12).")]
    WriterActive {
        address: String,
        age_ms: u64,
        takeover_after_ms: u64,
    },
    #[error("cas-failover requires the shared \"object-store\" log; [log] backend is \"{0}\"")]
    CasRequiresSharedLog(String),
    #[error("storage backend cannot support cas-failover: {reason}. Use [coordinator] backend = \"designated-writer\" (spec §12, D5).")]
    CasUnsupported { reason: String },
    #[error("writer fenced: {0}")]
    WriterFenced(String),
    #[error("writer submission queue is full; retry")]
    Backpressure,
}

fn label_filter(labels: &LabelSpec) -> LabelFilter<'_> {
    match labels {
        LabelSpec::All(labels) if labels.len() == 1 => LabelFilter::Single(labels[0].as_str()),
        LabelSpec::All(labels) => LabelFilter::All(labels),
        LabelSpec::Any(labels) => LabelFilter::Any(labels),
    }
}

pub(crate) fn validate_user_graph_name(graph: &str) -> Result<(), EngineError> {
    if graph.starts_with("__") {
        return Err(EngineError::Unsupported(format!(
            "graph names starting with '__' are reserved: {graph}"
        )));
    }
    Ok(())
}

/// Group-commit tuning read from `[log]` (spec §6): a batch flushes when its
/// window elapses OR its size reaches `group_commit_max_bytes`, whichever
/// comes first. Unknown keys in `[log]` (`backend`, the `local` subtable)
/// are ignored — this struct only pulls out the two tuning knobs it knows.
#[derive(serde::Deserialize)]
struct LogTuning {
    #[serde(default = "default_window_ms")]
    group_commit_window_ms: u64,
    #[serde(default = "default_group_commit_max_bytes")]
    group_commit_max_bytes: ByteSize,
}

/// Group-commit window (`[log] group_commit_window_ms`); shared with the
/// generated configuration reference (`varve-testkit/src/config_reference.rs`)
/// so the docs page cannot drift from this default.
pub const DEFAULT_GROUP_COMMIT_WINDOW_MS: u64 = 15;

fn default_window_ms() -> u64 {
    DEFAULT_GROUP_COMMIT_WINDOW_MS
}

fn default_group_commit_max_bytes() -> ByteSize {
    ByteSize::from_bytes(8 * 1024 * 1024)
}

/// Block-flush tuning read from `[storage]` (spec §9). Unknown keys
/// (`backend`, the `local` subtable) are ignored, as with `LogTuning`.
#[derive(serde::Deserialize)]
struct StorageTuning {
    #[serde(default = "default_max_block_rows")]
    max_block_rows: usize,
    /// Live-index memory watermark (Task 11): forces an early block flush
    /// once the live tables' approximate in-memory footprint
    /// (`Event::approx_bytes`, summed) reaches this, independent of
    /// `max_block_rows` — bounds writer memory when rows are large.
    #[serde(default = "default_max_live_bytes")]
    max_live_bytes: ByteSize,
    #[serde(default = "default_flush_interval_ms")]
    flush_interval_ms: u64,
}

/// Row count that triggers an early block flush (`[storage] max_block_rows`);
/// shared with the generated configuration reference.
pub const DEFAULT_MAX_BLOCK_ROWS: usize = 100_000;

fn default_max_block_rows() -> usize {
    DEFAULT_MAX_BLOCK_ROWS
}

fn default_max_live_bytes() -> ByteSize {
    ByteSize::from_bytes(512 * 1024 * 1024)
}

fn default_flush_interval_ms() -> u64 {
    300_000
}

/// Query planning tuning read from `[query]` (spec §10). Unknown keys are
/// ignored, as with `LogTuning`/`StorageTuning`.
#[derive(serde::Deserialize)]
struct QueryTuning {
    #[serde(default = "default_max_path_depth")]
    max_path_depth: u32,
    #[serde(default = "default_path_output_batch_rows")]
    path_output_batch_rows: usize,
    #[serde(default = "default_path_row_budget")]
    path_row_budget: usize,
    #[serde(default = "default_path_frontier_budget")]
    path_frontier_budget: usize,
    #[serde(default = "default_traversal_node_budget")]
    traversal_node_budget: usize,
    #[serde(default = "default_traversal_adjacency_budget")]
    traversal_adjacency_budget: usize,
}

fn default_max_path_depth() -> u32 {
    10
}

fn default_path_output_batch_rows() -> usize {
    8_192
}

fn default_path_row_budget() -> usize {
    100_000
}

fn default_path_frontier_budget() -> usize {
    100_000
}

fn default_traversal_node_budget() -> usize {
    100_000
}

fn default_traversal_adjacency_budget() -> usize {
    250_000
}

impl QueryTuning {
    fn limits(&self) -> varve_plan::QueryLimits {
        varve_plan::QueryLimits {
            path_output_batch_rows: self.path_output_batch_rows,
            path_expand: varve_plan::PathExpandLimits::new(
                self.path_row_budget,
                self.path_frontier_budget,
                self.max_path_depth,
            ),
            traversal_node_budget: self.traversal_node_budget,
            traversal_adjacency_budget: self.traversal_adjacency_budget,
        }
    }
}

impl Default for QueryTuning {
    fn default() -> Self {
        Self {
            max_path_depth: default_max_path_depth(),
            path_output_batch_rows: default_path_output_batch_rows(),
            path_row_budget: default_path_row_budget(),
            path_frontier_budget: default_path_frontier_budget(),
            traversal_node_budget: default_traversal_node_budget(),
            traversal_adjacency_budget: default_traversal_adjacency_budget(),
        }
    }
}

#[derive(serde::Deserialize)]
struct GcTuning {
    #[serde(default)]
    enabled: bool,
    #[serde(default = "default_gc_blocks_to_keep")]
    blocks_to_keep: u64,
    #[serde(default = "default_gc_garbage_lifetime_hours")]
    garbage_lifetime_hours: i64,
}

/// Flushed blocks retained behind the GC frontier (`[gc] blocks_to_keep`);
/// shared with the generated configuration reference.
pub const DEFAULT_GC_BLOCKS_TO_KEEP: u64 = 10;

fn default_gc_blocks_to_keep() -> u64 {
    DEFAULT_GC_BLOCKS_TO_KEEP
}

fn default_gc_garbage_lifetime_hours() -> i64 {
    24
}

impl GcTuning {
    fn into_config(self) -> GcConfig {
        GcConfig {
            enabled: self.enabled,
            blocks_to_keep: self.blocks_to_keep,
            garbage_lifetime_us: self
                .garbage_lifetime_hours
                .saturating_mul(60 * 60 * 1_000_000),
        }
    }
}

fn match_bounds(
    query_temporal: &varve_gql::ast::TemporalClauses,
    match_temporal: &varve_gql::ast::TemporalClauses,
    now: Instant,
) -> varve_types::TemporalBounds {
    varve_types::TemporalBounds {
        valid: match_temporal
            .valid
            .or(query_temporal.valid)
            .unwrap_or_else(|| varve_types::TemporalDimension::at(now)),
        system: match_temporal
            .system
            .or(query_temporal.system)
            .unwrap_or_else(|| varve_types::TemporalDimension::at(now)),
    }
}

pub(crate) fn bounds_per_clause(body: &QueryBody, now: Instant) -> Vec<TemporalBounds> {
    let mut bounds = Vec::new();
    let mut active_match_temporal = varve_gql::ast::TemporalClauses::default();
    for clause in &body.clauses {
        let varve_gql::ast::Clause::Match { temporal, .. } = clause else {
            if let varve_gql::ast::Clause::Filter(expr) = clause {
                let (exists, _) = varve_plan::split_conjuncts(Some(expr));
                if !exists.is_empty() {
                    bounds.push(match_bounds(&body.temporal, &active_match_temporal, now));
                }
            }
            continue;
        };
        active_match_temporal = *temporal;
        bounds.push(match_bounds(&body.temporal, temporal, now));
    }
    bounds
}

/// `[cache]` (spec §4/§9): named tiers composed OUTERMOST-FIRST —
/// `tiers = ["memory", "disk"]` checks memory, then disk, then the backend.
/// An empty list runs uncached. Per-tier tuning lives in `[cache.<name>]`.
#[derive(serde::Deserialize)]
struct CacheConfig {
    #[serde(default = "default_cache_tiers")]
    tiers: Vec<String>,
}

fn default_cache_tiers() -> Vec<String> {
    vec!["memory".to_string()]
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SideEffects {
    pub nodes_created: usize,
    pub nodes_deleted: usize,
    pub relationships_created: usize,
    pub relationships_deleted: usize,
    pub properties_set: usize,
    pub properties_removed: usize,
    pub labels_added: usize,
    pub labels_removed: usize,
}

impl SideEffects {
    pub fn is_empty(self) -> bool {
        self == SideEffects::default()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct TxReceipt {
    pub tx_id: u64,
    pub system_time: Instant,
    pub side_effects: SideEffects,
}

pub(crate) const WRITER_ADVERTISEMENT_KEY: &str = "v1/writer.json";

/// Published to `v1/writer.json` by the writer role (spec §12: query nodes
/// read it to redirect mutations; the `designated-writer`/`cas-failover`
/// coordinators read it for the startup guard and lease renewal). Field
/// declaration order is the canonical JSON key order.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize, Eq, PartialEq)]
pub struct WriterAdvertisement {
    pub address: String,
    /// The advertising node's identity (Task 1 `generate_node_id`); empty
    /// for advertisements published outside a coordinator (pre-slice-10
    /// callers, or `#[serde(default)]` on documents written before this
    /// field existed).
    #[serde(default)]
    pub node_id: String,
    /// The log epoch this writer is appending at.
    #[serde(default)]
    pub epoch: u16,
    /// Wall-clock microseconds at last publish — the coordinator startup
    /// guard's freshness clock (spec §12; best-effort, see
    /// `EngineError::WriterActive`).
    #[serde(default)]
    pub heartbeat_us: i64,
}

/// Free fn extracted from `Db::writer_advertisement`'s list-then-get
/// pattern: shared by `Db` and the coordinators, which both need to read the
/// current advertisement without going through a `Db` handle.
pub(crate) async fn read_writer_advertisement(
    store: &dyn ObjectStore,
) -> Result<Option<WriterAdvertisement>, EngineError> {
    let mut keys = store.list(WRITER_ADVERTISEMENT_KEY).await?;
    if !keys.iter().any(|key| key == WRITER_ADVERTISEMENT_KEY) {
        // object_store treats a list prefix as a directory and excludes
        // an object whose key equals that prefix. Keep the exact-prefix
        // check above for backends that include it, then inspect the
        // parent inventory without ever probing GET on an absent key.
        keys = store.list("v1").await?;
    }
    if !keys.iter().any(|key| key == WRITER_ADVERTISEMENT_KEY) {
        return Ok(None);
    }
    let bytes = store.get(WRITER_ADVERTISEMENT_KEY).await?;
    Ok(Some(serde_json::from_slice(&bytes)?))
}

/// Default in-memory cache budget for `Db::memory()`/`Db::local()`, which
/// take no config file and so never read `[cache]` (that wiring lives in
/// `open_with`).
const DEFAULT_CACHE_MEMORY_BYTES: usize = 512 * 1024 * 1024;

/// Embedded, in-process database handle. All mutations flow through the
/// writer loop (spec §3, D3): submissions are resolved serially, group-
/// committed to the log, applied to the live index after durability, then
/// acked — so concurrent `execute()` calls are fully supported, and an
/// acked transaction is both durable and visible.
///
/// Node internals are intentionally not part of the public API:
///
/// ```compile_fail
/// fn expose(_: &varve_engine::db::DbInner) {}
/// ```
#[derive(Clone)]
pub struct Db {
    inner: Arc<DbInner>,
}

pub struct Query {
    db: Db,
    gql: String,
    params: BTreeMap<String, Value>,
    basis: Option<BasisToken>,
    timeout: Duration,
}

impl Query {
    pub fn params(mut self, params: BTreeMap<String, Value>) -> Query {
        self.params = params;
        self
    }

    pub fn basis(mut self, basis: impl Into<BasisToken>) -> Query {
        self.basis = Some(basis.into());
        self
    }

    pub fn basis_timeout(mut self, timeout: Duration) -> Query {
        self.timeout = timeout;
        self
    }

    pub async fn stream(self) -> Result<SendableRecordBatchStream, EngineError> {
        self.db.require_role(NodeRole::Query)?;
        if let Some(basis) = self.basis {
            self.db.wait_for_basis(basis, self.timeout).await?;
        }
        self.db.query_stream_impl(&self.gql, &self.params).await
    }
}

impl std::future::IntoFuture for Query {
    type Output = Result<Vec<RecordBatch>, EngineError>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let stream = self.stream().await?;
            stream
                .try_collect::<Vec<_>>()
                .await
                .map_err(|error| EngineError::Plan(PlanError::DataFusion(error)))
        })
    }
}

fn basis_satisfied(basis: BasisToken, applied: AppliedProgress) -> bool {
    match basis {
        BasisToken::TxId(tx_id) => applied.tx_id >= tx_id,
        BasisToken::At(position) => applied.log_position >= position,
    }
}

enum BasisTimeoutChannel {
    Open,
    Pending,
    Closed,
}

struct BasisTimeoutObservation {
    current: ProgressState,
    channel: BasisTimeoutChannel,
}

fn basis_result_after_timeout_with(
    basis: BasisToken,
    mut observe: impl FnMut() -> BasisTimeoutObservation,
) -> Result<(), EngineError> {
    let first = observe();
    if let Some(result) = basis_result_for_observation(basis, &first.current) {
        return result;
    }
    if matches!(first.channel, BasisTimeoutChannel::Open) {
        return Err(EngineError::BasisTimeout {
            requested: basis,
            applied: first.current.applied,
        });
    }

    let second = observe();
    if let Some(result) = basis_result_for_observation(basis, &second.current) {
        return result;
    }
    if matches!(
        (&first.channel, &second.channel),
        (BasisTimeoutChannel::Closed, _) | (_, BasisTimeoutChannel::Closed)
    ) {
        return Err(EngineError::FollowerFailed(
            "progress channel closed".into(),
        ));
    }
    Err(EngineError::BasisTimeout {
        requested: basis,
        applied: second.current.applied,
    })
}

fn basis_result_for_observation(
    basis: BasisToken,
    current: &ProgressState,
) -> Option<Result<(), EngineError>> {
    if let Some(error) = &current.follower_error {
        return Some(Err(EngineError::FollowerFailed(error.clone())));
    }
    basis_satisfied(basis, current.applied).then_some(Ok(()))
}

fn basis_result_after_timeout(
    progress: &watch::Receiver<ProgressState>,
    basis: BasisToken,
) -> Result<(), EngineError> {
    let mut progress = progress.clone();
    basis_result_after_timeout_with(basis, || {
        let current = progress.borrow_and_update().clone();
        let channel = match progress.has_changed() {
            Ok(true) => BasisTimeoutChannel::Pending,
            Ok(false) => BasisTimeoutChannel::Open,
            Err(_) => BasisTimeoutChannel::Closed,
        };
        BasisTimeoutObservation { current, channel }
    })
}

struct DbInner {
    state: Arc<RwLock<GraphsState>>,
    #[allow(dead_code)] // ownership keeps the configured log alive for the node lifetime
    log: Arc<dyn Log>,
    store: Arc<dyn ObjectStore>,
    clock: Arc<dyn Clock>,
    writer: Option<WriterHandle>,
    functions: Arc<varve_plan::FunctionRegistry>,
    gc_config: GcConfig,
    roles: NodeRoles,
    progress: watch::Receiver<ProgressState>,
    basis_timeout: Duration,
    #[allow(dead_code)] // dropping the final owner signals follower shutdown
    follower: Option<FollowerHandle>,
    /// Cap on quantified-hop expansion length (spec §10, `[query]
    /// max_path_depth`); an unbounded `*`/`{m,}` quantifier is lowered to this
    /// depth. Task 9 consumes it during expansion; Task 8 already validates
    /// `scan_specs` quantifier bounds against it.
    max_path_depth: u32,
    query_limits: varve_plan::QueryLimits,
    /// The writer-role startup gate and advertisement/lease heartbeat (spec
    /// §12); `None` for `Db::memory()`/`Db::local()` and non-writer nodes.
    coordinator: Option<Arc<dyn Coordinator>>,
    /// This node's process-unique identity (Task 1 `generate_node_id`) —
    /// used to tag the plain-PUT writer advertisement when no coordinator is
    /// configured; the coordinator path tags its own copy internally.
    node_id: String,
    /// Abort-on-drop handle for the background heartbeat task; `None` when
    /// no coordinator is configured, the node isn't writer-role, or
    /// `coordinator.heartbeat_interval()` is `Duration::ZERO`.
    #[allow(dead_code)] // ownership keeps the heartbeat task alive for the node lifetime
    heartbeat: Option<HeartbeatHandle>,
    /// Writer-owned counters (Task 12, spec §12), shared with `WriterState`;
    /// reading them is I/O-free.
    metrics: Arc<EngineMetrics>,
    /// One `(tier name, stats)` pair per configured cache tier, in the same
    /// order as `[cache] tiers` (Task 12); read I/O-free for `Db::metrics`.
    cache_tiers: CacheTierList,
}

/// One `(tier name, stats)` pair per configured cache tier (Task 12); see
/// `DbInner::cache_tiers` and `EngineMetricsSnapshot::cache_tiers`.
type CacheTierList = Vec<(String, Arc<CacheStats>)>;

/// `Db::memory`/`Db::local`'s single default cache tier, named `"memory"`
/// for the `EngineMetricsSnapshot::cache_tiers` label.
fn cached(store: Arc<dyn ObjectStore>) -> (Arc<dyn ObjectStore>, CacheTierList) {
    let stats = Arc::new(CacheStats::default());
    let wrapped = Arc::new(CachedStore::with_stats(
        store,
        Arc::new(MemoryCache::new(DEFAULT_CACHE_MEMORY_BYTES)),
        Arc::clone(&stats),
    ));
    (wrapped, vec![("memory".to_string(), stats)])
}

impl std::fmt::Debug for Db {
    /// Opaque handle: internals (live index, clock, writer channel) carry no
    /// useful debug representation, so this only identifies the type — e.g.
    /// for `Result<Db, _>::unwrap_err()` in tests.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Db").finish_non_exhaustive()
    }
}

/// Task 12 anchor-reachable fast-path artifact, built once by
/// [`Db::plan_fast_path`] when the query's shape is provably coverable. It is a
/// pruned-but-complete drop-in for the full-scan input, never a replacement:
/// when `plan_fast_path` returns `None`, `query` uses the full scan verbatim.
pub(crate) enum FastPath {
    /// Fixed homogeneous path (all hops `Edge`, one label + direction, no
    /// inline edge props, no edge var referenced): the shared reachable-edge
    /// batch (or `None` when nothing is reachable) fed to EVERY `Edge` spec.
    FixedEdges(Option<RecordBatch>),
    /// Single quantified hop: the reachable adjacency fed to the `Expand` spec.
    QuantifiedAdjacency(Arc<varve_plan::EdgeAdjacency>),
}

fn const_props(
    props: &[(String, Expr)],
    params: &BTreeMap<String, Value>,
) -> Result<Vec<(String, Value)>, EngineError> {
    props
        .iter()
        .map(|(k, v)| const_value(v, params).map(|value| (k.clone(), value)))
        .collect()
}

#[derive(Default)]
pub(crate) struct Overlay {
    pub nodes: LiveTable,
    pub edges: LiveTable,
}

impl Overlay {
    pub(crate) fn table(&self, kind: TableKind) -> &LiveTable {
        match kind {
            TableKind::Nodes => &self.nodes,
            TableKind::Edges => &self.edges,
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn scan_inputs_for(
    state: &Arc<RwLock<GraphsState>>,
    store: &Arc<dyn ObjectStore>,
    graph: &str,
    clause_specs: &[varve_plan::ClauseSpecs],
    bounds_per_clause: &[TemporalBounds],
    params: &BTreeMap<String, Value>,
    query_limits: varve_plan::QueryLimits,
    fast_paths: Option<&[Option<FastPath>]>,
    overlay: Option<&Overlay>,
) -> Result<Vec<Vec<varve_plan::ScanInput>>, EngineError> {
    scan_inputs_for_impl(
        state,
        store,
        graph,
        clause_specs,
        bounds_per_clause,
        params,
        query_limits,
        fast_paths,
        overlay,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn scan_inputs_for_impl(
    state: &Arc<RwLock<GraphsState>>,
    store: &Arc<dyn ObjectStore>,
    graph: &str,
    clause_specs: &[varve_plan::ClauseSpecs],
    bounds_per_clause: &[TemporalBounds],
    params: &BTreeMap<String, Value>,
    query_limits: varve_plan::QueryLimits,
    fast_paths: Option<&[Option<FastPath>]>,
    overlay: Option<&Overlay>,
) -> Result<Vec<Vec<varve_plan::ScanInput>>, EngineError> {
    if clause_specs.len() != bounds_per_clause.len() {
        return Err(EngineError::Unsupported(
            "internal: clause specs/bounds length mismatch".into(),
        ));
    }
    if fast_paths.is_some_and(|paths| paths.len() != clause_specs.len()) {
        return Err(EngineError::Unsupported(
            "internal: clause specs/fast paths length mismatch".into(),
        ));
    }

    let mut inputs = Vec::with_capacity(clause_specs.len());
    for (idx, (clause_spec, bounds)) in clause_specs.iter().zip(bounds_per_clause).enumerate() {
        let fast_path = fast_paths
            .and_then(|paths| paths.get(idx))
            .and_then(Option::as_ref);
        inputs.push(
            scan_inputs_for_clause(
                state,
                store,
                graph,
                clause_spec,
                params,
                bounds,
                query_limits,
                fast_path,
                overlay,
            )
            .await?,
        );
    }
    Ok(inputs)
}

// Keeps graph, bounds, fast-path, and overlay explicit across recursive EXISTS scans.
#[allow(clippy::too_many_arguments)]
async fn scan_inputs_for_clause(
    state: &Arc<RwLock<GraphsState>>,
    store: &Arc<dyn ObjectStore>,
    graph: &str,
    clause_spec: &varve_plan::ClauseSpecs,
    params: &BTreeMap<String, Value>,
    bounds: &TemporalBounds,
    query_limits: varve_plan::QueryLimits,
    fast_path: Option<&FastPath>,
    overlay: Option<&Overlay>,
) -> Result<Vec<varve_plan::ScanInput>, EngineError> {
    let mut clause_inputs = Vec::with_capacity(
        clause_spec.specs.len()
            + clause_spec
                .exists
                .iter()
                .map(|exists| exists.specs.len())
                .sum::<usize>(),
    );
    for spec in &clause_spec.specs {
        clause_inputs.push(
            scan_input_for(
                state,
                store,
                graph,
                spec,
                params,
                bounds,
                query_limits,
                fast_path,
                overlay,
            )
            .await?,
        );
    }
    for exists in &clause_spec.exists {
        for spec in &exists.specs {
            clause_inputs.push(
                scan_input_for(
                    state,
                    store,
                    graph,
                    spec,
                    params,
                    bounds,
                    query_limits,
                    None,
                    overlay,
                )
                .await?,
            );
        }
    }
    Ok(clause_inputs)
}

// One conversion point from planner scan specs to executable scan inputs.
#[allow(clippy::too_many_arguments)]
async fn scan_input_for(
    state: &Arc<RwLock<GraphsState>>,
    store: &Arc<dyn ObjectStore>,
    graph: &str,
    spec: &varve_plan::ScanSpec,
    params: &BTreeMap<String, Value>,
    bounds: &TemporalBounds,
    query_limits: varve_plan::QueryLimits,
    fast_path: Option<&FastPath>,
    overlay: Option<&Overlay>,
) -> Result<varve_plan::ScanInput, EngineError> {
    Ok(match &spec.kind {
        varve_plan::SpecKind::Node { labels, iid_point } => varve_plan::ScanInput::Batch(
            merged_snapshot(
                state,
                store,
                graph,
                TableKind::Nodes,
                label_filter(labels),
                bounds,
                *iid_point,
                overlay,
            )
            .await?,
        ),
        varve_plan::SpecKind::Edge { label, .. } => match fast_path {
            Some(FastPath::FixedEdges(batch)) => varve_plan::ScanInput::Batch(batch.clone()),
            _ => varve_plan::ScanInput::Batch(
                merged_snapshot(
                    state,
                    store,
                    graph,
                    TableKind::Edges,
                    LabelFilter::Single(label),
                    bounds,
                    None,
                    overlay,
                )
                .await?,
            ),
        },
        varve_plan::SpecKind::Expand {
            label,
            direction,
            props,
            ..
        } => match fast_path {
            Some(FastPath::QuantifiedAdjacency(adjacency)) => {
                varve_plan::ScanInput::Adjacency(Arc::clone(adjacency))
            }
            _ => {
                let adj_direction = match direction {
                    varve_gql::ast::Direction::Out => crate::scan::AdjDirection::Out,
                    varve_gql::ast::Direction::In => crate::scan::AdjDirection::In,
                };
                let entries = crate::scan::edge_adjacency(
                    state,
                    store,
                    graph,
                    label,
                    &const_props(props, params)?,
                    adj_direction,
                    None,
                    bounds,
                    Some(query_limits.traversal_adjacency_budget),
                    overlay,
                )
                .await?;
                let adjacency =
                    varve_plan::EdgeAdjacency::from_entries(entries.into_iter().map(|e| {
                        (
                            e.node,
                            varve_plan::AdjEdge {
                                neighbor: e.neighbor,
                                edge: e.edge,
                            },
                        )
                    }));
                varve_plan::ScanInput::Adjacency(Arc::new(adjacency))
            }
        },
    })
}

impl Db {
    /// Volatile database: memory log, zero group-commit window (there is no
    /// fsync to amortize — decision 11). Requires a Tokio runtime.
    pub fn memory() -> Db {
        let (store, cache_tiers) = cached(memory_store());
        Db::assemble(
            GraphsState::new(),
            Arc::new(MemoryLog::new()),
            store,
            cache_tiers,
            Arc::new(MonotonicClock::new()),
            WriterConfig {
                window: Duration::ZERO,
                ..WriterConfig::default()
            },
            0,
            0,
            LogPosition::ZERO,
            default_max_path_depth(),
            QueryTuning::default().limits(),
            GcConfig::default(),
            NodeRoles::all(),
            FollowerConfig {
                poll_interval: Duration::from_millis(50),
                batch_records: 1024,
            },
            Duration::from_millis(5000),
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn assemble(
        graphs_state: GraphsState,
        log: Arc<dyn varve_log::Log>,
        store: Arc<dyn ObjectStore>,
        cache_tiers: CacheTierList,
        clock: Arc<dyn Clock>,
        cfg: WriterConfig,
        next_tx_id: u64,
        next_block_id: u64,
        durable_watermark: LogPosition,
        max_path_depth: u32,
        query_limits: varve_plan::QueryLimits,
        gc_config: GcConfig,
        roles: NodeRoles,
        follower_config: FollowerConfig,
        basis_timeout: Duration,
        coordinator: Option<Arc<dyn Coordinator>>,
    ) -> Db {
        let state = Arc::new(RwLock::new(graphs_state));
        let functions = Arc::new(varve_plan::FunctionRegistry::with_builtins());
        let metrics = Arc::new(EngineMetrics::default());
        let (progress_tx, progress_rx) = watch::channel(ProgressState::running(
            next_tx_id,
            durable_watermark,
            durable_watermark,
        ));
        let (lease_tx, lease_rx) = watch::channel(LeaseState::Unfenced);
        let writer_state = WriterState {
            state: Arc::clone(&state),
            store: Arc::clone(&store),
            clock: Arc::clone(&clock),
            functions: Arc::clone(&functions),
            max_path_depth,
            query_limits,
            log: Arc::clone(&log),
            next_tx_id,
            next_block_id,
            durable_watermark,
            progress: progress_tx.clone(),
            lease: lease_rx,
            metrics: Arc::clone(&metrics),
        };
        let is_writer = roles.contains(NodeRole::Writer);
        // Heartbeat only for a writer-role node with a coordinator whose
        // heartbeat_interval() is non-zero — Task 6's lifecycle wiring; the
        // handle lives on DbInner so dropping the Db aborts the task.
        let heartbeat = match (&coordinator, is_writer) {
            (Some(c), true) if c.heartbeat_interval() > Duration::ZERO => {
                Some(spawn_heartbeat(Arc::clone(c), lease_tx))
            }
            _ => None,
        };
        let writer = is_writer.then(|| spawn_writer(writer_state, cfg));
        let follower = if is_writer {
            None
        } else {
            Some(spawn_follower(FollowerState {
                state: Arc::clone(&state),
                log: Arc::clone(&log),
                store: Arc::clone(&store),
                cursor: durable_watermark,
                config: follower_config,
                progress: progress_tx,
                fences: crate::coord::fence::FenceMap::default(),
            }))
        };
        Db {
            inner: Arc::new(DbInner {
                state,
                log,
                store,
                clock,
                writer,
                functions,
                gc_config,
                roles,
                progress: progress_rx,
                basis_timeout,
                follower,
                max_path_depth,
                query_limits,
                coordinator,
                node_id: crate::coord::identity::generate_node_id(),
                heartbeat,
                metrics,
                cache_tiers,
            }),
        }
    }

    /// Opens a database from configuration using the built-in backends
    /// (spec §11: `Db::open(Config::from_file("varve.toml")?)`).
    pub async fn open(config: Config) -> Result<Db, EngineError> {
        Self::open_with(&config, &Registries::with_builtins()).await
    }

    /// Like [`Db::open`], but with caller-supplied registries — the spec §4
    /// extension point: register custom `Log`/`Clock` factories, then open.
    pub async fn open_with(config: &Config, registries: &Registries) -> Result<Db, EngineError> {
        // Storage FIRST: later factories may consume the raw store through
        // the BuildContext (spec §4 ctx) — the object-store log shares the
        // block store's bucket and keyspace (spec §9).
        let storage_section = config
            .section("storage")
            .unwrap_or_else(ConfigSection::empty);
        let storage_backend = storage_section.backend().unwrap_or("memory").to_string();
        let raw_store =
            registries
                .storage
                .build(&storage_backend, &storage_section, &BuildContext::empty())?;

        // The RAW store goes into the context: log traffic must not flow
        // through (or fill) the query-path cache wired below.
        let mut ctx = BuildContext::empty();
        ctx.insert(Arc::clone(&raw_store));

        let log_section = config.section("log").unwrap_or_else(ConfigSection::empty);
        let log_backend = log_section.backend().unwrap_or("memory").to_string();
        // Decision 11 (slice 4): a DURABLE log over a volatile block store
        // would trim durable data while blocks evaporate on restart.
        if log_backend == "local" && storage_backend == "memory" {
            return Err(EngineError::VolatileBlockStore);
        }
        let log = registries.log.build(&log_backend, &log_section, &ctx)?;

        // [cache] tiers, folded outermost-first over raw_store (Task 6).
        let cache_section = config.section("cache").unwrap_or_else(ConfigSection::empty);
        let cache_config: CacheConfig = cache_section.get()?;
        let mut store: Arc<dyn ObjectStore> = Arc::clone(&raw_store);
        // Innermost tier wraps first, so the FIRST listed tier is the first
        // one checked on a read.
        let mut cache_tiers: CacheTierList = Vec::new();
        for name in cache_config.tiers.iter().rev() {
            let tier = registries.cache.build(name, &cache_section, &ctx)?;
            let stats = Arc::new(CacheStats::default());
            store = Arc::new(CachedStore::with_stats(store, tier, Arc::clone(&stats)));
            cache_tiers.push((name.clone(), stats));
        }
        // Restore `[cache] tiers` declaration order (the loop above walked
        // it in reverse to wrap innermost-first).
        cache_tiers.reverse();

        let clock_section = config.section("clock").unwrap_or_else(ConfigSection::empty);
        let clock = registries.clock.build(
            clock_section.backend().unwrap_or("system"),
            &clock_section,
            &ctx,
        )?;
        // The coordinator factories (below) read the clock through the
        // context, same as they read the raw store.
        ctx.insert::<Arc<dyn Clock>>(Arc::clone(&clock));

        let log_tuning: LogTuning = log_section.get()?;
        let storage_tuning: StorageTuning = storage_section.get()?;
        let query_section = config.section("query").unwrap_or_else(ConfigSection::empty);
        let query_tuning: QueryTuning = query_section.get()?;
        let gc_section = config.section("gc").unwrap_or_else(ConfigSection::empty);
        let gc_config = gc_section.get::<GcTuning>()?.into_config();
        let node_section = config.section("node").unwrap_or_else(ConfigSection::empty);
        let node_tuning: NodeTuning = node_section.get()?;
        let (roles, node_tuning) = node_tuning
            .validate()
            .map_err(EngineError::InvalidNodeConfig)?;
        let cfg = WriterConfig {
            window: Duration::from_millis(log_tuning.group_commit_window_ms),
            max_bytes: log_tuning.group_commit_max_bytes.as_usize(),
            max_block_rows: storage_tuning.max_block_rows,
            max_live_bytes: storage_tuning.max_live_bytes.as_usize(),
            flush_interval: Duration::from_millis(storage_tuning.flush_interval_ms),
            queue_len: node_tuning.submission_queue_len,
        };

        // Coordinator (spec §12): the writer-role startup gate. `acquire`
        // runs BEFORE recovery — a cas-failover standby (Task 7) blocks
        // here, so recovery only ever runs once this node holds the write
        // lease (or designated-writer's best-effort guard has cleared).
        let coord_section = config
            .section("coordinator")
            .unwrap_or_else(ConfigSection::empty);
        let coord_backend = coord_section
            .backend()
            .unwrap_or("designated-writer")
            .to_string();
        if coord_backend == "cas-failover" && log_backend != "object-store" {
            return Err(EngineError::CasRequiresSharedLog(log_backend));
        }
        let coordinator = if roles.contains(NodeRole::Writer) {
            Some(
                registries
                    .coordinator
                    .build(&coord_backend, &coord_section, &ctx)?,
            )
        } else {
            None
        };
        let grant = match &coordinator {
            Some(c) => c.acquire(&log).await?, // may BLOCK (cas standby)
            None => WriterGrant { epoch: None },
        };
        let recovered = recover(log.as_ref(), clock.as_ref(), &store).await?; // fence-aware since Task 3
        if let Some(epoch) = grant.epoch {
            log.start_epoch(epoch).await?;
        }
        Ok(Self::assemble(
            recovered.state,
            log,
            store,
            cache_tiers,
            clock,
            cfg,
            recovered.next_tx_id,
            recovered.next_block_id,
            recovered.watermark,
            query_tuning.max_path_depth,
            query_tuning.limits(),
            gc_config,
            roles,
            FollowerConfig {
                poll_interval: Duration::from_millis(node_tuning.tail_poll_interval_ms),
                batch_records: node_tuning.tail_batch_records,
            },
            Duration::from_millis(node_tuning.basis_timeout_ms),
            coordinator,
        ))
    }

    /// Local-filesystem database at `dir` with default tuning (spec §11
    /// `Db::local(path)` convenience — no config file needed). Log and store
    /// are both durable, under `dir/log` and `dir/store` respectively
    /// (decision 11; dev convenience — no migration of slice-3 layouts).
    pub async fn local(dir: impl AsRef<Path>) -> Result<Db, EngineError> {
        let dir = dir.as_ref();
        let log: Arc<dyn Log> =
            Arc::new(LocalLog::open(&dir.join("log"), DEFAULT_SEGMENT_MAX_BYTES)?);
        let (store, cache_tiers) = cached(varve_storage::local_store(&dir.join("store"))?);
        let clock: Arc<dyn Clock> = Arc::new(MonotonicClock::new());
        let recovered = recover(log.as_ref(), clock.as_ref(), &store).await?;
        Ok(Self::assemble(
            recovered.state,
            log,
            store,
            cache_tiers,
            clock,
            WriterConfig::default(),
            recovered.next_tx_id,
            recovered.next_block_id,
            recovered.watermark,
            default_max_path_depth(),
            QueryTuning::default().limits(),
            GcConfig::default(),
            NodeRoles::all(),
            FollowerConfig {
                poll_interval: Duration::from_millis(50),
                batch_records: 1024,
            },
            Duration::from_millis(5000),
            None,
        ))
    }

    /// Publishes this writer's advertised address so query nodes can answer a
    /// misdirected mutation (HTTP 421) with the writer's location. With a
    /// coordinator configured it delegates to the coordinator's `advertise`;
    /// otherwise it writes a canonical-JSON `WriterAdvertisement` to
    /// `v1/writer.json` with a plain PUT. This is advertisement only — NOT a
    /// lock, lease, or leader election; exactly-one-writer remains a
    /// deployment guarantee.
    pub async fn publish_writer(&self, address: &str) -> Result<(), EngineError> {
        self.require_role(NodeRole::Writer)?;
        match &self.inner.coordinator {
            Some(coordinator) => coordinator.advertise(address).await,
            None => {
                let bytes = serde_json::to_vec(&WriterAdvertisement {
                    address: address.to_string(),
                    node_id: self.inner.node_id.clone(),
                    epoch: 0,
                    heartbeat_us: self.inner.clock.watermark().as_micros(),
                })?;
                self.inner
                    .store
                    .put(WRITER_ADVERTISEMENT_KEY, bytes::Bytes::from(bytes))
                    .await?;
                Ok(())
            }
        }
    }

    pub async fn writer_advertisement(&self) -> Result<Option<WriterAdvertisement>, EngineError> {
        read_writer_advertisement(self.inner.store.as_ref()).await
    }

    pub async fn verify(&self) -> Result<VerifyReport, EngineError> {
        if !self.inner.roles.contains(NodeRole::Writer)
            && !self.inner.roles.contains(NodeRole::Query)
        {
            return Err(EngineError::RoleDisabled(NodeRole::Query));
        }
        verify_database(self.inner.store.as_ref(), self.inner.log.as_ref()).await
    }

    pub async fn compact_once(&self) -> Result<CompactionReport, EngineError> {
        self.require_role(NodeRole::Compactor)?;
        self.writer_handle()?.compact_once().await
    }

    pub async fn gc_once(&self) -> Result<GcReport, EngineError> {
        self.require_role(NodeRole::Compactor)?;
        Ok(execute_gc(&self.inner.store, &self.inner.gc_config).await?)
    }

    /// Executes a mutation statement (INSERT, MATCH … DELETE): parses here,
    /// resolves and commits inside the writer loop, and returns once the tx
    /// is durable AND visible.
    pub async fn execute(&self, gql: &str) -> Result<TxReceipt, EngineError> {
        let params = BTreeMap::new();
        self.execute_with(gql, &params).await
    }

    pub async fn execute_with(
        &self,
        gql: &str,
        params: &BTreeMap<String, Value>,
    ) -> Result<TxReceipt, EngineError> {
        self.execute_as(gql, params, "").await
    }

    pub async fn execute_as(
        &self,
        gql: &str,
        params: &BTreeMap<String, Value>,
        user: &str,
    ) -> Result<TxReceipt, EngineError> {
        let (graph, statements) = self.parse_mutation_program(gql)?;
        let (ack, rx) = oneshot::channel();
        // `varve.submit` (Task 13): the submit+ack future, instrumented
        // rather than `entered()` since it awaits twice (the writer-queue
        // send and the ack channel).
        async {
            self.writer_handle()?
                .submit(Submission {
                    statements,
                    params: params.clone(),
                    graph,
                    user: user.to_string(),
                    ack,
                })
                .await?;
            rx.await.map_err(|_| EngineError::WriterUnavailable)?
        }
        .instrument(tracing::info_span!("varve.submit", user = %user))
        .await
    }

    /// Like [`Self::execute_as`], but returns `EngineError::Backpressure`
    /// instead of waiting when the writer's submission queue is full — the
    /// server's `/v1/tx` 429 path (slice 10).
    pub async fn try_execute_as(
        &self,
        gql: &str,
        params: &BTreeMap<String, Value>,
        user: &str,
    ) -> Result<TxReceipt, EngineError> {
        let (graph, statements) = self.parse_mutation_program(gql)?;
        let (ack, rx) = oneshot::channel();
        if let Err(error) = self.writer_handle()?.try_submit(Submission {
            statements,
            params: params.clone(),
            graph,
            user: user.to_string(),
            ack,
        }) {
            if matches!(error, EngineError::Backpressure) {
                self.inner
                    .metrics
                    .backpressure_rejections
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            return Err(error);
        }
        rx.await.map_err(|_| EngineError::WriterUnavailable)?
    }

    /// Shared preamble for `execute_as`/`try_execute_as`: requires the
    /// Writer role, parses `gql`, resolves the target graph, and rejects a
    /// query (or empty program) before it ever reaches the writer queue.
    fn parse_mutation_program(&self, gql: &str) -> Result<(String, Vec<Statement>), EngineError> {
        self.require_role(NodeRole::Writer)?;
        let program = varve_gql::parse_program(gql)?;
        let graph = program
            .use_graph
            .unwrap_or_else(|| DEFAULT_GRAPH.to_string());
        validate_user_graph_name(&graph)?;
        if program.statements.is_empty()
            || program
                .statements
                .iter()
                .any(|stmt| matches!(stmt, Statement::Query(_)))
        {
            return Err(EngineError::NotAMutation);
        }
        Ok((graph, program.statements))
    }

    /// Borrows the writer handle, or `RoleDisabled(Writer)` if this node has
    /// no Writer role.
    fn writer_handle(&self) -> Result<&WriterHandle, EngineError> {
        self.inner
            .writer
            .as_ref()
            .ok_or(EngineError::RoleDisabled(NodeRole::Writer))
    }

    /// Builds an owned read query. Await it to collect Arrow batches, or call
    /// [`Query::stream`] to consume batches lazily.
    pub fn query(&self, gql: impl Into<String>) -> Query {
        Query {
            db: self.clone(),
            gql: gql.into(),
            params: BTreeMap::new(),
            basis: None,
            timeout: self.inner.basis_timeout,
        }
    }

    pub async fn wait_for_basis(
        &self,
        basis: BasisToken,
        timeout: Duration,
    ) -> Result<(), EngineError> {
        let mut progress = self.inner.progress.clone();
        let wait = async {
            loop {
                let current = progress.borrow().clone();
                if let Some(error) = current.follower_error {
                    return Err(EngineError::FollowerFailed(error));
                }
                if basis_satisfied(basis, current.applied) {
                    return Ok(());
                }
                if progress.changed().await.is_err() {
                    return Err(EngineError::FollowerFailed(
                        "progress channel closed".into(),
                    ));
                }
            }
        };
        match tokio::time::timeout(timeout, wait).await {
            Ok(result) => result,
            Err(_) => basis_result_after_timeout(&progress, basis),
        }
    }

    async fn query_stream_impl(
        &self,
        gql: &str,
        params: &BTreeMap<String, Value>,
    ) -> Result<SendableRecordBatchStream, EngineError> {
        // `varve.query.parse` (Task 13): fully synchronous — parsing,
        // graph-name validation/existence and the query-shape checks never
        // await, so this is a plain `entered()` guard, dropped well before
        // any `.await` below. Fields are deliberately omitted: the only
        // candidate (the GQL text itself) is exactly what "keep fields
        // cheap" rules out.
        let (graph, q) = {
            let _g = tracing::info_span!("varve.query.parse").entered();
            let program = varve_gql::parse_program(gql)?;
            let graph = program
                .use_graph
                .unwrap_or_else(|| DEFAULT_GRAPH.to_string());
            validate_user_graph_name(&graph)?;
            if !self
                .inner
                .state
                .read()
                .map_err(|_| EngineError::Poisoned)?
                .graphs
                .contains_key(&graph)
            {
                return Err(EngineError::UnknownGraph(graph));
            }
            if program.statements.len() != 1 {
                return Err(EngineError::NotAQuery);
            }
            let mut statements = program.statements;
            let Statement::Query(q) = statements.remove(0) else {
                return Err(EngineError::NotAQuery);
            };
            (graph, q)
        };
        let now = self.inner.clock.watermark();
        if !q.unions.is_empty() {
            // Union queries: mirror the single-body path below — `plan`
            // covers scan-spec/scan-input construction for every arm (first
            // + each union arm), and `execute` covers the actual per-arm
            // evaluation plus the union merge. That keeps the two spans
            // meaning the same thing here as they do for a non-union query:
            // `varve.query.execute` is the execution-dominant span, not
            // `varve.query.plan`.
            let (first_plan, union_plans) = async {
                let first_plan = self.plan_body_inputs(&graph, &q.first, params, now).await?;
                let mut union_plans = Vec::with_capacity(q.unions.len());
                for (_, body) in &q.unions {
                    union_plans.push(self.plan_body_inputs(&graph, body, params, now).await?);
                }
                Ok::<_, EngineError>((first_plan, union_plans))
            }
            .instrument(tracing::info_span!("varve.query.plan"))
            .await?;
            return Ok(async {
                let (first_specs, first_inputs) = first_plan;
                let first = varve_plan::execute_body_with_limits(
                    &q.first,
                    &first_specs,
                    first_inputs,
                    self.inner.functions.as_ref(),
                    self.inner.query_limits.path_expand,
                    params,
                )
                .await?;
                let mut unions = Vec::with_capacity(union_plans.len());
                for ((kind, body), (specs, inputs)) in q.unions.iter().zip(union_plans) {
                    let batches = varve_plan::execute_body_with_limits(
                        body,
                        &specs,
                        inputs,
                        self.inner.functions.as_ref(),
                        self.inner.query_limits.path_expand,
                        params,
                    )
                    .await?;
                    unions.push((kind.clone(), batches));
                }
                varve_plan::union_query_results_stream(first, unions, self.inner.functions.as_ref())
                    .await
            }
            .instrument(tracing::info_span!("varve.query.execute", graph = %graph))
            .await?);
        }
        let (clause_specs, inputs) = async {
            let clause_specs = varve_plan::scan_specs_with_params(
                &q.first,
                &graph,
                self.inner.max_path_depth,
                params,
            )?;
            let bounds = bounds_per_clause(&q.first, now);
            let fast_path = if q.first.clauses.len() == 1
                && clause_specs.len() == 1
                && matches!(
                    &q.first.clauses[0],
                    varve_gql::ast::Clause::Match {
                        optional: false,
                        paths,
                        ..
                    } if paths.len() == 1
                ) {
                self.plan_fast_path(&graph, &q, &clause_specs[0].specs, params, &bounds[0])
                    .await?
            } else {
                None
            };
            let fast_paths = fast_path.map(|fast_path| {
                let mut paths: Vec<Option<FastPath>> = std::iter::repeat_with(|| None)
                    .take(clause_specs.len())
                    .collect();
                paths[0] = Some(fast_path);
                paths
            });
            let inputs = scan_inputs_for(
                &self.inner.state,
                &self.inner.store,
                &graph,
                &clause_specs,
                &bounds,
                params,
                self.inner.query_limits,
                fast_paths.as_deref(),
                None,
            )
            .await?;
            Ok::<_, EngineError>((clause_specs, inputs))
        }
        .instrument(tracing::info_span!("varve.query.plan"))
        .await?;
        Ok(varve_plan::execute_body_stream_with_limits(
            &q.first,
            &clause_specs,
            inputs,
            self.inner.functions.as_ref(),
            self.inner.query_limits.path_expand,
            params,
        )
        .instrument(tracing::info_span!("varve.query.execute", graph = %graph))
        .await?)
    }

    /// Task 13 fix: split out of the former `query_body_batches` so the
    /// union path above can attribute plan vs execute the same way the
    /// single-body path does. This covers only scan-spec and scan-input
    /// construction (the planning half); evaluation
    /// (`execute_body_with_limits`) is left to the caller so it lands under
    /// `varve.query.execute` instead of being folded into `varve.query.plan`.
    async fn plan_body_inputs(
        &self,
        graph: &str,
        body: &QueryBody,
        params: &BTreeMap<String, Value>,
        now: Instant,
    ) -> Result<
        (
            Vec<varve_plan::ClauseSpecs>,
            Vec<Vec<varve_plan::ScanInput>>,
        ),
        EngineError,
    > {
        let clause_specs =
            varve_plan::scan_specs_with_params(body, graph, self.inner.max_path_depth, params)?;
        let bounds = bounds_per_clause(body, now);
        let inputs = scan_inputs_for(
            &self.inner.state,
            &self.inner.store,
            graph,
            &clause_specs,
            &bounds,
            params,
            self.inner.query_limits,
            None,
            None,
        )
        .await?;
        Ok((clause_specs, inputs))
    }

    /// Task 12 fast-path selection: decide whether this query's shape is one
    /// the bounded reachable-edge BFS provably covers with a SUPERSET of the
    /// edges that can lie on a qualifying path, and if so build the pruned
    /// input. Returns `None` — the caller then uses the full-scan path
    /// verbatim — for anything not confidently covered (unanchored start, a
    /// heterogeneous fixed path, a mix of fixed and quantified hops, an edge
    /// element whose properties the query references, …). Correctness over
    /// speed: the fast path is a layer over the unchanged, already-correct
    /// scan, never a replacement, so an unsure verdict simply falls back.
    async fn plan_fast_path(
        &self,
        graph: &str,
        q: &varve_gql::ast::QueryStmt,
        specs: &[varve_plan::ScanSpec],
        params: &BTreeMap<String, Value>,
        bounds: &varve_types::TemporalBounds,
    ) -> Result<Option<FastPath>, EngineError> {
        use varve_gql::ast::Direction;
        // Require a point-anchored start node and at least one hop.
        let Some(varve_plan::ScanSpec {
            kind:
                varve_plan::SpecKind::Node {
                    iid_point: Some(anchor),
                    ..
                },
            ..
        }) = specs.first()
        else {
            return Ok(None);
        };
        let query = varve_plan::exec::degenerate_query(q)?;
        let anchor = *anchor;
        let Some(path) = query.paths.first() else {
            return Ok(None);
        };
        if path.hops.is_empty() {
            return Ok(None);
        }
        let dir_to_adj = |d: Direction| match d {
            Direction::Out => crate::scan::AdjDirection::Out,
            Direction::In => crate::scan::AdjDirection::In,
        };

        // Case B: a single quantified hop `(a)-[:L*m..n]->(b)`. All `max`
        // levels use the same (label, direction, props) — homogeneous — so the
        // BFS collects a superset of the reachable adjacency; `expand_paths`
        // only ever reads nodes reachable from the anchor, so the pruned
        // adjacency yields identical walks to the full one. No edge doc columns
        // exist for a quantified hop (adjacency is iid-only), so no schema
        // check is needed.
        if specs.len() == 3 {
            if let varve_plan::SpecKind::Expand {
                label,
                direction,
                props,
                max,
                ..
            } = &specs[1].kind
            {
                let props = const_props(props, params)?;
                let hop = crate::scan::HopSpec {
                    label,
                    props: &props,
                    direction: dir_to_adj(*direction),
                };
                let hops = vec![hop; *max as usize];
                let reachable = crate::scan::reachable_edges(
                    &self.inner.state,
                    &self.inner.store,
                    graph,
                    anchor,
                    &hops,
                    None,
                    bounds,
                    self.inner.query_limits.traversal_node_budget,
                    self.inner.query_limits.traversal_adjacency_budget,
                    None,
                )
                .await?;
                let adjacency = varve_plan::EdgeAdjacency::from_entries(
                    reachable.entries.into_iter().map(|e| {
                        (
                            e.node,
                            varve_plan::AdjEdge {
                                neighbor: e.neighbor,
                                edge: e.edge,
                            },
                        )
                    }),
                );
                return Ok(Some(FastPath::QuantifiedAdjacency(Arc::new(adjacency))));
            }
        }

        // Case A: a fixed k-hop path, all `Edge` hops. Every odd spec must be
        // an `Edge`; a single `Expand` (or a mix) bails to the fallback.
        let mut edge_specs: Vec<(&str, &str, Direction)> = Vec::with_capacity(path.hops.len());
        for spec in specs.iter().skip(1).step_by(2) {
            match &spec.kind {
                varve_plan::SpecKind::Edge { label, direction } => {
                    edge_specs.push((spec.var.as_str(), label.as_str(), *direction));
                }
                _ => return Ok(None),
            }
        }
        // Homogeneous label + direction across all hops (so the global
        // `expanded` dedup in the BFS is sound, and one batch under one label
        // serves every `Edge` element).
        let (_, first_label, first_dir) = edge_specs[0];
        if !edge_specs
            .iter()
            .all(|(_, l, d)| *l == first_label && *d == first_dir)
        {
            return Ok(None);
        }
        // No inline edge props, and no edge element referenced by WHERE or
        // RETURN. Otherwise the reachable subset's (possibly narrower)
        // doc-column schema could differ from the full scan's, turning an
        // empty result into an `UnknownColumn` error (not result-identical).
        // The structural join/temporal columns are always present, so a query
        // that touches no edge doc property is safe.
        if path.hops.iter().any(|(edge, _)| !edge.props.is_empty()) {
            return Ok(None);
        }
        let references_edge_var = |var: &str| edge_specs.iter().any(|(v, _, _)| *v == var);
        if let Some(where_clause) = query.where_clause {
            for expr in where_clause.conjuncts() {
                let Some((var, _, _)) = expr.as_prop_eq() else {
                    return Ok(None);
                };
                if references_edge_var(var) {
                    return Ok(None);
                }
            }
        }
        for (expr, _alias) in &query.ret.items {
            match expr {
                varve_gql::ast::Expr::Prop { var, .. } | varve_gql::ast::Expr::Var(var) => {
                    if references_edge_var(var) {
                        return Ok(None);
                    }
                }
                varve_gql::ast::Expr::FnCall { args, .. } => {
                    for arg in args {
                        match arg {
                            varve_gql::ast::Expr::Var(var)
                            | varve_gql::ast::Expr::Prop { var, .. } => {
                                if references_edge_var(var) {
                                    return Ok(None);
                                }
                            }
                            _ => return Ok(None),
                        }
                    }
                }
                _ => return Ok(None),
            }
        }

        // Build the shared reachable-edge batch: level i follows hop i (all the
        // same label + direction). Props are ignored in the BFS (a wider, still
        // correct, superset) and re-applied per element by
        // `apply_element_predicates`.
        let hops: Vec<crate::scan::HopSpec> = edge_specs
            .iter()
            .map(|(_, _, direction)| crate::scan::HopSpec {
                label: first_label,
                props: &[],
                direction: dir_to_adj(*direction),
            })
            .collect();
        let reachable = crate::scan::reachable_edges(
            &self.inner.state,
            &self.inner.store,
            graph,
            anchor,
            &hops,
            Some(first_label),
            bounds,
            self.inner.query_limits.traversal_node_budget,
            self.inner.query_limits.traversal_adjacency_budget,
            None,
        )
        .await?;
        Ok(Some(FastPath::FixedEdges(reachable.batch)))
    }

    /// Report-only capability probe (spec §12, D5): classifies whether the
    /// store's conditional-PUT semantics actually hold, against a fresh key
    /// under `v1/probe/`. Slice-10's cas-failover coordinator gates on this
    /// verdict at startup; nothing in v1 changes behavior based on it.
    /// Burns one clock tick for key uniqueness (harmless: tx times only
    /// ever need to keep increasing).
    pub async fn probe_capabilities(&self) -> Result<ProbeReport, EngineError> {
        let key = format!(
            "{}/{}-{}",
            varve_storage::PROBE_PREFIX,
            self.inner.clock.next().as_micros(),
            crate::coord::identity::generate_node_id()
        );
        Ok(varve_storage::probe_conditional_put(self.inner.store.as_ref(), &key).await?)
    }

    pub fn roles(&self) -> &NodeRoles {
        &self.inner.roles
    }

    /// I/O-free liveness signal for the public, unauthenticated `/healthz`
    /// route (spec Task 9's health contract): the terminal follower-error
    /// string, read only from the in-memory progress watch. Unlike
    /// [`Db::status`], this never touches the object store, so a momentary
    /// manifest LIST/GET failure — or unauthenticated traffic hammering the
    /// endpoint — can never turn a live node's health check into storage
    /// I/O. `None` means healthy; `Some(_)` is the same follower-error
    /// string `status().follower_error` would carry.
    pub fn follower_error(&self) -> Option<String> {
        self.inner.progress.borrow().follower_error.clone()
    }

    pub async fn status(&self) -> Result<NodeStatus, EngineError> {
        let progress = self.inner.progress.borrow().clone();
        let manifest = varve_storage::latest_manifest(self.inner.store.as_ref()).await?;
        Ok(NodeStatus {
            roles: self.inner.roles.clone(),
            applied: progress.applied,
            manifest_block_id: manifest.as_ref().map(|value| value.block_id),
            manifest_watermark: manifest.as_ref().map_or(LogPosition::ZERO, |value| {
                LogPosition::from_u64(value.watermark)
            }),
            log_head: progress.log_head,
            follower_error: progress.follower_error,
        })
    }

    /// I/O-free engine metrics (Task 12, spec §12, decision 10): the writer's
    /// atomics plus one read-lock pass over the in-memory queryable
    /// inventory. Never touches the object store.
    pub fn metrics(&self) -> EngineMetricsSnapshot {
        use std::sync::atomic::Ordering;
        let state = self.inner.state.read().unwrap_or_else(|e| e.into_inner());
        let live_rows = state.live_rows() as u64;
        let live_bytes = state.live_bytes() as u64;
        let mut persisted_tries: u64 = 0;
        let mut compaction_debt_tries: u64 = 0;
        for graph in state.graphs.values() {
            for scope_len in [
                graph.nodes.tries.len(),
                graph.edges.tries.len(),
                graph.adj_out.len(),
                graph.adj_in.len(),
            ] {
                persisted_tries += scope_len as u64;
                compaction_debt_tries += scope_len.saturating_sub(1) as u64;
            }
        }
        drop(state);
        let cache_tiers = self
            .inner
            .cache_tiers
            .iter()
            .map(|(tier, stats)| CacheTierStats {
                tier: tier.clone(),
                hits: stats.hits.load(Ordering::Relaxed),
                misses: stats.misses.load(Ordering::Relaxed),
            })
            .collect();
        EngineMetricsSnapshot {
            txs_committed: self.inner.metrics.txs_committed.load(Ordering::Relaxed),
            events_committed: self.inner.metrics.events_committed.load(Ordering::Relaxed),
            commit_failures: self.inner.metrics.commit_failures.load(Ordering::Relaxed),
            flush_blocks: self.inner.metrics.flush_blocks.load(Ordering::Relaxed),
            flush_failures: self.inner.metrics.flush_failures.load(Ordering::Relaxed),
            compaction_runs: self.inner.metrics.compaction_runs.load(Ordering::Relaxed),
            backpressure_rejections: self
                .inner
                .metrics
                .backpressure_rejections
                .load(Ordering::Relaxed),
            live_rows,
            live_bytes,
            persisted_tries,
            compaction_debt_tries,
            cache_tiers,
        }
    }

    fn require_role(&self, role: NodeRole) -> Result<(), EngineError> {
        if self.inner.roles.contains(role) {
            Ok(())
        } else {
            Err(EngineError::RoleDisabled(role))
        }
    }
}

/// The result of [`recover`]: the reconstructed table state (live tail +
/// persisted-trie inventory) plus the floors the writer must resume above.
struct Recovered {
    state: GraphsState,
    next_tx_id: u64,
    next_block_id: u64,
    watermark: LogPosition,
}

impl std::fmt::Debug for Recovered {
    /// `TableState` carries no `Debug` impl (its `LiveTable`/`PersistedTrie`
    /// contents aren't debug-formatted anywhere else either), so this only
    /// surfaces the scalar floors — enough for a failed test assertion's
    /// `{:?}` to be useful.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Recovered")
            .field("next_tx_id", &self.next_tx_id)
            .field("next_block_id", &self.next_block_id)
            .field("watermark", &self.watermark)
            .finish_non_exhaustive()
    }
}

/// Spec §6 recovery: latest block manifest (§9) + log tail replay from its
/// watermark. Without a manifest this is exactly slice-3 recovery from
/// position zero. The floors (`max_tx_id`, `max_system_time_us`) come from
/// the manifest because a trimmed log can no longer provide them; replayed
/// records raise them further. The final watermark takes the max of both
/// sources so it never regresses even against a volatile log.
async fn recover(
    log: &dyn varve_log::Log,
    clock: &dyn Clock,
    store: &Arc<dyn ObjectStore>,
) -> Result<Recovered, EngineError> {
    let manifest = varve_storage::latest_manifest(store.as_ref()).await?;
    let mut state = GraphsState::new();
    let (mut next_tx_id, next_block_id, mut watermark, mut max_system) = match &manifest {
        Some(m) => {
            for table in &m.tables {
                let table_state = state
                    .graphs
                    .entry(table.graph.clone())
                    .or_insert_with(TableState::new);
                let dest = match (table.table.as_str(), table.family.as_str()) {
                    (NODES_TABLE, "") => &mut table_state.nodes.tries,
                    (EDGES_TABLE, "") => &mut table_state.edges.tries,
                    (EDGES_TABLE, varve_storage::ADJ_OUT) => &mut table_state.adj_out,
                    (EDGES_TABLE, varve_storage::ADJ_IN) => &mut table_state.adj_in,
                    _ => {
                        return Err(EngineError::UnknownTable(format!(
                            "{}/{}/{}",
                            table.graph, table.table, table.family
                        )))
                    }
                };
                for entry in &table.tries {
                    let meta_key = varve_storage::keys::meta_key_for_family(
                        &table.graph,
                        &table.table,
                        &table.family,
                        &entry.trie_key,
                    );
                    let meta = store.get(&meta_key).await?;
                    let pages = varve_index::block::decode_meta(&meta)?;
                    dest.push(PersistedTrie {
                        entry: entry.clone(),
                        pages: Arc::new(pages),
                    });
                }
            }
            (
                m.max_tx_id,
                m.block_id + 1,
                LogPosition::from_u64(m.watermark),
                Some(Instant::from_micros(m.max_system_time_us)),
            )
        }
        None => (0, 0, LogPosition::ZERO, None),
    };
    apply_persisted_catalog_entries(&mut state, store).await?;

    // Epoch fences (spec §12): a record at a dead position is a zombie —
    // written by a writer whose epoch was seized after a failover — and its
    // tx id is reassigned by the successor epoch's writer. Skip it BEFORE it
    // reaches next_tx_id, the clock floor, or the state fold. `jump` then
    // advances the watermark straight past the whole dead epoch in one step
    // (rather than one skipped record at a time): an unreplayed dead suffix
    // must never be re-read on the next recovery.
    let fences = crate::coord::fence::load_fences(store.as_ref()).await?;

    // Fence-aware contiguity guard (mirrors `follower::apply_range_once` and
    // `verify::verify_database`): `expected` tracks the cursor a LIVE record
    // must land on. A live record at any other position means the log
    // silently dropped something recovery cannot account for — e.g. a stale
    // manifest watermark behind trimmed records — and must fail loudly
    // rather than replay a corrupted-looking prefix. Dead (fenced) records
    // instead jump `expected` across the epoch boundary via `FenceMap::jump`,
    // the exact helper the follower and verify use, so a legitimate
    // successor-epoch record right after a fence is never flagged as a gap.
    let mut expected = watermark;
    for (position, record) in log.tail(watermark).await? {
        if !fences.is_live(position) {
            if let Some(resume) = fences.jump(position)? {
                watermark = watermark.max(resume);
                expected = expected.max(resume);
            }
            continue;
        }
        if position != expected {
            return Err(EngineError::LogGap {
                expected,
                actual: position,
            });
        }
        let decoded = decode_log_record(&record)?;
        let system_time = decoded.system_time;
        let tx_id = decoded.tx_id;
        apply_decoded_log_record(&mut state, decoded)?;
        next_tx_id = next_tx_id.max(tx_id);
        max_system = Some(max_system.map_or(system_time, |current| current.max(system_time)));
        watermark = watermark.max(position.advance(1)?);
        expected = position.advance(1)?;
    }

    if let Some(floor) = max_system {
        clock.advance_to(floor);
    }

    Ok(Recovered {
        state,
        next_tx_id,
        next_block_id,
        watermark,
    })
}

async fn apply_persisted_catalog_entries(
    state: &mut GraphsState,
    store: &Arc<dyn ObjectStore>,
) -> Result<(), EngineError> {
    let Some(meta) = state.graph(META_GRAPH) else {
        return Ok(());
    };
    let tries = meta.nodes.tries.clone();
    for trie in tries {
        let data_key = varve_storage::keys::data_key(META_GRAPH, NODES_TABLE, &trie.entry.trie_key);
        for page in trie.pages.iter() {
            let bytes = store
                .get_range(&data_key, page.offset..page.offset + page.len)
                .await?;
            for event in decode_events(&bytes)? {
                apply_catalog_event(state, &event);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scan::{edge_adjacency, AdjDirection};
    use varve_log::{LogRecord, TableEffects};
    use varve_types::{Iid, TemporalBounds, TemporalDimension, Value};

    static QUERY_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn timeout_boundary_prefers_follower_error_over_satisfied_basis() {
        let (progress_tx, progress) = watch::channel(ProgressState::running(
            0,
            LogPosition::ZERO,
            LogPosition::ZERO,
        ));
        progress_tx.send_modify(|state| {
            state.applied.tx_id = 7;
            state.follower_error = Some("terminal".into());
        });

        assert!(matches!(
            basis_result_after_timeout(&progress, BasisToken::TxId(7)),
            Err(EngineError::FollowerFailed(error)) if error == "terminal"
        ));
    }

    #[test]
    fn timeout_boundary_accepts_at_basis_at_or_below_current_position() {
        let position = LogPosition::from_u64(7);
        let (_progress_tx, progress) =
            watch::channel(ProgressState::running(3, position, position));

        assert!(basis_result_after_timeout(&progress, BasisToken::At(position)).is_ok());
        assert!(
            basis_result_after_timeout(&progress, BasisToken::At(LogPosition::from_u64(6))).is_ok()
        );
    }

    #[test]
    fn timeout_boundary_reports_closed_progress_channel_before_timeout() {
        let (progress_tx, progress) = watch::channel(ProgressState::running(
            0,
            LogPosition::ZERO,
            LogPosition::ZERO,
        ));
        drop(progress_tx);

        assert!(matches!(
            basis_result_after_timeout(&progress, BasisToken::TxId(1)),
            Err(EngineError::FollowerFailed(error)) if error == "progress channel closed"
        ));
    }

    #[test]
    fn timeout_boundary_observes_final_update_before_closed_channel() {
        let (progress_tx, progress) = watch::channel(ProgressState::running(
            0,
            LogPosition::ZERO,
            LogPosition::ZERO,
        ));
        progress_tx.send_modify(|state| {
            state.applied.tx_id = 7;
            state.applied.log_position = LogPosition::from_u64(8);
        });
        drop(progress_tx);

        assert!(basis_result_after_timeout(&progress, BasisToken::TxId(7)).is_ok());
    }

    #[test]
    fn timeout_boundary_uses_latest_progress_for_true_timeout() {
        let (_progress_tx, progress) = watch::channel(ProgressState::running(
            3,
            LogPosition::from_u64(4),
            LogPosition::from_u64(4),
        ));

        assert!(matches!(
            basis_result_after_timeout(&progress, BasisToken::TxId(5)),
            Err(EngineError::BasisTimeout {
                requested: BasisToken::TxId(5),
                applied: AppliedProgress { tx_id: 3, log_position },
            }) if log_position == LogPosition::from_u64(4)
        ));
    }

    #[test]
    fn timeout_boundary_uses_bounded_snapshots_during_continuous_publication() {
        let calls = std::cell::Cell::new(0_u64);

        let result = basis_result_after_timeout_with(BasisToken::TxId(100), || {
            let next = calls.get() + 1;
            calls.set(next);
            BasisTimeoutObservation {
                current: ProgressState::running(
                    next,
                    LogPosition::from_u64(next),
                    LogPosition::from_u64(next),
                ),
                channel: BasisTimeoutChannel::Pending,
            }
        });

        assert_eq!(calls.get(), 2, "timeout resolution must do fixed work");
        assert!(matches!(
            result,
            Err(EngineError::BasisTimeout {
                applied: AppliedProgress { tx_id: 2, .. },
                ..
            })
        ));
    }

    #[test]
    fn log_tuning_uses_human_readable_byte_sizes() {
        let config = Config::from_toml_str("[log]\ngroup_commit_max_bytes = \"1MiB\"\n").unwrap();
        let tuning: LogTuning = config.section("log").unwrap().get().unwrap();

        assert_eq!(
            tuning.group_commit_max_bytes,
            ByteSize::from_bytes(1024 * 1024)
        );
    }

    #[test]
    fn log_tuning_rejects_numeric_byte_sizes() {
        let config = Config::from_toml_str("[log]\ngroup_commit_max_bytes = 1048576\n").unwrap();
        let error = config
            .section("log")
            .unwrap()
            .get::<LogTuning>()
            .err()
            .unwrap();

        assert!(matches!(error, ConfigError::Deserialize(_)));
    }

    #[test]
    fn query_tuning_parses_budget_fields() {
        let _guard = QUERY_ENV_LOCK.lock().unwrap();
        std::env::remove_var("VARVE__QUERY__PATH_ROW_BUDGET");
        std::env::remove_var("VARVE__QUERY__TRAVERSAL_ADJACENCY_BUDGET");
        let cfg = Config::from_toml_str(
            r#"
            [query]
            max_path_depth = 7
            path_output_batch_rows = 16
            path_row_budget = 17
            path_frontier_budget = 18
            traversal_node_budget = 19
            traversal_adjacency_budget = 20
            "#,
        )
        .unwrap();
        let tuning: QueryTuning = cfg.section("query").unwrap().get().unwrap();

        assert_eq!(tuning.max_path_depth, 7);
        assert_eq!(tuning.path_output_batch_rows, 16);
        assert_eq!(tuning.path_row_budget, 17);
        assert_eq!(tuning.path_frontier_budget, 18);
        assert_eq!(tuning.traversal_node_budget, 19);
        assert_eq!(tuning.traversal_adjacency_budget, 20);
    }

    #[test]
    fn query_tuning_env_overrides_budget_fields() {
        let _guard = QUERY_ENV_LOCK.lock().unwrap();
        std::env::remove_var("VARVE__QUERY__PATH_ROW_BUDGET");
        std::env::remove_var("VARVE__QUERY__TRAVERSAL_ADJACENCY_BUDGET");
        std::env::set_var("VARVE__QUERY__PATH_ROW_BUDGET", "33");
        std::env::set_var("VARVE__QUERY__TRAVERSAL_ADJACENCY_BUDGET", "44");

        let cfg = Config::from_toml_str("").unwrap();
        let tuning: QueryTuning = cfg.section("query").unwrap().get().unwrap();

        assert_eq!(tuning.path_row_budget, 33);
        assert_eq!(tuning.traversal_adjacency_budget, 44);

        std::env::remove_var("VARVE__QUERY__PATH_ROW_BUDGET");
        std::env::remove_var("VARVE__QUERY__TRAVERSAL_ADJACENCY_BUDGET");
    }

    #[tokio::test]
    async fn insert_edge_with_inline_nodes_populates_edges_live() {
        let db = Db::memory();
        db.execute("INSERT (:Person {_id: 1, name: 'Ada'})-[:KNOWS {since: 2020}]->(:Person {_id: 2, name: 'Bob'})")
            .await
            .unwrap();
        let s = db.inner.state.read().unwrap();
        assert_eq!(s.graph(DEFAULT_GRAPH).unwrap().nodes.live.event_count(), 2);
        assert_eq!(s.graph(DEFAULT_GRAPH).unwrap().edges.live.event_count(), 1);
        let ada = Iid::derive("default", "nodes", &Value::Int(1).id_bytes().unwrap());
        let out: Vec<_> = s
            .graph(DEFAULT_GRAPH)
            .unwrap()
            .edges
            .live
            .out_edges(&ada)
            .collect();
        assert_eq!(out.len(), 1);
    }

    /// Slice 10: on an idle `Db` (writer queue nowhere near full),
    /// `try_execute_as` behaves exactly like `execute_as` — the full-queue
    /// `Backpressure` path is exercised deterministically at the
    /// `WriterHandle` level in `writer::tests`.
    #[tokio::test]
    async fn try_execute_as_succeeds_on_an_idle_writer() {
        let db = Db::memory();
        let params = BTreeMap::new();
        let receipt = db
            .try_execute_as("INSERT (:Person {_id: 1, name: 'Ada'})", &params, "ada")
            .await
            .unwrap();
        assert_eq!(receipt.side_effects.nodes_created, 1);
        let s = db.inner.state.read().unwrap();
        assert_eq!(s.graph(DEFAULT_GRAPH).unwrap().nodes.live.event_count(), 1);
    }

    /// Task 12: `try_execute_as` increments `backpressure_rejections` on a
    /// `Backpressure` rejection. Rather than racing a real writer loop (see
    /// `writer::tests::try_submit_on_a_full_queue_is_backpressure`), this
    /// polls each submission's future exactly once (`now_or_never`): the
    /// synchronous `try_submit` prefix runs on that single poll, then the
    /// future either pends at `rx.await` (accepted) or resolves immediately
    /// with `Err(Backpressure)` (rejected) — the writer task itself never
    /// gets a chance to run, since nothing here ever awaits.
    #[tokio::test]
    async fn try_execute_as_backpressure_increments_the_counter() {
        use futures::FutureExt;

        let (store, cache_tiers) = cached(memory_store());
        let db = Db::assemble(
            GraphsState::new(),
            Arc::new(MemoryLog::new()),
            store,
            cache_tiers,
            Arc::new(MonotonicClock::new()),
            WriterConfig {
                window: Duration::ZERO,
                queue_len: 1,
                ..WriterConfig::default()
            },
            0,
            0,
            LogPosition::ZERO,
            default_max_path_depth(),
            QueryTuning::default().limits(),
            GcConfig::default(),
            NodeRoles::all(),
            FollowerConfig {
                poll_interval: Duration::from_millis(50),
                batch_records: 1024,
            },
            Duration::from_millis(5000),
            None,
        );
        let params = BTreeMap::new();

        // Fills the queue_len=1 channel's one slot; pends at `rx.await`.
        let _first = db
            .try_execute_as("INSERT (:P {_id: 1})", &params, "u")
            .now_or_never();

        // The channel has no room left, so this must reject synchronously.
        let second = db
            .try_execute_as("INSERT (:P {_id: 2})", &params, "u")
            .now_or_never()
            .expect("a synchronous Backpressure rejection never pends");
        assert!(matches!(second, Err(EngineError::Backpressure)));

        assert_eq!(db.metrics().backpressure_rejections, 1);
    }

    #[tokio::test]
    async fn insert_edge_var_reuse_binds_within_statement() {
        let db = Db::memory();
        db.execute("INSERT (a:Person {_id: 1}), (a)-[:KNOWS]->(b:Person {_id: 2})")
            .await
            .unwrap();
        let s = db.inner.state.read().unwrap();
        assert_eq!(s.graph(DEFAULT_GRAPH).unwrap().nodes.live.event_count(), 2);
        assert_eq!(s.graph(DEFAULT_GRAPH).unwrap().edges.live.event_count(), 1);
    }

    #[tokio::test]
    async fn insert_edge_binding_errors() {
        let db = Db::memory();
        // Bare (a) with no prior binding in THIS statement (bindings are
        // statement-local, never carried across execute calls):
        let err = db
            .execute("INSERT (a)-[:K]->(:P {_id: 9})")
            .await
            .unwrap_err();
        assert!(matches!(err, EngineError::UnboundVariable(_)));
        // Re-using a bound var with labels/props is an error — a reference must
        // be a bare (x):
        let err2 = db
            .execute("INSERT (x:P {_id: 3}), (x:P)-[:K]->(:P {_id: 4})")
            .await
            .unwrap_err();
        assert!(matches!(err2, EngineError::AlreadyBoundVariable(_)));
        // Atomicity: both statements failed at resolve, so NOTHING was applied —
        // not even the syntactically fine (:P {_id: 9}) / (x:P {_id: 3}) parts.
        let s = db.inner.state.read().unwrap();
        assert_eq!(s.graph(DEFAULT_GRAPH).unwrap().nodes.live.event_count(), 0);
        assert_eq!(s.graph(DEFAULT_GRAPH).unwrap().edges.live.event_count(), 0);
    }

    #[tokio::test]
    async fn match_insert_binds_matched_nodes_cartesian() {
        let db = Db::memory();
        db.execute("INSERT (:Person {_id: 1, name: 'Ada'}), (:Person {_id: 2, name: 'Bob'}), (:Person {_id: 3, name: 'Bob'})")
            .await
            .unwrap();
        db.execute(
            "MATCH (a:Person {name: 'Ada'}), (b:Person {name: 'Bob'}) INSERT (a)-[:KNOWS]->(b)",
        )
        .await
        .unwrap();
        let s = db.inner.state.read().unwrap();
        // 1 Ada × 2 Bobs = 2 edges.
        assert_eq!(s.graph(DEFAULT_GRAPH).unwrap().edges.live.event_count(), 2);
    }

    #[tokio::test]
    async fn match_insert_complex_where_filters_candidates() {
        let db = Db::memory();
        db.execute("INSERT (:A {_id: 1, x: 1, y: 3}), (:A {_id: 2, x: 1, y: 1})")
            .await
            .unwrap();
        db.execute("MATCH (a:A) WHERE a.x = 1 AND a.y > 2 INSERT (a)-[:K]->(:B {_id: 'b'})")
            .await
            .unwrap();

        let s = db.inner.state.read().unwrap();
        assert_eq!(s.graph(DEFAULT_GRAPH).unwrap().nodes.live.event_count(), 3);
        assert_eq!(s.graph(DEFAULT_GRAPH).unwrap().edges.live.event_count(), 1);
    }

    #[tokio::test]
    async fn match_insert_applies_all_conjuncts() {
        let db = Db::memory();
        db.execute("INSERT (:A {_id: 1, x: 1, y: 2})")
            .await
            .unwrap();

        db.execute("MATCH (a:A) WHERE a.x = 1 AND a.y = 2 INSERT (a)-[:K]->(:B {_id: 1})")
            .await
            .unwrap();

        let s = db.inner.state.read().unwrap();
        assert_eq!(s.graph(DEFAULT_GRAPH).unwrap().nodes.live.event_count(), 2);
        assert_eq!(s.graph(DEFAULT_GRAPH).unwrap().edges.live.event_count(), 1);
    }

    #[tokio::test]
    async fn match_insert_rejects_unmatched_where_variable_without_partial_insert() {
        let db = Db::memory();
        db.execute("INSERT (:A {_id: 1, x: 1})").await.unwrap();

        let err = db
            .execute("MATCH (a:A) WHERE b.x = 1 INSERT (a)-[:K]->(:B {_id: 1})")
            .await
            .unwrap_err();
        assert!(
            matches!(err, EngineError::Plan(PlanError::UnknownVariable(ref var)) if var == "b"),
            "{err:?}"
        );

        let s = db.inner.state.read().unwrap();
        assert_eq!(s.graph(DEFAULT_GRAPH).unwrap().nodes.live.event_count(), 1);
        assert_eq!(s.graph(DEFAULT_GRAPH).unwrap().edges.live.event_count(), 0);
    }

    #[tokio::test]
    async fn edge_ids_user_supplied_and_derived_are_durable() {
        let db = Db::memory();
        db.execute("INSERT (:P {_id: 1})-[:K {_id: 7}]->(:P {_id: 2})")
            .await
            .unwrap();
        db.execute("INSERT (:P {_id: 3})-[:K]->(:P {_id: 4})")
            .await
            .unwrap();
        let s = db.inner.state.read().unwrap();
        let user = Iid::derive("default", "edges", &Value::Int(7).id_bytes().unwrap());
        assert!(s
            .graph(DEFAULT_GRAPH)
            .unwrap()
            .edges
            .live
            .events_for(&user)
            .is_some());
        assert_eq!(s.graph(DEFAULT_GRAPH).unwrap().edges.live.event_count(), 2);
    }

    /// log + storage both `local` under `dir`, `max_block_rows` low so any
    /// committed tx trips the size-flush trigger — the exact slice-4 block
    /// mechanism (see `crates/varve/tests/blocks.rs`), reused here so
    /// `force_flush` can push a replayed live tail into a durable block.
    fn blocks_config(dir: &std::path::Path, max_block_rows: usize) -> Config {
        let log_dir = format!("{:?}", dir.join("log").display().to_string());
        let store_dir = format!("{:?}", dir.join("store").display().to_string());
        Config::from_toml_str(&format!(
            "[log]\nbackend = \"local\"\ngroup_commit_window_ms = 1\n\
             [log.local]\ndir = {log_dir}\n\
             [storage]\nbackend = \"local\"\nmax_block_rows = {max_block_rows}\n\
             [storage.local]\ndir = {store_dir}\n"
        ))
        .unwrap()
    }

    /// Forces the writer to flush its live tail to a block: the `db` must have
    /// been opened with a low `max_block_rows` (via `blocks_config`), so one
    /// committed tx trips the post-batch size trigger. Polls the trie
    /// inventory (populated under the flush's write lock, strictly after the
    /// atomic manifest PUT) until the flush has landed.
    async fn force_flush(db: &Db) {
        db.execute("INSERT (:_Flush {_id: 0})").await.unwrap();
        for _ in 0..200 {
            {
                let s = db.inner.state.read().unwrap();
                if !s.graph(DEFAULT_GRAPH).unwrap().edges.tries.is_empty()
                    && !s.graph(DEFAULT_GRAPH).unwrap().nodes.tries.is_empty()
                {
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("force_flush: block flush did not land within 5s");
    }

    /// Still-connected incidence (T7) must see FLUSHED edges, not just live
    /// ones: `force_flush` needs a low `max_block_rows` to actually trigger
    /// (its own doc comment), so this reuses `blocks_config(dir, 1)` — the
    /// same size-flush setup as `node_only_flush_preserves_prior_edges_trie_inventory`
    /// below, whose block-0 step persists this exact edge shape.
    #[tokio::test]
    async fn detach_delete_sees_flushed_edges() {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open(blocks_config(dir.path(), 1)).await.unwrap();
        db.execute("INSERT (:P {_id: 1})-[:K]->(:P {_id: 2})")
            .await
            .unwrap();
        force_flush(&db).await;
        let err = db
            .execute("MATCH (p:P) WHERE p._id = 1 DELETE p")
            .await
            .unwrap_err();
        assert!(matches!(err, EngineError::StillConnected(1)));
        db.execute("MATCH (p:P) WHERE p._id = 1 DETACH DELETE p")
            .await
            .unwrap();
        let s = db.inner.state.read().unwrap();
        // Two edge events now exist for the edge (Put flushed + Delete live).
        assert_eq!(s.graph(DEFAULT_GRAPH).unwrap().edges.live.event_count(), 1);
    }

    #[tokio::test]
    async fn edges_survive_log_replay_and_block_flush_restart() {
        let dir = tempfile::tempdir().unwrap();
        {
            // `Db::local` uses the default (high) block threshold, so this tx
            // stays in the log only — segment 2 must recover it via replay.
            let db = Db::local(dir.path()).await.unwrap();
            db.execute("INSERT (:P {_id: 1, name: 'Ada'})-[:KNOWS]->(:P {_id: 2, name: 'Bob'})")
                .await
                .unwrap();
        } // drop = close; events only in the log
        {
            // Reopen with a flush-forcing config over the SAME dirs: replay
            // restores the edge to the live tail, then `force_flush` commits
            // a block.
            let db = Db::open(blocks_config(dir.path(), 1)).await.unwrap();
            {
                let s = db.inner.state.read().unwrap();
                assert_eq!(
                    s.graph(DEFAULT_GRAPH).unwrap().edges.live.event_count(),
                    1,
                    "log replay must restore edges"
                );
            }
            force_flush(&db).await;
        }
        {
            let db = Db::local(dir.path()).await.unwrap();
            let s = db.inner.state.read().unwrap();
            assert_eq!(
                s.graph(DEFAULT_GRAPH).unwrap().edges.live.event_count(),
                0,
                "flushed"
            );
            assert_eq!(
                s.graph(DEFAULT_GRAPH).unwrap().edges.tries.len(),
                1,
                "edges primary trie recovered from manifest"
            );
        }
    }

    /// Regression guard for `flush_block`'s "full inventory" comment
    /// (flush.rs): manifest.tables gets one `TableTries` per table with
    /// PRIOR OR NEW tries — including a table that flushed nothing THIS
    /// block. Block 0 gives edges its only trie; block 1 flushes nodes
    /// ONLY (edges' live tail is empty by then), so recovery from block 1's
    /// manifest is the only way to see whether edges' entry survived a
    /// block where edges had nothing new to contribute. Reuses the exact
    /// `blocks_config`/`max_block_rows = 1` size-flush trigger as
    /// `edges_survive_log_replay_and_block_flush_restart` above — no new
    /// flush mechanism, no writer-loop changes.
    #[tokio::test]
    async fn node_only_flush_preserves_prior_edges_trie_inventory() {
        let dir = tempfile::tempdir().unwrap();
        {
            let db = Db::open(blocks_config(dir.path(), 1)).await.unwrap();

            // Block 0: an edge insert also creates its two endpoint nodes,
            // so BOTH tables have live rows when `max_block_rows = 1` trips
            // the post-batch flush — nodes and edges each get their first
            // persisted trie in this same block.
            db.execute("INSERT (:P {_id: 1})-[:K]->(:P {_id: 2})")
                .await
                .unwrap();
            for _ in 0..200 {
                {
                    let s = db.inner.state.read().unwrap();
                    if s.graph(DEFAULT_GRAPH).unwrap().nodes.tries.len() == 1
                        && s.graph(DEFAULT_GRAPH).unwrap().edges.tries.len() == 1
                    {
                        break;
                    }
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            {
                let s = db.inner.state.read().unwrap();
                assert_eq!(
                    s.graph(DEFAULT_GRAPH).unwrap().nodes.tries.len(),
                    1,
                    "block 0: nodes trie landed"
                );
                assert_eq!(
                    s.graph(DEFAULT_GRAPH).unwrap().edges.tries.len(),
                    1,
                    "block 0: edges trie landed"
                );
                assert_eq!(
                    s.graph(DEFAULT_GRAPH).unwrap().edges.live.event_count(),
                    0,
                    "block 0's flush reset edges' live tail"
                );
            }

            // Block 1: a node-only insert. edges.live is empty, so this
            // flush has nothing new for edges — it must still carry
            // edges' PRIOR trie into the manifest, or recovery from THIS
            // manifest silently loses the block-0 edge trie.
            db.execute("INSERT (:P {_id: 3})").await.unwrap();
            for _ in 0..200 {
                {
                    let s = db.inner.state.read().unwrap();
                    if s.graph(DEFAULT_GRAPH).unwrap().nodes.tries.len() == 2 {
                        break;
                    }
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            assert_eq!(
                db.inner
                    .state
                    .read()
                    .unwrap()
                    .graph(DEFAULT_GRAPH)
                    .unwrap()
                    .nodes
                    .tries
                    .len(),
                2,
                "block 1: node-only flush landed as its own block"
            );
        } // drop = close
        {
            // Fresh handle: recovery reads ONLY the latest manifest (block
            // 1's, nodes-only new data) to rebuild the trie inventory. If
            // `flush_block` only emitted a `TableTries` entry for tables
            // that flushed something THIS block, block 1's manifest would
            // omit `edges` entirely and this would recover 0 edge tries —
            // even though the block-0 edge data/meta objects still sit in
            // the store, now unreachable.
            let db = Db::local(dir.path()).await.unwrap();
            let s = db.inner.state.read().unwrap();
            assert_eq!(
                s.graph(DEFAULT_GRAPH).unwrap().edges.tries.len(),
                1,
                "edges' block-0 trie must survive block 1's node-only manifest write"
            );
            assert_eq!(
                s.graph(DEFAULT_GRAPH).unwrap().nodes.tries.len(),
                2,
                "both node tries also recovered"
            );
        }
    }

    /// Live-index memory watermark (Task 11): `max_block_rows` is set far out
    /// of reach (1_000_000) so only the byte watermark can trip the flush.
    /// `max_live_bytes = "4KiB"` is small enough that a handful of ~1 KiB
    /// docs cross it well before any row-count trigger would.
    fn byte_watermark_config(dir: &std::path::Path) -> Config {
        let log_dir = format!("{:?}", dir.join("log").display().to_string());
        let store_dir = format!("{:?}", dir.join("store").display().to_string());
        Config::from_toml_str(&format!(
            "[log]\nbackend = \"local\"\ngroup_commit_window_ms = 1\n\
             [log.local]\ndir = {log_dir}\n\
             [storage]\nbackend = \"local\"\nmax_block_rows = 1000000\n\
             max_live_bytes = \"4KiB\"\nflush_interval_ms = 0\n\
             [storage.local]\ndir = {store_dir}\n"
        ))
        .unwrap()
    }

    #[tokio::test]
    async fn live_bytes_watermark_forces_an_early_flush() {
        use datafusion::arrow::array::{Array, Int64Array, StringArray};

        let dir = tempfile::tempdir().unwrap();
        let db = Db::open(byte_watermark_config(dir.path())).await.unwrap();
        let payload: String = "a".repeat(1024);
        for i in 0..8 {
            db.execute(&format!("INSERT (:P {{_id: {i}, blob: '{payload}'}})"))
                .await
                .unwrap();
        }
        let mut flushed = false;
        for _ in 0..200 {
            {
                let s = db.inner.state.read().unwrap();
                if !s.graph(DEFAULT_GRAPH).unwrap().nodes.tries.is_empty() {
                    flushed = true;
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(
            flushed,
            "live_bytes_watermark_forces_an_early_flush: no block flush within 5s \
             even though max_block_rows is far from tripping"
        );

        // `approx_bytes` is a heuristic (Task 11) that must NEVER affect
        // correctness: a flush firing early is not enough — the docs it
        // swept up must still be intact and correct through the normal read
        // path. This queries AFTER the flush was observed above, so a live
        // table alone can't satisfy it; the block encode/decode round trip
        // has to be right.
        let batches = db
            .query("MATCH (p:P) RETURN p._id AS id, p.blob AS blob")
            .await
            .unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            rows, 8,
            "all 8 docs must still be readable after the early flush"
        );
        let mut found_three = false;
        for batch in &batches {
            let ids: &Int64Array = batch
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref()
                .unwrap();
            let blobs: &StringArray = batch
                .column_by_name("blob")
                .unwrap()
                .as_any()
                .downcast_ref()
                .unwrap();
            for i in 0..batch.num_rows() {
                if ids.value(i) == 3 {
                    found_three = true;
                    assert_eq!(
                        blobs.value(i),
                        payload,
                        "blob for _id=3 must round-trip byte-for-byte through the early flush"
                    );
                }
            }
        }
        assert!(found_three, "expected _id=3 among the post-flush rows");
    }

    /// Task 12 (spec §12, decision 10): `Db::metrics()` reflects committed
    /// transactions and the live (unflushed) row count purely from atomics
    /// and one in-memory read-lock pass — no object-store I/O, no flush
    /// needed for these two fields to update.
    #[tokio::test]
    async fn metrics_snapshot_tracks_committed_transactions_and_live_rows() {
        let db = Db::memory();
        db.execute("INSERT (:P {_id: 1})").await.unwrap();
        db.execute("INSERT (:P {_id: 2})").await.unwrap();

        let snapshot = db.metrics();
        assert_eq!(snapshot.txs_committed, 2);
        assert_eq!(snapshot.live_rows, 2);
    }

    /// Task 12: a real block flush (triggered here by a tiny
    /// `max_live_bytes` watermark, mirroring
    /// `live_bytes_watermark_forces_an_early_flush` above) increments
    /// `flush_blocks` exactly once per successful `flush_block` call.
    #[tokio::test]
    async fn metrics_snapshot_counts_a_flush_block() {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open(byte_watermark_config(dir.path())).await.unwrap();
        let payload: String = "a".repeat(1024);
        for i in 0..8 {
            db.execute(&format!("INSERT (:P {{_id: {i}, blob: '{payload}'}})"))
                .await
                .unwrap();
        }

        let mut flush_blocks = 0;
        for _ in 0..200 {
            flush_blocks = db.metrics().flush_blocks;
            if flush_blocks >= 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert_eq!(
            flush_blocks, 1,
            "metrics_snapshot_counts_a_flush_block: expected exactly one flush_block within 5s of crossing the byte watermark"
        );
    }

    /// `recover`'s log-tail loop checks `effect.table` before ever touching
    /// `arrow_ipc` (v1 targets are `nodes` and `edges`), so an unknown table
    /// must hard-fail with `UnknownTable` — even though the log record here
    /// carries deliberately undecodable bytes that would blow up
    /// `decode_events` if the guard were ever removed or reordered. (Slice 6
    /// flip: `edges` is now a real table, so the rejection case uses an
    /// invented `widgets` table instead.)
    #[tokio::test]
    async fn recover_rejects_unknown_table_in_the_log_tail() {
        let log = MemoryLog::new();
        log.append(vec![LogRecord {
            tx_id: 1,
            system_time_us: 0,
            user: String::new(),
            effects: vec![TableEffects {
                table: "widgets".to_string(),
                arrow_ipc: vec![0xAA], // never decoded: table check happens first
                graph: String::new(),
            }],
        }])
        .await
        .unwrap();

        let clock = MonotonicClock::new();
        let store = memory_store();
        match recover(&log, &clock, &store).await {
            Err(EngineError::UnknownTable(t)) => assert_eq!(t, "widgets"),
            Err(other) => panic!("expected UnknownTable, got {other:?}"),
            Ok(_) => panic!("expected recover to fail on the unknown table"),
        }
    }

    /// Finding 1 (Slice-10 final-whole-branch-review): `recover`'s tail loop
    /// must reject a GENUINE gap between two LIVE records — one that no
    /// epoch fence explains — exactly as `follower::apply_range_once` and
    /// `verify::verify_database` already do. Here the log jumps straight
    /// from epoch 0 to epoch 2 (skipping epoch 1 entirely) with no fence
    /// document ever written, so the missing `(1, 0)` is unaccounted for
    /// and recovery must fail loudly with `LogGap` instead of silently
    /// replaying past it (a stale-manifest-watermark-behind-trimmed-records
    /// scenario would look exactly like this to the tail loop).
    #[tokio::test]
    async fn recover_rejects_a_genuine_gap_not_explained_by_any_fence() {
        let log = MemoryLog::new();
        log.append(vec![
            LogRecord {
                tx_id: 1,
                system_time_us: 1,
                user: String::new(),
                effects: vec![],
            },
            LogRecord {
                tx_id: 2,
                system_time_us: 2,
                user: String::new(),
                effects: vec![],
            },
        ])
        .await
        .unwrap();
        // Skip epoch 1 entirely — no fence document is ever written for it,
        // so nothing explains the missing (1, 0).
        log.start_epoch(2).await.unwrap();
        log.append(vec![LogRecord {
            tx_id: 3,
            system_time_us: 3,
            user: String::new(),
            effects: vec![],
        }])
        .await
        .unwrap();

        let clock = MonotonicClock::new();
        let store = memory_store();
        match recover(&log, &clock, &store).await {
            Err(EngineError::LogGap { expected, actual }) => {
                assert_eq!(expected, LogPosition::new(0, 2).unwrap());
                assert_eq!(actual, LogPosition::new(2, 0).unwrap());
            }
            Err(other) => panic!("expected LogGap, got {other:?}"),
            Ok(_) => panic!("expected recover to reject the unexplained gap"),
        }
    }

    /// The slice-6 flip's positive half: a log record carrying an `edges`
    /// effect batch now REPLAYS (rather than hard-failing) — its events land
    /// in the edges live table.
    #[tokio::test]
    async fn recover_replays_edges_effect_batch_from_the_log_tail() {
        use crate::state::{DEFAULT_GRAPH, EDGES_TABLE, NODES_TABLE};
        use varve_index::{encode_events, Event, Op};
        use varve_types::Doc;

        let edge = Event {
            iid: Iid::derive(
                DEFAULT_GRAPH,
                EDGES_TABLE,
                &Value::Int(7).id_bytes().unwrap(),
            ),
            system_from: Instant::from_micros(1),
            valid_from: Instant::from_micros(1),
            valid_to: Instant::END_OF_TIME,
            src: Some(Iid::derive(DEFAULT_GRAPH, NODES_TABLE, &[1])),
            dst: Some(Iid::derive(DEFAULT_GRAPH, NODES_TABLE, &[2])),
            op: Op::Put {
                labels: vec!["KNOWS".into()],
                doc: Doc::new(),
            },
        };
        let log = MemoryLog::new();
        log.append(vec![LogRecord {
            tx_id: 1,
            system_time_us: 1,
            user: String::new(),
            effects: vec![TableEffects {
                table: EDGES_TABLE.to_string(),
                arrow_ipc: encode_events(std::slice::from_ref(&edge)).unwrap(),
                graph: String::new(),
            }],
        }])
        .await
        .unwrap();

        let clock = MonotonicClock::new();
        let store = memory_store();
        let recovered = recover(&log, &clock, &store).await.unwrap();
        assert_eq!(
            recovered
                .state
                .graph(DEFAULT_GRAPH)
                .unwrap()
                .edges
                .live
                .event_count(),
            1
        );
        assert_eq!(
            recovered
                .state
                .graph(DEFAULT_GRAPH)
                .unwrap()
                .nodes
                .live
                .event_count(),
            0
        );
    }

    /// Replay starts AT the manifest watermark: records below it (still in
    /// an untrimmed log — the post-manifest-crash shape) are NOT re-applied.
    #[tokio::test]
    async fn recover_skips_records_below_the_manifest_watermark() {
        use crate::state::{DEFAULT_GRAPH, NODES_TABLE};
        use bytes::Bytes;
        use varve_index::block::encode_block;
        use varve_index::{encode_events, Event, Op};
        use varve_log::TableEffects;
        use varve_storage::{keys, memory_store, BlockManifest, TableTries, TrieEntry};
        use varve_types::Doc;

        fn event(n: u8, sf: i64) -> Event {
            Event {
                iid: varve_types::Iid::derive(DEFAULT_GRAPH, NODES_TABLE, &[n]),
                system_from: Instant::from_micros(sf),
                valid_from: Instant::from_micros(sf),
                valid_to: Instant::END_OF_TIME,
                src: None,
                dst: None,
                op: Op::Put {
                    labels: vec!["P".into()],
                    doc: Doc::new(),
                },
            }
        }
        fn record(tx_id: u64, e: &Event) -> LogRecord {
            LogRecord {
                tx_id,
                system_time_us: e.system_from.as_micros(),
                user: String::new(),
                effects: vec![TableEffects {
                    table: NODES_TABLE.to_string(),
                    arrow_ipc: encode_events(std::slice::from_ref(e)).unwrap(),
                    graph: String::new(),
                }],
            }
        }

        let (e1, e2, e3) = (event(1, 1), event(2, 2), event(3, 3));
        // Full, UNTRIMMED log: positions 0, 1, 2.
        let log = MemoryLog::new();
        for (tx, e) in [(1u64, &e1), (2, &e2), (3, &e3)] {
            log.append(vec![record(tx, e)]).await.unwrap();
        }
        // Manifest says: block 0 holds e1+e2, replay from position 2.
        let store = memory_store();
        let mut flushed = LiveTable::new();
        flushed.append(e1).unwrap();
        flushed.append(e2).unwrap();
        let block = encode_block(&flushed, 1024).unwrap();
        let trie_key = keys::l0_trie_key(0);
        store
            .put(
                &keys::data_key(DEFAULT_GRAPH, NODES_TABLE, &trie_key),
                Bytes::from(block.data),
            )
            .await
            .unwrap();
        store
            .put(
                &keys::meta_key(DEFAULT_GRAPH, NODES_TABLE, &trie_key),
                Bytes::from(block.meta),
            )
            .await
            .unwrap();
        let manifest = BlockManifest {
            block_id: 0,
            watermark: 2,
            max_tx_id: 2,
            max_system_time_us: 2,
            tables: vec![TableTries {
                graph: DEFAULT_GRAPH.to_string(),
                table: NODES_TABLE.to_string(),
                family: String::new(),
                tries: vec![TrieEntry {
                    trie_key,
                    row_count: 2,
                    data_len: 0,
                }],
            }],
        };
        store
            .put(&keys::manifest_key(0), Bytes::from(manifest.to_wire()))
            .await
            .unwrap();

        let clock = MonotonicClock::new();
        let recovered = recover(&log, &clock, &store).await.unwrap();
        assert_eq!(
            recovered
                .state
                .graph(DEFAULT_GRAPH)
                .unwrap()
                .nodes
                .live
                .event_count(),
            1,
            "only the post-watermark record replays"
        );
        assert_eq!(
            recovered
                .state
                .graph(DEFAULT_GRAPH)
                .unwrap()
                .nodes
                .tries
                .len(),
            1
        );
        assert_eq!(recovered.next_tx_id, 3);
        assert_eq!(recovered.next_block_id, 1);
        assert_eq!(recovered.watermark.as_u64(), 3);
    }

    #[tokio::test]
    async fn restart_preserves_path_pruning_metadata() {
        use bytes::Bytes;
        use varve_index::{encode_sorted_events_by, Event, Op, SortOrder};
        use varve_storage::{keys, memory_store, BlockManifest, TableTries, TrieEntry};
        use varve_types::Doc;

        fn raw_iid(first: u8) -> Iid {
            Iid::from_bytes([first, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
        }

        fn event(first: u8, sf: i64) -> Event {
            Event {
                iid: raw_iid(first),
                system_from: Instant::from_micros(sf),
                valid_from: Instant::from_micros(sf),
                valid_to: Instant::END_OF_TIME,
                src: None,
                dst: None,
                op: Op::Put {
                    labels: vec!["P".into()],
                    doc: Doc::new(),
                },
            }
        }

        let rows = vec![event(0x00, 1), event(0x40, 2)];
        let block = encode_sorted_events_by(&rows, 1, SortOrder::ByIid, 1).unwrap();
        let expected_paths: Vec<Vec<u8>> =
            block.pages.iter().map(|page| page.path.clone()).collect();
        assert_eq!(expected_paths, vec![vec![0], vec![1]]);

        let store = memory_store();
        let trie_key = "l01-rc-b00".to_string();
        let data_len = block.data.len() as u64;
        store
            .put(
                &keys::data_key(DEFAULT_GRAPH, NODES_TABLE, &trie_key),
                Bytes::from(block.data),
            )
            .await
            .unwrap();
        store
            .put(
                &keys::meta_key(DEFAULT_GRAPH, NODES_TABLE, &trie_key),
                Bytes::from(block.meta),
            )
            .await
            .unwrap();
        store
            .put(
                &keys::manifest_key(0),
                Bytes::from(
                    BlockManifest {
                        block_id: 0,
                        watermark: 0,
                        max_tx_id: 0,
                        max_system_time_us: 2,
                        tables: vec![TableTries {
                            graph: DEFAULT_GRAPH.to_string(),
                            table: NODES_TABLE.to_string(),
                            family: String::new(),
                            tries: vec![TrieEntry {
                                trie_key: trie_key.clone(),
                                row_count: rows.len() as u64,
                                data_len,
                            }],
                        }],
                    }
                    .to_wire(),
                ),
            )
            .await
            .unwrap();

        let log = MemoryLog::new();
        let clock = MonotonicClock::new();
        let recovered = recover(&log, &clock, &store).await.unwrap();
        let graph = recovered.state.graph(DEFAULT_GRAPH).unwrap();
        assert_eq!(graph.nodes.tries.len(), 1);
        assert_eq!(graph.nodes.tries[0].entry.trie_key, trie_key);
        let recovered_paths: Vec<Vec<u8>> = graph.nodes.tries[0]
            .pages
            .iter()
            .map(|page| page.path.clone())
            .collect();
        assert_eq!(recovered_paths, expected_paths);
        assert!(recovered_paths.iter().all(|path| !path.is_empty()));
    }

    /// Anchor-pruned adjacency must return exactly the full-scan result
    /// filtered to that anchor — across a LIVE tail, a FLUSHED adjacency
    /// family, and a fresh recovery from the manifest. Uses the T5 flush-
    /// forcing route: `blocks_config` with a threshold of 8 so the setup
    /// statements accumulate WITHOUT flushing (live_rows 3 → 4 → 5 → 6 → 7),
    /// then `force_flush`'s one `_Flush` node pushes live_rows to 8 and
    /// commits block 0 (nodes 1-4 + `_Flush`, edges 1→2, 3→4, 1→3) — after
    /// which one more edge (3→2) stays live.
    ///
    /// Node 1 (`ada`)'s two out-edges (1→2, 1→3) are BOTH persisted, so an
    /// anchored query there alone would never exercise the live-merge path
    /// — node 3 (`cyd`) instead gets ONE persisted out-edge (3→4, added
    /// before the flush) AND the one live out-edge (3→2), so `edge_adjacency`
    /// under that anchor genuinely merges both sources. Node 2 (`bob`)
    /// symmetrically gets one persisted (1→2) and one live (3→2) in-edge,
    /// giving `AdjDirection::In` — never functionally exercised before —
    /// the same live+persisted merge coverage.
    #[tokio::test]
    async fn adjacency_matches_across_live_and_flushed_and_restart() {
        let dir = tempfile::tempdir().unwrap();
        let ada = Iid::derive("default", "nodes", &Value::Int(1).id_bytes().unwrap());
        let bob = Iid::derive("default", "nodes", &Value::Int(2).id_bytes().unwrap());
        let cyd = Iid::derive("default", "nodes", &Value::Int(3).id_bytes().unwrap());
        {
            let db = Db::open(blocks_config(dir.path(), 8)).await.unwrap();
            db.execute("INSERT (:P {_id: 1})-[:KNOWS]->(:P {_id: 2})")
                .await
                .unwrap();
            db.execute("INSERT (:P {_id: 3})").await.unwrap();
            db.execute("INSERT (:P {_id: 4})").await.unwrap();
            // Node 3's out-edge to node 4 lands BEFORE the flush — it will
            // be persisted, so it merges with node 3's LIVE out-edge added
            // after the flush below (Gap A: anchored-live-merge coverage).
            db.execute("MATCH (a:P {_id: 3}), (b:P {_id: 4}) INSERT (a)-[:KNOWS]->(b)")
                .await
                .unwrap();
            db.execute("MATCH (a:P {_id: 1}), (b:P {_id: 3}) INSERT (a)-[:KNOWS]->(b)")
                .await
                .unwrap();
            // Flush the three edges into the adj families, then add one more
            // LIVE edge so BOTH sources contribute to the merges below.
            force_flush(&db).await;
            db.execute("MATCH (a:P {_id: 3}), (b:P {_id: 2}) INSERT (a)-[:KNOWS]->(b)")
                .await
                .unwrap();
            let now = db.inner.clock.watermark();
            let bounds = TemporalBounds {
                valid: TemporalDimension::at(now),
                system: TemporalDimension::at(now),
            };
            let out = edge_adjacency(
                &db.inner.state,
                &db.inner.store,
                DEFAULT_GRAPH,
                "KNOWS",
                &[],
                AdjDirection::Out,
                Some(ada),
                &bounds,
                None,
                None,
            )
            .await
            .unwrap();
            assert!(!out.is_empty());
            // Ground truth: same answer from a full scan filtered to the anchor.
            let all = edge_adjacency(
                &db.inner.state,
                &db.inner.store,
                DEFAULT_GRAPH,
                "KNOWS",
                &[],
                AdjDirection::Out,
                None,
                &bounds,
                None,
                None,
            )
            .await
            .unwrap();
            let expected: Vec<_> = all.iter().filter(|e| e.node == ada).copied().collect();
            assert_eq!(out, expected);

            // Gap A: anchored `Out` at node 3 has a persisted (3→4) AND a
            // live (3→2) out-edge — the live contribution is non-empty and
            // must survive the anchor narrowing.
            let out_cyd = edge_adjacency(
                &db.inner.state,
                &db.inner.store,
                DEFAULT_GRAPH,
                "KNOWS",
                &[],
                AdjDirection::Out,
                Some(cyd),
                &bounds,
                None,
                None,
            )
            .await
            .unwrap();
            assert!(!out_cyd.is_empty());
            assert!(
                out_cyd.iter().any(|e| e.neighbor == bob),
                "anchored Out at node 3 must include the LIVE edge 3→2, got {out_cyd:?}"
            );
            let expected_cyd: Vec<_> = all.iter().filter(|e| e.node == cyd).copied().collect();
            assert_eq!(out_cyd, expected_cyd);

            // Gap B: `AdjDirection::In` was never functionally tested.
            // Node 2's in-edges are the dst-side mirror of node 3's
            // out-edges above: one persisted (1→2), one live (3→2).
            let in_bob = edge_adjacency(
                &db.inner.state,
                &db.inner.store,
                DEFAULT_GRAPH,
                "KNOWS",
                &[],
                AdjDirection::In,
                Some(bob),
                &bounds,
                None,
                None,
            )
            .await
            .unwrap();
            assert!(!in_bob.is_empty());
            let all_in = edge_adjacency(
                &db.inner.state,
                &db.inner.store,
                DEFAULT_GRAPH,
                "KNOWS",
                &[],
                AdjDirection::In,
                None,
                &bounds,
                None,
                None,
            )
            .await
            .unwrap();
            let expected_in: Vec<_> = all_in.into_iter().filter(|e| e.node == bob).collect();
            assert_eq!(in_bob, expected_in);
        }
        {
            // Restart: the persisted adj families recover from the manifest,
            // in lockstep with the edges primary inventory.
            let db = Db::local(dir.path()).await.unwrap();
            {
                let s = db.inner.state.read().unwrap();
                assert_eq!(
                    s.graph(DEFAULT_GRAPH).unwrap().adj_out.len(),
                    s.graph(DEFAULT_GRAPH).unwrap().edges.tries.len()
                );
                assert_eq!(
                    s.graph(DEFAULT_GRAPH).unwrap().adj_in.len(),
                    s.graph(DEFAULT_GRAPH).unwrap().edges.tries.len()
                );
                assert!(
                    !s.graph(DEFAULT_GRAPH).unwrap().edges.tries.is_empty(),
                    "at least one edges block flushed"
                );
            }

            // Gap C: the anchor==full-filtered contract must also hold
            // against RECOVERED data, not just live state. `ada`'s two
            // out-edges (1→2, 1→3) were both persisted in block 0, so this
            // exercises the recovered adj-out family specifically.
            let now = db.inner.clock.watermark();
            let bounds = TemporalBounds {
                valid: TemporalDimension::at(now),
                system: TemporalDimension::at(now),
            };
            let out = edge_adjacency(
                &db.inner.state,
                &db.inner.store,
                DEFAULT_GRAPH,
                "KNOWS",
                &[],
                AdjDirection::Out,
                Some(ada),
                &bounds,
                None,
                None,
            )
            .await
            .unwrap();
            assert!(
                !out.is_empty(),
                "recovered persisted out-edges for node 1 must be non-empty"
            );
            let all = edge_adjacency(
                &db.inner.state,
                &db.inner.store,
                DEFAULT_GRAPH,
                "KNOWS",
                &[],
                AdjDirection::Out,
                None,
                &bounds,
                None,
                None,
            )
            .await
            .unwrap();
            let expected: Vec<_> = all.into_iter().filter(|e| e.node == ada).collect();
            assert_eq!(out, expected);
        }
    }

    #[tokio::test]
    async fn recover_rejects_unknown_manifest_tables() {
        use bytes::Bytes;
        use varve_storage::{keys, memory_store, BlockManifest, TableTries};

        let store = memory_store();
        let manifest = BlockManifest {
            block_id: 0,
            watermark: 0,
            max_tx_id: 0,
            max_system_time_us: 0,
            tables: vec![TableTries {
                graph: "default".to_string(),
                // `edges` recovers now (slice 6); an invented table must fail.
                table: "widgets".to_string(),
                family: String::new(),
                tries: vec![],
            }],
        };
        store
            .put(&keys::manifest_key(0), Bytes::from(manifest.to_wire()))
            .await
            .unwrap();
        let log = MemoryLog::new();
        let clock = MonotonicClock::new();
        match recover(&log, &clock, &store).await {
            Err(EngineError::UnknownTable(t)) => assert!(t.contains("widgets"), "{t}"),
            other => panic!("expected UnknownTable, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn probe_keys_are_unique_even_at_the_same_clock_tick() {
        // Two probes never collide on key even if the clock regressed:
        // the key carries a process-unique nonce.
        let db = Db::memory();
        let a = db.probe_capabilities().await.unwrap();
        let b = db.probe_capabilities().await.unwrap();
        assert_ne!(a.probe_key, b.probe_key);
        assert!(a.probe_key.starts_with("v1/probe/"));
        assert!(a.probe_key.contains('-'), "key must carry the nonce suffix");
    }
}
