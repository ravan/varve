use crate::clock::{Clock, MonotonicClock};
use crate::registries::Registries;
use crate::scan::merged_snapshot;
use crate::state::{
    PersistedTrie, TableCore, TableKind, TableState, DEFAULT_GRAPH, EDGES_TABLE, NODES_TABLE,
};
use crate::writer::{spawn_writer, Submission, WriterConfig, WriterState};
use datafusion::arrow::record_batch::RecordBatch;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use varve_config::{BuildContext, Config, ConfigError, ConfigSection, RegistryError};
use varve_gql::ast::Statement;
use varve_gql::token::GqlError;
use varve_index::{decode_events, IndexError, LiveTable};
use varve_log::{LocalLog, Log, LogError, MemoryLog, DEFAULT_SEGMENT_MAX_BYTES};
use varve_plan::PlanError;
use varve_storage::{
    memory_store, CachedStore, MemoryCache, ObjectStore, ProbeReport, StorageError,
};
use varve_types::{Instant, LogPosition, TypeError};

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
    #[error(transparent)]
    Storage(#[from] StorageError),
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
    #[error("cannot DELETE {0} still-connected node(s); use DETACH DELETE")]
    StillConnected(usize),
}

/// Group-commit tuning read from `[log]` (spec §6): a batch flushes when its
/// window elapses OR its size reaches `group_commit_max_bytes`, whichever
/// comes first. Unknown keys in `[log]` (`backend`, the `local` subtable)
/// are ignored — this struct only pulls out the two tuning knobs it knows.
#[derive(serde::Deserialize)]
struct LogTuning {
    #[serde(default = "default_window_ms")]
    group_commit_window_ms: u64,
    #[serde(default = "default_max_bytes")]
    group_commit_max_bytes: usize,
}

fn default_window_ms() -> u64 {
    15
}

fn default_max_bytes() -> usize {
    8 * 1024 * 1024
}

/// Block-flush tuning read from `[storage]` (spec §9). Unknown keys
/// (`backend`, the `local` subtable) are ignored, as with `LogTuning`.
#[derive(serde::Deserialize)]
struct StorageTuning {
    #[serde(default = "default_max_block_rows")]
    max_block_rows: usize,
    #[serde(default = "default_flush_interval_ms")]
    flush_interval_ms: u64,
}

fn default_max_block_rows() -> usize {
    100_000
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
}

fn default_max_path_depth() -> u32 {
    10
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

#[derive(Debug, Clone, Copy)]
pub struct TxReceipt {
    pub tx_id: u64,
    pub system_time: Instant,
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
pub struct Db {
    state: Arc<RwLock<TableState>>,
    store: Arc<dyn ObjectStore>,
    clock: Arc<dyn Clock>,
    submit: mpsc::Sender<Submission>,
    /// Cap on quantified-hop expansion length (spec §10, `[query]
    /// max_path_depth`); an unbounded `*`/`{m,}` quantifier is lowered to this
    /// depth. Task 9 consumes it during expansion; Task 8 already validates
    /// `scan_specs` quantifier bounds against it.
    max_path_depth: u32,
}

fn cached(store: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
    Arc::new(CachedStore::new(
        store,
        Arc::new(MemoryCache::new(DEFAULT_CACHE_MEMORY_BYTES)),
    ))
}

impl std::fmt::Debug for Db {
    /// Opaque handle: internals (live index, clock, writer channel) carry no
    /// useful debug representation, so this only identifies the type — e.g.
    /// for `Result<Db, _>::unwrap_err()` in tests.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Db").finish_non_exhaustive()
    }
}

impl Db {
    /// Volatile database: memory log, zero group-commit window (there is no
    /// fsync to amortize — decision 11). Requires a Tokio runtime.
    pub fn memory() -> Db {
        Db::assemble(
            TableState::new(),
            Arc::new(MemoryLog::new()),
            cached(memory_store()),
            Arc::new(MonotonicClock::new()),
            WriterConfig {
                window: Duration::ZERO,
                ..WriterConfig::default()
            },
            0,
            0,
            LogPosition::ZERO,
            default_max_path_depth(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn assemble(
        table_state: TableState,
        log: Arc<dyn varve_log::Log>,
        store: Arc<dyn ObjectStore>,
        clock: Arc<dyn Clock>,
        cfg: WriterConfig,
        next_tx_id: u64,
        next_block_id: u64,
        durable_watermark: LogPosition,
        max_path_depth: u32,
    ) -> Db {
        let state = Arc::new(RwLock::new(table_state));
        let writer_state = WriterState {
            state: Arc::clone(&state),
            store: Arc::clone(&store),
            clock: Arc::clone(&clock),
            log,
            next_tx_id,
            next_block_id,
            durable_watermark,
        };
        let submit = spawn_writer(writer_state, cfg);
        Db {
            state,
            store,
            clock,
            submit,
            max_path_depth,
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
        for name in cache_config.tiers.iter().rev() {
            let tier = registries.cache.build(name, &cache_section, &ctx)?;
            store = Arc::new(CachedStore::new(store, tier));
        }

        let clock_section = config.section("clock").unwrap_or_else(ConfigSection::empty);
        let clock = registries.clock.build(
            clock_section.backend().unwrap_or("system"),
            &clock_section,
            &ctx,
        )?;

        let log_tuning: LogTuning = log_section.get()?;
        let storage_tuning: StorageTuning = storage_section.get()?;
        let query_section = config.section("query").unwrap_or_else(ConfigSection::empty);
        let query_tuning: QueryTuning = query_section.get()?;
        let cfg = WriterConfig {
            window: Duration::from_millis(log_tuning.group_commit_window_ms),
            max_bytes: log_tuning.group_commit_max_bytes,
            max_block_rows: storage_tuning.max_block_rows,
            flush_interval: Duration::from_millis(storage_tuning.flush_interval_ms),
        };
        let recovered = recover(log.as_ref(), clock.as_ref(), &store).await?;
        Ok(Self::assemble(
            recovered.state,
            log,
            store,
            clock,
            cfg,
            recovered.next_tx_id,
            recovered.next_block_id,
            recovered.watermark,
            query_tuning.max_path_depth,
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
        let store = cached(varve_storage::local_store(&dir.join("store"))?);
        let clock: Arc<dyn Clock> = Arc::new(MonotonicClock::new());
        let recovered = recover(log.as_ref(), clock.as_ref(), &store).await?;
        Ok(Self::assemble(
            recovered.state,
            log,
            store,
            clock,
            WriterConfig::default(),
            recovered.next_tx_id,
            recovered.next_block_id,
            recovered.watermark,
            default_max_path_depth(),
        ))
    }

    /// Executes a mutation statement (INSERT, MATCH … DELETE): parses here,
    /// resolves and commits inside the writer loop, and returns once the tx
    /// is durable AND visible.
    pub async fn execute(&self, gql: &str) -> Result<TxReceipt, EngineError> {
        let stmt = varve_gql::parse(gql)?;
        if matches!(stmt, Statement::Query(_)) {
            return Err(EngineError::NotAMutation);
        }
        let (ack, rx) = oneshot::channel();
        self.submit
            .send(Submission { stmt, ack })
            .await
            .map_err(|_| EngineError::WriterUnavailable)?;
        rx.await.map_err(|_| EngineError::WriterUnavailable)?
    }

    /// Executes a read query, returning Arrow batches.
    pub async fn query(&self, gql: &str) -> Result<Vec<RecordBatch>, EngineError> {
        let Statement::Query(q) = varve_gql::parse(gql)? else {
            return Err(EngineError::NotAQuery);
        };
        let now = self.clock.watermark();
        let bounds = varve_plan::effective_bounds(&q, now);
        // Lower the MATCH pattern to one scan spec per element (task 8): a
        // single-node MATCH is the zero-hop, zero-join case; multi-element
        // MATCH becomes a left-deep chain of hash joins in `execute_pattern`.
        // The query's temporal bounds flow into every element's snapshot.
        let specs = varve_plan::scan_specs(&q, DEFAULT_GRAPH, self.max_path_depth)?;
        let mut inputs = Vec::with_capacity(specs.len());
        for spec in &specs {
            let input = match &spec.kind {
                varve_plan::SpecKind::Node { label, iid_point } => varve_plan::ScanInput::Batch(
                    merged_snapshot(
                        &self.state,
                        &self.store,
                        TableKind::Nodes,
                        label.as_deref().unwrap_or(""),
                        &bounds,
                        *iid_point,
                    )
                    .await?,
                ),
                varve_plan::SpecKind::Edge { label, .. } => varve_plan::ScanInput::Batch(
                    merged_snapshot(
                        &self.state,
                        &self.store,
                        TableKind::Edges,
                        label,
                        &bounds,
                        None,
                    )
                    .await?,
                ),
                // Quantified hops (task 9): the placeholder adjacency input is
                // rejected by `execute_pattern` with `Unsupported` until then.
                varve_plan::SpecKind::Expand { .. } => {
                    varve_plan::ScanInput::Adjacency(Arc::new(varve_plan::EdgeAdjacency))
                }
            };
            inputs.push(input);
        }
        Ok(varve_plan::execute_pattern(&q, &specs, inputs).await?)
    }

    /// Report-only capability probe (spec §12, D5): classifies whether the
    /// store's conditional-PUT semantics actually hold, against a fresh key
    /// under `v1/probe/`. Slice-10's cas-failover coordinator gates on this
    /// verdict at startup; nothing in v1 changes behavior based on it.
    /// Burns one clock tick for key uniqueness (harmless: tx times only
    /// ever need to keep increasing).
    pub async fn probe_capabilities(&self) -> Result<ProbeReport, EngineError> {
        let key = format!(
            "{}/{}",
            varve_storage::PROBE_PREFIX,
            self.clock.next().as_micros()
        );
        Ok(varve_storage::probe_conditional_put(self.store.as_ref(), &key).await?)
    }
}

/// The result of [`recover`]: the reconstructed table state (live tail +
/// persisted-trie inventory) plus the floors the writer must resume above.
struct Recovered {
    state: TableState,
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
    let mut nodes_tries = Vec::new();
    let mut edges_tries = Vec::new();
    let mut adj_out_tries = Vec::new();
    let mut adj_in_tries = Vec::new();
    let (mut next_tx_id, next_block_id, mut watermark, mut max_system) = match &manifest {
        Some(m) => {
            for table in &m.tables {
                // v1 (table, family): default/nodes and default/edges primary
                // (family ""), plus the two edge adjacency families (spec §5.1,
                // slice 6). An empty family means the primary key namespace.
                let dest = match (
                    table.graph.as_str(),
                    table.table.as_str(),
                    table.family.as_str(),
                ) {
                    (DEFAULT_GRAPH, NODES_TABLE, "") => &mut nodes_tries,
                    (DEFAULT_GRAPH, EDGES_TABLE, "") => &mut edges_tries,
                    (DEFAULT_GRAPH, EDGES_TABLE, varve_storage::ADJ_OUT) => &mut adj_out_tries,
                    (DEFAULT_GRAPH, EDGES_TABLE, varve_storage::ADJ_IN) => &mut adj_in_tries,
                    _ => {
                        return Err(EngineError::UnknownTable(format!(
                            "{}/{}/{}",
                            table.graph, table.table, table.family
                        )))
                    }
                };
                for entry in &table.tries {
                    let meta_key = if table.family.is_empty() {
                        varve_storage::keys::meta_key(&table.graph, &table.table, &entry.trie_key)
                    } else {
                        varve_storage::keys::adj_meta_key(
                            &table.graph,
                            &table.table,
                            &table.family,
                            &entry.trie_key,
                        )
                    };
                    let meta = store.get(&meta_key).await?;
                    dest.push(PersistedTrie {
                        entry: entry.clone(),
                        pages: Arc::new(varve_index::block::decode_meta(&meta)?),
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

    let mut nodes_live = LiveTable::new();
    let mut edges_live = LiveTable::new();
    for (position, record) in log.tail(watermark).await? {
        for effect in &record.effects {
            // The table check precedes `decode_events` so an unknown table
            // hard-fails before we ever touch (possibly bad) effect bytes.
            let live = match effect.table.as_str() {
                NODES_TABLE => &mut nodes_live,
                EDGES_TABLE => &mut edges_live,
                _ => return Err(EngineError::UnknownTable(effect.table.clone())),
            };
            for event in decode_events(&effect.arrow_ipc)? {
                live.append(event)?;
            }
        }
        next_tx_id = next_tx_id.max(record.tx_id);
        let system = Instant::from_micros(record.system_time_us);
        max_system = Some(max_system.map_or(system, |m| m.max(system)));
        watermark = watermark.max(position.advance(1)?);
    }
    if let Some(floor) = max_system {
        clock.advance_to(floor);
    }
    Ok(Recovered {
        state: TableState {
            nodes: TableCore {
                live: nodes_live,
                tries: nodes_tries,
            },
            edges: TableCore {
                live: edges_live,
                tries: edges_tries,
            },
            adj_out: adj_out_tries,
            adj_in: adj_in_tries,
        },
        next_tx_id,
        next_block_id,
        watermark,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scan::{edge_adjacency, AdjDirection};
    use varve_log::{LogRecord, TableEffects};
    use varve_types::{Iid, TemporalBounds, TemporalDimension, Value};

    #[tokio::test]
    async fn insert_edge_with_inline_nodes_populates_edges_live() {
        let db = Db::memory();
        db.execute("INSERT (:Person {_id: 1, name: 'Ada'})-[:KNOWS {since: 2020}]->(:Person {_id: 2, name: 'Bob'})")
            .await
            .unwrap();
        let s = db.state.read().unwrap();
        assert_eq!(s.nodes.live.event_count(), 2);
        assert_eq!(s.edges.live.event_count(), 1);
        let ada = Iid::derive("default", "nodes", &Value::Int(1).id_bytes().unwrap());
        let out: Vec<_> = s.edges.live.out_edges(&ada).collect();
        assert_eq!(out.len(), 1);
    }

    #[tokio::test]
    async fn insert_edge_var_reuse_binds_within_statement() {
        let db = Db::memory();
        db.execute("INSERT (a:Person {_id: 1}), (a)-[:KNOWS]->(b:Person {_id: 2})")
            .await
            .unwrap();
        let s = db.state.read().unwrap();
        assert_eq!(s.nodes.live.event_count(), 2);
        assert_eq!(s.edges.live.event_count(), 1);
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
        let s = db.state.read().unwrap();
        assert_eq!(s.nodes.live.event_count(), 0);
        assert_eq!(s.edges.live.event_count(), 0);
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
        let s = db.state.read().unwrap();
        // 1 Ada × 2 Bobs = 2 edges.
        assert_eq!(s.edges.live.event_count(), 2);
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
        let s = db.state.read().unwrap();
        let user = Iid::derive("default", "edges", &Value::Int(7).id_bytes().unwrap());
        assert!(s.edges.live.events_for(&user).is_some());
        assert_eq!(s.edges.live.event_count(), 2);
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
                let s = db.state.read().unwrap();
                if !s.edges.tries.is_empty() && !s.nodes.tries.is_empty() {
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
        let s = db.state.read().unwrap();
        // Two edge events now exist for the edge (Put flushed + Delete live).
        assert_eq!(s.edges.live.event_count(), 1);
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
                let s = db.state.read().unwrap();
                assert_eq!(
                    s.edges.live.event_count(),
                    1,
                    "log replay must restore edges"
                );
            }
            force_flush(&db).await;
        }
        {
            let db = Db::local(dir.path()).await.unwrap();
            let s = db.state.read().unwrap();
            assert_eq!(s.edges.live.event_count(), 0, "flushed");
            assert_eq!(
                s.edges.tries.len(),
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
                    let s = db.state.read().unwrap();
                    if s.nodes.tries.len() == 1 && s.edges.tries.len() == 1 {
                        break;
                    }
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            {
                let s = db.state.read().unwrap();
                assert_eq!(s.nodes.tries.len(), 1, "block 0: nodes trie landed");
                assert_eq!(s.edges.tries.len(), 1, "block 0: edges trie landed");
                assert_eq!(
                    s.edges.live.event_count(),
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
                    let s = db.state.read().unwrap();
                    if s.nodes.tries.len() == 2 {
                        break;
                    }
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            assert_eq!(
                db.state.read().unwrap().nodes.tries.len(),
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
            let s = db.state.read().unwrap();
            assert_eq!(
                s.edges.tries.len(),
                1,
                "edges' block-0 trie must survive block 1's node-only manifest write"
            );
            assert_eq!(s.nodes.tries.len(), 2, "both node tries also recovered");
        }
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
            }],
        }])
        .await
        .unwrap();

        let clock = MonotonicClock::new();
        let store = memory_store();
        let recovered = recover(&log, &clock, &store).await.unwrap();
        assert_eq!(recovered.state.edges.live.event_count(), 1);
        assert_eq!(recovered.state.nodes.live.event_count(), 0);
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
            recovered.state.nodes.live.event_count(),
            1,
            "only the post-watermark record replays"
        );
        assert_eq!(recovered.state.nodes.tries.len(), 1);
        assert_eq!(recovered.next_tx_id, 3);
        assert_eq!(recovered.next_block_id, 1);
        assert_eq!(recovered.watermark.as_u64(), 3);
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
            let now = db.clock.watermark();
            let bounds = TemporalBounds {
                valid: TemporalDimension::at(now),
                system: TemporalDimension::at(now),
            };
            let out = edge_adjacency(
                &db.state,
                &db.store,
                "KNOWS",
                AdjDirection::Out,
                Some(ada),
                &bounds,
            )
            .await
            .unwrap();
            assert!(!out.is_empty());
            // Ground truth: same answer from a full scan filtered to the anchor.
            let all = edge_adjacency(
                &db.state,
                &db.store,
                "KNOWS",
                AdjDirection::Out,
                None,
                &bounds,
            )
            .await
            .unwrap();
            let expected: Vec<_> = all.iter().filter(|e| e.node == ada).copied().collect();
            assert_eq!(out, expected);

            // Gap A: anchored `Out` at node 3 has a persisted (3→4) AND a
            // live (3→2) out-edge — the live contribution is non-empty and
            // must survive the anchor narrowing.
            let out_cyd = edge_adjacency(
                &db.state,
                &db.store,
                "KNOWS",
                AdjDirection::Out,
                Some(cyd),
                &bounds,
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
                &db.state,
                &db.store,
                "KNOWS",
                AdjDirection::In,
                Some(bob),
                &bounds,
            )
            .await
            .unwrap();
            assert!(!in_bob.is_empty());
            let all_in = edge_adjacency(
                &db.state,
                &db.store,
                "KNOWS",
                AdjDirection::In,
                None,
                &bounds,
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
                let s = db.state.read().unwrap();
                assert_eq!(s.adj_out.len(), s.edges.tries.len());
                assert_eq!(s.adj_in.len(), s.edges.tries.len());
                assert!(
                    !s.edges.tries.is_empty(),
                    "at least one edges block flushed"
                );
            }

            // Gap C: the anchor==full-filtered contract must also hold
            // against RECOVERED data, not just live state. `ada`'s two
            // out-edges (1→2, 1→3) were both persisted in block 0, so this
            // exercises the recovered adj-out family specifically.
            let now = db.clock.watermark();
            let bounds = TemporalBounds {
                valid: TemporalDimension::at(now),
                system: TemporalDimension::at(now),
            };
            let out = edge_adjacency(
                &db.state,
                &db.store,
                "KNOWS",
                AdjDirection::Out,
                Some(ada),
                &bounds,
            )
            .await
            .unwrap();
            assert!(
                !out.is_empty(),
                "recovered persisted out-edges for node 1 must be non-empty"
            );
            let all = edge_adjacency(
                &db.state,
                &db.store,
                "KNOWS",
                AdjDirection::Out,
                None,
                &bounds,
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
}
