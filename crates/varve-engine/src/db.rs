use crate::clock::{Clock, MonotonicClock};
use crate::registries::Registries;
use crate::scan::merged_snapshot;
use crate::state::{TableState, DEFAULT_GRAPH, NODES_TABLE};
use crate::writer::{spawn_writer, Submission, WriterConfig, WriterState};
use datafusion::arrow::record_batch::RecordBatch;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use varve_config::{Config, ConfigError, ConfigSection, RegistryError};
use varve_gql::ast::Statement;
use varve_gql::token::GqlError;
use varve_index::{decode_events, IndexError, LiveTable};
use varve_log::{LocalLog, Log, LogError, MemoryLog, DEFAULT_SEGMENT_MAX_BYTES};
use varve_plan::PlanError;
use varve_storage::{memory_store, CachedStore, MemoryCache, ObjectStore, StorageError};
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

#[derive(Debug, Clone, Copy)]
pub struct TxReceipt {
    pub tx_id: u64,
    pub system_time: Instant,
}

/// Default in-memory cache budget until `[cache]` wiring lands (Task 11).
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
        let live = LiveTable::new();
        Db::assemble(
            live,
            Arc::new(MemoryLog::new()),
            cached(memory_store()),
            Arc::new(MonotonicClock::new()),
            WriterConfig {
                window: Duration::ZERO,
                ..WriterConfig::default()
            },
            0,
        )
    }

    fn assemble(
        live: LiveTable,
        log: Arc<dyn varve_log::Log>,
        store: Arc<dyn ObjectStore>,
        clock: Arc<dyn Clock>,
        cfg: WriterConfig,
        next_tx_id: u64,
    ) -> Db {
        let mut table_state = TableState::new();
        table_state.live = live;
        let state = Arc::new(RwLock::new(table_state));
        let writer_state = WriterState {
            state: Arc::clone(&state),
            store: Arc::clone(&store),
            clock: Arc::clone(&clock),
            log,
            next_tx_id,
        };
        let submit = spawn_writer(writer_state, cfg);
        Db {
            state,
            store,
            clock,
            submit,
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
        let log_section = config.section("log").unwrap_or_else(ConfigSection::empty);
        let log = registries
            .log
            .build(log_section.backend().unwrap_or("memory"), &log_section)?;
        let clock_section = config.section("clock").unwrap_or_else(ConfigSection::empty);
        let clock = registries
            .clock
            .build(clock_section.backend().unwrap_or("system"), &clock_section)?;
        let tuning: LogTuning = log_section.get()?;
        let cfg = WriterConfig {
            window: Duration::from_millis(tuning.group_commit_window_ms),
            max_bytes: tuning.group_commit_max_bytes,
        };
        let (live, next_tx_id) = replay(log.as_ref(), clock.as_ref()).await?;
        // storage config selection + manifest recovery land in Task 11 —
        // nothing writes to storage before Task 10's flush.
        let store = cached(memory_store());
        Ok(Self::assemble(live, log, store, clock, cfg, next_tx_id))
    }

    /// Local-filesystem database at `dir` with default tuning (spec §11
    /// `Db::local(path)` convenience — no config file needed).
    pub async fn local(dir: impl AsRef<Path>) -> Result<Db, EngineError> {
        let log: Arc<dyn Log> = Arc::new(LocalLog::open(dir.as_ref(), DEFAULT_SEGMENT_MAX_BYTES)?);
        let clock: Arc<dyn Clock> = Arc::new(MonotonicClock::new());
        let (live, next_tx_id) = replay(log.as_ref(), clock.as_ref()).await?;
        // storage config selection + manifest recovery land in Task 11 —
        // nothing writes to storage before Task 10's flush.
        let store = cached(memory_store());
        Ok(Self::assemble(
            live,
            log,
            store,
            clock,
            WriterConfig::default(),
            next_tx_id,
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
        let label = q.pattern.label.as_deref().unwrap_or("");
        let iid = varve_plan::iid_point(&q.where_clause, DEFAULT_GRAPH, NODES_TABLE);
        let snapshot = merged_snapshot(&self.state, &self.store, label, &bounds, iid).await?;
        Ok(varve_plan::execute_query(&q, snapshot).await?)
    }
}

/// Spec §6 recovery: fold the whole log into a fresh live index; floor the
/// clock and tx counter above everything replayed. Blocks + manifest
/// watermarks (replay-from-position) arrive in slice 4.
async fn replay(
    log: &dyn varve_log::Log,
    clock: &dyn Clock,
) -> Result<(LiveTable, u64), EngineError> {
    let mut live = LiveTable::new();
    let mut next_tx_id = 0u64;
    let mut max_system: Option<Instant> = None;
    for (_position, record) in log.tail(LogPosition::ZERO).await? {
        for effect in &record.effects {
            if effect.table != NODES_TABLE {
                return Err(EngineError::UnknownTable(effect.table.clone()));
            }
            for event in decode_events(&effect.arrow_ipc)? {
                live.append(event)?;
            }
        }
        next_tx_id = next_tx_id.max(record.tx_id);
        let system = Instant::from_micros(record.system_time_us);
        max_system = Some(max_system.map_or(system, |m| m.max(system)));
    }
    if let Some(floor) = max_system {
        clock.advance_to(floor);
    }
    Ok((live, next_tx_id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use varve_log::{LogRecord, TableEffects};

    /// `replay` checks `effect.table` before ever touching `arrow_ipc` (spec
    /// v1: nodes is the only effect batch target), so a non-`nodes` table
    /// must hard-fail with `UnknownTable` — even though the log record here
    /// carries deliberately undecodable bytes that would blow up
    /// `decode_events` if the guard were ever removed or reordered.
    #[tokio::test]
    async fn replay_rejects_unknown_table() {
        let log = MemoryLog::new();
        log.append(vec![LogRecord {
            tx_id: 1,
            system_time_us: 0,
            user: String::new(),
            effects: vec![TableEffects {
                table: "edges".to_string(),
                arrow_ipc: vec![0xAA], // never decoded: table check happens first
            }],
        }])
        .await
        .unwrap();

        let clock = MonotonicClock::new();
        match replay(&log, &clock).await {
            Err(EngineError::UnknownTable(t)) => assert_eq!(t, "edges"),
            Err(other) => panic!("expected UnknownTable, got {other:?}"),
            Ok(_) => panic!("expected replay to fail on the non-nodes table"),
        }
    }
}
