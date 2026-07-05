use crate::clock::{Clock, MonotonicClock};
use crate::registries::Registries;
use crate::scan::merged_snapshot;
use crate::state::{PersistedTrie, TableState, DEFAULT_GRAPH, NODES_TABLE};
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
    #[error(
        "[log] backend \"local\" with [storage] backend \"memory\" would lose \
         flushed blocks on restart while trimming the durable log; set \
         [storage] backend = \"local\" (with [storage.local] dir) or use a \
         memory log"
    )]
    VolatileBlockStore,
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

/// `[cache]` tuning (decision 14: integer bytes, like group_commit_max_bytes).
#[derive(serde::Deserialize)]
struct CacheTuning {
    #[serde(default = "default_cache_memory_max_bytes")]
    memory_max_bytes: usize,
}

fn default_cache_memory_max_bytes() -> usize {
    512 * 1024 * 1024
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
        let log_backend = log_section.backend().unwrap_or("memory").to_string();
        let log = registries.log.build(&log_backend, &log_section)?;

        let storage_section = config
            .section("storage")
            .unwrap_or_else(ConfigSection::empty);
        let storage_backend = storage_section.backend().unwrap_or("memory").to_string();
        // Decision 11: flushing trims the durable log; blocks must be at
        // least as durable as the log they replace.
        if log_backend == "local" && storage_backend == "memory" {
            return Err(EngineError::VolatileBlockStore);
        }
        let backend = registries
            .storage
            .build(&storage_backend, &storage_section)?;
        let cache_tuning: CacheTuning = config
            .section("cache")
            .unwrap_or_else(ConfigSection::empty)
            .get()?;
        let store: Arc<dyn ObjectStore> = Arc::new(CachedStore::new(
            backend,
            Arc::new(MemoryCache::new(cache_tuning.memory_max_bytes)),
        ));

        let clock_section = config.section("clock").unwrap_or_else(ConfigSection::empty);
        let clock = registries
            .clock
            .build(clock_section.backend().unwrap_or("system"), &clock_section)?;

        let log_tuning: LogTuning = log_section.get()?;
        let storage_tuning: StorageTuning = storage_section.get()?;
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
    let mut tries = Vec::new();
    let (mut next_tx_id, next_block_id, mut watermark, mut max_system) = match &manifest {
        Some(m) => {
            for table in &m.tables {
                if table.graph != DEFAULT_GRAPH || table.table != NODES_TABLE {
                    return Err(EngineError::UnknownTable(format!(
                        "{}/{}",
                        table.graph, table.table
                    )));
                }
                for entry in &table.tries {
                    let meta = store
                        .get(&varve_storage::keys::meta_key(
                            &table.graph,
                            &table.table,
                            &entry.trie_key,
                        ))
                        .await?;
                    tries.push(PersistedTrie {
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

    let mut live = LiveTable::new();
    for (position, record) in log.tail(watermark).await? {
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
        watermark = watermark.max(position.advance(1)?);
    }
    if let Some(floor) = max_system {
        clock.advance_to(floor);
    }
    Ok(Recovered {
        state: TableState { live, tries },
        next_tx_id,
        next_block_id,
        watermark,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use varve_log::{LogRecord, TableEffects};

    /// `recover`'s log-tail loop checks `effect.table` before ever touching
    /// `arrow_ipc` (spec v1: nodes is the only effect batch target), so a
    /// non-`nodes` table must hard-fail with `UnknownTable` — even though the
    /// log record here carries deliberately undecodable bytes that would
    /// blow up `decode_events` if the guard were ever removed or reordered.
    #[tokio::test]
    async fn recover_rejects_unknown_table_in_the_log_tail() {
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
        let store = memory_store();
        match recover(&log, &clock, &store).await {
            Err(EngineError::UnknownTable(t)) => assert_eq!(t, "edges"),
            Err(other) => panic!("expected UnknownTable, got {other:?}"),
            Ok(_) => panic!("expected recover to fail on the non-nodes table"),
        }
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
            recovered.state.live.event_count(),
            1,
            "only the post-watermark record replays"
        );
        assert_eq!(recovered.state.tries.len(), 1);
        assert_eq!(recovered.next_tx_id, 3);
        assert_eq!(recovered.next_block_id, 1);
        assert_eq!(recovered.watermark.as_u64(), 3);
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
                table: "edges".to_string(), // slice 6 format — must hard-fail
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
            Err(EngineError::UnknownTable(t)) => assert!(t.contains("edges"), "{t}"),
            other => panic!("expected UnknownTable, got {other:?}"),
        }
    }
}
