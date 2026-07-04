use crate::clock::{Clock, MonotonicClock};
use crate::writer::{spawn_writer, Submission, WriterConfig, WriterState};
use datafusion::arrow::record_batch::RecordBatch;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use varve_gql::ast::Statement;
use varve_gql::token::GqlError;
use varve_index::{IndexError, LiveTable};
use varve_log::{LogError, MemoryLog};
use varve_plan::PlanError;
use varve_types::{Instant, TypeError};

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
}

#[derive(Debug, Clone, Copy)]
pub struct TxReceipt {
    pub tx_id: u64,
    pub system_time: Instant,
}

/// Embedded, in-process database handle. All mutations flow through the
/// writer loop (spec §3, D3): submissions are resolved serially, group-
/// committed to the log, applied to the live index after durability, then
/// acked — so concurrent `execute()` calls are fully supported, and an
/// acked transaction is both durable and visible.
pub struct Db {
    live: Arc<RwLock<LiveTable>>,
    clock: Arc<dyn Clock>,
    submit: mpsc::Sender<Submission>,
}

impl Db {
    /// Volatile database: memory log, zero group-commit window (there is no
    /// fsync to amortize — decision 11). Requires a Tokio runtime.
    pub fn memory() -> Db {
        let live = LiveTable::new();
        Db::assemble(
            live,
            Arc::new(MemoryLog::new()),
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
        clock: Arc<dyn Clock>,
        cfg: WriterConfig,
        next_tx_id: u64,
    ) -> Db {
        let live = Arc::new(RwLock::new(live));
        let state = WriterState {
            live: Arc::clone(&live),
            clock: Arc::clone(&clock),
            log,
            next_tx_id,
        };
        let submit = spawn_writer(state, cfg);
        Db {
            live,
            clock,
            submit,
        }
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
        // Snapshot under the read lock, drop the guard, then run DataFusion
        // on the owned batch — no await while holding the lock (slice-2
        // pattern).
        let snapshot = {
            let live = self.live.read().map_err(|_| EngineError::Poisoned)?;
            varve_plan::snapshot_for_query(&q, &live, now)?
        };
        Ok(varve_plan::execute_query(&q, snapshot).await?)
    }
}
