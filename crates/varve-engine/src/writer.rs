//! The writer loop — Varve's single serialization point (spec §3, D3). Every
//! mutating statement is resolved HERE, serially, so tx N always sees tx
//! N−1. Events are applied to the live index only AFTER their batch is
//! durable, and acks fire after apply: once a tx is acked its effects are
//! both durable and visible; queries never observe un-durable data.

use crate::clock::Clock;
use crate::db::{EngineError, TxReceipt};
use crate::scan::merged_snapshot;
use crate::state::{TableState, DEFAULT_GRAPH, NODES_TABLE};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use varve_gql::ast::{DeleteStmt, InsertStmt, Literal, Statement};
use varve_index::{encode_events, Event, Op};
use varve_log::{Log, LogRecord, TableEffects};
use varve_types::{Doc, Iid, Instant, LogPosition, TemporalBounds, TemporalDimension, Value};

/// Bounded submission queue (roadmap slice 3). Config-driven backpressure
/// semantics arrive in slice 10.
pub(crate) const SUBMISSION_QUEUE_LEN: usize = 256;

pub(crate) struct Submission {
    pub stmt: Statement,
    pub ack: oneshot::Sender<Result<TxReceipt, EngineError>>,
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
    pub state: Arc<RwLock<TableState>>,
    pub store: Arc<dyn varve_storage::ObjectStore>,
    pub clock: Arc<dyn Clock>,
    pub log: Arc<dyn Log>,
    pub next_tx_id: u64,
    /// Next L0 block id (recovered from the latest manifest in Task 11).
    pub next_block_id: u64,
    /// Exclusive end of the durably-appended log prefix — becomes the next
    /// flushed block's manifest watermark (decision 6).
    pub durable_watermark: LogPosition,
}

/// One staged-but-not-yet-durable transaction: its log record, the events it
/// will apply to the live index once durable, its receipt, and the caller's
/// ack channel.
struct Staged {
    record: LogRecord,
    events: Vec<Event>,
    receipt: TxReceipt,
    ack: oneshot::Sender<Result<TxReceipt, EngineError>>,
}

/// Distinguishes a real submission from a flush-timer wakeup in the writer
/// loop's `select!` below.
enum Received {
    Submission(Option<Submission>),
    FlushTimeout,
}

/// Spawns the writer loop on a dedicated task and returns the submission
/// channel `Db` sends statements through.
pub(crate) fn spawn_writer(mut state: WriterState, cfg: WriterConfig) -> mpsc::Sender<Submission> {
    let (sender, mut rx) = mpsc::channel::<Submission>(SUBMISSION_QUEUE_LEN);
    tokio::spawn(async move {
        // Armed (Some) while unflushed rows exist and a flush interval is
        // configured; disarmed after every flush and whenever the live
        // table is empty.
        let mut flush_deadline: Option<tokio::time::Instant> = None;
        loop {
            let received = match flush_deadline {
                Some(deadline) => tokio::select! {
                    sub = rx.recv() => Received::Submission(sub),
                    _ = tokio::time::sleep_until(deadline) => Received::FlushTimeout,
                },
                None => Received::Submission(rx.recv().await),
            };
            match received {
                Received::Submission(Some(first)) => {
                    run_batch(&mut state, &cfg, &mut rx, first).await;
                    if live_rows(&state) >= cfg.max_block_rows {
                        // A failed flush leaves the live table intact and
                        // retries at the next trigger (decision 10).
                        let _ = crate::flush::flush_block(&mut state).await;
                    }
                    flush_deadline = next_deadline(&state, &cfg, flush_deadline);
                }
                Received::Submission(None) => {
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
    sender
}

fn live_rows(state: &WriterState) -> usize {
    state
        .state
        .read()
        .map(|s| s.live.event_count())
        .unwrap_or(0)
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
    rx: &mut mpsc::Receiver<Submission>,
    first: Submission,
) {
    let deadline = tokio::time::Instant::now() + cfg.window;
    let mut staged: Vec<Staged> = Vec::new();
    let mut staged_bytes = 0usize;
    let mut pending = Some(first);
    loop {
        if let Some(sub) = pending.take() {
            // A reading statement must observe every earlier tx, and events
            // apply only after durability — so flush any staged batch first.
            if statement_reads(&sub.stmt) && !staged.is_empty() {
                flush(state, std::mem::take(&mut staged)).await;
                staged_bytes = 0;
            }
            match resolve(state, sub.stmt).await {
                Ok((record, events, receipt)) => {
                    staged_bytes += record.wire_len();
                    staged.push(Staged {
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
            Ok(Some(sub)) => pending = Some(sub),
            Ok(None) | Err(_) => break, // channel closed or window elapsed
        }
    }
    if !staged.is_empty() {
        flush(state, staged).await;
    }
}

fn statement_reads(stmt: &Statement) -> bool {
    matches!(stmt, Statement::Delete(_))
}

/// Assigns `(tx_id, system_time)` and resolves a statement into its effect
/// events and log record.
async fn resolve(
    state: &mut WriterState,
    stmt: Statement,
) -> Result<(LogRecord, Vec<Event>, TxReceipt), EngineError> {
    state.next_tx_id += 1;
    let tx_id = state.next_tx_id;
    let system = state.clock.next();

    let events = match &stmt {
        Statement::Insert(ins) => resolve_insert(ins, tx_id, system)?,
        Statement::Delete(del) => resolve_delete(state, del, system).await?,
        Statement::Query(_) => return Err(EngineError::NotAMutation),
    };

    let effects = if events.is_empty() {
        // e.g. a DELETE that matched nothing: still a real, durable tx.
        Vec::new()
    } else {
        vec![TableEffects {
            table: NODES_TABLE.to_string(),
            arrow_ipc: encode_events(&events)?,
        }]
    };
    let record = LogRecord {
        tx_id,
        system_time_us: system.as_micros(),
        user: String::new(),
        effects,
    };
    let receipt = TxReceipt {
        tx_id,
        system_time: system,
    };
    Ok((record, events, receipt))
}

fn literal_to_value(l: &Literal) -> Value {
    match l {
        Literal::Int(i) => Value::Int(*i),
        Literal::Float(f) => Value::Float(*f),
        Literal::Str(s) => Value::Str(s.clone()),
        Literal::Bool(b) => Value::Bool(*b),
        Literal::Null => Value::Null,
    }
}

fn resolve_insert(
    ins: &InsertStmt,
    tx_id: u64,
    system: Instant,
) -> Result<Vec<Event>, EngineError> {
    let valid_from = ins.valid_from.unwrap_or(system);
    let valid_to = ins.valid_to.unwrap_or(Instant::END_OF_TIME);
    if valid_from >= valid_to {
        return Err(EngineError::InvalidValidRange {
            from: valid_from,
            to: valid_to,
        });
    }
    // Build and validate EVERY node's (iid, labels, doc) triple — including
    // the fallible `id_bytes()?` — before returning, so a later node's
    // invalid `_id` can't leave earlier nodes committed (slice-1 review fix,
    // pinned by `multi_node_insert_is_atomic_on_invalid_id`).
    let mut events = Vec::with_capacity(ins.nodes.len());
    for (ordinal, node) in ins.nodes.iter().enumerate() {
        let mut doc: Doc = node
            .props
            .iter()
            .map(|(k, v)| (k.clone(), literal_to_value(v)))
            .collect();
        let id = match doc.get("_id") {
            Some(v) => v.clone(),
            None => {
                // Durable generated id: (tx_id, ordinal) replaces slice-1's
                // process-local counter.
                let v = Value::Str(format!("varve:gen:{tx_id}:{ordinal}"));
                doc.insert("_id".into(), v.clone());
                v
            }
        };
        let iid = Iid::derive(DEFAULT_GRAPH, NODES_TABLE, &id.id_bytes()?);
        events.push(Event {
            iid,
            system_from: system,
            valid_from,
            valid_to,
            src: None,
            dst: None,
            op: Op::Put {
                labels: node.labels.clone(),
                doc,
            },
        });
    }
    Ok(events)
}

/// MATCH … DELETE (spec §10 DML): resolves the read side against the merged
/// live∪persisted snapshot at (valid=now, system=now) — a delete must find
/// flushed entities too (slice-4 plan, decision 13).
async fn resolve_delete(
    state: &WriterState,
    del: &DeleteStmt,
    system: Instant,
) -> Result<Vec<Event>, EngineError> {
    let bounds = TemporalBounds {
        valid: TemporalDimension::at(system),
        system: TemporalDimension::at(system),
    };
    let label = del.pattern.label.as_deref().unwrap_or("");
    let iid = varve_plan::iid_point(&del.where_clause, DEFAULT_GRAPH, NODES_TABLE);
    let snapshot = merged_snapshot(&state.state, &state.store, label, &bounds, iid).await?;
    let iids = varve_plan::iids_from_snapshot(snapshot, &del.where_clause).await?;
    Ok(iids
        .into_iter()
        .map(|iid| Event {
            iid,
            system_from: system,
            valid_from: system,
            valid_to: Instant::END_OF_TIME,
            src: None,
            dst: None,
            op: Op::Delete,
        })
        .collect())
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
        for event in std::mem::take(&mut s.events) {
            // OutOfOrderEvent cannot happen here: the writer loop is the only
            // caller of `append`, and `system` is assigned monotonically by
            // this same loop just before the event was built.
            shared.live.append(event).map_err(|e| e.to_string())?;
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

    fn spawn(
        log: Arc<dyn Log>,
        cfg: WriterConfig,
    ) -> (mpsc::Sender<Submission>, Arc<RwLock<TableState>>) {
        let state = Arc::new(RwLock::new(TableState::new()));
        let writer_state = WriterState {
            state: Arc::clone(&state),
            store: varve_storage::memory_store(),
            clock: Arc::new(MonotonicClock::new()),
            log,
            next_tx_id: 0,
            next_block_id: 0,
            durable_watermark: LogPosition::ZERO,
        };
        (spawn_writer(writer_state, cfg), state)
    }

    /// try_send keeps submission order deterministic (mpsc is FIFO).
    fn submit(
        sender: &mpsc::Sender<Submission>,
        gql: &str,
    ) -> oneshot::Receiver<Result<TxReceipt, EngineError>> {
        let stmt = varve_gql::parse(gql).unwrap();
        let (ack, rx) = oneshot::channel();
        sender.try_send(Submission { stmt, ack }).unwrap();
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
        assert_eq!(state.read().unwrap().live.event_count(), 0);

        let second = submit(&sender, "INSERT (:P {_id: 2})");
        second.await.unwrap().unwrap();
        assert_eq!(state.read().unwrap().live.event_count(), 1);
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
