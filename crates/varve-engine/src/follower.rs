use crate::coord::fence::{load_fences, FenceMap};
use crate::db::EngineError;
use crate::node::ProgressState;
use crate::replay::{apply_decoded_log_record, decode_log_record};
use crate::state::GraphsState;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::sync::watch;
use tracing::Instrument;
use varve_log::Log;
use varve_storage::{latest_manifest, ObjectStore};
use varve_types::LogPosition;

pub(crate) struct FollowerState {
    pub state: Arc<RwLock<GraphsState>>,
    pub log: Arc<dyn Log>,
    pub store: Arc<dyn ObjectStore>,
    pub cursor: LogPosition,
    pub config: FollowerConfig,
    pub progress: watch::Sender<ProgressState>,
    /// Epoch fences (spec §12), refreshed from the store whenever a read
    /// comes back empty — see `apply_range_once`. Starts empty so a fresh
    /// follower behaves exactly like the unfenced case until its first
    /// empty poll.
    pub fences: FenceMap,
}

#[derive(Clone, Copy)]
pub(crate) struct FollowerConfig {
    pub poll_interval: Duration,
    pub batch_records: u64,
}

pub(crate) struct FollowerHandle {
    shutdown: watch::Sender<bool>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for FollowerHandle {
    fn drop(&mut self) {
        self.shutdown.send_replace(true);
        self.task.abort();
    }
}

/// Outcome of a single `read_range` + apply pass. `Jumped` means the read
/// was empty and the cursor sat behind a fence, so `apply_next_range` should
/// retry the read immediately (rather than waiting a full `poll_interval`)
/// before reporting progress to its caller.
enum RangeOutcome {
    Applied(usize),
    Jumped,
}

/// Publishes `progress` with `applied` set to `(tx_id, log_position)` and
/// `log_head` set to the Task 12 formula: `max(last read position + 1,
/// manifest watermark seen in the gap check)` — or, on a fence jump, the
/// jumped cursor. `follower_error` carries over unchanged.
fn publish(state: &FollowerState, tx_id: u64, log_position: LogPosition, log_head: LogPosition) {
    let follower_error = state.progress.borrow().follower_error.clone();
    state.progress.send_replace(ProgressState {
        applied: crate::node::AppliedProgress {
            tx_id,
            log_position,
        },
        log_head,
        follower_error,
    });
}

async fn apply_range_once(state: &mut FollowerState) -> Result<RangeOutcome, EngineError> {
    let upper = state.cursor.advance(state.config.batch_records)?;
    let records = state.log.read_range(state.cursor, upper).await?;
    if records.is_empty() {
        state.fences = load_fences(state.store.as_ref()).await?;
        if let Some(next) = state.fences.jump(state.cursor)? {
            state.cursor = next;
            // On a fence jump, log_head becomes the jumped cursor itself.
            let tx_id = state.progress.borrow().applied.tx_id;
            let applied_position = state.progress.borrow().applied.log_position;
            publish(state, tx_id, applied_position, next);
            return Ok(RangeOutcome::Jumped);
        }
        let mut log_head = state.cursor;
        if let Some(manifest) = latest_manifest(state.store.as_ref()).await? {
            let manifest_watermark = LogPosition::from_u64(manifest.watermark);
            if state.cursor < manifest_watermark {
                return Err(EngineError::LogGap {
                    expected: state.cursor,
                    actual: manifest_watermark,
                });
            }
            log_head = log_head.max(manifest_watermark);
        }
        let tx_id = state.progress.borrow().applied.tx_id;
        let applied_position = state.progress.borrow().applied.log_position;
        publish(state, tx_id, applied_position, log_head);
        return Ok(RangeOutcome::Applied(0));
    }
    if let Some((actual, _)) = records.first() {
        if *actual != state.cursor {
            return Err(EngineError::LogGap {
                expected: state.cursor,
                actual: *actual,
            });
        }
    }
    let mut applied = 0;
    for (position, record) in records {
        let next = position.advance(1)?;
        if !state.fences.is_live(position) {
            // Dead record: a fence reassigned this tx id to the successor
            // epoch's writer, so it must not be applied. Advance the cursor
            // past it and republish progress with the position moved but
            // the last-applied tx_id unchanged.
            state.cursor = next;
            let tx_id = state.progress.borrow().applied.tx_id;
            publish(state, tx_id, next, next);
            continue;
        }
        let decoded = decode_log_record(&record)?;
        let tx_id = decoded.tx_id;
        {
            let mut graphs = state.state.write().map_err(|_| EngineError::Poisoned)?;
            apply_decoded_log_record(&mut graphs, decoded)?;
        }
        state.cursor = next;
        publish(state, tx_id, next, next);
        applied += 1;
    }
    Ok(RangeOutcome::Applied(applied))
}

/// `varve.follower.apply` (Task 13, fields `from`, `applied`): created HERE,
/// inside `apply_next_range` itself, so it stays observable whether this is
/// called from `spawn_follower`'s loop or directly (as several existing
/// tests in this module do). `from` is known at entry; `applied` isn't known
/// until the (possibly retried) `apply_range_once` call(s) resolve, so it
/// starts `Empty` and is recorded just before returning.
pub(crate) async fn apply_next_range(state: &mut FollowerState) -> Result<usize, EngineError> {
    let from = state.cursor.as_u64();
    let span = tracing::info_span!(
        "varve.follower.apply",
        from,
        applied = tracing::field::Empty
    );
    apply_next_range_impl(state).instrument(span).await
}

async fn apply_next_range_impl(state: &mut FollowerState) -> Result<usize, EngineError> {
    let applied = match apply_range_once(state).await? {
        RangeOutcome::Applied(applied) => applied,
        RangeOutcome::Jumped => {
            // One immediate retry after an epoch jump so takeover catch-up
            // is not delayed by poll_interval. If the retry itself hits
            // another empty/jumped read, fall back to reporting 0 so the
            // normal poll-delay path resumes.
            match apply_range_once(state).await? {
                RangeOutcome::Applied(applied) => applied,
                RangeOutcome::Jumped => 0,
            }
        }
    };
    tracing::Span::current().record("applied", applied);
    Ok(applied)
}

fn poll_delay_after(applied: usize, interval: Duration) -> Option<Duration> {
    (applied == 0).then_some(interval)
}

pub(crate) fn spawn_follower(mut state: FollowerState) -> FollowerHandle {
    let (shutdown, mut shutdown_rx) = watch::channel(false);
    let task = tokio::spawn(async move {
        loop {
            if *shutdown_rx.borrow() {
                break;
            }
            match apply_next_range(&mut state).await {
                Ok(applied) => {
                    let Some(delay) = poll_delay_after(applied, state.config.poll_interval) else {
                        continue;
                    };
                    tokio::select! {
                        _ = tokio::time::sleep(delay) => {}
                        changed = shutdown_rx.changed() => {
                            if changed.is_err() || *shutdown_rx.borrow() {
                                break;
                            }
                        }
                    }
                }
                Err(error) => {
                    let mut progress = state.progress.borrow().clone();
                    if progress.follower_error.is_none() {
                        progress.follower_error = Some(error.to_string());
                        state.progress.send_replace(progress);
                    }
                    break;
                }
            }
        }
    });
    FollowerHandle { shutdown, task }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::collections::VecDeque;
    use std::future::pending;
    use std::sync::Mutex;
    use tokio::sync::oneshot;
    use varve_log::{LogError, LogRecord, TableEffects};
    use varve_storage::{keys, BlockManifest};

    struct PendingLog {
        entered: Mutex<Option<oneshot::Sender<()>>>,
        canceled: Mutex<Option<oneshot::Sender<()>>>,
    }

    struct CancellationProbe(Option<oneshot::Sender<()>>);

    type ReadResult = Result<Vec<(LogPosition, LogRecord)>, LogError>;

    struct ScriptedLog {
        reads: Mutex<VecDeque<ReadResult>>,
    }

    impl ScriptedLog {
        fn new(reads: Vec<ReadResult>) -> Arc<Self> {
            Arc::new(Self {
                reads: Mutex::new(reads.into()),
            })
        }
    }

    #[async_trait::async_trait]
    impl Log for ScriptedLog {
        async fn append(&self, _records: Vec<LogRecord>) -> Result<LogPosition, LogError> {
            unreachable!("follower never appends")
        }

        async fn read_range(
            &self,
            _from: LogPosition,
            _to: LogPosition,
        ) -> Result<Vec<(LogPosition, LogRecord)>, LogError> {
            self.reads
                .lock()
                .unwrap()
                .pop_front()
                .expect("scripted read")
        }

        async fn trim(&self, _up_to: LogPosition) -> Result<(), LogError> {
            unreachable!("follower never trims")
        }

        async fn head(&self) -> Result<LogPosition, LogError> {
            unreachable!("test double")
        }

        async fn start_epoch(&self, _epoch: u16) -> Result<(), LogError> {
            unreachable!("test double")
        }
    }

    fn record(tx_id: u64) -> LogRecord {
        LogRecord {
            tx_id,
            system_time_us: tx_id as i64,
            user: String::new(),
            effects: Vec::new(),
        }
    }

    fn follower_state(
        log: Arc<dyn Log>,
        store: Arc<dyn ObjectStore>,
    ) -> (FollowerState, watch::Receiver<ProgressState>) {
        let (progress, progress_rx) = watch::channel(ProgressState::running(
            0,
            LogPosition::ZERO,
            LogPosition::ZERO,
        ));
        (
            FollowerState {
                state: Arc::new(RwLock::new(GraphsState::new())),
                log,
                store,
                cursor: LogPosition::ZERO,
                config: FollowerConfig {
                    poll_interval: Duration::from_secs(60),
                    batch_records: 2,
                },
                progress,
                fences: FenceMap::default(),
            },
            progress_rx,
        )
    }

    impl Drop for CancellationProbe {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }

    #[async_trait::async_trait]
    impl Log for PendingLog {
        async fn append(&self, _records: Vec<LogRecord>) -> Result<LogPosition, LogError> {
            unreachable!("follower never appends")
        }

        async fn read_range(
            &self,
            _from: LogPosition,
            _to: LogPosition,
        ) -> Result<Vec<(LogPosition, LogRecord)>, LogError> {
            if let Some(sender) = self.entered.lock().unwrap().take() {
                let _ = sender.send(());
            }
            let _probe = CancellationProbe(self.canceled.lock().unwrap().take());
            pending().await
        }

        async fn trim(&self, _up_to: LogPosition) -> Result<(), LogError> {
            unreachable!("follower never trims")
        }

        async fn head(&self) -> Result<LogPosition, LogError> {
            unreachable!("test double")
        }

        async fn start_epoch(&self, _epoch: u16) -> Result<(), LogError> {
            unreachable!("test double")
        }
    }

    #[tokio::test]
    async fn final_handle_drop_cancels_an_in_flight_log_poll() {
        let (entered_tx, entered_rx) = oneshot::channel();
        let (canceled_tx, canceled_rx) = oneshot::channel();
        let log: Arc<dyn Log> = Arc::new(PendingLog {
            entered: Mutex::new(Some(entered_tx)),
            canceled: Mutex::new(Some(canceled_tx)),
        });
        let state = Arc::new(RwLock::new(GraphsState::new()));
        let (progress, progress_rx) = watch::channel(ProgressState::running(
            0,
            LogPosition::ZERO,
            LogPosition::ZERO,
        ));
        let handle = spawn_follower(FollowerState {
            state: Arc::clone(&state),
            log,
            store: varve_storage::memory_store(),
            cursor: LogPosition::ZERO,
            config: FollowerConfig {
                poll_interval: Duration::from_secs(60),
                batch_records: 1,
            },
            progress,
            fences: FenceMap::default(),
        });

        entered_rx.await.unwrap();
        drop(handle);
        tokio::time::timeout(Duration::from_millis(100), canceled_rx)
            .await
            .expect("dropping the final follower handle must cancel read_range")
            .unwrap();

        assert_eq!(progress_rx.borrow().applied.tx_id, 0);
        assert_eq!(
            state
                .read()
                .unwrap()
                .graph(crate::state::DEFAULT_GRAPH)
                .unwrap()
                .nodes
                .live
                .event_count(),
            0
        );
    }

    #[tokio::test]
    async fn terminal_log_error_is_published_before_follower_exits() {
        let log = ScriptedLog::new(vec![Err(LogError::Io(std::io::Error::other(
            "terminal read failure",
        )))]);
        let (state, mut progress) = follower_state(log, varve_storage::memory_store());
        let handle = spawn_follower(state);

        progress.changed().await.unwrap();
        assert_eq!(
            progress.borrow().follower_error.as_deref(),
            Some("log I/O error: terminal read failure")
        );
        drop(handle);
    }

    #[tokio::test]
    async fn failed_record_leaves_cursor_and_progress_unchanged() {
        let malformed = LogRecord {
            tx_id: 1,
            system_time_us: 1,
            user: String::new(),
            effects: vec![TableEffects {
                graph: "missing".into(),
                table: crate::state::NODES_TABLE.into(),
                arrow_ipc: varve_index::encode_events(&[]).unwrap(),
            }],
        };
        let log = ScriptedLog::new(vec![Ok(vec![(LogPosition::ZERO, malformed)])]);
        let (mut state, progress) = follower_state(log, varve_storage::memory_store());

        assert!(matches!(
            apply_next_range(&mut state).await,
            Err(EngineError::UnknownGraph(graph)) if graph == "missing"
        ));
        assert_eq!(state.cursor, LogPosition::ZERO);
        assert_eq!(progress.borrow().applied.log_position, LogPosition::ZERO);
        assert_eq!(progress.borrow().applied.tx_id, 0);
    }

    #[tokio::test]
    async fn nonempty_range_must_start_at_cursor() {
        let actual = LogPosition::from_u64(1);
        let log = ScriptedLog::new(vec![Ok(vec![(actual, record(1))])]);
        let (mut state, _progress) = follower_state(log, varve_storage::memory_store());

        assert!(matches!(
            apply_next_range(&mut state).await,
            Err(EngineError::LogGap { expected, actual: found })
                if expected == LogPosition::ZERO && found == actual
        ));
        assert_eq!(state.cursor, LogPosition::ZERO);
    }

    #[tokio::test]
    async fn empty_range_behind_manifest_watermark_is_a_gap() {
        let store = varve_storage::memory_store();
        let manifest = BlockManifest {
            block_id: 7,
            watermark: 3,
            max_tx_id: 3,
            max_system_time_us: 3,
            tables: Vec::new(),
        };
        store
            .put(
                &keys::manifest_key(manifest.block_id),
                Bytes::from(manifest.to_wire()),
            )
            .await
            .unwrap();
        let log = ScriptedLog::new(vec![Ok(Vec::new())]);
        let (mut state, _progress) = follower_state(log, store);

        assert!(matches!(
            apply_next_range(&mut state).await,
            Err(EngineError::LogGap { expected, actual })
                if expected == LogPosition::ZERO && actual == LogPosition::from_u64(3)
        ));
    }

    #[test]
    fn nonempty_batches_repoll_immediately_and_empty_batches_use_interval() {
        let interval = Duration::from_millis(37);
        assert_eq!(poll_delay_after(1, interval), None);
        assert_eq!(poll_delay_after(0, interval), Some(interval));
    }

    async fn put_fence(store: &Arc<dyn ObjectStore>, epoch: u16, fence_offset: u64) {
        store
            .put(
                &keys::epoch_fence_key(epoch),
                Bytes::from(
                    serde_json::json!({
                        "epoch": epoch, "fence_offset": fence_offset,
                        "fenced_by": "test", "fenced_at_us": 0
                    })
                    .to_string(),
                ),
            )
            .await
            .unwrap();
    }

    async fn load_fences_blocking(store: &Arc<dyn ObjectStore>) -> crate::coord::fence::FenceMap {
        crate::coord::fence::load_fences(store.as_ref())
            .await
            .unwrap()
    }

    async fn put_manifest_with_watermark(store: &Arc<dyn ObjectStore>, watermark: LogPosition) {
        let manifest = BlockManifest {
            block_id: 7,
            watermark: watermark.as_u64(),
            max_tx_id: 0,
            max_system_time_us: 0,
            tables: Vec::new(),
        };
        store
            .put(
                &keys::manifest_key(manifest.block_id),
                Bytes::from(manifest.to_wire()),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn dead_records_advance_the_cursor_without_applying() {
        let store = varve_storage::memory_store();
        put_fence(&store, 0, 1).await; // fence epoch 0 at offset 1
        let log = ScriptedLog::new(vec![Ok(vec![
            (LogPosition::ZERO, record(1)),
            (LogPosition::new(0, 1).unwrap(), record(2)),
        ])]);
        let (mut state, progress) = follower_state(log, store);
        state.fences = load_fences_blocking(&state.store).await;

        let applied = apply_next_range(&mut state).await.unwrap();
        assert_eq!(applied, 1, "only the live record applies");
        assert_eq!(state.cursor, LogPosition::new(0, 2).unwrap());
        assert_eq!(
            progress.borrow().applied.tx_id,
            1,
            "dead tx id 2 never published"
        );
        assert_eq!(
            progress.borrow().applied.log_position,
            LogPosition::new(0, 2).unwrap()
        );
    }

    #[tokio::test]
    async fn empty_read_at_a_fence_jumps_to_the_next_epoch_and_reads_it() {
        let store = varve_storage::memory_store();
        put_fence(&store, 0, 0).await; // whole epoch 0 is dead
        let log = ScriptedLog::new(vec![
            Ok(Vec::new()),                                         // epoch-0 read: empty
            Ok(vec![(LogPosition::new(1, 0).unwrap(), record(5))]), // post-jump read
        ]);
        let (mut state, progress) = follower_state(log, store);

        let applied = apply_next_range(&mut state).await.unwrap();
        assert_eq!(applied, 1, "jump retries the read immediately");
        assert_eq!(state.cursor, LogPosition::new(1, 1).unwrap());
        assert_eq!(progress.borrow().applied.tx_id, 5);
    }

    #[tokio::test]
    async fn manifest_watermark_in_a_later_epoch_is_not_a_gap_when_a_jump_is_pending() {
        let store = varve_storage::memory_store();
        put_fence(&store, 0, 0).await;
        put_manifest_with_watermark(&store, LogPosition::new(1, 0).unwrap()).await;
        let log = ScriptedLog::new(vec![Ok(Vec::new()), Ok(Vec::new())]);
        let (mut state, _progress) = follower_state(log, store);

        // jump lands the cursor exactly at the watermark — no LogGap
        assert_eq!(apply_next_range(&mut state).await.unwrap(), 0);
        assert_eq!(state.cursor, LogPosition::new(1, 0).unwrap());
    }
}
