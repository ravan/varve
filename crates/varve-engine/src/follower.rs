use crate::db::EngineError;
use crate::node::ProgressState;
use crate::replay::{apply_decoded_log_record, decode_log_record};
use crate::state::GraphsState;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::sync::watch;
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

pub(crate) async fn apply_next_range(state: &mut FollowerState) -> Result<usize, EngineError> {
    let upper = state.cursor.advance(state.config.batch_records)?;
    let records = state.log.read_range(state.cursor, upper).await?;
    if records.is_empty() {
        if let Some(manifest) = latest_manifest(state.store.as_ref()).await? {
            let manifest_watermark = LogPosition::from_u64(manifest.watermark);
            if state.cursor < manifest_watermark {
                return Err(EngineError::LogGap {
                    expected: state.cursor,
                    actual: manifest_watermark,
                });
            }
        }
        return Ok(0);
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
        let decoded = decode_log_record(&record)?;
        let tx_id = decoded.tx_id;
        let next = position.advance(1)?;
        {
            let mut graphs = state.state.write().map_err(|_| EngineError::Poisoned)?;
            apply_decoded_log_record(&mut graphs, decoded)?;
        }
        state.cursor = next;
        state
            .progress
            .send_replace(ProgressState::running(tx_id, next));
        applied += 1;
    }
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
        let (progress, progress_rx) = watch::channel(ProgressState::running(0, LogPosition::ZERO));
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
        let (progress, progress_rx) = watch::channel(ProgressState::running(0, LogPosition::ZERO));
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
}
