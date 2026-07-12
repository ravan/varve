//! Coordination (spec §12): writer identity, fences, and the Coordinator
//! trait land across slice-10 tasks 1–7.

use crate::db::EngineError;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use varve_log::Log;

#[cfg(feature = "cas-failover")]
pub(crate) mod cas;
pub(crate) mod designated;
pub(crate) mod fence;

/// The writer-role startup gate's outcome (spec §12): `epoch` selects which
/// log epoch the writer loop must append at.
#[derive(Debug)]
pub struct WriterGrant {
    /// Epoch this writer must append at (`Log::start_epoch`); `None` =
    /// continue the log's recovered epoch.
    pub epoch: Option<u16>,
}

/// Published by the heartbeat task; the writer loop gates acks on it.
#[derive(Clone, Debug)]
pub enum LeaseState {
    /// designated-writer: nothing to lose.
    Unfenced,
    /// cas-failover: acks allowed while `tokio::time::Instant::now() < deadline`.
    ValidUntil(tokio::time::Instant),
    /// Terminal: the lease was seized or could not be renewed in time.
    Lost(String),
}

/// One pluggable coordination backend (spec §4, §12): the writer-role
/// startup gate plus the advertisement/lease heartbeat. Implementations
/// register in `Registries::coordinator` (spec §4 extension point) — engine
/// code never depends on a concrete backend.
#[async_trait::async_trait]
pub trait Coordinator: Send + Sync {
    /// Writer-role startup gate. Blocks (standby) until this node may write,
    /// or refuses with a diagnostic error. Runs BEFORE recovery.
    async fn acquire(&self, log: &Arc<dyn Log>) -> Result<WriterGrant, EngineError>;
    /// Records the advertised address and publishes v1/writer.json now.
    async fn advertise(&self, address: &str) -> Result<(), EngineError>;
    /// One heartbeat: refresh the advertisement (and lease, in cas mode).
    async fn heartbeat(&self) -> LeaseState;
    /// Duration::ZERO disables the heartbeat task.
    fn heartbeat_interval(&self) -> Duration;
}

fn default_heartbeat_interval_ms() -> u64 {
    5000
}

fn default_takeover_after_ms() -> u64 {
    15000
}

/// Shared tuning knobs (spec §12): read by BOTH coordinator factories
/// (`designated-writer` and `cas-failover`) from their `[coordinator]`
/// section.
#[derive(serde::Deserialize)]
pub(crate) struct CoordTuning {
    #[serde(default = "default_heartbeat_interval_ms")]
    pub heartbeat_interval_ms: u64,
    #[serde(default = "default_takeover_after_ms")]
    pub takeover_after_ms: u64,
}

impl CoordTuning {
    /// `heartbeat_interval_ms > 0` requires `takeover_after_ms >= 2 *
    /// heartbeat_interval_ms` (a takeover deadline shorter than two
    /// heartbeats would seize a still-live writer on ordinary jitter).
    /// `heartbeat_interval_ms == 0` (heartbeat task disabled) has no such
    /// constraint.
    pub(crate) fn validate(self) -> Result<(Duration, Duration), String> {
        if self.heartbeat_interval_ms > 0 && self.takeover_after_ms < 2 * self.heartbeat_interval_ms
        {
            return Err(format!(
                "takeover_after_ms ({}) must be at least 2x heartbeat_interval_ms ({})",
                self.takeover_after_ms, self.heartbeat_interval_ms
            ));
        }
        Ok((
            Duration::from_millis(self.heartbeat_interval_ms),
            Duration::from_millis(self.takeover_after_ms),
        ))
    }
}

/// Abort-on-drop handle for the background heartbeat task (the
/// `FollowerHandle` pattern, `follower.rs:27-37`). Not yet wired into any
/// production path in this slice — Task 6 owns `Db::open_with` holding one
/// of these for the lifetime of a writer-role node; exercised here by the
/// `spawn_heartbeat` unit tests ahead of that landing (mirrors
/// `coord::fence::write_fence`).
#[allow(dead_code)]
pub(crate) struct HeartbeatHandle {
    shutdown: watch::Sender<bool>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for HeartbeatHandle {
    fn drop(&mut self) {
        self.shutdown.send_replace(true);
        self.task.abort();
    }
}

/// Spawns the background heartbeat loop: a zero interval disables it
/// entirely (one `Unfenced` publish, then exit); otherwise it ticks every
/// `coordinator.heartbeat_interval()`, publishing each `heartbeat()` result
/// onto `lease`, and stops (leaving the terminal state published) as soon as
/// a heartbeat reports `LeaseState::Lost`.
#[allow(dead_code)]
pub(crate) fn spawn_heartbeat(
    coordinator: Arc<dyn Coordinator>,
    lease: watch::Sender<LeaseState>,
) -> HeartbeatHandle {
    let (shutdown, mut shutdown_rx) = watch::channel(false);
    let task = tokio::spawn(async move {
        let interval = coordinator.heartbeat_interval();
        if interval.is_zero() {
            lease.send_replace(LeaseState::Unfenced);
            return;
        }
        loop {
            if *shutdown_rx.borrow() {
                break;
            }
            tokio::select! {
                _ = tokio::time::sleep(interval) => {}
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        break;
                    }
                    continue;
                }
            }
            let state = coordinator.heartbeat().await;
            let lost = matches!(state, LeaseState::Lost(_));
            if let LeaseState::Lost(reason) = &state {
                tracing::error!(reason = %reason, "lease lost; writer role must stop");
            }
            lease.send_replace(state);
            if lost {
                break;
            }
        }
    });
    HeartbeatHandle { shutdown, task }
}

pub(crate) mod identity {
    use std::sync::atomic::{AtomicU64, Ordering};
    use xxhash_rust::xxh3::xxh3_128;

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    /// Process-unique 128-bit id as 32 lower-hex chars: xxh3_128 over
    /// (pid, unix-nanos, per-process counter). Not cryptographic — just
    /// collision-resistant enough that two writer instances (or two probe
    /// runs after a wall-clock regression) never share an id.
    pub(crate) fn generate_node_id() -> String {
        let pid = std::process::id() as u128;
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let count = COUNTER.fetch_add(1, Ordering::Relaxed) as u128;
        let mut buf = [0u8; 48];
        buf[..16].copy_from_slice(&pid.to_le_bytes());
        buf[16..32].copy_from_slice(&nanos.to_le_bytes());
        buf[32..].copy_from_slice(&count.to_le_bytes());
        format!("{:032x}", xxh3_128(&buf))
    }
}

#[cfg(test)]
mod tests {
    use super::identity::generate_node_id;
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn node_ids_are_32_hex_and_unique_per_call() {
        let a = generate_node_id();
        let b = generate_node_id();
        assert_eq!(a.len(), 32);
        assert!(a.chars().all(|c| c.is_ascii_hexdigit()));
        assert_ne!(a, b, "same-process calls must differ (counter)");
    }

    /// Test double: `interval` and `lose_after` (0 = never) drive the
    /// heartbeat loop's behavior; `calls` counts `heartbeat()` invocations.
    struct FakeCoordinator {
        interval: Duration,
        calls: AtomicUsize,
        lose_after: usize,
    }

    #[async_trait::async_trait]
    impl Coordinator for FakeCoordinator {
        async fn acquire(&self, _log: &Arc<dyn Log>) -> Result<WriterGrant, EngineError> {
            Ok(WriterGrant { epoch: None })
        }

        async fn advertise(&self, _address: &str) -> Result<(), EngineError> {
            Ok(())
        }

        async fn heartbeat(&self) -> LeaseState {
            let n = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            if self.lose_after != 0 && n >= self.lose_after {
                LeaseState::Lost("seized".into())
            } else {
                LeaseState::Unfenced
            }
        }

        fn heartbeat_interval(&self) -> Duration {
            self.interval
        }
    }

    #[tokio::test]
    async fn zero_interval_disables_the_heartbeat_task() {
        let coordinator = Arc::new(FakeCoordinator {
            interval: Duration::ZERO,
            calls: AtomicUsize::new(0),
            lose_after: 0,
        });
        // Distinct initial state so a spurious re-send of the same variant
        // is still observable via `changed()`.
        let (lease_tx, mut lease_rx) = watch::channel(LeaseState::Lost("init".into()));
        let handle = spawn_heartbeat(Arc::clone(&coordinator) as Arc<dyn Coordinator>, lease_tx);
        lease_rx.changed().await.unwrap();
        assert!(matches!(*lease_rx.borrow(), LeaseState::Unfenced));
        assert_eq!(
            coordinator.calls.load(Ordering::SeqCst),
            0,
            "a zero interval must never call heartbeat()"
        );
        drop(handle);
    }

    #[tokio::test]
    async fn heartbeat_task_stops_after_a_lost_lease_and_leaves_it_published() {
        let coordinator: Arc<dyn Coordinator> = Arc::new(FakeCoordinator {
            interval: Duration::from_millis(5),
            calls: AtomicUsize::new(0),
            lose_after: 1,
        });
        let (lease_tx, mut lease_rx) = watch::channel(LeaseState::Unfenced);
        let handle = spawn_heartbeat(coordinator, lease_tx);
        loop {
            lease_rx.changed().await.unwrap();
            if matches!(*lease_rx.borrow(), LeaseState::Lost(_)) {
                break;
            }
        }
        assert!(matches!(*lease_rx.borrow(), LeaseState::Lost(ref reason) if reason == "seized"));
        drop(handle);
    }

    #[tokio::test]
    async fn dropping_the_handle_aborts_further_heartbeats() {
        let coordinator = Arc::new(FakeCoordinator {
            interval: Duration::from_millis(10),
            calls: AtomicUsize::new(0),
            lose_after: 0,
        });
        let (lease_tx, mut lease_rx) = watch::channel(LeaseState::Lost("init".into()));
        let handle = spawn_heartbeat(Arc::clone(&coordinator) as Arc<dyn Coordinator>, lease_tx);
        lease_rx.changed().await.unwrap(); // at least one heartbeat landed
        drop(handle);
        let calls_at_drop = coordinator.calls.load(Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert_eq!(
            coordinator.calls.load(Ordering::SeqCst),
            calls_at_drop,
            "dropping the handle must abort the heartbeat loop"
        );
    }
}
