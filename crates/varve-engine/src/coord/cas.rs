//! `cas-failover` (spec §12, D5/D7): lease acquisition via CAS
//! (`put_if_absent`/`put_if_matches`), clock-skew-free staleness via ETag
//! double-observation, seizure with an epoch bump + fence write, and
//! heartbeat renewal. Opt-in and capability-probed at startup (spec §1, D7):
//! nothing here requires more than plain S3 PUT/GET/LIST — a backend that
//! cannot prove real conditional-write semantics refuses cleanly, naming the
//! capability, so `designated-writer` remains available everywhere.

use crate::clock::Clock;
use crate::coord::fence::{write_fence, FenceDoc};
use crate::coord::identity;
use crate::coord::{CoordTuning, Coordinator, LeaseState, WriterGrant};
use crate::db::{EngineError, WriterAdvertisement, WRITER_ADVERTISEMENT_KEY};
use async_trait::async_trait;
use bytes::Bytes;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use varve_config::{BuildContext, ComponentFactory, ConfigSection, RegistryError};
use varve_log::Log;
use varve_storage::{CondPut, ConditionalStore, ObjectStore, ProbeVerdict, PROBE_PREFIX};

pub(crate) const LEASE_KEY: &str = "v1/lease.json";

/// The lease document (spec §12): one PUT/GET target guarded by
/// `put_if_absent`/`put_if_matches`. `heartbeat_us` is informational only
/// (ops/diagnostics) — staleness is decided purely by ETag double-observation
/// (clock-skew-free, decision 2).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub(crate) struct LeaseDoc {
    pub holder: String,
    pub address: String,
    pub epoch: u16,
    pub heartbeat_us: i64,
}

/// The lease this instance currently holds: the ETag that must still match
/// for the next renewal to succeed, the granted epoch, and the local instant
/// at/after which the lease MUST be considered lost absent a fresh renewal.
struct HeldLease {
    etag: String,
    epoch: u16,
    valid_until: tokio::time::Instant,
}

pub(crate) struct CasFailover {
    store: Arc<dyn ObjectStore>,
    clock: Arc<dyn Clock>,
    node_id: String,
    heartbeat_interval: Duration,
    takeover_after: Duration,
    address: Mutex<Option<String>>,
    held: tokio::sync::Mutex<Option<HeldLease>>,
}

impl CasFailover {
    fn address_snapshot(&self) -> Option<String> {
        self.address.lock().ok().and_then(|guard| guard.clone())
    }

    fn address_or_empty(&self) -> String {
        self.address_snapshot().unwrap_or_default()
    }

    /// Publishes `v1/writer.json` (plain PUT — never the lease) with the
    /// given `epoch`. Shared by `advertise` and `heartbeat`; `acquire` never
    /// calls this — winning or seizing a lease is silent about the address
    /// until the writer role calls `advertise` (spec §12).
    async fn publish_writer(&self, address: &str, epoch: u16) -> Result<(), EngineError> {
        let advertisement = WriterAdvertisement {
            address: address.to_string(),
            node_id: self.node_id.clone(),
            epoch,
            heartbeat_us: self.clock.next().as_micros(),
        };
        let bytes = serde_json::to_vec(&advertisement)?;
        self.store
            .put(WRITER_ADVERTISEMENT_KEY, Bytes::from(bytes))
            .await?;
        Ok(())
    }

    fn conditional(&self) -> Result<&dyn ConditionalStore, EngineError> {
        self.store
            .conditional()
            .ok_or_else(|| EngineError::CasUnsupported {
                reason: "backend exposes no conditional-write API".to_string(),
            })
    }

    async fn hold(&self, etag: String, epoch: u16, valid_until: tokio::time::Instant) {
        let mut guard = self.held.lock().await;
        *guard = Some(HeldLease {
            etag,
            epoch,
            valid_until,
        });
    }
}

#[async_trait]
impl Coordinator for CasFailover {
    async fn acquire(&self, log: &Arc<dyn Log>) -> Result<WriterGrant, EngineError> {
        let probe_key = format!(
            "{PROBE_PREFIX}/{}-{}",
            self.clock.next().as_micros(),
            self.node_id
        );
        let report = varve_storage::probe_conditional_put(self.store.as_ref(), &probe_key).await?;
        match report.verdict {
            ProbeVerdict::Supported => {}
            ProbeVerdict::Unsupported { reason } => {
                return Err(EngineError::CasUnsupported {
                    reason: format!("probe verdict Unsupported: {reason}"),
                });
            }
            ProbeVerdict::Inconsistent { reason } => {
                return Err(EngineError::CasUnsupported {
                    reason: format!("probe verdict Inconsistent: {reason}"),
                });
            }
        }
        let cond = self.conditional()?;

        loop {
            match cond.get_versioned(LEASE_KEY).await? {
                None => {
                    let head = log.head().await?;
                    let doc = LeaseDoc {
                        holder: self.node_id.clone(),
                        address: self.address_or_empty(),
                        epoch: head.epoch(),
                        heartbeat_us: self.clock.next().as_micros(),
                    };
                    let bytes = Bytes::from(serde_json::to_vec(&doc)?);
                    match cond.put_if_absent(LEASE_KEY, bytes).await? {
                        CondPut::Stored { etag: Some(etag) } => {
                            self.hold(
                                etag,
                                head.epoch(),
                                tokio::time::Instant::now() + self.takeover_after,
                            )
                            .await;
                            return Ok(WriterGrant { epoch: None });
                        }
                        CondPut::Stored { etag: None } => {
                            return Err(EngineError::CasUnsupported {
                                reason:
                                    "PUT returns no ETag; conditional workflows are inexpressible"
                                        .to_string(),
                            });
                        }
                        CondPut::AlreadyExists | CondPut::PreconditionFailed => continue,
                        CondPut::Unsupported { reason } => {
                            return Err(EngineError::CasUnsupported { reason });
                        }
                    }
                }
                Some((bytes, etag)) => {
                    let doc: LeaseDoc = serde_json::from_slice(&bytes)?;
                    // Clock-skew-free staleness (decision 2): rather than
                    // trust any embedded timestamp, wait a full local
                    // takeover window and see whether the SAME version is
                    // still there. node_ids are per-instance, so
                    // `doc.holder == self.node_id` can never happen here —
                    // this instance never wrote this document.
                    tokio::time::sleep(self.takeover_after).await;
                    match cond.get_versioned(LEASE_KEY).await? {
                        Some((_, etag2)) if etag2 == etag => {
                            // Stale for a full window: seize.
                            let new_epoch = doc
                                .epoch
                                .checked_add(1)
                                .ok_or(EngineError::EpochExhausted)?;
                            let t0 = tokio::time::Instant::now();
                            let seize = LeaseDoc {
                                holder: self.node_id.clone(),
                                address: self.address_or_empty(),
                                epoch: new_epoch,
                                heartbeat_us: self.clock.next().as_micros(),
                            };
                            let seize_bytes = Bytes::from(serde_json::to_vec(&seize)?);
                            match cond.put_if_matches(LEASE_KEY, seize_bytes, &etag).await? {
                                CondPut::Stored {
                                    etag: Some(new_etag),
                                } => {
                                    // Fresh scan: this instance never
                                    // appended, so its log cursor is
                                    // unprimed. The fence lands on the log's
                                    // ACTUAL head, not the dead holder's
                                    // last-known epoch — the only offset a
                                    // zombie writer with a cached cursor
                                    // could still reach (decision 2).
                                    let head = log.head().await?;
                                    write_fence(
                                        self.store.as_ref(),
                                        &FenceDoc {
                                            epoch: head.epoch(),
                                            fence_offset: head.offset(),
                                            fenced_by: self.node_id.clone(),
                                            fenced_at_us: self.clock.next().as_micros(),
                                        },
                                    )
                                    .await?;
                                    self.hold(new_etag, new_epoch, t0 + self.takeover_after)
                                        .await;
                                    return Ok(WriterGrant {
                                        epoch: Some(new_epoch),
                                    });
                                }
                                CondPut::Stored { etag: None } => {
                                    return Err(EngineError::CasUnsupported {
                                        reason: "update returns no ETag; conditional workflows \
                                                 are inexpressible"
                                            .to_string(),
                                    });
                                }
                                CondPut::PreconditionFailed | CondPut::AlreadyExists => continue,
                                CondPut::Unsupported { reason } => {
                                    return Err(EngineError::CasUnsupported { reason });
                                }
                            }
                        }
                        _ => continue, // rotated etag or vanished lease: holder alive, or racing.
                    }
                }
            }
        }
    }

    async fn advertise(&self, address: &str) -> Result<(), EngineError> {
        let address = address.to_string();
        match self.address.lock() {
            Ok(mut guard) => *guard = Some(address.clone()),
            Err(_) => return Err(EngineError::Poisoned),
        }
        let epoch = self.held.lock().await.as_ref().map_or(0, |h| h.epoch);
        self.publish_writer(&address, epoch).await
    }

    async fn heartbeat(&self) -> LeaseState {
        let mut held_guard = self.held.lock().await;
        let Some(held) = held_guard.as_ref() else {
            return LeaseState::Lost("no lease held".to_string());
        };
        let etag = held.etag.clone();
        let epoch = held.epoch;
        let prior_valid_until = held.valid_until;

        let cond = match self.conditional() {
            Ok(cond) => cond,
            Err(e) => return LeaseState::Lost(e.to_string()),
        };
        let doc = LeaseDoc {
            holder: self.node_id.clone(),
            address: self.address_or_empty(),
            epoch,
            heartbeat_us: self.clock.next().as_micros(),
        };
        let bytes = match serde_json::to_vec(&doc) {
            Ok(bytes) => Bytes::from(bytes),
            Err(e) => return LeaseState::Lost(format!("lease renewal failed: {e}")),
        };

        let t0 = tokio::time::Instant::now();
        match cond.put_if_matches(LEASE_KEY, bytes, &etag).await {
            Ok(CondPut::Stored {
                etag: Some(new_etag),
            }) => {
                let valid_until = t0 + self.takeover_after;
                if let Some(h) = held_guard.as_mut() {
                    h.etag = new_etag;
                    h.valid_until = valid_until;
                }
                drop(held_guard);
                if let Some(address) = self.address_snapshot() {
                    if let Err(err) = self.publish_writer(&address, epoch).await {
                        tracing::warn!(
                            error = %err,
                            "cas-failover heartbeat writer.json PUT failed; lease renewal unaffected"
                        );
                    }
                }
                LeaseState::ValidUntil(valid_until)
            }
            Ok(CondPut::Stored { etag: None }) => {
                *held_guard = None;
                LeaseState::Lost(
                    "update returns no ETag; conditional workflows are inexpressible".to_string(),
                )
            }
            Ok(CondPut::PreconditionFailed) | Ok(CondPut::AlreadyExists) => {
                *held_guard = None;
                LeaseState::Lost("lease seized by another writer".to_string())
            }
            Ok(CondPut::Unsupported { reason }) => LeaseState::Lost(reason),
            Err(e) => {
                drop(held_guard);
                if tokio::time::Instant::now() < prior_valid_until {
                    LeaseState::ValidUntil(prior_valid_until)
                } else {
                    LeaseState::Lost(format!(
                        "lease renewal failed past the takeover window: {e}"
                    ))
                }
            }
        }
    }

    fn heartbeat_interval(&self) -> Duration {
        self.heartbeat_interval
    }
}

pub(crate) struct CasFailoverFactory;

impl ComponentFactory<dyn Coordinator> for CasFailoverFactory {
    fn name(&self) -> &'static str {
        "cas-failover"
    }

    fn build(
        &self,
        cfg: &ConfigSection,
        ctx: &BuildContext,
    ) -> Result<Arc<dyn Coordinator>, RegistryError> {
        let result = (|| -> Result<CasFailover, Box<dyn std::error::Error + Send + Sync>> {
            let store = ctx.get::<Arc<dyn ObjectStore>>().ok_or_else(|| {
                std::io::Error::other(
                    "cas-failover coordinator requires ObjectStore in BuildContext \
                     (open through Db::open)",
                )
            })?;
            let clock = ctx.get::<Arc<dyn Clock>>().ok_or_else(|| {
                std::io::Error::other(
                    "cas-failover coordinator requires Clock in BuildContext \
                     (open through Db::open)",
                )
            })?;
            let tuning: CoordTuning = cfg.get()?;
            let (heartbeat_interval, takeover_after) =
                tuning.validate().map_err(std::io::Error::other)?;
            Ok(CasFailover {
                store,
                clock,
                node_id: identity::generate_node_id(),
                heartbeat_interval,
                takeover_after,
                address: Mutex::new(None),
                held: tokio::sync::Mutex::new(None),
            })
        })();
        result
            .map(|writer| Arc::new(writer) as Arc<dyn Coordinator>)
            .map_err(|source| RegistryError::Build {
                kind: "coordinator",
                name: "cas-failover".into(),
                source,
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::EngineError;
    use std::ops::Range;
    use std::sync::atomic::{AtomicBool, Ordering};
    use varve_log::LogRecord;
    use varve_storage::StorageError;
    use varve_types::LogPosition;

    fn cas(store: &Arc<dyn ObjectStore>, node: &str, takeover_ms: u64) -> CasFailover {
        CasFailover {
            store: Arc::clone(store),
            clock: Arc::new(crate::clock::MonotonicClock::new()),
            node_id: node.to_string(),
            heartbeat_interval: Duration::from_millis(takeover_ms / 2),
            takeover_after: Duration::from_millis(takeover_ms),
            address: Mutex::new(None),
            held: tokio::sync::Mutex::new(None),
        }
    }

    fn record(tx_id: u64) -> LogRecord {
        LogRecord {
            tx_id,
            system_time_us: tx_id as i64,
            user: String::new(),
            effects: vec![],
        }
    }

    #[tokio::test]
    async fn first_acquire_creates_the_lease_and_continues_the_epoch() {
        let store = varve_storage::memory_store();
        let log: Arc<dyn Log> = Arc::new(varve_log::MemoryLog::new());
        let grant = cas(&store, "a", 200).acquire(&log).await.unwrap();
        assert!(grant.epoch.is_none());
        let doc: LeaseDoc = serde_json::from_slice(&store.get(LEASE_KEY).await.unwrap()).unwrap();
        assert_eq!(doc.holder, "a");
        assert_eq!(doc.epoch, 0);
    }

    #[tokio::test]
    async fn a_stale_lease_is_seized_with_an_epoch_bump_and_a_fence() {
        let store = varve_storage::memory_store();
        let log: Arc<dyn Log> = Arc::new(varve_log::MemoryLog::new());
        log.append(vec![record(1), record(2)]).await.unwrap(); // head (0,2)
        let a = cas(&store, "a", 200);
        a.acquire(&log).await.unwrap();
        // a never heartbeats again -> stale after 200 ms.

        let started = std::time::Instant::now();
        let grant = cas(&store, "b", 200).acquire(&log).await.unwrap();
        assert_eq!(grant.epoch, Some(1));
        assert!(started.elapsed() < Duration::from_secs(5));
        let fences = crate::coord::fence::load_fences(store.as_ref())
            .await
            .unwrap();
        assert!(!fences.is_live(LogPosition::new(0, 2).unwrap()));
        assert!(fences.is_live(LogPosition::new(0, 1).unwrap()));
    }

    #[tokio::test]
    async fn a_live_holder_keeps_the_standby_waiting_and_heartbeat_lost_after_seizure() {
        let store = varve_storage::memory_store();
        let log: Arc<dyn Log> = Arc::new(varve_log::MemoryLog::new());
        let a = Arc::new(cas(&store, "a", 300));
        a.acquire(&log).await.unwrap();

        // A heartbeats concurrently while B tries to acquire: B must not win
        // while heartbeats keep rotating the etag.
        let a_hb = Arc::clone(&a);
        let stop = Arc::new(AtomicBool::new(false));
        let stop_hb = Arc::clone(&stop);
        let hb = tokio::spawn(async move {
            while !stop_hb.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(100)).await;
                if stop_hb.load(Ordering::SeqCst) {
                    break;
                }
                a_hb.heartbeat().await;
            }
        });

        let b = cas(&store, "b", 300);
        let b_acquire = tokio::time::timeout(Duration::from_millis(450), b.acquire(&log));
        assert!(
            b_acquire.await.is_err(),
            "B must still be waiting while A heartbeats"
        );
        stop.store(true, Ordering::SeqCst);
        hb.await.unwrap();

        // A stops: B wins; A's next heartbeat is Lost.
        let grant = b.acquire(&log).await.unwrap();
        assert_eq!(grant.epoch, Some(1));
        assert!(matches!(a.heartbeat().await, LeaseState::Lost(_)));
    }

    /// A store whose `ObjectStore` impl never overrides `conditional()`
    /// (the `probe.rs` test pattern) — no conditional-write API at all.
    struct PlainStore(Arc<dyn ObjectStore>);

    #[async_trait::async_trait]
    impl ObjectStore for PlainStore {
        async fn put(&self, key: &str, bytes: Bytes) -> Result<(), StorageError> {
            self.0.put(key, bytes).await
        }
        async fn get(&self, key: &str) -> Result<Bytes, StorageError> {
            self.0.get(key).await
        }
        async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Bytes, StorageError> {
            self.0.get_range(key, range).await
        }
        async fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
            self.0.list(prefix).await
        }
        async fn delete(&self, key: &str) -> Result<(), StorageError> {
            self.0.delete(key).await
        }
    }

    #[tokio::test]
    async fn probe_failure_refuses_cas_naming_the_capability() {
        let store: Arc<dyn ObjectStore> = Arc::new(PlainStore(varve_storage::memory_store()));
        let log: Arc<dyn Log> = Arc::new(varve_log::MemoryLog::new());
        let err = cas(&store, "a", 200).acquire(&log).await.unwrap_err();
        match err {
            EngineError::CasUnsupported { reason } => {
                assert!(reason.contains("conditional"), "{reason}");
            }
            other => panic!("expected CasUnsupported, got {other}"),
        }
    }
}
