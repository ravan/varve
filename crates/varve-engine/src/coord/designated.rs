//! `designated-writer` (spec §12): the default coordinator. No coordination
//! at all beyond a best-effort startup guard and periodic advertisement
//! heartbeats — a second writer accidentally started against the same store
//! is refused (not fenced) while the first writer's heartbeat looks fresh.

use crate::clock::Clock;
use crate::coord::identity;
use crate::coord::{CoordTuning, Coordinator, LeaseState, WriterGrant};
use crate::db::{
    read_writer_advertisement, EngineError, WriterAdvertisement, WRITER_ADVERTISEMENT_KEY,
};
use async_trait::async_trait;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use varve_config::{BuildContext, ComponentFactory, ConfigSection, RegistryError};
use varve_log::Log;
use varve_storage::ObjectStore;

pub(crate) struct DesignatedWriter {
    store: Arc<dyn ObjectStore>,
    clock: Arc<dyn Clock>,
    node_id: String,
    heartbeat_interval: Duration,
    takeover_after: Duration,
    address: Mutex<Option<String>>,
}

impl DesignatedWriter {
    /// Publishes `address` with a freshly minted `heartbeat_us`. Shared by
    /// `advertise` (first publish) and `heartbeat` (republish).
    async fn publish(&self, address: String) -> Result<(), EngineError> {
        let advertisement = WriterAdvertisement {
            address,
            node_id: self.node_id.clone(),
            epoch: 0,
            heartbeat_us: self.clock.next().as_micros(),
        };
        let bytes = serde_json::to_vec(&advertisement)?;
        self.store
            .put(WRITER_ADVERTISEMENT_KEY, bytes::Bytes::from(bytes))
            .await?;
        Ok(())
    }
}

#[async_trait]
impl Coordinator for DesignatedWriter {
    /// Best-effort second-writer guard (spec §12): refuses only when a
    /// FRESH advertisement from a DIFFERENT node_id exists. Never fences —
    /// designated-writer always continues the log's recovered epoch.
    async fn acquire(&self, _log: &Arc<dyn Log>) -> Result<WriterGrant, EngineError> {
        if let Some(advertisement) = read_writer_advertisement(self.store.as_ref()).await? {
            if advertisement.node_id != self.node_id && advertisement.heartbeat_us > 0 {
                let now_us = self.clock.next().as_micros();
                let age_us = (now_us - advertisement.heartbeat_us).max(0);
                let takeover_after_us = self.takeover_after.as_micros() as i64;
                if age_us < takeover_after_us {
                    return Err(EngineError::WriterActive {
                        address: advertisement.address,
                        age_ms: (age_us / 1000) as u64,
                        takeover_after_ms: self.takeover_after.as_millis() as u64,
                    });
                }
            }
        }
        Ok(WriterGrant { epoch: None })
    }

    async fn advertise(&self, address: &str) -> Result<(), EngineError> {
        let address = address.to_string();
        match self.address.lock() {
            Ok(mut guard) => *guard = Some(address.clone()),
            Err(_) => return Err(EngineError::Poisoned),
        }
        self.publish(address).await
    }

    /// Best-effort by design (spec §12): a PUT failure is not fatal — it is
    /// logged (`tracing::warn!`) and `Unfenced` is returned either way, since
    /// designated-writer never fences anything.
    async fn heartbeat(&self) -> LeaseState {
        let address = match self.address.lock() {
            Ok(guard) => guard.clone(),
            Err(_) => None,
        };
        if let Some(address) = address {
            if let Err(err) = self.publish(address).await {
                tracing::warn!(error = %err, "designated-writer heartbeat PUT failed; continuing unfenced");
            }
        }
        LeaseState::Unfenced
    }

    fn heartbeat_interval(&self) -> Duration {
        self.heartbeat_interval
    }
}

pub(crate) struct DesignatedWriterFactory;

impl ComponentFactory<dyn Coordinator> for DesignatedWriterFactory {
    fn name(&self) -> &'static str {
        "designated-writer"
    }

    fn build(
        &self,
        cfg: &ConfigSection,
        ctx: &BuildContext,
    ) -> Result<Arc<dyn Coordinator>, RegistryError> {
        let result = (|| -> Result<DesignatedWriter, Box<dyn std::error::Error + Send + Sync>> {
            let store = ctx.get::<Arc<dyn ObjectStore>>().ok_or_else(|| {
                std::io::Error::other(
                    "designated-writer coordinator requires ObjectStore in BuildContext \
                     (open through Db::open)",
                )
            })?;
            let clock = ctx.get::<Arc<dyn Clock>>().ok_or_else(|| {
                std::io::Error::other(
                    "designated-writer coordinator requires Clock in BuildContext \
                     (open through Db::open)",
                )
            })?;
            let tuning: CoordTuning = cfg.get()?;
            let (heartbeat_interval, takeover_after) =
                tuning.validate().map_err(std::io::Error::other)?;
            Ok(DesignatedWriter {
                store,
                clock,
                node_id: identity::generate_node_id(),
                heartbeat_interval,
                takeover_after,
                address: Mutex::new(None),
            })
        })();
        result
            .map(|writer| Arc::new(writer) as Arc<dyn Coordinator>)
            .map_err(|source| RegistryError::Build {
                kind: "coordinator",
                name: "designated-writer".into(),
                source,
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn designated(store: Arc<dyn ObjectStore>, node_id: &str) -> DesignatedWriter {
        DesignatedWriter {
            store,
            clock: Arc::new(crate::clock::MonotonicClock::new()),
            node_id: node_id.into(),
            heartbeat_interval: Duration::from_millis(5000),
            takeover_after: Duration::from_millis(15000),
            address: Mutex::new(None),
        }
    }

    #[tokio::test]
    async fn acquire_on_an_empty_store_grants_without_an_epoch() {
        let store = varve_storage::memory_store();
        let log: Arc<dyn Log> = Arc::new(varve_log::MemoryLog::new());
        let grant = designated(store, "a").acquire(&log).await.unwrap();
        assert!(grant.epoch.is_none());
    }

    #[tokio::test]
    async fn a_fresh_foreign_heartbeat_refuses_startup_with_a_clear_error() {
        let store = varve_storage::memory_store();
        let a = designated(Arc::clone(&store), "a");
        a.advertise("http://a:8080").await.unwrap(); // publishes with heartbeat_us = now
        let log: Arc<dyn Log> = Arc::new(varve_log::MemoryLog::new());
        let err = designated(store, "b").acquire(&log).await.unwrap_err();
        match err {
            EngineError::WriterActive {
                address,
                takeover_after_ms,
                ..
            } => {
                assert_eq!(address, "http://a:8080");
                assert_eq!(takeover_after_ms, 15000);
            }
            other => panic!("expected WriterActive, got {other}"),
        }
    }

    #[tokio::test]
    async fn a_stale_or_own_heartbeat_does_not_refuse() {
        let store = varve_storage::memory_store();
        // Stale: heartbeat_us far in the past — write the advertisement JSON directly.
        store
            .put(
                "v1/writer.json",
                bytes::Bytes::from(
                    serde_json::to_vec(&WriterAdvertisement {
                        address: "http://old:1".into(),
                        node_id: "old".into(),
                        epoch: 0,
                        heartbeat_us: 1,
                    })
                    .unwrap(),
                ),
            )
            .await
            .unwrap();
        let log: Arc<dyn Log> = Arc::new(varve_log::MemoryLog::new());
        designated(Arc::clone(&store), "b")
            .acquire(&log)
            .await
            .unwrap();

        // Own node_id (restart of the same instance handle): never refuses.
        let me = designated(store, "old");
        me.acquire(&log).await.unwrap();
    }

    #[tokio::test]
    async fn heartbeat_republishes_the_advertisement_with_a_fresh_timestamp() {
        let store = varve_storage::memory_store();
        let c = designated(Arc::clone(&store), "a");
        assert!(matches!(c.heartbeat().await, LeaseState::Unfenced)); // no address yet: no PUT
        assert!(store.list("v1").await.unwrap().is_empty());

        c.advertise("http://a:8080").await.unwrap();
        let first: WriterAdvertisement =
            serde_json::from_slice(&store.get("v1/writer.json").await.unwrap()).unwrap();
        assert!(matches!(c.heartbeat().await, LeaseState::Unfenced));
        let second: WriterAdvertisement =
            serde_json::from_slice(&store.get("v1/writer.json").await.unwrap()).unwrap();
        assert_eq!(second.node_id, "a");
        assert!(second.heartbeat_us > first.heartbeat_us);
    }
}
