//! `log/object-store` (spec §6): one object per group-commit batch at
//! `v1/log/<epoch>/<offset-lexhex>.vlog`, sharing the block store (D7:
//! plain PUT/GET/LIST only — the designated writer assigns positions
//! locally, so no CAS is ever needed). Durability is the backing store's: a
//! PUT that returns Ok is exactly as durable as the backend makes it.
//!
//! `trim` is a documented NO-OP: the sovereign `ObjectStore` trait has no
//! delete on the `Log` trait itself (slice-4 decision); superseded log
//! objects are instead swept by GC (`Db::gc_once`) once wholly below the
//! minimum retained manifest watermark. Replay cost stays bounded
//! regardless, because recovery reads only `tail(manifest.watermark)`.

use crate::log::{Log, LogError};
use crate::record::LogRecord;
use bytes::Bytes;
use std::sync::Arc;
use varve_config::{BuildContext, ComponentFactory, ConfigSection, RegistryError};
use varve_storage::{keys, ObjectStore};
use varve_types::LogPosition;

pub struct ObjectStoreLog {
    store: Arc<dyn ObjectStore>,
    /// Position of the next appended record. `None` until first use:
    /// factories are synchronous, so the open-time store scan happens
    /// lazily on the first `append` (reads never need it). Exactly one
    /// writer exists per database (D5), so nothing can append between
    /// construction and that first scan.
    next: tokio::sync::Mutex<Option<LogPosition>>,
}

impl ObjectStoreLog {
    pub fn new(store: Arc<dyn ObjectStore>) -> ObjectStoreLog {
        ObjectStoreLog {
            store,
            next: tokio::sync::Mutex::new(None),
        }
    }

    /// Sorted `(first-position, key)` pairs for every log object. Foreign
    /// keys under the prefix are ignored (`parse_log_key` policy).
    async fn list_objects(&self) -> Result<Vec<(LogPosition, String)>, LogError> {
        let listed = self.store.list(keys::LOG_PREFIX).await?;
        let mut objects: Vec<(LogPosition, String)> = listed
            .into_iter()
            .filter_map(|k| keys::parse_log_key(&k).map(|p| (p, k)))
            .collect();
        objects.sort_by_key(|(p, _)| *p);
        Ok(objects)
    }

    /// The position after the last stored record (ZERO on a fresh store):
    /// list the prefix, read the LAST object, count its frames.
    async fn recover_next(&self) -> Result<LogPosition, LogError> {
        let objects = self.list_objects().await?;
        match objects.last() {
            None => Ok(LogPosition::ZERO),
            Some((first, key)) => {
                let bytes = self.store.get(key).await?;
                let count = decode_object(key, &bytes)?.len() as u64;
                Ok(first.advance(count)?)
            }
        }
    }
}

/// Delegates the whole-object grammar to the shared, fuzzed strict decoder
/// (`record::decode_frames`); a log object is additionally never empty
/// (that invariant is this backend's, not the shared frame grammar's).
fn decode_object(key: &str, bytes: &[u8]) -> Result<Vec<LogRecord>, LogError> {
    let records = crate::record::decode_frames(key, bytes)?;
    if records.is_empty() {
        return Err(corrupt(key, 0, "empty log object"));
    }
    Ok(records)
}

fn corrupt(key: &str, off: usize, reason: &str) -> LogError {
    LogError::Corrupt {
        path: key.to_string(),
        offset: off as u64,
        reason: reason.to_string(),
    }
}

#[async_trait::async_trait]
impl Log for ObjectStoreLog {
    async fn append(&self, records: Vec<LogRecord>) -> Result<LogPosition, LogError> {
        if records.is_empty() {
            return Err(LogError::EmptyAppend);
        }
        let mut next = self.next.lock().await;
        let first = match *next {
            Some(position) => position,
            None => self.recover_next().await?,
        };
        let after_batch = first.advance(records.len() as u64)?; // fail before writing
        let mut buf = Vec::new();
        for record in &records {
            let payload = record.to_wire();
            buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
            buf.extend_from_slice(&crc32c::crc32c(&payload).to_le_bytes());
            buf.extend_from_slice(&payload);
        }
        // ONE PUT per batch (spec §6): commit latency ≈ backend PUT latency,
        // throughput scales with group-commit batching.
        self.store
            .put(&keys::log_key(first), Bytes::from(buf))
            .await?;
        *next = Some(after_batch);
        Ok(first)
    }

    async fn read_range(
        &self,
        from: LogPosition,
        to: LogPosition,
    ) -> Result<Vec<(LogPosition, LogRecord)>, LogError> {
        let objects = self.list_objects().await?;
        let mut out = Vec::new();
        for (i, (first, key)) in objects.iter().enumerate() {
            if *first >= to {
                break;
            }
            // An object's span ends where the next object begins, so one
            // whose successor starts at or below `from` is entirely below
            // the range. The LAST object's span is unknown without reading
            // it, so this bound never skips it.
            if let Some((next_first, _)) = objects.get(i + 1) {
                if *next_first <= from {
                    continue;
                }
            }
            let bytes = self.store.get(key).await?;
            let mut position = *first;
            let mut off = 0usize;
            if bytes.is_empty() {
                return Err(corrupt(key, 0, "empty log object"));
            }
            loop {
                if position >= to {
                    break;
                }
                let Some(record) = crate::record::decode_one_frame(key, &bytes, &mut off)? else {
                    break;
                };
                if position >= from && position < to {
                    out.push((position, record));
                }
                position = position.advance(1)?;
            }
        }
        Ok(out)
    }

    /// NO-OP (documented): the sovereign store exposes no delete on this
    /// trait, so superseded objects stay until GC (`Db::gc_once`) sweeps
    /// them once wholly below the minimum retained manifest watermark. The
    /// `Log::trim` contract ("earlier records MAY be retained") is satisfied
    /// trivially, and positions never regress because `next` is tracked
    /// independently of what a trim could remove.
    async fn trim(&self, _up_to: LogPosition) -> Result<(), LogError> {
        Ok(())
    }

    async fn head(&self) -> Result<LogPosition, LogError> {
        let mut next = self.next.lock().await;
        match *next {
            Some(position) => Ok(position),
            None => {
                let position = self.recover_next().await?;
                *next = Some(position);
                Ok(position)
            }
        }
    }

    async fn start_epoch(&self, epoch: u16) -> Result<(), LogError> {
        let mut next = self.next.lock().await;
        let head = match *next {
            Some(position) => position,
            None => self.recover_next().await?,
        };
        crate::log::validate_epoch_start(epoch, head)?;
        *next = Some(LogPosition::new(epoch, 0)?);
        Ok(())
    }
}

/// Registry factory: `[log] backend = "object-store"`. Consumes the
/// already-built storage component from the `BuildContext` (spec §4 ctx) —
/// the log shares the block store's bucket and keyspace (spec §9).
pub struct ObjectStoreLogFactory;

impl ComponentFactory<dyn Log> for ObjectStoreLogFactory {
    fn name(&self) -> &'static str {
        "object-store"
    }

    fn build(
        &self,
        _cfg: &ConfigSection,
        ctx: &BuildContext,
    ) -> Result<Arc<dyn Log>, RegistryError> {
        let store = ctx
            .get::<Arc<dyn ObjectStore>>()
            .ok_or_else(|| RegistryError::Build {
                kind: "log",
                name: "object-store".into(),
                source: "no storage component in the build context; the \
                         object-store log shares the [storage] backend — open \
                         through Db::open, which builds storage first"
                    .to_string()
                    .into(),
            })?;
        Ok(Arc::new(ObjectStoreLog::new(store)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use varve_storage::memory_store;

    fn record(tx_id: u64) -> LogRecord {
        LogRecord {
            tx_id,
            system_time_us: tx_id as i64,
            user: String::new(),
            effects: vec![],
        }
    }

    #[allow(clippy::unwrap_used)]
    async fn corrupt_frame_crc(store: &Arc<dyn ObjectStore>, frame_index: usize) {
        let key = keys::log_key(LogPosition::ZERO);
        let mut bytes = store.get(&key).await.unwrap().to_vec();
        let mut off = 0usize;
        for index in 0..=frame_index {
            let len = u32::from_le_bytes(bytes[off..off + 4].try_into().unwrap()) as usize;
            if index == frame_index {
                let crc = u32::from_le_bytes(bytes[off + 4..off + 8].try_into().unwrap());
                bytes[off + 4..off + 8].copy_from_slice(&(crc ^ 1).to_le_bytes());
                store.put(&key, Bytes::from(bytes)).await.unwrap();
                return;
            }
            off += crate::record::FRAME_HEADER + len;
        }
        panic!("frame {frame_index} not found");
    }

    #[allow(clippy::unwrap_used)]
    #[tokio::test]
    async fn bounded_read_does_not_decode_a_corrupt_excluded_object_frame() {
        let store = memory_store();
        let log = ObjectStoreLog::new(Arc::clone(&store));
        log.append(vec![record(1), record(2)]).await.unwrap();
        corrupt_frame_crc(&(Arc::clone(&store) as Arc<dyn ObjectStore>), 1).await;

        let rows = log
            .read_range(LogPosition::ZERO, LogPosition::ZERO.advance(1).unwrap())
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1.tx_id, 1);
        assert!(matches!(
            log.read_range(LogPosition::ZERO, LogPosition::ZERO.advance(2).unwrap())
                .await,
            Err(LogError::Corrupt { .. })
        ));
    }

    #[allow(clippy::unwrap_used)]
    #[tokio::test]
    async fn head_scans_once_and_start_epoch_moves_the_next_append() {
        let store = memory_store();
        let log = ObjectStoreLog::new(Arc::clone(&store));
        log.append(vec![record(1), record(2)]).await.unwrap();

        // A SECOND instance over the same store scans the durable head.
        let other = ObjectStoreLog::new(Arc::clone(&store));
        assert_eq!(other.head().await.unwrap(), LogPosition::new(0, 2).unwrap());

        other.start_epoch(1).await.unwrap();
        let first = other.append(vec![record(3)]).await.unwrap();
        assert_eq!(first, LogPosition::new(1, 0).unwrap());
        // key landed under the new epoch directory
        let keys = store.list("v1/log/0001").await.unwrap();
        assert_eq!(keys.len(), 1);
    }

    #[allow(clippy::unwrap_used)]
    #[tokio::test]
    async fn zombie_primer_head_caches_the_stale_cursor() {
        // The failover test (Task 9) relies on this: a handle whose head() was
        // taken BEFORE a takeover keeps appending at its stale cached position.
        let store = memory_store();
        let log = ObjectStoreLog::new(Arc::clone(&store));
        log.append(vec![record(1)]).await.unwrap();

        let zombie = ObjectStoreLog::new(Arc::clone(&store));
        assert_eq!(
            zombie.head().await.unwrap(),
            LogPosition::new(0, 1).unwrap()
        );

        // Someone else moves on to epoch 1...
        let successor = ObjectStoreLog::new(Arc::clone(&store));
        successor.start_epoch(1).await.unwrap();
        successor.append(vec![record(10)]).await.unwrap();

        // ...but the zombie still writes at its cached (0, 1).
        let pos = zombie.append(vec![record(99)]).await.unwrap();
        assert_eq!(pos, LogPosition::new(0, 1).unwrap());
    }
}
