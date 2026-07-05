//! `log/object-store` (spec §6): one object per group-commit batch at
//! `v1/log/<epoch>/<offset-lexhex>.vlog`, sharing the block store (D7:
//! plain PUT/GET/LIST only — the designated writer assigns positions
//! locally, so no CAS is ever needed). Durability is the backing store's: a
//! PUT that returns Ok is exactly as durable as the backend makes it.
//!
//! `trim` is a documented NO-OP: the sovereign `ObjectStore` trait has no
//! delete (slice-4 decision); superseded log objects are swept by slice-8
//! GC. Replay cost stays bounded regardless, because recovery reads only
//! `tail(manifest.watermark)`.

use crate::log::{Log, LogError};
use crate::record::LogRecord;
use bytes::Bytes;
use std::sync::Arc;
use varve_config::{BuildContext, ComponentFactory, ConfigSection, RegistryError};
use varve_storage::{keys, ObjectStore};
use varve_types::LogPosition;

/// Frame header: `len: u32 LE` + `crc: u32 LE` (CRC32C of the payload) —
/// the exact `LocalLog` frame grammar, so both durable backends share one
/// on-disk format. Decoding here is STRICT (any malformed frame is
/// `Corrupt`): object PUTs are atomic, so a torn tail cannot exist.
const FRAME_HEADER: usize = 8;

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

/// Strict frame walk: every byte must belong to a complete, CRC-valid frame.
fn decode_object(key: &str, bytes: &[u8]) -> Result<Vec<LogRecord>, LogError> {
    let mut records = Vec::new();
    let mut off = 0usize;
    while off < bytes.len() {
        if bytes.len() - off < FRAME_HEADER {
            return Err(corrupt(key, off, "truncated frame header"));
        }
        let len = u32::from_le_bytes([bytes[off], bytes[off + 1], bytes[off + 2], bytes[off + 3]])
            as usize;
        let crc = u32::from_le_bytes([
            bytes[off + 4],
            bytes[off + 5],
            bytes[off + 6],
            bytes[off + 7],
        ]);
        if bytes.len() - off - FRAME_HEADER < len {
            return Err(corrupt(key, off, "truncated frame payload"));
        }
        let payload = &bytes[off + FRAME_HEADER..off + FRAME_HEADER + len];
        if crc32c::crc32c(payload) != crc {
            return Err(corrupt(key, off, "CRC mismatch"));
        }
        records.push(LogRecord::from_wire(payload)?);
        off += FRAME_HEADER + len;
    }
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
            for record in decode_object(key, &bytes)? {
                if position >= from && position < to {
                    out.push((position, record));
                }
                position = position.advance(1)?;
            }
        }
        Ok(out)
    }

    /// NO-OP (documented): the sovereign store exposes no delete, so
    /// superseded objects stay until slice-8 GC. The `Log::trim` contract
    /// ("earlier records MAY be retained") is satisfied trivially, and
    /// positions never regress because `next` is tracked independently of
    /// what a trim could remove.
    async fn trim(&self, _up_to: LogPosition) -> Result<(), LogError> {
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

    fn build(&self, _cfg: &ConfigSection, ctx: &BuildContext) -> Result<Arc<dyn Log>, RegistryError> {
        let store = ctx.get::<Arc<dyn ObjectStore>>().ok_or_else(|| {
            RegistryError::Build {
                kind: "log",
                name: "object-store".into(),
                source: "no storage component in the build context; the \
                         object-store log shares the [storage] backend — open \
                         through Db::open, which builds storage first"
                    .to_string()
                    .into(),
            }
        })?;
        Ok(Arc::new(ObjectStoreLog::new(store)))
    }
}
