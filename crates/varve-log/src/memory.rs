use crate::log::{validate_epoch_start, Log, LogError};
use crate::record::LogRecord;
use std::sync::{Arc, Mutex};
use varve_config::{BuildContext, ComponentFactory, ConfigSection, RegistryError};
use varve_types::LogPosition;

struct Inner {
    records: Vec<(LogPosition, LogRecord)>,
    /// Position the next appended record will receive. Explicit (not derived
    /// from `records.last()`) so a trim never resets the sequence.
    next: LogPosition,
}

/// Volatile in-process log (spec §6). Records live only for the process
/// lifetime — restart loses everything. Useful for tests and non-durable
/// deployments; the `local` factory (Task 5) adds real durability.
pub struct MemoryLog {
    inner: Mutex<Inner>,
}

impl Default for MemoryLog {
    fn default() -> Self {
        MemoryLog {
            inner: Mutex::new(Inner {
                records: Vec::new(),
                next: LogPosition::ZERO,
            }),
        }
    }
}

impl MemoryLog {
    pub fn new() -> MemoryLog {
        MemoryLog::default()
    }
}

#[async_trait::async_trait]
impl Log for MemoryLog {
    async fn append(&self, records: Vec<LogRecord>) -> Result<LogPosition, LogError> {
        if records.is_empty() {
            return Err(LogError::EmptyAppend);
        }
        let mut inner = self.inner.lock().map_err(|_| LogError::Poisoned)?;
        let first = inner.next;
        // Pre-compute positions so an overflow fails before any mutation.
        let after_batch = first.advance(records.len() as u64)?;
        let mut positioned = Vec::with_capacity(records.len());
        for (i, record) in records.into_iter().enumerate() {
            positioned.push((first.advance(i as u64)?, record));
        }
        inner.records.extend(positioned);
        inner.next = after_batch;
        Ok(first)
    }

    async fn read_range(
        &self,
        from: LogPosition,
        to: LogPosition,
    ) -> Result<Vec<(LogPosition, LogRecord)>, LogError> {
        let inner = self.inner.lock().map_err(|_| LogError::Poisoned)?;
        Ok(inner
            .records
            .iter()
            .filter(|(p, _)| *p >= from && *p < to)
            .cloned()
            .collect())
    }

    async fn trim(&self, up_to: LogPosition) -> Result<(), LogError> {
        let mut inner = self.inner.lock().map_err(|_| LogError::Poisoned)?;
        inner.records.retain(|(p, _)| *p >= up_to);
        Ok(())
    }

    async fn head(&self) -> Result<LogPosition, LogError> {
        Ok(self.inner.lock().map_err(|_| LogError::Poisoned)?.next)
    }

    async fn start_epoch(&self, epoch: u16) -> Result<(), LogError> {
        let mut inner = self.inner.lock().map_err(|_| LogError::Poisoned)?;
        validate_epoch_start(epoch, inner.next)?;
        inner.next = LogPosition::new(epoch, 0)?;
        Ok(())
    }
}

/// Registry factory: `[log] backend = "memory"`.
pub struct MemoryLogFactory;

impl ComponentFactory<dyn Log> for MemoryLogFactory {
    fn name(&self) -> &'static str {
        "memory"
    }

    fn build(
        &self,
        _cfg: &ConfigSection,
        _ctx: &BuildContext,
    ) -> Result<Arc<dyn Log>, RegistryError> {
        Ok(Arc::new(MemoryLog::new()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(tx_id: u64) -> LogRecord {
        LogRecord {
            tx_id,
            system_time_us: tx_id as i64,
            user: String::new(),
            effects: vec![],
        }
    }

    #[tokio::test]
    async fn head_and_start_epoch_reposition_appends() {
        let log = MemoryLog::new();
        assert_eq!(log.head().await.unwrap(), LogPosition::ZERO);
        log.append(vec![record(1), record(2)]).await.unwrap();
        assert_eq!(log.head().await.unwrap(), LogPosition::new(0, 2).unwrap());

        log.start_epoch(1).await.unwrap();
        assert_eq!(log.head().await.unwrap(), LogPosition::new(1, 0).unwrap());
        let first = log.append(vec![record(3)]).await.unwrap();
        assert_eq!(first, LogPosition::new(1, 0).unwrap());

        // regression: back into an occupied epoch
        assert!(matches!(
            log.start_epoch(0).await,
            Err(LogError::EpochRegression {
                requested: 0,
                head: 1
            })
        ));
        // idempotent at an empty epoch origin is allowed
        log.start_epoch(2).await.unwrap();
        log.start_epoch(2).await.unwrap();
    }
}
