use crate::log::{Log, LogError};
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
}

/// Registry factory: `[log] backend = "memory"`.
pub struct MemoryLogFactory;

impl ComponentFactory<dyn Log> for MemoryLogFactory {
    fn name(&self) -> &'static str {
        "memory"
    }

    fn build(&self, _cfg: &ConfigSection, _ctx: &BuildContext) -> Result<Arc<dyn Log>, RegistryError> {
        Ok(Arc::new(MemoryLog::new()))
    }
}
