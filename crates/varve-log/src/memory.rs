use crate::log::{Log, LogError};
use crate::record::LogRecord;
use std::sync::{Arc, Mutex};
use varve_config::{ComponentFactory, ConfigSection, RegistryError};
use varve_types::LogPosition;

/// Volatile in-process log (spec §6). Records live only for the process
/// lifetime — restart loses everything. Useful for tests and non-durable
/// deployments; the `local` factory (Task 5) adds real durability.
#[derive(Default)]
pub struct MemoryLog {
    records: Mutex<Vec<(LogPosition, LogRecord)>>,
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

        let mut stored = self.records.lock().map_err(|_| LogError::Poisoned)?;
        let first = match stored.last() {
            Some((last, _)) => last.advance(1)?,
            None => LogPosition::ZERO,
        };
        // Pre-compute positions so an overflow fails before any mutation.
        let mut positioned = Vec::with_capacity(records.len());
        for (i, record) in records.into_iter().enumerate() {
            positioned.push((first.advance(i as u64)?, record));
        }
        stored.extend(positioned);
        Ok(first)
    }

    async fn read_range(
        &self,
        from: LogPosition,
        to: LogPosition,
    ) -> Result<Vec<(LogPosition, LogRecord)>, LogError> {
        let stored = self.records.lock().map_err(|_| LogError::Poisoned)?;
        Ok(stored
            .iter()
            .filter(|(p, _)| *p >= from && *p < to)
            .cloned()
            .collect())
    }
}

/// Registry factory: `[log] backend = "memory"`.
pub struct MemoryLogFactory;

impl ComponentFactory<dyn Log> for MemoryLogFactory {
    fn name(&self) -> &'static str {
        "memory"
    }

    fn build(&self, _cfg: &ConfigSection) -> Result<Arc<dyn Log>, RegistryError> {
        Ok(Arc::new(MemoryLog::new()))
    }
}
