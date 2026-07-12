use crate::record::LogRecord;
use varve_types::LogPosition;

#[derive(Debug, thiserror::Error)]
pub enum LogError {
    #[error("log I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("cannot append an empty record batch")]
    EmptyAppend,
    #[error("log record decode failed: {0}")]
    Decode(#[from] prost::DecodeError),
    #[error("log corrupt in {path} at byte offset {offset}: {reason}")]
    Corrupt {
        path: String,
        offset: u64,
        reason: String,
    },
    #[error("log poisoned by an earlier failed append; reopen to recover")]
    Poisoned,
    #[error(transparent)]
    Type(#[from] varve_types::TypeError),
    #[cfg(feature = "object-store")]
    #[error("log storage backend error: {0}")]
    Storage(#[from] varve_storage::StorageError),
    #[error("log backend '{0}' does not support epochs; cas-failover requires the shared object-store log")]
    EpochUnsupported(&'static str),
    #[error("cannot start epoch {requested}: log head is already at epoch {head}")]
    EpochRegression { requested: u16, head: u16 },
}

/// Ordered, durable stream of transaction records (spec §6). One `LogRecord`
/// per transaction. `append` writes a batch of records as ONE durable unit
/// (one fsync / one object PUT); records receive consecutive positions.
/// Durability contract: when `append` returns Ok, every record in the batch
/// survives `kill -9`.
#[async_trait::async_trait]
pub trait Log: Send + Sync {
    /// Durably append `records`; returns the position of the FIRST record.
    async fn append(&self, records: Vec<LogRecord>) -> Result<LogPosition, LogError>;
    /// Records with `from <= position < to`, in position order.
    async fn read_range(
        &self,
        from: LogPosition,
        to: LogPosition,
    ) -> Result<Vec<(LogPosition, LogRecord)>, LogError>;
    /// Every record at or after `from`. v1 tailing is poll-based: callers
    /// re-invoke to observe new records (streaming tail arrives with the
    /// query-node role, slice 9).
    async fn tail(&self, from: LogPosition) -> Result<Vec<(LogPosition, LogRecord)>, LogError> {
        self.read_range(from, LogPosition::from_u64(u64::MAX)).await
    }
    /// Physically discards records with `position < up_to` where that is
    /// cheap in whole durability units (whole segments / whole objects).
    /// Records at or after `up_to` are NEVER removed; earlier ones MAY be
    /// retained. Positions are never reused after a trim. Called by the
    /// writer once a block manifest commits (spec §9: the manifest trims
    /// the log-replay watermark).
    async fn trim(&self, up_to: LogPosition) -> Result<(), LogError>;
    /// Position the NEXT appended record will receive (== the exclusive end
    /// of the durable prefix). For the object-store log the first call scans
    /// the store and primes the internal cursor; later calls return the
    /// cached value.
    async fn head(&self) -> Result<LogPosition, LogError>;
    /// Repositions the next append at `(epoch, 0)`. `epoch` must be at or
    /// above the current head's epoch; moving INTO an epoch that already
    /// holds records is `EpochRegression`. Idempotent when the head is
    /// already `(epoch, 0)`.
    async fn start_epoch(&self, epoch: u16) -> Result<(), LogError>;
}

/// `EpochRegression` unless `(epoch, 0)` is at or beyond `head`.
pub(crate) fn validate_epoch_start(epoch: u16, head: LogPosition) -> Result<(), LogError> {
    if epoch < head.epoch() || (epoch == head.epoch() && head.offset() > 0) {
        return Err(LogError::EpochRegression {
            requested: epoch,
            head: head.epoch(),
        });
    }
    Ok(())
}
