use crate::log::{Log, LogError};
use crate::record::LogRecord;
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use varve_config::{BuildContext, ComponentFactory, ConfigSection, RegistryError};
use varve_types::LogPosition;

pub const DEFAULT_SEGMENT_MAX_BYTES: u64 = 64 * 1024 * 1024;

/// Frame header: `len: u32 LE` (payload length) + `crc: u32 LE` (CRC32C of
/// payload), immediately followed by the payload itself.
const FRAME_HEADER: usize = 8;

/// Durable, segmented append-only log (spec §6 `local` backend). Records are
/// framed with a CRC32C-checked header and written in batches; each `append`
/// is one `write_all` + one `File::sync_all` (fsync before ack). In-memory
/// state (`segment_len`, `next`) only commits after that fsync succeeds.
pub struct LocalLog {
    inner: Arc<Mutex<Inner>>,
}

struct Inner {
    dir: PathBuf,
    segment_max_bytes: u64,
    /// The currently open, append-only active segment file.
    segment: File,
    /// Bytes written to `segment` so far (tracked in memory to decide when
    /// to roll; always matches the file's on-disk length).
    segment_len: u64,
    /// Position the next appended record will receive.
    next: LogPosition,
    /// Set when an append's rollback (post-failure truncate) itself fails,
    /// leaving the active segment's tail in an unknown state. All further
    /// calls fail fast; reopening the log re-scans and recovers.
    poisoned: bool,
}

/// Segment file name for the segment whose first record is at `first`:
/// 16 lower-case hex digits of the packed `LogPosition`, so lexicographic
/// filename order equals position order.
fn segment_name(first: LogPosition) -> String {
    format!("{:016x}.vseg", first.as_u64())
}

/// fsyncs a directory's own metadata (e.g. after creating/renaming an entry
/// in it). Plain-file, no-CAS: opening a directory for fsync is standard
/// POSIX practice and needs nothing beyond std fs.
fn fsync_dir(dir: &Path) -> Result<(), LogError> {
    File::open(dir)?.sync_all()?;
    Ok(())
}

/// Sorted `(first-position, path)` pairs for every `.vseg` segment in `dir`.
fn list_segments(dir: &Path) -> Result<Vec<(u64, PathBuf)>, LogError> {
    let mut segments = Vec::new();
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if path.extension().is_none_or(|ext| ext != "vseg") {
            continue;
        }
        let stem = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or_default();
        let first = u64::from_str_radix(stem, 16).map_err(|_| LogError::Corrupt {
            path: path.display().to_string(),
            offset: 0,
            reason: "unrecognized segment file name".into(),
        })?;
        segments.push((first, path));
    }
    segments.sort();
    Ok(segments)
}

struct ScanOutcome {
    records: u64,
    valid_len: u64,
    /// Reason the tail beyond `valid_len` is unusable, if any.
    torn: Option<String>,
}

/// Walks a segment's frames, verifying lengths and CRCs (payloads are NOT
/// protobuf-decoded here; that happens on read).
fn scan_segment(path: &Path) -> Result<ScanOutcome, LogError> {
    let bytes = fs::read(path)?;
    let mut off = 0usize;
    let mut records = 0u64;
    loop {
        let remaining = bytes.len() - off;
        if remaining == 0 {
            return Ok(ScanOutcome {
                records,
                valid_len: off as u64,
                torn: None,
            });
        }
        if remaining < FRAME_HEADER {
            return Ok(ScanOutcome {
                records,
                valid_len: off as u64,
                torn: Some("truncated frame header".into()),
            });
        }
        let len = u32::from_le_bytes([bytes[off], bytes[off + 1], bytes[off + 2], bytes[off + 3]])
            as usize;
        let crc = u32::from_le_bytes([
            bytes[off + 4],
            bytes[off + 5],
            bytes[off + 6],
            bytes[off + 7],
        ]);
        if remaining < FRAME_HEADER + len {
            return Ok(ScanOutcome {
                records,
                valid_len: off as u64,
                torn: Some("truncated frame payload".into()),
            });
        }
        let payload = &bytes[off + FRAME_HEADER..off + FRAME_HEADER + len];
        if crc32c::crc32c(payload) != crc {
            return Ok(ScanOutcome {
                records,
                valid_len: off as u64,
                torn: Some("CRC mismatch".into()),
            });
        }
        records += 1;
        off += FRAME_HEADER + len;
    }
}

impl LocalLog {
    /// Opens the durable log at `dir`, validating every segment. A torn
    /// tail on the LAST segment is truncated away (ordinary crash
    /// recovery); the same damage anywhere else is fatal (`LogError::Corrupt`).
    pub fn open(dir: &Path, segment_max_bytes: u64) -> Result<LocalLog, LogError> {
        fs::create_dir_all(dir)?;
        let segments = list_segments(dir)?;

        if segments.is_empty() {
            let path = dir.join(segment_name(LogPosition::ZERO));
            let segment = OpenOptions::new().create(true).append(true).open(&path)?;
            segment.sync_all()?;
            fsync_dir(dir)?;
            return Ok(LocalLog {
                inner: Arc::new(Mutex::new(Inner {
                    dir: dir.to_path_buf(),
                    segment_max_bytes,
                    segment,
                    segment_len: 0,
                    next: LogPosition::ZERO,
                    poisoned: false,
                })),
            });
        }

        let mut expected = LogPosition::from_u64(segments[0].0);
        for (idx, (first, path)) in segments.iter().enumerate() {
            let is_last = idx == segments.len() - 1;
            if *first != expected.as_u64() {
                return Err(LogError::Corrupt {
                    path: path.display().to_string(),
                    offset: 0,
                    reason: format!(
                        "segment starts at position {first:#x}, expected {:#x}",
                        expected.as_u64()
                    ),
                });
            }
            let outcome = scan_segment(path)?;
            if let Some(reason) = outcome.torn {
                if !is_last {
                    return Err(LogError::Corrupt {
                        path: path.display().to_string(),
                        offset: outcome.valid_len,
                        reason,
                    });
                }
                // Torn tail on the active segment: truncate to the last
                // complete, CRC-valid frame. Every dropped record was never
                // acked (ack requires a completed fsync of the whole batch).
                let file = OpenOptions::new().write(true).open(path)?;
                file.set_len(outcome.valid_len)?;
                file.sync_all()?;
            }
            expected = expected.advance(outcome.records)?;
        }

        let (_, last_path) = segments
            .last()
            .cloned()
            .unwrap_or_else(|| (0, dir.join(segment_name(LogPosition::ZERO))));
        let segment = OpenOptions::new().append(true).open(&last_path)?;
        let segment_len = segment.metadata()?.len();
        Ok(LocalLog {
            inner: Arc::new(Mutex::new(Inner {
                dir: dir.to_path_buf(),
                segment_max_bytes,
                segment,
                segment_len,
                next: expected,
                poisoned: false,
            })),
        })
    }
}

fn append_sync(inner: &mut Inner, records: Vec<LogRecord>) -> Result<LogPosition, LogError> {
    if inner.poisoned {
        return Err(LogError::Poisoned);
    }
    if records.is_empty() {
        return Err(LogError::EmptyAppend);
    }

    crash_point("pre-append");

    // Roll to a new segment BEFORE appending if the active one has already
    // reached the budget — a batch is never split across segments, so the
    // active segment may overshoot `segment_max_bytes` by up to one batch.
    if inner.segment_len >= inner.segment_max_bytes {
        let path = inner.dir.join(segment_name(inner.next));
        let segment = OpenOptions::new()
            .create_new(true)
            .append(true)
            .open(&path)?;
        segment.sync_all()?;
        fsync_dir(&inner.dir)?;
        inner.segment = segment;
        inner.segment_len = 0;
    }

    let first = inner.next;
    let after_batch = first.advance(records.len() as u64)?; // fail before writing

    let mut buf = Vec::new();
    for record in &records {
        let payload = record.to_wire();
        buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        buf.extend_from_slice(&crc32c::crc32c(&payload).to_le_bytes());
        buf.extend_from_slice(&payload);
    }

    let write_result = (|| -> std::io::Result<()> {
        use std::io::Write as _;
        inner.segment.write_all(&buf)?;
        inner.segment.sync_all()
    })();
    if let Err(e) = write_result {
        // Roll the file back so the tail stays clean for the next append.
        let restored = inner
            .segment
            .set_len(inner.segment_len)
            .and_then(|_| inner.segment.sync_all());
        if restored.is_err() {
            inner.poisoned = true;
        }
        return Err(LogError::Io(e));
    }

    inner.segment_len += buf.len() as u64;
    inner.next = after_batch;
    crash_point("post-append");
    Ok(first)
}

fn read_range_sync(
    inner: &Inner,
    from: LogPosition,
    to: LogPosition,
) -> Result<Vec<(LogPosition, LogRecord)>, LogError> {
    if inner.poisoned {
        return Err(LogError::Poisoned);
    }
    let mut out = Vec::new();
    for (first, path) in list_segments(&inner.dir)? {
        if LogPosition::from_u64(first) >= to {
            break;
        }
        let mut position = LogPosition::from_u64(first);
        let bytes = fs::read(&path)?;
        let mut off = 0usize;
        while bytes.len() - off >= FRAME_HEADER {
            if position >= to {
                return Ok(out);
            }
            let len =
                u32::from_le_bytes([bytes[off], bytes[off + 1], bytes[off + 2], bytes[off + 3]])
                    as usize;
            let crc = u32::from_le_bytes([
                bytes[off + 4],
                bytes[off + 5],
                bytes[off + 6],
                bytes[off + 7],
            ]);
            if bytes.len() - off < FRAME_HEADER + len {
                break;
            }
            let payload = &bytes[off + FRAME_HEADER..off + FRAME_HEADER + len];
            if crc32c::crc32c(payload) != crc {
                return Err(LogError::Corrupt {
                    path: path.display().to_string(),
                    offset: off as u64,
                    reason: "CRC mismatch on read".into(),
                });
            }
            if position >= from && position < to {
                out.push((position, LogRecord::from_wire(payload)?));
            }
            position = position.advance(1)?;
            off += FRAME_HEADER + len;
        }
    }
    Ok(out)
}

/// Whole-segment trim: a segment is deletable iff the NEXT segment exists
/// and starts at or below `up_to` (every record in it is then < up_to). The
/// active (last) segment is never deleted, so `next`/`segment_len` stay
/// valid and positions never regress. No mid-segment truncation.
fn trim_sync(inner: &Inner, up_to: LogPosition) -> Result<(), LogError> {
    if inner.poisoned {
        return Err(LogError::Poisoned);
    }
    let segments = list_segments(&inner.dir)?;
    let mut removed = false;
    for pair in segments.windows(2) {
        let (_, path) = &pair[0];
        let (next_first, _) = &pair[1];
        if *next_first <= up_to.as_u64() {
            fs::remove_file(path)?;
            removed = true;
        }
    }
    if removed {
        fsync_dir(&inner.dir)?;
    }
    Ok(())
}

#[async_trait::async_trait]
impl Log for LocalLog {
    async fn append(&self, records: Vec<LogRecord>) -> Result<LogPosition, LogError> {
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || {
            let mut guard = inner.lock().map_err(|_| LogError::Poisoned)?;
            append_sync(&mut guard, records)
        })
        .await
        .map_err(|e| LogError::Io(std::io::Error::other(e)))?
    }

    async fn read_range(
        &self,
        from: LogPosition,
        to: LogPosition,
    ) -> Result<Vec<(LogPosition, LogRecord)>, LogError> {
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || {
            let guard = inner.lock().map_err(|_| LogError::Poisoned)?;
            read_range_sync(&guard, from, to)
        })
        .await
        .map_err(|e| LogError::Io(std::io::Error::other(e)))?
    }

    async fn trim(&self, up_to: LogPosition) -> Result<(), LogError> {
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || {
            let guard = inner.lock().map_err(|_| LogError::Poisoned)?;
            trim_sync(&guard, up_to)
        })
        .await
        .map_err(|e| LogError::Io(std::io::Error::other(e)))?
    }

    async fn head(&self) -> Result<LogPosition, LogError> {
        let inner = self.inner.lock().map_err(|_| LogError::Poisoned)?;
        if inner.poisoned {
            return Err(LogError::Poisoned);
        }
        Ok(inner.next)
    }

    async fn start_epoch(&self, _epoch: u16) -> Result<(), LogError> {
        Err(LogError::EpochUnsupported("local"))
    }
}

/// Test-only crash hook for the `varve-testkit` `kill -9` harness. Inert
/// (compiles to a no-op) unless built with the `fault-injection` feature,
/// and even then does nothing unless `VARVE_CRASH_TRIGGER` points at a file
/// containing exactly this point's name. When armed, announces the point on
/// stdout and parks the thread until the harness delivers `kill -9`.
#[cfg(feature = "fault-injection")]
fn crash_point(point: &str) {
    let Ok(path) = std::env::var("VARVE_CRASH_TRIGGER") else {
        return;
    };
    match std::fs::read_to_string(&path) {
        Ok(armed) if armed.trim() == point => {}
        _ => return,
    }
    println!("CRASH_POINT {point}");
    use std::io::Write as _;
    let _ = std::io::stdout().flush();
    loop {
        std::thread::sleep(std::time::Duration::from_secs(3600));
    }
}

#[cfg(not(feature = "fault-injection"))]
fn crash_point(_point: &str) {}

#[derive(serde::Deserialize)]
struct LocalLogConfig {
    dir: String,
    #[serde(default = "default_segment_max_bytes")]
    segment_max_bytes: u64,
}

fn default_segment_max_bytes() -> u64 {
    DEFAULT_SEGMENT_MAX_BYTES
}

/// Registry factory: `[log] backend = "local"`, configured via a nested
/// `[log.local]` table (`dir` required, `segment_max_bytes` optional).
pub struct LocalLogFactory;

impl ComponentFactory<dyn Log> for LocalLogFactory {
    fn name(&self) -> &'static str {
        "local"
    }

    fn build(
        &self,
        cfg: &ConfigSection,
        _ctx: &BuildContext,
    ) -> Result<Arc<dyn Log>, RegistryError> {
        let local = cfg.child("local").ok_or_else(|| RegistryError::Build {
            kind: "log",
            name: "local".into(),
            source: "missing [log.local] section (requires `dir`)"
                .to_string()
                .into(),
        })?;
        let config: LocalLogConfig = local.get()?;
        let log =
            LocalLog::open(Path::new(&config.dir), config.segment_max_bytes).map_err(|e| {
                RegistryError::Build {
                    kind: "log",
                    name: "local".into(),
                    source: Box::new(e),
                }
            })?;
        Ok(Arc::new(log))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Seek as _, SeekFrom, Write as _};

    fn record(tx_id: u64) -> LogRecord {
        LogRecord {
            tx_id,
            system_time_us: tx_id as i64,
            user: String::new(),
            effects: vec![],
        }
    }

    #[allow(clippy::unwrap_used)]
    fn corrupt_frame_crc(dir: &Path, frame_index: usize) {
        let (_, path) = list_segments(dir).unwrap().into_iter().next().unwrap();
        let bytes = fs::read(&path).unwrap();
        let mut off = 0usize;
        for index in 0..=frame_index {
            let len = u32::from_le_bytes(bytes[off..off + 4].try_into().unwrap()) as usize;
            if index == frame_index {
                let crc = u32::from_le_bytes(bytes[off + 4..off + 8].try_into().unwrap());
                let mut file = OpenOptions::new().write(true).open(path).unwrap();
                file.seek(SeekFrom::Start((off + 4) as u64)).unwrap();
                file.write_all(&(crc ^ 1).to_le_bytes()).unwrap();
                file.sync_all().unwrap();
                return;
            }
            off += FRAME_HEADER + len;
        }
        panic!("frame {frame_index} not found");
    }

    #[allow(clippy::unwrap_used)]
    #[tokio::test]
    async fn bounded_read_does_not_decode_a_corrupt_excluded_frame() {
        let dir = tempfile::tempdir().unwrap();
        let log = LocalLog::open(dir.path(), DEFAULT_SEGMENT_MAX_BYTES).unwrap();
        log.append(vec![record(1)]).await.unwrap();
        log.append(vec![record(2)]).await.unwrap();
        corrupt_frame_crc(dir.path(), 1);

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
    async fn local_log_reports_head_and_refuses_epochs() {
        let dir = tempfile::tempdir().unwrap();
        let log = LocalLog::open(dir.path(), DEFAULT_SEGMENT_MAX_BYTES).unwrap();
        assert_eq!(log.head().await.unwrap(), LogPosition::ZERO);
        assert!(matches!(
            log.start_epoch(1).await,
            Err(LogError::EpochUnsupported("local"))
        ));
    }
}
