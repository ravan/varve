use std::fs;
use std::path::{Path, PathBuf};
use varve_log::{LocalLog, Log, LogError, LogRecord, DEFAULT_SEGMENT_MAX_BYTES};
use varve_types::LogPosition;

fn rec(tx_id: u64) -> LogRecord {
    LogRecord {
        tx_id,
        system_time_us: tx_id as i64,
        user: String::new(),
        effects: vec![],
    }
}

#[allow(clippy::unwrap_used)]
fn pos(offset: u64) -> LogPosition {
    LogPosition::new(0, offset).unwrap()
}

#[allow(clippy::unwrap_used)]
fn open(dir: &Path) -> LocalLog {
    LocalLog::open(dir, DEFAULT_SEGMENT_MAX_BYTES).unwrap()
}

#[allow(clippy::unwrap_used)]
fn segment_paths(dir: &Path) -> Vec<PathBuf> {
    let mut paths: Vec<PathBuf> = fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|x| x == "vseg"))
        .collect();
    paths.sort();
    paths
}

/// (start, end) byte range of each frame in a segment file.
fn frame_ranges(bytes: &[u8]) -> Vec<(usize, usize)> {
    let mut ranges = Vec::new();
    let mut off = 0;
    while off + 8 <= bytes.len() {
        let len = u32::from_le_bytes([bytes[off], bytes[off + 1], bytes[off + 2], bytes[off + 3]])
            as usize;
        let end = off + 8 + len;
        assert!(end <= bytes.len(), "test helper walked off the segment");
        ranges.push((off, end));
        off = end;
    }
    ranges
}

#[allow(clippy::unwrap_used)]
async fn write_three(dir: &Path) {
    let log = open(dir);
    log.append(vec![rec(1)]).await.unwrap();
    log.append(vec![rec(2)]).await.unwrap();
    log.append(vec![rec(3)]).await.unwrap();
}

fn tx_ids(records: &[(LogPosition, LogRecord)]) -> Vec<u64> {
    records.iter().map(|(_, r)| r.tx_id).collect()
}

#[allow(clippy::unwrap_used)]
#[tokio::test]
async fn corrupt_tail_byte_recovers_cleanly_and_positions_rewind() {
    let dir = tempfile::tempdir().unwrap();
    write_three(dir.path()).await;

    // Flip the last byte: record 3's payload no longer matches its CRC.
    let seg = segment_paths(dir.path()).pop().unwrap();
    let mut bytes = fs::read(&seg).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 0xFF;
    fs::write(&seg, &bytes).unwrap();

    let log = open(dir.path());
    assert_eq!(
        tx_ids(&log.tail(LogPosition::ZERO).await.unwrap()),
        vec![1, 2]
    );

    // The truncated position is reused and the log keeps working.
    assert_eq!(log.append(vec![rec(33)]).await.unwrap(), pos(2));
    assert_eq!(
        tx_ids(&log.tail(LogPosition::ZERO).await.unwrap()),
        vec![1, 2, 33]
    );
}

#[allow(clippy::unwrap_used)]
#[tokio::test]
async fn partial_trailing_frame_is_dropped() {
    let dir = tempfile::tempdir().unwrap();
    write_three(dir.path()).await;

    // Simulate a torn write: garbage that looks like the start of a frame.
    let seg = segment_paths(dir.path()).pop().unwrap();
    let mut bytes = fs::read(&seg).unwrap();
    let clean_len = bytes.len();
    bytes.extend_from_slice(&[0xFF, 0xFF, 0xFF]); // < frame header size
    fs::write(&seg, &bytes).unwrap();

    let log = open(dir.path());
    assert_eq!(
        tx_ids(&log.tail(LogPosition::ZERO).await.unwrap()),
        vec![1, 2, 3]
    );
    drop(log);
    assert_eq!(fs::read(&seg).unwrap().len(), clean_len, "tail truncated");
}

#[allow(clippy::unwrap_used)]
#[tokio::test]
async fn corruption_before_the_tail_truncates_everything_after_it() {
    let dir = tempfile::tempdir().unwrap();
    write_three(dir.path()).await;

    // Corrupt record 2 (middle frame of the LAST segment): recovery keeps
    // only the clean prefix — records 2 AND 3 are dropped (they were part of
    // batches whose ack the crashed process may never have sent; a valid
    // frame AFTER a torn one cannot be trusted as committed order).
    let seg = segment_paths(dir.path()).pop().unwrap();
    let mut bytes = fs::read(&seg).unwrap();
    let ranges = frame_ranges(&bytes);
    assert_eq!(ranges.len(), 3);
    let (start, _) = ranges[1];
    bytes[start + 8] ^= 0xFF; // first payload byte of frame 2
    fs::write(&seg, &bytes).unwrap();

    let log = open(dir.path());
    assert_eq!(tx_ids(&log.tail(LogPosition::ZERO).await.unwrap()), vec![1]);
    assert_eq!(log.append(vec![rec(22)]).await.unwrap(), pos(1));
}

#[allow(clippy::unwrap_used)]
#[tokio::test]
async fn corruption_in_a_non_final_segment_is_fatal() {
    let dir = tempfile::tempdir().unwrap();
    {
        // 1-byte budget forces one segment per batch.
        let log = LocalLog::open(dir.path(), 1).unwrap();
        log.append(vec![rec(1)]).await.unwrap();
        log.append(vec![rec(2)]).await.unwrap();
    }
    let first_seg = segment_paths(dir.path()).into_iter().next().unwrap();
    let mut bytes = fs::read(&first_seg).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 0xFF;
    fs::write(&first_seg, &bytes).unwrap();

    // Committed history is damaged — refuse to open rather than silently
    // truncate acked transactions.
    assert!(matches!(
        LocalLog::open(dir.path(), 1),
        Err(LogError::Corrupt { .. })
    ));
}

#[allow(clippy::unwrap_used)]
#[tokio::test]
async fn recovery_is_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    write_three(dir.path()).await;
    let seg = segment_paths(dir.path()).pop().unwrap();
    let mut bytes = fs::read(&seg).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 0xFF;
    fs::write(&seg, &bytes).unwrap();

    for _ in 0..2 {
        let log = open(dir.path());
        assert_eq!(
            tx_ids(&log.tail(LogPosition::ZERO).await.unwrap()),
            vec![1, 2]
        );
        drop(log);
    }
}
