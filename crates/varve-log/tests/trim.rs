#![allow(clippy::unwrap_used)]
use varve_log::{LocalLog, Log, LogRecord, MemoryLog};
use varve_types::LogPosition;

fn record(tx_id: u64) -> LogRecord {
    LogRecord {
        tx_id,
        system_time_us: tx_id as i64,
        user: String::new(),
        effects: vec![],
    }
}

fn positions(records: &[(LogPosition, LogRecord)]) -> Vec<u64> {
    records.iter().map(|(p, _)| p.as_u64()).collect()
}

fn segment_count(dir: &std::path::Path) -> usize {
    std::fs::read_dir(dir)
        .unwrap()
        .filter(|e| {
            e.as_ref()
                .unwrap()
                .path()
                .extension()
                .is_some_and(|x| x == "vseg")
        })
        .count()
}

#[tokio::test]
async fn memory_trim_drops_below_and_positions_never_regress() {
    let log = MemoryLog::new();
    log.append(vec![record(1), record(2)]).await.unwrap(); // positions 0, 1
    log.append(vec![record(3)]).await.unwrap(); // position 2
    log.trim(LogPosition::from_u64(2)).await.unwrap();
    let rest = log.tail(LogPosition::ZERO).await.unwrap();
    assert_eq!(positions(&rest), vec![2]);
    assert_eq!(rest[0].1.tx_id, 3);

    // Trimming EVERYTHING must not reset the position sequence.
    log.trim(LogPosition::from_u64(3)).await.unwrap();
    assert!(log.tail(LogPosition::ZERO).await.unwrap().is_empty());
    let first = log.append(vec![record(4)]).await.unwrap();
    assert_eq!(first.as_u64(), 3);
}

#[tokio::test]
async fn local_trim_deletes_only_whole_covered_segments() {
    let dir = tempfile::tempdir().unwrap();
    // 1-byte budget: every append rolls a fresh segment first.
    let log = LocalLog::open(dir.path(), 1).unwrap();
    log.append(vec![record(1)]).await.unwrap(); // segment @0
    log.append(vec![record(2)]).await.unwrap(); // segment @1
    log.append(vec![record(3)]).await.unwrap(); // segment @2 (active)
    assert_eq!(segment_count(dir.path()), 3);

    log.trim(LogPosition::from_u64(2)).await.unwrap();
    assert_eq!(segment_count(dir.path()), 1); // only the active segment left
    assert_eq!(
        positions(&log.tail(LogPosition::ZERO).await.unwrap()),
        vec![2]
    );
}

#[tokio::test]
async fn local_trim_never_touches_the_active_segment() {
    let dir = tempfile::tempdir().unwrap();
    // Huge budget: everything lands in ONE segment — nothing to delete even
    // with the watermark past every record (whole-unit-only rule).
    let log = LocalLog::open(dir.path(), 64 * 1024 * 1024).unwrap();
    log.append(vec![record(1), record(2)]).await.unwrap();
    log.trim(LogPosition::from_u64(2)).await.unwrap();
    assert_eq!(segment_count(dir.path()), 1);
    // Retaining below-watermark records is allowed; replay filters by position.
    assert_eq!(
        positions(&log.tail(LogPosition::from_u64(2)).await.unwrap()),
        Vec::<u64>::new()
    );
    assert_eq!(
        positions(&log.tail(LogPosition::ZERO).await.unwrap()),
        vec![0, 1]
    );
}

#[tokio::test]
async fn local_log_reopens_after_trim_with_positions_intact() {
    let dir = tempfile::tempdir().unwrap();
    {
        let log = LocalLog::open(dir.path(), 1).unwrap();
        log.append(vec![record(1)]).await.unwrap();
        log.append(vec![record(2)]).await.unwrap();
        log.append(vec![record(3)]).await.unwrap();
        log.trim(LogPosition::from_u64(2)).await.unwrap();
    }
    // First remaining segment starts at position 2, not 0 — open() already
    // accepts an arbitrary starting position; this pins it.
    let log = LocalLog::open(dir.path(), 1).unwrap();
    let first = log.append(vec![record(4)]).await.unwrap();
    assert_eq!(first.as_u64(), 3);
    assert_eq!(
        positions(&log.tail(LogPosition::ZERO).await.unwrap()),
        vec![2, 3]
    );
}

#[tokio::test]
async fn trim_at_zero_is_a_no_op() {
    let log = MemoryLog::new();
    log.append(vec![record(1)]).await.unwrap();
    log.trim(LogPosition::ZERO).await.unwrap();
    assert_eq!(
        positions(&log.tail(LogPosition::ZERO).await.unwrap()),
        vec![0]
    );
}
