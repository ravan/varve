use std::path::Path;
use varve_config::{BuildContext, Config};
use varve_log::{log_registry, LocalLog, Log, LogError, LogRecord, DEFAULT_SEGMENT_MAX_BYTES};
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
#[tokio::test]
async fn append_read_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let log = open(dir.path());
        assert_eq!(log.append(vec![rec(1), rec(2)]).await.unwrap(), pos(0));
        assert_eq!(log.append(vec![rec(3)]).await.unwrap(), pos(2));
        let mid = log.read_range(pos(1), pos(2)).await.unwrap();
        assert_eq!(mid.len(), 1);
        assert_eq!(mid[0].1.tx_id, 2);
    }
    // Reopen: durable, positions continue.
    let log = open(dir.path());
    let all = log.tail(LogPosition::ZERO).await.unwrap();
    assert_eq!(
        all.iter().map(|(p, r)| (*p, r.tx_id)).collect::<Vec<_>>(),
        vec![(pos(0), 1), (pos(1), 2), (pos(2), 3)]
    );
    assert_eq!(log.append(vec![rec(4)]).await.unwrap(), pos(3));
}

#[allow(clippy::unwrap_used)]
#[tokio::test]
async fn read_range_obeys_half_open_follower_batch_bounds() {
    let dir = tempfile::tempdir().unwrap();
    let log = open(dir.path());
    log.append(vec![rec(1), rec(2), rec(3), rec(4)])
        .await
        .unwrap();

    for (from, to, expected) in [(0, 0, vec![]), (1, 3, vec![2, 3]), (4, 8, vec![])] {
        let rows = log.read_range(pos(from), pos(to)).await.unwrap();
        assert_eq!(
            rows.iter()
                .map(|(_, record)| record.tx_id)
                .collect::<Vec<_>>(),
            expected
        );
    }
}

#[allow(clippy::unwrap_used)]
#[tokio::test]
async fn rolls_segments_and_reads_across_them() {
    let dir = tempfile::tempdir().unwrap();
    {
        // 1-byte segment_max_bytes forces a roll before every append after
        // the first record fills the "budget".
        let log = LocalLog::open(dir.path(), 1).unwrap();
        log.append(vec![rec(1)]).await.unwrap();
        log.append(vec![rec(2)]).await.unwrap();
        log.append(vec![rec(3)]).await.unwrap();

        let segments: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|x| x == "vseg"))
            .collect();
        assert!(
            segments.len() > 1,
            "expected multiple segments, got {}",
            segments.len()
        );

        let all = log.tail(LogPosition::ZERO).await.unwrap();
        assert_eq!(
            all.iter().map(|(_, r)| r.tx_id).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
    }
    // Reopen across multiple segments; positions continue correctly.
    let log = LocalLog::open(dir.path(), 1).unwrap();
    assert_eq!(log.append(vec![rec(4)]).await.unwrap(), pos(3));
}

#[allow(clippy::unwrap_used)]
#[tokio::test]
async fn records_round_trip_effects() {
    let dir = tempfile::tempdir().unwrap();
    let log = open(dir.path());
    let record = LogRecord {
        tx_id: 9,
        system_time_us: 99,
        user: String::new(),
        effects: vec![varve_log::TableEffects {
            table: "nodes".into(),
            arrow_ipc: vec![0xDE, 0xAD, 0xBE, 0xEF],
            graph: String::new(),
        }],
    };
    log.append(vec![record.clone()]).await.unwrap();
    let all = log.tail(LogPosition::ZERO).await.unwrap();
    assert_eq!(all[0].1, record);
}

#[allow(clippy::unwrap_used)]
#[tokio::test]
async fn empty_append_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let log = open(dir.path());
    assert!(matches!(
        log.append(vec![]).await,
        Err(LogError::EmptyAppend)
    ));
}

#[allow(clippy::unwrap_used)]
#[tokio::test]
async fn factory_builds_from_toml_and_requires_dir() {
    let dir = tempfile::tempdir().unwrap();
    let reg = log_registry();

    // toml::Value renders a correctly escaped TOML string for the path.
    let dir_toml = toml::Value::String(dir.path().display().to_string()).to_string();
    let cfg = Config::from_toml_str(&format!(
        "[log]\nbackend = \"local\"\n[log.local]\ndir = {dir_toml}\n"
    ))
    .unwrap()
    .section("log")
    .unwrap();
    let log = reg.build("local", &cfg, &BuildContext::empty()).unwrap();
    log.append(vec![rec(1)]).await.unwrap();
    assert_eq!(log.tail(LogPosition::ZERO).await.unwrap().len(), 1);

    // Missing [log.local] section gives an actionable build error.
    // `.unwrap_err()` needs `Arc<dyn Log>: Debug`, which `Log` does not
    // require, so extract the error via `match` instead (see memory_log.rs).
    let bare = Config::from_toml_str("[log]\nbackend = \"local\"")
        .unwrap()
        .section("log")
        .unwrap();
    let err = match reg.build("local", &bare, &BuildContext::empty()) {
        Ok(_) => panic!("expected build(\"local\") with no [log.local] to fail"),
        Err(e) => e.to_string(),
    };
    assert!(err.contains("log.local"), "{err}");
}
