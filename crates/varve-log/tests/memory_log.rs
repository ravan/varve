use varve_config::BuildContext;
use varve_log::{log_registry, Log, LogError, LogRecord, MemoryLog};
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

#[tokio::test]
async fn append_assigns_consecutive_positions_per_record() {
    let log = MemoryLog::new();
    // batch of 2, then batch of 1 — positions 0,1,2; append returns the first
    assert_eq!(log.append(vec![rec(1), rec(2)]).await.unwrap(), pos(0));
    assert_eq!(log.append(vec![rec(3)]).await.unwrap(), pos(2));

    let all = log.tail(LogPosition::ZERO).await.unwrap();
    assert_eq!(
        all.iter().map(|(p, r)| (*p, r.tx_id)).collect::<Vec<_>>(),
        vec![(pos(0), 1), (pos(1), 2), (pos(2), 3)]
    );
}

#[tokio::test]
async fn read_range_is_half_open() {
    let log = MemoryLog::new();
    log.append(vec![rec(1), rec(2), rec(3)]).await.unwrap();
    let mid = log.read_range(pos(1), pos(2)).await.unwrap();
    assert_eq!(mid.len(), 1);
    assert_eq!(mid[0].1.tx_id, 2);
    assert!(log.read_range(pos(3), pos(10)).await.unwrap().is_empty());
}

#[tokio::test]
async fn tail_from_midpoint() {
    let log = MemoryLog::new();
    log.append(vec![rec(1), rec(2), rec(3)]).await.unwrap();
    let tail = log.tail(pos(1)).await.unwrap();
    assert_eq!(tail.len(), 2);
    assert_eq!(tail[0].1.tx_id, 2);
}

#[tokio::test]
async fn empty_append_is_rejected() {
    let log = MemoryLog::new();
    assert!(matches!(
        log.append(vec![]).await,
        Err(LogError::EmptyAppend)
    ));
}

#[tokio::test]
async fn registry_builds_memory_by_name_and_lists_available_on_unknown() {
    use varve_config::Config;
    let reg = log_registry();
    let cfg = Config::from_toml_str("[log]\nbackend = \"memory\"")
        .unwrap()
        .section("log")
        .unwrap();
    let log = reg.build("memory", &cfg, &BuildContext::empty()).unwrap();
    log.append(vec![rec(1)]).await.unwrap();
    assert_eq!(log.tail(LogPosition::ZERO).await.unwrap().len(), 1);

    // `.unwrap_err()` needs `Arc<dyn Log>: Debug`, which `Log` does not
    // require (unlike the `Greeter: Debug` example in registry_test.rs), so
    // extract the error via `match` instead.
    let err = match reg.build("kafka", &cfg, &BuildContext::empty()) {
        Ok(_) => panic!("expected build(\"kafka\") to fail"),
        Err(e) => e.to_string(),
    };
    assert!(err.contains("kafka"), "{err}");
    assert!(err.contains("memory"), "{err}");
}
