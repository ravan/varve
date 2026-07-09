// The object-store log lives behind the (default-on) `object-store` feature;
// gate the whole integration test so `cargo test -p varve-log
// --no-default-features` compiles (this file becomes an empty crate) rather
// than failing on the feature-gated imports below.
#![cfg(feature = "object-store")]
#![allow(clippy::unwrap_used)]
use bytes::Bytes;
use std::ops::Range;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use varve_config::{BuildContext, ConfigSection};
use varve_log::{log_registry, Log, LogError, LogRecord, ObjectStoreLog};
use varve_storage::{keys, memory_store, ObjectStore, StorageError};
use varve_types::LogPosition;

fn rec(tx_id: u64) -> LogRecord {
    LogRecord {
        tx_id,
        system_time_us: tx_id as i64,
        user: String::new(),
        effects: vec![],
    }
}

fn tx_ids(records: &[(LogPosition, LogRecord)]) -> Vec<u64> {
    records.iter().map(|(_, r)| r.tx_id).collect()
}

#[tokio::test]
async fn append_assigns_consecutive_positions_across_batches() {
    let log = ObjectStoreLog::new(memory_store());
    assert_eq!(
        log.append(vec![rec(1), rec(2)]).await.unwrap(),
        LogPosition::ZERO
    );
    assert_eq!(log.append(vec![rec(3)]).await.unwrap().offset(), 2);
    let all = log.tail(LogPosition::ZERO).await.unwrap();
    assert_eq!(tx_ids(&all), vec![1, 2, 3]);
    assert_eq!(
        all.iter().map(|(p, _)| p.offset()).collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
}

#[tokio::test]
async fn one_object_per_batch_under_the_spec_keys() {
    let store = memory_store();
    let log = ObjectStoreLog::new(Arc::clone(&store));
    log.append(vec![rec(1), rec(2)]).await.unwrap();
    log.append(vec![rec(3)]).await.unwrap();
    assert_eq!(
        store.list(keys::LOG_PREFIX).await.unwrap(),
        vec![
            "v1/log/0000/00.vlog".to_string(),
            "v1/log/0000/02.vlog".to_string()
        ]
    );
}

#[tokio::test]
async fn a_fresh_handle_continues_after_the_last_object() {
    let store = memory_store();
    ObjectStoreLog::new(Arc::clone(&store))
        .append(vec![rec(1), rec(2)])
        .await
        .unwrap();
    // A restart = a new handle over the same store: the lazy open scan
    // (list + count the last object's frames) restores the position.
    let reopened = ObjectStoreLog::new(Arc::clone(&store));
    assert_eq!(reopened.append(vec![rec(3)]).await.unwrap().offset(), 2);
    assert_eq!(
        tx_ids(&reopened.tail(LogPosition::ZERO).await.unwrap()),
        vec![1, 2, 3]
    );
}

#[tokio::test]
async fn survives_reopen_on_a_local_fs_store() {
    // Durability = the backing store's: a local-FS store round-trips the
    // log across a process-restart-equivalent (fresh store + fresh log).
    let dir = tempfile::tempdir().unwrap();
    {
        let store = varve_storage::local_store(dir.path()).unwrap();
        ObjectStoreLog::new(store)
            .append(vec![rec(1), rec(2)])
            .await
            .unwrap();
    }
    let store = varve_storage::local_store(dir.path()).unwrap();
    let log = ObjectStoreLog::new(store);
    assert_eq!(
        tx_ids(&log.tail(LogPosition::ZERO).await.unwrap()),
        vec![1, 2]
    );
    assert_eq!(log.append(vec![rec(3)]).await.unwrap().offset(), 2);
}

/// Counts backend object reads so the range test can pin object skipping.
struct CountingStore {
    inner: Arc<dyn ObjectStore>,
    gets: AtomicUsize,
}

#[async_trait::async_trait]
impl ObjectStore for CountingStore {
    async fn put(&self, key: &str, bytes: Bytes) -> Result<(), StorageError> {
        self.inner.put(key, bytes).await
    }
    async fn get(&self, key: &str) -> Result<Bytes, StorageError> {
        self.gets.fetch_add(1, Ordering::SeqCst);
        self.inner.get(key).await
    }
    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Bytes, StorageError> {
        self.inner.get_range(key, range).await
    }
    async fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
        self.inner.list(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), StorageError> {
        self.inner.delete(key).await
    }
}

#[tokio::test]
async fn read_range_filters_and_skips_disjoint_objects() {
    let counting = Arc::new(CountingStore {
        inner: memory_store(),
        gets: AtomicUsize::new(0),
    });
    let log = ObjectStoreLog::new(Arc::clone(&counting) as Arc<dyn ObjectStore>);
    log.append(vec![rec(1), rec(2)]).await.unwrap(); // positions 0,1
    log.append(vec![rec(3), rec(4)]).await.unwrap(); // positions 2,3
    log.append(vec![rec(5)]).await.unwrap(); // position 4
    counting.gets.store(0, Ordering::SeqCst);

    let mid = log
        .read_range(LogPosition::from_u64(3), LogPosition::from_u64(5))
        .await
        .unwrap();
    assert_eq!(tx_ids(&mid), vec![4, 5]);
    // Object 1 (positions 0–1) is provably below the range and is never
    // fetched; objects 2 and 3 are.
    assert_eq!(counting.gets.load(Ordering::SeqCst), 2);

    let empty = log
        .read_range(LogPosition::from_u64(5), LogPosition::from_u64(100))
        .await
        .unwrap();
    assert!(empty.is_empty());
}

#[tokio::test]
async fn trim_is_a_noop_and_positions_never_regress() {
    let log = ObjectStoreLog::new(memory_store());
    log.append(vec![rec(1), rec(2)]).await.unwrap();
    log.trim(LogPosition::from_u64(u64::MAX)).await.unwrap();
    // Nothing removed (no delete in the sovereign trait; GC = slice 8)…
    assert_eq!(
        tx_ids(&log.tail(LogPosition::ZERO).await.unwrap()),
        vec![1, 2]
    );
    // …and the sequence continues where it left off.
    assert_eq!(log.append(vec![rec(3)]).await.unwrap().offset(), 2);
}

#[tokio::test]
async fn corrupt_object_is_a_hard_error() {
    let store = memory_store();
    let log = ObjectStoreLog::new(Arc::clone(&store));
    log.append(vec![rec(1)]).await.unwrap();
    // Object PUTs are atomic, so damage is never a recoverable torn tail —
    // decode is strict.
    store
        .put(
            &keys::log_key(LogPosition::from_u64(1)),
            Bytes::from_static(b"\xFF\xFF\xFF"),
        )
        .await
        .unwrap();
    assert!(matches!(
        log.tail(LogPosition::ZERO).await,
        Err(LogError::Corrupt { .. })
    ));
}

#[tokio::test]
async fn empty_append_is_rejected() {
    let log = ObjectStoreLog::new(memory_store());
    assert!(matches!(
        log.append(vec![]).await,
        Err(LogError::EmptyAppend)
    ));
}

#[tokio::test]
async fn factory_requires_the_storage_component() {
    let reg = log_registry();
    assert_eq!(reg.names(), vec!["local", "memory", "object-store"]);
    let err = match reg.build(
        "object-store",
        &ConfigSection::empty(),
        &BuildContext::empty(),
    ) {
        Ok(_) => panic!("expected build without a storage component to fail"),
        Err(e) => e.to_string(),
    };
    assert!(err.contains("storage component"), "{err}");
}

#[tokio::test]
async fn factory_builds_with_the_storage_component() {
    let mut ctx = BuildContext::empty();
    ctx.insert(memory_store());
    let log = log_registry()
        .build("object-store", &ConfigSection::empty(), &ctx)
        .unwrap();
    log.append(vec![rec(1)]).await.unwrap();
    assert_eq!(log.tail(LogPosition::ZERO).await.unwrap().len(), 1);
}
