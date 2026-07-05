//! Backend integration matrix (spec §13.5; slice-5 exit criteria): storage
//! contract, object-store-log contract, Db end-to-end with restart, and the
//! capability probe with per-backend expected verdicts — against real S3
//! containers. Skips silently unless VARVE_S3_BACKENDS names the backend.
#![allow(clippy::unwrap_used)]

use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;
use varve::{Config, Db, ProbeVerdict};
use varve_log::{Log, LogRecord, ObjectStoreLog};
use varve_storage::{latest_manifest, ObjectStore, StorageError};
use varve_testkit::backends::{self, Backend, CONTRACT_BUCKET};
use varve_types::LogPosition;

/// What the probe must report per backend (D5). `NotSupported` asserts only
/// the operationally load-bearing fact — slice-10 cas-failover must refuse —
/// without pinning Unsupported vs Inconsistent; `RecordOnly` prints the
/// verdict so the first CI run can pin it.
/// AFTER THE FIRST OBSERVED RUN: tighten every RecordOnly/NotSupported to
/// the exact verdict seen, and record the final table in STATUS.md.
enum Expectation {
    Supported,
    NotSupported,
    RecordOnly,
}

fn expected_probe(name: &str) -> Expectation {
    match name {
        // D5: "Garage: never CAS".
        "garage" => Expectation::NotSupported,
        // D5: "SeaweedFS: unconfirmed/buggy CAS" — pin from the first run.
        // A Supported verdict here deserves skepticism before slice 10
        // trusts it (that is exactly what Inconsistent-detection is for).
        "seaweedfs" => Expectation::RecordOnly,
        // MinIO implements the standard HTTP preconditions.
        "minio" => Expectation::Supported,
        "ceph" => Expectation::RecordOnly,
        other => panic!("unknown backend '{other}'"),
    }
}

/// Mirror of varve-storage/tests/store_test.rs::exercise — duplicated
/// because varve-storage cannot dev-depend on varve-testkit (cycle).
async fn storage_contract(store: Arc<dyn ObjectStore>) {
    store
        .put("v1/a/one", Bytes::from_static(b"hello"))
        .await
        .unwrap();
    assert_eq!(
        store.get("v1/a/one").await.unwrap(),
        Bytes::from_static(b"hello")
    );
    store
        .put("v1/a/one", Bytes::from_static(b"world"))
        .await
        .unwrap();
    assert_eq!(
        store.get("v1/a/one").await.unwrap(),
        Bytes::from_static(b"world")
    );
    assert_eq!(
        store.get_range("v1/a/one", 1..4).await.unwrap(),
        Bytes::from_static(b"orl")
    );
    assert!(matches!(
        store.get("v1/a/absent").await,
        Err(StorageError::NotFound(k)) if k == "v1/a/absent"
    ));
    store
        .put("v1/a/two", Bytes::from_static(b"2"))
        .await
        .unwrap();
    store
        .put("v1/b/three", Bytes::from_static(b"3"))
        .await
        .unwrap();
    assert_eq!(
        store.list("v1/a").await.unwrap(),
        vec!["v1/a/one".to_string(), "v1/a/two".to_string()]
    );
    assert_eq!(store.list("v1/absent").await.unwrap(), Vec::<String>::new());
}

async fn log_contract(store: Arc<dyn ObjectStore>) {
    let rec = |tx_id: u64| LogRecord {
        tx_id,
        system_time_us: tx_id as i64,
        user: String::new(),
        effects: vec![],
    };
    let log = ObjectStoreLog::new(Arc::clone(&store));
    assert_eq!(
        log.append(vec![rec(1), rec(2)]).await.unwrap(),
        LogPosition::ZERO
    );
    assert_eq!(log.append(vec![rec(3)]).await.unwrap().offset(), 2);
    // A fresh handle (= restart) continues after the last object.
    let reopened = ObjectStoreLog::new(Arc::clone(&store));
    assert_eq!(reopened.append(vec![rec(4)]).await.unwrap().offset(), 3);
    let all = reopened.tail(LogPosition::ZERO).await.unwrap();
    assert_eq!(
        all.iter().map(|(_, r)| r.tx_id).collect::<Vec<_>>(),
        vec![1, 2, 3, 4]
    );
    // trim is a no-op on object-store logs (GC = slice 8).
    log.trim(LogPosition::from_u64(u64::MAX)).await.unwrap();
    assert_eq!(reopened.tail(LogPosition::ZERO).await.unwrap().len(), 4);
}

/// storage = "s3" + log = "object-store", flush to blocks, restart, query —
/// every durable byte lives on the backend.
async fn db_end_to_end(backend: &Backend) {
    let toml = format!(
        "[log]\nbackend = \"object-store\"\n{}",
        backend.params.storage_toml_with("max_block_rows = 2\n")
    );
    let store = backend.params.store();
    {
        let db = Db::open(Config::from_toml_str(&toml).unwrap())
            .await
            .unwrap();
        for i in 1..=4 {
            db.execute(&format!("INSERT (:Person {{_id: {i}, name: 'p{i}'}})"))
                .await
                .unwrap();
        }
        // Flush runs after acks — wait for ≥1 committed block manifest.
        let mut flushed = false;
        for _ in 0..240 {
            if latest_manifest(store.as_ref()).await.unwrap().is_some() {
                flushed = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        assert!(flushed, "no block manifest appeared on {}", backend.name);
        // One more tx that stays in the log tail past the watermark.
        db.execute("INSERT (:Person {_id: 5, name: 'p5'})")
            .await
            .unwrap();
    }
    let db = Db::open(Config::from_toml_str(&toml).unwrap())
        .await
        .unwrap();
    let batches = db.query("MATCH (p:Person) RETURN p.name").await.unwrap();
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 5, "blocks + log-tail replay from {}", backend.name);
}

async fn probe_phase(backend: &Backend) {
    let store = backend.params.with_bucket(CONTRACT_BUCKET).store();
    let key = format!(
        "v1/probe/{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros()
    );
    let report = varve_storage::probe_conditional_put(store.as_ref(), &key)
        .await
        .unwrap();
    eprintln!("[{}] capability probe: {:?}", backend.name, report.verdict);
    match expected_probe(backend.name) {
        Expectation::Supported => assert_eq!(report.verdict, ProbeVerdict::Supported),
        Expectation::NotSupported => assert!(
            !matches!(report.verdict, ProbeVerdict::Supported),
            "cas-failover must refuse on {}: got {:?}",
            backend.name,
            report.verdict
        ),
        Expectation::RecordOnly => {}
    }
}

async fn full_suite(name: &'static str) {
    if !backends::enabled(name) {
        eprintln!("skipping {name} (set VARVE_S3_BACKENDS={name} to run)");
        return;
    }
    let backend = backends::start(name).await;
    let contract_store = backend.params.with_bucket(CONTRACT_BUCKET).store();
    storage_contract(Arc::clone(&contract_store)).await;
    log_contract(contract_store).await;
    db_end_to_end(&backend).await;
    probe_phase(&backend).await;
}

#[tokio::test]
async fn garage_matrix() {
    full_suite("garage").await;
}

#[tokio::test]
async fn seaweedfs_matrix() {
    full_suite("seaweedfs").await;
}

#[tokio::test]
async fn minio_matrix() {
    full_suite("minio").await;
}

#[tokio::test]
async fn ceph_matrix() {
    full_suite("ceph").await;
}
