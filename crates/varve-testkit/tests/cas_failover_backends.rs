//! Task 9 (roadmap failover exit criteria): live-backend gates for
//! cas-failover. Mirrors `backend_matrix.rs`'s env gating and container
//! lifecycle exactly — skips silently unless `VARVE_S3_BACKENDS` names the
//! backend.
//!
//! garage → `Db::open` over a live bucket with `[coordinator] backend =
//! "cas-failover"` must refuse with `EngineError::CasUnsupported`, naming
//! the probe's actual reason (Garage's create-if-absent precondition is
//! ignored — D5, spec §12) — the roadmap's "actionable error naming the
//! backend capability".
//! minio   → the full in-process takeover flow from `varve-engine`'s
//! `failover.rs` (writer A commits, crashes, writer B seizes/fences/
//! recovers, a zombie's late append lands durably but dead) runs over a
//! real bucket instead of a shared in-memory store.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;
use varve::{Config, Db, EngineError};
use varve_index::{encode_events, Event, Op};
use varve_log::{Log, LogRecord, ObjectStoreLog, TableEffects};
use varve_storage::ObjectStore;
use varve_testkit::backends::{self, Backend};
use varve_testkit::oracle::column_i64;
use varve_types::{Instant, LogPosition, Value};

fn cas_toml(backend: &Backend) -> String {
    format!(
        "[log]\nbackend = \"object-store\"\n[coordinator]\nbackend = \"cas-failover\"\n\
         heartbeat_interval_ms = 200\ntakeover_after_ms = 1000\n{}",
        backend.params.storage_toml()
    )
}

/// One INSERT-equivalent effect event — a `:Chaos`-labeled node carrying
/// `_id: id` (mirrors `varve-engine/tests/common/mod.rs::put_record`;
/// duplicated here since live-backend tests in this crate stand alone).
fn put_record(tx_id: u64, id: i64) -> LogRecord {
    let mut doc = varve_types::Doc::new();
    doc.insert("_id".to_string(), Value::Int(id));
    let iid = varve_types::Iid::derive("default", "nodes", &Value::Int(id).id_bytes().unwrap());
    let event = Event {
        iid,
        system_from: Instant::from_micros(tx_id as i64),
        valid_from: Instant::from_micros(tx_id as i64),
        valid_to: Instant::END_OF_TIME,
        src: None,
        dst: None,
        op: Op::Put {
            labels: vec!["Chaos".into()],
            doc,
        },
    };
    LogRecord {
        tx_id,
        system_time_us: tx_id as i64,
        user: String::new(),
        effects: vec![TableEffects {
            graph: String::new(),
            table: "nodes".into(),
            arrow_ipc: encode_events(&[event]).unwrap(),
        }],
    }
}

async fn garage_refuses_cas_failover() {
    if !backends::enabled("garage") {
        eprintln!("skipping garage (set VARVE_S3_BACKENDS=garage to run)");
        return;
    }
    let backend = backends::start("garage").await;
    let toml = cas_toml(&backend);
    let err = Db::open(Config::from_toml_str(&toml).unwrap())
        .await
        .expect_err("garage lacks real conditional-write semantics; cas-failover must refuse");
    assert!(
        matches!(err, EngineError::CasUnsupported { .. }),
        "expected EngineError::CasUnsupported, got {err:?}"
    );
    let message = err.to_string();
    assert!(
        message.contains("precondition ignored"),
        "the refusal must name the probe's actual reason, got: {message}"
    );
    eprintln!("[garage] refused cas-failover: {message}");
}

async fn minio_takeover_over_a_live_bucket() {
    if !backends::enabled("minio") {
        eprintln!("skipping minio (set VARVE_S3_BACKENDS=minio to run)");
        return;
    }
    let backend = backends::start("minio").await;
    let toml = cas_toml(&backend);
    let store: Arc<dyn ObjectStore> = backend.params.store();

    // Writer A: commit 3 acked txs against the live bucket.
    let a = Db::open(Config::from_toml_str(&toml).unwrap())
        .await
        .unwrap();
    for n in 1..=3i64 {
        a.execute(&format!("INSERT (:Chaos {{_id: {n}}})"))
            .await
            .unwrap();
    }

    // Prime the zombie BEFORE the takeover: a raw log handle whose `head()`
    // lazily caches the pre-takeover cursor.
    let zombie = ObjectStoreLog::new(Arc::clone(&store));
    let stale_head = zombie.head().await.unwrap();
    assert_eq!(stale_head, LogPosition::new(0, 3).unwrap());

    // A "crashes": drop without releasing the lease.
    drop(a);

    // Writer B: standby-acquires, seizes, fences, recovers — over the real
    // bucket, network round trips included.
    let started = std::time::Instant::now();
    let b = Db::open(Config::from_toml_str(&toml).unwrap())
        .await
        .unwrap();
    let takeover = started.elapsed();
    assert!(
        takeover < Duration::from_secs(10),
        "takeover took {takeover:?}, want < 10s"
    );

    // Zero acked-tx loss.
    let rows = b
        .query("MATCH (p:Chaos) RETURN p._id AS _id")
        .await
        .unwrap();
    let mut ids = column_i64(&rows, "_id");
    ids.sort_unstable();
    assert_eq!(ids, vec![1, 2, 3]);

    // B commits in its new epoch.
    let receipt = b.execute("INSERT (:Chaos {_id: 4})").await.unwrap();

    // The zombie's late append lands at its stale cached position — durable
    // (a real object on the live bucket), but DEAD (behind B's fence).
    let zombie_position = zombie.append(vec![put_record(999, 999)]).await.unwrap();
    assert_eq!(
        zombie_position,
        LogPosition::new(0, 3).unwrap(),
        "the zombie must land exactly at its stale pre-takeover cursor"
    );
    let zombie_key = varve_storage::keys::log_key(zombie_position);
    assert!(
        store.get(&zombie_key).await.is_ok(),
        "the zombie append must be durable on the live bucket, not silently dropped"
    );

    // B never sees it…
    let rows = b
        .query("MATCH (p:Chaos) RETURN p._id AS _id")
        .await
        .unwrap();
    let mut ids = column_i64(&rows, "_id");
    ids.sort_unstable();
    assert_eq!(ids, vec![1, 2, 3, 4], "zombie _id 999 must be invisible");
    // …verify walks clean over it…
    b.verify().await.unwrap();

    // …and a fresh query-only node, waiting on B's last commit, agrees too.
    let q_toml = format!(
        "[log]\nbackend = \"object-store\"\n[node]\nroles = [\"query\"]\n{}",
        backend.params.storage_toml()
    );
    let q = Db::open(Config::from_toml_str(&q_toml).unwrap())
        .await
        .unwrap();
    let rows = q
        .query("MATCH (p:Chaos) RETURN p._id AS _id")
        .basis(receipt)
        .basis_timeout(Duration::from_secs(5))
        .await
        .unwrap();
    let mut ids = column_i64(&rows, "_id");
    ids.sort_unstable();
    assert_eq!(ids, vec![1, 2, 3, 4]);
}

#[tokio::test]
async fn garage_cas_failover_gate() {
    garage_refuses_cas_failover().await;
}

#[tokio::test]
async fn minio_cas_failover_gate() {
    minio_takeover_over_a_live_bucket().await;
}
