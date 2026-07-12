#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Task 9: the whole-workspace failover checkpoint. Two sequential
//! `cas-failover` writer `Db`s share one in-memory store (Task 6's
//! `SharedStoreFactory` pattern) standing in for "two processes, one
//! bucket" — writer A commits, "crashes" (drops without releasing its
//! lease), writer B stands up, seizes the lease, fences A's abandoned
//! epoch, and recovers. A raw `ObjectStoreLog` handle primed BEFORE the
//! takeover plays the zombie: its cached cursor is stale, so its late
//! append lands durably at a now-fenced position — proving "durable but
//! dead", not "silently dropped".

mod common;

use common::{cas_config, collect_i64_column, put_record, shared_registries};
use std::sync::Arc;
use std::time::Duration;
use varve_engine::{Db, EngineError};
use varve_log::{Log, ObjectStoreLog};
use varve_storage::ObjectStore;
use varve_types::LogPosition;

/// The wire key `coord::cas::CasFailover` writes its lease document under.
/// Not importable (the coordinator module is `pub(crate)`), so this test
/// reconstructs the JSON shape by hand to drive a raw, competing seizure —
/// exactly what a second real coordinator instance would do.
const LEASE_KEY: &str = "v1/lease.json";

#[tokio::test]
async fn failover_preserves_acked_txs_and_fences_the_zombie() {
    let store: Arc<dyn ObjectStore> = varve_storage::memory_store();
    let registries = shared_registries(Arc::clone(&store));

    // Writer A: commit 3 acked txs.
    let a = Db::open_with(&cas_config(), &registries).await.unwrap();
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

    // A "crashes": dropping it stops its heartbeat task WITHOUT releasing
    // the lease (a real release would be a clean shutdown, not a crash).
    drop(a);

    // Writer B: standby-acquires, detects the stale lease, seizes it, fences
    // A's abandoned epoch 0 at offset 3, and recovers — measure the whole
    // thing end to end.
    let started = std::time::Instant::now();
    let b = Db::open_with(&cas_config(), &registries).await.unwrap();
    let takeover = started.elapsed();
    assert!(
        takeover < Duration::from_secs(10),
        "takeover took {takeover:?}, want < 10s"
    );

    // Zero acked-tx loss: B recovers exactly A's 3 committed rows.
    let rows = b.query("MATCH (p:Chaos) RETURN p._id").await.unwrap();
    assert_eq!(collect_i64_column(&rows), vec![1, 2, 3]);

    // B commits in its new epoch.
    let receipt = b.execute("INSERT (:Chaos {_id: 4})").await.unwrap();

    // The zombie's late append lands at ITS stale cached position — durable
    // (the object really is written), but DEAD (behind the fence B wrote at
    // seizure time).
    let zombie_position = zombie.append(vec![put_record(999, 999)]).await.unwrap();
    assert_eq!(
        zombie_position,
        LogPosition::new(0, 3).unwrap(),
        "the zombie must land exactly at its stale pre-takeover cursor"
    );
    let zombie_key = varve_storage::keys::log_key(zombie_position);
    assert!(
        store.get(&zombie_key).await.is_ok(),
        "the zombie append must be durable (a real object at {zombie_key}), not silently dropped"
    );

    // B (writer+query) never sees the zombie's row…
    let rows = b.query("MATCH (p:Chaos) RETURN p._id").await.unwrap();
    assert_eq!(
        collect_i64_column(&rows),
        vec![1, 2, 3, 4],
        "zombie _id 999 must be invisible"
    );
    // …verify walks clean over it (fence-aware, Task 5)…
    b.verify().await.unwrap();

    // …and a fresh query-only node, told to wait for B's last commit, also
    // agrees — the follower jumps the fence exactly as B's own recovery did.
    let q_cfg = common::writer_config("[node]\nroles = [\"query\"]");
    let q = Db::open_with(&q_cfg, &registries).await.unwrap();
    let rows = q
        .query("MATCH (p:Chaos) RETURN p._id")
        .basis(receipt)
        .basis_timeout(Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(collect_i64_column(&rows), vec![1, 2, 3, 4]);
}

#[tokio::test]
async fn the_seized_writer_fences_instead_of_acking() {
    let store: Arc<dyn ObjectStore> = varve_storage::memory_store();
    let registries = shared_registries(Arc::clone(&store));

    let a = Db::open_with(&cas_config(), &registries).await.unwrap();
    a.execute("INSERT (:Chaos {_id: 1})").await.unwrap();

    // Seize the lease out from under A directly (raw CAS on the store) —
    // exactly what a competing standby's `acquire()` does under the hood.
    seize_lease_directly(&store).await;

    // A's heartbeat task discovers the loss within ~2 intervals; until then,
    // acks may legitimately still land (pre-fence-discovery window) — the
    // assertion is that fencing HAPPENS, not that it is instant.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        match a.execute("INSERT (:Chaos {_id: 2})").await {
            Err(EngineError::WriterFenced(_)) => break,
            Ok(_) if std::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            other => panic!("expected WriterFenced before the deadline, got {other:?}"),
        }
    }
    assert!(
        a.follower_error().is_some(),
        "the fenced writer must report itself fatally broken on the progress watch"
    );
}

/// Reads the current lease document, then overwrites it with a foreign
/// holder at `epoch + 1` via `put_if_matches` on its etag — retrying if A's
/// own heartbeat renews the lease first (the same contention a real
/// competing coordinator would have to win against).
async fn seize_lease_directly(store: &Arc<dyn ObjectStore>) {
    let conditional = store
        .conditional()
        .expect("the memory store supports conditional writes (slice-5 verified)");
    for _ in 0..100 {
        let (bytes, etag) = conditional
            .get_versioned(LEASE_KEY)
            .await
            .unwrap()
            .expect("writer A must have created the lease before this runs");
        let doc: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let epoch = doc["epoch"].as_u64().expect("lease doc has an epoch field");
        let seized = serde_json::json!({
            "holder": "zombie-standby",
            "address": "",
            "epoch": epoch + 1,
            "heartbeat_us": 0,
        });
        let payload = bytes::Bytes::from(seized.to_string());
        match conditional
            .put_if_matches(LEASE_KEY, payload, &etag)
            .await
            .unwrap()
        {
            varve_storage::CondPut::Stored { .. } => return,
            // A's own heartbeat renewed the lease between our read and our
            // write — retry against the fresh etag, same as a real
            // competing coordinator would.
            varve_storage::CondPut::PreconditionFailed | varve_storage::CondPut::AlreadyExists => {
                continue
            }
            other => panic!("unexpected conditional-write outcome: {other:?}"),
        }
    }
    panic!("could not seize the lease against A's heartbeat within 100 attempts");
}
