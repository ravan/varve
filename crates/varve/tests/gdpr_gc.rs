#![allow(clippy::unwrap_used)]

use std::path::Path;
use varve::{Db, Instant};
use varve_testkit::db_harness::{
    compact_until_idle, local_gc_blocks_config as gc_config, row_count as rows,
    wait_for_manifest_count,
};

async fn graph_object_bytes(dir: &Path) -> Vec<u8> {
    let store = varve_storage::local_store(&dir.join("store")).unwrap();
    let mut bytes = Vec::new();
    for key in store.list("v1/graphs").await.unwrap() {
        if key.ends_with(".arrow") {
            bytes.extend_from_slice(&store.get(&key).await.unwrap());
        }
    }
    bytes
}

async fn assert_reinsert_visible_and_history_hidden(
    db: &Db,
    inserted_system_time: Instant,
    erased_system_time: Instant,
    old_token: &str,
    fresh_token: &str,
) {
    let current = db
        .query("MATCH (p:P {_id: 1}) RETURN p.token")
        .await
        .unwrap();
    assert_eq!(rows(&current), 1);

    let fresh = db
        .query(format!(
            "MATCH (p:P {{_id: 1}}) WHERE p.token = '{fresh_token}' RETURN p.token"
        ))
        .await
        .unwrap();
    assert_eq!(rows(&fresh), 1);

    let old = db
        .query(format!(
            "MATCH (p:P {{_id: 1}}) WHERE p.token = '{old_token}' RETURN p.token"
        ))
        .await
        .unwrap();
    assert_eq!(rows(&old), 0);

    for system_time in [inserted_system_time, erased_system_time] {
        let history = db
            .query(format!(
                "FOR SYSTEM_TIME AS OF TIMESTAMP '{}' MATCH (p:P {{_id: 1}}) RETURN p.token",
                system_time
            ))
            .await
            .unwrap();
        assert_eq!(rows(&history), 0);
    }
}

#[tokio::test]
async fn erased_property_bytes_absent_after_compaction_and_gc() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(gc_config(dir.path(), 1)).await.unwrap();
    let secret = "gdpr-secret-sentinel-8f2f1de0";

    db.execute(&format!("INSERT (:P {{_id: 1, token: '{secret}'}})"))
        .await
        .unwrap();
    db.execute("MATCH (p:P {_id: 1}) ERASE p").await.unwrap();
    db.execute("INSERT (:P {_id: 1, token: 'fresh-public'})")
        .await
        .unwrap();
    for id in 2..=62 {
        db.execute(&format!("INSERT (:P {{_id: {id}, token: 'filler-{id}'}})"))
            .await
            .unwrap();
    }
    wait_for_manifest_count(dir.path(), 64).await;

    let before = graph_object_bytes(dir.path()).await;
    assert!(String::from_utf8_lossy(&before).contains(secret));

    let compact = db.compact_once().await.unwrap();
    assert_eq!(compact.jobs, 1);
    db.gc_once().await.unwrap();

    let after = graph_object_bytes(dir.path()).await;
    assert!(!String::from_utf8_lossy(&after).contains(secret));
    let current = db
        .query("MATCH (p:P {_id: 1}) WHERE p.token = 'fresh-public' RETURN p.token")
        .await
        .unwrap();
    assert_eq!(rows(&current), 1);
}

#[tokio::test]
async fn post_erase_reinsert_survives_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(gc_config(dir.path(), 1)).await.unwrap();
    let old_token = "gdpr-reinsert-secret-sentinel-49181ba4";
    let fresh_token = "gdpr-reinsert-fresh-public";

    let inserted = db
        .execute(&format!("INSERT (:P {{_id: 1, token: '{old_token}'}})"))
        .await
        .unwrap();
    let erased = db.execute("MATCH (p:P {_id: 1}) ERASE p").await.unwrap();
    db.execute(&format!("INSERT (:P {{_id: 1, token: '{fresh_token}'}})"))
        .await
        .unwrap();
    for id in 2..=62 {
        db.execute(&format!("INSERT (:P {{_id: {id}, token: 'filler-{id}'}})"))
            .await
            .unwrap();
    }
    wait_for_manifest_count(dir.path(), 64).await;

    assert_reinsert_visible_and_history_hidden(
        &db,
        inserted.system_time,
        erased.system_time,
        old_token,
        fresh_token,
    )
    .await;

    let jobs = compact_until_idle(&db).await.unwrap();
    assert!(jobs >= 1);

    assert_reinsert_visible_and_history_hidden(
        &db,
        inserted.system_time,
        erased.system_time,
        old_token,
        fresh_token,
    )
    .await;

    drop(db);
    let restarted = Db::open(gc_config(dir.path(), 1)).await.unwrap();
    assert_reinsert_visible_and_history_hidden(
        &restarted,
        inserted.system_time,
        erased.system_time,
        old_token,
        fresh_token,
    )
    .await;
}
