#![allow(clippy::unwrap_used)]

use std::path::Path;
use std::time::Duration;
use varve::{Db, Instant};
use varve_testkit::db_harness::{
    compact_until_idle, local_blocks_config as blocks_config,
    local_gc_blocks_config as gc_blocks_config, wait_for_manifest_count,
};

fn manifest_count(dir: &Path) -> usize {
    dir.join("store")
        .join("v1")
        .join("blocks")
        .read_dir()
        .map(|entries| entries.count())
        .unwrap_or(0)
}

async fn wait_for_l0_data_count(dir: &Path, count: usize) {
    let store = varve_storage::local_store(&dir.join("store")).unwrap();
    for _ in 0..200 {
        let got = store
            .list("v1/graphs")
            .await
            .unwrap()
            .into_iter()
            .filter(|key| key.contains("/data/l00-rc-b"))
            .count();
        if got >= count {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("expected at least {count} L0 data objects");
}

fn column_i64(batches: &[varve::RecordBatch], name: &str) -> Vec<i64> {
    let mut out = Vec::new();
    for batch in batches {
        let Some(column) = batch.column_by_name(name) else {
            continue;
        };
        let values: &arrow::array::Int64Array = column.as_any().downcast_ref().unwrap();
        for row in 0..values.len() {
            out.push(values.value(row));
        }
    }
    out.sort_unstable();
    out
}

async fn query_i64(db: &Db, gql: &str, name: &str) -> Vec<i64> {
    column_i64(&db.query(gql).await.unwrap(), name)
}

#[tokio::test]
async fn compaction_preserves_node_query_results() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(blocks_config(dir.path(), 1)).await.unwrap();

    let first = db
        .execute("INSERT (:P {_id: 1, v: 1})")
        .await
        .unwrap()
        .system_time;
    wait_for_manifest_count(dir.path(), 1).await;
    for v in 2..=64 {
        db.execute(&format!("INSERT (:P {{_id: 1, v: {v}}})"))
            .await
            .unwrap();
        wait_for_manifest_count(dir.path(), v as usize).await;
    }

    let current = "MATCH (p:P) RETURN p.v AS v";
    let historical = historical_query(first);
    let before_current = query_i64(&db, current, "v").await;
    let before_historical = query_i64(&db, &historical, "v").await;

    compact_until_idle(&db).await.unwrap();

    assert_eq!(query_i64(&db, current, "v").await, before_current);
    assert_eq!(query_i64(&db, &historical, "v").await, before_historical);
}

#[tokio::test]
async fn compaction_preserves_adjacency_traversal_results() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(blocks_config(dir.path(), 1)).await.unwrap();

    let nodes = (1..=65)
        .map(|id| format!("(:P {{_id: {id}}})"))
        .collect::<Vec<_>>()
        .join(", ");
    db.execute(&format!("INSERT {nodes}")).await.unwrap();
    wait_for_manifest_count(dir.path(), 1).await;

    for dst in 2..=65 {
        db.execute(&format!(
            "MATCH (a:P {{_id: 1}}), (b:P {{_id: {dst}}}) INSERT (a)-[:K]->(b)"
        ))
        .await
        .unwrap();
        wait_for_manifest_count(dir.path(), dst as usize).await;
    }

    let gql = "MATCH (a:P)-[:K]->(b:P) WHERE a._id = 1 RETURN b._id AS id";
    let before = query_i64(&db, gql, "id").await;

    compact_until_idle(&db).await.unwrap();

    assert_eq!(query_i64(&db, gql, "id").await, before);
}

#[tokio::test]
async fn compaction_preserves_delete_and_erase_query_results() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(blocks_config(dir.path(), 1)).await.unwrap();

    for id in 1..=62 {
        db.execute(&format!("INSERT (:P {{_id: {id}}})"))
            .await
            .unwrap();
        wait_for_manifest_count(dir.path(), id as usize).await;
    }
    db.execute("MATCH (p:P) WHERE p._id = 2 DELETE p")
        .await
        .unwrap();
    wait_for_manifest_count(dir.path(), 63).await;
    db.execute("MATCH (p:P) WHERE p._id = 3 ERASE p")
        .await
        .unwrap();
    wait_for_manifest_count(dir.path(), 64).await;

    let gql = "MATCH (p:P) RETURN p._id AS id";
    let before = query_i64(&db, gql, "id").await;

    compact_until_idle(&db).await.unwrap();

    assert_eq!(query_i64(&db, gql, "id").await, before);
    assert!(!before.contains(&2));
    assert!(!before.contains(&3));
}

fn historical_query(system_time: Instant) -> String {
    format!(
        "FOR SYSTEM_TIME AS OF TIMESTAMP '{}' MATCH (p:P) RETURN p.v AS v",
        system_time
    )
}

async fn block_store_object_count(dir: &Path) -> usize {
    let store = varve_storage::local_store(&dir.join("store")).unwrap();
    store.list("v1/graphs").await.unwrap().len() + store.list("v1/blocks").await.unwrap().len()
}

#[tokio::test]
async fn storage_object_count_plateaus_under_update_churn() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(gc_blocks_config(dir.path(), 1)).await.unwrap();
    let mut max_objects = 0;
    let mut latest = 0;

    for cycle in 0..5 {
        let baseline_manifests = manifest_count(dir.path());
        for offset in 0..64 {
            latest = cycle * 64 + offset + 1;
            db.execute(&format!("INSERT (:P {{_id: {latest}, v: {latest}}})"))
                .await
                .unwrap();
        }
        wait_for_manifest_count(dir.path(), baseline_manifests + 64).await;
        wait_for_l0_data_count(dir.path(), 64).await;
        compact_until_idle(&db).await.unwrap();
        db.gc_once().await.unwrap();
        let objects = block_store_object_count(dir.path()).await;
        max_objects = max_objects.max(objects);
        assert!(objects <= 12, "cycle {cycle} left {objects} objects");
    }

    let values = query_i64(&db, "MATCH (p:P) RETURN p.v AS v", "v").await;
    assert_eq!(values.len(), 320);
    assert_eq!(values.last().copied(), Some(latest as i64));
    assert!(max_objects <= 12, "max object count was {max_objects}");
}
