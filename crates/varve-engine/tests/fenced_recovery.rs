#![allow(clippy::unwrap_used)]

mod common;

use common::{collect_i64_column, put_record, shared_registries};
use std::sync::Arc;
use varve_config::Config;
use varve_engine::Db;
use varve_log::{Log, ObjectStoreLog};
use varve_storage::ObjectStore;

async fn open_query_writer_db_over(store: Arc<dyn ObjectStore>) -> Db {
    let config = Config::from_toml_str(
        "[node]\nroles = [\"writer\", \"query\"]\n\
         [log]\nbackend = \"object-store\"\n\
         [storage]\nbackend = \"shared\"\n",
    )
    .unwrap();
    Db::open_with(&config, &shared_registries(store))
        .await
        .unwrap()
}

#[tokio::test]
async fn recovery_skips_fenced_records_and_replays_across_the_epoch_jump() {
    let store = varve_storage::memory_store();
    let log = ObjectStoreLog::new(Arc::clone(&store));
    log.append(vec![put_record(1, 1), put_record(2, 2)])
        .await
        .unwrap();
    // Zombie record at (0,2): fence epoch 0 at offset 2, then append it.
    store
        .put(
            &varve_storage::keys::epoch_fence_key(0),
            bytes::Bytes::from(
                serde_json::json!({
                    "epoch": 0, "fence_offset": 2, "fenced_by": "test", "fenced_at_us": 0
                })
                .to_string(),
            ),
        )
        .await
        .unwrap();
    log.append(vec![put_record(99, 99)]).await.unwrap(); // lands at (0,2) — DEAD
                                                         // epoch-1 successor record
    let log2 = ObjectStoreLog::new(Arc::clone(&store));
    log2.start_epoch(1).await.unwrap();
    log2.append(vec![put_record(3, 3)]).await.unwrap();

    let db = open_query_writer_db_over(store).await;
    let rows = db.query("MATCH (c:Chaos) RETURN c._id").await.unwrap();
    let ids = collect_i64_column(&rows);
    assert_eq!(ids, vec![1, 2, 3], "zombie _id 99 must NOT be visible");
}
