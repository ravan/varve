#![allow(clippy::unwrap_used)]

//! Task 6: `Coordinator` wired through `Db::open_with` — the writer-role
//! startup guard, the heartbeat lifecycle, and the `cas-failover` config
//! error that fires before the (not-yet-registered, Task 7) registry lookup.

mod common;

use common::{shared_registries, writer_config};
use varve_config::Config;
use varve_engine::{Db, EngineError};

#[tokio::test]
async fn second_advertised_writer_is_refused_while_the_heartbeat_is_fresh() {
    let store = varve_storage::memory_store();
    let a = Db::open_with(
        &writer_config("[coordinator]\nheartbeat_interval_ms = 100\ntakeover_after_ms = 300"),
        &shared_registries(store.clone()),
    )
    .await
    .unwrap();
    a.publish_writer("http://a:1").await.unwrap();

    let err = Db::open_with(
        &writer_config("[coordinator]\nheartbeat_interval_ms = 100\ntakeover_after_ms = 300"),
        &shared_registries(store.clone()),
    )
    .await
    .unwrap_err();
    assert!(matches!(err, EngineError::WriterActive { .. }), "{err}");

    // ...and once the heartbeat goes stale (drop A, wait past takeover_after),
    // a new writer starts fine.
    drop(a);
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    Db::open_with(
        &writer_config("[coordinator]\nheartbeat_interval_ms = 100\ntakeover_after_ms = 300"),
        &shared_registries(store),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn an_unadvertised_writer_restarts_immediately_without_a_guard() {
    let store = varve_storage::memory_store();
    let cfg = writer_config("[coordinator]\nheartbeat_interval_ms = 100\ntakeover_after_ms = 300");
    let a = Db::open_with(&cfg, &shared_registries(store.clone()))
        .await
        .unwrap();
    a.execute("INSERT (:P {_id: 1})").await.unwrap();
    drop(a);
    // no publish_writer ⇒ no advertisement ⇒ immediate restart is fine
    let b = Db::open_with(&cfg, &shared_registries(store))
        .await
        .unwrap();
    let rows = b.query("MATCH (p:P) RETURN p._id").await.unwrap();
    assert_eq!(rows.iter().map(|batch| batch.num_rows()).sum::<usize>(), 1);
}

#[tokio::test]
async fn cas_failover_without_the_object_store_log_is_a_config_error() {
    let cfg = Config::from_toml_str(
        "[storage]\nbackend = \"memory\"\n[coordinator]\nbackend = \"cas-failover\"\n",
    )
    .unwrap(); // [log] defaults to "memory"
    let err = Db::open(cfg).await.unwrap_err();
    assert!(matches!(err, EngineError::CasRequiresSharedLog(name) if name == "memory"));
}
