//! Slice 10 demo (roadmap failover exit criteria): kill the writer, watch a
//! standby take over in under 10 seconds with zero acked-tx loss, and prove
//! a zombie writer's late append is durable but provably dead.
//!
//! Uses only the `varve` crate's public API (`Db`, `Registries`, `Config`)
//! plus `varve-config`/`varve-index`/`varve-log`/`varve-storage`/
//! `varve-types` (already dependencies of this crate) to build a local
//! `SharedStoreFactory` — the same "two processes, one bucket" pattern the
//! engine's own integration tests use, standing in here for two real
//! writer processes sharing one object-store bucket.
//!
//! Run: `cargo run --release --example failover -p varve`
//! Exits non-zero (a panic from a failed `assert!`, or an `Err` returned
//! from a fallible operation) on any assertion failure, so CI can wire this
//! in as a smoke gate.

use std::sync::Arc;
use std::time::Duration;
use varve::{Config, Db, RecordBatch, Registries};
use varve_config::{BuildContext, ComponentFactory, ConfigError, ConfigSection, RegistryError};
use varve_index::{encode_events, Event, Op};
use varve_log::{Log, LogRecord, ObjectStoreLog, TableEffects};
use varve_storage::ObjectStore;
use varve_types::{Instant, LogPosition, Value};

/// Registry factory: `[storage] backend = "shared"`. Always returns the
/// same pre-built store instance — the counterpart to the built-in
/// `"memory"` factory, which mints a fresh store every call.
struct SharedStoreFactory(Arc<dyn ObjectStore>);

impl ComponentFactory<dyn ObjectStore> for SharedStoreFactory {
    fn name(&self) -> &'static str {
        "shared"
    }

    fn build(
        &self,
        _cfg: &ConfigSection,
        _ctx: &BuildContext,
    ) -> Result<Arc<dyn ObjectStore>, RegistryError> {
        Ok(Arc::clone(&self.0))
    }
}

fn shared_registries(store: Arc<dyn ObjectStore>) -> Result<Registries, RegistryError> {
    let mut registries = Registries::with_builtins();
    registries
        .storage
        .register(Box::new(SharedStoreFactory(store)))?;
    Ok(registries)
}

fn cas_config() -> Result<Config, ConfigError> {
    Config::from_toml_str(
        "[storage]\nbackend = \"shared\"\n[log]\nbackend = \"object-store\"\n\
         [coordinator]\nbackend = \"cas-failover\"\nheartbeat_interval_ms = 100\n\
         takeover_after_ms = 300\n",
    )
}

fn query_only_config() -> Result<Config, ConfigError> {
    Config::from_toml_str(
        "[storage]\nbackend = \"shared\"\n[log]\nbackend = \"object-store\"\n\
         [node]\nroles = [\"query\"]\n",
    )
}

/// One INSERT-equivalent effect event: a `:Chaos`-labeled node carrying
/// `_id: id` (the same shape `varve-engine`'s integration tests use).
fn put_record(tx_id: u64, id: i64) -> Result<LogRecord, Box<dyn std::error::Error>> {
    let mut doc = varve_types::Doc::new();
    doc.insert("_id".to_string(), Value::Int(id));
    let iid = varve_types::Iid::derive("default", "nodes", &Value::Int(id).id_bytes()?);
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
    Ok(LogRecord {
        tx_id,
        system_time_us: tx_id as i64,
        user: String::new(),
        effects: vec![TableEffects {
            graph: String::new(),
            table: "nodes".into(),
            arrow_ipc: encode_events(&[event])?,
        }],
    })
}

fn count_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|batch| batch.num_rows()).sum()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store: Arc<dyn ObjectStore> = varve_storage::memory_store();
    let registries = shared_registries(Arc::clone(&store))?;

    // Writer A: acquire the lease, commit 3 acked txs.
    let a = Db::open_with(&cas_config()?, &registries).await?;
    for n in 1..=3i64 {
        a.execute(&format!("INSERT (:Chaos {{_id: {n}}})")).await?;
    }
    println!("writer A acquired lease (epoch 0), committed 3 txs");

    // Prime the zombie BEFORE the takeover: a raw log handle whose `head()`
    // lazily caches the pre-takeover cursor.
    let zombie = ObjectStoreLog::new(Arc::clone(&store));
    let stale_head = zombie.head().await?;
    let expected_stale_head = LogPosition::new(0, 3)?;
    assert_eq!(stale_head, expected_stale_head);

    // A "crashes": dropping it stops its heartbeat task WITHOUT releasing
    // the lease.
    drop(a);
    println!("writer A crashed (heartbeats stopped)");

    // Writer B: standby-acquires, seizes, fences, recovers — measured.
    let started = std::time::Instant::now();
    let b = Db::open_with(&cas_config()?, &registries).await?;
    let takeover = started.elapsed();
    assert!(
        takeover < Duration::from_secs(10),
        "takeover took {takeover:?}, want < 10s"
    );
    // Zero acked-tx loss.
    let rows = b.query("MATCH (p:Chaos) RETURN p._id").await?;
    assert_eq!(count_rows(&rows), 3, "zero acked-tx loss");

    // B commits in its new epoch.
    let receipt = b.execute("INSERT (:Chaos {_id: 4})").await?;

    // A fresh, independently-scanned log handle now sees B's own commit,
    // so its head genuinely reflects B's new epoch (unlike a scan taken
    // before B had written anything there, which would still show the
    // last epoch anyone actually wrote a byte to).
    let new_head = ObjectStoreLog::new(Arc::clone(&store)).head().await?;
    println!(
        "writer B took over in {}ms: epoch {}, fence {}@{}",
        takeover.as_millis(),
        new_head.epoch(),
        stale_head.epoch(),
        stale_head.offset()
    );

    // The zombie's late append lands at ITS stale cached position — durable
    // (a real object in the store), but DEAD (behind B's fence).
    let zombie_position = zombie.append(vec![put_record(999, 999)?]).await?;
    assert_eq!(
        zombie_position, stale_head,
        "the zombie lands exactly at its stale pre-takeover cursor"
    );
    let zombie_key = varve_storage::keys::log_key(zombie_position);
    assert!(
        store.get(&zombie_key).await.is_ok(),
        "the zombie append must be a real, durable object"
    );

    // B never sees it…
    let rows = b.query("MATCH (p:Chaos) RETURN p._id").await?;
    assert_eq!(
        count_rows(&rows),
        4,
        "zombie _id 999 must be invisible to B"
    );
    // …verify walks clean over it…
    b.verify().await?;

    // …and a fresh query-only node, waiting on B's last commit, agrees too.
    let q = Db::open_with(&query_only_config()?, &registries).await?;
    let rows = q
        .query("MATCH (p:Chaos) RETURN p._id")
        .basis(receipt)
        .basis_timeout(Duration::from_secs(5))
        .await?;
    let final_count = count_rows(&rows);
    assert_eq!(
        final_count, 4,
        "the query node must also see exactly 4 rows"
    );

    println!(
        "zombie append landed at ({},{}) — IGNORED by B, query node, and verify",
        zombie_position.epoch(),
        zombie_position.offset()
    );
    println!("final row count everywhere: {final_count}");

    Ok(())
}
