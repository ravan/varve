#![allow(clippy::unwrap_used, clippy::expect_used, dead_code)]
//! Shared test-support helpers for integration tests that need a `Db` built
//! from `Db::open_with` over a store that the test ALSO wrote to directly
//! (e.g. to plant a raw log record or an epoch fence ahead of `Db::open`).
//! `MemoryStoreFactory`/`Registries::with_builtins()` always mint a FRESH
//! store per build, so tests that pre-populate a store need a factory that
//! hands back that exact instance instead — this module's `SharedStoreFactory`.
//!
//! Deliberately minimal: only what `fenced_recovery.rs` and `admin.rs`'s
//! fence-aware verify test need today. Tasks 6 and 9 extend this module
//! rather than duplicating it.

use std::sync::Arc;
use varve_config::{BuildContext, ComponentFactory, ConfigSection, RegistryError};
use varve_engine::Registries;
use varve_index::{encode_events, Event, Op};
use varve_log::{LogRecord, TableEffects};
use varve_storage::ObjectStore;
use varve_types::{Instant, Value};

/// Registry factory: `[storage] backend = "shared"`. Always returns the same
/// pre-built store instance, ignoring `cfg`/`ctx` — the counterpart to
/// `varve_storage::MemoryStoreFactory`, which mints a fresh store every call.
pub(crate) struct SharedStoreFactory(pub Arc<dyn ObjectStore>);

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

/// Builtin registries plus the `"shared"` storage factory wired to `store`.
pub(crate) fn shared_registries(store: Arc<dyn ObjectStore>) -> Registries {
    let mut registries = Registries::with_builtins();
    registries
        .storage
        .register(Box::new(SharedStoreFactory(store)))
        .unwrap();
    registries
}

/// A `Config` selecting the `"shared"` storage factory (pair with
/// `shared_registries(store)`) and the `"object-store"` log — two
/// `Db::open_with` calls over the same `shared_registries(store)` are then
/// "two processes sharing a bucket", in-process. `extra` splices in
/// additional TOML, e.g. a `[coordinator]` section (Task 6's coordination
/// tests).
pub(crate) fn writer_config(extra: &str) -> varve_config::Config {
    varve_config::Config::from_toml_str(&format!(
        "[storage]\nbackend = \"shared\"\n[log]\nbackend = \"object-store\"\n{extra}\n"
    ))
    .unwrap()
}

/// One INSERT-equivalent effect event on the default graph's `nodes` table:
/// a `:Chaos`-labeled node carrying `_id: id`.
pub(crate) fn put_record(tx_id: u64, id: i64) -> LogRecord {
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

/// `writer_config` plus a `[coordinator]` section selecting `cas-failover`
/// with tight timings (heartbeat 100 ms / takeover 300 ms — the minimum the
/// `2x heartbeat` validation allows) so an in-process takeover over a memory
/// store completes in milliseconds, well under the roadmap's 10 s bar
/// (Task 9).
pub(crate) fn cas_config() -> varve_config::Config {
    writer_config(
        "[coordinator]\nbackend = \"cas-failover\"\nheartbeat_interval_ms = 100\n\
         takeover_after_ms = 300",
    )
}

/// Every `Int64` value across every column-0 batch, sorted ascending — the
/// row order a bare `MATCH ... RETURN` produces is an implementation detail,
/// so tests that only care about SET membership sort before comparing
/// (mirrors `varve/tests/compaction.rs`'s `column_i64`).
pub(crate) fn collect_i64_column(batches: &[varve_engine::RecordBatch]) -> Vec<i64> {
    use datafusion::arrow::array::Int64Array;
    let mut out = Vec::new();
    for batch in batches {
        let column = batch.column(0);
        let values = column
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("expected an Int64 column");
        for row in 0..values.len() {
            out.push(values.value(row));
        }
    }
    out.sort_unstable();
    out
}
