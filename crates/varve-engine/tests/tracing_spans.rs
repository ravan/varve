//! Task 13: `tracing` spans across the query path (parse → plan → execute).
//!
//! `set_default` is thread-local and `Db::query`/`Query::stream` run on the
//! CALLING task, so this is deterministic. Writer-loop spans (submit,
//! resolve, commit, apply, flush_block, compact, follower.apply) run on the
//! writer's SPAWNED task and are asserted instead in a `writer.rs` unit test
//! that calls `resolve_program`/`flush` directly under the same kind of
//! scoped subscriber — trying to observe a spawned task's spans through
//! `set_default` here would be non-deterministic.

#![allow(clippy::unwrap_used)]

use std::sync::{Arc, Mutex};
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::{registry, Layer};
use varve_engine::Db;

#[derive(Clone, Default)]
struct SpanNames(Arc<Mutex<Vec<&'static str>>>);

impl<S: tracing::Subscriber> Layer<S> for SpanNames {
    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        _: &tracing::span::Id,
        _: Context<'_, S>,
    ) {
        self.0.lock().unwrap().push(attrs.metadata().name());
    }
}

#[tokio::test]
async fn query_path_emits_parse_plan_execute_spans() {
    let names = SpanNames::default();
    let subscriber = registry().with(names.clone());
    let _guard = tracing::subscriber::set_default(subscriber);

    let db = Db::memory();
    db.execute("INSERT (:P {_id: 1})").await.unwrap();
    db.query("MATCH (p:P) RETURN p._id").await.unwrap();

    let seen = names.0.lock().unwrap().clone();
    for expected in [
        "varve.query.parse",
        "varve.query.plan",
        "varve.query.execute",
    ] {
        assert!(
            seen.contains(&expected),
            "missing span {expected}; saw {seen:?}"
        );
    }
}

/// Task 13 review fix (Finding 1): the UNION path takes a different code
/// path through `Db::query_stream_impl` than the single-body path above
/// (`query_stream_impl` plans every arm before executing any of them and
/// merging), so it needs its own coverage to keep the plan/execute boundary
/// honest on both shapes.
#[tokio::test]
async fn union_query_emits_parse_plan_execute_spans() {
    let names = SpanNames::default();
    let subscriber = registry().with(names.clone());
    let _guard = tracing::subscriber::set_default(subscriber);

    let db = Db::memory();
    db.execute("INSERT (:P {_id: 1})").await.unwrap();
    db.execute("INSERT (:P {_id: 2})").await.unwrap();
    db.query(
        "MATCH (p:P) RETURN p._id AS id \
         UNION ALL \
         MATCH (p:P) RETURN p._id AS id",
    )
    .await
    .unwrap();

    let seen = names.0.lock().unwrap().clone();
    for expected in [
        "varve.query.parse",
        "varve.query.plan",
        "varve.query.execute",
    ] {
        assert!(
            seen.contains(&expected),
            "missing span {expected} for union query; saw {seen:?}"
        );
    }
}
