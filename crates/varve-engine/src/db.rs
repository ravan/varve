use datafusion::arrow::record_batch::RecordBatch;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use thiserror::Error;
use varve_gql::ast::{Literal, Statement};
use varve_gql::token::GqlError;
use varve_index::{Event, IndexError, LiveTable, Op};
use varve_plan::PlanError;
use varve_types::{Doc, Iid, Instant, TypeError, Value};

#[derive(Debug, Error)]
pub enum EngineError {
    #[error(transparent)]
    Gql(#[from] GqlError),
    #[error(transparent)]
    Plan(#[from] PlanError),
    #[error(transparent)]
    Index(#[from] IndexError),
    #[error(transparent)]
    Type(#[from] TypeError),
    #[error("statement is a query; use query()")]
    NotAMutation,
    #[error("statement is a mutation; use execute()")]
    NotAQuery,
    #[error("internal lock poisoned")]
    Poisoned,
}

#[derive(Debug, Clone, Copy)]
pub struct TxReceipt {
    pub tx_id: u64,
}

/// Embedded, in-process database handle. v0: single in-memory `LiveTable`
/// with system-time joins (Task 6); no persistence until slice 3/4.
pub struct Db {
    live: Arc<RwLock<LiveTable>>,
    tx_counter: AtomicU64,
    id_counter: AtomicU64,
}

fn literal_to_value(l: &Literal) -> Value {
    match l {
        Literal::Int(i) => Value::Int(*i),
        Literal::Float(f) => Value::Float(*f),
        Literal::Str(s) => Value::Str(s.clone()),
        Literal::Bool(b) => Value::Bool(*b),
        Literal::Null => Value::Null,
    }
}

impl Db {
    pub fn memory() -> Db {
        Db {
            live: Arc::new(RwLock::new(LiveTable::new())),
            tx_counter: AtomicU64::new(0),
            id_counter: AtomicU64::new(0),
        }
    }

    /// Execute a mutation statement. v0: INSERT only.
    ///
    /// v0 is single-writer only: `system_from` is derived from a tx-counter
    /// increment taken before the `live.write()` lock, so a smaller-`tx_id`
    /// writer that loses the lock race could be rejected by `LiveTable`'s
    /// monotonicity check. Resolved when Task 8 adds a strictly-increasing `MonotonicClock`.
    pub async fn execute(&self, gql: &str) -> Result<TxReceipt, EngineError> {
        let Statement::Insert(ins) = varve_gql::parse(gql)? else {
            return Err(EngineError::NotAMutation);
        };
        let tx_id = self.tx_counter.fetch_add(1, Ordering::SeqCst) + 1;
        // Interim system time = tx counter as µs; the real monotonic wall
        // clock lands with temporal mutations (Task 8 of the slice-2 plan).
        let system = Instant::from_micros(i64::try_from(tx_id).unwrap_or(i64::MAX));
        // v0: build and validate every node's (iid, labels, doc) triple up front —
        // including the fallible `id_bytes()?` — before touching the live table, so a
        // later node's invalid id can't leave earlier nodes from this statement
        // partially committed. Only once all triples are known-good do we append them.
        let mut rows = Vec::with_capacity(ins.nodes.len());
        for node in &ins.nodes {
            let mut doc: Doc = node
                .props
                .iter()
                .map(|(k, v)| (k.clone(), literal_to_value(v)))
                .collect();
            let id = match doc.get("_id") {
                Some(v) => v.clone(),
                None => {
                    // v0: process-local generated ids; proper generation arrives in slice 2
                    let n = self.id_counter.fetch_add(1, Ordering::SeqCst);
                    let v = Value::Str(format!("varve:gen:{n}"));
                    doc.insert("_id".into(), v.clone());
                    v
                }
            };
            let iid = Iid::derive("default", "nodes", &id.id_bytes()?);
            rows.push(Event {
                iid,
                system_from: system,
                valid_from: system,
                valid_to: Instant::END_OF_TIME,
                op: Op::Put {
                    labels: node.labels.clone(),
                    doc,
                },
            });
        }
        let mut live = self.live.write().map_err(|_| EngineError::Poisoned)?;
        for event in rows {
            live.append(event)?;
        }
        Ok(TxReceipt { tx_id })
    }

    /// Execute a read query, returning Arrow batches.
    pub async fn query(&self, gql: &str) -> Result<Vec<RecordBatch>, EngineError> {
        let Statement::Query(q) = varve_gql::parse(gql)? else {
            return Err(EngineError::NotAQuery);
        };
        let now = Instant::from_micros(
            i64::try_from(self.tx_counter.load(Ordering::SeqCst)).unwrap_or(i64::MAX),
        );
        // Snapshot under the read lock, drop the guard, then run DataFusion
        // on the owned batch — no await while holding the lock.
        let snapshot = {
            let live = self.live.read().map_err(|_| EngineError::Poisoned)?;
            varve_plan::snapshot_for_query(&q, &live, now)?
        };
        Ok(varve_plan::execute_query(&q, snapshot).await?)
    }
}
