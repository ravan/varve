use datafusion::arrow::record_batch::RecordBatch;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use thiserror::Error;
use varve_gql::ast::{Literal, Statement};
use varve_gql::token::GqlError;
use varve_index::{IndexError, LiveTable};
use varve_plan::PlanError;
use varve_types::{Doc, Iid, TypeError, Value};

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

/// Embedded, in-process database handle. v0: single in-memory `LiveTable`,
/// no persistence, no system_time joins (arrive in slice 2).
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
    pub async fn execute(&self, gql: &str) -> Result<TxReceipt, EngineError> {
        let Statement::Insert(ins) = varve_gql::parse(gql)? else {
            return Err(EngineError::NotAMutation);
        };
        let tx_id = self.tx_counter.fetch_add(1, Ordering::SeqCst) + 1;
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
            rows.push((iid, node.labels.clone(), doc));
        }
        let mut live = self.live.write().map_err(|_| EngineError::Poisoned)?;
        for (iid, labels, doc) in rows {
            live.append(iid, labels, doc)?;
        }
        Ok(TxReceipt { tx_id })
    }

    /// Execute a read query, returning Arrow batches.
    // v0: clone-free read under lock; snapshotting becomes cheap-Arc in slice 2. The
    // `RwLockReadGuard` is held across `run_query`'s internal `.await` (DataFusion's
    // `collect()`) — acceptable for v0's single-writer walking skeleton, with no
    // concurrent-access contention model yet.
    #[allow(clippy::await_holding_lock)]
    pub async fn query(&self, gql: &str) -> Result<Vec<RecordBatch>, EngineError> {
        let Statement::Query(q) = varve_gql::parse(gql)? else {
            return Err(EngineError::NotAQuery);
        };
        let live = self.live.read().map_err(|_| EngineError::Poisoned)?;
        Ok(varve_plan::run_query(&q, &live).await?)
    }
}
