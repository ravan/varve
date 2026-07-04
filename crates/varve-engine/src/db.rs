use crate::clock::MonotonicClock;
use datafusion::arrow::record_batch::RecordBatch;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use thiserror::Error;
use varve_gql::ast::{DeleteStmt, InsertStmt, Literal, Statement};
use varve_gql::token::GqlError;
use varve_index::{Event, IndexError, LiveTable, Op};
use varve_plan::PlanError;
use varve_types::{Doc, Iid, Instant, TemporalBounds, TemporalDimension, TypeError, Value};

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
    #[error("VALID FROM {from} must be earlier than VALID TO {to}")]
    InvalidValidRange { from: Instant, to: Instant },
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
    pub system_time: Instant,
}

/// Embedded, in-process database handle. Single in-memory `LiveTable`;
/// the writer assigns a monotonic system time per tx (durability arrives
/// in slice 3).
pub struct Db {
    live: Arc<RwLock<LiveTable>>,
    clock: MonotonicClock,
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
            clock: MonotonicClock::new(),
            tx_counter: AtomicU64::new(0),
            id_counter: AtomicU64::new(0),
        }
    }

    /// Executes a mutation statement (INSERT or MATCH … DELETE).
    ///
    /// v0 is single-writer only: `clock.next()` is taken *before* the
    /// `live.write()` lock, so a strictly-increasing clock alone does not
    /// serialize concurrent writers — a caller assigned a smaller tx time
    /// that loses the lock race would still be rejected by `LiveTable`'s
    /// monotonicity check as an `OutOfOrderEvent`. Full write serialization
    /// (a single writer loop, spec D3) arrives in slice 3; concurrent
    /// `execute()` calls are unsupported until then.
    pub async fn execute(&self, gql: &str) -> Result<TxReceipt, EngineError> {
        match varve_gql::parse(gql)? {
            Statement::Insert(ins) => self.execute_insert(&ins).await,
            Statement::Delete(del) => self.execute_delete(&del).await,
            Statement::Query(_) => Err(EngineError::NotAMutation),
        }
    }

    async fn execute_insert(&self, ins: &InsertStmt) -> Result<TxReceipt, EngineError> {
        let tx_id = self.tx_counter.fetch_add(1, Ordering::SeqCst) + 1;
        let system = self.clock.next();
        let valid_from = ins.valid_from.unwrap_or(system);
        let valid_to = ins.valid_to.unwrap_or(Instant::END_OF_TIME);
        if valid_from >= valid_to {
            return Err(EngineError::InvalidValidRange {
                from: valid_from,
                to: valid_to,
            });
        }

        // Build and validate EVERY node's (iid, labels, doc) triple —
        // including the fallible `id_bytes()?` — before the first append, so
        // a later node's invalid `_id` can't leave earlier nodes from this
        // statement partially committed (slice-1 review fix, pinned by
        // `multi_node_insert_is_atomic_on_invalid_id`).
        let mut events = Vec::with_capacity(ins.nodes.len());
        for node in &ins.nodes {
            let mut doc: Doc = node
                .props
                .iter()
                .map(|(k, v)| (k.clone(), literal_to_value(v)))
                .collect();
            let id = match doc.get("_id") {
                Some(v) => v.clone(),
                None => {
                    // process-local generated id; user-durable ids arrive in slice 3
                    let n = self.id_counter.fetch_add(1, Ordering::SeqCst);
                    let v = Value::Str(format!("varve:gen:{n}"));
                    doc.insert("_id".into(), v.clone());
                    v
                }
            };
            let iid = Iid::derive("default", "nodes", &id.id_bytes()?);
            events.push(Event {
                iid,
                system_from: system,
                valid_from,
                valid_to,
                op: Op::Put {
                    labels: node.labels.clone(),
                    doc,
                },
            });
        }

        let mut live = self.live.write().map_err(|_| EngineError::Poisoned)?;
        for event in events {
            live.append(event)?;
        }
        Ok(TxReceipt {
            tx_id,
            system_time: system,
        })
    }

    // DELETE plans its read (`matching_iids`) as part of the same tx's own
    // snapshot (spec §10 DML), holding the write lock across an internal
    // await — acceptable for the single-writer embedded engine (no
    // concurrent access model until slice 3's writer loop).
    #[allow(clippy::await_holding_lock)]
    async fn execute_delete(&self, del: &DeleteStmt) -> Result<TxReceipt, EngineError> {
        let tx_id = self.tx_counter.fetch_add(1, Ordering::SeqCst) + 1;
        let system = self.clock.next();
        let bounds = TemporalBounds {
            valid: TemporalDimension::at(system),
            system: TemporalDimension::at(system),
        };
        let mut live = self.live.write().map_err(|_| EngineError::Poisoned)?;
        let iids: Vec<Iid> =
            varve_plan::matching_iids(&del.pattern, &del.where_clause, &live, &bounds).await?;
        for iid in iids {
            live.append(Event {
                iid,
                system_from: system,
                valid_from: system,
                valid_to: Instant::END_OF_TIME,
                op: Op::Delete,
            })?;
        }
        Ok(TxReceipt {
            tx_id,
            system_time: system,
        })
    }

    /// Executes a read query, returning Arrow batches.
    pub async fn query(&self, gql: &str) -> Result<Vec<RecordBatch>, EngineError> {
        let Statement::Query(q) = varve_gql::parse(gql)? else {
            return Err(EngineError::NotAQuery);
        };
        let now = self.clock.watermark();
        // Snapshot under the read lock, drop the guard, then run DataFusion
        // on the owned batch — no await while holding the lock.
        let snapshot = {
            let live = self.live.read().map_err(|_| EngineError::Poisoned)?;
            varve_plan::snapshot_for_query(&q, &live, now)?
        };
        Ok(varve_plan::execute_query(&q, snapshot).await?)
    }
}
