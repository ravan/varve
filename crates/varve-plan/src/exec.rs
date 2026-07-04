use datafusion::arrow::array::{Array, FixedSizeBinaryArray};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use std::sync::Arc;
use thiserror::Error;
use varve_gql::ast::{Expr, Literal, NodePattern, QueryStmt, ReturnItem, TemporalFnKind};
use varve_index::LiveTable;
use varve_types::{Iid, Instant, TemporalBounds, TemporalDimension};

#[derive(Debug, Error)]
pub enum PlanError {
    #[error(transparent)]
    DataFusion(#[from] datafusion::error::DataFusionError),
    #[error(transparent)]
    Index(#[from] varve_index::IndexError),
    #[error("unknown column '{0}' in RETURN/WHERE")]
    UnknownColumn(String),
    #[error("internal: _iid column malformed")]
    MalformedIid,
}

fn to_df_literal(l: &Literal) -> datafusion::prelude::Expr {
    match l {
        Literal::Int(i) => lit(*i),
        Literal::Float(f) => lit(*f),
        Literal::Str(s) => lit(s.clone()),
        Literal::Bool(b) => lit(*b),
        Literal::Null => lit(datafusion::scalar::ScalarValue::Null),
    }
}

/// Resolves the effective `TemporalBounds` for a query: per-`MATCH` clause
/// wins, else the query-level clause, else the spec §7 default — AS OF `now`
/// on both axes (the writer clock is monotonic, so `at(now)` sees exactly the
/// current versions).
fn effective_bounds(stmt: &QueryStmt, now: Instant) -> TemporalBounds {
    TemporalBounds {
        valid: stmt
            .match_temporal
            .valid
            .or(stmt.temporal.valid)
            .unwrap_or_else(|| TemporalDimension::at(now)),
        system: stmt
            .match_temporal
            .system
            .or(stmt.temporal.system)
            .unwrap_or_else(|| TemporalDimension::at(now)),
    }
}

/// (hidden column, default output name) for a `RETURN`-position temporal function.
fn temporal_fn_columns(func: TemporalFnKind) -> (&'static str, &'static str) {
    match func {
        TemporalFnKind::ValidFrom => ("_valid_from", "valid_from"),
        TemporalFnKind::ValidTo => ("_valid_to", "valid_to"),
        TemporalFnKind::SystemFrom => ("_system_from", "system_from"),
    }
}

/// Sync phase: resolve + snapshot under the caller's lock.
pub fn snapshot_for_query(
    stmt: &QueryStmt,
    live: &LiveTable,
    now: Instant,
) -> Result<Option<RecordBatch>, PlanError> {
    let bounds = effective_bounds(stmt, now);
    let label = stmt.pattern.label.as_deref().unwrap_or("");
    Ok(live.snapshot_for_label(label, &bounds)?)
}

/// Async phase: DataFusion filter/projection over an OWNED snapshot — callers
/// drop their live-table lock before awaiting this.
pub async fn execute_query(
    stmt: &QueryStmt,
    snapshot: Option<RecordBatch>,
) -> Result<Vec<RecordBatch>, PlanError> {
    let Some(batch) = snapshot else {
        return Ok(vec![]);
    };
    let schema = batch.schema();
    let has_col = |name: &str| schema.column_with_name(name).is_some();

    let ctx = SessionContext::new();
    let table = MemTable::try_new(schema.clone(), vec![vec![batch]])?;
    let mut df = ctx.read_table(Arc::new(table))?;

    if let Some(Expr::PropEq { prop, value, .. }) = &stmt.where_clause {
        if !has_col(prop) {
            return Err(PlanError::UnknownColumn(prop.clone()));
        }
        df = df.filter(col(prop.as_str()).eq(to_df_literal(value)))?;
    }

    let mut projection = Vec::new();
    for item in &stmt.return_items {
        let (source, out_name) = match item {
            ReturnItem::Prop { prop, alias, .. } => {
                if !has_col(prop) {
                    return Err(PlanError::UnknownColumn(prop.clone()));
                }
                (prop.as_str(), alias.clone().unwrap_or_else(|| prop.clone()))
            }
            ReturnItem::TemporalFn { func, alias, .. } => {
                let (hidden, default_name) = temporal_fn_columns(*func);
                (
                    hidden,
                    alias.clone().unwrap_or_else(|| default_name.to_string()),
                )
            }
        };
        projection.push(col(source).alias(out_name));
    }
    let df = df.select(projection)?;

    Ok(df.collect().await?)
}

/// Resolves the IIDs of entities visible at `bounds` that match `pattern` +
/// `where_clause` — the read side of writer-driven DML (MATCH … DELETE,
/// spec §10). Sorted and deduplicated so mutation application order is
/// deterministic.
pub async fn matching_iids(
    pattern: &NodePattern,
    where_clause: &Option<Expr>,
    live: &LiveTable,
    bounds: &TemporalBounds,
) -> Result<Vec<Iid>, PlanError> {
    let label = pattern.label.as_deref().unwrap_or("");
    let Some(batch) = live.snapshot_for_label(label, bounds)? else {
        return Ok(vec![]);
    };
    let schema = batch.schema();
    let has_col = |name: &str| schema.column_with_name(name).is_some();

    let ctx = SessionContext::new();
    let table = MemTable::try_new(schema.clone(), vec![vec![batch]])?;
    let mut df = ctx.read_table(Arc::new(table))?;

    if let Some(Expr::PropEq { prop, value, .. }) = where_clause {
        if !has_col(prop) {
            return Err(PlanError::UnknownColumn(prop.clone()));
        }
        df = df.filter(col(prop.as_str()).eq(to_df_literal(value)))?;
    }
    let df = df.select(vec![col("_iid")])?;

    let mut iids = Vec::new();
    for batch in df.collect().await? {
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .ok_or(PlanError::MalformedIid)?;
        for i in 0..col.len() {
            let bytes: [u8; 16] = col
                .value(i)
                .try_into()
                .map_err(|_| PlanError::MalformedIid)?;
            iids.push(Iid::from_bytes(bytes));
        }
    }
    iids.sort();
    iids.dedup();
    Ok(iids)
}

/// One-shot convenience for tests and non-locking callers.
pub async fn run_query(
    stmt: &QueryStmt,
    live: &LiveTable,
    now: Instant,
) -> Result<Vec<RecordBatch>, PlanError> {
    execute_query(stmt, snapshot_for_query(stmt, live, now)?).await
}
