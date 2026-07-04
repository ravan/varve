use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use std::sync::Arc;
use thiserror::Error;
use varve_gql::ast::{Expr, Literal, QueryStmt};
use varve_index::LiveTable;
use varve_types::{Instant, TemporalBounds, TemporalDimension};

#[derive(Debug, Error)]
pub enum PlanError {
    #[error(transparent)]
    DataFusion(#[from] datafusion::error::DataFusionError),
    #[error(transparent)]
    Index(#[from] varve_index::IndexError),
    #[error("unknown column '{0}' in RETURN/WHERE")]
    UnknownColumn(String),
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

/// Sync phase: resolve + snapshot under the caller's lock. Bounds are the
/// spec §7 defaults — valid AS OF now, system AS OF now (the writer clock is
/// monotonic, so at(now) sees exactly the current versions). Task 7 derives
/// bounds from the statement's FOR clauses.
pub fn snapshot_for_query(
    stmt: &QueryStmt,
    live: &LiveTable,
    now: Instant,
) -> Result<Option<RecordBatch>, PlanError> {
    let bounds = TemporalBounds {
        valid: TemporalDimension::at(now),
        system: TemporalDimension::at(now),
    };
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
        if !has_col(&item.prop) {
            return Err(PlanError::UnknownColumn(item.prop.clone()));
        }
        let out_name = item.alias.clone().unwrap_or_else(|| item.prop.clone());
        projection.push(col(item.prop.as_str()).alias(out_name));
    }
    let df = df.select(projection)?;

    Ok(df.collect().await?)
}

/// One-shot convenience for tests and non-locking callers.
pub async fn run_query(
    stmt: &QueryStmt,
    live: &LiveTable,
    now: Instant,
) -> Result<Vec<RecordBatch>, PlanError> {
    execute_query(stmt, snapshot_for_query(stmt, live, now)?).await
}
