use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use std::sync::Arc;
use thiserror::Error;
use varve_gql::ast::{Expr, Literal, QueryStmt};
use varve_index::LiveTable;

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

pub async fn run_query(stmt: &QueryStmt, live: &LiveTable) -> Result<Vec<RecordBatch>, PlanError> {
    // v0 scan: label pruning happens in the snapshot (spec §10 — labels prune scans).
    let label = stmt.pattern.label.as_deref().unwrap_or("");
    let Some(batch) = live.snapshot_for_label(label)? else {
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
    df = df.select(projection)?;

    Ok(df.collect().await?)
}
