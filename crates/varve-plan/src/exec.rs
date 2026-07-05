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
    #[error("unsupported in v1: {0}")]
    Unsupported(String),
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
pub fn effective_bounds(stmt: &QueryStmt, now: Instant) -> TemporalBounds {
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

fn literal_value(l: &Literal) -> varve_types::Value {
    use varve_types::Value;
    match l {
        Literal::Int(i) => Value::Int(*i),
        Literal::Float(f) => Value::Float(*f),
        Literal::Str(s) => Value::Str(s.clone()),
        Literal::Bool(b) => Value::Bool(*b),
        Literal::Null => Value::Null,
    }
}

/// IID point pushdown (spec §10): `WHERE v._id = <literal>` pins the scan to
/// exactly one entity, letting it prune persisted pages by IID range and
/// read a single live entity. `None` when the filter isn't an `_id`
/// equality or the literal can't be an id (Float/Null) — the scan stays
/// unpruned and DataFusion applies the WHERE afterwards either way, so this
/// is purely an access-path optimization, never a semantics change.
pub fn iid_point(where_clause: &Option<Expr>, graph: &str, table: &str) -> Option<Iid> {
    let Some(Expr::PropEq { prop, value, .. }) = where_clause else {
        return None;
    };
    if prop != "_id" {
        return None;
    }
    let bytes = literal_value(value).id_bytes().ok()?;
    Some(Iid::derive(graph, table, &bytes))
}

/// (hidden column, default output name) for a `RETURN`-position temporal function.
fn temporal_fn_columns(func: TemporalFnKind) -> (&'static str, &'static str) {
    match func {
        TemporalFnKind::ValidFrom => ("_valid_from", "valid_from"),
        TemporalFnKind::ValidTo => ("_valid_to", "valid_to"),
        TemporalFnKind::SystemFrom => ("_system_from", "system_from"),
    }
}

/// v1: only a single-node, hop-free, unnamed MATCH is supported (multi-
/// element MATCH lands in task 8 of slice 6); a node pattern with props is
/// likewise deferred — there is no plan-level filtering on them yet.
fn require_single_node(stmt: &QueryStmt) -> Result<&NodePattern, PlanError> {
    let node = stmt.single_node().ok_or_else(|| {
        PlanError::Unsupported("multi-element MATCH lands in task 8 of slice 6".into())
    })?;
    if !node.props.is_empty() {
        return Err(PlanError::Unsupported(
            "multi-element MATCH lands in task 8 of slice 6".into(),
        ));
    }
    Ok(node)
}

/// Sync phase: resolve + snapshot under the caller's lock.
pub fn snapshot_for_query(
    stmt: &QueryStmt,
    live: &LiveTable,
    now: Instant,
) -> Result<Option<RecordBatch>, PlanError> {
    let bounds = effective_bounds(stmt, now);
    let node = require_single_node(stmt)?;
    let label = node.labels.first().map(String::as_str).unwrap_or("");
    Ok(live.snapshot_for_label(label, &bounds)?)
}

/// Async phase: DataFusion filter/projection over an OWNED snapshot — callers
/// drop their live-table lock before awaiting this.
pub async fn execute_query(
    stmt: &QueryStmt,
    snapshot: Option<RecordBatch>,
) -> Result<Vec<RecordBatch>, PlanError> {
    require_single_node(stmt)?;
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
            ReturnItem::Var { .. } => {
                return Err(PlanError::Unsupported(
                    "path variables land in task 9 of slice 6".into(),
                ));
            }
        };
        projection.push(col(source).alias(out_name));
    }
    let df = df.select(projection)?;

    Ok(df.collect().await?)
}

/// Sync phase of DML matching (MATCH … DELETE, spec §10): resolve the
/// pattern's label and take the snapshot under the caller's brief read lock
/// (mirror of `snapshot_for_query`).
pub fn matching_snapshot(
    pattern: &NodePattern,
    live: &LiveTable,
    bounds: &TemporalBounds,
) -> Result<Option<RecordBatch>, PlanError> {
    let label = pattern.labels.first().map(String::as_str).unwrap_or("");
    Ok(live.snapshot_for_label(label, bounds)?)
}

/// Async phase: WHERE filter + inline-prop equality filters + IID extraction
/// over an OWNED snapshot — callers drop their live-table lock before awaiting
/// this. `props` are extra `prop = literal` equalities (a matched pattern's
/// inline `{k: v}` props, e.g. `MATCH (b:Person {name: 'Bob'})`), ANDed with
/// `where_clause`. Sorted and deduplicated so mutation application order is
/// deterministic.
pub async fn iids_from_snapshot(
    snapshot: Option<RecordBatch>,
    where_clause: &Option<Expr>,
    props: &[(String, Literal)],
) -> Result<Vec<Iid>, PlanError> {
    let Some(batch) = snapshot else {
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
    for (prop, value) in props {
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

/// Resolves the IIDs of entities visible at `bounds` that match `pattern` +
/// `where_clause` — the read side of writer-driven DML (MATCH … DELETE,
/// spec §10). One-shot composition of `matching_snapshot` + `iids_from_snapshot`
/// for tests and non-locking callers.
pub async fn matching_iids(
    pattern: &NodePattern,
    where_clause: &Option<Expr>,
    live: &LiveTable,
    bounds: &TemporalBounds,
) -> Result<Vec<Iid>, PlanError> {
    iids_from_snapshot(
        matching_snapshot(pattern, live, bounds)?,
        where_clause,
        &pattern.props,
    )
    .await
}

/// One-shot convenience for tests and non-locking callers.
pub async fn run_query(
    stmt: &QueryStmt,
    live: &LiveTable,
    now: Instant,
) -> Result<Vec<RecordBatch>, PlanError> {
    execute_query(stmt, snapshot_for_query(stmt, live, now)?).await
}
