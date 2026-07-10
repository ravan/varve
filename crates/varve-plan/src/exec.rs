use datafusion::arrow::array::{Array, FixedSizeBinaryArray};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use thiserror::Error;
use varve_gql::ast::{
    Clause, Expr, LabelSpec, Literal, NodePattern, PathPattern, QueryStmt, ReturnClause,
    TemporalClauses,
};
use varve_index::{snapshot_entities, LabelFilter, LiveTable};
use varve_types::{Iid, Instant, TemporalBounds, TemporalDimension};

use crate::expr::{iid_from_conjuncts, lower_expr, split_conjuncts, ElementCols, Scope};
use crate::functions::{session_context, FunctionRegistry};
use crate::pattern::{mangle_batch, mangled};

#[derive(Debug, Error)]
pub enum PlanError {
    #[error(transparent)]
    DataFusion(#[from] datafusion::error::DataFusionError),
    #[error(transparent)]
    Index(#[from] varve_index::IndexError),
    #[error("unknown column '{0}' in RETURN/WHERE")]
    UnknownColumn(String),
    #[error("unknown variable '{0}' in WHERE/RETURN")]
    UnknownVariable(String),
    #[error("missing parameter '${0}'")]
    MissingParam(String),
    #[error("unknown function '{0}'")]
    UnknownFunction(String),
    #[error("internal: _iid column malformed")]
    MalformedIid,
    #[error("unsupported in v1: {0}")]
    Unsupported(String),
}

fn label_filter(labels: &LabelSpec) -> LabelFilter<'_> {
    match labels {
        LabelSpec::All(labels) if labels.len() == 1 => LabelFilter::Single(labels[0].as_str()),
        LabelSpec::All(labels) => LabelFilter::All(labels),
        LabelSpec::Any(labels) => LabelFilter::Any(labels),
    }
}

pub(crate) fn to_df_literal(l: &Literal) -> datafusion::prelude::Expr {
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
pub const PIPELINE_UNSUPPORTED: &str = "query shape is not supported by this execution path";

pub struct DegenerateQuery<'a> {
    pub query_temporal: &'a TemporalClauses,
    pub match_temporal: &'a TemporalClauses,
    pub paths: &'a [PathPattern],
    pub where_clause: &'a Option<Expr>,
    pub ret: &'a ReturnClause,
}

pub fn degenerate_query(stmt: &QueryStmt) -> Result<DegenerateQuery<'_>, PlanError> {
    if !stmt.unions.is_empty() {
        return Err(PlanError::Unsupported(PIPELINE_UNSUPPORTED.into()));
    }
    let [Clause::Match {
        optional: false,
        paths,
        temporal,
        where_clause,
    }] = stmt.first.clauses.as_slice()
    else {
        return Err(PlanError::Unsupported(PIPELINE_UNSUPPORTED.into()));
    };
    let ret = &stmt.first.ret;
    if ret.distinct || !ret.order_by.is_empty() || ret.skip.is_some() || ret.limit.is_some() {
        return Err(PlanError::Unsupported(PIPELINE_UNSUPPORTED.into()));
    }

    Ok(DegenerateQuery {
        query_temporal: &stmt.first.temporal,
        match_temporal: temporal,
        paths,
        where_clause,
        ret,
    })
}

pub fn effective_bounds(stmt: &QueryStmt, now: Instant) -> Result<TemporalBounds, PlanError> {
    let query = degenerate_query(stmt)?;
    Ok(TemporalBounds {
        valid: query
            .match_temporal
            .valid
            .or(query.query_temporal.valid)
            .unwrap_or_else(|| TemporalDimension::at(now)),
        system: query
            .match_temporal
            .system
            .or(query.query_temporal.system)
            .unwrap_or_else(|| TemporalDimension::at(now)),
    })
}

/// IID point pushdown (spec §10): `WHERE v._id = <literal>` pins the scan to
/// exactly one entity, letting it prune persisted pages by IID range and
/// read a single live entity. `None` when the filter isn't an `_id`
/// equality or the literal can't be an id (Float/Null) — the scan stays
/// unpruned and DataFusion applies the WHERE afterwards either way, so this
/// is purely an access-path optimization, never a semantics change.
pub fn iid_point(
    where_clause: &Option<Expr>,
    params: &BTreeMap<String, varve_types::Value>,
    graph: &str,
    table: &str,
) -> Option<Iid> {
    let (_, rest) = split_conjuncts(where_clause.as_ref());
    for expr in &rest {
        if let Some((var, prop, _)) = expr.as_prop_eq() {
            if prop != "_id" {
                continue;
            }
            if let Some(iid) = iid_from_conjuncts(&rest, var, params, graph, table) {
                return Some(iid);
            }
        }
    }
    None
}

/// The IID a `_id = <literal>` binding points at, or `None` when the literal
/// can't be an id (Float/Null). The shared literal→`Iid` derivation behind
/// both [`iid_point`] (WHERE `_id` pushdown) and `pattern::scan_specs`
/// (inline `{_id: …}` prop pushdown per element).
/// Sync phase of DML matching (MATCH … DELETE, spec §10): resolve the
/// pattern's label and take the snapshot under the caller's brief read lock
/// (mirror of `snapshot_for_query`).
pub fn matching_snapshot(
    pattern: &NodePattern,
    live: &LiveTable,
    bounds: &TemporalBounds,
) -> Result<Option<RecordBatch>, PlanError> {
    Ok(snapshot_entities(
        live.entities().map(|(iid, events)| (*iid, events)),
        label_filter(&pattern.labels),
        bounds,
    )?)
}

/// Async phase: WHERE filter + inline-prop equality filters + IID extraction
/// over an OWNED snapshot — callers drop their live-table lock before awaiting
/// this. `props` are extra `prop = literal` equalities (a matched pattern's
/// inline `{k: v}` props, e.g. `MATCH (b:Person {name: 'Bob'})`), ANDed with
/// `where_clause`. Sorted and deduplicated so mutation application order is
/// deterministic.
pub async fn iids_from_snapshot(
    snapshot: Option<RecordBatch>,
    matched_var: Option<&str>,
    where_clause: &Option<Expr>,
    props: &[(String, Expr)],
) -> Result<Vec<Iid>, PlanError> {
    let functions = FunctionRegistry::with_builtins();
    let params = BTreeMap::new();
    iids_from_snapshot_with_functions(
        snapshot,
        matched_var,
        where_clause,
        props,
        &params,
        &functions,
    )
    .await
}

pub async fn iids_from_snapshot_with_functions(
    snapshot: Option<RecordBatch>,
    matched_var: Option<&str>,
    where_clause: &Option<Expr>,
    props: &[(String, Expr)],
    params: &BTreeMap<String, varve_types::Value>,
    functions: &FunctionRegistry,
) -> Result<Vec<Iid>, PlanError> {
    let Some(batch) = snapshot else {
        return Ok(vec![]);
    };
    let batch = match matched_var {
        Some(var) => mangle_batch(var, &batch)?,
        None => batch,
    };
    let schema = batch.schema();
    let has_col = |name: &str| schema.column_with_name(name).is_some();

    let ctx = session_context(functions);
    let table = MemTable::try_new(schema.clone(), vec![vec![batch]])?;
    let mut df = ctx.read_table(Arc::new(table))?;

    if let Some(where_clause) = where_clause {
        let (exists, rest) = split_conjuncts(Some(where_clause));
        if !exists.is_empty() {
            return Err(PlanError::Unsupported(
                "EXISTS outside top-level WHERE conjunction".into(),
            ));
        }
        let scope = match matched_var {
            Some(var) => scope_for_snapshot(schema.as_ref(), var),
            None => Scope::new(BTreeMap::new(), BTreeSet::new(), BTreeSet::new()),
        };
        for expr in rest {
            df = df.filter(lower_expr(expr, &scope, params, functions)?)?;
        }
    }
    for (prop, value) in props {
        if let Some(var) = matched_var {
            let scope = scope_for_snapshot(schema.as_ref(), var);
            let expr = Expr::Binary {
                op: varve_gql::ast::BinaryOp::Eq,
                lhs: Box::new(Expr::Prop {
                    var: var.to_string(),
                    prop: prop.clone(),
                }),
                rhs: Box::new(value.clone()),
            };
            df = df.filter(lower_expr(&expr, &scope, params, functions)?)?;
        } else {
            if !has_col(prop) {
                return Err(PlanError::UnknownColumn(prop.clone()));
            }
            let scope = Scope::new(BTreeMap::new(), BTreeSet::new(), BTreeSet::new());
            df = df.filter(col(prop.as_str()).eq(lower_expr(value, &scope, params, functions)?))?;
        }
    }
    let iid_col = matched_var
        .map(|var| mangled(var, "_iid"))
        .unwrap_or_else(|| "_iid".to_string());
    let df = df.select(vec![col(iid_col)])?;

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
fn scope_for_snapshot(schema: &datafusion::arrow::datatypes::Schema, var: &str) -> Scope<'static> {
    let available = schema
        .fields()
        .iter()
        .map(|field| field.name().to_string())
        .collect();
    let mut elements = BTreeMap::new();
    elements.insert(var.to_string(), ElementCols { available });
    Scope::new(elements, BTreeSet::new(), BTreeSet::new())
}

pub async fn matching_iids(
    pattern: &NodePattern,
    where_clause: &Option<Expr>,
    live: &LiveTable,
    bounds: &TemporalBounds,
) -> Result<Vec<Iid>, PlanError> {
    let functions = FunctionRegistry::with_builtins();
    matching_iids_with_functions(pattern, where_clause, live, bounds, &functions).await
}

pub async fn matching_iids_with_functions(
    pattern: &NodePattern,
    where_clause: &Option<Expr>,
    live: &LiveTable,
    bounds: &TemporalBounds,
    functions: &FunctionRegistry,
) -> Result<Vec<Iid>, PlanError> {
    iids_from_snapshot_with_functions(
        matching_snapshot(pattern, live, bounds)?,
        pattern.var.as_deref(),
        where_clause,
        &pattern.props,
        &BTreeMap::new(),
        functions,
    )
    .await
}

/// One-shot convenience for tests and non-locking callers, over a bare
/// [`LiveTable`] (no persisted blocks, no store). Lowers through the same
/// [`crate::pattern`] path the engine uses, so single-element MATCH exercises
/// `execute_pattern`'s zero-join case directly. Only node scans can be served
/// from a `LiveTable` alone; edge/expansion elements need the engine's
/// `merged_snapshot`, so a multi-element query here returns `Unsupported`.
pub async fn run_query(
    stmt: &QueryStmt,
    live: &LiveTable,
    now: Instant,
) -> Result<Vec<RecordBatch>, PlanError> {
    let functions = FunctionRegistry::with_builtins();
    run_query_with_functions(stmt, live, now, &functions).await
}

pub async fn run_query_with_functions(
    stmt: &QueryStmt,
    live: &LiveTable,
    now: Instant,
    functions: &FunctionRegistry,
) -> Result<Vec<RecordBatch>, PlanError> {
    use crate::pattern::{execute_pattern, scan_specs_for_stmt, ScanInput, SpecKind};
    let bounds = effective_bounds(stmt, now)?;
    let specs = scan_specs_for_stmt(
        stmt,
        crate::pattern::DEFAULT_GRAPH,
        crate::pattern::DEFAULT_MAX_PATH_DEPTH,
        &BTreeMap::new(),
    )?;
    let mut inputs = Vec::with_capacity(specs.len());
    for spec in &specs {
        match &spec.kind {
            SpecKind::Node { labels, .. } => inputs.push(ScanInput::Batch(snapshot_entities(
                live.entities().map(|(iid, events)| (*iid, events)),
                label_filter(labels),
                &bounds,
            )?)),
            _ => {
                return Err(PlanError::Unsupported(
                    "run_query serves single-node LiveTable queries only".into(),
                ))
            }
        }
    }
    execute_pattern(stmt, &specs, inputs, &BTreeMap::new(), functions).await
}
