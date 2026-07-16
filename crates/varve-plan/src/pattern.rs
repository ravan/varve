//! Multi-element MATCH lowering (slice 6, task 8): each pattern element
//! becomes its own scan whose columns are mangled with the element's variable
//! (`{var}__{col}`), per-element predicates (label is applied by the snapshot,
//! inline props + a matching WHERE equality here) fire BEFORE any join, and
//! the linear path lowers to a left-deep chain of inner hash joins on the iid
//! endpoint columns (`_iid` ↔ `_src_iid`/`_dst_iid`). A single-element MATCH
//! is the degenerate zero-hop, zero-join case, so slice-1/2/4 queries flow
//! through here unchanged.
//!
//! Task 9 plugs `PathExpand` into the `Expand`/`Adjacency` slots this module
//! emits: a quantified hop lowers to a [`crate::expand::PathExpandNode`]
//! (`LogicalPlan::Extension`) whose produced `expand_iid` is joined to the end
//! node's `_iid`, and all execution runs through the planner-installed
//! [`crate::functions::session_context`].

use crate::exec::{degenerate_query, PIPELINE_UNSUPPORTED};
use crate::expand::{EdgeAdjacency, PathExpandLimits, PathExpandNode};
use crate::expr::{
    iid_from_conjuncts, iid_from_expr, lower_expr, split_conjuncts, ElementCols, ExistsConjunct,
    Scope,
};
use crate::functions::{lower_aggregate, session_context, FunctionRegistry};
use crate::PlanError;
use datafusion::arrow::array::{new_empty_array, Array, FixedSizeBinaryArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{Column, UnnestOptions};
use datafusion::datasource::MemTable;
use datafusion::logical_expr::{Expr as DfExpr, Extension, LogicalPlan};
use datafusion::physical_plan::{EmptyRecordBatchStream, SendableRecordBatchStream};
use datafusion::prelude::*;
use futures::TryStreamExt;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use varve_gql::ast::{
    display_expr, Clause, Direction, EdgePattern, Expr, LabelSpec, Literal, NodePattern,
    PathPattern, QueryBody, QueryStmt, ReturnClause, SortItem, UnionKind,
};
use varve_types::{Iid, Value};

/// v1 single default graph (mirrors the engine's `DEFAULT_GRAPH`) — the graph
/// component of an entity's derived IID.
pub(crate) const DEFAULT_GRAPH: &str = "default";
/// Nodes table name, for inline-`_id` IID derivation (mirrors the engine's
/// `NODES_TABLE`; edges never take an iid_point).
const NODES_TABLE: &str = "nodes";
/// Fallback path-depth cap for the `LiveTable`-direct [`crate::exec::run_query`]
/// helper; the engine passes its own `[query] max_path_depth`.
pub(crate) const DEFAULT_MAX_PATH_DEPTH: u32 = 10;

/// Synthesized variable prefix for anonymous elements: element `i` becomes
/// `__el{i}`, keeping every element's mangled columns globally unique.
pub const SYNTH_PREFIX: &str = "__el";

/// `{var}__{column}`, e.g. `a___iid` for var `a`'s `_iid`.
pub fn mangled(var: &str, col: &str) -> String {
    format!("{var}__{col}")
}

/// What the engine must fetch for one pattern element, in path order:
/// element 0 is the start node, then one Edge/Expand + one Node per hop.
#[derive(Debug, Clone, PartialEq)]
pub struct ScanSpec {
    /// User-given variable, or a synthesized `__el{i}` for an anonymous element.
    pub var: String,
    pub kind: SpecKind,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SpecKind {
    Node {
        labels: LabelSpec,
        iid_point: Option<Iid>,
    },
    Edge {
        label: String,
        direction: Direction,
    },
    /// Quantified hop (Task 9 consumes; `scan_specs` already emits it).
    Expand {
        label: String,
        direction: Direction,
        min: u32,
        max: u32,
        props: Vec<(String, Expr)>,
        path_var: Option<String>,
    },
}

/// The engine's answer to one [`ScanSpec`], in the same order as `specs`.
pub enum ScanInput {
    Batch(Option<RecordBatch>),
    /// Task 9's adjacency input for an `Expand` element: the resolved
    /// (bounds-applied) adjacency the engine built for this quantified hop.
    Adjacency(Arc<EdgeAdjacency>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct ClauseSpecs {
    pub optional: bool,
    pub specs: Vec<ScanSpec>,
    pub shared_vars: Vec<String>,
    pub exists: Vec<ExistsSpecs>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExistsSpecs {
    pub specs: Vec<ScanSpec>,
    pub shared_vars: Vec<String>,
    pub shared_keys: Vec<String>,
}

pub fn scan_specs(
    body: &QueryBody,
    graph: &str,
    max_path_depth: u32,
) -> Result<Vec<ClauseSpecs>, PlanError> {
    scan_specs_with_params(body, graph, max_path_depth, &BTreeMap::new())
}

pub fn scan_specs_with_params(
    body: &QueryBody,
    graph: &str,
    max_path_depth: u32,
    params: &BTreeMap<String, Value>,
) -> Result<Vec<ClauseSpecs>, PlanError> {
    let mut clauses = Vec::new();
    let mut bound_vars = BTreeSet::new();
    let mut bound_node_labels = BTreeMap::new();
    for (idx, clause) in body.clauses.iter().enumerate() {
        let Clause::Match {
            optional,
            paths,
            where_clause,
            ..
        } = clause
        else {
            if let Clause::Filter(expr) = clause {
                let exists = exists_specs_for_expr(
                    idx,
                    expr,
                    &bound_vars,
                    &bound_node_labels,
                    graph,
                    max_path_depth,
                    params,
                )?;
                if !exists.is_empty() {
                    clauses.push(ClauseSpecs {
                        optional: false,
                        specs: Vec::new(),
                        shared_vars: Vec::new(),
                        exists,
                    });
                }
            }
            continue;
        };
        if idx == 0 && *optional {
            return Err(PlanError::Unsupported(
                "OPTIONAL MATCH cannot be the first clause".into(),
            ));
        }
        if idx == 0 && paths.is_empty() {
            return Err(PlanError::Unsupported("MATCH without path".into()));
        }
        let mut specs = Vec::new();
        let mut clause_vars = BTreeSet::new();
        for (path_idx, path) in paths.iter().enumerate() {
            let mut path_specs =
                path_scan_specs(path, graph, max_path_depth, where_clause, params)?;
            for spec in &mut path_specs {
                if spec.var.starts_with(SYNTH_PREFIX) {
                    spec.var = format!("{SYNTH_PREFIX}{idx}_{path_idx}_{}", spec.var);
                }
            }
            for spec in &path_specs {
                if !spec.var.starts_with(SYNTH_PREFIX) {
                    clause_vars.insert(spec.var.clone());
                }
            }
            specs.extend(path_specs);
        }
        let shared_vars = clause_vars
            .intersection(&bound_vars)
            .cloned()
            .collect::<Vec<_>>();
        let outer_vars = bound_vars
            .union(&clause_vars)
            .cloned()
            .collect::<BTreeSet<_>>();
        let mut outer_node_labels = bound_node_labels.clone();
        record_node_labels(&specs, &mut outer_node_labels);
        let exists = exists_specs_for_clause(
            idx,
            where_clause,
            &outer_vars,
            &outer_node_labels,
            graph,
            max_path_depth,
            params,
        )?;
        record_node_labels(&specs, &mut bound_node_labels);
        bound_vars.extend(clause_vars);
        clauses.push(ClauseSpecs {
            optional: *optional,
            specs,
            shared_vars,
            exists,
        });
    }
    if clauses.is_empty()
        || body.clauses.first().is_some_and(|c| {
            !matches!(
                c,
                Clause::Match {
                    optional: false,
                    ..
                }
            )
        })
    {
        return Err(PlanError::Unsupported(
            "first clause must be non-OPTIONAL MATCH".into(),
        ));
    }
    Ok(clauses)
}

/// Validates the statement (single linear path, at most one label per node,
/// quantifier bounds vs `max_path_depth`, path-var rules) and derives one
/// [`ScanSpec`] per element, in path order `[n0, e0, n1, e1, n2, …]`.
pub fn scan_specs_for_stmt(
    stmt: &QueryStmt,
    graph: &str,
    max_path_depth: u32,
    params: &BTreeMap<String, Value>,
) -> Result<Vec<ScanSpec>, PlanError> {
    let query = degenerate_query(stmt)?;
    if query.paths.len() != 1 {
        return Err(PlanError::Unsupported(PIPELINE_UNSUPPORTED.into()));
    }
    path_scan_specs(
        &query.paths[0],
        graph,
        max_path_depth,
        query.where_clause,
        params,
    )
}

fn path_scan_specs(
    path: &PathPattern,
    graph: &str,
    max_path_depth: u32,
    where_clause: &Option<Expr>,
    params: &BTreeMap<String, Value>,
) -> Result<Vec<ScanSpec>, PlanError> {
    if path.var.is_some() {
        // A path variable binds the sequence of a single hop. An
        // unquantified hop is normalized to a 1..1 expansion below.
        if path.hops.len() != 1 {
            return Err(PlanError::Unsupported(
                "path variables need a single hop in v1".into(),
            ));
        }
    }

    let mut specs = Vec::with_capacity(1 + 2 * path.hops.len());
    specs.push(node_spec(&path.start, 0, graph, where_clause, params)?);
    for (h, (edge, node)) in path.hops.iter().enumerate() {
        specs.push(edge_spec(
            edge,
            1 + 2 * h,
            max_path_depth,
            path.var.clone(),
        )?);
        specs.push(node_spec(node, 2 + 2 * h, graph, where_clause, params)?);
    }
    Ok(specs)
}

fn exists_specs_for_clause(
    clause_idx: usize,
    where_clause: &Option<Expr>,
    outer_vars: &BTreeSet<String>,
    outer_node_labels: &BTreeMap<String, LabelSpec>,
    graph: &str,
    max_path_depth: u32,
    params: &BTreeMap<String, Value>,
) -> Result<Vec<ExistsSpecs>, PlanError> {
    let Some(where_clause) = where_clause.as_ref() else {
        return Ok(Vec::new());
    };
    exists_specs_for_expr(
        clause_idx,
        where_clause,
        outer_vars,
        outer_node_labels,
        graph,
        max_path_depth,
        params,
    )
}

fn exists_specs_for_expr(
    clause_idx: usize,
    expr: &Expr,
    outer_vars: &BTreeSet<String>,
    outer_node_labels: &BTreeMap<String, LabelSpec>,
    graph: &str,
    max_path_depth: u32,
    params: &BTreeMap<String, Value>,
) -> Result<Vec<ExistsSpecs>, PlanError> {
    let (exists, _) = split_conjuncts(Some(expr));
    exists
        .iter()
        .enumerate()
        .map(|(exists_idx, exists)| {
            let mut specs = Vec::new();
            let mut inner_vars = BTreeSet::new();
            let mut shared_key_by_var = BTreeMap::new();
            let inner_where = exists.where_clause.cloned();
            for (path_idx, path) in exists.paths.iter().enumerate() {
                let mut path_specs =
                    path_scan_specs(path, graph, max_path_depth, &inner_where, params)?;
                inherit_shared_node_labels(&mut path_specs, outer_vars, outer_node_labels);
                for spec in &mut path_specs {
                    if spec.var.starts_with(SYNTH_PREFIX) {
                        spec.var = format!(
                            "{SYNTH_PREFIX}{clause_idx}_exists_{exists_idx}_{path_idx}_{}",
                            spec.var
                        );
                    }
                }
                for spec in &path_specs {
                    if !spec.var.starts_with(SYNTH_PREFIX) {
                        inner_vars.insert(spec.var.clone());
                    }
                }
                for spec in &path_specs {
                    if !spec.var.starts_with(SYNTH_PREFIX) && outer_vars.contains(&spec.var) {
                        if let Some(key) = shared_key_for_path(&spec.var, path, &path_specs) {
                            shared_key_by_var.entry(spec.var.clone()).or_insert(key);
                        }
                    }
                }
                specs.extend(path_specs);
            }
            let shared_vars = inner_vars
                .intersection(outer_vars)
                .cloned()
                .collect::<Vec<_>>();
            if shared_vars.is_empty() {
                return Err(PlanError::Unsupported(
                    "EXISTS must share variable enclosing pattern".into(),
                ));
            }
            let shared_keys = shared_vars
                .iter()
                .map(|var| {
                    shared_key_by_var
                        .get(var)
                        .cloned()
                        .unwrap_or_else(|| exists_join_key(var))
                })
                .collect();
            Ok(ExistsSpecs {
                specs,
                shared_vars,
                shared_keys,
            })
        })
        .collect()
}

fn record_node_labels(specs: &[ScanSpec], labels_by_var: &mut BTreeMap<String, LabelSpec>) {
    for spec in specs {
        if spec.var.starts_with(SYNTH_PREFIX) {
            continue;
        }
        if let SpecKind::Node { labels, .. } = &spec.kind {
            labels_by_var
                .entry(spec.var.clone())
                .or_insert_with(|| labels.clone());
        }
    }
}

fn inherit_shared_node_labels(
    specs: &mut [ScanSpec],
    outer_vars: &BTreeSet<String>,
    outer_node_labels: &BTreeMap<String, LabelSpec>,
) {
    for spec in specs {
        if !outer_vars.contains(&spec.var) {
            continue;
        }
        if let SpecKind::Node { labels, .. } = &mut spec.kind {
            if labels.is_empty() {
                if let Some(outer_labels) = outer_node_labels.get(&spec.var) {
                    *labels = outer_labels.clone();
                }
            }
        }
    }
}

fn shared_key_for_path(var: &str, path: &PathPattern, specs: &[ScanSpec]) -> Option<String> {
    if path.start.var.as_deref() == Some(var) {
        return path.hops.first().map(|(edge, _)| {
            if matches!(specs[1].kind, SpecKind::Expand { .. }) {
                return exists_join_key(var);
            }
            let endpoint = match edge.direction {
                Direction::Out => "_src_iid",
                Direction::In => "_dst_iid",
            };
            mangled(&specs[1].var, endpoint)
        });
    }
    for (hop_idx, (edge, node)) in path.hops.iter().enumerate() {
        let edge_idx = 1 + 2 * hop_idx;
        if edge.var.as_deref() == Some(var) {
            return Some(mangled(&specs[edge_idx].var, "_iid"));
        }
        if node.var.as_deref() == Some(var) {
            if matches!(specs[edge_idx].kind, SpecKind::Expand { .. }) {
                return Some(exists_join_key(var));
            }
            let endpoint = match edge.direction {
                Direction::Out => "_dst_iid",
                Direction::In => "_src_iid",
            };
            return Some(mangled(&specs[edge_idx].var, endpoint));
        }
    }
    None
}

/// This element's effective variable: the user's, or a synthesized `__el{i}`.
fn element_var(user_var: Option<&str>, idx: usize) -> String {
    match user_var {
        Some(v) => v.to_string(),
        None => format!("{SYNTH_PREFIX}{idx}"),
    }
}

fn node_spec(
    node: &NodePattern,
    idx: usize,
    graph: &str,
    where_clause: &Option<Expr>,
    params: &BTreeMap<String, Value>,
) -> Result<ScanSpec, PlanError> {
    Ok(ScanSpec {
        var: element_var(node.var.as_deref(), idx),
        kind: SpecKind::Node {
            labels: node.labels.clone(),
            iid_point: node_iid_point(node, graph, where_clause, params),
        },
    })
}

/// IID pushdown for a node (spec §10): an inline `{_id: <lit>}` prop, else a
/// `WHERE <this var>._id = <lit>` equality, pins the scan to one entity. Pure
/// access-path optimization — the same equality is re-applied as a filter, so
/// dropping to `None` only widens the scan, never the result.
fn node_iid_point(
    node: &NodePattern,
    graph: &str,
    where_clause: &Option<Expr>,
    params: &BTreeMap<String, Value>,
) -> Option<Iid> {
    for (k, v) in &node.props {
        if k == "_id" {
            if let Some(iid) = iid_from_expr(v, params, graph, NODES_TABLE) {
                return Some(iid);
            }
        }
    }
    if let (Some(uvar), Some(where_clause)) = (node.var.as_deref(), where_clause.as_ref()) {
        let (_, rest) = split_conjuncts(Some(where_clause));
        if let Some(iid) = iid_from_conjuncts(&rest, uvar, params, graph, NODES_TABLE) {
            return Some(iid);
        }
    }
    None
}

fn edge_spec(
    edge: &EdgePattern,
    idx: usize,
    max_path_depth: u32,
    path_var: Option<String>,
) -> Result<ScanSpec, PlanError> {
    let var = element_var(edge.var.as_deref(), idx);
    let kind = match edge.quantifier {
        None if path_var.is_none() => SpecKind::Edge {
            label: edge.label.clone(),
            direction: edge.direction,
        },
        None => SpecKind::Expand {
            label: edge.label.clone(),
            direction: edge.direction,
            min: 1,
            max: 1,
            props: edge.props.clone(),
            path_var,
        },
        Some(q) => {
            let max = q.max.unwrap_or(max_path_depth);
            if max > max_path_depth {
                return Err(PlanError::Unsupported(format!(
                    "quantifier max {max} exceeds max_path_depth {max_path_depth}"
                )));
            }
            SpecKind::Expand {
                label: edge.label.clone(),
                direction: edge.direction,
                min: q.min,
                max,
                props: edge.props.clone(),
                path_var,
            }
        }
    };
    Ok(ScanSpec { var, kind })
}

/// Lower + execute: mangle each element's batch, apply its predicates, chain
/// the joins (and Task 9's expansions), then project the RETURN clause.
/// `inputs.len() == specs.len()`, in the same order.
pub async fn execute_pattern(
    stmt: &QueryStmt,
    specs: &[ScanSpec],
    inputs: Vec<ScanInput>,
    params: &BTreeMap<String, Value>,
    functions: &FunctionRegistry,
) -> Result<Vec<RecordBatch>, PlanError> {
    let query = degenerate_query(stmt)?;
    let Some(path) = query.paths.first() else {
        return Err(PlanError::Unsupported("MATCH without a path".into()));
    };

    // A WHERE equality must name a pattern variable — check once, up front, so
    // `RETURN`-less filters on a bogus var still error (rather than silently
    // matching nothing).
    if let Some(where_clause) = query.where_clause {
        for expr in where_clause.conjuncts() {
            if let Some((var, _, _)) = expr.as_prop_eq() {
                if !specs.iter().any(|s| s.var == var) {
                    return Err(PlanError::UnknownVariable(var.to_string()));
                }
            }
        }
    }

    // The planner-installed context: `PathExpand` extension nodes lower to
    // `PathExpandExec` through it, and non-expand queries fall through to the
    // default planner unchanged.
    let ctx = session_context(functions);

    // 1. One mangled DataFrame per element, with its own predicates applied.
    //    A `None` batch ⇒ that element matched nothing ⇒ the whole join is
    //    empty. `Adjacency` inputs (Task 9) push a `None` frame and keep the
    //    resolved adjacency for the expansion arm, consumed positionally.
    let mut frames: Vec<Option<DataFrame>> = Vec::with_capacity(specs.len());
    let mut adjacencies: Vec<Option<Arc<EdgeAdjacency>>> = Vec::with_capacity(specs.len());
    let mut row_counts: Vec<usize> = Vec::with_capacity(specs.len());
    for (i, (spec, input)) in specs.iter().zip(inputs).enumerate() {
        match input {
            ScanInput::Batch(None) => return Ok(vec![]),
            ScanInput::Batch(Some(batch)) => {
                row_counts.push(batch.num_rows());
                let batch = mangle_batch(&spec.var, &batch)?;
                let schema = batch.schema();
                let table = MemTable::try_new(schema, vec![vec![batch]])?;
                let df = ctx.read_table(Arc::new(table))?;
                let df = apply_element_predicates(
                    df,
                    &spec.var,
                    element_props(path, i),
                    query.where_clause,
                    params,
                    functions,
                )?;
                frames.push(Some(df));
                adjacencies.push(None);
            }
            ScanInput::Adjacency(adj) => {
                row_counts.push(0);
                frames.push(None);
                adjacencies.push(Some(adj));
            }
        }
    }

    // 2. Left-deep join chain; direction by terminal-size heuristic
    //    (decision 9). Expansion hops anchor on the start side (Task 9), so
    //    any Expand forces the forward walk.
    let has_expand = specs
        .iter()
        .any(|s| matches!(s.kind, SpecKind::Expand { .. }));
    let forward = has_expand
        || row_counts.first().copied().unwrap_or(0) <= row_counts.last().copied().unwrap_or(0);
    let df = join_chain(
        frames,
        adjacencies,
        specs,
        path,
        forward,
        PathExpandLimits::default(),
    )?;
    let df = apply_where(df, query.where_clause, specs, params, functions)?;

    // 3. RETURN projection over the mangled columns.
    project_return(df, stmt, specs, params, functions).await
}

pub async fn execute_body(
    body: &QueryBody,
    clause_specs: &[ClauseSpecs],
    inputs: Vec<Vec<ScanInput>>,
    functions: &FunctionRegistry,
    params: &BTreeMap<String, Value>,
) -> Result<Vec<RecordBatch>, PlanError> {
    execute_body_with_limits(
        body,
        clause_specs,
        inputs,
        functions,
        PathExpandLimits::default(),
        params,
    )
    .await
}

pub async fn execute_body_with_limits(
    body: &QueryBody,
    clause_specs: &[ClauseSpecs],
    inputs: Vec<Vec<ScanInput>>,
    functions: &FunctionRegistry,
    path_expand_limits: PathExpandLimits,
    params: &BTreeMap<String, Value>,
) -> Result<Vec<RecordBatch>, PlanError> {
    let stream = execute_body_stream_with_limits(
        body,
        clause_specs,
        inputs,
        functions,
        path_expand_limits,
        params,
    )
    .await?;
    let schema = stream.schema();
    let batches = stream.try_collect::<Vec<_>>().await?;
    if batches.is_empty() && !schema.fields().is_empty() {
        Ok(vec![RecordBatch::new_empty(schema)])
    } else {
        Ok(batches)
    }
}

pub async fn execute_body_stream_with_limits(
    body: &QueryBody,
    clause_specs: &[ClauseSpecs],
    inputs: Vec<Vec<ScanInput>>,
    functions: &FunctionRegistry,
    path_expand_limits: PathExpandLimits,
    params: &BTreeMap<String, Value>,
) -> Result<SendableRecordBatchStream, PlanError> {
    match build_body_frame_with_limits(
        body,
        clause_specs,
        inputs,
        functions,
        path_expand_limits,
        params,
    )? {
        Some(frame) => Ok(frame.execute_stream().await?),
        None => Ok(Box::pin(EmptyRecordBatchStream::new(Arc::new(
            Schema::empty(),
        )))),
    }
}

fn build_body_frame_with_limits(
    body: &QueryBody,
    clause_specs: &[ClauseSpecs],
    inputs: Vec<Vec<ScanInput>>,
    functions: &FunctionRegistry,
    path_expand_limits: PathExpandLimits,
    params: &BTreeMap<String, Value>,
) -> Result<Option<DataFrame>, PlanError> {
    if clause_specs.len() != inputs.len() {
        return Err(PlanError::Unsupported(
            "internal: clause specs/input length mismatch".into(),
        ));
    }

    let mut clause_specs_iter = clause_specs.iter();
    let mut inputs_iter = inputs.into_iter();
    let mut acc: Option<DataFrame> = None;
    let mut all_specs = Vec::new();
    let mut value_vars = BTreeSet::new();
    let lowering = LoweringContext {
        params,
        functions,
        path_expand_limits,
    };

    for clause in &body.clauses {
        let Clause::Match {
            paths,
            where_clause,
            ..
        } = clause
        else {
            if let Clause::Filter(expr) = clause {
                let df = acc
                    .take()
                    .ok_or_else(|| PlanError::Unsupported("FILTER before MATCH".into()))?;
                let (exists, _) = split_conjuncts(Some(expr));
                if exists.is_empty() {
                    let scope = pipeline_scope(&df, &all_specs, &value_vars);
                    acc = Some(df.filter(lower_expr(expr, &scope, params, functions)?)?);
                    continue;
                }
                let clause_spec = clause_specs_iter.next().ok_or_else(|| {
                    PlanError::Unsupported("internal: missing FILTER EXISTS specs".into())
                })?;
                if !clause_spec.specs.is_empty() {
                    return Err(PlanError::Unsupported(
                        "internal: FILTER EXISTS carried MATCH specs".into(),
                    ));
                }
                let filter_inputs = inputs_iter.next().ok_or_else(|| {
                    PlanError::Unsupported("internal: missing FILTER EXISTS inputs".into())
                })?;
                let filter_clause = Some(expr.clone());
                acc = Some(apply_clause_where(
                    df,
                    &filter_clause,
                    &all_specs,
                    &value_vars,
                    &clause_spec.exists,
                    filter_inputs,
                    &lowering,
                )?);
                continue;
            }
            if let Clause::Let(bindings) = clause {
                let mut df = acc
                    .take()
                    .ok_or_else(|| PlanError::Unsupported("LET before MATCH".into()))?;
                for (var, expr) in bindings {
                    if value_vars.contains(var) || is_bound_element_var(&all_specs, var) {
                        return Err(PlanError::Unsupported(format!("re-binding variable {var}")));
                    }
                    let scope = pipeline_scope(&df, &all_specs, &value_vars);
                    df = df.with_column(var, lower_expr(expr, &scope, params, functions)?)?;
                    value_vars.insert(var.clone());
                }
                acc = Some(df);
                continue;
            }
            if let Clause::For { var, list } = clause {
                if value_vars.contains(var) || is_bound_element_var(&all_specs, var) {
                    return Err(PlanError::Unsupported(format!("re-binding variable {var}")));
                }
                if matches!(list, Expr::Literal(Literal::Null)) {
                    return Ok(None);
                }
                let mut df = acc
                    .take()
                    .ok_or_else(|| PlanError::Unsupported("FOR before MATCH".into()))?;
                let scope = pipeline_scope(&df, &all_specs, &value_vars);
                df = df.with_column(var, lower_expr(list, &scope, params, functions)?)?;
                df = df.unnest_columns_with_options(
                    &[var.as_str()],
                    UnnestOptions {
                        preserve_nulls: false,
                        ..Default::default()
                    },
                )?;
                value_vars.insert(var.clone());
                acc = Some(df);
                continue;
            }
            return Err(PlanError::Unsupported(PIPELINE_UNSUPPORTED.into()));
        };
        let clause_spec = clause_specs_iter
            .next()
            .ok_or_else(|| PlanError::Unsupported("internal: missing clause specs".into()))?;
        if let Some(var) = clause_spec
            .specs
            .iter()
            .find(|spec| !spec.var.starts_with(SYNTH_PREFIX) && value_vars.contains(&spec.var))
            .map(|spec| spec.var.clone())
        {
            return Err(PlanError::Unsupported(format!("re-binding variable {var}")));
        }
        let mut clause_inputs = inputs_iter
            .next()
            .ok_or_else(|| PlanError::Unsupported("internal: missing clause inputs".into()))?;
        let mut specs_remaining = clause_spec.specs.as_slice();
        let mut clause_df: Option<DataFrame> = None;
        let mut clause_specs_so_far: Vec<ScanSpec> = Vec::new();

        for path in paths {
            let path_len = 1 + 2 * path.hops.len();
            if specs_remaining.len() < path_len || clause_inputs.len() < path_len {
                return Err(PlanError::Unsupported(
                    "internal: path specs/input length mismatch".into(),
                ));
            }
            let specs = &specs_remaining[..path_len];
            let path_inputs = clause_inputs.drain(..path_len).collect::<Vec<_>>();
            if path_inputs
                .iter()
                .any(|input| matches!(input, ScanInput::Batch(None)))
            {
                if clause_spec.optional {
                    let path_df = empty_path_dataframe(specs, &path_inputs, functions, &[])?;
                    clause_df = Some(match clause_df {
                        Some(left) => join_dataframes(
                            left,
                            path_df,
                            &shared_vars(&clause_specs_so_far, specs),
                            JoinType::Inner,
                        )?,
                        None => path_df,
                    });
                    clause_specs_so_far.extend_from_slice(specs);
                    all_specs.extend_from_slice(specs);
                    specs_remaining = &specs_remaining[path_len..];
                    continue;
                }
                return Ok(None);
            }
            let path_df = path_dataframe(path, where_clause, specs, path_inputs, &lowering, &[])?;
            clause_df = Some(match clause_df {
                Some(left) => join_dataframes(
                    left,
                    path_df,
                    &shared_vars(&clause_specs_so_far, specs),
                    JoinType::Inner,
                )?,
                None => path_df,
            });
            clause_specs_so_far.extend_from_slice(specs);
            all_specs.extend_from_slice(specs);
            specs_remaining = &specs_remaining[path_len..];
        }
        let mut exists_inputs = Some(clause_inputs);

        let mut clause_df =
            clause_df.ok_or_else(|| PlanError::Unsupported("MATCH without path".into()))?;
        let (exists, rest) = split_conjuncts(where_clause.as_ref());
        if clause_spec.optional {
            clause_df = apply_exists_conjuncts(
                clause_df,
                &exists,
                &clause_spec.exists,
                exists_inputs.take().ok_or_else(|| {
                    PlanError::Unsupported("internal: missing OPTIONAL EXISTS inputs".into())
                })?,
                &lowering,
            )?;
        }
        let mut joined = match acc {
            Some(left) => {
                if clause_spec.optional {
                    join_optional_dataframes(
                        left,
                        clause_df,
                        &clause_spec.shared_vars,
                        &rest,
                        PredicateScope {
                            specs: &all_specs,
                            value_vars: &value_vars,
                        },
                        &lowering,
                    )?
                } else {
                    join_dataframes(left, clause_df, &clause_spec.shared_vars, JoinType::Inner)?
                }
            }
            None => {
                if clause_spec.optional {
                    return Err(PlanError::Unsupported(
                        "OPTIONAL MATCH cannot be the first clause".into(),
                    ));
                }
                clause_df
            }
        };
        if !clause_spec.optional {
            joined = apply_clause_where(
                joined,
                where_clause,
                &all_specs,
                &value_vars,
                &clause_spec.exists,
                exists_inputs.take().ok_or_else(|| {
                    PlanError::Unsupported("internal: missing MATCH EXISTS inputs".into())
                })?,
                &lowering,
            )?;
        }
        acc = Some(joined);
    }

    let df = acc
        .ok_or_else(|| PlanError::Unsupported("first clause must be non-OPTIONAL MATCH".into()))?;
    Ok(Some(project_return_body_frame(
        df,
        &body.ret,
        &all_specs,
        &value_vars,
        params,
        functions,
    )?))
}

pub async fn binding_rows(
    body: &QueryBody,
    clause_specs: &[ClauseSpecs],
    inputs: Vec<Vec<ScanInput>>,
    functions: &FunctionRegistry,
    params: &BTreeMap<String, Value>,
    vars: &[String],
) -> Result<Vec<BTreeMap<String, Iid>>, PlanError> {
    binding_rows_with_limits(
        body,
        clause_specs,
        inputs,
        functions,
        PathExpandLimits::default(),
        params,
        vars,
    )
    .await
}

pub async fn binding_rows_with_limits(
    body: &QueryBody,
    clause_specs: &[ClauseSpecs],
    inputs: Vec<Vec<ScanInput>>,
    functions: &FunctionRegistry,
    path_expand_limits: PathExpandLimits,
    params: &BTreeMap<String, Value>,
    vars: &[String],
) -> Result<Vec<BTreeMap<String, Iid>>, PlanError> {
    let batches = execute_body_with_limits(
        body,
        clause_specs,
        inputs,
        functions,
        path_expand_limits,
        params,
    )
    .await?;
    let mut seen = BTreeSet::new();
    let mut rows = Vec::new();

    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            let mut tuple = Vec::with_capacity(vars.len());
            for var in vars {
                tuple.push(
                    binding_iid(&batch, var, row_idx)?
                        .ok_or_else(|| PlanError::Unsupported("null binding iid".into()))?,
                );
            }
            if seen.insert(tuple.clone()) {
                rows.push(vars.iter().cloned().zip(tuple).collect::<BTreeMap<_, _>>());
            }
        }
    }

    rows.sort_by(|left, right| {
        let left_tuple = vars.iter().map(|var| left[var]).collect::<Vec<_>>();
        let right_tuple = vars.iter().map(|var| right[var]).collect::<Vec<_>>();
        left_tuple.cmp(&right_tuple)
    });
    Ok(rows)
}

/// Reads one projected element binding. Mutation planning and query binding
/// extraction share this validation so IID width/null semantics cannot drift.
pub fn binding_iid(
    batch: &RecordBatch,
    var: &str,
    row_idx: usize,
) -> Result<Option<Iid>, PlanError> {
    let (idx, _) = batch
        .schema()
        .column_with_name(var)
        .ok_or_else(|| PlanError::UnknownColumn(var.to_string()))?;
    let column = batch
        .column(idx)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .ok_or_else(|| PlanError::Unsupported(format!("binding column {var} is not an iid")))?;
    if column.value_length() != 16 {
        return Err(PlanError::Unsupported(format!(
            "binding column {var} has iid width {}",
            column.value_length()
        )));
    }
    if column.is_null(row_idx) {
        return Ok(None);
    }
    let bytes: [u8; 16] = column
        .value(row_idx)
        .try_into()
        .map_err(|_| PlanError::Unsupported("binding iid must be 16 bytes".into()))?;
    Ok(Some(Iid::from_bytes(bytes)))
}

pub async fn union_query_results(
    first: Vec<RecordBatch>,
    unions: Vec<(UnionKind, Vec<RecordBatch>)>,
    functions: &FunctionRegistry,
) -> Result<Vec<RecordBatch>, PlanError> {
    Ok(union_query_results_stream(first, unions, functions)
        .await?
        .try_collect::<Vec<_>>()
        .await?)
}

pub async fn union_query_results_stream(
    first: Vec<RecordBatch>,
    unions: Vec<(UnionKind, Vec<RecordBatch>)>,
    functions: &FunctionRegistry,
) -> Result<SendableRecordBatchStream, PlanError> {
    let expected_schema = first
        .first()
        .map(|batch| batch.schema())
        .ok_or_else(|| PlanError::Unsupported("UNION body produced no schema".into()))?;
    let mut df = batches_dataframe(first, functions)?;
    for (kind, batches) in unions {
        let schema = batches
            .first()
            .map(|batch| batch.schema())
            .ok_or_else(|| PlanError::Unsupported("UNION body produced no schema".into()))?;
        if !same_projected_schema(expected_schema.as_ref(), schema.as_ref()) {
            return Err(datafusion::error::DataFusionError::Plan(
                "UNION inputs must have identical column names and types".into(),
            )
            .into());
        }
        df = df.union(batches_dataframe(batches, functions)?)?;
        if matches!(kind, UnionKind::Distinct) {
            df = df.distinct()?;
        }
    }
    Ok(df.execute_stream().await?)
}

fn same_projected_schema(left: &Schema, right: &Schema) -> bool {
    left.fields().len() == right.fields().len()
        && left
            .fields()
            .iter()
            .zip(right.fields())
            .all(|(left, right)| {
                left.name() == right.name() && left.data_type() == right.data_type()
            })
}

fn batches_dataframe(
    batches: Vec<RecordBatch>,
    functions: &FunctionRegistry,
) -> Result<DataFrame, PlanError> {
    let schema = batches
        .first()
        .map(|batch| batch.schema())
        .ok_or_else(|| PlanError::Unsupported("UNION body produced no schema".into()))?;
    let table = MemTable::try_new(schema, vec![batches])?;
    Ok(session_context(functions).read_table(Arc::new(table))?)
}

struct LoweringContext<'a> {
    params: &'a BTreeMap<String, Value>,
    functions: &'a FunctionRegistry,
    path_expand_limits: PathExpandLimits,
}

#[derive(Clone, Copy)]
struct PredicateScope<'a> {
    specs: &'a [ScanSpec],
    value_vars: &'a BTreeSet<String>,
}

fn path_dataframe(
    path: &PathPattern,
    where_clause: &Option<Expr>,
    specs: &[ScanSpec],
    inputs: Vec<ScanInput>,
    lowering: &LoweringContext<'_>,
    preserve_vars: &[String],
) -> Result<DataFrame, PlanError> {
    let ctx = session_context(lowering.functions);
    let mut frames: Vec<Option<DataFrame>> = Vec::with_capacity(specs.len());
    let mut adjacencies: Vec<Option<Arc<EdgeAdjacency>>> = Vec::with_capacity(specs.len());
    let mut row_counts: Vec<usize> = Vec::with_capacity(specs.len());
    for (i, (spec, input)) in specs.iter().zip(inputs).enumerate() {
        match input {
            ScanInput::Batch(None) => {
                return Err(PlanError::Unsupported(
                    "empty path input is not supported by this execution path".into(),
                ));
            }
            ScanInput::Batch(Some(batch)) => {
                row_counts.push(batch.num_rows());
                let batch = mangle_batch(&spec.var, &batch)?;
                let schema = batch.schema();
                let table = MemTable::try_new(schema, vec![vec![batch]])?;
                let df = ctx.read_table(Arc::new(table))?;
                let df = apply_element_predicates(
                    df,
                    &spec.var,
                    element_props(path, i),
                    where_clause,
                    lowering.params,
                    lowering.functions,
                )?;
                let df = if preserve_vars.contains(&spec.var) {
                    df.with_column(&exists_join_key(&spec.var), col(mangled(&spec.var, "_iid")))?
                } else {
                    df
                };
                frames.push(Some(df));
                adjacencies.push(None);
            }
            ScanInput::Adjacency(adj) => {
                row_counts.push(0);
                frames.push(None);
                adjacencies.push(Some(adj));
            }
        }
    }

    let has_expand = specs
        .iter()
        .any(|s| matches!(s.kind, SpecKind::Expand { .. }));
    let forward = has_expand
        || row_counts.first().copied().unwrap_or(0) <= row_counts.last().copied().unwrap_or(0);
    join_chain(
        frames,
        adjacencies,
        specs,
        path,
        forward,
        lowering.path_expand_limits,
    )
}

fn empty_path_dataframe(
    specs: &[ScanSpec],
    inputs: &[ScanInput],
    functions: &FunctionRegistry,
    extra_keys: &[String],
) -> Result<DataFrame, PlanError> {
    let ctx = session_context(functions);
    let mut fields = Vec::new();
    let mut columns = Vec::new();
    for (spec, input) in specs.iter().zip(inputs) {
        match input {
            ScanInput::Batch(Some(batch)) => {
                let batch = mangle_batch(&spec.var, batch)?;
                for field in batch.schema().fields() {
                    fields.push(field.as_ref().clone());
                    columns.push(new_empty_array(field.data_type()));
                }
            }
            ScanInput::Batch(None) => {
                for field in empty_scan_fields(spec) {
                    columns.push(new_empty_array(field.data_type()));
                    fields.push(field);
                }
            }
            ScanInput::Adjacency(_) => {}
        }
    }
    if fields.is_empty() {
        fields.push(Field::new("__empty_optional", DataType::Int64, true));
        columns.push(new_empty_array(&DataType::Int64));
    }
    for key in extra_keys {
        if fields.iter().any(|field| field.name() == key) {
            continue;
        }
        let field = Field::new(key.clone(), DataType::FixedSizeBinary(16), false);
        columns.push(new_empty_array(field.data_type()));
        fields.push(field);
    }
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(varve_index::IndexError::Arrow)
        .map_err(PlanError::Index)?;
    let schema = batch.schema();
    let table = MemTable::try_new(schema, vec![vec![batch]])?;
    Ok(ctx.read_table(Arc::new(table))?)
}

fn empty_scan_fields(spec: &ScanSpec) -> Vec<Field> {
    let mut fields = vec![Field::new(
        mangled(&spec.var, "_iid"),
        DataType::FixedSizeBinary(16),
        false,
    )];
    if matches!(spec.kind, SpecKind::Edge { .. }) {
        fields.push(Field::new(
            mangled(&spec.var, "_src_iid"),
            DataType::FixedSizeBinary(16),
            false,
        ));
        fields.push(Field::new(
            mangled(&spec.var, "_dst_iid"),
            DataType::FixedSizeBinary(16),
            false,
        ));
    }
    fields
}

fn shared_vars(left_specs: &[ScanSpec], right_specs: &[ScanSpec]) -> Vec<String> {
    let left = left_specs
        .iter()
        .filter(|spec| !spec.var.starts_with(SYNTH_PREFIX))
        .map(|spec| spec.var.as_str())
        .collect::<BTreeSet<_>>();
    right_specs
        .iter()
        .filter(|spec| !spec.var.starts_with(SYNTH_PREFIX))
        .filter_map(|spec| left.contains(spec.var.as_str()).then_some(spec.var.clone()))
        .collect()
}

fn is_bound_element_var(specs: &[ScanSpec], var: &str) -> bool {
    specs
        .iter()
        .any(|spec| !spec.var.starts_with(SYNTH_PREFIX) && spec.var == var)
}

fn pipeline_scope(
    df: &DataFrame,
    specs: &[ScanSpec],
    value_vars: &BTreeSet<String>,
) -> Scope<'static> {
    let vars = specs
        .iter()
        .map(|spec| spec.var.as_str())
        .collect::<Vec<_>>();
    let mut scope = scope_from_schema(df.schema(), &vars);
    scope.value_vars = value_vars.clone();
    scope
}

fn apply_clause_where(
    mut df: DataFrame,
    where_clause: &Option<Expr>,
    specs: &[ScanSpec],
    value_vars: &BTreeSet<String>,
    exists_specs: &[ExistsSpecs],
    exists_inputs: Vec<ScanInput>,
    lowering: &LoweringContext<'_>,
) -> Result<DataFrame, PlanError> {
    let Some(where_clause) = where_clause else {
        if !exists_inputs.is_empty() {
            return Err(PlanError::Unsupported(
                "internal: unexpected EXISTS inputs".into(),
            ));
        }
        return Ok(df);
    };
    let (exists, rest) = split_conjuncts(Some(where_clause));
    if exists.len() != exists_specs.len() {
        return Err(PlanError::Unsupported(
            "internal: EXISTS specs/input length mismatch".into(),
        ));
    }
    let vars = specs
        .iter()
        .map(|spec| spec.var.as_str())
        .collect::<Vec<_>>();
    let mut scope = scope_from_schema(df.schema(), &vars);
    scope.value_vars = value_vars.clone();
    for expr in rest {
        df = df.filter(lower_expr(
            expr,
            &scope,
            lowering.params,
            lowering.functions,
        )?)?;
    }
    apply_exists_conjuncts(df, &exists, exists_specs, exists_inputs, lowering)
}

fn apply_exists_conjuncts(
    mut df: DataFrame,
    exists: &[ExistsConjunct<'_>],
    exists_specs: &[ExistsSpecs],
    mut exists_inputs: Vec<ScanInput>,
    lowering: &LoweringContext<'_>,
) -> Result<DataFrame, PlanError> {
    if exists.len() != exists_specs.len() {
        return Err(PlanError::Unsupported(
            "internal: EXISTS specs/input length mismatch".into(),
        ));
    }
    for (exists, specs) in exists.iter().copied().zip(exists_specs) {
        let subquery = exists_dataframe(exists, specs, &mut exists_inputs, lowering)?;
        let join_type = if exists.negated {
            JoinType::LeftAnti
        } else {
            JoinType::LeftSemi
        };
        df = join_exists(
            df,
            subquery,
            &specs.shared_vars,
            &specs.shared_keys,
            join_type,
        )?;
    }
    if !exists_inputs.is_empty() {
        return Err(PlanError::Unsupported(
            "internal: unused EXISTS inputs".into(),
        ));
    }
    Ok(df)
}

fn exists_dataframe(
    exists: ExistsConjunct<'_>,
    exists_specs: &ExistsSpecs,
    inputs: &mut Vec<ScanInput>,
    lowering: &LoweringContext<'_>,
) -> Result<DataFrame, PlanError> {
    let inner_where = exists.where_clause.cloned();
    let mut specs_remaining = exists_specs.specs.as_slice();
    let mut df: Option<DataFrame> = None;
    let mut specs_so_far = Vec::new();
    for path in exists.paths {
        let path_len = 1 + 2 * path.hops.len();
        if specs_remaining.len() < path_len || inputs.len() < path_len {
            return Err(PlanError::Unsupported(
                "internal: EXISTS path specs/input length mismatch".into(),
            ));
        }
        let path_specs = &specs_remaining[..path_len];
        let path_inputs = inputs.drain(..path_len).collect::<Vec<_>>();
        let path_df = if path_inputs
            .iter()
            .any(|input| matches!(input, ScanInput::Batch(None)))
        {
            empty_path_dataframe(
                path_specs,
                &path_inputs,
                lowering.functions,
                &exists_specs.shared_keys,
            )?
        } else {
            path_dataframe(
                path,
                &inner_where,
                path_specs,
                path_inputs,
                lowering,
                &exists_specs.shared_vars,
            )?
        };
        df = Some(match df {
            Some(left) => join_dataframes(
                left,
                path_df,
                &shared_vars(&specs_so_far, path_specs),
                JoinType::Inner,
            )?,
            None => path_df,
        });
        specs_so_far.extend_from_slice(path_specs);
        specs_remaining = &specs_remaining[path_len..];
    }
    let df = df.ok_or_else(|| PlanError::Unsupported("EXISTS without path".into()))?;
    apply_where(
        df,
        &inner_where,
        &exists_specs.specs,
        lowering.params,
        lowering.functions,
    )
}

fn join_dataframes(
    left: DataFrame,
    mut right: DataFrame,
    shared_vars: &[String],
    join_type: JoinType,
) -> Result<DataFrame, PlanError> {
    if shared_vars.is_empty() {
        let left = left.with_column("__cross", lit(1i64))?;
        let right = right.with_column("__cross_right", lit(1i64))?;
        return Ok(left
            .join(right, join_type, &["__cross"], &["__cross_right"], None)?
            .drop_columns(&["__cross", "__cross_right"])?);
    }
    let keys = shared_vars
        .iter()
        .map(|var| mangled(var, "_iid"))
        .collect::<Vec<_>>();
    let mut right_keys = Vec::with_capacity(keys.len());
    let mut drop_cols = Vec::new();
    for (var, key) in shared_vars.iter().zip(&keys) {
        let right_key = format!("__join_{key}");
        right = right.with_column(&right_key, col(key.clone()))?;
        right_keys.push(right_key);
        let prefix = format!("{var}__");
        drop_cols.extend(
            right
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().to_string())
                .filter(|name| name.starts_with(&prefix)),
        );
    }
    if !drop_cols.is_empty() {
        let drop_refs = drop_cols.iter().map(String::as_str).collect::<Vec<_>>();
        right = right.drop_columns(&drop_refs)?;
    }
    let key_refs = keys.iter().map(String::as_str).collect::<Vec<_>>();
    let right_key_refs = right_keys.iter().map(String::as_str).collect::<Vec<_>>();
    Ok(left
        .join(right, join_type, &key_refs, &right_key_refs, None)?
        .drop_columns(&right_key_refs)?)
}

fn join_optional_dataframes(
    left: DataFrame,
    mut right: DataFrame,
    shared_vars: &[String],
    predicates: &[&Expr],
    predicate_scope: PredicateScope<'_>,
    lowering: &LoweringContext<'_>,
) -> Result<DataFrame, PlanError> {
    let mut conditions = Vec::new();
    let mut right_keys = Vec::new();
    let mut drop_cols = Vec::new();

    if shared_vars.is_empty() {
        let left = left.with_column("__cross", lit(1i64))?;
        right = right.with_column("__cross_right", lit(1i64))?;
        conditions.push(col("__cross").eq(col("__cross_right")));
        right_keys.push("__cross_right".to_string());
        return finish_optional_join(
            left,
            right,
            conditions,
            predicates,
            predicate_scope,
            lowering,
            &["__cross", "__cross_right"],
        );
    }

    for var in shared_vars {
        let key = mangled(var, "_iid");
        let right_key = format!("__join_{key}");
        right = right.with_column(&right_key, col(key.clone()))?;
        conditions.push(col(key).eq(col(right_key.clone())));
        right_keys.push(right_key);
        let prefix = format!("{var}__");
        drop_cols.extend(
            right
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().to_string())
                .filter(|name| name.starts_with(&prefix)),
        );
    }
    if !drop_cols.is_empty() {
        let refs = drop_cols.iter().map(String::as_str).collect::<Vec<_>>();
        right = right.drop_columns(&refs)?;
    }
    let drop_after = right_keys.iter().map(String::as_str).collect::<Vec<_>>();
    finish_optional_join(
        left,
        right,
        conditions,
        predicates,
        predicate_scope,
        lowering,
        &drop_after,
    )
}

fn finish_optional_join(
    left: DataFrame,
    right: DataFrame,
    mut conditions: Vec<DfExpr>,
    predicates: &[&Expr],
    predicate_scope: PredicateScope<'_>,
    lowering: &LoweringContext<'_>,
    drop_after: &[&str],
) -> Result<DataFrame, PlanError> {
    let vars = predicate_scope
        .specs
        .iter()
        .map(|spec| spec.var.as_str())
        .collect::<Vec<_>>();
    let mut scope = scope_from_join_schemas(left.schema(), right.schema(), &vars);
    scope.value_vars = predicate_scope.value_vars.clone();
    for predicate in predicates {
        conditions.push(lower_expr(
            predicate,
            &scope,
            lowering.params,
            lowering.functions,
        )?);
    }
    Ok(left
        .join_on(right, JoinType::Left, conditions)?
        .drop_columns(drop_after)?)
}

fn scope_from_join_schemas(
    left: &datafusion::common::DFSchema,
    right: &datafusion::common::DFSchema,
    vars: &[&str],
) -> Scope<'static> {
    let names = left
        .fields()
        .iter()
        .chain(right.fields())
        .map(|field| field.name().to_string())
        .collect::<BTreeSet<_>>();
    let elements = vars
        .iter()
        .map(|var| {
            let prefix = format!("{var}__");
            let available = names
                .iter()
                .filter(|name| name.starts_with(&prefix))
                .cloned()
                .collect();
            ((*var).to_string(), ElementCols { available })
        })
        .collect();
    Scope::new(elements, BTreeSet::new(), BTreeSet::new())
}

fn join_exists(
    left: DataFrame,
    right: DataFrame,
    shared_vars: &[String],
    shared_keys: &[String],
    join_type: JoinType,
) -> Result<DataFrame, PlanError> {
    if shared_vars.is_empty() {
        return Err(PlanError::Unsupported(
            "EXISTS must share variable enclosing pattern".into(),
        ));
    }
    if shared_vars.len() != shared_keys.len() {
        return Err(PlanError::Unsupported(
            "internal: EXISTS shared key length mismatch".into(),
        ));
    }
    let keys = shared_vars
        .iter()
        .map(|var| mangled(var, "_iid"))
        .collect::<Vec<_>>();
    let key_refs = keys.iter().map(String::as_str).collect::<Vec<_>>();
    let right_key_refs = shared_keys.iter().map(String::as_str).collect::<Vec<_>>();
    Ok(left.join(right, join_type, &key_refs, &right_key_refs, None)?)
}

fn exists_join_key(var: &str) -> String {
    format!("__exists_key_{var}")
}

/// Rebuilds `batch` with every column renamed `{var}__{name}` — zero-copy
/// (the column arrays are shared, only the schema is rebuilt).
pub(crate) fn mangle_batch(var: &str, batch: &RecordBatch) -> Result<RecordBatch, PlanError> {
    let mut fields = Vec::new();
    let mut columns = Vec::new();
    for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
        fields.push(Field::new(
            mangled(var, field.name()),
            field.data_type().clone(),
            field.is_nullable(),
        ));
        columns.push(column.clone());
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(varve_index::IndexError::Arrow)
        .map_err(PlanError::Index)
}

/// The inline `{k: v}` properties on element `i` of the path.
fn element_props(path: &PathPattern, i: usize) -> &[(String, Expr)] {
    if i == 0 {
        &path.start.props
    } else if i % 2 == 1 {
        &path.hops[(i - 1) / 2].0.props
    } else {
        &path.hops[(i - 2) / 2].1.props
    }
}

/// Applies an element's inline-prop equalities and, if it names this element,
/// the query WHERE equality — all as filters over the mangled scan, before any
/// join. An inline `{_id: …}` is applied too: it is a real (Int/Str) column
/// (the writer keeps `_id` in the doc), and re-applying it after the iid
/// pushdown is harmless.
fn apply_element_predicates(
    mut df: DataFrame,
    var: &str,
    props: &[(String, Expr)],
    where_clause: &Option<Expr>,
    params: &BTreeMap<String, Value>,
    functions: &FunctionRegistry,
) -> Result<DataFrame, PlanError> {
    let schema = df.schema().clone();
    let scope = scope_from_schema(&schema, &[var]);
    for (k, v) in props {
        let expr = Expr::Binary {
            op: varve_gql::ast::BinaryOp::Eq,
            lhs: Box::new(Expr::Prop {
                var: var.to_string(),
                prop: k.clone(),
            }),
            rhs: Box::new(v.clone()),
        };
        df = df.filter(lower_expr(&expr, &scope, params, functions)?)?;
    }
    if let Some(where_clause) = where_clause {
        let (_, rest) = split_conjuncts(Some(where_clause));
        for expr in rest {
            if expr_references_only_element(expr, var) {
                df = df.filter(lower_expr(expr, &scope, params, functions)?)?;
            }
        }
    }
    Ok(df)
}

/// One hop step: `acc ⋈ edge` on the near node's iid, then `⋈ far node`. For
/// an `Out` edge the src endpoint is on the path's left; `In` swaps src/dst.
/// Walking the chain backward swaps which side is "near" — hence `near_is_left`.
fn apply_where(
    mut df: DataFrame,
    where_clause: &Option<Expr>,
    specs: &[ScanSpec],
    params: &BTreeMap<String, Value>,
    functions: &FunctionRegistry,
) -> Result<DataFrame, PlanError> {
    let Some(where_clause) = where_clause else {
        return Ok(df);
    };
    let (exists, rest) = split_conjuncts(Some(where_clause));
    if !exists.is_empty() {
        return Err(PlanError::Unsupported(
            "EXISTS outside top-level WHERE conjunction".into(),
        ));
    }
    let vars: Vec<&str> = specs.iter().map(|spec| spec.var.as_str()).collect();
    let scope = scope_from_schema(df.schema(), &vars);
    for expr in rest {
        df = df.filter(lower_expr(expr, &scope, params, functions)?)?;
    }
    Ok(df)
}

fn scope_from_schema(schema: &datafusion::common::DFSchema, vars: &[&str]) -> Scope<'static> {
    let mut elements = BTreeMap::new();
    for var in vars {
        let prefix = format!("{var}__");
        let available = schema
            .fields()
            .iter()
            .map(|field| field.name().to_string())
            .filter(|name| name.starts_with(&prefix))
            .collect();
        elements.insert((*var).to_string(), ElementCols { available });
    }
    Scope::new(elements, BTreeSet::new(), BTreeSet::new())
}

fn expr_references_only_element(expr: &Expr, var: &str) -> bool {
    let mut vars = BTreeSet::new();
    collect_prop_vars(expr, &mut vars);
    vars.is_empty() || (vars.len() == 1 && vars.contains(var))
}

fn collect_prop_vars(expr: &Expr, vars: &mut BTreeSet<String>) {
    match expr {
        Expr::Prop { var, .. } => {
            vars.insert(var.clone());
        }
        Expr::List(items) => {
            for item in items {
                collect_prop_vars(item, vars);
            }
        }
        Expr::Unary { expr, .. } | Expr::Cast { expr, .. } => collect_prop_vars(expr, vars),
        Expr::Binary { lhs, rhs, .. } => {
            collect_prop_vars(lhs, vars);
            collect_prop_vars(rhs, vars);
        }
        Expr::Case {
            operand,
            whens,
            otherwise,
        } => {
            if let Some(operand) = operand {
                collect_prop_vars(operand, vars);
            }
            for (when_expr, then_expr) in whens {
                collect_prop_vars(when_expr, vars);
                collect_prop_vars(then_expr, vars);
            }
            if let Some(otherwise) = otherwise {
                collect_prop_vars(otherwise, vars);
            }
        }
        Expr::FnCall { args, .. } => {
            for arg in args {
                collect_prop_vars(arg, vars);
            }
        }
        Expr::Exists { where_clause, .. } => {
            if let Some(where_clause) = where_clause {
                collect_prop_vars(where_clause, vars);
            }
        }
        Expr::Literal(_) | Expr::Param(_) | Expr::Var(_) | Expr::Star => {}
    }
}

#[allow(clippy::too_many_arguments)]
fn join_hop(
    acc: DataFrame,
    edge_frame: DataFrame,
    node_frame: DataFrame,
    edge: &EdgePattern,
    near_var: &str,
    far_var: &str,
    edge_var: &str,
    near_is_left: bool,
) -> Result<DataFrame, PlanError> {
    let (left_end, right_end) = match edge.direction {
        Direction::Out => ("_src_iid", "_dst_iid"),
        Direction::In => ("_dst_iid", "_src_iid"),
    };
    let (near_end, far_end) = if near_is_left {
        (left_end, right_end)
    } else {
        (right_end, left_end)
    };
    let near_iid = mangled(near_var, "_iid");
    let near_key = mangled(edge_var, near_end);
    let acc = acc.join(
        edge_frame,
        JoinType::Inner,
        &[near_iid.as_str()],
        &[near_key.as_str()],
        None,
    )?;
    let far_key = mangled(edge_var, far_end);
    let far_iid = mangled(far_var, "_iid");
    Ok(acc.join(
        node_frame,
        JoinType::Inner,
        &[far_key.as_str()],
        &[far_iid.as_str()],
        None,
    )?)
}

/// A quantified hop: builds a [`PathExpandNode`] over `acc` (which ends at the
/// hop's start node `prev_var`), then joins its produced `expand_iid` to the
/// end node's `_iid`. Direction is already baked into `adjacency`'s orientation
/// by the engine, so no edge-direction handling is needed here.
#[allow(clippy::too_many_arguments)]
fn expand_hop(
    acc: DataFrame,
    end_frame: DataFrame,
    adjacency: Arc<EdgeAdjacency>,
    prev_var: &str,
    end_var: &str,
    path_var: Option<String>,
    min: u32,
    max: u32,
    path_expand_limits: PathExpandLimits,
) -> Result<DataFrame, PlanError> {
    let (state, plan) = acc.into_parts();
    let node = PathExpandNode::try_new_with_limits(
        plan,
        adjacency,
        mangled(prev_var, "_iid"),
        mangled(end_var, "expand_iid"),
        path_var.as_ref().map(|p| mangled(p, "path")),
        min,
        max,
        path_expand_limits,
    )?;
    let plan = LogicalPlan::Extension(Extension {
        node: Arc::new(node),
    });
    let acc = DataFrame::new(state, plan);
    let expand_key = mangled(end_var, "expand_iid");
    let end_iid = mangled(end_var, "_iid");
    Ok(acc.join(
        end_frame,
        JoinType::Inner,
        &[expand_key.as_str()],
        &[end_iid.as_str()],
        None,
    )?)
}

fn join_chain(
    mut frames: Vec<Option<DataFrame>>,
    mut adjacencies: Vec<Option<Arc<EdgeAdjacency>>>,
    specs: &[ScanSpec],
    path: &PathPattern,
    forward: bool,
    path_expand_limits: PathExpandLimits,
) -> Result<DataFrame, PlanError> {
    // Element order in specs/frames is `[n0, e0, n1, e1, n2, …]`: node i is
    // index `2*i`, hop i's edge is index `1 + 2*i`.
    let hops = path.hops.len();
    fn take(frames: &mut [Option<DataFrame>], idx: usize) -> Result<DataFrame, PlanError> {
        frames.get_mut(idx).and_then(Option::take).ok_or_else(|| {
            PlanError::Unsupported("internal: missing element frame in join chain".into())
        })
    }
    if forward {
        // A quantified hop anchors on the start side, so any Expand forces the
        // forward walk (see the `forward` computation in `execute_pattern`).
        let mut acc = take(&mut frames, 0)?;
        for i in 0..hops {
            let edge_idx = 1 + 2 * i;
            let node_idx = 2 + 2 * i;
            if let SpecKind::Expand {
                min, max, path_var, ..
            } = &specs[edge_idx].kind
            {
                let adjacency = adjacencies
                    .get_mut(edge_idx)
                    .and_then(Option::take)
                    .ok_or_else(|| {
                        PlanError::Unsupported(
                            "internal: missing adjacency for quantified hop".into(),
                        )
                    })?;
                let end_frame = take(&mut frames, node_idx)?;
                acc = expand_hop(
                    acc,
                    end_frame,
                    adjacency,
                    &specs[2 * i].var,
                    &specs[node_idx].var,
                    path_var.clone(),
                    *min,
                    *max,
                    path_expand_limits,
                )?;
            } else {
                let edge_frame = take(&mut frames, edge_idx)?;
                let node_frame = take(&mut frames, node_idx)?;
                acc = join_hop(
                    acc,
                    edge_frame,
                    node_frame,
                    &path.hops[i].0,
                    &specs[2 * i].var,
                    &specs[node_idx].var,
                    &specs[edge_idx].var,
                    true,
                )?;
            }
        }
        Ok(acc)
    } else {
        let mut acc = take(&mut frames, 2 * hops)?;
        for i in (0..hops).rev() {
            let edge_frame = take(&mut frames, 1 + 2 * i)?;
            let node_frame = take(&mut frames, 2 * i)?;
            acc = join_hop(
                acc,
                edge_frame,
                node_frame,
                &path.hops[i].0,
                &specs[2 + 2 * i].var,
                &specs[2 * i].var,
                &specs[1 + 2 * i].var,
                false,
            )?;
        }
        Ok(acc)
    }
}

/// Projects the RETURN clause over the joined, mangled frame: `var.prop` reads
/// `mangled(var, prop)` and outputs the alias (or the bare prop name), so the
/// mangling never leaks into result column names.
async fn project_return(
    df: DataFrame,
    stmt: &QueryStmt,
    specs: &[ScanSpec],
    params: &BTreeMap<String, Value>,
    functions: &FunctionRegistry,
) -> Result<Vec<RecordBatch>, PlanError> {
    let query = degenerate_query(stmt)?;
    project_return_body(df, query.ret, specs, &BTreeSet::new(), params, functions).await
}

async fn project_return_body(
    df: DataFrame,
    ret: &ReturnClause,
    specs: &[ScanSpec],
    value_vars: &BTreeSet<String>,
    params: &BTreeMap<String, Value>,
    functions: &FunctionRegistry,
) -> Result<Vec<RecordBatch>, PlanError> {
    collect_preserving_schema(project_return_body_frame(
        df, ret, specs, value_vars, params, functions,
    )?)
    .await
}

fn project_return_body_frame(
    mut df: DataFrame,
    ret: &ReturnClause,
    specs: &[ScanSpec],
    value_vars: &BTreeSet<String>,
    params: &BTreeMap<String, Value>,
    functions: &FunctionRegistry,
) -> Result<DataFrame, PlanError> {
    let vars: Vec<&str> = specs.iter().map(|spec| spec.var.as_str()).collect();
    let mut scope = scope_from_schema(df.schema(), &vars);
    scope.value_vars = value_vars.clone();

    let has_aggregate = ret
        .items
        .iter()
        .any(|(expr, _)| contains_aggregate(expr, functions));

    let mut aggregate_exprs = Vec::new();
    let mut aggregate_aliases = Vec::new();
    let mut group_aliases = Vec::new();
    if has_aggregate {
        let mut group_exprs = Vec::new();
        let mut agg_exprs = Vec::new();
        for (expr, _) in &ret.items {
            if contains_aggregate(expr, functions) {
                collect_aggregate_exprs(expr, functions, &mut aggregate_exprs);
            } else {
                let (group_name, added) =
                    add_internal_alias(&mut group_aliases, expr, "__varve_group");
                if added {
                    group_exprs
                        .push(lower_expr(expr, &scope, params, functions)?.alias(group_name));
                }
            }
        }
        for expr in &aggregate_exprs {
            let (aggregate_name, _) =
                add_internal_alias(&mut aggregate_aliases, expr, "__varve_agg");
            agg_exprs.push(lower_return_aggregate(
                expr,
                &aggregate_name,
                &scope,
                params,
                functions,
            )?);
        }
        df = df.aggregate(group_exprs, agg_exprs)?;
        for (_, alias) in group_aliases.iter().chain(aggregate_aliases.iter()) {
            scope.value_vars.insert(alias.clone());
        }
    }

    let mut projection = Vec::new();
    let mut output_names = Vec::new();
    for (idx, (expr, alias)) in ret.items.iter().enumerate() {
        if !has_aggregate {
            if let Expr::Var(var) = expr {
                if is_bound_element_var(specs, var) && !value_vars.contains(var) {
                    let output_var = alias.as_deref().unwrap_or(var);
                    project_bare_element(
                        df.schema(),
                        var,
                        output_var,
                        &mut projection,
                        &mut output_names,
                    );
                    continue;
                }
                let path_col = mangled(var, "path");
                if df.schema().has_column_with_unqualified_name(&path_col) {
                    let out_name = alias.clone().unwrap_or_else(|| var.clone());
                    projection.push(col(path_col).alias(out_name.clone()));
                    output_names.push(out_name);
                    continue;
                }
            }
        }

        let out_name = alias
            .clone()
            .unwrap_or_else(|| implicit_return_name(expr, idx));
        let df_expr = if has_aggregate && contains_aggregate(expr, functions) {
            let expr =
                replace_aggregate_exprs(expr, functions, &aggregate_aliases, &group_aliases)?;
            lower_expr(&expr, &scope, params, functions)?
        } else if has_aggregate {
            let alias = alias_for_expr(&group_aliases, expr)
                .ok_or_else(|| PlanError::Unsupported("RETURN grouping expression".into()))?;
            output_column(alias)
        } else {
            lower_expr(expr, &scope, params, functions)?
        };
        projection.push(df_expr.alias(out_name.clone()));
        output_names.push(out_name);
    }

    df = df.select(projection)?;
    if ret.distinct {
        df = df.distinct()?;
    }
    if !ret.order_by.is_empty() {
        let mut sort_exprs = Vec::with_capacity(ret.order_by.len());
        for item in &ret.order_by {
            let expr = order_by_output_expr(item, ret, &output_names)?;
            sort_exprs.push(expr.sort(item.asc, !item.asc));
        }
        df = df.sort(sort_exprs)?;
    }
    if ret.skip.is_some() || ret.limit.is_some() {
        let skip = usize_limit(ret.skip.unwrap_or(0))?;
        let fetch = ret.limit.map(usize_limit).transpose()?;
        df = df.limit(skip, fetch)?;
    }
    Ok(df)
}

fn order_by_output_expr(
    item: &SortItem,
    ret: &ReturnClause,
    output_names: &[String],
) -> Result<DfExpr, PlanError> {
    if let Expr::Var(var) = &item.expr {
        if output_names.iter().any(|name| name == var) {
            return Ok(output_column(var.clone()));
        }
    }
    let rendered = implicit_return_name(&item.expr, 0);
    if output_names.iter().any(|name| name == &rendered) {
        return Ok(output_column(rendered));
    }
    for (idx, ((expr, alias), output_name)) in ret.items.iter().zip(output_names).enumerate() {
        if &item.expr == expr
            || alias
                .as_ref()
                .is_some_and(|alias| matches!(&item.expr, Expr::Var(var) if var == alias))
        {
            return Ok(output_column(output_name.clone()));
        }
        if implicit_return_name(expr, idx) == implicit_return_name(&item.expr, idx) {
            return Ok(output_column(output_name.clone()));
        }
    }
    Err(PlanError::Unsupported(
        "ORDER BY expression must refer to RETURN output".into(),
    ))
}

fn output_column(name: impl Into<String>) -> DfExpr {
    DfExpr::Column(Column::new_unqualified(name))
}

fn project_bare_element(
    schema: &datafusion::common::DFSchema,
    input_var: &str,
    output_var: &str,
    projection: &mut Vec<DfExpr>,
    output_names: &mut Vec<String>,
) {
    let prefix = format!("{input_var}__");
    let mut suffixes = schema
        .fields()
        .iter()
        .filter_map(|field| field.name().strip_prefix(&prefix).map(str::to_string))
        .collect::<Vec<_>>();
    suffixes.sort();

    let mut ordered = Vec::new();
    for required in ["_iid", "_labels", "_src_iid", "_dst_iid"] {
        if suffixes.iter().any(|suffix| suffix == required) {
            ordered.push(required.to_string());
        }
    }
    ordered.extend(suffixes.into_iter().filter(|suffix| {
        !matches!(
            suffix.as_str(),
            "_iid"
                | "_labels"
                | "_src_iid"
                | "_dst_iid"
                | "_system_from"
                | "_system_to"
                | "_valid_from"
                | "_valid_to"
                | "path"
        )
    }));

    for suffix in ordered {
        let input_name = mangled(input_var, &suffix);
        let output_name = format!("{output_var}.{suffix}");
        projection.push(col(input_name).alias(output_name.clone()));
        output_names.push(output_name);
    }
}

async fn collect_preserving_schema(df: DataFrame) -> Result<Vec<RecordBatch>, PlanError> {
    let schema = Arc::new(df.schema().as_arrow().clone());
    let batches = df.collect().await?;
    if batches.is_empty() {
        Ok(vec![RecordBatch::new_empty(schema)])
    } else {
        Ok(batches)
    }
}

fn usize_limit(value: u64) -> Result<usize, PlanError> {
    usize::try_from(value).map_err(|_| {
        PlanError::Unsupported(format!("RETURN limit value {value} exceeds platform usize"))
    })
}

fn contains_aggregate(expr: &Expr, functions: &FunctionRegistry) -> bool {
    match expr {
        Expr::FnCall { name, args, .. } => {
            functions.is_aggregate(name)
                || args.iter().any(|arg| contains_aggregate(arg, functions))
        }
        Expr::List(items) => items.iter().any(|item| contains_aggregate(item, functions)),
        Expr::Unary { expr, .. } | Expr::Cast { expr, .. } => contains_aggregate(expr, functions),
        Expr::Binary { lhs, rhs, .. } => {
            contains_aggregate(lhs, functions) || contains_aggregate(rhs, functions)
        }
        Expr::Case {
            operand,
            whens,
            otherwise,
        } => {
            operand
                .as_deref()
                .is_some_and(|expr| contains_aggregate(expr, functions))
                || whens.iter().any(|(when_expr, then_expr)| {
                    contains_aggregate(when_expr, functions)
                        || contains_aggregate(then_expr, functions)
                })
                || otherwise
                    .as_deref()
                    .is_some_and(|expr| contains_aggregate(expr, functions))
        }
        Expr::Exists { where_clause, .. } => where_clause
            .as_deref()
            .is_some_and(|expr| contains_aggregate(expr, functions)),
        Expr::Literal(_) | Expr::Param(_) | Expr::Prop { .. } | Expr::Var(_) | Expr::Star => false,
    }
}

fn collect_aggregate_exprs(expr: &Expr, functions: &FunctionRegistry, out: &mut Vec<Expr>) {
    match expr {
        Expr::FnCall { name, args, .. } if functions.is_aggregate(name) => {
            if !out.iter().any(|existing| existing == expr) {
                out.push(expr.clone());
            }
            for arg in args {
                if contains_aggregate(arg, functions) {
                    collect_aggregate_exprs(arg, functions, out);
                }
            }
        }
        Expr::FnCall { args, .. } | Expr::List(args) => {
            for arg in args {
                collect_aggregate_exprs(arg, functions, out);
            }
        }
        Expr::Unary { expr, .. } | Expr::Cast { expr, .. } => {
            collect_aggregate_exprs(expr, functions, out);
        }
        Expr::Binary { lhs, rhs, .. } => {
            collect_aggregate_exprs(lhs, functions, out);
            collect_aggregate_exprs(rhs, functions, out);
        }
        Expr::Case {
            operand,
            whens,
            otherwise,
        } => {
            if let Some(expr) = operand {
                collect_aggregate_exprs(expr, functions, out);
            }
            for (when_expr, then_expr) in whens {
                collect_aggregate_exprs(when_expr, functions, out);
                collect_aggregate_exprs(then_expr, functions, out);
            }
            if let Some(expr) = otherwise {
                collect_aggregate_exprs(expr, functions, out);
            }
        }
        Expr::Exists { where_clause, .. } => {
            if let Some(expr) = where_clause {
                collect_aggregate_exprs(expr, functions, out);
            }
        }
        Expr::Literal(_) | Expr::Param(_) | Expr::Prop { .. } | Expr::Var(_) | Expr::Star => {}
    }
}

fn add_internal_alias(
    aliases: &mut Vec<(Expr, String)>,
    expr: &Expr,
    prefix: &str,
) -> (String, bool) {
    if let Some((_, alias)) = aliases.iter().find(|(existing, _)| existing == expr) {
        return (alias.clone(), false);
    }
    let alias = format!("{prefix}_{}", aliases.len());
    aliases.push((expr.clone(), alias.clone()));
    (alias, true)
}

fn alias_for_expr(aliases: &[(Expr, String)], expr: &Expr) -> Option<String> {
    aliases
        .iter()
        .find(|(existing, _)| existing == expr)
        .map(|(_, alias)| alias.clone())
}

fn replace_aggregate_exprs(
    expr: &Expr,
    functions: &FunctionRegistry,
    aggregate_aliases: &[(Expr, String)],
    group_aliases: &[(Expr, String)],
) -> Result<Expr, PlanError> {
    if let Some(alias) = alias_for_expr(group_aliases, expr) {
        return Ok(Expr::Var(alias));
    }
    match expr {
        Expr::FnCall { name, .. } if functions.is_aggregate(name) => {
            let alias = alias_for_expr(aggregate_aliases, expr).ok_or_else(|| {
                PlanError::Unsupported("internal aggregate RETURN alias missing".into())
            })?;
            Ok(Expr::Var(alias))
        }
        Expr::FnCall {
            name,
            args,
            distinct,
        } => Ok(Expr::FnCall {
            name: name.clone(),
            args: args
                .iter()
                .map(|arg| {
                    replace_aggregate_exprs(arg, functions, aggregate_aliases, group_aliases)
                })
                .collect::<Result<Vec<_>, _>>()?,
            distinct: *distinct,
        }),
        Expr::List(items) => Ok(Expr::List(
            items
                .iter()
                .map(|item| {
                    replace_aggregate_exprs(item, functions, aggregate_aliases, group_aliases)
                })
                .collect::<Result<Vec<_>, _>>()?,
        )),
        Expr::Unary { op, expr } => Ok(Expr::Unary {
            op: op.clone(),
            expr: Box::new(replace_aggregate_exprs(
                expr,
                functions,
                aggregate_aliases,
                group_aliases,
            )?),
        }),
        Expr::Binary { op, lhs, rhs } => Ok(Expr::Binary {
            op: op.clone(),
            lhs: Box::new(replace_aggregate_exprs(
                lhs,
                functions,
                aggregate_aliases,
                group_aliases,
            )?),
            rhs: Box::new(replace_aggregate_exprs(
                rhs,
                functions,
                aggregate_aliases,
                group_aliases,
            )?),
        }),
        Expr::Case {
            operand,
            whens,
            otherwise,
        } => Ok(Expr::Case {
            operand: operand
                .as_ref()
                .map(|expr| {
                    replace_aggregate_exprs(expr, functions, aggregate_aliases, group_aliases)
                        .map(Box::new)
                })
                .transpose()?,
            whens: whens
                .iter()
                .map(|(when_expr, then_expr)| {
                    Ok((
                        replace_aggregate_exprs(
                            when_expr,
                            functions,
                            aggregate_aliases,
                            group_aliases,
                        )?,
                        replace_aggregate_exprs(
                            then_expr,
                            functions,
                            aggregate_aliases,
                            group_aliases,
                        )?,
                    ))
                })
                .collect::<Result<Vec<_>, PlanError>>()?,
            otherwise: otherwise
                .as_ref()
                .map(|expr| {
                    replace_aggregate_exprs(expr, functions, aggregate_aliases, group_aliases)
                        .map(Box::new)
                })
                .transpose()?,
        }),
        Expr::Cast { expr, ty } => Ok(Expr::Cast {
            expr: Box::new(replace_aggregate_exprs(
                expr,
                functions,
                aggregate_aliases,
                group_aliases,
            )?),
            ty: ty.clone(),
        }),
        Expr::Exists {
            paths,
            where_clause,
        } => Ok(Expr::Exists {
            paths: paths.clone(),
            where_clause: where_clause
                .as_ref()
                .map(|expr| {
                    replace_aggregate_exprs(expr, functions, aggregate_aliases, group_aliases)
                        .map(Box::new)
                })
                .transpose()?,
        }),
        Expr::Literal(_) | Expr::Param(_) | Expr::Prop { .. } | Expr::Var(_) | Expr::Star => {
            Ok(expr.clone())
        }
    }
}

fn lower_return_aggregate(
    expr: &Expr,
    output_name: &str,
    scope: &Scope<'_>,
    params: &BTreeMap<String, Value>,
    functions: &FunctionRegistry,
) -> Result<DfExpr, PlanError> {
    let Expr::FnCall {
        name,
        args,
        distinct,
    } = expr
    else {
        return Err(PlanError::Unsupported(
            "aggregate expressions cannot be nested in RETURN".into(),
        ));
    };
    if !functions.is_aggregate(name) {
        return Err(PlanError::Unsupported(
            "aggregate expressions cannot be nested in RETURN".into(),
        ));
    }
    let lowered_args = if matches!(args.as_slice(), [Expr::Star]) {
        vec![lit(1_i64)]
    } else {
        args.iter()
            .map(|arg| lower_expr(arg, scope, params, functions))
            .collect::<Result<Vec<_>, _>>()?
    };
    Ok(lower_aggregate(name, lowered_args, *distinct)?.alias(output_name))
}

fn implicit_return_name(expr: &Expr, idx: usize) -> String {
    let rendered = display_expr(expr);
    if rendered.is_empty() {
        format!("__varve_ret_{idx}")
    } else {
        rendered
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::StringArray;
    use varve_gql::ast::Statement;

    /// Parses `gql` and unwraps the `Query` variant — every case here is a
    /// `MATCH … RETURN …` read, never an INSERT/DELETE.
    fn query(gql: &str) -> QueryStmt {
        match varve_gql::parse(gql).unwrap() {
            Statement::Query(q) => *q,
            other => panic!("expected a query statement, got {other:?}"),
        }
    }

    #[test]
    fn mangle_batch_keeps_labels_for_bare_return() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("_labels", DataType::Utf8, false),
                Field::new("name", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["A"])),
                Arc::new(StringArray::from(vec![Some("Ada")])),
            ],
        )
        .unwrap();

        let mangled = mangle_batch("n", &batch).unwrap();
        assert!(mangled.column_by_name("n___labels").is_some());
        assert!(mangled.column_by_name("n__name").is_some());
    }

    // ---- scan_specs rejection branches -------------------------------

    #[test]
    fn scan_specs_emits_multi_label_node() {
        // The grammar allows `(a:A:B)` (see varve-gql's
        // `parses_node_props_and_multi_labels_in_match`), and lowering must
        // preserve the full label spec for engine-side label filtering.
        let stmt = query("MATCH (a:A:B) RETURN a.name");
        let specs = scan_specs_for_stmt(
            &stmt,
            DEFAULT_GRAPH,
            DEFAULT_MAX_PATH_DEPTH,
            &BTreeMap::new(),
        )
        .unwrap();
        assert_eq!(
            specs[0].kind,
            SpecKind::Node {
                labels: LabelSpec::All(vec!["A".to_string(), "B".to_string()]),
                iid_point: None,
            }
        );
    }

    #[test]
    fn scan_specs_rejects_comma_separated_multi_path() {
        // `scan_specs` only lowers `stmt.paths[0]`; more than one
        // comma-separated path in a MATCH is rejected up front.
        let stmt = query("MATCH (a:A)-[:K]->(b:A), (c:A)-[:K]->(d:A) RETURN a.name");
        let err = scan_specs_for_stmt(
            &stmt,
            DEFAULT_GRAPH,
            DEFAULT_MAX_PATH_DEPTH,
            &BTreeMap::new(),
        )
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("query shape is not supported by this execution path"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn scan_specs_walks_all_match_clauses_body() {
        let stmt = query("MATCH (a:A)-[:K]->(b:A) MATCH (b:A)-[:K]->(c:A) RETURN c.name");
        let specs = scan_specs(&stmt.first, DEFAULT_GRAPH, DEFAULT_MAX_PATH_DEPTH).unwrap();

        assert_eq!(specs.len(), 2);
        assert_eq!(specs[1].shared_vars, vec!["b".to_string()]);
    }

    #[test]
    fn scan_specs_rejects_quantifier_max_exceeding_max_path_depth() {
        // The quantifier's own `{1,99}` max is within the parser's bounds,
        // but 99 exceeds the `max_path_depth` we pass to `scan_specs`.
        let stmt = query("MATCH (a:A)-[:K]->{1,99}(b:A) RETURN a.name");
        let err = scan_specs_for_stmt(&stmt, DEFAULT_GRAPH, 10, &BTreeMap::new()).unwrap_err();
        assert!(
            err.to_string()
                .contains("quantifier max 99 exceeds max_path_depth 10"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn scan_specs_rejects_path_var_on_multiple_hops() {
        let stmt = query("MATCH p = (a:A)-[:K]->(b:A)-[:K]->(c:A) RETURN p");
        let err = scan_specs_for_stmt(
            &stmt,
            DEFAULT_GRAPH,
            DEFAULT_MAX_PATH_DEPTH,
            &BTreeMap::new(),
        )
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("path variables need a single hop in v1"),
            "unexpected error: {err}"
        );
    }
}
