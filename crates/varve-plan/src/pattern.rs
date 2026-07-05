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
//! already emits; until then a quantified hop lowers to `SpecKind::Expand` and
//! is rejected with `Unsupported` at execution time.

use crate::exec::{iid_of, temporal_fn_columns, to_df_literal};
use crate::PlanError;
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use std::sync::Arc;
use varve_gql::ast::{
    Direction, EdgePattern, Expr, Literal, NodePattern, PathPattern, QueryStmt, ReturnItem,
};
use varve_types::Iid;

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
        label: Option<String>,
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
        props: Vec<(String, Literal)>,
        path_var: Option<String>,
    },
}

/// Placeholder for Task 9's adjacency handle. Task 8 emits it only for a
/// quantified hop, whose `execute_pattern` arm rejects the query with
/// `Unsupported` until Task 9 moves the real type into `expand.rs`.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct EdgeAdjacency;

/// The engine's answer to one [`ScanSpec`], in the same order as `specs`.
pub enum ScanInput {
    Batch(Option<RecordBatch>),
    /// Task 9's adjacency input for an `Expand` element.
    Adjacency(Arc<EdgeAdjacency>),
}

/// Validates the statement (single linear path, at most one label per node,
/// quantifier bounds vs `max_path_depth`, path-var rules) and derives one
/// [`ScanSpec`] per element, in path order `[n0, e0, n1, e1, n2, …]`.
pub fn scan_specs(
    stmt: &QueryStmt,
    graph: &str,
    max_path_depth: u32,
) -> Result<Vec<ScanSpec>, PlanError> {
    if stmt.paths.len() != 1 {
        return Err(PlanError::Unsupported(
            "comma-separated MATCH paths in queries land in slice 7".into(),
        ));
    }
    let path = &stmt.paths[0];
    if path.var.is_some() {
        // A path variable binds the sequence of a single quantified hop
        // (`p = (a)-[:K*1..3]->(b)`); anything else has no v1 meaning.
        let single_quantified = path.hops.len() == 1 && path.hops[0].0.quantifier.is_some();
        if !single_quantified {
            return Err(PlanError::Unsupported(
                "path variables need a single quantified hop in v1".into(),
            ));
        }
    }

    let mut specs = Vec::with_capacity(1 + 2 * path.hops.len());
    specs.push(node_spec(&path.start, 0, graph, &stmt.where_clause)?);
    for (h, (edge, node)) in path.hops.iter().enumerate() {
        specs.push(edge_spec(
            edge,
            1 + 2 * h,
            max_path_depth,
            path.var.clone(),
        )?);
        specs.push(node_spec(node, 2 + 2 * h, graph, &stmt.where_clause)?);
    }
    Ok(specs)
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
) -> Result<ScanSpec, PlanError> {
    if node.labels.len() > 1 {
        return Err(PlanError::Unsupported(
            "multi-label MATCH lands in slice 7".into(),
        ));
    }
    Ok(ScanSpec {
        var: element_var(node.var.as_deref(), idx),
        kind: SpecKind::Node {
            label: node.labels.first().cloned(),
            iid_point: node_iid_point(node, graph, where_clause),
        },
    })
}

/// IID pushdown for a node (spec §10): an inline `{_id: <lit>}` prop, else a
/// `WHERE <this var>._id = <lit>` equality, pins the scan to one entity. Pure
/// access-path optimization — the same equality is re-applied as a filter, so
/// dropping to `None` only widens the scan, never the result.
fn node_iid_point(node: &NodePattern, graph: &str, where_clause: &Option<Expr>) -> Option<Iid> {
    for (k, v) in &node.props {
        if k == "_id" {
            if let Some(iid) = iid_of(graph, NODES_TABLE, v) {
                return Some(iid);
            }
        }
    }
    if let (Some(uvar), Some(Expr::PropEq { var, prop, value })) =
        (node.var.as_deref(), where_clause)
    {
        if var == uvar && prop == "_id" {
            return iid_of(graph, NODES_TABLE, value);
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
        None => SpecKind::Edge {
            label: edge.label.clone(),
            direction: edge.direction,
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
) -> Result<Vec<RecordBatch>, PlanError> {
    let Some(path) = stmt.paths.first() else {
        return Err(PlanError::Unsupported("MATCH without a path".into()));
    };

    // A WHERE equality must name a pattern variable — check once, up front, so
    // `RETURN`-less filters on a bogus var still error (rather than silently
    // matching nothing).
    if let Some(Expr::PropEq { var, .. }) = &stmt.where_clause {
        if !specs.iter().any(|s| &s.var == var) {
            return Err(PlanError::UnknownVariable(var.clone()));
        }
    }

    let ctx = SessionContext::new();

    // 1. One mangled DataFrame per element, with its own predicates applied.
    //    A `None` batch ⇒ that element matched nothing ⇒ the whole join is
    //    empty. `Adjacency` inputs (Task 9) push a `None` frame consumed
    //    positionally by the expansion arm.
    let mut frames: Vec<Option<DataFrame>> = Vec::with_capacity(specs.len());
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
                    &stmt.where_clause,
                )?;
                frames.push(Some(df));
            }
            ScanInput::Adjacency(_) => {
                row_counts.push(0);
                frames.push(None);
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
    let df = join_chain(frames, specs, path, forward)?;

    // 3. RETURN projection over the mangled columns.
    project_return(df, stmt, specs).await
}

/// Rebuilds `batch` with every column renamed `{var}__{name}` — zero-copy
/// (the column arrays are shared, only the schema is rebuilt).
fn mangle_batch(var: &str, batch: &RecordBatch) -> Result<RecordBatch, PlanError> {
    let fields: Vec<Field> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| {
            Field::new(
                mangled(var, f.name()),
                f.data_type().clone(),
                f.is_nullable(),
            )
        })
        .collect();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), batch.columns().to_vec())
        .map_err(varve_index::IndexError::Arrow)
        .map_err(PlanError::Index)
}

/// The inline `{k: v}` properties on element `i` of the path.
fn element_props(path: &PathPattern, i: usize) -> &[(String, Literal)] {
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
    props: &[(String, Literal)],
    where_clause: &Option<Expr>,
) -> Result<DataFrame, PlanError> {
    let schema = df.schema().clone();
    let has_col = |name: &str| schema.has_column_with_unqualified_name(name);
    for (k, v) in props {
        let col_name = mangled(var, k);
        if !has_col(&col_name) {
            return Err(PlanError::UnknownColumn(k.clone()));
        }
        df = df.filter(col(col_name).eq(to_df_literal(v)))?;
    }
    if let Some(Expr::PropEq {
        var: wvar,
        prop,
        value,
    }) = where_clause
    {
        if wvar == var {
            let col_name = mangled(var, prop);
            if !has_col(&col_name) {
                return Err(PlanError::UnknownColumn(prop.clone()));
            }
            df = df.filter(col(col_name).eq(to_df_literal(value)))?;
        }
    }
    Ok(df)
}

/// One hop step: `acc ⋈ edge` on the near node's iid, then `⋈ far node`. For
/// an `Out` edge the src endpoint is on the path's left; `In` swaps src/dst.
/// Walking the chain backward swaps which side is "near" — hence `near_is_left`.
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

fn join_chain(
    mut frames: Vec<Option<DataFrame>>,
    specs: &[ScanSpec],
    path: &PathPattern,
    forward: bool,
) -> Result<DataFrame, PlanError> {
    // Element order in specs/frames is `[n0, e0, n1, e1, n2, …]`: node i is
    // index `2*i`, hop i's edge is index `1 + 2*i`.
    let hops = path.hops.len();
    fn take(frames: &mut [Option<DataFrame>], idx: usize) -> Result<DataFrame, PlanError> {
        frames.get_mut(idx).and_then(Option::take).ok_or_else(|| {
            PlanError::Unsupported("quantified paths land in task 9 of slice 6".into())
        })
    }
    if forward {
        let mut acc = take(&mut frames, 0)?;
        for i in 0..hops {
            let edge_frame = take(&mut frames, 1 + 2 * i)?;
            let node_frame = take(&mut frames, 2 + 2 * i)?;
            acc = join_hop(
                acc,
                edge_frame,
                node_frame,
                &path.hops[i].0,
                &specs[2 * i].var,
                &specs[2 + 2 * i].var,
                &specs[1 + 2 * i].var,
                true,
            )?;
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
) -> Result<Vec<RecordBatch>, PlanError> {
    let schema = df.schema().clone();
    let has_col = |name: &str| schema.has_column_with_unqualified_name(name);
    let is_var = |var: &str| specs.iter().any(|s| s.var == var);

    let mut projection = Vec::new();
    for item in &stmt.return_items {
        let (col_name, out_name) = match item {
            ReturnItem::Prop { var, prop, alias } => {
                if !is_var(var) {
                    return Err(PlanError::UnknownVariable(var.clone()));
                }
                let col_name = mangled(var, prop);
                if !has_col(&col_name) {
                    return Err(PlanError::UnknownColumn(prop.clone()));
                }
                (col_name, alias.clone().unwrap_or_else(|| prop.clone()))
            }
            ReturnItem::TemporalFn { func, var, alias } => {
                if !is_var(var) {
                    return Err(PlanError::UnknownVariable(var.clone()));
                }
                let (hidden, default_name) = temporal_fn_columns(*func);
                let col_name = mangled(var, hidden);
                if !has_col(&col_name) {
                    return Err(PlanError::UnknownColumn(hidden.to_string()));
                }
                (
                    col_name,
                    alias.clone().unwrap_or_else(|| default_name.to_string()),
                )
            }
            ReturnItem::Var { .. } => {
                return Err(PlanError::Unsupported(
                    "path variables land in task 9 of slice 6".into(),
                ));
            }
        };
        projection.push(col(col_name).alias(out_name));
    }
    let df = df.select(projection)?;
    Ok(df.collect().await?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use varve_gql::ast::Statement;

    /// Parses `gql` and unwraps the `Query` variant — every case here is a
    /// `MATCH … RETURN …` read, never an INSERT/DELETE.
    fn query(gql: &str) -> QueryStmt {
        match varve_gql::parse(gql).unwrap() {
            Statement::Query(q) => *q,
            other => panic!("expected a query statement, got {other:?}"),
        }
    }

    // ---- scan_specs rejection branches -------------------------------

    #[test]
    fn scan_specs_rejects_multi_label_node() {
        // `node_spec` rejects as soon as a node pattern carries >1 label;
        // the grammar itself allows `(a:A:B)` (see varve-gql's
        // `parses_node_props_and_multi_labels_in_match`), so this must be
        // rejected at the lowering layer, not the parser.
        let stmt = query("MATCH (a:A:B) RETURN a.name");
        let err = scan_specs(&stmt, DEFAULT_GRAPH, DEFAULT_MAX_PATH_DEPTH).unwrap_err();
        assert!(
            err.to_string()
                .contains("multi-label MATCH lands in slice 7"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn scan_specs_rejects_comma_separated_multi_path() {
        // `scan_specs` only lowers `stmt.paths[0]`; more than one
        // comma-separated path in a MATCH is rejected up front.
        let stmt = query("MATCH (a:A)-[:K]->(b:A), (c:A)-[:K]->(d:A) RETURN a.name");
        let err = scan_specs(&stmt, DEFAULT_GRAPH, DEFAULT_MAX_PATH_DEPTH).unwrap_err();
        assert!(
            err.to_string()
                .contains("comma-separated MATCH paths in queries land in slice 7"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn scan_specs_rejects_quantifier_max_exceeding_max_path_depth() {
        // The quantifier's own `{1,99}` max is within the parser's bounds,
        // but 99 exceeds the `max_path_depth` we pass to `scan_specs`.
        let stmt = query("MATCH (a:A)-[:K]->{1,99}(b:A) RETURN a.name");
        let err = scan_specs(&stmt, DEFAULT_GRAPH, 10).unwrap_err();
        assert!(
            err.to_string()
                .contains("quantifier max 99 exceeds max_path_depth 10"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn scan_specs_rejects_path_var_on_non_single_quantified_hop() {
        // A path variable is only meaningful (in v1) over a single
        // quantified hop (`p = (a)-[:K*1..3]->(b)`); this path var sits on
        // a single but *unquantified* hop, so it must be rejected.
        let stmt = query("MATCH p = (a:A)-[:K]->(b:A) RETURN p");
        let err = scan_specs(&stmt, DEFAULT_GRAPH, DEFAULT_MAX_PATH_DEPTH).unwrap_err();
        assert!(
            err.to_string()
                .contains("path variables need a single quantified hop in v1"),
            "unexpected error: {err}"
        );
    }
}
