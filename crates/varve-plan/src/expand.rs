//! `PathExpand` — quantified-path traversal (slice 6, task 9).
//!
//! Two layers live here:
//!  1. A **pure WALK-semantics core** ([`EdgeAdjacency`] + [`expand_paths`]):
//!     given an already-resolved (bounds-applied) adjacency and a start node,
//!     it enumerates every path of `min..=max` hops, breadth-wise, allowing
//!     repeated nodes and edges (a cycle simply re-traverses; termination is
//!     the depth cap alone). This is the semantic contract of the whole
//!     database's traversal and is property-tested against a naive walker.
//!  2. A **custom DataFusion operator** ([`PathExpandNode`] /
//!     `PathExpandExec` / `PathExpandPlanner` / `VarveQueryPlanner`) that runs
//!     the pure core per input row inside a query plan, plus
//!     [`session_context`] which installs the planner so `pattern.rs` can lower
//!     a quantified hop to `LogicalPlan::Extension`.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, FixedSizeBinaryArray, FixedSizeBinaryBuilder, ListBuilder, RecordBatch,
    UInt32Array,
};
use datafusion::arrow::compute::take;
use datafusion::arrow::datatypes::{DataType, Field, SchemaRef};
use datafusion::common::{
    DFSchema, DFSchemaRef, DataFusionError, Result as DfResult, TableReference,
};
use datafusion::execution::context::{QueryPlanner, TaskContext};
use datafusion::execution::{SessionState, SessionStateBuilder};
use datafusion::logical_expr::{
    Expr, LogicalPlan, UserDefinedLogicalNode, UserDefinedLogicalNodeCore,
};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};
use datafusion::prelude::SessionContext;
use futures::StreamExt;

use crate::PlanError;
use varve_types::Iid;

/// One traversable edge from a node (bounds already applied by the engine).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdjEdge {
    pub neighbor: Iid,
    pub edge: Iid,
}

/// Node → outgoing (or incoming, per the hop's direction — the engine builds
/// the right orientation) traversable edges, each `Vec` sorted by
/// `(neighbor, edge)` for deterministic expansion order.
#[derive(Debug, Default)]
pub struct EdgeAdjacency {
    map: HashMap<Iid, Vec<AdjEdge>>,
}

impl EdgeAdjacency {
    /// `entries` need not be sorted; they are sorted+deduped per node here.
    pub fn from_entries(entries: impl IntoIterator<Item = (Iid, AdjEdge)>) -> Self {
        let mut map: HashMap<Iid, Vec<AdjEdge>> = HashMap::new();
        for (node, edge) in entries {
            map.entry(node).or_default().push(edge);
        }
        for v in map.values_mut() {
            v.sort_by_key(|a| (a.neighbor, a.edge));
            v.dedup();
        }
        EdgeAdjacency { map }
    }

    /// The traversable edges out of `node`, in deterministic `(neighbor, edge)`
    /// order (empty slice if the node has none).
    pub fn neighbors(&self, node: &Iid) -> &[AdjEdge] {
        self.map.get(node).map(Vec::as_slice).unwrap_or(&[])
    }
}

/// Pure WALK-semantics breadth-wise expansion (the exec's core; also the
/// property-test surface). Returns, for the start node, every path of
/// `min..=max` hops as `(end_iid, interleaved [n0, e1, n1, …] path)`.
/// Depth 0 (when `min == 0`) yields `(start, [start])`.
pub fn expand_paths(
    adjacency: &EdgeAdjacency,
    start: Iid,
    min: u32,
    max: u32,
) -> Vec<(Iid, Vec<Iid>)> {
    let mut out = Vec::new();
    let mut frontier: Vec<(Iid, Vec<Iid>)> = vec![(start, vec![start])];
    if min == 0 {
        out.push((start, vec![start]));
    }
    for depth in 1..=max {
        let mut next = Vec::new();
        for (node, path) in &frontier {
            for adj in adjacency.neighbors(node) {
                let mut p = path.clone();
                p.push(adj.edge);
                p.push(adj.neighbor);
                if depth >= min {
                    out.push((adj.neighbor, p.clone()));
                }
                next.push((adj.neighbor, p));
            }
        }
        if next.is_empty() {
            break;
        }
        frontier = next;
    }
    out
}

// ---- DataFusion custom operator ---------------------------------------------

/// Output schema of a `PathExpand`: the input's fields, then `end_col`
/// (`FixedSizeBinary(16)`, non-null), then optionally `path_col`
/// (`List<FixedSizeBinary(16)>`, non-null list, nullable item to match what
/// `ListBuilder<FixedSizeBinaryBuilder>` produces).
fn path_expand_schema(
    input: &LogicalPlan,
    end_col: &str,
    path_col: Option<&str>,
) -> DfResult<DFSchemaRef> {
    let mut fields: Vec<(Option<TableReference>, Arc<Field>)> = input
        .schema()
        .iter()
        .map(|(q, f)| (q.cloned(), Arc::clone(f)))
        .collect();
    fields.push((
        None,
        Arc::new(Field::new(end_col, DataType::FixedSizeBinary(16), false)),
    ));
    if let Some(pc) = path_col {
        // NOTE: the item field is declared NULLABLE to match what
        // `ListBuilder<FixedSizeBinaryBuilder>` produces by default — a
        // non-null item field here would make `RecordBatch::try_new` reject the
        // built arrays on DataType mismatch. Path elements are never actually
        // null.
        fields.push((
            None,
            Arc::new(Field::new(
                pc,
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::FixedSizeBinary(16),
                    true,
                ))),
                false,
            )),
        ));
    }
    Ok(Arc::new(DFSchema::new_with_metadata(
        fields,
        HashMap::new(),
    )?))
}

/// Logical node for a quantified hop. Reads the start iid from `start_col` of
/// its input, and appends `end_col` (the reached node's iid) plus optionally
/// `path_col` (the interleaved node/edge iid list) per produced path.
#[derive(Debug)]
pub struct PathExpandNode {
    input: LogicalPlan,
    adjacency: Arc<EdgeAdjacency>,
    start_col: String,
    end_col: String,
    path_col: Option<String>,
    min: u32,
    max: u32,
    schema: DFSchemaRef,
}

impl PathExpandNode {
    /// `end_col = mangled(end_var, "expand_iid")` — joined to the end node's
    /// `{var}___iid` by the caller; `path_col = mangled(path_var, "path")`.
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        input: LogicalPlan,
        adjacency: Arc<EdgeAdjacency>,
        start_col: String,
        end_col: String,
        path_col: Option<String>,
        min: u32,
        max: u32,
    ) -> Result<Self, PlanError> {
        let schema = path_expand_schema(&input, &end_col, path_col.as_deref())?;
        Ok(Self {
            input,
            adjacency,
            start_col,
            end_col,
            path_col,
            min,
            max,
            schema,
        })
    }
}

// Manual `PartialEq`/`Eq`/`Hash`/`PartialOrd`: the adjacency is EXCLUDED from
// structural equality (compared by `Arc::ptr_eq`) and from hashing — it is an
// opaque, potentially large handle whose identity, not contents, matters for
// plan de-duplication.
impl PartialEq for PathExpandNode {
    fn eq(&self, other: &Self) -> bool {
        self.start_col == other.start_col
            && self.end_col == other.end_col
            && self.path_col == other.path_col
            && self.min == other.min
            && self.max == other.max
            && self.input == other.input
            && Arc::ptr_eq(&self.adjacency, &other.adjacency)
    }
}
impl Eq for PathExpandNode {}
impl std::hash::Hash for PathExpandNode {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.start_col.hash(state);
        self.end_col.hash(state);
        self.path_col.hash(state);
        self.min.hash(state);
        self.max.hash(state);
        self.input.hash(state);
        // adjacency intentionally excluded (see impl PartialEq above).
    }
}
impl PartialOrd for PathExpandNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        (&self.start_col, &self.end_col, self.min, self.max).partial_cmp(&(
            &other.start_col,
            &other.end_col,
            other.min,
            other.max,
        ))
    }
}

impl UserDefinedLogicalNodeCore for PathExpandNode {
    fn name(&self) -> &str {
        "PathExpand"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "PathExpand: {} -[{},{}]-> {}",
            self.start_col, self.min, self.max, self.end_col
        )
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> DfResult<Self> {
        if inputs.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "PathExpand expects exactly one input, got {}",
                inputs.len()
            )));
        }
        let input = inputs.swap_remove(0);
        let schema = path_expand_schema(&input, &self.end_col, self.path_col.as_deref())?;
        Ok(Self {
            input,
            adjacency: Arc::clone(&self.adjacency),
            start_col: self.start_col.clone(),
            end_col: self.end_col.clone(),
            path_col: self.path_col.clone(),
            min: self.min,
            max: self.max,
            schema,
        })
    }
}

/// Runs [`expand_paths`] over one input batch, producing the output batch:
/// each input row is repeated once per produced path (via `take`), with
/// `end_col` and optional `path_col` appended. A zero-total-paths batch yields
/// an empty batch with the output schema.
fn expand_batch(
    batch: &RecordBatch,
    schema: &SchemaRef,
    adjacency: &EdgeAdjacency,
    start_idx: usize,
    min: u32,
    max: u32,
    has_path: bool,
) -> DfResult<RecordBatch> {
    let start = batch
        .column(start_idx)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .ok_or_else(|| {
            DataFusionError::Internal("PathExpand start column is not FixedSizeBinary(16)".into())
        })?;

    let mut indices: Vec<u32> = Vec::new();
    let mut ends = FixedSizeBinaryBuilder::new(16);
    let mut paths = ListBuilder::new(FixedSizeBinaryBuilder::new(16));

    for row in 0..batch.num_rows() {
        let bytes: [u8; 16] = start.value(row).try_into().map_err(|_| {
            DataFusionError::Internal("PathExpand start iid is not 16 bytes".into())
        })?;
        let start_iid = Iid::from_bytes(bytes);
        for (end, path) in expand_paths(adjacency, start_iid, min, max) {
            let idx = u32::try_from(row)
                .map_err(|_| DataFusionError::Internal("PathExpand row index overflow".into()))?;
            indices.push(idx);
            ends.append_value(end.as_bytes())?;
            if has_path {
                for iid in &path {
                    paths.values().append_value(iid.as_bytes())?;
                }
                paths.append(true);
            }
        }
    }

    if indices.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::clone(schema)));
    }

    let idx_array = UInt32Array::from(indices);
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    for col in batch.columns() {
        columns.push(take(col, &idx_array, None)?);
    }
    columns.push(Arc::new(ends.finish()));
    if has_path {
        columns.push(Arc::new(paths.finish()));
    }
    Ok(RecordBatch::try_new(Arc::clone(schema), columns)?)
}

/// Physical plan for [`PathExpandNode`]. Its single input is required to be a
/// single partition; it streams the expansion one input batch at a time.
pub(crate) struct PathExpandExec {
    input: Arc<dyn ExecutionPlan>,
    adjacency: Arc<EdgeAdjacency>,
    schema: SchemaRef,
    start_idx: usize,
    min: u32,
    max: u32,
    has_path: bool,
    cache: Arc<PlanProperties>,
}

impl PathExpandExec {
    fn try_new(node: &PathExpandNode, input: Arc<dyn ExecutionPlan>) -> DfResult<Self> {
        let schema: SchemaRef = Arc::clone(node.schema.inner());
        let start_idx = schema.index_of(&node.start_col).map_err(|_| {
            DataFusionError::Internal(format!(
                "PathExpand start column '{}' not found in input schema",
                node.start_col
            ))
        })?;
        let cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            input,
            adjacency: Arc::clone(&node.adjacency),
            schema,
            start_idx,
            min: node.min,
            max: node.max,
            has_path: node.path_col.is_some(),
            cache,
        })
    }
}

impl fmt::Debug for PathExpandExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "PathExpandExec {{ start_idx: {}, min: {}, max: {}, has_path: {} }}",
            self.start_idx, self.min, self.max, self.has_path
        )
    }
}

impl DisplayAs for PathExpandExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "PathExpandExec: start_idx={}, min={}, max={}",
            self.start_idx, self.min, self.max
        )
    }
}

impl ExecutionPlan for PathExpandExec {
    fn name(&self) -> &str {
        "PathExpandExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "PathExpandExec expects exactly one child, got {}",
                children.len()
            )));
        }
        Ok(Arc::new(PathExpandExec {
            input: children.swap_remove(0),
            adjacency: Arc::clone(&self.adjacency),
            schema: Arc::clone(&self.schema),
            start_idx: self.start_idx,
            min: self.min,
            max: self.max,
            has_path: self.has_path,
            cache: Arc::clone(&self.cache),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        let stream = self.input.execute(partition, context)?;
        let schema = Arc::clone(&self.schema);
        let adjacency = Arc::clone(&self.adjacency);
        let (start_idx, min, max, has_path) = (self.start_idx, self.min, self.max, self.has_path);
        let out = stream.map(move |batch| {
            let batch = batch?;
            expand_batch(&batch, &schema, &adjacency, start_idx, min, max, has_path)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            out,
        )))
    }
}

/// Turns a [`PathExpandNode`] into a [`PathExpandExec`]; delegates every other
/// extension node back to the default planner.
struct PathExpandPlanner;

#[async_trait::async_trait]
impl ExtensionPlanner for PathExpandPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> DfResult<Option<Arc<dyn ExecutionPlan>>> {
        let Some(node) = node.as_any().downcast_ref::<PathExpandNode>() else {
            return Ok(None);
        };
        let input = physical_inputs.first().ok_or_else(|| {
            DataFusionError::Internal("PathExpand requires exactly one physical input".into())
        })?;
        Ok(Some(Arc::new(PathExpandExec::try_new(
            node,
            Arc::clone(input),
        )?)))
    }
}

/// Query planner that installs [`PathExpandPlanner`] on top of the default
/// physical planner; non-PathExpand nodes fall through to the default.
#[derive(Debug)]
struct VarveQueryPlanner;

#[async_trait::async_trait]
impl QueryPlanner for VarveQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let planner =
            DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(PathExpandPlanner)]);
        planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}

/// A [`SessionContext`] with the Varve planner installed. ALL varve-plan query
/// execution goes through this so a `PathExpand` extension node can lower to
/// [`PathExpandExec`].
pub fn session_context() -> SessionContext {
    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_query_planner(Arc::new(VarveQueryPlanner))
        .build();
    SessionContext::new_with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use varve_types::Iid;

    fn n(i: u8) -> Iid {
        Iid::derive("g", "nodes", &[i])
    }
    fn e(i: u8) -> Iid {
        Iid::derive("g", "edges", &[i])
    }

    fn line() -> EdgeAdjacency {
        // 1 -e1-> 2 -e2-> 3 -e3-> 4
        EdgeAdjacency::from_entries([
            (
                n(1),
                AdjEdge {
                    neighbor: n(2),
                    edge: e(1),
                },
            ),
            (
                n(2),
                AdjEdge {
                    neighbor: n(3),
                    edge: e(2),
                },
            ),
            (
                n(3),
                AdjEdge {
                    neighbor: n(4),
                    edge: e(3),
                },
            ),
        ])
    }

    #[test]
    fn expands_min_to_max_hops() {
        let paths = expand_paths(&line(), n(1), 1, 3);
        let ends: Vec<Iid> = paths.iter().map(|(end, _)| *end).collect();
        assert_eq!(ends, vec![n(2), n(3), n(4)]); // breadth order: depth 1, 2, 3
        assert_eq!(paths[1].1, vec![n(1), e(1), n(2), e(2), n(3)]);
    }

    #[test]
    fn zero_length_includes_start() {
        let paths = expand_paths(&line(), n(1), 0, 1);
        assert_eq!(paths[0], (n(1), vec![n(1)]));
        assert_eq!(paths.len(), 2);
    }

    #[test]
    fn walk_semantics_allow_cycles_capped_by_max() {
        // 1 -e1-> 2 -e2-> 1 (cycle)
        let adj = EdgeAdjacency::from_entries([
            (
                n(1),
                AdjEdge {
                    neighbor: n(2),
                    edge: e(1),
                },
            ),
            (
                n(2),
                AdjEdge {
                    neighbor: n(1),
                    edge: e(2),
                },
            ),
        ]);
        let paths = expand_paths(&adj, n(1), 1, 4);
        assert_eq!(paths.len(), 4); // one path per depth 1..=4, repeats allowed
        assert_eq!(paths[3].1.len(), 9);
    }

    #[test]
    fn min_beyond_reachability_is_empty() {
        assert!(expand_paths(&line(), n(4), 1, 3).is_empty());
    }
}
