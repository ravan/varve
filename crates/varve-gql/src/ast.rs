use varve_types::{Instant, TemporalDimension};

#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    Int(i64),
    Float(f64),
    Str(String),
    Bool(bool),
    Null,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NodePattern {
    pub var: Option<String>,
    pub labels: Vec<String>,
    pub props: Vec<(String, Literal)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Out, // (a)-[..]->(b)
    In,  // (a)<-[..]-(b)
}

/// `{n}` ⇒ min=n, max=Some(n); `{m,n}` ⇒ (m, Some(n)); `{m,}` ⇒ (m, None);
/// `*` ⇒ (0, None). `None` max is capped to `max_path_depth` at lowering.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Quantifier {
    pub min: u32,
    pub max: Option<u32>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct EdgePattern {
    pub var: Option<String>,
    pub label: String, // required in v1 (decision 15)
    pub props: Vec<(String, Literal)>,
    pub direction: Direction,
    pub quantifier: Option<Quantifier>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PathPattern {
    /// `p = (a)-[:K]->(b)` path variable.
    pub var: Option<String>,
    pub start: NodePattern,
    pub hops: Vec<(EdgePattern, NodePattern)>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MatchPart {
    /// v1: single-node patterns only (paths in mutation reads: slice 7).
    pub patterns: Vec<NodePattern>,
    pub where_clause: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct InsertStmt {
    pub match_part: Option<MatchPart>,
    pub paths: Vec<PathPattern>,
    /// `INSERT … VALID FROM <dt> [TO <dt>]` / `VALID TO <dt>` — applies to
    /// every node in the statement. `None` defers to the engine's default
    /// (valid_from = system time, valid_to = end of time).
    pub valid_from: Option<Instant>,
    pub valid_to: Option<Instant>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    PropEq {
        var: String,
        prop: String,
        value: Literal,
    },
}

/// `FOR VALID_TIME …` / `FOR SYSTEM_TIME …` clauses, either at query level
/// (before the first `MATCH`) or per-`MATCH` (immediately after the pattern).
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct TemporalClauses {
    pub valid: Option<TemporalDimension>,
    pub system: Option<TemporalDimension>,
}

/// History-access functions usable in `RETURN`: `valid_from(x)`, `valid_to(x)`,
/// `system_from(x)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TemporalFnKind {
    ValidFrom,
    ValidTo,
    SystemFrom,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ReturnItem {
    Prop {
        var: String,
        prop: String,
        alias: Option<String>,
    },
    /// `valid_from(x)` / `valid_to(x)` / `system_from(x)`.
    TemporalFn {
        func: TemporalFnKind,
        var: String,
        alias: Option<String>,
    },
    /// Bare `RETURN p` for a path variable.
    Var { var: String, alias: Option<String> },
}

#[derive(Debug, Clone, PartialEq)]
pub struct DeleteStmt {
    pub pattern: NodePattern,
    pub where_clause: Option<Expr>,
    /// The identifier named after `DELETE`; must equal `pattern.var`.
    pub target: String,
    pub detach: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueryStmt {
    /// Query-level `FOR VALID_TIME`/`FOR SYSTEM_TIME` clauses, given before
    /// the (first) `MATCH`.
    pub temporal: TemporalClauses,
    pub paths: Vec<PathPattern>,
    /// Per-`MATCH` `FOR VALID_TIME`/`FOR SYSTEM_TIME` clauses, given right
    /// after the pattern; these override the query-level clauses.
    pub match_temporal: TemporalClauses,
    pub where_clause: Option<Expr>,
    pub return_items: Vec<ReturnItem>,
}

impl QueryStmt {
    /// v1 helper: the single node of a hop-free, single-path, unnamed MATCH.
    pub fn single_node(&self) -> Option<&NodePattern> {
        match self.paths.as_slice() {
            [p] if p.hops.is_empty() && p.var.is_none() => Some(&p.start),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Insert(InsertStmt),
    Query(Box<QueryStmt>),
    Delete(DeleteStmt),
}
