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
pub struct InsertNode {
    pub labels: Vec<String>,
    pub props: Vec<(String, Literal)>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct InsertStmt {
    pub nodes: Vec<InsertNode>,
    /// `INSERT … VALID FROM <dt> [TO <dt>]` / `VALID TO <dt>` — applies to
    /// every node in the statement. `None` defers to the engine's default
    /// (valid_from = system time, valid_to = end of time).
    pub valid_from: Option<Instant>,
    pub valid_to: Option<Instant>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NodePattern {
    pub var: String,
    pub label: Option<String>,
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
}

#[derive(Debug, Clone, PartialEq)]
pub struct DeleteStmt {
    pub pattern: NodePattern,
    pub where_clause: Option<Expr>,
    /// The identifier named after `DELETE`; must equal `pattern.var`.
    pub target: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueryStmt {
    /// Query-level `FOR VALID_TIME`/`FOR SYSTEM_TIME` clauses, given before
    /// the (first) `MATCH`.
    pub temporal: TemporalClauses,
    pub pattern: NodePattern,
    /// Per-`MATCH` `FOR VALID_TIME`/`FOR SYSTEM_TIME` clauses, given right
    /// after the pattern; these override the query-level clauses.
    pub match_temporal: TemporalClauses,
    pub where_clause: Option<Expr>,
    pub return_items: Vec<ReturnItem>,
}

// QueryStmt legitimately carries two TemporalClauses (query-level +
// per-MATCH) plus its pattern/where/return payload, so it is much larger
// than InsertStmt or DeleteStmt; boxing it would break the brief's verbatim
// `Statement::Query(q) => q` test helpers, so the size lint is suppressed
// instead of changing the variant's shape.
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum Statement {
    Insert(InsertStmt),
    Query(QueryStmt),
    Delete(DeleteStmt),
}
