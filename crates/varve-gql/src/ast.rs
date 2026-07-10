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
pub enum LabelSpec {
    All(Vec<String>),
    Any(Vec<String>),
}

impl LabelSpec {
    pub fn is_empty(&self) -> bool {
        match self {
            LabelSpec::All(labels) | LabelSpec::Any(labels) => labels.is_empty(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NodePattern {
    pub var: Option<String>,
    pub labels: LabelSpec,
    pub props: Vec<(String, Expr)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Out, // (a)-[..]->(b)
    In,  // (a)<-[..]-(b)
}

/// `{n}` => min=n, max=Some(n); `{m,n}` => (m, Some(n)); `{m,}` => (m, None);
/// `*` => (0, None). `None` max capped to `max_path_depth` at lowering.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Quantifier {
    pub min: u32,
    pub max: Option<u32>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct EdgePattern {
    pub var: Option<String>,
    pub label: String, // required in v1 (decision 15)
    pub props: Vec<(String, Expr)>,
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
    pub paths: Vec<PathPattern>,
    pub where_clause: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct InsertStmt {
    pub match_part: Option<MatchPart>,
    pub paths: Vec<PathPattern>,
    /// `INSERT VALID FROM <dt> [TO <dt>]` / `VALID TO <dt>` applies to every
    /// node in the statement. `None` defers to the engine default.
    pub valid_from: Option<Instant>,
    pub valid_to: Option<Instant>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum UnaryOp {
    Not,
    Neg,
    IsNull,
    IsNotNull,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Eq,
    Neq,
    Lt,
    Lte,
    Gt,
    Gte,
    And,
    Or,
    Xor,
    In,
    StartsWith,
    EndsWith,
    Contains,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CastType {
    Int,
    Float,
    Str,
    Bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Literal(Literal),
    Param(String),
    Prop {
        var: String,
        prop: String,
    },
    Var(String),
    Star,
    List(Vec<Expr>),
    Unary {
        op: UnaryOp,
        expr: Box<Expr>,
    },
    Binary {
        op: BinaryOp,
        lhs: Box<Expr>,
        rhs: Box<Expr>,
    },
    Case {
        operand: Option<Box<Expr>>,
        whens: Vec<(Expr, Expr)>,
        otherwise: Option<Box<Expr>>,
    },
    FnCall {
        name: String,
        args: Vec<Expr>,
        distinct: bool,
    },
    Cast {
        expr: Box<Expr>,
        ty: CastType,
    },
    Exists {
        paths: Vec<PathPattern>,
        where_clause: Option<Box<Expr>>,
    },
}

impl Expr {
    pub fn conjuncts(&self) -> Vec<&Expr> {
        let mut out = Vec::new();
        self.push_conjuncts(&mut out);
        out
    }

    fn push_conjuncts<'a>(&'a self, out: &mut Vec<&'a Expr>) {
        match self {
            Expr::Binary {
                op: BinaryOp::And,
                lhs,
                rhs,
            } => {
                lhs.push_conjuncts(out);
                rhs.push_conjuncts(out);
            }
            other => out.push(other),
        }
    }

    pub fn as_prop_eq(&self) -> Option<(&str, &str, &Expr)> {
        match self {
            Expr::Binary {
                op: BinaryOp::Eq,
                lhs,
                rhs,
            } => match (lhs.as_ref(), rhs.as_ref()) {
                (Expr::Prop { var, prop }, expr) | (expr, Expr::Prop { var, prop }) => {
                    Some((var, prop, expr))
                }
                _ => None,
            },
            _ => None,
        }
    }
}

pub fn display_expr(expr: &Expr) -> String {
    crate::print::print_expr(expr)
}

/// `FOR VALID_TIME ...` / `FOR SYSTEM_TIME ...` clauses.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct TemporalClauses {
    pub valid: Option<TemporalDimension>,
    pub system: Option<TemporalDimension>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Clause {
    Match {
        optional: bool,
        paths: Vec<PathPattern>,
        temporal: TemporalClauses,
        where_clause: Option<Expr>,
    },
    Filter(Expr),
    Let(Vec<(String, Expr)>),
    For {
        var: String,
        list: Expr,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct SortItem {
    pub expr: Expr,
    pub asc: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReturnClause {
    pub distinct: bool,
    pub items: Vec<(Expr, Option<String>)>,
    pub order_by: Vec<SortItem>,
    pub skip: Option<u64>,
    pub limit: Option<u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueryBody {
    pub temporal: TemporalClauses,
    pub clauses: Vec<Clause>,
    pub ret: ReturnClause,
}

#[derive(Debug, Clone, PartialEq)]
pub enum UnionKind {
    Distinct,
    All,
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueryStmt {
    pub first: QueryBody,
    pub unions: Vec<(UnionKind, QueryBody)>,
}

impl QueryStmt {
    /// v1 helper: first-clause, single-node, hop-free, unnamed MATCH.
    pub fn single_node(&self) -> Option<&NodePattern> {
        let [Clause::Match {
            optional: false,
            paths,
            temporal: _,
            where_clause: _,
        }] = self.first.clauses.as_slice()
        else {
            return None;
        };
        match paths.as_slice() {
            [p] if p.hops.is_empty() && p.var.is_none() => Some(&p.start),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum MutKind {
    Delete,
    Erase,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MutateStmt {
    pub match_part: MatchPart,
    pub kind: MutKind,
    pub target: String,
    pub detach: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SetItem {
    Prop {
        var: String,
        prop: String,
        value: Expr,
    },
    Label {
        var: String,
        label: String,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum RemoveItem {
    Prop { var: String, prop: String },
    Label { var: String, label: String },
}

#[derive(Debug, Clone, PartialEq)]
pub struct SetStmt {
    pub match_part: MatchPart,
    pub items: Vec<SetItem>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RemoveStmt {
    pub match_part: MatchPart,
    pub items: Vec<RemoveItem>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum GraphStmt {
    Create(String),
    Drop(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Program {
    pub use_graph: Option<String>,
    pub statements: Vec<Statement>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Insert(InsertStmt),
    Query(Box<QueryStmt>),
    Mutate(MutateStmt),
    Set(SetStmt),
    Remove(RemoveStmt),
    Graph(GraphStmt),
}
