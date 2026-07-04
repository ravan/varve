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

#[derive(Debug, Clone, PartialEq)]
pub struct ReturnItem {
    pub var: String,
    pub prop: String,
    pub alias: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueryStmt {
    pub pattern: NodePattern,
    pub where_clause: Option<Expr>,
    pub return_items: Vec<ReturnItem>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Insert(InsertStmt),
    Query(QueryStmt),
}
