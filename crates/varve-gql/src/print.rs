use crate::ast::*;
use varve_types::{Instant, TemporalDimension};

pub fn to_gql(stmt: &Statement) -> String {
    print_statement(stmt)
}

pub fn to_gql_program(program: &Program) -> String {
    let mut parts = Vec::new();
    if let Some(graph) = &program.use_graph {
        parts.push(format!("USE {graph}"));
    }
    parts.extend(program.statements.iter().map(print_statement));
    parts.join("; ")
}

fn print_statement(stmt: &Statement) -> String {
    match stmt {
        Statement::Insert(stmt) => print_insert(stmt),
        Statement::Query(stmt) => print_query(stmt),
        Statement::Mutate(stmt) => print_mutate(stmt),
        Statement::Set(stmt) => print_set(stmt),
        Statement::Remove(stmt) => print_remove(stmt),
        Statement::Graph(GraphStmt::Create(graph)) => format!("CREATE GRAPH {graph}"),
        Statement::Graph(GraphStmt::Drop(graph)) => format!("DROP GRAPH {graph}"),
    }
}

fn print_insert(stmt: &InsertStmt) -> String {
    let mut out = String::new();
    if let Some(match_part) = &stmt.match_part {
        out.push_str(&print_match_part(match_part));
        out.push(' ');
    }
    out.push_str("INSERT ");
    out.push_str(&join_paths(&stmt.paths));
    match (&stmt.valid_from, &stmt.valid_to) {
        (Some(from), Some(to)) => {
            out.push_str(" VALID FROM ");
            out.push_str(&print_instant(from));
            out.push_str(" TO ");
            out.push_str(&print_instant(to));
        }
        (Some(from), None) => {
            out.push_str(" VALID FROM ");
            out.push_str(&print_instant(from));
        }
        (None, Some(to)) => {
            out.push_str(" VALID TO ");
            out.push_str(&print_instant(to));
        }
        (None, None) => {}
    }
    out
}

fn print_query(stmt: &QueryStmt) -> String {
    let mut out = print_query_body(&stmt.first);
    for (kind, body) in &stmt.unions {
        out.push_str(match kind {
            UnionKind::Distinct => " UNION ",
            UnionKind::All => " UNION ALL ",
        });
        out.push_str(&print_query_body(body));
    }
    out
}

fn print_query_body(body: &QueryBody) -> String {
    let mut parts = Vec::new();
    push_temporal_clauses(&mut parts, &body.temporal);
    parts.extend(body.clauses.iter().map(print_clause));
    parts.push(print_return(&body.ret));
    parts.join(" ")
}

fn print_mutate(stmt: &MutateStmt) -> String {
    let mut out = print_match_part(&stmt.match_part);
    out.push(' ');
    if stmt.detach {
        out.push_str("DETACH ");
    }
    out.push_str(match stmt.kind {
        MutKind::Delete => "DELETE ",
        MutKind::Erase => "ERASE ",
    });
    out.push_str(&stmt.target);
    out
}

fn print_set(stmt: &SetStmt) -> String {
    let mut out = print_match_part(&stmt.match_part);
    out.push_str(" SET ");
    out.push_str(
        &stmt
            .items
            .iter()
            .map(print_set_item)
            .collect::<Vec<_>>()
            .join(", "),
    );
    out
}

fn print_remove(stmt: &RemoveStmt) -> String {
    let mut out = print_match_part(&stmt.match_part);
    out.push_str(" REMOVE ");
    out.push_str(
        &stmt
            .items
            .iter()
            .map(print_remove_item)
            .collect::<Vec<_>>()
            .join(", "),
    );
    out
}

fn print_match_part(part: &MatchPart) -> String {
    let mut out = String::from("MATCH");
    if !part.paths.is_empty() {
        out.push(' ');
        out.push_str(&join_paths(&part.paths));
    }
    if let Some(expr) = &part.where_clause {
        out.push_str(" WHERE ");
        out.push_str(&print_expr(expr));
    }
    out
}

fn print_clause(clause: &Clause) -> String {
    match clause {
        Clause::Match {
            optional,
            paths,
            temporal,
            where_clause,
        } => {
            let mut out = if *optional {
                String::from("OPTIONAL MATCH")
            } else {
                String::from("MATCH")
            };
            if !paths.is_empty() {
                out.push(' ');
                out.push_str(&join_paths(paths));
            }
            let mut temporal_parts = Vec::new();
            push_temporal_clauses(&mut temporal_parts, temporal);
            if !temporal_parts.is_empty() {
                out.push(' ');
                out.push_str(&temporal_parts.join(" "));
            }
            if let Some(expr) = where_clause {
                out.push_str(" WHERE ");
                out.push_str(&print_expr(expr));
            }
            out
        }
        Clause::Filter(expr) => format!("FILTER {}", print_expr(expr)),
        Clause::Let(items) => {
            let items = items
                .iter()
                .map(|(var, expr)| format!("{var} = {}", print_expr(expr)))
                .collect::<Vec<_>>()
                .join(", ");
            format!("LET {items}")
        }
        Clause::For { var, list } => format!("FOR {var} IN {}", print_expr(list)),
    }
}

fn print_return(ret: &ReturnClause) -> String {
    let mut out = String::from("RETURN");
    if ret.distinct {
        out.push_str(" DISTINCT");
    }
    out.push(' ');
    out.push_str(
        &ret.items
            .iter()
            .map(|(expr, alias)| {
                let mut item = print_expr(expr);
                if let Some(alias) = alias {
                    item.push_str(" AS ");
                    item.push_str(alias);
                }
                item
            })
            .collect::<Vec<_>>()
            .join(", "),
    );
    if !ret.order_by.is_empty() {
        out.push_str(" ORDER BY ");
        out.push_str(
            &ret.order_by
                .iter()
                .map(|item| {
                    format!(
                        "{} {}",
                        print_expr(&item.expr),
                        if item.asc { "ASC" } else { "DESC" }
                    )
                })
                .collect::<Vec<_>>()
                .join(", "),
        );
    }
    if let Some(skip) = ret.skip {
        out.push_str(" SKIP ");
        out.push_str(&skip.to_string());
    }
    if let Some(limit) = ret.limit {
        out.push_str(" LIMIT ");
        out.push_str(&limit.to_string());
    }
    out
}

fn print_set_item(item: &SetItem) -> String {
    match item {
        SetItem::Prop { var, prop, value } => format!("{var}.{prop} = {}", print_expr(value)),
        SetItem::Label { var, label } => format!("{var}:{label}"),
    }
}

fn print_remove_item(item: &RemoveItem) -> String {
    match item {
        RemoveItem::Prop { var, prop } => format!("{var}.{prop}"),
        RemoveItem::Label { var, label } => format!("{var}:{label}"),
    }
}

fn join_paths(paths: &[PathPattern]) -> String {
    paths.iter().map(print_path).collect::<Vec<_>>().join(", ")
}

fn print_path(path: &PathPattern) -> String {
    let mut out = String::new();
    if let Some(var) = &path.var {
        out.push_str(var);
        out.push_str(" = ");
    }
    out.push_str(&print_node(&path.start));
    for (edge, node) in &path.hops {
        out.push_str(&print_edge(edge));
        out.push_str(&print_node(node));
    }
    out
}

fn print_node(node: &NodePattern) -> String {
    let mut out = String::from("(");
    if let Some(var) = &node.var {
        out.push_str(var);
    }
    out.push_str(&print_labels(&node.labels));
    if !node.props.is_empty() {
        out.push(' ');
        out.push_str(&print_props(&node.props));
    }
    out.push(')');
    out
}

fn print_edge(edge: &EdgePattern) -> String {
    let mut out = String::new();
    if edge.direction == Direction::In {
        out.push('<');
    }
    out.push_str("-[");
    if let Some(var) = &edge.var {
        out.push_str(var);
    }
    out.push(':');
    out.push_str(&edge.label);
    if !edge.props.is_empty() {
        out.push(' ');
        out.push_str(&print_props(&edge.props));
    }
    out.push_str("]-");
    if edge.direction == Direction::Out {
        out.push('>');
    }
    if let Some(quantifier) = &edge.quantifier {
        out.push_str(&print_quantifier(quantifier));
    }
    out
}

fn print_labels(labels: &LabelSpec) -> String {
    match labels {
        LabelSpec::All(labels) if labels.is_empty() => String::new(),
        LabelSpec::All(labels) => format!(":{}", labels.join(":")),
        LabelSpec::Any(labels) if labels.is_empty() => String::new(),
        LabelSpec::Any(labels) => format!(":{}", labels.join("|")),
    }
}

fn print_props(props: &[(String, Expr)]) -> String {
    let props = props
        .iter()
        .map(|(key, value)| format!("{key}: {}", print_expr(value)))
        .collect::<Vec<_>>()
        .join(", ");
    format!("{{{props}}}")
}

fn print_quantifier(quantifier: &Quantifier) -> String {
    match (quantifier.min, quantifier.max) {
        (0, None) => "*".to_string(),
        (min, Some(max)) if min == max => format!("{{{min}}}"),
        (min, Some(max)) => format!("{{{min},{max}}}"),
        (min, None) => format!("{{{min},}}"),
    }
}

fn push_temporal_clauses(parts: &mut Vec<String>, temporal: &TemporalClauses) {
    if let Some(valid) = &temporal.valid {
        parts.push(format!(
            "FOR VALID_TIME {}",
            print_temporal_dimension(valid)
        ));
    }
    if let Some(system) = &temporal.system {
        parts.push(format!(
            "FOR SYSTEM_TIME {}",
            print_temporal_dimension(system)
        ));
    }
}

fn print_temporal_dimension(dim: &TemporalDimension) -> String {
    if dim.lower == Instant::MIN && dim.upper == Instant::END_OF_TIME {
        "ALL".to_string()
    } else if dim
        .lower
        .as_micros()
        .checked_add(1)
        .is_some_and(|upper| upper == dim.upper.as_micros())
    {
        format!("AS OF {}", print_instant(&dim.lower))
    } else {
        format!(
            "FROM {} TO {}",
            print_instant(&dim.lower),
            print_instant(&dim.upper)
        )
    }
}

fn print_instant(instant: &Instant) -> String {
    format!("TIMESTAMP '{}'", instant)
}

fn print_expr(expr: &Expr) -> String {
    print_expr_with_parent(expr, None, ChildSide::Left)
}

#[derive(Clone, Copy)]
enum ChildSide {
    Left,
    Right,
}

fn print_expr_with_parent(expr: &Expr, parent: Option<u8>, side: ChildSide) -> String {
    let printed = match expr {
        Expr::Literal(literal) => print_literal(literal),
        Expr::Param(name) => format!("${name}"),
        Expr::Prop { var, prop } => format!("{var}.{prop}"),
        Expr::Var(var) => var.clone(),
        Expr::Star => "*".to_string(),
        Expr::List(items) => {
            let items = items.iter().map(print_expr).collect::<Vec<_>>().join(", ");
            format!("[{items}]")
        }
        Expr::Unary { op, expr } => print_unary(op, expr),
        Expr::Binary { op, lhs, rhs } => {
            let prec = expr_prec(expr);
            format!(
                "{} {} {}",
                print_expr_with_parent(lhs, Some(prec), ChildSide::Left),
                print_binary_op(op),
                print_expr_with_parent(rhs, Some(prec), ChildSide::Right)
            )
        }
        Expr::Case {
            operand,
            whens,
            otherwise,
        } => print_case(operand.as_deref(), whens, otherwise.as_deref()),
        Expr::FnCall {
            name,
            args,
            distinct,
        } => print_fn_call(name, args, *distinct),
        Expr::Cast { expr, ty } => format!("CAST({} AS {})", print_expr(expr), print_cast_type(ty)),
        Expr::Exists {
            paths,
            where_clause,
        } => {
            let mut out = format!("EXISTS {{ {}", join_paths(paths));
            if let Some(expr) = where_clause {
                out.push_str(" WHERE ");
                out.push_str(&print_expr(expr));
            }
            out.push_str(" }");
            out
        }
    };

    if needs_parentheses(expr, parent, side) {
        format!("({printed})")
    } else {
        printed
    }
}

fn print_unary(op: &UnaryOp, expr: &Expr) -> String {
    match op {
        UnaryOp::Not => format!(
            "NOT {}",
            print_expr_with_parent(
                expr,
                Some(expr_prec(&Expr::Unary {
                    op: UnaryOp::Not,
                    expr: Box::new(expr.clone()),
                })),
                ChildSide::Right
            )
        ),
        UnaryOp::Neg => format!(
            "-{}",
            print_expr_with_parent(
                expr,
                Some(expr_prec(&Expr::Unary {
                    op: UnaryOp::Neg,
                    expr: Box::new(expr.clone()),
                })),
                ChildSide::Right
            )
        ),
        UnaryOp::IsNull => format!("{} IS NULL", print_postfix_operand(expr)),
        UnaryOp::IsNotNull => format!("{} IS NOT NULL", print_postfix_operand(expr)),
    }
}

fn print_postfix_operand(expr: &Expr) -> String {
    match expr {
        Expr::Literal(_)
        | Expr::Param(_)
        | Expr::Prop { .. }
        | Expr::Var(_)
        | Expr::Star
        | Expr::List(_)
        | Expr::FnCall { .. }
        | Expr::Cast { .. }
        | Expr::Case { .. }
        | Expr::Exists { .. } => print_expr(expr),
        Expr::Unary { .. } | Expr::Binary { .. } => format!("({})", print_expr(expr)),
    }
}

fn print_case(operand: Option<&Expr>, whens: &[(Expr, Expr)], otherwise: Option<&Expr>) -> String {
    let mut out = String::from("CASE");
    if let Some(operand) = operand {
        out.push(' ');
        out.push_str(&print_expr(operand));
    }
    for (when_expr, then_expr) in whens {
        out.push_str(" WHEN ");
        out.push_str(&print_expr(when_expr));
        out.push_str(" THEN ");
        out.push_str(&print_expr(then_expr));
    }
    if let Some(otherwise) = otherwise {
        out.push_str(" ELSE ");
        out.push_str(&print_expr(otherwise));
    }
    out.push_str(" END");
    out
}

fn print_fn_call(name: &str, args: &[Expr], distinct: bool) -> String {
    let args = args.iter().map(print_expr).collect::<Vec<_>>().join(", ");
    if distinct && args.is_empty() {
        format!("{name}(DISTINCT)")
    } else if distinct {
        format!("{name}(DISTINCT {args})")
    } else {
        format!("{name}({args})")
    }
}

fn needs_parentheses(expr: &Expr, parent: Option<u8>, side: ChildSide) -> bool {
    let Some(parent_prec) = parent else {
        return false;
    };
    let prec = expr_prec(expr);
    prec < parent_prec || (matches!(side, ChildSide::Right) && prec == parent_prec)
}

fn expr_prec(expr: &Expr) -> u8 {
    match expr {
        Expr::Binary { op, .. } => binary_prec(op),
        Expr::Unary {
            op: UnaryOp::IsNull | UnaryOp::IsNotNull,
            ..
        } => 9,
        Expr::Unary {
            op: UnaryOp::Neg, ..
        } => 8,
        Expr::Unary {
            op: UnaryOp::Not, ..
        } => 4,
        Expr::Literal(_)
        | Expr::Param(_)
        | Expr::Prop { .. }
        | Expr::Var(_)
        | Expr::Star
        | Expr::List(_)
        | Expr::Case { .. }
        | Expr::FnCall { .. }
        | Expr::Cast { .. }
        | Expr::Exists { .. } => 10,
    }
}

fn binary_prec(op: &BinaryOp) -> u8 {
    match op {
        BinaryOp::Or => 1,
        BinaryOp::Xor => 2,
        BinaryOp::And => 3,
        BinaryOp::Eq
        | BinaryOp::Neq
        | BinaryOp::Lt
        | BinaryOp::Lte
        | BinaryOp::Gt
        | BinaryOp::Gte
        | BinaryOp::In
        | BinaryOp::StartsWith
        | BinaryOp::EndsWith
        | BinaryOp::Contains => 5,
        BinaryOp::Add | BinaryOp::Sub => 6,
        BinaryOp::Mul | BinaryOp::Div | BinaryOp::Mod => 7,
    }
}

fn print_literal(literal: &Literal) -> String {
    match literal {
        Literal::Int(value) => value.to_string(),
        Literal::Float(value) => print_float(*value),
        Literal::Str(value) => format!("'{}'", value.replace('\'', "''")),
        Literal::Bool(value) => value.to_string(),
        Literal::Null => "NULL".to_string(),
    }
}

fn print_float(value: f64) -> String {
    let printed = value.to_string();
    if value.is_finite()
        && !printed.contains('.')
        && !printed.contains('e')
        && !printed.contains('E')
    {
        format!("{printed}.0")
    } else {
        printed
    }
}

fn print_binary_op(op: &BinaryOp) -> &'static str {
    match op {
        BinaryOp::Add => "+",
        BinaryOp::Sub => "-",
        BinaryOp::Mul => "*",
        BinaryOp::Div => "/",
        BinaryOp::Mod => "%",
        BinaryOp::Eq => "=",
        BinaryOp::Neq => "<>",
        BinaryOp::Lt => "<",
        BinaryOp::Lte => "<=",
        BinaryOp::Gt => ">",
        BinaryOp::Gte => ">=",
        BinaryOp::And => "AND",
        BinaryOp::Or => "OR",
        BinaryOp::Xor => "XOR",
        BinaryOp::In => "IN",
        BinaryOp::StartsWith => "STARTS WITH",
        BinaryOp::EndsWith => "ENDS WITH",
        BinaryOp::Contains => "CONTAINS",
    }
}

fn print_cast_type(ty: &CastType) -> &'static str {
    match ty {
        CastType::Int => "INT",
        CastType::Float => "FLOAT",
        CastType::Str => "STRING",
        CastType::Bool => "BOOL",
    }
}
