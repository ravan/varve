use crate::db::EngineError;
use std::collections::BTreeMap;
use varve_gql::ast::{Expr, Literal};
use varve_plan::PlanError;
use varve_types::Value;

pub(crate) fn const_value(
    expr: &Expr,
    params: &BTreeMap<String, Value>,
) -> Result<Value, EngineError> {
    match expr {
        Expr::Literal(l) => Ok(literal_to_value(l)),
        Expr::Param(name) => params
            .get(name)
            .cloned()
            .ok_or_else(|| EngineError::Plan(PlanError::MissingParam(name.clone()))),
        Expr::Unary {
            op: varve_gql::ast::UnaryOp::Neg,
            expr,
        } => match expr.as_ref() {
            Expr::Literal(Literal::Int(i)) => Ok(Value::Int(-i)),
            Expr::Literal(Literal::Float(f)) => Ok(Value::Float(-f)),
            _ => Err(EngineError::Unsupported(
                "unary negative requires numeric constant expression".into(),
            )),
        },
        _ => Err(EngineError::Unsupported(
            "constant expressions support only literal, parameter, or unary negative numeric literal"
                .into(),
        )),
    }
}

fn literal_to_value(l: &Literal) -> Value {
    match l {
        Literal::Int(i) => Value::Int(*i),
        Literal::Float(f) => Value::Float(*f),
        Literal::Str(s) => Value::Str(s.clone()),
        Literal::Bool(b) => Value::Bool(*b),
        Literal::Null => Value::Null,
    }
}
