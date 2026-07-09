use std::collections::{BTreeMap, BTreeSet};
use std::marker::PhantomData;

use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{binary_expr, cast, col, lit, when, Expr as DfExpr, Operator};
use datafusion::scalar::ScalarValue;
use varve_gql::ast::{BinaryOp, CastType, Expr, Literal, PathPattern, UnaryOp};
use varve_types::{Iid, Value};

use crate::exec::to_df_literal;
use crate::functions::{temporal_column, FunctionRegistry, ScalarFn};
use crate::pattern::mangled;
use crate::PlanError;

pub struct Scope<'a> {
    pub elements: BTreeMap<String, ElementCols>,
    pub value_vars: BTreeSet<String>,
    pub path_vars: BTreeSet<String>,
    marker: PhantomData<&'a ()>,
}

pub struct ElementCols {
    pub available: BTreeSet<String>,
}

impl<'a> Scope<'a> {
    pub fn new(
        elements: BTreeMap<String, ElementCols>,
        value_vars: BTreeSet<String>,
        path_vars: BTreeSet<String>,
    ) -> Self {
        Self {
            elements,
            value_vars,
            path_vars,
            marker: PhantomData,
        }
    }

    pub fn element(&self, var: &str) -> Option<&ElementCols> {
        self.elements.get(var)
    }

    pub fn has_value_var(&self, var: &str) -> bool {
        self.value_vars.contains(var)
    }
}

impl ElementCols {
    pub fn has_column(&self, col: &str) -> bool {
        self.available.contains(col)
    }
}

pub fn lower_expr(
    expr: &Expr,
    scope: &Scope<'_>,
    params: &BTreeMap<String, Value>,
    functions: &FunctionRegistry,
) -> Result<DfExpr, PlanError> {
    match expr {
        Expr::Literal(literal) => Ok(to_df_literal(literal)),
        Expr::Param(name) => params
            .get(name)
            .map(value_to_df_literal)
            .ok_or_else(|| PlanError::MissingParam(name.clone())),
        Expr::Prop { var, prop } => {
            let Some(cols) = scope.element(var) else {
                return Err(PlanError::UnknownVariable(var.clone()));
            };
            let col_name = mangled(var, prop);
            if cols.has_column(&col_name) {
                Ok(col(col_name))
            } else {
                Ok(lit(ScalarValue::Null))
            }
        }
        Expr::Var(var) => {
            if scope.has_value_var(var) {
                Ok(col(var))
            } else {
                Err(PlanError::UnknownVariable(var.clone()))
            }
        }
        Expr::Star => Err(PlanError::Unsupported(
            "star expression outside aggregate/function context".into(),
        )),
        Expr::List(items) => {
            let mut lowered = Vec::with_capacity(items.len());
            for item in items {
                lowered.push(lower_expr(item, scope, params, functions)?);
            }
            Ok(datafusion::functions_nested::expr_fn::make_array(lowered))
        }
        Expr::Unary { op, expr } => {
            let inner = lower_expr(expr, scope, params, functions)?;
            match op {
                UnaryOp::Not => Ok(!inner),
                UnaryOp::Neg => Ok(-inner),
                UnaryOp::IsNull => Ok(inner.is_null()),
                UnaryOp::IsNotNull => Ok(inner.is_not_null()),
            }
        }
        Expr::Binary { op, lhs, rhs } => lower_binary(op, lhs, rhs, scope, params, functions),
        Expr::Case {
            operand,
            whens,
            otherwise,
        } => lower_case(
            operand.as_deref(),
            whens,
            otherwise.as_deref(),
            scope,
            params,
            functions,
        ),
        Expr::FnCall {
            name,
            args,
            distinct,
        } => lower_fn_call(name, args, *distinct, scope, params, functions),
        Expr::Cast { expr, ty } => Ok(cast(
            lower_expr(expr, scope, params, functions)?,
            match ty {
                CastType::Int => DataType::Int64,
                CastType::Float => DataType::Float64,
                CastType::Str => DataType::Utf8,
                CastType::Bool => DataType::Boolean,
            },
        )),
        Expr::Exists { .. } => Err(PlanError::Unsupported(
            "EXISTS outside top-level WHERE conjunction".into(),
        )),
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ExistsConjunct<'a> {
    pub negated: bool,
    pub paths: &'a [PathPattern],
    pub where_clause: Option<&'a Expr>,
}

pub fn split_conjuncts(where_clause: Option<&Expr>) -> (Vec<ExistsConjunct<'_>>, Vec<&Expr>) {
    let mut exists = Vec::new();
    let mut rest = Vec::new();
    if let Some(expr) = where_clause {
        split_conjunct(expr, &mut exists, &mut rest);
    }
    (exists, rest)
}

pub fn iid_from_conjuncts(
    conjuncts: &[&Expr],
    var: &str,
    params: &BTreeMap<String, Value>,
    graph: &str,
    table: &str,
) -> Option<Iid> {
    conjuncts
        .iter()
        .find_map(|expr| iid_value_from_equality(expr, var, params))
        .and_then(|value| value.id_bytes().ok())
        .map(|bytes| Iid::derive(graph, table, &bytes))
}

pub fn iid_from_expr(
    expr: &Expr,
    params: &BTreeMap<String, Value>,
    graph: &str,
    table: &str,
) -> Option<Iid> {
    expr_to_iid_value(expr, params)
        .and_then(|value| value.id_bytes().ok())
        .map(|bytes| Iid::derive(graph, table, &bytes))
}

fn lower_fn_call(
    name: &str,
    args: &[Expr],
    distinct: bool,
    scope: &Scope<'_>,
    params: &BTreeMap<String, Value>,
    functions: &FunctionRegistry,
) -> Result<DfExpr, PlanError> {
    if distinct {
        return Err(PlanError::Unsupported(format!(
            "DISTINCT function call {name} lands in aggregate lowering"
        )));
    }
    if functions.is_aggregate(name) {
        return Err(PlanError::Unsupported(format!(
            "aggregate function {name} lands in slice 7 task 8"
        )));
    }
    let Some(function) = functions.scalar(name) else {
        return Err(PlanError::UnknownFunction(name.to_string()));
    };
    if temporal_column(name).is_some() {
        return lower_temporal_fn(name, args, scope, function);
    }
    let mut lowered = Vec::with_capacity(args.len());
    for arg in args {
        lowered.push(lower_expr(arg, scope, params, functions)?);
    }
    match function {
        ScalarFn::Udf(udf) => Ok(udf.call(lowered)),
        ScalarFn::Builder(builder) => builder(lowered),
    }
}

fn lower_temporal_fn(
    name: &str,
    args: &[Expr],
    scope: &Scope<'_>,
    function: &ScalarFn,
) -> Result<DfExpr, PlanError> {
    let Some((hidden, _)) = temporal_column(name) else {
        return Err(PlanError::UnknownFunction(name.to_string()));
    };
    let [Expr::Var(var)] = args else {
        return Err(PlanError::Unsupported(format!(
            "{name} requires a single element variable argument"
        )));
    };
    let Some(cols) = scope.element(var) else {
        return Err(PlanError::UnknownVariable(var.clone()));
    };
    let col_name = mangled(var, hidden);
    if !cols.has_column(&col_name) {
        return Err(PlanError::UnknownColumn(hidden.to_string()));
    }
    let lowered_arg = col(col_name);
    match function {
        ScalarFn::Udf(udf) => Ok(udf.call(vec![lowered_arg])),
        ScalarFn::Builder(builder) => builder(vec![lowered_arg]),
    }
}

fn lower_binary(
    op: &BinaryOp,
    lhs: &Expr,
    rhs: &Expr,
    scope: &Scope<'_>,
    params: &BTreeMap<String, Value>,
    functions: &FunctionRegistry,
) -> Result<DfExpr, PlanError> {
    if matches!(op, BinaryOp::In) {
        let Expr::List(items) = rhs else {
            return Err(PlanError::Unsupported(
                "IN requires a list right-hand side".into(),
            ));
        };
        let lhs = lower_expr(lhs, scope, params, functions)?;
        let mut lowered_items = Vec::with_capacity(items.len());
        for item in items {
            lowered_items.push(lower_expr(item, scope, params, functions)?);
        }
        return Ok(lhs.in_list(lowered_items, false));
    }

    let lhs = lower_expr(lhs, scope, params, functions)?;
    let rhs = lower_expr(rhs, scope, params, functions)?;
    match op {
        BinaryOp::Add => Ok(binary_expr(lhs, Operator::Plus, rhs)),
        BinaryOp::Sub => Ok(binary_expr(lhs, Operator::Minus, rhs)),
        BinaryOp::Mul => Ok(binary_expr(lhs, Operator::Multiply, rhs)),
        BinaryOp::Div => Ok(binary_expr(lhs, Operator::Divide, rhs)),
        BinaryOp::Mod => Ok(binary_expr(lhs, Operator::Modulo, rhs)),
        BinaryOp::Eq => Ok(lhs.eq(rhs)),
        BinaryOp::Neq => Ok(lhs.not_eq(rhs)),
        BinaryOp::Lt => Ok(lhs.lt(rhs)),
        BinaryOp::Lte => Ok(lhs.lt_eq(rhs)),
        BinaryOp::Gt => Ok(lhs.gt(rhs)),
        BinaryOp::Gte => Ok(lhs.gt_eq(rhs)),
        BinaryOp::And => Ok(lhs.and(rhs)),
        BinaryOp::Or => Ok(lhs.or(rhs)),
        BinaryOp::Xor => Ok(lhs.not_eq(rhs)),
        BinaryOp::In => unreachable!("handled before lowering rhs"),
        BinaryOp::StartsWith | BinaryOp::EndsWith | BinaryOp::Contains => Err(
            PlanError::Unsupported(format!("binary string operator {op:?}")),
        ),
    }
}

fn lower_case(
    operand: Option<&Expr>,
    whens: &[(Expr, Expr)],
    otherwise: Option<&Expr>,
    scope: &Scope<'_>,
    params: &BTreeMap<String, Value>,
    functions: &FunctionRegistry,
) -> Result<DfExpr, PlanError> {
    let Some((first_when, first_then)) = whens.first() else {
        return Err(PlanError::Unsupported(
            "CASE requires at least one WHEN".into(),
        ));
    };

    let operand = operand
        .map(|expr| lower_expr(expr, scope, params, functions))
        .transpose()?;
    let condition = lower_case_condition(operand.as_ref(), first_when, scope, params, functions)?;
    let first_then = lower_expr(first_then, scope, params, functions)?;
    let mut builder = when(condition, first_then);

    for (when_expr, then_expr) in whens.iter().skip(1) {
        let condition =
            lower_case_condition(operand.as_ref(), when_expr, scope, params, functions)?;
        builder = builder.when(condition, lower_expr(then_expr, scope, params, functions)?);
    }

    match otherwise {
        Some(expr) => Ok(builder.otherwise(lower_expr(expr, scope, params, functions)?)?),
        None => Ok(builder.end()?),
    }
}

fn lower_case_condition(
    operand: Option<&DfExpr>,
    when_expr: &Expr,
    scope: &Scope<'_>,
    params: &BTreeMap<String, Value>,
    functions: &FunctionRegistry,
) -> Result<DfExpr, PlanError> {
    let when_expr = lower_expr(when_expr, scope, params, functions)?;
    Ok(match operand {
        Some(operand) => operand.clone().eq(when_expr),
        None => when_expr,
    })
}

fn split_conjunct<'a>(
    expr: &'a Expr,
    exists: &mut Vec<ExistsConjunct<'a>>,
    rest: &mut Vec<&'a Expr>,
) {
    match expr {
        Expr::Binary {
            op: BinaryOp::And,
            lhs,
            rhs,
        } => {
            split_conjunct(lhs, exists, rest);
            split_conjunct(rhs, exists, rest);
        }
        Expr::Exists {
            paths,
            where_clause,
        } => exists.push(ExistsConjunct {
            negated: false,
            paths,
            where_clause: where_clause.as_deref(),
        }),
        Expr::Unary {
            op: UnaryOp::Not,
            expr: operand,
        } => match operand.as_ref() {
            Expr::Exists {
                paths,
                where_clause,
            } => exists.push(ExistsConjunct {
                negated: true,
                paths,
                where_clause: where_clause.as_deref(),
            }),
            _ => rest.push(expr),
        },
        other => rest.push(other),
    }
}

fn iid_value_from_equality(
    expr: &Expr,
    var: &str,
    params: &BTreeMap<String, Value>,
) -> Option<Value> {
    let Expr::Binary {
        op: BinaryOp::Eq,
        lhs,
        rhs,
    } = expr
    else {
        return None;
    };

    prop_id_eq(lhs, rhs, var, params).or_else(|| prop_id_eq(rhs, lhs, var, params))
}

fn prop_id_eq(
    prop_expr: &Expr,
    value_expr: &Expr,
    var: &str,
    params: &BTreeMap<String, Value>,
) -> Option<Value> {
    let Expr::Prop {
        var: prop_var,
        prop,
    } = prop_expr
    else {
        return None;
    };
    if prop_var != var || prop != "_id" {
        return None;
    }
    expr_to_iid_value(value_expr, params)
}

fn expr_to_iid_value(expr: &Expr, params: &BTreeMap<String, Value>) -> Option<Value> {
    match expr {
        Expr::Literal(lit) => Some(literal_value(lit)),
        Expr::Param(name) => params.get(name).cloned(),
        _ => None,
    }
}

fn value_to_df_literal(value: &Value) -> DfExpr {
    match value {
        Value::Null => lit(ScalarValue::Null),
        Value::Bool(b) => lit(*b),
        Value::Int(i) => lit(*i),
        Value::Float(f) => lit(*f),
        Value::Str(s) => lit(s.clone()),
        Value::Bytes(bytes) => lit(bytes.clone()),
    }
}

fn literal_value(lit: &Literal) -> Value {
    match lit {
        Literal::Null => Value::Null,
        Literal::Bool(b) => Value::Bool(*b),
        Literal::Int(i) => Value::Int(*i),
        Literal::Float(f) => Value::Float(*f),
        Literal::Str(s) => Value::Str(s.clone()),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use varve_gql::ast::{BinaryOp, CastType, Expr, Literal, UnaryOp};
    use varve_types::{Iid, Value};

    use super::{
        iid_from_conjuncts, lower_expr as lower_expr_impl, split_conjuncts, ElementCols, Scope,
    };
    use crate::functions::{FunctionRegistry, ScalarFn};
    use crate::PlanError;

    fn scope_with_node() -> Scope<'static> {
        let mut available = BTreeSet::new();
        available.insert("n__x".to_string());
        available.insert("n___id".to_string());

        let mut elements = BTreeMap::new();
        elements.insert("n".to_string(), ElementCols { available });

        Scope::new(elements, BTreeSet::new(), BTreeSet::new())
    }

    fn lower_expr(
        expr: &Expr,
        scope: &Scope<'_>,
        params: &BTreeMap<String, Value>,
    ) -> Result<datafusion::logical_expr::Expr, PlanError> {
        lower_expr_impl(expr, scope, params, &FunctionRegistry::with_builtins())
    }

    fn scope_with_temporal_node() -> Scope<'static> {
        let mut available = BTreeSet::new();
        available.insert("p___valid_from".to_string());

        let mut elements = BTreeMap::new();
        elements.insert("p".to_string(), ElementCols { available });

        Scope::new(elements, BTreeSet::new(), BTreeSet::new())
    }

    fn sentinel_builder(
        _args: Vec<datafusion::logical_expr::Expr>,
    ) -> Result<datafusion::logical_expr::Expr, PlanError> {
        Ok(datafusion::logical_expr::lit(777_i64))
    }

    #[test]
    fn temporal_function_uses_registry_entry_before_builtin_substitution() {
        let mut functions = FunctionRegistry::with_builtins();
        functions.register_scalar("valid_from", ScalarFn::Builder(sentinel_builder));
        let expr = Expr::FnCall {
            name: "valid_from".into(),
            args: vec![Expr::Var("p".into())],
            distinct: false,
        };

        let lowered = lower_expr_impl(
            &expr,
            &scope_with_temporal_node(),
            &BTreeMap::new(),
            &functions,
        )
        .unwrap();

        assert_eq!(lowered.to_string(), "Int64(777)");
    }

    #[test]
    fn lower_property_uses_mangled_column_null_for_absent_and_errors_for_unknown_var() {
        let scope = scope_with_node();
        let params = BTreeMap::new();

        let known = lower_expr(
            &Expr::Prop {
                var: "n".into(),
                prop: "x".into(),
            },
            &scope,
            &params,
        )
        .unwrap();
        assert_eq!(known.to_string(), "n__x");

        let absent = lower_expr(
            &Expr::Prop {
                var: "n".into(),
                prop: "ghost".into(),
            },
            &scope,
            &params,
        )
        .unwrap();
        assert_eq!(absent.to_string(), "NULL");

        let err = lower_expr(
            &Expr::Prop {
                var: "missing".into(),
                prop: "x".into(),
            },
            &scope,
            &params,
        )
        .unwrap_err();
        assert!(matches!(err, PlanError::UnknownVariable(var) if var == "missing"));
    }

    #[test]
    fn lower_params_require_presence_and_present_params_become_literals() {
        let scope = scope_with_node();
        let mut params = BTreeMap::new();
        params.insert("wanted".to_string(), Value::Int(42));

        let present = lower_expr(&Expr::Param("wanted".into()), &scope, &params).unwrap();
        assert_eq!(present.to_string(), "Int64(42)");

        let err = lower_expr(&Expr::Param("missing".into()), &scope, &params).unwrap_err();
        assert!(matches!(err, PlanError::MissingParam(name) if name == "missing"));
    }

    #[test]
    fn split_conjuncts_and_iid_extraction_find_id_equality_with_extra_filters() {
        let params = BTreeMap::new();
        let where_clause = Expr::Binary {
            op: BinaryOp::And,
            lhs: Box::new(Expr::Binary {
                op: BinaryOp::Eq,
                lhs: Box::new(Expr::Prop {
                    var: "n".into(),
                    prop: "_id".into(),
                }),
                rhs: Box::new(Expr::Literal(Literal::Str("a".into()))),
            }),
            rhs: Box::new(Expr::Binary {
                op: BinaryOp::Gt,
                lhs: Box::new(Expr::Prop {
                    var: "n".into(),
                    prop: "x".into(),
                }),
                rhs: Box::new(Expr::Literal(Literal::Int(0))),
            }),
        };

        let (exists, rest) = split_conjuncts(Some(&where_clause));
        assert!(exists.is_empty());
        assert_eq!(rest.len(), 2);

        let expected = Iid::derive(
            "default",
            "nodes",
            &Value::Str("a".into()).id_bytes().unwrap(),
        );
        assert_eq!(
            iid_from_conjuncts(&rest, "n", &params, "default", "nodes"),
            Some(expected)
        );
    }

    #[test]
    fn lower_case_cast_and_in_build_datafusion_expressions() {
        let scope = scope_with_node();
        let params = BTreeMap::new();

        let case_expr = Expr::Case {
            operand: None,
            whens: vec![(
                Expr::Binary {
                    op: BinaryOp::Eq,
                    lhs: Box::new(Expr::Prop {
                        var: "n".into(),
                        prop: "x".into(),
                    }),
                    rhs: Box::new(Expr::Literal(Literal::Int(1))),
                },
                Expr::Literal(Literal::Bool(true)),
            )],
            otherwise: Some(Box::new(Expr::Literal(Literal::Bool(false)))),
        };
        lower_expr(&case_expr, &scope, &params).unwrap();

        let cast_expr = Expr::Cast {
            expr: Box::new(Expr::Prop {
                var: "n".into(),
                prop: "x".into(),
            }),
            ty: CastType::Float,
        };
        lower_expr(&cast_expr, &scope, &params).unwrap();

        let in_expr = Expr::Binary {
            op: BinaryOp::In,
            lhs: Box::new(Expr::Prop {
                var: "n".into(),
                prop: "x".into(),
            }),
            rhs: Box::new(Expr::List(vec![
                Expr::Literal(Literal::Int(1)),
                Expr::Literal(Literal::Int(2)),
            ])),
        };
        lower_expr(&in_expr, &scope, &params).unwrap();

        let is_null_expr = Expr::Unary {
            op: UnaryOp::IsNull,
            expr: Box::new(Expr::Prop {
                var: "n".into(),
                prop: "ghost".into(),
            }),
        };
        lower_expr(&is_null_expr, &scope, &params).unwrap();
    }
}
