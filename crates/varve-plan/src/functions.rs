use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use datafusion::execution::SessionStateBuilder;
use datafusion::functions_aggregate::expr_fn as aggregate_expr_fn;
use datafusion::logical_expr::{Expr, ScalarUDF};
use datafusion::prelude::SessionContext;

use crate::expand::VarveQueryPlanner;
use crate::PlanError;

pub enum ScalarFn {
    Udf(Arc<ScalarUDF>),
    Builder(fn(Vec<Expr>) -> Result<Expr, PlanError>),
}

pub struct FunctionRegistry {
    scalars: BTreeMap<String, ScalarFn>,
    aggregates: BTreeSet<&'static str>,
}

impl FunctionRegistry {
    pub fn with_builtins() -> FunctionRegistry {
        let mut functions = FunctionRegistry {
            scalars: BTreeMap::new(),
            aggregates: BTreeSet::from(["avg", "collect", "count", "max", "min", "sum"]),
        };

        for (name, builder) in [
            ("upper", upper as fn(Vec<Expr>) -> Result<Expr, PlanError>),
            ("lower", lower),
            ("trim", trim),
            ("ltrim", ltrim),
            ("rtrim", rtrim),
            ("replace", replace),
            ("char_length", character_length),
            ("character_length", character_length),
            ("substring", substring),
            ("left", left),
            ("right", right),
            ("reverse", reverse),
            ("contains", contains),
            ("starts_with", starts_with),
            ("ends_with", ends_with),
            ("abs", abs),
            ("ceil", ceil),
            ("floor", floor),
            ("round", round),
            ("sqrt", sqrt),
            ("power", power),
            ("sign", sign),
            ("trunc", trunc),
            ("size", size),
            ("head", head),
            ("last", last),
            ("valid_from", temporal_column_builder),
            ("valid_to", temporal_column_builder),
            ("system_from", temporal_column_builder),
        ] {
            functions.register_scalar(name, ScalarFn::Builder(builder));
        }

        functions
    }

    pub fn register_scalar(&mut self, name: &str, f: ScalarFn) {
        self.scalars.insert(normalize_name(name), f);
    }

    pub fn scalar(&self, name: &str) -> Option<&ScalarFn> {
        self.scalars.get(&normalize_name(name))
    }

    pub fn is_aggregate(&self, name: &str) -> bool {
        self.aggregates.contains(normalize_name(name).as_str())
    }
}

pub fn session_context(functions: &FunctionRegistry) -> SessionContext {
    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_query_planner(Arc::new(VarveQueryPlanner))
        .build();
    let ctx = SessionContext::new_with_state(state);

    for scalar in functions.scalars.values() {
        if let ScalarFn::Udf(udf) = scalar {
            ctx.register_udf(udf.as_ref().clone());
        }
    }

    ctx
}

pub(crate) fn temporal_column(name: &str) -> Option<(&'static str, &'static str)> {
    match normalize_name(name).as_str() {
        "valid_from" => Some(("_valid_from", "valid_from")),
        "valid_to" => Some(("_valid_to", "valid_to")),
        "system_from" => Some(("_system_from", "system_from")),
        _ => None,
    }
}

fn normalize_name(name: &str) -> String {
    name.to_ascii_lowercase()
}

pub fn lower_aggregate(name: &str, args: Vec<Expr>, distinct: bool) -> Result<Expr, PlanError> {
    let normalized = normalize_name(name);
    match (normalized.as_str(), distinct) {
        ("count", true) => one_aggregate_arg(name, args, aggregate_expr_fn::count_distinct),
        (_, true) => Err(PlanError::Unsupported(format!(
            "DISTINCT inside aggregate {name} is not supported"
        ))),
        ("count", false) => one_aggregate_arg(name, args, aggregate_expr_fn::count),
        ("collect", false) => one_aggregate_arg(name, args, aggregate_expr_fn::array_agg),
        ("avg", false) => one_aggregate_arg(name, args, aggregate_expr_fn::avg),
        ("max", false) => one_aggregate_arg(name, args, aggregate_expr_fn::max),
        ("min", false) => one_aggregate_arg(name, args, aggregate_expr_fn::min),
        ("sum", false) => one_aggregate_arg(name, args, aggregate_expr_fn::sum),
        _ => Err(PlanError::Unsupported(format!(
            "aggregate function {name} is not registered"
        ))),
    }
}

fn one_aggregate_arg(
    name: &str,
    mut args: Vec<Expr>,
    f: fn(Expr) -> Expr,
) -> Result<Expr, PlanError> {
    if args.len() != 1 {
        return Err(unsupported_arity(name, "1", args.len()));
    }
    Ok(f(args.remove(0)))
}

fn unsupported_arity(name: &str, expected: &str, actual: usize) -> PlanError {
    PlanError::Unsupported(format!(
        "{name} requires {expected} argument(s), got {actual}"
    ))
}

fn unary(name: &str, mut args: Vec<Expr>, f: fn(Expr) -> Expr) -> Result<Expr, PlanError> {
    if args.len() != 1 {
        return Err(unsupported_arity(name, "1", args.len()));
    }
    Ok(f(args.remove(0)))
}

fn binary(name: &str, mut args: Vec<Expr>, f: fn(Expr, Expr) -> Expr) -> Result<Expr, PlanError> {
    if args.len() != 2 {
        return Err(unsupported_arity(name, "2", args.len()));
    }
    let rhs = args.remove(1);
    let lhs = args.remove(0);
    Ok(f(lhs, rhs))
}

fn ternary(
    name: &str,
    mut args: Vec<Expr>,
    f: fn(Expr, Expr, Expr) -> Expr,
) -> Result<Expr, PlanError> {
    if args.len() != 3 {
        return Err(unsupported_arity(name, "3", args.len()));
    }
    let third = args.remove(2);
    let second = args.remove(1);
    let first = args.remove(0);
    Ok(f(first, second, third))
}

fn varargs(
    name: &str,
    args: Vec<Expr>,
    min: usize,
    max: usize,
    f: fn(Vec<Expr>) -> Expr,
) -> Result<Expr, PlanError> {
    if args.len() < min || args.len() > max {
        return Err(unsupported_arity(
            name,
            &format!("{min}..={max}"),
            args.len(),
        ));
    }
    Ok(f(args))
}

fn upper(args: Vec<Expr>) -> Result<Expr, PlanError> {
    unary("upper", args, datafusion::functions::expr_fn::upper)
}

fn lower(args: Vec<Expr>) -> Result<Expr, PlanError> {
    unary("lower", args, datafusion::functions::expr_fn::lower)
}

fn trim(args: Vec<Expr>) -> Result<Expr, PlanError> {
    varargs("trim", args, 1, 2, datafusion::functions::expr_fn::btrim)
}

fn ltrim(args: Vec<Expr>) -> Result<Expr, PlanError> {
    varargs("ltrim", args, 1, 2, datafusion::functions::expr_fn::ltrim)
}

fn rtrim(args: Vec<Expr>) -> Result<Expr, PlanError> {
    varargs("rtrim", args, 1, 2, datafusion::functions::expr_fn::rtrim)
}

fn replace(args: Vec<Expr>) -> Result<Expr, PlanError> {
    ternary("replace", args, datafusion::functions::expr_fn::replace)
}

fn character_length(args: Vec<Expr>) -> Result<Expr, PlanError> {
    unary(
        "character_length",
        args,
        datafusion::functions::expr_fn::character_length,
    )
}

fn substring(args: Vec<Expr>) -> Result<Expr, PlanError> {
    match args.len() {
        2 => binary("substring", args, datafusion::functions::expr_fn::substr),
        3 => ternary("substring", args, datafusion::functions::expr_fn::substring),
        actual => Err(unsupported_arity("substring", "2..=3", actual)),
    }
}

fn left(args: Vec<Expr>) -> Result<Expr, PlanError> {
    binary("left", args, datafusion::functions::expr_fn::left)
}

fn right(args: Vec<Expr>) -> Result<Expr, PlanError> {
    binary("right", args, datafusion::functions::expr_fn::right)
}

fn reverse(args: Vec<Expr>) -> Result<Expr, PlanError> {
    unary("reverse", args, datafusion::functions::expr_fn::reverse)
}

fn contains(args: Vec<Expr>) -> Result<Expr, PlanError> {
    binary("contains", args, datafusion::functions::expr_fn::contains)
}

fn starts_with(args: Vec<Expr>) -> Result<Expr, PlanError> {
    binary(
        "starts_with",
        args,
        datafusion::functions::expr_fn::starts_with,
    )
}

fn ends_with(args: Vec<Expr>) -> Result<Expr, PlanError> {
    binary("ends_with", args, datafusion::functions::expr_fn::ends_with)
}

fn abs(args: Vec<Expr>) -> Result<Expr, PlanError> {
    unary("abs", args, datafusion::functions::expr_fn::abs)
}

fn ceil(args: Vec<Expr>) -> Result<Expr, PlanError> {
    unary("ceil", args, datafusion::functions::expr_fn::ceil)
}

fn floor(args: Vec<Expr>) -> Result<Expr, PlanError> {
    unary("floor", args, datafusion::functions::expr_fn::floor)
}

fn round(args: Vec<Expr>) -> Result<Expr, PlanError> {
    varargs("round", args, 1, 2, datafusion::functions::expr_fn::round)
}

fn sqrt(args: Vec<Expr>) -> Result<Expr, PlanError> {
    unary("sqrt", args, datafusion::functions::expr_fn::sqrt)
}

fn power(args: Vec<Expr>) -> Result<Expr, PlanError> {
    binary("power", args, datafusion::functions::expr_fn::power)
}

fn sign(args: Vec<Expr>) -> Result<Expr, PlanError> {
    unary("sign", args, datafusion::functions::expr_fn::signum)
}

fn trunc(args: Vec<Expr>) -> Result<Expr, PlanError> {
    varargs("trunc", args, 1, 2, datafusion::functions::expr_fn::trunc)
}

fn size(args: Vec<Expr>) -> Result<Expr, PlanError> {
    unary(
        "size",
        args,
        datafusion::functions_nested::expr_fn::cardinality,
    )
}

fn head(args: Vec<Expr>) -> Result<Expr, PlanError> {
    unary("head", args, |expr| {
        datafusion::functions_nested::expr_fn::array_element(expr, datafusion::logical_expr::lit(1))
    })
}

fn last(args: Vec<Expr>) -> Result<Expr, PlanError> {
    unary("last", args, |expr| {
        let len = datafusion::functions_nested::expr_fn::cardinality(expr.clone());
        datafusion::functions_nested::expr_fn::array_element(expr, len)
    })
}

fn temporal_column_builder(args: Vec<Expr>) -> Result<Expr, PlanError> {
    unary("temporal", args, |expr| expr)
}

#[cfg(test)]
mod tests {
    use datafusion::logical_expr::{lit, Expr};

    use super::{FunctionRegistry, ScalarFn};

    #[test]
    fn builtins_resolve_by_name() {
        let functions = FunctionRegistry::with_builtins();

        assert!(functions.scalar("upper").is_some());
        assert!(functions.scalar("trim").is_some());
        assert!(functions.scalar("size").is_some());
        assert!(functions.scalar("valid_from").is_some());
    }

    #[test]
    fn unknown_function_is_absent() {
        let functions = FunctionRegistry::with_builtins();

        assert!(functions.scalar("definitely_missing").is_none());
    }

    #[test]
    fn is_aggregate_set() {
        let functions = FunctionRegistry::with_builtins();

        for name in ["count", "sum", "avg", "min", "max", "collect"] {
            assert!(
                functions.is_aggregate(name),
                "{name} should be an aggregate"
            );
        }
        assert!(!functions.is_aggregate("upper"));
        assert!(!functions.is_aggregate("definitely_missing"));
    }

    #[test]
    fn register_scalar_normalizes_names() {
        fn identity(args: Vec<Expr>) -> Result<Expr, crate::PlanError> {
            args.into_iter().next().ok_or_else(|| {
                crate::PlanError::Unsupported("identity requires one argument".into())
            })
        }

        let mut functions = FunctionRegistry::with_builtins();
        functions.register_scalar("MiXeD", ScalarFn::Builder(identity));

        assert!(functions.scalar("mixed").is_some());
        assert!(functions.scalar("MIXED").is_some());
        assert!(matches!(
            functions.scalar("mixed"),
            Some(ScalarFn::Builder(_))
        ));
        assert!(identity(vec![lit(1)]).is_ok());
    }
}
