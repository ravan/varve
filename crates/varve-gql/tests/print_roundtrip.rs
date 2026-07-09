use proptest::prelude::*;
use varve_gql::ast::*;
use varve_gql::{parse, parse_program, to_gql, to_gql_program};

fn assert_statement_roundtrip(src: &str) {
    let stmt = match parse(src) {
        Ok(stmt) => stmt,
        Err(err) => panic!("failed to parse source GQL {src:?}: {err}"),
    };
    let printed = to_gql(&stmt);
    let reparsed = match parse(&printed) {
        Ok(stmt) => stmt,
        Err(err) => panic!("failed to parse printed GQL {printed:?}: {err}"),
    };
    assert_eq!(reparsed, stmt, "printed GQL: {printed}");
}

fn assert_program_roundtrip(src: &str) {
    let program = match parse_program(src) {
        Ok(program) => program,
        Err(err) => panic!("failed to parse source GQL program {src:?}: {err}"),
    };
    let printed = to_gql_program(&program);
    let reparsed = match parse_program(&printed) {
        Ok(program) => program,
        Err(err) => panic!("failed to parse printed GQL program {printed:?}: {err}"),
    };
    assert_eq!(reparsed, program, "printed GQL program: {printed}");
}

#[test]
fn prints_reparses_query_pipeline() {
    assert_statement_roundtrip(
        "MATCH (a:Person) OPTIONAL MATCH (a)-[:KNOWS]->(b:Person) \
         FILTER b.age > 18 LET name = b.name FOR friend IN [b] \
         RETURN name AS n, friend ORDER BY n ASC SKIP 1 LIMIT 2",
    );
}

#[test]
fn prints_reparses_mutations_and_programs() {
    assert_statement_roundtrip("MATCH (n:Person) WHERE n._id = 1 SET n.name = 'Ada', n:Employee");
    assert_statement_roundtrip("MATCH (n:Person) WHERE n._id = 1 REMOVE n.name, n:Employee");
    assert_statement_roundtrip("MATCH (n:Person) WHERE n._id = 1 DETACH ERASE n");
    assert_statement_roundtrip(
        "MATCH (a:Person {name: 'Ada'}) INSERT (a)-[:KNOWS]->(:Person {_id: 2}) \
         VALID FROM DATE '2020-01-01' TO DATE '2021-01-01'",
    );
    assert_program_roundtrip(
        "CREATE GRAPH people; USE people; MATCH (n:Person) RETURN n; DROP GRAPH people;",
    );
}

#[test]
fn prints_reparses_temporal_clauses_and_extensions() {
    assert_statement_roundtrip(
        "FOR VALID_TIME BETWEEN DATE '2020-01-01' AND DATE '2021-01-01' \
         FOR SYSTEM_TIME AS OF TIMESTAMP '2024-06-01T12:34:56.789012Z' \
         MATCH (p:Person) FOR VALID_TIME ALL WHERE EXISTS { (p)-[:KNOWS]->(q:Person) \
         WHERE q.name STARTS WITH 'A' } RETURN DISTINCT p.name AS name \
         UNION ALL MATCH (x:X|Y) RETURN x.name AS name",
    );
}

#[test]
fn prints_reparses_negative_year_temporal_date_program() {
    assert_program_roundtrip(
        "FOR VALID_TIME BETWEEN DATE '-020-01-01' AND DATE '2020-01-01' FOR SYSTEM_TIME AS OF TIMESTAMP '2024-06-01T12:34:56.789012Z' MATCH (p:Person) FOR VALID_TIME ALL RETURN p.name",
    );
}

#[test]
fn prints_reparses_utf8_string_literals() {
    assert_statement_roundtrip("MATCH (n:Person) WHERE n._id = 1 SET n.name = 'Åsa''s'");
}

#[test]
fn printer_parenthesizes_precedence_correctly() {
    assert_statement_roundtrip("MATCH (n:Number) RETURN (1 + 2) * 3 AS value");
}

fn arb_ident() -> impl Strategy<Value = String> {
    "[a-z][a-z0-9]{0,7}".prop_filter("identifiers must not contain reserved __", |s| {
        !s.contains("__") && !is_keyword(s)
    })
}

fn is_keyword(s: &str) -> bool {
    matches!(
        s,
        "insert"
            | "match"
            | "where"
            | "return"
            | "as"
            | "true"
            | "false"
            | "null"
            | "for"
            | "valid_time"
            | "system_time"
            | "of"
            | "all"
            | "from"
            | "to"
            | "between"
            | "and"
            | "valid"
            | "delete"
            | "timestamp"
            | "date"
            | "detach"
            | "not"
            | "or"
            | "xor"
            | "is"
            | "case"
            | "when"
            | "then"
            | "else"
            | "end"
            | "exists"
            | "cast"
            | "in"
            | "starts"
            | "ends"
            | "with"
            | "contains"
            | "optional"
            | "filter"
            | "let"
            | "set"
            | "remove"
            | "erase"
            | "union"
            | "distinct"
            | "order"
            | "by"
            | "asc"
            | "ascending"
            | "desc"
            | "descending"
            | "skip"
            | "limit"
            | "offset"
            | "create"
            | "drop"
            | "graph"
            | "use"
    )
}

fn arb_label() -> impl Strategy<Value = String> {
    "[A-Z][A-Za-z0-9]{0,7}".prop_filter("labels must not be keywords", |s| {
        !is_keyword(&s.to_ascii_lowercase())
    })
}

fn arb_literal() -> impl Strategy<Value = Literal> {
    prop_oneof![
        (0_i64..1_000).prop_map(Literal::Int),
        prop_oneof![Just(0.5), Just(1.0), Just(42.25)].prop_map(Literal::Float),
        "[a-z']{0,8}".prop_map(Literal::Str),
        any::<bool>().prop_map(Literal::Bool),
        Just(Literal::Null),
    ]
}

fn arb_scalar_expr() -> BoxedStrategy<Expr> {
    let leaf = prop_oneof![
        arb_literal().prop_map(Expr::Literal),
        arb_ident().prop_map(Expr::Var),
        (arb_ident(), arb_ident()).prop_map(|(var, prop)| Expr::Prop { var, prop }),
        arb_ident().prop_map(Expr::Param),
        Just(Expr::List(Vec::new())),
    ];

    leaf.prop_recursive(3, 32, 3, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..4).prop_map(Expr::List),
            (Just(UnaryOp::Neg), inner.clone()).prop_map(|(op, expr)| Expr::Unary {
                op,
                expr: Box::new(expr),
            }),
            (
                prop_oneof![
                    Just(BinaryOp::Add),
                    Just(BinaryOp::Sub),
                    Just(BinaryOp::Mul),
                    Just(BinaryOp::Div),
                    Just(BinaryOp::Mod),
                ],
                inner.clone(),
                inner.clone(),
            )
                .prop_map(|(op, lhs, rhs)| Expr::Binary {
                    op,
                    lhs: Box::new(lhs),
                    rhs: Box::new(rhs),
                }),
            (
                arb_ident(),
                prop::collection::vec(inner.clone(), 0..3),
                any::<bool>(),
            )
                .prop_map(|(name, args, distinct)| Expr::FnCall {
                    name,
                    args,
                    distinct,
                }),
            (
                inner.clone(),
                prop_oneof![
                    Just(CastType::Int),
                    Just(CastType::Float),
                    Just(CastType::Str),
                    Just(CastType::Bool)
                ]
            )
                .prop_map(|(expr, ty)| Expr::Cast {
                    expr: Box::new(expr),
                    ty,
                }),
        ]
    })
    .boxed()
}

fn arb_comparison_expr() -> BoxedStrategy<Expr> {
    (
        prop_oneof![
            Just(BinaryOp::Eq),
            Just(BinaryOp::Neq),
            Just(BinaryOp::Lt),
            Just(BinaryOp::Lte),
            Just(BinaryOp::Gt),
            Just(BinaryOp::Gte),
            Just(BinaryOp::In),
            Just(BinaryOp::StartsWith),
            Just(BinaryOp::EndsWith),
            Just(BinaryOp::Contains),
        ],
        arb_scalar_expr(),
        arb_scalar_expr(),
    )
        .prop_map(|(op, lhs, rhs)| Expr::Binary {
            op,
            lhs: Box::new(lhs),
            rhs: Box::new(rhs),
        })
        .boxed()
}

fn arb_expr() -> BoxedStrategy<Expr> {
    prop_oneof![arb_scalar_expr(), arb_comparison_expr()]
        .prop_recursive(2, 24, 3, |inner| {
            prop_oneof![
                (Just(UnaryOp::Not), inner.clone()).prop_map(|(op, expr)| Expr::Unary {
                    op,
                    expr: Box::new(expr),
                }),
                (
                    prop_oneof![Just(BinaryOp::And), Just(BinaryOp::Or), Just(BinaryOp::Xor),],
                    inner.clone(),
                    inner.clone(),
                )
                    .prop_map(|(op, lhs, rhs)| Expr::Binary {
                        op,
                        lhs: Box::new(lhs),
                        rhs: Box::new(rhs),
                    }),
                arb_comparison_expr(),
            ]
        })
        .boxed()
}

fn arb_node_pattern() -> impl Strategy<Value = NodePattern> {
    (
        prop::option::of(arb_ident()),
        prop::collection::vec(arb_label(), 0..3),
        prop::collection::vec((arb_ident(), arb_expr()), 0..3),
    )
        .prop_map(|(var, labels, props)| NodePattern {
            var,
            labels: LabelSpec::All(labels),
            props,
        })
}

fn arb_path_pattern() -> impl Strategy<Value = PathPattern> {
    (
        arb_node_pattern(),
        prop::collection::vec((arb_label(), arb_node_pattern()), 0..3),
    )
        .prop_map(|(start, hops)| PathPattern {
            var: None,
            start,
            hops: hops
                .into_iter()
                .map(|(label, node)| {
                    (
                        EdgePattern {
                            var: None,
                            label,
                            props: Vec::new(),
                            direction: Direction::Out,
                            quantifier: None,
                        },
                        node,
                    )
                })
                .collect(),
        })
}

fn arb_statement() -> impl Strategy<Value = Statement> {
    (
        prop::collection::vec(arb_path_pattern(), 1..3),
        prop::collection::vec((arb_expr(), prop::option::of(arb_ident())), 1..4),
    )
        .prop_map(|(paths, items)| {
            Statement::Query(Box::new(QueryStmt {
                first: QueryBody {
                    temporal: TemporalClauses::default(),
                    clauses: vec![Clause::Match {
                        optional: false,
                        paths,
                        temporal: TemporalClauses::default(),
                        where_clause: None,
                    }],
                    ret: ReturnClause {
                        distinct: false,
                        items,
                        order_by: Vec::new(),
                        skip: None,
                        limit: None,
                    },
                },
                unions: Vec::new(),
            }))
        })
}

proptest! {
    #[test]
    fn parse_print_reparse_statement_property(stmt in arb_statement()) {
        let printed = to_gql(&stmt);
        let reparsed = match parse(&printed) {
            Ok(stmt) => stmt,
            Err(err) => panic!("failed to parse printed GQL {printed:?}: {err}"),
        };
        prop_assert_eq!(reparsed, stmt, "printed GQL: {}", printed);
    }
}
