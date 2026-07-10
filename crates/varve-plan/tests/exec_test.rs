#![allow(clippy::unwrap_used)] // tests may use unwrap; crate-level allow covers helper fns
use arrow::array::{Array, Int64Array, StringArray, TimestampMicrosecondArray};
use varve_gql::ast::{Clause, Expr, Statement};
use varve_index::{Event, LiveTable, Op};
use varve_plan::run_query;
use varve_types::{Doc, Iid, Instant, Value};

const NOW: Instant = Instant::from_micros(100);

fn person(n: u8, sf: i64, name: &str, age: i64) -> Event {
    let mut doc = Doc::new();
    doc.insert("name".into(), Value::Str(name.into()));
    doc.insert("age".into(), Value::Int(age));
    Event {
        iid: Iid::derive("g", "nodes", &[n]),
        system_from: Instant::from_micros(sf),
        valid_from: Instant::from_micros(sf),
        valid_to: Instant::END_OF_TIME,
        src: None,
        dst: None,
        op: Op::Put {
            labels: vec!["Person".into()],
            doc,
        },
    }
}

fn setup() -> LiveTable {
    let mut t = LiveTable::new();
    for (n, sf, name, age) in [(1u8, 1, "Ada", 36i64), (2, 2, "Bob", 41), (3, 3, "Cyd", 36)] {
        t.append(person(n, sf, name, age)).unwrap();
    }
    t
}

fn query_stmt(src: &str) -> varve_gql::ast::QueryStmt {
    match varve_gql::parse(src).unwrap() {
        Statement::Query(q) => *q,
        _ => panic!("not a query"),
    }
}

fn names(batches: &[arrow::record_batch::RecordBatch]) -> Vec<String> {
    let mut out: Vec<String> = batches
        .iter()
        .flat_map(|b| {
            let col: &StringArray = b
                .column_by_name("name")
                .unwrap()
                .as_any()
                .downcast_ref()
                .unwrap();
            (0..col.len())
                .map(|i| col.value(i).to_string())
                .collect::<Vec<_>>()
        })
        .collect();
    out.sort();
    out
}

#[tokio::test]
async fn match_where_return_filters_rows() {
    let live = setup();
    let q = query_stmt("MATCH (p:Person) WHERE p.age = 36 RETURN p.name AS name");
    let batches = run_query(&q, &live, NOW).await.unwrap();
    assert_eq!(names(&batches), vec!["Ada", "Cyd"]);
}

#[tokio::test]
async fn unknown_label_returns_empty() {
    let live = setup();
    let q = query_stmt("MATCH (r:Robot) RETURN r.name");
    assert!(run_query(&q, &live, NOW).await.unwrap().is_empty());
}

#[tokio::test]
async fn current_query_sees_only_the_latest_version() {
    let mut live = setup();
    let mut doc = Doc::new();
    doc.insert("name".into(), Value::Str("Adele".into()));
    doc.insert("age".into(), Value::Int(36));
    live.append(Event {
        iid: Iid::derive("g", "nodes", &[1u8]),
        system_from: Instant::from_micros(10),
        valid_from: Instant::from_micros(10),
        valid_to: Instant::END_OF_TIME,
        src: None,
        dst: None,
        op: Op::Put {
            labels: vec!["Person".into()],
            doc,
        },
    })
    .unwrap();
    let q = query_stmt("MATCH (p:Person) WHERE p.age = 36 RETURN p.name AS name");
    let batches = run_query(&q, &live, NOW).await.unwrap();
    assert_eq!(names(&batches), vec!["Adele", "Cyd"]);
}

#[tokio::test]
async fn where_absent_property_and_return_are_null() {
    let live = setup();
    let q = query_stmt("MATCH (p:Person) WHERE p.ghost = 1 RETURN p.name");
    let batches = run_query(&q, &live, NOW).await.unwrap();
    assert!(batches.iter().all(|batch| batch.num_rows() == 0));
    let q = query_stmt("MATCH (p:Person) RETURN p.ghost");
    let batches = run_query(&q, &live, NOW).await.unwrap();
    let rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(rows, 3);
    for batch in &batches {
        let ghost = batch.column_by_name("p.ghost").unwrap();
        assert!(matches!(
            ghost.data_type(),
            arrow::datatypes::DataType::Null
        ));
    }
}

#[tokio::test]
async fn expression_return_projects_value() {
    let live = setup();
    let q = query_stmt("MATCH (p:Person) RETURN p.age + 1");
    let batches = run_query(&q, &live, NOW).await.unwrap();
    let mut values = batches
        .iter()
        .flat_map(|batch| {
            let ages = batch
                .column_by_name("p.age + 1")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            (0..ages.len()).map(|idx| ages.value(idx))
        })
        .collect::<Vec<_>>();
    values.sort();
    assert_eq!(values, vec![37, 37, 42]);
}

#[tokio::test]
async fn for_system_time_as_of_travels_back() {
    let mut live = setup();
    let mut doc = Doc::new();
    doc.insert("name".into(), Value::Str("Adele".into()));
    doc.insert("age".into(), Value::Int(36));
    live.append(Event {
        iid: Iid::derive("g", "nodes", &[1u8]),
        system_from: Instant::from_micros(10),
        valid_from: Instant::from_micros(10),
        valid_to: Instant::END_OF_TIME,
        src: None,
        dst: None,
        op: Op::Put {
            labels: vec!["Person".into()],
            doc,
        },
    })
    .unwrap();

    // Snapshot system time 5µs: rename at 10µs hasn't happened.
    let q = query_stmt(
        "FOR SYSTEM_TIME AS OF TIMESTAMP '1970-01-01T00:00:00.000005Z' \
         MATCH (p:Person) WHERE p.age = 36 RETURN p.name AS name",
    );
    let batches = run_query(&q, &live, NOW).await.unwrap();
    assert_eq!(names(&batches), vec!["Ada", "Cyd"]);

    // Per-MATCH placement behaves identically.
    let q = query_stmt(
        "MATCH (p:Person) FOR SYSTEM_TIME AS OF TIMESTAMP '1970-01-01T00:00:00.000005Z' \
         WHERE p.age = 36 RETURN p.name AS name",
    );
    let batches = run_query(&q, &live, NOW).await.unwrap();
    assert_eq!(names(&batches), vec!["Ada", "Cyd"]);
}

#[tokio::test]
async fn temporal_functions_project_hidden_columns() {
    let live = setup();
    let q = query_stmt(
        "MATCH (p:Person) WHERE p.name = 'Ada' \
         RETURN p.name AS name, valid_from(p) AS vf, valid_to(p), system_from(p)",
    );
    let batches = run_query(&q, &live, NOW).await.unwrap();
    let batch = &batches[0];

    let vf: &TimestampMicrosecondArray = batch
        .column_by_name("vf")
        .unwrap()
        .as_any()
        .downcast_ref()
        .unwrap();
    let vt: &TimestampMicrosecondArray = batch
        .column_by_name("valid_to(p)")
        .unwrap()
        .as_any()
        .downcast_ref()
        .unwrap();
    let sf: &TimestampMicrosecondArray = batch
        .column_by_name("system_from(p)")
        .unwrap()
        .as_any()
        .downcast_ref()
        .unwrap();
    assert_eq!(vf.value(0), 1);
    assert_eq!(vt.value(0), Instant::END_OF_TIME.as_micros());
    assert_eq!(sf.value(0), 1);
}

#[tokio::test]
async fn split_matching_equals_one_shot() {
    use varve_plan::{iids_from_snapshot, matching_iids, matching_snapshot};
    use varve_types::{TemporalBounds, TemporalDimension};

    let live = setup();
    let q = query_stmt("MATCH (p:Person) WHERE p.age = 36 RETURN p.name");
    let bounds = TemporalBounds {
        valid: TemporalDimension::at(varve_types::Instant::from_micros(100)),
        system: TemporalDimension::at(varve_types::Instant::from_micros(100)),
    };

    let pattern = q.single_node().unwrap();
    let Clause::Match { where_clause, .. } = &q.first.clauses[0] else {
        panic!("expected match clause")
    };
    let snapshot = matching_snapshot(pattern, &live, &bounds).unwrap();
    let split = iids_from_snapshot(snapshot, pattern.var.as_deref(), where_clause, &[])
        .await
        .unwrap();
    let one_shot = matching_iids(pattern, where_clause, &live, &bounds)
        .await
        .unwrap();

    assert_eq!(split.len(), 2); // Ada and Cyd are 36
    assert_eq!(split, one_shot);
}

#[tokio::test]
async fn inline_absent_property_matching_returns_no_iids_not_unknown_column() {
    use varve_gql::ast::Literal;
    use varve_plan::{iids_from_snapshot, matching_snapshot};
    use varve_types::{TemporalBounds, TemporalDimension};

    let live = setup();
    let q = query_stmt("MATCH (p:Person) RETURN p.name");
    let bounds = TemporalBounds {
        valid: TemporalDimension::at(varve_types::Instant::from_micros(100)),
        system: TemporalDimension::at(varve_types::Instant::from_micros(100)),
    };
    let pattern = q.single_node().unwrap();
    let snapshot = matching_snapshot(pattern, &live, &bounds).unwrap();
    let iids = iids_from_snapshot(
        snapshot,
        pattern.var.as_deref(),
        &None,
        &[("ghost".to_string(), Expr::Literal(Literal::Int(1)))],
    )
    .await
    .unwrap();

    assert!(iids.is_empty());
}

mod pushdown {
    use std::collections::BTreeMap;
    use varve_gql::ast::{Expr, Literal, Statement};
    use varve_plan::{effective_bounds, iid_point, scan_specs_for_stmt, SpecKind};
    use varve_types::{Iid, Instant, TemporalDimension, Value};

    fn query(gql: &str) -> varve_gql::ast::QueryStmt {
        let Statement::Query(q) = varve_gql::parse(gql).unwrap() else {
            panic!("not a query");
        };
        *q
    }

    #[test]
    fn effective_bounds_default_to_now_on_both_axes() {
        let q = query("MATCH (p:P) RETURN p.x");
        let now = Instant::from_micros(1000);
        let b = effective_bounds(&q, now).unwrap();
        assert_eq!(b.valid, TemporalDimension::at(now));
        assert_eq!(b.system, TemporalDimension::at(now));
    }

    #[test]
    fn effective_bounds_honor_query_level_clauses() {
        let q =
            query("FOR VALID_TIME AS OF TIMESTAMP '2020-01-01T00:00:00Z' MATCH (p:P) RETURN p.x");
        let now = Instant::from_micros(2_000_000_000_000_000);
        let b = effective_bounds(&q, now).unwrap();
        let t2020 = Instant::parse_rfc3339("2020-01-01T00:00:00Z").unwrap();
        assert_eq!(b.valid, TemporalDimension::at(t2020));
        assert_eq!(b.system, TemporalDimension::at(now)); // unstated axis defaults
    }

    #[test]
    fn return_modifiers_are_rejected_by_degenerate_execution_path() {
        for gql in [
            "MATCH (p:P) RETURN DISTINCT p.x",
            "MATCH (p:P) RETURN p.x ORDER BY p.x",
            "MATCH (p:P) RETURN p.x SKIP 1",
            "MATCH (p:P) RETURN p.x OFFSET 1",
            "MATCH (p:P) RETURN p.x LIMIT 1",
        ] {
            let q = query(gql);
            let err = scan_specs_for_stmt(&q, "default", 8, &BTreeMap::new()).unwrap_err();
            assert!(
                err.to_string()
                    .contains("query shape is not supported by this execution path"),
                "{gql}: {err}"
            );
        }
    }

    #[test]
    fn multi_path_match_uses_pipeline_unsupported_sentinel() {
        let q = query("MATCH (a:A), (b:B) RETURN a.x");
        let err = scan_specs_for_stmt(&q, "default", 8, &BTreeMap::new()).unwrap_err();
        assert!(
            err.to_string()
                .contains("query shape is not supported by this execution path"),
            "{err}"
        );
    }

    fn id_eq(prop: &str, value: Literal) -> Option<Expr> {
        id_eq_expr(prop, Expr::Literal(value))
    }

    fn id_eq_expr(prop: &str, rhs: Expr) -> Option<Expr> {
        Some(Expr::Binary {
            op: varve_gql::ast::BinaryOp::Eq,
            lhs: Box::new(Expr::Prop {
                var: "p".into(),
                prop: prop.into(),
            }),
            rhs: Box::new(rhs),
        })
    }

    fn params() -> BTreeMap<String, Value> {
        BTreeMap::new()
    }

    #[test]
    fn iid_point_from_id_equality() {
        let expected = Iid::derive("default", "nodes", &Value::Int(42).id_bytes().unwrap());
        assert_eq!(
            iid_point(
                &id_eq("_id", Literal::Int(42)),
                &params(),
                "default",
                "nodes",
            ),
            Some(expected)
        );
    }

    #[test]
    fn param_id_equality_derives_iid_point() {
        let params = BTreeMap::from([("id".to_string(), Value::Int(42))]);
        let expected = Iid::derive("default", "nodes", &Value::Int(42).id_bytes().unwrap());
        let where_clause = id_eq_expr("_id", Expr::Param("id".into()));

        assert_eq!(
            iid_point(&where_clause, &params, "default", "nodes"),
            Some(expected)
        );

        let q = query("MATCH (p:P) WHERE p._id = $id RETURN p.name");
        let specs = scan_specs_for_stmt(&q, "default", 10, &params).unwrap();
        assert_eq!(specs.len(), 1);
        assert!(matches!(
            &specs[0].kind,
            SpecKind::Node {
                iid_point: Some(iid),
                ..
            } if *iid == expected
        ));
    }

    #[test]
    fn iid_point_distinguishes_literal_types() {
        // Int(49) and Str("1") collide as raw bytes; id_bytes type tags differ.
        let a = iid_point(
            &id_eq("_id", Literal::Int(0x31)),
            &params(),
            "default",
            "nodes",
        );
        let b = iid_point(
            &id_eq("_id", Literal::Str("1".into())),
            &params(),
            "default",
            "nodes",
        );
        assert!(a.is_some() && b.is_some());
        assert_ne!(a, b);
    }

    #[test]
    fn iid_point_falls_back_to_none() {
        // non-_id property
        assert_eq!(
            iid_point(
                &id_eq("name", Literal::Int(1)),
                &params(),
                "default",
                "nodes",
            ),
            None
        );
        // literals that cannot be ids (Value::id_bytes errors)
        assert_eq!(
            iid_point(
                &id_eq("_id", Literal::Float(2.5)),
                &params(),
                "default",
                "nodes",
            ),
            None
        );
        assert_eq!(
            iid_point(&id_eq("_id", Literal::Null), &params(), "default", "nodes",),
            None
        );
        // no WHERE at all
        assert_eq!(iid_point(&None, &params(), "default", "nodes"), None);
    }
}
