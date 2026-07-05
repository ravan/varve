#![allow(clippy::unwrap_used)] // tests may use unwrap; crate-level allow covers helper fns
use arrow::array::{Array, StringArray, TimestampMicrosecondArray};
use varve_gql::ast::Statement;
use varve_index::{Event, LiveTable, Op};
use varve_plan::run_query;
use varve_plan::PlanError;
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

// Deferred from slice 1 (STATUS.md remediation list): both UnknownColumn paths.
#[tokio::test]
async fn where_and_return_on_absent_property_are_unknown_column() {
    let live = setup();
    let q = query_stmt("MATCH (p:Person) WHERE p.ghost = 1 RETURN p.name");
    assert!(matches!(
        run_query(&q, &live, NOW).await,
        Err(PlanError::UnknownColumn(c)) if c == "ghost"
    ));
    let q = query_stmt("MATCH (p:Person) RETURN p.ghost");
    assert!(matches!(
        run_query(&q, &live, NOW).await,
        Err(PlanError::UnknownColumn(c)) if c == "ghost"
    ));
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
        .column_by_name("valid_to")
        .unwrap()
        .as_any()
        .downcast_ref()
        .unwrap();
    let sf: &TimestampMicrosecondArray = batch
        .column_by_name("system_from")
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

    let snapshot = matching_snapshot(&q.pattern, &live, &bounds).unwrap();
    let split = iids_from_snapshot(snapshot, &q.where_clause).await.unwrap();
    let one_shot = matching_iids(&q.pattern, &q.where_clause, &live, &bounds)
        .await
        .unwrap();

    assert_eq!(split.len(), 2); // Ada and Cyd are 36
    assert_eq!(split, one_shot);
}

mod pushdown {
    use varve_gql::ast::{Expr, Literal, Statement};
    use varve_plan::{effective_bounds, iid_point};
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
        let b = effective_bounds(&q, now);
        assert_eq!(b.valid, TemporalDimension::at(now));
        assert_eq!(b.system, TemporalDimension::at(now));
    }

    #[test]
    fn effective_bounds_honor_query_level_clauses() {
        let q =
            query("FOR VALID_TIME AS OF TIMESTAMP '2020-01-01T00:00:00Z' MATCH (p:P) RETURN p.x");
        let now = Instant::from_micros(2_000_000_000_000_000);
        let b = effective_bounds(&q, now);
        let t2020 = Instant::parse_rfc3339("2020-01-01T00:00:00Z").unwrap();
        assert_eq!(b.valid, TemporalDimension::at(t2020));
        assert_eq!(b.system, TemporalDimension::at(now)); // unstated axis defaults
    }

    fn id_eq(prop: &str, value: Literal) -> Option<Expr> {
        Some(Expr::PropEq {
            var: "p".into(),
            prop: prop.into(),
            value,
        })
    }

    #[test]
    fn iid_point_from_id_equality() {
        let expected = Iid::derive("default", "nodes", &Value::Int(42).id_bytes().unwrap());
        assert_eq!(
            iid_point(&id_eq("_id", Literal::Int(42)), "default", "nodes"),
            Some(expected)
        );
    }

    #[test]
    fn iid_point_distinguishes_literal_types() {
        // Int(49) and Str("1") collide as raw bytes; id_bytes type tags differ.
        let a = iid_point(&id_eq("_id", Literal::Int(0x31)), "default", "nodes");
        let b = iid_point(&id_eq("_id", Literal::Str("1".into())), "default", "nodes");
        assert!(a.is_some() && b.is_some());
        assert_ne!(a, b);
    }

    #[test]
    fn iid_point_falls_back_to_none() {
        // non-_id property
        assert_eq!(
            iid_point(&id_eq("name", Literal::Int(1)), "default", "nodes"),
            None
        );
        // literals that cannot be ids (Value::id_bytes errors)
        assert_eq!(
            iid_point(&id_eq("_id", Literal::Float(2.5)), "default", "nodes"),
            None
        );
        assert_eq!(
            iid_point(&id_eq("_id", Literal::Null), "default", "nodes"),
            None
        );
        // no WHERE at all
        assert_eq!(iid_point(&None, "default", "nodes"), None);
    }
}
