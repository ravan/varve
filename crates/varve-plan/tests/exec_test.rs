#![allow(clippy::unwrap_used)] // tests may use unwrap; crate-level allow covers helper fns
use arrow::array::{Array, StringArray};
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
        Statement::Query(q) => q,
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
