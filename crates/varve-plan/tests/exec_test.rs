#![allow(clippy::unwrap_used)] // tests may use unwrap (see project rules); the
                               // workspace's allow-unwrap-in-tests clippy.toml setting
                               // only recognizes #[test]-annotated fns / #[cfg(test)]
                               // modules, not plain helper fns in an integration test
                               // binary, so this crate-level allow covers `setup`/`query_stmt`.
use arrow::array::{Array, StringArray};
use varve_gql::ast::Statement;
use varve_index::LiveTable;
use varve_plan::run_query;
use varve_types::{Doc, Iid, Value};

fn setup() -> LiveTable {
    let mut t = LiveTable::new();
    for (n, name, age) in [(1u8, "Ada", 36i64), (2, "Bob", 41), (3, "Cyd", 36)] {
        let mut doc = Doc::new();
        doc.insert("name".into(), Value::Str(name.into()));
        doc.insert("age".into(), Value::Int(age));
        t.append(Iid::derive("g", "nodes", &[n]), vec!["Person".into()], doc)
            .unwrap();
    }
    t
}

fn query_stmt(src: &str) -> varve_gql::ast::QueryStmt {
    match varve_gql::parse(src).unwrap() {
        Statement::Query(q) => q,
        _ => panic!("not a query"),
    }
}

#[tokio::test]
async fn match_where_return_filters_rows() {
    let live = setup();
    let q = query_stmt("MATCH (p:Person) WHERE p.age = 36 RETURN p.name AS name");
    let batches = run_query(&q, &live).await.unwrap();
    let names: Vec<String> = batches
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
    let mut sorted = names.clone();
    sorted.sort();
    assert_eq!(sorted, vec!["Ada", "Cyd"]);
}

#[tokio::test]
async fn unknown_label_returns_empty() {
    let live = setup();
    let q = query_stmt("MATCH (r:Robot) RETURN r.name");
    assert!(run_query(&q, &live).await.unwrap().is_empty());
}
