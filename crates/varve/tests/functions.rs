#![allow(clippy::unwrap_used)]

use arrow::array::{Array, StringArray};
use varve::{Db, RecordBatch};

fn strings(batches: &[RecordBatch], col: &str) -> Vec<String> {
    let mut out: Vec<String> = batches
        .iter()
        .flat_map(|batch| {
            let col = batch
                .column_by_name(col)
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            (0..col.len()).map(|idx| col.value(idx).to_string())
        })
        .collect();
    out.sort();
    out
}

#[tokio::test]
async fn string_functions_in_where_and_return() {
    let db = Db::memory();
    db.execute("INSERT (:Person {_id: 1, name: '  Ada  '})")
        .await
        .unwrap();
    db.execute("INSERT (:Person {_id: 2, name: 'Bob'})")
        .await
        .unwrap();

    let batches = db
        .query(
            "MATCH (n:Person) \
             WHERE upper(trim(n.name)) = 'ADA' \
             RETURN lower(trim(n.name)) AS name",
        )
        .await
        .unwrap();

    assert_eq!(strings(&batches, "name"), vec!["ada"]);
}

#[tokio::test]
async fn string_predicate_operators_in_where() {
    let db = Db::memory();
    db.execute("INSERT (:Person {_id: 1, name: 'Ada Lovelace'})")
        .await
        .unwrap();
    db.execute("INSERT (:Person {_id: 2, name: 'Grace Hopper'})")
        .await
        .unwrap();

    for (predicate, expected) in [
        ("n.name STARTS WITH 'Ada'", vec!["Ada Lovelace"]),
        ("n.name ENDS WITH 'Hopper'", vec!["Grace Hopper"]),
        ("n.name CONTAINS 'Love'", vec!["Ada Lovelace"]),
    ] {
        let batches = db
            .query(format!(
                "MATCH (n:Person) WHERE {predicate} RETURN n.name AS name"
            ))
            .await
            .unwrap();
        assert_eq!(strings(&batches, "name"), expected);
    }
}

#[tokio::test]
async fn numeric_functions_in_where() {
    let db = Db::memory();
    db.execute("INSERT (:T {_id: 1, name: 'neg', delta: -3})")
        .await
        .unwrap();
    db.execute("INSERT (:T {_id: 2, name: 'small', delta: 2})")
        .await
        .unwrap();

    let batches = db
        .query("MATCH (n:T) WHERE abs(n.delta) = 3 RETURN n.name AS name")
        .await
        .unwrap();

    assert_eq!(strings(&batches, "name"), vec!["neg"]);
}

#[tokio::test]
async fn temporal_fns_still_work_via_registry() {
    let db = Db::memory();
    db.execute(
        "INSERT (:P {_id: 1, name: 'Eve'}) \
         VALID FROM TIMESTAMP '2021-03-04T05:06:07Z'",
    )
    .await
    .unwrap();

    let batches = db
        .query(
            "MATCH (p:P) \
             WHERE valid_from(p) IS NOT NULL \
             RETURN p.name AS name",
        )
        .await
        .unwrap();

    assert_eq!(strings(&batches, "name"), vec!["Eve"]);
}

#[tokio::test]
async fn nested_function_calls() {
    let db = Db::memory();
    db.execute("INSERT (:Person {_id: 1, name: '  ada  '})")
        .await
        .unwrap();
    db.execute("INSERT (:Person {_id: 2, name: 'Ada Lovelace'})")
        .await
        .unwrap();

    let batches = db
        .query("MATCH (n:Person) WHERE upper(trim(n.name)) = 'ADA' RETURN n.name AS name")
        .await
        .unwrap();

    assert_eq!(strings(&batches, "name"), vec!["  ada  "]);
}
