#![allow(clippy::unwrap_used)]

use arrow::array::{Array, StringArray};
use varve::{Db, RecordBatch};

fn string_rows(batches: &[RecordBatch], col: &str) -> Vec<String> {
    let mut rows = Vec::new();
    for batch in batches {
        let idx = batch.schema().column_with_name(col).unwrap().0;
        let array = batch
            .column(idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..array.len() {
            rows.push(array.value(row).to_string());
        }
    }
    rows.sort();
    rows
}

fn row_count(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

#[tokio::test]
async fn multi_label_insert_and_conjunction_match() {
    let db = Db::memory();
    db.execute(
        "INSERT (:A:B {_id: 1, name: 'ab'}), (:A {_id: 2, name: 'a'}), (:B {_id: 3, name: 'b'})",
    )
    .await
    .unwrap();

    let hit = db
        .query("MATCH (n:A:B) RETURN n.name AS name")
        .await
        .unwrap();
    assert_eq!(string_rows(&hit, "name"), vec!["ab"]);

    let miss = db
        .query("MATCH (n:A:C) RETURN n.name AS name")
        .await
        .unwrap();
    assert_eq!(row_count(&miss), 0);
}

#[tokio::test]
async fn label_alternation_match() {
    let db = Db::memory();
    db.execute(
        "INSERT (:A {_id: 1, name: 'a'}), (:B {_id: 2, name: 'b'}), (:C {_id: 3, name: 'c'})",
    )
    .await
    .unwrap();

    let rows = db
        .query("MATCH (n:A|B) RETURN n.name AS name")
        .await
        .unwrap();
    assert_eq!(string_rows(&rows, "name"), vec!["a", "b"]);
}

#[tokio::test]
async fn alternation_with_edges_still_single_label() {
    let db = Db::memory();
    let err = db
        .query("MATCH (a:A)-[:A|B]->(b:A) RETURN b._id AS id")
        .await
        .unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("expected ']'") && msg.contains("Pipe"),
        "unexpected error: {msg}"
    );
}
