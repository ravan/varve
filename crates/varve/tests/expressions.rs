#![allow(clippy::unwrap_used)]

use arrow::array::{Array, StringArray};
use varve::{Db, RecordBatch};

async fn setup() -> Db {
    let db = Db::memory();
    db.execute("INSERT (:T {_id: 'a', name: 'a', x: 1})")
        .await
        .unwrap();
    db.execute("INSERT (:T {_id: 'b', name: 'b'})")
        .await
        .unwrap();
    db.execute("INSERT (:T {_id: 'c', name: 'c', x: 2})")
        .await
        .unwrap();
    db
}

fn names(batches: &[RecordBatch]) -> Vec<String> {
    let mut out: Vec<String> = batches
        .iter()
        .flat_map(|batch| {
            let col = batch
                .column_by_name("name")
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
async fn where_or_preserves_sql_three_valued_logic() {
    let db = setup().await;

    let batches = db
        .query("MATCH (n:T) WHERE n.x = 1 OR n.x = 2 RETURN n.name AS name")
        .await
        .unwrap();

    assert_eq!(names(&batches), vec!["a", "c"]);
}

#[tokio::test]
async fn where_not_drops_unknown_rows() {
    let db = setup().await;

    let batches = db
        .query("MATCH (n:T) WHERE NOT n.x = 1 RETURN n.name AS name")
        .await
        .unwrap();

    assert_eq!(names(&batches), vec!["c"]);
}

#[tokio::test]
async fn where_null_predicates_observe_absent_properties() {
    let db = setup().await;

    let null_rows = db
        .query("MATCH (n:T) WHERE n.x IS NULL RETURN n.name AS name")
        .await
        .unwrap();
    assert_eq!(names(&null_rows), vec!["b"]);

    let not_null_rows = db
        .query("MATCH (n:T) WHERE n.x IS NOT NULL RETURN n.name AS name")
        .await
        .unwrap();
    assert_eq!(names(&not_null_rows), vec!["a", "c"]);
}

#[tokio::test]
async fn case_expression_can_filter_rows() {
    let db = setup().await;

    let batches = db
        .query("MATCH (n:T) WHERE CASE WHEN n.x = 1 THEN true ELSE false END RETURN n.name AS name")
        .await
        .unwrap();

    assert_eq!(names(&batches), vec!["a"]);
}

#[tokio::test]
async fn iid_fast_path_keeps_extra_conjuncts_as_filters() {
    let db = setup().await;

    let batches = db
        .query("MATCH (n:T) WHERE n._id = 'a' AND n.x > 0 RETURN n.name AS name")
        .await
        .unwrap();

    assert_eq!(names(&batches), vec!["a"]);
}

#[tokio::test]
async fn absent_property_where_returns_no_rows_not_error() {
    let db = setup().await;

    let batches = db
        .query("MATCH (n:T) WHERE n.no_such = 1 RETURN n.name AS name")
        .await
        .unwrap();

    assert!(batches.iter().all(|batch| batch.num_rows() == 0));
}

#[tokio::test]
async fn absent_inline_property_returns_no_rows_not_error() {
    let db = setup().await;

    let batches = db
        .query("MATCH (n:T {no_such: 1}) RETURN n.name AS name")
        .await
        .unwrap();

    assert!(batches.iter().all(|batch| batch.num_rows() == 0));
}
