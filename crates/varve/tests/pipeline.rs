#![allow(clippy::unwrap_used)]

use arrow::array::{Array, Int64Array, StringArray};
use arrow::datatypes::DataType;
use varve::{Db, RecordBatch};

async fn seed_people(db: &Db) {
    db.execute(
        "INSERT (:Person {_id: 1, name: 'Ada'}),
                (:Person {_id: 2, name: 'Bob'}),
                (:Person {_id: 3, name: 'Cy'})",
    )
    .await
    .unwrap();
    db.execute("MATCH (a:Person {_id: 1}), (b:Person {_id: 2}) INSERT (a)-[:KNOWS]->(b)")
        .await
        .unwrap();
    db.execute("MATCH (b:Person {_id: 2}), (c:Person {_id: 3}) INSERT (b)-[:LIKES]->(c)")
        .await
        .unwrap();
}

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

fn int_rows(batches: &[RecordBatch], col: &str) -> Vec<i64> {
    let mut rows = Vec::new();
    for batch in batches {
        let idx = batch.schema().column_with_name(col).unwrap().0;
        let array = batch
            .column(idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..array.len() {
            rows.push(array.value(row));
        }
    }
    rows.sort();
    rows
}

fn assert_single_null(batches: &[RecordBatch], col: &str) {
    assert_eq!(row_count(batches), 1);
    let batch = &batches[0];
    let idx = batch.schema().column_with_name(col).unwrap().0;
    assert_eq!(batch.column(idx).data_type(), &DataType::Null);
}

fn nullable_string_pairs(
    batches: &[RecordBatch],
    left: &str,
    right: &str,
) -> Vec<(String, Option<String>)> {
    let mut rows = Vec::new();
    for batch in batches {
        let left_idx = batch.schema().column_with_name(left).unwrap().0;
        let right_idx = batch.schema().column_with_name(right).unwrap().0;
        let left_array = batch
            .column(left_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let right_array = batch
            .column(right_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            let right_value = if right_array.is_null(row) {
                None
            } else {
                Some(right_array.value(row).to_string())
            };
            rows.push((left_array.value(row).to_string(), right_value));
        }
    }
    rows.sort();
    rows
}

#[tokio::test]
async fn two_match_clauses_join_on_shared_var() {
    let db = Db::memory();
    seed_people(&db).await;

    let rows = db
        .query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person)
             MATCH (b:Person)-[:LIKES]->(c:Person)
             RETURN c.name AS name",
        )
        .await
        .unwrap();

    assert_eq!(string_rows(&rows, "name"), vec!["Cy"]);
}

#[tokio::test]
async fn optional_match_preserves_left_rows_with_nulls() {
    let db = Db::memory();
    seed_people(&db).await;

    let rows = db
        .query(
            "MATCH (p:Person)
             OPTIONAL MATCH (p:Person)-[:MISSING]->(f:Person)
             RETURN p.name AS person, f.name AS friend",
        )
        .await
        .unwrap();

    assert_eq!(
        nullable_string_pairs(&rows, "person", "friend"),
        vec![
            ("Ada".to_string(), None),
            ("Bob".to_string(), None),
            ("Cy".to_string(), None),
        ]
    );
}

#[tokio::test]
async fn optional_match_chained_after_hop() {
    let db = Db::memory();
    seed_people(&db).await;

    let rows = db
        .query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person)
             OPTIONAL MATCH (b:Person)-[:LIKES]->(c:Person)
             RETURN c.name AS name",
        )
        .await
        .unwrap();

    assert_eq!(string_rows(&rows, "name"), vec!["Cy"]);
}

#[tokio::test]
async fn comma_separated_paths_shared_var_join() {
    let db = Db::memory();
    seed_people(&db).await;

    let rows = db
        .query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person), (b:Person)-[:LIKES]->(c:Person)
             RETURN c.name AS name",
        )
        .await
        .unwrap();

    assert_eq!(string_rows(&rows, "name"), vec!["Cy"]);
}

#[tokio::test]
async fn comma_separated_paths_disjoint_cross_product() {
    let db = Db::memory();
    seed_people(&db).await;

    let rows = db
        .query("MATCH (a:Person), (b:Person) RETURN a.name AS a, b.name AS b")
        .await
        .unwrap();

    assert_eq!(row_count(&rows), 9);
}

#[tokio::test]
async fn later_match_multi_path_joins_accumulator_after_clause_paths() {
    let db = Db::memory();
    seed_people(&db).await;

    let rows = db
        .query(
            "MATCH (a:Person {_id: 1}) \
             MATCH (b:Person), (a:Person)-[:KNOWS]->(c:Person) \
             RETURN b.name AS name",
        )
        .await
        .unwrap();

    assert_eq!(string_rows(&rows, "name"), vec!["Ada", "Bob", "Cy"]);
}

#[tokio::test]
async fn filter_mid_pipeline() {
    let db = Db::memory();
    seed_people(&db).await;

    let rows = db
        .query(
            "MATCH (p:Person)
             FILTER p.name = 'Ada'
             MATCH (p:Person)-[:KNOWS]->(f:Person)
             RETURN f.name AS name",
        )
        .await
        .unwrap();

    assert_eq!(string_rows(&rows, "name"), vec!["Bob"]);
}

#[tokio::test]
async fn let_binds_expression_value() {
    let db = Db::memory();
    seed_people(&db).await;

    let rows = db
        .query(
            "MATCH (p:Person)
             FILTER p.name = 'Ada'
             LET score = 40 + 2
             RETURN score AS score",
        )
        .await
        .unwrap();

    assert_eq!(int_rows(&rows, "score"), vec![42]);
}

#[tokio::test]
async fn for_unwinds_list_literal() {
    let db = Db::memory();
    seed_people(&db).await;

    let rows = db
        .query(
            "MATCH (p:Person)
             FILTER p.name = 'Ada'
             FOR x IN [1, 2, 3]
             RETURN x AS x",
        )
        .await
        .unwrap();

    assert_eq!(int_rows(&rows, "x"), vec![1, 2, 3]);
}

#[tokio::test]
async fn for_over_null_or_empty_eliminates_row() {
    let db = Db::memory();
    seed_people(&db).await;

    let empty_rows = db
        .query(
            "MATCH (p:Person)
             FILTER p.name = 'Ada'
             FOR x IN []
             RETURN x AS x",
        )
        .await
        .unwrap();
    let null_rows = db
        .query(
            "MATCH (p:Person)
             FILTER p.name = 'Ada'
             FOR x IN NULL
             RETURN x AS x",
        )
        .await
        .unwrap();

    assert_eq!(row_count(&empty_rows), 0);
    assert_eq!(row_count(&null_rows), 0);
}

#[tokio::test]
async fn optional_match_first_clause_unsupported() {
    let db = Db::memory();
    seed_people(&db).await;

    let err = db
        .query("OPTIONAL MATCH (p:Person) RETURN p.name AS name")
        .await
        .unwrap_err();

    assert!(err.to_string().contains("unsupported in v1"), "{err}");
}

#[tokio::test]
async fn match_where_can_reference_later_path_var() {
    let db = Db::memory();
    seed_people(&db).await;

    let rows = db
        .query("MATCH (a:Person), (b:Person) WHERE b.name = 'Bob' RETURN a.name AS name")
        .await
        .unwrap();

    assert_eq!(string_rows(&rows, "name"), vec!["Ada", "Bob", "Cy"]);
}

#[tokio::test]
async fn later_match_where_can_reference_accumulator_var() {
    let db = Db::memory();
    seed_people(&db).await;

    let rows = db
        .query(
            "MATCH (a:Person {_id: 1}) MATCH (b:Person) WHERE a.name = 'Ada' \
             RETURN b.name AS name",
        )
        .await
        .unwrap();

    assert_eq!(string_rows(&rows, "name"), vec!["Ada", "Bob", "Cy"]);
}

#[tokio::test]
async fn let_cannot_rebind_element_var() {
    let db = Db::memory();
    seed_people(&db).await;

    let err = db
        .query("MATCH (p:Person) LET p = 1 RETURN p AS p")
        .await
        .unwrap_err();

    assert!(err.to_string().contains("re-binding"), "{err}");
}

#[tokio::test]
async fn match_cannot_rebind_value_var() {
    let db = Db::memory();
    seed_people(&db).await;

    let err = db
        .query("MATCH (p:Person) LET x = 1 MATCH (x:Person) RETURN x.name AS name")
        .await
        .unwrap_err();

    assert!(err.to_string().contains("re-binding"), "{err}");
}

#[tokio::test]
async fn optional_match_without_rhs_schema_returns_null_columns() {
    let db = Db::memory();
    seed_people(&db).await;

    let rows = db
        .query("MATCH (p:Person {_id: 1}) OPTIONAL MATCH (m:Missing) RETURN m.name AS missing")
        .await
        .unwrap();

    assert_single_null(&rows, "missing");
}
