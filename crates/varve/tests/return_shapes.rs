#![allow(clippy::unwrap_used)]

use arrow::array::{Array, FixedSizeBinaryArray, Float64Array, Int64Array, ListArray, StringArray};
use varve::{Db, RecordBatch};

async fn people_db() -> Db {
    let db = Db::memory();
    db.execute("INSERT (:Person {_id: 1, name: 'Ada', age: 36, city: 'London'})")
        .await
        .unwrap();
    db.execute("INSERT (:Person {_id: 2, name: 'Bob', age: 41, city: 'Paris'})")
        .await
        .unwrap();
    db.execute("INSERT (:Person {_id: 3, name: 'Cy', age: 36, city: 'London'})")
        .await
        .unwrap();
    db
}

fn int_column(batches: &[RecordBatch], col: &str) -> Vec<i64> {
    let mut out: Vec<_> = batches
        .iter()
        .flat_map(|batch| {
            let values = batch
                .column_by_name(col)
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            (0..values.len()).map(|idx| values.value(idx))
        })
        .collect();
    // determinism: DataFusion output order is not stable unless the query has ORDER BY.
    out.sort();
    out
}

fn string_column(batches: &[RecordBatch], col: &str) -> Vec<String> {
    let mut out: Vec<_> = batches
        .iter()
        .flat_map(|batch| {
            let values = batch
                .column_by_name(col)
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            (0..values.len()).map(|idx| values.value(idx).to_string())
        })
        .collect();
    // determinism: DataFusion output order is not stable unless the query has ORDER BY.
    out.sort();
    out
}

fn string_pair_rows(
    batches: &[RecordBatch],
    left_col: &str,
    right_col: &str,
) -> Vec<(String, String)> {
    batches
        .iter()
        .flat_map(|batch| {
            let left = batch
                .column_by_name(left_col)
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let right = batch
                .column_by_name(right_col)
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            (0..batch.num_rows())
                .map(|idx| (left.value(idx).to_string(), right.value(idx).to_string()))
        })
        .collect()
}

fn string_int_rows(batches: &[RecordBatch], str_col: &str, int_col: &str) -> Vec<(String, i64)> {
    let mut out: Vec<_> = batches
        .iter()
        .flat_map(|batch| {
            let strings = batch
                .column_by_name(str_col)
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let ints = batch
                .column_by_name(int_col)
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            (0..batch.num_rows()).map(|idx| (strings.value(idx).to_string(), ints.value(idx)))
        })
        .collect();
    // determinism: DataFusion output order is not stable unless the query has ORDER BY.
    out.sort();
    out
}

fn int_int_rows(batches: &[RecordBatch], left_col: &str, right_col: &str) -> Vec<(i64, i64)> {
    let mut out: Vec<_> = batches
        .iter()
        .flat_map(|batch| {
            let left = batch
                .column_by_name(left_col)
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let right = batch
                .column_by_name(right_col)
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            (0..batch.num_rows()).map(|idx| (left.value(idx), right.value(idx)))
        })
        .collect();
    // determinism: DataFusion output order is not stable unless query has ORDER BY.
    out.sort();
    out
}

fn int_int_int_rows(
    batches: &[RecordBatch],
    first_col: &str,
    second_col: &str,
    third_col: &str,
) -> Vec<(i64, i64, i64)> {
    let mut out: Vec<_> = batches
        .iter()
        .flat_map(|batch| {
            let first = batch
                .column_by_name(first_col)
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let second = batch
                .column_by_name(second_col)
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let third = batch
                .column_by_name(third_col)
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            (0..batch.num_rows()).map(|idx| (first.value(idx), second.value(idx), third.value(idx)))
        })
        .collect();
    // determinism: DataFusion output order is not stable unless query has ORDER BY.
    out.sort();
    out
}

fn single_int_pair(batches: &[RecordBatch], left_col: &str, right_col: &str) -> (i64, i64) {
    let rows: Vec<_> = batches
        .iter()
        .flat_map(|batch| {
            let left = batch
                .column_by_name(left_col)
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let right = batch
                .column_by_name(right_col)
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            (0..batch.num_rows()).map(|idx| (left.value(idx), right.value(idx)))
        })
        .collect();
    assert_eq!(rows.len(), 1);
    rows[0]
}

fn single_string_list(batches: &[RecordBatch], col: &str) -> Vec<String> {
    let values: Vec<_> = batches
        .iter()
        .flat_map(|batch| {
            let lists = batch
                .column_by_name(col)
                .unwrap()
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            (0..lists.len()).map(|idx| {
                lists
                    .value(idx)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .iter()
                    .map(|value| value.unwrap().to_string())
                    .collect::<Vec<_>>()
            })
        })
        .collect();
    assert_eq!(values.len(), 1);
    let mut values = values.into_iter().next().unwrap();
    // determinism: DataFusion output order is not stable unless the query has ORDER BY.
    values.sort();
    values
}

fn fixed_binary_widths(batches: &[RecordBatch], col: &str) -> Vec<usize> {
    let mut out: Vec<_> = batches
        .iter()
        .flat_map(|batch| {
            let values = batch
                .column_by_name(col)
                .unwrap()
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap();
            (0..values.len()).map(|idx| values.value(idx).len())
        })
        .collect();
    out.sort();
    out
}

fn single_numeric_aggregate_row(batches: &[RecordBatch]) -> (i64, i64, i64, f64) {
    let rows: Vec<_> = batches
        .iter()
        .flat_map(|batch| {
            let min = batch
                .column_by_name("min_age")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let max = batch
                .column_by_name("max_age")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let sum = batch
                .column_by_name("sum_age")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let avg = batch
                .column_by_name("avg_age")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            (0..batch.num_rows()).map(|idx| {
                (
                    min.value(idx),
                    max.value(idx),
                    sum.value(idx),
                    avg.value(idx),
                )
            })
        })
        .collect();
    assert_eq!(rows.len(), 1);
    rows[0]
}

#[tokio::test]
async fn return_arbitrary_expressions() {
    let db = people_db().await;

    let batches = db
        .query("MATCH (n:Person) RETURN n.age * 2 AS double_age")
        .await
        .unwrap();

    assert_eq!(int_column(&batches, "double_age"), vec![72, 72, 82]);
}

#[tokio::test]
async fn implicit_grouping_count_by_key() {
    let db = people_db().await;

    let batches = db
        .query("MATCH (n:Person) RETURN n.city AS city, count(n.age) AS c")
        .await
        .unwrap();

    assert_eq!(
        string_int_rows(&batches, "city", "c"),
        vec![("London".to_string(), 2), ("Paris".to_string(), 1)]
    );
}

#[tokio::test]
async fn count_star_and_count_distinct() {
    let db = people_db().await;

    let batches = db
        .query("MATCH (n:Person) RETURN count(*), count(DISTINCT n.city)")
        .await
        .unwrap();

    assert_eq!(
        single_int_pair(&batches, "count(*)", "count(DISTINCT n.city)"),
        (3, 2)
    );
}

#[tokio::test]
async fn collect_returns_list() {
    let db = people_db().await;

    let batches = db
        .query("MATCH (n:Person) RETURN collect(n.name) AS names")
        .await
        .unwrap();

    assert_eq!(
        single_string_list(&batches, "names"),
        vec!["Ada", "Bob", "Cy"]
    );
}

#[tokio::test]
async fn min_max_sum_avg() {
    let db = people_db().await;

    let batches = db
        .query(
            "MATCH (n:Person) \
             RETURN min(n.age) AS min_age, max(n.age) AS max_age, \
                    sum(n.age) AS sum_age, avg(n.age) AS avg_age",
        )
        .await
        .unwrap();

    let (min, max, sum, avg) = single_numeric_aggregate_row(&batches);
    assert_eq!((min, max, sum), (36, 41, 113));
    assert!((avg - (113.0 / 3.0)).abs() < f64::EPSILON);
}

#[tokio::test]
async fn return_distinct_dedupes() {
    let db = people_db().await;

    let batches = db
        .query("MATCH (n:Person) RETURN DISTINCT n.city AS city")
        .await
        .unwrap();

    assert_eq!(string_column(&batches, "city"), vec!["London", "Paris"]);
}

#[tokio::test]
async fn order_by_multi_key_asc_desc_with_skip_limit() {
    let db = people_db().await;

    let batches = db
        .query(
            "MATCH (n:Person) \
             RETURN n.city AS city, n.name AS name \
             ORDER BY city ASC, name DESC SKIP 1 LIMIT 2",
        )
        .await
        .unwrap();

    assert_eq!(
        string_pair_rows(&batches, "city", "name"),
        vec![
            ("London".to_string(), "Ada".to_string()),
            ("Paris".to_string(), "Bob".to_string()),
        ]
    );
}

#[tokio::test]
async fn offset_is_skip_synonym() {
    let db = people_db().await;

    let batches = db
        .query("MATCH (n:Person) RETURN n.name AS name ORDER BY name ASC OFFSET 1 LIMIT 1")
        .await
        .unwrap();

    assert_eq!(string_column(&batches, "name"), vec!["Bob"]);
}

#[tokio::test]
async fn union_all_concatenates_union_dedupes() {
    let db = people_db().await;

    let all = db
        .query(
            "MATCH (n:Person) WHERE n.city = 'London' RETURN n.name AS name \
             UNION ALL \
             MATCH (n:Person) WHERE n.city = 'London' RETURN n.name AS name",
        )
        .await
        .unwrap();
    assert_eq!(string_column(&all, "name"), vec!["Ada", "Ada", "Cy", "Cy"]);

    let distinct = db
        .query(
            "MATCH (n:Person) WHERE n.city = 'London' RETURN n.name AS name \
             UNION \
             MATCH (n:Person) WHERE n.city = 'London' RETURN n.name AS name",
        )
        .await
        .unwrap();
    assert_eq!(string_column(&distinct, "name"), vec!["Ada", "Cy"]);
}

#[tokio::test]
async fn union_schema_mismatch_errors() {
    let db = people_db().await;

    let result = db
        .query(
            "MATCH (n:Person) RETURN n.name AS name \
             UNION \
             MATCH (n:Person) RETURN n.name AS name, n.age AS age",
        )
        .await;

    assert!(result.is_err());
}

#[tokio::test]
async fn union_schema_name_mismatch_errors() {
    let db = people_db().await;

    let result = db
        .query(
            "MATCH (n:Person) RETURN n.name AS name
             UNION
             MATCH (n:Person) RETURN n.city AS city",
        )
        .await;

    assert!(result.is_err());
}

#[tokio::test]
async fn return_bare_node_materializes_labels_and_props() {
    let db = people_db().await;

    let batches = db
        .query("MATCH (n:Person) WHERE n.name = 'Ada' RETURN n")
        .await
        .unwrap();

    let row_count: usize = batches.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(row_count, 1);
    let first = batches.iter().find(|batch| batch.num_rows() > 0).unwrap();
    assert!(first.column_by_name("n._iid").is_some());
    assert_eq!(single_string_list(&batches, "n._labels"), vec!["Person"]);
    assert_eq!(int_column(&batches, "n._id"), vec![1]);
    assert_eq!(string_column(&batches, "n.name"), vec!["Ada"]);
    assert_eq!(int_column(&batches, "n.age"), vec![36]);
    assert_eq!(string_column(&batches, "n.city"), vec!["London"]);
}

#[tokio::test]
async fn return_bare_edge_includes_endpoints_labels_and_props() {
    let db = people_db().await;
    db.execute(
        "MATCH (a:Person {_id: 1}), (b:Person {_id: 2})
         INSERT (a)-[:KNOWS {since: 2020}]->(b)",
    )
    .await
    .unwrap();

    let batches = db
        .query("MATCH (a:Person)-[e:KNOWS]->(b:Person) WHERE a._id = 1 RETURN e")
        .await
        .unwrap();

    let row_count: usize = batches.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(row_count, 1);
    let first = batches.iter().find(|batch| batch.num_rows() > 0).unwrap();
    assert!(first.column_by_name("e._iid").is_some());
    assert_eq!(single_string_list(&batches, "e._labels"), vec!["KNOWS"]);
    assert_eq!(fixed_binary_widths(&batches, "e._src_iid"), vec![16]);
    assert_eq!(fixed_binary_widths(&batches, "e._dst_iid"), vec![16]);
    assert_eq!(int_column(&batches, "e.since"), vec![2020]);
}

#[tokio::test]
async fn aliased_path_variable_keeps_path_list() {
    let db = people_db().await;
    db.execute(
        "MATCH (a:Person {_id: 1}), (b:Person {_id: 2}) \
         INSERT (a)-[:KNOWS]->(b)",
    )
    .await
    .unwrap();

    let batches = db
        .query("MATCH p = (a:Person)-[:KNOWS]->{1,1}(b:Person) WHERE a._id = 1 RETURN p AS path")
        .await
        .unwrap();

    let rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(rows, 1);
    assert!(batches[0].column_by_name("path").is_some());
}

#[tokio::test]
async fn order_by_can_reference_expanded_bare_element_output() {
    let db = people_db().await;

    let batches = db
        .query("MATCH (n:Person) RETURN n ORDER BY n.name ASC LIMIT 1")
        .await
        .unwrap();

    assert_eq!(string_column(&batches, "n.name"), vec!["Ada"]);
    assert_eq!(int_column(&batches, "n.age"), vec![36]);
}

#[tokio::test]
async fn aggregate_result_can_feed_return_expression() {
    let db = people_db().await;

    let batches = db
        .query("MATCH (n:Person) RETURN count(*) + 1 AS total_plus_one")
        .await
        .unwrap();

    assert_eq!(int_column(&batches, "total_plus_one"), vec![4]);
}

#[tokio::test]
async fn aggregate_expression_groups_by_non_aggregate_subexpression() {
    let db = people_db().await;

    let batches = db
        .query("MATCH (n:Person) RETURN n.age + count(*) AS mixed")
        .await
        .unwrap();

    assert_eq!(int_column(&batches, "mixed"), vec![38, 42]);
}

#[tokio::test]
async fn aggregate_query_projects_non_property_group_expression() {
    let db = people_db().await;

    let batches = db
        .query("MATCH (n:Person) RETURN n.age + 1 AS bucket, count(*) AS c")
        .await
        .unwrap();

    assert_eq!(
        int_int_rows(&batches, "bucket", "c"),
        vec![(37, 2), (42, 1)]
    );
}

#[tokio::test]
async fn aggregate_group_keys_use_structural_identity_not_display_text() {
    let db = people_db().await;

    let batches = db
        .query(
            "MATCH (n:Person) \
             RETURN n.age + 1 * 2 AS first, (n.age + 1) * 2 AS second, count(*) AS c",
        )
        .await
        .unwrap();

    assert_eq!(
        int_int_int_rows(&batches, "first", "second", "c"),
        vec![(38, 74, 2), (43, 84, 1)]
    );
}

#[tokio::test]
async fn aggregate_expression_reuses_existing_grouping_key() {
    let db = people_db().await;

    let batches = db
        .query("MATCH (n:Person) RETURN n.age AS age, n.age + count(*) AS mixed")
        .await
        .unwrap();

    assert_eq!(
        int_int_rows(&batches, "age", "mixed"),
        vec![(36, 38), (41, 42)]
    );
}

#[tokio::test]
async fn aliased_bare_node_materializes_with_alias_prefix() {
    let db = people_db().await;

    let batches = db
        .query("MATCH (n:Person) WHERE n.name = 'Ada' RETURN n AS person")
        .await
        .unwrap();

    let row_count: usize = batches.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(row_count, 1);
    assert_eq!(
        single_string_list(&batches, "person._labels"),
        vec!["Person"]
    );
    assert_eq!(int_column(&batches, "person._id"), vec![1]);
    assert_eq!(string_column(&batches, "person.name"), vec!["Ada"]);
}

#[tokio::test]
async fn union_all_empty_projected_branch_contributes_zero_rows() {
    let db = people_db().await;

    let batches = db
        .query(
            "MATCH (n:Person) WHERE n.name = 'Missing' RETURN n.name AS name
             UNION ALL
             MATCH (n:Person) RETURN n.name AS name",
        )
        .await
        .unwrap();

    assert_eq!(string_column(&batches, "name"), vec!["Ada", "Bob", "Cy"]);
}

#[tokio::test]
async fn union_same_name_and_type_allows_nullability_difference() {
    let db = people_db().await;

    let batches = db
        .query(
            "MATCH (n:Person) RETURN n.age AS x
             UNION
             MATCH (n:Person) RETURN 1 AS x",
        )
        .await
        .unwrap();

    assert_eq!(int_column(&batches, "x"), vec![1, 36, 41]);
}
