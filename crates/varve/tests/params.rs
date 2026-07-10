#![allow(clippy::unwrap_used)]

use arrow::array::{Array, Int64Array, StringArray};
use std::collections::BTreeMap;
use varve::Db;
use varve_types::Value;

fn strings(batches: &[varve::RecordBatch], col: &str) -> Vec<String> {
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

fn ints(batches: &[varve::RecordBatch], col: &str) -> Vec<i64> {
    let mut out: Vec<i64> = batches
        .iter()
        .flat_map(|batch| {
            let col = batch
                .column_by_name(col)
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            (0..col.len()).map(|idx| col.value(idx))
        })
        .collect();
    out.sort();
    out
}

fn one_param(name: &str, value: Value) -> BTreeMap<String, Value> {
    BTreeMap::from([(name.to_string(), value)])
}

#[tokio::test]
async fn param_in_where() {
    let db = Db::memory();
    db.execute("INSERT (:P {_id: 1, name: 'Ada'})")
        .await
        .unwrap();
    db.execute("INSERT (:P {_id: 2, name: 'Bob'})")
        .await
        .unwrap();

    let params = one_param("name", Value::Str("Ada".to_string()));
    let batches = db
        .query_with(
            "MATCH (n:P) WHERE n.name = $name RETURN n.name AS name",
            &params,
        )
        .await
        .unwrap();

    assert_eq!(strings(&batches, "name"), vec!["Ada"]);
}

#[tokio::test]
async fn param_in_insert_props() {
    let db = Db::memory();
    let params = one_param("n", Value::Str("Ada".to_string()));

    db.execute_with("INSERT (:P {name: $n})", &params)
        .await
        .unwrap();

    let batches = db.query("MATCH (n:P) RETURN n.name AS name").await.unwrap();
    assert_eq!(strings(&batches, "name"), vec!["Ada"]);
}

#[tokio::test]
async fn param_as_iid_fast_path() {
    let db = Db::memory();
    db.execute("INSERT (:P {_id: 1, name: 'Ada'})")
        .await
        .unwrap();
    db.execute("INSERT (:P {_id: 2, name: 'Bob'})")
        .await
        .unwrap();

    let params = one_param("id", Value::Int(1));
    let batches = db
        .query_with(
            "MATCH (n:P) WHERE n._id = $id RETURN n.name AS name",
            &params,
        )
        .await
        .unwrap();

    assert_eq!(strings(&batches, "name"), vec!["Ada"]);
}

#[tokio::test]
async fn missing_param_is_error() {
    let db = Db::memory();
    db.execute("INSERT (:P {_id: 1, name: 'Ada'})")
        .await
        .unwrap();

    let query_err = db
        .query_with(
            "MATCH (n:P) WHERE n.name = $name RETURN n.name AS name",
            &BTreeMap::new(),
        )
        .await
        .unwrap_err();
    assert!(query_err.to_string().contains("missing parameter '$name'"));

    let insert_err = db
        .execute_with("INSERT (:P {_id: 2, name: $name})", &BTreeMap::new())
        .await
        .unwrap_err();
    assert!(insert_err.to_string().contains("missing parameter '$name'"));

    let batches = db.query("MATCH (n:P) RETURN n._id AS id").await.unwrap();
    assert_eq!(ints(&batches, "id"), vec![1]);
}

#[tokio::test]
async fn param_in_return() {
    let db = Db::memory();
    db.execute("INSERT (:P {_id: 1})").await.unwrap();

    let params = one_param("value", Value::Str("hello".to_string()));
    let batches = db
        .query_with("MATCH (n:P) RETURN $value AS value", &params)
        .await
        .unwrap();

    assert_eq!(strings(&batches, "value"), vec!["hello"]);
}

#[tokio::test]
async fn byte_array_param_in_expression_is_unsupported() {
    let db = Db::memory();
    db.execute("INSERT (:P {_id: 1})").await.unwrap();
    let params = one_param("value", Value::Bytes(vec![1, 2, 3]));

    let err = db
        .query_with("MATCH (n:P) RETURN $value AS value", &params)
        .await
        .unwrap_err();

    assert!(err
        .to_string()
        .contains("byte-array parameters are not supported in expressions"));
}

#[tokio::test]
async fn unary_negation_rejects_param_operand_in_insert_props() {
    let db = Db::memory();
    let params = one_param("n", Value::Int(7));

    let err = db
        .execute_with("INSERT (:P {_id: 1, x: -$n})", &params)
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("unary negative")
            || err
                .to_string()
                .contains("property expressions must constant"),
        "{err}"
    );

    let batches = db.query("MATCH (n:P) RETURN n._id AS id").await.unwrap();
    assert_eq!(ints(&batches, "id"), Vec::<i64>::new());
}
