use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use serde_json::json;
use std::sync::Arc;

fn sample_batch_with_null() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int64, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["Ada"])),
            Arc::new(Int64Array::from(vec![None])),
        ],
    )
    .unwrap_or_else(|error| panic!("sample batch must be valid: {error}"))
}

#[test]
fn rows_include_explicit_null_fields() {
    let rows: Vec<varve::JsonRow> = varve::rows(&[sample_batch_with_null()]).unwrap().collect();
    assert_eq!(
        rows,
        vec![json!({"name": "Ada", "age": null})
            .as_object()
            .unwrap()
            .clone()]
    );
}

#[test]
fn rows_are_empty_for_empty_batches() {
    assert!(varve::rows(&[]).unwrap().next().is_none());
}
