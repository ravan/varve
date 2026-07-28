use arrow::array::{Int64Array, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
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

/// A one-column batch of `Timestamp(µs, UTC)`, mirroring the engine's temporal
/// system columns (`_valid_from`, `_valid_to`, `_system_from`, `_system_to`).
fn timestamp_batch(name: &str, micros: Vec<Option<i64>>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        name,
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        true,
    )]));
    let array = TimestampMicrosecondArray::from(micros).with_timezone("UTC");
    RecordBatch::try_new(schema, vec![Arc::new(array)])
        .unwrap_or_else(|error| panic!("timestamp batch must be valid: {error}"))
}

fn encoded(batches: &[RecordBatch]) -> Vec<varve::JsonRow> {
    varve::rows(batches)
        .unwrap_or_else(|error| panic!("rows must encode: {error}"))
        .collect()
}

/// `_valid_to` on an open-ended fact is `Instant::END_OF_TIME` (`i64::MAX` µs).
/// arrow-json cannot render it as a calendar date and writes its *error text*
/// into the stream — text containing unescaped quotes, which corrupted the whole
/// response into a JSON parse failure and a 500.
#[test]
fn rows_render_out_of_range_instants_as_raw_micros() {
    let rows = encoded(&[timestamp_batch("vt", vec![Some(i64::MAX)])]);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["vt"], json!("9223372036854775807us"));
}

#[test]
fn rows_render_the_beginning_of_time_sentinel_too() {
    let rows = encoded(&[timestamp_batch("vf", vec![Some(i64::MIN)])]);
    assert_eq!(rows[0]["vf"], json!("-9223372036854775808us"));
}

/// The fix must not reformat instants that already worked: sub-second precision
/// is kept, and whole seconds stay free of a `.000000` tail.
#[test]
fn rows_keep_renderable_instants_byte_identical() {
    let rows = encoded(&[timestamp_batch(
        "vf",
        vec![Some(1_639_131_300_000_000), Some(1_785_260_790_464_335)],
    )]);
    assert_eq!(rows[0]["vf"], json!("2021-12-10T10:15:00Z"));
    assert_eq!(rows[1]["vf"], json!("2026-07-28T17:46:30.464335Z"));
}

#[test]
fn rows_mix_sentinels_and_dates_in_one_column() {
    let rows = encoded(&[timestamp_batch(
        "vt",
        vec![Some(1_639_131_300_000_000), Some(i64::MAX), None],
    )]);
    assert_eq!(rows[0]["vt"], json!("2021-12-10T10:15:00Z"));
    assert_eq!(rows[1]["vt"], json!("9223372036854775807us"));
    assert_eq!(rows[2]["vt"], json!(null), "explicit nulls still survive");
}

/// Rows are patched by their position in the concatenated output, so a sentinel
/// in a later batch must not rewrite a row from an earlier one.
#[test]
fn rows_patch_the_right_row_across_batches() {
    let rows = encoded(&[
        timestamp_batch("vt", vec![Some(1_639_131_300_000_000)]),
        timestamp_batch("vt", vec![Some(i64::MAX), Some(1_639_131_300_000_000)]),
    ]);
    assert_eq!(rows[0]["vt"], json!("2021-12-10T10:15:00Z"));
    assert_eq!(rows[1]["vt"], json!("9223372036854775807us"));
    assert_eq!(rows[2]["vt"], json!("2021-12-10T10:15:00Z"));
}

#[test]
fn rows_leave_other_columns_untouched_beside_a_sentinel() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new(
            "vt",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["Ada"])),
            Arc::new(TimestampMicrosecondArray::from(vec![Some(i64::MAX)]).with_timezone("UTC")),
        ],
    )
    .unwrap();

    let rows = encoded(&[batch]);
    assert_eq!(rows[0]["name"], json!("Ada"));
    assert_eq!(rows[0]["vt"], json!("9223372036854775807us"));
}

/// The defect as a client hit it: `valid_to(x)` over a real store, encoded the
/// way the HTTP layer encodes it. Before the fix this failed with
/// `expected ',' or '}'`, surfacing as `{"code":"internal"}`.
#[tokio::test]
async fn valid_to_of_an_open_ended_fact_encodes_over_a_real_store() {
    let db = varve::Db::memory();
    db.execute("INSERT (:Person {_id: 1, name: 'Ada'})")
        .await
        .unwrap();

    let batches = db
        .query("MATCH (p:Person) RETURN p.name AS name, valid_from(p) AS vf, valid_to(p) AS vt")
        .await
        .unwrap();
    let rows = encoded(&batches);

    assert_eq!(rows[0]["name"], json!("Ada"));
    assert_eq!(rows[0]["vt"], json!("9223372036854775807us"));
    let vf = rows[0]["vf"]
        .as_str()
        .expect("valid_from is a calendar date");
    assert!(vf.ends_with('Z'), "valid_from must stay RFC 3339: {vf}");
}
