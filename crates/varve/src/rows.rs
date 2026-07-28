use arrow_json::writer::{JsonArray, WriterBuilder};
use datafusion::arrow::array::{Array, TimestampMicrosecondArray};
use datafusion::arrow::datatypes::{DataType, Schema, TimeUnit};
use serde_json::Value;
use std::sync::Arc;
use varve_types::Instant;

use crate::RecordBatch;

pub type JsonRow = serde_json::Map<String, Value>;

#[derive(Debug, thiserror::Error)]
pub enum RowError {
    #[error(transparent)]
    Arrow(#[from] datafusion::arrow::error::ArrowError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

pub struct RowIter {
    inner: std::vec::IntoIter<JsonRow>,
}

impl Iterator for RowIter {
    type Item = JsonRow;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

/// One cell arrow-json cannot render, keyed by its position in the concatenated
/// output, plus the text to write there instead.
struct InstantOverride {
    row: usize,
    column: String,
    rendered: String,
}

pub fn rows(batches: &[RecordBatch]) -> Result<RowIter, RowError> {
    let (batches, overrides) = mask_unrenderable_instants(batches)?;

    let mut bytes = Vec::new();
    let mut writer = WriterBuilder::new()
        .with_explicit_nulls(true)
        .build::<_, JsonArray>(&mut bytes);
    let batches = batches.iter().collect::<Vec<_>>();
    writer.write_batches(&batches)?;
    writer.finish()?;
    let mut rows = serde_json::from_slice::<Vec<JsonRow>>(&bytes)?;

    for InstantOverride {
        row,
        column,
        rendered,
    } in overrides
    {
        debug_assert!(row < rows.len(), "override row must exist in the output");
        if let Some(target) = rows.get_mut(row) {
            target.insert(column, Value::String(rendered));
        }
    }

    Ok(RowIter {
        inner: rows.into_iter(),
    })
}

/// Nulls out µs instants no calendar can render and reports what belonged there.
///
/// Handed `Instant::END_OF_TIME` — the `_valid_to` / `_system_to` of every
/// open-ended fact — arrow-json writes its *error text* into the stream rather
/// than failing, and that text carries unescaped quotes, so the whole document
/// stops being JSON. Masking the cell keeps arrow on the path it renders
/// correctly and leaves the sentinel to `Instant`'s own raw-µs spelling, the
/// same one `TxResponse` uses.
///
/// Only `Timestamp(µs)` is inspected: that is the sole timestamp Varve stores
/// (Global Constraint: Timestamp(µs, UTC)), so it is the only column where a
/// sentinel can appear.
fn mask_unrenderable_instants(
    batches: &[RecordBatch],
) -> Result<(Vec<RecordBatch>, Vec<InstantOverride>), RowError> {
    let mut masked = Vec::with_capacity(batches.len());
    let mut overrides = Vec::new();
    let mut row_offset = 0;

    for batch in batches {
        let mut columns = batch.columns().to_vec();
        let mut fields = batch.schema().fields().to_vec();
        let mut rewritten = false;

        for (index, column) in batch.columns().iter().enumerate() {
            if !matches!(
                column.data_type(),
                DataType::Timestamp(TimeUnit::Microsecond, _)
            ) {
                continue;
            }
            let Some(instants) = column.as_any().downcast_ref::<TimestampMicrosecondArray>() else {
                continue;
            };

            let mut kept = Vec::with_capacity(instants.len());
            let mut masked_any = false;
            for row in 0..instants.len() {
                if instants.is_null(row) {
                    kept.push(None);
                    continue;
                }
                let instant = Instant::from_micros(instants.value(row));
                if instant.is_calendar_renderable() {
                    kept.push(Some(instant.as_micros()));
                    continue;
                }
                kept.push(None);
                masked_any = true;
                overrides.push(InstantOverride {
                    row: row_offset + row,
                    column: batch.schema().field(index).name().clone(),
                    rendered: instant.to_string(),
                });
            }
            if !masked_any {
                continue;
            }

            let timezone = match column.data_type() {
                DataType::Timestamp(_, timezone) => timezone.clone(),
                _ => None,
            };
            columns[index] =
                Arc::new(TimestampMicrosecondArray::from(kept).with_timezone_opt(timezone));
            // A masked cell is null, which a non-nullable field would reject.
            fields[index] = Arc::new(batch.schema().field(index).clone().with_nullable(true));
            rewritten = true;
        }

        row_offset += batch.num_rows();
        masked.push(if rewritten {
            RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?
        } else {
            batch.clone()
        });
    }

    Ok((masked, overrides))
}
