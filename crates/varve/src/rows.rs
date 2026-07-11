use arrow_json::writer::{JsonArray, WriterBuilder};
use serde_json::Value;

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

pub fn rows(batches: &[RecordBatch]) -> Result<RowIter, RowError> {
    let mut bytes = Vec::new();
    let mut writer = WriterBuilder::new()
        .with_explicit_nulls(true)
        .build::<_, JsonArray>(&mut bytes);
    let batches = batches.iter().collect::<Vec<_>>();
    writer.write_batches(&batches)?;
    writer.finish()?;
    let rows = serde_json::from_slice::<Vec<JsonRow>>(&bytes)?;
    Ok(RowIter {
        inner: rows.into_iter(),
    })
}
