use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, FixedSizeBinaryBuilder, Float64Builder, Int64Builder,
    StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::collections::BTreeMap;
use std::sync::Arc;
use thiserror::Error;
use varve_types::{Doc, Iid, Value};

#[derive(Debug, Error)]
pub enum IndexError {
    #[error(
        "property '{property}' has mixed types across rows (v0 limitation, lifted in slice 2)"
    )]
    MixedPropertyTypes { property: String },
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
}

struct NodeRow {
    iid: Iid,
    labels: Vec<String>,
    doc: Doc,
}

/// Live, in-memory index of appended node rows. v0: no temporal versioning —
/// overwrite semantics arrive in slice 2. Produces per-label Arrow snapshots
/// with a schema inferred from observed property names/types (spec §10).
#[derive(Default)]
pub struct LiveTable {
    rows: Vec<NodeRow>,
}

/// Maps an observed property value to its Arrow column type. `Value::Null`
/// carries no type information (returns `None`, so it doesn't constrain the
/// column); `Value::Bytes` maps to `Binary`, not `MixedPropertyTypes`.
fn value_type(v: &Value) -> Option<DataType> {
    match v {
        Value::Int(_) => Some(DataType::Int64),
        Value::Float(_) => Some(DataType::Float64),
        Value::Str(_) => Some(DataType::Utf8),
        Value::Bool(_) => Some(DataType::Boolean),
        Value::Bytes(_) => Some(DataType::Binary),
        Value::Null => None,
    }
}

impl LiveTable {
    pub fn new() -> Self {
        Self::default()
    }

    /// Append a node row. v0: overwrite semantics ignored (temporal arrives slice 2).
    pub fn append(&mut self, iid: Iid, labels: Vec<String>, doc: Doc) -> Result<(), IndexError> {
        self.rows.push(NodeRow { iid, labels, doc });
        Ok(())
    }

    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    /// Snapshot rows carrying `label` into one RecordBatch.
    /// Schema: `_iid` FixedSizeBinary(16) + one nullable column per property name
    /// observed across matching rows (Int64|Float64|Utf8|Boolean|Binary by first non-null).
    /// Returns `None` when no rows match.
    pub fn snapshot_for_label(&self, label: &str) -> Result<Option<RecordBatch>, IndexError> {
        let rows: Vec<&NodeRow> = self
            .rows
            .iter()
            .filter(|r| r.labels.iter().any(|l| l == label))
            .collect();
        if rows.is_empty() {
            return Ok(None);
        }

        // Column plan: property name -> type of first non-null value; conflicts are v0 errors.
        let mut col_types: BTreeMap<&str, DataType> = BTreeMap::new();
        for row in &rows {
            for (k, v) in &row.doc {
                if let Some(dt) = value_type(v) {
                    match col_types.get(k.as_str()) {
                        None => {
                            col_types.insert(k, dt);
                        }
                        Some(existing) if *existing == dt => {}
                        Some(_) => {
                            return Err(IndexError::MixedPropertyTypes {
                                property: k.clone(),
                            })
                        }
                    }
                }
            }
        }

        let mut fields = vec![Field::new("_iid", DataType::FixedSizeBinary(16), false)];
        let mut iid_b = FixedSizeBinaryBuilder::new(16);
        for row in &rows {
            iid_b.append_value(row.iid.as_bytes())?;
        }
        let mut columns: Vec<ArrayRef> = vec![Arc::new(iid_b.finish())];

        for (name, dt) in &col_types {
            fields.push(Field::new(*name, dt.clone(), true));
            let col: ArrayRef = match dt {
                DataType::Int64 => {
                    let mut b = Int64Builder::new();
                    for row in &rows {
                        match row.doc.get(*name) {
                            Some(Value::Int(i)) => b.append_value(*i),
                            _ => b.append_null(),
                        }
                    }
                    Arc::new(b.finish())
                }
                DataType::Float64 => {
                    let mut b = Float64Builder::new();
                    for row in &rows {
                        match row.doc.get(*name) {
                            Some(Value::Float(f)) => b.append_value(*f),
                            _ => b.append_null(),
                        }
                    }
                    Arc::new(b.finish())
                }
                DataType::Utf8 => {
                    let mut b = StringBuilder::new();
                    for row in &rows {
                        match row.doc.get(*name) {
                            Some(Value::Str(s)) => b.append_value(s),
                            _ => b.append_null(),
                        }
                    }
                    Arc::new(b.finish())
                }
                DataType::Boolean => {
                    let mut b = BooleanBuilder::new();
                    for row in &rows {
                        match row.doc.get(*name) {
                            Some(Value::Bool(v)) => b.append_value(*v),
                            _ => b.append_null(),
                        }
                    }
                    Arc::new(b.finish())
                }
                _ => {
                    let mut b = BinaryBuilder::new();
                    for row in &rows {
                        match row.doc.get(*name) {
                            Some(Value::Bytes(v)) => b.append_value(v),
                            _ => b.append_null(),
                        }
                    }
                    Arc::new(b.finish())
                }
            };
            columns.push(col);
        }

        Ok(Some(RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            columns,
        )?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int64Array, StringArray};
    use varve_types::{Doc, Iid, Value};

    fn doc(pairs: &[(&str, Value)]) -> Doc {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    fn iid(n: u8) -> Iid {
        Iid::derive("g", "nodes", &[n])
    }

    #[test]
    fn snapshot_builds_columns_from_observed_props() {
        let mut t = LiveTable::new();
        t.append(
            iid(1),
            vec!["Person".into()],
            doc(&[("name", Value::Str("Ada".into())), ("age", Value::Int(36))]),
        )
        .unwrap();
        t.append(
            iid(2),
            vec!["Person".into()],
            doc(&[("name", Value::Str("Bob".into()))]),
        )
        .unwrap();

        let batch = t.snapshot_for_label("Person").unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);
        let names: &StringArray = batch
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert_eq!(names.value(0), "Ada");
        let ages: &Int64Array = batch
            .column_by_name("age")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert_eq!(ages.value(0), 36);
        assert!(ages.is_null(1)); // Bob has no age
    }

    #[test]
    fn label_filtering() {
        let mut t = LiveTable::new();
        t.append(
            iid(1),
            vec!["Person".into()],
            doc(&[("name", Value::Str("Ada".into()))]),
        )
        .unwrap();
        t.append(
            iid(2),
            vec!["City".into()],
            doc(&[("name", Value::Str("Oslo".into()))]),
        )
        .unwrap();
        assert_eq!(
            t.snapshot_for_label("Person").unwrap().unwrap().num_rows(),
            1
        );
        assert!(t.snapshot_for_label("Robot").unwrap().is_none());
    }

    #[test]
    fn mixed_property_types_rejected_v0() {
        let mut t = LiveTable::new();
        t.append(iid(1), vec!["P".into()], doc(&[("x", Value::Int(1))]))
            .unwrap();
        t.append(
            iid(2),
            vec!["P".into()],
            doc(&[("x", Value::Str("one".into()))]),
        )
        .unwrap();
        assert!(matches!(
            t.snapshot_for_label("P"),
            Err(IndexError::MixedPropertyTypes { .. })
        ));
    }
}
