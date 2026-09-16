//! Per-table property catalog: every property name a table has ever stored,
//! with the widest Arrow type seen for it. The lazy scan needs a fixed
//! schema before the first page is read; this is where it comes from. Kept
//! incrementally on the live tail and persisted per block as a sidecar
//! (`props/<trie>.arrow`), so a scan's schema is the union of a handful of
//! small maps, never a pass over the rows.

use crate::codec::downcast;
use crate::event::{Event, Op};
use crate::live::IndexError;
use arrow::array::{ArrayRef, StringArray, StringBuilder, UInt8Array, UInt8Builder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use std::collections::BTreeMap;
use std::sync::Arc;
use varve_types::{Doc, Value};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PropType {
    Int,
    Float,
    Str,
    Bool,
    Bytes,
    /// Two incompatible types were stored under one name. The lazy scan
    /// cannot type such a column up front, so the table falls back to the
    /// eager scan (which errors only if the conflict is visible).
    Mixed,
}

impl PropType {
    fn of(value: &Value) -> Option<PropType> {
        match value {
            Value::Int(_) => Some(PropType::Int),
            Value::Float(_) => Some(PropType::Float),
            Value::Str(_) => Some(PropType::Str),
            Value::Bool(_) => Some(PropType::Bool),
            Value::Bytes(_) => Some(PropType::Bytes),
            Value::Null | Value::List(_) => None,
        }
    }

    /// Same widening rule as the eager snapshot: Int and Float share a
    /// Float64 column; any other disagreement is a conflict.
    fn widen(self, other: PropType) -> PropType {
        match (self, other) {
            _ if self == other => self,
            (PropType::Int, PropType::Float) | (PropType::Float, PropType::Int) => PropType::Float,
            _ => PropType::Mixed,
        }
    }

    pub fn arrow_type(self) -> Option<DataType> {
        match self {
            PropType::Int => Some(DataType::Int64),
            PropType::Float => Some(DataType::Float64),
            PropType::Str => Some(DataType::Utf8),
            PropType::Bool => Some(DataType::Boolean),
            PropType::Bytes => Some(DataType::Binary),
            PropType::Mixed => None,
        }
    }

    fn tag(self) -> u8 {
        match self {
            PropType::Int => 0,
            PropType::Float => 1,
            PropType::Str => 2,
            PropType::Bool => 3,
            PropType::Bytes => 4,
            PropType::Mixed => 5,
        }
    }

    fn from_tag(tag: u8) -> Result<PropType, IndexError> {
        Ok(match tag {
            0 => PropType::Int,
            1 => PropType::Float,
            2 => PropType::Str,
            3 => PropType::Bool,
            4 => PropType::Bytes,
            5 => PropType::Mixed,
            other => {
                return Err(IndexError::Codec(format!(
                    "property catalog: unknown type tag {other}"
                )))
            }
        })
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PropSchema(BTreeMap<String, PropType>);

impl PropSchema {
    pub fn new() -> PropSchema {
        PropSchema::default()
    }

    pub fn build(rows: &[Event]) -> PropSchema {
        let mut schema = PropSchema::new();
        for event in rows {
            schema.observe_event(event);
        }
        schema
    }

    pub fn observe_event(&mut self, event: &Event) {
        if let Op::Put { doc, .. } = &event.op {
            self.observe(doc);
        }
    }

    pub fn observe(&mut self, doc: &Doc) {
        for (name, value) in doc {
            if name == "_labels" {
                continue;
            }
            let Some(ty) = PropType::of(value) else {
                continue;
            };
            self.insert(name, ty);
        }
    }

    fn insert(&mut self, name: &str, ty: PropType) {
        match self.0.get_mut(name) {
            Some(existing) => *existing = existing.widen(ty),
            None => {
                self.0.insert(name.to_string(), ty);
            }
        }
    }

    pub fn merge(&mut self, other: &PropSchema) {
        for (name, ty) in &other.0 {
            self.insert(name, *ty);
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, PropType)> {
        self.0.iter().map(|(name, ty)| (name.as_str(), *ty))
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn has_mixed(&self) -> bool {
        self.0.values().any(|ty| *ty == PropType::Mixed)
    }

    /// One nullable Arrow field per property, in name order — the column
    /// order the eager snapshot produces. `None` when a property is `Mixed`.
    pub fn fields(&self) -> Option<Vec<Field>> {
        self.0
            .iter()
            .map(|(name, ty)| ty.arrow_type().map(|dt| Field::new(name, dt, true)))
            .collect()
    }

    pub fn encode(&self) -> Result<Vec<u8>, IndexError> {
        let schema = prop_schema_schema();
        let mut buf = Vec::new();
        let mut writer = crate::codec::ipc_writer(&mut buf, &schema)?;
        if !self.0.is_empty() {
            let mut name_b = StringBuilder::new();
            let mut tag_b = UInt8Builder::new();
            for (name, ty) in &self.0 {
                name_b.append_value(name);
                tag_b.append_value(ty.tag());
            }
            let columns: Vec<ArrayRef> = vec![Arc::new(name_b.finish()), Arc::new(tag_b.finish())];
            writer.write(&RecordBatch::try_new(schema.clone(), columns)?)?;
        }
        writer.finish()?;
        drop(writer);
        Ok(buf)
    }

    pub fn decode(bytes: &[u8]) -> Result<PropSchema, IndexError> {
        crate::codec::validate_ipc_framing(bytes)?;
        match crate::codec::catch_arrow_panic(|| Self::decode_uncaught(bytes)) {
            Ok(result) => result,
            Err(_) => Err(IndexError::Codec(
                "arrow IPC decode panicked (corrupt input)".into(),
            )),
        }
    }

    fn decode_uncaught(bytes: &[u8]) -> Result<PropSchema, IndexError> {
        let reader = StreamReader::try_new(std::io::Cursor::new(bytes), None)?;
        if reader.schema() != prop_schema_schema() {
            return Err(IndexError::Codec("property catalog schema mismatch".into()));
        }
        let mut schema = PropSchema::new();
        for batch in reader {
            let batch = batch?;
            let names = downcast::<StringArray>(&batch, 0)?;
            let tags = downcast::<UInt8Array>(&batch, 1)?;
            for row in 0..batch.num_rows() {
                schema.insert(names.value(row), PropType::from_tag(tags.value(row))?);
            }
        }
        Ok(schema)
    }
}

fn prop_schema_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("type", DataType::UInt8, false),
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn doc(pairs: &[(&str, Value)]) -> Doc {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    #[test]
    fn widens_int_and_float_and_marks_conflicts() {
        let mut schema = PropSchema::new();
        schema.observe(&doc(&[
            ("score", Value::Int(1)),
            ("name", Value::Str("a".into())),
        ]));
        schema.observe(&doc(&[("score", Value::Float(1.5)), ("name", Value::Null)]));
        schema.observe(&doc(&[("flag", Value::Bool(true))]));
        let got: Vec<_> = schema.iter().collect();
        assert_eq!(
            got,
            vec![
                ("flag", PropType::Bool),
                ("name", PropType::Str),
                ("score", PropType::Float)
            ]
        );
        assert!(!schema.has_mixed());
        schema.observe(&doc(&[("name", Value::Int(3))]));
        assert!(schema.has_mixed());
        assert!(schema.fields().is_none());
    }

    #[test]
    fn labels_key_is_not_a_property() {
        let mut schema = PropSchema::new();
        schema.observe(&doc(&[("_labels", Value::Str("x".into()))]));
        assert!(schema.is_empty());
    }

    #[test]
    fn encode_decode_round_trips() {
        let mut schema = PropSchema::new();
        schema.observe(&doc(&[
            ("a", Value::Int(1)),
            ("b", Value::Bytes(vec![1])),
            ("c", Value::Str("s".into())),
        ]));
        schema.observe(&doc(&[("a", Value::Str("conflict".into()))]));
        let decoded = PropSchema::decode(&schema.encode().unwrap()).unwrap();
        assert_eq!(decoded, schema);
        let empty = PropSchema::decode(&PropSchema::new().encode().unwrap()).unwrap();
        assert!(empty.is_empty());
    }

    #[test]
    fn merge_widens_across_sources() {
        let mut left = PropSchema::new();
        left.observe(&doc(&[("n", Value::Int(1))]));
        let mut right = PropSchema::new();
        right.observe(&doc(&[("n", Value::Float(2.0)), ("m", Value::Bool(false))]));
        left.merge(&right);
        assert_eq!(
            left.iter().collect::<Vec<_>>(),
            vec![("m", PropType::Bool), ("n", PropType::Float)]
        );
    }
}
