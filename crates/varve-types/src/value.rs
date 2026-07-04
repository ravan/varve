use crate::position::TypeError;
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    Bytes(Vec<u8>),
}

/// Property document. BTreeMap for deterministic iteration order.
pub type Doc = BTreeMap<String, Value>;

impl Value {
    /// Canonical bytes for IID derivation (type-tagged to avoid cross-type collisions).
    pub fn id_bytes(&self) -> Result<Vec<u8>, TypeError> {
        match self {
            Value::Int(i) => {
                let mut b = vec![0x01];
                b.extend_from_slice(&i.to_be_bytes());
                Ok(b)
            }
            Value::Str(s) => {
                let mut b = vec![0x02];
                b.extend_from_slice(s.as_bytes());
                Ok(b)
            }
            Value::Bytes(bytes) => {
                let mut b = vec![0x03];
                b.extend_from_slice(bytes);
                Ok(b)
            }
            Value::Bool(v) => Ok(vec![0x04, *v as u8]),
            other => Err(TypeError::InvalidId(format!("{other:?}"))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn int_and_str_ids_do_not_collide() {
        // Int(49) vs Str("1"): '1' is byte 0x31 == 49 — tags must disambiguate
        let i = Value::Int(0x31).id_bytes().unwrap();
        let s = Value::Str("1".into()).id_bytes().unwrap();
        assert_ne!(i, s);
    }

    #[test]
    fn id_bytes_deterministic() {
        assert_eq!(
            Value::Str("ada".into()).id_bytes().unwrap(),
            Value::Str("ada".into()).id_bytes().unwrap()
        );
    }

    #[test]
    fn float_and_null_rejected_as_ids() {
        assert!(Value::Float(1.0).id_bytes().is_err());
        assert!(Value::Null.id_bytes().is_err());
    }
}
