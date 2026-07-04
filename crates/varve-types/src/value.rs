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

// Canonical byte-encoding tags for log wire format (unrelated to id_bytes() tags).
const TAG_NULL: u8 = 0x00;
const TAG_BOOL: u8 = 0x01;
const TAG_INT: u8 = 0x02;
const TAG_FLOAT: u8 = 0x03;
const TAG_STR: u8 = 0x04;
const TAG_BYTES: u8 = 0x05;

fn take<'a>(input: &mut &'a [u8], n: usize) -> Result<&'a [u8], TypeError> {
    if input.len() < n {
        return Err(TypeError::MalformedEncoding(format!(
            "need {n} bytes, have {}",
            input.len()
        )));
    }
    let (head, rest) = input.split_at(n);
    *input = rest;
    Ok(head)
}

fn read_u32(input: &mut &[u8]) -> Result<u32, TypeError> {
    let b = take(input, 4)?;
    let arr: [u8; 4] = b
        .try_into()
        .map_err(|_| TypeError::MalformedEncoding("u32".into()))?;
    Ok(u32::from_le_bytes(arr))
}

fn write_len_prefixed(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    out.extend_from_slice(bytes);
}

impl Value {
    /// Canonical bytes for IID derivation (type-tagged to avoid cross-type collisions).
    /// NOTE: these tags (0x01–0x04) are unrelated to the canonical wire-format tags
    /// (0x00–0x05) used by encode_into/decode_from. IID and codec are independent.
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
            other @ (Value::Float(_) | Value::Null) => {
                Err(TypeError::InvalidId(format!("{other:?}")))
            }
        }
    }

    /// Appends value's canonical byte encoding (deterministic; log
    /// wire format stored property values).
    pub fn encode_into(&self, out: &mut Vec<u8>) {
        match self {
            Value::Null => out.push(TAG_NULL),
            Value::Bool(b) => {
                out.push(TAG_BOOL);
                out.push(*b as u8);
            }
            Value::Int(i) => {
                out.push(TAG_INT);
                out.extend_from_slice(&i.to_le_bytes());
            }
            Value::Float(f) => {
                out.push(TAG_FLOAT);
                out.extend_from_slice(&f.to_le_bytes());
            }
            Value::Str(s) => {
                out.push(TAG_STR);
                write_len_prefixed(out, s.as_bytes());
            }
            Value::Bytes(bytes) => {
                out.push(TAG_BYTES);
                write_len_prefixed(out, bytes);
            }
        }
    }

    /// Consumes exactly one canonically encoded value from `input`.
    pub fn decode_from(input: &mut &[u8]) -> Result<Value, TypeError> {
        let tag = take(input, 1)?[0];
        match tag {
            TAG_NULL => Ok(Value::Null),
            TAG_BOOL => {
                let b = take(input, 1)?[0];
                match b {
                    0 => Ok(Value::Bool(false)),
                    1 => Ok(Value::Bool(true)),
                    _ => Err(TypeError::MalformedEncoding(format!("bool byte {b:#04x}"))),
                }
            }
            TAG_INT => {
                let b = take(input, 8)?;
                let arr: [u8; 8] = b
                    .try_into()
                    .map_err(|_| TypeError::MalformedEncoding("i64".into()))?;
                Ok(Value::Int(i64::from_le_bytes(arr)))
            }
            TAG_FLOAT => {
                let b = take(input, 8)?;
                let arr: [u8; 8] = b
                    .try_into()
                    .map_err(|_| TypeError::MalformedEncoding("f64".into()))?;
                Ok(Value::Float(f64::from_le_bytes(arr)))
            }
            TAG_STR => {
                let len = read_u32(input)? as usize;
                let s = std::str::from_utf8(take(input, len)?)
                    .map_err(|e| TypeError::MalformedEncoding(format!("string not UTF-8: {e}")))?;
                Ok(Value::Str(s.to_string()))
            }
            TAG_BYTES => {
                let len = read_u32(input)? as usize;
                Ok(Value::Bytes(take(input, len)?.to_vec()))
            }
            other => Err(TypeError::MalformedEncoding(format!(
                "unknown tag {other:#04x}"
            ))),
        }
    }
}

/// Canonical byte encoding of whole document (deterministic — `Doc` is
/// BTreeMap, so iteration order is fixed).
pub fn encode_doc(doc: &Doc) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&(doc.len() as u32).to_le_bytes());
    for (k, v) in doc {
        write_len_prefixed(&mut out, k.as_bytes());
        v.encode_into(&mut out);
    }
    out
}

/// Consumes exactly one canonically encoded document from `input`.
pub fn decode_doc(input: &mut &[u8]) -> Result<Doc, TypeError> {
    let count = read_u32(input)?;
    let mut doc = Doc::new();
    for _ in 0..count {
        let klen = read_u32(input)? as usize;
        let key = std::str::from_utf8(take(input, klen)?)
            .map_err(|e| TypeError::MalformedEncoding(format!("doc key not UTF-8: {e}")))?
            .to_string();
        doc.insert(key, Value::decode_from(input)?);
    }
    Ok(doc)
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

    #[test]
    fn same_length_encodings_do_not_collide() {
        // Int encodes to 9 bytes (tag + BE i64); an 8-byte string also encodes
        // to 9 bytes with an IDENTICAL payload — only the tag disambiguates.
        let i = Value::Int(0x3132_3334_3536_3738).id_bytes().unwrap(); // BE bytes == b"12345678"
        let s = Value::Str("12345678".into()).id_bytes().unwrap();
        assert_eq!(i.len(), s.len());
        assert_ne!(i, s);
    }

    #[test]
    fn doc_codec_round_trips_every_variant() {
        let mut doc = Doc::new();
        doc.insert("b".into(), Value::Bool(true));
        doc.insert("by".into(), Value::Bytes(vec![0, 255, 7]));
        doc.insert("f".into(), Value::Float(-2.5));
        doc.insert("i".into(), Value::Int(-42));
        doc.insert("n".into(), Value::Null);
        doc.insert("s".into(), Value::Str("héllo".into()));
        let bytes = encode_doc(&doc);
        let mut input = bytes.as_slice();
        assert_eq!(decode_doc(&mut input).unwrap(), doc);
        assert!(input.is_empty(), "decode must consume the whole encoding");
    }

    #[test]
    fn doc_codec_golden_bytes() {
        // Pins the exact on-disk canonical encoding (like slice 0's Iid golden
        // vector). Changing this output is a conscious breaking format change.
        let mut doc = Doc::new();
        doc.insert("a".into(), Value::Int(7));
        doc.insert("b".into(), Value::Str("hi".into()));
        assert_eq!(
            encode_doc(&doc),
            vec![
                2, 0, 0, 0, // entry count
                1, 0, 0, 0, b'a', // key "a"
                0x02, 7, 0, 0, 0, 0, 0, 0, 0, // Int(7), LE
                1, 0, 0, 0, b'b', // key "b"
                0x04, 2, 0, 0, 0, b'h', b'i', // Str("hi")
            ]
        );
    }

    #[test]
    fn float_nan_round_trips_bit_exactly() {
        let mut out = Vec::new();
        Value::Float(f64::NAN).encode_into(&mut out);
        let mut input = out.as_slice();
        let Value::Float(f) = Value::decode_from(&mut input).unwrap() else {
            panic!("expected Float");
        };
        assert_eq!(f.to_bits(), f64::NAN.to_bits());
    }

    #[test]
    fn truncated_and_garbage_inputs_error_cleanly() {
        // truncated payload
        let mut out = Vec::new();
        Value::Str("abcdef".into()).encode_into(&mut out);
        let mut short = &out[..out.len() - 2];
        assert!(matches!(
            Value::decode_from(&mut short),
            Err(TypeError::MalformedEncoding(_))
        ));
        // unknown tag
        let mut bad = &[0x7F_u8][..];
        assert!(matches!(
            Value::decode_from(&mut bad),
            Err(TypeError::MalformedEncoding(_))
        ));
        // non-UTF-8 string payload
        let mut bad_utf8 = &[0x04, 1, 0, 0, 0, 0xFF][..];
        assert!(matches!(
            Value::decode_from(&mut bad_utf8),
            Err(TypeError::MalformedEncoding(_))
        ));
        // truncated doc (claims 1 entry, has none)
        let mut bad_doc = &[1, 0, 0, 0][..];
        assert!(matches!(
            decode_doc(&mut bad_doc),
            Err(TypeError::MalformedEncoding(_))
        ));
    }
}
