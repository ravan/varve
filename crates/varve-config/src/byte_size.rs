use serde::de::Visitor;
use serde::{Deserialize, Deserializer};
use std::fmt;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ByteSize(usize);

impl ByteSize {
    pub const fn from_bytes(bytes: usize) -> ByteSize {
        ByteSize(bytes)
    }

    pub const fn as_usize(self) -> usize {
        self.0
    }
}

fn parse(input: &str) -> Result<ByteSize, String> {
    let (digits, multiplier) = [
        ("GiB", 1024usize.pow(3)),
        ("MiB", 1024usize.pow(2)),
        ("KiB", 1024usize),
        ("B", 1usize),
    ]
    .into_iter()
    .find_map(|(suffix, multiplier)| input.strip_suffix(suffix).map(|d| (d, multiplier)))
    .ok_or_else(|| "byte size must end in B, KiB, MiB, or GiB".to_string())?;
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err("byte size must contain an unsigned integer followed by an IEC unit".into());
    }
    let amount = digits
        .parse::<usize>()
        .map_err(|error| format!("invalid byte-size integer: {error}"))?;
    amount
        .checked_mul(multiplier)
        .map(ByteSize)
        .ok_or_else(|| "byte size overflows usize".to_string())
}

struct ByteSizeVisitor;

impl Visitor<'_> for ByteSizeVisitor {
    type Value = ByteSize;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a quoted IEC byte size such as 8MiB")
    }

    fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Self::Value, E> {
        parse(value).map_err(E::custom)
    }
}

impl<'de> Deserialize<'de> for ByteSize {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_str(ByteSizeVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[derive(Deserialize)]
    struct Tuning {
        size: ByteSize,
    }

    fn parse(value: &str) -> Result<usize, toml::de::Error> {
        toml::from_str::<Tuning>(&format!("size = {value}")).map(|tuning| tuning.size.as_usize())
    }

    #[test]
    fn parses_exact_iec_units() {
        assert_eq!(parse("\"0B\"").unwrap(), 0);
        assert_eq!(parse("\"8KiB\"").unwrap(), 8 * 1024);
        assert_eq!(parse("\"8MiB\"").unwrap(), 8 * 1024 * 1024);
        assert_eq!(parse("\"2GiB\"").unwrap(), 2 * 1024 * 1024 * 1024);
    }

    #[test]
    fn rejects_numeric_and_ambiguous_forms() {
        for value in ["8388608", "\"8MB\"", "\"8 MiB\"", "\"1.5GiB\"", "\"MiB\""] {
            assert!(parse(value).is_err(), "{value} must be rejected");
        }
    }

    #[test]
    fn rejects_overflow() {
        assert!(parse("\"18446744073709551615GiB\"").is_err());
    }
}
