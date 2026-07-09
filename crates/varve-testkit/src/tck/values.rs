use std::collections::{BTreeMap, BTreeSet};

use arrow::array::{
    Array, BooleanArray, FixedSizeBinaryArray, Float64Array, Int64Array, LargeStringArray,
    ListArray, MapArray, StringArray, StructArray,
};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq)]
pub enum TckValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    List(Vec<TckValue>),
    Map(BTreeMap<String, TckValue>),
    Node {
        labels: Vec<String>,
        props: BTreeMap<String, TckValue>,
    },
    Rel {
        typ: String,
        props: BTreeMap<String, TckValue>,
    },
    Path(Vec<TckValue>),
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ValueError {
    #[error("at byte {offset}: {message}")]
    Syntax { offset: usize, message: String },
}

pub fn parse_value(s: &str) -> Result<TckValue, ValueError> {
    let mut parser = Parser::new(s);
    let value = parser.parse_value()?;
    parser.skip_ws();
    if parser.is_eof() {
        Ok(value)
    } else {
        parser.syntax("trailing input".to_string())
    }
}

pub fn compare_results(
    expected_header: &[String],
    expected_rows: &[Vec<TckValue>],
    actual: &[RecordBatch],
    ordered: bool,
) -> Result<(), String> {
    for (idx, row) in expected_rows.iter().enumerate() {
        if row.len() != expected_header.len() {
            return Err(format!(
                "expected row {idx} has {} cells for {} headers",
                row.len(),
                expected_header.len()
            ));
        }
    }

    let mut actual_rows = Vec::new();
    for batch in actual {
        validate_actual_schema(expected_header, batch)?;
        for row in 0..batch.num_rows() {
            let mut values = Vec::with_capacity(expected_header.len());
            for header in expected_header {
                values.push(actual_cell(batch, row, header)?);
            }
            actual_rows.push(values);
        }
    }

    if expected_rows.len() != actual_rows.len() {
        return Err(format!(
            "row count differs: expected {}, actual {}",
            expected_rows.len(),
            actual_rows.len()
        ));
    }

    if ordered {
        for (idx, (expected, actual)) in expected_rows.iter().zip(actual_rows.iter()).enumerate() {
            let expected_key = canonical_row(expected);
            let actual_key = canonical_row(actual);
            if expected_key != actual_key {
                return Err(format!(
                    "ordered row {idx} differs: expected {expected_key}, actual {actual_key}"
                ));
            }
        }
        return Ok(());
    }

    let expected_counts = multiset(expected_rows);
    let actual_counts = multiset(&actual_rows);
    if expected_counts == actual_counts {
        Ok(())
    } else {
        Err(format!(
            "unordered rows differ: expected {:?}, actual {:?}",
            expected_counts, actual_counts
        ))
    }
}

fn validate_actual_schema(expected_header: &[String], batch: &RecordBatch) -> Result<(), String> {
    let schema = batch.schema();
    let mut consumed = BTreeSet::new();

    for header in expected_header {
        let labels_name = format!("{header}._labels");
        if schema.column_with_name(&labels_name).is_some() {
            let prefix = format!("{header}.");
            for field in schema.fields() {
                let name = field.name();
                if name.starts_with(&prefix) {
                    consumed.insert(name.to_string());
                }
            }
        } else if schema.column_with_name(header).is_some() {
            consumed.insert(header.clone());
        } else {
            return Err(format!("actual column `{header}` not found"));
        }
    }

    for field in schema.fields() {
        if !consumed.contains(field.name()) {
            return Err(format!("unexpected actual column `{}`", field.name()));
        }
    }

    Ok(())
}

struct Parser<'a> {
    input: &'a str,
    pos: usize,
}

impl<'a> Parser<'a> {
    fn new(input: &'a str) -> Self {
        Self { input, pos: 0 }
    }

    fn parse_value(&mut self) -> Result<TckValue, ValueError> {
        self.skip_ws();

        if self.consume_keyword("null") {
            return Ok(TckValue::Null);
        }
        if self.consume_keyword("true") {
            return Ok(TckValue::Bool(true));
        }
        if self.consume_keyword("false") {
            return Ok(TckValue::Bool(false));
        }
        if self.consume_keyword("NaN") {
            return Ok(TckValue::Float(f64::NAN));
        }

        match self.peek_char() {
            Some('\'') => self.parse_string().map(TckValue::Str),
            Some('[') if self.peek_non_ws_after('[') == Some(':') => self.parse_rel(),
            Some('[') => self.parse_list(),
            Some('{') => self.parse_map().map(TckValue::Map),
            Some('(') => self.parse_node(),
            Some('<') => self.parse_path(),
            Some('-') | Some('0'..='9') => self.parse_number(),
            Some(ch) => self.syntax(format!("unexpected character `{ch}`")),
            None => self.syntax("empty value".to_string()),
        }
    }

    fn parse_list(&mut self) -> Result<TckValue, ValueError> {
        self.expect_char('[')?;
        let mut values = Vec::new();
        self.skip_ws();
        if self.consume_char_if(']') {
            return Ok(TckValue::List(values));
        }

        loop {
            values.push(self.parse_value()?);
            self.skip_ws();
            if self.consume_char_if(',') {
                continue;
            }
            self.expect_char(']')?;
            return Ok(TckValue::List(values));
        }
    }

    fn parse_map(&mut self) -> Result<BTreeMap<String, TckValue>, ValueError> {
        self.expect_char('{')?;
        let mut values = BTreeMap::new();
        self.skip_ws();
        if self.consume_char_if('}') {
            return Ok(values);
        }

        loop {
            let key = self.parse_key()?;
            self.skip_ws();
            self.expect_char(':')?;
            let value = self.parse_value()?;
            values.insert(key, value);
            self.skip_ws();
            if self.consume_char_if(',') {
                continue;
            }
            self.expect_char('}')?;
            return Ok(values);
        }
    }

    fn parse_node(&mut self) -> Result<TckValue, ValueError> {
        self.expect_char('(')?;
        let mut labels = Vec::new();
        loop {
            self.skip_ws();
            if !self.consume_char_if(':') {
                break;
            }
            labels.push(self.parse_identifier()?);
        }

        self.skip_ws();
        let props = if self.peek_char() == Some('{') {
            self.parse_map()?
        } else {
            BTreeMap::new()
        };
        self.skip_ws();
        self.expect_char(')')?;
        Ok(TckValue::Node { labels, props })
    }

    fn parse_rel(&mut self) -> Result<TckValue, ValueError> {
        self.expect_char('[')?;
        self.skip_ws();
        self.expect_char(':')?;
        let typ = self.parse_identifier()?;
        self.skip_ws();
        let props = if self.peek_char() == Some('{') {
            self.parse_map()?
        } else {
            BTreeMap::new()
        };
        self.skip_ws();
        self.expect_char(']')?;
        Ok(TckValue::Rel { typ, props })
    }

    fn parse_path(&mut self) -> Result<TckValue, ValueError> {
        self.expect_char('<')?;
        let mut values = Vec::new();
        values.push(self.parse_node()?);

        loop {
            self.skip_ws();
            if self.consume_char_if('>') {
                return Ok(TckValue::Path(values));
            }

            if self.consume_char_if('-') {
                values.push(self.parse_rel()?);
                self.skip_ws();
                self.expect_char('-')?;
                self.skip_ws();
                self.expect_char('>')?;
            } else if self.consume_char_if('<') {
                self.skip_ws();
                self.expect_char('-')?;
                values.push(self.parse_rel()?);
                self.skip_ws();
                self.expect_char('-')?;
            } else {
                return self.syntax("expected path relationship or `>`".to_string());
            }

            values.push(self.parse_node()?);
        }
    }

    fn parse_string(&mut self) -> Result<String, ValueError> {
        self.expect_char('\'')?;
        let mut value = String::new();

        while let Some(ch) = self.consume_char() {
            match ch {
                '\'' => {
                    if self.consume_char_if('\'') {
                        value.push('\'');
                    } else {
                        return Ok(value);
                    }
                }
                '\\' => match self.consume_char() {
                    Some('n') => value.push('\n'),
                    Some('r') => value.push('\r'),
                    Some('t') => value.push('\t'),
                    Some('\\') => value.push('\\'),
                    Some('\'') => value.push('\''),
                    Some(other) => value.push(other),
                    None => return self.syntax("unterminated string escape".to_string()),
                },
                other => value.push(other),
            }
        }

        self.syntax("unterminated string".to_string())
    }

    fn parse_number(&mut self) -> Result<TckValue, ValueError> {
        let start = self.pos;
        self.consume_char_if('-');
        let mut digits = 0usize;
        while matches!(self.peek_char(), Some('0'..='9')) {
            self.consume_char();
            digits += 1;
        }
        if digits == 0 {
            return self.syntax("number has no digits".to_string());
        }

        let mut is_float = false;
        if self.consume_char_if('.') {
            is_float = true;
            let mut fraction_digits = 0usize;
            while matches!(self.peek_char(), Some('0'..='9')) {
                self.consume_char();
                fraction_digits += 1;
            }
            if fraction_digits == 0 {
                return self.syntax("float has no fractional digits".to_string());
            }
        }

        if matches!(self.peek_char(), Some('e') | Some('E')) {
            is_float = true;
            self.consume_char();
            if matches!(self.peek_char(), Some('+') | Some('-')) {
                self.consume_char();
            }
            let mut exponent_digits = 0usize;
            while matches!(self.peek_char(), Some('0'..='9')) {
                self.consume_char();
                exponent_digits += 1;
            }
            if exponent_digits == 0 {
                return self.syntax("float exponent has no digits".to_string());
            }
        }

        let number = &self.input[start..self.pos];
        if is_float {
            number
                .parse::<f64>()
                .map(TckValue::Float)
                .map_err(|err| ValueError::Syntax {
                    offset: start,
                    message: format!("invalid float `{number}`: {err}"),
                })
        } else {
            number
                .parse::<i64>()
                .map(TckValue::Int)
                .map_err(|err| ValueError::Syntax {
                    offset: start,
                    message: format!("invalid integer `{number}`: {err}"),
                })
        }
    }

    fn parse_key(&mut self) -> Result<String, ValueError> {
        self.skip_ws();
        if self.peek_char() == Some('\'') {
            self.parse_string()
        } else {
            self.parse_identifier()
        }
    }

    fn parse_identifier(&mut self) -> Result<String, ValueError> {
        self.skip_ws();
        if self.consume_char_if('`') {
            let mut value = String::new();
            while let Some(ch) = self.consume_char() {
                if ch == '`' {
                    if self.consume_char_if('`') {
                        value.push('`');
                    } else {
                        return Ok(value);
                    }
                } else {
                    value.push(ch);
                }
            }
            return self.syntax("unterminated backtick identifier".to_string());
        }

        let mut value = String::new();
        match self.peek_char() {
            Some(ch) if is_identifier_start(ch) => {
                value.push(ch);
                self.consume_char();
            }
            Some(ch) => return self.syntax(format!("invalid identifier start `{ch}`")),
            None => return self.syntax("expected identifier".to_string()),
        }

        while let Some(ch) = self.peek_char() {
            if is_identifier_continue(ch) {
                value.push(ch);
                self.consume_char();
            } else {
                break;
            }
        }

        Ok(value)
    }

    fn consume_keyword(&mut self, keyword: &str) -> bool {
        if !self.input[self.pos..].starts_with(keyword) {
            return false;
        }
        let next = self.pos + keyword.len();
        if self
            .input
            .get(next..)
            .and_then(|rest| rest.chars().next())
            .is_some_and(is_identifier_continue)
        {
            return false;
        }
        self.pos = next;
        true
    }

    fn peek_non_ws_after(&self, expected: char) -> Option<char> {
        let mut chars = self.input[self.pos..].chars();
        if chars.next() != Some(expected) {
            return None;
        }
        chars.find(|ch| !ch.is_whitespace())
    }

    fn skip_ws(&mut self) {
        while self.peek_char().is_some_and(char::is_whitespace) {
            self.consume_char();
        }
    }

    fn expect_char(&mut self, expected: char) -> Result<(), ValueError> {
        self.skip_ws();
        match self.consume_char() {
            Some(actual) if actual == expected => Ok(()),
            Some(actual) => self.syntax(format!("expected `{expected}`, got `{actual}`")),
            None => self.syntax(format!("expected `{expected}`, got end of input")),
        }
    }

    fn consume_char_if(&mut self, expected: char) -> bool {
        if self.peek_char() == Some(expected) {
            self.consume_char();
            true
        } else {
            false
        }
    }

    fn consume_char(&mut self) -> Option<char> {
        let ch = self.peek_char()?;
        self.pos += ch.len_utf8();
        Some(ch)
    }

    fn peek_char(&self) -> Option<char> {
        self.input[self.pos..].chars().next()
    }

    fn is_eof(&self) -> bool {
        self.pos >= self.input.len()
    }

    fn syntax<T>(&self, message: String) -> Result<T, ValueError> {
        Err(ValueError::Syntax {
            offset: self.pos,
            message,
        })
    }
}

fn is_identifier_start(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphabetic()
}

fn is_identifier_continue(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphanumeric()
}

fn actual_cell(batch: &RecordBatch, row: usize, header: &str) -> Result<TckValue, String> {
    if let Some(value) = reconstruct_element(batch, row, header)? {
        return Ok(value);
    }

    let schema = batch.schema();
    let Some((idx, _)) = schema.column_with_name(header) else {
        return Err(format!("actual column `{header}` not found"));
    };
    scalar_value(batch.column(idx).as_ref(), row)
}

fn reconstruct_element(
    batch: &RecordBatch,
    row: usize,
    header: &str,
) -> Result<Option<TckValue>, String> {
    let labels_name = format!("{header}._labels");
    let schema = batch.schema();
    let Some((labels_idx, _)) = schema.column_with_name(&labels_name) else {
        return Ok(None);
    };

    let labels = labels_at(batch.column(labels_idx).as_ref(), row, &labels_name)?;
    let prefix = format!("{header}.");
    let is_rel = schema
        .column_with_name(&format!("{header}._src_iid"))
        .is_some()
        || schema
            .column_with_name(&format!("{header}._dst_iid"))
            .is_some();

    let mut props = BTreeMap::new();
    for (idx, field) in schema.fields().iter().enumerate() {
        let name = field.name();
        let Some(prop_name) = name.strip_prefix(&prefix) else {
            continue;
        };
        if is_system_element_column(prop_name) {
            continue;
        }
        let value = scalar_value(batch.column(idx).as_ref(), row)?;
        if value != TckValue::Null {
            props.insert(prop_name.to_string(), value);
        }
    }

    if is_rel {
        Ok(Some(TckValue::Rel {
            typ: labels.first().cloned().unwrap_or_default(),
            props,
        }))
    } else {
        Ok(Some(TckValue::Node { labels, props }))
    }
}

fn is_system_element_column(name: &str) -> bool {
    matches!(
        name,
        "_iid"
            | "_id"
            | "_labels"
            | "_src_iid"
            | "_dst_iid"
            | "_system_from"
            | "_system_to"
            | "_valid_from"
            | "_valid_to"
    )
}

fn scalar_value(array: &dyn Array, row: usize) -> Result<TckValue, String> {
    if array.is_null(row) {
        return Ok(TckValue::Null);
    }

    match array.data_type() {
        DataType::Null => Ok(TckValue::Null),
        DataType::Boolean => {
            let Some(values) = array.as_any().downcast_ref::<BooleanArray>() else {
                return Err("Boolean array downcast failed".to_string());
            };
            Ok(TckValue::Bool(values.value(row)))
        }
        DataType::Int64 => {
            let Some(values) = array.as_any().downcast_ref::<Int64Array>() else {
                return Err("Int64 array downcast failed".to_string());
            };
            Ok(TckValue::Int(values.value(row)))
        }
        DataType::Float64 => {
            let Some(values) = array.as_any().downcast_ref::<Float64Array>() else {
                return Err("Float64 array downcast failed".to_string());
            };
            Ok(TckValue::Float(values.value(row)))
        }
        DataType::Utf8 => {
            let Some(values) = array.as_any().downcast_ref::<StringArray>() else {
                return Err("Utf8 array downcast failed".to_string());
            };
            Ok(TckValue::Str(values.value(row).to_string()))
        }
        DataType::LargeUtf8 => {
            let Some(values) = array.as_any().downcast_ref::<LargeStringArray>() else {
                return Err("LargeUtf8 array downcast failed".to_string());
            };
            Ok(TckValue::Str(values.value(row).to_string()))
        }
        DataType::List(_) => list_value(array, row),
        DataType::Struct(_) => struct_value(array, row),
        DataType::Map(_, _) => map_value(array, row),
        other => Err(format!("unsupported scalar Arrow type {other:?}")),
    }
}

fn list_value(array: &dyn Array, row: usize) -> Result<TckValue, String> {
    let Some(list) = array.as_any().downcast_ref::<ListArray>() else {
        return Err("ListArray downcast failed".to_string());
    };
    let values = list.value(row);
    if matches!(values.data_type(), DataType::FixedSizeBinary(16)) {
        let Some(iids) = values.as_any().downcast_ref::<FixedSizeBinaryArray>() else {
            return Err("FixedSizeBinary path array downcast failed".to_string());
        };
        for idx in 0..iids.len() {
            if iids.is_null(idx) {
                return Err("path column contains null iid".to_string());
            }
        }
        return Ok(path_shape(iids.len()));
    }

    let mut out = Vec::with_capacity(values.len());
    for idx in 0..values.len() {
        out.push(scalar_value(values.as_ref(), idx)?);
    }
    Ok(TckValue::List(out))
}

fn path_shape(len: usize) -> TckValue {
    let mut values = Vec::with_capacity(len);
    for idx in 0..len {
        if idx % 2 == 0 {
            values.push(TckValue::Node {
                labels: Vec::new(),
                props: BTreeMap::new(),
            });
        } else {
            values.push(TckValue::Rel {
                typ: String::new(),
                props: BTreeMap::new(),
            });
        }
    }
    TckValue::Path(values)
}

fn struct_value(array: &dyn Array, row: usize) -> Result<TckValue, String> {
    let Some(values) = array.as_any().downcast_ref::<StructArray>() else {
        return Err("StructArray downcast failed".to_string());
    };

    let mut map = BTreeMap::new();
    for (field, column) in values.fields().iter().zip(values.columns()) {
        map.insert(
            field.name().to_string(),
            scalar_value(column.as_ref(), row)?,
        );
    }
    Ok(TckValue::Map(map))
}

fn map_value(array: &dyn Array, row: usize) -> Result<TckValue, String> {
    let Some(map_array) = array.as_any().downcast_ref::<MapArray>() else {
        return Err("MapArray downcast failed".to_string());
    };

    let offsets = map_array.offsets();
    let start = offsets[row] as usize;
    let end = offsets[row + 1] as usize;
    let keys = map_array.keys();
    let values = map_array.values();
    let mut map = BTreeMap::new();
    for idx in start..end {
        map.insert(
            map_key(keys.as_ref(), idx)?,
            scalar_value(values.as_ref(), idx)?,
        );
    }
    Ok(TckValue::Map(map))
}

fn map_key(array: &dyn Array, row: usize) -> Result<String, String> {
    match array.data_type() {
        DataType::Utf8 => {
            let Some(values) = array.as_any().downcast_ref::<StringArray>() else {
                return Err("Map key Utf8 array downcast failed".to_string());
            };
            Ok(values.value(row).to_string())
        }
        DataType::LargeUtf8 => {
            let Some(values) = array.as_any().downcast_ref::<LargeStringArray>() else {
                return Err("Map key LargeUtf8 array downcast failed".to_string());
            };
            Ok(values.value(row).to_string())
        }
        other => Err(format!("unsupported map key Arrow type {other:?}")),
    }
}

fn labels_at(array: &dyn Array, row: usize, column: &str) -> Result<Vec<String>, String> {
    let DataType::List(item) = array.data_type() else {
        return Err(format!("`{column}` must be List<Utf8>"));
    };
    if item.data_type() != &DataType::Utf8 {
        return Err(format!("`{column}` must be List<Utf8>"));
    }
    let Some(list) = array.as_any().downcast_ref::<ListArray>() else {
        return Err(format!("`{column}` ListArray downcast failed"));
    };
    if list.is_null(row) {
        return Ok(Vec::new());
    }
    let values = list.value(row);
    let Some(strings) = values.as_any().downcast_ref::<StringArray>() else {
        return Err(format!("`{column}` values are not Utf8"));
    };

    let mut labels = Vec::with_capacity(strings.len());
    for idx in 0..strings.len() {
        if strings.is_null(idx) {
            return Err(format!("`{column}` contains null label"));
        }
        labels.push(strings.value(idx).to_string());
    }
    Ok(labels)
}

fn multiset(rows: &[Vec<TckValue>]) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for row in rows {
        *counts.entry(canonical_row(row)).or_insert(0) += 1;
    }
    counts
}

fn canonical_row(row: &[TckValue]) -> String {
    row.iter()
        .map(canonical_value)
        .collect::<Vec<_>>()
        .join("\u{1f}")
}

fn canonical_value(value: &TckValue) -> String {
    match value {
        TckValue::Null => "null".to_string(),
        TckValue::Bool(value) => format!("bool:{value}"),
        TckValue::Int(value) => format!("int:{value}"),
        TckValue::Float(value) if value.is_nan() => "float:NaN".to_string(),
        TckValue::Float(value) => format!("float:{value:?}"),
        TckValue::Str(value) => format!("str:{value:?}"),
        TckValue::List(values) => format!(
            "list:[{}]",
            values
                .iter()
                .map(canonical_value)
                .collect::<Vec<_>>()
                .join(",")
        ),
        TckValue::Map(values) => format!(
            "map:{{{}}}",
            values
                .iter()
                .map(|(key, value)| format!("{key:?}:{}", canonical_value(value)))
                .collect::<Vec<_>>()
                .join(",")
        ),
        TckValue::Node { labels, props } => format!(
            "node:{}:{}",
            labels.join(":"),
            canonical_value(&TckValue::Map(props.clone()))
        ),
        TckValue::Rel { typ, props } => format!(
            "rel:{typ}:{}",
            canonical_value(&TckValue::Map(props.clone()))
        ),
        TckValue::Path(values) => format!(
            "path:<{}>",
            values
                .iter()
                .map(|value| match value {
                    TckValue::Node { .. } => "n",
                    TckValue::Rel { .. } => "r",
                    _ => "?",
                })
                .collect::<Vec<_>>()
                .join(",")
        ),
    }
}
