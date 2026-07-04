use serde::de::DeserializeOwned;
use std::path::Path;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read config file: {0}")]
    Io(#[from] std::io::Error),
    #[error("failed to parse TOML: {0}")]
    Parse(#[from] toml::de::Error),
    #[error("failed to deserialize section: {0}")]
    Deserialize(String),
}

#[derive(Debug, Clone)]
pub struct Config {
    root: toml::Table,
}

#[derive(Debug, Clone)]
pub struct ConfigSection {
    table: toml::Table,
}

const ENV_PREFIX: &str = "VARVE__";

impl Config {
    /// Parses `s` as TOML and applies process-environment overrides on top
    /// (see [`apply_env_overrides`] for the exact rules).
    pub fn from_toml_str(s: &str) -> Result<Self, ConfigError> {
        let mut root: toml::Table = toml::from_str(s)?;
        apply_env_overrides(&mut root, std::env::vars());
        Ok(Config { root })
    }

    pub fn from_file(path: &Path) -> Result<Self, ConfigError> {
        Self::from_toml_str(&std::fs::read_to_string(path)?)
    }

    pub fn section(&self, name: &str) -> Option<ConfigSection> {
        match self.root.get(name) {
            Some(toml::Value::Table(t)) => Some(ConfigSection { table: t.clone() }),
            _ => None,
        }
    }
}

impl ConfigSection {
    /// An empty section (no keys). Used wherever config omits an optional
    /// `[section]`; `get::<T>()` then falls back entirely to `T`'s
    /// `#[serde(default = ...)]` values.
    pub fn empty() -> ConfigSection {
        ConfigSection {
            table: toml::Table::new(),
        }
    }

    pub fn backend(&self) -> Option<&str> {
        self.table.get("backend").and_then(|v| v.as_str())
    }

    pub fn child(&self, name: &str) -> Option<ConfigSection> {
        match self.table.get(name) {
            Some(toml::Value::Table(t)) => Some(ConfigSection { table: t.clone() }),
            _ => None,
        }
    }

    pub fn get<T: DeserializeOwned>(&self) -> Result<T, ConfigError> {
        T::deserialize(toml::Value::Table(self.table.clone()))
            .map_err(|e| ConfigError::Deserialize(e.to_string()))
    }
}

/// Applies `VARVE__`-prefixed environment overrides onto `root`.
///
/// Each var name is stripped of the `VARVE__` prefix and split on `__` into
/// lowercased segments. The **last** segment is the key to set; **all
/// preceding segments** form the nested table path leading to it. For
/// example:
///
/// - `VARVE__LOG__BACKEND=memory` → path `[log]`, key `backend`.
/// - `VARVE__LOG__LOCAL__DIR=/x` → path `[log, local]`, key `dir`, i.e.
///   `root.log.local.dir = "/x"`.
/// - `VARVE__STORAGE__S3__ENDPOINT=...` → path `[storage, s3]`, key
///   `endpoint`.
///
/// Intermediate tables along the path are created as needed. If fewer than
/// two segments remain after the prefix (no key component, e.g.
/// `VARVE__FOO`), the var is skipped as a no-op.
///
/// **Non-table intermediates are never clobbered.** If walking the path
/// finds an existing entry that is *not* a table (e.g. `log.backend` is
/// already a string and a var tries to set `log.backend.sub`), the whole
/// override is skipped: the existing scalar is left untouched and no panic
/// occurs.
///
/// **Scalar coercion.** The string value is coerced to a TOML scalar by
/// trying, in order: `bool` (only the exact literals `true`/`false`), then
/// `i64`, then `f64`. If none parse, the value is kept as a
/// `toml::Value::String`. So `VARVE__LOG__GROUP_COMMIT_WINDOW_MS=30` becomes
/// an integer, `...=1.5` becomes a float, `...=true` becomes a bool, and
/// `...=memory` remains the string `"memory"`.
fn apply_env_overrides(root: &mut toml::Table, vars: impl Iterator<Item = (String, String)>) {
    for (k, v) in vars {
        let Some(rest) = k.strip_prefix(ENV_PREFIX) else {
            continue;
        };
        let parts: Vec<String> = rest.split("__").map(|p| p.to_lowercase()).collect();
        // Need at least [path-segment..., key] i.e. >= 2 segments total.
        let Some((key, path)) = parts.split_last() else {
            continue;
        };
        if path.is_empty() {
            continue;
        }

        insert_nested(root, path, key, coerce_scalar(v));
    }
}

/// Walks `path` from `table`, creating nested `toml::Table`s as needed,
/// then inserts `value` under `key` in the table found at the end of the
/// path. If any existing entry along the path is not a table, the insert is
/// silently skipped rather than clobbering that value.
fn insert_nested(table: &mut toml::Table, path: &[String], key: &str, value: toml::Value) {
    match path.split_first() {
        None => {
            table.insert(key.to_string(), value);
        }
        Some((segment, rest)) => {
            let entry = table
                .entry(segment.clone())
                .or_insert_with(|| toml::Value::Table(toml::Table::new()));
            if let toml::Value::Table(nested) = entry {
                insert_nested(nested, rest, key, value);
            }
            // else: path walks through an existing non-table value — skip
            // this override entirely rather than clobbering it.
        }
    }
}

/// Coerces a raw environment-variable string into the most specific TOML
/// scalar it cleanly parses as, trying `bool` (exact `true`/`false` only),
/// then `i64`, then `f64`, and falling back to a `toml::Value::String`.
fn coerce_scalar(v: String) -> toml::Value {
    if v == "true" {
        toml::Value::Boolean(true)
    } else if v == "false" {
        toml::Value::Boolean(false)
    } else if let Ok(i) = v.parse::<i64>() {
        toml::Value::Integer(i)
    } else if let Ok(f) = v.parse::<f64>() {
        toml::Value::Float(f)
    } else {
        toml::Value::String(v)
    }
}
