# Slice 0: Foundation — Workspace, Types, Config, Registry, CI

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** A compiling, CI-green Cargo workspace containing `varve-types` (Iid, LogPosition, errors) and `varve-config` (TOML config + component registry) — the composition backbone every later slice plugs into.

**Architecture:** Workspace at repo root, crates under `crates/`. `varve-types` holds shared value types with zero heavy deps. `varve-config` implements the spec §4 pattern: typed `Registry<T>` per subsystem, explicit factory registration, TOML sections select implementations by name.

**Tech Stack:** Rust stable, `xxhash-rust` (xxh3), `toml`, `serde`, `thiserror`.

## Global Constraints

- TDD: every task writes its failing test first (see roadmap Global constraints).
- `cargo clippy --workspace --all-targets -- -D warnings` must stay clean; no `unwrap()`/`expect()` in library code (tests OK).
- Errors via `thiserror`; each crate exports its own error type.
- Spec references: `docs/design/2026-07-04-varve-design.md` §4 (composition), §5.3 (IID), §6 (positions), §15 (workspace).

---

### Task 1: Workspace scaffold + Iid in varve-types

**Files:**
- Create: `Cargo.toml` (workspace root)
- Create: `rust-toolchain.toml`
- Create: `crates/varve-types/Cargo.toml`
- Create: `crates/varve-types/src/lib.rs`
- Create: `crates/varve-types/src/iid.rs`
- Test: in-module `#[cfg(test)]` (unit) in `iid.rs`

**Interfaces:**
- Produces: `varve_types::Iid` — `Iid::derive(graph: &str, table: &str, user_id: &UserIdBytes) -> Iid`; `Iid::as_bytes(&self) -> &[u8; 16]`; `Iid::from_bytes([u8; 16]) -> Iid`. `Iid: Copy + Eq + Ord + Hash + Debug`.
- Note: `derive` takes the *canonical byte encoding* of the user id (`&[u8]`), not `Value` (which doesn't exist until slice 1). Slice 1 adds `Value::id_bytes()` producing this encoding.

- [x] **Step 1: Create workspace + crate scaffolding**

`Cargo.toml` (root):
```toml
[workspace]
resolver = "2"
members = ["crates/*"]

[workspace.package]
version = "0.1.0"
edition = "2021"
license = "Apache-2.0"
repository = "https://github.com/ravan/varve"

[workspace.dependencies]
thiserror = "2"
serde = { version = "1", features = ["derive"] }
toml = "0.8"
xxhash-rust = { version = "0.8", features = ["xxh3"] }

[workspace.lints.clippy]
unwrap_used = "deny"
expect_used = "deny"
```

`rust-toolchain.toml`:
```toml
[toolchain]
channel = "stable"
components = ["rustfmt", "clippy"]
```

`crates/varve-types/Cargo.toml`:
```toml
[package]
name = "varve-types"
version.workspace = true
edition.workspace = true
license.workspace = true

[dependencies]
thiserror = { workspace = true }
xxhash-rust = { workspace = true }

[lints]
workspace = true
```

`crates/varve-types/src/lib.rs`:
```rust
pub mod iid;
pub use iid::Iid;
```

- [x] **Step 2: Write the failing test**

`crates/varve-types/src/iid.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_inputs_same_iid() {
        let a = Iid::derive("default", "nodes", b"42");
        let b = Iid::derive("default", "nodes", b"42");
        assert_eq!(a, b);
    }

    #[test]
    fn different_table_different_iid() {
        let a = Iid::derive("default", "nodes", b"42");
        let b = Iid::derive("default", "edges", b"42");
        assert_ne!(a, b);
    }

    #[test]
    fn no_concat_ambiguity() {
        // ("ab","c") must differ from ("a","bc") — length-prefixed hashing
        let a = Iid::derive("g", "ab", b"c");
        let b = Iid::derive("g", "a", b"bc");
        assert_ne!(a, b);
    }

    #[test]
    fn round_trips_bytes() {
        let a = Iid::derive("g", "t", b"x");
        assert_eq!(Iid::from_bytes(*a.as_bytes()), a);
    }
}
```

- [x] **Step 3: Run test to verify it fails**

Run: `cargo test -p varve-types`
Expected: compile error — `Iid` not defined.

- [x] **Step 4: Write minimal implementation**

Prepend to `crates/varve-types/src/iid.rs`:
```rust
use xxhash_rust::xxh3::Xxh3;

/// 16-byte internal entity id: xxh3-128 over length-prefixed (graph, table, user id bytes).
/// Spec §5.3.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Iid([u8; 16]);

impl Iid {
    pub fn derive(graph: &str, table: &str, user_id: &[u8]) -> Self {
        let mut h = Xxh3::new();
        for part in [graph.as_bytes(), table.as_bytes(), user_id] {
            h.update(&(part.len() as u64).to_le_bytes());
            h.update(part);
        }
        Iid(h.digest128().to_be_bytes())
    }

    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Iid(bytes)
    }
}
```

- [x] **Step 5: Run test to verify it passes**

Run: `cargo test -p varve-types`
Expected: 4 passed.

- [x] **Step 6: Commit**

```bash
git add Cargo.toml rust-toolchain.toml crates/
git commit -m "feat: workspace scaffold + Iid derivation in varve-types"
```

---

### Task 2: LogPosition (epoch/offset packed u64)

**Files:**
- Create: `crates/varve-types/src/position.rs`
- Modify: `crates/varve-types/src/lib.rs` (add `pub mod position; pub use position::LogPosition;`)

**Interfaces:**
- Produces: `varve_types::LogPosition` — `LogPosition::new(epoch: u16, offset: u64) -> Result<LogPosition, TypeError>` (offset must fit 48 bits); `epoch() -> u16`; `offset() -> u64`; `as_u64() -> u64`; `from_u64(u64) -> LogPosition`; `next(&self) -> Result<LogPosition, TypeError>`. Ordering of `LogPosition` == ordering of `as_u64()` (epoch-major). Also produces `varve_types::TypeError` (thiserror enum, first variant `OffsetOverflow`).

- [x] **Step 1: Write the failing test**

`crates/varve-types/src/position.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packs_and_unpacks() {
        let p = LogPosition::new(3, 0x0000_ABCD_EF01_2345).unwrap();
        assert_eq!(p.epoch(), 3);
        assert_eq!(p.offset(), 0x0000_ABCD_EF01_2345);
        assert_eq!(LogPosition::from_u64(p.as_u64()), p);
    }

    #[test]
    fn epoch_major_ordering() {
        let old = LogPosition::new(1, u64::MAX >> 16).unwrap(); // max 48-bit offset
        let new = LogPosition::new(2, 0).unwrap();
        assert!(new > old);
        assert!(new.as_u64() > old.as_u64());
    }

    #[test]
    fn rejects_offset_over_48_bits() {
        assert!(LogPosition::new(0, 1u64 << 48).is_err());
    }

    #[test]
    fn next_increments_offset() {
        let p = LogPosition::new(0, 7).unwrap();
        assert_eq!(p.next().unwrap(), LogPosition::new(0, 8).unwrap());
    }
}
```

- [x] **Step 2: Run test to verify it fails**

Run: `cargo test -p varve-types position`
Expected: compile error — `LogPosition` not defined.

- [x] **Step 3: Write minimal implementation**

Prepend to `crates/varve-types/src/position.rs`:
```rust
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum TypeError {
    #[error("log offset {0} exceeds 48 bits")]
    OffsetOverflow(u64),
}

const OFFSET_BITS: u32 = 48;
const OFFSET_MASK: u64 = (1 << OFFSET_BITS) - 1;

/// Position in the transaction log: epoch (high 16 bits) | offset (low 48 bits).
/// Epoch-major packing keeps u64 ordering == logical ordering. Spec §6.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct LogPosition(u64);

impl LogPosition {
    pub fn new(epoch: u16, offset: u64) -> Result<Self, TypeError> {
        if offset > OFFSET_MASK {
            return Err(TypeError::OffsetOverflow(offset));
        }
        Ok(LogPosition(((epoch as u64) << OFFSET_BITS) | offset))
    }

    pub fn epoch(&self) -> u16 {
        (self.0 >> OFFSET_BITS) as u16
    }

    pub fn offset(&self) -> u64 {
        self.0 & OFFSET_MASK
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }

    pub fn from_u64(v: u64) -> Self {
        LogPosition(v)
    }

    pub fn next(&self) -> Result<Self, TypeError> {
        Self::new(self.epoch(), self.offset() + 1)
    }
}
```

Update `lib.rs`:
```rust
pub mod iid;
pub mod position;
pub use iid::Iid;
pub use position::{LogPosition, TypeError};
```

- [x] **Step 4: Run test to verify it passes**

Run: `cargo test -p varve-types`
Expected: all pass (Iid tests + 4 position tests).

- [x] **Step 5: Commit**

```bash
git add crates/varve-types/
git commit -m "feat: LogPosition with epoch-major u64 packing"
```

---

### Task 3: Config loading (TOML + env overrides)

**Files:**
- Create: `crates/varve-config/Cargo.toml`
- Create: `crates/varve-config/src/lib.rs`
- Create: `crates/varve-config/src/config.rs`
- Test: `crates/varve-config/tests/config_test.rs`

**Interfaces:**
- Produces: `varve_config::Config` — `Config::from_toml_str(&str) -> Result<Config, ConfigError>`; `Config::from_file(&Path) -> Result<Config, ConfigError>`; `section(&self, name: &str) -> Option<ConfigSection>`.
- Produces: `varve_config::ConfigSection` — `backend(&self) -> Option<&str>` (the `backend` key); `get<T: DeserializeOwned>(&self) -> Result<T, ConfigError>` (deserialize whole section); `child(&self, name: &str) -> Option<ConfigSection>` (nested table, e.g. `[storage.s3]`).
- Produces: `varve_config::ConfigError` (thiserror: `Io`, `Parse`, `Deserialize`).
- Env overrides: `VARVE__LOG__BACKEND=memory` overrides `[log] backend` (double underscore = nesting); applied inside `from_toml_str`/`from_file`.

- [x] **Step 1: Write the failing test**

`crates/varve-config/tests/config_test.rs`:
```rust
use serde::Deserialize;
use varve_config::Config;

const SAMPLE: &str = r#"
[node]
roles = ["writer", "query"]

[log]
backend = "local"
group_commit_window_ms = 15

[log.local]
dir = "/tmp/varve-log"
"#;

#[derive(Deserialize, Debug, PartialEq)]
struct LogLocal {
    dir: String,
}

#[test]
fn reads_sections_and_backend() {
    let cfg = Config::from_toml_str(SAMPLE).unwrap();
    let log = cfg.section("log").unwrap();
    assert_eq!(log.backend(), Some("local"));
    let local: LogLocal = log.child("local").unwrap().get().unwrap();
    assert_eq!(local.dir, "/tmp/varve-log");
}

#[test]
fn missing_section_is_none() {
    let cfg = Config::from_toml_str(SAMPLE).unwrap();
    assert!(cfg.section("storage").is_none());
}

#[test]
fn env_var_overrides_key() {
    // env override: VARVE__LOG__BACKEND=memory
    std::env::set_var("VARVE__LOG__BACKEND", "memory");
    let cfg = Config::from_toml_str(SAMPLE).unwrap();
    assert_eq!(cfg.section("log").unwrap().backend(), Some("memory"));
    std::env::remove_var("VARVE__LOG__BACKEND");
}
```

- [x] **Step 2: Run test to verify it fails**

Run: `cargo test -p varve-config`
Expected: compile error — crate/module missing.

- [x] **Step 3: Write minimal implementation**

`crates/varve-config/Cargo.toml`:
```toml
[package]
name = "varve-config"
version.workspace = true
edition.workspace = true
license.workspace = true

[dependencies]
thiserror = { workspace = true }
serde = { workspace = true }
toml = { workspace = true }

[lints]
workspace = true
```

`crates/varve-config/src/lib.rs`:
```rust
pub mod config;
pub mod registry;
pub use config::{Config, ConfigError, ConfigSection};
```
(Note: `registry` module arrives in Task 4; create it as an empty `pub mod` file now or add the `pub mod registry;` line in Task 4 — Task 4's step shows the line again.)

`crates/varve-config/src/config.rs`:
```rust
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

/// VARVE__SECTION__KEY=value → root[section][key] = value (string).
fn apply_env_overrides(root: &mut toml::Table, vars: impl Iterator<Item = (String, String)>) {
    for (k, v) in vars {
        let Some(rest) = k.strip_prefix(ENV_PREFIX) else { continue };
        let parts: Vec<String> = rest.split("__").map(|p| p.to_lowercase()).collect();
        let [section, key] = parts.as_slice() else { continue };
        let entry = root
            .entry(section.clone())
            .or_insert_with(|| toml::Value::Table(toml::Table::new()));
        if let toml::Value::Table(t) = entry {
            t.insert(key.clone(), toml::Value::String(v));
        }
    }
}
```

- [x] **Step 4: Run test to verify it passes**

Run: `cargo test -p varve-config`
Expected: 3 passed. (If the env test flakes under parallel test execution later, mark it `#[serial]` with the `serial_test` crate — only if observed.)

- [x] **Step 5: Commit**

```bash
git add crates/varve-config/
git commit -m "feat: TOML config with sections and env overrides"
```

---

### Task 4: Component registry

**Files:**
- Create: `crates/varve-config/src/registry.rs`
- Modify: `crates/varve-config/src/lib.rs` (export registry items)
- Test: `crates/varve-config/tests/registry_test.rs`

**Interfaces:**
- Produces: `varve_config::registry::{Registry, ComponentFactory, RegistryError}`:

```rust
pub trait ComponentFactory<T: ?Sized>: Send + Sync {
    fn name(&self) -> &'static str;
    fn build(&self, cfg: &ConfigSection) -> Result<std::sync::Arc<T>, RegistryError>;
}

pub struct Registry<T: ?Sized> { /* kind + name → factory */ }

impl<T: ?Sized> Registry<T> {
    pub fn new(kind: &'static str) -> Self;
    pub fn register(&mut self, f: Box<dyn ComponentFactory<T>>) -> Result<(), RegistryError>; // Duplicate error
    pub fn build(&self, name: &str, cfg: &ConfigSection) -> Result<std::sync::Arc<T>, RegistryError>; // Unknown error lists candidates
    pub fn names(&self) -> Vec<&'static str>;
}
```

- `RegistryError` variants: `Duplicate { kind, name }`, `Unknown { kind, name, available: Vec<String> }`, `Build { kind, name, source: Box<dyn Error + Send + Sync> }`, and `From<ConfigError>`.
- Later slices aggregate per-subsystem registries in `varve-engine` (e.g. `registries.log: Registry<dyn Log>`); this crate only provides the generic mechanism. A `BuildContext` parameter is deliberately deferred until a factory actually needs cross-component access (YAGNI; revisit in slice 3 when `log/local` needs a data directory root — pass it via config instead if that suffices).

- [x] **Step 1: Write the failing test**

`crates/varve-config/tests/registry_test.rs`:
```rust
use std::sync::Arc;
use varve_config::registry::{ComponentFactory, Registry, RegistryError};
use varve_config::{Config, ConfigSection};

// Toy subsystem trait standing in for Log/ObjectStore/…
trait Greeter: Send + Sync {
    fn greet(&self) -> String;
}

struct EnglishGreeter {
    name: String,
}
impl Greeter for EnglishGreeter {
    fn greet(&self) -> String {
        format!("hello {}", self.name)
    }
}

struct EnglishFactory;
impl ComponentFactory<dyn Greeter> for EnglishFactory {
    fn name(&self) -> &'static str {
        "english"
    }
    fn build(&self, cfg: &ConfigSection) -> Result<Arc<dyn Greeter>, RegistryError> {
        #[derive(serde::Deserialize)]
        struct C {
            name: String,
        }
        let c: C = cfg.get()?;
        Ok(Arc::new(EnglishGreeter { name: c.name }))
    }
}

fn section(toml: &str, name: &str) -> ConfigSection {
    Config::from_toml_str(toml).unwrap().section(name).unwrap()
}

#[test]
fn builds_registered_component_from_config() {
    let mut reg: Registry<dyn Greeter> = Registry::new("greeter");
    reg.register(Box::new(EnglishFactory)).unwrap();
    let cfg = section("[greeter]\nname = \"ada\"", "greeter");
    let g = reg.build("english", &cfg).unwrap();
    assert_eq!(g.greet(), "hello ada");
}

#[test]
fn unknown_name_error_lists_available() {
    let mut reg: Registry<dyn Greeter> = Registry::new("greeter");
    reg.register(Box::new(EnglishFactory)).unwrap();
    let cfg = section("[greeter]\nname = \"x\"", "greeter");
    let err = reg.build("klingon", &cfg).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("klingon"), "{msg}");
    assert!(msg.contains("english"), "{msg}");
}

#[test]
fn duplicate_registration_rejected() {
    let mut reg: Registry<dyn Greeter> = Registry::new("greeter");
    reg.register(Box::new(EnglishFactory)).unwrap();
    assert!(reg.register(Box::new(EnglishFactory)).is_err());
}
```

- [x] **Step 2: Run test to verify it fails**

Run: `cargo test -p varve-config --test registry_test`
Expected: compile error — `registry` module missing.

- [x] **Step 3: Write minimal implementation**

`crates/varve-config/src/registry.rs`:
```rust
use crate::{ConfigError, ConfigSection};
use std::collections::BTreeMap;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RegistryError {
    #[error("{kind} implementation '{name}' is already registered")]
    Duplicate { kind: &'static str, name: &'static str },
    #[error("unknown {kind} implementation '{name}'; available: [{}]", available.join(", "))]
    Unknown {
        kind: &'static str,
        name: String,
        available: Vec<String>,
    },
    #[error("failed to build {kind} '{name}': {source}")]
    Build {
        kind: &'static str,
        name: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[error(transparent)]
    Config(#[from] ConfigError),
}

pub trait ComponentFactory<T: ?Sized>: Send + Sync {
    fn name(&self) -> &'static str;
    fn build(&self, cfg: &ConfigSection) -> Result<Arc<T>, RegistryError>;
}

pub struct Registry<T: ?Sized> {
    kind: &'static str,
    factories: BTreeMap<&'static str, Box<dyn ComponentFactory<T>>>,
}

impl<T: ?Sized> Registry<T> {
    pub fn new(kind: &'static str) -> Self {
        Registry { kind, factories: BTreeMap::new() }
    }

    pub fn register(&mut self, f: Box<dyn ComponentFactory<T>>) -> Result<(), RegistryError> {
        let name = f.name();
        if self.factories.contains_key(name) {
            return Err(RegistryError::Duplicate { kind: self.kind, name });
        }
        self.factories.insert(name, f);
        Ok(())
    }

    pub fn build(&self, name: &str, cfg: &ConfigSection) -> Result<Arc<T>, RegistryError> {
        match self.factories.get(name) {
            Some(f) => f.build(cfg),
            None => Err(RegistryError::Unknown {
                kind: self.kind,
                name: name.to_string(),
                available: self.names().iter().map(|s| s.to_string()).collect(),
            }),
        }
    }

    pub fn names(&self) -> Vec<&'static str> {
        self.factories.keys().copied().collect()
    }
}
```

Update `crates/varve-config/src/lib.rs`:
```rust
pub mod config;
pub mod registry;
pub use config::{Config, ConfigError, ConfigSection};
pub use registry::{ComponentFactory, Registry, RegistryError};
```

- [x] **Step 4: Run test to verify it passes**

Run: `cargo test -p varve-config`
Expected: all pass (3 config + 3 registry).

- [x] **Step 5: Commit**

```bash
git add crates/varve-config/
git commit -m "feat: typed component registry with factory registration"
```

---

### Task 5: CI pipeline + justfile

**Files:**
- Create: `.github/workflows/ci.yml`
- Create: `justfile`

**Interfaces:**
- Produces: `just check` = fmt-check + clippy + test; CI runs the same on push/PR to `main`. Later slices append recipes (`just fuzz`, `just bench`, `just matrix`) and CI jobs; they modify these files, never replace them.

- [x] **Step 1: Write the check script (the "test" for CI is running it locally)**

`justfile`:
```make
default: check

fmt:
    cargo fmt --all

check:
    cargo fmt --all --check
    cargo clippy --workspace --all-targets -- -D warnings
    cargo test --workspace

test:
    cargo test --workspace
```

`.github/workflows/ci.yml`:
```yaml
name: CI
on:
  push:
    branches: [main]
  pull_request:

env:
  CARGO_TERM_COLOR: always

jobs:
  check:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
        with:
          components: rustfmt, clippy
      - uses: Swatinem/rust-cache@v2
      - run: cargo fmt --all --check
      - run: cargo clippy --workspace --all-targets -- -D warnings
      - run: cargo test --workspace
```

- [x] **Step 2: Run to verify it passes locally**

Run: `just check` (install: `brew install just` / `cargo install just` if missing)
Expected: fmt clean, clippy clean, all tests pass. Fix any fmt/clippy fallout now.

- [x] **Step 3: Commit**

```bash
git add justfile .github/
git commit -m "ci: fmt + clippy + test pipeline"
```

---

## Slice exit checklist

- [x] `just check` green.
- [x] `git log` shows one commit per task, conventional messages.
- [x] Update `docs/plans/STATUS.md`: slice 0 complete, demo command = `cargo test --workspace`, note any deviations.
- [x] Tick slice 0 boxes in `docs/plans/varve-v1-roadmap.md`; commit.
