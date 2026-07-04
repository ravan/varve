# Slice 1: Walking Skeleton — INSERT → MATCH end-to-end in memory

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `Db::memory()` accepts `INSERT (:Person {_id: 1, name: 'Ada'})` and answers `MATCH (p:Person) WHERE p.name = 'Ada' RETURN p.name` through the real pipeline: tokenizer → parser → AST → live table → DataFusion → Arrow record batches.

**Architecture:** Minimal-but-real version of every pipeline stage (spec §3, §10, §11). Nodes only; no temporal semantics (slice 2), no durability (slice 3), no persistence (slice 4). Each stage's v0 shape is the seed of its final form — nothing here is throwaway except explicitly marked v0 limitations.

**Tech Stack:** Adds `datafusion`, `arrow`, `tokio` to the workspace.

## Global Constraints

- All roadmap Global Constraints apply (TDD; clippy -D warnings; no unwrap in lib code).
- **Dependency pinning:** at implementation time pin the latest stable `datafusion` in the root `Cargo.toml` and set the workspace `arrow` version to the one DataFusion re-exports (`cargo tree -p datafusion | grep " arrow "`). The **test code in this plan is the contract**; if a DataFrame-API sketch differs from the pinned DataFusion's API, adapt the implementation, not the test.
- v0 limitations (each gets a `// v0:` comment at the code site and is lifted in the named slice): no temporal columns (slice 2), single default graph (slice 7), property columns require a consistent type per name (slice 2's typed event docs), `_id` optional with process-local generation (slice 2).

## Workspace changes (folded into Task 1)

Root `Cargo.toml` `[workspace.dependencies]` additions:

```toml
datafusion = "46"            # pin latest stable at implementation time
arrow = "53"                 # MUST match datafusion's re-exported arrow major
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
async-trait = "0.1"
```

---

### Task 1: Value type and Doc in varve-types

**Files:**
- Create: `crates/varve-types/src/value.rs`
- Modify: `crates/varve-types/src/lib.rs`
- Test: in-module `#[cfg(test)]`

**Interfaces:**
- Produces: `varve_types::Value` — `enum Value { Null, Bool(bool), Int(i64), Float(f64), Str(String), Bytes(Vec<u8>) }` with `PartialEq, Clone, Debug`.
- Produces: `Value::id_bytes(&self) -> Vec<u8>` — canonical byte encoding for IID derivation: 1 type-tag byte + payload (`Int` → tag 0x01 + big-endian i64; `Str` → 0x02 + UTF-8; `Bytes` → 0x03 + raw; `Bool` → 0x04 + 0/1; `Float`/`Null` are **not** valid ids → return `Err(TypeError::InvalidId)`); actual signature `id_bytes(&self) -> Result<Vec<u8>, TypeError>`.
- Produces: `varve_types::Doc` = `std::collections::BTreeMap<String, Value>` (type alias; BTreeMap for deterministic iteration).
- Modify `TypeError`: add variant `#[error("value cannot be used as an id: {0}")] InvalidId(String)`.

- [ ] **Step 1: Write the failing test**

`crates/varve-types/src/value.rs`:
```rust
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p varve-types value`
Expected: compile error — `Value` not defined.

- [ ] **Step 3: Write minimal implementation**

Prepend to `crates/varve-types/src/value.rs`:
```rust
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
```

Add to `TypeError` in `position.rs`:
```rust
    #[error("value cannot be used as an id: {0}")]
    InvalidId(String),
```

Update `lib.rs`:
```rust
pub mod iid;
pub mod position;
pub mod value;
pub use iid::Iid;
pub use position::{LogPosition, TypeError};
pub use value::{Doc, Value};
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p varve-types`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml crates/varve-types/
git commit -m "feat: Value type with canonical id encoding"
```

---

### Task 2: Tokenizer in varve-gql

**Files:**
- Create: `crates/varve-gql/Cargo.toml` (deps: `thiserror` workspace; `[lints] workspace = true`)
- Create: `crates/varve-gql/src/lib.rs` (`pub mod token; pub mod ast; pub mod parser;` — `ast`/`parser` land in Tasks 3–4; add the mods as they land)
- Create: `crates/varve-gql/src/token.rs`
- Test: in-module `#[cfg(test)]`

**Interfaces:**
- Produces: `varve_gql::token::{Token, TokenKind, tokenize, GqlError}`.
- `tokenize(&str) -> Result<Vec<Token>, GqlError>`; `Token { kind: TokenKind, offset: usize }`.
- `TokenKind`: `Kw(Keyword)` (`Keyword` enum: `Insert, Match, Where, Return, As, True, False, Null` — extended in later slices), `Ident(String)` (case preserved), `Str(String)` (single-quoted, `''` escape), `Int(i64)`, `Float(f64)`, `LParen, RParen, LBrace, RBrace, Colon, Comma, Dot, Eq, Dollar, Eof`.
- Keywords are case-insensitive. `GqlError` (thiserror): `Lex { offset, msg }`, `Parse { offset, msg }` (Parse used by Tasks 3–4).

- [ ] **Step 1: Write the failing test**

In `crates/varve-gql/src/token.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn kinds(src: &str) -> Vec<TokenKind> {
        tokenize(src).unwrap().into_iter().map(|t| t.kind).collect()
    }

    #[test]
    fn tokenizes_insert_statement() {
        use Keyword::*;
        use TokenKind::*;
        assert_eq!(
            kinds("INSERT (:Person {name: 'Ada', age: 36})"),
            vec![
                Kw(Insert), LParen, Colon, Ident("Person".into()), LBrace,
                Ident("name".into()), Colon, Str("Ada".into()), Comma,
                Ident("age".into()), Colon, Int(36), RBrace, RParen, Eof
            ]
        );
    }

    #[test]
    fn keywords_case_insensitive_idents_preserved() {
        use Keyword::*;
        use TokenKind::*;
        assert_eq!(
            kinds("match RETURN Persona"),
            vec![Kw(Match), Kw(Return), Ident("Persona".into()), Eof]
        );
    }

    #[test]
    fn string_escape_and_float() {
        use TokenKind::*;
        assert_eq!(
            kinds("'it''s' 3.5"),
            vec![Str("it's".into()), Float(3.5), Eof]
        );
    }

    #[test]
    fn error_carries_offset() {
        let err = tokenize("MATCH ^").unwrap_err();
        assert!(err.to_string().contains("offset 6"), "{err}");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p varve-gql`
Expected: compile error.

- [ ] **Step 3: Write minimal implementation**

Prepend to `crates/varve-gql/src/token.rs`:
```rust
use thiserror::Error;

#[derive(Debug, Error)]
pub enum GqlError {
    #[error("lex error at offset {offset}: {msg}")]
    Lex { offset: usize, msg: String },
    #[error("parse error at offset {offset}: {msg}")]
    Parse { offset: usize, msg: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Keyword {
    Insert,
    Match,
    Where,
    Return,
    As,
    True,
    False,
    Null,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TokenKind {
    Kw(Keyword),
    Ident(String),
    Str(String),
    Int(i64),
    Float(f64),
    LParen,
    RParen,
    LBrace,
    RBrace,
    Colon,
    Comma,
    Dot,
    Eq,
    Dollar,
    Eof,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Token {
    pub kind: TokenKind,
    pub offset: usize,
}

fn keyword(word: &str) -> Option<Keyword> {
    match word.to_ascii_uppercase().as_str() {
        "INSERT" => Some(Keyword::Insert),
        "MATCH" => Some(Keyword::Match),
        "WHERE" => Some(Keyword::Where),
        "RETURN" => Some(Keyword::Return),
        "AS" => Some(Keyword::As),
        "TRUE" => Some(Keyword::True),
        "FALSE" => Some(Keyword::False),
        "NULL" => Some(Keyword::Null),
        _ => None,
    }
}

pub fn tokenize(src: &str) -> Result<Vec<Token>, GqlError> {
    let bytes = src.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i] as char;
        let offset = i;
        let kind = match c {
            ' ' | '\t' | '\r' | '\n' => {
                i += 1;
                continue;
            }
            '(' => { i += 1; TokenKind::LParen }
            ')' => { i += 1; TokenKind::RParen }
            '{' => { i += 1; TokenKind::LBrace }
            '}' => { i += 1; TokenKind::RBrace }
            ':' => { i += 1; TokenKind::Colon }
            ',' => { i += 1; TokenKind::Comma }
            '.' => { i += 1; TokenKind::Dot }
            '=' => { i += 1; TokenKind::Eq }
            '$' => { i += 1; TokenKind::Dollar }
            '\'' => {
                i += 1;
                let mut s = String::new();
                loop {
                    match bytes.get(i) {
                        Some(b'\'') if bytes.get(i + 1) == Some(&b'\'') => {
                            s.push('\'');
                            i += 2;
                        }
                        Some(b'\'') => {
                            i += 1;
                            break;
                        }
                        Some(&b) => {
                            s.push(b as char);
                            i += 1;
                        }
                        None => {
                            return Err(GqlError::Lex { offset, msg: "unterminated string".into() })
                        }
                    }
                }
                TokenKind::Str(s)
            }
            '0'..='9' => {
                let start = i;
                while i < bytes.len() && (bytes[i] as char).is_ascii_digit() {
                    i += 1;
                }
                let is_float = i < bytes.len()
                    && bytes[i] == b'.'
                    && bytes.get(i + 1).is_some_and(|b| (*b as char).is_ascii_digit());
                if is_float {
                    i += 1;
                    while i < bytes.len() && (bytes[i] as char).is_ascii_digit() {
                        i += 1;
                    }
                    let text = &src[start..i];
                    TokenKind::Float(text.parse().map_err(|e| GqlError::Lex {
                        offset,
                        msg: format!("bad float: {e}"),
                    })?)
                } else {
                    let text = &src[start..i];
                    TokenKind::Int(text.parse().map_err(|e| GqlError::Lex {
                        offset,
                        msg: format!("bad int: {e}"),
                    })?)
                }
            }
            c if c.is_ascii_alphabetic() || c == '_' => {
                let start = i;
                while i < bytes.len()
                    && ((bytes[i] as char).is_ascii_alphanumeric() || bytes[i] == b'_')
                {
                    i += 1;
                }
                let word = &src[start..i];
                match keyword(word) {
                    Some(kw) => TokenKind::Kw(kw),
                    None => TokenKind::Ident(word.to_string()),
                }
            }
            other => {
                return Err(GqlError::Lex { offset, msg: format!("unexpected character '{other}'") })
            }
        };
        out.push(Token { kind, offset });
    }
    out.push(Token { kind: TokenKind::Eof, offset: bytes.len() });
    Ok(out)
}
```

(v0: ASCII-oriented string handling — multi-byte UTF-8 inside strings passes through bytes and this is exercised properly when slice 7 adds the full literal grammar; keep a `// v0` note.)

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p varve-gql`
Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add crates/varve-gql/
git commit -m "feat: GQL tokenizer for walking-skeleton subset"
```

---

### Task 3: AST + INSERT parser

**Files:**
- Create: `crates/varve-gql/src/ast.rs`
- Create: `crates/varve-gql/src/parser.rs`
- Modify: `crates/varve-gql/src/lib.rs` (`pub mod ast; pub mod parser; pub use parser::parse;`)
- Test: in-module `#[cfg(test)]` in `parser.rs`

**Interfaces:**
- Produces: `varve_gql::ast`:

```rust
#[derive(Debug, Clone, PartialEq)]
pub enum Literal { Int(i64), Float(f64), Str(String), Bool(bool), Null }

#[derive(Debug, Clone, PartialEq)]
pub struct InsertNode {
    pub labels: Vec<String>,
    pub props: Vec<(String, Literal)>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct InsertStmt { pub nodes: Vec<InsertNode> }   // v0: nodes only; edges slice 6

#[derive(Debug, Clone, PartialEq)]
pub struct NodePattern { pub var: String, pub label: Option<String> }

#[derive(Debug, Clone, PartialEq)]
pub enum Expr { PropEq { var: String, prop: String, value: Literal } } // v0; full exprs slice 7

#[derive(Debug, Clone, PartialEq)]
pub struct ReturnItem { pub var: String, pub prop: String, pub alias: Option<String> }

#[derive(Debug, Clone, PartialEq)]
pub struct QueryStmt {
    pub pattern: NodePattern,        // v0: single node; paths slice 6
    pub where_clause: Option<Expr>,
    pub return_items: Vec<ReturnItem>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Statement { Insert(InsertStmt), Query(QueryStmt) }
```

- Produces: `varve_gql::parse(src: &str) -> Result<Statement, GqlError>`.

- [ ] **Step 1: Write the failing test**

In `crates/varve-gql/src/parser.rs`:
```rust
#[cfg(test)]
mod tests {
    use crate::ast::*;
    use crate::parse;

    #[test]
    fn parses_insert_node() {
        let stmt = parse("INSERT (:Person {_id: 1, name: 'Ada'})").unwrap();
        assert_eq!(
            stmt,
            Statement::Insert(InsertStmt {
                nodes: vec![InsertNode {
                    labels: vec!["Person".into()],
                    props: vec![
                        ("_id".into(), Literal::Int(1)),
                        ("name".into(), Literal::Str("Ada".into())),
                    ],
                }]
            })
        );
    }

    #[test]
    fn parses_insert_two_nodes_comma_separated() {
        let stmt = parse("INSERT (:A {x: 1}), (:B {x: 2})").unwrap();
        let Statement::Insert(ins) = stmt else { panic!() };
        assert_eq!(ins.nodes.len(), 2);
    }

    #[test]
    fn insert_without_label_or_props_errors_helpfully() {
        let err = parse("INSERT ()").unwrap_err();
        assert!(err.to_string().contains("label"), "{err}");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p varve-gql parser`
Expected: compile error — modules missing.

- [ ] **Step 3: Write minimal implementation**

`crates/varve-gql/src/ast.rs`: exactly the AST from **Interfaces** above.

`crates/varve-gql/src/parser.rs` (prepend):
```rust
use crate::ast::*;
use crate::token::{tokenize, GqlError, Keyword, Token, TokenKind};

pub fn parse(src: &str) -> Result<Statement, GqlError> {
    Parser { tokens: tokenize(src)?, pos: 0 }.statement()
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    fn peek(&self) -> &TokenKind {
        &self.tokens[self.pos].kind
    }

    fn offset(&self) -> usize {
        self.tokens[self.pos].offset
    }

    fn bump(&mut self) -> TokenKind {
        let t = self.tokens[self.pos].kind.clone();
        self.pos += 1;
        t
    }

    fn err(&self, msg: impl Into<String>) -> GqlError {
        GqlError::Parse { offset: self.offset(), msg: msg.into() }
    }

    fn expect(&mut self, kind: &TokenKind, what: &str) -> Result<(), GqlError> {
        if self.peek() == kind {
            self.pos += 1;
            Ok(())
        } else {
            Err(self.err(format!("expected {what}, found {:?}", self.peek())))
        }
    }

    fn ident(&mut self, what: &str) -> Result<String, GqlError> {
        match self.bump() {
            TokenKind::Ident(s) => Ok(s),
            other => Err(GqlError::Parse {
                offset: self.tokens[self.pos - 1].offset,
                msg: format!("expected {what}, found {other:?}"),
            }),
        }
    }

    fn statement(&mut self) -> Result<Statement, GqlError> {
        match self.peek() {
            TokenKind::Kw(Keyword::Insert) => {
                self.pos += 1;
                self.insert_stmt().map(Statement::Insert)
            }
            TokenKind::Kw(Keyword::Match) => {
                self.pos += 1;
                self.query_stmt().map(Statement::Query)
            }
            _ => Err(self.err("expected INSERT or MATCH")),
        }
    }

    fn insert_stmt(&mut self) -> Result<InsertStmt, GqlError> {
        let mut nodes = vec![self.insert_node()?];
        while *self.peek() == TokenKind::Comma {
            self.pos += 1;
            nodes.push(self.insert_node()?);
        }
        self.expect(&TokenKind::Eof, "end of statement")?;
        Ok(InsertStmt { nodes })
    }

    fn insert_node(&mut self) -> Result<InsertNode, GqlError> {
        self.expect(&TokenKind::LParen, "'('")?;
        let mut labels = Vec::new();
        while *self.peek() == TokenKind::Colon {
            self.pos += 1;
            labels.push(self.ident("label name")?);
        }
        if labels.is_empty() {
            return Err(self.err("INSERT node requires at least one label"));
        }
        let mut props = Vec::new();
        if *self.peek() == TokenKind::LBrace {
            self.pos += 1;
            loop {
                let key = self.ident("property name")?;
                self.expect(&TokenKind::Colon, "':'")?;
                props.push((key, self.literal()?));
                match self.bump() {
                    TokenKind::Comma => continue,
                    TokenKind::RBrace => break,
                    other => {
                        return Err(GqlError::Parse {
                            offset: self.tokens[self.pos - 1].offset,
                            msg: format!("expected ',' or '}}', found {other:?}"),
                        })
                    }
                }
            }
        }
        self.expect(&TokenKind::RParen, "')'")?;
        Ok(InsertNode { labels, props })
    }

    fn literal(&mut self) -> Result<Literal, GqlError> {
        match self.bump() {
            TokenKind::Int(i) => Ok(Literal::Int(i)),
            TokenKind::Float(f) => Ok(Literal::Float(f)),
            TokenKind::Str(s) => Ok(Literal::Str(s)),
            TokenKind::Kw(Keyword::True) => Ok(Literal::Bool(true)),
            TokenKind::Kw(Keyword::False) => Ok(Literal::Bool(false)),
            TokenKind::Kw(Keyword::Null) => Ok(Literal::Null),
            other => Err(GqlError::Parse {
                offset: self.tokens[self.pos - 1].offset,
                msg: format!("expected literal, found {other:?}"),
            }),
        }
    }

    fn query_stmt(&mut self) -> Result<QueryStmt, GqlError> {
        Err(self.err("MATCH not implemented yet")) // Task 4
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p varve-gql`
Expected: Task-3 tests pass (tokenizer tests still green).

- [ ] **Step 5: Commit**

```bash
git add crates/varve-gql/
git commit -m "feat: AST and INSERT parser"
```

---

### Task 4: MATCH / WHERE / RETURN parser

**Files:**
- Modify: `crates/varve-gql/src/parser.rs` (replace the `query_stmt` stub; add tests)

**Interfaces:**
- Consumes/Produces: the `QueryStmt`, `NodePattern`, `Expr`, `ReturnItem` types from Task 3, via `parse()`.

- [ ] **Step 1: Write the failing test**

Append to `parser.rs` tests:
```rust
    #[test]
    fn parses_match_where_return() {
        let stmt = parse("MATCH (p:Person) WHERE p.name = 'Ada' RETURN p.name AS n, p.age").unwrap();
        assert_eq!(
            stmt,
            Statement::Query(QueryStmt {
                pattern: NodePattern { var: "p".into(), label: Some("Person".into()) },
                where_clause: Some(Expr::PropEq {
                    var: "p".into(),
                    prop: "name".into(),
                    value: Literal::Str("Ada".into()),
                }),
                return_items: vec![
                    ReturnItem { var: "p".into(), prop: "name".into(), alias: Some("n".into()) },
                    ReturnItem { var: "p".into(), prop: "age".into(), alias: None },
                ],
            })
        );
    }

    #[test]
    fn match_without_where() {
        let stmt = parse("MATCH (p:Person) RETURN p.name").unwrap();
        let Statement::Query(q) = stmt else { panic!() };
        assert!(q.where_clause.is_none());
        assert_eq!(q.return_items.len(), 1);
    }

    #[test]
    fn match_without_return_errors() {
        assert!(parse("MATCH (p:Person)").is_err());
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p varve-gql parses_match`
Expected: FAIL — "MATCH not implemented yet".

- [ ] **Step 3: Write minimal implementation**

Replace the `query_stmt` stub in `parser.rs`:
```rust
    fn query_stmt(&mut self) -> Result<QueryStmt, GqlError> {
        self.expect(&TokenKind::LParen, "'('")?;
        let var = self.ident("pattern variable")?;
        let label = if *self.peek() == TokenKind::Colon {
            self.pos += 1;
            Some(self.ident("label name")?)
        } else {
            None
        };
        self.expect(&TokenKind::RParen, "')'")?;

        let where_clause = if *self.peek() == TokenKind::Kw(Keyword::Where) {
            self.pos += 1;
            Some(self.prop_eq_expr()?)
        } else {
            None
        };

        self.expect(&TokenKind::Kw(Keyword::Return), "RETURN")?;
        let mut return_items = vec![self.return_item()?];
        while *self.peek() == TokenKind::Comma {
            self.pos += 1;
            return_items.push(self.return_item()?);
        }
        self.expect(&TokenKind::Eof, "end of statement")?;
        Ok(QueryStmt { pattern: NodePattern { var, label }, where_clause, return_items })
    }

    fn prop_eq_expr(&mut self) -> Result<Expr, GqlError> {
        let var = self.ident("variable")?;
        self.expect(&TokenKind::Dot, "'.'")?;
        let prop = self.ident("property name")?;
        self.expect(&TokenKind::Eq, "'='")?;
        Ok(Expr::PropEq { var, prop, value: self.literal()? })
    }

    fn return_item(&mut self) -> Result<ReturnItem, GqlError> {
        let var = self.ident("variable")?;
        self.expect(&TokenKind::Dot, "'.'")?;
        let prop = self.ident("property name")?;
        let alias = if *self.peek() == TokenKind::Kw(Keyword::As) {
            self.pos += 1;
            Some(self.ident("alias")?)
        } else {
            None
        };
        Ok(ReturnItem { var, prop, alias })
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p varve-gql`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add crates/varve-gql/
git commit -m "feat: MATCH/WHERE/RETURN parser"
```

---

### Task 5: LiveTable v0 in varve-index

**Files:**
- Create: `crates/varve-index/Cargo.toml` (deps: `varve-types` path, `arrow` workspace, `thiserror`; `[lints] workspace = true`)
- Create: `crates/varve-index/src/lib.rs` (`pub mod live; pub use live::{LiveTable, IndexError};`)
- Create: `crates/varve-index/src/live.rs`
- Test: in-module `#[cfg(test)]`

**Interfaces:**
- Produces: `varve_index::LiveTable`:

```rust
impl LiveTable {
    pub fn new() -> Self;
    /// Append a node row. v0: overwrite semantics ignored (temporal arrives slice 2).
    pub fn append(&mut self, iid: Iid, labels: Vec<String>, doc: Doc) -> Result<(), IndexError>;
    /// Snapshot rows carrying `label` as one RecordBatch.
    /// Schema: _iid FixedSizeBinary(16) + one nullable column per property name
    /// observed across matching rows (Int64|Float64|Utf8|Boolean by first non-null).
    /// Returns None when no rows match.
    pub fn snapshot_for_label(&self, label: &str) -> Result<Option<RecordBatch>, IndexError>;
    pub fn row_count(&self) -> usize;
}
```

- `IndexError` (thiserror): `MixedPropertyTypes { property: String }`, `Arrow(#[from] arrow::error::ArrowError)`.
- v0 notes: label filtering happens here (seed of scan-level label pruning, spec §10); `Value::Bytes`/`Value::Null` properties: Bytes → `IndexError::MixedPropertyTypes` is NOT the right error — Bytes maps to `Binary` column; Null values are just nulls in whatever column type wins.

- [ ] **Step 1: Write the failing test**

In `crates/varve-index/src/live.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int64Array, StringArray};
    use varve_types::{Doc, Iid, Value};

    fn doc(pairs: &[(&str, Value)]) -> Doc {
        pairs.iter().map(|(k, v)| (k.to_string(), v.clone())).collect()
    }

    fn iid(n: u8) -> Iid {
        Iid::derive("g", "nodes", &[n])
    }

    #[test]
    fn snapshot_builds_columns_from_observed_props() {
        let mut t = LiveTable::new();
        t.append(iid(1), vec!["Person".into()], doc(&[("name", Value::Str("Ada".into())), ("age", Value::Int(36))])).unwrap();
        t.append(iid(2), vec!["Person".into()], doc(&[("name", Value::Str("Bob".into()))])).unwrap();

        let batch = t.snapshot_for_label("Person").unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);
        let names: &StringArray = batch.column_by_name("name").unwrap().as_any().downcast_ref().unwrap();
        assert_eq!(names.value(0), "Ada");
        let ages: &Int64Array = batch.column_by_name("age").unwrap().as_any().downcast_ref().unwrap();
        assert_eq!(ages.value(0), 36);
        assert!(ages.is_null(1)); // Bob has no age
    }

    #[test]
    fn label_filtering() {
        let mut t = LiveTable::new();
        t.append(iid(1), vec!["Person".into()], doc(&[("name", Value::Str("Ada".into()))])).unwrap();
        t.append(iid(2), vec!["City".into()], doc(&[("name", Value::Str("Oslo".into()))])).unwrap();
        assert_eq!(t.snapshot_for_label("Person").unwrap().unwrap().num_rows(), 1);
        assert!(t.snapshot_for_label("Robot").unwrap().is_none());
    }

    #[test]
    fn mixed_property_types_rejected_v0() {
        let mut t = LiveTable::new();
        t.append(iid(1), vec!["P".into()], doc(&[("x", Value::Int(1))])).unwrap();
        t.append(iid(2), vec!["P".into()], doc(&[("x", Value::Str("one".into()))])).unwrap();
        assert!(matches!(
            t.snapshot_for_label("P"),
            Err(IndexError::MixedPropertyTypes { .. })
        ));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p varve-index`
Expected: compile error.

- [ ] **Step 3: Write minimal implementation**

Prepend to `crates/varve-index/src/live.rs`:
```rust
use arrow::array::{ArrayRef, BinaryBuilder, BooleanBuilder, FixedSizeBinaryBuilder, Float64Builder, Int64Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::collections::BTreeMap;
use std::sync::Arc;
use thiserror::Error;
use varve_types::{Doc, Iid, Value};

#[derive(Debug, Error)]
pub enum IndexError {
    #[error("property '{property}' has mixed types across rows (v0 limitation, lifted in slice 2)")]
    MixedPropertyTypes { property: String },
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
}

struct NodeRow {
    iid: Iid,
    labels: Vec<String>,
    doc: Doc,
}

#[derive(Default)]
pub struct LiveTable {
    rows: Vec<NodeRow>,
}

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

    pub fn append(&mut self, iid: Iid, labels: Vec<String>, doc: Doc) -> Result<(), IndexError> {
        self.rows.push(NodeRow { iid, labels, doc });
        Ok(())
    }

    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    pub fn snapshot_for_label(&self, label: &str) -> Result<Option<RecordBatch>, IndexError> {
        let rows: Vec<&NodeRow> =
            self.rows.iter().filter(|r| r.labels.iter().any(|l| l == label)).collect();
        if rows.is_empty() {
            return Ok(None);
        }

        // Column plan: property name → type of first non-null value; conflicts are v0 errors.
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
                            return Err(IndexError::MixedPropertyTypes { property: k.clone() })
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

        Ok(Some(RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?))
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p varve-index`
Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git add crates/varve-index/
git commit -m "feat: LiveTable v0 with label-filtered Arrow snapshots"
```

---

### Task 6: Query planning/execution in varve-plan

**Files:**
- Create: `crates/varve-plan/Cargo.toml` (deps: `varve-gql`, `varve-index` path; `datafusion`, `thiserror` workspace; dev-deps: `tokio` with `macros`, `arrow`, `varve-types` — the workspace `arrow` pin is the same crate DataFusion re-exports, so `arrow::` types in tests unify with `datafusion::arrow::` types)
- Create: `crates/varve-plan/src/lib.rs` (`pub mod exec; pub use exec::{run_query, PlanError};`)
- Create: `crates/varve-plan/src/exec.rs`
- Test: `crates/varve-plan/tests/exec_test.rs`

**Interfaces:**
- Produces: `varve_plan::run_query(stmt: &QueryStmt, live: &LiveTable) -> Result<Vec<RecordBatch>, PlanError>` — async fn. Empty result (label unknown) → `Ok(vec![])`.
- Output schema: one column per `ReturnItem`, named `alias` if present else `"<var>.<prop>"` — but DataFusion column names can't contain `.` without quoting; use alias-or-`prop` (document: duplicate output names allowed only via alias in v0; RETURN of a property absent from all matched rows is a `PlanError::UnknownColumn`).
- `PlanError` (thiserror): `DataFusion(#[from] datafusion::error::DataFusionError)`, `Index(#[from] varve_index::IndexError)`, `UnknownColumn(String)`.
- v0: `GraphPlan` is implicit (Scan→Filter→Project expressed directly via the DataFrame API); the named `GraphPlan` IR is introduced in slice 2 when temporal scopes need a home. This is a deliberate YAGNI: don't build the IR before there are two consumers.

- [ ] **Step 1: Write the failing test**

`crates/varve-plan/tests/exec_test.rs`:
```rust
use arrow::array::StringArray;
use varve_gql::ast::Statement;
use varve_index::LiveTable;
use varve_plan::run_query;
use varve_types::{Doc, Iid, Value};

fn setup() -> LiveTable {
    let mut t = LiveTable::new();
    for (n, name, age) in [(1u8, "Ada", 36i64), (2, "Bob", 41), (3, "Cyd", 36)] {
        let mut doc = Doc::new();
        doc.insert("name".into(), Value::Str(name.into()));
        doc.insert("age".into(), Value::Int(age));
        t.append(Iid::derive("g", "nodes", &[n]), vec!["Person".into()], doc).unwrap();
    }
    t
}

fn query_stmt(src: &str) -> varve_gql::ast::QueryStmt {
    match varve_gql::parse(src).unwrap() {
        Statement::Query(q) => q,
        _ => panic!("not a query"),
    }
}

#[tokio::test]
async fn match_where_return_filters_rows() {
    let live = setup();
    let q = query_stmt("MATCH (p:Person) WHERE p.age = 36 RETURN p.name AS name");
    let batches = run_query(&q, &live).await.unwrap();
    let names: Vec<String> = batches
        .iter()
        .flat_map(|b| {
            let col: &StringArray =
                b.column_by_name("name").unwrap().as_any().downcast_ref().unwrap();
            (0..col.len()).map(|i| col.value(i).to_string()).collect::<Vec<_>>()
        })
        .collect();
    let mut sorted = names.clone();
    sorted.sort();
    assert_eq!(sorted, vec!["Ada", "Cyd"]);
}

#[tokio::test]
async fn unknown_label_returns_empty() {
    let live = setup();
    let q = query_stmt("MATCH (r:Robot) RETURN r.name");
    assert!(run_query(&q, &live).await.unwrap().is_empty());
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p varve-plan`
Expected: compile error.

- [ ] **Step 3: Write minimal implementation**

`crates/varve-plan/src/exec.rs`:
```rust
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use std::sync::Arc;
use thiserror::Error;
use varve_gql::ast::{Expr, Literal, QueryStmt};
use varve_index::LiveTable;

#[derive(Debug, Error)]
pub enum PlanError {
    #[error(transparent)]
    DataFusion(#[from] datafusion::error::DataFusionError),
    #[error(transparent)]
    Index(#[from] varve_index::IndexError),
    #[error("unknown column '{0}' in RETURN/WHERE")]
    UnknownColumn(String),
}

fn to_df_literal(l: &Literal) -> datafusion::prelude::Expr {
    match l {
        Literal::Int(i) => lit(*i),
        Literal::Float(f) => lit(*f),
        Literal::Str(s) => lit(s.clone()),
        Literal::Bool(b) => lit(*b),
        Literal::Null => lit(datafusion::scalar::ScalarValue::Null),
    }
}

pub async fn run_query(stmt: &QueryStmt, live: &LiveTable) -> Result<Vec<RecordBatch>, PlanError> {
    // v0 scan: label pruning happens in the snapshot (spec §10 — labels prune scans).
    let label = stmt.pattern.label.as_deref().unwrap_or("");
    let Some(batch) = live.snapshot_for_label(label)? else {
        return Ok(vec![]);
    };
    let schema = batch.schema();
    let has_col = |name: &str| schema.column_with_name(name).is_some();

    let ctx = SessionContext::new();
    let table = MemTable::try_new(schema.clone(), vec![vec![batch]])?;
    let mut df = ctx.read_table(Arc::new(table))?;

    if let Some(Expr::PropEq { prop, value, .. }) = &stmt.where_clause {
        if !has_col(prop) {
            return Err(PlanError::UnknownColumn(prop.clone()));
        }
        df = df.filter(col(prop.as_str()).eq(to_df_literal(value)))?;
    }

    let mut projection = Vec::new();
    for item in &stmt.return_items {
        if !has_col(&item.prop) {
            return Err(PlanError::UnknownColumn(item.prop.clone()));
        }
        let out_name = item.alias.clone().unwrap_or_else(|| item.prop.clone());
        projection.push(col(item.prop.as_str()).alias(out_name));
    }
    df = df.select(projection)?;

    Ok(df.collect().await?)
}
```

(API sketch note: `MemTable::try_new`, `SessionContext::read_table`, `DataFrame::{filter,select,collect}` have been stable across many DataFusion majors; adapt names if the pinned version differs — tests are the contract.)

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p varve-plan`
Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add crates/varve-plan/ Cargo.toml
git commit -m "feat: query execution over live snapshots via DataFusion"
```

---

### Task 7: Db facade — varve-engine and varve crates, walking-skeleton test

**Files:**
- Create: `crates/varve-engine/Cargo.toml` (deps: `varve-types`, `varve-gql`, `varve-index`, `varve-plan` paths; `datafusion`, `tokio` workspace; `thiserror`)
- Create: `crates/varve-engine/src/lib.rs` (`pub mod db; pub use db::{Db, EngineError, TxReceipt};`)
- Create: `crates/varve-engine/src/db.rs`
- Create: `crates/varve/Cargo.toml` (facade: re-export engine) + `crates/varve/src/lib.rs`
- Create: `crates/varve/examples/hello.rs`
- Test: `crates/varve/tests/walking_skeleton.rs`

**Interfaces:**
- Produces (the public v0 API, spec §11 shape):

```rust
pub struct TxReceipt { pub tx_id: u64 }   // system_time joins in slice 2

impl Db {
    pub fn memory() -> Db;
    /// Execute a mutation statement. v0: INSERT only.
    pub async fn execute(&self, gql: &str) -> Result<TxReceipt, EngineError>;
    /// Execute a read query, returning Arrow batches.
    pub async fn query(&self, gql: &str) -> Result<Vec<RecordBatch>, EngineError>;
}
```

- `EngineError` (thiserror): `Gql(#[from] GqlError)`, `Plan(#[from] PlanError)`, `Index(#[from] IndexError)`, `Type(#[from] TypeError)`, `NotAQuery`, `NotAMutation`.
- `varve` facade crate: `pub use varve_engine::{Db, EngineError, TxReceipt}; pub use datafusion::arrow::record_batch::RecordBatch;`
- Internals: `Db` holds `Arc<RwLock<LiveTable>>` (std RwLock; async wrapper when the writer loop arrives in slice 3) + `AtomicU64` tx counter + `AtomicU64` generated-id counter. `_id` prop used for IID derivation when present; else generated id `Value::Str(format!("varve:gen:{n}"))` inserted into the doc. `_id` is removed from... **no** — `_id` stays in the doc as a property (it is user data).

- [ ] **Step 1: Write the failing test**

`crates/varve/tests/walking_skeleton.rs`:
```rust
use arrow::array::StringArray;
use varve::Db;

#[tokio::test]
async fn insert_then_match_end_to_end() {
    let db = Db::memory();
    db.execute("INSERT (:Person {_id: 1, name: 'Ada'})").await.unwrap();
    db.execute("INSERT (:Person {_id: 2, name: 'Bob'})").await.unwrap();
    db.execute("INSERT (:City {_id: 3, name: 'Oslo'})").await.unwrap();

    let batches = db
        .query("MATCH (p:Person) WHERE p.name = 'Ada' RETURN p.name AS name")
        .await
        .unwrap();

    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 1);
    let names: &StringArray =
        batches[0].column_by_name("name").unwrap().as_any().downcast_ref().unwrap();
    assert_eq!(names.value(0), "Ada");
}

#[tokio::test]
async fn tx_ids_are_monotonic() {
    let db = Db::memory();
    let a = db.execute("INSERT (:X {_id: 1})").await.unwrap();
    let b = db.execute("INSERT (:X {_id: 2})").await.unwrap();
    assert!(b.tx_id > a.tx_id);
}

#[tokio::test]
async fn query_via_execute_is_error_and_vice_versa() {
    let db = Db::memory();
    assert!(db.execute("MATCH (p:P) RETURN p.x").await.is_err());
    assert!(db.query("INSERT (:P {_id: 1})").await.is_err());
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p varve`
Expected: compile error.

- [ ] **Step 3: Write minimal implementation**

`crates/varve-engine/src/db.rs`:
```rust
use datafusion::arrow::record_batch::RecordBatch;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use thiserror::Error;
use varve_gql::ast::{Literal, Statement};
use varve_gql::token::GqlError;
use varve_index::{IndexError, LiveTable};
use varve_plan::PlanError;
use varve_types::{Doc, Iid, TypeError, Value};

#[derive(Debug, Error)]
pub enum EngineError {
    #[error(transparent)]
    Gql(#[from] GqlError),
    #[error(transparent)]
    Plan(#[from] PlanError),
    #[error(transparent)]
    Index(#[from] IndexError),
    #[error(transparent)]
    Type(#[from] TypeError),
    #[error("statement is a query; use query()")]
    NotAMutation,
    #[error("statement is a mutation; use execute()")]
    NotAQuery,
    #[error("internal lock poisoned")]
    Poisoned,
}

#[derive(Debug, Clone, Copy)]
pub struct TxReceipt {
    pub tx_id: u64,
}

pub struct Db {
    live: Arc<RwLock<LiveTable>>,
    tx_counter: AtomicU64,
    id_counter: AtomicU64,
}

fn literal_to_value(l: &Literal) -> Value {
    match l {
        Literal::Int(i) => Value::Int(*i),
        Literal::Float(f) => Value::Float(*f),
        Literal::Str(s) => Value::Str(s.clone()),
        Literal::Bool(b) => Value::Bool(*b),
        Literal::Null => Value::Null,
    }
}

impl Db {
    pub fn memory() -> Db {
        Db {
            live: Arc::new(RwLock::new(LiveTable::new())),
            tx_counter: AtomicU64::new(0),
            id_counter: AtomicU64::new(0),
        }
    }

    pub async fn execute(&self, gql: &str) -> Result<TxReceipt, EngineError> {
        let Statement::Insert(ins) = varve_gql::parse(gql)? else {
            return Err(EngineError::NotAMutation);
        };
        let tx_id = self.tx_counter.fetch_add(1, Ordering::SeqCst) + 1;
        let mut live = self.live.write().map_err(|_| EngineError::Poisoned)?;
        for node in &ins.nodes {
            let mut doc: Doc =
                node.props.iter().map(|(k, v)| (k.clone(), literal_to_value(v))).collect();
            let id = match doc.get("_id") {
                Some(v) => v.clone(),
                None => {
                    // v0: process-local generated ids; proper generation in slice 2
                    let n = self.id_counter.fetch_add(1, Ordering::SeqCst);
                    let v = Value::Str(format!("varve:gen:{n}"));
                    doc.insert("_id".into(), v.clone());
                    v
                }
            };
            let iid = Iid::derive("default", "nodes", &id.id_bytes()?);
            live.append(iid, node.labels.clone(), doc)?;
        }
        Ok(TxReceipt { tx_id })
    }

    pub async fn query(&self, gql: &str) -> Result<Vec<RecordBatch>, EngineError> {
        let Statement::Query(q) = varve_gql::parse(gql)? else {
            return Err(EngineError::NotAQuery);
        };
        // v0: clone-free read under lock; snapshotting becomes cheap-Arc in slice 2
        let live = self.live.read().map_err(|_| EngineError::Poisoned)?;
        Ok(varve_plan::run_query(&q, &live).await?)
    }
}
```

`crates/varve/src/lib.rs`:
```rust
pub use varve_engine::{Db, EngineError, TxReceipt};
pub use datafusion::arrow::record_batch::RecordBatch;
```

`crates/varve/examples/hello.rs`:
```rust
use varve::Db;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Db::memory();
    db.execute("INSERT (:Person {_id: 1, name: 'Ada'})").await?;
    let batches = db.query("MATCH (p:Person) RETURN p.name AS name").await?;
    println!("{}", datafusion::arrow::util::pretty::pretty_format_batches(&batches)?);
    Ok(())
}
```

(Add `arrow` + `datafusion` to `crates/varve/Cargo.toml` deps for the test/example; `tokio` with `macros` as dev-dependency.)

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p varve && cargo run --example hello -p varve`
Expected: 3 tests pass; example prints a one-row table with "Ada".

- [ ] **Step 5: Run the full gate**

Run: `just check`
Expected: green. Fix fmt/clippy fallout in this task's code only.

- [ ] **Step 6: Commit**

```bash
git add crates/
git commit -m "feat: Db facade — walking skeleton complete, INSERT→MATCH end to end"
```

---

## Slice exit checklist

- [ ] `just check` green; walking-skeleton test + hello example demonstrably work.
- [ ] Update `docs/plans/STATUS.md`: slice 1 complete; demo = `cargo run --example hello -p varve`; record the DataFusion/arrow versions actually pinned and any API adaptations made (they feed the slice-2 plan).
- [ ] Tick slice 1 boxes in `docs/plans/varve-v1-roadmap.md`; commit.
