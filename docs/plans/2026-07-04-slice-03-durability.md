# Slice 3: Durability — pluggable log, group commit, crash safety

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Every acknowledged transaction survives `kill -9`. Writes flow through a log-serialized writer loop with group commit onto a pluggable `Log` (spec §6): `varve-log` gains the `Log` trait, a protobuf record envelope (prost, per-table Arrow IPC effects), a `memory` backend, and a `local` backend (segmented append-only files, CRC32C per record, fsync-before-ack, torn-tail truncation). `Db::open(Config)` selects the backend by name through the registry and replays the log into the live index. A crash harness in `varve-testkit` kills a child process at injected fault points and proves the recovery contract.

**Architecture:** The writer loop (spec §3, D3) is the single serialization point: `Db::execute` parses and submits statements to a bounded queue; a dedicated tokio task assigns `(tx_id, system_time)`, resolves DML to events, group-commits batches of records to the `Log` (window + size triggers), applies events to the `LiveTable` **only after the batch is durable**, then acks. This dissolves slice 2's documented single-writer clock assumption and DELETE's `#[allow(clippy::await_holding_lock)]` — concurrent `execute()` is now supported. The `Clock` becomes a pluggable trait (honoring the slice-2 STATUS decision "the pluggable Clock registry interface arrives with durability config wiring"), and the `Registries` aggregate deferred since slice 0 lands in `varve-engine`.

**Tech Stack:** Adds `prost` (protobuf envelope, derive-only — NO protoc/build.rs), `crc32c` (Castagnoli CRC), `tempfile` (dev). Extends the workspace `tokio` features with `sync` + `time` (mpsc/oneshot channels, group-commit window timer).

## Global Constraints

- All roadmap Global Constraints apply: TDD (failing test first, minimal implementation, commit per green cycle); `cargo clippy --workspace --all-targets -- -D warnings` clean; `unwrap()`/`expect()` forbidden in library code (allowed in tests via repo-root `clippy.toml` — **note:** the `crash_child` test *binary* is not a test target, so it carries an explicit `#![allow(clippy::expect_used, clippy::unwrap_used)]` with a justifying comment); errors via `thiserror` per crate; conventional-commit prefixes; **no `Co-Authored-By` trailer**.
- **Sovereignty (spec §1, D7):** the local log uses nothing beyond plain files + fsync; the trait's durability contract ("Ok from `append` ⇒ batch survives `kill -9`") is implementable by plain S3 PUT (slice 5). No CAS anywhere.
- **Bitemporal invariant (spec §5.2):** untouched — the log stores the same append-only events; `_system_to` stays derived.
- **Determinism:** the canonical Value/Doc byte encoding and the protobuf wire encoding are golden-tested (exact bytes pinned, like slice 0's `Iid`/`LogPosition` vectors). Replay is a pure fold over the log. Arrow IPC bytes are NOT golden-pinned (arrow does not guarantee byte stability across versions); IPC is covered by round-trip + property tests instead.
- **Dependency pinning (verified against crates.io + compiled in a live probe on 2026-07-04):** `prost = "0.14"` (resolves 0.14.4 — derive works without protoc: `#[derive(::prost::Message)]` + `encode_to_vec`/`decode`/`encoded_len`), `crc32c = "0.6"` (resolves 0.6.8 — `crc32c()`, `crc32c_append()`; check value `crc32c(b"123456789") == 0xE3069283` verified), `tempfile = "3"` (resolves 3.27.0, dev-only). `arrow` 58.3 IPC `StreamWriter`/`StreamReader` round-trip verified against the exact event schema below. **The test code in this plan is the contract** — if an API sketch differs from the pinned crate APIs, adapt the implementation, not the test.
- We are in development: **no backward compatibility, no migration shims** — formats and APIs change freely; existing call sites are updated, not wrapped.

## Design decisions locked by this plan

1. **Writer-loop semantics (spec §3 order: group-commit → apply → ack).** Statements are resolved *inside* the loop (serial, so tx N sees tx N−1). Events are applied to the live index **only after** the batch is durable, and acks fire after apply — so an acked tx is both durable and visible (read-your-writes), and queries can never observe un-durable data. A **reading** statement (v1: `DELETE`; slice 7's `SET` inherits the rule) first flushes any staged batch so its snapshot includes every earlier tx.
2. **Failed append ⇒ clean rollback.** Because nothing was applied, a failed durable append just acks `EngineError::CommitFailed` to every tx in the batch and the loop continues with consistent state. `LocalLog` restores its file to the pre-batch length (and poisons itself if the restore fails). A failed *apply* after a durable append (impossible by construction — the only append error is out-of-order, and the loop is the sole writer) also acks `CommitFailed`; a restart replays the durable log and heals.
3. **Positions are per record; the batch is the durability unit.** `append(Vec<LogRecord>)` durably writes all records with ONE fsync (later: one S3 PUT) and returns the first record's `LogPosition`. One record = one transaction, so tx atomicity holds even if a torn batch leaves a durable prefix (each surviving record is a complete tx; none were acked). This is forward-compatible with slice 5's one-object-per-batch log (offsets assigned locally).
4. **Envelope = protobuf via prost derive; effects = per-table Arrow IPC.** `LogRecord { tx_id, system_time_us, user, effects }` per spec §6 (`user` carried as an empty string in v1 — protobuf makes it free). Docs and labels ride inside the Arrow event batch as ONE nullable `payload` Binary column holding a canonical byte encoding (owned by us, golden-tested). Columnar doc structs (dense unions) arrive with slice 4's block format; the log codec deliberately does NOT import the live table's v0 mixed-type restriction — the log accepts any doc.
5. **Generated ids become durable:** `varve:gen:{tx_id}:{ordinal}` replaces slice 1's process-local counter (`varve:gen:{n}`), which would reset on restart and silently merge histories. `tx_id` is recovered from the log, so uniqueness survives restarts. (This discharges the `// v0` comment in `db.rs`: "user-durable ids arrive in slice 3".)
6. **`Clock` is now a pluggable trait** (`varve_engine::Clock`; builtin `system` = the existing `MonotonicClock`, gaining `advance_to(floor)` for recovery). **`Registries` aggregate** (`{ log, clock }`, `with_builtins()`) lands in `varve-engine`; `Db::open_with(&config, &registries)` is the embedder extension point (spec §4 "embedding applications may register custom implementations before opening a database"). **`BuildContext` stays deferred:** `log/local` gets its directory via `[log.local] dir` config, which suffices (slice-0 YAGNI note holds; revisit when a factory needs another *component*, e.g. slice 5 cache tiers).
7. **`group_commit_max_bytes` is an integer byte count** (default `8388608`). The spec §4 sketch shows the string `"8MiB"`; human-size parsing is config polish deferred to the server slice. Deviation recorded here and in STATUS.
8. **Crash-fault surface:** two feature-gated hooks inside `LocalLog::append` (`pre-append` = before any byte is written, `post-append` = after fsync, before Ok/ack) armed via a trigger file; the child announces `CRASH_POINT <name>` on stdout and parks; the parent test delivers a real `kill -9` (SIGKILL). `post-ack` needs no hook (observable by the child). The roadmap's "no unacked tx visible" is formalized as: a tx killed **before durability** (pre-append) never surfaces; a durable-but-unacked tx (post-append) MAY surface after restart — the standard WAL contract (the client saw no ack and must treat the tx as unknown).
9. **Bounded submission queue:** constant `SUBMISSION_QUEUE_LEN = 256` (senders await when full). Making it config-driven with real backpressure semantics (429/wait, memory watermarks) is slice 10.
10. **No lock file on the local log dir.** Exactly-one-writer is enforced by deployment (spec §12); two processes opening the same dir concurrently is undefined behavior in v1 (slice 10's `writer.json` heartbeat adds a best-effort guard). Sequential open-after-drop (restart, tests) is fully supported.
11. **`Db::memory()` uses `window = 0`** (flush whatever is already queued, never wait — there is no fsync to amortize), keeping embedded in-memory latency at slice-2 levels. `Db::open` reads `group_commit_window_ms` (default 15) / `group_commit_max_bytes` from `[log]`.

## File structure

```
Cargo.toml                                   # + prost, crc32c, tempfile; tokio features += sync,time
.github/workflows/ci.yml                     # + crash-matrix job (100 iterations)
justfile                                     # + crash recipe
crates/
  varve-types/src/value.rs                   # + canonical Value/Doc codec (encode_into/decode_from/encode_doc/decode_doc)
  varve-types/src/position.rs                # + TypeError::MalformedEncoding; LogPosition::ZERO, ::advance(n)
  varve-types/src/lib.rs                     # + re-exports
  varve-log/                                 # NEW CRATE (spec §15)
    Cargo.toml                               #   features: fault-injection = []
    src/lib.rs                               #   modules, re-exports, log_registry()
    src/record.rs                            #   LogRecord, TableEffects (prost derive), to_wire/from_wire/wire_len
    src/log.rs                               #   Log trait (append/read_range/tail), LogError
    src/memory.rs                            #   MemoryLog + MemoryLogFactory
    src/local.rs                             #   LocalLog (segments, CRC32C frames, fsync, torn-tail recovery),
                                             #   LocalLogFactory, crash_point() hooks
    tests/local_log.rs                       #   happy path + factory
    tests/recovery.rs                        #   torn tail, corruption, idempotence
  varve-config/src/config.rs                 # + ConfigSection::empty(); rustdoc sweep; from_file tests
  varve-index/src/codec.rs                   # NEW: events ↔ Arrow IPC (payload column)
  varve-index/src/live.rs                    # + IndexError::Codec
  varve-index/src/lib.rs                     # + pub mod codec, re-exports
  varve-index/Cargo.toml                     # + proptest dev-dep
  varve-plan/src/exec.rs                     # matching_iids split: matching_snapshot + iids_from_snapshot
  varve-plan/src/lib.rs                      # + re-exports
  varve-engine/src/clock.rs                  # Clock trait; MonotonicClock impls it + advance_to; SystemClockFactory
  varve-engine/src/registries.rs             # NEW: Registries { log, clock } + with_builtins()
  varve-engine/src/writer.rs                 # NEW: writer loop, group commit, Submission, WriterConfig
  varve-engine/src/db.rs                     # REWRITE: submit-based execute, open/open_with/local, replay
  varve-engine/src/lib.rs                    # + pub mod clock/registries, mod writer
  varve-engine/Cargo.toml                    # + varve-log, varve-config, tokio, serde; dev: async-trait
  varve-testkit/Cargo.toml                   # + varve, varve-log (features=[fault-injection]), tokio, tempfile(dev)
  varve-testkit/src/bin/crash_child.rs       # NEW: crash-test child binary
  varve-testkit/tests/crash_recovery.rs      # NEW: kill -9 fault matrix
  varve/src/lib.rs                           # + Config, ConfigError, Registries re-exports
  varve/Cargo.toml                           # + varve-config; dev: tempfile
  varve/tests/durability.rs                  # NEW: restart/replay acceptance
  varve/examples/write_bench.rs              # NEW: throughput smoke bench (STATUS exit criterion)
```

No XTDB porting references apply to this slice (the roadmap cites none for slice 3; XTDB's log is Kafka/JVM-shaped — spec §14 explicitly replaces it with the object-store/local log design specced in §6).

---

### Task 1: Canonical Value/Doc byte codec in varve-types

The log stores each Put event's labels+doc as opaque bytes inside an Arrow Binary column (design decision 4). This task provides the deterministic, golden-pinned byte encoding for `Value` and `Doc`.

**Files:**
- Modify: `crates/varve-types/src/value.rs`
- Modify: `crates/varve-types/src/position.rs` (add `TypeError::MalformedEncoding`)
- Modify: `crates/varve-types/src/lib.rs` (re-export `encode_doc`, `decode_doc`)
- Test: in-module `#[cfg(test)]` in `value.rs`

**Interfaces:**
- Consumes: existing `Value`, `Doc`, `TypeError`.
- Produces:
  - `Value::encode_into(&self, out: &mut Vec<u8>)` — appends the canonical encoding: tag byte `0x00` Null · `0x01` Bool + 1 byte (0/1) · `0x02` Int + 8 bytes LE · `0x03` Float + 8 bytes LE (f64 bits — NaN round-trips bit-exactly) · `0x04` Str + u32 LE length + UTF-8 · `0x05` Bytes + u32 LE length + raw.
  - `Value::decode_from(input: &mut &[u8]) -> Result<Value, TypeError>` — consumes exactly one value from the front of `input`.
  - `varve_types::encode_doc(doc: &Doc) -> Vec<u8>` — u32 LE entry count, then per entry: u32 LE key length + key UTF-8 + value encoding. BTreeMap iteration ⇒ deterministic output.
  - `varve_types::decode_doc(input: &mut &[u8]) -> Result<Doc, TypeError>`.
  - `TypeError::MalformedEncoding(String)` — new variant: `#[error("malformed canonical encoding: {0}")]`.
- NOTE: these tags are unrelated to `Value::id_bytes()`'s tags (that is a hash-input encoding for IID derivation) — a code comment must say so at both sites to prevent future conflation.

- [x] **Step 1: Write the failing test**

Append to the `tests` module in `crates/varve-types/src/value.rs`:

```rust
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
```

The test module also needs `use super::*;` (already present).

- [x] **Step 2: Run test to verify it fails**

Run: `cargo test -p varve-types value`
Expected: compile error — `encode_doc` / `encode_into` not defined.

- [x] **Step 3: Write minimal implementation**

Add `TypeError::MalformedEncoding` in `crates/varve-types/src/position.rs`:

```rust
    #[error("malformed canonical encoding: {0}")]
    MalformedEncoding(String),
```

Add to `crates/varve-types/src/value.rs` (below the `id_bytes` impl):

```rust
// Canonical byte codec for log payloads (slice 3). NOTE: these tags are
// unrelated to `id_bytes()`'s tags above — that encoding is a hash input for
// IID derivation; this one is the durable wire format for stored values.
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
    /// Appends this value's canonical byte encoding (deterministic; the log
    /// wire format for stored property values).
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
            Value::Bytes(b) => {
                out.push(TAG_BYTES);
                write_len_prefixed(out, b);
            }
        }
    }

    /// Consumes exactly one canonically encoded value from the front of `input`.
    pub fn decode_from(input: &mut &[u8]) -> Result<Value, TypeError> {
        let tag = take(input, 1)?[0];
        match tag {
            TAG_NULL => Ok(Value::Null),
            TAG_BOOL => match take(input, 1)?[0] {
                0 => Ok(Value::Bool(false)),
                1 => Ok(Value::Bool(true)),
                other => Err(TypeError::MalformedEncoding(format!("bool byte {other}"))),
            },
            TAG_INT => {
                let arr: [u8; 8] = take(input, 8)?
                    .try_into()
                    .map_err(|_| TypeError::MalformedEncoding("i64".into()))?;
                Ok(Value::Int(i64::from_le_bytes(arr)))
            }
            TAG_FLOAT => {
                let arr: [u8; 8] = take(input, 8)?
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
            other => Err(TypeError::MalformedEncoding(format!("unknown tag {other:#04x}"))),
        }
    }
}

/// Canonical byte encoding of a whole document (deterministic — `Doc` is a
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

/// Consumes exactly one canonically encoded document from the front of `input`.
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
```

Update `crates/varve-types/src/lib.rs`:

```rust
pub mod iid;
pub mod position;
pub mod temporal;
pub mod value;
pub use iid::Iid;
pub use position::{LogPosition, TypeError};
pub use temporal::{Instant, TemporalBounds, TemporalDimension};
pub use value::{decode_doc, encode_doc, Doc, Value};
```

Also add a mirror comment on `id_bytes` in `value.rs` (above the existing doc comment line, extend it):

```rust
    /// Canonical bytes for IID derivation (type-tagged to avoid cross-type collisions).
    /// NOTE: unrelated to the storage codec tags below (`encode_into`) — this is a
    /// hash input, never decoded.
```

- [x] **Step 4: Run test to verify it passes**

Run: `cargo test -p varve-types`
Expected: all pass (existing + 4 new).

- [x] **Step 5: Commit**

```bash
git add crates/varve-types/
git commit -m "feat: canonical Value/Doc byte codec for log payloads"
```

---
### Task 2: varve-log crate — record envelope (prost) and Log trait

**Files:**
- Modify: root `Cargo.toml` (workspace deps: `prost`, `crc32c`, `tempfile`; tokio features += `sync`, `time`)
- Modify: `crates/varve-types/src/position.rs` (add `LogPosition::ZERO`, `LogPosition::advance`)
- Create: `crates/varve-log/Cargo.toml`
- Create: `crates/varve-log/src/lib.rs`
- Create: `crates/varve-log/src/record.rs`
- Create: `crates/varve-log/src/log.rs`
- Test: in-module `#[cfg(test)]` in `record.rs` and `position.rs`

**Interfaces:**
- Consumes: `varve_types::{LogPosition, TypeError}`.
- Produces `varve_types::LogPosition` additions: `pub const ZERO: LogPosition` (epoch 0, offset 0); `pub fn advance(&self, n: u64) -> Result<LogPosition, TypeError>` (offset + n within the same epoch; `OffsetOverflow` past 48 bits).
- Produces `varve_log::record::{LogRecord, TableEffects}` (re-exported at crate root) — the spec §6 protobuf envelope, hand-derived (NO protoc, NO build.rs):

```rust
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TableEffects {
    #[prost(string, tag = "1")]
    pub table: String,
    #[prost(bytes = "vec", tag = "2")]
    pub arrow_ipc: Vec<u8>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LogRecord {
    #[prost(uint64, tag = "1")]
    pub tx_id: u64,
    #[prost(int64, tag = "2")]
    pub system_time_us: i64,
    #[prost(string, tag = "3")]
    pub user: String, // empty in v1; carried per spec §6 envelope shape
    #[prost(message, repeated, tag = "4")]
    pub effects: Vec<TableEffects>,
}

impl LogRecord {
    pub fn to_wire(&self) -> Vec<u8>;                              // prost encode_to_vec
    pub fn from_wire(bytes: &[u8]) -> Result<LogRecord, LogError>; // prost decode
    pub fn wire_len(&self) -> usize;                               // prost encoded_len (no alloc)
}
```

(prost's `Message` derive also generates `Debug` and `Default` impls — verified in the probe.)
- Produces `varve_log::log::{Log, LogError}` (re-exported at crate root):

```rust
#[derive(Debug, thiserror::Error)]
pub enum LogError {
    #[error("log I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("cannot append an empty record batch")]
    EmptyAppend,
    #[error("log record decode failed: {0}")]
    Decode(#[from] prost::DecodeError),
    #[error("log corrupt in {path} at byte offset {offset}: {reason}")]
    Corrupt { path: String, offset: u64, reason: String },
    #[error("log poisoned by an earlier failed append; reopen to recover")]
    Poisoned,
    #[error(transparent)]
    Type(#[from] varve_types::TypeError),
}

/// Ordered, durable stream of transaction records (spec §6). One `LogRecord`
/// per transaction. `append` writes a batch of records as ONE durable unit
/// (one fsync / one object PUT); records receive consecutive positions.
/// Durability contract: when `append` returns Ok, every record in the batch
/// survives `kill -9`.
#[async_trait::async_trait]
pub trait Log: Send + Sync {
    /// Durably append `records`; returns the position of the FIRST record.
    async fn append(&self, records: Vec<LogRecord>) -> Result<LogPosition, LogError>;
    /// Records with `from <= position < to`, in position order.
    async fn read_range(
        &self,
        from: LogPosition,
        to: LogPosition,
    ) -> Result<Vec<(LogPosition, LogRecord)>, LogError>;
    /// Every record at or after `from`. v1 tailing is poll-based: callers
    /// re-invoke to observe new records (streaming tail arrives with the
    /// query-node role, slice 9).
    async fn tail(&self, from: LogPosition) -> Result<Vec<(LogPosition, LogRecord)>, LogError> {
        self.read_range(from, LogPosition::from_u64(u64::MAX)).await
    }
}
```

- [x] **Step 1: Workspace + crate scaffolding**

Root `Cargo.toml` — change the tokio line and add three deps in `[workspace.dependencies]`:

```toml
tokio = { version = "1", features = ["rt-multi-thread", "macros", "sync", "time"] }
async-trait = "0.1"          # (already present)
prost = "0.14"
crc32c = "0.6"
tempfile = "3"
```

`crates/varve-log/Cargo.toml`:

```toml
[package]
name = "varve-log"
version.workspace = true
edition.workspace = true
license.workspace = true

[features]
# Test-only crash hooks for the varve-testkit kill -9 harness. Inert unless
# the VARVE_CRASH_TRIGGER env var points at an armed trigger file.
fault-injection = []

[dependencies]
varve-types = { path = "../varve-types" }
varve-config = { path = "../varve-config" }
prost = { workspace = true }
crc32c = { workspace = true }
serde = { workspace = true }
thiserror = { workspace = true }
async-trait = { workspace = true }
tokio = { workspace = true }

[dev-dependencies]
tempfile = { workspace = true }

[lints]
workspace = true
```

`crates/varve-log/src/lib.rs` (modules land across Tasks 2–5; start with):

```rust
pub mod log;
pub mod record;

pub use log::{Log, LogError};
pub use record::{LogRecord, TableEffects};
```

- [x] **Step 2: Write the failing tests**

In-module tests in `crates/varve-log/src/record.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> LogRecord {
        LogRecord {
            tx_id: 1,
            system_time_us: 2,
            user: String::new(),
            effects: vec![TableEffects {
                table: "nodes".into(),
                arrow_ipc: vec![0xAA],
            }],
        }
    }

    #[test]
    fn wire_round_trips() {
        let rec = sample();
        let bytes = rec.to_wire();
        assert_eq!(rec.wire_len(), bytes.len());
        assert_eq!(LogRecord::from_wire(&bytes).unwrap(), rec);
    }

    #[test]
    fn wire_golden_bytes() {
        // Pins field numbers and wire types (protobuf wire format is stable,
        // so exact bytes are safe to golden-test — unlike Arrow IPC).
        assert_eq!(
            sample().to_wire(),
            vec![
                0x08, 0x01, // field 1 varint: tx_id = 1
                0x10, 0x02, // field 2 varint: system_time_us = 2
                // field 3 (user) omitted: proto3 default (empty string)
                0x22, 0x0A, // field 4, length-delimited, 10 bytes
                0x0A, 0x05, b'n', b'o', b'd', b'e', b's', // effects.table
                0x12, 0x01, 0xAA, // effects.arrow_ipc
            ]
        );
    }

    #[test]
    fn from_wire_rejects_garbage() {
        assert!(matches!(
            LogRecord::from_wire(&[0xFF, 0xFF, 0xFF]),
            Err(LogError::Decode(_))
        ));
    }
}
```

Append to the `tests` module in `crates/varve-types/src/position.rs`:

```rust
    #[test]
    fn zero_and_advance() {
        assert_eq!(LogPosition::ZERO, LogPosition::new(0, 0).unwrap());
        let p = LogPosition::new(3, 10).unwrap();
        assert_eq!(p.advance(0).unwrap(), p);
        assert_eq!(p.advance(5).unwrap(), LogPosition::new(3, 15).unwrap());
        assert!(p.advance(1u64 << 48).is_err()); // overflows the 48-bit offset
    }
```

- [x] **Step 3: Run tests to verify they fail**

Run: `cargo test -p varve-types position && cargo test -p varve-log`
Expected: compile errors — `ZERO`/`advance` missing; `varve-log` modules missing.

- [x] **Step 4: Write minimal implementation**

`crates/varve-types/src/position.rs` — add inside `impl LogPosition`:

```rust
    /// Epoch 0, offset 0 — where replay starts.
    pub const ZERO: LogPosition = LogPosition(0);

    /// The position `n` records after this one, within the same epoch.
    pub fn advance(&self, n: u64) -> Result<Self, TypeError> {
        Self::new(self.epoch(), self.offset() + n)
    }
```

`crates/varve-log/src/record.rs`:

```rust
use crate::log::LogError;
use prost::Message;

/// Resolved effects for one table: Arrow IPC bytes of the event batch
/// (spec §6 "Arrow for the payload").
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TableEffects {
    #[prost(string, tag = "1")]
    pub table: String,
    #[prost(bytes = "vec", tag = "2")]
    pub arrow_ipc: Vec<u8>,
}

/// One transaction's log record — the spec §6 protobuf envelope
/// `{tx_id, system_time, user, effects}`. `user` is carried empty in v1.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LogRecord {
    #[prost(uint64, tag = "1")]
    pub tx_id: u64,
    #[prost(int64, tag = "2")]
    pub system_time_us: i64,
    #[prost(string, tag = "3")]
    pub user: String,
    #[prost(message, repeated, tag = "4")]
    pub effects: Vec<TableEffects>,
}

impl LogRecord {
    /// Protobuf wire bytes (the payload framed by each log backend).
    pub fn to_wire(&self) -> Vec<u8> {
        self.encode_to_vec()
    }

    pub fn from_wire(bytes: &[u8]) -> Result<LogRecord, LogError> {
        Ok(<LogRecord as Message>::decode(bytes)?)
    }

    /// Encoded size without allocating (group-commit size accounting).
    pub fn wire_len(&self) -> usize {
        self.encoded_len()
    }
}
```

`crates/varve-log/src/log.rs`: exactly the `LogError` + `Log` trait from **Interfaces** above (with `use crate::record::LogRecord;` and `use varve_types::LogPosition;`).

- [x] **Step 5: Run tests to verify they pass**

Run: `cargo test -p varve-types && cargo test -p varve-log`
Expected: all pass (3 new record tests, 1 new position test).

- [x] **Step 6: Commit**

```bash
git add Cargo.toml crates/varve-types/ crates/varve-log/
git commit -m "feat: varve-log crate with Log trait and prost record envelope"
```

---

### Task 3: MemoryLog on the trait + registry factory

Formalizes slice 1's "in-memory log v0" (which was just a tx counter in `Db`) onto the real `Log` trait, and starts the `log` registry (roadmap: factories `log/memory`, `log/local`).

**Files:**
- Create: `crates/varve-log/src/memory.rs`
- Modify: `crates/varve-log/src/lib.rs` (module, re-exports, `log_registry()`)
- Modify: `crates/varve-config/src/config.rs` (add `ConfigSection::empty()`)
- Test: `crates/varve-log/tests/memory_log.rs`; one test appended to `crates/varve-config/tests/config_test.rs`

**Interfaces:**
- Consumes: `Log`, `LogError`, `LogRecord` (Task 2); `varve_config::{ComponentFactory, ConfigSection, Registry, RegistryError}`.
- Produces:
  - `varve_log::MemoryLog` — `MemoryLog::new() -> MemoryLog`; implements `Log`. Volatile `Mutex<Vec<(LogPosition, LogRecord)>>`; positions start at `LogPosition::ZERO` and are consecutive across appends.
  - `varve_log::MemoryLogFactory` — `ComponentFactory<dyn Log>`, `name() == "memory"`.
  - `varve_log::log_registry() -> Registry<dyn Log>` — kind `"log"`, builtins registered (this task: `memory`; Task 5 adds `local`).
  - `varve_config::ConfigSection::empty() -> ConfigSection` — empty table, used wherever a config omits an optional `[section]`.

- [x] **Step 1: Write the failing tests**

`crates/varve-log/tests/memory_log.rs`:

```rust
use varve_log::{log_registry, LogError, LogRecord, MemoryLog};
use varve_types::LogPosition;

fn rec(tx_id: u64) -> LogRecord {
    LogRecord {
        tx_id,
        system_time_us: tx_id as i64,
        user: String::new(),
        effects: vec![],
    }
}

fn pos(offset: u64) -> LogPosition {
    LogPosition::new(0, offset).unwrap()
}

#[tokio::test]
async fn append_assigns_consecutive_positions_per_record() {
    let log = MemoryLog::new();
    // batch of 2, then batch of 1 — positions 0,1,2; append returns the first
    assert_eq!(log.append(vec![rec(1), rec(2)]).await.unwrap(), pos(0));
    assert_eq!(log.append(vec![rec(3)]).await.unwrap(), pos(2));

    let all = log.tail(LogPosition::ZERO).await.unwrap();
    assert_eq!(
        all.iter().map(|(p, r)| (*p, r.tx_id)).collect::<Vec<_>>(),
        vec![(pos(0), 1), (pos(1), 2), (pos(2), 3)]
    );
}

#[tokio::test]
async fn read_range_is_half_open() {
    let log = MemoryLog::new();
    log.append(vec![rec(1), rec(2), rec(3)]).await.unwrap();
    let mid = log.read_range(pos(1), pos(2)).await.unwrap();
    assert_eq!(mid.len(), 1);
    assert_eq!(mid[0].1.tx_id, 2);
    assert!(log.read_range(pos(3), pos(10)).await.unwrap().is_empty());
}

#[tokio::test]
async fn tail_from_midpoint() {
    let log = MemoryLog::new();
    log.append(vec![rec(1), rec(2), rec(3)]).await.unwrap();
    let tail = log.tail(pos(1)).await.unwrap();
    assert_eq!(tail.len(), 2);
    assert_eq!(tail[0].1.tx_id, 2);
}

#[tokio::test]
async fn empty_append_is_rejected() {
    let log = MemoryLog::new();
    assert!(matches!(
        log.append(vec![]).await,
        Err(LogError::EmptyAppend)
    ));
}

#[tokio::test]
async fn registry_builds_memory_by_name_and_lists_available_on_unknown() {
    use varve_config::Config;
    let reg = log_registry();
    let cfg = Config::from_toml_str("[log]\nbackend = \"memory\"")
        .unwrap()
        .section("log")
        .unwrap();
    let log = reg.build("memory", &cfg).unwrap();
    log.append(vec![rec(1)]).await.unwrap();
    assert_eq!(log.tail(LogPosition::ZERO).await.unwrap().len(), 1);

    let err = reg.build("kafka", &cfg).unwrap_err().to_string();
    assert!(err.contains("kafka"), "{err}");
    assert!(err.contains("memory"), "{err}");
}
```

Append to `crates/varve-config/tests/config_test.rs`:

```rust
#[test]
fn empty_section_has_no_backend_and_deserializes_defaults() {
    use varve_config::ConfigSection;

    #[derive(Deserialize, Debug, PartialEq)]
    struct Tuning {
        #[serde(default = "one")]
        knob: i64,
    }
    fn one() -> i64 {
        1
    }

    let empty = ConfigSection::empty();
    assert!(empty.backend().is_none());
    assert_eq!(empty.get::<Tuning>().unwrap(), Tuning { knob: 1 });
}
```

- [x] **Step 2: Run tests to verify they fail**

Run: `cargo test -p varve-log --test memory_log && cargo test -p varve-config`
Expected: compile errors — `MemoryLog`, `log_registry`, `ConfigSection::empty` missing.

- [x] **Step 3: Write minimal implementation**

`crates/varve-config/src/config.rs` — add inside `impl ConfigSection`:

```rust
    /// An empty section — used when a config omits an optional `[section]`
    /// entirely (all lookups miss; `get` sees serde defaults).
    pub fn empty() -> ConfigSection {
        ConfigSection {
            table: toml::Table::new(),
        }
    }
```

`crates/varve-log/src/memory.rs`:

```rust
use crate::log::{Log, LogError};
use crate::record::LogRecord;
use async_trait::async_trait;
use std::sync::{Arc, Mutex};
use varve_config::{ComponentFactory, ConfigSection, RegistryError};
use varve_types::LogPosition;

/// Volatile in-process log — tests and `Db::memory()` (spec §6 `memory`
/// backend: "ring buffer, no durability").
#[derive(Default)]
pub struct MemoryLog {
    records: Mutex<Vec<(LogPosition, LogRecord)>>,
}

impl MemoryLog {
    pub fn new() -> MemoryLog {
        MemoryLog::default()
    }
}

#[async_trait]
impl Log for MemoryLog {
    async fn append(&self, records: Vec<LogRecord>) -> Result<LogPosition, LogError> {
        if records.is_empty() {
            return Err(LogError::EmptyAppend);
        }
        let mut stored = self.records.lock().map_err(|_| LogError::Poisoned)?;
        let first = match stored.last() {
            Some((last, _)) => last.advance(1)?,
            None => LogPosition::ZERO,
        };
        // Pre-compute every position so an overflow fails before any mutation.
        let mut positioned = Vec::with_capacity(records.len());
        for (i, record) in records.into_iter().enumerate() {
            positioned.push((first.advance(i as u64)?, record));
        }
        stored.extend(positioned);
        Ok(first)
    }

    async fn read_range(
        &self,
        from: LogPosition,
        to: LogPosition,
    ) -> Result<Vec<(LogPosition, LogRecord)>, LogError> {
        let stored = self.records.lock().map_err(|_| LogError::Poisoned)?;
        Ok(stored
            .iter()
            .filter(|(p, _)| *p >= from && *p < to)
            .cloned()
            .collect())
    }
}

/// Registry factory: `[log] backend = "memory"`.
pub struct MemoryLogFactory;

impl ComponentFactory<dyn Log> for MemoryLogFactory {
    fn name(&self) -> &'static str {
        "memory"
    }

    fn build(&self, _cfg: &ConfigSection) -> Result<Arc<dyn Log>, RegistryError> {
        Ok(Arc::new(MemoryLog::new()))
    }
}
```

`crates/varve-log/src/lib.rs`:

```rust
pub mod log;
pub mod memory;
pub mod record;

pub use log::{Log, LogError};
pub use memory::{MemoryLog, MemoryLogFactory};
pub use record::{LogRecord, TableEffects};

use varve_config::{ComponentFactory, Registry};

/// All built-in log backends, registered under kind "log".
pub fn log_registry() -> Registry<dyn Log> {
    let mut reg = Registry::new("log");
    register_builtin(&mut reg, Box::new(MemoryLogFactory));
    reg
}

fn register_builtin(
    reg: &mut Registry<dyn Log>,
    factory: Box<dyn ComponentFactory<dyn Log>>,
) {
    // Built-in names are a static, distinct set — a duplicate here is a
    // programming error, not a runtime condition.
    if let Err(e) = reg.register(factory) {
        unreachable!("duplicate builtin log factory: {e}");
    }
}
```

- [x] **Step 4: Run tests to verify they pass**

Run: `cargo test -p varve-log && cargo test -p varve-config`
Expected: all pass (5 memory-log tests, 1 new config test).

- [x] **Step 5: Commit**

```bash
git add crates/varve-log/ crates/varve-config/
git commit -m "feat: MemoryLog on the Log trait with registry factory"
```

---
### Task 4: Event ↔ Arrow IPC codec in varve-index

The bridge between the engine's `Event` structs and the envelope's per-table `arrow_ipc` bytes (roadmap: "effects: per-table Arrow IPC"). Lives in `varve-index` because it owns `Event` and already depends on `arrow`; `varve-log` stays payload-agnostic.

**Files:**
- Create: `crates/varve-index/src/codec.rs`
- Modify: `crates/varve-index/src/live.rs` (add `IndexError::Codec`)
- Modify: `crates/varve-index/src/lib.rs` (`pub mod codec;` + re-exports)
- Modify: `crates/varve-index/Cargo.toml` (dev-dep `proptest`)
- Test: in-module `#[cfg(test)]` in `codec.rs`

**Interfaces:**
- Consumes: `Event`, `Op` (varve-index); `varve_types::{decode_doc, encode_doc, Iid, Instant}`; arrow IPC (`StreamWriter`/`StreamReader`, verified on arrow 58.3).
- Produces (re-exported at crate root):
  - `varve_index::codec::encode_events(events: &[Event]) -> Result<Vec<u8>, IndexError>` — one Arrow IPC stream containing one RecordBatch (zero batches when `events` is empty).
  - `varve_index::codec::decode_events(bytes: &[u8]) -> Result<Vec<Event>, IndexError>`.
  - `IndexError::Codec(String)` — new variant: `#[error("event codec: {0}")]`.
- **Event batch schema (the log wire format for effects, v1):**

| column | type | nullable |
|---|---|---|
| `_iid` | FixedSizeBinary(16) | no |
| `_system_from` | Timestamp(µs, "UTC") | no |
| `_valid_from` | Timestamp(µs, "UTC") | no |
| `_valid_to` | Timestamp(µs, "UTC") | no |
| `op` | UInt8 (0=Put, 1=Delete, 2=Erase) | no |
| `payload` | Binary | yes (null unless Put) |

- **Put payload encoding:** u32 LE label count · per label: u32 LE length + UTF-8 · then `encode_doc(doc)` (Task 1). Trailing bytes after the doc are a decode error.
- Deliberately NOT columnar docs: dense-union doc structs are slice 4's block format; the log only needs faithful round-trip for replay/follower apply, and this codec must not inherit the live table's v0 mixed-type restriction.

- [x] **Step 1: Write the failing tests**

`crates/varve-index/src/codec.rs` (start with the test module):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{Event, Op};
    use proptest::prelude::*;
    use varve_types::{Doc, Iid, Instant, Value};

    fn iid(n: u8) -> Iid {
        Iid::derive("g", "nodes", &[n])
    }

    fn us(n: i64) -> Instant {
        Instant::from_micros(n)
    }

    #[test]
    fn round_trips_put_delete_erase() {
        let mut doc = Doc::new();
        doc.insert("name".into(), Value::Str("Ada".into()));
        doc.insert("age".into(), Value::Int(36));
        doc.insert("ghost".into(), Value::Null);
        doc.insert("raw".into(), Value::Bytes(vec![1, 2]));
        let events = vec![
            Event {
                iid: iid(1),
                system_from: us(10),
                valid_from: us(5),
                valid_to: Instant::END_OF_TIME,
                op: Op::Put {
                    labels: vec!["Person".into(), "Admin".into()],
                    doc,
                },
            },
            Event {
                iid: iid(2),
                system_from: us(10),
                valid_from: us(10),
                valid_to: Instant::END_OF_TIME,
                op: Op::Delete,
            },
            Event {
                iid: iid(3),
                system_from: us(10),
                valid_from: Instant::MIN,
                valid_to: Instant::END_OF_TIME,
                op: Op::Erase,
            },
        ];
        let bytes = encode_events(&events).unwrap();
        assert_eq!(decode_events(&bytes).unwrap(), events);
    }

    #[test]
    fn round_trips_empty_labels_and_empty_doc() {
        let events = vec![Event {
            iid: iid(1),
            system_from: us(1),
            valid_from: us(1),
            valid_to: us(2),
            op: Op::Put {
                labels: vec![],
                doc: Doc::new(),
            },
        }];
        let bytes = encode_events(&events).unwrap();
        assert_eq!(decode_events(&bytes).unwrap(), events);
    }

    #[test]
    fn empty_event_slice_round_trips() {
        let bytes = encode_events(&[]).unwrap();
        assert_eq!(decode_events(&bytes).unwrap(), vec![]);
    }

    #[test]
    fn unknown_op_tag_is_rejected() {
        // Build a schema-conformant batch by hand with op = 7.
        use arrow::array::{
            ArrayRef, BinaryBuilder, FixedSizeBinaryBuilder, TimestampMicrosecondBuilder,
            UInt8Builder,
        };
        use arrow::ipc::writer::StreamWriter;
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let schema = event_schema();
        let mut iid_b = FixedSizeBinaryBuilder::new(16);
        iid_b.append_value([0u8; 16]).unwrap();
        let ts = || {
            let mut b = TimestampMicrosecondBuilder::new().with_timezone("UTC");
            b.append_value(1);
            Arc::new(b.finish()) as ArrayRef
        };
        let mut op_b = UInt8Builder::new();
        op_b.append_value(7);
        let mut payload_b = BinaryBuilder::new();
        payload_b.append_null();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(iid_b.finish()),
                ts(),
                ts(),
                ts(),
                Arc::new(op_b.finish()),
                Arc::new(payload_b.finish()),
            ],
        )
        .unwrap();
        let mut buf = Vec::new();
        {
            let mut w = StreamWriter::try_new(&mut buf, &schema).unwrap();
            w.write(&batch).unwrap();
            w.finish().unwrap();
        }
        assert!(matches!(
            decode_events(&buf),
            Err(IndexError::Codec(msg)) if msg.contains("op tag")
        ));
    }

    #[test]
    fn put_without_payload_is_rejected() {
        // op = 0 (Put) but payload null.
        use arrow::array::{
            ArrayRef, BinaryBuilder, FixedSizeBinaryBuilder, TimestampMicrosecondBuilder,
            UInt8Builder,
        };
        use arrow::ipc::writer::StreamWriter;
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let schema = event_schema();
        let mut iid_b = FixedSizeBinaryBuilder::new(16);
        iid_b.append_value([0u8; 16]).unwrap();
        let ts = || {
            let mut b = TimestampMicrosecondBuilder::new().with_timezone("UTC");
            b.append_value(1);
            Arc::new(b.finish()) as ArrayRef
        };
        let mut op_b = UInt8Builder::new();
        op_b.append_value(0);
        let mut payload_b = BinaryBuilder::new();
        payload_b.append_null();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(iid_b.finish()),
                ts(),
                ts(),
                ts(),
                Arc::new(op_b.finish()),
                Arc::new(payload_b.finish()),
            ],
        )
        .unwrap();
        let mut buf = Vec::new();
        {
            let mut w = StreamWriter::try_new(&mut buf, &schema).unwrap();
            w.write(&batch).unwrap();
            w.finish().unwrap();
        }
        assert!(matches!(decode_events(&buf), Err(IndexError::Codec(_))));
    }

    #[test]
    fn wrong_schema_is_rejected() {
        assert!(matches!(
            decode_events(b"not an ipc stream"),
            Err(IndexError::Arrow(_))
        ));
    }

    // Bounded strategies: no NaN (Event's PartialEq would fail); NaN
    // round-tripping is covered bit-exactly in varve-types Task 1.
    fn value_strategy() -> impl Strategy<Value = Value> {
        prop_oneof![
            Just(Value::Null),
            any::<bool>().prop_map(Value::Bool),
            any::<i64>().prop_map(Value::Int),
            (-1.0e12_f64..1.0e12).prop_map(Value::Float),
            "[a-zA-Z0-9 ]{0,12}".prop_map(Value::Str),
            proptest::collection::vec(any::<u8>(), 0..16).prop_map(Value::Bytes),
        ]
    }

    fn event_strategy() -> impl Strategy<Value = Event> {
        let doc = proptest::collection::btree_map("[a-z]{1,8}", value_strategy(), 0..5);
        let labels = proptest::collection::vec("[A-Z][a-z]{0,6}", 0..3);
        let op = prop_oneof![
            (labels, doc).prop_map(|(labels, doc)| Op::Put { labels, doc }),
            Just(Op::Delete),
            Just(Op::Erase),
        ];
        (any::<u8>(), 0..1000i64, 0..1000i64, 1000..2000i64, op).prop_map(
            |(entity, sf, vf, vt, op)| Event {
                iid: Iid::derive("g", "nodes", &[entity]),
                system_from: Instant::from_micros(sf),
                valid_from: Instant::from_micros(vf),
                valid_to: Instant::from_micros(vt),
                op,
            },
        )
    }

    proptest! {
        #[test]
        fn codec_round_trips_random_events(events in proptest::collection::vec(event_strategy(), 0..20)) {
            let bytes = encode_events(&events).unwrap();
            prop_assert_eq!(decode_events(&bytes).unwrap(), events);
        }
    }
}
```

- [x] **Step 2: Run tests to verify they fail**

Run: `cargo test -p varve-index codec`
Expected: compile error — module/functions missing.

- [x] **Step 3: Write minimal implementation**

Add to `IndexError` in `crates/varve-index/src/live.rs`:

```rust
    #[error("event codec: {0}")]
    Codec(String),
```

`crates/varve-index/src/codec.rs` (prepend above the test module):

```rust
//! Events ↔ Arrow IPC — the wire format for the log envelope's per-table
//! `arrow_ipc` effect bytes (spec §6). Docs and labels ride in a single
//! Binary `payload` column via the canonical varve-types codec; columnar doc
//! structs (dense unions) arrive with slice 4's block format.

use crate::event::{Event, Op};
use crate::live::IndexError;
use arrow::array::{
    ArrayRef, BinaryArray, BinaryBuilder, FixedSizeBinaryArray, FixedSizeBinaryBuilder,
    TimestampMicrosecondArray, TimestampMicrosecondBuilder, UInt8Array, UInt8Builder,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use varve_types::{decode_doc, encode_doc, Doc, Iid, Instant};

const OP_PUT: u8 = 0;
const OP_DELETE: u8 = 1;
const OP_ERASE: u8 = 2;

fn codec_err(msg: impl Into<String>) -> IndexError {
    IndexError::Codec(msg.into())
}

fn event_schema() -> Arc<Schema> {
    let ts = || DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    Arc::new(Schema::new(vec![
        Field::new("_iid", DataType::FixedSizeBinary(16), false),
        Field::new("_system_from", ts(), false),
        Field::new("_valid_from", ts(), false),
        Field::new("_valid_to", ts(), false),
        Field::new("op", DataType::UInt8, false),
        Field::new("payload", DataType::Binary, true),
    ]))
}

fn encode_put_payload(labels: &[String], doc: &Doc) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&(labels.len() as u32).to_le_bytes());
    for label in labels {
        out.extend_from_slice(&(label.len() as u32).to_le_bytes());
        out.extend_from_slice(label.as_bytes());
    }
    out.extend_from_slice(&encode_doc(doc));
    out
}

fn decode_put_payload(mut input: &[u8]) -> Result<(Vec<String>, Doc), IndexError> {
    fn take<'a>(input: &mut &'a [u8], n: usize) -> Result<&'a [u8], IndexError> {
        if input.len() < n {
            return Err(codec_err(format!("payload: need {n} bytes, have {}", input.len())));
        }
        let (head, rest) = input.split_at(n);
        *input = rest;
        Ok(head)
    }
    fn read_u32(input: &mut &[u8]) -> Result<u32, IndexError> {
        let b = take(input, 4)?;
        let arr: [u8; 4] = b.try_into().map_err(|_| codec_err("payload u32"))?;
        Ok(u32::from_le_bytes(arr))
    }

    let label_count = read_u32(&mut input)?;
    let mut labels = Vec::with_capacity(label_count as usize);
    for _ in 0..label_count {
        let len = read_u32(&mut input)? as usize;
        let label = std::str::from_utf8(take(&mut input, len)?)
            .map_err(|e| codec_err(format!("label not UTF-8: {e}")))?;
        labels.push(label.to_string());
    }
    let doc = decode_doc(&mut input).map_err(|e| codec_err(e.to_string()))?;
    if !input.is_empty() {
        return Err(codec_err(format!("{} trailing payload bytes", input.len())));
    }
    Ok((labels, doc))
}

/// Serializes events as one Arrow IPC stream (one RecordBatch; zero batches
/// for an empty slice).
pub fn encode_events(events: &[Event]) -> Result<Vec<u8>, IndexError> {
    let schema = event_schema();
    let mut buf = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buf, &schema)?;
    if !events.is_empty() {
        let mut iid_b = FixedSizeBinaryBuilder::new(16);
        let mut system_from_b = TimestampMicrosecondBuilder::new().with_timezone("UTC");
        let mut valid_from_b = TimestampMicrosecondBuilder::new().with_timezone("UTC");
        let mut valid_to_b = TimestampMicrosecondBuilder::new().with_timezone("UTC");
        let mut op_b = UInt8Builder::new();
        let mut payload_b = BinaryBuilder::new();
        for event in events {
            iid_b.append_value(event.iid.as_bytes())?;
            system_from_b.append_value(event.system_from.as_micros());
            valid_from_b.append_value(event.valid_from.as_micros());
            valid_to_b.append_value(event.valid_to.as_micros());
            match &event.op {
                Op::Put { labels, doc } => {
                    op_b.append_value(OP_PUT);
                    payload_b.append_value(encode_put_payload(labels, doc));
                }
                Op::Delete => {
                    op_b.append_value(OP_DELETE);
                    payload_b.append_null();
                }
                Op::Erase => {
                    op_b.append_value(OP_ERASE);
                    payload_b.append_null();
                }
            }
        }
        let columns: Vec<ArrayRef> = vec![
            Arc::new(iid_b.finish()),
            Arc::new(system_from_b.finish()),
            Arc::new(valid_from_b.finish()),
            Arc::new(valid_to_b.finish()),
            Arc::new(op_b.finish()),
            Arc::new(payload_b.finish()),
        ];
        writer.write(&RecordBatch::try_new(schema.clone(), columns)?)?;
    }
    writer.finish()?;
    drop(writer);
    Ok(buf)
}

/// Inverse of [`encode_events`].
pub fn decode_events(bytes: &[u8]) -> Result<Vec<Event>, IndexError> {
    let reader = StreamReader::try_new(std::io::Cursor::new(bytes), None)?;
    let mut events = Vec::new();
    for batch in reader {
        let batch = batch?;
        if batch.schema() != event_schema() {
            return Err(codec_err("unexpected event batch schema"));
        }
        let iids = downcast::<FixedSizeBinaryArray>(&batch, 0)?;
        let system_from = downcast::<TimestampMicrosecondArray>(&batch, 1)?;
        let valid_from = downcast::<TimestampMicrosecondArray>(&batch, 2)?;
        let valid_to = downcast::<TimestampMicrosecondArray>(&batch, 3)?;
        let ops = downcast::<UInt8Array>(&batch, 4)?;
        let payloads = downcast::<BinaryArray>(&batch, 5)?;
        for row in 0..batch.num_rows() {
            let iid_bytes: [u8; 16] = iids
                .value(row)
                .try_into()
                .map_err(|_| codec_err("_iid width"))?;
            let op = match ops.value(row) {
                OP_PUT => {
                    if payloads.is_null(row) {
                        return Err(codec_err("Put event with null payload"));
                    }
                    let (labels, doc) = decode_put_payload(payloads.value(row))?;
                    Op::Put { labels, doc }
                }
                OP_DELETE => Op::Delete,
                OP_ERASE => Op::Erase,
                other => return Err(codec_err(format!("unknown op tag {other}"))),
            };
            events.push(Event {
                iid: Iid::from_bytes(iid_bytes),
                system_from: Instant::from_micros(system_from.value(row)),
                valid_from: Instant::from_micros(valid_from.value(row)),
                valid_to: Instant::from_micros(valid_to.value(row)),
                op,
            });
        }
    }
    Ok(events)
}

fn downcast<'a, T: 'static>(batch: &'a RecordBatch, index: usize) -> Result<&'a T, IndexError> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| codec_err(format!("column {index} has unexpected array type")))
}
```

Update `crates/varve-index/src/lib.rs`:

```rust
pub mod bitemporal;
pub mod codec;
pub mod event;
pub mod live;

pub use bitemporal::{resolve, Ceiling, Polygon, ResolvedVersion};
pub use codec::{decode_events, encode_events};
pub use event::{Event, Op};
pub use live::{IndexError, LiveTable};
```

Add to `crates/varve-index/Cargo.toml`:

```toml
[dev-dependencies]
proptest = { workspace = true }
```

(Note: `StreamWriter::finish` then `drop(writer)` releases the `&mut buf` borrow before returning `buf`. If the pinned arrow API differs anywhere, the tests are the contract — adapt the implementation.)

- [x] **Step 4: Run tests to verify they pass**

Run: `cargo test -p varve-index`
Expected: all pass (existing live/bitemporal tests + 6 codec tests + 1 property test at 256 cases).

- [x] **Step 5: Commit**

```bash
git add crates/varve-index/
git commit -m "feat: event batch Arrow IPC codec for log effects"
```

---

### Task 5: LocalLog — segmented files, CRC32C frames, fsync-before-ack

The durable backend (spec §6 `local` row: "segmented append-only files; fsync before ack; CRC32C per record"). Torn-tail recovery is Task 6; this task is the happy path + factory.

**Files:**
- Create: `crates/varve-log/src/local.rs`
- Modify: `crates/varve-log/src/lib.rs` (module, re-exports, register `local` in `log_registry`)
- Test: `crates/varve-log/tests/local_log.rs`

**Interfaces:**
- Consumes: Tasks 2–3 (`Log`, `LogError`, `LogRecord`, `log_registry`).
- Produces (re-exported at crate root):
  - `varve_log::LocalLog` — `LocalLog::open(dir: &Path, segment_max_bytes: u64) -> Result<LocalLog, LogError>` (sync constructor: creates the dir, scans/validates segments, opens the active one); implements `Log`.
  - `varve_log::DEFAULT_SEGMENT_MAX_BYTES: u64 = 64 * 1024 * 1024`.
  - `varve_log::LocalLogFactory` — `ComponentFactory<dyn Log>`, `name() == "local"`; reads `[log.local]`: `dir` (required string), `segment_max_bytes` (optional, default above).
- **On-disk format (v1, golden-shaped by the frame tests):**
  - Segment file: `<first-position-as-packed-u64, 16 lower-case hex digits>.vseg` — lexicographic name order == position order (epoch-major packing, slice 0).
  - Frame per record: `len: u32 LE` (payload length) · `crc: u32 LE` (CRC32C of payload) · `payload` (protobuf `LogRecord` wire bytes).
  - A batch = its records' frames written with ONE `write_all` + ONE `File::sync_all` (fsync; on macOS Rust std upgrades this to `F_FULLFSYNC`). Positions/state commit only after the sync succeeds.
  - Roll to a new segment when the active one has reached `segment_max_bytes` **before** appending (a batch never splits across segments; segments may overshoot by at most one batch). New segment files (and the initial one) are followed by an fsync of the directory so the file itself survives a crash.
  - On append failure: truncate back to the pre-batch length (+fsync); if the truncate fails, the log is **poisoned** (all further calls return `LogError::Poisoned`; reopen recovers).
- Blocking file I/O runs inside `tokio::task::spawn_blocking`; `Inner` state sits behind `Arc<Mutex<…>>` (std Mutex, locked only inside the blocking closure).
- v1 simplification (documented): `read_range` re-reads segment files from disk per call (`fs::read` whole segments, ≤ `segment_max_bytes` each) — reads happen at open/replay/tests only until slice 9 tails.

- [x] **Step 1: Write the failing tests**

`crates/varve-log/tests/local_log.rs`:

```rust
use std::path::Path;
use varve_config::Config;
use varve_log::{log_registry, LocalLog, LogError, LogRecord, DEFAULT_SEGMENT_MAX_BYTES};
use varve_types::LogPosition;

fn rec(tx_id: u64) -> LogRecord {
    LogRecord {
        tx_id,
        system_time_us: tx_id as i64,
        user: String::new(),
        effects: vec![],
    }
}

fn pos(offset: u64) -> LogPosition {
    LogPosition::new(0, offset).unwrap()
}

fn open(dir: &Path) -> LocalLog {
    LocalLog::open(dir, DEFAULT_SEGMENT_MAX_BYTES).unwrap()
}

#[tokio::test]
async fn append_read_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let log = open(dir.path());
        assert_eq!(log.append(vec![rec(1), rec(2)]).await.unwrap(), pos(0));
        assert_eq!(log.append(vec![rec(3)]).await.unwrap(), pos(2));
        let mid = log.read_range(pos(1), pos(2)).await.unwrap();
        assert_eq!(mid.len(), 1);
        assert_eq!(mid[0].1.tx_id, 2);
    }
    // Reopen: everything durable, positions continue.
    let log = open(dir.path());
    let all = log.tail(LogPosition::ZERO).await.unwrap();
    assert_eq!(
        all.iter().map(|(p, r)| (*p, r.tx_id)).collect::<Vec<_>>(),
        vec![(pos(0), 1), (pos(1), 2), (pos(2), 3)]
    );
    assert_eq!(log.append(vec![rec(4)]).await.unwrap(), pos(3));
}

#[tokio::test]
async fn rolls_segments_and_reads_across_them() {
    let dir = tempfile::tempdir().unwrap();
    // 1-byte budget: every batch after the first byte rolls a new segment.
    let log = LocalLog::open(dir.path(), 1).unwrap();
    log.append(vec![rec(1)]).await.unwrap();
    log.append(vec![rec(2)]).await.unwrap();
    log.append(vec![rec(3)]).await.unwrap();

    let segments: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|x| x == "vseg"))
        .collect();
    assert!(segments.len() >= 2, "expected rolled segments, got {}", segments.len());

    let all = log.tail(LogPosition::ZERO).await.unwrap();
    assert_eq!(
        all.iter().map(|(_, r)| r.tx_id).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );

    // Reopen across segments preserves positions too.
    drop(log);
    let log = LocalLog::open(dir.path(), 1).unwrap();
    assert_eq!(log.append(vec![rec(4)]).await.unwrap(), pos(3));
}

#[tokio::test]
async fn records_round_trip_effects() {
    let dir = tempfile::tempdir().unwrap();
    let log = open(dir.path());
    let record = LogRecord {
        tx_id: 9,
        system_time_us: 99,
        user: String::new(),
        effects: vec![varve_log::TableEffects {
            table: "nodes".into(),
            arrow_ipc: vec![0xDE, 0xAD, 0xBE, 0xEF],
        }],
    };
    log.append(vec![record.clone()]).await.unwrap();
    let all = log.tail(LogPosition::ZERO).await.unwrap();
    assert_eq!(all[0].1, record);
}

#[tokio::test]
async fn empty_append_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let log = open(dir.path());
    assert!(matches!(log.append(vec![]).await, Err(LogError::EmptyAppend)));
}

#[tokio::test]
async fn factory_builds_from_toml_and_requires_dir() {
    let dir = tempfile::tempdir().unwrap();
    let reg = log_registry();

    // toml::Value renders a correctly escaped TOML string for the path.
    let dir_toml = toml::Value::String(dir.path().display().to_string()).to_string();
    let cfg = Config::from_toml_str(&format!(
        "[log]\nbackend = \"local\"\n[log.local]\ndir = {dir_toml}\n"
    ))
    .unwrap()
    .section("log")
    .unwrap();
    let log = reg.build("local", &cfg).unwrap();
    log.append(vec![rec(1)]).await.unwrap();
    assert_eq!(log.tail(LogPosition::ZERO).await.unwrap().len(), 1);

    // Missing [log.local] section is an actionable build error.
    let bare = Config::from_toml_str("[log]\nbackend = \"local\"")
        .unwrap()
        .section("log")
        .unwrap();
    let err = reg.build("local", &bare).unwrap_err().to_string();
    assert!(err.contains("log.local"), "{err}");
}
```

Add `toml` to varve-log's dev-dependencies in `crates/varve-log/Cargo.toml` (test-only, for path escaping):

```toml
[dev-dependencies]
tempfile = { workspace = true }
toml = { workspace = true }
```

- [x] **Step 2: Run tests to verify they fail**

Run: `cargo test -p varve-log --test local_log`
Expected: compile error — `LocalLog` missing.

- [x] **Step 3: Write minimal implementation**

`crates/varve-log/src/local.rs`:

```rust
use crate::log::{Log, LogError};
use crate::record::LogRecord;
use async_trait::async_trait;
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use varve_config::{ComponentFactory, ConfigSection, RegistryError};
use varve_types::LogPosition;

pub const DEFAULT_SEGMENT_MAX_BYTES: u64 = 64 * 1024 * 1024;

const FRAME_HEADER: usize = 8; // len u32 LE + crc32c u32 LE

/// Durable local log (spec §6 `local`): segmented append-only files, one
/// CRC32C-checked frame per record, one fsync per appended batch.
pub struct LocalLog {
    inner: Arc<Mutex<Inner>>,
}

struct Inner {
    dir: PathBuf,
    segment_max_bytes: u64,
    /// Active (last) segment, opened in append mode.
    segment: File,
    segment_len: u64,
    /// Position the next appended record will receive.
    next: LogPosition,
    poisoned: bool,
}

fn segment_name(first: LogPosition) -> String {
    format!("{:016x}.vseg", first.as_u64())
}

fn fsync_dir(dir: &Path) -> Result<(), LogError> {
    File::open(dir)?.sync_all()?;
    Ok(())
}

/// Sorted (first-position, path) pairs for every segment in `dir`.
fn list_segments(dir: &Path) -> Result<Vec<(u64, PathBuf)>, LogError> {
    let mut segments = Vec::new();
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if path.extension().is_none_or(|ext| ext != "vseg") {
            continue;
        }
        let stem = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or_default();
        let first = u64::from_str_radix(stem, 16).map_err(|_| LogError::Corrupt {
            path: path.display().to_string(),
            offset: 0,
            reason: "unrecognized segment file name".into(),
        })?;
        segments.push((first, path));
    }
    segments.sort();
    Ok(segments)
}

struct ScanOutcome {
    records: u64,
    valid_len: u64,
    /// Reason the tail beyond `valid_len` is unusable, if any.
    torn: Option<String>,
}

/// Walks a segment's frames, verifying lengths and CRCs (payloads are NOT
/// protobuf-decoded here; that happens on read).
fn scan_segment(path: &Path) -> Result<ScanOutcome, LogError> {
    let bytes = fs::read(path)?;
    let mut off = 0usize;
    let mut records = 0u64;
    loop {
        let remaining = bytes.len() - off;
        if remaining == 0 {
            return Ok(ScanOutcome { records, valid_len: off as u64, torn: None });
        }
        if remaining < FRAME_HEADER {
            return Ok(ScanOutcome {
                records,
                valid_len: off as u64,
                torn: Some("truncated frame header".into()),
            });
        }
        let len = u32::from_le_bytes([bytes[off], bytes[off + 1], bytes[off + 2], bytes[off + 3]])
            as usize;
        let crc =
            u32::from_le_bytes([bytes[off + 4], bytes[off + 5], bytes[off + 6], bytes[off + 7]]);
        if remaining < FRAME_HEADER + len {
            return Ok(ScanOutcome {
                records,
                valid_len: off as u64,
                torn: Some("truncated frame payload".into()),
            });
        }
        let payload = &bytes[off + FRAME_HEADER..off + FRAME_HEADER + len];
        if crc32c::crc32c(payload) != crc {
            return Ok(ScanOutcome {
                records,
                valid_len: off as u64,
                torn: Some("crc mismatch".into()),
            });
        }
        records += 1;
        off += FRAME_HEADER + len;
    }
}

impl LocalLog {
    /// Opens (or creates) the log at `dir`, validating every segment. A torn
    /// tail on the LAST segment is truncated away (crash recovery); damage
    /// anywhere else is fatal `LogError::Corrupt`.
    pub fn open(dir: &Path, segment_max_bytes: u64) -> Result<LocalLog, LogError> {
        fs::create_dir_all(dir)?;
        let segments = list_segments(dir)?;

        if segments.is_empty() {
            let path = dir.join(segment_name(LogPosition::ZERO));
            let segment = OpenOptions::new().create(true).append(true).open(&path)?;
            segment.sync_all()?;
            fsync_dir(dir)?;
            return Ok(LocalLog {
                inner: Arc::new(Mutex::new(Inner {
                    dir: dir.to_path_buf(),
                    segment_max_bytes,
                    segment,
                    segment_len: 0,
                    next: LogPosition::ZERO,
                    poisoned: false,
                })),
            });
        }

        let mut expected = LogPosition::from_u64(segments[0].0);
        for (idx, (first, path)) in segments.iter().enumerate() {
            let is_last = idx == segments.len() - 1;
            if *first != expected.as_u64() {
                return Err(LogError::Corrupt {
                    path: path.display().to_string(),
                    offset: 0,
                    reason: format!(
                        "segment starts at position {first:#x}, expected {:#x}",
                        expected.as_u64()
                    ),
                });
            }
            let outcome = scan_segment(path)?;
            if let Some(reason) = outcome.torn {
                if !is_last {
                    return Err(LogError::Corrupt {
                        path: path.display().to_string(),
                        offset: outcome.valid_len,
                        reason,
                    });
                }
                // Torn tail on the active segment: truncate to the last
                // complete, CRC-valid frame. Every dropped record was unacked
                // (ack requires a completed fsync of the whole batch).
                let file = OpenOptions::new().write(true).open(path)?;
                file.set_len(outcome.valid_len)?;
                file.sync_all()?;
            }
            expected = expected.advance(outcome.records)?;
        }

        let (_, last_path) = segments
            .last()
            .cloned()
            .unwrap_or((0, dir.join(segment_name(LogPosition::ZERO))));
        let segment = OpenOptions::new().append(true).open(&last_path)?;
        let segment_len = segment.metadata()?.len();
        Ok(LocalLog {
            inner: Arc::new(Mutex::new(Inner {
                dir: dir.to_path_buf(),
                segment_max_bytes,
                segment,
                segment_len,
                next: expected,
                poisoned: false,
            })),
        })
    }
}

fn append_sync(inner: &mut Inner, records: Vec<LogRecord>) -> Result<LogPosition, LogError> {
    if inner.poisoned {
        return Err(LogError::Poisoned);
    }
    if records.is_empty() {
        return Err(LogError::EmptyAppend);
    }
    crash_point("pre-append");

    // Roll BEFORE appending so a batch never splits across segments.
    if inner.segment_len >= inner.segment_max_bytes {
        let path = inner.dir.join(segment_name(inner.next));
        let segment = OpenOptions::new().create_new(true).append(true).open(&path)?;
        segment.sync_all()?;
        fsync_dir(&inner.dir)?;
        inner.segment = segment;
        inner.segment_len = 0;
    }

    let first = inner.next;
    let after_batch = first.advance(records.len() as u64)?; // fail before writing

    let mut buf = Vec::new();
    for record in &records {
        let payload = record.to_wire();
        buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        buf.extend_from_slice(&crc32c::crc32c(&payload).to_le_bytes());
        buf.extend_from_slice(&payload);
    }

    let write_result = (|| -> std::io::Result<()> {
        use std::io::Write as _;
        inner.segment.write_all(&buf)?;
        inner.segment.sync_all()
    })();
    if let Err(e) = write_result {
        // Roll the file back so the tail stays clean for the next append.
        let restored = inner
            .segment
            .set_len(inner.segment_len)
            .and_then(|_| inner.segment.sync_all());
        if restored.is_err() {
            inner.poisoned = true;
        }
        return Err(LogError::Io(e));
    }

    inner.segment_len += buf.len() as u64;
    inner.next = after_batch;
    crash_point("post-append");
    Ok(first)
}

fn read_range_sync(
    inner: &Inner,
    from: LogPosition,
    to: LogPosition,
) -> Result<Vec<(LogPosition, LogRecord)>, LogError> {
    if inner.poisoned {
        return Err(LogError::Poisoned);
    }
    let mut out = Vec::new();
    for (first, path) in list_segments(&inner.dir)? {
        let mut position = LogPosition::from_u64(first);
        let bytes = fs::read(&path)?;
        let mut off = 0usize;
        while bytes.len() - off >= FRAME_HEADER {
            let len =
                u32::from_le_bytes([bytes[off], bytes[off + 1], bytes[off + 2], bytes[off + 3]])
                    as usize;
            let crc = u32::from_le_bytes([
                bytes[off + 4],
                bytes[off + 5],
                bytes[off + 6],
                bytes[off + 7],
            ]);
            if bytes.len() - off < FRAME_HEADER + len {
                break; // open() already truncated torn tails; defensive
            }
            let payload = &bytes[off + FRAME_HEADER..off + FRAME_HEADER + len];
            if crc32c::crc32c(payload) != crc {
                return Err(LogError::Corrupt {
                    path: path.display().to_string(),
                    offset: off as u64,
                    reason: "crc mismatch on read".into(),
                });
            }
            if position >= to {
                return Ok(out);
            }
            if position >= from {
                out.push((position, LogRecord::from_wire(payload)?));
            }
            position = position.advance(1)?;
            off += FRAME_HEADER + len;
        }
    }
    Ok(out)
}

#[async_trait]
impl Log for LocalLog {
    async fn append(&self, records: Vec<LogRecord>) -> Result<LogPosition, LogError> {
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || {
            let mut guard = inner.lock().map_err(|_| LogError::Poisoned)?;
            append_sync(&mut guard, records)
        })
        .await
        .map_err(|e| LogError::Io(std::io::Error::other(e)))?
    }

    async fn read_range(
        &self,
        from: LogPosition,
        to: LogPosition,
    ) -> Result<Vec<(LogPosition, LogRecord)>, LogError> {
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || {
            let guard = inner.lock().map_err(|_| LogError::Poisoned)?;
            read_range_sync(&guard, from, to)
        })
        .await
        .map_err(|e| LogError::Io(std::io::Error::other(e)))?
    }
}

/// Test-only crash hook (feature `fault-injection`; inert otherwise and
/// whenever VARVE_CRASH_TRIGGER is unset or the trigger file doesn't name
/// this point). Announces the point on stdout and parks until the crash
/// harness delivers `kill -9`.
#[cfg(feature = "fault-injection")]
fn crash_point(point: &str) {
    let Ok(path) = std::env::var("VARVE_CRASH_TRIGGER") else {
        return;
    };
    match std::fs::read_to_string(&path) {
        Ok(armed) if armed.trim() == point => {}
        _ => return,
    }
    println!("CRASH_POINT {point}");
    use std::io::Write as _;
    let _ = std::io::stdout().flush();
    loop {
        std::thread::sleep(std::time::Duration::from_secs(3600));
    }
}

#[cfg(not(feature = "fault-injection"))]
fn crash_point(_point: &str) {}

#[derive(serde::Deserialize)]
struct LocalLogConfig {
    dir: String,
    #[serde(default = "default_segment_max_bytes")]
    segment_max_bytes: u64,
}

fn default_segment_max_bytes() -> u64 {
    DEFAULT_SEGMENT_MAX_BYTES
}

/// Registry factory: `[log] backend = "local"` + `[log.local] dir = "…"`.
pub struct LocalLogFactory;

impl ComponentFactory<dyn Log> for LocalLogFactory {
    fn name(&self) -> &'static str {
        "local"
    }

    fn build(&self, cfg: &ConfigSection) -> Result<Arc<dyn Log>, RegistryError> {
        let build_err = |source: Box<dyn std::error::Error + Send + Sync>| RegistryError::Build {
            kind: "log",
            name: "local".into(),
            source,
        };
        let local = cfg
            .child("local")
            .ok_or_else(|| build_err("missing [log.local] section (needs `dir`)".into()))?;
        let config: LocalLogConfig = local.get()?;
        let log = LocalLog::open(Path::new(&config.dir), config.segment_max_bytes)
            .map_err(|e| build_err(Box::new(e)))?;
        Ok(Arc::new(log))
    }
}
```

Update `crates/varve-log/src/lib.rs` — add the module, re-exports, and register the factory:

```rust
pub mod local;
pub mod log;
pub mod memory;
pub mod record;

pub use local::{LocalLog, LocalLogFactory, DEFAULT_SEGMENT_MAX_BYTES};
pub use log::{Log, LogError};
pub use memory::{MemoryLog, MemoryLogFactory};
pub use record::{LogRecord, TableEffects};

use varve_config::{ComponentFactory, Registry};

/// All built-in log backends, registered under kind "log".
pub fn log_registry() -> Registry<dyn Log> {
    let mut reg = Registry::new("log");
    register_builtin(&mut reg, Box::new(MemoryLogFactory));
    register_builtin(&mut reg, Box::new(LocalLogFactory));
    reg
}
```

(`register_builtin` stays as written in Task 3.)

- [x] **Step 4: Run tests to verify they pass**

Run: `cargo test -p varve-log`
Expected: all pass (record + memory + 5 local-log tests). Also update the Task 3 registry test if it asserted the exact available-set — it checked `contains("memory")`, which still holds.

- [x] **Step 5: Commit**

```bash
git add crates/varve-log/
git commit -m "feat: LocalLog with segmented CRC32C frames and fsync-before-ack"
```

---
### Task 6: LocalLog torn-tail recovery and corruption handling

The recovery behavior is already implemented inside `LocalLog::open` (Task 5 wrote it as one coherent unit); this task pins it with the tests the roadmap demands ("test: corrupt tail bytes → clean recovery") plus the failure modes that must stay FATAL. If any test fails, fix `open`/`scan_segment` — do not weaken a test.

**Files:**
- Test: `crates/varve-log/tests/recovery.rs`
- Modify (only if a test exposes a gap): `crates/varve-log/src/local.rs`

**Interfaces:**
- Consumes: `LocalLog::open`, frame format from Task 5 (`len u32 LE · crc u32 LE · payload`).
- Produces: no new API — pinned behavior:
  - torn/CRC-bad tail of the LAST segment → truncated on open; earlier records intact; the next append reuses the truncated position;
  - a valid-CRC prefix of a partially-written batch survives (each record is a complete tx — decision 3);
  - damage in a NON-last segment → `LogError::Corrupt` (never silent truncation of committed history);
  - recovery is idempotent.

- [x] **Step 1: Write the failing tests**

`crates/varve-log/tests/recovery.rs`:

```rust
use std::fs;
use std::path::{Path, PathBuf};
use varve_log::{LocalLog, LogError, LogRecord, DEFAULT_SEGMENT_MAX_BYTES};
use varve_types::LogPosition;

fn rec(tx_id: u64) -> LogRecord {
    LogRecord {
        tx_id,
        system_time_us: tx_id as i64,
        user: String::new(),
        effects: vec![],
    }
}

fn pos(offset: u64) -> LogPosition {
    LogPosition::new(0, offset).unwrap()
}

fn open(dir: &Path) -> LocalLog {
    LocalLog::open(dir, DEFAULT_SEGMENT_MAX_BYTES).unwrap()
}

fn segment_paths(dir: &Path) -> Vec<PathBuf> {
    let mut paths: Vec<PathBuf> = fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|x| x == "vseg"))
        .collect();
    paths.sort();
    paths
}

/// (start, end) byte range of each frame in a segment file.
fn frame_ranges(bytes: &[u8]) -> Vec<(usize, usize)> {
    let mut ranges = Vec::new();
    let mut off = 0;
    while off + 8 <= bytes.len() {
        let len = u32::from_le_bytes([bytes[off], bytes[off + 1], bytes[off + 2], bytes[off + 3]])
            as usize;
        let end = off + 8 + len;
        assert!(end <= bytes.len(), "test helper walked off the segment");
        ranges.push((off, end));
        off = end;
    }
    ranges
}

async fn write_three(dir: &Path) {
    let log = open(dir);
    log.append(vec![rec(1)]).await.unwrap();
    log.append(vec![rec(2)]).await.unwrap();
    log.append(vec![rec(3)]).await.unwrap();
}

fn tx_ids(records: &[(LogPosition, LogRecord)]) -> Vec<u64> {
    records.iter().map(|(_, r)| r.tx_id).collect()
}

#[tokio::test]
async fn corrupt_tail_byte_recovers_cleanly_and_positions_rewind() {
    let dir = tempfile::tempdir().unwrap();
    write_three(dir.path()).await;

    // Flip the last byte: record 3's payload no longer matches its CRC.
    let seg = segment_paths(dir.path()).pop().unwrap();
    let mut bytes = fs::read(&seg).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 0xFF;
    fs::write(&seg, &bytes).unwrap();

    let log = open(dir.path());
    assert_eq!(tx_ids(&log.tail(LogPosition::ZERO).await.unwrap()), vec![1, 2]);

    // The truncated position is reused and the log keeps working.
    assert_eq!(log.append(vec![rec(33)]).await.unwrap(), pos(2));
    assert_eq!(
        tx_ids(&log.tail(LogPosition::ZERO).await.unwrap()),
        vec![1, 2, 33]
    );
}

#[tokio::test]
async fn partial_trailing_frame_is_dropped() {
    let dir = tempfile::tempdir().unwrap();
    write_three(dir.path()).await;

    // Simulate a torn write: garbage that looks like the start of a frame.
    let seg = segment_paths(dir.path()).pop().unwrap();
    let mut bytes = fs::read(&seg).unwrap();
    let clean_len = bytes.len();
    bytes.extend_from_slice(&[0xFF, 0xFF, 0xFF]); // < frame header size
    fs::write(&seg, &bytes).unwrap();

    let log = open(dir.path());
    assert_eq!(tx_ids(&log.tail(LogPosition::ZERO).await.unwrap()), vec![1, 2, 3]);
    drop(log);
    assert_eq!(fs::read(&seg).unwrap().len(), clean_len, "tail truncated");
}

#[tokio::test]
async fn corruption_before_the_tail_truncates_everything_after_it() {
    let dir = tempfile::tempdir().unwrap();
    write_three(dir.path()).await;

    // Corrupt record 2 (middle frame of the LAST segment): recovery keeps
    // only the clean prefix — records 2 AND 3 are dropped (they were part of
    // batches whose ack the crashed process may never have sent; a valid
    // frame AFTER a torn one cannot be trusted as committed order).
    let seg = segment_paths(dir.path()).pop().unwrap();
    let mut bytes = fs::read(&seg).unwrap();
    let ranges = frame_ranges(&bytes);
    assert_eq!(ranges.len(), 3);
    let (start, _) = ranges[1];
    bytes[start + 8] ^= 0xFF; // first payload byte of frame 2
    fs::write(&seg, &bytes).unwrap();

    let log = open(dir.path());
    assert_eq!(tx_ids(&log.tail(LogPosition::ZERO).await.unwrap()), vec![1]);
    assert_eq!(log.append(vec![rec(22)]).await.unwrap(), pos(1));
}

#[tokio::test]
async fn corruption_in_a_non_final_segment_is_fatal() {
    let dir = tempfile::tempdir().unwrap();
    {
        // 1-byte budget forces one segment per batch.
        let log = LocalLog::open(dir.path(), 1).unwrap();
        log.append(vec![rec(1)]).await.unwrap();
        log.append(vec![rec(2)]).await.unwrap();
    }
    let first_seg = segment_paths(dir.path()).into_iter().next().unwrap();
    let mut bytes = fs::read(&first_seg).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 0xFF;
    fs::write(&first_seg, &bytes).unwrap();

    // Committed history is damaged — refuse to open rather than silently
    // truncate acked transactions.
    assert!(matches!(
        LocalLog::open(dir.path(), 1),
        Err(LogError::Corrupt { .. })
    ));
}

#[tokio::test]
async fn recovery_is_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    write_three(dir.path()).await;
    let seg = segment_paths(dir.path()).pop().unwrap();
    let mut bytes = fs::read(&seg).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 0xFF;
    fs::write(&seg, &bytes).unwrap();

    for _ in 0..2 {
        let log = open(dir.path());
        assert_eq!(tx_ids(&log.tail(LogPosition::ZERO).await.unwrap()), vec![1, 2]);
        drop(log);
    }
}
```

- [x] **Step 2: Run tests to verify the pinned behavior**

Run: `cargo test -p varve-log --test recovery`
Expected: all 5 pass against the Task-5 implementation. If any fails, the implementation has a recovery gap — fix `scan_segment`/`open` (likely candidates: truncation not fsynced, `expected` continuity math, or the mid-frame case) and re-run. Tests are the contract.

- [x] **Step 3: Run the full crate suite**

Run: `cargo test -p varve-log`
Expected: all pass.

- [x] **Step 4: Commit**

```bash
git add crates/varve-log/
git commit -m "test: pin LocalLog torn-tail recovery and corruption semantics"
```

---

### Task 7: Clock trait, SystemClock factory, Registries aggregate

Honors two STATUS decisions: "the pluggable `Clock` registry interface (spec §4) arrives with durability config wiring" (slice 2) and "`Registries` aggregate deferred to `varve-engine`" (slice 0). `MonotonicClock` gains `advance_to` so recovery can floor the clock above replayed system times.

**Files:**
- Modify: `crates/varve-engine/src/clock.rs` (trait + `advance_to` + factory)
- Create: `crates/varve-engine/src/registries.rs`
- Modify: `crates/varve-engine/src/lib.rs` (`pub mod clock; pub mod registries;` + re-exports)
- Modify: `crates/varve-engine/Cargo.toml` (deps += `varve-log`, `varve-config`, `tokio`, `serde`)
- Test: in-module `#[cfg(test)]` in `clock.rs` and `registries.rs`

**Interfaces:**
- Consumes: `varve_config::{ComponentFactory, ConfigSection, Registry, RegistryError}`, `varve_log::{log_registry, Log}`.
- Produces:
  - `varve_engine::Clock` (public trait):

```rust
/// Transaction-time source (spec §4 `Clock`). `next()` is called once per
/// transaction by the writer loop — its only caller — and is strictly
/// increasing; `watermark()` is a read-only "now" that is >= every assigned
/// tx time; `advance_to(floor)` raises the floor so every future `next()` is
/// strictly after `floor` (recovery: replayed events must stay in the past).
pub trait Clock: Send + Sync {
    fn next(&self) -> Instant;
    fn watermark(&self) -> Instant;
    fn advance_to(&self, floor: Instant);
}
```

  - `varve_engine::MonotonicClock` (now public) implements `Clock`; existing behavior unchanged, plus `advance_to` = `fetch_max` on the internal atomic.
  - `varve_engine::clock::SystemClockFactory` — `ComponentFactory<dyn Clock>`, `name() == "system"`, builds a fresh `MonotonicClock` (ignores its config section).
  - `varve_engine::Registries`:

```rust
pub struct Registries {
    pub log: Registry<dyn Log>,
    pub clock: Registry<dyn Clock>,
}

impl Registries {
    /// Every backend compiled into this build (spec §4: embedders may
    /// register custom implementations on top before opening a database).
    pub fn with_builtins() -> Registries;
}
```

- [x] **Step 1: Write the failing tests**

Append to the `tests` module in `crates/varve-engine/src/clock.rs`:

```rust
    #[test]
    fn advance_to_floors_future_ticks() {
        let clock = MonotonicClock::new();
        let far_future = Instant::from_micros(i64::MAX - 10);
        clock.advance_to(far_future);
        assert!(clock.next() > far_future);
        assert!(clock.watermark() > far_future);
    }

    #[test]
    fn advance_to_never_moves_backwards() {
        let clock = MonotonicClock::new();
        let t = clock.next();
        clock.advance_to(Instant::from_micros(0)); // long in the past
        assert!(clock.next() > t);
    }

    #[test]
    fn system_factory_builds_a_clock() {
        use varve_config::{ComponentFactory, ConfigSection};
        let clock = SystemClockFactory.build(&ConfigSection::empty()).unwrap();
        assert!(clock.next() > Instant::from_micros(0));
    }
```

`crates/varve-engine/src/registries.rs` (test module first):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use varve_config::ConfigSection;

    #[test]
    fn builtins_cover_log_and_clock() {
        let registries = Registries::with_builtins();
        assert_eq!(registries.log.names(), vec!["local", "memory"]);
        assert_eq!(registries.clock.names(), vec!["system"]);
    }

    #[test]
    fn builds_by_name_from_empty_sections() {
        let registries = Registries::with_builtins();
        let _log = registries.log.build("memory", &ConfigSection::empty()).unwrap();
        let clock = registries.clock.build("system", &ConfigSection::empty()).unwrap();
        assert!(clock.next().as_micros() > 0);
    }
}
```

- [x] **Step 2: Run tests to verify they fail**

Run: `cargo test -p varve-engine`
Expected: compile errors — `advance_to`, `SystemClockFactory`, `registries` missing.

- [x] **Step 3: Write minimal implementation**

`crates/varve-engine/Cargo.toml` — add to `[dependencies]`:

```toml
varve-log = { path = "../varve-log" }
varve-config = { path = "../varve-config" }
tokio = { workspace = true }
serde = { workspace = true }
```

(`tokio`/`serde` are consumed by Tasks 9–10; adding them here keeps this task's manifest change the last one.)

`crates/varve-engine/src/clock.rs` — reshape (keep `wall_us` and the existing struct/tests):

```rust
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use varve_config::{ComponentFactory, ConfigSection, RegistryError};
use varve_types::Instant;

/// Transaction-time source (spec §4 `Clock`). `next()` is called once per
/// transaction by the writer loop — its only caller — and is strictly
/// increasing; `watermark()` is a read-only "now" that is >= every assigned
/// tx time; `advance_to(floor)` raises the floor so every future `next()` is
/// strictly after `floor` (recovery: replayed events must stay in the past).
pub trait Clock: Send + Sync {
    fn next(&self) -> Instant;
    fn watermark(&self) -> Instant;
    fn advance_to(&self, floor: Instant);
}

/// Strictly increasing wall-clock µs source — the builtin `system` clock
/// (spec §5.2: system_from is "assigned by the writer, monotonic per log").
#[derive(Default)]
pub struct MonotonicClock {
    last_us: AtomicI64,
}

fn wall_us() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_micros()).unwrap_or(i64::MAX))
        .unwrap_or(0) // pre-1970 clock: fall back to the monotonic counter
}

impl MonotonicClock {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Clock for MonotonicClock {
    /// Next transaction time: max(wall, last + 1).
    fn next(&self) -> Instant {
        let wall = wall_us();
        let mut last = self.last_us.load(Ordering::SeqCst);
        loop {
            let candidate = wall.max(last + 1);
            match self
                .last_us
                .compare_exchange(last, candidate, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => return Instant::from_micros(candidate),
                Err(actual) => last = actual,
            }
        }
    }

    /// max(wall, last) WITHOUT advancing the clock — query-time "now".
    fn watermark(&self) -> Instant {
        Instant::from_micros(wall_us().max(self.last_us.load(Ordering::SeqCst)))
    }

    fn advance_to(&self, floor: Instant) {
        self.last_us.fetch_max(floor.as_micros(), Ordering::SeqCst);
    }
}

/// Registry factory: `[clock] backend = "system"` (the default).
pub struct SystemClockFactory;

impl ComponentFactory<dyn Clock> for SystemClockFactory {
    fn name(&self) -> &'static str {
        "system"
    }

    fn build(&self, _cfg: &ConfigSection) -> Result<Arc<dyn Clock>, RegistryError> {
        Ok(Arc::new(MonotonicClock::new()))
    }
}
```

The pre-existing `MonotonicClock` in-module tests keep working because the trait lives in the same module (`use super::*;` brings `Clock` into scope). The slice-2 doc comment about "single-writer, one call per tx before the lock" is replaced by the trait docs above — the writer loop (Task 9) is now the serialization point.

`crates/varve-engine/src/registries.rs` (prepend above the tests):

```rust
use crate::clock::{Clock, SystemClockFactory};
use varve_config::{ComponentFactory, Registry};
use varve_log::Log;

/// Per-subsystem component registries (spec §4). `with_builtins()` registers
/// everything compiled in; embedding applications may `register` additional
/// factories before calling `Db::open_with`.
pub struct Registries {
    pub log: Registry<dyn Log>,
    pub clock: Registry<dyn Clock>,
}

impl Registries {
    pub fn with_builtins() -> Registries {
        let mut clock = Registry::new("clock");
        // Builtin names are a static, distinct set — duplicates are bugs.
        if let Err(e) = clock.register(Box::new(SystemClockFactory)) {
            unreachable!("duplicate builtin clock factory: {e}");
        }
        Registries {
            log: varve_log::log_registry(),
            clock,
        }
    }
}
```

`crates/varve-engine/src/lib.rs`:

```rust
pub mod clock;
pub mod db;
pub mod registries;

pub use clock::{Clock, MonotonicClock};
pub use datafusion::arrow::record_batch::RecordBatch;
pub use db::{Db, EngineError, TxReceipt};
pub use registries::Registries;
```

(`db.rs` still refers to `crate::clock::MonotonicClock` — it now also needs `use crate::clock::Clock;` for the trait methods; make that one-line adjustment so the crate compiles. The full `db.rs` rework is Task 9.)

- [x] **Step 4: Run tests to verify they pass**

Run: `cargo test -p varve-engine`
Expected: all pass (existing clock/mutations tests + 3 clock tests + 2 registries tests).

- [x] **Step 5: Commit**

```bash
git add crates/varve-engine/
git commit -m "feat: pluggable Clock trait and Registries aggregate"
```

---

### Task 8: varve-plan — lock-split DML matching

`matching_iids` currently forces its caller to hold a live-table lock across a DataFusion await (the documented slice-2 deferral on `execute_delete`). Split it the same way slice 2 split the query path: sync snapshot phase + async execution phase over an owned batch.

**Files:**
- Modify: `crates/varve-plan/src/exec.rs`
- Modify: `crates/varve-plan/src/lib.rs` (re-exports)
- Test: `crates/varve-plan/tests/exec_test.rs` (append)

**Interfaces:**
- Consumes: existing `matching_iids` internals, `LiveTable::snapshot_for_label`.
- Produces:
  - `varve_plan::matching_snapshot(pattern: &NodePattern, live: &LiveTable, bounds: &TemporalBounds) -> Result<Option<RecordBatch>, PlanError>` — sync; call under a brief read lock.
  - `varve_plan::iids_from_snapshot(snapshot: Option<RecordBatch>, where_clause: &Option<Expr>) -> Result<Vec<Iid>, PlanError>` — async; sorted + deduplicated IIDs; no lock needed.
  - `matching_iids` is kept as the one-shot composition of the two (existing signature and tests unchanged).

- [x] **Step 1: Write the failing test**

Append to `crates/varve-plan/tests/exec_test.rs`:

```rust
#[tokio::test]
async fn split_matching_equals_one_shot() {
    use varve_plan::{iids_from_snapshot, matching_iids, matching_snapshot};
    use varve_types::{TemporalBounds, TemporalDimension};

    let live = setup();
    let q = query_stmt("MATCH (p:Person) WHERE p.age = 36 RETURN p.name");
    let bounds = TemporalBounds {
        valid: TemporalDimension::at(varve_types::Instant::from_micros(100)),
        system: TemporalDimension::at(varve_types::Instant::from_micros(100)),
    };

    let snapshot = matching_snapshot(&q.pattern, &live, &bounds).unwrap();
    let split = iids_from_snapshot(snapshot, &q.where_clause).await.unwrap();
    let one_shot = matching_iids(&q.pattern, &q.where_clause, &live, &bounds)
        .await
        .unwrap();

    assert_eq!(split.len(), 2); // Ada and Cyd are 36
    assert_eq!(split, one_shot);
}
```

(Uses the file's existing `setup()`/`query_stmt()` helpers — `setup()` seeds Ada/Bob/Cyd at system\_from µs 1–3 with open valid ranges, so bounds at µs 100 see all three; the file's `NOW` const is that same instant. Ada and Cyd have age 36 — "2 of 3 match" is the contract.)

- [x] **Step 2: Run test to verify it fails**

Run: `cargo test -p varve-plan --test exec_test split_matching`
Expected: compile error — `matching_snapshot`/`iids_from_snapshot` not found.

- [x] **Step 3: Write minimal implementation**

In `crates/varve-plan/src/exec.rs`, replace the body of `matching_iids` with the composition and add the two phases:

```rust
/// Sync phase of DML matching (MATCH … DELETE): resolve + snapshot under the
/// caller's lock (mirror of `snapshot_for_query`).
pub fn matching_snapshot(
    pattern: &NodePattern,
    live: &LiveTable,
    bounds: &TemporalBounds,
) -> Result<Option<RecordBatch>, PlanError> {
    let label = pattern.label.as_deref().unwrap_or("");
    Ok(live.snapshot_for_label(label, bounds)?)
}

/// Async phase: WHERE filter + IID extraction over an OWNED snapshot —
/// callers drop their live-table lock before awaiting this. Sorted and
/// deduplicated so mutation application order is deterministic.
pub async fn iids_from_snapshot(
    snapshot: Option<RecordBatch>,
    where_clause: &Option<Expr>,
) -> Result<Vec<Iid>, PlanError> {
    let Some(batch) = snapshot else {
        return Ok(vec![]);
    };
    let schema = batch.schema();
    let has_col = |name: &str| schema.column_with_name(name).is_some();

    let ctx = SessionContext::new();
    let table = MemTable::try_new(schema.clone(), vec![vec![batch]])?;
    let mut df = ctx.read_table(Arc::new(table))?;

    if let Some(Expr::PropEq { prop, value, .. }) = where_clause {
        if !has_col(prop) {
            return Err(PlanError::UnknownColumn(prop.clone()));
        }
        df = df.filter(col(prop.as_str()).eq(to_df_literal(value)))?;
    }
    let df = df.select(vec![col("_iid")])?;

    let mut iids = Vec::new();
    for batch in df.collect().await? {
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .ok_or(PlanError::MalformedIid)?;
        for i in 0..col.len() {
            let bytes: [u8; 16] = col
                .value(i)
                .try_into()
                .map_err(|_| PlanError::MalformedIid)?;
            iids.push(Iid::from_bytes(bytes));
        }
    }
    iids.sort();
    iids.dedup();
    Ok(iids)
}

/// One-shot convenience (tests and non-locking callers).
pub async fn matching_iids(
    pattern: &NodePattern,
    where_clause: &Option<Expr>,
    live: &LiveTable,
    bounds: &TemporalBounds,
) -> Result<Vec<Iid>, PlanError> {
    iids_from_snapshot(matching_snapshot(pattern, live, bounds)?, where_clause).await
}
```

(The old duplicated body inside `matching_iids` is deleted — the filter/extract logic now lives once, in `iids_from_snapshot`.)

Update `crates/varve-plan/src/lib.rs`:

```rust
pub mod exec;

pub use exec::{
    execute_query, iids_from_snapshot, matching_iids, matching_snapshot, run_query,
    snapshot_for_query, PlanError,
};
```

- [x] **Step 4: Run tests to verify they pass**

Run: `cargo test -p varve-plan && cargo test -p varve-engine`
Expected: all pass (the engine still calls the one-shot `matching_iids` until Task 9).

- [x] **Step 5: Commit**

```bash
git add crates/varve-plan/
git commit -m "refactor: split DML matching into sync snapshot + async iid phases"
```

---
### Task 9: Writer loop with group commit; Db rewired through it

The serialization point (spec §3, D3). `Db::execute` parses and submits; a dedicated task assigns `(tx_id, system_time)`, resolves statements to events, group-commits record batches (window + size triggers), applies to the live index **after** durability, then acks. Dissolves the slice-2 single-writer clock caveat and deletes `execute_delete`'s `#[allow(clippy::await_holding_lock)]`.

**Files:**
- Create: `crates/varve-engine/src/writer.rs`
- Modify: `crates/varve-engine/src/db.rs` (full rewrite shown below)
- Modify: `crates/varve-engine/src/lib.rs` (add `mod writer;`)
- Modify: `crates/varve-engine/Cargo.toml` (dev-dep `async-trait` for test log wrappers)
- Test: in-module `#[cfg(test)]` in `writer.rs`; `crates/varve-engine/tests/concurrency.rs`

**Interfaces:**
- Consumes: `Clock`/`MonotonicClock` (Task 7), `Log`/`LogRecord`/`TableEffects`/`MemoryLog` (Tasks 2–3), `encode_events` (Task 4), `matching_snapshot`/`iids_from_snapshot` (Task 8), existing AST (`Statement`, `InsertStmt`, `DeleteStmt`, `Literal`).
- Produces (crate-internal, `mod writer`):
  - `Submission { stmt: Statement, ack: oneshot::Sender<Result<TxReceipt, EngineError>> }`
  - `WriterConfig { window: Duration, max_bytes: usize }` with `Default` = 15 ms / 8 MiB; `SUBMISSION_QUEUE_LEN: usize = 256`
  - `WriterState { live: Arc<RwLock<LiveTable>>, clock: Arc<dyn Clock>, log: Arc<dyn Log>, next_tx_id: u64 }`
  - `spawn_writer(state: WriterState, cfg: WriterConfig) -> mpsc::Sender<Submission>`
  - `NODES_TABLE: &str = "nodes"` (v1 single table; Task 10's replay validates against it)
- Produces (public API changes):
  - `EngineError` gains: `Log(#[from] varve_log::LogError)`, `CommitFailed(String)` (`"transaction failed to commit: {0}"`), `WriterUnavailable` (`"writer is not running (database closed)"`). All existing variants stay.
  - `Db::memory()` unchanged signature; internally builds `MemoryLog` + `MonotonicClock` + writer loop with `window = Duration::ZERO` (decision 11).
  - `Db::execute` / `Db::query` signatures unchanged — every existing caller and test keeps compiling.
- **Loop invariants (decision 1, verify in review):** (a) all resolution happens in the loop, serially; (b) a reading statement (`Delete`) resolves only after any staged batch is flushed; (c) events reach the live index only after their batch's durable append; (d) acks fire after apply; (e) a failed append acks `CommitFailed` to the whole batch and the loop continues (nothing was applied).

- [x] **Step 1: Write the failing tests**

`crates/varve-engine/src/writer.rs` — start with the test module:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::MonotonicClock;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering};
    use varve_log::{LogError, MemoryLog};
    use varve_types::LogPosition;

    struct CountingLog {
        inner: MemoryLog,
        appends: AtomicUsize,
    }

    impl CountingLog {
        fn new() -> Arc<CountingLog> {
            Arc::new(CountingLog {
                inner: MemoryLog::new(),
                appends: AtomicUsize::new(0),
            })
        }
    }

    #[async_trait::async_trait]
    impl Log for CountingLog {
        async fn append(&self, records: Vec<LogRecord>) -> Result<LogPosition, LogError> {
            self.appends.fetch_add(1, AtomicOrdering::SeqCst);
            self.inner.append(records).await
        }
        async fn read_range(
            &self,
            from: LogPosition,
            to: LogPosition,
        ) -> Result<Vec<(LogPosition, LogRecord)>, LogError> {
            self.inner.read_range(from, to).await
        }
    }

    /// Fails the first append with an I/O error, then delegates.
    struct FailOnceLog {
        inner: MemoryLog,
        failed: AtomicBool,
    }

    #[async_trait::async_trait]
    impl Log for FailOnceLog {
        async fn append(&self, records: Vec<LogRecord>) -> Result<LogPosition, LogError> {
            if !self.failed.swap(true, AtomicOrdering::SeqCst) {
                return Err(LogError::Io(std::io::Error::other("injected append failure")));
            }
            self.inner.append(records).await
        }
        async fn read_range(
            &self,
            from: LogPosition,
            to: LogPosition,
        ) -> Result<Vec<(LogPosition, LogRecord)>, LogError> {
            self.inner.read_range(from, to).await
        }
    }

    fn spawn(log: Arc<dyn Log>, cfg: WriterConfig) -> (mpsc::Sender<Submission>, Arc<RwLock<LiveTable>>) {
        let live = Arc::new(RwLock::new(LiveTable::new()));
        let state = WriterState {
            live: Arc::clone(&live),
            clock: Arc::new(MonotonicClock::new()),
            log,
            next_tx_id: 0,
        };
        (spawn_writer(state, cfg), live)
    }

    /// try_send keeps submission order deterministic (mpsc is FIFO).
    fn submit(
        sender: &mpsc::Sender<Submission>,
        gql: &str,
    ) -> oneshot::Receiver<Result<TxReceipt, EngineError>> {
        let stmt = varve_gql::parse(gql).unwrap();
        let (ack, rx) = oneshot::channel();
        sender.try_send(Submission { stmt, ack }).unwrap();
        rx
    }

    #[tokio::test]
    async fn concurrent_submissions_share_one_durable_append() {
        let log = CountingLog::new();
        let (sender, _live) = spawn(Arc::clone(&log) as Arc<dyn Log>, WriterConfig {
            window: Duration::from_secs(1),
            max_bytes: 8 * 1024 * 1024,
        });
        let acks: Vec<_> = (1..=5)
            .map(|i| submit(&sender, &format!("INSERT (:P {{_id: {i}}})")))
            .collect();
        for ack in acks {
            ack.await.unwrap().unwrap();
        }
        assert_eq!(log.appends.load(AtomicOrdering::SeqCst), 1);
        let records = log.inner.tail(LogPosition::ZERO).await.unwrap();
        assert_eq!(
            records.iter().map(|(_, r)| r.tx_id).collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 5]
        );
    }

    #[tokio::test]
    async fn size_threshold_flushes_without_waiting_for_the_window() {
        let log = CountingLog::new();
        let (sender, _live) = spawn(Arc::clone(&log) as Arc<dyn Log>, WriterConfig {
            window: Duration::from_secs(3600),
            max_bytes: 1, // every record trips the threshold
        });
        for i in 0..2 {
            let ack = submit(&sender, &format!("INSERT (:P {{_id: {i}}})"));
            // A broken size trigger would park until the 1h window: time out fast.
            tokio::time::timeout(Duration::from_secs(5), ack)
                .await
                .expect("size-triggered flush")
                .unwrap()
                .unwrap();
        }
        assert_eq!(log.appends.load(AtomicOrdering::SeqCst), 2);
    }

    #[tokio::test]
    async fn a_reading_statement_flushes_the_staged_batch_first() {
        let log = CountingLog::new();
        let (sender, _live) = spawn(Arc::clone(&log) as Arc<dyn Log>, WriterConfig {
            window: Duration::from_secs(1),
            max_bytes: 8 * 1024 * 1024,
        });
        let insert = submit(&sender, "INSERT (:P {_id: 1})");
        let delete = submit(&sender, "MATCH (p:P) DELETE p");
        insert.await.unwrap().unwrap();
        delete.await.unwrap().unwrap();
        // Two appends: the DELETE forced the staged INSERT out first…
        assert_eq!(log.appends.load(AtomicOrdering::SeqCst), 2);
        // …and therefore SAW the insert: its record carries one delete event.
        let records = log.inner.tail(LogPosition::ZERO).await.unwrap();
        assert_eq!(records.len(), 2);
        assert!(!records[1].1.effects.is_empty(), "delete resolved against the flushed insert");
    }

    #[tokio::test]
    async fn resolve_errors_are_acked_and_the_loop_survives() {
        let log = CountingLog::new();
        let (sender, _live) = spawn(Arc::clone(&log) as Arc<dyn Log>, WriterConfig {
            window: Duration::ZERO,
            max_bytes: 8 * 1024 * 1024,
        });
        // valid_from defaults to tx time (2026+) which lands AFTER VALID TO.
        let bad = submit(&sender, "INSERT (:P {_id: 1}) VALID TO DATE '2020-01-01'");
        assert!(matches!(
            bad.await.unwrap(),
            Err(EngineError::InvalidValidRange { .. })
        ));
        let good = submit(&sender, "INSERT (:P {_id: 2})");
        good.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn failed_append_acks_commit_failed_and_applies_nothing() {
        let log = Arc::new(FailOnceLog {
            inner: MemoryLog::new(),
            failed: AtomicBool::new(false),
        });
        let (sender, live) = spawn(Arc::clone(&log) as Arc<dyn Log>, WriterConfig {
            window: Duration::ZERO,
            max_bytes: 8 * 1024 * 1024,
        });
        let first = submit(&sender, "INSERT (:P {_id: 1})");
        assert!(matches!(
            first.await.unwrap(),
            Err(EngineError::CommitFailed(_))
        ));
        // Apply-after-durable: the failed batch never touched the live index.
        assert_eq!(live.read().unwrap().event_count(), 0);

        let second = submit(&sender, "INSERT (:P {_id: 2})");
        second.await.unwrap().unwrap();
        assert_eq!(live.read().unwrap().event_count(), 1);
        assert_eq!(log.inner.tail(LogPosition::ZERO).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn closing_the_channel_flushes_the_staged_batch() {
        let log = CountingLog::new();
        let (sender, _live) = spawn(Arc::clone(&log) as Arc<dyn Log>, WriterConfig {
            window: Duration::from_secs(3600),
            max_bytes: 8 * 1024 * 1024,
        });
        let ack = submit(&sender, "INSERT (:P {_id: 1})");
        drop(sender); // Db dropped mid-window
        tokio::time::timeout(Duration::from_secs(5), ack)
            .await
            .expect("close-triggered flush")
            .unwrap()
            .unwrap();
        assert_eq!(log.appends.load(AtomicOrdering::SeqCst), 1);
    }
}
```

`crates/varve-engine/tests/concurrency.rs`:

```rust
use std::sync::Arc;
use varve_engine::Db;

// This exact workload raced the slice-2 engine's clock/lock pair into
// OutOfOrderEvent; the writer loop must serialize it.
#[tokio::test]
async fn concurrent_executes_are_serialized_and_all_committed() {
    let db = Arc::new(Db::memory());
    let mut handles = Vec::new();
    for i in 0..50 {
        let db = Arc::clone(&db);
        handles.push(tokio::spawn(async move {
            db.execute(&format!("INSERT (:C {{_id: {i}, n: {i}}})")).await
        }));
    }
    let mut receipts = Vec::new();
    for handle in handles {
        receipts.push(handle.await.unwrap().unwrap());
    }
    receipts.sort_by_key(|r| r.tx_id);
    for pair in receipts.windows(2) {
        assert!(pair[1].tx_id > pair[0].tx_id);
        assert!(pair[1].system_time > pair[0].system_time);
    }
    assert_eq!(receipts.first().unwrap().tx_id, 1);
    assert_eq!(receipts.last().unwrap().tx_id, 50); // 50 unique ids, no gaps

    let batches = db.query("MATCH (c:C) RETURN c.n").await.unwrap();
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 50);
}
```

Add to `crates/varve-engine/Cargo.toml`:

```toml
[dev-dependencies]
tokio = { workspace = true }
async-trait = { workspace = true }
```

- [x] **Step 2: Run tests to verify they fail**

Run: `cargo test -p varve-engine`
Expected: compile error — `writer` module missing.

- [x] **Step 3: Write the writer loop**

`crates/varve-engine/src/writer.rs` (prepend above the test module):

```rust
//! The writer loop — Varve's serialization point (spec §3, D3). Statements
//! are resolved HERE, serially, so tx N always sees tx N−1. Events are
//! applied to the live index only AFTER their batch is durable, and acks
//! fire after apply — an acked tx is durable and visible; queries never see
//! un-durable data.

use crate::clock::Clock;
use crate::db::{EngineError, TxReceipt};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use varve_gql::ast::{DeleteStmt, InsertStmt, Literal, Statement};
use varve_index::{encode_events, Event, LiveTable, Op};
use varve_log::{Log, LogRecord, TableEffects};
use varve_types::{Doc, Iid, Instant, TemporalBounds, TemporalDimension, Value};

/// v1: single default graph, nodes only — every effect batch targets this table.
pub(crate) const NODES_TABLE: &str = "nodes";

/// Bounded submission queue (roadmap slice 3). Config-driven backpressure
/// semantics arrive with slice 10.
pub(crate) const SUBMISSION_QUEUE_LEN: usize = 256;

pub(crate) struct Submission {
    pub stmt: Statement,
    pub ack: oneshot::Sender<Result<TxReceipt, EngineError>>,
}

/// Group-commit tuning (spec §6): a batch flushes when its window elapses or
/// its encoded size reaches `max_bytes`, whichever comes first.
#[derive(Clone, Copy, Debug)]
pub(crate) struct WriterConfig {
    pub window: Duration,
    pub max_bytes: usize,
}

impl Default for WriterConfig {
    fn default() -> Self {
        WriterConfig {
            window: Duration::from_millis(15),
            max_bytes: 8 * 1024 * 1024,
        }
    }
}

pub(crate) struct WriterState {
    pub live: Arc<RwLock<LiveTable>>,
    pub clock: Arc<dyn Clock>,
    pub log: Arc<dyn Log>,
    pub next_tx_id: u64,
}

struct Staged {
    record: LogRecord,
    events: Vec<Event>,
    receipt: TxReceipt,
    ack: oneshot::Sender<Result<TxReceipt, EngineError>>,
}

pub(crate) fn spawn_writer(mut state: WriterState, cfg: WriterConfig) -> mpsc::Sender<Submission> {
    let (sender, mut rx) = mpsc::channel::<Submission>(SUBMISSION_QUEUE_LEN);
    tokio::spawn(async move {
        while let Some(first) = rx.recv().await {
            run_batch(&mut state, &cfg, &mut rx, first).await;
        }
        // Channel closed (Db dropped): run_batch already flushed anything
        // staged before returning, so exiting here loses nothing.
    });
    sender
}

/// One group-commit batch: stage from `first` until the window elapses, the
/// size threshold trips, or the channel closes — then flush.
async fn run_batch(
    state: &mut WriterState,
    cfg: &WriterConfig,
    rx: &mut mpsc::Receiver<Submission>,
    first: Submission,
) {
    let deadline = tokio::time::Instant::now() + cfg.window;
    let mut staged: Vec<Staged> = Vec::new();
    let mut staged_bytes = 0usize;
    let mut pending = Some(first);
    loop {
        if let Some(sub) = pending.take() {
            // A reading statement must observe every earlier tx, and events
            // apply only after durability — so flush what's staged first.
            if statement_reads(&sub.stmt) && !staged.is_empty() {
                flush(state, std::mem::take(&mut staged)).await;
                staged_bytes = 0;
            }
            match resolve(state, sub.stmt).await {
                Ok((record, events, receipt)) => {
                    staged_bytes += record.wire_len();
                    staged.push(Staged {
                        record,
                        events,
                        receipt,
                        ack: sub.ack,
                    });
                }
                Err(e) => {
                    let _ = sub.ack.send(Err(e));
                }
            }
            if staged_bytes >= cfg.max_bytes {
                break;
            }
        }
        match tokio::time::timeout_at(deadline, rx.recv()).await {
            Ok(Some(sub)) => pending = Some(sub),
            Ok(None) | Err(_) => break, // channel closed or window elapsed
        }
    }
    if !staged.is_empty() {
        flush(state, staged).await;
    }
}

fn statement_reads(stmt: &Statement) -> bool {
    matches!(stmt, Statement::Delete(_))
}

/// Assigns (tx_id, system_time) and resolves the statement to effect events.
async fn resolve(
    state: &mut WriterState,
    stmt: Statement,
) -> Result<(LogRecord, Vec<Event>, TxReceipt), EngineError> {
    state.next_tx_id += 1;
    let tx_id = state.next_tx_id;
    let system = state.clock.next();
    let events = match &stmt {
        Statement::Insert(ins) => resolve_insert(ins, tx_id, system)?,
        Statement::Delete(del) => resolve_delete(state, del, system).await?,
        // Filtered out in Db::execute; kept total for safety.
        Statement::Query(_) => return Err(EngineError::NotAMutation),
    };
    let effects = if events.is_empty() {
        vec![] // e.g. DELETE with no matches: an empty (but logged) tx
    } else {
        vec![TableEffects {
            table: NODES_TABLE.to_string(),
            arrow_ipc: encode_events(&events)?,
        }]
    };
    let record = LogRecord {
        tx_id,
        system_time_us: system.as_micros(),
        user: String::new(),
        effects,
    };
    let receipt = TxReceipt {
        tx_id,
        system_time: system,
    };
    Ok((record, events, receipt))
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

fn resolve_insert(
    ins: &InsertStmt,
    tx_id: u64,
    system: Instant,
) -> Result<Vec<Event>, EngineError> {
    let valid_from = ins.valid_from.unwrap_or(system);
    let valid_to = ins.valid_to.unwrap_or(Instant::END_OF_TIME);
    if valid_from >= valid_to {
        return Err(EngineError::InvalidValidRange {
            from: valid_from,
            to: valid_to,
        });
    }
    // Build and validate EVERY node's (iid, labels, doc) triple — including
    // fallible `id_bytes()?` — before returning, so a later node's invalid
    // `_id` can't leave earlier nodes committed (slice-1 review fix, pinned
    // by `multi_node_insert_is_atomic_on_invalid_id`).
    let mut events = Vec::with_capacity(ins.nodes.len());
    for (ordinal, node) in ins.nodes.iter().enumerate() {
        let mut doc: Doc = node
            .props
            .iter()
            .map(|(k, v)| (k.clone(), literal_to_value(v)))
            .collect();
        let id = match doc.get("_id") {
            Some(v) => v.clone(),
            None => {
                // Durable generated id: (tx_id, ordinal) is unique across
                // restarts because tx_ids are recovered from the log.
                let v = Value::Str(format!("varve:gen:{tx_id}:{ordinal}"));
                doc.insert("_id".into(), v.clone());
                v
            }
        };
        let iid = Iid::derive("default", NODES_TABLE, &id.id_bytes()?);
        events.push(Event {
            iid,
            system_from: system,
            valid_from,
            valid_to,
            op: Op::Put {
                labels: node.labels.clone(),
                doc,
            },
        });
    }
    Ok(events)
}

/// MATCH … DELETE reads current state at (valid=now, system=now) — the
/// slice-2 decision. The snapshot happens under a brief read lock; the
/// DataFusion phase runs on the owned batch with NO lock held (the loop
/// itself serializes writes, so nothing can change in between).
async fn resolve_delete(
    state: &WriterState,
    del: &DeleteStmt,
    system: Instant,
) -> Result<Vec<Event>, EngineError> {
    let bounds = TemporalBounds {
        valid: TemporalDimension::at(system),
        system: TemporalDimension::at(system),
    };
    let snapshot = {
        let live = state.live.read().map_err(|_| EngineError::Poisoned)?;
        varve_plan::matching_snapshot(&del.pattern, &live, &bounds)?
    };
    let iids = varve_plan::iids_from_snapshot(snapshot, &del.where_clause).await?;
    Ok(iids
        .into_iter()
        .map(|iid| Event {
            iid,
            system_from: system,
            valid_from: system,
            valid_to: Instant::END_OF_TIME,
            op: Op::Delete,
        })
        .collect())
}

/// Durable append → apply → ack (in that order — decision 1).
async fn flush(state: &mut WriterState, mut staged: Vec<Staged>) {
    let records: Vec<LogRecord> = staged.iter().map(|s| s.record.clone()).collect();
    match state.log.append(records).await {
        Ok(_first_position) => {
            let applied = apply(state, &mut staged);
            for s in staged {
                let _ = s.ack.send(match &applied {
                    Ok(()) => Ok(s.receipt),
                    Err(msg) => Err(EngineError::CommitFailed(msg.clone())),
                });
            }
        }
        Err(e) => {
            // Nothing was applied, so state is untouched: fail the batch and
            // keep serving (the log itself rolled back or poisoned — its call).
            let msg = e.to_string();
            for s in staged {
                let _ = s.ack.send(Err(EngineError::CommitFailed(msg.clone())));
            }
        }
    }
}

fn apply(state: &WriterState, staged: &mut [Staged]) -> Result<(), String> {
    let mut live = state
        .live
        .write()
        .map_err(|_| "live index lock poisoned".to_string())?;
    for s in staged.iter_mut() {
        for event in std::mem::take(&mut s.events) {
            // Only possible failure is OutOfOrderEvent — structurally
            // impossible here (single loop, monotonic clock). If it ever
            // fires, the durable log is ahead of the live index; a restart
            // replays and heals.
            live.append(event).map_err(|e| e.to_string())?;
        }
    }
    Ok(())
}
```

- [x] **Step 4: Rewrite db.rs on top of the loop**

`crates/varve-engine/src/db.rs` — full replacement:

```rust
use crate::clock::{Clock, MonotonicClock};
use crate::writer::{spawn_writer, Submission, WriterConfig, WriterState};
use datafusion::arrow::record_batch::RecordBatch;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use varve_gql::ast::Statement;
use varve_gql::token::GqlError;
use varve_index::{IndexError, LiveTable};
use varve_log::{LogError, MemoryLog};
use varve_plan::PlanError;
use varve_types::{Instant, TypeError};

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
    #[error(transparent)]
    Log(#[from] LogError),
    #[error("VALID FROM {from} must be earlier than VALID TO {to}")]
    InvalidValidRange { from: Instant, to: Instant },
    #[error("transaction failed to commit: {0}")]
    CommitFailed(String),
    #[error("writer is not running (database closed)")]
    WriterUnavailable,
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
    pub system_time: Instant,
}

/// Embedded, in-process database handle. All mutations flow through the
/// writer loop (spec §3, D3): submissions are resolved serially, group-
/// committed to the log, applied to the live index after durability, then
/// acked — so concurrent `execute()` calls are fully supported, and an
/// acked transaction is both durable and visible.
pub struct Db {
    live: Arc<RwLock<LiveTable>>,
    clock: Arc<dyn Clock>,
    submit: mpsc::Sender<Submission>,
}

impl Db {
    /// Volatile database: memory log, zero group-commit window (there is no
    /// fsync to amortize — decision 11). Requires a tokio runtime.
    pub fn memory() -> Db {
        Self::assemble(
            LiveTable::new(),
            Arc::new(MemoryLog::new()),
            Arc::new(MonotonicClock::new()),
            WriterConfig {
                window: Duration::ZERO,
                ..WriterConfig::default()
            },
            0,
        )
    }

    pub(crate) fn assemble(
        live: LiveTable,
        log: Arc<dyn varve_log::Log>,
        clock: Arc<dyn Clock>,
        cfg: WriterConfig,
        next_tx_id: u64,
    ) -> Db {
        let live = Arc::new(RwLock::new(live));
        let submit = spawn_writer(
            WriterState {
                live: Arc::clone(&live),
                clock: Arc::clone(&clock),
                log,
                next_tx_id,
            },
            cfg,
        );
        Db {
            live,
            clock,
            submit,
        }
    }

    /// Executes a mutation statement (INSERT, MATCH … DELETE): parses here,
    /// resolves and commits inside the writer loop, returns after the tx is
    /// durable AND visible.
    pub async fn execute(&self, gql: &str) -> Result<TxReceipt, EngineError> {
        let stmt = varve_gql::parse(gql)?;
        if matches!(stmt, Statement::Query(_)) {
            return Err(EngineError::NotAMutation);
        }
        let (ack, rx) = oneshot::channel();
        self.submit
            .send(Submission { stmt, ack })
            .await
            .map_err(|_| EngineError::WriterUnavailable)?;
        rx.await.map_err(|_| EngineError::WriterUnavailable)?
    }

    /// Executes a read query, returning Arrow batches.
    pub async fn query(&self, gql: &str) -> Result<Vec<RecordBatch>, EngineError> {
        let Statement::Query(q) = varve_gql::parse(gql)? else {
            return Err(EngineError::NotAQuery);
        };
        let now = self.clock.watermark();
        // Snapshot under the read lock, drop the guard, run DataFusion on the
        // owned batch — no await while holding the lock (slice-2 pattern).
        let snapshot = {
            let live = self.live.read().map_err(|_| EngineError::Poisoned)?;
            varve_plan::snapshot_for_query(&q, &live, now)?
        };
        Ok(varve_plan::execute_query(&q, snapshot).await?)
    }
}
```

Note what DIED here: the `execute_insert`/`execute_delete` methods (moved into the loop as `resolve_insert`/`resolve_delete`), the `tx_counter`/`id_counter` atomics (tx ids live in `WriterState`; generated ids derive from `tx_id:ordinal`), the `#[allow(clippy::await_holding_lock)]` on the delete path, and the "v0 SINGLE-WRITER" doc caveat. Grep to confirm: `grep -rn "await_holding_lock\|id_counter\|SINGLE-WRITER" crates/varve-engine/src/` must return nothing.

Update `crates/varve-engine/src/lib.rs`:

```rust
pub mod clock;
pub mod db;
pub mod registries;
mod writer;

pub use clock::{Clock, MonotonicClock};
pub use datafusion::arrow::record_batch::RecordBatch;
pub use db::{Db, EngineError, TxReceipt};
pub use registries::Registries;
```

- [x] **Step 5: Run tests to verify they pass**

Run: `cargo test -p varve-engine`
Expected: all pass — 6 in-module writer tests, the concurrency test, and every pre-existing test (`mutations.rs` unchanged: same public behavior through the loop).

- [x] **Step 6: Verify the whole workspace still holds**

Run: `cargo test --workspace`
Expected: green — in particular `varve/tests/walking_skeleton.rs` (incl. `multi_node_insert_is_atomic_on_invalid_id` — atomic validation now lives in `resolve_insert`), `varve/tests/temporal.rs`, and both examples' test builds. `Db::memory()`'s zero window keeps latency at slice-2 levels.

- [x] **Step 7: Commit**

```bash
git add crates/varve-engine/
git commit -m "feat: log-serialized writer loop with group commit"
```

---
### Task 10: Db::open — registry-driven backends, log replay, restart survival

Recovery per spec §6: "on writer start — replay log from position 0 into the live index, resume" (block manifests arrive in slice 4). Config selects `log = "local"` vs `"memory"` by registry name — the slice exit criterion. Also folds in the slice-0 deferred varve-config items (rustdoc + `from_file` tests) now that the API gains its first real consumer.

**Files:**
- Modify: `crates/varve-engine/src/db.rs` (add `open`, `open_with`, `local`, `replay`, `LogTuning`)
- Modify: `crates/varve/src/lib.rs` + `crates/varve/Cargo.toml` (re-export `Config`; dev-dep `tempfile`)
- Modify: `crates/varve-config/src/config.rs` (rustdoc sweep — no behavior change)
- Test: `crates/varve/tests/durability.rs`; append to `crates/varve-config/tests/config_test.rs`

**Interfaces:**
- Consumes: `Registries` (Task 7), `log_registry` factories (Tasks 3/5), `decode_events` (Task 4), `LocalLog`/`DEFAULT_SEGMENT_MAX_BYTES` (Task 5), `ConfigSection::empty` (Task 3), `Clock::advance_to` (Task 7), `NODES_TABLE` (Task 9).
- Produces:
  - `Db::open(config: Config) -> Result<Db, EngineError>` (async) — `open_with` with builtins.
  - `Db::open_with(config: &Config, registries: &Registries) -> Result<Db, EngineError>` (async) — the embedder extension point: `[log] backend` (default `"memory"`), `[clock] backend` (default `"system"`), group-commit tuning from `[log]` (`group_commit_window_ms` default 15, `group_commit_max_bytes` default 8388608), then replay.
  - `Db::local(dir: impl AsRef<Path>) -> Result<Db, EngineError>` (async) — spec §11 convenience: `LocalLog` at `dir` with defaults (no TOML round-trip, so exotic paths need no escaping).
  - `EngineError` gains: `Registry(#[from] varve_config::RegistryError)`, `Config(#[from] varve_config::ConfigError)`, `UnknownTable(String)` (`"log record references unknown table '{0}'"`).
  - Replay contract: fold `log.tail(LogPosition::ZERO)` into a fresh `LiveTable`; `next_tx_id` = max replayed `tx_id`; `clock.advance_to(max replayed system_time)` so new txs sort after history. Effects for any table other than `nodes` are a hard error (future-format guard).
  - `varve` facade re-exports: `Config`, `ConfigError` (from varve-config), `Registries`.

- [x] **Step 1: Write the failing tests**

`crates/varve/tests/durability.rs`:

```rust
use std::path::Path;
use varve::{Config, Db};

fn local_config(dir: &Path) -> Config {
    let dir_toml = toml_escaped(dir);
    Config::from_toml_str(&format!(
        "[log]\nbackend = \"local\"\ngroup_commit_window_ms = 1\n[log.local]\ndir = {dir_toml}\n"
    ))
    .unwrap()
}

// tempdir paths are tame, but escape properly anyway.
fn toml_escaped(dir: &Path) -> String {
    format!("{:?}", dir.display().to_string()) // Rust debug-quotes ⊇ TOML basic strings for these paths
}

fn rows(batches: &[varve::RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

#[tokio::test]
async fn acked_transactions_survive_restart() {
    let dir = tempfile::tempdir().unwrap();
    let (ada, bob);
    {
        let db = Db::open(local_config(dir.path())).await.unwrap();
        ada = db.execute("INSERT (:Person {_id: 1, name: 'Ada'})").await.unwrap();
        bob = db.execute("INSERT (:Person {_id: 2, name: 'Bob'})").await.unwrap();
    } // drop closes the writer; every acked tx is already durable

    let db = Db::open(local_config(dir.path())).await.unwrap();
    let batches = db.query("MATCH (p:Person) RETURN p.name").await.unwrap();
    assert_eq!(rows(&batches), 2);

    // tx ids and system times continue past the replayed history.
    let cyd = db.execute("INSERT (:Person {_id: 3, name: 'Cyd'})").await.unwrap();
    assert_eq!(cyd.tx_id, 3);
    assert!(cyd.system_time > ada.system_time);
    assert!(cyd.system_time > bob.system_time);
    assert_eq!(rows(&db.query("MATCH (p:Person) RETURN p.name").await.unwrap()), 3);
}

#[tokio::test]
async fn bitemporal_history_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let before_delete;
    {
        let db = Db::open(local_config(dir.path())).await.unwrap();
        db.execute("INSERT (:P {_id: 1, name: 'Zoe'})").await.unwrap();
        before_delete = db.execute("INSERT (:P {_id: 2, name: 'Amy'})").await.unwrap();
        db.execute("MATCH (p:P) WHERE p.name = 'Zoe' DELETE p").await.unwrap();
        // valid-time axis too
        db.execute("INSERT (:Q {_id: 9, name: 'Eve'}) VALID FROM DATE '2020-06-01'")
            .await
            .unwrap();
    }

    let db = Db::open(local_config(dir.path())).await.unwrap();
    // Delete replayed: only Amy now…
    assert_eq!(rows(&db.query("MATCH (p:P) RETURN p.name").await.unwrap()), 1);
    // …but time travel to before the delete still sees both.
    let time_travel = format!(
        "FOR SYSTEM_TIME AS OF TIMESTAMP '{}' MATCH (p:P) RETURN p.name",
        before_delete.system_time
    );
    assert_eq!(rows(&db.query(&time_travel).await.unwrap()), 2);
    // Valid-time bounds replayed intact.
    assert_eq!(
        rows(&db.query("FOR VALID_TIME AS OF DATE '2019-01-01' MATCH (q:Q) RETURN q.name")
            .await
            .unwrap()),
        0
    );
    assert_eq!(
        rows(&db.query("FOR VALID_TIME AS OF DATE '2021-01-01' MATCH (q:Q) RETURN q.name")
            .await
            .unwrap()),
        1
    );
}

#[tokio::test]
async fn generated_ids_do_not_collide_across_restarts() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Db::open(local_config(dir.path())).await.unwrap();
        db.execute("INSERT (:G {v: 1})").await.unwrap(); // no _id → generated
    }
    let db = Db::open(local_config(dir.path())).await.unwrap();
    db.execute("INSERT (:G {v: 2})").await.unwrap(); // must NOT reuse the id
    assert_eq!(rows(&db.query("MATCH (g:G) RETURN g.v").await.unwrap()), 2);
}

#[tokio::test]
async fn db_local_convenience_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Db::local(dir.path()).await.unwrap();
        db.execute("INSERT (:L {_id: 1})").await.unwrap();
    }
    let db = Db::local(dir.path()).await.unwrap();
    assert_eq!(rows(&db.query("MATCH (l:L) RETURN l._id").await.unwrap()), 1);
}

#[tokio::test]
async fn memory_backend_via_config_and_default() {
    // Explicit memory backend…
    let db = Db::open(Config::from_toml_str("[log]\nbackend = \"memory\"").unwrap())
        .await
        .unwrap();
    db.execute("INSERT (:M {_id: 1})").await.unwrap();
    // …and no [log] section at all defaults to memory.
    let db = Db::open(Config::from_toml_str("").unwrap()).await.unwrap();
    db.execute("INSERT (:M {_id: 1})").await.unwrap();
}

#[tokio::test]
async fn unknown_backend_error_lists_available() {
    let err = Db::open(Config::from_toml_str("[log]\nbackend = \"kafka\"").unwrap())
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("kafka"), "{err}");
    assert!(err.contains("local"), "{err}");
    assert!(err.contains("memory"), "{err}");
}

#[tokio::test]
async fn open_from_config_file() {
    let dir = tempfile::tempdir().unwrap();
    let log_dir = dir.path().join("log");
    let config_path = dir.path().join("varve.toml");
    std::fs::write(
        &config_path,
        format!(
            "[log]\nbackend = \"local\"\n[log.local]\ndir = {}\n",
            toml_escaped(&log_dir)
        ),
    )
    .unwrap();

    let db = Db::open(Config::from_file(&config_path).unwrap()).await.unwrap();
    db.execute("INSERT (:F {_id: 1})").await.unwrap();
    drop(db);
    let db = Db::open(Config::from_file(&config_path).unwrap()).await.unwrap();
    assert_eq!(rows(&db.query("MATCH (f:F) RETURN f._id").await.unwrap()), 1);
}
```

Append to `crates/varve-config/tests/config_test.rs` (discharges the slice-0 deferred item "Config::from_file / ConfigError::Io have no direct test"):

```rust
#[test]
fn from_file_reads_and_missing_file_is_io_error() {
    use varve_config::ConfigError;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("varve.toml");
    std::fs::write(&path, "[log]\nbackend = \"memory\"\n").unwrap();
    let cfg = Config::from_file(&path).unwrap();
    assert_eq!(cfg.section("log").unwrap().backend(), Some("memory"));

    let err = Config::from_file(&dir.path().join("absent.toml")).unwrap_err();
    assert!(matches!(err, ConfigError::Io(_)));
}
```

Add `tempfile` to `crates/varve-config/Cargo.toml` and `crates/varve/Cargo.toml` `[dev-dependencies]`:

```toml
tempfile = { workspace = true }
```

- [x] **Step 2: Run tests to verify they fail**

Run: `cargo test -p varve --test durability`
Expected: compile errors — `varve::Config` not exported, `Db::open`/`Db::local` missing.

- [x] **Step 3: Write minimal implementation**

Add to `crates/varve-engine/src/db.rs` (imports grow: `use crate::registries::Registries; use crate::writer::NODES_TABLE; use varve_config::{Config, ConfigError, ConfigSection, RegistryError}; use varve_index::decode_events; use varve_log::{LocalLog, Log, DEFAULT_SEGMENT_MAX_BYTES}; use varve_types::LogPosition; use std::path::Path;`):

New `EngineError` variants:

```rust
    #[error(transparent)]
    Registry(#[from] RegistryError),
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error("log record references unknown table '{0}'")]
    UnknownTable(String),
```

Group-commit tuning deserialization (unknown keys in `[log]` — `backend`, the `local` subtable — are ignored by serde's defaults):

```rust
#[derive(serde::Deserialize)]
struct LogTuning {
    #[serde(default = "default_window_ms")]
    group_commit_window_ms: u64,
    #[serde(default = "default_max_bytes")]
    group_commit_max_bytes: usize,
}

fn default_window_ms() -> u64 {
    15
}

fn default_max_bytes() -> usize {
    8 * 1024 * 1024
}
```

New `impl Db` methods + free `replay` fn:

```rust
    /// Opens a database from configuration using the built-in backends
    /// (spec §11: `Db::open(Config::from_file("varve.toml")?)`).
    pub async fn open(config: Config) -> Result<Db, EngineError> {
        Self::open_with(&config, &Registries::with_builtins()).await
    }

    /// Like [`Db::open`], but with caller-supplied registries — the spec §4
    /// extension point: register custom `Log`/`Clock` factories, then open.
    pub async fn open_with(config: &Config, registries: &Registries) -> Result<Db, EngineError> {
        let log_section = config.section("log").unwrap_or_else(ConfigSection::empty);
        let log = registries
            .log
            .build(log_section.backend().unwrap_or("memory"), &log_section)?;
        let clock_section = config.section("clock").unwrap_or_else(ConfigSection::empty);
        let clock = registries
            .clock
            .build(clock_section.backend().unwrap_or("system"), &clock_section)?;
        let tuning: LogTuning = log_section.get()?;
        let cfg = WriterConfig {
            window: Duration::from_millis(tuning.group_commit_window_ms),
            max_bytes: tuning.group_commit_max_bytes,
        };
        let (live, next_tx_id) = replay(log.as_ref(), clock.as_ref()).await?;
        Ok(Self::assemble(live, log, clock, cfg, next_tx_id))
    }

    /// Local-filesystem database at `dir` with default tuning (spec §11
    /// `Db::local(path)` convenience — no config file needed).
    pub async fn local(dir: impl AsRef<Path>) -> Result<Db, EngineError> {
        let log: Arc<dyn Log> = Arc::new(LocalLog::open(dir.as_ref(), DEFAULT_SEGMENT_MAX_BYTES)?);
        let clock: Arc<dyn Clock> = Arc::new(MonotonicClock::new());
        let (live, next_tx_id) = replay(log.as_ref(), clock.as_ref()).await?;
        Ok(Self::assemble(live, log, clock, WriterConfig::default(), next_tx_id))
    }
```

```rust
/// Spec §6 recovery: fold the whole log into a fresh live index; floor the
/// clock and tx counter above everything replayed. Blocks + manifest
/// watermarks (replay-from-position) arrive in slice 4.
async fn replay(
    log: &dyn varve_log::Log,
    clock: &dyn Clock,
) -> Result<(LiveTable, u64), EngineError> {
    let mut live = LiveTable::new();
    let mut next_tx_id = 0u64;
    let mut max_system: Option<Instant> = None;
    for (_position, record) in log.tail(LogPosition::ZERO).await? {
        for effect in &record.effects {
            if effect.table != NODES_TABLE {
                return Err(EngineError::UnknownTable(effect.table.clone()));
            }
            for event in decode_events(&effect.arrow_ipc)? {
                live.append(event)?;
            }
        }
        next_tx_id = next_tx_id.max(record.tx_id);
        let system = Instant::from_micros(record.system_time_us);
        max_system = Some(max_system.map_or(system, |m| m.max(system)));
    }
    if let Some(floor) = max_system {
        clock.advance_to(floor);
    }
    Ok((live, next_tx_id))
}
```

`crates/varve/src/lib.rs`:

```rust
pub use datafusion::arrow::record_batch::RecordBatch;
pub use varve_config::{Config, ConfigError};
pub use varve_engine::{Db, EngineError, Registries, TxReceipt};
pub use varve_types::{Instant, TemporalBounds, TemporalDimension};
```

`crates/varve/Cargo.toml` `[dependencies]` gains:

```toml
varve-config = { path = "../varve-config" }
```

- [x] **Step 4: Run tests to verify they pass**

Run: `cargo test -p varve -p varve-config`
Expected: all pass (7 durability tests + existing walking-skeleton/temporal + new config tests).

- [x] **Step 5: Rustdoc sweep on varve-config's public API**

Discharges the slice-0 deferred item ("rustdoc on the public varve-config API … before the API grows consumers" — `Db::open_with` is that consumer). Add `///` doc comments in `crates/varve-config/src/config.rs` and `registry.rs` to every public item that lacks one — `Config` (TOML root + env overrides: `VARVE__SECTION__KEY`, `__`-nesting, bool→int→float→string coercion), `Config::from_toml_str`, `Config::from_file`, `Config::section`, `ConfigSection` (+ `backend`, `child`, `get`), `ConfigError` (variant meanings), `ComponentFactory` (contract: `name` is the registry key; `build` reads its own section), `Registry` (+ `new`, `register`, `build`, `names`), `RegistryError`. Two to four lines each, stating contract + one example key path; no behavior changes. Then:

Run: `cargo doc -p varve-config --no-deps 2>&1 | grep -i warn; cargo test -p varve-config`
Expected: no warnings, tests green.

- [x] **Step 6: Run the full gate**

Run: `just check`
Expected: green (fmt, clippy `-D warnings`, full workspace).

- [x] **Step 7: Commit**

```bash
git add crates/varve-engine/ crates/varve/ crates/varve-config/
git commit -m "feat: Db::open with registry-selected log backend and replay recovery"
```

---
### Task 11: Crash harness — kill -9 fault matrix in varve-testkit

Roadmap: "spawn child process doing writes, `kill -9` at injected fault points (pre-append, post-append-pre-ack, post-ack), restart, assert: every acked tx present, no unacked tx visible, log parses cleanly" — with the contract formalized per design decision 8.

**Files:**
- Modify: `crates/varve-testkit/Cargo.toml` (deps += `varve`, `varve-log` with `fault-injection`, `tokio`; dev-dep `tempfile`)
- Create: `crates/varve-testkit/src/bin/crash_child.rs`
- Create: `crates/varve-testkit/tests/crash_recovery.rs`
- Modify: `.github/workflows/ci.yml` (crash-matrix job), `justfile` (`crash` recipe)

**Interfaces:**
- Consumes: `crash_point` hooks in `LocalLog::append` (Task 5, feature `fault-injection`, armed via the `VARVE_CRASH_TRIGGER` trigger file), `Db::local` (Task 10), `LocalLog`/`decode_events` for direct log validation.
- Produces:
  - Binary `crash_child <work-dir> <point> <acked-count>`: opens `Db::local(<work>/log)`, performs `<acked-count>` inserts (`INSERT (:Crash {_id: i, seq: i})`), records each ack durably in `<work>/acked.txt` (line + fsync), then per `<point>`: `post-ack` → print `CRASH_POINT post-ack` and park · `pre-append`/`post-append` → write the point name into `<work>/trigger` and attempt one more insert (the armed hook prints and parks) · `none` → exit 0 (clean-run sanity).
  - Test `crash_recovery.rs`: for each point × `VARVE_CRASH_ITERS` iterations (default 3; CI job runs 100): spawn child, wait for the `CRASH_POINT` stdout marker (30 s deadline), deliver SIGKILL, then assert per point — **pre-append:** survivors == acked (the in-flight tx never surfaces), log has exactly K records · **post-append:** acked ⊆ survivors, log has K or K+1 records (durable-but-unacked may legally surface) · **post-ack:** survivors == acked == K. In every case the log parses cleanly end-to-end (frames + protobuf + Arrow payload decode) and `Db::local` reopens successfully.
- Feature note: `varve-testkit`'s dependency enables `fault-injection` for workspace builds via feature unification — the hooks are inert unless `VARVE_CRASH_TRIGGER` names an armed file, and downstream (non-workspace) consumers of `varve` never enable the feature.

- [x] **Step 1: Wire the manifests and write the child**

`crates/varve-testkit/Cargo.toml` — replace `[dependencies]` (bin targets need regular deps) and add dev-deps:

```toml
[dependencies]
proptest = { workspace = true }
varve-index = { path = "../varve-index" }
varve-types = { path = "../varve-types" }
varve = { path = "../varve" }
varve-log = { path = "../varve-log", features = ["fault-injection"] }
tokio = { workspace = true }

[dev-dependencies]
tempfile = { workspace = true }
arrow = { workspace = true }
```

(The `crash_child` binary is auto-discovered from `src/bin/`; no `[[bin]]` section needed. `CARGO_BIN_EXE_crash_child` becomes available inside this package's integration tests. The `arrow` dev-dep is the same crate DataFusion re-exports — slice-1 pin invariant — used to downcast query results in the matrix test.)

`crates/varve-testkit/src/bin/crash_child.rs`:

```rust
//! Crash-test child (slice 3 harness): does K acked inserts against a
//! local-log Db, durably records each ack, then arms the requested crash
//! point and lets the parent deliver `kill -9`.
//
// A test-support binary, not library code: unwrap/expect read better than
// error plumbing that would itself abort the process anyway.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::io::Write as _;
use std::path::PathBuf;

fn append_acked(path: &PathBuf, seq: u64) {
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .expect("open acked file");
    writeln!(file, "{seq}").expect("record ack");
    // The acked file is the harness's ground truth — it must not itself
    // lose acked lines to the kill.
    file.sync_all().expect("fsync acked file");
}

fn park_for_kill(point: &str) -> ! {
    println!("CRASH_POINT {point}");
    std::io::stdout().flush().expect("flush marker");
    loop {
        std::thread::sleep(std::time::Duration::from_secs(3600));
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let mut args = std::env::args().skip(1);
    let work: PathBuf = args.next().expect("usage: crash_child <work> <point> <k>").into();
    let point = args.next().expect("crash point");
    let k: u64 = args.next().expect("acked count").parse().expect("k as u64");

    // The parent set VARVE_CRASH_TRIGGER=<work>/trigger; the file does not
    // exist yet, so every hook is inert until we arm it below.
    let db = varve::Db::local(work.join("log")).await.expect("open db");
    let acked_path = work.join("acked.txt");

    for i in 1..=k {
        db.execute(&format!("INSERT (:Crash {{_id: {i}, seq: {i}}})"))
            .await
            .expect("acked insert");
        append_acked(&acked_path, i);
    }

    match point.as_str() {
        "none" => {} // clean run: exit 0
        "post-ack" => park_for_kill("post-ack"),
        p @ ("pre-append" | "post-append") => {
            std::fs::write(work.join("trigger"), p).expect("arm trigger");
            // This insert hits the armed hook inside LocalLog::append and
            // parks there; the parent kills us. Reaching the line after the
            // await means the hook failed to fire.
            let _ = db
                .execute(&format!("INSERT (:Crash {{_id: {n}, seq: {n}}})", n = k + 1))
                .await;
            eprintln!("crash point {p} never fired");
            std::process::exit(2);
        }
        other => {
            eprintln!("unknown crash point '{other}'");
            std::process::exit(2);
        }
    }
}
```

- [x] **Step 2: Write the failing matrix test**

`crates/varve-testkit/tests/crash_recovery.rs`:

```rust
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::Duration;
use varve_log::{LocalLog, DEFAULT_SEGMENT_MAX_BYTES};
use varve_types::LogPosition;

const K: u64 = 5;

fn iterations() -> usize {
    std::env::var("VARVE_CRASH_ITERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3)
}

fn spawn_child(work: &Path, point: &str) -> Child {
    Command::new(env!("CARGO_BIN_EXE_crash_child"))
        .arg(work)
        .arg(point)
        .arg(K.to_string())
        .env("VARVE_CRASH_TRIGGER", work.join("trigger"))
        .stdout(Stdio::piped())
        .spawn()
        .unwrap()
}

/// Waits for the child's CRASH_POINT marker, then delivers SIGKILL.
fn wait_for_marker_and_kill(mut child: Child, point: &str) {
    let stdout = child.stdout.take().unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    let expected = format!("CRASH_POINT {point}");
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            let Ok(line) = line else { return };
            if line.starts_with("CRASH_POINT") {
                let _ = tx.send(line);
                return;
            }
        }
    });
    let line = rx
        .recv_timeout(Duration::from_secs(30))
        .expect("child never reached its crash point");
    assert_eq!(line, expected);
    child.kill().unwrap(); // SIGKILL on unix — no destructors, no flushes
    child.wait().unwrap();
}

fn acked_seqs(work: &Path) -> Vec<u64> {
    match std::fs::read_to_string(work.join("acked.txt")) {
        Ok(text) => text.lines().map(|l| l.parse().unwrap()).collect(),
        Err(_) => vec![],
    }
}

/// Full log validation: every frame, every protobuf record, every Arrow
/// effect payload must decode ("log parses cleanly"). Returns record count.
async fn parse_log(work: &Path) -> usize {
    let log = LocalLog::open(&work.join("log"), DEFAULT_SEGMENT_MAX_BYTES).unwrap();
    let records = log.tail(LogPosition::ZERO).await.unwrap();
    for (_, record) in &records {
        for effect in &record.effects {
            varve_index::decode_events(&effect.arrow_ipc).unwrap();
        }
    }
    records.len()
}

async fn surviving_seqs(work: &Path) -> Vec<u64> {
    use arrow::array::Int64Array; // workspace arrow == DataFusion's re-export (slice-1 pin invariant)
    let db = varve::Db::local(work.join("log")).await.unwrap();
    let batches = db.query("MATCH (c:Crash) RETURN c.seq AS seq").await.unwrap();
    let mut seqs = Vec::new();
    for batch in &batches {
        let col: &Int64Array = batch
            .column_by_name("seq")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        for i in 0..col.len() {
            seqs.push(col.value(i) as u64);
        }
    }
    seqs.sort_unstable();
    seqs
}

#[tokio::test]
async fn clean_run_sanity() {
    let work = tempfile::tempdir().unwrap();
    let status = spawn_child(work.path(), "none").wait().unwrap();
    assert!(status.success());
    assert_eq!(acked_seqs(work.path()), (1..=K).collect::<Vec<_>>());
    assert_eq!(surviving_seqs(work.path()).await, (1..=K).collect::<Vec<_>>());
    assert_eq!(parse_log(work.path()).await, K as usize);
}

#[tokio::test]
async fn crash_matrix() {
    for _ in 0..iterations() {
        for point in ["pre-append", "post-append", "post-ack"] {
            let work = tempfile::tempdir().unwrap();
            let child = spawn_child(work.path(), point);
            wait_for_marker_and_kill(child, point);

            let acked = acked_seqs(work.path());
            let records = parse_log(work.path()).await; // must not panic
            let survived = surviving_seqs(work.path()).await;
            for a in &acked {
                assert!(survived.contains(a), "{point}: acked tx {a} lost");
            }
            match point {
                "pre-append" => {
                    // Killed before any byte hit the log: the in-flight tx
                    // must not surface, and history is exactly the acked set.
                    assert_eq!(acked, (1..=K).collect::<Vec<_>>(), "{point}");
                    assert_eq!(survived, acked, "{point}: unacked tx surfaced");
                    assert_eq!(records, K as usize, "{point}");
                }
                "post-append" => {
                    // Durable but unacked: the final tx MAY legally surface.
                    assert_eq!(acked, (1..=K).collect::<Vec<_>>(), "{point}");
                    assert!(
                        survived.len() as u64 == K || survived.len() as u64 == K + 1,
                        "{point}: {survived:?}"
                    );
                    assert!(records == K as usize || records == K as usize + 1, "{point}");
                }
                "post-ack" => {
                    assert_eq!(acked, (1..=K).collect::<Vec<_>>(), "{point}");
                    assert_eq!(survived, acked, "{point}");
                    assert_eq!(records, K as usize, "{point}");
                }
                _ => unreachable!(),
            }
        }
    }
}
```


- [x] **Step 3: Run the matrix locally**

Run: `cargo test -p varve-testkit --test crash_recovery`
Expected: `clean_run_sanity` + `crash_matrix` pass (3 iterations × 3 points). First run will be RED until any harness/child wiring bug is fixed — iterate until deterministically green. Then run once more with more iterations to shake out flake:

Run: `VARVE_CRASH_ITERS=25 cargo test -p varve-testkit --release --test crash_recovery`
Expected: green, no flake.

- [x] **Step 4: CI job + justfile recipe**

Append to `.github/workflows/ci.yml` (sibling of the existing jobs — the roadmap exit criterion "run 100× in CI without flake"):

```yaml
  crash-matrix:
    if: github.event_name != 'schedule'
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - uses: Swatinem/rust-cache@v2
      - run: cargo test -p varve-testkit --release --test crash_recovery
        env:
          VARVE_CRASH_ITERS: "100"
```

Append to `justfile`:

```make
crash:
    VARVE_CRASH_ITERS=10 cargo test -p varve-testkit --release --test crash_recovery
```

- [x] **Step 5: Run the full gate**

Run: `just check`
Expected: green (the default 3-iteration matrix runs inside `cargo test --workspace` too).

- [x] **Step 6: Commit**

```bash
git add crates/varve-testkit/ crates/varve-log/ .github/ justfile
git commit -m "test: kill -9 crash-recovery matrix with injected fault points"
```

---

### Task 12: Write-throughput smoke bench + slice wrap-up

Exit criterion: "write throughput smoke bench recorded in STATUS.md". A rough number, not criterion (that's slice 11).

**Files:**
- Create: `crates/varve/examples/write_bench.rs`

**Interfaces:**
- Consumes: `Db::memory`, `Db::local` (public API only).
- Produces: `cargo run --release --example write_bench -p varve` printing tx/s for the `memory` and `local (fsync)` profiles — the slice's demo command.

- [x] **Step 1: Write the bench example**

`crates/varve/examples/write_bench.rs`:

```rust
//! Write-throughput smoke bench (slice-3 exit criterion; record the printed
//! numbers in STATUS.md). Not criterion — the real suite is slice 11.
//! Run: cargo run --release --example write_bench -p varve

use std::sync::Arc;
use std::time::Instant;
use varve::Db;

const TOTAL: u64 = 4_000;
const WORKERS: u64 = 8;

async fn bench(label: &str, db: Db) -> Result<(), Box<dyn std::error::Error>> {
    let db = Arc::new(db);
    let start = Instant::now();
    let mut handles = Vec::new();
    for worker in 0..WORKERS {
        let db = Arc::clone(&db);
        handles.push(tokio::spawn(async move {
            for i in 0..TOTAL / WORKERS {
                let id = worker * 1_000_000 + i;
                db.execute(&format!("INSERT (:Bench {{_id: {id}, v: {i}}})"))
                    .await?;
            }
            Ok::<(), varve::EngineError>(())
        }));
    }
    for handle in handles {
        handle.await??;
    }
    let elapsed = start.elapsed();
    println!(
        "{label:>14}: {TOTAL} txs / {WORKERS} workers → {:>8.0} tx/s  ({elapsed:.2?})",
        TOTAL as f64 / elapsed.as_secs_f64()
    );
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    bench("memory", Db::memory()).await?;

    let dir = std::env::temp_dir().join(format!("varve-write-bench-{}", std::process::id()));
    bench("local (fsync)", Db::local(&dir).await?).await?;
    std::fs::remove_dir_all(&dir)?;
    Ok(())
}
```

- [x] **Step 2: Run it and record the numbers**

Run: `cargo run --release --example write_bench -p varve`
Expected: two lines of tx/s output; `local (fsync)` is group-commit-bound (~batches of ≤8 per 15 ms window on this workload — hundreds to thousands of tx/s depending on disk). Copy both numbers into the STATUS.md update below.

- [x] **Step 3: Verify the demos still work**

Run: `cargo run --example hello -p varve && cargo run --example time_travel -p varve`
Expected: both print their slice-1/slice-2 outputs unchanged.

- [x] **Step 4: Run the full gate one last time**

Run: `just check`
Expected: green.

- [x] **Step 5: Commit**

```bash
git add crates/varve/
git commit -m "feat: write-throughput smoke bench example"
```

---

## Slice exit checklist

- [x] `just check` green: fmt, clippy `-D warnings`, all workspace tests (including the 3-iteration crash matrix and both property suites).
- [x] Roadmap slice-3 exit criteria verified:
  - crash matrix green and flake-checked (`VARVE_CRASH_ITERS=25` release run in Task 11; CI job runs 100),
  - write-throughput smoke bench numbers recorded in STATUS.md,
  - config selects `log = "local"` vs `"memory"` via registry (`varve/tests/durability.rs::memory_backend_via_config_and_default`, `unknown_backend_error_lists_available`).
- [x] Slice-2 deferrals verified dissolved: `grep -rn "await_holding_lock" crates/` returns nothing; the `db.rs` "SINGLE-WRITER" caveat comment is gone; concurrent `execute()` is tested (`varve-engine/tests/concurrency.rs`).
- [x] Update `docs/plans/STATUS.md`:
  - Current position: slice 3 ✅ complete; next action = generate the slice-4 detailed plan (blocks & persisted scan, spec §9) with the writing-plans skill.
  - Slice log row: `3 durability (log) | ✅ complete | <sessions> | cargo run --release --example write_bench -p varve | Log trait + prost envelope + memory/local backends (CRC32C, fsync, torn-tail recovery) + writer loop group commit + Db::open replay + kill -9 crash matrix; <N> workspace tests`.
  - Record the bench numbers (memory + local tx/s, machine noted).
  - Decisions to record (from this plan's "Design decisions" section, abbreviated): writer loop resolves serially / applies after durable / acks after apply; reading DML flushes the staged batch; failed append ⇒ `CommitFailed` acks, state untouched; per-record positions, batch = durability unit; prost derive without protoc (`prost 0.14.x`, `crc32c 0.6.x`, `tempfile 3.x` resolved versions); doc/labels as canonical-binary `payload` column (columnar docs → slice 4); generated ids now `varve:gen:{tx_id}:{ordinal}` (durable); `Clock` trait + `Registries` landed (slice-0/2 deferrals discharged); `group_commit_max_bytes` is an integer (spec's `"8MiB"` string form deferred); `Db::memory()` window = 0; submission queue constant 256 (slice 10 makes it config); no local-log lock file (deployment enforces one writer); crash contract formalized (durable-but-unacked may surface).
  - Open items: mark the slice-0 "rustdoc + `Config::from_file` test" deferral RESOLVED; update the `BuildContext` item — still deferred deliberately (config-only factories suffice; revisit at slice 5 when a factory needs another component); note `fault-injection` feature unification is workspace-only.
  - Environment facts: tokio workspace features now `rt-multi-thread, macros, sync, time`; CI gained the `crash-matrix` job.
- [x] Tick the slice-3 checkboxes in `docs/plans/varve-v1-roadmap.md` (with parenthetical notes: TxReceipt item was already done in slice 2; "no unacked tx visible" formalized as pre-durability kills) and tick all checkboxes in this plan.
- [x] Commit:

```bash
git add docs/plans/
git commit -m "docs: slice 3 complete — durability shipped; STATUS and roadmap updated"
```






