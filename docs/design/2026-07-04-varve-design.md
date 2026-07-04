# Varve — Design Specification

**Date:** 2026-07-04
**Status:** Approved design, pre-implementation
**Name:** Varve (a *varve* is an annual layer of sediment — geologists read time out of layered deposits, exactly as this database reads history out of layered, immutable Arrow files). Product: **VarveDB** · library crate: `varve` · server binary: `varved` · CLI: `varve`.

---

## 1. Vision

Varve is a **bitemporal property-graph database** written in Rust that speaks **GQL (ISO/IEC 39075)**. It runs embedded on a laptop with minimal resources and scales to enterprise workloads through a storage/compute-separated architecture over any **S3-API-compatible object store**.

**Digital sovereignty is a core requirement, not a feature.** Varve must run entirely on open-source infrastructure under the operator's control. It depends on nothing but a filesystem or an S3-compatible store — with first-class support for sovereign backends: **Garage** (AGPLv3), **Ceph RGW** (LGPL-2.1), **SeaweedFS** (Apache-2.0). No proprietary cloud service is ever required. AWS S3 works but is never assumed.

Inspiration: XTDB 2.x (architecture studied in depth from `refs/xtdb`; see §14 for what we adopt, simplify, and reject). Varve is not a port — it is a Rust-native redesign.

### v1 success criteria

1. `varve` crate embeds in a Rust program; `varved` serves HTTP on a network.
2. GQL practical core (§8) passes an adapted openCypher TCK suite plus Varve's own temporal conformance suite.
3. Full bitemporality: `FOR VALID_TIME` / `FOR SYSTEM_TIME` travel on both axes, retroactive corrections, GDPR erase.
4. Runs on local FS and on Garage, Ceph RGW, SeaweedFS, MinIO, AWS S3 (verified by an integration matrix in CI).
5. One designated writer + N stateless query nodes sharing a log and object store; read scale-out demonstrated.
6. Deterministic compaction keeps storage bounded and queries fast as history accumulates.
7. Crash-safe: `kill -9` at any point loses no acknowledged transaction (verified by crash tests).
8. Shippable artifacts: single static binary, Docker image, docs site, benchmark report.

### Non-goals for v1 (roadmap, behind interfaces)

Multi-writer log submission, interactive ACID sessions (`BEGIN`/`COMMIT`), Kafka log backend, Bolt/pgwire protocol frontends, GQL schema/graph-type DDL enforcement (closed graph types), `SHORTEST`/`CHEAPEST` path search, factorized / worst-case-optimal joins, RBAC, replication of the log itself.

---

## 2. Decisions record

| # | Decision | Choice | Rationale |
|---|----------|--------|-----------|
| D1 | Temporal model | **Bitemporal** (valid time + system time) | Fully general time travel; retroactive corrections; audit. |
| D2 | Query language | **GQL practical core** | ISO-standard differentiation; core is ~90% Cypher-shaped so the openCypher TCK can be adapted for testing. |
| D3 | Transaction model | **Log-serialized transactions** | The log is the serialization point; no lock manager, no MVCC conflict machinery. Assertion-carrying txns are a roadmap item. |
| D4 | API surface | **Embedded crate + HTTP server** (JSON + Arrow IPC) | DuckDB-style embedding for the laptop story; thin server for network use. Bolt/pgwire are roadmap frontends behind the protocol interface. |
| D5 | Coordination | **Designated-writer first**; CAS failover opt-in after capability probe | Sovereign backends (Garage: never CAS; Ceph/SeaweedFS: unconfirmed/buggy CAS) make CAS impossible to require. Steady-state performance is identical — CAS only automates failover. |
| D6 | Query engine substrate | **DataFusion** | Inherit a battle-tested vectorized executor, expressions, joins, aggregation, sort/spill. Custom pieces only where graph+temporal semantics demand. |
| D7 | Backends | **S3 API only, cloud-independent** | AWS, Garage, Ceph RGW, SeaweedFS, MinIO — anything speaking S3. R2 rejected (proprietary). MinIO documented as legacy (repo archived 2026-04). |
| D8 | Architecture style | **Interfaces + registry + composition** | Every subsystem behind a trait; implementations chosen by registry lookup from external configuration; roadmap features slot in without core changes. |
| D9 | Name | **Varve** | `varve` free on crates.io; zero tech overload; the metaphor is the architecture. |

---

## 3. Macro-architecture

Two **golden stores** own all durable state; processes are otherwise stateless and disposable:

1. **Transaction log** — an ordered stream of *resolved transaction effects*.
2. **Object store** — immutable Arrow files (blocks, tries, manifests).

Three **roles**, composable into one process or many:

```
                     ┌────────────────────────────────────────────┐
 clients ──GQL──►    │ WRITER (designated, exactly one)           │
                     │  parse → plan → execute DML on snapshot    │
                     │  → resolved effects → group-commit to log  │
                     │  → apply to live index                     │
                     │  → flush full blocks to object store       │
                     └───────┬───────────────────────┬────────────┘
                             │ append                │ put blocks/tries
                             ▼                       ▼
                     ┌──────────────┐        ┌──────────────────┐
                     │ LOG          │        │ OBJECT STORE     │
                     │ (local file │        │ (local FS │ S3   │
                     │  │ S3 API)  │        │  API: Garage,    │
                     └──────┬───────┘        │  Ceph, Seaweed…) │
                            │ tail           └────────┬─────────┘
                            ▼                         │ get (cached)
                     ┌────────────────────────────────┴───────────┐
 clients ──GQL──►    │ QUERY NODES (0..N, stateless)              │
                     │  tail log → apply effects to own live index│
                     │  read blocks via mem+disk cache            │
                     └────────────────────────────────────────────┘
                     ┌────────────────────────────────────────────┐
                     │ COMPACTOR (any node; deterministic,        │
                     │  coordination-free, byte-identical output) │
                     └────────────────────────────────────────────┘
```

- **Laptop profile:** all three roles in one process, log = local file, storage = local directory. Zero dependencies.
- **Server profile:** one writer process (enforced by systemd/K8s StatefulSet), N query processes, compactor embedded in the writer or standalone.
- Followers apply *resolved effects* — they never re-execute GQL, so they need no query engine to stay current, and replay is deterministic by construction.

**One log, not two.** XTDB needs source+replica logs because clients submit unresolved transactions to the log and a leader resolves them. Varve v1 clients submit over the writer's API; the writer resolves DML against its snapshot at the current log head and appends only resolved effects. Multi-writer log submission is a roadmap feature behind the `Log` interface (requires CAS or Kafka).

---

## 4. Composition: interfaces, registry, configuration

Every subsystem is defined by a trait in a core crate and accessed through a **component registry** so that implementations — including roadmap features — plug in without touching the engine.

```rust
// varve-config
pub trait ComponentFactory<T: ?Sized>: Send + Sync {
    fn kind(&self) -> &'static str;          // e.g. "log"
    fn name(&self) -> &'static str;          // e.g. "object-store"
    fn build(&self, cfg: &ConfigSection, ctx: &BuildContext) -> Result<Arc<T>>;
}

pub struct Registry { /* (kind, name) → factory, explicit registration */ }
```

- **Explicit registration** (no link-time magic): `Registry::with_builtins()` registers everything compiled in; Cargo features gate optional backends; embedding applications may register custom implementations before opening a database.
- **Pluggable interfaces (v1 set):** `Log`, `ObjectStore` (thin wrapper over the `object_store` crate), `Coordinator` (designated-writer | cas-failover), `CacheTier`, `ProtocolFrontend` (http; bolt/pgwire are roadmap), `FunctionRegistry` (GQL scalar/aggregate functions), `Clock` (mockable for tests), `MetricsSink`, `Authenticator`.
- **Configuration:** TOML file (+ env-var overrides), deserialized with serde; each `[section]` names a registered implementation and carries its settings:

```toml
[node]
roles = ["writer", "query", "compactor"]   # laptop: all three

[log]
backend = "object-store"                   # or "local", "memory"
group_commit_window_ms = 15
group_commit_max_bytes = "8MiB"

[storage]
backend = "s3"
[storage.s3]
endpoint = "https://garage.internal:3900"
bucket = "varve"
region = "garage"

[coordination]
mode = "designated-writer"                 # "cas-failover" requires probe pass
capability_probe = "on-startup"

[cache]
memory = "512MiB"
disk_path = "/var/lib/varve/cache"
disk_max = "50GiB"
```

- **Node assembly** is a pure function: `Config → Registry lookups → composed Node`. The same code path builds the embedded node and the server node.

---

## 5. Data model

### 5.1 Property graph → tables

Each **graph** is a namespace containing two internal tables: `nodes` and `edges`.

- **Node:** `_id` (user-supplied or generated), label set (0..n strings), open property map.
- **Edge:** `_id`, exactly one label, `_src` / `_dst` node references, open property map.
- Properties are schema-optional ("open graph type" in GQL terms). Property values: the GQL scalar types mapped to Arrow types — BOOL, INT64, FLOAT64, STRING (UTF-8), BYTES, DATE, ZONED DATETIME (µs, UTC-normalized + offset), LOCAL DATETIME, DURATION, plus homogeneous LISTs. Per-column Arrow types are unioned across a block (dense union for heterogeneous columns).
- v1 ships named graphs cheaply (they are just table-prefix namespaces): `CREATE GRAPH`, `DROP GRAPH`, session `USE g`. Default graph: `default`.

### 5.2 Events — the unit of storage (adopted from XTDB)

Every mutation becomes an immutable **event**:

```
_iid          FixedSizeBinary(16)   // xxh3-128 of (graph, table, _id) — see §5.3; uniform key distribution
_system_from  Timestamp(µs, UTC)    // assigned by the writer, monotonic per log
_valid_from   Timestamp(µs, UTC)    // user-controllable (defaults to system_from)
_valid_to     Timestamp(µs, UTC)    // user-controllable (defaults to ∞)
op            Union { put(doc struct), delete, erase }
```

- `_system_to` and effective valid ranges are **never stored** — they are derived at read time by bitemporal resolution (§7). This is the key insight that makes the store append-only.
- Events live in Arrow IPC files sorted by `_iid`, then `_system_from` **descending** (newest first ⇒ current-time queries touch a prefix per entity).
- Edges additionally carry `_src_iid` / `_dst_iid` columns. Adjacency is served by maintaining the edge table under **two sort orders**: the primary trie keyed by `_iid` and secondary tries keyed by `_src_iid` and `_dst_iid` (forward and reverse adjacency).

### 5.3 Identity

`_iid = xxh3_128(graph, table, _id)`. User `_id` may be any scalar; it is stored in the document. Hashing gives uniform distribution for trie partitioning regardless of user key patterns.

---

## 6. Log

**Interface:** append a batch of records, read a half-open range, tail from a position; positions are `(epoch: u16, offset: u48)` packed in a `u64` (epoch in the high 16 bits so packed values sort correctly). Epochs support future log migration/reset.

**Record content:** one record per transaction — protobuf envelope `{tx_id, system_time, user, effects}` where `effects` is Arrow IPC bytes per affected table (the resolved events). Protobuf for the envelope (evolvable), Arrow for the payload (zero-copy apply).

**Group commit:** the writer accumulates concurrently submitted transactions for up to `group_commit_window_ms` (default 15 ms) or `group_commit_max_bytes`, writes one log object/append, then acks all of them. Commit latency ≈ backend PUT latency + window; throughput scales with batching.

**Implementations (v1):**

| name | mechanism | fsync/durability |
|---|---|---|
| `memory` | ring buffer | none (tests) |
| `local` | segmented append-only files | fsync before ack; CRC32C per record; torn-write recovery truncates tail |
| `object-store` | one object per batch: `log/<epoch>/<offset-lexhex>.vlog` | durable on PUT success; readers list+tail with a small poll interval; designated writer needs no CAS — offsets are assigned locally |

Kafka backend: roadmap, same interface.

**Recovery:** on writer start — find latest block manifest (§9), replay log from the manifest's log position into the live index, resume. Acked-but-unflushed data is always recoverable from the log by construction.

---

## 7. Bitemporal engine

Rust port of XTDB's proven resolution algorithms (from `core/src/main/kotlin/xtdb/bitemporal/`):

- **Ceiling:** as events for one `_iid` are scanned newest-system-time-first, maintain the descending staircase of "system time above which this valid range is superseded".
- **Polygon:** each event's effective bitemporal rectangle set, computed against the ceiling; also yields **recency** = the youngest instant at which the event still matters, which drives current/historical file routing (§9).
- Resolution runs in exactly two places: **scan** (query time) and **compaction** — never at write time.
- Query-level temporal predicates become a `TemporalBounds` filter pushed into the scan: `AS OF t`, `FROM a TO b`, `BETWEEN`, `ALL`. Defaults: valid time AS OF now, system time AS OF latest-visible.
- **Correctness harness:** a naive `BTreeMap`-based reference implementation with the same API; property tests (proptest) assert the vectorized engine matches the reference on randomized histories — including same-timestamp batches, retroactive corrections, deletes, and erases.

---

## 8. GQL surface (practical core)

**Queries:** `MATCH` / `OPTIONAL MATCH` with full pattern syntax — node/edge patterns, label alternation (`|`), property maps, inline `WHERE`, bounded quantifiers (`{m,n}`, `*` capped by `max_path_depth` config), path variables; `FILTER`/`WHERE`; `LET`; `FOR` (list unwinding); `RETURN` with `DISTINCT`, aggregation (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `COLLECT`), `GROUP BY` (implicit), `ORDER BY`, `SKIP`/`OFFSET`, `LIMIT`; `UNION`; `EXISTS` subqueries; `CASE`; three-valued logic; scalar/string/list/temporal function library; parameters (`$param`).

**Mutations (executed on the writer):** `INSERT` (nodes/edges/paths), `SET` (properties, labels), `REMOVE`, `DELETE` / `DETACH DELETE`, plus `ERASE` (Varve extension — GDPR hard delete: an erase event hides the entity immediately and physically removes matching data at next compaction).

**Sessions/catalog (minimal):** `CREATE GRAPH`, `DROP GRAPH`, `USE`. Everything else in the catalog chapter is out of scope for v1.

**Temporal extensions** (clearly documented as Varve extensions; GQL has no temporal chapter):

```gql
FOR VALID_TIME AS OF TIMESTAMP '2024-01-01T00:00:00Z'
FOR SYSTEM_TIME AS OF ...        | FOR ... FROM a TO b | FOR ... ALL
MATCH (p:Person)-[k:KNOWS]->(q) RETURN p, q

INSERT (:Person {_id: 42, name: 'Ada'}) VALID FROM DATE '2020-01-01'
```

Scope-level (whole query) and per-`MATCH` placement both supported. History introspection via zero-argument functions on bound elements: `valid_from(p)`, `valid_to(p)`, `system_from(p)`.

**Parser:** hand-written tokenizer + recursive-descent/Pratt parser (GQL isn't LALR-friendly; hand-rolled gives the best error messages). The vendored openGQL ANTLR grammar (`resources/gql-grammar/`, Apache-2.0, see its README) is the syntax reference; CI additionally runs an ANTLR-generated parser (test-only, Java tooling) as a differential accept/reject oracle against `varve-gql` over the test corpus and fuzz inputs. AST is versioned and owned by `varve-gql`; grammar tested by fuzzing plus the adapted TCK.

**Conformance testing:** openCypher TCK scenarios mechanically translated (`CREATE`→`INSERT`, etc.) where the languages overlap; scenarios exercising non-GQL semantics are excluded with a recorded reason. A separate Varve temporal TCK covers the extensions.

---

## 9. Storage layout & indexes

All object keys live under a format-version prefix `v1/`. Block manifests are **database-wide** (they record the log position watermark, which is per-database); table data is namespaced per graph:

```
v1/
  writer.json                           # advertised writer identity/address (plain PUT)
  log/<epoch>/<offset-lexhex>.vlog      # only when log backend = object-store
  blocks/<block-lexhex>.manifest        # protobuf: log position, graphs/tables, trie inventory
  graphs/<graph>/tables/<table>/
    data/<trie-key>.arrow               # event columns (§5.2), sorted by _iid
    meta/<trie-key>.arrow               # serialized trie + per-page column/temporal stats
    adj-out/{data,meta}/<trie-key>.arrow  # edges only: same events keyed/sorted by _src_iid
    adj-in/{data,meta}/<trie-key>.arrow   # edges only: same events keyed/sorted by _dst_iid
```

Edge events are thus persisted under three tries (primary `_iid`, forward `_src_iid`, reverse `_dst_iid`) — the classic forward/backward adjacency layout; the two adjacency families are maintained by the same flush/compaction machinery with a different sort key.

- **Trie key** (adopted from XTDB): `l<level>-r<recency>-p<part>-b<block>` with lex-hex encoding so lexicographic listing equals logical ordering. `recency = c` (current) or a week date (historical).
- **Live index:** per table, an in-memory hash trie (branch factor 4 on `_iid` bits) over Arrow builders. Queries take cheap immutable snapshots (copy-on-write trie + frozen row count watermark). At `max_block_rows` (default 100k) or a flush timeout, the writer serializes tries+data to the object store, writes the block manifest (the commit point), then trims the log-replay watermark.
- **Meta files** carry per-page min/max for `_system_from`/`_valid_from`/`_valid_to`, per-column min/max + bloom filters, and HLL sketches — the scan prunes files and pages by IID range, temporal bounds, and predicates before touching data.
- **The manifest write is the atomic commit** of a block; a data file without its manifest entry is invisible garbage (cleaned by GC).

### Compaction

Deterministic and coordination-free (XTDB's key operational property): job selection is a pure function of the trie inventory, and output files are **byte-identical** regardless of which node runs the job — duplicate work is wasted CPU, never corruption.

- L0 (raw flushed blocks) → split by recency into L1-current + L1-historical (weekly buckets).
- 4 full files at level n, same partition → one file at level n+1, partitioned by the next 2 IID bits. Target file size ~100 MB, pages ~1k rows.
- Merge = k-way merge by (`_iid`, `_system_from` desc) with bitemporal resolution; `erase` events drop matching rows for good.
- GC removes superseded tries after a configurable retention window (manifest history preserves the ability to pin old snapshots until then).

### Caching (query path)

Two tiers keyed by object path + byte range: an in-memory Arrow buffer cache and a disk cache that survives restarts (LRU, ref-count-pinned while in use). Arrow IPC footers cached separately so a page read costs one ranged GET.

---

## 10. Query engine (DataFusion substrate)

Pipeline: GQL AST → **GraphPlan** (Varve-owned logical plan: pattern nodes, expansions, temporal scopes) → lowering to DataFusion `LogicalPlan` → DataFusion optimizer + custom rules → vectorized execution → Arrow record batches.

Custom components (everything else is stock DataFusion):

1. **`BitemporalScan` TableProvider/ExecutionPlan** — merges live-index snapshot + persisted tries; pushes down temporal bounds, IID point/range predicates (from `_id` equality), property predicates (via page stats/blooms); emits resolved rows.
2. **Pattern lowering** — a `MATCH` pattern becomes scans + hash joins on `_src_iid`/`_dst_iid`; label predicates become dictionary filters. Join order chosen by simple cardinality heuristics from HLL sketches (cost-based ordering is roadmap).
3. **`PathExpand` operator** — iterative expansion for quantified paths (`{m,n}`): breadth-wise joins with visited-set semantics per GQL walk mode, depth-capped; streams path bindings.
4. **Planner rules** — predicate pushdown through expansions, temporal-scope propagation, `LIMIT` pushdown into topmost expansions.
5. **Function registry bridge** — GQL functions registered as DataFusion UDFs/UDAFs behind the `FunctionRegistry` interface.

DML executes on the writer as: plan the reading part with the same engine at the current snapshot → compute effect events → append to log → apply to live index → ack.

---

## 11. API surface

### Embedded (primary)

```rust
let db = varve::Db::open(varve::Config::from_file("varve.toml")?)?;   // or ::memory(), ::local(path)
let receipt = db.execute("INSERT (:Person {_id: 1, name: 'Ada'})", params!{}).await?;  // → TxReceipt{tx_id, system_time}
let batches: Vec<RecordBatch> = db.query("MATCH (p:Person) RETURN p.name", params!{})
    .basis(receipt)      // read-your-writes: wait until this tx is visible
    .await?;
```

Also: streaming variant returning a `RecordBatchStream`, and a serde-friendly row iterator for ergonomics.

### Server (`varved`)

HTTP/1.1+2 (axum): `POST /v1/query` (JSON in; JSON or Arrow IPC stream out via `Accept`), `POST /v1/tx`, `GET /healthz`, `GET /metrics` (Prometheus), `GET /v1/status` (log position, block watermark, role). Auth: static bearer tokens in v1, behind the `Authenticator` interface; TLS via rustls. Write requests to a query node return 421 + the writer's advertised address.

**Consistency tokens:** every tx ack carries `(tx_id, system_time)`; queries may carry `basis = tx_id` — a query node blocks (bounded wait) until its live index has applied that position. Snapshot pinning for repeatable reads within a session: `basis = at:{log position}`.

### CLI

`varve shell` (interactive REPL against embedded dir or remote server), `varve import/export` (JSONL/CSV bulk load through the normal tx path), `varve admin` (status, compact, gc, verify).

---

## 12. Coordination, failover, operations

- **Designated-writer (default, works everywhere):** exactly one writer per database, enforced by deployment. The writer's identity+address is published in the object store (`v1/writer.json`, plain PUT). Query nodes are unlimited. Writer crash → orchestrator restarts it → log replay → resume; reads never stop.
- **CAS failover (opt-in):** on backends passing a startup **capability probe** (conditional-PUT semantics verified against a probe key, including the versioned-bucket edge case), standby writers race a lease object with `If-None-Match`/`If-Match`; the winner fences the epoch (log epoch increment makes stale-writer appends visible as dead). If the probe fails, Varve refuses `cas-failover` mode with a clear error naming the backend capability.
- **Backpressure:** bounded submission queue on the writer; slow query nodes fall behind on the log but never affect the writer; live-index memory watermark forces early block flush.
- **Observability:** `tracing` throughout, Prometheus metrics (ingest rate, log lag per node, cache hit ratios, compaction debt, query latency histograms), OpenTelemetry export behind the `MetricsSink` interface.

---

## 13. Testing & quality strategy

1. **Adapted openCypher TCK** — translated scenarios as the language conformance floor; tracked pass-rate dashboard; new failures block merge.
2. **Bitemporal property tests** — vectorized engine vs. naive reference model over randomized histories (proptest); millions of cases in nightly runs.
3. **Deterministic-compaction golden tests** — same input tries on different "nodes" (threads/seeds) must produce byte-identical output files.
4. **Crash-recovery matrix** — `kill -9` injected at every stage boundary (pre/post log append, pre/post manifest write); restart must preserve every acked tx and lose every unacked one.
5. **Backend integration matrix** — CI runs the full storage test suite against containers: Garage, SeaweedFS, Ceph (demo), MinIO (legacy), plus `LocalFileSystem`; includes the capability probe's expected result per backend.
6. **Fuzzing** — parser (cargo-fuzz), log-record decoder, Arrow file reader against corrupted inputs.
7. **Benchmarks** — criterion micro-benches + an end-to-end suite (social-graph workload: ingest, point reads, 2-hop, temporal AS-OF) with tracked regressions.

**Performance targets (v1, laptop = M-series/16 GB):** ≥10k write ops/s sustained (batched), warm point lookup < 1 ms, warm 2-hop over 1M-node graph < 50 ms, AS-OF historical query within 2× of current-time equivalent. Server: ≥5k tx/s on object-store log via group commit; query throughput scales ~linearly to 4 nodes on the read benchmark. Targets, not guarantees — tracked in the benchmark report.

---

## 14. Relationship to XTDB (adopt / simplify / reject)

- **Adopt:** golden-stores architecture; event model with derived `_system_to`; Ceiling/Polygon resolution; IID hash-trie LSM (branch 4, lex-hex keys, current/historical recency split, weekly buckets); deterministic coordination-free compaction; two-tier path-keyed cache; manifest-as-commit-point.
- **Simplify:** one log instead of source+replica (writer-resolved effects; multi-writer submission is roadmap); no Kafka dependency (object-store/local log); designated-writer instead of consumer-group leader election; single config format wired through a registry.
- **Reject / replace:** JVM+Clojure runtime → Rust; SQL/XTQL → GQL; pgwire-first → embedded-first + HTTP; document-relation model → property-graph model with adjacency tries; Postgres-compatibility constraints → none.

---

## 15. Workspace layout

```
crates/
  varve-types      # values, temporal types, IID, errors, Arrow mappings
  varve-config     # config loading, component registry, node assembly
  varve-log        # Log trait + memory/local impls; varve-log-object (feature)
  varve-storage    # object-store wrapper, layout, manifests, cache tiers
  varve-index      # live index, tries, bitemporal resolution (Ceiling/Polygon)
  varve-compact    # compactor jobs, merge, GC
  varve-gql        # tokenizer, parser, AST
  varve-plan       # GraphPlan, DataFusion lowering, custom operators
  varve-engine     # roles (writer/query/compactor), tx pipeline, snapshots
  varve            # public embedded facade (re-exports; the crate users depend on)
  varve-server     # varved binary (axum HTTP, auth, metrics)
  varve-cli        # varve binary (shell, import/export, admin)
  varve-testkit    # reference model, TCK harness, backend containers, crash-test rig
```

Rust stable, tokio, arrow-rs, DataFusion, `object_store`, axum, prost, proptest. Every optional backend/protocol is a Cargo feature + registry entry.

---

## 16. Risks & mitigations

| Risk | Mitigation |
|---|---|
| GQL spec access (ISO paywall) | **openGQL's Apache-2.0 GQL ANTLR grammar** (vendored at `resources/gql-grammar/`, ~571 parser rules generated from the published ISO/IEC 39075:2024 BNF) serves as grammar reference and CI differential-test oracle for `varve-gql`; openCypher TCK covers semantics; conformance claims scoped honestly, with deviations from the final standard recorded as they're discovered. |
| DataFusion API churn | Pin + quarterly upgrade window; custom operators isolated in `varve-plan` behind our own GraphPlan boundary. |
| Bitemporal resolution subtleties | Reference-model property testing from day one (slice 2); XTDB's `.allium` specs used as the porting spec. |
| Object-store log latency disappoints | Group commit tuning; local-log + async object mirror as a documented middle profile; Kafka backend roadmap. |
| Backend capability drift (SeaweedFS CAS bugs, etc.) | Startup capability probe + CI backend matrix pins tested versions; capability matrix in docs. |
| Scope creep toward full GQL | Feature list in §8 is the contract; everything else explicitly roadmap. |
