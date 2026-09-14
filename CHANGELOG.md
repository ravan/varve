# Changelog

All notable changes to Varve are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); this project adheres
to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## 0.1.5 (2026-09-14)

### Changed

- Point lookups skip blocks that cannot hold the key. Node ids are
  hash-derived, so every block has one page whose key range covers any id,
  and a point lookup read one page per block; its cost grew with block
  count. Each block now carries a Bloom filter over its rows' sort keys
  (`iid` for the primary table, `src`/`dst` for the adjacency families),
  stored beside data/meta/labels as `keys/<trie>.bin` and held in the
  footer cache. A point lookup or anchored adjacency hop skips every block
  whose filter rules the key out. Blocks written before this release have
  no filter and keep the old path until compaction rewrites them. GC
  protects and deletes the new object like the others.
- Block pages are fetched concurrently. A scan issued one page read at a
  time, so a point lookup over N blocks paid N round trips in sequence.
  Page reads across all tries now run through a bounded buffered stream
  (16 in flight); results keep file order within a block and block order
  across tries.

### Added

- New metric `varve_blocks_skipped_total` counts the page reads avoided by
  the per-block key filter.

### Fixed

- The config reference generator emits the missing `decoded_page_cache_bytes`
  row, so the doc pin test passes again.

## 0.1.4 (2026-09-14)

### Changed

- Block flush is asynchronous. The writer seals the full live table under
  one lock and continues into a fresh one while a background task encodes,
  uploads and commits the sealed block. Scans read live, sealed and blocks,
  so results are unchanged. One flush runs at a time; the writer waits if
  the live table fills again before the flush lands, which bounds memory at
  two blocks. Compaction and shutdown await an in-flight flush, and a failed
  flush keeps the sealed table for a later retry.
- JSON ingest decodes in parallel. The framer splits lines only; each chunk
  is decoded on the rayon thread pool, off the async workers, and chunks
  still reach the writer in stream order. CSV ingest is unchanged.
- Blocks are written with LZ4 Arrow IPC compression.

Together these cut a 400k-record ingest to object storage from 5.0s to 2.1s
(0.1.3 took 9.8s).

## 0.1.3 (2026-09-14)

### Added

- Per-block label index (`v1/graphs/{g}/tables/{t}/labels/{trie}.arrow`):
  a labelled scan (`MATCH (j:Job …)`) now touches only the entities that
  carry the label instead of decoding every page. Written by flush and
  compaction; blocks from before this release have none and keep the full
  scan until `compact_full_once` (or `varve compact --full`) rewrites them.
  `verify` checks the index against its block.
- Decoded-page cache, `[query] decoded_page_cache_bytes` (default 256 MiB,
  `"0B"` disables): repeated block scans skip the Arrow decode. New gauge
  `varve_block_pages_cached_total`.

### Changed

- The anchored fixed-path fast path accepts hops with different edge labels
  (one reachable-edge batch per label). Silt's 3-label `CollectionOf` path
  no longer falls back to five full edge scans per document.

## 0.1.2 (2026-09-08)

### Changed

- `GET /metrics` is public, like `/healthz`. Every `/v1/*` route still needs a
  bearer token. Silt's Prometheus scrapes Varve with no credential.

## 0.1.1 (2026-09-03)

### Added

- `DELETE … VALID FROM <dt> [TO <dt>]` / `VALID TO <dt>`: end (or window) a
  fact at a chosen valid time instead of the transaction's system time. Same
  clause as `INSERT … VALID …`; `MATCH` still reads current state. `ERASE`
  rejects the clause. Silt's document remap needs this to keep the old
  release visible under a valid-time view.

## 0.1.0 (2026-09-02)

Silt slice 0a: named graphs on the HTTP surface, an OIDC bearer backend, and
the writing subject in every write answer. Existing callers see no change
except two new response fields.

### Added

- `graph` field on `POST /v1/query` and `POST /v1/tx`, and `?graph=` on
  `POST /v1/ingest`, select the target graph without a `USE` prefix. A field
  plus a `USE` is `400`.
- `TxResponse.subject` and `IngestResponse.subject`: the authenticated
  principal, as written to the log. `TxReceipt.user` carries it in the engine.
- `[auth] backend = "oidc"`: bearer JWTs verified against JWKS issuers
  (RS256, ES256, EdDSA; `exp`, `nbf`, `aud`, clock skew; discovery; rate-limited
  key refresh). Cargo feature `oidc`, on by default.
- Engine: `Query::graph`, `Db::execute_as_in`, `Db::try_execute_as_in`,
  `Db::ingest_in_as`, `Db::graph_exists`, `EngineError::GraphConflict`.
- `varve` shell prints `by <subject>` after `tx <id> @ <time>`.

### Changed

- An unknown graph over HTTP is `404 unknown_graph` naming the graph. It was
  an opaque `500`.

## 1.0.0 (2026-07-XX)

First stable release. Varve is a bitemporal property-graph database that speaks
GQL, embeds as a Rust library, and serves over HTTP with a single writer and N
read-scaling query nodes over any S3-compatible object store.

### Bitemporal engine

- Full bitemporality: every fact carries a system-time and a valid-time
  rectangle. `_system_to` and effective valid ranges are **derived at read
  time, never stored** (spec §5.2).
- Time travel over both axes: `FOR SYSTEM_TIME AS OF` / `FOR VALID_TIME`
  clauses; retroactive `INSERT ... VALID FROM`.
- Property-graph model: nodes and edges with immutable per-edge endpoints,
  label-blind incidence, and adjacency families indexed for traversal.

### GQL surface

- GQL parser and planner: MATCH with linear path patterns and quantified
  edges (`{m,n}`, `*`), WHERE pushdown, RETURN with DISTINCT/ORDER BY/LIMIT,
  UNION, INSERT (including `(a)-[:R]->(b)` edges), MATCH … DELETE / DETACH
  DELETE, and `ERASE` / `DETACH ERASE`.
- Traversal runs as a real DataFusion extension (`PathExpand`) with
  as-of-correct adjacency built at query bounds.
- **Range windows answer coincidently**
  (`docs/plans/2026-07-29-interval-results.md`): over `FROM … TO` / `BETWEEN` /
  `ALL`, a multi-element match is a row only if every bound element's validity
  shares an instant on the ranged axis — quantified hops intersect edge
  versions along the walk and prune non-coincident paths at the frontier.
  Point-window (`AS OF`, and default) plans carry zero coincidence machinery,
  guarded by plan-shape tests. Temporal projections cover all four fields —
  `system_to(v)` joins `valid_from`/`valid_to`/`system_from` — and the nullary
  `coincide_valid_from()` / `coincide_valid_to()` / `coincide_system_from()` /
  `coincide_system_to()` read the match's coincidence interval.
- An adapted GQL TCK plus the temporal suites run in CI (current pass rate
  ≈ 0.87 with reasoned exclusions — see `docs/book/src/gql/deviations.md`).

### Access control (ReBAC)

- Native relationship-based access control at label / edge-type granularity
  (Neo4j/Memgraph-class graph privileges): policy lives as bitemporal
  nodes/edges in the reserved `__security` graph, and a principal's effective
  privileges resolve by transitive `MEMBER_OF` traversal. Managed via GQL DDL
  (`CREATE ROLE`, `GRANT READ|WRITE|ALL ON GRAPH … NODES|EDGES … TO ROLE …`,
  `GRANT ADMIN`, `SHOW ROLES` / `SHOW GRANTS`).
- Deny-by-default enforcement on **both reads and writes** when
  `[security] enabled = true` and a principal is set: conservative
  multi-label reads, endpoint-visible edge traversal (including through
  quantified paths), whole-transaction rejection on any denied write effect,
  and read-filtered MATCH-driven DML. Denied HTTP requests map to
  403 `forbidden`.
- Grants replicate to query nodes through the normal log; resolved privileges
  are cached per subject with exact epoch invalidation. Disabled (the
  default) is byte-identical to the pre-security engine, and fully-wildcard
  grants short-circuit to the unrestricted path (measured ≤ ~1% overhead).
- Security-aware anchored pruning: filtered (non-wildcard) principals keep
  the anchored traversal fast path — the pruned edge inputs are built under
  the same visibility filters as the full scan, with quantified-hop endpoint
  visibility computed over the anchor-reachable set instead of the whole
  graph. Measured at parity with the unrestricted path on anchored 2-hop
  (~9 ms at 10k nodes, ~20 ms at 1M vs formerly ~96 ms / ~20 s) and within
  default `[query]` budgets on quantified hops. Unanchored quantified hops
  likewise probe endpoint visibility only over the adjacency's own
  (budget-capped) endpoints, never a graph-wide visible-node set — so
  enforcement adds only bounded per-row cost to the full-scan path too.
- Multi-label edge visibility is conservative everywhere: an edge is
  traversable only when every label it carries is granted, enforced
  identically on fixed-hop scans, quantified adjacency, and the anchored
  fast path (pinned by an engine test; multi-label edges are not yet
  constructible through GQL or bulk ingest). See
  `docs/book/src/security.md`.

### Durability

- Write-ahead log with group commit: a batch is durable (one fsync, or one
  object PUT) before any effect becomes visible, so an acked transaction is
  both durable and read-your-writes visible.
- Two log backends: local segment files and one object per batch on the
  shared object store.
- Crash safety proven by a `kill -9` matrix (100× in CI) and a nightly chaos
  soak (30 min).

### Object storage and backends

- Sovereign by construction: nothing requires more than plain S3 PUT/GET/LIST.
  Conditional writes are capability-probed and used only for opt-in failover.
- Verified in CI against Garage, SeaweedFS, and MinIO; Ceph on a weekly cron.
  AWS S3 is configuration-compatible but is **not** exercised in CI (see
  Known limitations).
- Tiered read cache (memory + restart-surviving disk tier).

### Bulk ingest and traversal at scale

- `Db::ingest(nodes, edges)`: xtdb-style bulk data ops as one atomic
  transaction — no GQL parse or planning and no per-edge endpoint `MATCH`
  (endpoints referenced by `_id`, deliberately unverified). ~266k entities/s
  on the reference laptop, ~1000× the per-edge GQL surface, and
  oracle-equivalent to GQL ingest.
- `Db::compact_full_once()`: an opt-in full-compaction sweep for the
  load→compact→serve procedure — drains the full-iid-space L0 tries a bulk
  load leaves behind so anchored traversals can prune pages. A full-sweep job
  merges its whole input group in memory; the limit is stated in the ops
  guide's "Bulk loads" section.
- Anchor-reachable node-scan pruning: anchored traversals prune non-anchor
  node scans to the anchor-reachable set (provably result-identical). Warm
  2-hop on a 1M-node/6M-edge graph: 17.85 ms after the one-time full sweep.
- `POST /v1/ingest`: the bulk fast path over HTTP. Streaming, writer-only,
  `Content-Type`-negotiated **NDJSON** (`type`-tagged node/edge records) or
  **Neo4j-dialect CSV** (`:ID`/`:LABEL`/`:START_ID`/`:END_ID`/`:TYPE` +
  typed columns), optional gzip, optional per-record valid time. Each
  server-side chunk (`[ingest] chunk_ops`) commits as one atomic `Db::ingest`;
  the stream is not atomic and a mid-stream failure reports committed progress
  (idempotent replay is the retry story). Measured ~166k records/s vs ~88 for
  the per-line `/v1/tx` surface (≈1,880×). The wire contract is the normative
  `docs/book/src/reference/bulk-ingest.md`.
- `varve import --format ndjson|csv|jsonl-legacy` (default `ndjson`): the bulk
  formats stream to `/v1/ingest` (`--url`) or commit `Db::ingest` chunks
  (`--dir`) with a records/s progress line; `jsonl-legacy` is the original
  one-`INSERT`-per-line mode. `varve export --format ndjson` writes the whole
  graph as bulk NDJSON so `export | import` copies a graph Varve→Varve
  (embedded, current-state; round-trip proven).

### Compaction, GC, and GDPR erase

- Deterministic embedded compaction through manifest state; GC is a pure
  function of `(manifests, listed_keys, config)`.
- GC now also sweeps superseded log objects (once wholly below the minimum
  retained manifest watermark) and single-use probe objects.
- `ERASE` scrubs history at every system time; end-to-end proofs scan **every
  stored byte** on the local profile and **every raw object** on the
  object-store-log profile after compaction + GC.

### Server and CLI

- `varved` HTTP server: bearer-authenticated `/v1/{query,tx,ingest,status,admin/*}`,
  public I/O-free `/healthz`, Prometheus `/metrics`, JSON by default with an
  opt-in chunked Arrow IPC stream. Query nodes answer misdirected mutations
  with HTTP 421 and the writer's address.
- `varve` CLI: embedded/remote shell, bulk + legacy import, query/whole-graph
  export, and admin (status/compact/gc/verify).
- Distroless container image; a Compose demo brings up 1 writer + 2 query
  nodes over Garage.

### Coordination and failover

- Pluggable coordinators behind a registry: `designated-writer` (plain-PUT
  heartbeat guard) and `cas-failover` (CAS-lease takeover with epoch fencing),
  opt-in and capability-probed — a backend without real conditional writes
  refuses with an error naming the missing capability.
- Latest-manifest selection is by `(watermark, block_id)`, so a fenced
  writer's stray manifest can never win recovery, verify, or follower reads.

### Observability

- Engine metrics, cache hit ratios, and log lag via an I/O-free `MetricsSink`
  scrape; `tracing` spans across submit→commit→apply→flush and
  parse→plan→execute; optional OTLP export.

### Fuzzing

- `cargo fuzz` targets over every untrusted decode boundary — GQL parser, log
  frame decoder, block manifest, block meta, and Arrow-IPC event decoder — run
  nightly in CI. Arrow-IPC decoders are hardened against upstream panics and
  unbounded allocations at the trust boundary.

### Known limitations

- **AWS S3 is not CI-verified** — it is configuration-compatible (standard
  S3 API) but not exercised in the backend matrix.
- The GQL surface passes an **adapted** TCK, not full-standard conformance.
- A single mutation program may contain catalog statements **or** data
  statements, not both (`USE g; MATCH …` in one transaction errors; run the
  `USE`/DDL and the DML as separate transactions).
- The v1 GQL write surface is one transaction per edge for `MATCH … INSERT`
  edge creation (an ingest-throughput characteristic, not a correctness
  limit); use the bulk path for large loads — `Db::ingest` embedded,
  `POST /v1/ingest` or `varve import` remotely.
- Whole-graph NDJSON export (`varve export --format ndjson`) captures the
  current state (one version per entity), not full history, and is embedded
  (`--dir`) only — there is no HTTP export endpoint. Async load jobs, S3-pull
  ingestion, and an Arrow-IPC request format are tracked as future work.
- Retroactive / as-of `DELETE` is deferred post-v1 (`DELETE` acts on current
  state; a `FOR` clause on `DELETE` is a parse error).
- A **range-form** temporal window (`FROM … TO`, `BETWEEN`, `ALL`) over
  `OPTIONAL MATCH` or `EXISTS` is rejected, on whichever axis carries the
  range — those two shapes need the coincidence predicate inside their own
  join, which is not built yet. Every other shape (multi-hop, comma patterns,
  chained `MATCH`, quantified hops) answers **coincidently**: a row is
  returned only if all matched versions share an instant on the ranged axis
  (see the GQL surface section). `AS OF` — a point window, and the default on
  both axes — is unaffected at any pattern depth.
- Every spec §13 laptop target that has been measured is met; the object-store
  tx/s and scale-out numbers are single-machine (loopback MinIO / shared-CPU
  processes) and should be re-measured on distributed hardware before being
  cited as datacenter claims. See `docs/benchmarks/v1.md`.

### Release checklist

Publish to crates.io in dependency order (topologically derived; verify with
`cargo tree` before publishing):

```
varve-types → varve-config → varve-gql → varve-index → varve-storage →
varve-log → varve-plan → varve-engine → varve → varve-server → varve-cli
```

`varve-testkit` is `publish = false`; the `fuzz/` crate is workspace-excluded.

1. Tag `v1.0.0` and push — the release workflow builds tarballs for
   `aarch64-apple-darwin`, `x86_64-unknown-linux-musl`, and
   `aarch64-unknown-linux-musl`, and pushes the container image to
   `ghcr.io/ravan/varve`.
2. `cargo publish` each crate in the order above, waiting for the index
   between crates.
3. Publish the draft GitHub release after inspecting the uploaded assets.
