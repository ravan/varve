# Changelog

All notable changes to Varve are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); this project adheres
to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
- An adapted GQL TCK plus the temporal suites run in CI (current pass rate
  ≈ 0.87 with reasoned exclusions — see `docs/book/src/gql/deviations.md`).

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

### Compaction, GC, and GDPR erase

- Deterministic embedded compaction through manifest state; GC is a pure
  function of `(manifests, listed_keys, config)`.
- GC now also sweeps superseded log objects (once wholly below the minimum
  retained manifest watermark) and single-use probe objects.
- `ERASE` scrubs history at every system time; end-to-end proofs scan **every
  stored byte** on the local profile and **every raw object** on the
  object-store-log profile after compaction + GC.

### Server and CLI

- `varved` HTTP server: bearer-authenticated `/v1/{query,tx,status,admin/*}`,
  public I/O-free `/healthz`, Prometheus `/metrics`, JSON by default with an
  opt-in chunked Arrow IPC stream. Query nodes answer misdirected mutations
  with HTTP 421 and the writer's address.
- `varve` CLI: embedded/remote shell, JSONL import/export, and admin
  (status/compact/gc/verify).
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
  limit); use `Db::ingest` for bulk loads.
- Retroactive / as-of `DELETE` is deferred post-v1 (`DELETE` acts on current
  state; a `FOR` clause on `DELETE` is a parse error).
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
