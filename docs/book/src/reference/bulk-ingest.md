# Bulk ingest

This is the normative wire-format contract for Varve's bulk-ingest fast path. It exposes the
engine's `Db::ingest` data-op path (no GQL parse, no plan, no per-edge endpoint `MATCH`) over
HTTP, so external systems can load large graphs at engine speed instead of one transaction at a
time. On the reference laptop the bulk path loads ~166k records/s versus ~88 records/s for the
per-line `/v1/tx` surface — about **1,880×** (see `docs/benchmarks/v1.md`).

Formats and shapes on this page are the public contract as of BI-1…BI-5; changes after that are
additive only.

## Endpoint

```
POST /v1/ingest[?graph=<name>]
Authorization: Bearer <token>
Content-Type: application/x-ndjson        # NDJSON (primary)
             | text/csv                   # Neo4j-dialect CSV
Content-Encoding: gzip                    # optional
```

- **Writer-only.** A non-writer node answers `421 Misdirected Request` with the writer's
  advertised address (or `503` if none is published yet), exactly like `/v1/tx`.
- **Streaming.** The request body is never buffered whole; the route is exempt from
  `[http] max_body_bytes`. The server cuts the stream into chunks and commits each as one atomic
  `Db::ingest` transaction (see [Atomicity](#atomicity)).
- **Authorization.** Under `[security] enabled`, the submitter's write grants are enforced
  against every affected label / edge-type exactly as a GQL `INSERT` would be; a denied
  label/type is `403 forbidden` naming it.
- **Target graph.** `?graph=<name>` selects the graph. The body is a stream of NDJSON or CSV,
  so it has no JSON envelope to hold a field; the query parameter keeps the URL the single
  address of the target and works with `curl --data-binary @file`. Absent ⇒ `default`. A
  reserved `__` name is `400`; an unknown graph is `404` before any chunk commits (the body
  reports `committed` all zeros). `ON GRAPH <name>` grants apply.

## NDJSON records (primary format)

One JSON object per `\n`-terminated line, discriminated by `type`. Blank lines are skipped; a
trailing newline on the last record is optional.

```jsonl
{"type":"node","labels":["Person"],"props":{"_id":"p1","name":"Ada","age":36}}
{"type":"edge","label":"KNOWS","src":"p1","dst":"p2","props":{"since":2001}}
```

**node**
- `labels` — array of strings (may be empty).
- `props` — a flat JSON object. `props._id` is the upsert key (string or integer); when absent an
  id is auto-generated with the same scheme as GQL `INSERT`. Repeated puts of the same `_id`
  supersede (upsert).

**edge**
- `label` — string (required).
- `src` / `dst` — the `_id` values of the endpoints (string or integer, required).
- `props` — optional flat object.
- **Endpoint existence is deliberately NOT verified** (xtdb put-docs semantics — this is what
  makes the path fast): a dangling edge is durable but never matches a traversal.

**Value mapping** (identical to `/v1/tx` params): string→Str, bool→Bool, integral number→Int,
other number→Float, `{"$bytes":"<base64>"}`→binary. **Nested arrays/objects and `null` are 422
errors** carrying the 1-based line number — absence expresses a missing property, not `null`.

**Valid time** (optional, per record): top-level `valid_from` / `valid_to`, each an RFC3339
string (exactly GQL's `TIMESTAMP` parse) or an integer number of microseconds, matching GQL
`INSERT … VALID FROM … TO …`. Absent ⇒ valid from now to the end of time. `valid_from >=
valid_to` is a 422.

Unknown record fields are **rejected**, not dropped — silently discarding a field a client
believed was honored would corrupt its intent.

## CSV dialect (Neo4j-compatible)

`Content-Type: text/csv`. One upload carries either nodes or edges, distinguished by the header
(presence of `:START_ID`/`:END_ID` ⇒ edges):

```csv
:ID,name,age:int,:LABEL                 ← nodes file
p1,Ada,36,Person

:START_ID,:END_ID,:TYPE,since:int       ← edges file
p1,p2,KNOWS,2001
```

- Meta-fields: `:ID`, `:LABEL` (multi-label via `;`), `:START_ID`, `:END_ID`, `:TYPE`,
  `:VALID_FROM`, `:VALID_TO`.
- Property types: `name` / `name:string` (default), `:int`, `:float`, `:boolean`. An empty cell
  means the property is absent.
- Ids map to strings. `:VALID_FROM`/`:VALID_TO` cells are RFC3339, or an integer microseconds.
- Unknown `:meta` headers or unknown types are a 422 with header context.

This is the exact shape `neo4j-admin database import` / `LOAD CSV` exporters emit, so those files
load without transformation.

## Response

Success is `200` with the folded chunk receipts. `basis` is the last committed transaction id —
usable as `?basis=` on a subsequent read, exactly like `TxResponse.basis`:

```json
{"nodes": 100000, "edges": 500000, "transactions": 60,
 "basis": 12345, "system_time": "…", "system_time_us": 1234567890,
 "subject": "ada"}
```

`subject` is the authenticated principal that made the writes, exactly as recorded in the
log. It is empty only for the embedded CLI import.

## Atomicity

Each server-side chunk (default `[ingest] chunk_ops = 10000`, configurable) commits as one atomic
`Db::ingest` transaction. **The stream as a whole is NOT atomic** — consistent with
Elasticsearch `_bulk`, ArangoDB `/_api/import`, and Neptune's loader. On the first bad record or
failed chunk the request fails fast; earlier chunks stay committed, and the error body reports
that committed progress:

```json
{"error": "line 5041: edge record missing `dst`",
 "committed": {"nodes": 40000, "edges": 0, "transactions": 4, "basis": 12290}}
```

Whole-stream atomicity would require unbounded writer memory and is explicitly out of scope.

### Idempotent retry

Node and edge puts are **upserts** keyed by `_id` (nodes) or endpoints + label (edges), so
replaying an interrupted stream from the beginning is safe — re-applied records supersede
in place rather than duplicating. This is the retry story after a mid-stream failure or a
client disconnect.

> **CSV edge caveat.** CSV edge files carry no `:ID`, so each POST auto-generates fresh edge
> ids — re-POSTing an edge CSV **duplicates** relationships (identical to re-running an edge
> `INSERT`, and to a `neo4j-admin` re-import). Idempotent replay applies to nodes and to edges
> given explicit ids.

## Limits

The `[ingest]` section (see [Configuration](../ops/configuration.md)):

| Key | Default | Meaning |
|---|---|---|
| `chunk_ops` | `10000` | Decoded ops per atomic `Db::ingest` transaction. Must be > 0. |
| `max_line_bytes` | `1 MiB` | The largest a single NDJSON line / CSV record may grow before the stream is rejected (bounds per-line buffering). |

## `curl` examples

```sh
# NDJSON
curl -X POST "$BASE/v1/ingest" \
  -H "authorization: Bearer $TOKEN" \
  -H 'content-type: application/x-ndjson' \
  --data-binary @social.ndjson
# → {"nodes":3,"edges":2,"transactions":1,"basis":1,"system_time":"…","system_time_us":…,"subject":"ada"}

# NDJSON into a named graph (create it first with `CREATE GRAPH org_x`)
curl -X POST "$BASE/v1/ingest?graph=org_x" \
  -H "authorization: Bearer $TOKEN" \
  -H 'content-type: application/x-ndjson' \
  --data-binary @social.ndjson

# gzipped NDJSON
gzip -c social.ndjson | curl -X POST "$BASE/v1/ingest" \
  -H "authorization: Bearer $TOKEN" \
  -H 'content-type: application/x-ndjson' -H 'content-encoding: gzip' \
  --data-binary @-

# CSV (nodes then edges)
curl -X POST "$BASE/v1/ingest" -H "authorization: Bearer $TOKEN" \
  -H 'content-type: text/csv' --data-binary @nodes.csv
curl -X POST "$BASE/v1/ingest" -H "authorization: Bearer $TOKEN" \
  -H 'content-type: text/csv' --data-binary @edges.csv
```

## CLI

`varve import` / `varve export` speak the same formats (see [CLI](cli.md)):

```sh
# Bulk import (streams to /v1/ingest when --url, or Db::ingest when --dir)
varve --url "$BASE" --token "$TOKEN" import social.ndjson              # --format ndjson (default)
varve --url "$BASE" --token "$TOKEN" import --format csv nodes.csv
gzip -dc social.ndjson.gz | varve --url "$BASE" --token "$TOKEN" import -   # stdin

# Legacy one-INSERT-per-line mode (per-line GQL validation; nodes only)
varve --dir ./db import --format jsonl-legacy --label Person rows.jsonl

# Whole-graph bulk export (embedded only) — Varve→Varve copy is export | import
varve --dir ./src export --format ndjson graph.ndjson
varve --dir ./dst import graph.ndjson
```

`varve export --format ndjson` writes the whole data graph as bulk NDJSON (nodes then edges) at
the **current** system+valid time (one version per entity — it is a copy of current state, not a
history dump). It is embedded-only: there is no HTTP export endpoint. An edge whose endpoint has
no `_id` in the node snapshot, or which carries more than one label, cannot be expressed as a
single bulk edge record and is skipped (and counted) rather than emitted wrong.

Stored properties are emitted verbatim, **including any engine-generated `_id`** (a node or edge
loaded without an explicit `_id` was assigned one — e.g. `varve:gen:…` for an edge — and that id
is a real stored property). Preserving it is what makes `export | import | export` a
deterministic fixpoint, so a Varve→Varve copy reproduces the same identities rather than minting
fresh ones on each hop.

## What is deliberately out of scope

Async load jobs (Neptune-style `loadId` + polling), S3-pull ingestion, an Arrow-IPC request
format, whole-stream atomicity, and parallel chunk submission (the writer is single; group commit
already coalesces). These are tracked as future work.
