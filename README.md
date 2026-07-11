# Varve

Varve is a bitemporal property-graph database written in Rust that speaks GQL. It is
embedded-first, with storage and compute separated over any S3-API object store. A
designated writer resolves GQL DML into effect events, group-commits them to a pluggable
log, and flushes Arrow blocks + hash tries to object storage; stateless query nodes tail
the log and answer reads through a DataFusion-based bitemporal engine.

See `docs/design/2026-07-04-varve-design.md` for the design contract and
`docs/plans/varve-v1-roadmap.md` for the implementation roadmap.

## Workspace

Rust stable (`rust-toolchain.toml`). Common gates:

```sh
just check          # cargo fmt --all --check + clippy -D warnings + cargo test --workspace
just compose-demo   # Slice 9 exit demo: Garage + 1 writer + 2 query nodes over Compose
```

## Embedded quick start

The `varve` crate is the embedded facade. `Db::local(dir)` opens a durable single-process
database (log under `dir/log`, blocks under `dir/store`); `Db::memory()` is volatile.

```rust
use varve::Db;

let db = Db::local("/tmp/varve-demo").await?;
db.execute("INSERT (:Person {name: 'Ada'});").await?;
let batches = db.query("MATCH (p:Person) RETURN p.name;").await?;
```

## CLI (`varve`)

The `varve` binary drives either an embedded directory (`--dir`) or a remote `varved`
server (`--url` + `--token`). `--dir` and `--url` are mutually exclusive; the bearer token
may come from `--token` or the `VARVE_TOKEN` environment variable (never echoed).

### Shell (embedded vs remote)

```sh
# Embedded REPL over a local directory
varve --dir /tmp/varve-demo shell

# Remote REPL against a running varved node
export VARVE_TOKEN=varve-demo-token
varve --url http://127.0.0.1:8081 shell
```

The shell prints results as tables, prints `(0 rows)` for empty results, and after a
mutation remembers the returned transaction id as the read basis so subsequent queries in
the session are read-your-writes.

### JSONL import / export

`import` sends one parameterized `INSERT` transaction per JSONL line; `export` runs a
query and writes line-delimited JSON (explicit nulls; bytes as `{"$bytes":"<base64>"}`).

```sh
# One :Person node per line of people.jsonl (optionally into a named graph)
varve --dir /tmp/varve-demo import --label Person --graph social people.jsonl

# Export a query result as JSONL (use '-' for stdin/stdout)
varve --dir /tmp/varve-demo export --query "MATCH (p:Person) RETURN p.name;" out.jsonl
varve --url http://127.0.0.1:8081 export --query "MATCH (p:Person) RETURN p;" --basis 42 -
```

### Admin

Each maps 1:1 to a server call. `--json` emits the raw response DTO instead of human text.

```sh
varve --url http://127.0.0.1:8080 admin status    # roles, applied progress, probe verdict
varve --url http://127.0.0.1:8080 admin compact
varve --url http://127.0.0.1:8080 admin gc
varve --url http://127.0.0.1:8080 admin verify --json
```

`compact`/`gc` require the writer node; a request to a query-only node is redirected with
the writer address.

## Server (`varved`)

`varved --config <toml>` runs an HTTP frontend over a `Db`. On a writer node it publishes
its `advertised_address` to `v1/writer.json` (a plain-PUT advertisement — NOT a lock or
election). Sample configs live in `deploy/varve-writer.toml` and `deploy/varve-query.toml`.

Minimal writer config:

```toml
[node]
roles = ["writer", "query", "compactor"]

[log]
backend = "object-store"

[storage]
backend = "s3"
[storage.s3]
endpoint = "http://garage:3900"
bucket = "varve"
path_style = true

[server]
backend = "http"
[server.http]
listen = "0.0.0.0:8080"
advertised_address = "http://writer:8080"   # required on writer nodes
max_body_bytes = "8MiB"
# tls_cert = "/etc/varve/tls/cert.pem"        # tls_cert and tls_key must be set together
# tls_key  = "/etc/varve/tls/key.pem"

[auth]
backend = "static"
[auth.static]
tokens = [{ subject = "demo", token = "varve-demo-token" }]

[metrics]
backend = "prometheus"
```

A query node omits the writer/compactor roles and the `advertised_address`, and points at
the same log + object store.

### Routes and auth

| Route | Method | Auth | Notes |
|---|---|---|---|
| `/healthz` | GET | public | liveness/readiness |
| `/metrics` | GET | bearer | Prometheus text (`text/plain; version=0.0.4`) |
| `/v1/status` | GET | bearer | role, log position, watermark, probe verdict |
| `/v1/query` | POST | bearer | JSON body; JSON or Arrow response |
| `/v1/tx` | POST | bearer | mutation; **421** on a query-only node with the writer address |
| `/v1/admin/compact\|gc\|verify` | POST | bearer | writer-gated |

Every `/v1/*` and `/metrics` request requires `Authorization: Bearer <token>`. TLS is
served by rustls (explicit `ring` provider) when `tls_cert`/`tls_key` are configured;
configuring exactly one of the pair is a startup error.

### tx / query over HTTP

```sh
# Mutation (writer). Response carries tx_id, side effects, and a read basis.
curl -sX POST http://127.0.0.1:8080/v1/tx \
  -H "Authorization: Bearer varve-demo-token" \
  -H "Content-Type: application/json" \
  -d '{"gql":"INSERT (:Person {name: $n});","params":{"n":"Ada"}}'

# Read on a query node, waiting until the writer's tx_id (basis) is applied.
curl -sX POST http://127.0.0.1:8081/v1/query \
  -H "Authorization: Bearer varve-demo-token" \
  -H "Content-Type: application/json" \
  -d '{"gql":"MATCH (p:Person) RETURN p.name;","basis":42,"basis_timeout_ms":5000}'
```

`basis` is either a bare transaction id (`42`) or a packed log position string
(`"at:<packed-u64>"`); the query blocks until that basis is applied or `basis_timeout_ms`
elapses (default 5000 ms). Omitting `basis` reads the query node's current state
(eventually consistent).

### Arrow IPC streaming

Send `Accept: application/vnd.apache.arrow.stream` to receive a chunked Arrow IPC stream
instead of JSON (default is JSON; `*/*` and `application/json` also yield JSON):

```sh
curl -sX POST http://127.0.0.1:8081/v1/query \
  -H "Authorization: Bearer varve-demo-token" \
  -H "Content-Type: application/json" \
  -H "Accept: application/vnd.apache.arrow.stream" \
  -d '{"gql":"MATCH (p:Person) RETURN p.name;"}' --output rows.arrows
```

## Compose scale-out demo

`just compose-demo` (`rtk proxy sh scripts/compose_demo.sh`) builds the distroless image,
brings up pinned Garage + one writer + two query nodes, loads the reduced Slice 6 fixture
over HTTP, verifies both query nodes agree under a basis read, decodes an Arrow stream,
round-trips the `varve` shell and admin surface, and always tears the stack (and volumes)
down. It prints `=== compose-demo: PASSED ===` on success.
