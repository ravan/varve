# HTTP API

This is the frozen slice-9 wire contract served by `varved` (`crates/varve-server`). Every route,
status code, and body on this page is copied from `crates/varve-server/tests/http_api.rs` and
`crates/varve-server/src/{api.rs,http/{mod.rs,handlers.rs}}` — nothing here is invented.

## Routes & auth matrix

| Route | Method | Auth | Notes |
|---|---|---|---|
| `/healthz` | GET | **public** | I/O-free — reads only the in-memory follower-error state, never touches the object store. |
| `/v1/query` | POST | bearer | Read-only; served by any node with the data. |
| `/v1/tx` | POST | bearer | Mutation; **writer-only** — redirects on a non-writer node. |
| `/v1/status` | GET | bearer | Roles, applied progress, manifest watermark, probe verdict. |
| `/metrics` | GET | bearer | Prometheus text format. |
| `/v1/admin/compact` | POST | bearer | **Compactor-role-only** — redirects otherwise. |
| `/v1/admin/gc` | POST | bearer | **Compactor-role-only** — redirects otherwise. |
| `/v1/admin/verify` | POST | bearer | Runs on any node (not role-gated). |

Bearer auth is `Authorization: Bearer <token>` against a configured `Authenticator` (the builtin
is `static`, a fixed subject→token table). A **duplicate** `Authorization` header is treated as
unauthenticated (401), not an error — a client sending two auth headers gets rejected exactly
like a client sending none, from `authorization_accept_and_body_limit_are_parsed_exactly`. Every
response, authenticated or not, carries `x-content-type-options: nosniff`.

## The 421 writer redirect

A non-writer node receiving `/v1/tx`, `/v1/admin/compact`, or `/v1/admin/gc` responds `421
Misdirected Request` with the current writer's advertised address, if one has been published:

```json
{"code": "misdirected_request", "message": "request must be sent to writer", "writer": "https://writer.example"}
```

If no writer has published an address yet, the response is `503 Service Unavailable` with
`{"code": "writer_unavailable", ...}` instead. (Source:
`query_node_tx_redirects_to_fresh_advertisement_or_503`.) The CLI's `RemoteClient` follows one
such redirect automatically and re-sends the request to the advertised writer; a **second** 421
in a row is treated as a `RedirectLoop` error rather than followed again.

## Request/response bodies

**`POST /v1/tx`** — a parameterized INSERT, from `tx_then_json_query_round_trips`:

```json
{"gql": "INSERT (:Person {_id: 1, name: $name})", "params": {"name": "Ada"}}
```

Response (`200 OK`, `TxResponse`):

```json
{"tx_id": 1, "system_time": "...", "system_time_us": ..., "side_effects": {...}, "basis": ...}
```

**`POST /v1/query`** — reading it back at a fixed transaction-id basis:

```json
{"gql": "MATCH (p:Person) RETURN p.name AS name", "basis": 1}
```

Response (`200 OK`, JSON default):

```json
{"rows": [{"name": "Ada"}]}
```

## Basis forms

`basis` accepts either a bare transaction id (`"basis": 1`) or an `at:<packed-u64>` string
(`BasisRequest::At`, decoded via `BasisToken::try_from`) naming a specific packed log position.
`basis_timeout_ms` bounds how long a query waits for that basis to actually be applied locally
before giving up. A basis that never arrives within the timeout is `408 Request Timeout`:

```json
{"gql": "MATCH (p) RETURN p", "basis": 999, "basis_timeout_ms": 1}
```
→ `{"code": "basis_timeout", ...}` (from `invalid_requests_negotiation_timeout_and_internal_errors_are_stable`).

## Content negotiation

`Accept` selects the response representation for `/v1/query`:

- `application/vnd.apache.arrow.stream` → a chunked Arrow IPC stream.
- `*/*` or `application/json` (or no `Accept` header at all) → the default JSON `{"rows": [...]}`
  shape shown above.
- Anything else (e.g. `application/jsonish`) → `406 Not Acceptable`.
- If **both** `application/json` and the Arrow media type are offered, Arrow wins (checked
  first) — from `authorization_accept_and_body_limit_are_parsed_exactly`.

## Tagged-bytes JSON convention

Binary property values round-trip through JSON as a single-key object with a `$bytes` key holding
standard base64: `{"$bytes": "<base64>"}`. This is handled symmetrically by
`params_from_json`/`scalar_from_json` on the way in and `batches_to_json`/the CLI's
`TaggedBytesEncoder` on the way out — arrays and any other multi-key or nested object shape are
rejected as invalid parameters.

## Error responses

Every error is `{"code": "...", "message": "...", "writer": null}` (`ErrorResponse`); `message`
never leaks internal detail — `internal` errors are deliberately generic even when the
underlying GQL referenced something sensitive (`invalid_requests_negotiation_timeout_and_
internal_errors_are_stable` asserts the literal string `secret_storage_credential` from a failing
query never appears in the response body).

| HTTP status | `code` | When |
|---|---|---|
| 400 | `invalid_request` | Malformed GQL, bad parameter shape, base64 decode failure, not-a-mutation/not-a-query mismatches. |
| 401 | `unauthorized` | Missing, invalid, or duplicated `Authorization` header. `WWW-Authenticate: Bearer` is set. |
| 406 | `not_acceptable` | No representation in `Accept` that the server can produce. |
| 408 | `basis_timeout` | The requested basis was not observed locally before `basis_timeout_ms` elapsed. |
| 413 | (body too large) | Request body exceeds `[http] max_body_bytes` (default 8 MiB). |
| 421 | `misdirected_request` | A writer-only or compactor-only route hit a node without that role; `writer` names the current writer if known. |
| 429 | `backpressure` | The writer's submission queue is full; response includes `Retry-After: 1`. |
| 503 | `writer_unavailable` | A redirect-eligible route with no writer advertisement published yet. |
| 503 | `follower_failed` | The node's log-follower loop has stopped (terminal state — also reflected by `/healthz` returning 503 `{"status":"degraded","error":"follower stopped"}`). |
| 503 | `writer_fenced` | This writer has been fenced by a newer epoch (`cas-failover` takeover elsewhere). |
| 500 | `internal` | Anything else — object-store I/O failure, unexpected engine error, etc. |

## TLS

`[server.http] tls_cert`/`tls_key` must both be set or both omitted — a lone cert or key is a
config-time error. `advertised_address` (used for the 421 redirect body) is required on any node
with the `Writer` role and must be an absolute `http://`/`https://` URL.
