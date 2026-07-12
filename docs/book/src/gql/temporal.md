# Bitemporal queries

Varve tracks two independent time axes per fact, per the
[design specification](../../../design/2026-07-04-varve-design.md) §3/§8 (Ceiling/Polygon
resolution, ported from XTDB):

- **Valid time** — when a fact was true *in the world*. User-controlled via `VALID FROM`/
  `VALID TO` on `INSERT`; defaults to "now" through "forever" (open-ended) if omitted.
- **System time** — when Varve *learned* the fact. Writer-assigned, monotonic, never
  user-settable; every mutation gets the writer's current system time as its `_system_from`.

A query's temporal position on each axis is set independently with `FOR VALID_TIME …` and
`FOR SYSTEM_TIME …`, each accepted **at most once** per query (a duplicate axis is a parse
error: `"duplicate ..."`). Omitting a `FOR` clause defaults to `AS OF now` on both axes — i.e.
plain `MATCH` without any `FOR` clause is exactly `FOR VALID_TIME AS OF now FOR SYSTEM_TIME AS
OF now`, current state as currently known.

## Forms

Each axis independently accepts one of four forms (all four verified against
`crates/varve-gql/src/parser.rs`'s own passing test suite — `cargo test -p varve-gql --lib`:
65 passed):

| Form | Meaning |
|---|---|
| `AS OF <datetime>` | a single instant |
| `FROM <datetime> TO <datetime>` | a half-open range (`from` must be strictly earlier than `to`) |
| `BETWEEN <datetime> AND <datetime>` | an inclusive range (`from` must be earlier than or equal to `to`) |
| `ALL` | every instant on that axis, unbounded |

`<datetime>` is `TIMESTAMP '<RFC 3339 string>'` or `DATE '<YYYY-MM-DD>'`.

Both axes on one query, combining a valid-time range with an all-system-time scan:

```gql
FOR VALID_TIME FROM TIMESTAMP '2020-01-01T00:00:00Z' TO TIMESTAMP '2021-01-01T00:00:00Z'
FOR SYSTEM_TIME ALL
MATCH (p:Person) RETURN p.name
```
*(parser test)*

An inclusive valid-time window:

```gql
FOR VALID_TIME BETWEEN DATE '2020-01-01' AND DATE '2021-01-01'
MATCH (p:Person) RETURN p.name
```
*(parser test)*

System-time travel — "what did we believe at this instant" — freshly exercised end to end by
`cargo run --release --example gql_tour -p varve` while writing this page:

```gql
FOR SYSTEM_TIME AS OF TIMESTAMP '2026-07-12T12:20:28.657968Z'
MATCH (p:Person) RETURN p._id, p.name ORDER BY p._id
```
*(getting-started.md's live transcript — a person inserted after this timestamp is correctly
absent from the result, while an earlier-valid-time-but-later-inserted person is also absent,
because system time governs "what Varve knew then", independent of valid time)*

`DELETE`/`MATCH … INSERT`/`DETACH DELETE` all read **current state only** — a `FOR` clause on
any of them is a parse error ("DELETE reads current state - temporal clauses not supported" /
"MATCH ... INSERT reads current state - temporal clauses not supported"). Retroactive/as-of
mutation is out of scope for v1.

## `INSERT … VALID FROM` / `VALID TO`

```gql
INSERT (:P {_id: 1}) VALID FROM DATE '2020-01-01' TO DATE '2021-01-01'
```
*(parser test — `VALID FROM` must be strictly earlier than `VALID TO` when both are given)*

```gql
INSERT (:P {_id: 1}) VALID TO DATE '2021-01-01'
```
*(parser test — `VALID FROM` defaults to "now" when omitted)*

A retro-dated insert with no upper bound (defaults to "forever"), exercised live in
[Getting started](../getting-started.md):

```gql
INSERT (:Person {_id: 3, name: 'Cleo'}) VALID FROM DATE '2020-01-01'
```

`VALID FROM`/`VALID TO` also apply to edges:

```gql
INSERT (:P {_id: 1})-[:K]->(:P {_id: 2}) VALID FROM TIMESTAMP '2020-01-01T00:00:00Z'
```
*(parser test)*

## Temporal functions

`valid_from(var)`, `valid_to(var)`, and `system_from(var)` project the bound-in-time fields of
a matched variable in a `RETURN` (backed by DataFusion columns `_valid_from`/`_valid_to`/
`_system_from` — `crates/varve-plan/src/functions.rs`):

```gql
MATCH (p:Person) RETURN valid_from(p) AS since, valid_to(p), system_from(p)
```
*(parser test)*

There is no `system_to()`: per the [architecture overview](../architecture.md), effective
system-time upper bounds are never stored — they are derived at read time by scanning newer
events for the same internal id, never persisted as their own field.

## `ERASE` vs `DELETE`: GDPR semantics

`DELETE` (and `DETACH DELETE`) is an ordinary bitemporal tombstone: the entity disappears from
*current* state, but every historical `FOR SYSTEM_TIME AS OF <before-the-delete>` query still
sees it, exactly as any other superseded fact. Nothing is ever physically removed by `DELETE`
alone.

`ERASE` (and `DETACH ERASE`) is Varve's GDPR **hard-delete extension** — not part of standard
GQL — and is fundamentally different: it hides the entity's entire history at *every* system-time
instant, not just from now on, and its underlying property/label bytes are physically removed
from every stored object once compaction and garbage collection have run. The following tests
(all currently green — `cargo test -p varve --test erase` and `cargo test -p varve --test
gdpr_gc`, run while writing this page) are the load-bearing proof, cited here **by exact test
name** so the claims can be checked directly against the source:

| Test (file) | Proves |
|---|---|
| `erase_hides_history_at_every_system_time` (`erase.rs`) | An erased entity is invisible even to a `FOR SYSTEM_TIME AS OF` query timestamped *before* the erase — a deliberate GDPR choice, not a time-travel bug. |
| `erase_connected_requires_detach` (`erase.rs`) | A plain `ERASE` on a node with incident edges fails with `EngineError::StillConnected` rather than silently orphaning edges; `DETACH ERASE` is required. |
| `detach_erase_erases_incident_edges` (`erase.rs`) | `DETACH ERASE` also erases the node's incident edges, not just the node itself. |
| `erase_then_reinsert_same_id_is_fresh_entity` (`erase.rs`) | Re-inserting the same `_id` after an erase starts a genuinely fresh entity, uncontaminated by the erased history. |
| `erased_property_bytes_absent_after_compaction_and_gc` (`gdpr_gc.rs`, Task 2) | After compaction + GC, an erased entity's property bytes are absent from the compacted store, not merely hidden by a filter. |
| `post_erase_reinsert_survives_compaction` (`gdpr_gc.rs`, Task 2) | A re-inserted-after-erase entity survives a subsequent compaction pass correctly (the erase doesn't corrupt later history for the same id). |
| `erased_bytes_absent_from_every_stored_object_and_log_segment` (`gdpr_gc.rs`, Task 2) | LOCAL-profile whole-disk proof: scans **every** stored byte — every log segment and every store object on disk — and asserts the erased entity's property values appear in none of them. |
| `erased_bytes_absent_from_every_raw_object_on_the_object_store_log_profile` (`gdpr_gc.rs`, Task 3) | Same proof against the object-store-log profile: lists and GETs every raw `v1/` key in the object store directly (not through the engine's own read path), so the check cannot be fooled by a caching or indexing bug. |
| `detach_erase_scrubs_edge_property_bytes_too` (`gdpr_gc.rs`, Task 2) | `DETACH ERASE` scrubs edge/adjacency property bytes as thoroughly as node bytes — the edge-side GDPR proof. |

In short: reach for `DELETE` for ordinary "this is no longer true" business logic (history stays
auditable); reach for `ERASE`/`DETACH ERASE` only when a legal erasure obligation requires the
data to become genuinely unrecoverable after the next compaction + GC cycle.
