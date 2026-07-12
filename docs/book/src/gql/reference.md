# GQL reference

Varve implements the **practical core** of GQL (ISO/IEC 39075:2024) described in the
[design specification](../../../design/2026-07-04-varve-design.md) §8: an openCypher-flavored
subset covering pattern matching, mutation, temporal travel, and the usual read-query pipeline
(`FILTER`/`LET`/`FOR`/`ORDER BY`/`UNION`/aggregation). See [Deviations](deviations.md) for what
is deliberately out of scope, and [Bitemporal queries](temporal.md) for `FOR VALID_TIME` /
`FOR SYSTEM_TIME`.

Every example below is copied **verbatim** from one of two verified sources — never invented —
and is confirmed to parse on this exact commit:

- `crates/varve-gql/src/parser.rs`'s own test suite (`cargo test -p varve-gql --lib`: **65
  passed, 0 failed**), for examples marked "parser test".
- `crates/varve/examples/gql_tour.rs`, freshly executed end to end (`cargo run --release
  --example gql_tour -p varve`) against a real in-memory `Db` while writing this page, for
  examples marked "gql_tour". Its full source and captured output are both worth reading start
  to end for a single coherent walkthrough.

## INSERT

Create nodes and edges. A bare node needs at least a label, a property, or an outgoing/incoming
hop; edges always require a label (see [Deviations](deviations.md)).

```gql
INSERT (:Person {_id: 1, name: 'Ada', age: 36, city: 'London', legacy: 'yes'}),
       (:Person {_id: 2, name: 'Bob', age: 41, city: 'Paris'}),
       (:Person {_id: 3, name: 'Cy', age: 36, city: 'London'})
```
*(gql_tour — seeds the tour's three people)*

`MATCH … INSERT` binds existing nodes and inserts an edge between them:

```gql
MATCH (a:Person {_id: 1}), (b:Person {_id: 2}) INSERT (a)-[:KNOWS]->(b)
```
*(gql_tour)*

`INSERT` also accepts `VALID FROM`/`VALID TO` to backdate or bound a fact's valid-time range —
see [Bitemporal queries](temporal.md).

## MATCH

Binds one or more comma-separated **linear** paths (see [Deviations](deviations.md) for what
"linear" excludes) and an optional `WHERE`:

```gql
MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name
```

## OPTIONAL MATCH

Chained after `MATCH` (or another `OPTIONAL MATCH`); unmatched variables are `NULL` rather than
dropping the row:

```gql
MATCH (p:Person)
OPTIONAL MATCH (p)-[:MENTORS]->(mentor:Person)
RETURN p.name AS person, mentor.name AS mentor
ORDER BY person ASC
```
*(gql_tour — Ada/Bob/Cy all have no mentor yet, so `mentor` is `NULL` for every row, and every
row still appears)*

## WHERE

Filters a `MATCH`'s bindings by a boolean expression; supports parameters (`$name`):

```gql
MATCH (p:Person) WHERE p.city = $city RETURN p.name AS name ORDER BY name ASC
```
*(gql_tour, run with `params = {"city": "London"}`)*

## FILTER / LET / FOR (pipeline) / ORDER BY / SKIP / LIMIT / OFFSET

Beyond the opening `MATCH`, a query body is a pipeline: any number of `MATCH`/`OPTIONAL MATCH`,
`FILTER <expr>` (post-match predicate), `LET var = expr[, …]` (bind a computed value), and
`FOR var IN <list-expr>` (unwind a list into one row per element), terminated by `RETURN`:

```gql
MATCH (a:Person) OPTIONAL MATCH (a)-[:KNOWS]->(b:Person)
FILTER b.age > 18
LET name = b.name
FOR friend IN [b]
RETURN name AS n, friend
```
*(parser test — exercises FILTER, LET, and pipeline FOR together)*

`RETURN` itself supports `DISTINCT`, `ORDER BY <expr> [ASC|DESC]` (comma-separated, mixed
directions), and `SKIP`/`LIMIT`:

```gql
MATCH (n:Person)
RETURN DISTINCT n.name AS name, n.age AS age
ORDER BY n.age DESC, n.name ASC
SKIP 5 LIMIT 10
```
*(parser test)*

`OFFSET` is accepted as a synonym for `SKIP`:

```gql
MATCH (n) RETURN n OFFSET 7 LIMIT 8
```
*(parser test)*

## UNION [ALL]

Combines the result shapes of two or more `RETURN`s with matching columns; bare `UNION`
de-duplicates rows (equivalent to `UNION DISTINCT`), `UNION ALL` keeps duplicates:

```gql
MATCH (a:A) RETURN a
UNION
MATCH (b:B) RETURN b
UNION ALL
MATCH (c:C) RETURN c
```
*(parser test)*

A real two-leg example from the tour, combining a plain property read with a label-filtered
read under a shared `source`/`name` shape:

```gql
MATCH (p:Person {_id: 2}) RETURN 'city' AS source, p.name AS name
UNION
MATCH (p:Speaker) RETURN 'label' AS source, p.name AS name
```
*(gql_tour)*

## RETURN [DISTINCT] and aggregation

`count`, `sum`, `avg`, `min`, `max`, and `collect` are the supported aggregate functions
(`count(DISTINCT expr)` is also accepted); a bare `RETURN` column list without any aggregate
call is a plain projection.

```gql
MATCH (p:Person)
RETURN p.city AS city, count(*) AS people
ORDER BY city ASC
```
*(gql_tour — output: `London | 2`, `Paris | 1`)*

## SET

Assigns a property or adds a label to an already-bound variable (both forms may appear in one
comma-separated `SET`):

```gql
MATCH (p:Person {_id: 1}) SET p.role = 'founder', p:Speaker
```
*(gql_tour)*

## REMOVE

Removes a property or a label from a bound variable:

```gql
MATCH (p:Person {_id: 1}) REMOVE p.legacy
```
*(gql_tour)*

## DELETE [DETACH]

Soft-deletes matched nodes/edges — a normal bitemporal tombstone, current-state-only (no `FOR`
clause is accepted on a `DELETE`; see [Bitemporal queries](temporal.md)):

```gql
MATCH (p:Person) WHERE p.name = 'Ada' DELETE p
```
*(parser test)*

`DETACH DELETE` also removes incident edges, required whenever the target still has any:

```gql
MATCH (p:Person) WHERE p.name = 'Ada' DETACH DELETE p
```
*(parser test)*

## ERASE [DETACH]

Varve's GDPR hard-delete extension (not part of standard GQL): erases a node's history at
*every* system-time and valid-time instant, not just from now on. See
[Bitemporal queries](temporal.md) for the full DELETE-vs-ERASE contrast and the tests that prove
bytes are physically gone after compaction + GC.

```gql
MATCH (n:Person) ERASE n
```
*(parser test — fails with `StillConnected` if `n` still has incident edges; use `DETACH ERASE`)*

```gql
MATCH (p:Person {_id: 1}) DETACH ERASE p
```
*(gql_tour — after this, even a `FOR SYSTEM_TIME AS OF` query timestamped before the erase
returns zero rows for this entity, confirmed by the tour's own printed
`history after erase visible rows: 0`)*

## CREATE GRAPH / DROP GRAPH

Catalog statements; a program may not mix catalog statements with data statements (`INSERT`,
`MATCH`, mutations) in the same transaction — see [Deviations](deviations.md).

```gql
CREATE GRAPH people
```
*(parser test; also `gql_tour`'s first statement: `CREATE GRAPH tour`)*

```gql
DROP GRAPH people
```
*(parser test)*

## USE

Selects the active graph for the statements that follow, within one semicolon-separated
program:

```gql
CREATE GRAPH g; USE g; MATCH (n) RETURN n; DROP GRAPH g;
```
*(parser test, `parse_program` — this parses as shown, but catalog and data statements cannot
share one transaction at execute time (see [Deviations](deviations.md)), so in practice this
must be run as three separate transactions: `CREATE GRAPH g;`, then `USE g; MATCH (n) RETURN
n;`, then `DROP GRAPH g;`)*

## Parameters

`$name` placeholders bind to a caller-supplied parameter map (see `WHERE`, above, for a live
example) — this is how the HTTP API's `params` field and the CLI's JSONL import both pass
values without string-building GQL.

## CASE

```gql
CASE x.kind WHEN 'a' THEN 1 ELSE 2 END
```
*(parser test, as one expression inside a `RETURN` list — see `CAST`, below, for the full line)*

## EXISTS

A boolean subquery: true iff the nested pattern has at least one match for the outer row's
bindings.

```gql
MATCH (p:Person)
WHERE EXISTS { (p)-[:KNOWS]->(friend:Person) }
RETURN p.name AS connector
ORDER BY connector ASC
```
*(gql_tour)*

## CAST

`CAST(expr AS type)`, with `INT`, `FLOAT`, `STRING`/`STR`, and `BOOL`/`BOOLEAN` as target types.
The full parser-test line exercises `CASE`, `CAST`, list literals, parameters, and
`count(DISTINCT …)` together in one `RETURN`:

```gql
MATCH (x:X)
RETURN $param, [1, x.y],
       CASE x.kind WHEN 'a' THEN 1 ELSE 2 END,
       CAST(x.y AS INT),
       count(DISTINCT x.y)
```
*(parser test)*
