# Conformance and deviations

Varve implements a practical core of GQL/openCypher (design spec §8), not the full ISO
standard. This page records where v1 deviates, sourced from the parser and engine, plus the
current standing of the two conformance oracles the project runs against: an adapted openCypher
TCK, and a differential check against the official GQL ANTLR grammar.

## v1 deviations

- **Edge labels are required.** `crates/varve-gql/src/parser.rs`'s edge-pattern parser calls
  `self.ident("edge label")` unconditionally, so `-[]->` with no label is a parse error. Cypher
  allows unlabeled edges; Varve does not.
- **Each path pattern in a `MATCH` must be a single linear chain**: a start node followed by
  zero or more `(edge, node)` hops, never a branching or star-shaped pattern within one path.
  Multiple such linear paths, comma-separated, are supported in one `MATCH` and share variables
  across them (e.g. `MATCH (a:Person {_id: 1}), (b:Person {_id: 2})` from
  [the GQL reference](reference.md#insert)). The restriction is on the shape of one path, not
  on how many paths a `MATCH` may hold.
- **Quantified edges cannot carry a group variable.** `(a)-[r*1..3]->(b)` is rejected: "edge
  variables on quantified edges (group variables) are not supported" (`parser.rs`). Bare
  quantifiers without a variable (`(a)-[*1..3]->(b)`) are fine.
- **Catalog and data statements cannot mix in one transaction.** A program containing both a
  `CREATE GRAPH`/`DROP GRAPH` statement and any data statement (`INSERT`, `MATCH`, a mutation)
  is rejected at resolve time: `EngineError::Unsupported("mixing catalog and data statements in
  one transaction")` (`crates/varve-engine/src/writer.rs`). Issue catalog and data changes as
  separate transactions.
- **Multi-label `MATCH` limits.** `(n:A:B)` (AND, match all labels) and `(n:A|B)` (OR, match
  any label) are both supported, but they cannot be mixed in one label expression, and label
  negation (`!A`) is not supported at all: any of these hits `"label expression nesting
  post-v1"` (`parser.rs`).
- **`max_path_depth` caps unbounded quantifiers.** An unbounded quantifier (`*` or `{m,}`) on a
  path hop is lowered to this configured cap rather than truly unbounded traversal; default
  **10**, tunable via `[query] max_path_depth` (`crates/varve-engine/src/db.rs`).
- **The full 59-keyword reserved-word list** (`crates/varve-gql/src/token.rs`) can never be used
  as a bare identifier (property/label/variable name) without becoming a keyword token: `INSERT,
  MATCH, WHERE, RETURN, AS, TRUE, FALSE, NULL, FOR, VALID_TIME, SYSTEM_TIME, OF, ALL, FROM, TO,
  BETWEEN, AND, VALID, DELETE, TIMESTAMP, DATE, DETACH, NOT, OR, XOR, IS, CASE, WHEN, THEN, ELSE,
  END, EXISTS, CAST, IN, STARTS, ENDS, WITH, CONTAINS, OPTIONAL, FILTER, LET, SET, REMOVE, ERASE,
  UNION, DISTINCT, ORDER, BY, ASC, ASCENDING, DESC, DESCENDING, SKIP, LIMIT, OFFSET, CREATE,
  DROP, GRAPH, USE`. (Keywords are case-insensitive; matching is done on the uppercased token.)
- **`DELETE`/`DETACH DELETE`/`MATCH … INSERT` read current state only**: a `FOR VALID_TIME`/
  `FOR SYSTEM_TIME` clause on any of them is a parse error (see
  [Bitemporal queries](temporal.md)). Retroactive/as-of mutation is out of scope for v1.
- **`ERASE`/`DETACH ERASE` are Varve extensions**, not part of standard GQL. See
  [Bitemporal queries](temporal.md) for their semantics and proofs.

## openCypher TCK standing

Varve runs an adapted subset of the openCypher Technology Compatibility Kit
(`crates/varve-testkit/tests/tck.rs`, feature files + scenario translation in
`crates/varve-testkit/src/tck/`) as a CI gate. Scenarios are drawn from the vendored TCK feature
corpus, translated from Cypher into Varve GQL, and run against a real in-memory `Db`. Every
excluded scenario carries a reasoned entry in
`resources/tck/exclusions.toml` (13,546 lines, one section per excluded scenario, each with a
`reason` explaining why, e.g. "TCK uses multi-CREATE Cypher clauses in one query; the v1
adapter does not rewrite that form into Varve multi-statement INSERT safely"), and the
`exclusions_do_not_hide_current_failures` test independently asserts no exclusion reason is
phrased as a live failure in disguise.

**Current standing:**

```
$ cargo test -p varve-testkit --test tck -- --nocapture
...
TCK summary
total    3897
excluded 3386
adapted  511
passed   445
failed   66
rate     0.871
test open_cypher_tck_gate ... ok
```

445 of 511 adapted scenarios pass (87.1%), gated at a minimum 85% pass rate
(`PASS_RATE_GATE` in `tck.rs`) plus a fixed core-scenario allowlist
(`resources/tck/core.txt`) that must always fully pass. The 66 remaining failures are within the
adapted set's known gaps (see the exclusions file for the reasoned non-adapted 3,386).

## ANTLR differential oracle

Beyond the TCK, `scripts/gql_diff/` cross-checks Varve's hand-written parser against a
generated parser for the official GQL grammar (`resources/gql-grammar/GQL.g4`, vendored
from `opengql/grammar`, ISO/IEC 39075:2024). The check is one-directional: it only
fails when Varve accepts a non-extension `.gql` corpus file that the official grammar
rejects. Varve rejecting valid GQL (it is a practical-core subset parser by design) is
reported as `VARVE_SUBSET` without failing the check. Corpus files under
`resources/gql-corpus/*.ext.gql` (temporal clauses, `ERASE`, Varve's semicolon-delimited
program/catalog shorthand) are Varve extensions the official grammar is expected to reject.
