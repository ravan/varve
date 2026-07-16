# Security (ReBAC access control)

Varve ships native, relationship-based access control (ReBAC) at
**label / edge-type granularity** — the same class of graph privileges Neo4j
and Memgraph offer, modeled the graph-native way: **the policy is itself a
graph**. Users, roles, and grants live as ordinary bitemporal nodes and edges
in the reserved `__security` graph, and a principal's effective privileges are
resolved by relationship traversal (transitive role membership). Because
policy is ordinary Varve data, grants are durable, replicate to query nodes
through the normal log, and carry full system-time history.

## Enabling

```toml
[security]
enabled = true          # default false: exactly the pre-security engine
admins  = ["root"]      # bootstrap subjects; bypass all checks
```

- **Disabled (default):** zero enforcement and zero overhead — nothing changes.
- **Enabled:** requests that carry a principal are **deny-by-default**; only
  granted labels and edge types are readable/writable. Requests *without* a
  principal — the embedded `Db` API unless you opt in — keep full access,
  SQLite-style (the process owner). The server always sets the principal from
  the authenticated bearer token (see `[auth.static]`), so every HTTP request
  is enforced.
- `admins` subjects bypass every check and bootstrap the policy; delegate with
  `GRANT ADMIN TO ROLE …`.

Embedded enforcement is opt-in per call:

```rust
let rows = db.query("MATCH (p:Person) RETURN p.name")
    .as_principal("ada")     // enforced when [security] enabled
    .await?;
db.execute_as(gql, &params, "ada").await?;  // writes enforced the same way
```

## The policy graph

```text
(:User {subject})-[:MEMBER_OF]->(:Role {name})
(:Role)-[:MEMBER_OF]->(:Role)                 // transitive inheritance
(:Role)-[:GRANTED]->(:Privilege {action, graph, kind, name})
```

`action ∈ {read, write, admin}`, `kind ∈ {nodes, edges}`, and `graph`/`name`
accept `*`. Users are auto-created on first mention; subjects come from the
authenticator. The `__security` graph is unreachable through user GQL (all
`__`-prefixed graph names are reserved) — only the DDL below touches it.

## DDL

All statements below are mutations (they flow through `/v1/tx` or
`Db::execute_as`) except `SHOW …`, which are reads (`/v1/query` or
`Db::query`). Both require the caller to be a configured admin or hold
`ADMIN`.

```gql
CREATE ROLE reader                       DROP ROLE reader
GRANT ROLE reader TO USER 'ada'          REVOKE ROLE reader FROM USER 'ada'
GRANT ROLE senior TO ROLE junior         REVOKE ROLE senior FROM ROLE junior

GRANT READ  ON GRAPH g NODES Person, Company TO ROLE reader
GRANT WRITE ON GRAPH * NODES *                TO ROLE writer
GRANT ALL   ON GRAPH g EDGES KNOWS            TO ROLE writer
REVOKE READ ON GRAPH g NODES Person FROM ROLE reader

GRANT ADMIN TO ROLE ops                  REVOKE ADMIN FROM ROLE ops

SHOW ROLES
SHOW GRANTS                              -- every role's direct grants
SHOW GRANTS FOR USER 'ada'               -- transitive closure for a subject
SHOW GRANTS FOR ROLE reader              -- transitive closure for a role
```

`ALL` expands to READ + WRITE. Grants are idempotent; a revoke removes exactly
the grant it names. `DROP ROLE` severs every membership and grant that flowed
through the role, atomically.

## Semantics (v1)

- **Deny-by-default.** A principal with no matching grant sees nothing and
  writes nothing. There is no `DENY` precedence in v1 — grants only.
- **Reading a node** requires READ on **every** label the node carries
  (conservative multi-label rule: a `:Person` scan never surfaces a
  `:Person:Secret` node unless both are granted). A node with no labels needs
  the `*` grant.
- **Reading an edge** requires READ on its type **and** both endpoints
  visible — including every intermediate node of a quantified path
  (`-[:KNOWS]->{1,3}`): traversal cannot route *through* a node the principal
  cannot see.
- **Writing** (create/delete node, set/remove property or label) requires
  WRITE on every label the affected node carries — before *and* after the
  change; edge create/delete requires WRITE on the edge type. Any denied
  effect rejects the **whole transaction**: nothing partial ever reaches the
  log.
- **MATCH-driven DML is read-filtered too:** a principal cannot delete (or
  even count) what it cannot read.
- **Catalog DDL** (`CREATE GRAPH` / `DROP GRAPH`) requires admin when
  enforcement applies to the caller.
- **Scoping:** `ON GRAPH g` scopes a grant to one graph; `ON GRAPH *` covers
  all graphs, current and future.
- HTTP: a denied request maps to **403 `forbidden`** (distinct from 401
  authentication failures).

## Operational notes

- **Replication:** grants replicate to query nodes through the normal log —
  a grant acked on the writer is enforced on a follower as soon as its record
  applies (use a basis token to pin a read past a specific grant).
- **Performance:** resolved privileges are cached per subject and invalidated
  exactly (the writer and followers bump a policy epoch on any `__security`
  change), so the steady-state cost for **unrestricted** principals —
  security disabled, no principal, admins, and fully-wildcard grants — is one
  hash lookup per request (measured ≤ ~1% on the traversal benchmark).
- **Anchored traversals keep their fast path under active enforcement.**
  Filtered (non-wildcard) principals keep task-12's anchor-reachable pruning:
  the pruned inputs carry the same visibility semantics as the filtered full
  scan (the fixed-path edge batch is built under the same `Visible` filter;
  the quantified adjacency keeps only hops with both endpoints visible,
  computed over the anchor-reachable set instead of the whole graph).
  Measured with `VARVE_TRAVERSAL_SECURITY=granted` (all nodes/edges granted
  by name, so filtering excludes nothing — pure mechanism cost): warm
  anchored 2-hop is ~8.7 ms at 10k nodes / 60k edges and ~20 ms at 1M / 6M —
  parity with the unrestricted path — and the quantified `{1,3}` is ~25 ms /
  ~100 ms (vs ~24 ms / ~51 ms unrestricted; the gap is the reachable-set
  endpoint-visibility check), all within the default `[query]` budgets.
- **Enforcement adds no asymptotic cost to unanchored traversals either.**
  The fast path requires a point anchor (`{_id: …}` or `WHERE x._id = …`)
  and its usual shape conditions (single MATCH, homogeneous hops); anything
  else takes the full scan for every principal — that path is O(graph) by
  nature, filtered or not (an unanchored quantified hop at 1M / 6M builds a
  multi-million-entry adjacency and can exceed the default `[query]
  traversal_adjacency_budget` — a deterministic `ResourcesExhausted`, not a
  wrong answer). What enforcement adds on top is bounded: per-row `Visible`
  label checks, and an endpoint-visibility scan that probes only the
  adjacency's own (budget-capped) endpoints — never a graph-wide
  visible-node set. Anchor latency-sensitive queries by `_id` regardless of
  grants.
- **Multi-label edges follow the same conservative rule everywhere.** An
  edge is readable/traversable only when EVERY label it carries is granted —
  enforced identically on fixed-hop scans, quantified adjacency, and the
  anchored fast path. (Multi-label edges are not currently constructible
  through GQL or bulk ingest; the rule is pinned by an engine test so any
  future ingest path inherits it.)
- **Audit:** the policy graph is bitemporal; `SHOW GRANTS … FOR SYSTEM_TIME
  AS OF …`-style time-traveling audit is a planned follow-up on the same
  foundation, as are `DENY` precedence, property-level privileges,
  TRAVERSE-without-READ, and instance-level (per-node) ReBAC.
