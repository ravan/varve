# GUAC on `varve` — gallery demo

[GUAC](https://guac.sh) (Graph for Understanding Artifact Composition) ingests
software supply-chain metadata — SBOMs, vulnerability reports, attestations —
into a graph you can query. This gallery runs GUAC's `guacgql` against a
purpose-built **`varve` graphql backend**, so the entire GUAC knowledge graph
lives in a bitemporal, object-store-backed graph database.

The headline it demonstrates: **time travel**. Because `varve` records the
system time of every write, you can ask "what did the supply-chain graph look
like *before* this SBOM landed?" — on the same connection, with one extra
clause — which a non-temporal graph store cannot do.

## Architecture

```
  guacone (CLI: collect files / query known / query bad)
      │  GraphQL over HTTP
      ▼
  guacgql  ──(in-tree "varve" backend: deterministic-ID path-INSERT programs
      │        + MATCH reads over HTTP /v1/tx and /v1/query)
      ▼
  varve writer :8080 ───────────────┐   varve query-1 :8081 (read-only follower)
      │  group-commit to shared log  │        │  follows the same durable log
      ▼                              ▼        ▼
  ┌─────────────────────────────────────────────────────┐
  │  Garage (single-node S3)  —  object-store log + blocks │
  └─────────────────────────────────────────────────────┘
```

- **writer** owns the log tail (writer + query + compactor roles); it is the
  only node that accepts mutations.
- **query-1** is a read-only follower over the *same* shared object-store log —
  it serves the time-travel reads, proving history is durable and replicated,
  not a writer-local artifact.
- **guacgql** speaks the GUAC GraphQL schema and translates it to `varve` GQL.
- **guacone** (baked into the same image) drives ingest and the `query`
  workflows; the demo execs it inside the `guacgql` container.

## Quickstart

```sh
# From the timedb repo root. In this dev environment, route docker through rtk:
rtk proxy sh gallery/guac/demo.sh
# Portable form (any host with Docker + compose):
sh gallery/guac/demo.sh          # add --keep to leave the stack running
```

The script builds the images (the `varved` image is compiled from this repo, so
it carries the engine fix described below), starts the stack from clean
volumes, and runs the six steps end-to-end, always tearing down on exit unless
`--keep` is given. Ports: writer `:8080`, query-1 `:8081`, guacgql `:8010`.

## What the demo does

1. **Build + up** from clean volumes (Garage → writer → query-1 → guacgql).
2. **Wait** for both varve `/healthz` endpoints and a guacgql `POST /query`.
3. **Ingest** the GUAC `exampledata` corpus with `guacone collect files`
   (~22 s over Garage/S3 for 65 documents; a handful of non-SBOM / unsupported
   fixtures are skipped — that is upstream GUAC parser behaviour, not a backend
   error).
4. **Direct varve GQL**: per-label node counts and a real 4-hop dependency-tree
   traversal (`PkgType -[:E {kind:'PkgHasNamespace'}]-> … -> PkgVersion`).
5. **GUAC queries**: `guacone query known` (built on the GraphQL `Neighbors`
   resolver) and `guacone query bad`.
6. **Bitemporal time travel**: capture `T1`, ingest one more SBOM that adds
   `gallery-newdep@9.9.9`, then show it is **present in the latest graph but
   absent `FOR SYSTEM_TIME AS OF TIMESTAMP '<T1>'`** — the same query, two
   points in history.

Representative counts after step 3: `PkgVersion` 669, `PkgName` 643,
`IsDependency` 1269, `HasSBOM` 30, `CertifyVuln` 27.

## What this showcases about `varve`

- **Deterministic-ID upsert without `MERGE`.** Every GUAC node/edge has a
  content-derived `_id`; `INSERT` with a supplied `_id` is an idempotent upsert.
  Re-ingesting the same corpus is a no-op — counts stay stable — with no
  `MERGE`/`ON MATCH` machinery.
- **Atomic multi-statement path-INSERT programs.** The software tree
  (type→namespace→name→version) is written as a single inline path per
  package, so one transaction creates many nodes + edges at once.
- **`basis` read-your-writes.** The backend threads the writer's returned
  transaction id as a read `basis`, so a query after an ingest is guaranteed to
  observe it.
- **Bitemporal time travel** via `FOR SYSTEM_TIME AS OF TIMESTAMP '…'`, served
  by a read-only follower.
- **Group commit on object storage.** Writes are batched into a shared S3
  (Garage) log; the query node follows that same log.

## Ingest performance (Phase 6)

The first working backend planned one `MATCH … INSERT` per node/edge; at a
per-statement planning cost of ~1 ms, ingest was dominated by planning, not
I/O. Rewriting the self-contained software-tree ingest as a single inline
**path-INSERT** per package (many nodes + edges, no per-node `MATCH`) removed
that overhead:

| Workload (synthetic, 4000 pkgs)     | Before             | After                | Speedup |
| ----------------------------------- | ------------------ | -------------------- | ------- |
| `IngestPackages`                    | 9.09 s / 420 pkg/s | 102.9 ms / 38.9k pkg/s | ~95×  |

- **Full `exampledata` corpus (65 docs):** ~14.5 s on local disk
  (`group_commit_window_ms = 0`), ~22 s in this gallery over Garage/S3
  (`group_commit_window_ms = 15`) — the object-store round-trips are the
  difference, and are exactly the durability the gallery is showing off.
- **Residual:** evidence edges (e.g. `isDependency`) still use
  `MATCH … INSERT`, because inlining their endpoints by `_id` alone risks
  superseding an existing node's properties. `IngestDependencies` therefore
  stays at ~409 edges/s — the motivation for the `/v1/ingest` follow-up below.

**Go/No-Go on a Rust `POST /v1/ingest` fast path:** *NO-GO for the demo.* The
client-side path-INSERT already delivered the ~95× win, so the demo needs no
engine ingest endpoint. It remains a tracked backlog item (below) because it is
the only thing that would also close the evidence-edge gap.

## Lessons fed back into `varve`

1. **Mixed `Int`/`Float` property columns now widen to `Float64`** (engine fix,
   `crates/varve-index/src/scan.rs`). GUAC renders a whole-number score (e.g.
   `10`) with no decimal, so `varve`'s tokenizer stored it as `Int` on some
   rows and `Float` on others. An **unlabeled** traversal endpoint — exactly
   what GUAC's `Neighbors` query emits (`(a {_id})-[e:E]->(b)`) — unions every
   node label, and that column clash used to fail the whole query with
   `MixedPropertyTypes`, breaking `guacone query known`. The engine now widens
   Int→Float (whole-number rows cast up) and only rejects genuinely
   incompatible mixes (e.g. Int/String). Covered by new tests in
   `crates/varve/tests/traversal.rs` and `scan.rs`.
2. **Bulk `POST /v1/ingest`** (backlog). The engine already has
   `Db::ingest(Vec<NodePut>, Vec<EdgePut>)` with `_id`-referenced edges and no
   per-edge `MATCH`; exposing it over HTTP and wiring the Go client to it would
   eliminate the evidence-edge planning cost for all consumers.
3. **Query-execution errors are now classified, not blanket-opaque** (engine +
   server fix). Previously the server mapped every failed result stream to a
   generic `internal server error` with no logging, which made the
   `MixedPropertyTypes` bug hard to diagnose. `EngineError::client_query_error()`
   now separates *statement-caused* failures (a type clash, a mixed-type
   property column, an unknown column, an unsupported feature) — which reference
   only the caller's own request and carry no server secret — from internal or
   infrastructure faults. `varved` returns the former as `422 query_error` with
   the real reason (so `guacone`/the Explorer show an actionable message), and
   keeps the latter opaque (`500 internal`) while logging the true cause
   server-side. Secret-bearing identifiers such as an unknown graph name stay
   opaque by design (covered by `post_header_execution_errors_do_not_expose_secrets`).
4. **Numeric literal round-tripping** (client-side note). GUAC's float
   rendering (`strconv.FormatFloat`) drops the decimal for whole numbers;
   emitting a canonical `10.0` would keep such properties consistently `Float`
   even before the engine-side widening.

## Notes

- The `varve` backend carries a compile-time `Backend` interface assertion
  (`var _ backends.Backend = (*varveBackend)(nil)`). Unimplemented GraphQL
  methods return errors rather than aborting; the only `panic()` is a defensive
  guard in `gql.Lit` for an unsupported literal *type* (a programmer error,
  never reachable from ingested data).
- All tokens/credentials in this gallery (`varve-demo-token`, the Garage key)
  are **demo-only** fixed material, not production secrets.

