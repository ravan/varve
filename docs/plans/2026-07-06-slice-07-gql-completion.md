# Slice 7: GQL Practical-Core Completion + Conformance Harness

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Varve speaks the GQL practical core — full expression language with 3-valued logic, `CASE`, `EXISTS`, `CAST`, parameters, and a function library behind `FunctionRegistry`; pipelined queries (`OPTIONAL MATCH`, `FILTER`, `LET`, `FOR`, aggregation with implicit grouping, `DISTINCT`, `ORDER BY`/`SKIP`/`LIMIT`/`OFFSET`, `UNION`); completed mutations (`SET`, `REMOVE`, `ERASE`, multi-statement all-or-nothing transactions, MATCH parts resolved by the query engine); a minimal catalog (`CREATE GRAPH`/`DROP GRAPH`/`USE`); and a conformance harness (adapted openCypher TCK with pass-rate gates, ANTLR differential parser oracle, cargo-fuzz parse-print-reparse target).

**Architecture:** The single-variant `Expr` becomes a real expression tree parsed by a Pratt parser and lowered to DataFusion `Expr`s (SQL 3VL == GQL 3VL). `QueryStmt` becomes a clause pipeline folded over a `DataFrame` (MATCH joins on shared vars, `OPTIONAL` = Left join, `FOR` = unnest, aggregation via `DataFrame::aggregate` with implicit grouping). Mutations resolve their MATCH parts through the same query engine (`scan_specs` + `execute_pattern`) at the writer snapshot, gaining hops; SET/REMOVE do read-modify-write over visible payloads; multi-statement programs share one tx/one `LogRecord` with statement-local overlay visibility. Graphs become a `BTreeMap` of `TableState` keyed by graph name; catalog entries are ordinary node events in a reserved `__meta` graph so log/flush/manifest/recovery machinery is reused verbatim. Conformance lives in `varve-testkit` (hand-rolled Gherkin subset parser, Cypher→GQL translation, exclusion/baseline gates) plus two CI-only oracles (ANTLR differential, cargo-fuzz).

**Tech Stack:** No new workspace dependencies. datafusion 54.0.0 / arrow 58.3.0 (all APIs named below verified against `~/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/datafusion-*-54.0.0` sources — see "Verified API table"). New *tooling* only: `cargo-fuzz` (separate `fuzz/` crate, nightly, CI-only) and Java 17 + antlr4-tools (CI-only, never a build dependency).

## Global Constraints (from the roadmap — apply to every task)

- **TDD, no exceptions:** failing test first, minimal implementation, refactor, commit. `superpowers:test-driven-development` during execution.
- **Interfaces + registry + composition (spec §4):** subsystems behind traits; engine never depends on a concrete backend.
- **Sovereignty (spec §1, D7):** nothing beyond plain S3 semantics. (This slice touches no storage semantics; the constraint forbids adding any.)
- **Bitemporal invariant (spec §5.2):** `_system_to` and effective valid ranges are never stored, always derived. SET/REMOVE/ERASE emit append-only events.
- **Determinism:** no wall-clock, no randomness, no map-iteration-order dependence in outputs. All new row/binding orders must be explicitly sorted.
- Lints: `cargo clippy --workspace --all-targets -- -D warnings`; `unwrap()`/`expect()` forbidden in library code (allowed in tests via repo `clippy.toml`); errors via `thiserror` per crate.
- Timestamps `Timestamp(µs, UTC)`; IIDs `xxh3_128(graph, table, _id)`.
- Commits: `feat:`/`fix:`/`test:`/`refactor:`/`docs:` prefixes. **NO `Co-Authored-By` trailer.**
- **We are in development: NO backward compatibility anywhere** (AST shapes, `Statement` variants, engine internals change freely) — but everything is production code: no placeholders, no stubs.
- **The test code in this plan is the contract:** every DataFusion/arrow API named here was verified against the pinned 54.0.0/58.3.0 registry sources; if an implementation sketch still drifts, adapt the implementation, not the test.
- Slice ends with: all workspace tests green, clippy clean, STATUS.md updated, roadmap boxes ticked, runnable demo command recorded.

## Inputs consulted (STATUS.md overrides roadmap where they conflict)

- `docs/plans/STATUS.md` — slice-6 decisions (AST shapes, mangling `{var}__{col}`, `scan_specs`/`execute_pattern`, PathExpand, writer read-modify-write, one-tx-one-LogRecord) and the explicit slice-7 carry-ins: GQL `ERASE` statement (slice-2 deferral), `where_for_var` single-variant-`Expr` closure (will stop compiling the moment `Expr` grows — fixed in Task 1), multi-edge INSERT bodies (resolved by multi-statement programs, Task 13), comma-separated MATCH paths, MATCH…INSERT with hops, empty-prop-block/keyword-identifier parser follow-ups, WHERE-on-absent-property `UnknownColumn` deviation, `_labels` divergence (resolved for real in Task 7).
- `docs/plans/varve-v1-roadmap.md` slice-7 entry + Global Constraints.
- `docs/design/2026-07-04-varve-design.md` §8 (GQL surface), §10 (function registry bridge, pattern lowering), D2 (TCK adaptation strategy).
- Actual code in `crates/` at commit `7216d4b` (post-slice-6). `resources/gql-grammar/GQL.g4` (vendored ANTLR reference).

## Design decisions (read before any task; tasks cite these by number)

1. **`Expr` becomes a tree; every old match site moves to helpers.** Exact AST in Task 1. The single-variant blast radius (STATUS fast-follow): `writer.rs::resolve_insert`'s irrefutable `where_for_var` closure, `exec.rs::{iid_point, iids_from_snapshot}`, `pattern.rs::apply_element_predicates`, `db.rs::plan_fast_path`. All move to `Expr::conjuncts()` (flatten top-level ANDs) + `Expr::as_prop_eq()` (recognize `var.prop = literal`, either operand order).
2. **3VL = DataFusion SQL semantics.** GQL's three-valued logic for comparisons, `AND`/`OR`/`NOT`, `IS [NOT] NULL` coincides with SQL Kleene logic, which DataFusion implements. `WHERE`/`FILTER` keep rows where the predicate is TRUE (not NULL). A truth-table e2e test pins this (Task 2); no custom evaluation code.
3. **Absent property ⇒ typed NULL, not error.** Referencing a property that exists on no visible entity of the element currently errors `UnknownColumn` (slice-1 deviation). New rule: a `var.prop` whose mangled column is absent from that element's snapshot schema lowers to a NULL literal. `UnknownVariable` (var not bound by any clause) stays an error.
4. **IID fast-path via conjunct extraction.** `var._id = <const>` is recognized among `where_clause.conjuncts()` (const = `Literal` or `Param`); everything is still re-applied as a filter afterwards, exactly as today. Fast-path behavior (point lookup) is preserved, now tolerant of extra conjuncts.
5. **Property maps widen to `Vec<(String, Expr)>`.** INSERT/SET docs need values: the writer const-evaluates prop-map exprs (`Literal`, `Param`, unary `-` on numeric literal — anything else `EngineError::Unsupported`) into `Value`s. MATCH inline props lower to equality filters and may be any expr.
6. **Clause-pipeline lowering (exact AST in Task 5).** Query = `Vec<Clause>` + `ReturnClause`, folded left-to-right over a `DataFrame`. First clause must be a non-OPTIONAL `MATCH` (an `OPTIONAL MATCH` first → `PlanError::Unsupported`, recorded TCK exclusion). A later `MATCH` builds its pattern DataFrame independently (existing per-path machinery) and joins the accumulator on all shared variables' `_iid` columns — `JoinType::Inner`, or `JoinType::Left` for `OPTIONAL MATCH`. Zero shared variables ⇒ synthetic cross join: `with_column("__cross", lit(1_i64))` on both sides, inner-join on it, drop it (**`DataFrame::cross_join` does not exist in DF 54** — verified). Multi-path within one MATCH clause: paths lowered separately, combined by the same shared-var/cross rule. `FILTER`/`WHERE` → `filter()`; `LET x = e` → `with_column("x", …)`; `FOR x IN e` → `with_column` + `unnest_columns_with_options(&["x"], UnnestOptions{preserve_nulls: false, ..})` (NULL/empty list ⇒ row eliminated — openCypher UNWIND semantics).
7. **Value variables are plain-named columns; `__` is reserved.** `LET`/`FOR` variables become columns named exactly the variable. Since element columns are mangled `{var}__{col}`, any user variable containing `__` could collide ⇒ parser rejects variable names containing `__` ("reserved for internal use").
8. **`FunctionRegistry` lives in `varve-plan`** (spec §10.5). `name → ScalarFn::{Udf(Arc<ScalarUDF>), Builder(fn(Vec<DfExpr>) -> Result<DfExpr, PlanError>)}` + a fixed aggregate-name set `{count, sum, avg, min, max, collect}`. `session_context(&FunctionRegistry)` registers Udfs via `SessionContext::register_udf` and replaces BOTH existing context constructors (`expand.rs::session_context` and the bare `SessionContext::new()` in `exec.rs::iids_from_snapshot`) so DML reads see the same functions. Builtins map GQL names onto verified DF functions (table below). `valid_from`/`valid_to`/`system_from` become ordinary registry entries lowered by column substitution (the `TemporalFnKind` special case in AST/parser is deleted). Config-driven embedder function registration: deferred post-v1 (the registry type is the seam; recorded in STATUS).
9. **Parameters.** `Expr::Param(name)` resolved at lowering from `&BTreeMap<String, Value>`; missing ⇒ new `PlanError::MissingParam(String)`. Public API: `Db::query_with(gql, &params)` / `Db::execute_with(gql, &params)`; the paramless methods delegate with an empty map. The writer `Submission` carries the params map.
10. **`EXISTS { <paths> [WHERE e] }` only as a top-level conjunct** of WHERE/FILTER, optionally under exactly one `NOT`. Lowered by building the subpattern DataFrame and joining the accumulator `JoinType::LeftSemi` (or `LeftAnti` under NOT) on shared vars' `_iid` columns; no shared vars ⇒ `Unsupported`. `EXISTS` anywhere else (OR branch, CASE, RETURN item) ⇒ `PlanError::Unsupported("EXISTS outside a top-level WHERE conjunction")` — recorded TCK exclusion.
11. **Aggregation with implicit grouping.** A RETURN item is *aggregating* iff its expr contains a `FnCall` whose name `is_aggregate()` (checked recursively). If any item aggregates: group keys = the full exprs of every non-aggregating item; lower `df.aggregate(group_exprs, agg_exprs)` then project to output aliases in item order. `collect(x)` → `array_agg`, `count(*)` → `count(lit(1_i64))` (`Expr::Star` legal only there), `count(DISTINCT x)` → `count_distinct`; `DISTINCT` inside other aggregates ⇒ `Unsupported`. `RETURN DISTINCT` → `distinct()` after projection. `ORDER BY` sorts *after* projection by output-column expressions (`SortExpr` via `Expr::sort(asc, nulls_last: asc)` — DF default null ordering, recorded as the v1 choice); `SKIP n` / `OFFSET n` / `LIMIT n` → `limit(skip, Some(n))`.
12. **`UNION [ALL]` between whole query bodies.** `union()` (= ALL) / then `distinct()` for plain `UNION`. Bodies must project identical schemas (names + types) — a mismatch surfaces the DataFusion error as-is.
13. **Node labels: conjunction + single-level alternation via `LabelSpec`/`LabelFilter`; snapshots gain `_labels`.** `NodePattern.labels: LabelSpec::{All(Vec<String>), Any(Vec<String>)}` (`(:A:B)`/`(:A&B)` = All, `(:A|B)` = Any; mixing or negation ⇒ parse error "label expression nesting is post-v1"). Filtering happens inside `snapshot_entities` via `LabelFilter<'_>` — no scan-layer changes. Snapshots (nodes AND edges) additionally emit a `_labels` `List<Utf8>` non-null column (query-time view only — blocks store events, so zero storage/manifest change). This discharges the slice-1 `_labels` open item and enables bare-element RETURN (decision 14). Edge patterns keep exactly one required label (edge alternation ⇒ recorded TCK exclusion). Unlabeled node MATCH keeps the existing returns-empty quirk (recorded; revisit post-v1).
14. **`RETURN n` for a node/edge element materializes the element.** Projection emits, for element var `n`: `n._iid` (hidden identity), `n._labels`, and every property column, output-named `n.<prop>`. The TCK comparator (Task 17) reconstructs `(:L {props})`/`[:T {props}]` values from these columns; edge vars additionally get `n._src_iid`/`n._dst_iid`. Path vars keep the existing `List<FixedSizeBinary(16)>` behavior.
15. **Mutation MATCH parts resolve through the query engine.** `MatchPart` becomes `{ paths: Vec<PathPattern>, where_clause: Option<Expr> }`. The writer builds `ScanInput`s from its snapshot (same construction `Db::query` uses — factored into one shared `pub(crate) async fn execute_body(...)`) and runs `scan_specs` + `execute_pattern`; bindings = distinct rows of the named vars' `_iid` columns, **sorted by iid tuple for determinism**. This replaces the per-variable candidate Cartesian product and legalizes hops in `MATCH … INSERT`/`DELETE`/`SET`/`REMOVE`/`ERASE` (a slice-6 promise). Path variables and quantified hops in mutation MATCH parts: `Unsupported` in v1 (recorded).
16. **SET/REMOVE = writer-side RMW.** New `varve-index` helper `visible_events` (factored from the same resolution core as `snapshot_entities` — one visibility implementation, never two) yields each matched entity's visible `(labels, doc)`. SET prop values are evaluated **per binding row** by projecting the value exprs over the match-result batch through DataFusion (roadmap's "RMW planning via the query engine"); arrow scalars convert back to `Value`. One `Op::Put` event per touched entity per statement (all items merged; several binding rows hitting one entity apply in row order, last wins — recorded); valid = `[now, ∞)`, system = now. `SET n:Label` / `REMOVE n:Label` adjust the labels vec (dedup, preserve first-seen order); removing a missing prop/label is a no-op; an entity whose merged doc/labels are unchanged emits no event.
17. **`ERASE` / `DETACH ERASE` (Varve extension, spec §8).** Mirrors DELETE via shared `MutateStmt{kind: MutKind::{Delete, Erase}}`: plain `ERASE` on a node with incident edges ⇒ `StillConnected`; `DETACH ERASE` emits `Op::Erase` for the node and every distinct incident edge (self-loops deduped) in one tx. `Op::Erase` semantics (hides history at every system time) are already implemented and property-tested since slice 2; this adds only the statement surface + e2e tests. Full GDPR object-scan verification stays in slice 11.
18. **Multi-statement programs: one tx, one `LogRecord`, overlay visibility.** `parse_program` splits on `;` (trailing `;` fine). `Db::execute` accepts a program of ≥1 mutations (any `Query` inside ⇒ `NotAMutation`); `Db::query` requires exactly one `Query` statement. All statements share `tx_id`/`system_time` (equal `system_from` appends are legal — `LiveTable` monotonicity is non-strict, verified `live.rs:46`). Statement N's reads see committed snapshot + an `Overlay{nodes: LiveTable, edges: LiveTable}` holding statements 1..N-1's events, threaded as `Option<&Overlay>` through `merged_snapshot`/`edge_adjacency`/`incident_edges` and merged by the existing `merge_sources` (overlay last). Any statement error ⇒ nothing appended, nothing applied. This resolves the slice-6 ingest note (multi-edge INSERT bodies): the social-graph bench batches edge INSERTs into programs.
19. **Catalog = reserved `__meta` graph; state goes multi-graph.** `TableState` per graph in `GraphsState{graphs: BTreeMap<String, TableState>}` under the ONE existing RwLock (slice-4 decision 8 upheld). `CREATE GRAPH g` / `DROP GRAPH g` = Put/Delete node events in graph `__meta`, table `nodes`, label `Graph`, `_id` = name — so log, flush, manifest (`TableTries.graph` **already exists**, prost tag 1 — verified), recovery, and queries reuse every existing mechanism. `TableEffects` gains `graph` (prost `string` tag 3; `""` = `default`, old golden wire unchanged). `USE g` is a program prefix; absent = `default`; target graph must exist in the catalog (`default` and `__meta` always exist implicitly); user statements naming a `__`-prefixed graph ⇒ error. `CREATE` of an existing graph ⇒ `GraphExists`; `DROP` of a missing graph or of `default` ⇒ error; DROP leaves objects for slice-8 GC (recorded). IID derivation, storage keys, and flush all take the statement's graph (today's hard-coded `DEFAULT_GRAPH` becomes a parameter).
20. **Printer + fuzz.** `to_gql(&Statement) -> String` (new `print.rs`) covering the full AST; a proptest round-trip (arb AST → print → parse == AST) runs in normal CI; `fuzz/` (cargo-fuzz, nightly) target `parse`: arbitrary bytes must not panic, and `Ok(stmt)` must survive print→reparse with an equal AST. CI job `fuzz-nightly` runs `-max_total_time=600` (roadmap: 10 min).
21. **TCK harness gates.** Features vendored under `resources/tck/features/` from `opencypher/openCypher` at a pinned commit (recorded in `resources/tck/README.md`, Apache-2.0 license copied). Hand-rolled Gherkin-subset parser (zero new deps). Per-scenario disposition: `exclusions.toml` (scenario → reason; includes every Untranslatable class), `core.txt` (curated core list — must be 100% green), `baseline.txt` (expected-pass set — a baseline scenario failing OR a non-baseline scenario newly passing both FAIL the test with an update instruction, keeping the file honest). Overall gate: passed/adapted ≥ 0.85 where adapted = total − excluded. Report written to `target/tck-report.json`, uploaded as a CI artifact. TCK side-effect assertions (`+nodes 2` rows, `no side effects`) are checked against `TxReceipt::side_effects`; the runner also continues through later control queries in a scenario instead of stopping at the first assertion.
22. **ANTLR differential oracle (CI-only).** Committed corpus `resources/gql-corpus/*.gql` (one statement per file; `*.ext.gql` = Varve temporal/ERASE extensions, exempt from the ANTLR-must-accept rule) + fuzz seeds. `varve-testkit` bin `parse_corpus` prints `<file>\tACCEPT|REJECT`; CI job generates a Java parser from `resources/gql-grammar/GQL.g4` (antlr4-tools) and a tiny `scripts/gql_diff/Main.java` prints the same; `scripts/gql_diff/compare.py` fails iff varve ACCEPTs && ANTLR REJECTs && not `.ext.gql`. (Varve rejecting valid GQL is fine — we are a practical-core subset.)

**Explicit non-goals this slice (each is a recorded TCK-exclusion reason where TCK touches it):** group variables (quantified-edge vars), path variables on INSERT, `SHORTEST`/cheapest paths, label negation/nested label exprs, edge-label alternation, `EXISTS` in general expression position, `OPTIONAL MATCH` as first clause, `DISTINCT` in non-count aggregates, `labels()`/`element_id()` functions, Cypher `WITH`/`MERGE`/`CALL`/`FOREACH` translation (GQL has no such practical-core equivalents in v1), retroactive/as-of mutations.

## Verified DataFusion 54.0.0 API table (registry sources; paths under `~/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/`)

| API | Where verified |
|---|---|
| `DataFrame::{filter, select, with_column, aggregate, sort(Vec<SortExpr>), sort_by, limit(skip: usize, fetch: Option<usize>), distinct, distinct_on, union, union_distinct, join, join_on, unnest_columns_with_options}` | `datafusion-54.0.0/src/dataframe/mod.rs` (lines 411–2180) |
| **NO `DataFrame::cross_join`** (only `LogicalPlanBuilder::cross_join`) — use the `__cross` lit-key join (decision 6) | same file (absence) + `datafusion-expr-54.0.0/src/logical_plan/builder.rs:1247` |
| `JoinType::{Inner, Left, Right, Full, LeftSemi, RightSemi, LeftAnti, …}` | `datafusion-common-54.0.0/src/join_type.rs:31–52` |
| `functions_aggregate::expr_fn::{count, count_distinct, sum, avg, min, max, array_agg}` | `datafusion-functions-aggregate-54.0.0/src/lib.rs:104–120` + `count.rs:76` |
| `functions::expr_fn::{upper, lower, btrim, ltrim, rtrim, replace, split_part, concat, contains, starts_with, ends_with}` | `datafusion-functions-54.0.0/src/string/mod.rs` expr_fn export list |
| `functions::expr_fn::{character_length, substr, substring, left, right, reverse}` | `.../src/unicode/mod.rs` expr_fn export list |
| `functions::expr_fn::{abs, ceil, floor, round, sqrt, power, signum, trunc}` | `.../src/math/mod.rs` export_functions! list |
| `functions_nested::expr_fn::{cardinality, array_element}` (umbrella re-export `datafusion::functions_nested`, default feature `nested_expressions`) | `datafusion-functions-nested-54.0.0/src/{cardinality.rs:39, extract.rs:54}`; `datafusion-54.0.0/src/lib.rs:873` |
| `logical_expr` builders: `when(cond, then)`, `case(expr)` → `CaseBuilder::{when, otherwise, end}`; `cast(expr, DataType)`, `try_cast`, `is_null`, `is_not_null`, `not`, `lit`, `col`; `Expr::sort(asc, nulls_first) -> SortExpr` | `datafusion-expr-54.0.0/src/expr_fn.rs` (330–408), `conditional_expressions.rs:48–63`, `expr.rs:1951` |
| `create_udf(name, arg_types, return_type, volatility, fun)`; `SessionContext::register_udf(ScalarUDF)` | `datafusion-expr-54.0.0/src/expr_fn.rs`, `datafusion-54.0.0/src/execution/context/mod.rs:1615` |
| `UnnestOptions { preserve_nulls: bool, recursions: Vec<RecursionUnnestOption> }` | `datafusion-common-54.0.0/src/unnest.rs:70` |
| `DataFrame::new(session_state, plan)` (escape hatch if a builder-level plan is ever needed) | `dataframe/mod.rs:258` |

## File structure

```
crates/varve-gql/src/token.rs        # ops: <> <= >= + / % | ; &  keywords: NOT OR XOR IS CASE WHEN THEN ELSE END
                                     #   EXISTS CAST IN STARTS ENDS WITH CONTAINS OPTIONAL FILTER LET SET REMOVE
                                     #   ERASE UNION DISTINCT ORDER BY ASC ASCENDING DESC DESCENDING SKIP LIMIT
                                     #   OFFSET CREATE DROP GRAPH USE
crates/varve-gql/src/ast.rs          # Expr tree, LabelSpec, Clause pipeline, ReturnClause, QueryBody/QueryStmt,
                                     #   MatchPart(paths), MutateStmt, SetStmt/RemoveStmt, GraphStmt, Program
crates/varve-gql/src/parser.rs       # Pratt expressions, clause loop, program parsing, parse_program
crates/varve-gql/src/print.rs        # NEW: to_gql printer (fuzz + differential dependency)
crates/varve-plan/src/expr.rs        # NEW: Expr→DataFusion lowering, Scope, conjunct split, iid extraction, params
crates/varve-plan/src/functions.rs   # NEW: FunctionRegistry + builtins + session_context(&FunctionRegistry)
crates/varve-plan/src/pattern.rs     # pipeline lowering, aggregation/order/limit/union, EXISTS, RETURN n
crates/varve-plan/src/exec.rs        # shared context use, LabelFilter plumbing, param-aware iid pushdown
crates/varve-index/src/scan.rs       # LabelFilter, _labels column, visible_events
crates/varve-engine/src/state.rs     # GraphsState (BTreeMap<String, TableState>), __meta constants
crates/varve-engine/src/scan.rs      # overlay params, LabelFilter, graph routing
crates/varve-engine/src/writer.rs    # program resolve, Overlay, SET/REMOVE/ERASE, engine-resolved MATCH parts
crates/varve-engine/src/db.rs        # query_with/execute_with, execute_body factoring, catalog checks
crates/varve-engine/src/flush.rs     # per-graph flush
crates/varve-log/src/record.rs       # TableEffects.graph (prost tag 3)
crates/varve-testkit/src/tck/        # NEW: gherkin.rs, values.rs, translate.rs, runner.rs
crates/varve-testkit/src/bin/parse_corpus.rs   # NEW: differential-oracle side A
crates/varve-testkit/tests/tck.rs    # NEW: the gated TCK run
crates/varve/examples/gql_tour.rs    # NEW: slice demo
resources/tck/{README.md, features/**, exclusions.toml, core.txt, baseline.txt}
resources/gql-corpus/*.gql, *.ext.gql
fuzz/{Cargo.toml, fuzz_targets/parse.rs}   # NEW: cargo-fuzz crate (not a workspace member)
scripts/gql_diff/{Main.java, compare.py}
.github/workflows/ci.yml             # tck-report artifact, fuzz-nightly, gql-differential jobs
```

**Session boundaries (advisory; resume at first unchecked task):** Session A = Tasks 1–4 (expressions), Session B = Tasks 5–9 (query pipeline), Session C = Tasks 10–14 (mutations + catalog), Session D = Tasks 15–19 (conformance + exit).

---
## Session A — Expressions

### Task 1: Expression AST + lexer operators + Pratt parser

**Files:**
- Modify: `crates/varve-gql/src/token.rs` (new tokens/keywords; fold in the slice-1 lexer follow-up: hoist the per-ident `to_ascii_uppercase` allocation)
- Modify: `crates/varve-gql/src/ast.rs`, `crates/varve-gql/src/parser.rs`
- Modify (mechanical, keep-green): `crates/varve-plan/src/{exec.rs,pattern.rs}`, `crates/varve-engine/src/{writer.rs,db.rs}`
- Tests: inline `mod tests` in token.rs/parser.rs

**Interfaces (produces — later tasks depend on these exact shapes):**
```rust
// token.rs — TokenKind grows: Neq, Lte, Gte, Plus, Slash, Percent, Pipe, Semicolon
// Keyword grows: Not, Or, Xor, Is, Case, When, Then, Else, End, Exists, Cast, In,
//   Starts, Ends, With, Contains, Optional, Filter, Let, Set, Remove, Erase, Union,
//   Distinct, Order, By, Asc, Ascending, Desc, Descending, Skip, Limit, Offset,
//   Create, Drop, Graph, Use

// ast.rs
pub enum UnaryOp { Not, Neg, IsNull, IsNotNull }
pub enum BinaryOp { Add, Sub, Mul, Div, Mod, Eq, Neq, Lt, Lte, Gt, Gte,
                    And, Or, Xor, In, StartsWith, EndsWith, Contains }
pub enum CastType { Int, Float, Str, Bool }
pub enum Expr {
    Literal(Literal),
    Param(String),                                   // $name
    Prop { var: String, prop: String },
    Var(String),
    Star,                                            // ONLY inside count(*)
    List(Vec<Expr>),
    Unary { op: UnaryOp, expr: Box<Expr> },
    Binary { op: BinaryOp, lhs: Box<Expr>, rhs: Box<Expr> },
    Case { operand: Option<Box<Expr>>, whens: Vec<(Expr, Expr)>, otherwise: Option<Box<Expr>> },
    FnCall { name: String, args: Vec<Expr>, distinct: bool },
    Cast { expr: Box<Expr>, ty: CastType },
    Exists { paths: Vec<PathPattern>, where_clause: Option<Box<Expr>> },
}
impl Expr {
    pub fn conjuncts(&self) -> Vec<&Expr>;                       // flatten nested top-level ANDs
    pub fn as_prop_eq(&self) -> Option<(&str, &str, &Literal)>;  // var.prop = literal, either side
}
```
Pratt binding powers (low→high, left-assoc unless noted): `OR`(1) < `XOR`(2) < `AND`(3) < `NOT`(prefix, 4) < comparisons `= <> < <= > >= IN STARTS WITH ENDS WITH CONTAINS`(5, non-chaining) < `+ -`(6) < `* / %`(7) < unary `-`(8) < postfix `IS [NOT] NULL`(9) < primary. Primaries: literal, `$param`, ident (var), `ident.prop`, `ident(args…)` fn call (with optional `DISTINCT` first arg-position keyword, `*` only for `count`), `CASE`, `EXISTS {…}`, `CAST(e AS T)`, `[e, …]` list, `(e)` parens. `valid_from`/`valid_to`/`system_from` parse as ordinary `FnCall` — **delete `TemporalFnKind` and `ReturnItem::TemporalFn`** (ReturnItem itself is replaced in Task 5; this task keeps `ReturnItem` compiling by mapping the temporal-fn parse onto `FnCall` inside the existing item type: change `ReturnItem::TemporalFn` to `ReturnItem::Expr { expr: Expr, alias: Option<String> }` and parse prop/temporal-fn/var items into it).

Parser fold-ins from STATUS (slice-1 follow-ups): empty prop block `(:L {})` parses; keywords accepted as property names after `.` and as prop-map keys (contextual: a `Kw` token in those positions is its identifier text); variable names containing `__` rejected (decision 7).

- [x] **Steps (TDD, one commit per green cycle):**
  1. Failing lexer tests: `lexes_comparison_and_arithmetic_operators`, `lexes_semicolon_and_pipe`, `keywords_are_case_insensitive_including_new_ones`. Implement tokens. Commit `feat(gql): operator and keyword tokens for expression grammar`.
  2. Failing parser tests (WHERE position, assert exact AST): `parses_precedence_or_xor_and_not` (`WHERE a.x = 1 OR NOT b.y = 2 AND c.z = 3` → Or(_, And(Not(_), _))), `parses_arithmetic_precedence` (`1 + 2 * 3`), `parses_comparisons_and_string_predicates`, `parses_is_null_postfix`, `parses_case_searched_and_simple`, `parses_cast_and_list_and_in`, `parses_fn_call_and_count_star_and_count_distinct`, `parses_param`, `parses_exists_block`, `keyword_property_names_accepted`, `empty_prop_block_parses`, `double_underscore_var_rejected`. Implement Pratt parser. Commit.
  3. `conjuncts`/`as_prop_eq` unit tests (incl. reversed operand order `1 = a.x`); implement. Commit.
  4. **Keep-green mechanical migration:** replace every `Expr::PropEq` pattern-site with `conjuncts()`+`as_prop_eq()`: `writer.rs` `where_for_var` (STATUS fast-follow — it stops compiling this task, by design), `exec.rs::iid_point` + `iids_from_snapshot`, `pattern.rs::apply_element_predicates`, `db.rs::plan_fast_path`. WHERE shapes beyond conjunctions of prop-eq lower to `PlanError::Unsupported("expression lowering lands in Task 2")` for now. Run `cargo test --workspace` — everything green. Commit `refactor: Expr tree with prop-eq compatibility lowering`.

**Run:** `cargo test -p varve-gql && cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings`

### Task 2: Expression lowering + 3VL + absent-property-NULL + iid fast path

**Files:**
- Create: `crates/varve-plan/src/expr.rs`; Modify: `pattern.rs`, `exec.rs`, `lib.rs` (re-exports)
- Tests: `expr.rs` inline (pure lowering), `crates/varve-plan/tests/exec_test.rs` + `crates/varve/tests/expressions.rs` (NEW, e2e)

**Interfaces:**
```rust
// expr.rs
pub struct Scope<'a> { /* element vars → available mangled columns; value vars; path vars */ }
impl<'a> Scope<'a> {
    pub fn element(&self, var: &str) -> Option<&ElementCols>;   // schema-derived
    pub fn has_value_var(&self, var: &str) -> bool;
}
pub fn lower_expr(expr: &Expr, scope: &Scope, params: &BTreeMap<String, Value>)
    -> Result<datafusion::logical_expr::Expr, PlanError>
pub fn split_conjuncts(where_clause: Option<&Expr>) -> (Vec<&Expr>, Vec<&Expr>)  // (exists-conjuncts, rest)
pub fn iid_from_conjuncts(conjuncts: &[&Expr], var: &str, params: &BTreeMap<String, Value>,
    graph: &str, table: &str) -> Option<Iid>
// PlanError grows: MissingParam(String), UnknownFunction(String)
```
Lowering rules (decisions 2–4): `Prop` → `col(mangled(var, prop))`, absent column → `lit(ScalarValue::Null)`; unknown var → `UnknownVariable`. Binary/Unary → DF operators (`and/or/not/binary ops`, `IS NULL` → `is_null`); `Xor` → `(a OR b) AND NOT (a AND b)` on wrapped null-safe? — NO: lower `Xor` as `a != b` on booleans via DF `not_eq` (SQL 3VL-correct for XOR: NULL-propagating; add a comment). `Case` → `case`/`when` builders; `Cast` → `cast(e, DataType::{Int64, Float64, Utf8, Boolean})`; `In` → chained `OR` of equalities (3VL-correct, list literals only in v1 — `x IN <non-list-expr>` ⇒ Unsupported); `StartsWith/EndsWith/Contains` → verified DF fns; `List` outside `IN`/`FOR` ⇒ Unsupported for now (lands with FOR in Task 6); `FnCall`/`Exists` here ⇒ `Unsupported` (Tasks 3/9 lift). Params → `lit` of converted `ScalarValue` (Int64/Float64/Utf8/Boolean/Null; `Value::Bytes` ⇒ Unsupported, recorded).

- [x] **Steps:**
  1. Failing pure tests in expr.rs: `lowers_prop_to_mangled_col`, `absent_property_lowers_to_null`, `unknown_variable_errors`, `lowers_case_cast_in_string_predicates`, `missing_param_errors`. Implement `Scope`/`lower_expr`. Commit.
  2. Failing e2e `crates/varve/tests/expressions.rs`: `where_full_boolean_expressions` (arith + comparison + AND/OR/NOT over inserted rows), **`three_valued_logic_truth_table`** — the load-bearing test; pin it exactly:
     ```rust
     // rows: a={x:1}, b={x:null via absent prop}, c={x:2}
     // WHERE x = 1        → {a}          (NULL comparison ⇒ not TRUE)
     // WHERE NOT n.x = 1  → {c}          (NOT NULL ⇒ NULL ⇒ filtered)
     // WHERE n.x = 1 OR n.x = 2   → {a, c}
     // WHERE n.x IS NULL          → {b}
     // WHERE n.x IS NOT NULL      → {a, c}
     ```
     plus `where_extra_conjuncts_keep_iid_fast_path` (`WHERE n._id = 7 AND n.age > 3` still answers correctly; assert via result, fast-path exercised by the existing point-lookup test patterns), `case_expression_in_where`, `absent_property_where_returns_no_rows_not_error` (kills the slice-1 `UnknownColumn` deviation — update any test that pinned the old error). Implement: `apply_element_predicates`/WHERE application in pattern.rs + `iids_from_snapshot` in exec.rs route through `lower_expr`; `iid_point` → `iid_from_conjuncts`. Commit.
  3. Clippy + full suite. Commit `feat(plan): full expression lowering with 3VL semantics`.

**Run:** `cargo test -p varve-plan && cargo test -p varve --test expressions && cargo test --workspace`

### Task 3: FunctionRegistry + builtin library + one session context

**Files:**
- Create: `crates/varve-plan/src/functions.rs`; Modify: `expr.rs` (FnCall lowering), `expand.rs` (session_context moves/absorbs registry), `exec.rs` (bare `SessionContext::new()` replaced), `lib.rs`
- Tests: functions.rs inline + `crates/varve/tests/functions.rs` (NEW)

**Interfaces:**
```rust
pub enum ScalarFn {
    Udf(std::sync::Arc<datafusion::logical_expr::ScalarUDF>),
    Builder(fn(Vec<datafusion::logical_expr::Expr>) -> Result<datafusion::logical_expr::Expr, PlanError>),
}
pub struct FunctionRegistry { /* scalars: BTreeMap<String, ScalarFn>, aggregates: BTreeSet<&'static str> */ }
impl FunctionRegistry {
    pub fn with_builtins() -> FunctionRegistry;
    pub fn register_scalar(&mut self, name: &str, f: ScalarFn);   // embedder seam (spec §10.5)
    pub fn scalar(&self, name: &str) -> Option<&ScalarFn>;
    pub fn is_aggregate(&self, name: &str) -> bool;               // count/sum/avg/min/max/collect
}
pub fn session_context(functions: &FunctionRegistry) -> SessionContext  // keeps VarveQueryPlanner
```
Builtins (GQL name → verified DF fn; all `Builder`-style unless a real UDF is required): string `upper, lower, trim→btrim, ltrim, rtrim, replace, char_length→character_length, character_length, substring→substr, left, right, reverse, contains, starts_with, ends_with`; numeric `abs, ceil, floor, round, sqrt, power, sign→signum, trunc`; list `size→cardinality, head→array_element(e,1), last→array_element(e, cardinality(e))`; temporal `valid_from/valid_to/system_from` → **column substitution** builders (arg must be `Expr::Var` of an element; lowered to `col(mangled(var, "_valid_from"))` etc. — the Task-1 parse change made these plain FnCalls; the old `temporal_fn_columns` mechanism folds in here). Aggregates are names only (lowered in Task 8).

- [x] **Steps:**
  1. Failing registry unit tests: `builtins_resolve_by_name`, `unknown_function_errors`, `is_aggregate_set`. Implement types + builtins. Commit.
  2. Failing e2e `functions.rs`: `string_functions_in_where_and_return`, `numeric_functions`, `temporal_fns_still_work_via_registry` (pins the old `valid_from(p)` surface — existing temporal tests must stay green unmodified), `nested_function_calls` (`upper(trim(n.name))`). Implement FnCall lowering + `session_context(&FunctionRegistry)`; both `execute_pattern` and `iids_from_snapshot` use it (one context path — DML reads get functions too). `Db` holds one `FunctionRegistry::with_builtins()` (constructed in `assemble`, `Arc`ed). Commit `feat(plan): FunctionRegistry with builtin scalar library`.

**Run:** `cargo test -p varve-plan && cargo test -p varve --test functions && cargo test --workspace`

### Task 4: Parameters end-to-end

**Files:**
- Modify: `crates/varve-engine/src/db.rs` (`query_with`/`execute_with`, thread params), `writer.rs` (`Submission` carries params; const-eval prop maps), `crates/varve-plan/src/{expr.rs,pattern.rs,exec.rs}` (params threading — done in Task 2 signatures)
- Tests: `crates/varve/tests/params.rs` (NEW)

**Interfaces:**
```rust
impl Db {
    pub async fn query_with(&self, gql: &str, params: &BTreeMap<String, Value>) -> Result<Vec<RecordBatch>, EngineError>;
    pub async fn execute_with(&self, gql: &str, params: &BTreeMap<String, Value>) -> Result<TxReceipt, EngineError>;
    // query()/execute() delegate with an empty map
}
// writer: pub(crate) fn const_value(expr: &Expr, params: &BTreeMap<String, Value>) -> Result<Value, EngineError>
//   Literal | Param | Unary{Neg, numeric literal} only (decision 5)
```

- [x] **Steps:**
  1. Failing e2e: `param_in_where`, `param_in_insert_props` (`INSERT (:P {name: $n})`), `param_as_iid_fast_path` (`WHERE n._id = $id`), `missing_param_is_error`, `param_in_return`. (Scope here = INSERT/WHERE/RETURN; SET picks params up for free in Task 11 via `const_value`/`lower_expr`.) Implement. Commit `feat: query and mutation parameters`.
  2. Full suite + clippy. Commit.

**Run:** `cargo test -p varve --test params && cargo test --workspace`

---
## Session B — Query pipeline

### Task 5: Clause-pipeline AST + parser

**Files:**
- Modify: `crates/varve-gql/src/ast.rs`, `parser.rs`
- Modify (keep-green adapters): `crates/varve-plan/src/{exec.rs,pattern.rs}`, `crates/varve-engine/src/db.rs` (read the first/only `Clause::Match` where they read `stmt.paths`/`stmt.where_clause` today)
- Tests: parser.rs inline

**Interfaces (exact — Tasks 6–9 and the printer depend on these):**
```rust
pub enum LabelSpec { All(Vec<String>), Any(Vec<String>) }   // All(vec![]) = unlabeled
pub struct NodePattern { pub var: Option<String>, pub labels: LabelSpec, pub props: Vec<(String, Expr)> }
pub struct EdgePattern { pub var: Option<String>, pub label: String, pub props: Vec<(String, Expr)>,
                         pub direction: Direction, pub quantifier: Option<Quantifier> }
pub enum Clause {
    Match { optional: bool, paths: Vec<PathPattern>, temporal: TemporalClauses, where_clause: Option<Expr> },
    Filter(Expr),
    Let(Vec<(String, Expr)>),
    For { var: String, list: Expr },
}
pub struct SortItem { pub expr: Expr, pub asc: bool }
pub struct ReturnClause { pub distinct: bool, pub items: Vec<(Expr, Option<String>)>,
                          pub order_by: Vec<SortItem>, pub skip: Option<u64>, pub limit: Option<u64> }
pub struct QueryBody { pub temporal: TemporalClauses, pub clauses: Vec<Clause>, pub ret: ReturnClause }
pub enum UnionKind { Distinct, All }
pub struct QueryStmt { pub first: QueryBody, pub unions: Vec<(UnionKind, QueryBody)> }
pub struct MatchPart { pub paths: Vec<PathPattern>, pub where_clause: Option<Expr> }
pub enum MutKind { Delete, Erase }
pub struct MutateStmt { pub match_part: MatchPart, pub target: String, pub kind: MutKind, pub detach: bool }
pub enum SetItem { Prop { var: String, prop: String, value: Expr }, Label { var: String, label: String } }
pub enum RemoveItem { Prop { var: String, prop: String }, Label { var: String, label: String } }
pub struct SetStmt { pub match_part: MatchPart, pub items: Vec<SetItem> }
pub struct RemoveStmt { pub match_part: MatchPart, pub items: Vec<RemoveItem> }
pub enum GraphStmt { Create(String), Drop(String) }
pub enum Statement { Insert(InsertStmt), Query(Box<QueryStmt>), Mutate(MutateStmt),
                     Set(SetStmt), Remove(RemoveStmt), Graph(GraphStmt) }
pub struct Program { pub use_graph: Option<String>, pub statements: Vec<Statement> }
pub fn parse_program(src: &str) -> Result<Program, GqlError>   // splits on ';'; parse() = exactly-one convenience
```
`ReturnItem` is deleted (items are `(Expr, Option<alias>)`; bare `RETURN n`/`RETURN p` = `Expr::Var`). `DeleteStmt` is replaced by `MutateStmt` (dev mode — rename freely). `FOR` disambiguation: after `FOR`, `VALID_TIME`/`SYSTEM_TIME` keyword ⇒ temporal clause, identifier ⇒ unwind clause. Label parsing: `:A`/`(:A:B)`/`(:A&B)` → `All`, `(:A|B)` → `Any`; mixing `&`/`|` or `!` ⇒ parse error `"label expression nesting is post-v1"`. SET/REMOVE/ERASE/CREATE GRAPH/DROP GRAPH/USE parse here (execution lands Tasks 10–14; until then engine answers `EngineError::Unsupported` with the task number — keep-green). OPTIONAL MATCH/FILTER/LET/FOR/ORDER BY/SKIP/LIMIT/OFFSET/UNION/DISTINCT parse into the pipeline AST; lowering beyond the single-MATCH degenerate case is Task 6 (`Unsupported` until then).

- [x] **Steps:**
  1. Failing parser tests (exact-AST asserts): `parses_multi_clause_pipeline` (MATCH + OPTIONAL MATCH + FILTER + LET + FOR + RETURN), `parses_comma_separated_match_paths`, `parses_return_distinct_order_skip_limit_offset_aliases`, `parses_union_and_union_all_chain`, `parses_label_conjunction_and_alternation`, `label_mixing_rejected`, `parses_set_remove_statements`, `parses_erase_and_detach_erase`, `parses_create_drop_graph_and_use_prefix`, `parses_program_semicolon_split`, `for_temporal_vs_unwind_disambiguation`. Implement AST + parser. Commit.
  2. Keep-green adapters: `scan_specs`/`effective_bounds`/`db.rs` read the degenerate pipeline: `Clause::Match{..}` at index 0 and the `ret` items; any other clause shape ⇒ `Unsupported("pipeline lowering lands in Task 6")`). All existing workspace tests green. Commit `feat(gql): clause-pipeline AST and program parsing`.

**Run:** `cargo test -p varve-gql && cargo test --workspace`

### Task 6: Pipeline lowering — multi-MATCH, OPTIONAL, multi-path, FILTER/LET/FOR

**Files:**
- Modify: `crates/varve-plan/src/pattern.rs` (the lowering fold), `exec.rs`, `expr.rs` (Scope learns value vars), `crates/varve-engine/src/db.rs` (ScanInputs for ALL match clauses of a body)
- Tests: `crates/varve/tests/pipeline.rs` (NEW e2e), pattern.rs inline for pure pieces

**Interfaces:**
```rust
// pattern.rs — scan_specs now walks every Match clause of a body:
pub fn scan_specs(body: &QueryBody, graph: &str, max_path_depth: u32) -> Result<Vec<ClauseSpecs>, PlanError>
pub struct ClauseSpecs { pub optional: bool, pub specs: Vec<ScanSpec>, pub shared_vars: Vec<String> }
pub async fn execute_body(body: &QueryBody, clause_specs: &[ClauseSpecs], inputs: Vec<Vec<ScanInput>>,
    functions: &FunctionRegistry, params: &BTreeMap<String, Value>) -> Result<Vec<RecordBatch>, PlanError>
```
Fold semantics (decision 6): first clause = non-OPTIONAL MATCH (else `Unsupported`); each subsequent `Match` joins on every shared element var's `mangled(var, "_iid")` (Inner / Left for optional); no shared vars ⇒ `__cross` lit(1) key join, drop the key column after. Multi-path within one MATCH: same rule between path DataFrames. Per-clause temporal bounds already exist (`effective_bounds` moves to per-Match-clause). `Filter` → `filter(lower_expr(..))`; `Let` → `with_column(var, ..)` + Scope gains value var; `For` → `with_column` + `unnest_columns_with_options(&[var], UnnestOptions{preserve_nulls:false, ..Default::default()})` — `Expr::List` lowering (make_array via `functions_nested`) becomes legal as the FOR source and in RETURN. A var re-MATCHed with a label/prop pattern after being bound ⇒ join semantics make it a constraint (GQL-correct); re-binding a *value* var ⇒ `Unsupported`.

- [x] **Steps:**
  1. Failing e2e `pipeline.rs`: `two_match_clauses_join_on_shared_var`, `optional_match_preserves_left_rows_with_nulls` (assert NULL columns present for unmatched), `optional_match_chained_after_hop`, `comma_separated_paths_shared_var_join`, `comma_separated_paths_disjoint_cross_product` (assert row count = |A|×|B|), `filter_mid_pipeline`, `let_binds_expression_value`, `for_unwinds_list_literal` (`FOR x IN [1,2,3]`), `for_over_null_or_empty_eliminates_row`, `optional_match_first_clause_unsupported`. Implement the fold. This is the largest single lowering change — implement clause-by-clause, running the suite between steps; multiple commits welcome (e.g. multi-MATCH join first, then OPTIONAL, then LET/FOR).
  2. AS-OF regression: `per_match_temporal_bounds_still_honored` (two MATCH clauses with different `FOR SYSTEM_TIME` bounds — extends the slice-6 AS-OF coverage into multi-clause). Commit `feat(plan): clause-pipeline lowering`.

**Run:** `cargo test -p varve --test pipeline && cargo test --workspace` (traversal/temporal/social suites must stay green — they exercise the degenerate path)

### Task 7: Multi-label matching + `_labels` snapshot column

**Files:**
- Modify: `crates/varve-index/src/scan.rs` (LabelFilter + `_labels` column), `crates/varve-engine/src/scan.rs` (`merged_snapshot` takes `LabelFilter`), `crates/varve-plan/src/pattern.rs` (SpecKind::Node carries `LabelSpec`), `db.rs`/`writer.rs` call sites
- Tests: scan.rs inline + `crates/varve/tests/labels.rs` (NEW)

**Interfaces:**
```rust
// varve-index scan.rs
pub enum LabelFilter<'a> { Single(&'a str), All(&'a [String]), Any(&'a [String]) }
pub fn snapshot_entities<'a, I>(entities: I, label: LabelFilter<'_>, bounds: &TemporalBounds)
    -> Result<Option<RecordBatch>, IndexError>
// snapshot schema change: `_labels` List<Utf8> non-null column (item field NULLABLE — same
// RecordBatch::try_new constraint PathExpand hit in slice 6) inserted after the temporal
// columns (and after _src_iid/_dst_iid for edges), before property columns.
```
`SpecKind::Node.label: Option<String>` → `labels: LabelSpec`; `Single` remains the fast path (`All` of one). Property-type inference and existing consumers skip `_labels` like they skip `_iid`/temporal columns. Unlabeled MATCH keeps returning empty (recorded quirk, unchanged).

- [x] **Steps:**
  1. Failing index tests: `snapshot_emits_labels_column`, `label_filter_all_requires_every_label`, `label_filter_any_matches_either`. Implement. Commit.
  2. Failing e2e `labels.rs`: `multi_label_insert_and_conjunction_match` (`INSERT (:A:B {..})` then `MATCH (n:A:B)` hits, `MATCH (n:A:C)` misses), `label_alternation_match`, `alternation_with_edges_still_single_label` (edge `[:A|B]` is a parse error — asserted message). Update the slice-6 `Unsupported("multi-label MATCH lands in slice 7")` reject and its pinning test. Commit `feat: multi-label and label-alternation node matching`.

**Run:** `cargo test -p varve-index && cargo test -p varve --test labels && cargo test --workspace`

### Task 8: RETURN completion — expressions, aggregation, DISTINCT, ORDER BY/SKIP/LIMIT, UNION, bare elements

**Files:**
- Modify: `crates/varve-plan/src/pattern.rs` (`project_return` rewrite + post-projection stages), `functions.rs` (aggregate lowering map)
- Tests: `crates/varve/tests/return_shapes.rs` (NEW)

**Interfaces:** `project_return` becomes: classify items (decision 11) → optional `aggregate` → `select` with aliases → optional `distinct` → optional `sort(Vec<SortExpr>)` → optional `limit(skip, fetch)`; then `union`/`union().distinct()` across bodies (decision 12). Output naming: explicit alias wins; else a stable rendering of the item expr (`n.name`, `count(*)`) via a new `pub fn display_expr(&Expr) -> String` in varve-gql, implemented in THIS task; the Task-15 printer builds on it. Bare element var (decision 14): project `<var>._iid`, `<var>._labels`, `<var>._src_iid`/`_dst_iid` (edges), and every property column as `<var>.<prop>`.

- [x] **Steps:**
  1. Failing e2e: `return_arbitrary_expressions` (`RETURN n.age * 2 AS double_age`), `implicit_grouping_count_by_key` (mixed items group by non-aggregates — assert exact groups), `count_star_and_count_distinct`, `collect_returns_list`, `min_max_sum_avg`, `return_distinct_dedupes`, `order_by_multi_key_asc_desc_with_skip_limit`, `offset_is_skip_synonym`, `union_all_concatenates_union_dedupes`, `union_schema_mismatch_errors`, `return_bare_node_materializes_labels_and_props`, `return_bare_edge_includes_endpoints`, `aggregate_in_where_rejected` (`Unsupported`), `distinct_in_non_count_aggregate_rejected`. Implement in the order aggregation → distinct/order/limit → union → bare elements, committing per green step.
  2. Determinism check: ORDER BY-less results keep whatever join order DataFusion yields — tests must sort or use `ORDER BY`; add `// determinism` comment in test helpers. Commit `feat(plan): full RETURN clause`.

**Run:** `cargo test -p varve --test return_shapes && cargo test --workspace`

### Task 9: EXISTS subqueries

**Files:**
- Modify: `crates/varve-plan/src/{expr.rs,pattern.rs}` (conjunct split + semi/anti-join lowering), `crates/varve-engine/src/db.rs` (ScanInputs for EXISTS subpatterns)
- Tests: `crates/varve/tests/exists.rs` (NEW)

**Interfaces:** `split_conjuncts` (Task 2) routes `Exists`/`Unary{Not, Exists}` conjuncts to the pattern layer; each lowers its `paths` via the same per-clause machinery (a nested `ClauseSpecs`, temporal bounds = enclosing clause's), then `join(sub, JoinType::LeftSemi | LeftAnti, on = shared vars' iid columns)`. Shared vars = vars bound in the outer scope ∩ vars in the EXISTS paths; empty ⇒ `Unsupported("EXISTS must share a variable with the enclosing pattern")`. EXISTS anywhere else in the tree ⇒ `Unsupported` (decision 10).

- [x] **Steps:**
  1. Failing e2e: `exists_filters_to_connected_nodes` (`MATCH (a:Person) WHERE EXISTS { (a)-[:KNOWS]->(:Person) } RETURN a.name`), `not_exists_is_anti_join`, `exists_with_inner_where`, `exists_under_or_rejected`, `exists_no_shared_var_rejected`, `exists_respects_temporal_bounds` (AS-OF: edge deleted later still satisfies EXISTS at the earlier system time). Implement. Commit `feat(plan): EXISTS subqueries as semi/anti joins`.

**Run:** `cargo test -p varve --test exists && cargo test --workspace`

---
## Session C — Mutations + catalog

### Task 10: Mutation MATCH parts through the query engine

**Files:**
- Modify: `crates/varve-engine/src/db.rs` (factor the ScanInput-building I/O shell — `scan_inputs_for`, below — shared by `Db::query` and the writer), `writer.rs` (`resolve_insert`/`resolve_delete` bindings via pattern execution), `crates/varve-plan/src/pattern.rs` (binding-row extraction helper)
- Tests: `crates/varve-engine/tests/mutations.rs` (extend), `crates/varve/tests/mutation_patterns.rs` (NEW)

**Interfaces:**
```rust
// varve-plan pattern.rs
pub async fn binding_rows(body: &QueryBody, clause_specs: &[ClauseSpecs], inputs: Vec<Vec<ScanInput>>,
    functions: &FunctionRegistry, params: &BTreeMap<String, Value>, vars: &[String])
    -> Result<Vec<BTreeMap<String, Iid>>, PlanError>
// distinct var→iid rows, sorted by the iid tuple (decision 15 determinism)

// varve-engine db.rs — the shared shell (used by Db::query AND writer::resolve_*):
pub(crate) async fn scan_inputs_for(state: &..., store: &..., graph: &str, clause_specs: &[ClauseSpecs],
    bounds_per_clause: &[TemporalBounds], overlay: Option<&Overlay>) -> Result<Vec<Vec<ScanInput>>, EngineError>
// `Overlay` (nodes/edges LiveTables) is DEFINED in this task with all call sites passing None;
// Task 13 populates it. Keeps every signature stable across the two tasks.
```
`MatchPart{paths, where_clause}` converts to a synthetic `QueryBody` (clauses = one Match, ret = the match vars) and resolves via `binding_rows` at the writer snapshot. Hops in mutation MATCH parts now work (slice-6 promise); quantified hops / path vars in a MatchPart ⇒ `EngineError::Unsupported` (decision 15). The old per-variable candidate Cartesian machinery (`where_for_var`, per-var `matching_iids` composition) is **deleted**.

- [x] **Steps:**
  1. Failing e2e `mutation_patterns.rs`: `match_insert_with_hop_binds_endpoints` (`MATCH (a:P)-[:KNOWS]->(b:P) INSERT (a)-[:MET]->(b)` — the slice-6 reject test flips to positive), `match_insert_connected_pattern_not_cartesian` (two connected vars must NOT produce the cross product the old code would — this is the semantic fix that motivates the task; construct a graph where the two differ and assert exact edges), `delete_with_hop_pattern`, `match_part_quantified_hop_rejected`. Implement. Commit `feat(engine): mutation MATCH parts resolved by the query engine`.
  2. Full suite — slice-6 `MATCH…INSERT`/DETACH tests and both property oracles must stay green (they pin per-edge resolution semantics). Commit.

**Run:** `cargo test -p varve-engine && cargo test -p varve --test mutation_patterns && cargo test --workspace && PROPTEST_CASES=256 cargo test -p varve-testkit --release`

### Task 11: SET and REMOVE

**Files:**
- Modify: `crates/varve-index/src/scan.rs` (`visible_events` — factored from the `snapshot_entities` resolution core), `crates/varve-engine/src/writer.rs` (`resolve_set`/`resolve_remove`), `db.rs` (dispatch)
- Tests: scan.rs inline, `crates/varve/tests/set_remove.rs` (NEW)

**Interfaces:**
```rust
// varve-index — ONE visibility implementation (decision 16): extract the visible-version
// selection that snapshot_entities already performs into a shared internal fn, expose:
pub fn visible_events<'a, I>(entities: I, bounds: &TemporalBounds) -> Vec<(Iid, &'a [String], &'a Doc)>
    where I: IntoIterator<Item = (Iid, &'a [Event])>;
// varve-engine writer.rs
// resolve_set: binding_rows → evaluate SET-item value exprs per row by projecting them over
//   the match-result RecordBatch (DataFusion select) → arrow scalar → Value → merge into the
//   entity's visible doc/labels → one Op::Put per changed entity (valid [now,∞), system now).
// resolve_remove: same shape, dropping props/labels.
```
Rules pinned by tests (decision 16): several SET items merge into one event; several binding rows on one entity apply in row order (last wins); no-op changes emit no event; SET on an entity var bound by OPTIONAL-null ⇒ skipped row; unknown target var ⇒ `UnboundVariable`.

- [x] **Steps:**
  1. Failing index test: `visible_events_matches_snapshot_visibility` (same fixture through both paths agrees — the factoring guard). Implement extraction. Commit `refactor(index): shared visible-version core + visible_events`.
  2. Failing e2e `set_remove.rs`: `set_prop_updates_current_state_only` (**bitemporal check:** old value still visible `FOR VALID_TIME AS OF` before the SET, and `FOR SYSTEM_TIME AS OF` before the tx — the load-bearing test of this task), `set_prop_from_expression_per_row` (`SET n.double = n.age * 2` over 3 matched rows), `set_add_label_and_match_by_it`, `remove_prop_and_label`, `remove_missing_prop_is_noop_no_event` (assert via history: no new system version), `set_merges_multiple_items_one_event`, `set_with_param_value`. Implement. Commit `feat(engine): SET and REMOVE with bitemporal RMW`.

**Run:** `cargo test -p varve-index && cargo test -p varve --test set_remove && cargo test --workspace`

### Task 12: ERASE / DETACH ERASE statement

**Files:**
- Modify: `crates/varve-engine/src/writer.rs` (`resolve_delete` generalizes over `MutKind`), `db.rs`
- Tests: `crates/varve/tests/erase.rs` (NEW)

**Interfaces:** `MutateStmt.kind: MutKind::{Delete, Erase}` — one resolve path; `Erase` emits `Op::Erase` where `Delete` emits `Op::Delete`, StillConnected/DETACH semantics identical (decision 17).

- [x] **Steps:**
  1. Failing e2e `erase.rs`: `erase_hides_history_at_every_system_time` (insert → update → ERASE → `FOR SYSTEM_TIME AS OF <pre-erase>` returns NOTHING — pins the slice-2 GDPR property at the statement surface), `erase_connected_requires_detach` (`StillConnected`), `detach_erase_erases_incident_edges` (edge history also gone at all system times), `erase_then_reinsert_same_id_is_fresh_entity`. Implement (small: `MutKind` switch in event construction). Commit `feat(engine): ERASE and DETACH ERASE statements`.

**Run:** `cargo test -p varve --test erase && cargo test --workspace`

### Task 13: Multi-statement programs + overlay visibility

**Files:**
- Modify: `crates/varve-engine/src/db.rs` (program routing), `writer.rs` (`Submission{statements, params, graph}`, `resolve_program`, `Overlay`), `scan.rs` (`overlay: Option<&Overlay>` on `merged_snapshot`/`edge_adjacency`/`incident_edges`/`reachable_edges`)
- Modify: `crates/varve-testkit/src/fixture.rs` + `crates/varve/examples/traversal_bench.rs` (batched edge ingest — the slice-6 ingest-throughput note)
- Tests: `crates/varve/tests/programs.rs` (NEW)

**Interfaces:**
```rust
pub(crate) struct Overlay { pub nodes: LiveTable, pub edges: LiveTable }
// merged_snapshot(state, store, kind, label: LabelFilter<'_>, bounds, iid_point, overlay: Option<&Overlay>)
// — overlay events appended as a final merge_sources source (merge_sources already groups by
//   event.iid regardless of source interleaving — slice-4 decision, reused verbatim)
```
Decision 18 rules pinned by tests: one `TxReceipt` (one tx_id, one system_time, one LogRecord — assert via log inspection as slice-3 tests do); statement N sees statements 1..N-1 (INSERT node; then MATCH-INSERT edge onto it; then SET it — all in one program); failure in ANY statement (e.g. last statement `StillConnected`) leaves NOTHING visible and NOTHING in the log; equal-`system_from` apply verified (non-strict monotonicity, `live.rs:46`); `Db::query` on a 2-statement program ⇒ `NotAQuery`-class error; mixed query+mutation program ⇒ `NotAMutation`.

- [x] **Steps:**
  1. Failing e2e `programs.rs`: `program_is_one_tx_one_log_record`, `later_statement_sees_earlier_effects`, `failed_statement_rolls_back_whole_program`, `detach_delete_after_insert_in_same_program` (adjacency overlay path), `hundred_inserts_one_program_faster_than_hundred_txs` (coarse assert: fewer log records, not wall-clock). Implement `Overlay` + threading + program loop. Commit `feat(engine): multi-statement all-or-nothing programs`.
  2. Batch fixture/bench edge ingest into programs of 100; run `cargo run --release --example traversal_bench -p varve` and record the new ingest rate in STATUS at slice end. Commit `perf(testkit): batched edge ingest via programs`.

**Run:** `cargo test -p varve --test programs && cargo test --workspace && PROPTEST_CASES=256 cargo test -p varve-testkit --release`

### Task 14: Catalog — CREATE/DROP GRAPH, USE, multi-graph state

**Files:**
- Modify: `crates/varve-engine/src/state.rs` (`GraphsState`), `db.rs`, `writer.rs`, `scan.rs`, `flush.rs` (per-graph flush), `crates/varve-log/src/record.rs` (`TableEffects.graph` tag 3), recovery path in `db.rs`
- Tests: `crates/varve/tests/catalog.rs` (NEW), extend `crates/varve/tests/blocks.rs` restart coverage

**Interfaces:**
```rust
// state.rs
pub(crate) struct GraphsState { pub graphs: BTreeMap<String, TableState> }  // one RwLock<GraphsState>
pub(crate) const META_GRAPH: &str = "__meta";   // catalog entries: label "Graph", _id = name
// record.rs
pub struct TableEffects { /* table tag 1, arrow_ipc tag 2 */ #[prost(string, tag = "3")] pub graph: String }
// "" decodes as "default" — assert existing golden wire bytes UNCHANGED (empty string = zero bytes)
// EngineError grows: UnknownGraph(String), GraphExists(String)
```
Decision 19 mechanics: `USE g` routes the whole program; graph existence checked at resolve time against `__meta` (default/`__meta` implicit); `Iid::derive(graph, …)`, storage keys, flush, and recovery all take the routed graph (today's `DEFAULT_GRAPH` literal becomes a parameter — `pattern.rs` duplicate-literal fast-follow from STATUS dies here too). Flush iterates all dirty graphs into ONE manifest (`TableTries.graph` already exists — tag 1, verified). Recovery routes effects by `(graph, table)`; unknown table still hard-errors.

- [x] **Steps:**
  1. Failing prost test: `table_effects_graph_roundtrip_and_empty_is_default` + golden-wire-unchanged assert. Implement tag 3. Commit.
  2. Failing e2e `catalog.rs`: `create_graph_then_use_inserts_isolated` (same `_id` in two graphs = two distinct entities — IID derivation includes graph), `use_unknown_graph_errors`, `create_existing_graph_errors`, `drop_graph_then_use_errors`, `drop_default_rejected`, `meta_prefix_rejected_for_users`, `graphs_survive_restart` (CREATE → INSERT → flush → reopen → USE still works; catalog restored from `__meta` via normal replay+manifest machinery), `per_graph_flush_one_manifest`. Implement `GraphsState` + routing + catalog statements. This is the widest mechanical change of the slice; commit in stages (state reshape green → catalog statements green → restart green).
  3. Full suite incl. crash matrix: `cargo test -p varve-testkit --release --test crash_recovery`. Commit `feat(engine): minimal catalog and multi-graph namespaces`.

**Run:** `cargo test -p varve --test catalog && cargo test --workspace && cargo test -p varve-testkit --release`

---
## Session D — Conformance harness + exit

### Task 15: AST printer + parse-print-reparse + cargo-fuzz target

**Files:**
- Create: `crates/varve-gql/src/print.rs`; Modify: `lib.rs` (export), `ast.rs` (`display_expr` from Task 8 moves in/joins here)
- Create: `fuzz/Cargo.toml`, `fuzz/fuzz_targets/parse.rs`, `fuzz/.gitignore` (corpus artifacts), seed corpus `fuzz/corpus/parse/*.gql` (copied from `resources/gql-corpus/`)
- Modify: `.github/workflows/ci.yml` (`fuzz-nightly` job)
- Tests: print.rs inline + proptest round-trip in `crates/varve-gql/tests/print_roundtrip.rs` (NEW)

**Interfaces:**
```rust
pub fn to_gql(stmt: &Statement) -> String        // print.rs; also to_gql_program(&Program)
// Invariant (the fuzz oracle): parse(&to_gql(&s)) == Ok(s) for every s that parse() produces.
// Canonical output: single spaces, '…' strings with '' escapes, explicit parens NOT required
// (printer emits precedence-correct output; round-trip equality is on the AST, not the text).
```
`fuzz/` is a standalone cargo-fuzz crate (NOT a workspace member — add to root `Cargo.toml` `[workspace] exclude`), target body: bytes → `str::from_utf8` (ok-else-return) → `parse_program` → on Ok, print → reparse → `assert_eq!` ASTs. CI `fuzz-nightly` job (append to the existing nightly cron `0 3 * * *` workflow section): install nightly + `cargo-fuzz`, `cargo +nightly fuzz run parse -- -max_total_time=600 -rss_limit_mb=4096`; artifacts uploaded on failure.

- [x] **Steps:**
  1. Failing printer unit tests: `prints_reparses_query_pipeline`, `prints_reparses_mutations_and_programs`, `prints_reparses_temporal_clauses_and_extensions`, `printer_parenthesizes_precedence_correctly` (`(1+2)*3` round-trips inequal to `1+2*3`). Implement printer. Commit.
  2. Proptest `print_roundtrip.rs`: `arb_statement()` strategy (compose from existing testkit-style strategies; keep generated identifiers `[a-z][a-z0-9]*` and no `__`) — `parse(to_gql(s)) == s` at `PROPTEST_CASES`. Commit `test(gql): parse-print-reparse property`.
  3. cargo-fuzz target + 60s local smoke (`cargo +nightly fuzz run parse -- -max_total_time=60`) + CI job. Commit `test: parser fuzz target and nightly CI job`.

**Run:** `cargo test -p varve-gql && cargo +nightly fuzz run parse -- -max_total_time=60`

### Task 16: TCK vendoring + Gherkin/value parsers

**Files:**
- Create: `resources/tck/README.md` (source repo URL, pinned commit, Apache-2.0 notice, LICENSE copy), `resources/tck/features/**` (vendored from `github.com/opencypher/openCypher` `tck/features/` at a pinned commit — record the commit hash in README; if network is unavailable during execution, STOP and flag to the user)
- Create: `crates/varve-testkit/src/tck/mod.rs`, `tck/gherkin.rs`, `tck/values.rs`
- Tests: inline in both modules (pure, no Db)

**Interfaces:**
```rust
// gherkin.rs — the subset the TCK actually uses (verified against vendored files at execution
// time; extend only on demand): Feature:, Background:, Scenario:, Scenario Outline: + Examples:
// (expand outlines into concrete scenarios at parse time), tags (@…), steps Given/When/Then/And
// with ``` docstrings and | tables |.
pub struct Feature { pub name: String, pub background: Vec<Step>, pub scenarios: Vec<Scenario> }
pub struct Scenario { pub name: String, pub tags: Vec<String>, pub steps: Vec<Step> }
pub enum StepKind { Given, When, Then, And }
pub struct Step { pub kind: StepKind, pub text: String,
                  pub docstring: Option<String>, pub table: Option<Vec<Vec<String>>> }
pub fn parse_feature(src: &str) -> Result<Feature, GherkinError>   // thiserror enum

// values.rs — TCK expected-result value grammar + comparator
pub enum TckValue { Null, Bool(bool), Int(i64), Float(f64), Str(String), List(Vec<TckValue>),
                    Map(BTreeMap<String, TckValue>),
                    Node { labels: Vec<String>, props: BTreeMap<String, TckValue> },
                    Rel { typ: String, props: BTreeMap<String, TckValue> } }
pub fn parse_value(s: &str) -> Result<TckValue, ValueError>
pub fn compare_results(expected_header: &[String], expected_rows: &[Vec<TckValue>],
    actual: &[RecordBatch], ordered: bool) -> Result<(), String>
// compare_results reconstructs Node/Rel cells from the decision-14 column groups
// (`n._labels`, `n.<prop>`, `n._src_iid`…); unordered compare = multiset match.
```

- [x] **Steps:**
  1. Vendor the features (git clone at pinned commit → copy `tck/features` + LICENSE → commit). Record scenario count in README. Commit `test(tck): vendor openCypher TCK features`.
  2. Failing gherkin tests: `parses_scenario_with_docstring_and_table`, `expands_scenario_outline_examples`, `parses_background_and_tags`, plus `parses_every_vendored_feature_file` (walk `resources/tck/features/**` — parse errors list the file; this drives the subset to completeness against reality). Implement. Commit.
  3. Failing values tests: `parses_primitives_lists_maps`, `parses_node_and_rel_literals`, `unordered_multiset_compare`, `float_compare_is_exact_string_form` (TCK floats compare textually per TCK convention — record). Implement. Commit `test(tck): gherkin and value grammars`.

**Run:** `cargo test -p varve-testkit`

### Task 17: TCK translation, runner, gates, CI report

**Files:**
- Create: `crates/varve-testkit/src/tck/translate.rs`, `tck/runner.rs`, `crates/varve-testkit/tests/tck.rs`, `resources/tck/{exclusions.toml, core.txt, baseline.txt}`
- Modify: `.github/workflows/ci.yml` (report artifact upload in the main test job)

**Interfaces:**
```rust
// translate.rs — mechanical Cypher→GQL: CREATE→INSERT, UNWIND→FOR … IN, `<>`/`=`/ops verbatim,
// DETACH DELETE/DELETE/SET/REMOVE verbatim, relationship types `[:T]` verbatim.
pub fn translate(cypher: &str) -> Result<String, Untranslatable>
pub struct Untranslatable { pub construct: String }   // "WITH", "MERGE", "CALL", … (decision 21 list)
// runner.rs
pub enum Outcome { Passed, Failed(String), Excluded(String), Untranslatable(String) }
pub async fn run_scenario(sc: &Scenario, exclusions: &BTreeMap<String, String>) -> Outcome
// fresh Db::memory per scenario; Given "an empty graph"/"having executed" → execute translated
// setup; When "executing query" → query/execute; Then "the result should be (in any order|in order)"
// → compare_results; "a SyntaxError/TypeError should be raised" → assert Err.
// tests/tck.rs — the gate:
//   walk features → outcomes → report JSON {total, excluded, adapted, passed, rate, failures[]}
//   written to target/tck-report.json, printed as a summary table.
//   FAIL if: any core.txt scenario not Passed; rate < 0.85; any baseline.txt scenario not Passed;
//   any Passed scenario missing from baseline.txt ("newly passing — add to baseline").
```
File formats (exact): `exclusions.toml` = `["<feature>::<scenario>"] reason = "…"` entries; `core.txt`/`baseline.txt` = one `<feature>::<scenario>` per line, `#` comments. Scenario key = feature name + `::` + scenario name (outline expansions get ` #<n>` suffixes).

- [x] **Steps:**
  1. Failing translate tests: `create_becomes_insert`, `unwind_becomes_for`, `with_is_untranslatable`, `merge_is_untranslatable`. Implement. Commit.
  2. Failing runner test on 3 hand-picked simple scenarios (hard-coded strings, no files): `runs_create_match_return_scenario`, `error_expectation_scenario`, `unordered_result_scenario`. Implement runner. Commit.
  3. The full gate `tests/tck.rs`: first run in report-only mode (env `VARVE_TCK_REPORT_ONLY=1` skips the asserts) to populate `exclusions.toml` (every Untranslatable + every decision-21 non-goal, each with its reason), `baseline.txt` (everything that passes today), and `core.txt` (curate ~40–80 scenarios across `clauses/match`, `clauses/create` (as INSERT), `clauses/return`, `clauses/delete`, `expressions/comparison`, `expressions/boolean`, `expressions/null`, `expressions/aggregation`, `literals` — every one must pass; fix Varve bugs they expose BEFORE committing the lists, that's the point of the harness). Then commit lists + green gate. Iterate: this step is the slice's correctness harvest — budget real time for the fix loop. Commit(s) `fix:`/`test(tck): …`.
  4. CI: upload `target/tck-report.json` artifact from the test job. Commit.

**Run:** `cargo test -p varve-testkit --test tck` (the gate), full `cargo test --workspace`

### Task 18: ANTLR differential oracle (CI-only)

**Files:**
- Create: `resources/gql-corpus/*.gql` + `*.ext.gql` (every parser-unit-test input + one file per statement family this slice added + temporal/ERASE extension cases as `.ext.gql`), `crates/varve-testkit/src/bin/parse_corpus.rs`, `scripts/gql_diff/Main.java`, `scripts/gql_diff/compare.py`
- Modify: `.github/workflows/ci.yml` (`gql-differential` job)

**Interfaces:**
```rust
// parse_corpus.rs: for each CLI arg file: print "<path>\tACCEPT|REJECT" (parse_program result).
```
```
# CI job (ubuntu): actions/setup-java@v4 (temurin 17) + pip install antlr4-tools
antlr4 -Dlanguage=Java -o gen resources/gql-grammar/GQL.g4 && javac … Main.java
Main reads the same file list → "<path>\tACCEPT|REJECT"
compare.py: FAIL iff varve=ACCEPT && antlr=REJECT && path not *.ext.gql; print full diff table.
```
`Main.java` parses each file against the grammar's `gqlProgram` root rule with a bail-error listener (syntax error ⇒ REJECT). Note in `scripts/gql_diff/README.md`: statements are practical-core GQL; Varve rejecting valid GQL is expected and unreported.

- [x] **Steps:**
  1. `parse_corpus` bin + corpus files; local test `corpus_files_all_have_expected_verdicts` (a `corpus.expected` file pins each file's varve verdict so corpus rot is caught locally without Java). Commit.
  2. Java harness + compare.py + CI job; verify green in CI (or via local java if available — else mark the CI run as the verification step and confirm on push). Commit `test: ANTLR differential parser oracle`.

**Run:** `cargo run -p varve-testkit --bin parse_corpus -- resources/gql-corpus/*.gql` locally; job green on push.

### Task 19: Slice exit — demo, docs, STATUS, roadmap

**Files:**
- Create: `crates/varve/examples/gql_tour.rs` — the slice demo: CREATE GRAPH, USE, one multi-statement program (nodes + edges), SET/REMOVE, aggregation query, OPTIONAL MATCH, EXISTS, params, ORDER BY/LIMIT, UNION, ERASE with a system-time probe showing GDPR hiding; prints results.
- Modify: `docs/plans/STATUS.md`, `docs/plans/varve-v1-roadmap.md` (tick slice-7 boxes)

- [x] **Steps:**
  1. `gql_tour` example runs green: `cargo run --release --example gql_tour -p varve`. Commit `docs: gql_tour demo example`.
  2. Full verification: `cargo test --workspace`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo fmt --all --check`, `PROPTEST_CASES=1024 cargo test -p varve-testkit --release`, TCK gate green, traversal_bench rerun (record new batched ingest rate).
  3. STATUS.md update: slice-7 decision entries (mirror this plan's decision numbers), deviations, discharged fast-follows (`where_for_var`, `_labels`, `EngineError::Unsupported` dead variant — now alive again or removed, empty-prop-block, keyword-props, absent-prop-NULL, comma-multi-path, MATCH-INSERT hops, multi-edge INSERT throughput), NEW fast-follows found during execution, demo command, slice log row (test count, TCK pass rate). Tick roadmap boxes. Commit `docs: slice 7 complete — STATUS and roadmap`.
  4. **Never leave red tests at a session boundary** (session protocol) — if a session ends mid-slice, STATUS.md records the exact resume point instead.

---

## Slice exit checklist (roadmap exit criteria)

- [x] Curated core TCK list (`resources/tck/core.txt`) 100% green
- [x] Overall adapted TCK pass-rate ≥ 85% (report artifact in CI; exclusions all reasoned in `exclusions.toml`)
- [x] ANTLR differential oracle configured; local corpus/compare checks green (CI job validates on push)
- [x] Fuzzer: nightly CI job configured; 60s smoke green; 10-min manual run green (`cargo +nightly fuzz run parse -- -max_total_time=600 -rss_limit_mb=4096`, 19,788,044 execs, corpus 2448/666Kb, RSS 1216Mb)
- [x] All workspace tests green; clippy clean; fmt clean; property suites green at release cases
- [x] STATUS.md updated + roadmap slice-7 boxes ticked + demo command recorded (`cargo run --release --example gql_tour -p varve`)

Task 19 completed in Session D. Production deviation handled: cyclic `WALK`
queries now fail with `ResourcesExhausted` under shared per-batch row/frontier
budgets and a hop cap instead of materializing until OOM; DB-backed traversal
oracle release verification was optimized in Task 23 and reclosed in Task 25.

---

## Session G — Production hardening follow-ups

### Task 20: Configurable query budgets

**Goal:** traversal/path expansion budgets configurable, replacing hard-coded production constants while preserving current default behavior.

- [x] **Steps:** TDD added `QueryTuning` parse/env tests for `[query]` fields, plus an e2e cyclic quantified-path query proving `path_row_budget = 3` returns `ResourcesExhausted`. Implemented `[query] path_output_batch_rows`, `path_row_budget`, `path_frontier_budget`, `traversal_node_budget`, `traversal_adjacency_budget`; introduced `varve_plan::{QueryLimits, PathExpandLimits}`; propagated configured `PathExpandLimits` through `Db`, writer-side MATCH resolution, `execute_body_with_limits`, `PathExpandNode`, and `PathExpandExec`. `path_output_batch_rows` and traversal scan budgets are parsed/stored for Tasks 21–22.

**Run:** `cargo test -p varve-plan`; `cargo test -p varve --test traversal`; `cargo test -p varve-engine -- --test-threads=1`. (`QueryTuning` and its parse/env tests live in `varve-engine`; `varve-config` only provides the generic section decoder.)

### Task 21: Streaming `PathExpandExec`

- [x] Emit bounded output batches per input batch using `path_output_batch_rows`; preserve path variables across batch boundaries; keep low-budget cyclic tests non-materializing.

### Task 22: Budgeted adjacency scans

- [x] Apply `traversal_node_budget` and `traversal_adjacency_budget` to anchored reachable-edge BFS and unanchored adjacency materialization. TDD regressions cover anchored reachable-edge BFS node-budget exhaustion and unanchored adjacency materialization budget exhaustion; both return `ResourcesExhausted` through the existing planning error path.

### Task 23: Traversal oracle CPU reduction

- [x] Pre-index oracle adjacency and record release-case economics. `GraphOracle` keeps label/direction/node adjacency candidates while preserving `ReferenceStore::visible_at` for bitemporal correctness; DB-backed traversal oracle properties reuse one Tokio runtime and replay generated fixtures as ordered multi-statement programs. Release acceptance green: `PROPTEST_CASES=1024 cargo test -p varve-testkit --release --test traversal_oracle -- --test-threads=1` finished in 47.92s.

### Task 24: TCK side effects + path values

- [x] Add TCK path values plus side-effect assertions if not already covered by the decision-21 gate. `TckValue::Path` parses node/relationship path literals and reverse forms; TCK comparison accepts Varve `RETURN p` path columns as interleaved IID-list path shapes. Runner tests cover `no side effects`, `+nodes`, `+relationships`, `+labels`, `+properties`, and `MATCH p = ... RETURN p` path comparison. Verification: `cargo test -p varve-testkit --lib`, `cargo test -p varve-testkit --test tck_values`, `cargo test -p varve-testkit --test tck -- --test-threads=1`, `cargo test -p varve-testkit -- --test-threads=1`, `cargo clippy -p varve-testkit --all-targets -- -D warnings`, `cargo fmt --all --check`.

### Task 25: Fuzz confirmation + slice reclose

- [x] Re-run final traversal/TCK/fuzz verification and update STATUS/roadmap. Final verification green: `cargo fmt --all --check`; `cargo run --release --example gql_tour -p varve`; `cargo test --workspace -- --test-threads=1` (596 passed); `cargo clippy --workspace --all-targets -- -D warnings`; release traversal oracle with `PROPTEST_CASES=1024` (6 passed in 49.17s); TCK gate 445/511 adapted passed (0.870841); varve-testkit lib/TCK values/full suite green; parser fuzz 10-min run 13,903,093 execs with no crashes.

## Self-review record (writing-plans skill)

**Spec coverage (roadmap slice-7 bullets → tasks):** expression completion → T1–T4 (operators/3VL T1–2, CASE T1–2, EXISTS T9, params T4, function library/FunctionRegistry T3, CAST T1–2) · statement completion → T5–T8 (OPTIONAL MATCH T6, FILTER/LET/FOR T5–6, ORDER/SKIP/LIMIT/OFFSET + UNION [ALL] + RETURN DISTINCT + aggregation T8) · mutation completion → T10–T13 (SET/REMOVE T11, multi-statement tx T13, label ops T11, RMW via query engine T10–11) + ERASE T12 (STATUS slice-2 carry-in) · catalog → T14 · TCK harness → T16–T17 · ANTLR differential → T18 · parser fuzz → T15 · exit criteria → T19. STATUS carry-ins all mapped (see T19 step 3 list). Spec §8 label alternation → T7; §10.5 FunctionRegistry → T3; D2 TCK adaptation → T16–17.

**Known scope lines (not gaps):** the decision-21/non-goals list; each has a recorded reason and, where TCK-visible, an exclusion entry.

**Type consistency:** `LabelSpec` (AST) vs `LabelFilter` (index) are deliberately distinct types (owned parse shape vs borrowed filter); `QueryBody`/`ClauseSpecs`/`binding_rows`/`execute_body` names used consistently across T5/T6/T9/T10/T11; `MutateStmt{kind: MutKind}` consistently replaces `DeleteStmt` from T5 onward (T12 consumes it); `Overlay` introduced T13 and referenced by T10's `scan_inputs_for` signature — **execution note:** T10 lands `overlay: Option<&Overlay>` as a parameter defaulting to `None` call-sites; T13 populates it (the type is defined in T10 to keep signatures stable).
