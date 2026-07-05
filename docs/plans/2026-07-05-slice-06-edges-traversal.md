# Slice 6: Edges, Adjacency, Multi-Hop Traversal, Paths

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Varve becomes a real graph database: edge events with `_src_iid`/`_dst_iid`, `INSERT (a)-[:REL {props}]->(b)` (inline or MATCH-bound endpoints), edges flushed under three sort orders (`data/`, `adj-out/`, `adj-in/`), multi-element `MATCH` paths lowered to DataFusion hash joins, a `PathExpand` custom DataFusion operator for `{m,n}`/`*` quantified traversal with WALK semantics, `DETACH DELETE`, and an oracle-tested traversal property suite — all bitemporally correct.

**Architecture:** `Event` gains optional `src`/`dst` endpoints (spec §5.2 "edges additionally carry `_src_iid`/`_dst_iid`"); the event IPC codec gains two nullable `FixedSizeBinary(16)` columns. `LiveTable` maintains src-/dst-ordered adjacency views inline. The engine's `TableState` splits into `nodes` and `edges` cores; flush writes edge blocks under three sort orders whose `PageMeta` min/max record the *family sort key*, so the existing `PageMeta::selected` prune rule works verbatim for anchor-pruned adjacency lookups. `varve-plan` grows pattern lowering (per-element scans, name-mangled columns, left-deep hash joins with a size heuristic) and a `PathExpand` UserDefinedLogicalNode + ExecutionPlan registered through a custom `QueryPlanner`. The writer resolves `MATCH … INSERT` and `DETACH DELETE` with the same read-at-current-snapshot pattern slice 3 established for `MATCH … DELETE`.

**Tech Stack:** No new dependencies. datafusion 54 (verified: `DataFrame::{join, join_on, alias, select}`, `UserDefinedLogicalNodeCore`, `Extension`, `QueryPlanner`, `DefaultPhysicalPlanner::with_extension_planners`, `SessionStateBuilder::with_query_planner`, `SessionContext::new_with_state`, `PlanProperties::new(eq, partitioning, EmissionType, Boundedness)`, `RecordBatchStreamAdapter::new(schema, stream)`, `Distribution::SinglePartition`, hash support for `FixedSizeBinary` in `datafusion-common` `hash_utils.rs:990`) · arrow 58 (`FixedSizeBinaryBuilder::new(16)`, `ListBuilder`, `arrow::compute::take`) · prost 0.14 (manifest `family` field) · proptest 1.

## Global Constraints

- All roadmap Global Constraints apply: TDD (failing test first, minimal implementation, commit); `cargo clippy --workspace --all-targets -- -D warnings` clean; NO `unwrap()`/`expect()` in library code (tests OK per `clippy.toml`); errors via `thiserror` per crate; conventional commits, **no co-author trailers**; `cargo fmt --all` before every commit (slice-5 process note: fmt is part of every task gate, not just slice end).
- **Sovereignty (spec §1, D7):** this slice only ever calls `put`/`get`/`get_range`/`list`. Adjacency families are plain objects.
- **Bitemporal invariant (spec §5.2):** blocks store raw events; `_system_to` and effective valid ranges are never persisted. Adjacency families re-sort the SAME events — resolution still happens at scan time per edge entity.
- **Determinism:** `encode_block_by` sorts on total orders `(sort_key, iid)`; adjacency entries and expansion output are sorted by `(node, neighbor, edge)`; no wall-clock, no randomness, no map-iteration-order dependence (HashMap is used only inside `EdgeAdjacency` lookups whose output vecs are pre-sorted).
- **Dependency pinning:** datafusion 54.0.0 / arrow 58.3.0 (workspace pins unchanged). Every DataFusion/arrow API named in this plan was verified against the vendored 54.0.0/58.3.0 sources before writing. **The test code in this plan is the contract**: if an implementation sketch drifts from the pinned crates, adapt the implementation, not the test.
- Spec references: `docs/design/2026-07-04-varve-design.md` §5.1 (edge = `_id` + exactly one label + `_src`/`_dst` + props), §5.2 (edge events carry `_src_iid`/`_dst_iid`; two extra sort orders), §5.3 (IID derivation — edges use table `"edges"`), §8 (pattern syntax, quantifiers capped by `max_path_depth`), §9 (adj-out/adj-in layout: `tables/<table>/adj-out/{data,meta}/<trie-key>.arrow`), §10 (pattern lowering to hash joins on `_src_iid`/`_dst_iid`; `PathExpand` operator — breadth-wise, depth-capped, streams path bindings).
- We are in development: NO backward compatibility anywhere (AST shapes, `Event` struct, IPC schema, manifest proto, and test helpers change freely — Arrow IPC bytes are deliberately NOT golden-pinned, per slice-3 decision), but everything written is production code — no placeholders, no stubs.
- Slice depends on slice 4 only (works on local FS; slice 5's S3 backends are orthogonal and untouched).

## Design decisions (record in STATUS.md at slice end)

1. **`Event` gains `src: Option<Iid>, dst: Option<Iid>` fields** (spec §5.2 columns, not doc properties). ALL events of an edge — `Put`, `Delete`, `Erase` — carry endpoints, so every adjacency family co-locates an edge's full history under its src/dst sort key (endpoints are immutable per edge `_id` by construction: the writer stamps them from the resolved edge at DELETE/ERASE time). Node events carry `None`. The event IPC schema gains nullable `_src_iid`/`_dst_iid` `FixedSizeBinary(16)` columns between `_valid_to` and `op`.
2. **`LiveTable` maintains adjacency views inline** (`out`/`in_` `BTreeMap<Iid, BTreeSet<Iid>>` from event endpoints, empty for node tables) — the roadmap's "live table maintains src- and dst-ordered views". Views index every event (visibility is resolved at read time, same as the primary map).
3. **`TableState` = `{ nodes: TableCore, edges: TableCore, adj_out: Vec<PersistedTrie>, adj_in: Vec<PersistedTrie> }`** where `TableCore = { live, tries }`. Adjacency families are persisted-only alternates over the same edge events; the live side of adjacency is decision 2's views. Everything stays under the ONE `RwLock` (slice-4 decision 8 upheld).
4. **Adjacency families reuse the page-prune machinery verbatim:** `encode_block_by(live, page_rows, SortOrder::{ByIid,BySrc,ByDst})` sorts rows by `(sort_key asc, edge iid asc, system_from desc)` and records the FAMILY SORT KEY as `PageMeta.min_iid/max_iid` — so `PageMeta::selected(bounds, Some(anchor))` prunes adj-out pages by src anchor (and adj-in by dst) with zero new prune code. Keys: `adj_data_key`/`adj_meta_key(graph, table, family, trie_key)` → `v1/graphs/<g>/tables/<t>/<family>/{data,meta}/<trie>.arrow` (spec §9). `merge_sources` already groups by `event.iid` regardless of file interleaving, so per-edge resolution is unchanged.
5. **Manifest `TableTries` gains a `family` field** (prost `string` tag 4; `""` = primary, `"adj-out"`, `"adj-in"`). One flush = one block id = up to four `TableTries` entries (nodes primary, edges primary, edges adj-out, edges adj-in), ONE manifest PUT (still the atomic commit point). Old manifests decode with `family = ""` — but no migration support is promised (dev mode).
6. **Edge INSERT forms (roadmap: "binding previously matched or inline nodes"):** (a) inline endpoints `INSERT (:A {..})-[:R {..}]->(:B {..})`; (b) intra-statement variable reuse `INSERT (a:A {..}), (a)-[:R]->(b:B {..})` — a bound-variable reference must be a bare `(a)`; (c) MATCH-bound `MATCH (a:P {..}), (b:P {..}) [WHERE v.prop = lit] INSERT (a)-[:K]->(b)` where the match part is comma-separated SINGLE-NODE patterns (paths in the match part of a mutation arrive with slice 7's read-modify-write planning). INSERT executes once per binding row (cartesian product of per-variable matches). `statement_reads` returns true for match-part INSERTs (pre-flush of staged writes, like DELETE).
7. **Edge `_id`: user-supplied `_id` in the edge property map wins; else derived `varve:gen:{tx_id}:{ordinal}`** — the slice-3 durable generator, one ordinal counter across all nodes AND edges created by the statement. `_iid = xxh3_128(graph, "edges", id_bytes)`.
8. **DELETE per GQL:** plain `DELETE` on a node with ≥1 visible incident edge (at current time) fails the WHOLE tx with `EngineError::StillConnected(count)` — nothing is applied. `DETACH DELETE` emits the node `Delete` plus one edge `Delete` per distinct incident edge (self-loops deduped) in ONE tx (one log record, two `TableEffects`). Edge-variable DELETE (`MATCH ()-[r]->() DELETE r`) is NOT in this slice (slice 7 mutation completion).
9. **Pattern lowering scope:** ONE linear path chain per MATCH query (comma-separated multi-path queries → `PlanError::Unsupported`, slice 7). Column names are mangled `{var}__{column}` (e.g. `a___iid` for var `a`'s `_iid`) by rebuilding each element's `RecordBatch` schema (zero-copy). Label + inline-prop + WHERE predicates are applied per element BEFORE joins. Join order: left-deep chain built from whichever terminal node element has the smaller (pre-filter) snapshot row count — the roadmap's "simple size heuristics". Joins are `DataFrame::join(.., JoinType::Inner, ..)` on `FixedSizeBinary(16)` iid columns (hash support verified).
10. **`PathExpand` = real DataFusion extension:** `PathExpandNode` (`UserDefinedLogicalNodeCore`) + `PathExpandExec` (`ExecutionPlan`) planned via `VarveQueryPlanner` (`QueryPlanner` → `DefaultPhysicalPlanner::with_extension_planners`). GQL WALK semantics: repeated nodes/edges allowed, so termination comes from the depth cap alone. Quantifiers: `{n}` = `{n,n}`, `{m,n}`, `{m,}` (max capped to `max_path_depth`), `*` = `{0, max_path_depth}`; explicit bounds with `n > max_path_depth` or `m > n` are errors. Zero-length hops (m=0) bind end = start (no edge traversed; end-node predicates still apply via the terminal join). Expansion input requires `Distribution::SinglePartition`, emits `EmissionType::Incremental` (per input batch), `Boundedness::Bounded`. The adjacency (`EdgeAdjacency`, HashMap node→sorted `Vec<AdjEdge>`) is resolved AT THE QUERY'S TEMPORAL BOUNDS engine-side and carried in the logical node (excluded from its `Eq`/`Hash` by pointer identity — documented).
11. **Bitemporal traversal semantics (v1):** an edge is traversable iff `resolve(events, bounds)` yields ≥1 visible Put with the hop's label (dedup by edge iid — a valid-time RANGE query never duplicates a hop). Fixed single hops join the edge SNAPSHOT (one row per visible version — under point bounds, exactly one; range-query traversals with multiple versions per edge are a documented v1 semantic for fixed hops, revisited in slice 7). The oracle pins AS-OF-point behavior (roadmap exit criterion).
12. **Path variables (v1):** `p = (a)-[:K]->{1,3}(b)` binds `p` to a `List<FixedSizeBinary(16)>` of interleaved node/edge iids (`[n0, e1, n1, …]`, length `2h+1` for h hops). Allowed only when the path has exactly ONE hop and it is quantified (multi-hop path composition lands with slice 7's list functions); `RETURN p` is a new bare-variable return item. Edge variables on quantified edges are a parse error (GQL group variables — slice 7).
13. **Quantified-edge property maps filter EVERY traversed hop** (applied when building `EdgeAdjacency`); intermediate nodes of a quantified hop are unconstrained (GQL semantics — only endpoints carry patterns).
14. **Property-test economics:** the oracle suite has two layers — (a) a PURE layer (no Db, no tokio): random adjacency + `expand_paths` (the exec's core fn) vs the naive oracle walker, runs at full `PROPTEST_CASES`; (b) an e2e layer driving `Db` with GQL (random graphs ≤200 nodes incl. cycles/self-loops, all `{m,n}` with n ≤ 3, AS-OF probes), capped at `min(PROPTEST_CASES, 128)` cases because each case boots a Db — cap documented in-file.
15. **Unlabeled edge patterns are a v1 parse error** ("edge patterns require a label in v1") — an unlabeled edge would silently match nothing under the existing label-filter quirk (STATUS slice-1 note); explicit beats silent. Node patterns keep the existing quirk (unlabeled MATCH ⇒ empty). Node patterns may now carry MULTIPLE labels in the AST (`labels: Vec<String>`); MATCH scans still use at most one label in v1 (>1 ⇒ `PlanError::Unsupported`, slice 7 — INSERT uses all).
16. **Negative numeric literals** (`{x: -1}`) become parseable as a side effect of the new `Minus` token — handled in `literal()` (previously a lex error; pinned by test).

## File structure

```
crates/varve-types/                # (unchanged)
crates/varve-index/
  src/event.rs                     # Event gains src/dst (decision 1)
  src/codec.rs                     # event_schema + encode/decode: _src_iid/_dst_iid columns
  src/live.rs                      # adjacency views: out/in_ maps, out_edges()/in_edges() (decision 2)
  src/scan.rs                      # snapshot_entities emits _src_iid/_dst_iid columns for edges
  src/block.rs                     # SortOrder, encode_block_by (decision 4)
crates/varve-gql/
  src/token.rs                     # Minus/Lt/Gt/LBracket/RBracket/Star tokens; DETACH keyword
  src/ast.rs                       # NodePattern/EdgePattern/Quantifier/PathPattern/Direction/MatchPart;
                                   #   QueryStmt.paths; InsertStmt{match_part,paths}; DeleteStmt.detach;
                                   #   ReturnItem::Var
  src/parser.rs                    # element/path/quantifier parsing; MATCH…INSERT; DETACH DELETE
crates/varve-storage/
  src/keys.rs                      # adj_data_key/adj_meta_key (+ ADJ_OUT/ADJ_IN consts)
  src/manifest.rs                  # TableTries.family (tag 4)
crates/varve-plan/
  src/exec.rs                      # adapters for new AST; iids_from_snapshot gains props filter;
                                   #   PlanError::{Unsupported, UnknownVariable}
  src/pattern.rs                   # NEW: scan_specs, ScanSpec/SpecKind/ScanInput, mangling,
                                   #   execute_pattern (joins + heuristic + RETURN)
  src/expand.rs                    # NEW: EdgeAdjacency/AdjEdge, expand_paths, PathExpandNode,
                                   #   PathExpandExec, PathExpandPlanner, VarveQueryPlanner,
                                   #   session_context()
crates/varve-engine/
  src/state.rs                     # TableCore, TableState{nodes,edges,adj_out,adj_in}, EDGES_TABLE,
                                   #   TableKind
  src/writer.rs                    # Effects{nodes,edges}; resolve_insert paths+bindings (async);
                                   #   resolve_delete detach/connected-check; statement_reads
  src/scan.rs                      # merged_snapshot(table kind); edge_adjacency (anchor-pruned)
  src/flush.rs                     # flush both tables + 3 edge families, one manifest
  src/db.rs                        # recovery routes tables/families; query() → pattern path;
                                   #   [query] max_path_depth config; EngineError new variants
crates/varve/
  tests/traversal.rs               # NEW: e2e joins, quantified hops, bitemporal traversal
  tests/detach_delete.rs           # NEW: DETACH DELETE + still-connected DELETE e2e
  examples/traversal_bench.rs      # NEW: fixture ingest → flush → cold/warm 2-hop + {1,3} timings
crates/varve-testkit/
  src/oracle.rs                    # NEW: GraphOracle (naive walker), arb_graph strategies
  src/fixture.rs                   # NEW: deterministic social_graph (LCG)
  src/lib.rs                       # pub mod oracle; pub mod fixture
  tests/traversal_oracle.rs        # NEW: pure + e2e property suites (decision 14)
  tests/social_graph.rs            # NEW: 10k/60k fixture integration test
```

Task order: T1 event+codec → T2 live views+snapshot → T3 GQL MATCH patterns → T4 GQL INSERT/DELETE forms → T5 engine edges write path+flush(primary)+recovery → T6 adjacency families → T7 DETACH DELETE → T8 pattern-join lowering → T9 PathExpand → T10 oracle properties → T11 fixture+bench → T12 slice exit.

---
### Task 1: `Event` endpoints + event-IPC codec columns

**Files:**
- Modify: `crates/varve-index/src/event.rs`
- Modify: `crates/varve-index/src/codec.rs`
- Modify (mechanical, compiler-driven): every `Event { … }` struct literal in the workspace — `crates/varve-engine/src/writer.rs` (`resolve_insert`, `resolve_delete`), `crates/varve-testkit/src/strategy.rs` (`arb_history`), and the `#[cfg(test)]` modules of `crates/varve-index/src/{live,scan,block,codec,bitemporal}.rs`, `crates/varve-engine/src/{db,scan,flush}.rs` test modules, `crates/varve-testkit/tests/{equivalence,flush_equivalence}.rs` — each gains `src: None, dst: None`.

**Interfaces:**
- Consumes: `varve_types::Iid` (`derive`, `as_bytes`, `from_bytes`), existing `Event`/`Op`.
- Produces: `Event { iid, system_from, valid_from, valid_to, src: Option<Iid>, dst: Option<Iid>, op }`; event IPC schema `[_iid FSB(16) nn, _system_from ts nn, _valid_from ts nn, _valid_to ts nn, _src_iid FSB(16) NULLABLE, _dst_iid FSB(16) NULLABLE, op u8 nn, payload Binary nullable]`. `encode_events`/`decode_events` signatures unchanged.

- [ ] **Step 1: Write the failing tests** — append to the `tests` module of `crates/varve-index/src/codec.rs`:

```rust
fn edge_event(n: u8) -> Event {
    Event {
        iid: Iid::derive("g", "edges", &[n]),
        system_from: Instant::from_micros(10),
        valid_from: Instant::from_micros(10),
        valid_to: Instant::END_OF_TIME,
        src: Some(Iid::derive("g", "nodes", &[1])),
        dst: Some(Iid::derive("g", "nodes", &[2])),
        op: Op::Put {
            labels: vec!["KNOWS".into()],
            doc: BTreeMap::from([("_id".into(), Value::Int(n as i64))]),
        },
    }
}

#[test]
fn edge_events_round_trip_with_endpoints() {
    let events = vec![
        edge_event(1),
        Event {
            op: Op::Delete,
            ..edge_event(1)
        },
    ];
    let bytes = encode_events(&events).unwrap();
    assert_eq!(decode_events(&bytes).unwrap(), events);
}

#[test]
fn node_events_round_trip_with_null_endpoints() {
    let events = vec![Event {
        src: None,
        dst: None,
        ..edge_event(3)
    }];
    let bytes = encode_events(&events).unwrap();
    let decoded = decode_events(&bytes).unwrap();
    assert_eq!(decoded[0].src, None);
    assert_eq!(decoded[0].dst, None);
}
```

(If the existing test module constructs events through a helper, reuse it; the `..edge_event(1)` update syntax requires `Event: Clone`, already derived.)

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p varve-index codec`
Expected: FAIL — `Event` has no field `src` (compile error; that IS the red state for a struct change).

- [ ] **Step 3: Implement**

`crates/varve-index/src/event.rs` — replace the `Event` struct:

```rust
/// Every mutation becomes an immutable event; `_system_to` and effective
/// valid ranges are never stored — always derived at read time (spec §5.2).
/// `Op::Erase` events carry `valid_from: Instant::MIN, valid_to:
/// Instant::END_OF_TIME` by convention (an erase removes the whole entity).
///
/// `src`/`dst` are the edge endpoints (spec §5.2): `Some` on EVERY event of
/// an edges table — including Delete/Erase, so adjacency families co-locate
/// an edge's full history under its endpoint sort keys — and `None` on node
/// events. Endpoints are immutable per edge `_id`.
#[derive(Debug, Clone, PartialEq)]
pub struct Event {
    pub iid: Iid,
    pub system_from: Instant,
    pub valid_from: Instant,
    pub valid_to: Instant,
    pub src: Option<Iid>,
    pub dst: Option<Iid>,
    pub op: Op,
}
```

`crates/varve-index/src/codec.rs` — extend `event_schema()`:

```rust
fn event_schema() -> Arc<Schema> {
    let ts = || DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    Arc::new(Schema::new(vec![
        Field::new("_iid", DataType::FixedSizeBinary(16), false),
        Field::new("_system_from", ts(), false),
        Field::new("_valid_from", ts(), false),
        Field::new("_valid_to", ts(), false),
        Field::new("_src_iid", DataType::FixedSizeBinary(16), true),
        Field::new("_dst_iid", DataType::FixedSizeBinary(16), true),
        Field::new("op", DataType::UInt8, false),
        Field::new("payload", DataType::Binary, true),
    ]))
}
```

In `encode_events`, alongside the existing builders add:

```rust
let mut src_b = FixedSizeBinaryBuilder::new(16);
let mut dst_b = FixedSizeBinaryBuilder::new(16);
```

and inside the per-event loop:

```rust
match &event.src {
    Some(iid) => src_b.append_value(iid.as_bytes()).map_err(|e| IndexError::Codec(e.to_string()))?,
    None => src_b.append_null(),
}
match &event.dst {
    Some(iid) => dst_b.append_value(iid.as_bytes()).map_err(|e| IndexError::Codec(e.to_string()))?,
    None => dst_b.append_null(),
}
```

and add `Arc::new(src_b.finish()), Arc::new(dst_b.finish())` to the column vec at positions 4 and 5 (matching the schema). In `decode_events`, downcast columns 4/5 as `FixedSizeBinaryArray` and per row:

```rust
let src = if src_col.is_null(row) {
    None
} else {
    let mut b = [0u8; 16];
    b.copy_from_slice(src_col.value(row));
    Some(Iid::from_bytes(b))
};
```

(same for `dst`; shift the `op`/`payload` column indices to 6/7). Populate `Event { src, dst, .. }`. Import `FixedSizeBinaryBuilder` from `arrow::array`.

- [ ] **Step 4: Mechanical field addition across the workspace**

Run `cargo check --workspace 2>&1 | grep "missing.*src" ` and add `src: None, dst: None,` to every flagged `Event { … }` literal (files listed in **Files** above). In `crates/varve-testkit/src/strategy.rs::arb_history` node events get `src: None, dst: None` (edge strategies arrive in Task 10). No behavioral change anywhere.

- [ ] **Step 5: Run the full suite**

Run: `cargo test --workspace`
Expected: PASS (all ~293 tests; the two new codec tests green).

- [ ] **Step 6: Commit**

```bash
cargo fmt --all
git add -A
git commit -m "feat: edge endpoints on Event + _src_iid/_dst_iid event-IPC columns"
```

---

### Task 2: `LiveTable` adjacency views + edge columns in the scan snapshot

**Files:**
- Modify: `crates/varve-index/src/live.rs`
- Modify: `crates/varve-index/src/scan.rs`

**Interfaces:**
- Consumes: Task 1's `Event.src`/`Event.dst`.
- Produces:
  `LiveTable::out_edges(&self, src: &Iid) -> impl Iterator<Item = &Iid> + '_` (edge iids whose src == arg, ascending);
  `LiveTable::in_edges(&self, dst: &Iid) -> impl Iterator<Item = &Iid> + '_`;
  `snapshot_entities` unchanged signature, but when every visible row's event carries endpoints the batch gains `_src_iid`/`_dst_iid` `FixedSizeBinary(16)` non-null columns positioned after `_valid_to`, before property columns. Mixed Some/None endpoints across visible rows → `IndexError::Codec("mixed node and edge events in one snapshot")`.

- [ ] **Step 1: Write the failing tests** — append to `crates/varve-index/src/live.rs` tests:

```rust
fn edge(n: u8, src: u8, dst: u8, at: i64) -> Event {
    Event {
        iid: Iid::derive("g", "edges", &[n]),
        system_from: Instant::from_micros(at),
        valid_from: Instant::from_micros(at),
        valid_to: Instant::END_OF_TIME,
        src: Some(Iid::derive("g", "nodes", &[src])),
        dst: Some(Iid::derive("g", "nodes", &[dst])),
        op: Op::Put {
            labels: vec!["KNOWS".into()],
            doc: BTreeMap::new(),
        },
    }
}

#[test]
fn adjacency_views_track_endpoints() {
    let mut live = LiveTable::new();
    live.append(edge(1, 10, 20, 1)).unwrap();
    live.append(edge(2, 10, 30, 2)).unwrap();
    live.append(edge(3, 20, 10, 3)).unwrap();
    let n10 = Iid::derive("g", "nodes", &[10]);
    let n20 = Iid::derive("g", "nodes", &[20]);
    let out10: Vec<_> = live.out_edges(&n10).cloned().collect();
    assert_eq!(out10, {
        let mut v = vec![Iid::derive("g", "edges", &[1]), Iid::derive("g", "edges", &[2])];
        v.sort();
        v
    });
    let in10: Vec<_> = live.in_edges(&n10).cloned().collect();
    assert_eq!(in10, vec![Iid::derive("g", "edges", &[3])]);
    assert_eq!(live.out_edges(&n20).count(), 1);
    // A delete event still indexes (visibility is resolved at read time).
    live.append(Event { op: Op::Delete, ..edge(1, 10, 20, 4) }).unwrap();
    assert_eq!(live.out_edges(&n10).count(), 2);
}

#[test]
fn node_appends_leave_views_empty() {
    let mut live = LiveTable::new();
    live.append(Event { src: None, dst: None, ..edge(1, 0, 0, 1) }).unwrap();
    assert_eq!(live.out_edges(&Iid::derive("g", "nodes", &[0])).count(), 0);
}
```

And to `crates/varve-index/src/scan.rs` tests:

```rust
#[test]
fn edge_snapshot_carries_endpoint_columns() {
    // Inline edge helper (same shape as live.rs's test helper).
    let e = Event {
        iid: Iid::derive("g", "edges", &[1]),
        system_from: Instant::from_micros(1),
        valid_from: Instant::from_micros(1),
        valid_to: Instant::END_OF_TIME,
        src: Some(Iid::derive("g", "nodes", &[10])),
        dst: Some(Iid::derive("g", "nodes", &[20])),
        op: Op::Put { labels: vec!["KNOWS".into()], doc: BTreeMap::new() },
    };
    let batch = snapshot_entities(
        [(e.iid, std::slice::from_ref(&e))],
        "KNOWS",
        &TemporalBounds {
            valid: TemporalDimension::at(Instant::from_micros(5)),
            system: TemporalDimension::at(Instant::from_micros(5)),
        },
    )
    .unwrap()
    .unwrap();
    let schema = batch.schema();
    let src_idx = schema.column_with_name("_src_iid").unwrap().0;
    let dst_idx = schema.column_with_name("_dst_iid").unwrap().0;
    let src = batch
        .column(src_idx)
        .as_any()
        .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
        .unwrap();
    assert_eq!(src.value(0), e.src.unwrap().as_bytes());
    assert!(dst_idx > src_idx);
}
```

(Inline an `edge()` helper in the scan tests mirroring the live.rs one — the plan's test code is the contract; helper duplication across test modules is fine.)

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p varve-index live adjacency && cargo test -p varve-index scan`
Expected: FAIL — no method `out_edges`.

- [ ] **Step 3: Implement `LiveTable` views**

`crates/varve-index/src/live.rs`:

```rust
#[derive(Default)]
pub struct LiveTable {
    events: BTreeMap<Iid, Vec<Event>>,
    /// Src-ordered adjacency view: src node iid → edge iids (decision 2).
    out: BTreeMap<Iid, BTreeSet<Iid>>,
    /// Dst-ordered adjacency view: dst node iid → edge iids.
    in_: BTreeMap<Iid, BTreeSet<Iid>>,
    last_system_from: Option<Instant>,
    event_count: usize,
}
```

In `append`, after the ordering guard and before the final insert:

```rust
if let (Some(src), Some(dst)) = (event.src, event.dst) {
    self.out.entry(src).or_default().insert(event.iid);
    self.in_.entry(dst).or_default().insert(event.iid);
}
```

New accessors:

```rust
/// Edge iids whose `src` is the given node, ascending. Empty for node tables.
pub fn out_edges(&self, src: &Iid) -> impl Iterator<Item = &Iid> + '_ {
    self.out.get(src).into_iter().flatten()
}

/// Edge iids whose `dst` is the given node, ascending. Empty for node tables.
pub fn in_edges(&self, dst: &Iid) -> impl Iterator<Item = &Iid> + '_ {
    self.in_.get(dst).into_iter().flatten()
}
```

(`BTreeSet` import joins the existing `BTreeMap` use.)

- [ ] **Step 4: Implement snapshot endpoint columns**

`crates/varve-index/src/scan.rs` — `VisibleRow` gains `src: Option<Iid>, dst: Option<Iid>` (copied from `version.event`). After collecting rows, determine edge-ness:

```rust
let with_endpoints = rows.iter().filter(|r| r.src.is_some()).count();
if with_endpoints != 0 && with_endpoints != rows.len() {
    return Err(IndexError::Codec(
        "mixed node and edge events in one snapshot".into(),
    ));
}
let is_edges = with_endpoints == rows.len() && !rows.is_empty();
```

When `is_edges`, append two `FixedSizeBinary(16)` non-null fields `_src_iid`/`_dst_iid` (built with `FixedSizeBinaryBuilder`, `append_value(iid.as_bytes())`) immediately after the `_valid_to` column and before the property columns. `dst` presence follows `src` (both stamped together by the writer); return `IndexError::Codec("edge event missing dst endpoint")` if `dst.is_none()` on an edge row.

- [ ] **Step 5: Run tests**

Run: `cargo test -p varve-index && cargo test --workspace`
Expected: PASS (node snapshots byte-identical to before — no endpoint columns added).

- [ ] **Step 6: Commit**

```bash
cargo fmt --all
git add -A
git commit -m "feat: LiveTable src/dst adjacency views + endpoint columns in edge snapshots"
```

---
### Task 3: GQL — pattern tokens, AST reshape, MATCH path parsing (+ green-build adapters)

The AST reshape ripples into `varve-plan` and `varve-engine`; this task includes the MINIMAL adapters that keep every existing test green (full multi-element behavior arrives in T5–T9). Existing single-node GQL strings must parse to equivalent semantics.

**Files:**
- Modify: `crates/varve-gql/src/token.rs`
- Modify: `crates/varve-gql/src/ast.rs`
- Modify: `crates/varve-gql/src/parser.rs`
- Modify (adapters): `crates/varve-plan/src/exec.rs`, `crates/varve-engine/src/writer.rs`, `crates/varve-engine/src/db.rs`

**Interfaces:**
- Produces (ast.rs, replacing `NodePattern`/`InsertNode`/`InsertStmt`/`QueryStmt`/`DeleteStmt`):

```rust
#[derive(Debug, Clone, PartialEq)]
pub struct NodePattern {
    pub var: Option<String>,
    pub labels: Vec<String>,
    pub props: Vec<(String, Literal)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Out, // (a)-[..]->(b)
    In,  // (a)<-[..]-(b)
}

/// `{n}` ⇒ min=n, max=Some(n); `{m,n}` ⇒ (m, Some(n)); `{m,}` ⇒ (m, None);
/// `*` ⇒ (0, None). `None` max is capped to `max_path_depth` at lowering.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Quantifier {
    pub min: u32,
    pub max: Option<u32>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct EdgePattern {
    pub var: Option<String>,
    pub label: String, // required in v1 (decision 15)
    pub props: Vec<(String, Literal)>,
    pub direction: Direction,
    pub quantifier: Option<Quantifier>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PathPattern {
    /// `p = (a)-[:K]->(b)` path variable.
    pub var: Option<String>,
    pub start: NodePattern,
    pub hops: Vec<(EdgePattern, NodePattern)>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MatchPart {
    /// v1: single-node patterns only (paths in mutation reads: slice 7).
    pub patterns: Vec<NodePattern>,
    pub where_clause: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct InsertStmt {
    pub match_part: Option<MatchPart>,
    pub paths: Vec<PathPattern>,
    pub valid_from: Option<Instant>,
    pub valid_to: Option<Instant>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DeleteStmt {
    pub pattern: NodePattern,
    pub where_clause: Option<Expr>,
    pub target: String,
    pub detach: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueryStmt {
    pub temporal: TemporalClauses,
    pub paths: Vec<PathPattern>,
    pub match_temporal: TemporalClauses,
    pub where_clause: Option<Expr>,
    pub return_items: Vec<ReturnItem>,
}

impl QueryStmt {
    /// v1 helper: the single node of a hop-free, single-path, unnamed MATCH.
    pub fn single_node(&self) -> Option<&NodePattern> {
        match self.paths.as_slice() {
            [p] if p.hops.is_empty() && p.var.is_none() => Some(&p.start),
            _ => None,
        }
    }
}
```

  `ReturnItem` gains `Var { var: String, alias: Option<String> }` (bare `RETURN p` for path variables; validated at lowering). `InsertNode` is DELETED.
- Produces (token.rs): `TokenKind::{Minus, Lt, Gt, LBracket, RBracket, Star}`; `Keyword::Detach` (reserved word `DETACH`).
- Produces (parser.rs internals): `fn node_pattern(&mut self) -> Result<NodePattern, GqlError>`, `fn props_block(&mut self) -> Result<Vec<(String, Literal)>, GqlError>`, `fn edge_pattern(&mut self) -> Result<EdgePattern, GqlError>`, `fn quantifier(&mut self) -> Result<Option<Quantifier>, GqlError>`, `fn path_pattern(&mut self) -> Result<PathPattern, GqlError>`, `fn peek_at(&self, n: usize) -> &TokenKind`.
- Adapter contracts for later tasks: `varve_plan` fns that took `&NodePattern` now read `pattern.labels.first()`; engine paths that only handle single nodes call `q.single_node()` and return `EngineError::Unsupported(String)` otherwise (variant added here).

- [ ] **Step 1: Failing lexer tests** — append to `crates/varve-gql/src/token.rs` tests:

```rust
#[test]
fn tokenizes_edge_pattern_punctuation() {
    assert_eq!(
        kinds("-[ ]-> <-[ ]- * {1,3}"),
        vec![
            TokenKind::Minus, TokenKind::LBracket, TokenKind::RBracket,
            TokenKind::Minus, TokenKind::Gt,
            TokenKind::Lt, TokenKind::Minus, TokenKind::LBracket,
            TokenKind::RBracket, TokenKind::Minus,
            TokenKind::Star,
            TokenKind::LBrace, TokenKind::Int(1), TokenKind::Comma,
            TokenKind::Int(3), TokenKind::RBrace,
            TokenKind::Eof,
        ]
    );
}

#[test]
fn detach_is_a_keyword() {
    assert_eq!(
        kinds("detach DELETE"),
        vec![TokenKind::Kw(Keyword::Detach), TokenKind::Kw(Keyword::Delete), TokenKind::Eof]
    );
}
```

- [ ] **Step 2: Run** `cargo test -p varve-gql token` — Expected: FAIL (no `Minus` variant → compile error).

- [ ] **Step 3: Lexer implementation** — in `TokenKind` add `Minus, Lt, Gt, LBracket, RBracket, Star`; in `Keyword` add `Detach` (+ `"DETACH" => Some(Keyword::Detach)` in `keyword()`); in `tokenize`'s match add single-char arms (same shape as the existing `Colon` arm) for `'-' → Minus`, `'<' → Lt`, `'>' → Gt`, `'[' → LBracket`, `']' → RBracket`, `'*' → Star`. Run Step 2 again — PASS.

- [ ] **Step 4: Failing parser tests** — REPLACE the existing `parses_match_where_return` / `parses_match_delete` expectations with the new AST shapes and ADD path tests. In `crates/varve-gql/src/parser.rs` tests:

```rust
fn node(var: Option<&str>, labels: &[&str]) -> NodePattern {
    NodePattern {
        var: var.map(String::from),
        labels: labels.iter().map(|s| s.to_string()).collect(),
        props: vec![],
    }
}

#[test]
fn parses_single_node_match_as_one_path() {
    let q = query("MATCH (p:Person) WHERE p.name = 'Ada' RETURN p.name");
    assert_eq!(q.paths.len(), 1);
    assert_eq!(q.paths[0].start, node(Some("p"), &["Person"]));
    assert!(q.paths[0].hops.is_empty());
    assert_eq!(q.single_node(), Some(&q.paths[0].start));
}

#[test]
fn parses_two_hop_path() {
    let q = query("MATCH (a:Person)-[:KNOWS]->(b)-[k:KNOWS]->(c:Person) RETURN c.name");
    let p = &q.paths[0];
    assert_eq!(p.start, node(Some("a"), &["Person"]));
    assert_eq!(p.hops.len(), 2);
    let (e0, n1) = &p.hops[0];
    assert_eq!(
        *e0,
        EdgePattern {
            var: None,
            label: "KNOWS".into(),
            props: vec![],
            direction: Direction::Out,
            quantifier: None,
        }
    );
    assert_eq!(*n1, node(Some("b"), &[]));
    let (e1, n2) = &p.hops[1];
    assert_eq!(e1.var.as_deref(), Some("k"));
    assert_eq!(*n2, node(Some("c"), &["Person"]));
}

#[test]
fn parses_reverse_direction_and_props() {
    let q = query("MATCH (a)<-[:KNOWS {since: 2020}]-(b) RETURN a.name");
    let (e, _) = &q.paths[0].hops[0];
    assert_eq!(e.direction, Direction::In);
    assert_eq!(e.props, vec![("since".into(), Literal::Int(2020))]);
}

#[test]
fn parses_quantifiers() {
    let q = query("MATCH (a)-[:KNOWS]->{1,3}(b) RETURN b.name");
    assert_eq!(q.paths[0].hops[0].0.quantifier, Some(Quantifier { min: 1, max: Some(3) }));
    let q = query("MATCH (a)-[:KNOWS]->{2}(b) RETURN b.name");
    assert_eq!(q.paths[0].hops[0].0.quantifier, Some(Quantifier { min: 2, max: Some(2) }));
    let q = query("MATCH (a)-[:KNOWS]->{2,}(b) RETURN b.name");
    assert_eq!(q.paths[0].hops[0].0.quantifier, Some(Quantifier { min: 2, max: None }));
    let q = query("MATCH (a)-[:KNOWS]->*(b) RETURN b.name");
    assert_eq!(q.paths[0].hops[0].0.quantifier, Some(Quantifier { min: 0, max: None }));
}

#[test]
fn parses_path_variable_and_bare_return() {
    let q = query("MATCH p = (a)-[:KNOWS]->{1,3}(b) RETURN p");
    assert_eq!(q.paths[0].var.as_deref(), Some("p"));
    assert_eq!(
        q.return_items,
        vec![ReturnItem::Var { var: "p".into(), alias: None }]
    );
}

#[test]
fn parses_node_props_and_multi_labels_in_match() {
    let q = query("MATCH (a:Person:Admin {name: 'Ada', age: -1}) RETURN a.name");
    let n = &q.paths[0].start;
    assert_eq!(n.labels, vec!["Person".to_string(), "Admin".to_string()]);
    assert_eq!(
        n.props,
        vec![
            ("name".into(), Literal::Str("Ada".into())),
            ("age".into(), Literal::Int(-1)),
        ]
    );
}

#[test]
fn parses_comma_separated_paths() {
    let q = query("MATCH (a:Person), (b:Person) RETURN a.name");
    assert_eq!(q.paths.len(), 2);
}

#[test]
fn edge_without_label_errors() {
    let err = parse("MATCH (a)-[]->(b) RETURN a.name").unwrap_err();
    assert!(err.to_string().contains("label"));
}

#[test]
fn edge_var_on_quantified_edge_errors() {
    let err = parse("MATCH (a)-[k:KNOWS]->{1,3}(b) RETURN a.name").unwrap_err();
    assert!(err.to_string().contains("quantified"));
}

#[test]
fn quantifier_min_above_max_errors() {
    let err = parse("MATCH (a)-[:KNOWS]->{3,1}(b) RETURN a.name").unwrap_err();
    assert!(err.to_string().contains("quantifier"));
}

#[test]
fn parses_match_delete_with_new_shapes() {
    let stmt = parse("MATCH (p:Person) WHERE p.name = 'Ada' DELETE p").unwrap();
    let Statement::Delete(del) = stmt else { panic!("expected delete") };
    assert_eq!(del.pattern, node(Some("p"), &["Person"]));
    assert!(!del.detach);
}

#[test]
fn delete_rejects_paths_with_hops() {
    let err = parse("MATCH (a)-[:KNOWS]->(b) DELETE a").unwrap_err();
    assert!(err.to_string().contains("DELETE"));
}
```

Update the other existing tests mechanically (`NodePattern { var: "p".into(), label: Some(..) }` → the `node()` helper; `parses_insert_node` is reworked in Task 4 — for THIS task keep INSERT parsing output equivalent by mapping the old body onto `paths: vec![PathPattern { var: None, start: NodePattern {..}, hops: vec![] }]` with `match_part: None`).

- [ ] **Step 5: Run** `cargo test -p varve-gql` — Expected: FAIL (AST fields don't exist).

- [ ] **Step 6: Parser implementation**

ast.rs: apply the **Interfaces** block verbatim (plus `ReturnItem::Var`). parser.rs:

```rust
fn peek_at(&self, n: usize) -> &TokenKind {
    self.tokens
        .get(self.pos + n)
        .map(|t| &t.kind)
        .unwrap_or(&TokenKind::Eof)
}

/// '(' [var] (':' label)* [props] ')'
fn node_pattern(&mut self) -> Result<NodePattern, GqlError> {
    self.expect(&TokenKind::LParen, "'('")?;
    let var = if matches!(self.peek(), TokenKind::Ident(_)) {
        Some(self.ident("pattern variable")?)
    } else {
        None
    };
    let mut labels = Vec::new();
    while *self.peek() == TokenKind::Colon {
        self.pos += 1;
        labels.push(self.ident("label name")?);
    }
    let props = if *self.peek() == TokenKind::LBrace {
        self.props_block()?
    } else {
        Vec::new()
    };
    self.expect(&TokenKind::RParen, "')'")?;
    Ok(NodePattern { var, labels, props })
}

/// '{' [ident ':' literal (',' ident ':' literal)*] '}'
fn props_block(&mut self) -> Result<Vec<(String, Literal)>, GqlError> {
    self.expect(&TokenKind::LBrace, "'{'")?;
    let mut props = Vec::new();
    if *self.peek() != TokenKind::RBrace {
        loop {
            let key = self.ident("property name")?;
            self.expect(&TokenKind::Colon, "':'")?;
            props.push((key, self.literal()?));
            if *self.peek() == TokenKind::Comma {
                self.pos += 1;
            } else {
                break;
            }
        }
    }
    self.expect(&TokenKind::RBrace, "'}'")?;
    Ok(props)
}

/// '-[' body ']->' | '<-[' body ']-'  with optional postfix quantifier;
/// body = [var] ':' label [props]. Label is REQUIRED in v1 (decision 15).
fn edge_pattern(&mut self) -> Result<EdgePattern, GqlError> {
    let offset = self.offset();
    let direction = match self.peek() {
        TokenKind::Minus => Direction::Out,
        TokenKind::Lt => Direction::In,
        _ => return Err(self.err("expected '-[' or '<-[' edge pattern")),
    };
    if direction == Direction::In {
        self.expect(&TokenKind::Lt, "'<'")?;
    }
    self.expect(&TokenKind::Minus, "'-'")?;
    self.expect(&TokenKind::LBracket, "'['")?;
    let var = if matches!(self.peek(), TokenKind::Ident(_)) {
        Some(self.ident("edge variable")?)
    } else {
        None
    };
    if *self.peek() != TokenKind::Colon {
        return Err(GqlError::Parse {
            offset,
            msg: "edge patterns require a label in v1: -[:LABEL]->".into(),
        });
    }
    self.pos += 1;
    let label = self.ident("edge label")?;
    let props = if *self.peek() == TokenKind::LBrace {
        self.props_block()?
    } else {
        Vec::new()
    };
    self.expect(&TokenKind::RBracket, "']'")?;
    self.expect(&TokenKind::Minus, "'-'")?;
    if direction == Direction::Out {
        self.expect(&TokenKind::Gt, "'>'")?;
    }
    let quantifier = self.quantifier()?;
    if quantifier.is_some() && var.is_some() {
        return Err(GqlError::Parse {
            offset,
            msg: "edge variables on quantified edges (group variables) land in slice 7"
                .into(),
        });
    }
    Ok(EdgePattern { var, label, props, direction, quantifier })
}

/// Postfix '{n}' | '{m,n}' | '{m,}' | '*' — or nothing.
fn quantifier(&mut self) -> Result<Option<Quantifier>, GqlError> {
    match self.peek() {
        TokenKind::Star => {
            self.pos += 1;
            Ok(Some(Quantifier { min: 0, max: None }))
        }
        TokenKind::LBrace => {
            let offset = self.offset();
            self.pos += 1;
            let min = self.quantifier_bound()?;
            let quant = if *self.peek() == TokenKind::Comma {
                self.pos += 1;
                if *self.peek() == TokenKind::RBrace {
                    Quantifier { min, max: None }
                } else {
                    let max = self.quantifier_bound()?;
                    if max < min {
                        return Err(GqlError::Parse {
                            offset,
                            msg: format!("quantifier min {min} exceeds max {max}"),
                        });
                    }
                    Quantifier { min, max: Some(max) }
                }
            } else {
                Quantifier { min, max: Some(min) }
            };
            self.expect(&TokenKind::RBrace, "'}'")?;
            Ok(Some(quant))
        }
        _ => Ok(None),
    }
}

fn quantifier_bound(&mut self) -> Result<u32, GqlError> {
    let offset = self.offset();
    match self.bump() {
        TokenKind::Int(n) if (0..=u32::MAX as i64).contains(&n) => Ok(n as u32),
        other => Err(GqlError::Parse {
            offset,
            msg: format!("expected quantifier bound, found {other:?}"),
        }),
    }
}

/// [pvar '='] node (edge node)*
fn path_pattern(&mut self) -> Result<PathPattern, GqlError> {
    let var = if matches!(self.peek(), TokenKind::Ident(_)) && *self.peek_at(1) == TokenKind::Eq {
        let v = self.ident("path variable")?;
        self.pos += 1; // '='
        Some(v)
    } else {
        None
    };
    let start = self.node_pattern()?;
    let mut hops = Vec::new();
    while matches!(self.peek(), TokenKind::Minus | TokenKind::Lt) {
        let edge = self.edge_pattern()?;
        let node = self.node_pattern()?;
        hops.push((edge, node));
    }
    Ok(PathPattern { var, start, hops })
}
```

`literal()` gains a leading-`Minus` arm (decision 16):

```rust
TokenKind::Minus => match self.bump() {
    TokenKind::Int(i) => Ok(Literal::Int(-i)),
    TokenKind::Float(f) => Ok(Literal::Float(-f)),
    other => Err(GqlError::Parse {
        offset,
        msg: format!("expected number after '-', found {other:?}"),
    }),
},
```

`match_tail` rework: parse `paths` = comma-separated `path_pattern()`s, then per-match `for_clauses`, then optional `WHERE prop_eq_expr`, then branch:
- `RETURN` → `return_item` list; `ReturnItem::Var` produced when an item is a bare `Ident` NOT followed by `Dot` or `LParen` (temporal fns keep their existing `Ident '(' var ')'` shape); build `QueryStmt { temporal, paths, match_temporal, where_clause, return_items }`.
- `DETACH` → expect `DELETE` next, set `detach = true`, then the shared delete tail; `DELETE` → `detach = false`. Delete tail: temporal clauses still rejected; require `paths.len() == 1 && paths[0].hops.is_empty() && paths[0].var.is_none()` else `GqlError::Parse { msg: "DELETE supports a single node pattern in v1 (edge deletion lands in slice 7)" }`; target must equal the pattern var (which must be `Some`); build `DeleteStmt { pattern: paths.remove(0).start, where_clause, target, detach }`.
- `INSERT` → handled in Task 4 (`MATCH … INSERT`); until Task 4, fall through to the existing "expected RETURN or DELETE" error message (extended to mention DETACH).

`insert_stmt` (Task-3 interim): parse comma-separated `path_pattern()`s; require every path to be hop-free with no path var for now? NO — parse full paths (grammar is ready), but keep `InsertStmt { match_part: None, paths, valid_from, valid_to }`. The old `insert_node` function is DELETED; `INSERT (:Person {..})` yields a one-node path. Old error message expectation `"label"` for `INSERT ()`: node_pattern now ACCEPTS `()` — reinstate the guard in `insert_stmt`: a path whose start has `var: None, labels: [], props: []` and no hops → error "INSERT node needs a label or properties".

- [ ] **Step 7: Adapters (same task — keep the workspace green)**

- `crates/varve-plan/src/exec.rs`: add variant `#[error("unsupported in v1: {0}")] Unsupported(String)` to `PlanError`. `matching_snapshot(pattern, ..)`/`matching_iids`: `let label = pattern.labels.first().map(String::as_str).unwrap_or("");`. `snapshot_for_query`/`run_query`/`execute_query` call sites: `let Some(node) = stmt.single_node() else { return Err(PlanError::Unsupported("multi-element MATCH lands in task 8 of slice 6".into())) };` and use `node.labels.first()…`; non-empty `node.props` → same `Unsupported` error (task 8 lifts it). `ReturnItem::Var` in `execute_query` → `Unsupported("path variables land in task 9 of slice 6")`.
- `crates/varve-engine/src/db.rs`: add `#[error("unsupported in v1: {0}")] Unsupported(String)` to `EngineError`; `query()` uses `q.single_node()` the same way (task 8 replaces this).
- `crates/varve-engine/src/writer.rs`: `resolve_insert` iterates `ins.paths`, requires `match_part.is_none()` and every path hop-free (else `EngineError::Unsupported("… lands in task 5 of slice 6".into())`), and builds node events from `path.start` exactly as before (labels/props field names updated). `resolve_delete`: `del.detach` → `Unsupported("DETACH DELETE lands in task 7 of slice 6")`; label via `labels.first()`.

- [ ] **Step 8: Run everything**

Run: `cargo test --workspace`
Expected: PASS — all pre-slice tests green (hello/time_travel examples' GQL is single-node and unaffected), all new parser tests green.

- [ ] **Step 9: Commit**

```bash
cargo fmt --all
git add -A
git commit -m "feat: GQL edge/path/quantifier grammar, pattern AST reshape, DETACH keyword"
```

---
### Task 4: GQL — INSERT edge paths, `MATCH … INSERT`, DETACH DELETE parse coverage

Task 3 built the grammar machinery; this task wires the two new STATEMENT forms and pins the full INSERT surface with tests. (Writer semantics arrive in T5/T7 — until then the engine returns `Unsupported` for the new forms, which Task 3's adapters already do.)

**Files:**
- Modify: `crates/varve-gql/src/parser.rs`

**Interfaces:**
- Consumes: Task 3's AST (`InsertStmt { match_part, paths, valid_from, valid_to }`, `MatchPart`, `DeleteStmt.detach`).
- Produces: `parse()` accepts `INSERT <path>(, <path>)* [VALID …]`, `MATCH <node>(, <node>)* [WHERE …] INSERT <path>(, <path>)* [VALID …]`, and `MATCH <node> [WHERE …] DETACH DELETE <var>`.

- [ ] **Step 1: Failing parser tests**

```rust
#[test]
fn parses_insert_edge_with_inline_nodes() {
    let stmt = parse(
        "INSERT (:Person {_id: 1, name: 'Ada'})-[:KNOWS {since: 2020}]->(:Person {_id: 2})",
    )
    .unwrap();
    let Statement::Insert(ins) = stmt else { panic!("expected insert") };
    assert!(ins.match_part.is_none());
    assert_eq!(ins.paths.len(), 1);
    let p = &ins.paths[0];
    assert_eq!(p.start.labels, vec!["Person".to_string()]);
    assert_eq!(p.hops.len(), 1);
    let (e, end) = &p.hops[0];
    assert_eq!(e.label, "KNOWS");
    assert_eq!(e.props, vec![("since".into(), Literal::Int(2020))]);
    assert_eq!(end.props[0], ("_id".into(), Literal::Int(2)));
}

#[test]
fn parses_insert_var_reuse_across_paths() {
    let stmt = parse("INSERT (a:Person {_id: 1}), (a)-[:KNOWS]->(b:Person {_id: 2})").unwrap();
    let Statement::Insert(ins) = stmt else { panic!("expected insert") };
    assert_eq!(ins.paths.len(), 2);
    assert_eq!(ins.paths[1].start, NodePattern { var: Some("a".into()), labels: vec![], props: vec![] });
    assert_eq!(ins.paths[1].hops[0].1.var.as_deref(), Some("b"));
}

#[test]
fn parses_match_insert() {
    let stmt = parse(
        "MATCH (a:Person {name: 'Ada'}), (b:Person) WHERE b.name = 'Bob' INSERT (a)-[:KNOWS]->(b)",
    )
    .unwrap();
    let Statement::Insert(ins) = stmt else { panic!("expected insert") };
    let mp = ins.match_part.as_ref().unwrap();
    assert_eq!(mp.patterns.len(), 2);
    assert_eq!(mp.patterns[0].props, vec![("name".into(), Literal::Str("Ada".into()))]);
    assert!(mp.where_clause.is_some());
    assert_eq!(ins.paths[0].hops[0].0.label, "KNOWS");
}

#[test]
fn match_insert_rejects_hops_in_match_part() {
    let err = parse("MATCH (a)-[:KNOWS]->(b) INSERT (a)-[:LIKES]->(b)").unwrap_err();
    assert!(err.to_string().contains("slice 7"));
}

#[test]
fn match_insert_requires_vars_and_rejects_temporal() {
    let err = parse("MATCH (:Person) INSERT (:X {_id: 1})").unwrap_err();
    assert!(err.to_string().contains("variable"));
    let err = parse("FOR VALID_TIME ALL MATCH (a:Person) INSERT (a)-[:K]->(a)").unwrap_err();
    assert!(err.to_string().contains("INSERT"));
}

#[test]
fn parses_insert_edge_valid_clause() {
    let stmt = parse(
        "INSERT (:P {_id: 1})-[:K]->(:P {_id: 2}) VALID FROM TIMESTAMP '2020-01-01T00:00:00Z'",
    )
    .unwrap();
    let Statement::Insert(ins) = stmt else { panic!("expected insert") };
    assert!(ins.valid_from.is_some());
}

#[test]
fn parses_detach_delete() {
    let stmt = parse("MATCH (p:Person) WHERE p.name = 'Ada' DETACH DELETE p").unwrap();
    let Statement::Delete(del) = stmt else { panic!("expected delete") };
    assert!(del.detach);
    assert_eq!(del.target, "p");
}

#[test]
fn insert_paths_reject_quantifiers_and_path_vars() {
    let err = parse("INSERT (a:P {_id: 1})-[:K]->{1,3}(b:P {_id: 2})").unwrap_err();
    assert!(err.to_string().contains("quantifier"));
    let err = parse("INSERT p = (a:P {_id: 1})-[:K]->(b:P {_id: 2})").unwrap_err();
    assert!(err.to_string().contains("path variable"));
}
```

- [ ] **Step 2: Run** `cargo test -p varve-gql` — Expected: FAIL (`MATCH … INSERT` errors "expected RETURN, DELETE, or DETACH"; quantified INSERT path parses without error; DETACH test may already pass from Task 3 — that's fine, keep it as a pin).

- [ ] **Step 3: Implement**

`insert_stmt` gains a `match_part: Option<MatchPart>` parameter (statement dispatch passes `None` for plain `INSERT`):

```rust
fn insert_stmt(&mut self, match_part: Option<MatchPart>) -> Result<InsertStmt, GqlError> {
    let mut paths = Vec::new();
    loop {
        let offset = self.offset();
        let path = self.path_pattern()?;
        if path.var.is_some() {
            return Err(GqlError::Parse {
                offset,
                msg: "path variables are not allowed in INSERT".into(),
            });
        }
        for (edge, _) in &path.hops {
            if edge.quantifier.is_some() {
                return Err(GqlError::Parse {
                    offset,
                    msg: "quantifiers are not allowed in INSERT patterns".into(),
                });
            }
        }
        if path.hops.is_empty()
            && path.start.var.is_none()
            && path.start.labels.is_empty()
            && path.start.props.is_empty()
        {
            return Err(GqlError::Parse {
                offset,
                msg: "INSERT node needs a label or properties".into(),
            });
        }
        paths.push(path);
        if *self.peek() == TokenKind::Comma {
            self.pos += 1;
        } else {
            break;
        }
    }
    // …existing optional `VALID FROM/TO` parsing + Eof expect, unchanged…
    Ok(InsertStmt { match_part, paths, valid_from, valid_to })
}
```

In `match_tail`, add the `INSERT` branch (before the fall-through error). It fires after `paths`, per-match `for_clauses`, and `where_clause` have been parsed:

```rust
TokenKind::Kw(Keyword::Insert) => {
    if temporal != TemporalClauses::default() || match_temporal != TemporalClauses::default() {
        return Err(GqlError::Parse {
            offset,
            msg: "MATCH … INSERT reads current state — temporal clauses are not supported"
                .into(),
        });
    }
    self.pos += 1;
    let mut patterns = Vec::with_capacity(paths.len());
    for path in paths {
        if !path.hops.is_empty() || path.var.is_some() {
            return Err(GqlError::Parse {
                offset,
                msg: "the MATCH part of INSERT takes single-node patterns in v1; \
                      path reads land in slice 7"
                    .into(),
            });
        }
        if path.start.var.is_none() {
            return Err(GqlError::Parse {
                offset,
                msg: "MATCH … INSERT patterns must bind a variable".into(),
            });
        }
        patterns.push(path.start);
    }
    let match_part = MatchPart { patterns, where_clause };
    return self
        .insert_stmt(Some(match_part))
        .map(Statement::Insert);
}
```

(`insert_stmt` already expects Eof at the end, so `MATCH … INSERT … VALID …` composes for free.)

Update `match_tail`'s fall-through error to `"expected RETURN, DELETE, DETACH, or INSERT after MATCH"`.

- [ ] **Step 4: Run** `cargo test -p varve-gql && cargo test --workspace` — Expected: PASS (engine `execute` on the new forms returns `Unsupported` via Task-3 adapters; no engine test exercises them yet).

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add -A
git commit -m "feat: parse INSERT edge paths, MATCH…INSERT, DETACH DELETE"
```

---
### Task 5: Engine — edges table end-to-end (write, apply, replay, flush primary, recovery, scan param)

**Files:**
- Modify: `crates/varve-engine/src/state.rs`
- Modify: `crates/varve-engine/src/writer.rs`
- Modify: `crates/varve-engine/src/scan.rs`
- Modify: `crates/varve-engine/src/flush.rs`
- Modify: `crates/varve-engine/src/db.rs`
- Modify: `crates/varve-plan/src/exec.rs` (`iids_from_snapshot` gains prop filters)
- Modify: `crates/varve-testkit/src/bin/crash_child.rs` + `tests/crash_recovery.rs` (only if compile breaks; state shape is internal so likely untouched)

**Interfaces:**
- Produces (state.rs):

```rust
pub(crate) const EDGES_TABLE: &str = "edges";

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum TableKind {
    Nodes,
    Edges,
}

impl TableKind {
    pub fn name(self) -> &'static str {
        match self {
            TableKind::Nodes => NODES_TABLE,
            TableKind::Edges => EDGES_TABLE,
        }
    }
}

/// One table's queryable state: live tail + persisted primary tries.
pub(crate) struct TableCore {
    pub live: LiveTable,
    pub tries: Vec<PersistedTrie>,
}

pub(crate) struct TableState {
    pub nodes: TableCore,
    pub edges: TableCore,
    /// Persisted adjacency families (edges only; filled from Task 6).
    pub adj_out: Vec<PersistedTrie>,
    pub adj_in: Vec<PersistedTrie>,
}

impl TableState {
    pub fn new() -> TableState { /* all empty */ }
    pub fn core(&self, kind: TableKind) -> &TableCore { /* match */ }
    pub fn core_mut(&mut self, kind: TableKind) -> &mut TableCore { /* match */ }
    pub fn live_rows(&self) -> usize {
        self.nodes.live.event_count() + self.edges.live.event_count()
    }
}
```

- Produces (writer.rs): `pub(crate) struct Effects { pub nodes: Vec<Event>, pub edges: Vec<Event> }`; `Staged.events: Effects`; `async fn resolve_insert(state: &WriterState, ins: &InsertStmt, tx_id: u64, system: Instant) -> Result<Effects, EngineError>`; `statement_reads` true for `Delete(_)` and `Insert` with `match_part`.
- Produces (scan.rs): `merged_snapshot(state, store, kind: TableKind, label, bounds, iid_point)` (call sites updated).
- Produces (db.rs): `EngineError::{UnboundVariable(String), AlreadyBoundVariable(String)}`; recovery accepts `nodes` + `edges` tables (the two `UnknownTable` pin tests flip to a `widgets` table).
- Produces (varve-plan): `iids_from_snapshot(snapshot, where_clause, props: &[(String, Literal)])` — extra equality filters (used for match-part binding with inline props).

- [ ] **Step 1: Failing engine tests** — new `#[cfg(test)]` cases in `crates/varve-engine/src/db.rs` (existing test-module conventions: `#[tokio::test]`, `Db::memory()`):

```rust
#[tokio::test]
async fn insert_edge_with_inline_nodes_populates_edges_live() {
    let db = Db::memory();
    db.execute("INSERT (:Person {_id: 1, name: 'Ada'})-[:KNOWS {since: 2020}]->(:Person {_id: 2, name: 'Bob'})")
        .await
        .unwrap();
    let s = db.state.read().unwrap();
    assert_eq!(s.nodes.live.event_count(), 2);
    assert_eq!(s.edges.live.event_count(), 1);
    let ada = Iid::derive("default", "nodes", &Value::Int(1).id_bytes().unwrap());
    let out: Vec<_> = s.edges.live.out_edges(&ada).collect();
    assert_eq!(out.len(), 1);
}

#[tokio::test]
async fn insert_edge_var_reuse_binds_within_statement() {
    let db = Db::memory();
    db.execute("INSERT (a:Person {_id: 1}), (a)-[:KNOWS]->(b:Person {_id: 2})")
        .await
        .unwrap();
    let s = db.state.read().unwrap();
    assert_eq!(s.nodes.live.event_count(), 2);
    assert_eq!(s.edges.live.event_count(), 1);
}

#[tokio::test]
async fn insert_edge_binding_errors() {
    let db = Db::memory();
    // Bare (a) with no prior binding in THIS statement (bindings are
    // statement-local, never carried across execute calls):
    let err = db.execute("INSERT (a)-[:K]->(:P {_id: 9})").await.unwrap_err();
    assert!(matches!(err, EngineError::UnboundVariable(_)));
    // Re-using a bound var with labels/props is an error — a reference must
    // be a bare (x):
    let err2 = db
        .execute("INSERT (x:P {_id: 3}), (x:P)-[:K]->(:P {_id: 4})")
        .await
        .unwrap_err();
    assert!(matches!(err2, EngineError::AlreadyBoundVariable(_)));
    // Atomicity: both statements failed at resolve, so NOTHING was applied —
    // not even the syntactically fine (:P {_id: 9}) / (x:P {_id: 3}) parts.
    let s = db.state.read().unwrap();
    assert_eq!(s.nodes.live.event_count(), 0);
    assert_eq!(s.edges.live.event_count(), 0);
}
```

#[tokio::test]
async fn match_insert_binds_matched_nodes_cartesian() {
    let db = Db::memory();
    db.execute("INSERT (:Person {_id: 1, name: 'Ada'}), (:Person {_id: 2, name: 'Bob'}), (:Person {_id: 3, name: 'Bob'})")
        .await
        .unwrap();
    db.execute("MATCH (a:Person {name: 'Ada'}), (b:Person {name: 'Bob'}) INSERT (a)-[:KNOWS]->(b)")
        .await
        .unwrap();
    let s = db.state.read().unwrap();
    // 1 Ada × 2 Bobs = 2 edges.
    assert_eq!(s.edges.live.event_count(), 2);
}

#[tokio::test]
async fn edge_ids_user_supplied_and_derived_are_durable() {
    let db = Db::memory();
    db.execute("INSERT (:P {_id: 1})-[:K {_id: 7}]->(:P {_id: 2})").await.unwrap();
    db.execute("INSERT (:P {_id: 3})-[:K]->(:P {_id: 4})").await.unwrap();
    let s = db.state.read().unwrap();
    let user = Iid::derive("default", "edges", &Value::Int(7).id_bytes().unwrap());
    assert!(s.edges.live.events_for(&user).is_some());
    assert_eq!(s.edges.live.event_count(), 2);
}
```

And a replay/flush/restart test in the same module (mirrors the slice-4 recovery tests' config shape — memory log is NOT recoverable, so use the local-dir helper the existing tests use):

```rust
#[tokio::test]
async fn edges_survive_log_replay_and_block_flush_restart() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Db::local(dir.path()).await.unwrap();
        db.execute("INSERT (:P {_id: 1, name: 'Ada'})-[:KNOWS]->(:P {_id: 2, name: 'Bob'})")
            .await
            .unwrap();
    } // drop = close; events only in the log
    {
        let db = Db::local(dir.path()).await.unwrap();
        let s = db.state.read().unwrap();
        assert_eq!(s.edges.live.event_count(), 1, "log replay must restore edges");
        drop(s);
        force_flush(&db).await; // shared test helper — see the note below
    }
    {
        let db = Db::local(dir.path()).await.unwrap();
        let s = db.state.read().unwrap();
        assert_eq!(s.edges.live.event_count(), 0, "flushed");
        assert_eq!(s.edges.tries.len(), 1, "edges primary trie recovered from manifest");
    }
}
```

> `force_flush(&db)`: check `crates/varve/tests/blocks.rs` / the slice-4 engine tests for the established flush-forcing pattern (`[storage] max_block_rows = <small>` config + `open_with`, or an existing test helper). Define ONE `async fn force_flush(db: &Db)` in the db.rs test module on whichever mechanism already exists (config route preferred; a `#[cfg(test)]`-only method on `Db` as a last resort) — T6 and T7 tests reuse the same helper name.

Also FLIP the two recovery pin tests: `db.rs` tests asserting `"edges"` → `UnknownTable` now assert a manifest/log-record with table `"widgets"` errors and `"edges"` succeeds.

- [ ] **Step 2: Run** `cargo test -p varve-engine` — Expected: FAIL (no field `nodes` on `TableState`, `Unsupported` errors, etc.).

- [ ] **Step 3: Implement state split** — `state.rs` per the Interfaces block. Then compiler-drive the rename `s.live` → `s.nodes.live` / `s.tries` → `s.nodes.tries` through `scan.rs`, `flush.rs`, `writer.rs::apply`, `db.rs::recover` (every current use is nodes-scoped).

- [ ] **Step 4: Implement writer effects + INSERT resolution** — `writer.rs`:

```rust
#[derive(Default)]
pub(crate) struct Effects {
    pub nodes: Vec<Event>,
    pub edges: Vec<Event>,
}
```

`resolve()` becomes:

```rust
let effects = match &stmt {
    Statement::Insert(ins) => resolve_insert(state, ins, tx_id, system).await?,
    Statement::Delete(del) => resolve_delete(state, del, system).await?,
    Statement::Query(_) => return Err(EngineError::NotAMutation),
};
let mut table_effects = Vec::new();
if !effects.nodes.is_empty() {
    table_effects.push(TableEffects {
        table: NODES_TABLE.to_string(),
        arrow_ipc: encode_events(&effects.nodes)?,
    });
}
if !effects.edges.is_empty() {
    table_effects.push(TableEffects {
        table: EDGES_TABLE.to_string(),
        arrow_ipc: encode_events(&effects.edges)?,
    });
}
```

`resolve_delete` returns `Effects { nodes: …, edges: vec![] }` for now (T7 fills edges). `statement_reads`:

```rust
fn statement_reads(stmt: &Statement) -> bool {
    match stmt {
        Statement::Delete(_) => true,
        Statement::Insert(ins) => ins.match_part.is_some(),
        Statement::Query(_) => false,
    }
}
```

`resolve_insert` (full replacement — async, reads for match-part):

```rust
async fn resolve_insert(
    state: &WriterState,
    ins: &InsertStmt,
    tx_id: u64,
    system: Instant,
) -> Result<Effects, EngineError> {
    let valid_from = ins.valid_from.unwrap_or(system);
    let valid_to = ins.valid_to.unwrap_or(Instant::END_OF_TIME);
    if valid_from >= valid_to {
        return Err(EngineError::InvalidValidRange { from: valid_from, to: valid_to });
    }

    // 1. Resolve the match part (decision 6c): per-variable candidate iids.
    let binding_rows: Vec<HashMap<String, Iid>> = match &ins.match_part {
        None => vec![HashMap::new()],
        Some(mp) => {
            let bounds = TemporalBounds {
                valid: TemporalDimension::at(system),
                system: TemporalDimension::at(system),
            };
            let mut per_var: Vec<(String, Vec<Iid>)> = Vec::new();
            for pattern in &mp.patterns {
                let var = pattern.var.clone().ok_or_else(|| {
                    EngineError::UnboundVariable("match pattern without variable".into())
                })?;
                let label = pattern.labels.first().map(String::as_str).unwrap_or("");
                let where_for_var = mp
                    .where_clause
                    .clone()
                    .filter(|Expr::PropEq { var: v, .. }| *v == var);
                let point = varve_plan::iid_point(&where_for_var, DEFAULT_GRAPH, NODES_TABLE);
                let snapshot =
                    merged_snapshot(&state.state, &state.store, TableKind::Nodes, label, &bounds, point)
                        .await?;
                let iids =
                    varve_plan::iids_from_snapshot(snapshot, &where_for_var, &pattern.props).await?;
                per_var.push((var, iids));
            }
            // Cartesian product; any empty variable ⇒ zero binding rows ⇒ empty tx.
            let mut rows: Vec<HashMap<String, Iid>> = vec![HashMap::new()];
            for (var, iids) in per_var {
                let mut next = Vec::with_capacity(rows.len() * iids.len());
                for row in &rows {
                    for iid in &iids {
                        let mut r = row.clone();
                        r.insert(var.clone(), *iid);
                        next.push(r);
                    }
                }
                rows = next;
            }
            rows
        }
    };

    // 2. Per binding row, walk the INSERT paths creating events.
    let mut effects = Effects::default();
    let mut ordinal: usize = 0;
    for row in binding_rows {
        let mut bound = row; // statement-local bindings extend the row
        for path in &ins.paths {
            let mut prev = resolve_insert_node(
                &path.start, &mut bound, &mut effects, &mut ordinal, tx_id, system, valid_from, valid_to,
            )?;
            for (edge, end) in &path.hops {
                let next = resolve_insert_node(
                    end, &mut bound, &mut effects, &mut ordinal, tx_id, system, valid_from, valid_to,
                )?;
                let (src, dst) = match edge.direction {
                    Direction::Out => (prev, next),
                    Direction::In => (next, prev),
                };
                let mut doc: Doc = edge
                    .props
                    .iter()
                    .map(|(k, v)| (k.clone(), literal_to_value(v)))
                    .collect();
                let id = match doc.get("_id") {
                    Some(v) => v.clone(),
                    None => {
                        let v = Value::Str(format!("varve:gen:{tx_id}:{ordinal}"));
                        doc.insert("_id".into(), v.clone());
                        v
                    }
                };
                ordinal += 1;
                let iid = Iid::derive(DEFAULT_GRAPH, EDGES_TABLE, &id.id_bytes()?);
                effects.edges.push(Event {
                    iid,
                    system_from: system,
                    valid_from,
                    valid_to,
                    src: Some(src),
                    dst: Some(dst),
                    op: Op::Put { labels: vec![edge.label.clone()], doc },
                });
                prev = next;
            }
        }
    }
    Ok(effects)
}

/// A node element inside INSERT: bound reference `(a)` or a new node.
#[allow(clippy::too_many_arguments)]
fn resolve_insert_node(
    node: &NodePattern,
    bound: &mut HashMap<String, Iid>,
    effects: &mut Effects,
    ordinal: &mut usize,
    tx_id: u64,
    system: Instant,
    valid_from: Instant,
    valid_to: Instant,
) -> Result<Iid, EngineError> {
    if let Some(var) = &node.var {
        if let Some(iid) = bound.get(var) {
            if !node.labels.is_empty() || !node.props.is_empty() {
                return Err(EngineError::AlreadyBoundVariable(var.clone()));
            }
            return Ok(*iid);
        }
        if node.labels.is_empty() && node.props.is_empty() {
            return Err(EngineError::UnboundVariable(var.clone()));
        }
    }
    let mut doc: Doc = node
        .props
        .iter()
        .map(|(k, v)| (k.clone(), literal_to_value(v)))
        .collect();
    let id = match doc.get("_id") {
        Some(v) => v.clone(),
        None => {
            let v = Value::Str(format!("varve:gen:{tx_id}:{ordinal}"));
            doc.insert("_id".into(), v.clone());
            v
        }
    };
    *ordinal += 1;
    let iid = Iid::derive(DEFAULT_GRAPH, NODES_TABLE, &id.id_bytes()?);
    effects.nodes.push(Event {
        iid,
        system_from: system,
        valid_from,
        valid_to,
        src: None,
        dst: None,
        op: Op::Put { labels: node.labels.clone(), doc },
    });
    if let Some(var) = &node.var {
        bound.insert(var.clone(), iid);
    }
    Ok(iid)
}
```

> Atomicity note (slice-1 precedent `ad5b19a`): all `id_bytes()` validation happens during resolve, BEFORE anything is applied — a bad `_id` anywhere fails the whole tx with zero events applied. Preserve that by never applying partial `Effects` (apply() only runs after resolve succeeded — already the writer's shape).

`apply()` routes per table:

```rust
for event in std::mem::take(&mut s.events.nodes) {
    shared.nodes.live.append(event).map_err(|e| e.to_string())?;
}
for event in std::mem::take(&mut s.events.edges) {
    shared.edges.live.append(event).map_err(|e| e.to_string())?;
}
```

`iids_from_snapshot` in `varve-plan` gains the `props` parameter: after the existing where-clause filter, chain `df = df.filter(col(k.as_str()).eq(to_df_literal(v)))?;` per prop (missing column → `PlanError::UnknownColumn`, same `has_col` check). Update the two existing callers (`matching_iids`, writer delete path) with `&[]`.

- [ ] **Step 5: Flush + recovery + scan param**

`scan.rs::merged_snapshot`: add `kind: TableKind` param; body uses `s.core(kind)` and `keys::data_key(DEFAULT_GRAPH, kind.name(), …)`. Call sites: `db.rs::query` and `writer.rs::resolve_delete` pass `TableKind::Nodes`.

`flush.rs::flush_block`: generalize to flush BOTH tables into ONE block/manifest. Structure:

```rust
let (nodes_enc, edges_enc, prior_nodes, prior_edges, max_system_us) = { /* one read lock:
    encode_block(&s.nodes.live, PAGE_ROWS)? if nodes.live.event_count() > 0,
    encode_block(&s.edges.live, PAGE_ROWS)? if edges.live.event_count() > 0,
    prior trie entries per table, max(last_system_from over both lives) */ };
if nodes_enc.is_none() && edges_enc.is_none() { return Ok(()); }
let trie_key = keys::l0_trie_key(block_id);
// PUT data+meta per present table (nodes first, then edges), collecting TrieEntry per table.
// manifest.tables = for each table: TableTries { graph, table, family: String::new(), tries: prior ++ maybe(new) }
//   — include a TableTries entry for a table WITH prior tries even if it flushed nothing this block.
// crash_point calls stay exactly where they are (pre/post manifest PUT).
// ONE write lock: push new PersistedTrie into each flushed table's core + reset THAT table's live.
```

The `TableTries.family` field does not exist until Task 6 — in THIS task keep building `TableTries { graph, table, tries }` as today, one entry per table. Trigger: `writer.rs` `live_rows(&state)` helper now reads `s.live_rows()` (nodes + edges).

`db.rs::recover`: route manifest tables — `(DEFAULT_GRAPH, NODES_TABLE) → state.nodes.tries`, `(DEFAULT_GRAPH, EDGES_TABLE) → state.edges.tries`, anything else → `UnknownTable` (until Task 6 adds families). Meta fetch per entry uses `keys::meta_key(graph, table, trie_key)` as today. Log-tail replay routes `TableEffects.table`: `"nodes"` → `state.nodes.live`, `"edges"` → `state.edges.live`, else `UnknownTable`.

- [ ] **Step 6: Run** `cargo test --workspace` — Expected: PASS (all Task-1 tests, slice-4 flush/restart suite, crash matrix, plus the new edge tests).

- [ ] **Step 7: Commit**

```bash
cargo fmt --all
git add -A
git commit -m "feat: edges table end-to-end — writer effects, MATCH…INSERT bindings, flush+recovery"
```

---
### Task 6: Adjacency families — `adj-out`/`adj-in` encode, flush, recovery, anchor-pruned lookup

**Files:**
- Modify: `crates/varve-index/src/block.rs`
- Modify: `crates/varve-storage/src/keys.rs`
- Modify: `crates/varve-storage/src/manifest.rs`
- Modify: `crates/varve-engine/src/state.rs` (nothing new — `adj_out`/`adj_in` exist since T5)
- Modify: `crates/varve-engine/src/flush.rs`
- Modify: `crates/varve-engine/src/db.rs` (recovery family routing)
- Modify: `crates/varve-engine/src/scan.rs` (`edge_adjacency`)

**Interfaces:**
- Produces (varve-index):

```rust
/// Which key a block file is sorted (and page-pruned) by (decision 4).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SortOrder {
    ByIid,
    BySrc,
    ByDst,
}

/// Like `encode_block` but sorted by `(sort_key asc, iid asc, system_from
/// desc)`; `PageMeta.min_iid`/`max_iid` record the SORT KEY's range so
/// `PageMeta::selected` prunes adjacency lookups unchanged. `BySrc`/`ByDst`
/// on an event without endpoints ⇒ `IndexError::Codec`.
pub fn encode_block_by(
    live: &LiveTable,
    page_rows: usize,
    order: SortOrder,
) -> Result<EncodedBlock, IndexError>
```

  `encode_block(live, page_rows)` becomes a thin wrapper: `encode_block_by(live, page_rows, SortOrder::ByIid)`.
- Produces (varve-storage):

```rust
pub const ADJ_OUT: &str = "adj-out";
pub const ADJ_IN: &str = "adj-in";
pub fn adj_data_key(graph: &str, table: &str, family: &str, trie_key: &str) -> String
// "v1/graphs/{graph}/tables/{table}/{family}/data/{trie_key}.arrow"
pub fn adj_meta_key(graph: &str, table: &str, family: &str, trie_key: &str) -> String
// "v1/graphs/{graph}/tables/{table}/{family}/meta/{trie_key}.arrow"
```

  `TableTries` gains `#[prost(string, tag = "4")] pub family: ::prost::alloc::string::String` (`""` = primary).
- Produces (varve-engine scan.rs):

```rust
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum AdjDirection {
    Out,
    In,
}

/// One traversable edge at the given bounds: `node` is the anchor-side
/// endpoint (src for Out, dst for In), `neighbor` the other endpoint.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) struct AdjacencyEntry {
    pub node: Iid,
    pub neighbor: Iid,
    pub edge: Iid,
}

/// Visible-edge adjacency at `bounds` (decision 11): live views + persisted
/// adj family, pages pruned by `anchor` via PageMeta::selected. Output is
/// sorted by (node, neighbor, edge) and deduped by edge — deterministic.
pub(crate) async fn edge_adjacency(
    state: &Arc<RwLock<TableState>>,
    store: &Arc<dyn ObjectStore>,
    label: &str,
    direction: AdjDirection,
    anchor: Option<Iid>,
    bounds: &TemporalBounds,
) -> Result<Vec<AdjacencyEntry>, EngineError>
```

- [ ] **Step 1: Failing index tests** — `crates/varve-index/src/block.rs` tests (reuse the `edge()` helper shape from Task 2):

```rust
#[test]
fn encode_by_src_sorts_and_stats_by_src() {
    let mut live = LiveTable::new();
    // src 30 first by arrival, src 10 second — BySrc must reorder.
    live.append(edge(1, 30, 40, 1)).unwrap();
    live.append(edge(2, 10, 20, 2)).unwrap();
    live.append(Event { op: Op::Delete, ..edge(2, 10, 20, 3) }).unwrap();
    let block = encode_block_by(&live, 1024, SortOrder::BySrc).unwrap();
    assert_eq!(block.pages.len(), 1);
    let page = &block.pages[0];
    assert_eq!(page.min_iid, Iid::derive("g", "nodes", &[10]));
    assert_eq!(page.max_iid, Iid::derive("g", "nodes", &[30]));
    let events = varve_index::decode_events(
        &block.data[page.offset as usize..(page.offset + page.len) as usize],
    )
    .unwrap();
    // (src asc, iid asc, system_from desc): edge 2's two events (delete first) then edge 1.
    assert_eq!(events[0].iid, Iid::derive("g", "edges", &[2]));
    assert!(matches!(events[0].op, Op::Delete));
    assert_eq!(events[2].iid, Iid::derive("g", "edges", &[1]));
}

#[test]
fn encode_by_src_without_endpoints_errors() {
    let mut live = LiveTable::new();
    live.append(Event { src: None, dst: None, ..edge(1, 0, 0, 1) }).unwrap();
    assert!(encode_block_by(&live, 1024, SortOrder::BySrc).is_err());
}

#[test]
fn primary_encode_is_unchanged_by_the_refactor() {
    let mut live = LiveTable::new();
    live.append(edge(1, 30, 40, 1)).unwrap();
    live.append(edge(2, 10, 20, 2)).unwrap();
    let a = encode_block(&live, 1024).unwrap();
    let b = encode_block_by(&live, 1024, SortOrder::ByIid).unwrap();
    assert_eq!(a.data, b.data);
    assert_eq!(a.meta, b.meta);
}
```

Failing storage tests — `crates/varve-storage/src/keys.rs` tests:

```rust
#[test]
fn adjacency_family_keys() {
    assert_eq!(
        adj_data_key("default", "edges", ADJ_OUT, "l00-rc-b00"),
        "v1/graphs/default/tables/edges/adj-out/data/l00-rc-b00.arrow"
    );
    assert_eq!(
        adj_meta_key("default", "edges", ADJ_IN, "l00-rc-b00"),
        "v1/graphs/default/tables/edges/adj-in/meta/l00-rc-b00.arrow"
    );
}
```

`crates/varve-storage/src/manifest.rs` tests:

```rust
#[test]
fn table_tries_family_round_trips() {
    let m = BlockManifest {
        block_id: 1,
        watermark: 0,
        max_tx_id: 1,
        max_system_time_us: 1,
        tables: vec![TableTries {
            graph: "default".into(),
            table: "edges".into(),
            family: "adj-out".into(),
            tries: vec![],
        }],
    };
    let back = BlockManifest::from_wire(&m.to_wire()).unwrap();
    assert_eq!(back.tables[0].family, "adj-out");
}
```

(Existing manifest golden/round-trip tests construct `TableTries` literals — add `family: String::new()` there; empty string encodes to zero bytes in proto3, so any pinned wire bytes are unchanged.)

- [ ] **Step 2: Run** `cargo test -p varve-index block && cargo test -p varve-storage` — Expected: FAIL.

- [ ] **Step 3: Implement index + storage**

`block.rs`:

```rust
pub fn encode_block(live: &LiveTable, page_rows: usize) -> Result<EncodedBlock, IndexError> {
    encode_block_by(live, page_rows, SortOrder::ByIid)
}

pub fn encode_block_by(
    live: &LiveTable,
    page_rows: usize,
    order: SortOrder,
) -> Result<EncodedBlock, IndexError> {
    // Per-entity event lists in (system_from desc) file order, keyed for sorting.
    let mut groups: Vec<(Iid, Iid, Vec<Event>)> = Vec::new(); // (sort_key, iid, desc events)
    for (iid, events) in live.entities() {
        let desc: Vec<Event> = events.iter().rev().cloned().collect();
        let sort_key = match order {
            SortOrder::ByIid => *iid,
            SortOrder::BySrc => desc[0].src.ok_or_else(|| {
                IndexError::Codec("edge event missing src endpoint in adjacency encode".into())
            })?,
            SortOrder::ByDst => desc[0].dst.ok_or_else(|| {
                IndexError::Codec("edge event missing dst endpoint in adjacency encode".into())
            })?,
        };
        groups.push((sort_key, *iid, desc));
    }
    groups.sort_by(|a, b| (a.0, a.1).cmp(&(b.0, b.1))); // total order ⇒ deterministic
    let mut rows: Vec<Event> = Vec::with_capacity(live.event_count());
    let mut keys: Vec<Iid> = Vec::with_capacity(live.event_count()); // sort key per row
    for (key, _iid, events) in groups {
        for e in events {
            rows.push(e);
            keys.push(key);
        }
    }
    let mut data = Vec::new();
    let mut pages = Vec::new();
    let chunk = page_rows.max(1);
    for (i, events) in rows.chunks(chunk).enumerate() {
        let offset = data.len() as u64;
        let bytes = encode_events(events)?;
        data.extend_from_slice(&bytes);
        let key_chunk = &keys[i * chunk..i * chunk + events.len()];
        let mut meta = page_meta(events, offset, bytes.len() as u64);
        meta.min_iid = *key_chunk.iter().min().unwrap_or(&meta.min_iid);
        meta.max_iid = *key_chunk.iter().max().unwrap_or(&meta.max_iid);
        pages.push(meta);
    }
    let meta = encode_meta(&pages)?;
    Ok(EncodedBlock { data, meta, pages })
}
```

(For `ByIid`, `key_chunk` min/max equals what `page_meta` already computed — output byte-identical to the old `encode_block`, pinned by the refactor test. `min`/`max` on a non-empty chunk never hit the fallback; the `unwrap_or` keeps clippy's no-unwrap rule satisfied.)

`keys.rs`: the two fns + two consts per the Interfaces block (plain `format!`). `manifest.rs`: add the `family` field (prost derive handles wire).

- [ ] **Step 4: Flush + recovery**

`flush.rs`: in the encode phase also build, when `s.edges.live.event_count() > 0`:

```rust
let adj_out_enc = encode_block_by(&s.edges.live, PAGE_ROWS, SortOrder::BySrc)?;
let adj_in_enc = encode_block_by(&s.edges.live, PAGE_ROWS, SortOrder::ByDst)?;
```

PUT order: nodes data/meta → edges data/meta → adj-out data/meta (`adj_data_key`/`adj_meta_key`) → adj-in data/meta → (crash_point `pre-manifest-put`) → manifest → (crash_point `post-manifest-put`) → one write lock: push nodes/edges primary tries + `s.adj_out.push(..)` + `s.adj_in.push(..)` + reset flushed lives. Manifest entries now carry `family`: nodes `""`, edges `""`, edges `"adj-out"`, edges `"adj-in"` — prior entries per (table, family) preserved (track `prior_adj_out`/`prior_adj_in` alongside the existing priors).

`db.rs::recover` family routing:

```rust
let dest = match (t.graph.as_str(), t.table.as_str(), t.family.as_str()) {
    (DEFAULT_GRAPH, NODES_TABLE, "") => &mut state.nodes.tries,
    (DEFAULT_GRAPH, EDGES_TABLE, "") => &mut state.edges.tries,
    (DEFAULT_GRAPH, EDGES_TABLE, varve_storage::ADJ_OUT) => &mut state.adj_out,
    (DEFAULT_GRAPH, EDGES_TABLE, varve_storage::ADJ_IN) => &mut state.adj_in,
    _ => return Err(EngineError::UnknownTable(format!("{}/{}/{}", t.graph, t.table, t.family))),
};
```

with meta fetched via `adj_meta_key` when `family != ""` else `meta_key`.

- [ ] **Step 5: Implement `edge_adjacency`** — `crates/varve-engine/src/scan.rs`:

```rust
pub(crate) async fn edge_adjacency(
    state: &Arc<RwLock<TableState>>,
    store: &Arc<dyn ObjectStore>,
    label: &str,
    direction: AdjDirection,
    anchor: Option<Iid>,
    bounds: &TemporalBounds,
) -> Result<Vec<AdjacencyEntry>, EngineError> {
    // 1. One read lock: live edge events (narrowed by the live adjacency
    //    views when anchored) + the persisted family's trie list.
    let (live_events, tries) = {
        let s = state.read().map_err(|_| EngineError::Poisoned)?;
        let live = &s.edges.live;
        let live_events: Vec<(Iid, Vec<Event>)> = match anchor {
            Some(node) => {
                let edge_iids: Vec<Iid> = match direction {
                    AdjDirection::Out => live.out_edges(&node).cloned().collect(),
                    AdjDirection::In => live.in_edges(&node).cloned().collect(),
                };
                edge_iids
                    .into_iter()
                    .filter_map(|e| live.events_for(&e).map(|ev| (e, ev.to_vec())))
                    .collect()
            }
            None => live.entities().map(|(iid, ev)| (*iid, ev.to_vec())).collect(),
        };
        let tries = match direction {
            AdjDirection::Out => s.adj_out.clone(),
            AdjDirection::In => s.adj_in.clone(),
        };
        (live_events, tries)
    };

    // 2. Persisted family pages, pruned by the anchor as the sort-key point
    //    (decision 4), then filtered exactly.
    let family = match direction {
        AdjDirection::Out => varve_storage::ADJ_OUT,
        AdjDirection::In => varve_storage::ADJ_IN,
    };
    let mut blocks: Vec<Vec<Event>> = Vec::new();
    for trie in &tries {
        let key = varve_storage::adj_data_key(DEFAULT_GRAPH, EDGES_TABLE, family, &trie.entry.trie_key);
        let mut block_events = Vec::new();
        for page in trie.pages.iter().filter(|p| p.selected(bounds, anchor.as_ref())) {
            let bytes = store.get_range(&key, page.offset..page.offset + page.len).await?;
            for event in decode_events(&bytes)? {
                let key_iid = match direction {
                    AdjDirection::Out => event.src,
                    AdjDirection::In => event.dst,
                };
                if anchor.is_none() || key_iid == anchor {
                    block_events.push(event);
                }
            }
        }
        blocks.push(block_events);
    }

    // 3. Merge, resolve per edge, keep visible Puts with the label, dedup.
    let merged = varve_index::merge_sources(blocks, live_events);
    let mut entries = Vec::new();
    for (edge, events) in &merged {
        let visible = varve_index::resolve(events, bounds);
        let labeled = visible.iter().any(|v| match &v.event.op {
            Op::Put { labels, .. } => labels.iter().any(|l| l == label),
            _ => false,
        });
        if !labeled {
            continue;
        }
        let (Some(src), Some(dst)) = (events[0].src, events[0].dst) else {
            return Err(EngineError::Index(varve_index::IndexError::Codec(
                "edge event missing endpoints".into(),
            )));
        };
        let (node, neighbor) = match direction {
            AdjDirection::Out => (src, dst),
            AdjDirection::In => (dst, src),
        };
        entries.push(AdjacencyEntry { node, neighbor, edge: *edge });
    }
    entries.sort_by_key(|e| (e.node, e.neighbor, e.edge));
    Ok(entries)
}
```

- [ ] **Step 6: Failing-then-green engine tests** — add to `db.rs` tests (uses the T5 flush-forcing config route):

```rust
#[tokio::test]
async fn adjacency_matches_across_live_and_flushed_and_restart() {
    let dir = tempfile::tempdir().unwrap();
    let ada = Iid::derive("default", "nodes", &Value::Int(1).id_bytes().unwrap());
    {
        let db = Db::local(dir.path()).await.unwrap();
        db.execute("INSERT (:P {_id: 1})-[:KNOWS]->(:P {_id: 2})").await.unwrap();
        db.execute("INSERT (:P {_id: 3})").await.unwrap();
        db.execute("MATCH (a:P {_id: 1}), (b:P {_id: 3}) INSERT (a)-[:KNOWS]->(b)")
            .await
            .unwrap();
        // Flush, then add one more live edge so BOTH sources contribute.
        force_flush(&db).await;
        db.execute("MATCH (a:P {_id: 3}), (b:P {_id: 2}) INSERT (a)-[:KNOWS]->(b)")
            .await
            .unwrap();
        let now = db.clock.watermark();
        let bounds = TemporalBounds {
            valid: TemporalDimension::at(now),
            system: TemporalDimension::at(now),
        };
        let out = edge_adjacency(&db.state, &db.store, "KNOWS", AdjDirection::Out, Some(ada), &bounds)
            .await
            .unwrap();
        assert!(!out.is_empty());
        // Ground truth: same answer from the PRIMARY family (full scan + filter).
        let all = edge_adjacency(&db.state, &db.store, "KNOWS", AdjDirection::Out, None, &bounds)
            .await
            .unwrap();
        let expected: Vec<_> = all.into_iter().filter(|e| e.node == ada).collect();
        assert_eq!(out, expected);
    }
    { // restart: persisted families recovered from the manifest
        let db = Db::local(dir.path()).await.unwrap();
        let s = db.state.read().unwrap();
        assert_eq!(s.adj_out.len(), s.edges.tries.len());
        assert_eq!(s.adj_in.len(), s.edges.tries.len());
    }
}
```

> Write this test to the REAL statements available at this point in the slice (MATCH…INSERT exists since T5; simplify the setup GQL if any line above under-specifies — the assertions, sorted-entry equality between anchored and full-scan-filtered adjacency plus recovered family tries, are the contract.) Also extend the slice-4 kill-during-flush crash matrix expectations ONLY if a run shows adj PUT ordering changed pre-manifest invariants — it doesn't (all new PUTs are pre-manifest, i.e. invisible garbage on crash).

- [ ] **Step 7: Run** `cargo test --workspace` — Expected: PASS.

- [ ] **Step 8: Commit**

```bash
cargo fmt --all
git add -A
git commit -m "feat: adj-out/adj-in adjacency families — sorted blocks, manifest family, pruned lookups"
```

---
### Task 7: `DETACH DELETE` + still-connected `DELETE` error

**Files:**
- Modify: `crates/varve-engine/src/writer.rs` (`resolve_delete`)
- Modify: `crates/varve-engine/src/db.rs` (`EngineError::StillConnected`)

**Interfaces:**
- Consumes: T6 `edge_adjacency`, T5 `Effects`.
- Produces: `EngineError::StillConnected(usize)` — `#[error("cannot DELETE {0} still-connected node(s); use DETACH DELETE")]`; `resolve_delete` returns `Effects` with edge deletes on detach.

- [ ] **Step 1: Failing e2e tests** — new file `crates/varve/tests/detach_delete.rs` (public API only):

```rust
use varve::Db;

async fn seed(db: &Db) {
    db.execute("INSERT (:Person {_id: 1, name: 'Ada'})-[:KNOWS]->(:Person {_id: 2, name: 'Bob'})")
        .await
        .unwrap();
}

#[tokio::test]
async fn plain_delete_on_connected_node_fails_atomically() {
    let db = Db::memory();
    seed(&db).await;
    let err = db.execute("MATCH (p:Person) WHERE p.name = 'Ada' DELETE p").await.unwrap_err();
    assert!(err.to_string().contains("DETACH"));
    // Nothing was applied: Ada still visible.
    let rows = db.query("MATCH (p:Person) WHERE p.name = 'Ada' RETURN p.name").await.unwrap();
    assert_eq!(rows.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
}

#[tokio::test]
async fn detach_delete_removes_node_and_incident_edges_in_one_tx() {
    let db = Db::memory();
    seed(&db).await;
    db.execute("MATCH (p:Person) WHERE p.name = 'Ada' DETACH DELETE p").await.unwrap();
    let ada = db.query("MATCH (p:Person) WHERE p.name = 'Ada' RETURN p.name").await.unwrap();
    assert_eq!(ada.iter().map(|b| b.num_rows()).sum::<usize>(), 0);
    // Bob survives, and the edge is gone: deleting Bob plainly now succeeds.
    db.execute("MATCH (p:Person) WHERE p.name = 'Bob' DELETE p").await.unwrap();
}

#[tokio::test]
async fn detach_delete_handles_self_loops_and_unconnected_nodes() {
    let db = Db::memory();
    db.execute("INSERT (a:Person {_id: 7, name: 'Solo'}), (a)-[:LIKES]->(a)").await.unwrap();
    db.execute("MATCH (p:Person) WHERE p.name = 'Solo' DETACH DELETE p").await.unwrap();
    // Plain DELETE on an unconnected node keeps working.
    db.execute("INSERT (:Person {_id: 8, name: 'Free'})").await.unwrap();
    db.execute("MATCH (p:Person) WHERE p.name = 'Free' DELETE p").await.unwrap();
}
```

And a flushed-edge case in the engine test module (`db.rs`), because it needs `force_flush`:

```rust
#[tokio::test]
async fn detach_delete_sees_flushed_edges() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::local(dir.path()).await.unwrap();
    db.execute("INSERT (:P {_id: 1})-[:K]->(:P {_id: 2})").await.unwrap();
    force_flush(&db).await;
    let err = db.execute("MATCH (p:P) WHERE p._id = 1 DELETE p").await.unwrap_err();
    assert!(matches!(err, EngineError::StillConnected(1)));
    db.execute("MATCH (p:P) WHERE p._id = 1 DETACH DELETE p").await.unwrap();
    let s = db.state.read().unwrap();
    // Two edge events now exist for the edge (Put flushed + Delete live).
    assert_eq!(s.edges.live.event_count(), 1);
}
```

- [ ] **Step 2: Run** `cargo test -p varve --test detach_delete` — Expected: FAIL (`Unsupported("DETACH DELETE lands in task 7 …")` and plain delete succeeding where it must now error).

- [ ] **Step 3: Implement** — `resolve_delete` full replacement:

```rust
async fn resolve_delete(
    state: &WriterState,
    del: &DeleteStmt,
    system: Instant,
) -> Result<Effects, EngineError> {
    let bounds = TemporalBounds {
        valid: TemporalDimension::at(system),
        system: TemporalDimension::at(system),
    };
    let label = del.pattern.labels.first().map(String::as_str).unwrap_or("");
    let iid = varve_plan::iid_point(&del.where_clause, DEFAULT_GRAPH, NODES_TABLE);
    let snapshot =
        merged_snapshot(&state.state, &state.store, TableKind::Nodes, label, &bounds, iid).await?;
    let iids =
        varve_plan::iids_from_snapshot(snapshot, &del.where_clause, &del.pattern.props).await?;

    // Incident edges per matched node, both directions, deduped by edge iid.
    // Label "" would match nothing through edge_adjacency's label filter, so
    // incident lookup scans ALL labels: collect via a label-blind variant.
    let mut incident: BTreeMap<Iid, (Iid, Iid)> = BTreeMap::new(); // edge → (src, dst)
    for node in &iids {
        for dir in [AdjDirection::Out, AdjDirection::In] {
            for entry in incident_edges(&state.state, &state.store, dir, *node, &bounds).await? {
                let (src, dst) = match dir {
                    AdjDirection::Out => (entry.node, entry.neighbor),
                    AdjDirection::In => (entry.neighbor, entry.node),
                };
                incident.insert(entry.edge, (src, dst));
            }
        }
    }

    if !del.detach && !incident.is_empty() {
        return Err(EngineError::StillConnected(incident.len()));
    }

    let mut effects = Effects::default();
    for (edge, (src, dst)) in incident {
        effects.edges.push(Event {
            iid: edge,
            system_from: system,
            valid_from: system,
            valid_to: Instant::END_OF_TIME,
            src: Some(src),
            dst: Some(dst),
            op: Op::Delete,
        });
    }
    for iid in iids {
        effects.nodes.push(Event {
            iid,
            system_from: system,
            valid_from: system,
            valid_to: Instant::END_OF_TIME,
            src: None,
            dst: None,
            op: Op::Delete,
        });
    }
    Ok(effects)
}
```

`incident_edges` is `edge_adjacency` minus the label filter — refactor `edge_adjacency` to take `label: Option<&str>` internally (`pub(crate) fn edge_adjacency(.., label: &str, ..)` stays as a thin wrapper passing `Some(label)`, and `incident_edges(state, store, dir, node, bounds)` passes `None` with `anchor: Some(node)`). Keep both public-to-crate fns so T8/T9 call sites read clearly.

> `StillConnected(usize)` carries the count of distinct incident edges (`StillConnected(1)` for one edge — the flushed-edge test pins it).

- [ ] **Step 4: Run** `cargo test --workspace` — Expected: PASS (concurrency suite still green — DELETE stays a reading statement, unchanged group-commit behavior).

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add -A
git commit -m "feat: DETACH DELETE cascades incident edges; plain DELETE on connected node errors"
```

---
### Task 8: Pattern lowering — per-element scans, mangled columns, hash joins, size heuristic

**Files:**
- Create: `crates/varve-plan/src/pattern.rs`
- Modify: `crates/varve-plan/src/lib.rs` (`pub mod pattern; pub use pattern::{…}`)
- Modify: `crates/varve-plan/src/exec.rs` (`PlanError::UnknownVariable`; retire the T3 `Unsupported` adapters in `execute_query` callers)
- Modify: `crates/varve-engine/src/db.rs` (`query()` general path)
- Create: `crates/varve/tests/traversal.rs`

**Interfaces:**
- Produces (`varve_plan::pattern`):

```rust
pub const SYNTH_PREFIX: &str = "__el";

/// `{var}__{column}`, e.g. `a___iid` for var `a`'s `_iid`.
pub fn mangled(var: &str, col: &str) -> String

/// What the engine must fetch for one pattern element, in path order:
/// element 0 is the start node, then one Edge/Expand + one Node per hop.
#[derive(Debug, Clone, PartialEq)]
pub struct ScanSpec {
    pub var: String, // user var or synthesized `__el{i}`
    pub kind: SpecKind,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SpecKind {
    Node { label: Option<String>, iid_point: Option<Iid> },
    Edge { label: String, direction: Direction },
    /// Quantified hop (Task 9 consumes; scan_specs already emits it).
    Expand {
        label: String,
        direction: Direction,
        min: u32,
        max: u32,
        props: Vec<(String, Literal)>,
        path_var: Option<String>,
    },
}

/// Validates the statement (single path, labels, quantifier bounds vs
/// `max_path_depth`, path-var rules) and derives one ScanSpec per element.
pub fn scan_specs(stmt: &QueryStmt, graph: &str, max_path_depth: u32)
    -> Result<Vec<ScanSpec>, PlanError>

/// The engine's answer to one ScanSpec.
pub enum ScanInput {
    Batch(Option<RecordBatch>),
    Adjacency(Arc<EdgeAdjacency>), // Task 9's type; declared there
}

/// Lower + execute: mangle, per-element filters, joins (and Task 9's
/// expansions), WHERE, RETURN. `inputs.len() == specs.len()`, same order.
pub async fn execute_pattern(
    stmt: &QueryStmt,
    specs: &[ScanSpec],
    inputs: Vec<ScanInput>,
) -> Result<Vec<RecordBatch>, PlanError>
```

  Until Task 9, `SpecKind::Expand` inside `execute_pattern` returns `PlanError::Unsupported("quantified paths land in task 9 of slice 6")` and `ScanInput::Adjacency` is an empty placeholder enum variant guarded the same way (define `EdgeAdjacency` as a unit placeholder struct in `pattern.rs` this task; Task 9 moves the real type into `expand.rs` and re-exports).
- Produces (`varve_plan::exec`): `PlanError::UnknownVariable(String)` — `#[error("unknown variable '{0}' in WHERE/RETURN")]`.
- Consumes (engine): `merged_snapshot(.., TableKind::{Nodes,Edges}, ..)`.
- Engine `db.rs::query` contract after this task:

```rust
let specs = varve_plan::scan_specs(&q, DEFAULT_GRAPH, self.max_path_depth)?;
let mut inputs = Vec::with_capacity(specs.len());
for spec in &specs {
    let input = match &spec.kind {
        SpecKind::Node { label, iid_point } => ScanInput::Batch(
            merged_snapshot(&self.state, &self.store, TableKind::Nodes,
                label.as_deref().unwrap_or(""), &bounds, *iid_point).await?,
        ),
        SpecKind::Edge { label, .. } => ScanInput::Batch(
            merged_snapshot(&self.state, &self.store, TableKind::Edges, label, &bounds, None).await?,
        ),
        SpecKind::Expand { .. } => /* Task 9 */,
    };
    inputs.push(input);
}
Ok(varve_plan::execute_pattern(&q, &specs, inputs).await?)
```

  `Db` gains `max_path_depth: u32` (config `[query] max_path_depth`, serde default 10 — wired in Task 9 where it first matters; hardcode `10` here with a `// Task 9 wires config` comment ONLY if you must — better: wire the config now, it's three lines following the `StorageTuning` idiom).

- [ ] **Step 1: Failing e2e tests** — `crates/varve/tests/traversal.rs`:

```rust
use varve::Db;

async fn seed_triangle(db: &Db) {
    // ada -KNOWS-> bob -KNOWS-> cy;  ada -KNOWS-> cy
    db.execute("INSERT (:Person {_id: 1, name: 'Ada'}), (:Person {_id: 2, name: 'Bob'}), (:Person {_id: 3, name: 'Cy'})")
        .await.unwrap();
    db.execute("MATCH (a:Person {_id: 1}), (b:Person {_id: 2}) INSERT (a)-[:KNOWS {since: 2020}]->(b)").await.unwrap();
    db.execute("MATCH (a:Person {_id: 2}), (b:Person {_id: 3}) INSERT (a)-[:KNOWS {since: 2021}]->(b)").await.unwrap();
    db.execute("MATCH (a:Person {_id: 1}), (b:Person {_id: 3}) INSERT (a)-[:KNOWS {since: 2022}]->(b)").await.unwrap();
}

fn names(batches: &[varve::RecordBatch], col: &str) -> Vec<String> {
    let mut out = Vec::new();
    for b in batches {
        let idx = b.schema().column_with_name(col).unwrap().0;
        let arr = b.column(idx).as_any().downcast_ref::<datafusion::arrow::array::StringArray>().unwrap();
        for i in 0..arr.len() {
            out.push(arr.value(i).to_string());
        }
    }
    out.sort();
    out
}

#[tokio::test]
async fn single_hop_join() {
    let db = Db::memory();
    seed_triangle(&db).await;
    let rows = db
        .query("MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.name = 'Ada' RETURN b.name")
        .await
        .unwrap();
    assert_eq!(names(&rows, "name"), vec!["Bob".to_string(), "Cy".to_string()]);
}

#[tokio::test]
async fn two_hop_friend_of_friend() {
    let db = Db::memory();
    seed_triangle(&db).await;
    let rows = db
        .query("MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) WHERE a.name = 'Ada' RETURN c.name")
        .await
        .unwrap();
    assert_eq!(names(&rows, "name"), vec!["Cy".to_string()]);
}

#[tokio::test]
async fn reverse_direction_and_edge_props() {
    let db = Db::memory();
    seed_triangle(&db).await;
    let rows = db
        .query("MATCH (b:Person)<-[:KNOWS {since: 2020}]-(a:Person) RETURN b.name")
        .await
        .unwrap();
    assert_eq!(names(&rows, "name"), vec!["Bob".to_string()]);
}

#[tokio::test]
async fn node_inline_props_filter_scans() {
    let db = Db::memory();
    seed_triangle(&db).await;
    let rows = db
        .query("MATCH (a:Person {name: 'Ada'})-[:KNOWS]->(b:Person) RETURN b.name AS friend")
        .await
        .unwrap();
    assert_eq!(names(&rows, "friend"), vec!["Bob".to_string(), "Cy".to_string()]);
}

#[tokio::test]
async fn return_from_multiple_vars_and_edge_var() {
    let db = Db::memory();
    seed_triangle(&db).await;
    let rows = db
        .query("MATCH (a:Person)-[k:KNOWS]->(b:Person) WHERE a.name = 'Ada' RETURN a.name AS a, b.name AS b, k.since AS since")
        .await
        .unwrap();
    assert_eq!(rows.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
    assert!(names(&rows, "a").iter().all(|n| n == "Ada"));
}

#[tokio::test]
async fn traversal_respects_temporal_bounds() {
    let db = Db::memory();
    db.execute("INSERT (:P {_id: 1, name: 'a'}), (:P {_id: 2, name: 'b'})").await.unwrap();
    db.execute(
        "MATCH (a:P {_id: 1}), (b:P {_id: 2}) INSERT (a)-[:K]->(b) VALID FROM TIMESTAMP '2030-01-01T00:00:00Z'",
    )
    .await
    .unwrap();
    // Edge not valid yet at current valid time:
    let now_rows = db.query("MATCH (a:P)-[:K]->(b:P) RETURN b.name").await.unwrap();
    assert_eq!(now_rows.iter().map(|b| b.num_rows()).sum::<usize>(), 0);
    let then_rows = db
        .query("FOR VALID_TIME AS OF TIMESTAMP '2031-01-01T00:00:00Z' MATCH (a:P)-[:K]->(b:P) RETURN b.name")
        .await
        .unwrap();
    assert_eq!(then_rows.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
}

#[tokio::test]
async fn unknown_variable_in_return_errors() {
    let db = Db::memory();
    seed_triangle(&db).await;
    let err = db
        .query("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN z.name")
        .await
        .unwrap_err();
    assert!(err.to_string().contains("z"));
}
```

Plus a flushed-and-restarted variant of `two_hop_friend_of_friend` over `Db::local` + force-flush (same pattern as T6's restart test) — traversal must be identical from persisted blocks.

- [ ] **Step 2: Run** `cargo test -p varve --test traversal` — Expected: FAIL (`Unsupported("multi-element MATCH lands in task 8 …")`).

- [ ] **Step 3: Implement `pattern.rs`**

Mangling + batch renaming:

```rust
pub fn mangled(var: &str, col: &str) -> String {
    format!("{var}__{col}")
}

fn mangle_batch(var: &str, batch: &RecordBatch) -> Result<RecordBatch, PlanError> {
    let fields: Vec<Field> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| Field::new(mangled(var, f.name()), f.data_type().clone(), f.is_nullable()))
        .collect();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), batch.columns().to_vec())
        .map_err(varve_index::IndexError::Arrow)
        .map_err(PlanError::Index)
}
```

`scan_specs`: validate `stmt.paths.len() == 1` (else `Unsupported("comma-separated MATCH paths in queries land in slice 7")`); walk `paths[0]`: start node → `SpecKind::Node`, each hop → edge spec + node spec. Per node: `labels.len() > 1` → `Unsupported("multi-label MATCH lands in slice 7")`; `label = labels.first().cloned()`; `iid_point` = `varve_plan::iid_point`-style derivation from EITHER the WHERE clause (when its var matches) OR an inline `("_id", lit)` prop (extract the shared literal→`Iid` code into a small `fn iid_of(graph, table, lit) -> Option<Iid>` used by both — keep `exec::iid_point` delegating to it). Per edge: quantifier `None` → `SpecKind::Edge`; `Some(q)` → validate `q.max.unwrap_or(max_path_depth) <= max_path_depth` (else `Unsupported(format!("quantifier max {} exceeds max_path_depth {}", ..))`) and emit `SpecKind::Expand { min: q.min, max: q.max.unwrap_or(max_path_depth), .. }`. Anonymous vars get `format!("{SYNTH_PREFIX}{i}")`. Path var: `Some` requires exactly one hop and that hop quantified, else `Unsupported("path variables need a single quantified hop in v1")`.

`execute_pattern` (fixed-length portion; Expand arm added in Task 9):

```rust
pub async fn execute_pattern(
    stmt: &QueryStmt,
    specs: &[ScanSpec],
    inputs: Vec<ScanInput>,
) -> Result<Vec<RecordBatch>, PlanError> {
    let path = &stmt.paths[0];
    let ctx = crate::expand::session_context(); // Task 9; until then SessionContext::new()

    // 1. One mangled DataFrame per element, with its own predicates applied.
    //    None batch ⇒ that element matched nothing ⇒ empty result.
    let mut frames: Vec<Option<DataFrame>> = Vec::with_capacity(specs.len());
    let mut row_counts: Vec<usize> = Vec::with_capacity(specs.len());
    for (spec, input) in specs.iter().zip(inputs) {
        match input {
            ScanInput::Batch(None) => return Ok(vec![]),
            ScanInput::Batch(Some(batch)) => {
                row_counts.push(batch.num_rows());
                let batch = mangle_batch(&spec.var, &batch)?;
                let schema = batch.schema();
                let table = MemTable::try_new(schema, vec![vec![batch]])?;
                let mut df = ctx.read_table(Arc::new(table))?;
                df = apply_element_predicates(df, spec, stmt, path)?;
                frames.push(Some(df));
            }
            ScanInput::Adjacency(_) => {
                row_counts.push(0);
                frames.push(None); // consumed positionally by the Task-9 expansion arm
            }
        }
    }

    // 2. Left-deep join chain; direction by terminal-size heuristic
    //    (decision 9). Expansion hops anchor on the start side (Task 9),
    //    so any Expand forces forward.
    let has_expand = specs.iter().any(|s| matches!(s.kind, SpecKind::Expand { .. }));
    let forward = has_expand
        || row_counts.first().copied().unwrap_or(0) <= row_counts.last().copied().unwrap_or(0);
    let df = join_chain(frames, specs, path, forward)?;

    // 3. RETURN projection over mangled columns.
    project_return(df, stmt)
}
```

`apply_element_predicates`: for each `(k, v)` in the element's `props` (skip `_id` if it became the iid_point — applying it again is harmless and simpler: DO apply it; document), `df.filter(col(mangled(var, k)).eq(to_df_literal(v)))?` with `has_col` checks → `UnknownColumn`. If `stmt.where_clause` is `Some(Expr::PropEq { var, prop, value })` and `var == spec.var`, same. If the WHERE var matches NO spec var → `PlanError::UnknownVariable(var)` (check once up front in `execute_pattern`).

`join_chain` (forward direction; backward mirrors by iterating hops reversed and swapping join sides):

```rust
fn join_chain(
    mut frames: Vec<Option<DataFrame>>,
    specs: &[ScanSpec],
    path: &PathPattern,
    forward: bool,
) -> Result<DataFrame, PlanError> {
    // Element order in specs/frames: [n0, e0, n1, e1, n2, ...]:
    //   node i is index 2*i; hop i's edge is index 1 + 2*i.
    let hops = path.hops.len();
    let take = |frames: &mut Vec<Option<DataFrame>>, idx: usize| -> Result<DataFrame, PlanError> {
        frames[idx]
            .take()
            .ok_or_else(|| PlanError::Unsupported("quantified paths land in task 9 of slice 6".into()))
    };
    // One hop step: acc ⋈ edge on the near node's iid, then ⋈ far node.
    // For an Out edge, src is on the path's LEFT side; In swaps src/dst.
    // Walking backward swaps which side is "near" — hence `near_is_left`.
    let join_hop = |acc: DataFrame,
                    edge_frame: DataFrame,
                    node_frame: DataFrame,
                    edge: &EdgePattern,
                    near_var: &str,
                    far_var: &str,
                    edge_var: &str,
                    near_is_left: bool|
     -> Result<DataFrame, PlanError> {
        let (left_end, right_end) = match edge.direction {
            Direction::Out => ("_src_iid", "_dst_iid"),
            Direction::In => ("_dst_iid", "_src_iid"),
        };
        let (near_end, far_end) = if near_is_left { (left_end, right_end) } else { (right_end, left_end) };
        let acc = acc.join(
            edge_frame,
            JoinType::Inner,
            &[mangled(near_var, "_iid").as_str()],
            &[mangled(edge_var, near_end).as_str()],
            None,
        )?;
        Ok(acc.join(
            node_frame,
            JoinType::Inner,
            &[mangled(edge_var, far_end).as_str()],
            &[mangled(far_var, "_iid").as_str()],
            None,
        )?)
    };
    if forward {
        let mut acc = take(&mut frames, 0)?;
        for i in 0..hops {
            let edge_frame = take(&mut frames, 1 + 2 * i)?;
            let node_frame = take(&mut frames, 2 + 2 * i)?;
            acc = join_hop(
                acc, edge_frame, node_frame, &path.hops[i].0,
                &specs[2 * i].var, &specs[2 + 2 * i].var, &specs[1 + 2 * i].var, true,
            )?;
        }
        Ok(acc)
    } else {
        let mut acc = take(&mut frames, 2 * hops)?;
        for i in (0..hops).rev() {
            let edge_frame = take(&mut frames, 1 + 2 * i)?;
            let node_frame = take(&mut frames, 2 * i)?;
            acc = join_hop(
                acc, edge_frame, node_frame, &path.hops[i].0,
                &specs[2 + 2 * i].var, &specs[2 * i].var, &specs[1 + 2 * i].var, false,
            )?;
        }
        Ok(acc)
    }
}
```

(Borrow-checker note: the `join_hop` closure captures nothing mutable — if the closure form fights the checker, inline it as a free fn taking the same args. The e2e tests define the correct output; the sketch's join-key orientation is the contract.)

`project_return`: like the old `execute_query` projection, but resolving `ReturnItem::Prop { var, prop, alias }` → `col(mangled(var, prop))` (missing mangled column → `UnknownColumn(prop)`, unknown var → `UnknownVariable`), `TemporalFn` → `col(mangled(var, hidden))`, `ReturnItem::Var` → Task 9 (path cols; until then `Unsupported`). Delete `exec::execute_query` and move `snapshot_for_query`/`run_query` (LiveTable-direct helpers used only by varve-plan's own tests) onto the new path or inline into those tests — keep `matching_snapshot`/`matching_iids`/`iids_from_snapshot`/`iid_point`/`effective_bounds` as they are (writer consumers).

- [ ] **Step 4: Engine wiring** — `db.rs::query` per the Interfaces block; `[query] max_path_depth` read via a `QueryTuning { #[serde(default = "default_max_path_depth")] max_path_depth: u32 }` section struct (default fn returns `10`), stored on `Db`; `Db::memory()`/`local()` use the default.

- [ ] **Step 5: Run** `cargo test --workspace` — Expected: PASS. The old single-node queries now flow through `execute_pattern` (single element, zero joins) — `walking_skeleton`, `temporal`, `blocks`, examples all stay green.

- [ ] **Step 6: Commit**

```bash
cargo fmt --all
git add -A
git commit -m "feat: multi-element MATCH lowering — mangled per-element scans + hash joins"
```

---
### Task 9: `PathExpand` — custom DataFusion operator for quantified paths

**Files:**
- Create: `crates/varve-plan/src/expand.rs`
- Modify: `crates/varve-plan/src/lib.rs`, `crates/varve-plan/src/pattern.rs` (Expand arm)
- Modify: `crates/varve-engine/src/db.rs` (Expand input wiring)
- Modify: `crates/varve/tests/traversal.rs` (quantified e2e)

**Interfaces:**
- Produces (`varve_plan::expand`):

```rust
/// One traversable edge from a node (bounds already applied by the engine).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdjEdge {
    pub neighbor: Iid,
    pub edge: Iid,
}

/// Node → outgoing (or incoming, per the hop's direction — the engine builds
/// the right orientation) traversable edges, each Vec sorted by
/// (neighbor, edge) for deterministic expansion order.
#[derive(Debug, Default)]
pub struct EdgeAdjacency {
    map: HashMap<Iid, Vec<AdjEdge>>,
}

impl EdgeAdjacency {
    /// `entries` need not be sorted; they are sorted+deduped per node here.
    pub fn from_entries(entries: impl IntoIterator<Item = (Iid, AdjEdge)>) -> Self
    pub fn neighbors(&self, node: &Iid) -> &[AdjEdge]
}

/// Pure WALK-semantics breadth-wise expansion (the exec's core; also the
/// property-test surface). Returns, for the start node, every path of
/// min..=max hops as (end_iid, interleaved [n0, e1, n1, …] path).
/// Depth 0 (when min == 0) yields (start, [start]).
pub fn expand_paths(
    adjacency: &EdgeAdjacency,
    start: Iid,
    min: u32,
    max: u32,
) -> Vec<(Iid, Vec<Iid>)>

pub struct PathExpandNode { /* input: LogicalPlan, adjacency: Arc<EdgeAdjacency>,
    start_col: String, end_col: String, path_col: Option<String>,
    min: u32, max: u32, schema: DFSchemaRef */ }

impl PathExpandNode {
    /// end_col = mangled(end_var, "expand_iid") — joined to the end node's
    /// `{var}___iid` by the caller; path_col = mangled(path_var, "path").
    pub fn try_new(
        input: LogicalPlan,
        adjacency: Arc<EdgeAdjacency>,
        start_col: String,
        end_col: String,
        path_col: Option<String>,
        min: u32,
        max: u32,
    ) -> Result<Self, PlanError>
}

pub(crate) struct PathExpandExec { /* input: Arc<dyn ExecutionPlan>, … , properties: PlanProperties */ }
struct PathExpandPlanner;               // ExtensionPlanner
#[derive(Debug)]
struct VarveQueryPlanner;               // QueryPlanner

/// SessionContext with the Varve planner installed. ALL varve-plan query
/// execution goes through this (pattern.rs uses it since Task 8's stub).
pub fn session_context() -> SessionContext
```

- Engine: `SpecKind::Expand { label, direction, .. }` input = `ScanInput::Adjacency(Arc::new(EdgeAdjacency::from_entries(..)))` built from `edge_adjacency(&state, &store, label, dir, None, &bounds)` where `dir` maps `Direction::Out → AdjDirection::Out` etc.; entries map to `(entry.node, AdjEdge { neighbor: entry.neighbor, edge: entry.edge })`. Quantified-edge `props` (decision 13): filter hops when building adjacency — extend `edge_adjacency` with `props: &[(String, Literal)]` matched against each VISIBLE version's doc (equality on `Value`, missing key ⇒ no match); existing T6/T7 call sites pass `&[]`. Engine converts `Literal`→`Value` with the writer's `literal_to_value` (duplicate the 8-line helper in scan.rs — tiny and crate-private).

**DataFusion contracts verified against 54.0.0 sources** (paths for the implementer):
`UserDefinedLogicalNodeCore` (datafusion-expr `logical_plan/extension.rs:232`) requires `Debug + Eq + PartialOrd + Hash + Sized + Send + Sync` and fns `name`, `inputs`, `schema`, `expressions` (→ `vec![]`), `fmt_for_explain`, `with_exprs_and_inputs`. `Extension { node }` wraps it into `LogicalPlan::Extension`. `QueryPlanner` (datafusion `execution/context/mod.rs:2158`): `async fn create_physical_plan(&self, logical_plan, session_state) -> Result<Arc<dyn ExecutionPlan>>`. `DefaultPhysicalPlanner::with_extension_planners(vec![Arc<dyn ExtensionPlanner>])` (`physical_planner.rs:365`); `ExtensionPlanner::plan_extension(&self, planner, node, logical_inputs, physical_inputs, session_state) -> Result<Option<Arc<dyn ExecutionPlan>>>` (`physical_planner.rs:158`). `SessionStateBuilder::new().with_default_features().with_query_planner(Arc<dyn QueryPlanner>).build()`; `SessionContext::new_with_state(state)`. `ExecutionPlan` (datafusion-physical-plan `execution_plan.rs:94`): `name`, `as_any`, `properties -> &Arc<PlanProperties>` *(the trait returns a reference — cache the `PlanProperties` in the struct)*, `children -> Vec<&Arc<dyn ExecutionPlan>>`, `with_new_children`, `execute(partition, ctx) -> Result<SendableRecordBatchStream>`; override `required_input_distribution -> vec![Distribution::SinglePartition]`. `PlanProperties::new(EquivalenceProperties::new(schema), Partitioning::UnknownPartitioning(1), EmissionType::Incremental, Boundedness::Bounded)`. `RecordBatchStreamAdapter::new(schema, stream)` (datafusion-physical-plan `stream.rs:411`). `DFSchema::new_with_metadata(Vec<(Option<TableReference>, Arc<Field>)>, HashMap)` (datafusion-common `dfschema.rs:154`). `DataFrame::into_parts() -> (SessionState, LogicalPlan)` / `DataFrame::new(session_state, plan)` (`dataframe/mod.rs:1673/258`). If any name resolves differently under the `datafusion` facade re-exports, follow the facade — the tests are the contract.

- [ ] **Step 1: Failing unit tests for the pure core** — in `expand.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use varve_types::Iid;

    fn n(i: u8) -> Iid { Iid::derive("g", "nodes", &[i]) }
    fn e(i: u8) -> Iid { Iid::derive("g", "edges", &[i]) }

    fn line() -> EdgeAdjacency {
        // 1 -e1-> 2 -e2-> 3 -e3-> 4
        EdgeAdjacency::from_entries([
            (n(1), AdjEdge { neighbor: n(2), edge: e(1) }),
            (n(2), AdjEdge { neighbor: n(3), edge: e(2) }),
            (n(3), AdjEdge { neighbor: n(4), edge: e(3) }),
        ])
    }

    #[test]
    fn expands_min_to_max_hops() {
        let paths = expand_paths(&line(), n(1), 1, 3);
        let ends: Vec<Iid> = paths.iter().map(|(end, _)| *end).collect();
        assert_eq!(ends, vec![n(2), n(3), n(4)]); // breadth order: depth 1, 2, 3
        assert_eq!(paths[1].1, vec![n(1), e(1), n(2), e(2), n(3)]);
    }

    #[test]
    fn zero_length_includes_start() {
        let paths = expand_paths(&line(), n(1), 0, 1);
        assert_eq!(paths[0], (n(1), vec![n(1)]));
        assert_eq!(paths.len(), 2);
    }

    #[test]
    fn walk_semantics_allow_cycles_capped_by_max() {
        // 1 -e1-> 2 -e2-> 1 (cycle)
        let adj = EdgeAdjacency::from_entries([
            (n(1), AdjEdge { neighbor: n(2), edge: e(1) }),
            (n(2), AdjEdge { neighbor: n(1), edge: e(2) }),
        ]);
        let paths = expand_paths(&adj, n(1), 1, 4);
        assert_eq!(paths.len(), 4); // one path per depth 1..=4, repeats allowed
        assert_eq!(paths[3].1.len(), 9);
    }

    #[test]
    fn min_beyond_reachability_is_empty() {
        assert!(expand_paths(&line(), n(4), 1, 3).is_empty());
    }
}
```

- [ ] **Step 2: Run** `cargo test -p varve-plan expand` — Expected: FAIL (module doesn't exist).

- [ ] **Step 3: Implement the pure core**

```rust
impl EdgeAdjacency {
    pub fn from_entries(entries: impl IntoIterator<Item = (Iid, AdjEdge)>) -> Self {
        let mut map: HashMap<Iid, Vec<AdjEdge>> = HashMap::new();
        for (node, edge) in entries {
            map.entry(node).or_default().push(edge);
        }
        for v in map.values_mut() {
            v.sort_by_key(|a| (a.neighbor, a.edge));
            v.dedup();
        }
        EdgeAdjacency { map }
    }

    pub fn neighbors(&self, node: &Iid) -> &[AdjEdge] {
        self.map.get(node).map(Vec::as_slice).unwrap_or(&[])
    }
}

pub fn expand_paths(
    adjacency: &EdgeAdjacency,
    start: Iid,
    min: u32,
    max: u32,
) -> Vec<(Iid, Vec<Iid>)> {
    let mut out = Vec::new();
    let mut frontier: Vec<(Iid, Vec<Iid>)> = vec![(start, vec![start])];
    if min == 0 {
        out.push((start, vec![start]));
    }
    for depth in 1..=max {
        let mut next = Vec::new();
        for (node, path) in &frontier {
            for adj in adjacency.neighbors(node) {
                let mut p = path.clone();
                p.push(adj.edge);
                p.push(adj.neighbor);
                if depth >= min {
                    out.push((adj.neighbor, p.clone()));
                }
                next.push((adj.neighbor, p));
            }
        }
        if next.is_empty() {
            break;
        }
        frontier = next;
    }
    out
}
```

Run Step 2 again — PASS. Commit checkpoint is fine here (`feat: expand_paths pure WALK expansion core`) or continue.

- [ ] **Step 4: The logical node, exec, and planner**

`PathExpandNode`: fields per Interfaces; schema built from the input's `DFSchema` plus `end_col` (`FixedSizeBinary(16)`, non-null) and optional `path_col` (`List(Field::new("item", FixedSizeBinary(16), false))`, non-null):

```rust
let mut fields: Vec<(Option<TableReference>, Arc<Field>)> = input
    .schema()
    .iter()
    .map(|(q, f)| (q.cloned(), Arc::clone(f)))
    .collect();
fields.push((None, Arc::new(Field::new(&end_col, DataType::FixedSizeBinary(16), false))));
if let Some(pc) = &path_col {
    // NOTE: the item field is declared NULLABLE to match what
    // `ListBuilder<FixedSizeBinaryBuilder>` produces by default — a
    // non-null item field here would make RecordBatch::try_new reject the
    // built arrays on DataType mismatch. Path elements are never actually
    // null.
    fields.push((None, Arc::new(Field::new(
        pc,
        DataType::List(Arc::new(Field::new("item", DataType::FixedSizeBinary(16), true))),
        false,
    ))));
}
let schema = Arc::new(DFSchema::new_with_metadata(fields, HashMap::new())?);
```

Manual trait impls (adjacency EXCLUDED — compared by `Arc::ptr_eq`, hashed by the structural fields only; documented on the impl):

```rust
impl PartialEq for PathExpandNode {
    fn eq(&self, other: &Self) -> bool {
        self.start_col == other.start_col
            && self.end_col == other.end_col
            && self.path_col == other.path_col
            && self.min == other.min
            && self.max == other.max
            && self.input == other.input
            && Arc::ptr_eq(&self.adjacency, &other.adjacency)
    }
}
impl Eq for PathExpandNode {}
impl std::hash::Hash for PathExpandNode { /* hash all structural fields, skip adjacency */ }
impl PartialOrd for PathExpandNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        (&self.start_col, &self.end_col, self.min, self.max)
            .partial_cmp(&(&other.start_col, &other.end_col, other.min, other.max))
    }
}
```

`UserDefinedLogicalNodeCore for PathExpandNode`: `name` → `"PathExpand"`; `inputs` → `vec![&self.input]`; `schema` → `&self.schema`; `expressions` → `vec![]`; `fmt_for_explain` → `write!(f, "PathExpand: {} -[{},{}]-> {}", self.start_col, self.min, self.max, self.end_col)`; `with_exprs_and_inputs(_, mut inputs)` → rebuild with `inputs.swap_remove(0)` (error if `inputs.len() != 1`).

`PathExpandExec::try_new(node: &PathExpandNode, input: Arc<dyn ExecutionPlan>)`: converts the node's `DFSchema` to an arrow `SchemaRef` (`Arc::new(Schema::from(node.schema().as_ref()))` — `impl From<&DFSchema> for Schema` exists in datafusion-common), resolves `start_idx` by `column_with_name(&node.start_col)` (missing → internal `DataFusionError::Internal`), clones adjacency/min/max/path flags, and caches `PlanProperties::new(EquivalenceProperties::new(schema.clone()), Partitioning::UnknownPartitioning(1), EmissionType::Incremental, Boundedness::Bounded)`.

`PathExpandExec::execute(partition, ctx)`:

```rust
let stream = self.input.execute(partition, ctx)?;
let schema = self.schema.clone();
let adjacency = Arc::clone(&self.adjacency);
let (start_idx, min, max, has_path) = (self.start_idx, self.min, self.max, self.path_col.is_some());
let out = stream.map(move |batch| {
    let batch = batch?;
    expand_batch(&batch, &schema, &adjacency, start_idx, min, max, has_path)
});
Ok(Box::pin(RecordBatchStreamAdapter::new(self.schema.clone(), out)))
```

`expand_batch` (free fn, unit-testable): downcast `start_idx` column to `FixedSizeBinaryArray`; for each row call `expand_paths`; collect `indices: Vec<u32>` (input row repeated per produced path), `ends`, `paths`; output columns = `arrow::compute::take(col, &UInt32Array::from(indices), None)?` per input column, then `FixedSizeBinaryBuilder` for ends, then `ListBuilder::new(FixedSizeBinaryBuilder::new(16))` for paths (per path: `values().append_value(iid)?` each element, `append(true)`); `RecordBatch::try_new(schema, columns)`. Zero total paths ⇒ an empty batch with the output schema (`RecordBatch::new_empty`).

`PathExpandPlanner`:

```rust
#[async_trait]
impl ExtensionPlanner for PathExpandPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(node) = node.as_any().downcast_ref::<PathExpandNode>() else {
            return Ok(None);
        };
        Ok(Some(Arc::new(PathExpandExec::try_new(node, physical_inputs[0].clone())?)))
    }
}
```

`VarveQueryPlanner::create_physical_plan` delegates to `DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(PathExpandPlanner)]).create_physical_plan(plan, state).await`. `session_context()` per the Interfaces block; `pattern.rs` switches its `SessionContext::new()` stub to it.

- [ ] **Step 5: Lowering + engine wiring**

`pattern.rs` Expand arm (inside the join chain, where the hop's edge spec is `Expand`):

```rust
let (state, plan) = acc.into_parts();
let node = PathExpandNode::try_new(
    plan,
    adjacency,                               // the positional ScanInput::Adjacency
    mangled(&prev_var, "_iid"),
    mangled(&end_var, "expand_iid"),
    path_var.as_ref().map(|p| mangled(p, "path")),
    min,
    max,
)?;
let plan = LogicalPlan::Extension(Extension { node: Arc::new(node) });
let mut acc = DataFrame::new(state, plan);
acc = acc.join(end_frame, JoinType::Inner,
    &[mangled(&end_var, "expand_iid").as_str()],
    &[mangled(&end_var, "_iid").as_str()], None)?;
```

(Direction is already baked into the adjacency's orientation by the engine. The backward-heuristic path may simply force `forward = true` when any hop is an Expand — one line, documented: expansion anchors on the start side in v1.) `project_return` handles `ReturnItem::Var { var, alias }` → `col(mangled(var, "path")).alias(alias.unwrap_or(var))`, `UnknownVariable` if absent. `db.rs` builds the adjacency input per the Interfaces block.

- [ ] **Step 6: Failing-then-green e2e** — append to `crates/varve/tests/traversal.rs`:

```rust
#[tokio::test]
async fn quantified_hop_one_to_three() {
    let db = Db::memory();
    // chain: n1 -> n2 -> n3 -> n4 -> n5
    db.execute("INSERT (:P {_id: 1, name: 'n1'}), (:P {_id: 2, name: 'n2'}), (:P {_id: 3, name: 'n3'}), (:P {_id: 4, name: 'n4'}), (:P {_id: 5, name: 'n5'})").await.unwrap();
    for (a, b) in [(1, 2), (2, 3), (3, 4), (4, 5)] {
        db.execute(&format!("MATCH (a:P {{_id: {a}}}), (b:P {{_id: {b}}}) INSERT (a)-[:K]->(b)")).await.unwrap();
    }
    let rows = db
        .query("MATCH (a:P)-[:K]->{1,3}(b:P) WHERE a.name = 'n1' RETURN b.name")
        .await
        .unwrap();
    assert_eq!(names(&rows, "name"), vec!["n2".to_string(), "n3".to_string(), "n4".to_string()]);
}

#[tokio::test]
async fn star_is_zero_to_cap_and_zero_length_binds_start() {
    let db = Db::memory();
    db.execute("INSERT (:P {_id: 1, name: 'solo'})").await.unwrap();
    let rows = db.query("MATCH (a:P)-[:K]->*(b:P) WHERE a.name = 'solo' RETURN b.name").await.unwrap();
    assert_eq!(names(&rows, "name"), vec!["solo".to_string()]); // zero hops: b = a
}

#[tokio::test]
async fn quantifier_beyond_max_path_depth_errors() {
    let db = Db::memory();
    db.execute("INSERT (:P {_id: 1})").await.unwrap();
    let err = db.query("MATCH (a:P)-[:K]->{1,99}(b:P) RETURN b._id").await.unwrap_err();
    assert!(err.to_string().contains("max_path_depth"));
}

#[tokio::test]
async fn cycles_terminate_at_depth_cap() {
    let db = Db::memory();
    db.execute("INSERT (a:P {_id: 1, name: 'x'}), (a)-[:K]->(a)").await.unwrap();
    let rows = db.query("MATCH (a:P)-[:K]->{1,3}(b:P) RETURN b.name").await.unwrap();
    assert_eq!(rows.iter().map(|b| b.num_rows()).sum::<usize>(), 3); // one WALK per depth
}

#[tokio::test]
async fn path_variable_binds_element_list() {
    use datafusion::arrow::array::{FixedSizeBinaryArray, ListArray};
    let db = Db::memory();
    db.execute("INSERT (:P {_id: 1, name: 'a'})").await.unwrap();
    db.execute("INSERT (:P {_id: 2, name: 'b'})").await.unwrap();
    db.execute("MATCH (a:P {_id: 1}), (b:P {_id: 2}) INSERT (a)-[:K]->(b)").await.unwrap();
    let rows = db.query("MATCH p = (a:P)-[:K]->{1,2}(b:P) WHERE a._id = 1 RETURN p").await.unwrap();
    let batch = &rows[0];
    let list = batch.column(0).as_any().downcast_ref::<ListArray>().unwrap();
    let first = list.value(0);
    let elems = first.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
    assert_eq!(elems.len(), 3); // n, e, n
}

#[tokio::test]
async fn quantified_traversal_respects_as_of_time() {
    let db = Db::memory();
    db.execute("INSERT (:P {_id: 1, name: 'a'}), (:P {_id: 2, name: 'b'})").await.unwrap();
    db.execute("MATCH (a:P {_id: 1}), (b:P {_id: 2}) INSERT (a)-[:K]->(b) VALID FROM TIMESTAMP '2030-01-01T00:00:00Z'").await.unwrap();
    let rows = db.query("MATCH (a:P)-[:K]->{1,2}(b:P) WHERE a._id = 1 RETURN b.name").await.unwrap();
    assert_eq!(rows.iter().map(|b| b.num_rows()).sum::<usize>(), 0);
    let rows = db
        .query("FOR VALID_TIME AS OF TIMESTAMP '2031-01-01T00:00:00Z' MATCH (a:P)-[:K]->{1,2}(b:P) WHERE a._id = 1 RETURN b.name")
        .await
        .unwrap();
    assert_eq!(names(&rows, "name"), vec!["b".to_string()]);
}
```

Run: `cargo test -p varve --test traversal` — Expected: PASS after Steps 4–5.

- [ ] **Step 7: Run** `cargo test --workspace` — Expected: PASS.

- [ ] **Step 8: Commit**

```bash
cargo fmt --all
git add -A
git commit -m "feat: PathExpand UDLN + ExecutionPlan — quantified {m,n}/* WALK traversal, path vars, depth cap"
```

---
### Task 10: Traversal oracle + property suites

**Files:**
- Create: `crates/varve-testkit/src/oracle.rs`
- Modify: `crates/varve-testkit/src/lib.rs` (`pub mod oracle;`)
- Create: `crates/varve-testkit/tests/traversal_oracle.rs`
- Modify: `crates/varve-testkit/Cargo.toml` (dev-deps: `varve = { path = "../varve" }`, `varve-plan` (for `expand_paths` in the pure layer), `tokio`, `chrono` — check which already exist before adding)

**Interfaces:**
- Produces (`varve_testkit::oracle`):

```rust
/// Naive in-memory graph walker — the traversal oracle (roadmap task 6).
#[derive(Default)]
pub struct GraphOracle {
    nodes: ReferenceStore,
    edges: ReferenceStore,
    endpoints: BTreeMap<Iid, (Iid, Iid)>, // edge → (src, dst), immutable
}

impl GraphOracle {
    pub fn new() -> Self
    pub fn append_node(&mut self, event: Event)
    /// Panics (debug_assert) if the event lacks endpoints.
    pub fn append_edge(&mut self, event: Event)
    /// Edges with `label` visible at (valid, system) leaving `node`
    /// (direction Out) or entering it (In), sorted by (neighbor, edge).
    pub fn neighbors(&self, node: Iid, label: &str, dir: OracleDir, valid: Instant, system: Instant)
        -> Vec<(Iid, Iid)> // (neighbor, edge)
    /// WALK expansion min..=max hops; returns (end, interleaved path) in
    /// breadth order — the same contract as varve_plan::expand::expand_paths.
    pub fn walk(&self, start: Iid, label: &str, dir: OracleDir, min: u32, max: u32,
                valid: Instant, system: Instant) -> Vec<(Iid, Vec<Iid>)>
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum OracleDir { Out, In }

/// Proptest strategy: a random graph as GQL statements + parallel oracle
/// events. Nodes get `_id` 0..n and label "P"; edges get label "K", a valid
/// range from the T_POOL grid, and strictly increasing system order.
pub struct ArbGraph {
    pub inserts: Vec<String>,      // GQL, replayable against a Db
    pub oracle: GraphOracle,       // same content
    pub node_ids: Vec<i64>,        // dense 0..n, for picking anchors
    ids: BTreeMap<Iid, i64>,       // node iid → _id, for oracle↔engine compare
}
impl ArbGraph {
    pub fn node_id(&self, iid: Iid) -> i64 // panics on unknown iid (test infra)
}
pub fn arb_graph(max_nodes: usize, max_edges: usize) -> impl Strategy<Value = ArbGraph>
```

  `neighbors` derives from `edges.visible_at(edge, valid, system)` (a visible `Op::Put` whose labels contain `label`) + the `endpoints` map — no bitemporal logic beyond the already-property-tested `ReferenceStore`. `walk` is a ≤20-line BFS mirroring `expand_paths` but against `neighbors`.
  Strategy sketch: `(1..=max_nodes)` nodes; `prop::collection::vec((0..nodes, 0..nodes, arb_valid_range()), 0..=max_edges)` edges. Nodes: `INSERT (:P {_id: i}) VALID FROM TIMESTAMP '1970-01-01T00:00:00Z'` — **the epoch VALID FROM is load-bearing**: probes come from the small `T_POOL` µs grid, and if nodes defaulted to `valid_from = insert-time wall clock` they would be invisible at every grid probe, vacuously emptying the property. Edges: per edge k, `MATCH (a:P {_id: s}), (b:P {_id: d}) INSERT (a)-[:K {_id: 1000+k}]->(b) VALID FROM … [TO …]` (range from the grid, or open-to-EOT — reuse `strategy.rs::arb_valid_range`'s 3:2 weighting) with the matching oracle `Event` (edge iid = `Iid::derive("default", "edges", Value::Int(1000+k).id_bytes())` — MUST mirror the engine's `DEFAULT_GRAPH`/`EDGES_TABLE` derivation; endpoints from the node iids; `system_from` = insertion index µs — order-faithful, not value-faithful, so oracle SYSTEM probes always use `i64::MAX` "current" (system-time-travel equivalence is slice-2-covered); VALID probes use the grid).
- Consumes: `varve_plan::expand::{EdgeAdjacency, AdjEdge, expand_paths}` for the pure layer.

- [ ] **Step 1: Failing pure-layer property test** — `tests/traversal_oracle.rs`:

```rust
use proptest::prelude::*;
use varve_plan::expand::{expand_paths, AdjEdge, EdgeAdjacency};
use varve_testkit::oracle::{GraphOracle, OracleDir};
use varve_types::{Iid, Instant};

fn cases() -> u32 {
    std::env::var("PROPTEST_CASES").ok().and_then(|v| v.parse().ok()).unwrap_or(10_000)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(cases()))]
    /// PURE layer: expand_paths == oracle walk on identical adjacency,
    /// for every (min, max) with max ≤ 4 (decision 14a).
    #[test]
    fn expansion_matches_oracle_walk(
        edges in prop::collection::vec((0u8..20, 0u8..20), 0..60),
        start in 0u8..20,
        min in 0u32..=4,
        span in 0u32..=4,
    ) {
        let max = min + span;
        let node = |i: u8| Iid::derive("g", "nodes", &[i]);
        let mut oracle = GraphOracle::new();
        let mut entries = Vec::new();
        for (k, (s, d)) in edges.iter().enumerate() {
            let e = Iid::derive("g", "edges", &[k as u8]);
            entries.push((node(*s), AdjEdge { neighbor: node(*d), edge: e }));
            oracle.append_edge(varve_index::Event {
                iid: e,
                system_from: Instant::from_micros(k as i64),
                valid_from: Instant::MIN,
                valid_to: Instant::END_OF_TIME,
                src: Some(node(*s)),
                dst: Some(node(*d)),
                op: varve_index::Op::Put {
                    labels: vec!["K".into()],
                    doc: Default::default(),
                },
            });
        }
        let adj = EdgeAdjacency::from_entries(entries);
        let got = expand_paths(&adj, node(start), min, max);
        let want = oracle.walk(node(start), "K", OracleDir::Out,
                               min, max, Instant::from_micros(0), Instant::END_OF_TIME);
        prop_assert_eq!(got, want);
    }
}
```

- [ ] **Step 2: Run** `cargo test -p varve-testkit --test traversal_oracle` — Expected: FAIL (no `oracle` module).

- [ ] **Step 3: Implement `oracle.rs`** per the Interfaces block (`neighbors` + `walk` bodies are short; `walk`'s BFS mirrors `expand_paths` exactly, sorted `neighbors` giving deterministic order). Run Step 2 — PASS.

- [ ] **Step 4: e2e property suite (decision 14b)** — append to `tests/traversal_oracle.rs`:

```rust
fn e2e_cases() -> u32 {
    cases().min(128) // each case boots a Db + tokio runtime (decision 14b)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(e2e_cases()))]
    /// E2E: random graph (≤200 nodes) driven through the real Db via GQL;
    /// every {m,n} (n ≤ 3) expansion from a sampled anchor matches the
    /// oracle, at NOW and at sampled AS-OF valid times (exit criterion:
    /// edge validity respected).
    #[test]
    fn db_traversal_matches_oracle(
        graph in varve_testkit::oracle::arb_graph(200, 400),
        anchor_pick in any::<prop::sample::Index>(),
        min in 0u32..=3,
        span in 0u32..=3,
        valid_probe in 0i64..varve_testkit::strategy::T_POOL,
    ) {
        let max = min + span;
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async {
            let db = varve::Db::memory();
            for stmt in &graph.inserts {
                db.execute(stmt).await.unwrap();
            }
            let anchor_id = graph.node_ids[anchor_pick.index(graph.node_ids.len())];
            let anchor = Iid::derive("default", "nodes",
                &varve_types::Value::Int(anchor_id).id_bytes().unwrap());

            for (label, gql_time, probe_valid) in [
                ("now", String::new(), None),
                ("asof", format!("FOR VALID_TIME AS OF TIMESTAMP '{}' ", micros_to_rfc3339(valid_probe)), Some(valid_probe)),
            ] {
                let gql = format!(
                    "{gql_time}MATCH (a:P)-[:K]->{{{min},{max}}}(b:P) WHERE a._id = {anchor_id} RETURN b._id"
                );
                let rows = db.query(&gql).await.unwrap();
                let mut got: Vec<i64> = collect_i64(&rows, "_id");
                got.sort_unstable();
                let (valid, system) = probe_at(&db, probe_valid).await;
                let mut want: Vec<i64> = graph
                    .oracle
                    .walk(anchor, "K", OracleDir::Out, min, max, valid, system)
                    .into_iter()
                    .map(|(end, _)| graph.node_id(end))
                    .collect();
                want.sort_unstable();
                prop_assert_eq!(got, want, "layer {}", label);
            }
            Ok(())
        })?;
    }
}
```

Support helpers to write in the SAME test file (complete, not sketched, when implementing): `collect_i64(batches, col)` (downcast Int64Array), `micros_to_rfc3339(i64)` (chrono; the grid is small non-negative µs), `probe_at(&db, Option<i64>) -> (Instant, Instant)` — `valid` = the grid probe when `Some`, else any instant past the grid's end (e.g. `Instant::from_micros(i64::MAX - 1)` matches the engine's "AS OF now" for grid-ranged edges — open-ended edges are visible at both, ranged grid edges at neither); `system` = `Instant::from_micros(i64::MAX)` always (current state; the oracle's system axis is order-faithful only). `ArbGraph::node_id` comes from Task 10's Interfaces block.

> Anchor semantics: `arb_graph` emits node `_id`s densely (0..n) so `node_ids` is `(0..n).collect()`. Self-loops and duplicate edges are allowed by the strategy ON PURPOSE (multiset path counts must still match).

Add one more e2e property — flush invariance (mirrors `flush_equivalence.rs`):

```rust
proptest! {
    #![proptest_config(ProptestConfig::with_cases(e2e_cases().min(32)))]
    /// Traversal results are invariant under flushing: memory-only Db vs a
    /// Db that flushed mid-ingest give identical {1,2} expansions.
    #[test]
    fn traversal_invariant_under_flush(
        graph in varve_testkit::oracle::arb_graph(30, 60),
        anchor_pick in any::<prop::sample::Index>(),
    ) {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async {
            let plain = varve::Db::memory();
            // Memory log + memory storage is legal (only local-log +
            // memory-storage is the slice-4 VolatileBlockStore error);
            // max_block_rows = 4 forces a flush every few statements.
            let cfg = varve::Config::from_toml_str(
                "[log]\nbackend = \"memory\"\n[storage]\nbackend = \"memory\"\nmax_block_rows = 4\n",
            )
            .unwrap();
            let flushy = varve::Db::open(cfg).await.unwrap();
            for stmt in &graph.inserts {
                plain.execute(stmt).await.unwrap();
                flushy.execute(stmt).await.unwrap();
            }
            let anchor_id = graph.node_ids[anchor_pick.index(graph.node_ids.len())];
            let gql = format!(
                "MATCH (a:P)-[:K]->{{1,2}}(b:P) WHERE a._id = {anchor_id} RETURN b._id"
            );
            let mut a = collect_i64(&plain.query(&gql).await.unwrap(), "_id");
            let mut b = collect_i64(&flushy.query(&gql).await.unwrap(), "_id");
            a.sort_unstable();
            b.sort_unstable();
            prop_assert_eq!(a, b);
            Ok(())
        })?;
    }
}
```

(The config route for the second Db: `[log] backend = "memory"`, `[storage] backend = "memory"`, `max_block_rows = 4` — legal per slice-4 decision 11, only local-LOG+memory-STORAGE is forbidden.)

- [ ] **Step 5: Run** `PROPTEST_CASES=64 cargo test -p varve-testkit --test traversal_oracle` then the full `cargo test --workspace` — Expected: PASS.

- [ ] **Step 6: Commit**

```bash
cargo fmt --all
git add -A
git commit -m "test: traversal oracle + pure/e2e property suites incl. AS-OF and flush invariance"
```

---

### Task 11: Social-graph fixture, integration test, perf smoke

**Files:**
- Create: `crates/varve-testkit/src/fixture.rs`
- Modify: `crates/varve-testkit/src/lib.rs` (`pub mod fixture;`)
- Create: `crates/varve-testkit/tests/social_graph.rs`
- Create: `crates/varve/examples/traversal_bench.rs`

**Interfaces:**
- Produces (`varve_testkit::fixture`):

```rust
/// Deterministic social graph: `people` Person nodes (_id 0..people) and
/// `friendships` KNOWS edges from a seeded LCG (no self-loops, duplicates
/// allowed — they're distinct edges). Same seed ⇒ same graph, every run.
pub struct SocialGraph {
    pub people: usize,
    pub edges: Vec<(i64, i64)>,
}

pub fn social_graph(people: usize, friendships: usize, seed: u64) -> SocialGraph

impl SocialGraph {
    /// `batch` nodes per multi-node INSERT statement.
    pub fn node_statements(&self, batch: usize) -> Vec<String>
    /// One `MATCH (a:Person {_id: s}), (b:Person {_id: d}) INSERT
    /// (a)-[:KNOWS]->(b)` statement per edge (the v1 mutation surface —
    /// multi-edge INSERT bodies land with slice 7's statement blocks).
    pub fn edge_statements(&self) -> Vec<String>
    /// The same graph as a GraphOracle (nodes + edges, valid ALL).
    pub fn oracle(&self) -> GraphOracle
}
```

  LCG: `state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407)` (PCG-style constants), high bits for the draw — 5 lines, no new dependency, deterministic (Global Constraint).
  `node_statements(1000)` → `INSERT (:Person {_id: 0, name: 'p0'}), (:Person {_id: 1, name: 'p1'}), …` chunks. `edge_statements()` → per edge `MATCH (a:Person {_id: s}), (b:Person {_id: d}) INSERT (a)-[:KNOWS]->(b)`. 60k single-edge statements through the writer loop is the honest v1 write path (each is a tx; the group-commit window amortizes the log I/O; memory-log ingest of 70k events is seconds — measured in the bench).

- [ ] **Step 1: Failing fixture unit test** (in `fixture.rs`):

```rust
#[test]
fn fixture_is_deterministic_and_shaped() {
    let a = social_graph(10_000, 60_000, 42);
    let b = social_graph(10_000, 60_000, 42);
    assert_eq!(a.edges, b.edges);
    assert_eq!(a.edges.len(), 60_000);
    assert!(a.edges.iter().all(|(s, d)| s != d && (0..10_000).contains(s) && (0..10_000).contains(d)));
    assert_ne!(social_graph(10_000, 60_000, 43).edges, a.edges);
    assert_eq!(a.node_statements(1000).len(), 10);
    assert_eq!(a.edge_statements().len(), 60_000);
}
```

- [ ] **Step 2: Run** `cargo test -p varve-testkit fixture` — FAIL → implement per Interfaces → PASS.

- [ ] **Step 3: Integration test** — `crates/varve-testkit/tests/social_graph.rs`:

```rust
use varve_testkit::fixture::social_graph;

/// Roadmap exit shape: 2-hop friend-of-friend and {1,3} over the 10k/60k
/// fixture, answers cross-checked against the oracle. Ingest via GQL.
#[tokio::test]
async fn fixture_two_hop_and_quantified_match_oracle() {
    let g = social_graph(10_000, 60_000, 42);
    let db = varve::Db::memory();
    for stmt in g.node_statements(1000) {
        db.execute(&stmt).await.unwrap();
    }
    for stmt in g.edge_statements() {
        db.execute(&stmt).await.unwrap();
    }
    // Build the oracle from the same fixture (valid ALL, current system).
    let oracle = g.oracle();
    let anchor = 0i64;
    let anchor_iid = Iid::derive("default", "nodes", &Value::Int(anchor).id_bytes().unwrap());
    let now = (Instant::from_micros(i64::MAX - 1), Instant::from_micros(i64::MAX));

    let rows = db
        .query(&format!(
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) WHERE a._id = {anchor} RETURN c._id"
        ))
        .await
        .unwrap();
    let mut got2: Vec<i64> = column_i64(&rows, "_id");
    got2.sort_unstable();
    let mut want2: Vec<i64> = oracle
        .walk(anchor_iid, "KNOWS", OracleDir::Out, 2, 2, now.0, now.1)
        .into_iter()
        .map(|(end, _)| g.node_id_of(end))
        .collect();
    want2.sort_unstable();
    assert_eq!(got2, want2, "2-hop friend-of-friend vs oracle");

    let rows13 = db
        .query(&format!(
            "MATCH (a:Person)-[:KNOWS]->{{1,3}}(b:Person) WHERE a._id = {anchor} RETURN b._id"
        ))
        .await
        .unwrap();
    let mut got13: Vec<i64> = column_i64(&rows13, "_id");
    got13.sort_unstable();
    let mut want13: Vec<i64> = oracle
        .walk(anchor_iid, "KNOWS", OracleDir::Out, 1, 3, now.0, now.1)
        .into_iter()
        .map(|(end, _)| g.node_id_of(end))
        .collect();
    want13.sort_unstable();
    assert_eq!(got13, want13, "{{1,3}} expansion vs oracle");
}
```

Sharing: extract Task 10's `collect_i64` into `varve_testkit::oracle` as `pub fn column_i64(batches: &[RecordBatch], col: &str) -> Vec<i64>` so both test files use one copy; `SocialGraph::node_id_of(iid) -> i64` mirrors `ArbGraph::node_id` (fixture ids are dense 0..people, so it's a reverse lookup over `Iid::derive` — precompute the map in `oracle()`... simplest: have `oracle()` return `(GraphOracle, BTreeMap<Iid, i64>)` or add the map as a `SocialGraph` method; pick ONE and keep both call sites consistent). Fixture NOTE on the 2-hop query shape: `a` is pinned by `_id`, so the outer scans stay small. Runtime: ~70k statements through the writer in a debug test is minutes-not-hours; if it exceeds ~90s locally, drop the in-test fixture to `social_graph(2_000, 12_000, 42)` and leave the FULL 10k/60k to the bench example — record whichever shipped in STATUS.

- [ ] **Step 4: Bench example** — `crates/varve/examples/traversal_bench.rs`, following `block_bench.rs`'s structure: tempdir `Db::local`, ingest the 10k/60k fixture (timed), `force-flush` via config (`[storage] max_block_rows = 20000` through `Config::from_toml_str` + `Db::open`/`open_with` — same wiring block_bench uses), reopen, then:

```text
run 2-hop friend-of-friend (anchor _id 0): cold once (timed), then 100 warm iterations (report avg + p50)
run -[:KNOWS]->{1,3}: same shape
print: ingest s, tx/s, cold ms, warm avg ms — exit criterion: warm 2-hop < 50 ms
```

Run: `cargo run --release --example traversal_bench -p varve`
Expected output shape: `ingest 70000 stmts in …s · 2-hop cold …ms warm …ms · {1,3} cold …ms warm …ms` with warm 2-hop `< 50 ms` on the M3 Max (record actuals in STATUS.md).

- [ ] **Step 5: Run** `cargo test --workspace` — PASS.

- [ ] **Step 6: Commit**

```bash
cargo fmt --all
git add -A
git commit -m "test: 10k/60k social-graph fixture + integration oracle check; traversal_bench example"
```

---

### Task 12: Slice exit checklist

- [ ] **Step 1: Full gate** — run and confirm green:

```bash
just check          # fmt --check, clippy -D warnings, test --workspace
just crash          # crash matrix still green (flush order changed in T6)
cargo run --example hello -p varve
cargo run --example time_travel -p varve
cargo run --release --example traversal_bench -p varve
```

- [ ] **Step 2: Update `docs/plans/varve-v1-roadmap.md`** — tick all six slice-6 checkboxes, append to the slice-6 entry: `**✅ SLICE COMPLETE <date> (sessions, method, tasks).**` with the exit-criteria evidence line (oracle suites green; AS-OF property green; warm 2-hop measured ms).

- [ ] **Step 3: Update `docs/plans/STATUS.md`** — new slice-6 block at the top of Current position (shift slice 5 down), following the established format: what shipped (edge events + endpoints columns, adjacency families + family manifests, MATCH…INSERT, DETACH DELETE, pattern joins, PathExpand + `[query] max_path_depth`, oracle suites, fixture), design decisions 1–16 from this plan's header (verbatim where load-bearing), deviations encountered during execution, bench numbers from traversal_bench, demo command `cargo run --release --example traversal_bench -p varve`, slice-log table row, and any new fast-follows discovered.

- [ ] **Step 4: Commit**

```bash
git add docs/plans/STATUS.md docs/plans/varve-v1-roadmap.md
git commit -m "docs: slice 6 complete — edges, adjacency families, traversal, PathExpand"
```

**Slice exit criteria (from the roadmap — all must hold):**
- [ ] 2-hop friend-of-friend and `-[:KNOWS]->{1,3}` correct vs oracle (T10 e2e property + T11 fixture test).
- [ ] Bitemporal traversal correct — edge validity respected at AS-OF time (T8/T9 temporal tests + T10 AS-OF property).
- [ ] 2-hop over the 10k/60k fixture < 50 ms warm (T11 bench, numbers in STATUS.md).
- [ ] All workspace tests green, clippy clean, STATUS.md updated, roadmap boxes ticked, demo command recorded.









