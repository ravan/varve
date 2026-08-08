# The queries

`blast-radius.gql` holds **Q1** — the pattern the other three reuse verbatim.
The three `explore-*.gql` files are the same evidence shaped for Varve
Explorer's UI; see [the Explorer section](#the-explorer-filters) below.

## Why this file has no comments

**varve GQL has no comment syntax.** The tokenizer treats `-` as `Minus` and `/`
as `Slash` (`crates/varve-gql/src/token.rs`); there is no `--`, `//`, or `/* */`
form. A `.gql` file with a comment header fails to parse, so all prose lives here
instead and `blast-radius.gql` stays pure query text that can be POSTed verbatim.

## Q1 — blast radius, today

Seven hops. Read it right-to-left from the CVE: the anchored `VulnID` →
`CertifyVuln` → the vulnerable `log4j-core` `PkgVersion` → `IsDependency` → the
*product* that depends on it → its `IsOccurrence` → `Artifact` → `HasSBOM`.

**The tail is three hops, not one.** An earlier draft went straight from
`product:PkgVersion` to `HasSBOM`; that edge does not exist. `HasSBOM` attaches to
an **`Artifact`**, never to a `PkgVersion`, so the path must route through
`IsOccurrence → Artifact`. This was the single biggest correction the T1 spike
forced on the plan (5 hops → 7).

Every node carries a label because **unlabeled nodes in a varve v1 pattern make
the whole match return empty** — silently, with no error.

## Q2, Q3, Q4 — derived by prefixing one clause

The pattern and `RETURN` are unchanged; `demo.sh` prepends a temporal clause:

| query | prefix | expected |
|---|---|---|
| **Q1** | *(none)* | the 7 affected images, none of the 5 controls |
| **Q2** | `FOR SYSTEM_TIME AS OF TIMESTAMP '<T_ship>'` | **zero rows** — the CVE facts had not been ingested yet; this is what the dashboard showed on ship day |
| **Q3** | `FOR VALID_TIME AS OF TIMESTAMP '<T_ship>'` | **same rows as Q1** — the facts were written *now* but with `VALID FROM 2021-12-10`, so as far as the world is concerned they were already true |
| **Q4** | `FOR VALID_TIME AS OF TIMESTAMP '2021-12-09T00:00:00Z'` | **zero rows** — one day *before* the CVE was published, proving valid-time really moves independently rather than being a relabelled system time |

Q2 vs Q3 is the whole pitch: same query, same connection, one clause apart, and
the gap between them is the exposure window. Q4 exists because without it a
sceptic can argue valid-time is just system time wearing a hat.

`<T_ship>` must come from the writer's own `POST /v1/tx` response
(`system_time`), **never from shell `date -u`** — the container clock can skew,
and the existing `gallery/guac/demo.sh` gets this wrong.

## The Explorer filters

`explore.sh` drives the same data from Varve Explorer. Three files support that,
and each looks the way it does for a reason worth knowing before you edit them.

### `explore-two-clocks.gql` — the Query view

```gql
RETURN cv._id AS fact, valid_from(cv) AS became_true, system_from(cv) AS we_recorded_it
```

`valid_from`, `valid_to` and `system_from` are **GQL functions** in varve
(`crates/varve-plan/src/functions.rs`), lowering to the `_valid_from` /
`_system_from` system columns. So the two clocks that the whole demo turns on fit
in one table with no time-travel clause at all: five rows, `became_true`
2021-12-10, `we_recorded_it` today.

The aliases are for reading, not for correctness: unaliased, the column simply
comes back named after the call (`"valid_from(cv)": "2021-12-10T10:15:00Z"`).

`valid_to(cv)` used to answer HTTP 500 here — see [the fix](#the-valid_to-500)
below. `system_to` is still `unknown function`, even though `_system_to` is a
real system column (`crates/varve-index/src/scan.rs`); of varve's four axis
endpoints, three are reachable from GQL.

### `explore-blast-radius.gql` — the Time travel filter

Q1's pattern with a deliberately mixed `RETURN`: **nodes whole, edges projected
to `_iid` / `_src_iid` / `_dst_iid`**. That split is not stylistic — each half is
forced by a different constraint, and collapsing either one breaks the demo.

1. **No `FOR` clause.** Explorer's Time travel view owns the temporal clause and
   rejects a filter that supplies its own — the timeline is what positions the
   axis.
2. **No named path.** `MATCH path = (…)` would prove topology exactly, and varve
   v1 rejects it outright: *"path variables need a single hop in v1"*. So the
   graph is reconstructed from the engine's system columns, which is what
   Explorer's entity-column extractor keys on — an alias carrying `_src_iid` and
   `_dst_iid` is a relationship, an alias with only `_iid` is a node.
3. **Edges must be projected, or the query is 50–120× slower.** A bare edge
   variable at the top level of a `RETURN` is the one reference that makes
   `plan_fast_path` decline anchored pruning, because it depends on the shared
   edge batch's column *set* rather than on which rows it keeps. Measured on
   this corpus, warm, against `query-1` — **in both residency states, because the
   pruned path depends on residency and the full scan does not** (see
   "Performance caveat" below for what residency means and why it dominates):

   | `RETURN` form | block-resident | live-resident |
   |---|---:|---:|
   | every column projected | — | 34 ms |
   | bare **node** variables (`RETURN DISTINCT vid, bad, product`) | 46 ms | 21 ms |
   | nodes bare + edges projected — *this file* | 44 ms | 21 ms |
   | **one** bare **edge** variable (`RETURN vid._iid, cvv`) | **2 340 ms** | **2 509 ms** |

   Nodes are free; a single bare edge variable costs everything. Note the last row
   is the *only* one that did not improve when anchored lookups became degree-bound:
   a bare edge var declines pruning, and an unpruned full scan wants every row on
   every page, so nothing about decoding less per page helps it. That is what makes
   the penalty ~50× block-resident and ~120× on live rows — the pruned path got
   ~10× faster while the ceiling it is measured against stayed put. Timeline
   scrubbing reruns this on every click, so projecting edges is worth it in either
   state.
4. **Nodes stay whole only where the caption comes out right.** Explorer captions
   a node from its first non-underscore property. That is `vulnerabilityID`,
   `purl` and `purl` for `vid`, `bad` and `product` — exactly what you want — but
   `collector` for `CertifyVuln`, `IsDependency` and `IsOccurrence`, and
   `algorithm` for `Artifact`, so half the graph would read "FileCollector" and
   "sha256". Those four are projected to `_iid` + `_labels` only, which makes the
   caption fall back to the label. `HasSBOM` is projected for a second reason:
   whole, it carries a ~70 KB `includedDependencies` blob.

5. **The two `valid_from` lines set the timeline.** Time travel frames its window
   on the instants a result reports for the axis in play, so the filter, not the
   operator, decides where Dec 2021 is:

   ```gql
   valid_from(cv) AS cve_valid_from, valid_from(product) AS product_valid_from
   ```

   `product` became valid 2021-12-08, `cv` 2021-12-10T10:15Z; the view pads that
   extent by 10% and lands on ~Dec 7 18:10 → Dec 10 16:04. The CVE flip sits
   inside the window, which is the whole of act 2 — and it removes the
   custom-interval typing *and* the CET boundary trap that made `Dec 8 00:00`
   read as an hour too early. Three rules govern the aliases:

   * **The column name must contain the function name.** The view matches by name
     (`valid_from` / `valid_to` on the valid axis, `system_from` on the system
     axis). An unaliased call already satisfies that — its column *is*
     `valid_from(cv)` — so the aliases here are only for readability. Rename one
     `AS when_bad` and it quietly stops steering the timeline.
   * **Project one axis only.** Add `system_from(cv)` here and the fitted window
     would stretch from when the CVE became true to when this store recorded it
     — 4.5 years, showing neither end. Act 3 needs no such column: on the system
     axis in Dec 2021 the query returns zero rows, and an empty result leaves the
     window alone.
   * **They are free.** Measured warm against `query-1`: live-resident, 33 ms
     without them and 31 ms with; block-resident, this file measures 44 ms against
     46 ms for the same pattern with no temporal calls — within run-to-run noise in
     both states, and the ordering flips run to run. Unlike a bare edge variable, a
     temporal call lowers to a system column and does not disturb anchored pruning.

   **`valid_to` is not what you want here even though it now works.** On an
   open-ended fact it answers `"9223372036854775807us"` — `Instant::END_OF_TIME`
   in raw µs — which is not an instant to frame a window with, so only the
   `*_from` calls appear above. The view ignores it either way: a value that is
   not an ISO-8601 datetime never enters the extent.

#### The `valid_to` 500

This demo found a real defect while wiring the fitting up. `valid_to(cv)` used to
answer `{"code":"internal"}`, with `expected ',' or '}' at line 1 column 98` in
the server log — a *JSON* error, from a query that planned and executed fine.

Handed `i64::MAX` µs, arrow-json cannot render a calendar date and writes its own
error text into the output stream instead of failing:

```
[{"vt":"ERROR: Cast error: Failed to convert 9223372036854775807 to datetime for Timestamp(µs, "UTC")"}]
```

Those inner quotes around `UTC` are unescaped, so the whole response stopped being
JSON — one open-ended fact anywhere in a result set took the entire query down
with an opaque 500. Fixed in `crates/varve/src/rows.rs`, which now masks
unrenderable µs instants before handing the batch to arrow and writes
`Instant`'s own raw-µs spelling into those cells — the same convention
`TxResponse` already used for out-of-range instants.

It survived to here because every existing test asserted on Arrow `RecordBatch`
values (`crates/varve/tests/temporal.rs` covers `valid_to(p)` and passes), and
the corruption only happens in the JSON encoder the HTTP layer adds on top.
`crates/varve/tests/rows.rs` now covers that seam end to end.

The result is 17 lines, 8 rows, 52 columns, ~19 KB, and ~44 ms block-resident
(~20 ms while rows are still live) — and it leaves room for the graph and the
timeline on screen, which a fully-projected 50-line version did not.

Every edge still renders as type `E`: that is GUAC's model, not a bug.

### `explore-sboms.gql` — the control

Twelve `Artifact -> HasSBOM` pairs, no CVE anywhere in the pattern. On the valid
axis at ship day it shows all twelve SBOMs present while the blast-radius filter
shows nothing, which is what makes "we had the evidence and could not have known"
a statement about the data rather than about an empty database.

## Performance caveat

**Latency here depends on where the rows live — now ~2×, and it was ~30× until
anchored lookups became degree-bound.** Every number in this file is therefore
given twice. Keeping both columns is the point, not pedantry: that ~30× gap was
the only visible symptom of a real defect, and it is worth treating a residency
gap as something to explain rather than to note.

* **block-resident** — rows have been written out to flushed blocks. This is the
  steady state: it is what you get after any node restart, and after the
  background flush timer fires (`flush_interval_ms`, default `300000` — 5
  minutes; the gallery TOMLs do not override it). **Quote these numbers.**
* **live-resident** — rows are still in the writer's unflushed in-memory live
  table, which is only true in the few minutes between `demo.sh` finishing its
  ingest and the first flush. Faster, but transient.

Q1 measures **35 ms** block-resident (**18 ms** live-resident) on the full corpus,
against the plan's 250 ms target (S4) — met 7× over in both states. Two separate
bugs had to go for that. The `{kind: '…'}` edge-predicate bug that made this ~2.4 s
was fixed in `ba6a555` (`docs/plans/2026-07-28-edge-predicate-pruning.md`), which
left it at ~460 ms; the ceiling behind that was per-hop rather than per-query — an
anchored lookup against flushed blocks decoded a whole 1,024-row page to find the
anchor's handful of rows, so each hop cost ~60 ms regardless of how few nodes the
frontier held. Fixed by filtering the decode on the page's key column
(`docs/plans/2026-07-28-degree-bound-lookups.md`), which also cut the
block-vs-live spread from ~30× to ~2×.

Still worth quoting `varve_live_bytes` alongside any timing you take here: the
30× spread existed for a whole afternoon with nothing visible to explain it, and
two of this repo's documents recorded numbers from opposite residency states
without saying so.

Removing the predicates is not an option here — they are what distinguishes the
seven edge kinds.
