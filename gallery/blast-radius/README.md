# Blast radius: "what did you know, and when did you know it?"

A supply-chain evidence graph that is **bitemporal** can answer an auditor's real
question. This demo asks the same question four times, changing exactly one
clause, and gets three different answers — the difference between *what we knew on
ship day* and *what was actually true on ship day*.

```sh
sh gallery/blast-radius/demo.sh          # tears the stack down on exit
sh gallery/blast-radius/demo.sh --keep   # leave it up to poke at
sh gallery/blast-radius/explore.sh       # the same thing, in a browser
```

Offline apart from pulling the stack's own images: the SBOM corpus is committed.

## The scenario

Twelve real, digest-pinned container images from November/December 2021. Seven
genuinely bundle a `log4j-core` inside CVE-2021-44228's affected range; five are
clean controls. They are ingested **backdated to ship day** (2021-12-08) — the
earliest date on which all twelve existed, and one day before Log4Shell went
public. Then Log4Shell is published, and we ingest that fact **backdated to its
real publication date** (2021-12-10T10:15:00Z).

Two backdated ingests, two days apart, both written *today*. Every answer below
comes out of the gap between them.

Now the four queries — all the same seven-hop pattern from
[`queries/blast-radius.gql`](queries/blast-radius.gql), differing only in a
leading temporal clause:

| | clause | answer |
|---|---|---|
| **Q1** | *(none — today)* | the 7 affected images |
| **Q2** | `FOR SYSTEM_TIME AS OF T_ship` | **nothing.** What the dashboard showed on the day we shipped |
| **Q3** | `FOR VALID_TIME AS OF T_ship` | **the same 7.** What was *true* on the day we shipped |
| **Q4** | `FOR VALID_TIME AS OF <the day before>` | **nothing.** Valid-time really moves independently |

> `T_ship` is the **system** instant of the last corpus transaction — captured
> from the writer at demo time (2026), the value `demo.sh` prints and
> `explore.sh` reuses — **not** the 2021-12-08 valid-time ship date. Typing the
> valid date literally, `FOR VALID_TIME AS OF '2021-12-08…'`, returns **0 rows**,
> because the CVE fact is not valid until Dec 10 — the opposite of Q3's "the same
> 7". For the valid-axis view of ship week, see **Act 2 / Act 3** below. `<the
> day before>` in Q4 *is* a valid-time literal (`2021-12-09`), which is why it
> reads differently.

**Q2 vs Q3 is the whole point.** Same query, same connection, one clause apart.
The gap between them is the exposure window — the interval where you were
vulnerable and did not know it. Q4 exists because without it a sceptic can
reasonably argue that valid-time is just system-time wearing a hat. Note *why*
Q4 is empty: the images are all there at 2021-12-09, because the corpus is valid
from ship day. It is specifically the CVE that is not true yet.

A store with one time axis can express Q1. It cannot express the difference
between Q2 and Q3, because it has nowhere to record that a fact *became true*
before it *became known*.

## Driving it from a browser

`demo.sh` answers the four questions with curl. `explore.sh` points
[Varve Explorer](../../explore) at the same query-only follower so you can
answer them with a mouse instead, which is a better way to show someone:

```sh
sh gallery/blast-radius/demo.sh --keep   # ingest once (~10 min)
sh gallery/blast-radius/explore.sh       # then the UI, as often as you like
```

`explore.sh` runs the ingest itself if the store is empty, and prints the
instants and queries for three acts. Connect with `varve-demo-token`.

**Two traps first, because both produce an empty screen rather than an error.**
`explore.sh` prints every query in full precisely so you never have to pick a
file or a box; copy from its output.

- **`blast-radius.gql` is not `explore-blast-radius.gql`.** The first is
  `demo.sh`'s Q1 — anonymous edges and a scalar `RETURN DISTINCT product._id,
  s.uri`. In the Time travel filter it draws **nothing at all** while still
  returning seven perfectly correct rows, because that view renders graphs and
  has no table tab. It is also the name you would reach for. varve GQL has no
  comment syntax, so the warning cannot live inside the `.gql` file that needs
  it — only here and in `explore.sh`.
- **The two GQL boxes are not interchangeable.** Act 1 belongs in **New query**;
  acts 2 and 3 belong in **Time travel**. Both are in the left sidebar and both
  accept GQL, but the Time travel filter needs whole node/edge variables. Give it
  a scalar `RETURN` and you correctly get *"Graph topology is unavailable because
  the rows contain no returned entities. Return whole variables, for example
  RETURN a, r, b."* Nothing is broken; that is the right answer to the wrong
  question.

**Act 1 — two clocks, one row.** In **New query**, paste
[`queries/explore-two-clocks.gql`](queries/explore-two-clocks.gql).
`valid_from` and `system_from` are GQL functions, so the CVE fact's two
timestamps come back in one table: it *became true* on 2021-12-10 and we
*recorded it* today. Everything else follows from that one row.

**Act 2 — the blast radius appears.** In **Time travel** set the axis to
valid time and the filter to
[`queries/explore-blast-radius.gql`](queries/explore-blast-radius.gql), then press
Go. The interval needs no typing: that filter projects `valid_from(cv)` and
`valid_from(product)`, and the view frames its window on the instants a result
reports for the axis in play — here Dec 2021, with the CVE flip inside it. Drag the
timeline: nothing until the CVE becomes valid, then 47 nodes and 47
relationships — seven images, each through its own `log4j-core` →
`IsDependency` → `IsOccurrence` → `Artifact` → `HasSBOM` chain. The SBOMs did
not change; only whether the CVE was true yet did.

**Act 3 — and we knew none of it.** Switch the axis to system time over the same
December 2021 window: empty at every instant, because in system time this store
knew nothing in 2021. Then park the handle on **Dec 9** and compare two filters
on the valid axis:

| filter | nodes | |
|---|---:|---|
| [`explore-blast-radius.gql`](queries/explore-blast-radius.gql) | 0 | nothing to act on |
| [`explore-sboms.gql`](queries/explore-sboms.gql) | 24 | all twelve SBOMs, right there |

That pair is the point: the evidence was in hand and the conclusion was not
reachable from it. For the literal Q2-vs-Q3 pair, set the interval to *Last 1
hour* and scrub to the `T_ship` that `explore.sh` prints — on the system axis the
blast radius is empty, and flipping to the valid axis without moving the handle
brings the seven images back. Same instant, same follower, one setting apart.

Four things to know before presenting, none of them faults:

- **Connection status reads "Degraded".** Correct, and not about the data: Garage
  ignores the create-if-absent precondition varve probes for, so the probe
  verdict is `inconsistent`. See [backends](../../docs/book/src/backends.md).
  Reads and time travel are unaffected.
- **The timeline is local time.** A fact valid from `10:15Z` flips at 11:15 in
  CET. For the same reason the instructions say Dec 9 and not ship day itself:
  the corpus becomes valid at `2021-12-08T00:00Z`, and "Dec 8 00:00" typed into
  a CET picker is 23:00Z on Dec 7 — an hour early, which looks like a broken
  demo.
- **Collapse the filter box before you present** — the `−` beside its title,
  which keeps the first line and survives reloads. Measured at 1280 wide with
  this filter, the timeline is fully on screen from **~800 px** of viewport
  height collapsed, but needs **~1410 px** expanded: the graph canvas is
  `flex: 1`, so it absorbs most of any height you add and an expanded 17-line
  filter never really catches up. At 720p, collapse it *and* zoom to 90%.
- **The filter must not carry its own `FOR VALID_TIME` / `FOR SYSTEM_TIME`** —
  the Time travel view owns the temporal clause and rejects a filter that
  supplies one. And every edge renders as type `E`, because that is genuinely
  GUAC's model (caveat 2); click one and the inspector shows its `kind`.

## Why this gap is worth caring about

From the Chainloop source (`refs/chainloop`), a production supply-chain
attestation platform on Postgres:

- `app/controlplane/pkg/data/referrer.go:294` — `const maxTraverseLevels = 1`.
  Its flagship "discovery" feature is capped at **one hop**, because recursive
  many-to-many traversal through `ent`/Postgres with per-level ACL predicates does
  not scale. This demo's query is seven hops.
- `app/controlplane/pkg/auditor/{dispatcher,nats}.go` — audit events are published
  to NATS and **never persisted**. There is no queryable history at all, so
  "what did we know last Tuesday" is unanswerable in principle.

## What it measures

Numbers below are printed by `demo.sh` on a real run, not asserted here. Re-run it
and you will get your own.

Measured 2026-07-28 on an M-series laptop (Docker Desktop), 12-image corpus.

**The graph:**

| | count |
|---|---:|
| `PkgVersion` | 5,039 |
| `PkgName` | 3,850 |
| `IsDependency` | 12,600 |
| `IsOccurrence` | 1,701 |
| `Artifact` | 1,595 |
| `HasSBOM` | 12 *(one per image)* |
| `CertifyVuln` | 5 *(one per vulnerable `log4j-core` purl)* |

5,039 real `PkgVersion` from 6,694 SPDX packages, and **zero**
`pkg:v:guac/files/…` pseudo-packages — the SBOMs are file-stripped, so this is a
count of actual packages.

**The queries**, all served by `query-1` (a read-only follower), warm. Latency
depends on **where the rows live**; quote the block-resident column, which is the
steady state (see caveat 4):

| query | rows | block-resident | live-resident |
|---|---:|---:|---:|
| **Q1** blast radius, today | **7** | **35 ms** | 18 ms |
| **Q2** `SYSTEM_TIME AS OF` ship day | **0** | 1.7 ms | 1.4 ms |
| **Q3** `VALID_TIME AS OF` ship day | **7** | **39 ms** | 18 ms |
| **Q4** `VALID_TIME AS OF` 2021-12-09 | **0** | 1.7 ms | 1.3 ms |

**Block-resident** is what you get after any node restart, and after the flush
timer fires (`flush_interval_ms`, default `300000` — 5 minutes). **Live-resident**
holds only in the few minutes between `demo.sh` finishing its ingest and that first
flush, while the rows are still in the writer's unflushed in-memory table; the
live-resident column above is exactly what `demo.sh` prints on a clean run.

These are warm timings from one clean `demo.sh` run, not a p50 of many — which is
all the demo claims. Q1 over six consecutive block-resident runs: 33.8–40.0 ms
(`varve_live_bytes` 17,118,764 for every one of them). The zero-row queries are fast
because they exit early, not because they are cheap to answer.

**Q1 has come down 74× in two steps, both on 2026-07-28**, with nothing in the
corpus, the queries or the compose file changing:

| | Q1 block-resident |
|---|---:|
| before either fix | ~2.6 s |
| `ba6a555` — `{kind: '…'}` edge predicates stopped disabling anchored pruning | ~460 ms |
| degree-bound anchored lookups — a page is no longer decoded whole to find one anchor's rows | **35 ms** |

The second step also collapsed the residency spread that used to make this table
confusing: block-resident was 30× live-resident, and is now ~2×. See caveat 4 and
`docs/plans/2026-07-28-degree-bound-lookups.md`.

The seven Q1 rows are exactly the seven images `corpus/MANIFEST` marks AFFECTED —
solr, flink, neo4j, druid, elasticsearch, sonarqube, logstash — and none of the
five controls. That agreement matters: the verdict column was derived from the
SBOMs' `log4j-core` versions, while the seven rows come from an independent
seven-hop traversal. Two different methods, same answer.

**Ingest:** 467 s (~39 s/image) for the whole corpus, plus a full compaction
sweep. Corpus expansion is instant; the cost is GUAC's one-statement-per-edge
mutation surface.

Worth noting the tail does real work: truncated to five hops the same query
returns **11** rows, because intermediate Maven artifacts (`elasticsearch-sql-cli`,
`neo4j-logging`, `logstash-input-tcp`) also depend on `log4j-core`. The
`IsOccurrence → Artifact → HasSBOM` tail is what narrows it to things that
actually have an SBOM, i.e. the shipped images.

## Honest caveats

Read these before quoting anything above.

1. **The CVE facts are injected by this demo, not fetched from OSV live.**
   `cve/cve-2021-44228.json` is a hand-authored attestation listing the five
   `log4j-core` purls the corpus actually contains. The demo is *not* claiming to
   have detected anything. It claims something weaker and far more defensible:
   that it can **reconstruct what was believed at a point in time**.
2. **`syft` SBOMs of container images are largely flat.** The seven hops come from
   GUAC's *reified evidence model* (`IsDependency`, `IsOccurrence`, `HasSBOM` are
   each nodes, not edges), **not** from a deep transitive dependency tree. Do not
   read "seven hops" as "seven levels of transitive dependencies" — it is not.
3. **All tokens and credentials here are demo-only fixed material**
   (`varve-demo-token`, the Garage keys in `deploy/garage.toml`). They are in the
   repo on purpose and are not secrets.
4. **Q1's 250 ms target is met, in both residency states — and getting there took
   two fixes, one of which was a misdiagnosis worth knowing about.** Q1 originally
   landed near 2.4 s, because every hop carries an edge-property predicate
   (`{kind: '…'}`) and that disabled varve's anchored pruning at ~7× per hop —
   GUAC models every edge type as a `kind` property on a single `:E` label, so
   every hop paid it. That family of `plan_fast_path` bails was removed in
   `ba6a555` (`docs/plans/2026-07-28-edge-predicate-pruning.md`), taking Q1 to
   ~460 ms. Anchored lookups then became degree-bound
   (`docs/plans/2026-07-28-degree-bound-lookups.md`), taking it to **35 ms** — 7×
   under target.

   **What "residency" means, and the trap it sets.** Rows start in the writer's
   unflushed in-memory live table and move to flushed blocks on any node restart,
   or when the background flush timer fires (`flush_interval_ms`, default
   `300000` — 5 minutes; not overridden here). Between `ba6a555` and the
   degree-bound fix, that difference alone was **30×** on this query: page pruning
   correctly narrowed each hop to a single data page, but that page was then
   decoded *whole* — 1,024 rows with every label and doc deserialized — to find the
   handful of rows belonging to the anchor. So each hop cost ~60 ms no matter how
   small the frontier was. The decode now filters on the page's key column before
   building a row, and the spread is ~2×.

   **Always quote `varve_live_bytes` next to a traversal timing.** Two honest
   measurements of the same query on the same store differed by 30× with nothing
   visible to explain it, and this README and
   `docs/plans/2026-07-28-edge-predicate-pruning.md` recorded numbers from opposite
   states without saying which — they read as a contradiction. Reproduce the
   demotion in seconds on a live stack with `docker compose restart query-1`, then
   re-run Q1 and compare `varve_live_bytes` on `/metrics`.
   `varve_block_pages_read_total` and `varve_block_events_decoded_total` now expose
   the ratio directly: Q1 reads 667 pages and materializes 2,947 events per query,
   4.4 per page.

   This was *not* about compaction — an earlier version of this caveat claimed the
   win "depends on the store being compacted … serving uncompacted is O(pages)",
   which is **backwards**. Running compaction to completion on a settled store
   changes Q1's latency not at all (measured); residency was the whole story.
5. **The affected/control split was read out of the SBOMs, not from image tags.**
   Four tag-based guesses were wrong — `nifi:1.15.0` carries no `log4j-core` at
   all, `druid:0.22.0` has 2.8.2 rather than 2.14.x, and `elasticsearch:7.16.0`
   and `sonarqube:9.2.2` are affected via 2.11.1 despite looking clean. The
   authoritative record is [`corpus/MANIFEST`](corpus/MANIFEST).
6. **Ship day is a chosen date, not a recovered one.** 2021-12-08 is when this
   demo *asserts* the images were shipped: the last full day before Log4Shell
   went public, and late enough that every image in `corpus/MANIFEST` already
   existed (the newest are the Elastic 7.16.0 images, released earlier that same
   month). But no SBOM in the corpus carries a build timestamp, so nothing here
   derives it. The CVE's 2021-12-10T10:15:00Z is real; 2021-12-08 is a plausible
   stand-in for a date a real pipeline would know. The mechanism being
   demonstrated does not depend on which date it is.

## How it fits together

```
corpus/*.spdx.json.gz   12 syft SBOMs, digest-pinned, byte-reproducible (~1 MB)
corpus/MANIFEST         image@digest, sha256, package count, log4j-core, verdict
cve/                    the backdated CVE attestation (see cve/README.md)
queries/                the seven-hop pattern + the Explorer filters (queries/README.md)
patches/                the GUAC --varve-valid-from patch (see patches/README.md)
refresh-corpus.sh       regenerates the corpus from the registry; NOT run by demo.sh
demo.sh                 the four queries, over curl
explore.sh              the same data, driven from Varve Explorer's timeline
docker-compose.yml      garage + writer + query-1 + TWO guacgql servers
```

Two `guacgql` servers, because **valid-time is a property of the server process**,
not of a request: the varve backend lives inside the GraphQL server, so
`--varve-valid-from` is a server flag. `guacone` is only a client picking a
`--gql-addr`. So one server per date — the corpus goes in through :8020
(`--varve-valid-from=2021-12-08`), the CVE facts through :8022
(`--varve-valid-from=2021-12-10T10:15:00Z`). Both write to the same writer and
the same graph; only the valid-time differs.

The four queries — and Explorer — are served by **query-1, a read-only
follower** that has replayed the same durable object-store log as the writer, so
the time-travel answers are not a writer-local trick.

`refresh-corpus.sh` regenerates the corpus from the registry (`syft --from
oci-registry`, no `docker pull`, same bytes on arm64 and amd64) and is
byte-reproducible: syft's per-run `documentNamespace` UUID and
`creationInfo.created` are normalised away, so a re-run reproduces `MANIFEST`'s
checksums exactly. `gzip -n` alone is **not** sufficient for that.
