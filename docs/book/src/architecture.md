# Architecture overview

This chapter condenses the [design specification](../../design/2026-07-04-varve-design.md)
(§3, §5, §7, §9, §12) into an operational sketch. Read the spec for the full rationale and
algorithmic detail; this page is a map, not the territory.

## Two golden stores, three roles

Varve keeps all durable state in exactly two places, and every process is otherwise stateless
and disposable:

1. **Transaction log** — an ordered stream of *resolved* transaction effects.
2. **Object store** — immutable Arrow files: blocks, tries, and manifests.

Three composable roles run over that shared state:

- **Writer** (designated, exactly one) — parses and plans incoming GQL, executes DML against
  its own snapshot, resolves the result into effect events, group-commits them to the log,
  applies them to its live in-memory index, and periodically flushes full blocks to the object
  store.
- **Query nodes** (zero or more, stateless) — tail the log, apply the same resolved effects to
  their own live index, and serve reads from a combination of that live index and cached
  object-store blocks. Because they apply already-resolved effects rather than re-executing
  GQL, query nodes need no write-side query engine, and replay is deterministic by
  construction.
- **Compactor** (any node) — a deterministic, coordination-free background job that merges
  raw blocks into a compacted trie hierarchy (see below). Any node can run it, and duplicate
  work is merely wasted CPU, never corruption, because compaction output is byte-identical
  regardless of which node produced it.

A laptop deployment runs all three roles in one process, with the log as a local file and
storage as a local directory — zero external dependencies. A server deployment runs one
writer process (its uniqueness enforced by the deployment orchestrator, e.g. a systemd unit or
a Kubernetes `StatefulSet` of size 1), N query processes, and a compactor embedded in the
writer or run standalone.

Varve needs only **one** log, not the source+replica pair XTDB requires: clients submit GQL to
the writer's own API, and the writer resolves DML against its snapshot at the current log head
before appending only the *resolved* effects. Multi-writer log submission (unresolved
transactions queued by multiple writers) is an explicit v1 non-goal, kept open behind the
`Log` interface for a future slice.

## The event model

Every mutation becomes an immutable **event** with five fields: an internal id `_iid`
(`xxh3_128(graph, table, _id)`, giving uniform key distribution regardless of user id
patterns), a writer-assigned monotonic `_system_from`, a user-controllable `_valid_from` /
`_valid_to` pair (defaulting to "now" and "forever" respectively), and an operation —
`put(doc)`, `delete`, or `erase` (Varve's GDPR hard-delete extension).

The crucial design choice: **`_system_to` and effective valid ranges are never stored** — they
are *derived* at read time by bitemporal resolution (the spec's Ceiling/Polygon algorithms,
ported from XTDB). This is what keeps the store append-only: correcting or superseding a fact
never rewrites an existing event, it only appends a new one, and resolution reconstructs "what
was true, and when we believed it" by scanning events for one `_iid` newest-system-time-first.
Resolution happens in exactly two places — query-time scan and compaction — never at write
time.

Edges carry the same event shape plus `_src_iid` / `_dst_iid`, and are persisted under three
tries (primary `_iid`, plus forward and reverse adjacency) so both directions of traversal are
served without a join.

## Blocks, tries, and the manifest as commit point

Each table's live index is an in-memory hash trie (branch factor 4 over `_iid` bits) built on
Arrow array builders. At a configured row threshold or flush timeout, the writer serializes
that trie's data to the object store as a new **block**, then writes a **block manifest** —
and the manifest write is the atomic commit point: a data file that exists without a
corresponding manifest entry is invisible garbage, safely cleaned up later. Manifests are
database-wide, recording the log-position watermark up to which their blocks are complete, so
recovery is just "find the latest manifest, replay the log from its watermark, resume."

Compaction is a deterministic, coordination-free function of the current trie inventory: raw
blocks split by recency (current vs. weekly historical buckets), then merge upward through
levels (four same-partition files at level *n* become one file at level *n+1*, partitioned by
the next two `_iid` bits), with `erase` events physically dropping matching rows for good.
Because job selection and output are pure functions of the inputs, any node can run any
compaction job without coordination.

## Group commit

The writer batches concurrently submitted transactions for up to a configured time window or
byte threshold, writes them to the log as a single append or object, and then acknowledges all
of them together. This amortizes log-backend latency (a PUT to an object-store log, an fsync
to a local log) across a batch, so commit latency is roughly "backend write latency plus
window," while throughput scales with batch size rather than per-transaction overhead.

## Epoch fencing and coordination

Log positions are `(epoch, offset)` pairs, with the epoch in the high bits so packed positions
still sort correctly. In the default **designated-writer** mode, exactly one writer per
database is enforced by the deployment itself, and its identity is published in the object
store as a plain, unconditional `PUT` — no conditional-write semantics required anywhere in
this mode, which is why it works identically on every S3-API backend, sovereign or not.
**CAS failover** is an opt-in mode, gated by a startup capability probe against the backend:
only backends that pass the probe (verified conditional-PUT semantics, including the
versioned-bucket edge case) are allowed to run it. Standby writers race a lease object; the
winner increments the log epoch, which fences the old writer's appends by making them visible
as stale. If a backend fails the probe, Varve refuses `cas-failover` mode outright with an
error naming the missing capability, rather than silently degrading — see
[Failover](ops/failover.md) for the operational detail and [Backends](backends.md) for the
per-backend probe results.

## Determinism, end to end

Determinism is a load-bearing property, not an incidental nicety: query nodes apply the
writer's resolved effects rather than re-executing GQL, so every node reaches byte-identical
state from the same log prefix; compaction output is byte-identical regardless of which node
produced it, so duplicate compaction work is wasted CPU rather than a correctness hazard. This
is what lets Varve scale reads horizontally and run compaction opportunistically on any node
without a coordination protocol.

## Where the numbers live

This page deliberately carries no performance figures. See the
[v1 benchmark report](../../benchmarks/v1.md) for measured throughput and latency against the
design spec's §13 targets, with the exact command that reproduces each number.
