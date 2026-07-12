# Deployment profiles & sizing

Every profile below runs the same binary and the same wire format — only `varve.toml` changes.
See the [configuration reference](configuration.md) for the full key list.

## Laptop (memory or embedded, zero external dependencies)

`Db::memory()` or `Db::local(dir)` — everything in one process, no `varved`/HTTP layer at all.
This is [Getting started](../getting-started.md) Path 1. Good for development, tests, one-off
scripts, and the CLI's `--dir` mode. The in-process `Db::memory()` also uses a group-commit
window of **0 ms** (commit immediately, no batching) rather than the on-disk default of 15 ms,
since there is no backend write latency to amortize.

## Durable single node

One process, all three roles (`writer`, `query`, `compactor`), a **local** log file and a
**local** storage directory — no object store, no network. This is
[Getting started](../getting-started.md) Path 2's `varve.toml` shape:

```toml
[node]
roles = ["writer", "query", "compactor"]
[log]
backend = "local"
[log.local]
dir = "./data/log"
[storage]
backend = "local"
[storage.local]
dir = "./data/store"
```

Good for a single server or VM that needs crash-durability and a network API, but no horizontal
read scale-out or multi-writer failover.

## Sovereign scale-out

Object-store log + object-store block storage (any backend from the
[backend matrix](../backends.md)), **one writer** plus **N query nodes**, all reading and
writing the same bucket. This is how you get horizontal read scaling and (optionally) automatic
writer failover. `just compose-demo` (`scripts/compose_demo.sh`) is a runnable reference
topology: pinned Garage + a distroless writer + 2 query nodes, driven over HTTP, verifying both
query nodes agree on a basis read before tearing everything down.

```toml
[node]
roles = ["query"]              # a query-only node; the writer uses ["writer", "compactor"]
[log]
backend = "object-store"
[storage]
backend = "s3"
[storage.s3]
bucket = "varve"
endpoint = "http://garage:3900"
region = "garage"
```

Add `[coordinator] backend = "cas-failover"` only on backends with a `Supported` probe verdict
— see [Failover](failover.md).

## Sizing knobs

Defaults come straight from `crates/varve-engine/src/db.rs` and `crates/varve-storage/src/
{cache,disk}.rs`; every one is overridable in `varve.toml` and every default is chosen to be
reasonable, not merely "small enough to demo."

| Knob | Section | Default | Turn it when… |
|---|---|---|---|
| `[cache] tiers` (`memory`, `disk`) | `[cache]` | a 512 MiB memory tier only (`Db::memory()`/`Db::local()`) | Add a `disk` tier under `[cache.disk] dir` (default 50 GiB) whenever cold reads from the object store dominate latency — the disk tier survives restarts (self-describing files keyed by `xxh3_128(path, range)`), so a warm restart doesn't re-pay the network round trip. |
| `[cache.memory] max_bytes` | `[cache.memory]` | 512 MiB | Raise it if your working set of hot blocks exceeds 512 MiB and you have spare RAM; lower it on memory-constrained nodes (a query-only node with many concurrent readers is the usual candidate). |
| `[cache.disk] max_bytes` | `[cache.disk]` | 50 GiB | Size to your actual object-store round-trip cost and available local disk; this is a pure LRU budget, not a correctness knob. |
| `[storage] max_live_bytes` | `[storage]` | 512 MiB | Forces an early block flush once the writer's in-memory live-table footprint reaches this, independent of row count — lower it on memory-constrained writers or workloads with unusually large property values per row. |
| `[storage] max_block_rows` | `[storage]` | 100,000 | Raise it to produce fewer, larger blocks (less compaction overhead, more per-flush memory); lower it for tighter flush latency or memory-constrained writers. |
| `[storage] flush_interval_ms` | `[storage]` | 300,000 (5 min); `0` disables the timer | Lower it if you need a tighter bound on "how stale can the object store be relative to the log" independent of row/byte thresholds; disable it only if `max_block_rows`/`max_live_bytes` alone are enough for your write rate. |
| `[log] group_commit_window_ms` | `[log]` | 15 ms (`Db::memory()` uses 0) | Raise it to amortize backend write latency across a bigger batch under high concurrent write load (throughput over latency); lower it (toward 0) when per-transaction commit latency matters more than batch efficiency. |
| `[log] group_commit_max_bytes` | `[log]` | 8 MiB | The other half of the group-commit trigger (window elapses **or** this size is reached, whichever first) — raise it alongside the window for very high-throughput writers; lower it if a single batch object is bumping into backend size limits or latency cliffs. |
| `[gc] blocks_to_keep` | `[gc]` | 10 (GC itself is `enabled = false` by default) | Raise it if you run long-lived query followers or basis reads that may lag the writer by more than 10 flushed blocks — GC never deletes a log object a retained manifest's watermark still needs, but a follower that falls behind *that* retention window terminates fail-stop with `LogGap` (see [Failover](failover.md)) rather than silently serving stale data, so size this to your slowest follower's realistic lag, not just your fastest. |
| `[gc] garbage_lifetime_hours` | `[gc]` | 24 | Raise it for extra recovery margin against in-flight readers/backups before superseded objects become GC-eligible; lower it to reclaim object-store space faster once you're confident nothing still needs the older generation. |
