# Failover

Varve supports two writer-coordination modes, selected by `[coordinator] backend`. Exactly one
is active per deployment; neither requires more than one **log** for the database (see the
[architecture overview](../architecture.md) for why Varve needs no source/replica log pair).

## `designated-writer` (default)

Exactly one writer process runs per database, and its uniqueness is enforced entirely by the
**deployment orchestrator** — a systemd unit, a Kubernetes `StatefulSet` of size 1, or simply
"only one process was started." The writer publishes its identity as a plain, unconditional
`PUT` object; no conditional-write semantics are required anywhere in this mode. This is why
`designated-writer` works identically on **every** S3-API backend, sovereign or not — see the
[backend matrix](../backends.md). If the writer process dies, recovery is manual (or orchestrator
-driven restart): there is no automatic standby takeover in this mode.

## `cas-failover` (opt-in, probe-gated)

`cas-failover` lets a standby writer automatically take over from a dead one, using the object
store's conditional-write (CAS) support to arbitrate. Because that support cannot be assumed on
every S3-API backend, Varve gates this mode behind a startup capability probe
(`Db::probe_capabilities`, [backend matrix](../backends.md)): **only a `Supported` verdict is
allowed to enable it.** A backend that fails the probe refuses `cas-failover` outright, naming
the missing capability, rather than silently degrading to something unsafe.

Mechanics: standby writers race a lease object; the winner **increments the log epoch**, which
fences the old writer's future appends by making them provably stale, then recovers from the
latest committed manifest and resumes serving writes. Log positions are `(epoch, offset)` pairs
with the epoch in the high bits, so packed positions still sort correctly across an epoch bump.

**Live proof:** `cargo run --release --example failover -p varve`
(`crates/varve/examples/failover.rs`) runs two writer handles sharing one in-memory object
store, kills the first (drops it without releasing its lease — a true crash, not a graceful
handoff), and asserts the standby takes over in under 10 seconds with **zero acked-transaction
loss**, then proves the dead writer's late append is durable-but-dead: a real object lands in
the store, but is invisible to the new writer, to a fresh query-only node, and to `verify()`. A
run captured while writing this page:

```
writer A acquired lease (epoch 0), committed 3 txs
writer A crashed (heartbeats stopped)
writer B took over in 303ms: epoch 1, fence 0@3
zombie append landed at (0,3) — IGNORED by B, query node, and verify
final row count everywhere: 4
```

(Run-to-run timing varies with machine load — STATUS.md's slice-10 close recorded 304 ms on a
separate run — but the example's own assertion is `takeover < Duration::from_secs(10)`, so any
passing run is comfortably under the 10-second bound; a 30-minute chaos soak,
`VARVE_CHAOS_SECS=1800`, survived 64 kills with 2,463 acked transactions all present and
`verify()` clean.)

`[coordinator]` config:

```toml
[coordinator]
backend = "cas-failover"
heartbeat_interval_ms = 100
takeover_after_ms = 300
```

## Manifest selection hardening (Task 4)

`latest_manifest` selects the newest committed manifest by **`(watermark, block_id)`**,
watermark-first — `block_id` only breaks ties between manifests at equal watermark (a
flush-vs-compaction race). This closed a known limitation from slice 10: because a compaction or
flush manifest `PUT` is the commit point for the object store and has **no epoch-fence
equivalent** (only the log is epoch-fenced), a fenced writer's stray manifest — one with a
higher `block_id` but a **stale** watermark — could previously have won recovery/verify/follower
reads by block_id alone. Selecting by watermark first makes that permanently impossible: a stale
watermark can never win regardless of block_id. The before/after lease ack-gate (a fenced writer
never acks a compaction `Ok` and instead goes fatal) remains the liveness guard that stops a
zombie from completing a stray manifest `PUT` in the first place; watermark-first selection is
the correctness backstop if it somehow did.

## GC, followers, and `LogGap` (Task 1)

Garbage collection sweeps superseded `v1/log/**.vlog` objects — but only those **wholly below
the minimum retained manifest watermark** (the minimum `watermark` across the manifest set
`blocks_to_keep` protects), making the object-store log's `trim` a real no-op-safe operation
rather than an aspirational one. This has a direct operational consequence: **a query follower
that has lagged behind the writer by more than the retained watermark window loses its log tail
and terminates fail-stop with `LogGap`**, rather than silently serving a gap in history. This is
the correct, intentional behavior — a genuine, fence-unexplained gap in a follower's or
`verify()`'s log read is a correctness hazard, not something to paper over — and the follower
recovers cleanly by restarting from the latest manifest. Size `[gc] blocks_to_keep` (default 10)
to comfortably exceed your slowest follower's realistic lag; see
[sizing knobs](profiles.md#sizing-knobs) for the tuning table. A legitimate epoch bump during
`cas-failover` recovery is not mistaken for a gap (the same fence-aware contiguity check
`follower` and `verify` both use recognizes it as an intentional jump, not a hole).
