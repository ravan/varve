# Introduction

VarveDB is a **bitemporal property-graph database** written in Rust that speaks **GQL**
(ISO/IEC 39075), the new ISO graph query language. It runs *embedded* in a Rust process on a
laptop with minimal resources, and scales out to a storage/compute-separated deployment —
one designated writer plus any number of stateless query nodes — over any S3-API-compatible
object store.

The library crate is `varve`, the server binary is `varved`, and the CLI is `varve`.

## The sovereignty stance

Digital sovereignty is a design requirement for Varve, not an afterthought. Varve depends on
nothing but a filesystem or an S3-API-compatible object store, and is validated against fully
open-source, self-hostable backends: [Garage](https://garagehq.deuxfleurs.fr/) (AGPLv3),
[Ceph RGW](https://docs.ceph.com/en/latest/radosgw/) (LGPL-2.1), and
[SeaweedFS](https://github.com/seaweedfs/seaweedfs) (Apache-2.0). AWS S3 works too, but it is
never assumed, and no proprietary cloud service is ever required to run Varve at any scale.

Concretely, this means Varve's storage layer speaks only the plain-S3 verbs every
implementation supports — `PUT`, `GET`, `LIST` — and treats conditional writes
(compare-and-swap) as an *optional*, capability-probed accelerator for automated writer
failover, never a requirement. A designated-writer deployment with no CAS at all works
identically on every backend; see [Architecture overview](architecture.md) and
[Failover](ops/failover.md) for how that split works.

## Why "varve"

A *varve* is an annual layer of sediment deposited in a lake bed — geologists read time out of
these layered deposits the way tree rings read a forest's history, one deposit at a time,
never disturbing the layers beneath. That is exactly how this database reads history: every
write is an immutable, timestamped layer; nothing is ever overwritten or rewritten in place;
and the whole history of the graph is recoverable by reading back through the strata. The
bitemporal model formalizes two independent time axes — *valid time* (when a fact was true in
the world) and *system time* (when Varve learned about it) — so you can travel along either
axis independently, correct the past without losing the correction's own history, and answer
"what did we believe on this date" as easily as "what do we believe now."

## Where to go next

- **[Getting started](getting-started.md)** — running Varve embedded from source in five
  minutes, plus a minimal server + CLI setup.
- **[Architecture overview](architecture.md)** — the roles, the event model, and the
  storage/compaction pipeline, condensed from the design spec.
- **[Design specification](../../design/2026-07-04-varve-design.md)** — the full, authoritative
  design document this book is derived from.
- **[v1 implementation roadmap](../../plans/varve-v1-roadmap.md)** — the slice-by-slice plan
  that built Varve v1.
- **[v1 benchmark report](../../benchmarks/v1.md)** — measured performance against the design
  spec's §13 targets, with reproduction commands for every number.
