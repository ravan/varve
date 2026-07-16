# Backends

Varve speaks to any S3-API object store through `object_store::aws` (`crates/varve-storage/
src/s3.rs`), adapted to Varve's own `ObjectStore` trait, so the engine only ever sees
`put`/`get`/`get_range`/`list`. Conditional-write (CAS) support is probed, never assumed (spec
D5): a 4-step startup probe (`Db::probe_capabilities`) classifies a backend as `Supported`,
`Unsupported`, or `Inconsistent`, and only a `Supported` verdict is allowed to run the opt-in
`cas-failover` coordinator mode. See [Failover](ops/failover.md) for what that mode buys you and
[the architecture overview](architecture.md) for why the default `designated-writer` mode needs
no conditional-write semantics at all.

## Matrix

Image pins and probe verdicts below are read directly from
`crates/varve-testkit/src/backends.rs` and STATUS.md's recorded slice-5 probe run; the CI
cadence matches `.github/workflows/ci.yml`'s `backend-matrix` (push/PR) and
`backend-ceph-weekly` (cron) jobs.

| Backend | Version tested (CI pin) | Probe verdict | `cas-failover` | CI cadence |
|---|---|---|---|---|
| Garage | `dxflrs/garage:v1.0.1` | Inconsistent (precondition ignored) | refused (by design) | every push/PR |
| SeaweedFS | `chrislusf/seaweedfs:3.80` | Inconsistent | refused | every push/PR |
| MinIO | `minio/minio:RELEASE.2025-04-22T22-12-26Z` | Supported | available | every push/PR (legacy note: repo archived 2026-04) |
| Ceph RGW | `quay.io/ceph/demo:latest-quincy` | (weekly job; verdict recorded when the cron runs) | per probe | weekly |
| AWS S3 | n/a | expected Supported | per probe | **not CI-verified** (documented gap; config-compatible via `[storage.s3]`) |
| Local FS | n/a | Supported (blanket impl) | n/a (single node) | every push/PR |

Garage and SeaweedFS both fail the probe the same way: a create-if-absent `PUT` against an
existing key succeeds instead of being refused. The precondition is silently ignored rather
than enforced, which the probe's four steps (create → create-again-must-refuse →
swap-current-etag → swap-STALE-must-refuse) are designed to catch. Both assert
`NotSupported` (the negation `cas-failover`'s gate actually checks), not a pinned
`Inconsistent{reason}` string, since backend-specific wording could legitimately change.

The image pins above are the single source of truth in `backends.rs`, reproduced here
verbatim for reference (do not duplicate the version strings anywhere else):

```rust
pub const GARAGE_IMAGE: &str = "dxflrs/garage:v1.0.1";
pub const SEAWEEDFS_IMAGE: &str = "chrislusf/seaweedfs:3.80";
pub const MINIO_IMAGE: &str = "minio/minio:RELEASE.2025-04-22T22-12-26Z";
pub const MC_IMAGE: &str = "minio/mc:RELEASE.2025-04-16T18-13-26Z";
pub const CEPH_IMAGE: &str = "quay.io/ceph/demo:latest-quincy";
```

Run the live matrix yourself with `VARVE_S3_BACKENDS=all just s3-matrix` (or a comma-list of
specific backend names); it skips silently without Docker, so `just check` never needs it.

## `[storage.s3]` configuration

All five S3-API backends share one config section (`crates/varve-storage/src/s3.rs`); only
`bucket` is required. The builder starts from `AmazonS3Builder::from_env()` (standard `AWS_*`
environment variables and the AWS provider chain), so explicit config keys are overrides, not
requirements. An AWS deployment using ambient credentials needs only `bucket`.

```toml
[storage]
backend = "s3"

[storage.s3]
bucket = "varve"
# All of the below are optional.
endpoint = "http://127.0.0.1:3900"    # omit for AWS's own endpoint resolution
region = "garage"                     # Garage requires this to match its s3_region
access_key_id = "varve"
secret_access_key = "varvesecret123"
path_style = true                     # DEFAULT true (Garage/MinIO); false = virtual-hosted (AWS default)
allow_http = true                     # DEFAULT: derived from the endpoint scheme (http:// => true)
```

**Garage / MinIO / SeaweedFS / Ceph RGW (self-hosted, loopback or in-cluster):** set `endpoint`
to the backend's HTTP(S) address, keep `path_style = true` (the default), and set `region` only
if the backend enforces one (Garage does, conventionally `"garage"`).

**AWS S3:** omit `endpoint` entirely (falls back to AWS's own regional endpoint resolution), set
`region` to your bucket's actual region, and either supply `access_key_id`/`secret_access_key` or
rely on the ambient environment/instance-role credential chain. `path_style` should be `false`
for AWS proper, though AWS still accepts path-style requests for backward compatibility.

## Sovereignty

Plain, unconditional `PUT`/`GET`/`LIST` is always sufficient to run Varve in its default
`designated-writer` mode: no backend capability beyond basic S3-API object storage is
required, which is why Varve runs identically on a fully sovereign, self-hosted, open-source
backend (Garage, SeaweedFS, Ceph RGW) as it does on AWS. Conditional-write (CAS) support is
strictly optional. It unlocks the opt-in `cas-failover` coordinator mode, is detected by a
startup probe rather than assumed, and its absence is never an error for normal operation. A
backend that fails the probe simply cannot run `cas-failover`, and falls back to (or stays on)
`designated-writer`, enforced by the deployment orchestrator instead of the object store. Varve
will never make CAS a hard requirement for shipping v1 functionality.
