# Varve v1.0.0 — Acceptance Report

**Branch:** `slice-11` (base `f72ca08` = slice-10 closed) · **Date:** 2026-07-12 ·
**Host:** Apple M3-Max-class (14 CPU), macOS Darwin 25.3.0 arm64, Rust 1.93.0.

This report walks the eight success criteria from the design spec §1 with concrete,
named evidence, records the whole-slice verification transcript, and states the explicit
exceptions the release ships with. It is the gate for the user-decided `v1.0.0` tag/publish.

## Spec §1 success criteria

| # | Criterion | Verdict | Evidence |
|---|---|---|---|
| 1 | Embeds as a library and serves over HTTP | ✅ MET | `crates/varve/examples/hello.rs` (embedded facade); `crates/varve-server/tests/http_api.rs` (route/auth/negotiation matrix); `just compose-demo` → **PASSED** (writer + 2 query nodes over HTTP). |
| 2 | GQL core passes an adapted TCK + temporal suite | ✅ MET (adapted) | `crates/varve-testkit/tests/tck.rs` gate at `PASS_RATE_GATE=0.85` — current rate **≈ 0.871** (445/511) with reasoned exclusions; temporal behavior in `crates/varve/tests/` + slice-2 property suites. **Honesty note:** this is an *adapted* TCK, not full-standard GQL conformance (see `docs/book/src/gql/deviations.md`). |
| 3 | Full bitemporality + GDPR erase | ✅ MET | slice-2 bitemporal property tests; `crates/varve/tests/erase.rs`; `crates/varve/tests/gdpr_gc.rs` — the full-byte (local profile) and raw-object (object-store-log profile) erase proofs (**5 passed** this run), each scanning every persisted byte after compaction + GC. |
| 4 | Local FS + Garage / Ceph / SeaweedFS / MinIO / AWS | ⚠️ MET with exception | CI `backend-matrix` (Garage, SeaweedFS, MinIO) + `backend-ceph-weekly`; capability probe + sovereignty enforced (`crates/varve-testkit/tests/backend_matrix.rs`). **Exception: AWS S3 is configuration-compatible (standard S3 API) but is NOT exercised in CI** — an explicit, acknowledged gap. |
| 5 | One writer + N query nodes, read scale-out | ✅ MET | `just compose-demo` (1 writer + 2 query nodes agreeing under a basis read) → PASSED; `crates/varve-server/tests/process_scale_out.rs`; `crates/varve-server/tests/scale_out_bench.rs` (env-gated 1/2/4-node QPS table — see `docs/benchmarks/v1.md`; single-box slope, re-measure on distributed HW). |
| 6 | Deterministic compaction, bounded storage | ✅ MET | golden determinism tests; `crates/varve-testkit/tests/compaction_equivalence.rs`; `cargo run --release --example compaction_gc -p varve` (churn plateau); GC is a pure function of `(manifests, listed_keys, config)` and now sweeps superseded log + probe objects. |
| 7 | Crash-safe under `kill -9` | ✅ MET | CI `crash-matrix` (`VARVE_CRASH_ITERS=100`); `chaos-nightly` (30-min soak, `VARVE_CHAOS_SECS=1800`); this run: `VARVE_CRASH_ITERS=10 … crash_recovery` → **2 passed**. |
| 8 | Shippable artifacts | ✅ MET | Release workflow + container image (`.github/workflows/release.yml`, Task 16; host-triple tarball built + both binaries `--help`-verified this run); mdBook site (`just docs` → no stubs); benchmark report (`docs/benchmarks/v1.md`); all five fuzz targets build + survive 60 s. |

**Overall:** all eight criteria met; criterion 4 ships with one explicit, acknowledged
exception (AWS S3 not CI-verified). Two spec §13 performance targets were declared
**not measured** on this loaded single box (below) rather than proxied — **both were
subsequently measured and met on 2026-07-16** (see the strikethrough notes below and the
update section in `docs/benchmarks/v1.md`).

## Explicit exceptions and non-measured targets

- **AWS S3 not CI-verified** (criterion 4) — configuration-compatible via the standard
  S3 API; the backend matrix covers Garage, SeaweedFS, MinIO (+ Ceph weekly).
- **Adapted TCK, not full conformance** (criterion 2) — pass rate ≈ 0.871 with reasoned
  exclusions.
- ~~**1M-node 2-hop < 50 ms: NOT MEASURED**~~ — **MET 2026-07-16**: 17.85 ms warm avg at
  1M nodes / 6M edges via the documented bulk-ingest → full-compaction procedure
  (`Db::ingest` + `Db::compact_full_once`); uncompacted it is ~640 ms. See the 2026-07-16
  update in `docs/benchmarks/v1.md` for the procedure caveat.
- ~~**Server ≥ 5k tx/s on the object-store log: NOT MEASURED**~~ — **MET 2026-07-16**:
  5,368 / 5,367 tx/s (two runs, 128 workers, 15 ms group commit) against loopback MinIO via
  `object_store_tx_bench`; loopback/in-process caveats stated in `docs/benchmarks/v1.md`.
- **Ingest throughput this session is load-bound** (~719 events/s in `social_bench` vs the
  ~40k events/s recorded on an unloaded box for the batched `block_bench` path) — the box
  was ~95% full with many concurrent worktrees; read-path latencies are unaffected and
  within target.

## Whole-slice verification transcript (2026-07-12, this session)

| Gate | Command | Result |
|---|---|---|
| Format | `cargo fmt --all --check` | clean |
| Lints | `cargo clippy --workspace --all-targets -- -D warnings` | clean |
| Tests | `cargo test --workspace -- --test-threads=1` | **903 passed, 0 failed, 107 suites** |
| GDPR | `cargo test -p varve --test gdpr_gc -- --test-threads=1` | 5 passed |
| Fuzz | `cargo +nightly fuzz build` + 60 s each of parse / log_record / manifest / block_meta / events | build OK; 0 crashes (4.5M / 6.0M / 2.7M / 1.1M / 1.2M runs) |
| Benches | `cargo bench -p varve-{index,types,gql} --bench <t> -- --quick` | ran clean (13 measurements) |
| E2E bench | `cargo run --release --example social_bench -p varve` | OK — warm point 0.37 ms, warm 2-hop 16.97 ms, AS-OF 1.00× current |
| Crash | `VARVE_CRASH_ITERS=10 cargo test -p varve-testkit --release --test crash_recovery` | 2 passed |
| Docs | `just docs` + `cargo test -p varve-testkit --test config_reference_doc` | build OK (no stubs); drift test 1 passed |
| Compose | `just compose-demo` | **=== compose-demo: PASSED ===** |
| Package | `sh scripts/package_release.sh aarch64-apple-darwin 1.0.0` | tarball + sha256; `varve`/`varved` `--help` OK |
| Whitespace | `git diff --check` | clean |
| Co-author | `git log … f72ca08..HEAD | grep -ci co-authored` | **0** |

### Deviation from the plan's gate command

The plan's `cargo bench … -- --quick` fails on this workspace: passing `--quick` to the
multi-crate `cargo bench` reaches the libtest unit-test bench runner, which rejects the flag
(the Task-7 justfile note documents this). The gate was run per-bench-target
(`--bench <name> -- --quick`), which is the correct invocation; `just bench-micro` runs the
full (non-`--quick`) sweep.

## Ship steps (USER-GATED — not executed by the agent)

> **Precondition — repo name must be `varve` before tagging.** `release.yml` derives the image
> name from `${{ github.repository }}`, and README / `CHANGELOG.md` / every crate's `repository`
> URL hardcode `ravan/varve`. If the GitHub repo is still named `timedb` at tag time, the pushed
> image is `ghcr.io/ravan/timedb` and the documented `docker run ghcr.io/ravan/varve:v1.0.0` /
> repo URLs 404. Rename the GitHub repo (and, if desired, the working dir) to `varve` first, or
> reconcile the hardcoded URLs to the real name before step 1.

1. `git tag v1.0.0 && git push origin main v1.0.0` — the release workflow builds the three
   target tarballs and pushes `ghcr.io/ravan/varve:v1.0.0` / `:latest`; publish the DRAFT
   GitHub release after inspecting assets.
2. `cargo publish` in dependency order (see `CHANGELOG.md` release checklist):
   `varve-types → varve-config → varve-gql → varve-index → varve-storage → varve-log →
   varve-plan → varve-engine → varve → varve-server → varve-cli`.
3. Post-publish smoke (the roadmap's ≤ 5-minute install criterion, measurable only after
   publish): `cargo install varve-cli && varve shell --dir /tmp/varve-smoke`;
   `docker run ghcr.io/ravan/varve:v1.0.0 --help`.
