default: check

fmt:
    cargo fmt --all

check:
    cargo fmt --all --check
    cargo clippy --workspace --all-targets -- -D warnings
    cargo test --workspace

test:
    cargo test --workspace

crash:
    VARVE_CRASH_ITERS=10 cargo test -p varve-testkit --release --test crash_recovery

chaos secs="60":
    VARVE_CHAOS_SECS={{secs}} cargo test -p varve-testkit --release --test chaos -- --nocapture

s3-matrix backends="garage,seaweedfs,minio":
    VARVE_S3_BACKENDS={{backends}} cargo test -p varve-testkit --test backend_matrix -- --nocapture

# Spec §13.7 criterion micro-benches: resolve, trie ops, parse.
# Per-crate targets required: cargo bench -- --quick without --bench <name> conflicts with libtest unit-test runner.
bench-micro:
    cargo bench -p varve-index -p varve-types -p varve-gql

# Slice 11 task 9: env-gated release-mode read scale-out bench (1/2/4 query
# nodes of one cluster, single ingest). Prints a markdown QPS table for
# docs/benchmarks/v1.md. Skips fast without VARVE_SCALE_BENCH=1.
bench-scale-out:
    VARVE_SCALE_BENCH=1 cargo test -p varve-server --release --test scale_out_bench -- --nocapture --test-threads=1

# Slice 9 exit demo: Garage + writer + two query nodes over Compose, with
# fixture load, cross-node basis/Arrow verify, shell/admin drive, and teardown.
# Routed through `rtk proxy` so the repo's RTK rule stays visible.
compose-demo:
    rtk proxy sh scripts/compose_demo.sh

# Varve's arrow-IPC decoders may lazily over-reserve (never-touched) buffers on adversarial input while returning clean errors; gate on RSS (real memory), not on allocation request-size.
# NOTE: libFuzzer treats -malloc_limit_mb=0 as "inherit -rss_limit_mb", so 0 does NOT disable the single-malloc hook. Raise it to 1 TiB so the request-size hook is effectively unbounded while -rss_limit_mb=4096 stays the real (touched-memory) gate.
fuzz target="parse" secs="60":
    cargo +nightly fuzz run {{target}} -- -max_total_time={{secs}} -rss_limit_mb=4096 -malloc_limit_mb=1048576

# Slice 11 task 11: build the mdBook docs site (fails if any SUMMARY.md entry
# is missing a stub — create-missing = false in book.toml).
docs:
    mdbook build docs/book

# Serve the docs site locally with live reload.
docs-serve:
    mdbook serve docs/book --open

# Slice 11 task 13: regenerate docs/book/src/ops/configuration.md from the
# real config schema (varve-testkit/src/config_reference.rs). Run this and
# commit the result whenever a serde default in a [section] key changes;
# config_reference_doc.rs fails `just check`/`just docs` otherwise.
docs-gen:
    cargo run -p varve-testkit --bin config_reference > docs/book/src/ops/configuration.md

# Slice 11 task 16: build the release tarball (varve + varved + LICENSE +
# README + CHANGELOG + sha256) for one target, into dist/. Mirrors what the
# tag-triggered release.yml runs per matrix leg; use it to verify the host
# triple locally, e.g. `just package aarch64-apple-darwin 1.0.0`.
package target version:
    sh scripts/package_release.sh {{target}} {{version}}
