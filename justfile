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

# Slice 9 exit demo: Garage + writer + two query nodes over Compose, with
# fixture load, cross-node basis/Arrow verify, shell/admin drive, and teardown.
# Routed through `rtk proxy` so the repo's RTK rule stays visible.
compose-demo:
    rtk proxy sh scripts/compose_demo.sh

# Varve's arrow-IPC decoders may lazily over-reserve (never-touched) buffers on adversarial input while returning clean errors; gate on RSS (real memory), not on allocation request-size.
# NOTE: libFuzzer treats -malloc_limit_mb=0 as "inherit -rss_limit_mb", so 0 does NOT disable the single-malloc hook. Raise it to 1 TiB so the request-size hook is effectively unbounded while -rss_limit_mb=4096 stays the real (touched-memory) gate.
fuzz target="parse" secs="60":
    cargo +nightly fuzz run {{target}} -- -max_total_time={{secs}} -rss_limit_mb=4096 -malloc_limit_mb=1048576
