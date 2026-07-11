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

s3-matrix backends="garage,seaweedfs,minio":
    VARVE_S3_BACKENDS={{backends}} cargo test -p varve-testkit --test backend_matrix -- --nocapture

# Slice 9 exit demo: Garage + writer + two query nodes over Compose, with
# fixture load, cross-node basis/Arrow verify, shell/admin drive, and teardown.
# Routed through `rtk proxy` so the repo's RTK rule stays visible.
compose-demo:
    rtk proxy sh scripts/compose_demo.sh
