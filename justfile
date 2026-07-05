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
