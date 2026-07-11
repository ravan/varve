# syntax=docker/dockerfile:1
#
# Two-stage build for the `varved` server. The builder compiles the release
# binary with the committed lockfile; the runtime stage is a distroless image
# carrying ONLY the binary (plus distroless CA certs / libc), no toolchain and
# no source tree.

# ---- builder ------------------------------------------------------------
FROM rust:1.93-bookworm AS builder
WORKDIR /build
# The whole workspace is copied in (the crate graph resolves across crates/),
# but `.dockerignore` keeps target/, VCS, and local tooling state out of the
# context so `--locked` compiles against the committed Cargo.lock.
COPY . .
RUN cargo build --locked --release -p varve-server --bin varved

# ---- runtime ------------------------------------------------------------
FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=builder /build/target/release/varved /usr/local/bin/varved
USER nonroot
EXPOSE 8080
# Compose supplies `--config /etc/varve/varve.toml`.
ENTRYPOINT ["/usr/local/bin/varved"]
