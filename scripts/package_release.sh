#!/usr/bin/env sh
# Usage: package_release.sh <target-triple> <version> [cargo-cmd]
# Builds varve (CLI) + varved (server) for <target> and produces
# dist/varve-<version>-<target>.tar.gz + .sha256.
set -eu
TARGET="$1"
VERSION="$2"
CARGO="${3:-cargo}"

$CARGO build --release --locked --target "$TARGET" -p varve-cli -p varve-server
STAGE="$(mktemp -d)"
trap 'rm -rf "$STAGE"' EXIT
mkdir -p "$STAGE/varve-$VERSION" dist
cp "target/$TARGET/release/varve" "target/$TARGET/release/varved" \
   LICENSE README.md CHANGELOG.md "$STAGE/varve-$VERSION/"
TARBALL="dist/varve-$VERSION-$TARGET.tar.gz"
tar -C "$STAGE" -czf "$TARBALL" "varve-$VERSION"
if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$TARBALL" > "$TARBALL.sha256"
else
    shasum -a 256 "$TARBALL" > "$TARBALL.sha256"
fi
echo "packaged $TARBALL"
