#!/bin/sh
# One-shot Garage cluster bootstrap for the Compose demo. Idempotent at every
# resource-exists case so a `docker compose up` re-run is harmless.
#
# The literal access key / secret / rpc secret below are TEST/DEMO-ONLY fixed
# material — they are NOT production secrets and make no confidentiality
# claim; they exist so the demo cluster is byte-for-byte reproducible.
#
# Runs in an alpine image that carries the static `garage` binary at /garage
# (see deploy/Dockerfile.garage-init) plus a POSIX shell + awk — the upstream
# `dxflrs/garage` image has no shell. The CLI reaches the running node over RPC
# via `rpc_public_addr` in the mounted /etc/garage.toml.
set -eu

# Fixed demo credentials (TEST/DEMO-ONLY — see header).
ACCESS_KEY="GK000000000000000000000000"
SECRET_KEY="0000000000000000000000000000000000000000000000000000000000000000"

echo "garage-init: waiting for the node RPC to answer..."
until status=$(/garage status 2>/dev/null); do sleep 1; done

# First field of the first line whose hex-only remainder is >= 16 chars: the
# node id (garage prints it short, with a trailing ellipsis, which is stripped
# by dropping every non-hex byte). Mirrors backends.rs's id extraction.
node_id=$(printf '%s\n' "$status" | awk '{ id=$1; gsub(/[^0-9a-f]/, "", id); if (length(id) >= 16) { print id; exit } }')
test -n "$node_id"
echo "garage-init: node id = $node_id"

/garage layout assign -z dc1 -c 1G "$node_id" || true
/garage layout apply --version 1 || true

/garage bucket create varve || /garage bucket info varve >/dev/null

/garage key import --yes -n varve-demo "$ACCESS_KEY" "$SECRET_KEY" \
  || /garage key info varve-demo >/dev/null

/garage bucket allow --read --write --owner varve --key varve-demo

touch /shared/ready
echo "garage-init: ready — bucket 'varve' + key 'varve-demo' granted."
