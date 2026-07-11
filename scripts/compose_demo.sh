#!/bin/sh
# Slice 9 exit demo: stand up the Garage-backed 1-writer/2-query-node Compose
# deployment, prove cross-node basis reads + Arrow end to end, drive the `varve`
# shell + admin surface against it, and ALWAYS tear the stack (incl. volumes)
# down.
#
# Every docker/cargo command goes through `rtk` (repo rule). Invoke as:
#   rtk proxy sh scripts/compose_demo.sh
set -eu

WRITER="http://127.0.0.1:8080"
QUERY1="http://127.0.0.1:8081"
QUERY2="http://127.0.0.1:8082"
TOKEN="varve-demo-token"

# Always tear down — stack + named volumes + any orphans — on ANY exit.
cleanup() {
  echo "=== compose-demo: tearing down (down -v --remove-orphans) ==="
  rtk docker compose down -v --remove-orphans || true
}
trap cleanup EXIT
trap 'cleanup; exit 130' INT TERM

echo "=== compose-demo: build + up ==="
rtk docker compose up -d --build

# Wait for all three frontends to report healthy (public GET /healthz). The
# distroless varved image has no shell for a container healthcheck, so the host
# polls instead.
wait_healthy() {
  name="$1"
  url="$2"
  i=0
  while [ "$i" -lt 120 ]; do
    if curl -sf -o /dev/null "$url/healthz"; then
      echo "=== compose-demo: $name healthy at $url ==="
      return 0
    fi
    i=$((i + 1))
    sleep 1
  done
  echo "compose-demo: $name never became healthy at $url" >&2
  rtk docker compose ps || true
  rtk docker compose logs --tail=50 || true
  return 1
}
wait_healthy writer "$WRITER"
wait_healthy query-1 "$QUERY1"
wait_healthy query-2 "$QUERY2"

echo "=== compose-demo: load fixture + cross-node basis/Arrow verify ==="
rtk cargo run -p varve-testkit --bin http_fixture -- \
  --writer "$WRITER" \
  --query "$QUERY1" \
  --query "$QUERY2" \
  --token "$TOKEN"

echo "=== compose-demo: varve shell (query-1) — MATCH (p:Person) RETURN p.name LIMIT 3 ==="
printf 'MATCH (p:Person) RETURN p.name LIMIT 3;\n:quit\n' \
  | rtk cargo run -p varve-cli --bin varve -- \
      --url "$QUERY1" --token "$TOKEN" shell

echo "=== compose-demo: admin status (writer) ==="
rtk cargo run -p varve-cli --bin varve -- \
  --url "$WRITER" --token "$TOKEN" admin status

echo "=== compose-demo: admin verify (writer) ==="
rtk cargo run -p varve-cli --bin varve -- \
  --url "$WRITER" --token "$TOKEN" admin verify

echo "=== compose-demo: PASSED ==="
