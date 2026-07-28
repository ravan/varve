#!/bin/sh
# GUAC-on-varve gallery demo. Stands up Garage (S3) + a varve writer + one
# query-only node, runs GUAC's guacgql (varve backend) + guacone against them,
# and finishes with a bitemporal time-travel read that a non-temporal graph DB
# cannot serve. ALWAYS tears the stack (incl. volumes) down on exit unless
# --keep is passed.
#
# Usage:   sh gallery/guac/demo.sh [--keep]
# In the timedb dev environment, route docker through rtk:
#          rtk proxy sh gallery/guac/demo.sh
set -eu

cd "$(dirname "$0")"

KEEP=0
[ "${1:-}" = "--keep" ] && KEEP=1

WRITER="http://127.0.0.1:8080"
QUERY1="http://127.0.0.1:8081"
GQL="http://127.0.0.1:8010"
TOKEN="varve-demo-token"
NEWPKG="pkg:v:golang/example.com/gallery-newdep/9.9.9++"

cleanup() {
  if [ "$KEEP" -eq 1 ]; then
    echo "=== --keep set: leaving stack up (down with: docker compose down -v) ==="
    return
  fi
  echo "=== tearing down (docker compose down -v --remove-orphans) ==="
  docker compose down -v --remove-orphans || true
}
trap cleanup EXIT
trap 'cleanup; exit 130' INT TERM

# Direct varve GQL against the writer's /v1/query (JSON rows). $1 = GQL text.
vq() {
  curl -s -X POST "$WRITER/v1/query" \
    -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
    --data "{\"gql\":\"$1\"}"
  echo
}

wait_http() {
  name="$1"; url="$2"; i=0
  while [ "$i" -lt 120 ]; do
    if curl -sf -o /dev/null "$url"; then echo "  $name ready"; return 0; fi
    i=$((i + 1)); sleep 1
  done
  echo "  $name never became ready at $url" >&2
  docker compose ps || true; docker compose logs --tail=40 || true
  return 1
}

# guacgql only serves POST /query (no GET / playground in this build), so probe
# it with a trivial GraphQL POST and wait for HTTP 200.
wait_gql() {
  i=0
  while [ "$i" -lt 120 ]; do
    # `|| echo 000` is load-bearing: under `set -eu` a failing command
    # substitution aborts the script, so without it a curl exit 7 (refused,
    # container not listening yet) or 56 (reset, listening but not serving yet)
    # kills the demo instead of retrying — the loop could never do its job.
    code=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$GQL/query" \
      -H "Content-Type: application/json" -d '{"query":"{__typename}"}' 2>/dev/null \
      || echo 000)
    if [ "$code" = "200" ]; then echo "  guacgql ready"; return 0; fi
    i=$((i + 1)); sleep 1
  done
  echo "  guacgql never became ready at $GQL/query" >&2
  docker compose ps || true; docker compose logs --tail=40 guacgql || true
  return 1
}

echo "=== [1/6] build + up (from clean volumes) ==="
docker compose down -v --remove-orphans >/dev/null 2>&1 || true
docker compose up -d --build

echo "=== [2/6] wait for varve writer + query node + guacgql ==="
wait_http writer  "$WRITER/healthz"
wait_http query-1 "$QUERY1/healthz"
wait_gql

echo "=== [3/6] ingest exampledata corpus (guacone, in-container) ==="
start=$(date +%s)
docker compose exec -T guacgql guacone collect files /data --gql-addr "$GQL/query" \
  >/tmp/guac-gallery-ingest.log 2>&1 || true
elapsed=$(( $(date +%s) - start ))
pkgs=$(vq "MATCH (n:PkgVersion) RETURN count(*) AS c" | sed 's/[^0-9]//g')
echo "  ingested in ${elapsed}s -> ${pkgs} PkgVersion nodes"
echo "  (non-fatal parser skips for a few non-SBOM/unsupported fixtures are expected)"

echo "=== [4/6] direct varve GQL: node counts + a 4-hop dependency path ==="
for L in PkgVersion PkgName IsDependency HasSBOM CertifyVuln; do
  printf "  %-14s " "$L"; vq "MATCH (n:$L) RETURN count(*) AS c"
done
echo "  dependency-tree path (PkgType -> Namespace -> Name -> Version):"
vq "MATCH (t:PkgType)-[:E {kind: 'PkgHasNamespace'}]->(ns:PkgNamespace)-[:E {kind: 'PkgHasName'}]->(n:PkgName)-[:E {kind: 'PkgHasVersion'}]->(v:PkgVersion) RETURN v._id AS version LIMIT 3"

echo "=== [5/6] GUAC queries: 'known' (Neighbors) + 'bad' ==="
echo "  guacone query known package pkg:golang/golang.org/x/text@v0.11.0"
docker compose exec -T guacgql guacone query known package \
  "pkg:golang/golang.org/x/text@v0.11.0" --gql-addr "$GQL/query" 2>&1 \
  | grep -viE '"level":"info"' | sed -n '1,24p' || true
echo "  guacone query bad (all certifyBad statements):"
docker compose exec -T guacgql guacone query bad --gql-addr "$GQL/query" 2>&1 \
  | grep -viE '"level":"info"' | sed -n '1,12p' || true

echo "=== [6/6] BITEMPORAL TIME TRAVEL (the neo4j-can't-do-this moment) ==="
echo "  newdep before change:"; vq "MATCH (v:PkgVersion {_id: '$NEWPKG'}) RETURN v._id AS id"
T1=$(date -u +%Y-%m-%dT%H:%M:%SZ)
echo "  captured T1=$T1 (graph has ${pkgs} packages)"
sleep 2
echo "  ingesting one more SBOM (adds gallery-newdep@9.9.9) ..."
docker compose exec -T guacgql guacone collect files /timetravel --gql-addr "$GQL/query" \
  >/tmp/guac-gallery-tt.log 2>&1 || true
sleep 1
echo "  LATEST: is gallery-newdep present now?"
vq "MATCH (v:PkgVersion {_id: '$NEWPKG'}) RETURN v._id AS id"
echo "  AS OF T1: was gallery-newdep present then? (expect none)"
vq "FOR SYSTEM_TIME AS OF TIMESTAMP '$T1' MATCH (v:PkgVersion {_id: '$NEWPKG'}) RETURN v._id AS id"
echo "  LATEST PkgVersion count vs AS OF T1 count (should differ by the new package):"
printf "    latest:   "; vq "MATCH (n:PkgVersion) RETURN count(*) AS c"
printf "    as-of-T1: "; vq "FOR SYSTEM_TIME AS OF TIMESTAMP '$T1' MATCH (n:PkgVersion) RETURN count(*) AS c"

echo "=== demo complete ==="
