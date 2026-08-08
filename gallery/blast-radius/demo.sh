#!/bin/sh
# Blast-radius gallery demo. Stands up Garage (S3) + a varve writer + one
# query-only node + two guacgql servers, ingests a 12-image SBOM corpus
# *backdated to ship day*, then ingests CVE-2021-44228 *backdated to its
# publication date two days later*, and finishes with four queries that differ
# by a single temporal clause.
#
# For the same data driven interactively from Varve Explorer's timeline instead
# of four curl calls, see explore.sh (it reuses a stack this script left up).
#
# The pitch is Q2 vs Q3: same query, same connection, one clause apart. Q2 is
# what the compliance dashboard showed on ship day (nothing). Q3 is what was
# actually true on ship day (seven vulnerable images). The gap is the exposure
# window, and a non-temporal graph DB cannot express the difference.
#
# ALWAYS tears the stack (incl. volumes) down on exit unless --keep is passed.
#
# Usage:   sh gallery/blast-radius/demo.sh [--keep]
# In the timedb dev environment, route docker through rtk:
#          rtk proxy sh gallery/blast-radius/demo.sh
set -eu

cd "$(dirname "$0")"

KEEP=0
[ "${1:-}" = "--keep" ] && KEEP=1

WRITER="http://127.0.0.1:8090"
QUERY1="http://127.0.0.1:8091"
GQL="http://127.0.0.1:8020"       # guacgql       — no valid-from (corpus)
# 8022, not 8021: macOS holds 8021 via a launchd socket-activated service (FTP
# proxy), so Docker cannot publish it and `lsof` shows nothing without sudo.
GQLPAST="http://127.0.0.1:8022"   # guacgql-past  — backdated  (CVE facts)
TOKEN="varve-demo-token"
# Must stay in step with docker-compose.yml's --varve-valid-from and with
# cve/cve-2021-44228.json's scanStartedOn. See cve/README.md.
CVE_PUBLISHED="2021-12-10T10:15:00Z"
DAY_BEFORE="2021-12-09T00:00:00Z"
# Ship day: guacgql's --varve-valid-from. The corpus is valid from here, so
# DAY_BEFORE lands INSIDE the corpus's valid interval but outside the CVE's.
CORPUS_VALID_FROM="2021-12-08T00:00:00Z"
# Written for explore.sh, which needs the same T_ship to drive the UI. Not the
# demo's source of truth for anything — just a handoff so a second script does
# not have to re-ingest to learn an instant only this run observed.
STATE=".demo-state"

cleanup() {
  if [ "$KEEP" -eq 1 ]; then
    echo "=== --keep set: leaving stack up (down with: docker compose down -v) ==="
    return
  fi
  echo "=== tearing down (docker compose down -v --remove-orphans) ==="
  docker compose down -v --remove-orphans || true
  # The volumes are gone, so a surviving T_ship would point explore.sh at an
  # instant no store can answer. Delete it with the data it describes.
  rm -f "$STATE"
}
trap cleanup EXIT
trap 'cleanup; exit 130' INT TERM

# ---------------------------------------------------------------- helpers ----

# Run GQL against a node's /v1/query and print the raw JSON. $1 = base URL,
# $2 = GQL text (single line).
vq() {
  curl -s -X POST "$1/v1/query" \
    -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
    --data "{\"gql\":\"$2\"}"
}

# Run GQL and print "<seconds> <json>" so a caller can report latency without a
# second round trip. macOS `date` has no %N, so curl's own timer is the portable
# way to get sub-second timings.
vq_timed() {
  curl -s -w ' %{time_total}' -X POST "$1/v1/query" \
    -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
    --data "{\"gql\":\"$2\"}"
}

# Count rows of a label on a node. Cheap scalar probe.
count_label() {
  vq "$1" "MATCH (n:$2) RETURN count(*) AS c" | tr -dc '0-9'
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

# Probe a guacgql with a trivial GraphQL POST until it answers 200.
# `|| echo 000` is load-bearing: under `set -eu` a failing command substitution
# aborts the script, so a curl exit 7 (refused — not listening yet) or 56
# (reset — listening but not serving yet) would kill the demo instead of
# retrying. gallery/guac/demo.sh had exactly that bug.
wait_gql() {
  name="$1"; base="$2"; i=0
  while [ "$i" -lt 120 ]; do
    code=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$base/query" \
      -H "Content-Type: application/json" -d '{"query":"{__typename}"}' 2>/dev/null \
      || echo 000)
    if [ "$code" = "200" ]; then echo "  $name ready"; return 0; fi
    i=$((i + 1)); sleep 1
  done
  echo "  $name never became ready at $base/query" >&2
  docker compose ps || true; docker compose logs --tail=40 "$name" || true
  return 1
}

# Block until the follower has replayed as far as the writer for a given label.
# Without this, a query against query-1 can legitimately return fewer rows just
# because the follower has not caught up yet — which would look like a wrong
# answer rather than replication lag.
wait_caught_up() {
  label="$1"; i=0
  want=$(count_label "$WRITER" "$label")
  while [ "$i" -lt 120 ]; do
    got=$(count_label "$QUERY1" "$label")
    if [ "${got:-0}" = "$want" ]; then
      echo "  query-1 caught up ($label = $want)"; return 0
    fi
    i=$((i + 1)); sleep 1
  done
  echo "  query-1 never caught up on $label (writer=$want, query-1=${got:-?})" >&2
  return 1
}

# The shared 7-hop pattern, as one line. Built from queries/blast-radius.gql so
# the file stays the single source of truth (varve GQL has no comment syntax, so
# that file is pure query text and needs no stripping).
pattern() {
  tr '\n' ' ' < queries/blast-radius.gql | tr -s ' '
}

# ------------------------------------------------------------------ [1/7] ----

echo "=== [1/7] build + up (from clean volumes) ==="
docker compose down -v --remove-orphans >/dev/null 2>&1 || true
docker compose up -d --build

echo "=== [2/7] wait for writer + query node + both guacgql servers ==="
wait_http writer  "$WRITER/healthz"
wait_http query-1 "$QUERY1/healthz"
wait_gql guacgql      "$GQL"
wait_gql guacgql-past "$GQLPAST"

echo "=== [3/7] expand + ingest the 12-image SBOM corpus (~8 min), valid from $CORPUS_VALID_FROM ==="
# Clear the CONTENTS, never the directory itself: corpus/expanded is a bind-mount
# source, and `rm -rf` on it detaches the mount — the container keeps pointing at
# the removed inode and sees an empty /corpus forever, so guacone silently
# ingests "0 documents of 0".
mkdir -p corpus/expanded
rm -f corpus/expanded/*.spdx.json
for gz in corpus/*.spdx.json.gz; do
  gunzip -c "$gz" > "corpus/expanded/$(basename "$gz" .gz)"
done
echo "  expanded $(ls corpus/expanded | wc -l | tr -d ' ') SBOMs"
# Prove the container can actually see them before spending 8 minutes on ingest.
SEEN=$(docker compose exec -T guacgql sh -c 'ls /corpus/*.spdx.json 2>/dev/null | wc -l' | tr -dc '0-9')
if [ "${SEEN:-0}" -lt 12 ]; then
  echo "  guacgql sees only ${SEEN:-0}/12 SBOMs in /corpus — bind mount is broken" >&2
  exit 1
fi
echo "  guacgql sees $SEEN/12 in /corpus"
start=$(date +%s)
# guacone runs INSIDE the guacgql container, so 127.0.0.1:8010 is that server —
# the one whose --varve-valid-from is ship day. The corpus lands with valid-time
# starting $CORPUS_VALID_FROM, system-time now.
docker compose exec -T guacgql \
  guacone collect files /corpus --gql-addr "http://127.0.0.1:8010/query" \
  >/tmp/blast-radius-corpus.log 2>&1 || true
echo "  corpus ingested in $(( $(date +%s) - start ))s"
for L in PkgVersion PkgName IsDependency IsOccurrence Artifact HasSBOM; do
  printf "    %-14s %s\n" "$L" "$(count_label "$WRITER" "$L")"
done
# guacone exits non-zero on the expected non-fatal parser skips, so its status is
# swallowed above. Assert on the GRAPH instead — otherwise a broken mount or a
# rejected corpus sails through and the demo "passes" with an empty graph, which
# is exactly what happened on the first run. S1's floor is 4,000.
PKGS=$(count_label "$WRITER" PkgVersion)
if [ "${PKGS:-0}" -lt 4000 ]; then
  echo "  only ${PKGS:-0} PkgVersion nodes (< 4000): corpus ingest did not work" >&2
  tail -20 /tmp/blast-radius-corpus.log >&2 || true
  exit 1
fi

echo "=== [4/7] capture T_ship from the writer's own tx response ==="
# NOT `date -u`: the container clock can skew, and the timestamp must be the
# writer's own notion of now. The marker node is written deliberately so there is
# a transaction whose system_time we can quote as "the moment we shipped".
#
# `system_time` is RFC3339 with MICROSECOND precision (Instant's Display uses
# SecondsFormat::Micros), and Instant::parse_rfc3339 accepts fractional seconds,
# so it round-trips into `AS OF TIMESTAMP '…'` unchanged. Do not be misled by
# api.rs's test asserting a bare "…us" string — that is the END_OF_TIME sentinel
# path, which only triggers for instants outside chrono's range, never for a real
# receipt.
TX=$(curl -s -X POST "$WRITER/v1/tx" \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  --data '{"gql":"INSERT (:ShipMarker {_id: '"'"'ship'"'"'})"}')
# One precise substitution, NOT `sed 's/.*://'` — RFC3339 is full of colons, so a
# greedy `.*:` leaves just the seconds field ("46.405431Z") and every AS OF query
# then fails with "invalid timestamp: premature end of input".
T_SHIP=$(printf '%s' "$TX" | sed -n 's/.*"system_time":"\([^"]*\)".*/\1/p')
case "$T_SHIP" in
  20??-??-??T??:??:??*Z) ;;
  *)
    echo "  system_time did not parse as RFC3339 (got '${T_SHIP:-}') from: $TX" >&2
    exit 1
    ;;
esac
echo "  T_ship = $T_SHIP  (from the writer, not the shell)"
# Hand T_ship to explore.sh. It is recoverable from the graph itself
# (MATCH (m:ShipMarker) RETURN system_from(m)), and explore.sh falls back to
# exactly that if this file is absent — writing it just saves a round trip.
printf 'T_SHIP=%s\nCORPUS_VALID_FROM=%s\nCVE_PUBLISHED=%s\n' \
  "$T_SHIP" "$CORPUS_VALID_FROM" "$CVE_PUBLISHED" > "$STATE"

echo "=== [5/7] full compaction sweep ==="
# The `full` flag is the route added alongside this demo: plain compact_once
# skips L0 groups under the log_limit gate and reports jobs: 0, which reads as
# "settled" when it is not. Loop until a sweep finds no work.
i=0
while [ "$i" -lt 60 ]; do
  REP=$(curl -s -X POST "$WRITER/v1/admin/compact" \
    -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
    --data '{"full": true}')
  JOBS=$(printf '%s' "$REP" | grep -o '"jobs":[0-9]*' | tr -dc '0-9')
  [ "${JOBS:-0}" = "0" ] && break
  echo "  compacted $JOBS job(s)"
  i=$((i + 1))
done
# `if`, not `[ … ] && echo`: under `set -e` a false test aborts the whole list
# (same interaction documented in the --keep block below). The loop exits two
# ways — a settled store (broke on jobs: 0, so i < 60) or 60 exhausted sweeps
# (i == 60). The demo still runs on an unsettled store; it just must not claim
# it settled when it did not.
if [ "$i" -lt 60 ]; then
  echo "  store settled"
else
  echo "  WARNING: compaction did not settle after 60 sweeps; continuing" >&2
fi

echo "=== [6/7] ingest CVE-2021-44228, backdated to $CVE_PUBLISHED ==="
# This exec targets guacgql-past, whose --varve-valid-from makes every statement
# carry `VALID FROM TIMESTAMP '$CVE_PUBLISHED'`. Same writer, same graph — only
# the valid-time differs. That is what splits Q2 from Q3.
docker compose exec -T guacgql-past \
  guacone collect files /cve --gql-addr "http://127.0.0.1:8010/query" \
  >/tmp/blast-radius-cve.log 2>&1 || true
echo "  CertifyVuln $(count_label "$WRITER" CertifyVuln)  VulnID $(count_label "$WRITER" VulnID)"
wait_caught_up CertifyVuln

echo "=== [7/7] four queries, one clause apart (served by query-1, a follower) ==="
P=$(pattern)

echo
echo "--- Q1: blast radius, today ---"
vq_timed "$QUERY1" "$P"
echo
echo "--- Q2: what we KNEW on ship day (FOR SYSTEM_TIME AS OF $T_SHIP) ---"
echo "     expect ZERO rows: the CVE facts had not been ingested yet"
vq_timed "$QUERY1" "FOR SYSTEM_TIME AS OF TIMESTAMP '$T_SHIP' $P"
echo
echo "--- Q3: what was TRUE on ship day (FOR VALID_TIME AS OF $T_SHIP) ---"
echo "     expect the SAME rows as Q1: written now, valid from $CVE_PUBLISHED"
vq_timed "$QUERY1" "FOR VALID_TIME AS OF TIMESTAMP '$T_SHIP' $P"
echo
echo "--- Q4: the day before the CVE was published ($DAY_BEFORE) ---"
echo "     expect ZERO rows: the corpus IS valid then (shipped $CORPUS_VALID_FROM),"
echo "     but the CVE is not — so valid-time really moves independently"
vq_timed "$QUERY1" "FOR VALID_TIME AS OF TIMESTAMP '$DAY_BEFORE' $P"
echo
echo "=== done. Q2 vs Q3 is the exposure window. ==="
echo "    Timings above are the trailing number on each row (seconds)."
echo "    Q1 shows ~0.018s HERE while the rows are still in the writer's unflushed"
echo "    live table. That lasts until the flush timer fires (flush_interval_ms,"
echo "    default 300000 = 5 min) or any node restarts; after that Q1 is ~0.035s,"
echo "    which is the steady state and the number to quote. Reproduce with"
echo "    'docker compose restart query-1' and re-run, and read varve_live_bytes"
echo "    off /metrics alongside it — a traversal timing without its residency is"
echo "    how the same query on the same store once measured 30x apart."
echo "    Q1 was ~2.4s before ba6a555 (edge-property predicates disabled anchored"
echo "    pruning) and ~0.46s before anchored lookups became degree-bound; see"
echo "    docs/plans/2026-07-28-degree-bound-lookups.md."
# `if`, not `[ … ] && echo`: under `set -e` a false test makes the whole list
# return 1 and aborts the script on its last line, so the demo would exit
# non-zero purely because --keep was not passed.
if [ "$KEEP" -eq 1 ]; then
  echo
  echo "    The stack is still up. To drive the same data from a browser:"
  echo "      sh explore.sh"
fi
