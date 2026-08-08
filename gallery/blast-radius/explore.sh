#!/bin/sh
# Blast-radius gallery, driven from Varve Explorer instead of curl.
#
# demo.sh proves the point with four curl calls. This script points Explorer's
# BFF at the SAME query-only follower (query-1, :8091) so the four questions
# become three things you do with a mouse:
#
#   1. Query view       — one table showing valid_from != system_from
#   2. Time travel, VALID axis  — scrub Dec 2021, watch the blast radius appear
#   3. Time travel, SYSTEM axis — same instants, and we knew nothing
#
# It reuses a stack demo.sh left up (`sh demo.sh --keep`). If the store is not
# loaded it runs that setup first, which takes ~10 minutes.
#
# Usage:   sh gallery/blast-radius/explore.sh [--port N]
# In the timedb dev environment, route docker through rtk:
#          rtk proxy sh gallery/blast-radius/explore.sh
#
# Ctrl-C stops Explorer. It does NOT tear the varve stack down — that is
# demo.sh's `docker compose down -v`, so you can restart the UI without paying
# for another ingest.
set -eu

cd "$(dirname "$0")"

QUERY1="http://127.0.0.1:8091"
TOKEN="varve-demo-token"
EXPLORE="../../explore"
STATE=".demo-state"
PORT=5173

case "${1:-}" in
  --port) PORT="${2:?--port needs a number}" ;;
  "") ;;
  *) echo "usage: sh explore.sh [--port N]" >&2; exit 2 ;;
esac

# ---------------------------------------------------------------- helpers ----

# Run GQL against query-1 and print the raw JSON.
vq() {
  curl -s -X POST "$QUERY1/v1/query" \
    -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
    --data "{\"gql\":\"$1\"}"
}

count_label() {
  vq "MATCH (n:$1) RETURN count(*) AS c" | tr -dc '0-9'
}

# ------------------------------------------------------- 1. ensure loaded ----

# Before anything else, and before printing a page of instructions quoting a URL
# that would not work. vite runs with --strictPort so the printed port is always
# the real one; the cost is that a taken port has to be caught here rather than
# silently landing on 5174. Explorer's dev server binds ::1, so probe that.
if nc -z localhost "$PORT" 2>/dev/null; then
  echo "Port $PORT is already serving something (another Explorer?)." >&2
  echo "Identify it with:  lsof -nP -iTCP:$PORT -sTCP:LISTEN" >&2
  echo "Then stop it, or pick a free port:  sh explore.sh --port $((PORT + 1))" >&2
  exit 1
fi

echo "=== [1/4] check query-1 for a loaded store ==="
LOADED=0
if curl -sf -o /dev/null "$QUERY1/healthz" 2>/dev/null; then
  # Both floors matter. PkgVersion alone proves the corpus landed; CertifyVuln
  # is what makes the blast radius resolvable at all, and it is ingested last —
  # so a stack interrupted between the two looks "up" but demos nothing.
  PKGS=$(count_label PkgVersion || echo 0)
  VULNS=$(count_label CertifyVuln || echo 0)
  echo "  query-1 up: PkgVersion=${PKGS:-0} CertifyVuln=${VULNS:-0}"
  if [ "${PKGS:-0}" -ge 4000 ] && [ "${VULNS:-0}" -ge 5 ]; then LOADED=1; fi
else
  echo "  query-1 is not answering on $QUERY1"
fi

if [ "$LOADED" -eq 0 ]; then
  echo
  echo "  The store is not loaded. Running 'sh demo.sh --keep' first."
  echo "  This ingests the 12-image corpus and takes ~10 minutes."
  echo
  sh demo.sh --keep
  echo
fi

# ----------------------------------------------------- 2. recover T_ship -----

echo "=== [2/4] recover T_ship (the system-time instant we 'shipped') ==="
T_SHIP=""
if [ -f "$STATE" ]; then
  T_SHIP=$(sed -n 's/^T_SHIP=//p' "$STATE")
fi
if [ -z "$T_SHIP" ]; then
  # No handoff file (a stack left up by an older run, say). The instant is still
  # in the graph: demo.sh wrote :ShipMarker in its own transaction precisely so
  # this is recoverable, and system_from() reads that transaction's clock.
  T_SHIP=$(vq "MATCH (m:ShipMarker) RETURN system_from(m) AS t" |
    sed -n 's/.*"t":"\([^"]*\)".*/\1/p')
fi
case "$T_SHIP" in
  20??-??-??T??:??:??*Z) echo "  T_ship = $T_SHIP" ;;
  *)
    echo "  could not determine T_ship (got '${T_SHIP:-}')" >&2
    echo "  the UI still works; act 3 just has no exact instant to quote" >&2
    T_SHIP="(unknown — scrub today's timeline instead)"
    ;;
esac

# --------------------------------------------------- 3. Explorer deps --------

echo "=== [3/4] Explorer dependencies ==="
if [ -d "$EXPLORE/node_modules" ]; then
  echo "  already installed"
else
  pnpm --dir "$EXPLORE" install --frozen-lockfile
fi

# ------------------------------------------------------- 4. the cheat sheet --

CVE_INSTANT="2021-12-10T10:15:00Z"

# Print every query VERBATIM, read from its file, indented. Two reasons this is
# worth the vertical space rather than naming the files and trusting the reader:
#
#  1. `queries/` holds both `blast-radius.gql` (demo.sh's Q1: anonymous edges, a
#     scalar `RETURN DISTINCT product._id, s.uri`) and
#     `queries/explore-blast-radius.gql` (this UI's: entity columns). Pasting the
#     first into the Time travel box renders NOTHING — the view draws graphs and
#     has no table tab, so seven perfectly good rows are simply invisible. The
#     names are one prefix apart and the wrong one is the one you would guess.
#  2. varve GQL has no comment syntax (crates/varve-gql/src/token.rs), so a
#     warning cannot be written INSIDE the .gql file that needs it. Printing the
#     right text here is the only place the guidance can live next to the query.
#
# Read from the files, never retyped, so the sheet cannot drift from what runs.
ACT1=$(sed 's/^/    /' queries/explore-two-clocks.gql)
FILTER_BLAST=$(sed 's/^/    /' queries/explore-blast-radius.gql)
FILTER_SBOM=$(sed 's/^/    /' queries/explore-sboms.gql)

cat <<SHEET

=== [4/4] Explorer -> query-1 (a read-only follower), http://localhost:$PORT ===

  Connect with token:   $TOKEN

  Before you present, two things that are expected and will be asked about:

    * Connection status reads DEGRADED. Correct: Garage ignores the
      create-if-absent precondition varve probes for, so the probe verdict is
      'inconsistent'. Known and documented (docs/book/src/backends.md); reads
      and time travel are unaffected.
    * Short on height? Collapse the 'Filter topology' box with the '-' next to
      its title (it keeps the first line and a '...'), and collapse the left
      sidebar with the panel button left of Connection status. Both stay
      collapsed across reloads.

  COPY THE QUERIES FROM BELOW, NOT FROM queries/. Every act's text is printed
  here in full. The directory also holds demo.sh's Q1 as 'blast-radius.gql',
  one prefix away from the 'explore-blast-radius.gql' this UI needs, and it is
  the one you would reach for. Pasted into the Time travel box it draws NOTHING
  while still returning seven correct rows — that view renders graphs and has no
  table tab, so a scalar RETURN is invisible rather than wrong. On macOS you can
  skip the mouse entirely:

    pbcopy < gallery/blast-radius/queries/explore-blast-radius.gql

  THE TWO BOXES ARE NOT INTERCHANGEABLE either. Act 1 goes in 'New query'; acts
  2 and 3 go in 'Time travel'. Both are in the left sidebar and both take GQL.
  Put a scalar RETURN in the Time travel filter and you get, correctly:

    "Graph topology is unavailable because the rows contain no returned
     entities. Return whole variables, for example RETURN a, r, b."

  That is the right answer to the wrong question. Nothing is broken; the rule is
  simply that the Time travel filter must return whole node/edge variables.

  --- ACT 1 -- two clocks, one row.  Sidebar -> NEW QUERY, and paste: ---------

$ACT1

  Five rows. became_true is $CVE_INSTANT; we_recorded_it is today.
  Every answer below follows from the gap between those two columns.

  --- ACT 2 -- the blast radius appears.  Sidebar -> TIME TRAVEL: ------------

    Axis:      valid time            (Visualization settings, top right)
    Interval:  nothing to set — the filter carries its own window (below)
    Filter:    paste this, all of it, and press Go --

$FILTER_BLAST

  The interval jumps to Dec 2021 by itself and the badge next to the picker
  reads 'Fitted to data'. That is the last two lines of the filter doing it:
  'valid_from(cv)' and 'valid_from(product)' report when those facts became
  true (2021-12-10 and 2021-12-08), and Time travel frames the window on the
  instants the query returns for the axis in play. Nothing to type into the
  custom-interval picker, and no CET off-by-an-hour to fall into.

  Drag the timeline. Empty until the CVE becomes valid, then 47 nodes / 47
  relationships: seven images, each via its own log4j-core -> IsDependency ->
  IsOccurrence -> Artifact -> HasSBOM chain. The SBOMs did not change — only
  whether the CVE was true yet.

  NOTE the timeline is LOCAL time, so a $CVE_INSTANT fact flips at
  11:15 in CET, not 10:15. Do not read the offset as a bug.

  Scrubbing never re-frames the window, so the handle stays where you put it;
  fitting happens only when you press Go, flip the axis, or load the view. To
  get out, use the interval picker or 'Go live' — either clears the badge.

  --- ACT 3 -- and we knew none of it. Still TIME TRAVEL, one setting: -------

    Axis:      system time,  same Dec 2021 window

  Empty at every instant: in system time this store knew nothing in 2021. The
  window stays in Dec 2021 when you flip the axis — with zero rows there are no
  instants to fit to, so an empty result never moves the timeline.
  Now put the handle on Dec 9 -- evidence present, CVE not yet true -- go back
  to the VALID axis, and compare the act 2 filter (0 nodes, nothing to act on)
  against this one (24 nodes, all 12 SBOMs, right there):

$FILTER_SBOM

  That pair is the point: the evidence was in hand and the conclusion was not
  reachable. Dec 9 rather than ship day itself because the corpus becomes valid
  at 2021-12-08T00:00Z, and a picker entry of 'Dec 8 00:00' in CET is 23:00Z on
  Dec 7 — an hour too early, which reads as a broken demo.

  For the literal Q2-vs-Q3 pair demo.sh prints, set Interval to 'Last 1 hour'
  and scrub to T_ship:

    T_ship = $T_SHIP

  On the SYSTEM axis at T_ship the blast radius is empty; flip to the VALID
  axis without moving the handle and the seven images are back.

  ---------------------------------------------------------------------------
  Ctrl-C stops Explorer. The varve stack stays up; tear it down with
  'docker compose down -v' in gallery/blast-radius.

SHEET

# VARVE_URL on the command line, not in explore/.env: SvelteKit's
# $env/dynamic/private prefers process env over dotenv, so this wins over the
# checked-out dev default (:8080) without editing a file the operator owns.
#
# 8MiB matches the varve nodes' own server.http.max_body_bytes. Explorer's 1MiB
# default is enough for the filters below, but not for the ad-hoc `RETURN n`
# that anyone poking at GUAC data types next: a single HasSBOM node carries a
# ~70KB includedDependencies blob.
exec env \
  VARVE_URL="$QUERY1" \
  VARVE_DISPLAY_NAME="Blast radius (query-1 follower)" \
  VARVE_MAX_REQUEST_BYTES=8388608 \
  pnpm --dir "$EXPLORE" exec vite dev --port "$PORT" --strictPort
