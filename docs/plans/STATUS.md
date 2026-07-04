# Varve Implementation Status Ledger

> Update at the end of EVERY session. This is the entry point for the next session —
> read this first, then `varve-v1-roadmap.md`, then the current slice's detailed plan.

## Current position

- **Current slice:** 0 (foundation) — not started
- **Next action:** execute `docs/plans/2026-07-04-slice-00-foundation.md` from Task 1
- **Detailed plans ready:** slice 0 ✅ · slice 1 ✅ · slices 2–11 to be generated
  just-in-time from the roadmap (writing-plans skill) at slice start

## Environment facts (verify before relying on)

- Repo dir is named `timedb` but the project is **Varve** (rename pending, user's call).
- `~/.gitignore_global` ignores any path containing `specs` — keep docs in
  `docs/design/` and `docs/plans/`.
- XTDB reference checkout at `refs/xtdb` (gitignored); porting references for
  bitemporal/trie/compaction live in `refs/xtdb/core/src/main/kotlin/xtdb/` and
  `refs/xtdb/dev/doc/*.allium`.
- GQL grammar reference vendored at `resources/gql-grammar/` (committed, Apache-2.0).

## Decisions made during implementation

_(append here: date, decision, why — e.g. actual DataFusion version pinned and any
API adaptations from the plan sketches)_

## Slice log

| Slice | Status | Sessions used | Demo command | Notes |
|---|---|---|---|---|
| 0 foundation | not started | – | – | – |
| 1 walking skeleton | not started | – | – | – |
| 2 bitemporal core | no detailed plan yet | – | – | – |
| 3 durability | no detailed plan yet | – | – | – |
| 4 blocks | no detailed plan yet | – | – | – |
| 5 s3 backends | no detailed plan yet | – | – | – |
| 6 edges/traversal | no detailed plan yet | – | – | – |
| 7 gql completion | no detailed plan yet | – | – | – |
| 8 compaction | no detailed plan yet | – | – | – |
| 9 server/cli | no detailed plan yet | – | – | – |
| 10 coordination | no detailed plan yet | – | – | – |
| 11 ship | no detailed plan yet | – | – | – |
