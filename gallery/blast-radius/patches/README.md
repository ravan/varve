# Vendored GUAC backend patches

`refs/guac/` is a **reference checkout and is gitignored**, so the GUAC-side
changes this demo depends on are not tracked by this repo. They are vendored here
so the demo can be reconstructed. Committing and pushing the GUAC fork properly
is tracked as follow-up **F1** in
`docs/plans/2026-07-28-gallery-blast-radius.md`; until that happens, these
patches are the only record.

## `0001-varve-backend-valid-from.patch`

Adds a `--varve-valid-from` flag to GUAC's in-tree `varve` backend, which
backdates the **valid-time** of everything ingested in that process. The demo
needs it to write the CVE-2021-44228 facts as if they had been known on
2021-12-10 while the SBOM corpus stays at its real ingest time — that is the
whole bitemporal point of the demo (Q2 vs Q3).

| | |
|---|---|
| Base commit | `a71cf26e2182098a744002d188123953b26942bf` ("feat: varve backend") |
| Base branch | `feat/varve-backend` |
| Upstream | `git@github.com:guacsec/guac.git` |
| Built with | go 1.26.5 |
| Files touched | `pkg/assembler/backends/varve/{backend,batch}.go` + 3 test files |

Apply with:

```sh
cd refs/guac
git apply /path/to/gallery/blast-radius/patches/0001-varve-backend-valid-from.patch
go test ./pkg/assembler/backends/varve/
```

### What it does

- `--varve-valid-from=<RFC3339>` → parsed **once at startup** (`parseFlags`), so
  a malformed timestamp fails immediately with a clear message instead of
  emitting a GQL literal that only errors part-way through an ingest. Stored as a
  `time.Time`; the zero value means "no clause".
- `txBatch.program(validFrom)` appends ` VALID FROM TIMESTAMP '…'` to **every**
  statement it renders. It has to be per-statement, not once at the end: a
  program is `;`-separated and its trailing statements are the per-edge
  `MATCH … INSERT …` forms. varve accepts the clause on both forms.
- The timestamp is rendered through `Lit`, so it is escaped and normalised to UTC
  RFC3339Nano whatever offset was supplied.

### Two things to know before using it

1. **The flag is on `guacgql`, not `guacone`.** The varve backend lives inside
   the GraphQL *server*; `guacone collect files` is just a client pushing over
   GraphQL. So valid-time is a property of the running server process, which is
   why the demo must either run two `guacgql` services or restart one between the
   corpus ingest (no flag) and the CVE ingest (flag set).
2. **The default is byte-identical to the unpatched backend.** Omitting the flag
   renders no clause at all — asserted by
   `TestBatchProgramValidFromDefaultIsUnchanged`. `gallery/guac` depends on this
   and must be re-run unchanged as the regression gate (task T4).

### Deviation from the plan

`docs/plans/2026-07-28-gallery-blast-radius.md` predicted edits to `client.go`
and `cmd/guacgql/cmd/server.go` as well. Neither was needed: `backends.Register`
already plumbs backend-specific flags generically, so registering the flag in the
backend's own `registerFlags` is sufficient and it appears in `guacgql --help`
automatically. The plan also sketched threading `validFrom` onto `txBatch` at
construction; that would have meant touching ~25 `var batch txBatch` sites, so it
is held on `varveBackend` and passed in at `sendBatch` — the single choke point
that already renders the program.
