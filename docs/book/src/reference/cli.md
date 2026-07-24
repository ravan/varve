# CLI

The `varve` binary (`crates/varve-cli`) talks to either an embedded local database or a remote
`varved` server, using the same subcommands either way. Every usage line on this page is copied
from `varve --help`/`varve <subcommand> --help`.

## Connection selection

```
$ varve --help
Varve bitemporal graph database client

Usage: varve [OPTIONS] <COMMAND>

Commands:
  shell   Start an interactive REPL against the selected connection
  import  Bulk-load a file or stdin through the engine's fast path (`/v1/ingest` / `Db::ingest`); `--format jsonl-legacy` keeps the old one-`INSERT`-per-line mode
  export  Write the whole graph as bulk NDJSON, or a GQL query's rows as line-delimited JSON (`--format`)
  admin   Node administration: status, compaction, garbage collection, and integrity verification
  help    Print this message or the help of the given subcommand(s)

Options:
      --dir <DIR>      Path to a local database directory. Mutually exclusive with `--url`
      --url <URL>      Base URL of a remote `varved` HTTP frontend. Mutually exclusive with `--dir`
      --token <TOKEN>  Bearer token sent with every request to `--url`. Falls back to the `VARVE_TOKEN` environment variable; never echoed back by `--help` or any diagnostic output [env: VARVE_TOKEN]
  -h, --help           Print help
```

`--dir` and `--url` (and `--token`) are global flags: they must come before the subcommand
(`varve --dir ./mydb shell`, not `varve shell --dir ./mydb`). Exactly one connection mode is
required:

- `--dir <DIR>`: open a local database directory embedded in the CLI process.
- `--url <URL>` + `--token <TOKEN>` (or `VARVE_TOKEN` env var): talk to a remote `varved` over
  HTTP/HTTPS.

Both together, or neither, is a startup error with one of these exact messages
(`crates/varve-cli/src/cli.rs`):

- `"--dir and --url are mutually exclusive"`
- `"one of --dir or --url is required"`
- `"--token (or VARVE_TOKEN) is required when using --url"`

## `shell`

```
$ varve shell --help
Start an interactive REPL against the selected connection

Usage: varve shell

Options:
  -h, --help  Print help
```

An interactive REPL: statements are buffered until a `;` ends them (so a statement may span
multiple lines), and `:status`, `:help`, `:quit`/`:exit` are built-in commands. See
[Getting started](../getting-started.md) for full transcript examples of both `--dir` and
`--url` mode. Every completed write echoes a `tx <id> @ <system_time>` receipt line followed by
one line per nonzero side-effect count (nodes/relationships created or deleted, properties set
or removed, labels added or removed); a query prints an Arrow-pretty table, or `(0 rows)` if
every returned batch is empty.

## `import`

```
$ varve import --help
Bulk-load a file or stdin through the engine's fast path (`/v1/ingest` / `Db::ingest`); `--format jsonl-legacy` keeps the old one-`INSERT`-per-line mode

Usage: varve import [OPTIONS] <FILE>

Arguments:
  <FILE>  Path to the input file, or `-` to read from stdin

Options:
      --format <FORMAT>  Wire format of the input [default: ndjson] [possible values: ndjson, csv, jsonl-legacy]
      --label <LABEL>    Label applied to every inserted node. Only valid with `--format jsonl-legacy` (bulk formats carry labels per record)
      --graph <GRAPH>    Graph to `USE` before each insert. Only valid with `--format jsonl-legacy` (bulk formats load the default graph)
  -h, --help             Print help
```

`--format` selects the wire format (defaults to `ndjson`):

- **`ndjson` / `csv`** take the engine bulk fast path documented in
  [Bulk ingest](bulk-ingest.md): with `--url`, the file (or stdin) is **streamed** to
  `POST /v1/ingest` in one request; with `--dir`, it is decoded and committed in
  `[ingest] chunk_ops`-sized `Db::ingest` chunks. Neither buffers the whole input. On completion
  a progress line reports records and records/s; a mid-stream failure reports the committed
  counts (earlier chunks stay committed — idempotent replay from the start is the retry story).
  `--label`/`--graph` are **not** used here (records carry their own labels and load the default
  graph); passing them is a usage error.
- **`jsonl-legacy`** is the original mode: each line becomes one parameterized
  `INSERT (:<LABEL> {...})` transaction, validated with `varve_gql::parse_program` before any
  request is sent (it is the only mode with per-line GQL validation). It requires `--label`;
  `--graph` prefixes a `USE`. Object keys and the `--label`/`--graph` values are validated as GQL
  identifiers (ASCII shape; the parser remains the authority on reserved words). Import stops at
  the first failing line, reporting its 1-based line number and how many lines committed.

## `export`

```
$ varve export --help
Write the whole graph as bulk NDJSON, or a GQL query's rows as line-delimited JSON (`--format`)

Usage: varve export [OPTIONS] <FILE>

Arguments:
  <FILE>  Path to write to, or `-` to write to stdout

Options:
      --format <FORMAT>  Output format [default: jsonl] [possible values: jsonl, ndjson]
      --query <QUERY>    The GQL query to run. Required for `--format jsonl`; rejected for `--format ndjson` (which exports the whole graph)
      --basis <BASIS>    Read basis: a bare transaction id, or `at:<packed-u64>`. `--format jsonl` only
  -h, --help             Print help
```

- **`jsonl`** (default) streams a GQL query's Arrow result to one JSON object per line. Binary
  column values use the tagged-bytes convention `{"$bytes": "<base64>"}` (`TaggedBytesEncoder`)
  and nulls are written explicitly (`with_explicit_nulls(true)`). `--query` is required; `--basis`
  selects the read basis.
- **`ndjson`** writes the **whole data graph** as bulk NDJSON (nodes then edges) at the current
  system+valid time, so `varve export --format ndjson | varve import` copies a graph
  Varve→Varve. It is **embedded-only** (`--dir`) — there is no HTTP export endpoint — and
  `--query`/`--basis` are rejected. See [Bulk ingest](bulk-ingest.md) for the format and the
  edge-skipping caveat.

## `admin`

```
$ varve admin --help
Node administration: status, compaction, garbage collection, and integrity verification

Usage: varve admin [OPTIONS] <COMMAND>

Commands:
  status   Report node role(s), applied progress, and probe verdict
  compact  Run compaction
  gc       Run garbage collection
  verify   Verify manifest/trie/log integrity
  help     Print this message or the help of the given subcommand(s)

Options:
      --json  Emit the exact server response as JSON instead of human-readable key/value text
  -h, --help  Print help
```

Each subcommand (`status`/`compact`/`gc`/`verify`) makes exactly one corresponding call
(`CommandClient::status`/`compact`/`gc`/`verify`). With `--json`, the raw response struct is
printed via `serde_json::to_string`; without it, a fixed-field-order human-readable rendering is
printed (e.g. `format_status`/`format_compaction`/`format_gc`/`format_verify` in
`crates/varve-cli/src/output.rs`), where a missing `manifest_block_id` renders as the literal
text `none`, never a blank or `null`.

## JSONL format notes

For `--format jsonl-legacy` import input and `--format jsonl` export output, every line is a
flat JSON object. Scalar values map directly (`null`, `true`/`false`, numbers, and strings;
integers must fit `i64`/`u64` and floats must be finite, i.e. no `NaN`/`Infinity`); binary values
are the single-key `{"$bytes": "<base64>"}` form described above; arrays and any other
nested-object shape are rejected as invalid parameters (the same validation the HTTP API's
`params_from_json` enforces, since the CLI reuses it directly).

The bulk `ndjson`/`csv` formats (`type`-tagged node/edge records, valid-time fields, the CSV
header dialect) are specified on the [Bulk ingest](bulk-ingest.md) page — that is the normative
contract for both `varve import`/`export` and `POST /v1/ingest`.
