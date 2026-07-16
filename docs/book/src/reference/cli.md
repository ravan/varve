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
  import  Import newline-delimited JSON objects as one parameterized `INSERT` transaction per line
  export  Run a GQL query and write results as line-delimited JSON
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
Import newline-delimited JSON objects as one parameterized `INSERT` transaction per line

Usage: varve import [OPTIONS] --label <LABEL> <FILE>

Arguments:
  <FILE>  Path to a JSONL file, or `-` to read from stdin

Options:
      --label <LABEL>  Label applied to every inserted node
      --graph <GRAPH>  Graph to `USE` before each insert. Omitted entirely (no `USE` clause) when not given
  -h, --help           Print help
```

Each line of the JSONL file becomes one parameterized `INSERT (:<LABEL> {...})` transaction.
Object keys become `$pN`-style bound parameters (sorted, deterministic), never
string-interpolated into the GQL, and the resulting statement is validated with
`varve_gql::parse_program` before any request is sent. Import stops at the first failing line,
reporting its 1-based line number and how many lines were already committed. JSON object keys
and the `--label`/`--graph` values are all validated as GQL identifiers (ASCII shape only: first
byte a letter or `_`, the rest alphanumeric or `_`); the parser is still the sole authority on
whether the resulting GQL is actually valid (e.g. reserved-word collisions), not this shape
check.

## `export`

```
$ varve export --help
Run a GQL query and write results as line-delimited JSON

Usage: varve export [OPTIONS] --query <QUERY> <FILE>

Arguments:
  <FILE>  Path to write line-delimited JSON to, or `-` to write to stdout

Options:
      --query <QUERY>  The GQL query to run
      --basis <BASIS>  Read basis: a bare transaction id, or `at:<packed-u64>`
  -h, --help           Print help
```

Streams the query's Arrow result to one JSON object per line. Binary column values are encoded
using the same tagged-bytes convention as the HTTP API: `{"$bytes": "<base64>"}`
(`TaggedBytesEncoder`), and nulls are written explicitly rather than omitted
(`with_explicit_nulls(true)`).

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

Every JSONL line (both `import` input and `export` output) is a flat JSON object. Scalar values
map directly (`null`, `true`/`false`, numbers, and strings; integers must fit `i64`/`u64` and
floats must be finite, i.e. no `NaN`/`Infinity`); binary values are the single-key `{"$bytes":
"<base64>"}` form described above; arrays and any other nested-object shape are rejected as
invalid parameters (the same validation the HTTP API's `params_from_json` enforces, since the
CLI reuses it directly).
