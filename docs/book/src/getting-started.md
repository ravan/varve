# Getting started (laptop, 5 minutes)

Every example on this page was actually run against this repository to produce the pasted
output — nothing here is invented.

## Path 1: embedded, from source (works today)

Clone the repository and build the CLI:

```bash
git clone <this-repo-url> varve
cd varve
cargo build --release -p varve-cli
```

Start an interactive shell against a brand-new local database directory. `--dir` (and `--url`)
are **global** flags: they must come *before* the subcommand, e.g. `varve --dir ./mydb shell`
— `varve shell --dir ./mydb` is rejected by the argument parser.

```bash
cargo run --release -p varve-cli -- --dir ./mydb shell
```

You now have a `varve>` prompt. Statements are buffered until they end with `;`; type or paste
one at a time (or pipe them via stdin, as this page's examples do:
`printf '%s\n' 'STATEMENT;' ':quit' | cargo run --release -p varve-cli -- --dir ./mydb shell`).

**Insert two people and an edge between them, then read them back:**

```
varve> INSERT (:Person {_id: 1, name: 'Ada'})-[:KNOWS]->(:Person {_id: 2, name: 'Bob'});
tx 1 @ 2026-07-12T12:20:28.657968Z
  nodes created: 2
  relationships created: 1
  properties set: 2
  labels added: 1
varve> MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name;
+--------+--------+
| a.name | b.name |
+--------+--------+
| Ada    | Bob    |
+--------+--------+
```

Every write echoes `tx <id> @ <system_time>` — that `system_time` is the writer's monotonic
clock, and you'll use it below to demonstrate system-time travel. (Your own timestamps will
differ; substitute whatever your shell actually prints.)

**Now add a third person with a *retro-dated* valid time** (she was valid starting 2020, even
though we're only recording her now), **look at current state, travel back to a system time
before she existed, and erase her** (Varve's GDPR hard-delete extension):

```
varve> INSERT (:Person {_id: 3, name: 'Cleo'}) VALID FROM DATE '2020-01-01';
tx 2 @ 2026-07-12T12:20:34.547760Z
  nodes created: 1
  properties set: 1
  labels added: 1
varve> MATCH (p:Person) RETURN p._id, p.name ORDER BY p._id;
+-------+--------+
| p._id | p.name |
+-------+--------+
| 1     | Ada    |
| 2     | Bob    |
| 3     | Cleo   |
+-------+--------+
varve> FOR SYSTEM_TIME AS OF TIMESTAMP '2026-07-12T12:20:28.657968Z' MATCH (p:Person) RETURN p._id, p.name ORDER BY p._id;
+-------+--------+
| p._id | p.name |
+-------+--------+
| 1     | Ada    |
| 2     | Bob    |
+-------+--------+
varve> MATCH (p:Person) WHERE p._id = 3 DETACH ERASE p;
tx 3 @ 2026-07-12T12:20:34.576884Z
  nodes deleted: 1
  properties removed: 1
  labels removed: 1
varve> :quit
```

Notice the `FOR SYSTEM_TIME AS OF` query — timestamped *before* Cleo's `INSERT` — shows only
Ada and Bob, even though Cleo's *valid* time (2020-01-01) is far earlier still. That's the
bitemporal distinction in one query: `VALID FROM` controls when a fact was true in the world;
`FOR SYSTEM_TIME AS OF` controls what Varve *knew* at a given instant. The final
`DETACH ERASE` is Varve's GDPR extension — it hides the entity immediately and physically
removes it at the next compaction, unlike a normal `DELETE` which is itself just another
timestamped layer.

## Path 2: server + CLI in three commands

This spins up a real `varved` server (writer + query + compactor roles, local log and storage
directories — no object store needed for this walkthrough) and connects to it with the same
`varve` CLI, over HTTP.

**1. Write a minimal `varve.toml`:**

```toml
[node]
roles = ["writer", "query", "compactor"]

[log]
backend = "local"
[log.local]
dir = "./data/log"

[storage]
backend = "local"
[storage.local]
dir = "./data/store"

[server]
backend = "http"
[server.http]
listen = "127.0.0.1:8080"
advertised_address = "http://127.0.0.1:8080"

[auth]
backend = "static"
[auth.static]
tokens = [{ subject = "getting-started", token = "dev-token" }]
```

**2. Start the server.** `varved` prints a single `VARVED_LISTENING <addr>` line to stdout once
it's actually ready to accept connections (all its own logs go to stderr, so this contract line
is easy to script against):

```bash
cargo build --release -p varve-server --bin varved
./target/release/varved --config varve.toml
```

```
VARVED_LISTENING 127.0.0.1:8080
```

**3. Connect with the CLI, over the network, using the static token:**

```bash
printf '%s\n' \
  "INSERT (:Person {_id: 1, name: 'Ada'});" \
  "MATCH (p:Person) RETURN p._id, p.name;" \
  ":quit" \
  | cargo run --release -p varve-cli -- --url http://127.0.0.1:8080 --token dev-token shell
```

```
tx 1 @ 2026-07-12T12:17:36.571997Z
  nodes created: 1
  properties set: 1
  labels added: 1
+-------+--------+
| p._id | p.name |
+-------+--------+
| 1     | Ada    |
+-------+--------+
```

Stop the server with `Ctrl-C` (or `kill` its PID) when you're done; both the log and store
directories persist on disk, so restarting `varved` against the same `varve.toml` resumes
exactly where you left off.

## Path 3: from release artifacts (available from v1.0.0)

The following become available once v1.0.0 is tagged and released (Task 15/16 of the v1 ship
plan); exact artifact names are finalized in that release, but will match this shape:

```bash
# crates.io
cargo install varve-cli
varve --dir /tmp/varve-smoke shell

# container image (ghcr)
docker run --rm ghcr.io/<owner>/<repo>:v1.0.0 --help

# release tarball (GitHub Releases), one per target triple:
#   varve-<version>-aarch64-apple-darwin.tar.gz
#   varve-<version>-x86_64-unknown-linux-musl.tar.gz
#   varve-<version>-aarch64-unknown-linux-musl.tar.gz
curl -LO https://github.com/<owner>/<repo>/releases/download/v1.0.0/varve-1.0.0-<target-triple>.tar.gz
tar xzf varve-1.0.0-<target-triple>.tar.gz
./varve-1.0.0/varve --dir /tmp/varve-smoke shell
```

Each tarball contains the `varve` CLI, the `varved` server binary, `LICENSE`, `README.md`, and
`CHANGELOG.md`, alongside a `.sha256` checksum.
