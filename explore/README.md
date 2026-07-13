# Varve Explorer

Varve Explorer is an independently authored, MIT-licensed SvelteKit application for running GQL against Varve. The browser talks only to Explorer's same-origin backend-for-frontend (BFF); the BFF talks to the single Varve target selected by the operator.

## Prerequisites

- A current Rust toolchain capable of building the repository workspace
- Node.js 22 or newer
- pnpm 11 or newer
- Optional: the Playwright CLI for the browser sanity flow

Run commands below from the repository root. Install the locked JavaScript dependencies with:

```bash
rtk pnpm --dir explore install --frozen-lockfile
```

## Local development with real Varve

Start the real `varved` binary in one terminal:

```bash
rtk pnpm --dir explore run dev:varve
```

The development configuration runs one node with the writer, query, and compactor roles. It listens on `127.0.0.1:8080`, advertises `http://127.0.0.1:8080`, exports Prometheus metrics, and writes its local log and store beneath `explore/.varve-explorer-dev/`. Stop the process before deleting that directory.

In a second terminal, copy the non-secret environment template and start Explorer:

```bash
rtk cp explore/.env.example explore/.env
rtk pnpm --dir explore run dev
```

Open the URL printed by Vite. Connect with the development token `varve-explorer-dev-token`. The token belongs in Explorer's connection form, not in `.env`.

Create and read a sample node with:

```gql
INSERT (:Person {_id: 1, name: 'Ada'})
```

```gql
MATCH (p:Person) RETURN p.name AS name
```

The first statement is a write and is sent to `/v1/tx`; the second is a read and is sent to `/v1/query`.

## Production build and start

Build the deployable SvelteKit Node application:

```bash
rtk pnpm --dir explore run build
```

Supply `VARVE_URL` to the running Node process; production startup does not load `explore/.env.example` automatically. For example:

```bash
rtk env NODE_ENV=production VARVE_URL=https://varve.internal.example HOST=127.0.0.1 PORT=3000 pnpm --dir explore run start
```

Put Explorer behind a trusted reverse proxy that terminates browser-facing TLS. Do not expose an HTTP-only production listener to browsers: production session cookies are `Secure`. Restrict direct access to the Node listener and protect the proxy-to-Explorer and Explorer-to-Varve network paths according to the deployment's trust boundary.

## Environment variables

| Variable                       | Required   | Meaning                                                                                                                              |
| ------------------------------ | ---------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| `VARVE_URL`                    | Production | Absolute, credential-free HTTP(S) Varve base URL. Development defaults to `http://127.0.0.1:8080`. Browser users cannot override it. |
| `VARVE_DISPLAY_NAME`           | No         | Human-readable connection name. Default: `Local Varve`.                                                                              |
| `VARVE_ALLOWED_WRITER_ORIGINS` | No         | Comma-separated HTTP(S) origins allowed for a writer retry. The `VARVE_URL` origin is always allowed.                                |
| `VARVE_REQUEST_TIMEOUT_MS`     | No         | Upstream timeout from 1 through 120000 milliseconds. Default: `60000`.                                                               |
| `VARVE_MAX_REQUEST_BYTES`      | No         | Request and response body limit from 1 through 16777216 bytes. Default: `1048576`.                                                   |

Do not put a bearer token in process environment or checked-in configuration. A user submits it through the connection form. Explorer validates it against `/v1/status`, then stores it only in the host-only `varve_explorer_session` cookie. The cookie is session-only, `HttpOnly`, `SameSite=Strict`, path `/`, and `Secure` in production. The token is not returned to browser JavaScript or written to persistent browser storage or request logs. Closing the browser session or disconnecting removes access; users must reconnect after a new browser session.

## Writer routing and result limitations

When a write sent to a query node returns Varve's `421 misdirected_request` with an advertised writer, Explorer retries once only if the writer's origin is `VARVE_URL`'s origin or is explicitly listed in `VARVE_ALLOWED_WRITER_ORIGINS`. Other origins, malformed destinations, credentials, redirect loops, and further redirects are rejected. Configure every legitimate writer origin explicitly in multi-node deployments.

Varve v1 has no schema-introspection HTTP endpoint. Explorer's Observed Schema is query-derived: it reports labels and relationship types seen in successfully executed or favorited GQL, not authoritative database metadata. Extraction failure does not block execution.

Varve's JSON query API returns rows rather than rich graph entities. Explorer enables Graph only when submitted GQL and returned opaque identifiers prove an unambiguous topology. If identity or topology is ambiguous, Graph is disabled with a reason and Table and Raw remain available. Graph rendering displays at most 2,000 nodes and 4,000 relationships; relationships whose endpoints fall outside the node limit are omitted, and truncation does not alter Table or Raw results.

Return scalar node properties alongside a path to provide deterministic graph captions. Explorer prefers `name`, then `title`, then `label`, then the first other returned scalar property:

```gql
MATCH path = (a:Person)-[:KNOWS]->(b:Person)
RETURN path, a.name AS from_name, b.name AS to_name
```

## Quality and license checks

Run individual checks with:

```bash
rtk pnpm --dir explore test
rtk pnpm --dir explore run check
rtk pnpm --dir explore run lint
rtk pnpm --dir explore run format:check
rtk pnpm --dir explore run build
rtk pnpm --dir explore run licenses
rtk pnpm --dir explore licenses list --prod
```

`licenses` is also a pnpm 11 built-in command, so `run licenses` is required to invoke this package's strict license-gate script. `licenses list --prod` is the separate pnpm inventory command. Run the complete package gate with:

```bash
rtk pnpm --dir explore run verify
```

The gate rejects GPL, AGPL, LGPL, SSPL, BUSL, unknown, and any other license outside its explicit allowlist. The sole MPL-2.0 exception is version-pinned indirect Lightning CSS build tooling; it is not a direct runtime dependency. See [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).

## Playwright CLI sanity flow

Run this flow against the real server and Explorer processes started above, never mocked API routes. Playwright refs such as `<token-ref>` come from the preceding snapshot and vary by page state.

```bash
rtk playwright-cli open http://127.0.0.1:5173
rtk playwright-cli snapshot
rtk playwright-cli fill <token-ref> "varve-explorer-dev-token"
rtk playwright-cli click <connect-ref>
rtk playwright-cli snapshot
```

Using fresh refs after each snapshot:

1. Confirm healthy connected status.
2. Run the sample write, then the sample read; inspect Table and Raw.
3. Run the caption-enabled topology query above; when Graph is available, confirm the `KNOWS` shaft and arrow, non-overlapping captioned circles, semantic zoom, selection and inspector focus, dragging, zoom, fit, and relayout. Check both themes and reduced motion, and confirm an ambiguous result falls back to Table/Raw with an explanation.
4. Exercise history, favorites, Observed Schema, settings, reload persistence, and responsive navigation.
5. Submit invalid GQL, test an invalid token, and cancel a request; confirm each state is recoverable.
6. Disconnect, close the browser, open a new session, and confirm the bearer token is not restored.
7. Inspect diagnostics and close the CLI session:

```bash
rtk playwright-cli console
rtk playwright-cli requests
rtk playwright-cli cookie-list
rtk playwright-cli close
```

The console should have no unexpected errors, and requests, cookies, snapshots, and persistent storage must not expose the bearer token.

## Original work and licensing

Explorer is original work licensed under the MIT License in [LICENSE](LICENSE). The repository path `refs/neo4j-browser` contributed no code, assets, styles, prose, tests, snapshots, names, or algorithms to Explorer. Direct runtime dependency notices are in [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).
