# Varve Explorer v1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build and ship an independently authored Varve Explorer v1 under `explore/`, providing a production SvelteKit BFF and an accessible GQL workspace verified against a real Varve HTTP server.

**Architecture:** A SvelteKit Node application serves an original Svelte 5 UI and a narrow same-origin BFF. The BFF forwards authenticated requests to one configured Varve target; browser-side modules normalize results, conservatively extract graph topology, and persist only non-secret workspace state.

**Tech Stack:** pnpm, Vite, strict TypeScript, latest stable SvelteKit, Svelte 5 runes, Tailwind CSS 4, shadcn-svelte, CodeMirror 6, Cytoscape.js, Vitest, Oxlint, Oxfmt, `@sveltejs/adapter-node`, Playwright CLI.

## Global Constraints

- All application code and package artifacts live under `explore/`; only design and plan documents live under `docs/superpowers/`.
- Use the latest stable SvelteKit and Svelte 5 releases resolved during scaffolding, then commit the exact pnpm lockfile.
- Use Svelte 5 runes; do not introduce legacy `$:` component reactivity.
- Compose shadcn-svelte primitives for controls, overlays, navigation, tables, feedback, and result tabs whenever a suitable primitive exists.
- Apply TDD only to TypeScript logic, never to Svelte UI components.
- Prefix every shell command with `rtk`, including pnpm, Git, Cargo, curl, and Playwright CLI commands.
- Prefix every commit message with an allowed semantic prefix such as `feat:`, `fix:`, `test:`, `docs:`, or `chore:`.
- Do not copy or adapt source, styles, assets, prose, tests, snapshots, internal names, or algorithms from `refs/neo4j-browser`.
- Use only MIT-compatible runtime dependencies; `explore/` itself is MIT licensed.
- Tokens are session-only, `HttpOnly`, `SameSite=Strict`, host-only cookies and are never returned to client JavaScript or written to persistent storage or logs.
- `VARVE_URL` is the only upstream target; the browser cannot submit arbitrary proxy destinations.
- The final browser pass must use the real Rust `varved` server, not mocked API routes.

---

## File Map

```text
explore/
  .gitignore                         generated/build/runtime exclusions
  .oxfmtrc.json                      Oxfmt policy
  .env.example                      documented BFF configuration
  LICENSE                            MIT license for Explorer
  README.md                          development, deployment, security, verification
  THIRD_PARTY_NOTICES.md             direct runtime dependency notices
  components.json                    shadcn-svelte generator aliases/theme
  oxlint.json                        Oxlint policy
  package.json                       scripts and dependencies
  pnpm-lock.yaml                     reproducible dependency graph
  svelte.config.js                   adapter-node configuration
  tsconfig.json                      strict TypeScript configuration
  vite.config.ts                     SvelteKit, Tailwind 4, Vitest
  varve.dev.toml                     real local Varve server configuration
  src/
    app.css                          Tailwind 4 tokens and original Varve theme
    app.d.ts                         app locals typing
    app.html                         document shell
    hooks.server.ts                  request ID and safe server logging
    lib/
      types.ts                       shared request/result/workspace contracts
      utils.ts                       shadcn class merge helper
      logic/
        gql.ts                       read/write and query-shape extraction
        gql.test.ts
        validation.ts                parameters, basis, and request validation
        validation.test.ts
        results.ts                   response normalization/table formatting
        results.test.ts
        graph.ts                     conservative topology extraction
        graph.test.ts
        schema.ts                    observed-schema aggregation
        schema.test.ts
        workspace.ts                 history/favorites/settings transitions
        workspace.test.ts
      server/
        config.ts                    validated environment configuration
        config.test.ts
        session.ts                   token cookie codec/options
        session.test.ts
        upstream.ts                  bounded Varve forwarding and error mapping
        upstream.test.ts
      stores/
        connection.svelte.ts         runes-based connection state
        workspace.svelte.ts          runes-based frames/history/favorites/settings
      components/
        ui/                           generated shadcn-svelte base components
        AppShell.svelte
        ConnectionDialog.svelte
        ConnectionStatus.svelte
        QueryComposer.svelte
        ParametersPanel.svelte
        ResultFeed.svelte
        ResultFrame.svelte
        ResultTable.svelte
        RawResult.svelte
        GraphResult.svelte
        ElementInspector.svelte
        HistoryPanel.svelte
        FavoritesPanel.svelte
        ObservedSchemaPanel.svelte
        SettingsPanel.svelte
    routes/
      +layout.svelte                  global CSS, theme, toast host
      +page.svelte                    workspace composition
      api/config/+server.ts
      api/session/+server.ts
      api/session/connect/+server.ts
      api/varve/health/+server.ts
      api/varve/status/+server.ts
      api/varve/query/+server.ts
      api/varve/tx/+server.ts
```

---

### Task 1: Scaffold the MIT SvelteKit and shadcn-svelte Foundation

**Files:**
- Create: `explore/package.json`
- Create: `explore/pnpm-lock.yaml`
- Create: `explore/svelte.config.js`
- Create: `explore/vite.config.ts`
- Create: `explore/tsconfig.json`
- Create: `explore/components.json`
- Create: `explore/oxlint.json`
- Create: `explore/.oxfmtrc.json`
- Create: `explore/src/app.css`
- Create: `explore/src/app.d.ts`
- Create: `explore/src/app.html`
- Create: `explore/src/lib/utils.ts`
- Generate: `explore/src/lib/components/ui/**`
- Create: `explore/src/routes/+layout.svelte`
- Create: `explore/src/routes/+page.svelte`
- Create: `explore/LICENSE`
- Create: `explore/.gitignore`

**Interfaces:**
- Consumes: no application interfaces.
- Produces: a buildable SvelteKit Node package; `$lib/utils.cn(...inputs: ClassValue[]): string`; generated shadcn-svelte primitives importable from `$lib/components/ui/<name>`.

- [ ] **Step 1: Scaffold the latest strict SvelteKit project and install the required toolchain**

Run from the repository root:

```bash
rtk pnpm dlx sv@latest create explore --template minimal --types ts --no-add-ons --no-install
rtk pnpm --dir explore install
rtk pnpm --dir explore add -D @sveltejs/adapter-node @tailwindcss/vite tailwindcss vitest oxlint oxfmt svelte-check @types/node @types/cytoscape license-checker-rseidelsohn
rtk pnpm --dir explore add bits-ui clsx tailwind-merge tailwind-variants lucide-svelte mode-watcher svelte-sonner @internationalized/date cytoscape @codemirror/state @codemirror/view @codemirror/commands @codemirror/language @codemirror/lang-json @lezer/highlight
```

Expected: pnpm exits 0 and creates `explore/pnpm-lock.yaml` without modifying the Rust workspace.

- [ ] **Step 2: Configure adapter-node, Tailwind 4, Vitest, Oxlint, and Oxfmt**

Set `explore/package.json` scripts to this contract:

```json
{
  "scripts": {
    "dev": "vite dev",
    "build": "vite build",
    "preview": "vite preview",
    "start": "node build",
    "check": "svelte-kit sync && svelte-check --tsconfig ./tsconfig.json",
    "test": "vitest run",
    "test:watch": "vitest",
    "lint": "oxlint .",
    "format": "oxfmt .",
    "format:check": "oxfmt --check .",
    "licenses": "license-checker-rseidelsohn --production --onlyAllow 'MIT;ISC;Apache-2.0;BSD-2-Clause;BSD-3-Clause;0BSD;CC0-1.0;BlueOak-1.0.0'"
  },
  "license": "MIT",
  "type": "module"
}
```

Use `adapter()` from `@sveltejs/adapter-node` in `svelte.config.js`. Configure `vite.config.ts` with `tailwindcss()`, `sveltekit()`, and a Vitest `include` of `src/**/*.test.ts` in Node environment. Enable TypeScript correctness and suspicious/correctness Oxlint categories, and set Oxfmt to two-space indentation, single quotes, trailing commas, and 100-character width.

- [ ] **Step 3: Initialize and generate shadcn-svelte base components**

Run:

```bash
rtk pnpm --dir explore dlx shadcn-svelte@latest init
rtk pnpm --dir explore dlx shadcn-svelte@latest add button input textarea label checkbox switch dialog sheet tabs dropdown-menu tooltip table badge card separator scroll-area skeleton alert sonner select collapsible
```

During init choose Tailwind CSS path `src/app.css`, base color `slate`, components alias `$lib/components`, utilities alias `$lib/utils`, and UI alias `$lib/components/ui`. Verify `components.json` records those exact aliases.

- [ ] **Step 4: Establish the original Varve theme and minimal shell**

In `src/app.css`, import Tailwind 4 and define light/dark semantic variables for background, foreground, card, border, primary amber, and data cyan. Use system sans and monospace stacks only. In `+layout.svelte`, import `app.css`, mount shadcn-svelte's Sonner host, and render children through Svelte 5 `$props`. In `+page.svelte`, render a temporary semantic heading `Varve Explorer` and subtitle `GQL workspace for Varve`.

- [ ] **Step 5: Add the Explorer MIT license**

Create `explore/LICENSE` with the standard MIT license text and copyright line `Copyright (c) 2026 Varve contributors`. Add `.svelte-kit/`, `build/`, `node_modules/`, `.env`, `.varve-explorer-dev/`, and Playwright CLI artifacts to `explore/.gitignore`.

- [ ] **Step 6: Verify the foundation and commit**

Run:

```bash
rtk pnpm --dir explore check
rtk pnpm --dir explore lint
rtk pnpm --dir explore format:check
rtk pnpm --dir explore build
rtk git add explore
rtk git commit -m "chore: scaffold Varve Explorer"
```

Expected: every command exits 0; the production output uses adapter-node; the commit includes the lockfile and generated shadcn-svelte primitives.

---

### Task 2: Add Typed Contracts, Input Validation, and GQL Classification

**Files:**
- Create: `explore/src/lib/types.ts`
- Create: `explore/src/lib/logic/validation.test.ts`
- Create: `explore/src/lib/logic/validation.ts`
- Create: `explore/src/lib/logic/gql.test.ts`
- Create: `explore/src/lib/logic/gql.ts`

**Interfaces:**
- Consumes: Varve HTTP DTOs documented in `crates/varve-server/src/api.rs`.
- Produces: `ExecutionMode`, `QueryRequest`, `TxReceipt`, `QueryResponse`, `ExplorerError`, `validateParameters`, `parseBasis`, `classifyGql`, and `extractQueryShape`.

- [ ] **Step 1: Define shared DTOs without behavior**

Create discriminated contracts including:

```ts
export type ExecutionMode = 'read' | 'write';
export type JsonScalar = null | boolean | number | string | { $bytes: string };
export type QueryParameters = Record<string, JsonScalar>;
export type Basis = number | `at:${number}`;
export interface QueryRequest {
  gql: string;
  params?: QueryParameters;
  basis?: Basis;
  basis_timeout_ms?: number;
}
export interface QueryResponse { rows: Record<string, unknown>[] }
export interface TxReceipt {
  tx_id: number;
  system_time: string;
  system_time_us: number;
  basis: number;
  side_effects: Record<string, number>;
}
export type ExplorerErrorCode =
  | 'unauthorized' | 'invalid_request' | 'not_acceptable' | 'basis_timeout'
  | 'backpressure' | 'misdirected_request' | 'writer_unavailable'
  | 'writer_fenced' | 'follower_failed' | 'internal' | 'network'
  | 'timeout' | 'cancelled' | 'malformed_response';
export interface ExplorerError {
  code: ExplorerErrorCode;
  message: string;
  status?: number;
  retryAfterMs?: number;
}
```

- [ ] **Step 2: Write failing validation tests**

Cover signed integers, finite floats, exact `$bytes` objects, rejection of arrays and nested objects, invalid JSON, non-object roots, transaction IDs, `at:` bases, and positive integer timeouts:

```ts
import { describe, expect, it } from 'vitest';
import { parseBasis, validateParameters } from './validation';

describe('validateParameters', () => {
  it('accepts Varve scalar parameters', () => {
    expect(validateParameters('{"name":"Ada","age":36,"blob":{"$bytes":"YQ=="}}'))
      .toEqual({ ok: true, value: { name: 'Ada', age: 36, blob: { $bytes: 'YQ==' } } });
  });
  it('rejects arrays before sending them to Varve', () => {
    expect(validateParameters('{"bad":[1]}')).toMatchObject({ ok: false });
  });
});

describe('parseBasis', () => {
  it('accepts tx ids and packed positions', () => {
    expect(parseBasis('42')).toEqual({ ok: true, value: 42 });
    expect(parseBasis('at:99')).toEqual({ ok: true, value: 'at:99' });
  });
});
```

- [ ] **Step 3: Run the validation tests red, then implement the validators**

Run `rtk pnpm --dir explore test -- validation.test.ts` and confirm failure because the module is missing. Implement pure result-returning validators; reject unsafe integers, `NaN`, infinities, invalid base64, negative bases, and non-positive/non-integer timeouts. Run the same command and expect all validation tests to pass.

- [ ] **Step 4: Write failing GQL classification and shape tests**

Pin comment/string safety and top-level mutation clauses:

```ts
import { describe, expect, it } from 'vitest';
import { classifyGql, extractQueryShape } from './gql';

describe('classifyGql', () => {
  it.each(['INSERT (:P {_id: 1})', 'MATCH (p:P) SET p.name = "Ada"', 'DROP GRAPH people'])
    ('classifies %s as write', (gql) => expect(classifyGql(gql)).toBe('write'));
  it('ignores mutation words inside strings and comments', () => {
    expect(classifyGql("MATCH (p:Person {name: 'DELETE'}) RETURN p")).toBe('read');
  });
});

it('extracts an unambiguous named path shape', () => {
  expect(extractQueryShape('MATCH p = (a:Person)-[:KNOWS]->(b:Person) RETURN p'))
    .toMatchObject({ paths: [{ alias: 'p', nodes: ['a', 'b'] }] });
});
```

- [ ] **Step 5: Run red, implement a conservative tokenizer, and run green**

Run `rtk pnpm --dir explore test -- gql.test.ts` and observe missing exports. Implement a single-pass tokenizer that removes line/block comments, preserves quoted/backtick content as opaque tokens, recognizes top-level statement keywords, pattern variables, labels, relationship types, directions, named paths, and return aliases. Unsupported syntax returns an empty/ambiguous shape rather than throwing. Run the test again and expect all cases to pass.

- [ ] **Step 6: Verify and commit the logic**

Run:

```bash
rtk pnpm --dir explore test
rtk pnpm --dir explore check
rtk pnpm --dir explore lint
rtk git add explore/src/lib/types.ts explore/src/lib/logic
rtk git commit -m "feat: add GQL request logic"
```

Expected: all logic tests pass and the commit contains no Svelte UI tests.

---

### Task 3: Implement Secure BFF Configuration and Session Cookies

**Files:**
- Create: `explore/src/lib/server/config.test.ts`
- Create: `explore/src/lib/server/config.ts`
- Create: `explore/src/lib/server/session.test.ts`
- Create: `explore/src/lib/server/session.ts`
- Modify: `explore/src/app.d.ts`
- Create: `explore/src/routes/api/config/+server.ts`
- Create: `explore/src/routes/api/session/+server.ts`

**Interfaces:**
- Consumes: SvelteKit `$env/dynamic/private`, `Cookies`, and global `fetch`.
- Produces: `loadServerConfig(env): ServerConfig`, `encodeSession(token): string`, `decodeSession(value): string | null`, `sessionCookieOptions(dev): CookieSerializeOptions`, public config route, and disconnect route.

- [ ] **Step 1: Write failing environment configuration tests**

```ts
import { describe, expect, it } from 'vitest';
import { loadServerConfig } from './config';

it('requires an absolute http(s) target in production', () => {
  expect(() => loadServerConfig({ NODE_ENV: 'production', VARVE_URL: 'file:///tmp/db' }))
    .toThrow('VARVE_URL');
});

it('normalizes writer origins and bounds numeric settings', () => {
  expect(loadServerConfig({
    NODE_ENV: 'production',
    VARVE_URL: 'https://query.example.test/',
    VARVE_ALLOWED_WRITER_ORIGINS: 'https://writer.example.test',
  }).allowedWriterOrigins).toEqual(new Set([
    'https://query.example.test', 'https://writer.example.test',
  ]));
});
```

- [ ] **Step 2: Run red, implement config validation, and run green**

Run `rtk pnpm --dir explore test -- config.test.ts`, confirm missing-module failure, then implement `ServerConfig` with `target`, `displayName`, `allowedWriterOrigins`, `timeoutMs`, `maxRequestBytes`, and `production`. Reject credentials in target URLs, fragments, non-HTTP schemes, out-of-range numbers, and malformed writer origins. Run the test again and expect pass.

- [ ] **Step 3: Write failing cookie tests**

```ts
import { expect, it } from 'vitest';
import { decodeSession, encodeSession, sessionCookieOptions } from './session';

it('round-trips empty and punctuation-bearing bearer tokens', () => {
  for (const token of ['', 'abc.def/+_=-']) expect(decodeSession(encodeSession(token))).toBe(token);
});

it('uses a session-only strict HttpOnly cookie', () => {
  expect(sessionCookieOptions(false)).toMatchObject({
    httpOnly: true, sameSite: 'strict', secure: true, path: '/',
  });
  expect(sessionCookieOptions(false)).not.toHaveProperty('maxAge');
});
```

- [ ] **Step 4: Run red, implement cookie helpers, and run green**

Encode `{ token }` as base64url JSON using `Buffer`; malformed values decode to null without throwing. Use cookie name `varve_explorer_session`. Run `rtk pnpm --dir explore test -- session.test.ts` and expect pass.

- [ ] **Step 5: Add config and disconnect endpoints**

`GET /api/config` returns:

```json
{ "displayName": "Local Varve", "target": "127.0.0.1:8080", "authenticated": false }
```

The target field contains only `host` and an HTTPS indicator, never credentials or path internals. `DELETE /api/session` deletes the cookie using the same path/options and returns 204. The authenticated connect endpoint is added after the bounded forwarding primitive exists in Task 4.

- [ ] **Step 6: Verify and commit**

Run:

```bash
rtk pnpm --dir explore test
rtk pnpm --dir explore check
rtk pnpm --dir explore lint
rtk git add explore/src/lib/server explore/src/routes/api/config explore/src/routes/api/session explore/src/app.d.ts
rtk git commit -m "feat: add secure Varve sessions"
```

Expected: tests and checks exit 0; no token appears in a response DTO or persistent browser API.

---

### Task 4: Implement Bounded Upstream Forwarding and Stable Errors

**Files:**
- Create: `explore/src/lib/server/upstream.test.ts`
- Create: `explore/src/lib/server/upstream.ts`
- Create: `explore/src/hooks.server.ts`
- Create: `explore/src/routes/api/session/connect/+server.ts`
- Create: `explore/src/routes/api/varve/health/+server.ts`
- Create: `explore/src/routes/api/varve/status/+server.ts`
- Create: `explore/src/routes/api/varve/query/+server.ts`
- Create: `explore/src/routes/api/varve/tx/+server.ts`

**Interfaces:**
- Consumes: `ServerConfig`, session token, Varve `ErrorResponse`, `QueryRequest`.
- Produces: `forwardVarve(input: ForwardInput): Promise<Response>`, `normalizeUpstreamError`, connect endpoint, and safe Varve route handlers.

- [ ] **Step 1: Write failing forwarding tests with injected fetch**

Cover authorization forwarding, JSON-only request/response, timeout, body limit, 401 mapping, `Retry-After`, secret redaction, and one allowed 421 retry:

```ts
it('retries a write once against an allowed advertised writer', async () => {
  const fetch = vi.fn()
    .mockResolvedValueOnce(new Response(JSON.stringify({
      code: 'misdirected_request', message: 'request must be sent to writer',
      writer: 'https://writer.example.test',
    }), { status: 421, headers: { 'content-type': 'application/json' } }))
    .mockResolvedValueOnce(new Response(JSON.stringify({ tx_id: 1, basis: 1,
      system_time: '2026-07-12T00:00:00Z', system_time_us: 1, side_effects: {} }),
      { status: 200, headers: { 'content-type': 'application/json' } }));

  const response = await forwardVarve(makeInput({ fetch, path: '/v1/tx', method: 'POST' }));
  expect(response.status).toBe(200);
  expect(fetch).toHaveBeenCalledTimes(2);
});
```

- [ ] **Step 2: Run red, implement forwarding, and run green**

Run `rtk pnpm --dir explore test -- upstream.test.ts` and observe missing implementation. Implement upstream URL joining, an `AbortSignal.timeout`, exact content type checks, bounded `Content-Length` plus byte-count fallback, allowed headers, bearer attachment, safe error parsing, and one writer retry for `/v1/tx`. Never forward upstream `set-cookie`, server banners, or hop-by-hop headers. Run the test again and expect pass.

- [ ] **Step 3: Add thin SvelteKit route adapters**

`POST /api/session/connect` accepts `{ "token": "..." }`, bounds the token to 4096 UTF-8 bytes, checks `/v1/status` through `forwardVarve`, sets the cookie only on success, and returns normalized status. Each Varve route loads config once per request, obtains the session token where required, and calls `forwardVarve`. Health is unauthenticated. Status/query/tx return a normalized 401 when no valid session cookie exists. Query and tx accept only POST JSON bodies. Add a request ID in `hooks.server.ts`; log method, route, status, and duration only, never bodies, authorization, cookies, writer URLs with credentials, or upstream error details.

- [ ] **Step 4: Verify and commit**

Run:

```bash
rtk pnpm --dir explore test
rtk pnpm --dir explore check
rtk pnpm --dir explore lint
rtk git add explore/src/lib/server/upstream* explore/src/routes/api/session/connect explore/src/routes/api/varve explore/src/hooks.server.ts
rtk git commit -m "feat: proxy Varve HTTP safely"
```

Expected: forwarding tests exercise real `Response` objects and all commands exit 0.

---

### Task 5: Normalize Query, Transaction, Table, and Raw Results

**Files:**
- Create: `explore/src/lib/logic/results.test.ts`
- Create: `explore/src/lib/logic/results.ts`

**Interfaces:**
- Consumes: `QueryResponse`, `TxReceipt`, unknown response JSON.
- Produces: `normalizeQueryResponse`, `normalizeTxReceipt`, `formatCell`, `sortRows`, `pageRows`, and `copyableJson`.

- [ ] **Step 1: Write failing normalization tests**

```ts
it('keeps first-seen column order and distinguishes missing from null', () => {
  const result = normalizeQueryResponse({ rows: [{ a: 1 }, { b: null, a: 2 }] });
  expect(result.columns).toEqual(['a', 'b']);
  expect(result.rows[0].b).toEqual({ kind: 'missing' });
  expect(result.rows[1].b).toEqual({ kind: 'value', value: null });
});

it('rejects malformed query envelopes', () => {
  expect(() => normalizeQueryResponse({ rows: 'not-an-array' })).toThrow('rows');
});
```

Also cover stable sorting across null/missing/boolean/number/string/structured values, 50-row pages, deterministic JSON, base64 byte presentation, and receipt side-effect defaults.

- [ ] **Step 2: Run red, implement result normalization, and run green**

Run `rtk pnpm --dir explore test -- results.test.ts`, confirm missing exports, then implement immutable normalized rows and stable sort with original index as tie-breaker. Keep `raw` untouched. Format objects as escaped compact JSON and binary objects as `bytes · <size/base64 preview>`. Run the test again and expect pass.

- [ ] **Step 3: Verify and commit**

```bash
rtk pnpm --dir explore test
rtk pnpm --dir explore check
rtk git add explore/src/lib/logic/results*
rtk git commit -m "feat: normalize Varve results"
```

Expected: all tests pass and no formatting function emits HTML.

---

### Task 6: Extract Conservative Graph Topology and Observed Schema

**Files:**
- Create: `explore/src/lib/logic/graph.test.ts`
- Create: `explore/src/lib/logic/graph.ts`
- Create: `explore/src/lib/logic/schema.test.ts`
- Create: `explore/src/lib/logic/schema.ts`

**Interfaces:**
- Consumes: `QueryShape` from `gql.ts`, raw normalized rows.
- Produces: `extractGraph(shape, rows, cap): GraphExtraction`, `extractObservedSchema(shape, timestamp)`, and `mergeObservedSchema`.

- [ ] **Step 1: Write failing graph extraction tests**

```ts
it('turns an alternating named path into deduplicated topology', () => {
  const shape = extractQueryShape('MATCH p = (a:Person)-[:KNOWS]->(b:Person) RETURN p');
  const graph = extractGraph(shape, [{ p: ['node-a', 'edge-ab', 'node-b'] }], 1000);
  expect(graph.available).toBe(true);
  expect(graph.nodes).toHaveLength(2);
  expect(graph.edges).toEqual([expect.objectContaining({
    source: 'node-a', target: 'node-b', type: 'KNOWS', inferred: true,
  })]);
});

it('refuses to invent topology from scalar table rows', () => {
  expect(extractGraph({ patterns: [], paths: [], returns: [] }, [{ name: 'Ada' }], 1000))
    .toMatchObject({ available: false, reason: expect.stringContaining('topology') });
});
```

Cover repeated identities, reverse direction, direct returned node/edge variables, malformed even-length paths, ambiguity, scalar attachments, and the 1,000-element cap.

- [ ] **Step 2: Run red, implement conservative graph extraction, and run green**

Run `rtk pnpm --dir explore test -- graph.test.ts`, observe missing implementation, then implement only provable mappings. Node and edge IDs are opaque strings; never infer identity from display properties. Return `{ available: false, reason }` for ambiguity. Set `truncated: true` when cap is reached without changing table/raw rows. Run the test again and expect pass.

- [ ] **Step 3: Write failing observed-schema tests**

```ts
it('aggregates query-derived labels and types with usage metadata', () => {
  const first = extractObservedSchema(
    extractQueryShape('MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b'), 10,
  );
  const merged = mergeObservedSchema(undefined, first);
  expect(merged.labels.Person).toMatchObject({ count: 2, firstSeen: 10, lastSeen: 10 });
  expect(merged.relationshipTypes.KNOWS.count).toBe(1);
});
```

- [ ] **Step 4: Run red, implement schema aggregation, and run green**

Run `rtk pnpm --dir explore test -- schema.test.ts`, then implement deterministic records keyed by label/type with count, first/last seen, and safe starter GQL builders that backtick-escape identifiers. Run again and expect pass.

- [ ] **Step 5: Verify and commit**

```bash
rtk pnpm --dir explore test
rtk pnpm --dir explore check
rtk git add explore/src/lib/logic/graph* explore/src/lib/logic/schema*
rtk git commit -m "feat: derive graph and schema views"
```

Expected: tests prove fallback behavior rather than forcing every query into a graph.

---

### Task 7: Add Versioned History, Favorites, Settings, and Runes Stores

**Files:**
- Create: `explore/src/lib/logic/workspace.test.ts`
- Create: `explore/src/lib/logic/workspace.ts`
- Create: `explore/src/lib/stores/workspace.svelte.ts`
- Create: `explore/src/lib/stores/connection.svelte.ts`

**Interfaces:**
- Consumes: execution DTOs, observed schema, a `StorageLike` interface.
- Produces: pure workspace transitions plus `createWorkspaceStore(storage)` and `createConnectionStore(fetch)` runes stores.

- [ ] **Step 1: Write failing workspace transition tests**

```ts
it('coalesces consecutive identical history and caps it at 100', () => {
  let state = emptyWorkspace();
  state = recordHistory(state, historyEntry({ gql: 'RETURN 1', finishedAt: 1 }));
  state = recordHistory(state, historyEntry({ gql: 'RETURN 1', finishedAt: 2 }));
  expect(state.history).toHaveLength(1);
  expect(state.history[0].runCount).toBe(2);
});

it('evicts the oldest completed unpinned frame above 25', () => {
  const state = Array.from({ length: 26 }, (_, id) => completedFrame(String(id)))
    .reduce((current, frame) => addFrame(current, frame), emptyWorkspace());
  expect(state.frames).toHaveLength(25);
  expect(state.frames.some((frame) => frame.id === '0')).toBe(false);
});
```

Cover favorites CRUD, pinned frame retention, history disabled mode, clear confirmation input, default settings, v1 storage decode, and incompatible-version reset without throwing.

- [ ] **Step 2: Run red, implement pure transitions and serialization, and run green**

Run `rtk pnpm --dir explore test -- workspace.test.ts`, observe missing module, then implement immutable transitions and `serializeWorkspace`/`deserializeWorkspace` with schema version `1`. Explicitly exclude tokens, raw responses, and active frame bodies from persisted data. Run again and expect pass.

- [ ] **Step 3: Wrap logic in Svelte 5 runes stores without UI tests**

`workspace.svelte.ts` owns `$state` for frames, history, favorites, observed schema, and settings; `$effect` writes the versioned safe subset to local storage. `connection.svelte.ts` owns `$state` for config, health, status, and session; it exposes `connect(token)`, `disconnect()`, and `refresh()` without ever storing or returning the token after connect completes.

- [ ] **Step 4: Verify and commit**

```bash
rtk pnpm --dir explore test
rtk pnpm --dir explore check
rtk pnpm --dir explore lint
rtk git add explore/src/lib/logic/workspace* explore/src/lib/stores
rtk git commit -m "feat: persist Explorer workspace"
```

Expected: tests pass; source search shows no `localStorage` or `sessionStorage` writes containing token fields.

---

### Task 8: Build the shadcn-svelte Application Shell, Connection, and Composer

**Files:**
- Create: `explore/src/lib/components/AppShell.svelte`
- Create: `explore/src/lib/components/ConnectionDialog.svelte`
- Create: `explore/src/lib/components/ConnectionStatus.svelte`
- Create: `explore/src/lib/components/QueryComposer.svelte`
- Create: `explore/src/lib/components/ParametersPanel.svelte`
- Modify: `explore/src/routes/+layout.svelte`
- Modify: `explore/src/routes/+page.svelte`
- Modify: `explore/src/app.css`

**Interfaces:**
- Consumes: connection/workspace stores, validators, classifier, CodeMirror.
- Produces: accessible connection and execution events; stable accessible names used by Playwright CLI.

- [ ] **Step 1: Compose the application shell from shadcn-svelte primitives**

Build the desktop rail with shadcn Buttons, Tooltip, Separator, and Scroll Area. Build the narrow-screen navigation with Sheet. Use icon-plus-text navigation names `New query`, `Observed schema`, `History`, `Favorites`, and `Settings`. The top bar contains `Connection status`, target display, `Reconnect`, and `Theme` controls. Use CSS grid with a 15rem rail, flexible workspace, and optional 20rem inspector; collapse below 900px.

- [ ] **Step 2: Build the secure connection flow**

Use shadcn Dialog, Input, Label, Alert, Button, Badge, and Skeleton. The password input is named `Bearer token`, has `autocomplete="off"`, and is cleared immediately after every connect attempt. Keep the Dialog modal while unauthenticated. Announce errors in an `aria-live="polite"` region and restore focus to `Reconnect` when dismissed.

- [ ] **Step 3: Integrate CodeMirror 6 in the composer**

Create one client-only editor instance after mount and destroy it on unmount. Add original GQL keyword highlighting, line numbers, bracket matching, history, and a `Mod-Enter` command that dispatches Run. Expose an accessible label `GQL query`. Synchronize editor content with store state without reconstructing the editor on each keystroke.

- [ ] **Step 4: Build mode, parameter, and basis controls**

Compose shadcn Tabs or Toggle-style Buttons for `Read` and `Write`; classifier changes the default until the user manually overrides it. Use Collapsible, Textarea, Input, Label, and Alert for parameters and basis. Disable `Run query` while validation fails or a request is active. Replace it with `Cancel query` during execution.

- [ ] **Step 5: Wire execution into immutable frames**

On run, validate input, add a running frame, call the appropriate same-origin endpoint with an AbortController, normalize the response, add history and observed schema on completion, and retain errors inline. On write success, store the returned basis as the default basis for the next read to provide read-your-writes behavior.

- [ ] **Step 6: Verify UI compilation and commit**

```bash
rtk pnpm --dir explore check
rtk pnpm --dir explore lint
rtk pnpm --dir explore format:check
rtk pnpm --dir explore build
rtk git add explore/src
rtk git commit -m "feat: build Explorer query workspace"
```

Expected: no Svelte UI unit tests are added; all checks pass and every control has a stable accessible name.

---

### Task 9: Build Result Frames, Table, Raw, and Transaction Views

**Files:**
- Create: `explore/src/lib/components/ResultFeed.svelte`
- Create: `explore/src/lib/components/ResultFrame.svelte`
- Create: `explore/src/lib/components/ResultTable.svelte`
- Create: `explore/src/lib/components/RawResult.svelte`
- Modify: `explore/src/routes/+page.svelte`
- Modify: `explore/src/app.css`

**Interfaces:**
- Consumes: normalized query/tx results and workspace frame actions.
- Produces: Graph/Table/Raw tabs for reads; side-effect summary/Raw for writes; frame actions.

- [ ] **Step 1: Compose immutable result frames**

Use shadcn Card, Badge, Button, Dropdown Menu, Collapsible, Skeleton, Alert, and Tabs. Each frame shows state, mode, duration, rows or side effects, and timestamp. Provide accessible actions `Collapse result`, `Rerun query`, `Copy GQL`, `Add to favorites`, `Pin result`, and `Close result`. Running frames use a non-animated skeleton when reduced motion is active.

- [ ] **Step 2: Build the table result component**

Use shadcn Table inside Scroll Area. Render semantic headers as sorting Buttons with `aria-sort`; display Missing, null, booleans, numbers, strings, bytes, arrays, and objects distinctly. Add Previous/Next pagination and `Page N of M`. Preserve column order from `normalizeQueryResponse`.

- [ ] **Step 3: Build raw and write result components**

Raw uses a read-only `pre` with escaped `copyableJson` output and a shadcn Copy Button. Write results use Cards and Badges for `tx_id`, `basis`, `system_time`, and every nonzero side-effect count. No response value is inserted through `{@html}`.

- [ ] **Step 4: Add empty, error, and cancelled frame states**

Use inline shadcn Alerts with stable headings: `No rows`, `Authentication required`, `Invalid request`, `Basis timeout`, `Server busy`, `Writer unavailable`, `Server error`, `Network error`, and `Request cancelled`. Keep the original GQL visible and provide `Rerun query`; do not auto-retry writes after backpressure.

- [ ] **Step 5: Verify and commit**

```bash
rtk pnpm --dir explore test
rtk pnpm --dir explore check
rtk pnpm --dir explore lint
rtk pnpm --dir explore build
rtk git add explore/src/lib/components explore/src/routes/+page.svelte explore/src/app.css
rtk git commit -m "feat: render Explorer result frames"
```

Expected: all logic tests and production build pass; a source scan finds no `{@html}`.

---

### Task 10: Build Graph, Inspector, Schema, History, Favorites, and Settings UI

**Files:**
- Create: `explore/src/lib/components/GraphResult.svelte`
- Create: `explore/src/lib/components/ElementInspector.svelte`
- Create: `explore/src/lib/components/ObservedSchemaPanel.svelte`
- Create: `explore/src/lib/components/HistoryPanel.svelte`
- Create: `explore/src/lib/components/FavoritesPanel.svelte`
- Create: `explore/src/lib/components/SettingsPanel.svelte`
- Modify: `explore/src/lib/components/AppShell.svelte`
- Modify: `explore/src/lib/components/ResultFrame.svelte`
- Modify: `explore/src/app.css`

**Interfaces:**
- Consumes: `GraphExtraction`, observed schema, persisted workspace actions/settings.
- Produces: interactive Cytoscape graph and all focused-browser navigation panels.

- [ ] **Step 1: Mount and dispose Cytoscape safely**

Create Cytoscape only in the browser, map normalized elements to Cytoscape data, and destroy the instance on input change/unmount. Use original Varve colors and shapes, `cose` layout with animation disabled under reduced motion, and no code/assets from the reference checkout. Cap elements before passing them to Cytoscape.

- [ ] **Step 2: Add graph controls and inspector**

Compose shadcn Buttons and Tooltips named `Zoom in`, `Zoom out`, `Fit graph`, and `Reset layout`. Selection updates an ElementInspector built with Card, Badge, Separator, and Scroll Area. The inspector states `Inferred from GQL` for labels/types and shows identity plus safely formatted related row values. Unavailable graph extraction renders the specific fallback reason.

- [ ] **Step 3: Build observed schema and starter query actions**

Use Cards, Badges, Scroll Area, and Buttons. The heading is `Observed schema` and supporting text says `Derived from successful and favorite queries; not authoritative database metadata.` Group node labels and relationship types with usage counts and last-seen dates. `Query label <name>` and `Query relationship <name>` insert escaped starter GQL in a new composer.

- [ ] **Step 4: Build history and favorites panels**

History shows latest first, outcome Badge, time, mode, row/effect count, and Buttons `Run history item` and `Clear history`. Favorites use Dialog for create/edit fields `Favorite name` and `Notes`, plus `Run favorite`, `Duplicate favorite`, and `Delete favorite`. Parameters are visible but token/session data never appears.

- [ ] **Step 5: Build settings and theme behavior**

Compose Select, Switch, Checkbox, Dialog, and Button for theme, default result tab, graph motion, history, and clear-data confirmation. System theme uses `mode-watcher`; setting changes persist through the workspace store. Clearing data requires a confirmation Dialog and preserves the authenticated session cookie.

- [ ] **Step 6: Verify and commit**

```bash
rtk pnpm --dir explore test
rtk pnpm --dir explore check
rtk pnpm --dir explore lint
rtk pnpm --dir explore format:check
rtk pnpm --dir explore build
rtk git add explore/src
rtk git commit -m "feat: complete Explorer navigation and graph UI"
```

Expected: checks pass; Cytoscape instances are disposed; all product controls build on shadcn-svelte primitives.

---

### Task 11: Add Real Varve Development Configuration and Shipping Documentation

**Files:**
- Create: `explore/varve.dev.toml`
- Create: `explore/.env.example`
- Create: `explore/README.md`
- Create: `explore/THIRD_PARTY_NOTICES.md`
- Modify: `explore/package.json`
- Modify: `explore/.gitignore`

**Interfaces:**
- Consumes: real `varved` binary from `crates/varve-server`.
- Produces: reproducible local integration environment and production operator guide.

- [ ] **Step 1: Add a local real-server configuration**

Create `varve.dev.toml` with one writer/query/compactor node, local log/store under `.varve-explorer-dev`, HTTP on `127.0.0.1:8080`, advertised address `http://127.0.0.1:8080`, static token `varve-explorer-dev-token`, and Prometheus metrics. Use the same valid backend structure proven by `crates/varve-server/tests/support/process_cluster.rs`.

- [ ] **Step 2: Add integration scripts and environment example**

Add package scripts:

```json
{
  "dev:varve": "cargo run --manifest-path ../Cargo.toml -p varve-server --bin varved -- --config explore/varve.dev.toml",
  "verify": "pnpm test && pnpm check && pnpm lint && pnpm format:check && pnpm build && pnpm licenses"
}
```

`.env.example` contains `VARVE_URL=http://127.0.0.1:8080`, `VARVE_DISPLAY_NAME=Local Varve`, and documented optional limits without a token.

- [ ] **Step 3: Write deployment and security documentation**

README sections must cover prerequisites, pnpm installation, real Varve startup, Explorer dev startup, token and sample GQL, production build/start, environment variables, reverse-proxy TLS expectations, cookie/session behavior, writer redirects, observed-schema limitations, graph capability fallback, quality commands, and Playwright CLI sanity steps. State explicitly that Explorer is original MIT work and that `refs/neo4j-browser` contributed no code or assets.

- [ ] **Step 4: Add third-party notices and run the license gate**

List every direct runtime dependency with project URL and license, then run:

```bash
rtk pnpm --dir explore licenses
rtk pnpm --dir explore licenses list --prod
```

Expected: no GPL, AGPL, LGPL, SSPL, BUSL, or unknown runtime license. Resolve any violation by replacing/removing the dependency and updating the lockfile rather than weakening the allowlist.

- [ ] **Step 5: Verify documentation paths and commit**

```bash
rtk pnpm --dir explore verify
rtk git add explore
rtk git commit -m "docs: document Varve Explorer shipping"
```

Expected: full package verification exits 0 and the documented commands match the committed scripts.

---

### Task 12: Verify Production Browser Flows Against the Real Varve HTTP Server

**Files:**
- Modify as failures require: `explore/src/**`
- Modify as failures require: `explore/README.md`
- Create: no automated Playwright test suite; use the requested Playwright CLI skill.

**Interfaces:**
- Consumes: production adapter-node build, real `varved`, accessible UI names.
- Produces: fresh command and browser evidence for every v1 acceptance path.

- [ ] **Step 1: Run fresh non-browser release gates**

```bash
rtk pnpm --dir explore install --frozen-lockfile
rtk pnpm --dir explore verify
rtk git diff --check
```

Expected: all commands exit 0 with zero test, type, lint, format, build, or license failures.

- [ ] **Step 2: Start the real Varve server**

Run in a persistent terminal session from the repository root:

```bash
rtk cargo run -p varve-server --bin varved -- --config explore/varve.dev.toml
```

Wait for `VARVED_LISTENING 127.0.0.1:8080`, then verify:

```bash
rtk curl -fsS http://127.0.0.1:8080/healthz
```

Expected: `{"status":"ok"}` from the real Rust process.

- [ ] **Step 3: Start the production Explorer server**

Run in another persistent session:

```bash
rtk pnpm --dir explore build
rtk env VARVE_URL=http://127.0.0.1:8080 VARVE_DISPLAY_NAME="Local Varve" PORT=4173 pnpm --dir explore start
```

Expected: adapter-node listens on `http://127.0.0.1:4173`.

- [ ] **Step 4: Connect and seed graph data through the UI**

```bash
rtk playwright-cli open http://127.0.0.1:4173
rtk playwright-cli snapshot
rtk playwright-cli fill "getByLabel('Bearer token')" "varve-explorer-dev-token"
rtk playwright-cli click "getByRole('button', { name: 'Connect' })"
rtk playwright-cli fill "getByLabel('GQL query')" "INSERT (:Person {_id: 1, name: 'Ada'}), (:Person {_id: 2, name: 'Grace'}); MATCH (a:Person {_id: 1}), (b:Person {_id: 2}) INSERT (a)-[:KNOWS {since: 2026}]->(b)"
rtk playwright-cli click "getByRole('tab', { name: 'Write' })"
rtk playwright-cli click "getByRole('button', { name: 'Run query' })"
rtk playwright-cli snapshot
```

Expected: healthy connection, transaction receipt, nonzero node/relationship side effects, and no browser error page.

- [ ] **Step 5: Verify parameterized table and raw results**

Enter `MATCH (p:Person) WHERE p.name = $name RETURN p.name AS name, p._id AS id`, set parameters to `{ "name": "Ada" }`, select Read, and run. Open Table and assert the Ada row, then Raw and assert the response contains `rows`. Use Playwright refs from each fresh snapshot for interactions whose generated DOM IDs vary.

- [ ] **Step 6: Verify graph interaction and fallback**

Run `MATCH p = (a:Person)-[:KNOWS]->(b:Person) RETURN p`. Open Graph, select a node, verify Element inspector and `Inferred from GQL`, then click Zoom in, Zoom out, Fit graph, and Reset layout. Run `MATCH (p:Person) RETURN p.name AS name`; verify Graph explains that topology is unavailable while Table remains usable.

- [ ] **Step 7: Verify history, favorites, schema, settings, and responsive UI**

Favorite the path query as `People connections`, open Favorites, rerun it, open History and rerun the parameterized query, open Observed schema and verify Person and KNOWS, switch theme, reload, and verify the non-secret choices persist. Resize to 390x844 with `rtk playwright-cli resize 390 844`; open navigation and inspector Sheets and verify no page-level horizontal overflow.

- [ ] **Step 8: Verify recoverable errors and cancellation**

Run malformed `MATCH (` and verify `Invalid request` while the composer is retained. Run a read with basis `999999999` and timeout `30000`, click Cancel query before completion, and verify `Request cancelled`. Disconnect, attempt `wrong-token`, verify `Authentication required`, reconnect with the development token, and confirm prior non-secret history remains.

- [ ] **Step 9: Verify the token is session-only and inspect browser diagnostics**

```bash
rtk playwright-cli localstorage-list
rtk playwright-cli sessionstorage-list
rtk playwright-cli console
rtk playwright-cli requests
rtk playwright-cli close
rtk playwright-cli open http://127.0.0.1:4173
rtk playwright-cli snapshot
```

Expected: no token appears in local/session storage, console, request URLs, rendered DOM, or response bodies; after closing the in-memory browser session and reopening, the connection Dialog requires the token again.

- [ ] **Step 10: Fix every discovered issue using the correct workflow**

For TypeScript logic defects, use systematic debugging and add a failing Vitest regression before changing production logic. For UI-only defects, reproduce with Playwright CLI, edit the Svelte/CSS implementation without UI TDD, rerun Svelte checks/build, and replay the affected browser flow. Commit fixes with `fix:` prefixes.

- [ ] **Step 11: Run the completion audit and final commit**

```bash
rtk pnpm --dir explore verify
rtk git diff --check
rtk git status --short
```

Audit every section of `docs/superpowers/specs/2026-07-12-varve-explorer-design.md` against files, fresh command output, and the completed browser session. If tracked changes remain, commit them with an accurate prefixed message. Do not declare v1 complete while any requirement lacks direct evidence.
