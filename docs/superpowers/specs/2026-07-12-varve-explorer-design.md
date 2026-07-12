# Varve Explorer v1 Design

**Status:** Approved design

**Date:** 2026-07-12

**Product:** Varve Explorer

## 1. Purpose

Varve Explorer is an independently authored, MIT-licensed graphical query workspace for Varve. It gives developers a browser-based way to connect to a real Varve HTTP server, write and execute GQL, inspect tabular and graph-shaped results, and reuse previous work.

The product takes high-level behavioral inspiration from graph database browsers, including Neo4j Browser, but it is not a source-code port. The implementation, visual design, component structure, copy, assets, tests, and documentation must be original.

## 2. V1 Scope

V1 is a focused graph browser and includes:

- Connection and health handling for a configured Varve HTTP server.
- A GQL editor with keyboard execution and read/write mode selection.
- JSON parameters and optional Varve basis controls.
- Read queries through `POST /v1/query` and mutations through `POST /v1/tx`.
- A session result feed with graph, table, and raw representations.
- Query history and favorites.
- An observed-schema panel derived from executed and saved GQL.
- User settings for appearance and workspace behavior.
- Clear loading, empty, degraded, authentication, timeout, backpressure, writer, cancellation, malformed-response, and network states.
- Responsive, keyboard-accessible interaction.
- Production build and deployment documentation.

V1 does not include database administration, guides, projects, file import, saved folders, multi-user accounts, cloud synchronization, telemetry, plugins, or full GQL language-server behavior.

## 3. Clean-Room and Licensing Boundary

The existing `refs/neo4j-browser` checkout is GPL-3.0 licensed. It may be consulted only to understand public, high-level product behavior such as the usefulness of a query composer, persistent history, result frames, and graph/table result modes.

The following must not be copied or adapted from that checkout:

- Source code, tests, configuration, architecture, internal names, or algorithms.
- Components, styles, layout measurements, color values, typography, icons, images, fonts, or other assets.
- User-facing prose, help content, examples, fixtures, snapshots, or test scenarios.
- Dependency-specific glue or graph rendering code.

Varve Explorer will have its own `explore/LICENSE` containing the MIT license, `package.json` will declare `MIT`, and its dependency inventory must contain only licenses compatible with MIT distribution. The root Rust workspace currently declares Apache-2.0; the nested Explorer package remains separately MIT-licensed as explicitly requested.

## 4. Technology Baseline

The application lives entirely under `explore/` except for repository-level design and plan documents.

- Package manager: pnpm with a committed `pnpm-lock.yaml`.
- Build system: Vite through the latest stable SvelteKit release available at implementation time.
- Framework: latest stable SvelteKit and Svelte 5 using runes (`$state`, `$derived`, `$effect`, and `$props`) rather than legacy component reactivity.
- Language: strict TypeScript.
- Deployment: `@sveltejs/adapter-node` so the same process serves the UI and BFF endpoints.
- Styling: Tailwind CSS 4.
- Component primitives: shadcn-svelte. Product components must compose shadcn-svelte primitives instead of replacing them with bespoke equivalents where a suitable primitive exists.
- Editor: CodeMirror 6 with independently authored lightweight GQL highlighting and key bindings.
- Graph rendering: Cytoscape.js with an original Varve theme.
- Icons: Lucide icons through the normal shadcn-svelte convention.
- TypeScript tests: Vitest.
- Static analysis: Oxlint.
- Formatting: Oxfmt.

Exact dependency versions are resolved and locked during scaffolding. Direct dependencies must use stable releases and avoid GPL/AGPL/copyleft runtime packages.

## 5. Architecture

Varve Explorer uses a SvelteKit backend-for-frontend (BFF). The Node server serves the application and proxies a narrow set of same-origin API routes to a configured Varve target. This works with the current Varve server, which does not expose browser CORS headers, without requiring a Rust protocol change.

The BFF is stateless with respect to application data. It does not persist credentials, queries, or results. The browser stores non-secret workspace data locally. Authentication uses a session-only HTTP cookie.

### 5.1 Deployment Configuration

The required environment variable is:

- `VARVE_URL`: absolute `http` or `https` base URL of the Varve server. Development defaults to `http://127.0.0.1:8080` only when no production build is running.

Optional variables are:

- `VARVE_DISPLAY_NAME`: human-readable connection name, default `Local Varve`.
- `VARVE_ALLOWED_WRITER_ORIGINS`: comma-separated absolute origins permitted for a writer redirect. The configured `VARVE_URL` origin is always allowed.
- `VARVE_REQUEST_TIMEOUT_MS`: upstream timeout, default `60000`.
- `VARVE_MAX_REQUEST_BYTES`: maximum Explorer proxy request size, default `1048576`.

Users cannot submit arbitrary upstream URLs through the browser. Supporting multiple configured targets is post-v1. This prevents the BFF from becoming an open SSRF proxy.

### 5.2 BFF Routes

The SvelteKit server exposes:

- `GET /api/config`: returns the public display name and safe target description, never credentials.
- `POST /api/session/connect`: validates a submitted bearer token by requesting Varve status, then creates a session cookie.
- `DELETE /api/session`: clears the session cookie.
- `GET /api/varve/health`: proxies public `/healthz`.
- `GET /api/varve/status`: proxies authenticated `/v1/status`.
- `POST /api/varve/query`: validates and proxies `/v1/query` with `Accept: application/json`.
- `POST /api/varve/tx`: validates and proxies `/v1/tx`.

The cookie is host-only, `HttpOnly`, `SameSite=Strict`, path `/`, and session-only. It is `Secure` in production. The token is never returned to client JavaScript, persisted to disk, included in logs, or embedded in error messages.

The proxy accepts JSON only, enforces the configured body limit and timeout, strips hop-by-hop headers, forwards only required headers, and maps upstream failures into a stable Explorer error envelope. A `421 misdirected_request` may be retried once against its advertised writer only when that writer origin is explicitly allowed. Redirect loops are rejected.

## 6. Client Modules

The frontend is divided by responsibility:

- **Connection session:** public target information, connectivity state, connect/disconnect, and health polling.
- **Varve client:** typed requests and normalized Explorer errors.
- **GQL execution:** read/write classification, request assembly, cancellation, timing, and immutable execution frames.
- **Result normalization:** stable columns, row values, transaction receipts, binary formatting, and raw response preservation.
- **Graph extraction:** conservative topology extraction from result rows plus the submitted GQL shape.
- **Observed schema:** labels and relationship types observed in executed or favorited GQL.
- **Workspace persistence:** versioned history, favorites, and settings in local storage.
- **UI composition:** Svelte components built from shadcn-svelte primitives.

Business logic modules do not import Svelte components. Browser storage and network access sit behind small interfaces so logic can be tested without a DOM.

## 7. User Experience

### 7.1 Application Shell

The desktop layout contains:

- A left navigation rail for New query, Observed Schema, History, Favorites, and Settings.
- A top connection bar showing health, target name, reconnect/disconnect, and theme controls.
- A central composer followed by a scrollable execution feed.
- A contextual inspector beside graph results when space permits.

On narrow screens, the rail and inspector become shadcn-svelte Sheets. The composer, result tabs, table, and graph remain usable without horizontal page overflow.

The original visual language uses a dark ink background, quiet slate surfaces, warm amber execution accents, and cool cyan data accents. It does not reproduce Neo4j branding or layout measurements.

### 7.2 shadcn-svelte Foundation

At minimum, the following shadcn-svelte primitives form the base layer:

- Button, Input, Textarea, Label, Checkbox, and Switch for controls.
- Dialog for connection and confirmation flows.
- Sheet for responsive navigation and inspectors.
- Tabs for result modes.
- Dropdown Menu for frame actions.
- Tooltip for icon controls.
- Table for result structure.
- Badge for status and result metadata.
- Card, Separator, Scroll Area, Skeleton, Alert, and Toast/Sonner for layout and feedback.

Varve-specific components wrap and compose these primitives. They may add behavior and product styling but must retain the primitives' accessibility contracts.

### 7.3 Connection Flow

On first load, Explorer checks public health and session status. If no authenticated session exists, it opens the connection Dialog. The Dialog shows the configured target, accepts a bearer token, and offers Connect. Tokens are never prefilled after a browser restart.

An empty token is allowed because Varve deployments may use an authenticator that accepts anonymous access. A failed status check keeps the Dialog open and distinguishes authentication, unreachable server, degraded server, and malformed response.

### 7.4 Composer

The composer contains:

- A CodeMirror GQL editor with line numbers, bracket matching, lightweight keyword highlighting, and `Mod-Enter` execution.
- A Read/Write segmented control. A conservative classifier selects Write only when the top-level statement contains a mutation or graph-definition clause; users can override it before execution.
- A collapsible parameters editor accepting a JSON object.
- Advanced basis controls accepting either a non-negative transaction ID or `at:<packed-u64>`, plus an optional positive timeout in milliseconds.
- Run and Cancel actions with visible keyboard hints.

Parameter values must match Varve's HTTP contract: null, boolean, signed integer, finite number, string, or an exact `{ "$bytes": "<base64>" }` object. Arrays and other objects are rejected locally with field-specific messages.

### 7.5 Execution Feed

Every run creates an immutable frame containing the submitted GQL, mode, sanitized parameters summary, state, timing, and response. The active request may be cancelled through `AbortController`. Cancellation is reported honestly as client cancellation; Explorer does not claim that server execution was rolled back.

Frames can be collapsed, rerun, copied, favorited, pinned for the session, or closed. To bound memory, Explorer retains at most 25 unpinned frames and evicts the oldest completed frame first.

Read frames offer Graph, Table, and Raw tabs. Write frames show the transaction receipt and side-effect counts plus Raw. Empty results have a deliberate empty state rather than a blank table.

### 7.6 Table and Raw Results

The Table view derives a stable union of columns in first-seen order. Missing values and explicit nulls remain distinguishable. Values receive safe, deterministic formatting for strings, numbers, booleans, arrays, objects, and binary/base64 identifiers. The table supports local sorting and pages of 50 rows without mutating the raw response.

The Raw view renders escaped, formatted JSON and supports copy-to-clipboard. It never interprets result content as HTML.

### 7.7 Graph Results

Varve's current JSON query response contains rows rather than rich graph entity objects. Explorer therefore uses conservative, capability-aware graph extraction:

- A small query-shape tokenizer recognizes variables, node labels, relationship types, directions, named paths, and return aliases in supported `MATCH` patterns. It is not a GQL validator.
- Opaque binary identifiers returned for node and relationship variables are mapped to pattern positions only when the mapping is unambiguous.
- Named path values represented as alternating node/relationship identifiers produce ordered topology.
- Repeated identifiers deduplicate into the same visual element.
- Scalar columns associated with an unambiguous returned variable appear in the inspector.
- Labels and relationship types come from the submitted pattern and are marked as inferred.
- If topology cannot be proven, the Graph tab is disabled with an explanation and Table remains available.

The graph canvas supports select, pan, zoom, fit, reset layout, and reduced-motion mode. Selecting an element opens an inspector with identity, inferred type/labels, and related row values. Rendering is capped at 1,000 elements; larger results show a truncation warning while preserving full table/raw data.

### 7.8 Observed Schema

Varve v1 has no schema-introspection HTTP endpoint. Explorer does not pretend otherwise. The Observed Schema panel extracts node labels and relationship types from successfully executed and favorited GQL, records first/last seen times and usage counts, and labels the view clearly as query-derived rather than authoritative database metadata.

Selecting an observed label or type inserts a safe starter pattern into a new composer. Extraction failure never blocks query execution.

### 7.9 History, Favorites, and Settings

History stores the latest 100 completed submissions with GQL, mode, non-secret parameters, timestamp, duration, row/effect counts, and outcome. Consecutive identical submissions coalesce while updating usage metadata. History can be rerun or cleared.

Favorites store a user-provided name, GQL, mode, parameters, and optional notes. Favorites can be edited, run, duplicated, and deleted. Tokens and server responses are never stored.

Settings include system/light/dark theme, graph motion, default result tab, history enabled/disabled, and confirmation before clearing data. Storage records include a schema version and migrate or reset safely when incompatible.

## 8. Error and Recovery Contract

Explorer maps failures to stable categories:

- `unauthorized`: reopen connection without revealing or retaining the rejected token.
- `invalid_request`: retain the composer and identify whether local input or Varve rejected it.
- `not_acceptable` or malformed response: report a client/server compatibility problem.
- `basis_timeout`: retain basis settings and offer rerun.
- `backpressure`: show the retry delay and allow manual retry; no hidden retry of writes.
- `misdirected_request`: retry a write once only to an allowed writer, otherwise show the advertised-target policy failure.
- `writer_unavailable`, `writer_fenced`, `follower_failed`, or degraded health: show service state and preserve work.
- `internal`: show a safe generic message and correlation timing without upstream secrets.
- network timeout, offline, or cancellation: distinguish transport failure from a server rejection.

Error frames remain in history without storing credential material. Toasts announce transient events; durable errors remain inline in their execution frame.

## 9. Accessibility

V1 requires:

- Full keyboard operation for connection, navigation, composer execution, result tabs, frame actions, and dialogs.
- Visible focus indicators and logical focus return after overlays close.
- Semantic labels and accessible names for icon-only controls.
- Status communication through text and icons rather than color alone.
- Contrast-safe light and dark themes.
- Respect for `prefers-reduced-motion` and a persistent graph-motion override.
- Scrollable tables and graph controls that do not trap keyboard focus.

## 10. Testing Strategy

Only TypeScript logic uses TDD, exactly as requested. Every new logic function begins with a failing Vitest test, the failure is observed, and then the minimal implementation is added. Covered logic includes:

- Target and proxy validation, header filtering, timeouts, cookie options, writer redirect policy, and upstream error mapping.
- GQL read/write classification and query-shape extraction.
- Parameter and basis validation.
- Result normalization, sorting, pagination, and formatting.
- Graph extraction, deduplication, ambiguity fallback, and element caps.
- Observed-schema extraction and aggregation.
- History/favorite reducers, limits, serialization, and migrations.

Svelte UI components are not developed with TDD. They are validated through Svelte type checking, linting, production builds, accessibility-conscious implementation, and browser interaction.

Playwright CLI must verify the built application against a real Varve HTTP server, not a mocked route. The sanity flow covers:

1. Start the repository's real Varve HTTP fixture/server with representative graph data.
2. Open Explorer, connect, and observe healthy status.
3. Execute a parameterized read and inspect table and raw results.
4. Execute a topology-returning query and interact with graph selection, zoom, fit, and inspector.
5. Execute a write and inspect transaction side effects.
6. Exercise history, favorites, observed schema, settings, reload persistence, and responsive navigation.
7. Trigger invalid GQL and an authentication failure and verify recoverable error states.
8. Start and cancel a request and verify cancellation messaging.
9. Restart the browser session and verify the token is not restored.
10. Inspect browser console and network activity for unexpected errors or secret leakage.

## 11. Release Gates

V1 is shippable only when all of the following pass from a clean checkout:

- pnpm installation with a frozen lockfile.
- Vitest logic suite.
- `svelte-check` with no errors.
- Oxlint with no errors.
- Oxfmt check with no differences.
- Production SvelteKit Node build.
- Dependency license audit with no incompatible runtime dependency.
- Playwright CLI sanity flow against the real Varve HTTP server.
- Browser console inspection with no unexpected errors.
- README instructions for development, configuration, production build, startup, security assumptions, and real-server verification.
- MIT license and third-party notices in `explore/`.

## 12. Acceptance Criteria

The design is complete when a developer can clone the repository, follow `explore/README.md`, run a real Varve HTTP server, start Varve Explorer, authenticate, execute supported GQL reads and writes, understand failures, inspect results in every applicable representation, reuse queries, and build a deployable Node artifact.

No acceptance claim may rely only on mocked data, a dev-server screenshot, or unit tests. The final audit must tie each explicit requirement in this document and the originating request to current files, fresh command output, and real browser behavior.
