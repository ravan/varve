# Third-Party Notices

Varve Explorer is MIT-licensed original work. The following are every direct runtime dependency declared in `package.json` under `dependencies`; transitive dependencies and development-only packages are covered by their own installed license files and the automated license inventory.

| Package                   | Project                                                             | License    |
| ------------------------- | ------------------------------------------------------------------- | ---------- |
| `@codemirror/commands`    | [CodeMirror](https://codemirror.net/)                               | MIT        |
| `@codemirror/lang-json`   | [CodeMirror JSON language](https://github.com/codemirror/lang-json) | MIT        |
| `@codemirror/language`    | [CodeMirror](https://codemirror.net/)                               | MIT        |
| `@codemirror/state`       | [CodeMirror](https://codemirror.net/)                               | MIT        |
| `@codemirror/view`        | [CodeMirror](https://codemirror.net/)                               | MIT        |
| `@internationalized/date` | [React Spectrum](https://github.com/adobe/react-spectrum)           | Apache-2.0 |
| `@lezer/highlight`        | [Lezer](https://lezer.codemirror.net/)                              | MIT        |
| `bits-ui`                 | [Bits UI](https://github.com/huntabyte/bits-ui)                     | MIT        |
| `cytoscape`               | [Cytoscape.js](https://js.cytoscape.org/)                           | MIT        |
| `lucide-svelte`           | [Lucide](https://lucide.dev/)                                       | ISC        |
| `mode-watcher`            | [mode-watcher](https://github.com/svecosystem/mode-watcher)         | MIT        |
| `svelte-sonner`           | [svelte-sonner](https://github.com/wobsoriano/svelte-sonner)        | MIT        |

## Authorized build-tool exception

`lightningcss` 1.32.0 and its matching platform packages are indirect Vite build tooling licensed under MPL-2.0. They are not direct runtime dependencies and are the sole controller-authorized MPL-2.0 exception. The package license gate excludes exactly `lightningcss@1.32.0` from its otherwise strict allowlist; changing its version or introducing any other MPL-2.0 package requires a fresh review.

Generate the strict installed dependency report with `rtk pnpm --dir explore run licenses` and the pnpm production inventory with `rtk pnpm --dir explore licenses list --prod`.
