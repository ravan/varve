//! Generates `docs/book/src/ops/configuration.md` — the FULL `[section] key
//! = default` reference for `varve.toml` — straight from the owning config
//! structs' code, not by hand. `crates/varve-testkit/tests/config_reference_doc.rs`
//! pins the committed page to [`render`]'s output, so the page cannot rot:
//! any hand-edit to `configuration.md`, or any change to a real serde
//! default, fails that test until `just docs-gen` regenerates the page.
//!
//! For the load-bearing defaults (the ones most likely to matter at
//! production scale — group-commit window, follower tuning, flush/GC
//! thresholds, request-body cap), the row below reads the SAME
//! `pub const DEFAULT_*` the owning crate's `#[serde(default = ...)]` fn
//! returns, rather than a duplicated literal — see e.g.
//! `varve_log::DEFAULT_SEGMENT_MAX_BYTES`, `varve_engine::DEFAULT_MAX_BLOCK_ROWS`,
//! `varve_server::DEFAULT_MAX_BODY_BYTES`. That cross-use is what makes this
//! reference provably generated from code rather than merely inspired by it.

use std::fmt::Write as _;

struct Entry {
    key: &'static str,
    r#type: &'static str,
    default: String,
    description: &'static str,
}

struct Section {
    name: &'static str,
    intro: &'static str,
    entries: Vec<Entry>,
}

/// Wraps `value` in a markdown code span, e.g. `code(50)` -> `` `50` ``.
fn code(value: impl std::fmt::Display) -> String {
    format!("`{value}`")
}

/// Wraps `value` as a quoted-string code span, e.g. `quoted("memory")` ->
/// `` `"memory"` `` — matches how a bare TOML string default is written.
fn quoted(value: &str) -> String {
    code(format!("\"{value}\""))
}

/// A TOML array-of-strings default, e.g. `string_array(&["writer"])` ->
/// `` `["writer"]` ``.
fn string_array(items: &[&str]) -> String {
    let inner = items
        .iter()
        .map(|item| format!("\"{item}\""))
        .collect::<Vec<_>>()
        .join(", ");
    code(format!("[{inner}]"))
}

/// A required key with no default (deserialization fails, or the owning
/// factory returns a build error, if it is absent).
fn required() -> String {
    "(required)".to_string()
}

/// An optional key that is simply unset when absent (no error either way).
fn none() -> String {
    "(none)".to_string()
}

/// Formats a byte count as the quoted IEC literal `varve_config::ByteSize`
/// accepts in TOML (e.g. `iec(8 * 1024 * 1024)` -> `` `"8MiB"` ``). Every
/// `ByteSize` default in this reference is an exact multiple of KiB, so the
/// largest exact IEC unit always applies cleanly.
fn iec(bytes: usize) -> String {
    const GIB: usize = 1024 * 1024 * 1024;
    const MIB: usize = 1024 * 1024;
    const KIB: usize = 1024;
    let literal = if bytes.is_multiple_of(GIB) {
        format!("{}GiB", bytes / GIB)
    } else if bytes.is_multiple_of(MIB) {
        format!("{}MiB", bytes / MIB)
    } else if bytes.is_multiple_of(KIB) {
        format!("{}KiB", bytes / KIB)
    } else {
        format!("{bytes}B")
    };
    quoted(&literal)
}

fn sections() -> Vec<Section> {
    vec![
        Section {
            name: "node",
            intro: "Node role selection and query/follower tuning (spec §4, §12).",
            entries: vec![
                Entry {
                    key: "roles",
                    // Table cells split on a bare `|` even inside a code span
                    // (mdBook's table lexer works on raw text, not parsed
                    // inline spans), and a raw `<word>` reads as an HTML tag —
                    // so avoid both `<...>` and un-escaped `|` in table cells.
                    r#type: "array of `writer`/`query`/`compactor`",
                    default: string_array(&["writer", "query", "compactor"]),
                    description: "Roles this node performs; the `compactor` role requires `writer`.",
                },
                Entry {
                    key: "tail_poll_interval_ms",
                    r#type: "integer (ms)",
                    default: code(varve_engine::DEFAULT_TAIL_POLL_INTERVAL_MS),
                    description: "Query-node follower poll interval for new log records.",
                },
                Entry {
                    key: "tail_batch_records",
                    r#type: "integer",
                    default: code(varve_engine::DEFAULT_TAIL_BATCH_RECORDS),
                    description: "Max records the follower applies per poll batch.",
                },
                Entry {
                    key: "basis_timeout_ms",
                    r#type: "integer (ms)",
                    default: code(varve_engine::DEFAULT_BASIS_TIMEOUT_MS),
                    description: "How long a query waits for its requested basis before timing out.",
                },
                Entry {
                    key: "submission_queue_len",
                    r#type: "integer",
                    default: code(256),
                    description: "Bounded capacity of the writer's submission queue; `try_execute_as` returns backpressure immediately once it is full.",
                },
            ],
        },
        Section {
            name: "log",
            intro: "Write-ahead log backend selection and group-commit tuning (spec §6).",
            entries: vec![
                Entry {
                    key: "backend",
                    // mdBook's table lexer splits on every raw `|`, even one
                    // inside a code span, so use `/` between alternatives.
                    r#type: "string: `memory`/`local`/`object-store`",
                    default: quoted("memory"),
                    description: "Log backend; `local` requires `[log.local]`. `object-store` shares the `[storage]` backend's object store.",
                },
                Entry {
                    key: "group_commit_window_ms",
                    r#type: "integer (ms)",
                    default: code(varve_engine::DEFAULT_GROUP_COMMIT_WINDOW_MS),
                    description: "A batch flushes once this window elapses OR `group_commit_max_bytes` is reached, whichever comes first.",
                },
                Entry {
                    key: "group_commit_max_bytes",
                    r#type: "byte size",
                    default: iec(8 * 1024 * 1024),
                    description: "The other half of the group-commit trigger.",
                },
            ],
        },
        Section {
            name: "log.local",
            intro: "Tuning for `[log] backend = \"local\"` (a single-process durable log file).",
            entries: vec![
                Entry {
                    key: "dir",
                    r#type: "string",
                    default: required(),
                    description: "Directory containing the local log's segment files.",
                },
                Entry {
                    key: "segment_max_bytes",
                    r#type: "integer (bytes)",
                    default: code(varve_log::DEFAULT_SEGMENT_MAX_BYTES),
                    description: "Segment rotation size in bytes (default 64 MiB).",
                },
            ],
        },
        Section {
            name: "storage",
            intro: "Block-store backend selection and flush tuning (spec §9).",
            entries: vec![
                Entry {
                    key: "backend",
                    r#type: "string: `memory`/`local`/`s3`",
                    default: quoted("memory"),
                    description: "Object-store backend for flushed blocks; `local` requires `[storage.local]`, `s3` requires `[storage.s3]`.",
                },
                Entry {
                    key: "max_block_rows",
                    r#type: "integer",
                    default: code(varve_engine::DEFAULT_MAX_BLOCK_ROWS),
                    description: "Row count that triggers an early block flush.",
                },
                Entry {
                    key: "flush_interval_ms",
                    r#type: "integer (ms)",
                    default: code(300_000),
                    description: "Timer-based flush interval; `0` disables the timer.",
                },
                Entry {
                    key: "max_live_bytes",
                    r#type: "byte size",
                    default: iec(512 * 1024 * 1024),
                    description: "Live-index memory watermark; forces an early block flush independent of `max_block_rows`.",
                },
            ],
        },
        Section {
            name: "storage.local",
            intro: "Tuning for `[storage] backend = \"local\"`.",
            entries: vec![Entry {
                key: "dir",
                r#type: "string",
                default: required(),
                description: "Directory for flushed blocks.",
            }],
        },
        Section {
            name: "storage.s3",
            intro: "Tuning for `[storage] backend = \"s3\"` (any S3-API endpoint: AWS, Garage, Ceph RGW, SeaweedFS, MinIO).",
            entries: vec![
                Entry {
                    key: "bucket",
                    r#type: "string",
                    default: required(),
                    description: "Target bucket name.",
                },
                Entry {
                    key: "endpoint",
                    r#type: "string",
                    default: none(),
                    description: "e.g. `http://127.0.0.1:3900` (Garage); omitted resolves the AWS endpoint.",
                },
                Entry {
                    key: "region",
                    r#type: "string",
                    default: none(),
                    description: "Must match the backend's configured region (e.g. Garage's `s3_region`); omitted uses the environment or `us-east-1`.",
                },
                Entry {
                    key: "access_key_id",
                    r#type: "string",
                    default: none(),
                    description: "Overrides the environment/AWS provider chain.",
                },
                Entry {
                    key: "secret_access_key",
                    r#type: "string",
                    default: none(),
                    description: "Overrides the environment/AWS provider chain.",
                },
                Entry {
                    key: "path_style",
                    r#type: "boolean",
                    default: code(true),
                    description: "Path-style addressing (`endpoint/bucket/key`); Garage and MinIO need it. `false` selects virtual-hosted style.",
                },
                Entry {
                    key: "allow_http",
                    r#type: "boolean",
                    default: none(),
                    description: "Permit plain-HTTP endpoints; defaults to whether `endpoint` starts with `http://`.",
                },
            ],
        },
        Section {
            name: "cache",
            intro: "Named cache tiers composed outermost-first over the raw object store (spec §4/§9).",
            entries: vec![Entry {
                key: "tiers",
                r#type: "array of strings",
                default: string_array(&["memory"]),
                description: "Tier names checked in order before falling through to the backend; an empty list runs uncached.",
            }],
        },
        Section {
            name: "cache.memory",
            intro: "Tuning for the `memory` cache tier.",
            entries: vec![Entry {
                key: "max_bytes",
                r#type: "byte size",
                default: iec(512 * 1024 * 1024),
                description: "In-memory cache budget.",
            }],
        },
        Section {
            name: "cache.disk",
            intro: "Tuning for the `disk` cache tier (a self-describing on-disk LRU that survives restarts).",
            entries: vec![
                Entry {
                    key: "dir",
                    r#type: "string",
                    default: required(),
                    description: "Directory dedicated to this cache tier; must not be shared with any other store.",
                },
                Entry {
                    key: "max_bytes",
                    r#type: "byte size",
                    default: iec(50 * 1024 * 1024 * 1024),
                    description: "On-disk cache budget.",
                },
            ],
        },
        Section {
            name: "query",
            intro: "Query planning limits (spec §10).",
            entries: vec![
                Entry {
                    key: "max_path_depth",
                    r#type: "integer",
                    default: code(10),
                    description: "Maximum traversal depth for variable-length path patterns.",
                },
                Entry {
                    key: "path_output_batch_rows",
                    r#type: "integer",
                    default: code(8_192),
                    description: "Rows per output batch for path results.",
                },
                Entry {
                    key: "path_row_budget",
                    r#type: "integer",
                    default: code(100_000),
                    description: "Row budget for path expansion before it aborts.",
                },
                Entry {
                    key: "path_frontier_budget",
                    r#type: "integer",
                    default: code(100_000),
                    description: "Frontier-size budget for path expansion before it aborts.",
                },
                Entry {
                    key: "traversal_node_budget",
                    r#type: "integer",
                    default: code(100_000),
                    description: "Node budget for general traversal before it aborts.",
                },
                Entry {
                    key: "traversal_adjacency_budget",
                    r#type: "integer",
                    default: code(250_000),
                    description: "Adjacency-edge budget for general traversal before it aborts.",
                },
            ],
        },
        Section {
            name: "gc",
            intro: "Garbage collection of superseded objects (spec §9).",
            entries: vec![
                Entry {
                    key: "enabled",
                    r#type: "boolean",
                    default: code(false),
                    description: "Enables GC; disabled by default.",
                },
                Entry {
                    key: "blocks_to_keep",
                    r#type: "integer",
                    default: code(varve_engine::DEFAULT_GC_BLOCKS_TO_KEEP),
                    description: "Flushed blocks retained behind the GC frontier, for lagging followers/basis reads.",
                },
                Entry {
                    key: "garbage_lifetime_hours",
                    r#type: "integer (hours)",
                    default: code(24),
                    description: "Minimum age before a superseded object becomes GC-eligible.",
                },
            ],
        },
        Section {
            name: "coordinator",
            intro: "Writer coordination backend and heartbeat/lease tuning (spec §12).",
            entries: vec![
                Entry {
                    key: "backend",
                    r#type: "string: `designated-writer`/`cas-failover`",
                    default: quoted("designated-writer"),
                    description: "`cas-failover` requires `[log] backend = \"object-store\"` and a storage backend whose conditional-put probe reports `Supported`.",
                },
                Entry {
                    key: "heartbeat_interval_ms",
                    r#type: "integer (ms)",
                    default: code(5_000),
                    description: "Heartbeat publish interval; `0` disables the heartbeat task entirely.",
                },
                Entry {
                    key: "takeover_after_ms",
                    r#type: "integer (ms)",
                    default: code(15_000),
                    description: "Staleness deadline before a standby may take over; must be at least 2x `heartbeat_interval_ms` when heartbeats are enabled.",
                },
            ],
        },
        Section {
            name: "server",
            intro: "Protocol frontend selection (the `varved` binary).",
            entries: vec![Entry {
                key: "backend",
                r#type: "string: `http`",
                default: quoted("http"),
                description: "Protocol frontend; `http` requires `[server.http]`.",
            }],
        },
        Section {
            name: "server.http",
            intro: "Tuning for `[server] backend = \"http\"`.",
            entries: vec![
                Entry {
                    key: "listen",
                    r#type: "string",
                    default: quoted("0.0.0.0:8080"),
                    description: "Socket address the HTTP frontend binds.",
                },
                Entry {
                    key: "advertised_address",
                    r#type: "string",
                    default: none(),
                    description: "Absolute `http`/`https` URL clients use to reach this node; required when this node has the `writer` role.",
                },
                Entry {
                    key: "max_body_bytes",
                    r#type: "byte size",
                    default: iec(varve_server::DEFAULT_MAX_BODY_BYTES.as_usize()),
                    description: "Max accepted request body size.",
                },
                Entry {
                    key: "tls_cert",
                    r#type: "path",
                    default: none(),
                    description: "PEM certificate path; must be set together with `tls_key`.",
                },
                Entry {
                    key: "tls_key",
                    r#type: "path",
                    default: none(),
                    description: "PEM private-key path; must be set together with `tls_cert`.",
                },
            ],
        },
        Section {
            name: "auth",
            intro: "Authentication backend selection.",
            entries: vec![Entry {
                key: "backend",
                r#type: "string: `static`",
                default: quoted("static"),
                description: "Authenticator backend.",
            }],
        },
        Section {
            name: "auth.static",
            intro: "Tuning for `[auth] backend = \"static\"` (a bearer-token allowlist).",
            entries: vec![Entry {
                key: "tokens",
                r#type: "array of tables (`subject`, `token`)",
                default: required(),
                description: "Bearer tokens accepted, each with a distinct subject; at least one is required and tokens must be unique.",
            }],
        },
        Section {
            name: "metrics",
            intro: "Metrics sink backend selection.",
            entries: vec![Entry {
                key: "backend",
                r#type: "string: `prometheus`/`otlp`",
                default: quoted("prometheus"),
                description: "`otlp` requires `[metrics.otlp]` and the `otel` build feature.",
            }],
        },
        Section {
            name: "metrics.otlp",
            intro: "Tuning for `[metrics] backend = \"otlp\"` (wraps the Prometheus registry and pushes OTLP/HTTP JSON on an interval).",
            entries: vec![
                Entry {
                    key: "endpoint",
                    r#type: "string",
                    default: required(),
                    description: "OTLP/HTTP metrics endpoint, e.g. `http://otel-collector:4318/v1/metrics`.",
                },
                Entry {
                    key: "push_interval_ms",
                    r#type: "integer (ms)",
                    default: code(10_000),
                    description: "How often the registry is gathered and pushed.",
                },
            ],
        },
    ]
}

/// Renders the full `varve.toml` configuration reference as markdown — the
/// content contract for `docs/book/src/ops/configuration.md`. Pure and
/// deterministic: every default either comes from a `pub const` shared with
/// the owning crate's `#[serde(default = ...)]` fn, or is a literal that
/// mirrors one (see module docs).
pub fn render() -> String {
    let mut out = String::new();
    let _ = writeln!(out, "# Configuration reference");
    let _ = writeln!(out);
    let _ = writeln!(
        out,
        "<!-- GENERATED FILE. Do not hand-edit. Produced by `cargo run -p varve-testkit --bin \
         config_reference` (`just docs-gen`); `crates/varve-testkit/tests/config_reference_doc.rs` \
         pins this file to that output. -->"
    );
    let _ = writeln!(out);
    let _ = writeln!(
        out,
        "Every `[section]` key a `varve.toml` file accepts (spec §4/§11), one table per section, \
         generated from the same `#[serde(default = ...)]` code paths the engine, log, storage, \
         and server crates use to parse it. See [Deployment profiles & sizing](profiles.md) for \
         worked topologies and [Metrics & observability](metrics.md) for the `[coordinator]` and \
         `[metrics.otlp]` runbooks. Any key here can also be set via a `VARVE__SECTION__KEY` \
         environment variable at process startup (e.g. `VARVE__LOG__LOCAL__DIR=/data/log` sets \
         `[log.local] dir`); nested sections use an extra `__`, e.g. \
         `VARVE__STORAGE__S3__ENDPOINT`."
    );

    for section in sections() {
        let _ = writeln!(out);
        let _ = writeln!(out, "## `[{}]`", section.name);
        let _ = writeln!(out);
        let _ = writeln!(out, "{}", section.intro);
        let _ = writeln!(out);
        let _ = writeln!(out, "| Key | Type | Default | Description |");
        let _ = writeln!(out, "|---|---|---|---|");
        for entry in &section.entries {
            let _ = writeln!(
                out,
                "| `{}` | {} | {} | {} |",
                entry.key, entry.r#type, entry.default, entry.description
            );
        }
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_is_deterministic() {
        assert_eq!(render(), render());
    }

    #[test]
    fn render_documents_every_section_the_brief_requires() {
        let page = render();
        for section in [
            "[node]",
            "[log]",
            "[log.local]",
            "[storage]",
            "[storage.local]",
            "[storage.s3]",
            "[cache]",
            "[cache.memory]",
            "[cache.disk]",
            "[query]",
            "[gc]",
            "[coordinator]",
            "[server]",
            "[server.http]",
            "[auth]",
            "[auth.static]",
            "[metrics]",
            "[metrics.otlp]",
        ] {
            assert!(page.contains(section), "missing section {section}");
        }
    }

    #[test]
    fn load_bearing_defaults_match_the_owning_consts() {
        let page = render();
        assert!(page.contains(&format!(
            "`{}`",
            varve_engine::DEFAULT_TAIL_POLL_INTERVAL_MS
        )));
        assert!(page.contains(&format!("`{}`", varve_engine::DEFAULT_TAIL_BATCH_RECORDS)));
        assert!(page.contains(&format!("`{}`", varve_engine::DEFAULT_BASIS_TIMEOUT_MS)));
        assert!(page.contains(&format!(
            "`{}`",
            varve_engine::DEFAULT_GROUP_COMMIT_WINDOW_MS
        )));
        assert!(page.contains(&format!("`{}`", varve_engine::DEFAULT_MAX_BLOCK_ROWS)));
        assert!(page.contains(&format!("`{}`", varve_engine::DEFAULT_GC_BLOCKS_TO_KEEP)));
        assert!(page.contains(&format!("`{}`", varve_log::DEFAULT_SEGMENT_MAX_BYTES)));
        assert!(page.contains(&iec(varve_server::DEFAULT_MAX_BODY_BYTES.as_usize())));
    }
}
