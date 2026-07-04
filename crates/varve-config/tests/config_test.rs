use serde::Deserialize;
use serial_test::serial;
use varve_config::Config;

const SAMPLE: &str = r#"
[node]
roles = ["writer", "query"]

[log]
backend = "local"
group_commit_window_ms = 15

[log.local]
dir = "/tmp/varve-log"
"#;

#[derive(Deserialize, Debug, PartialEq)]
struct LogLocal {
    dir: String,
}

#[test]
#[serial]
fn reads_sections_and_backend() {
    std::env::remove_var("VARVE__LOG__BACKEND");
    let cfg = Config::from_toml_str(SAMPLE).unwrap();
    let log = cfg.section("log").unwrap();
    assert_eq!(log.backend(), Some("local"));
    let local: LogLocal = log.child("local").unwrap().get().unwrap();
    assert_eq!(local.dir, "/tmp/varve-log");
}

#[test]
#[serial]
fn missing_section_is_none() {
    std::env::remove_var("VARVE__LOG__BACKEND");
    let cfg = Config::from_toml_str(SAMPLE).unwrap();
    assert!(cfg.section("storage").is_none());
}

#[test]
#[serial]
fn env_var_overrides_key() {
    // env override: VARVE__LOG__BACKEND=memory
    std::env::remove_var("VARVE__LOG__BACKEND");
    std::env::set_var("VARVE__LOG__BACKEND", "memory");
    let cfg = Config::from_toml_str(SAMPLE).unwrap();
    assert_eq!(cfg.section("log").unwrap().backend(), Some("memory"));
    std::env::remove_var("VARVE__LOG__BACKEND");
}

/// `apply_env_overrides` now supports nested keys: all segments but the
/// last form the nested table path, and the last segment is the key. A
/// 3-segment var like `VARVE__LOG__LOCAL__DIR` walks/creates `[log.local]`
/// and sets `dir` within it.
#[test]
#[serial]
fn nested_override_applies() {
    std::env::remove_var("VARVE__LOG__LOCAL__DIR");
    std::env::set_var("VARVE__LOG__LOCAL__DIR", "/override");
    let cfg = Config::from_toml_str(SAMPLE).unwrap();
    let local: LogLocal = cfg
        .section("log")
        .unwrap()
        .child("local")
        .unwrap()
        .get()
        .unwrap();
    assert_eq!(local.dir, "/override");
    std::env::remove_var("VARVE__LOG__LOCAL__DIR");
}

/// `apply_env_overrides` now coerces scalar values: a value that parses
/// cleanly as `bool`, then `i64`, then `f64` is stored as that TOML scalar
/// type rather than always as a string. A numeric override on an
/// integer-typed key must deserialize straight into an integer field.
#[test]
#[serial]
fn numeric_override_is_coerced() {
    #[derive(Deserialize)]
    struct GroupCommitWindowNum {
        group_commit_window_ms: u32,
    }

    std::env::remove_var("VARVE__LOG__GROUP_COMMIT_WINDOW_MS");
    std::env::set_var("VARVE__LOG__GROUP_COMMIT_WINDOW_MS", "30");
    let cfg = Config::from_toml_str(SAMPLE).unwrap();
    let log = cfg.section("log").unwrap();

    let as_number = log.get::<GroupCommitWindowNum>().unwrap();
    assert_eq!(as_number.group_commit_window_ms, 30);
    std::env::remove_var("VARVE__LOG__GROUP_COMMIT_WINDOW_MS");
}

/// A value that parses as an exact `true`/`false` literal is coerced to a
/// TOML bool, not left as a string.
#[test]
#[serial]
fn bool_override_is_coerced() {
    #[derive(Deserialize)]
    struct LogFlag {
        enabled: bool,
    }

    std::env::remove_var("VARVE__LOG__ENABLED");
    std::env::set_var("VARVE__LOG__ENABLED", "true");
    let cfg = Config::from_toml_str(SAMPLE).unwrap();
    let log = cfg.section("log").unwrap();
    let flag = log.get::<LogFlag>().unwrap();
    assert!(flag.enabled);
    std::env::remove_var("VARVE__LOG__ENABLED");
}

/// Deep nesting (3+ segments) walks/creates every intermediate table.
#[test]
#[serial]
fn deep_nested_override_applies() {
    #[derive(Deserialize)]
    struct S3Config {
        endpoint: String,
    }

    std::env::remove_var("VARVE__STORAGE__S3__ENDPOINT");
    std::env::set_var("VARVE__STORAGE__S3__ENDPOINT", "https://example.invalid");
    let cfg = Config::from_toml_str(SAMPLE).unwrap();
    let s3: S3Config = cfg
        .section("storage")
        .unwrap()
        .child("s3")
        .unwrap()
        .get()
        .unwrap();
    assert_eq!(s3.endpoint, "https://example.invalid");
    std::env::remove_var("VARVE__STORAGE__S3__ENDPOINT");
}

/// If the path to a nested override passes through an existing non-table
/// (scalar) value, the whole override is skipped: existing config is left
/// intact and no panic occurs.
#[test]
#[serial]
fn override_through_non_table_intermediate_is_skipped() {
    std::env::remove_var("VARVE__LOG__BACKEND__SUB");
    std::env::set_var("VARVE__LOG__BACKEND__SUB", "y");
    // `log.backend` is a scalar string in SAMPLE; walking through it to set
    // `log.backend.sub` must not panic and must not clobber `backend`.
    let cfg = Config::from_toml_str(SAMPLE).unwrap();
    assert_eq!(cfg.section("log").unwrap().backend(), Some("local"));
    std::env::remove_var("VARVE__LOG__BACKEND__SUB");
}
