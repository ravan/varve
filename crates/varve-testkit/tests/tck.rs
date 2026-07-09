#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use varve_testkit::tck::gherkin::{parse_feature, Scenario};
use varve_testkit::tck::runner::{run_scenario, Outcome};

const PASS_RATE_GATE: f64 = 0.85;

#[tokio::test]
async fn open_cypher_tck_gate() {
    let root = project_root();
    let resources = root.join("resources/tck");
    let features = resources.join("features");
    let exclusions = read_exclusions(&resources.join("exclusions.toml")).expect("exclusions load");
    let core = read_key_list(&resources.join("core.txt")).expect("core load");
    let baseline = read_key_list(&resources.join("baseline.txt")).expect("baseline load");
    let report_only = std::env::var_os("VARVE_TCK_REPORT_ONLY").is_some();

    let mut records = Vec::new();
    for file in collect_feature_files(&features).expect("feature files") {
        let src = fs::read_to_string(&file).expect("feature should be utf-8");
        let feature = parse_feature(&src).unwrap_or_else(|err| {
            panic!("{}: {err}", file.display());
        });
        for parsed in &feature.scenarios {
            let key = format!("{}::{}", feature.name, parsed.name);
            let scenario = with_background(&feature.background, parsed);
            let outcome = run_scenario(&scenario, &exclusions).await;
            records.push(Record { key, outcome });
        }
    }
    records.sort_by(|a, b| a.key.cmp(&b.key));

    let summary = Summary::from_records(&records);
    write_report(&root.join("target/tck-report.json"), &summary, &records).expect("write report");
    write_outcomes(&root.join("target/tck-outcomes.tsv"), &records).expect("write outcomes");
    print_summary(&summary);

    if report_only {
        return;
    }

    let mut errors = Vec::new();
    for key in &core {
        if !records
            .iter()
            .any(|record| record.key == *key && record.outcome == Outcome::Passed)
        {
            errors.push(format!("core scenario not passed: {key}"));
        }
    }
    if summary.rate < PASS_RATE_GATE {
        errors.push(format!(
            "TCK pass rate {:.3} below gate {:.3}",
            summary.rate, PASS_RATE_GATE
        ));
    }
    for key in &baseline {
        if !records
            .iter()
            .any(|record| record.key == *key && record.outcome == Outcome::Passed)
        {
            errors.push(format!("baseline scenario not passed: {key}"));
        }
    }
    for record in &records {
        if record.outcome == Outcome::Passed && !baseline.contains(&record.key) {
            errors.push(format!(
                "newly passing scenario missing baseline.txt: {}",
                record.key
            ));
        }
    }

    assert!(
        errors.is_empty(),
        "TCK gate failures:\n{}",
        errors.join("\n")
    );
}

fn project_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("project root should canonicalize")
}

fn with_background(background: &[varve_testkit::tck::gherkin::Step], sc: &Scenario) -> Scenario {
    let mut steps = background.to_vec();
    steps.extend(sc.steps.clone());
    Scenario {
        feature: sc.feature.clone(),
        name: sc.name.clone(),
        tags: sc.tags.clone(),
        steps,
    }
}

fn collect_feature_files(dir: &Path) -> std::io::Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    collect_feature_files_into(dir, &mut files)?;
    files.sort();
    Ok(files)
}

fn collect_feature_files_into(dir: &Path, files: &mut Vec<PathBuf>) -> std::io::Result<()> {
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if path.is_dir() {
            collect_feature_files_into(&path, files)?;
        } else if path.extension().and_then(|ext| ext.to_str()) == Some("feature") {
            files.push(path);
        }
    }
    Ok(())
}

fn read_key_list(path: &Path) -> std::io::Result<BTreeSet<String>> {
    let src = fs::read_to_string(path)?;
    let mut keys = BTreeSet::new();
    for line in src.lines() {
        let key = line.trim();
        if !key.is_empty() && !key.starts_with('#') {
            keys.insert(key.to_string());
        }
    }
    Ok(keys)
}

fn read_exclusions(path: &Path) -> std::io::Result<BTreeMap<String, String>> {
    let src = fs::read_to_string(path)?;
    let mut entries = BTreeMap::new();
    let mut current_key: Option<String> = None;
    for raw in src.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some(section) = line.strip_prefix("[\"").and_then(|s| s.strip_suffix("\"]")) {
            current_key = Some(unescape(section));
            continue;
        }
        if let Some(reason) = line.strip_prefix("reason = \"") {
            let Some(reason) = reason.strip_suffix('"') else {
                continue;
            };
            if let Some(key) = current_key.as_ref() {
                entries.insert(key.clone(), unescape(reason));
            }
        }
    }
    Ok(entries)
}

#[derive(Debug)]
struct Record {
    key: String,
    outcome: Outcome,
}

#[derive(Debug)]
struct Summary {
    total: usize,
    excluded: usize,
    adapted: usize,
    passed: usize,
    rate: f64,
    failures: usize,
}

impl Summary {
    fn from_records(records: &[Record]) -> Self {
        let total = records.len();
        let excluded = records
            .iter()
            .filter(|record| matches!(record.outcome, Outcome::Excluded(_)))
            .count();
        let adapted = total.saturating_sub(excluded);
        let passed = records
            .iter()
            .filter(|record| record.outcome == Outcome::Passed)
            .count();
        let failures = records
            .iter()
            .filter(|record| !matches!(record.outcome, Outcome::Passed | Outcome::Excluded(_)))
            .count();
        let rate = if adapted == 0 {
            1.0
        } else {
            passed as f64 / adapted as f64
        };
        Self {
            total,
            excluded,
            adapted,
            passed,
            rate,
            failures,
        }
    }
}

fn print_summary(summary: &Summary) {
    println!("TCK summary");
    println!("total    {}", summary.total);
    println!("excluded {}", summary.excluded);
    println!("adapted  {}", summary.adapted);
    println!("passed   {}", summary.passed);
    println!("failed   {}", summary.failures);
    println!("rate     {:.3}", summary.rate);
}

fn write_report(path: &Path, summary: &Summary, records: &[Record]) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let failures = records
        .iter()
        .filter(|record| !matches!(record.outcome, Outcome::Passed | Outcome::Excluded(_)))
        .map(failure_json)
        .collect::<Vec<_>>()
        .join(",\n");
    let json = format!(
        concat!(
            "{{\n",
            "  \"total\": {},\n",
            "  \"excluded\": {},\n",
            "  \"adapted\": {},\n",
            "  \"passed\": {},\n",
            "  \"rate\": {:.6},\n",
            "  \"failures\": [\n",
            "{}\n",
            "  ]\n",
            "}}\n"
        ),
        summary.total, summary.excluded, summary.adapted, summary.passed, summary.rate, failures
    );
    fs::write(path, json)
}

fn write_outcomes(path: &Path, records: &[Record]) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut lines = String::new();
    for record in records {
        let (kind, reason) = match &record.outcome {
            Outcome::Passed => ("passed", ""),
            Outcome::Excluded(reason) => ("excluded", reason.as_str()),
            Outcome::Failed(reason) => ("failed", reason.as_str()),
            Outcome::Untranslatable(reason) => ("untranslatable", reason.as_str()),
        };
        lines.push_str(&record.key);
        lines.push('\t');
        lines.push_str(kind);
        lines.push('\t');
        lines.push_str(reason);
        lines.push('\n');
    }
    fs::write(path, lines)
}

fn failure_json(record: &Record) -> String {
    let (kind, reason) = match &record.outcome {
        Outcome::Passed => ("passed", ""),
        Outcome::Excluded(reason) => ("excluded", reason.as_str()),
        Outcome::Failed(reason) => ("failed", reason.as_str()),
        Outcome::Untranslatable(reason) => ("untranslatable", reason.as_str()),
    };
    format!(
        "    {{\"scenario\":\"{}\",\"outcome\":\"{}\",\"reason\":\"{}\"}}",
        escape_json(&record.key),
        kind,
        escape_json(reason)
    )
}

fn escape_json(input: &str) -> String {
    let mut escaped = String::new();
    for ch in input.chars() {
        match ch {
            '"' => escaped.push_str("\\\""),
            '\\' => escaped.push_str("\\\\"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            other => escaped.push(other),
        }
    }
    escaped
}

fn unescape(input: &str) -> String {
    let mut output = String::new();
    let mut chars = input.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            output.push(ch);
            continue;
        }
        let Some(escaped) = chars.next() else {
            output.push('\\');
            break;
        };
        match escaped {
            'n' => output.push('\n'),
            'r' => output.push('\r'),
            't' => output.push('\t'),
            '"' => output.push('"'),
            '\\' => output.push('\\'),
            'u' => {
                let mut hex = String::new();
                for _ in 0..4 {
                    match chars.peek().copied() {
                        Some(digit) if digit.is_ascii_hexdigit() => {
                            hex.push(digit);
                            chars.next();
                        }
                        _ => {
                            output.push('u');
                            output.push_str(&hex);
                            break;
                        }
                    }
                }
                if hex.len() == 4 {
                    if let Ok(codepoint) = u32::from_str_radix(&hex, 16) {
                        if let Some(decoded) = char::from_u32(codepoint) {
                            output.push(decoded);
                        }
                    }
                }
            }
            other => output.push(other),
        }
    }
    output
}

#[test]
fn unescape_decodes_unicode_escapes() {
    assert_eq!(unescape("a\\u2013b"), "a\u{2013}b");
}

#[test]
fn exclusions_do_not_hide_current_failures() {
    let root = project_root();
    let exclusions = read_exclusions(&root.join("resources/tck/exclusions.toml"))
        .expect("exclusions should load");

    let unstable = exclusions
        .iter()
        .filter(|(_, reason)| exclusion_reason_is_outcome_shaped(reason))
        .map(|(key, _)| key.as_str())
        .collect::<Vec<_>>();

    assert!(
        unstable.is_empty(),
        "exclusions must record stable non-goal or harness-scope reasons, not current failures: {unstable:?}"
    );
}

#[test]
fn exclusions_file_uses_parseable_section_format() {
    let root = project_root();
    let exclusions = read_exclusions(&root.join("resources/tck/exclusions.toml"))
        .expect("exclusions should load");

    assert!(
        !exclusions.is_empty(),
        "exclusions.toml must contain [\"feature::scenario\"] sections"
    );
}

fn exclusion_reason_is_outcome_shaped(reason: &str) -> bool {
    const OUTCOME_SHAPES: &[&str] = &[
        "current adapted harness failure:",
        "known v1 TCK adaptation gap:",
        "setup failed:",
        "row count differs:",
        "expected result rows, but query failed:",
        "expected empty result, but query failed:",
        "expected query error, but query succeeded",
    ];

    OUTCOME_SHAPES
        .iter()
        .any(|shape| reason.starts_with(shape) || reason.contains(shape))
}
