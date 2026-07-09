#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::fs;
use std::path::{Path, PathBuf};

use varve_testkit::tck::gherkin::{parse_feature, StepKind};

#[test]
fn parses_scenario_with_docstring_and_table() {
    let feature = parse_feature(
        r#"
Feature: Query execution
  Scenario: runs a query
    Given an empty graph
    When executing query:
      """
      MATCH (n)
      RETURN n
      """
    Then result should be, in order:
      | n |
      | 1 |
"#,
    )
    .expect("feature should parse");

    let scenario = &feature.scenarios[0];
    assert_eq!(scenario.name, "runs a query");
    assert_eq!(scenario.steps.len(), 3);
    assert_eq!(scenario.steps[0].kind, StepKind::Given);
    assert_eq!(scenario.steps[1].kind, StepKind::When);
    assert_eq!(
        scenario.steps[1].docstring.as_deref(),
        Some("MATCH (n)\nRETURN n")
    );
    assert_eq!(
        scenario.steps[2].table.as_ref().unwrap(),
        &vec![vec!["n".to_string()], vec!["1".to_string()]]
    );
}

#[test]
fn expands_scenario_outline_examples() {
    let feature = parse_feature(
        r#"
Feature: Outlines
  Scenario Outline: returns <value>
    When executing query:
      """
      RETURN <value> AS value
      """
    Then result should be, in order:
      | value |
      | <value> |

    Examples:
      | value |
      | 1     |
      | 2     |
"#,
    )
    .expect("feature should parse");

    assert_eq!(feature.scenarios.len(), 2);
    assert_eq!(feature.scenarios[0].name, "returns 1");
    assert_eq!(feature.scenarios[1].name, "returns 2");
    assert_eq!(
        feature.scenarios[0].steps[0].docstring.as_deref(),
        Some("RETURN 1 AS value")
    );
    assert_eq!(
        feature.scenarios[1].steps[1].table.as_ref().unwrap(),
        &vec![vec!["value".to_string()], vec!["2".to_string()]]
    );
}

#[test]
fn duplicate_scenario_outline_expansions_get_suffixes() {
    let feature = parse_feature(
        r#"
Feature: Outlines

  Scenario Outline: returns a value
    When executing query:
      """
      RETURN <value> AS value
      """
    Then the result should be, in order:
      | value |
      | <value> |

    Examples:
      | value |
      | 1 |
      | 1 |
"#,
    )
    .expect("feature should parse");

    assert_eq!(feature.scenarios.len(), 2);
    assert_eq!(feature.scenarios[0].name, "returns a value #1");
    assert_eq!(feature.scenarios[1].name, "returns a value #2");
}

#[test]
fn parses_background_and_tags() {
    let feature = parse_feature(
        r#"
Feature: Tagged scenarios
  Background:
    Given an empty graph

  @skipGrammarCheck @slow
  Scenario: has tags
    When executing query:
      """
      RETURN 1
      """
    Then result should be, in order:
      | 1 |
"#,
    )
    .expect("feature should parse");

    assert_eq!(feature.name, "Tagged scenarios");
    assert_eq!(feature.background.len(), 1);
    assert_eq!(feature.background[0].text, "an empty graph");
    assert_eq!(feature.scenarios.len(), 1);
    assert_eq!(
        feature.scenarios[0].tags,
        vec!["skipGrammarCheck".to_string(), "slow".to_string()]
    );
}

#[test]
fn parses_table_escape_sequences() {
    let feature = parse_feature(
        r#"
Feature: Escaped tables
  Scenario: decodes table escapes
    Then result should be, in order:
      | text       | pipe | slash |
      | '\nFoo\n' | a\|b | c\\d  |
"#,
    )
    .expect("feature should parse");

    assert_eq!(
        feature.scenarios[0].steps[0].table.as_ref().unwrap(),
        &vec![
            vec!["text".to_string(), "pipe".to_string(), "slash".to_string()],
            vec![
                "'\nFoo\n'".to_string(),
                "a|b".to_string(),
                "c\\d".to_string(),
            ],
        ]
    );
}

#[test]
fn accumulates_consecutive_tag_lines() {
    let feature = parse_feature(
        r#"
Feature: Consecutive tags
  @skipGrammarCheck
  @slow @core
  Scenario: has all tags
    Given an empty graph
"#,
    )
    .expect("feature should parse");

    assert_eq!(
        feature.scenarios[0].tags,
        vec![
            "skipGrammarCheck".to_string(),
            "slow".to_string(),
            "core".to_string(),
        ]
    );
}

#[test]
fn parses_every_vendored_feature_file() {
    let feature_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../resources/tck/features")
        .canonicalize()
        .expect("vendored TCK feature root should exist");

    let mut files = Vec::new();
    collect_feature_files(&feature_root, &mut files).expect("should walk feature tree");
    files.sort();
    assert_eq!(files.len(), 220);

    let mut errors = Vec::new();
    for file in files {
        let src = fs::read_to_string(&file).expect("feature file should be utf-8");
        if let Err(err) = parse_feature(&src) {
            errors.push(format!("{}: {err}", file.display()));
        }
    }

    assert!(
        errors.is_empty(),
        "vendored TCK parse errors:\n{}",
        errors.join("\n")
    );
}

fn collect_feature_files(dir: &Path, files: &mut Vec<PathBuf>) -> std::io::Result<()> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_feature_files(&path, files)?;
        } else if path.extension().and_then(|ext| ext.to_str()) == Some("feature") {
            files.push(path);
        }
    }
    Ok(())
}
