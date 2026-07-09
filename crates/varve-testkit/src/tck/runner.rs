use std::collections::BTreeMap;

use varve::{Db, EngineError, RecordBatch, SideEffects};

use crate::tck::gherkin::Scenario;
use crate::tck::translate::translate;
use crate::tck::values::{compare_results, parse_value, TckValue};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Passed,
    Failed(String),
    Excluded(String),
    Untranslatable(String),
}

pub async fn run_scenario(sc: &Scenario, exclusions: &BTreeMap<String, String>) -> Outcome {
    if let Some(reason) = exclusion_reason(sc, exclusions) {
        return Outcome::Excluded(reason.clone());
    }

    let db = Db::memory();
    let mut last_result: Option<Vec<RecordBatch>> = None;
    let mut last_error: Option<String> = None;
    let mut last_side_effects = SideEffects::default();
    let mut asserted = false;

    for step in &sc.steps {
        let text = step.text.trim();
        if text == "an empty graph" || text == "any graph" {
            continue;
        }

        if text.starts_with("having executed") {
            let Some(cypher) = step.docstring.as_deref() else {
                return Outcome::Failed("having executed step has no query docstring".to_string());
            };
            let gql = match translate(cypher) {
                Ok(gql) => gql,
                Err(err) => return Outcome::Untranslatable(err.construct),
            };
            if let Err(err) = db.execute(&gql).await {
                return Outcome::Failed(format!("setup failed: {err}"));
            }
            continue;
        }

        if text.starts_with("executing query") || text.starts_with("executing control query") {
            let Some(cypher) = step.docstring.as_deref() else {
                return Outcome::Failed("executing query step has no query docstring".to_string());
            };
            let gql = match translate(cypher) {
                Ok(gql) => gql,
                Err(err) => return Outcome::Untranslatable(err.construct),
            };
            match query_or_execute(&db, &gql).await {
                Ok(result) => {
                    last_side_effects = result.side_effects;
                    last_result = Some(result.batches);
                    last_error = None;
                }
                Err(err) => {
                    last_side_effects = SideEffects::default();
                    last_result = None;
                    last_error = Some(err);
                }
            }
            continue;
        }

        if text.starts_with("the result should be, in any order") {
            asserted = true;
            let outcome = compare_step_result(
                step_table(step),
                last_result.as_deref(),
                last_error.as_deref(),
                false,
            );
            if outcome != Outcome::Passed {
                return outcome;
            }
            continue;
        }

        if text.starts_with("the result should be, in order") {
            asserted = true;
            let outcome = compare_step_result(
                step_table(step),
                last_result.as_deref(),
                last_error.as_deref(),
                true,
            );
            if outcome != Outcome::Passed {
                return outcome;
            }
            continue;
        }

        if text == "the result should be empty" {
            asserted = true;
            let outcome = empty_result(last_result.as_deref(), last_error.as_deref());
            if outcome != Outcome::Passed {
                return outcome;
            }
            continue;
        }

        if let Some(expected) = expected_error_class(text) {
            asserted = true;
            let outcome = match last_error.as_deref() {
                Some(error) => compare_error_class(expected, error),
                None => Outcome::Failed("expected query error, but query succeeded".to_string()),
            };
            if outcome != Outcome::Passed {
                return outcome;
            }
            continue;
        }

        if text.starts_with("the side effects should be") {
            asserted = true;
            let outcome =
                compare_side_effects(step_table(step), last_error.as_deref(), last_side_effects);
            if outcome != Outcome::Passed {
                return outcome;
            }
            continue;
        }

        if text == "no side effects" {
            asserted = true;
            let outcome = no_side_effects(last_error.as_deref(), last_side_effects);
            if outcome != Outcome::Passed {
                return outcome;
            }
            continue;
        }

        return Outcome::Failed(format!("unsupported TCK step `{text}`"));
    }

    if asserted {
        Outcome::Passed
    } else {
        Outcome::Failed("scenario had no assertion step".to_string())
    }
}

fn exclusion_reason<'a>(
    sc: &Scenario,
    exclusions: &'a BTreeMap<String, String>,
) -> Option<&'a String> {
    if !sc.feature.is_empty() {
        let key = format!("{}::{}", sc.feature, sc.name);
        exclusions.get(&key)
    } else {
        exclusions.get(&sc.name)
    }
}

struct StepExecution {
    batches: Vec<RecordBatch>,
    side_effects: SideEffects,
}

async fn query_or_execute(db: &Db, gql: &str) -> Result<StepExecution, String> {
    match db.query(gql).await {
        Ok(batches) => Ok(StepExecution {
            batches,
            side_effects: SideEffects::default(),
        }),
        Err(query_err) => match db.execute(gql).await {
            Ok(receipt) => Ok(StepExecution {
                batches: Vec::new(),
                side_effects: receipt.side_effects,
            }),
            Err(exec_err) if matches!(query_err, EngineError::NotAQuery) => {
                Err(exec_err.to_string())
            }
            Err(_) => Err(query_err.to_string()),
        },
    }
}

fn compare_step_result(
    table: Result<&[Vec<String>], String>,
    actual: Option<&[RecordBatch]>,
    error: Option<&str>,
    ordered: bool,
) -> Outcome {
    let table = match table {
        Ok(table) => table,
        Err(err) => return Outcome::Failed(err),
    };
    let Some(actual) = actual else {
        if let Some(error) = error {
            return Outcome::Failed(format!("expected result rows, but query failed: {error}"));
        }
        return Outcome::Failed("expected result rows, but query failed".to_string());
    };
    let (header, rows) = match parse_expected_rows(table) {
        Ok(expected) => expected,
        Err(err) => return Outcome::Failed(err),
    };
    match compare_results(&header, &rows, actual, ordered) {
        Ok(()) => Outcome::Passed,
        Err(err) => Outcome::Failed(err),
    }
}

fn empty_result(actual: Option<&[RecordBatch]>, error: Option<&str>) -> Outcome {
    if let Some(error) = error {
        return Outcome::Failed(format!("expected empty result, but query failed: {error}"));
    }
    let Some(actual) = actual else {
        return Outcome::Failed("expected empty result, but query did not run".to_string());
    };
    let rows = actual.iter().map(RecordBatch::num_rows).sum::<usize>();
    if rows == 0 {
        Outcome::Passed
    } else {
        Outcome::Failed(format!("expected empty result, got {rows} rows"))
    }
}

fn compare_side_effects(
    table: Result<&[Vec<String>], String>,
    error: Option<&str>,
    actual: SideEffects,
) -> Outcome {
    if let Some(error) = error {
        return Outcome::Failed(format!("expected side effects, but query failed: {error}"));
    }
    let table = match table {
        Ok(table) => table,
        Err(err) => return Outcome::Failed(err),
    };
    if table.is_empty() {
        return Outcome::Failed("side-effect step has no table rows".to_string());
    }
    for row in table {
        if row.len() != 2 {
            return Outcome::Failed(format!("side-effect row should have 2 cells, got {row:?}"));
        }
        let key = row[0].trim();
        let expected = match row[1].trim().parse::<usize>() {
            Ok(expected) => expected,
            Err(err) => {
                return Outcome::Failed(format!(
                    "side-effect {key} expected count is not an integer: {err}"
                ));
            }
        };
        let Some(got) = side_effect_value(actual, key) else {
            return Outcome::Failed(format!("unsupported side-effect key `{key}`"));
        };
        if got != expected {
            return Outcome::Failed(format!(
                "side-effect {key}: expected {expected}, got {got} ({})",
                format_side_effects(actual)
            ));
        }
    }
    Outcome::Passed
}

fn no_side_effects(_error: Option<&str>, actual: SideEffects) -> Outcome {
    if actual.is_empty() {
        Outcome::Passed
    } else {
        Outcome::Failed(format!(
            "expected no side effects, got {}",
            format_side_effects(actual)
        ))
    }
}

fn side_effect_value(actual: SideEffects, key: &str) -> Option<usize> {
    match key {
        "+nodes" => Some(actual.nodes_created),
        "-nodes" => Some(actual.nodes_deleted),
        "+relationships" => Some(actual.relationships_created),
        "-relationships" => Some(actual.relationships_deleted),
        "+properties" => Some(actual.properties_set),
        "-properties" => Some(actual.properties_removed),
        "+labels" => Some(actual.labels_added),
        "-labels" => Some(actual.labels_removed),
        _ => None,
    }
}

fn format_side_effects(actual: SideEffects) -> String {
    let entries = [
        ("+nodes", actual.nodes_created),
        ("-nodes", actual.nodes_deleted),
        ("+relationships", actual.relationships_created),
        ("-relationships", actual.relationships_deleted),
        ("+properties", actual.properties_set),
        ("-properties", actual.properties_removed),
        ("+labels", actual.labels_added),
        ("-labels", actual.labels_removed),
    ]
    .into_iter()
    .filter(|(_, count)| *count != 0)
    .map(|(key, count)| format!("{key}={count}"))
    .collect::<Vec<_>>();
    if entries.is_empty() {
        "none".to_string()
    } else {
        entries.join(", ")
    }
}

fn step_table(step: &crate::tck::gherkin::Step) -> Result<&[Vec<String>], String> {
    step.table
        .as_deref()
        .ok_or_else(|| "result step has no table".to_string())
}

fn parse_expected_rows(table: &[Vec<String>]) -> Result<(Vec<String>, Vec<Vec<TckValue>>), String> {
    let Some(header) = table.first() else {
        return Err("result table is empty".to_string());
    };
    let mut rows = Vec::with_capacity(table.len().saturating_sub(1));
    for (idx, row) in table.iter().enumerate().skip(1) {
        let mut parsed = Vec::with_capacity(row.len());
        for cell in row {
            parsed.push(parse_value(cell).map_err(|err| {
                format!("expected result table row {idx} value `{cell}` did not parse: {err}")
            })?);
        }
        rows.push(parsed);
    }
    Ok((header.clone(), rows))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ErrorClass {
    Syntax,
    Type,
    Argument,
}

impl ErrorClass {
    fn tck_name(self) -> &'static str {
        match self {
            Self::Syntax => "SyntaxError",
            Self::Type => "TypeError",
            Self::Argument => "ArgumentError",
        }
    }
}

fn expected_error_class(text: &str) -> Option<ErrorClass> {
    if !text.contains("should be raised") {
        return None;
    }
    if text.contains("SyntaxError") {
        Some(ErrorClass::Syntax)
    } else if text.contains("TypeError") {
        Some(ErrorClass::Type)
    } else if text.contains("ArgumentError") {
        Some(ErrorClass::Argument)
    } else {
        None
    }
}

fn compare_error_class(expected: ErrorClass, actual: &str) -> Outcome {
    let actual_class = classify_error(actual);
    if expected == actual_class {
        Outcome::Passed
    } else {
        Outcome::Failed(format!(
            "expected {}, got {}: {actual}",
            expected.tck_name(),
            actual_class.tck_name()
        ))
    }
}

fn classify_error(error: &str) -> ErrorClass {
    let lowered = error.to_ascii_lowercase();
    if lowered.contains("parse error") || lowered.contains("lex error") {
        ErrorClass::Syntax
    } else if lowered.contains("unsupported") || lowered.contains("argument") {
        ErrorClass::Argument
    } else {
        ErrorClass::Type
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use std::collections::BTreeMap;

    use crate::tck::gherkin::parse_feature;
    use crate::tck::runner::{run_scenario, Outcome};

    #[tokio::test]
    async fn runs_create_match_return_scenario() {
        let scenario = scenario(
            r#"
Feature: Runner

  Scenario: create match return
    Given an empty graph
    And having executed:
      """
      CREATE (:Person {name: 'Ada'})
      """
    When executing query:
      """
      MATCH (p:Person) RETURN p.name AS name
      """
    Then the result should be, in any order:
      | name  |
      | 'Ada' |
"#,
        );

        let outcome = run_scenario(&scenario, &BTreeMap::new()).await;

        assert_eq!(outcome, Outcome::Passed);
    }

    #[tokio::test]
    async fn error_expectation_scenario() {
        let scenario = scenario(
            r#"
Feature: Runner

Scenario: syntax error expectation
  Given an empty graph
  When executing query:
    """
    MATCH (
    """
  Then a SyntaxError should be raised at compile time: InvalidSyntax
"#,
        );

        let outcome = run_scenario(&scenario, &BTreeMap::new()).await;

        assert_eq!(outcome, Outcome::Passed);
    }

    #[tokio::test]
    async fn unordered_result_scenario() {
        let scenario = scenario(
            r#"
Feature: Runner

  Scenario: unordered result
    Given an empty graph
    And having executed:
      """
      CREATE (:Person {name: 'Ada'});
      CREATE (:Person {name: 'Bob'})
      """
    When executing query:
      """
      MATCH (p:Person) RETURN p.name AS name
      """
    Then the result should be, in any order:
      | name  |
      | 'Bob' |
      | 'Ada' |
"#,
        );

        let outcome = run_scenario(&scenario, &BTreeMap::new()).await;

        assert_eq!(outcome, Outcome::Passed);
    }

    #[tokio::test]
    async fn any_graph_scenario() {
        let scenario = scenario(
            r#"
Feature: Runner

  Scenario: any graph
    Given any graph
    And having executed:
      """
      CREATE (:Person {name: 'Ada'})
      """
    When executing query:
      """
      MATCH (p:Person) RETURN p.name AS name
      """
    Then the result should be, in any order:
      | name  |
      | 'Ada' |
"#,
        );

        let outcome = run_scenario(&scenario, &BTreeMap::new()).await;

        assert_eq!(outcome, Outcome::Passed);
    }

    #[tokio::test]
    async fn result_expectation_reports_query_error() {
        let scenario = scenario(
            r#"
Feature: Runner

  Scenario: query error details
    Given an empty graph
    When executing query:
      """
      RETURN 1 AS value
      """
    Then the result should be, in any order:
      | value |
      | 1     |
"#,
        );

        let outcome = run_scenario(&scenario, &BTreeMap::new()).await;

        match outcome {
            Outcome::Failed(message) => {
                assert!(message.contains("expected result rows, but query failed:"));
                assert!(message.contains("parse error"));
            }
            other => panic!("expected failed outcome, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn type_error_expectation_scenario() {
        let scenario = scenario(
            r#"
Feature: Runner

Scenario: type error expectation
  Given an empty graph
  And having executed:
    """
    CREATE (:Person {name: 'Ada'})
    """
  When executing query:
    """
    MATCH (p:Person) RETURN definitely_missing_function(p.name) AS value
    """
  Then a TypeError should be raised at runtime: InvalidArgumentType
"#,
        );

        let outcome = run_scenario(&scenario, &BTreeMap::new()).await;

        assert_eq!(outcome, Outcome::Passed);
    }

    #[tokio::test]
    async fn error_expectation_checks_requested_class() {
        let scenario = scenario(
            r#"
Feature: Runner

Scenario: syntax class mismatch
  Given an empty graph
  And having executed:
    """
    CREATE (:Person {name: 'Ada'})
    """
  When executing query:
    """
    MATCH (p:Person) RETURN definitely_missing_function(p.name) AS value
    """
  Then a SyntaxError should be raised at compile time: InvalidSyntax
"#,
        );
        let outcome = run_scenario(&scenario, &BTreeMap::new()).await;

        match outcome {
            Outcome::Failed(message) => {
                assert!(message.contains("expected SyntaxError"), "{message}");
            }
            other => panic!("expected class mismatch failure, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn excludes_by_feature_scenario_key() {
        let scenario = scenario(
            r#"
Feature: Runner

Scenario: excluded scenario
  Given an empty graph
  When executing query:
    """
    MATCH (p:Person) RETURN p.name AS name
    """
  Then the result should be, in any order:
    | name |
"#,
        );
        let mut exclusions = BTreeMap::new();
        exclusions.insert(
            "Runner::excluded scenario".to_string(),
            "covered by another TCK class".to_string(),
        );

        let outcome = run_scenario(&scenario, &exclusions).await;

        assert_eq!(
            outcome,
            Outcome::Excluded("covered by another TCK class".to_string())
        );
    }

    #[tokio::test]
    async fn side_effect_table_is_checked_after_result_assertion() {
        let scenario = scenario(
            r#"
Feature: Runner

Scenario: side effect mismatch
  Given an empty graph
  When executing query:
    """
    CREATE (:A)
    """
  Then the result should be empty
  And the side effects should be:
    | +nodes | 2 |
"#,
        );

        let outcome = run_scenario(&scenario, &BTreeMap::new()).await;

        match outcome {
            Outcome::Failed(message) => {
                assert!(message.contains("+nodes"), "{message}");
                assert!(message.contains("expected 2"), "{message}");
            }
            other => panic!("expected side-effect mismatch, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn no_side_effects_is_checked_after_result_assertion() {
        let scenario = scenario(
            r#"
Feature: Runner

Scenario: no side effects mismatch
  Given an empty graph
  When executing query:
    """
    CREATE (:A)
    """
  Then the result should be empty
  And no side effects
"#,
        );

        let outcome = run_scenario(&scenario, &BTreeMap::new()).await;

        match outcome {
            Outcome::Failed(message) => {
                assert!(message.contains("expected no side effects"), "{message}");
                assert!(message.contains("+nodes=1"), "{message}");
            }
            other => panic!("expected side-effect mismatch, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn side_effect_plus_counts_pass_for_create_path() {
        let scenario = scenario(
            r#"
Feature: Runner
Scenario: create path side effects
  Given an empty graph
  When executing query:
    """
    CREATE (:A {_id: 1, name: 'Ada'})-[:KNOWS {since: 2024}]->(:B {_id: 2})
    """
  Then the result should be empty
  And the side effects should be:
    | +nodes         | 2 |
    | +relationships | 1 |
    | +labels        | 2 |
    | +properties    | 2 |
"#,
        );
        let outcome = run_scenario(&scenario, &BTreeMap::new()).await;
        assert_eq!(outcome, Outcome::Passed);
    }

    #[tokio::test]
    async fn no_side_effects_passes_for_read_query() {
        let scenario = scenario(
            r#"
Feature: Runner
Scenario: read query has no side effects
  Given an empty graph
  And having executed:
    """
    CREATE (:Person {name: 'Ada'})
    """
  When executing query:
    """
    MATCH (p:Person) RETURN p.name AS name
    """
  Then the result should be, in any order:
    | name  |
    | 'Ada' |
  And no side effects
"#,
        );
        let outcome = run_scenario(&scenario, &BTreeMap::new()).await;
        assert_eq!(outcome, Outcome::Passed);
    }

    #[tokio::test]
    async fn compares_returned_path_values() {
        let scenario = scenario(
            r#"
Feature: Runner
Scenario: returned path value
  Given an empty graph
  And having executed:
    """
    CREATE (:A {_id: 1, name: 'Ada'})-[:KNOWS {since: 2024}]->(:B {_id: 2})
    """
  When executing query:
    """
    MATCH p = (a:A)-[:KNOWS]->{1,1}(b:B) RETURN p
    """
  Then the result should be, in any order:
    | p                                      |
    | <(:A {name: 'Ada'})-[:KNOWS]->(:B)>   |
"#,
        );
        let outcome = run_scenario(&scenario, &BTreeMap::new()).await;
        assert_eq!(outcome, Outcome::Passed);
    }

    fn scenario(src: &str) -> crate::tck::gherkin::Scenario {
        parse_feature(src)
            .expect("feature should parse")
            .scenarios
            .into_iter()
            .next()
            .expect("scenario should exist")
    }
}
