use std::collections::BTreeMap;

use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Feature {
    pub name: String,
    pub background: Vec<Step>,
    pub scenarios: Vec<Scenario>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Scenario {
    pub feature: String,
    pub name: String,
    pub tags: Vec<String>,
    pub steps: Vec<Step>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StepKind {
    Given,
    When,
    Then,
    And,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Step {
    pub kind: StepKind,
    pub text: String,
    pub docstring: Option<String>,
    pub table: Option<Vec<Vec<String>>>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum GherkinError {
    #[error("line {line}: {message}")]
    Syntax { line: usize, message: String },
}

pub fn parse_feature(src: &str) -> Result<Feature, GherkinError> {
    Parser::new(src).parse()
}

struct Parser<'a> {
    lines: Vec<&'a str>,
    pos: usize,
    feature_name: Option<String>,
    background: Vec<Step>,
    scenarios: Vec<Scenario>,
    current: Option<ScenarioBuilder>,
    target: Target,
    pending_tags: Vec<String>,
    example_header: Option<Vec<String>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Target {
    None,
    Background,
    Scenario,
    Examples,
}

#[derive(Debug, Clone)]
struct ScenarioBuilder {
    feature: String,
    name: String,
    tags: Vec<String>,
    steps: Vec<Step>,
    outline: bool,
    examples: Vec<BTreeMap<String, String>>,
}

impl<'a> Parser<'a> {
    fn new(src: &'a str) -> Self {
        Self {
            lines: src.lines().collect(),
            pos: 0,
            feature_name: None,
            background: Vec::new(),
            scenarios: Vec::new(),
            current: None,
            target: Target::None,
            pending_tags: Vec::new(),
            example_header: None,
        }
    }

    fn parse(mut self) -> Result<Feature, GherkinError> {
        while self.pos < self.lines.len() {
            let raw = self.lines[self.pos];
            let trimmed = raw.trim();

            if trimmed.is_empty() || trimmed.starts_with('#') {
                self.pos += 1;
                continue;
            }

            if let Some(name) = trimmed.strip_prefix("Feature:") {
                self.feature_name = Some(name.trim().to_string());
                self.target = Target::None;
                self.pending_tags.clear();
                self.pos += 1;
                continue;
            }

            if trimmed.starts_with('@') {
                self.pending_tags.extend(parse_tags(trimmed));
                self.pos += 1;
                continue;
            }

            if trimmed == "Background:" || trimmed.starts_with("Background: ") {
                self.finish_current()?;
                self.target = Target::Background;
                self.example_header = None;
                self.pending_tags.clear();
                self.pos += 1;
                continue;
            }

            if let Some(name) = trimmed.strip_prefix("Scenario Outline:") {
                self.start_scenario(name.trim(), true)?;
                continue;
            }

            if let Some(name) = trimmed.strip_prefix("Scenario:") {
                self.start_scenario(name.trim(), false)?;
                continue;
            }

            if trimmed == "Examples:" || trimmed.starts_with("Examples:") {
                self.start_examples()?;
                continue;
            }

            if let Some((kind, text)) = parse_step(trimmed) {
                self.push_step(kind, text)?;
                self.pos += 1;
                continue;
            }

            if is_docstring_delimiter(trimmed) {
                let docstring = self.parse_docstring(trimmed)?;
                self.attach_docstring(docstring)?;
                continue;
            }

            if is_table_row(trimmed) {
                let row = parse_table_row(trimmed);
                self.push_table_row(row)?;
                self.pos += 1;
                continue;
            }

            return self.syntax(format!("unsupported gherkin line `{trimmed}`"));
        }

        self.finish_current()?;
        let name = self.feature_name.ok_or_else(|| GherkinError::Syntax {
            line: 1,
            message: "missing Feature line".to_string(),
        })?;

        Ok(Feature {
            name,
            background: self.background,
            scenarios: self.scenarios,
        })
    }

    fn start_scenario(&mut self, name: &str, outline: bool) -> Result<(), GherkinError> {
        self.finish_current()?;
        self.current = Some(ScenarioBuilder {
            feature: self.feature_name.clone().unwrap_or_default(),
            name: name.to_string(),
            tags: std::mem::take(&mut self.pending_tags),
            steps: Vec::new(),
            outline,
            examples: Vec::new(),
        });
        self.target = Target::Scenario;
        self.example_header = None;
        self.pos += 1;
        Ok(())
    }

    fn start_examples(&mut self) -> Result<(), GherkinError> {
        match self.current.as_ref() {
            Some(builder) if builder.outline => {
                self.target = Target::Examples;
                self.example_header = None;
                self.pending_tags.clear();
                self.pos += 1;
                Ok(())
            }
            Some(_) => self.syntax("Examples can only follow Scenario Outline".to_string()),
            None => self.syntax("Examples without Scenario Outline".to_string()),
        }
    }

    fn push_step(&mut self, kind: StepKind, text: String) -> Result<(), GherkinError> {
        let step = Step {
            kind,
            text,
            docstring: None,
            table: None,
        };

        match self.target {
            Target::Background => {
                self.background.push(step);
                Ok(())
            }
            Target::Scenario => match self.current.as_mut() {
                Some(builder) => {
                    builder.steps.push(step);
                    Ok(())
                }
                None => self.syntax("step without scenario".to_string()),
            },
            Target::Examples => self.syntax("step found inside Examples".to_string()),
            Target::None => self.syntax("step before Background or Scenario".to_string()),
        }
    }

    fn parse_docstring(&mut self, delimiter: &str) -> Result<String, GherkinError> {
        let close = delimiter;
        self.pos += 1;
        let mut content = Vec::new();

        while self.pos < self.lines.len() {
            let raw = self.lines[self.pos];
            if raw.trim() == close {
                self.pos += 1;
                return Ok(normalize_docstring(&content));
            }
            content.push(raw);
            self.pos += 1;
        }

        self.syntax("unterminated docstring".to_string())
    }

    fn attach_docstring(&mut self, docstring: String) -> Result<(), GherkinError> {
        match self.last_step_mut() {
            Some(step) => {
                step.docstring = Some(docstring);
                Ok(())
            }
            None => self.syntax("docstring without step".to_string()),
        }
    }

    fn push_table_row(&mut self, row: Vec<String>) -> Result<(), GherkinError> {
        if self.target == Target::Examples {
            return self.push_example_row(row);
        }

        match self.last_step_mut() {
            Some(step) => {
                step.table.get_or_insert_with(Vec::new).push(row);
                Ok(())
            }
            None => self.syntax("table row without step".to_string()),
        }
    }

    fn push_example_row(&mut self, row: Vec<String>) -> Result<(), GherkinError> {
        if self.example_header.is_none() {
            self.example_header = Some(row);
            return Ok(());
        }

        let Some(header) = self.example_header.as_ref() else {
            return self.syntax("missing Examples header".to_string());
        };

        if header.len() != row.len() {
            return self.syntax(format!(
                "Examples row has {} cells, header has {}",
                row.len(),
                header.len()
            ));
        }

        let values = header
            .iter()
            .cloned()
            .zip(row)
            .collect::<BTreeMap<String, String>>();

        match self.current.as_mut() {
            Some(builder) => {
                builder.examples.push(values);
                Ok(())
            }
            None => self.syntax("Examples row without Scenario Outline".to_string()),
        }
    }

    fn last_step_mut(&mut self) -> Option<&mut Step> {
        match self.target {
            Target::Background => self.background.last_mut(),
            Target::Scenario => self
                .current
                .as_mut()
                .and_then(|builder| builder.steps.last_mut()),
            Target::Examples | Target::None => None,
        }
    }

    fn finish_current(&mut self) -> Result<(), GherkinError> {
        let Some(builder) = self.current.take() else {
            self.target = Target::None;
            self.example_header = None;
            return Ok(());
        };

        if builder.outline {
            if builder.examples.is_empty() {
                return self.syntax(format!(
                    "Scenario Outline `{}` has no examples",
                    builder.name
                ));
            }
            let mut expanded = builder
                .examples
                .iter()
                .map(|values| expand_outline(&builder, values))
                .collect::<Vec<_>>();
            suffix_duplicate_scenario_names(&mut expanded);
            for scenario in expanded {
                self.scenarios.push(scenario);
            }
        } else {
            self.scenarios.push(Scenario {
                feature: builder.feature,
                name: builder.name,
                tags: builder.tags,
                steps: builder.steps,
            });
        }

        self.target = Target::None;
        self.example_header = None;
        Ok(())
    }

    fn syntax<T>(&self, message: String) -> Result<T, GherkinError> {
        Err(GherkinError::Syntax {
            line: self.pos + 1,
            message,
        })
    }
}

fn parse_tags(line: &str) -> Vec<String> {
    line.split_whitespace()
        .filter_map(|tag| tag.strip_prefix('@'))
        .map(ToOwned::to_owned)
        .collect()
}

fn parse_step(line: &str) -> Option<(StepKind, String)> {
    for (prefix, kind) in [
        ("Given ", StepKind::Given),
        ("When ", StepKind::When),
        ("Then ", StepKind::Then),
        ("And ", StepKind::And),
    ] {
        if let Some(text) = line.strip_prefix(prefix) {
            return Some((kind, text.trim().to_string()));
        }
    }
    None
}

fn is_docstring_delimiter(line: &str) -> bool {
    line == r#"""""# || line == "'''"
}

fn normalize_docstring(lines: &[&str]) -> String {
    let indent = lines
        .iter()
        .filter_map(|line| {
            if line.trim().is_empty() {
                None
            } else {
                Some(line.chars().take_while(|ch| ch.is_whitespace()).count())
            }
        })
        .min()
        .unwrap_or(0);

    let mut normalized = lines
        .iter()
        .map(|line| strip_indent(line, indent).trim_end().to_string())
        .collect::<Vec<_>>();

    while normalized.first().is_some_and(|line| line.is_empty()) {
        normalized.remove(0);
    }
    while normalized.last().is_some_and(|line| line.is_empty()) {
        normalized.pop();
    }

    normalized.join("\n")
}

fn strip_indent(line: &str, indent: usize) -> &str {
    if indent == 0 {
        return line;
    }

    let mut byte_idx = 0;
    for (count, (idx, ch)) in line.char_indices().enumerate() {
        if count == indent {
            byte_idx = idx;
            break;
        }
        if !ch.is_whitespace() {
            return &line[idx..];
        }
        byte_idx = idx + ch.len_utf8();
    }
    &line[byte_idx..]
}

fn is_table_row(line: &str) -> bool {
    line.starts_with('|') && line.ends_with('|')
}

fn parse_table_row(line: &str) -> Vec<String> {
    let mut cells = Vec::new();
    let mut cell = String::new();
    let mut chars = line.chars().peekable();

    if chars.next() != Some('|') {
        return cells;
    }

    let mut escaped = false;
    for ch in chars {
        if escaped {
            match ch {
                'n' => cell.push('\n'),
                '|' => cell.push('|'),
                '\\' => cell.push('\\'),
                other => {
                    cell.push('\\');
                    cell.push(other);
                }
            }
            escaped = false;
            continue;
        }

        match ch {
            '\\' => escaped = true,
            '|' => {
                cells.push(cell.trim().to_string());
                cell.clear();
            }
            _ => cell.push(ch),
        }
    }
    if escaped {
        cell.push('\\');
    }

    cells
}

fn expand_outline(builder: &ScenarioBuilder, values: &BTreeMap<String, String>) -> Scenario {
    Scenario {
        feature: builder.feature.clone(),
        name: substitute_examples(&builder.name, values),
        tags: builder.tags.clone(),
        steps: builder
            .steps
            .iter()
            .map(|step| Step {
                kind: step.kind,
                text: substitute_examples(&step.text, values),
                docstring: step
                    .docstring
                    .as_ref()
                    .map(|docstring| substitute_examples(docstring, values)),
                table: step.table.as_ref().map(|table| {
                    table
                        .iter()
                        .map(|row| {
                            row.iter()
                                .map(|cell| substitute_examples(cell, values))
                                .collect()
                        })
                        .collect()
                }),
            })
            .collect(),
    }
}

fn suffix_duplicate_scenario_names(scenarios: &mut [Scenario]) {
    let mut counts = BTreeMap::<String, usize>::new();
    for scenario in scenarios.iter() {
        *counts.entry(scenario.name.clone()).or_default() += 1;
    }

    let mut seen = BTreeMap::<String, usize>::new();
    for scenario in scenarios {
        if counts.get(&scenario.name).copied().unwrap_or_default() <= 1 {
            continue;
        }
        let base = scenario.name.clone();
        let next = seen.entry(base.clone()).or_default();
        *next += 1;
        scenario.name = format!("{base} #{next}");
    }
}

fn substitute_examples(input: &str, values: &BTreeMap<String, String>) -> String {
    let mut output = input.to_string();
    for (name, value) in values {
        output = output.replace(&format!("<{name}>"), value);
    }
    output
}
