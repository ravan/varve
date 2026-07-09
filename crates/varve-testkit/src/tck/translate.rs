use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("untranslatable Cypher construct `{construct}`")]
pub struct Untranslatable {
    pub construct: String,
}

pub fn translate(cypher: &str) -> Result<String, Untranslatable> {
    if let Some(construct) = forbidden_construct(cypher) {
        return Err(Untranslatable { construct });
    }

    let mut gql = String::with_capacity(cypher.len());
    let mut pos = 0;

    while pos < cypher.len() {
        let rest = &cypher[pos..];
        let Some(ch) = rest.chars().next() else {
            break;
        };

        if ch == '\'' || ch == '"' {
            let next = copy_quoted(cypher, pos, &mut gql);
            pos = next;
            continue;
        }

        if is_ident_start(ch) {
            let end = consume_ident(cypher, pos);
            let word = &cypher[pos..end];
            if word.eq_ignore_ascii_case("CREATE") {
                gql.push_str("INSERT");
                pos = end;
                continue;
            }
            if word.eq_ignore_ascii_case("UNWIND") {
                let (replacement, next) = translate_unwind(cypher, end)?;
                gql.push_str(&replacement);
                pos = next;
                continue;
            }
            gql.push_str(word);
            pos = end;
            continue;
        }

        gql.push(ch);
        pos += ch.len_utf8();
    }

    Ok(gql)
}

fn forbidden_construct(cypher: &str) -> Option<String> {
    let words = words_outside_strings(cypher);
    if words
        .windows(2)
        .any(|pair| pair[0].text == "LOAD" && pair[1].text == "CSV")
    {
        return Some("LOAD CSV".to_string());
    }
    if words
        .windows(2)
        .any(|pair| pair[0].text == "OPTIONAL" && pair[1].text == "MATCH")
        && first_clause_is_optional_match(&words)
    {
        return Some("OPTIONAL MATCH first clause".to_string());
    }

    for (idx, word) in words.iter().enumerate() {
        match word.text.as_str() {
            "CALL" | "MERGE" | "FOREACH" | "START" | "SHORTEST" | "CHEAPEST" => {
                return Some(word.text.clone());
            }
            "WITH" if !is_string_predicate_with(&words, idx) => return Some("WITH".to_string()),
            "LABELS" if next_non_ws_is(cypher, word.end, '(') => {
                return Some("labels()".to_string());
            }
            "ELEMENT_ID" if next_non_ws_is(cypher, word.end, '(') => {
                return Some("element_id()".to_string());
            }
            "DISTINCT" if distinct_inside_non_count_aggregate(cypher, word.start) => {
                return Some("DISTINCT non-count aggregate".to_string());
            }
            _ => {}
        }
    }

    if has_path_variable_on_create(cypher) {
        return Some("path variables on INSERT".to_string());
    }
    if has_quantified_relationship_variable(cypher) {
        return Some("quantified-edge variables".to_string());
    }
    if has_edge_label_alternation(cypher) {
        return Some("edge-label alternation".to_string());
    }
    if has_label_negation(cypher) {
        return Some("label negation/nested label expressions".to_string());
    }

    None
}

fn translate_unwind(cypher: &str, start: usize) -> Result<(String, usize), Untranslatable> {
    let expr_start = skip_ws(cypher, start);
    let Some(as_start) = find_top_level_keyword(cypher, expr_start, "AS") else {
        return Err(Untranslatable {
            construct: "UNWIND".to_string(),
        });
    };
    let expr = cypher[expr_start..as_start].trim();
    let var_start = skip_ws(cypher, as_start + "AS".len());
    let Some(ch) = cypher[var_start..].chars().next() else {
        return Err(Untranslatable {
            construct: "UNWIND".to_string(),
        });
    };
    if !is_ident_start(ch) {
        return Err(Untranslatable {
            construct: "UNWIND".to_string(),
        });
    }
    let var_end = consume_ident(cypher, var_start);
    let var = &cypher[var_start..var_end];
    Ok((format!("FOR {var} IN {expr}"), var_end))
}

fn find_top_level_keyword(input: &str, start: usize, keyword: &str) -> Option<usize> {
    let mut pos = start;
    let mut depth = 0_i32;
    while pos < input.len() {
        let ch = input[pos..].chars().next()?;
        if ch == '\'' || ch == '"' {
            pos = skip_quoted(input, pos);
            continue;
        }
        match ch {
            '(' | '[' | '{' => depth += 1,
            ')' | ']' | '}' => depth -= 1,
            _ => {}
        }
        if depth == 0 && keyword_at(input, pos, keyword) {
            return Some(pos);
        }
        pos += ch.len_utf8();
    }
    None
}

#[derive(Debug)]
struct Word {
    text: String,
    start: usize,
    end: usize,
}

fn words_outside_strings(input: &str) -> Vec<Word> {
    let mut words = Vec::new();
    let mut pos = 0;
    while pos < input.len() {
        let Some(ch) = input[pos..].chars().next() else {
            break;
        };
        if ch == '\'' || ch == '"' {
            pos = skip_quoted(input, pos);
            continue;
        }
        if is_ident_start(ch) {
            let end = consume_ident(input, pos);
            words.push(Word {
                text: input[pos..end].to_ascii_uppercase(),
                start: pos,
                end,
            });
            pos = end;
            continue;
        }
        pos += ch.len_utf8();
    }
    words
}

fn first_clause_is_optional_match(words: &[Word]) -> bool {
    let Some(first) = words.first() else {
        return false;
    };
    let Some(second) = words.get(1) else {
        return false;
    };
    first.text == "OPTIONAL" && second.text == "MATCH"
}

fn is_string_predicate_with(words: &[Word], idx: usize) -> bool {
    matches!(
        idx.checked_sub(1).and_then(|prev| words.get(prev)),
        Some(prev) if prev.text == "STARTS" || prev.text == "ENDS"
    )
}

fn distinct_inside_non_count_aggregate(input: &str, distinct_pos: usize) -> bool {
    let Some(open_pos) = input[..distinct_pos].rfind('(') else {
        return false;
    };
    let name_end = input[..open_pos].trim_end().len();
    let name_start = input[..name_end]
        .rfind(|ch: char| !is_ident_continue(ch))
        .map_or(0, |idx| idx + 1);
    let fn_name = input[name_start..name_end].to_ascii_uppercase();
    matches!(fn_name.as_str(), "SUM" | "AVG" | "MIN" | "MAX" | "COLLECT")
}

fn has_path_variable_on_create(input: &str) -> bool {
    let mut pos = 0;
    while let Some(create_pos) = find_keyword_from(input, pos, "CREATE") {
        let after_create = skip_ws(input, create_pos + "CREATE".len());
        let Some(ch) = input[after_create..].chars().next() else {
            return false;
        };
        if is_ident_start(ch) {
            let var_end = consume_ident(input, after_create);
            let after_var = skip_ws(input, var_end);
            if input[after_var..].starts_with('=') {
                return true;
            }
        }
        pos = create_pos + "CREATE".len();
    }
    false
}

fn has_quantified_relationship_variable(input: &str) -> bool {
    relationship_bodies(input).any(|body| {
        let trimmed = body.trim_start();
        let Some(first) = trimmed.chars().next() else {
            return false;
        };
        if first == ':' || first == '*' {
            return false;
        }
        let var_end = trimmed
            .find(|ch: char| !is_ident_continue(ch))
            .unwrap_or(trimmed.len());
        trimmed[var_end..].trim_start().starts_with('*')
    })
}

fn has_edge_label_alternation(input: &str) -> bool {
    relationship_bodies(input).any(|body| body.contains('|'))
}

fn has_label_negation(input: &str) -> bool {
    relationship_bodies(input).any(|body| body.contains('!'))
        || input
            .char_indices()
            .any(|(idx, ch)| ch == '!' && !inside_string(input, idx))
}

fn relationship_bodies(input: &str) -> impl Iterator<Item = &str> {
    let mut bodies = Vec::new();
    let mut pos = 0;
    while pos < input.len() {
        let Some(ch) = input[pos..].chars().next() else {
            break;
        };
        if ch == '\'' || ch == '"' {
            pos = skip_quoted(input, pos);
            continue;
        }
        if ch == '[' {
            let body_start = pos + ch.len_utf8();
            let mut end = body_start;
            while end < input.len() {
                let Some(inner) = input[end..].chars().next() else {
                    break;
                };
                if inner == '\'' || inner == '"' {
                    end = skip_quoted(input, end);
                    continue;
                }
                if inner == ']' {
                    bodies.push(&input[body_start..end]);
                    end += inner.len_utf8();
                    break;
                }
                end += inner.len_utf8();
            }
            pos = end;
            continue;
        }
        pos += ch.len_utf8();
    }
    bodies.into_iter()
}

fn find_keyword_from(input: &str, start: usize, keyword: &str) -> Option<usize> {
    let mut pos = start;
    while pos < input.len() {
        let ch = input[pos..].chars().next()?;
        if ch == '\'' || ch == '"' {
            pos = skip_quoted(input, pos);
            continue;
        }
        if keyword_at(input, pos, keyword) {
            return Some(pos);
        }
        pos += ch.len_utf8();
    }
    None
}

fn keyword_at(input: &str, pos: usize, keyword: &str) -> bool {
    input[pos..]
        .get(..keyword.len())
        .is_some_and(|word| word.eq_ignore_ascii_case(keyword))
        && input[..pos]
            .chars()
            .next_back()
            .is_none_or(|ch| !is_ident_continue(ch))
        && input[pos + keyword.len()..]
            .chars()
            .next()
            .is_none_or(|ch| !is_ident_continue(ch))
}

fn next_non_ws_is(input: &str, start: usize, expected: char) -> bool {
    input[start..]
        .chars()
        .find(|ch| !ch.is_whitespace())
        .is_some_and(|ch| ch == expected)
}

fn skip_ws(input: &str, mut pos: usize) -> usize {
    while pos < input.len() {
        let Some(ch) = input[pos..].chars().next() else {
            break;
        };
        if !ch.is_whitespace() {
            break;
        }
        pos += ch.len_utf8();
    }
    pos
}

fn copy_quoted(input: &str, start: usize, output: &mut String) -> usize {
    let end = skip_quoted(input, start);
    output.push_str(&input[start..end]);
    end
}

fn skip_quoted(input: &str, start: usize) -> usize {
    let Some(quote) = input[start..].chars().next() else {
        return start;
    };
    let mut pos = start + quote.len_utf8();
    while pos < input.len() {
        let Some(ch) = input[pos..].chars().next() else {
            break;
        };
        pos += ch.len_utf8();
        if ch == '\\' {
            if let Some(escaped) = input[pos..].chars().next() {
                pos += escaped.len_utf8();
            }
            continue;
        }
        if ch == quote {
            if input[pos..].starts_with(quote) {
                pos += quote.len_utf8();
                continue;
            }
            break;
        }
    }
    pos
}

fn inside_string(input: &str, idx: usize) -> bool {
    let mut pos = 0;
    while pos < input.len() {
        let Some(ch) = input[pos..].chars().next() else {
            break;
        };
        if ch == '\'' || ch == '"' {
            let end = skip_quoted(input, pos);
            if idx > pos && idx < end {
                return true;
            }
            pos = end;
            continue;
        }
        pos += ch.len_utf8();
    }
    false
}

fn consume_ident(input: &str, start: usize) -> usize {
    let mut end = start;
    while end < input.len() {
        let Some(ch) = input[end..].chars().next() else {
            break;
        };
        if !is_ident_continue(ch) {
            break;
        }
        end += ch.len_utf8();
    }
    end
}

fn is_ident_start(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphabetic()
}

fn is_ident_continue(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphanumeric()
}

#[cfg(test)]
mod tests {
    use super::translate;

    #[test]
    fn create_becomes_insert() {
        assert_eq!(
            translate("CREATE (:Person {name: 'Ada'})").unwrap(),
            "INSERT (:Person {name: 'Ada'})"
        );
    }

    #[test]
    fn unwind_becomes_for() {
        assert_eq!(
            translate("UNWIND [1, 2] AS x RETURN x").unwrap(),
            "FOR x IN [1, 2] RETURN x"
        );
    }

    #[test]
    fn with_is_untranslatable() {
        let err = translate("MATCH (n) WITH n RETURN n").unwrap_err();
        assert_eq!(err.construct, "WITH");
    }

    #[test]
    fn merge_is_untranslatable() {
        let err = translate("MERGE (:Person {name: 'Ada'})").unwrap_err();
        assert_eq!(err.construct, "MERGE");
    }
}
