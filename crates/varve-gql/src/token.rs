use thiserror::Error;

#[derive(Debug, Error)]
pub enum GqlError {
    #[error("lex error at offset {offset}: {msg}")]
    Lex { offset: usize, msg: String },
    #[error("parse error at offset {offset}: {msg}")]
    Parse { offset: usize, msg: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Keyword {
    Insert,
    Match,
    Where,
    Return,
    As,
    True,
    False,
    Null,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TokenKind {
    Kw(Keyword),
    Ident(String),
    Str(String),
    Int(i64),
    Float(f64),
    LParen,
    RParen,
    LBrace,
    RBrace,
    Colon,
    Comma,
    Dot,
    Eq,
    Dollar,
    Eof,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Token {
    pub kind: TokenKind,
    pub offset: usize,
}

fn keyword(word: &str) -> Option<Keyword> {
    match word.to_ascii_uppercase().as_str() {
        "INSERT" => Some(Keyword::Insert),
        "MATCH" => Some(Keyword::Match),
        "WHERE" => Some(Keyword::Where),
        "RETURN" => Some(Keyword::Return),
        "AS" => Some(Keyword::As),
        "TRUE" => Some(Keyword::True),
        "FALSE" => Some(Keyword::False),
        "NULL" => Some(Keyword::Null),
        _ => None,
    }
}

// v0: ASCII-oriented string handling — multi-byte UTF-8 inside strings passes through bytes and
// this is exercised properly when slice 7 adds the full literal grammar.
pub fn tokenize(src: &str) -> Result<Vec<Token>, GqlError> {
    let bytes = src.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i] as char;
        let offset = i;
        let kind = match c {
            ' ' | '\t' | '\r' | '\n' => {
                i += 1;
                continue;
            }
            '(' => {
                i += 1;
                TokenKind::LParen
            }
            ')' => {
                i += 1;
                TokenKind::RParen
            }
            '{' => {
                i += 1;
                TokenKind::LBrace
            }
            '}' => {
                i += 1;
                TokenKind::RBrace
            }
            ':' => {
                i += 1;
                TokenKind::Colon
            }
            ',' => {
                i += 1;
                TokenKind::Comma
            }
            '.' => {
                i += 1;
                TokenKind::Dot
            }
            '=' => {
                i += 1;
                TokenKind::Eq
            }
            '$' => {
                i += 1;
                TokenKind::Dollar
            }
            '\'' => {
                i += 1;
                let mut s = String::new();
                loop {
                    match bytes.get(i) {
                        Some(b'\'') if bytes.get(i + 1) == Some(&b'\'') => {
                            s.push('\'');
                            i += 2;
                        }
                        Some(b'\'') => {
                            i += 1;
                            break;
                        }
                        Some(&b) => {
                            s.push(b as char);
                            i += 1;
                        }
                        None => {
                            return Err(GqlError::Lex {
                                offset,
                                msg: "unterminated string".into(),
                            })
                        }
                    }
                }
                TokenKind::Str(s)
            }
            '0'..='9' => {
                let start = i;
                while i < bytes.len() && (bytes[i] as char).is_ascii_digit() {
                    i += 1;
                }
                let is_float = i < bytes.len()
                    && bytes[i] == b'.'
                    && bytes
                        .get(i + 1)
                        .is_some_and(|b| (*b as char).is_ascii_digit());
                if is_float {
                    i += 1;
                    while i < bytes.len() && (bytes[i] as char).is_ascii_digit() {
                        i += 1;
                    }
                    let text = &src[start..i];
                    TokenKind::Float(text.parse().map_err(|e| GqlError::Lex {
                        offset,
                        msg: format!("bad float: {e}"),
                    })?)
                } else {
                    let text = &src[start..i];
                    TokenKind::Int(text.parse().map_err(|e| GqlError::Lex {
                        offset,
                        msg: format!("bad int: {e}"),
                    })?)
                }
            }
            c if c.is_ascii_alphabetic() || c == '_' => {
                let start = i;
                while i < bytes.len()
                    && ((bytes[i] as char).is_ascii_alphanumeric() || bytes[i] == b'_')
                {
                    i += 1;
                }
                let word = &src[start..i];
                match keyword(word) {
                    Some(kw) => TokenKind::Kw(kw),
                    None => TokenKind::Ident(word.to_string()),
                }
            }
            other => {
                return Err(GqlError::Lex {
                    offset,
                    msg: format!("unexpected character '{other}'"),
                })
            }
        };
        out.push(Token { kind, offset });
    }
    out.push(Token {
        kind: TokenKind::Eof,
        offset: bytes.len(),
    });
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kinds(src: &str) -> Vec<TokenKind> {
        tokenize(src).unwrap().into_iter().map(|t| t.kind).collect()
    }

    #[test]
    fn tokenizes_insert_statement() {
        use Keyword::*;
        use TokenKind::*;
        assert_eq!(
            kinds("INSERT (:Person {name: 'Ada', age: 36})"),
            vec![
                Kw(Insert),
                LParen,
                Colon,
                Ident("Person".into()),
                LBrace,
                Ident("name".into()),
                Colon,
                Str("Ada".into()),
                Comma,
                Ident("age".into()),
                Colon,
                Int(36),
                RBrace,
                RParen,
                Eof
            ]
        );
    }

    #[test]
    fn keywords_case_insensitive_idents_preserved() {
        use Keyword::*;
        use TokenKind::*;
        assert_eq!(
            kinds("match RETURN Persona"),
            vec![Kw(Match), Kw(Return), Ident("Persona".into()), Eof]
        );
    }

    #[test]
    fn string_escape_and_float() {
        use TokenKind::*;
        assert_eq!(
            kinds("'it''s' 3.5"),
            vec![Str("it's".into()), Float(3.5), Eof]
        );
    }

    #[test]
    fn error_carries_offset() {
        let err = tokenize("MATCH ^").unwrap_err();
        assert!(err.to_string().contains("offset 6"), "{err}");
    }
}
