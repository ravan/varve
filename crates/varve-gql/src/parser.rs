use crate::ast::*;
use crate::token::{tokenize, GqlError, Keyword, Token, TokenKind};

pub fn parse(src: &str) -> Result<Statement, GqlError> {
    Parser {
        tokens: tokenize(src)?,
        pos: 0,
    }
    .statement()
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    fn peek(&self) -> &TokenKind {
        &self.tokens[self.pos].kind
    }

    fn offset(&self) -> usize {
        self.tokens[self.pos].offset
    }

    fn bump(&mut self) -> TokenKind {
        let t = self.tokens[self.pos].kind.clone();
        self.pos += 1;
        t
    }

    fn err(&self, msg: impl Into<String>) -> GqlError {
        GqlError::Parse {
            offset: self.offset(),
            msg: msg.into(),
        }
    }

    fn expect(&mut self, kind: &TokenKind, what: &str) -> Result<(), GqlError> {
        if self.peek() == kind {
            self.pos += 1;
            Ok(())
        } else {
            Err(self.err(format!("expected {what}, found {:?}", self.peek())))
        }
    }

    fn ident(&mut self, what: &str) -> Result<String, GqlError> {
        match self.bump() {
            TokenKind::Ident(s) => Ok(s),
            other => Err(GqlError::Parse {
                offset: self.tokens[self.pos - 1].offset,
                msg: format!("expected {what}, found {other:?}"),
            }),
        }
    }

    fn statement(&mut self) -> Result<Statement, GqlError> {
        match self.peek() {
            TokenKind::Kw(Keyword::Insert) => {
                self.pos += 1;
                self.insert_stmt().map(Statement::Insert)
            }
            TokenKind::Kw(Keyword::Match) => {
                self.pos += 1;
                self.query_stmt().map(Statement::Query)
            }
            _ => Err(self.err("expected INSERT or MATCH")),
        }
    }

    fn insert_stmt(&mut self) -> Result<InsertStmt, GqlError> {
        let mut nodes = vec![self.insert_node()?];
        while *self.peek() == TokenKind::Comma {
            self.pos += 1;
            nodes.push(self.insert_node()?);
        }
        self.expect(&TokenKind::Eof, "end of statement")?;
        Ok(InsertStmt { nodes })
    }

    fn insert_node(&mut self) -> Result<InsertNode, GqlError> {
        self.expect(&TokenKind::LParen, "'('")?;
        let mut labels = Vec::new();
        while *self.peek() == TokenKind::Colon {
            self.pos += 1;
            labels.push(self.ident("label name")?);
        }
        if labels.is_empty() {
            return Err(self.err("INSERT node requires at least one label"));
        }
        let mut props = Vec::new();
        if *self.peek() == TokenKind::LBrace {
            self.pos += 1;
            loop {
                let key = self.ident("property name")?;
                self.expect(&TokenKind::Colon, "':'")?;
                props.push((key, self.literal()?));
                match self.bump() {
                    TokenKind::Comma => continue,
                    TokenKind::RBrace => break,
                    other => {
                        return Err(GqlError::Parse {
                            offset: self.tokens[self.pos - 1].offset,
                            msg: format!("expected ',' or '}}', found {other:?}"),
                        })
                    }
                }
            }
        }
        self.expect(&TokenKind::RParen, "')'")?;
        Ok(InsertNode { labels, props })
    }

    fn literal(&mut self) -> Result<Literal, GqlError> {
        match self.bump() {
            TokenKind::Int(i) => Ok(Literal::Int(i)),
            TokenKind::Float(f) => Ok(Literal::Float(f)),
            TokenKind::Str(s) => Ok(Literal::Str(s)),
            TokenKind::Kw(Keyword::True) => Ok(Literal::Bool(true)),
            TokenKind::Kw(Keyword::False) => Ok(Literal::Bool(false)),
            TokenKind::Kw(Keyword::Null) => Ok(Literal::Null),
            other => Err(GqlError::Parse {
                offset: self.tokens[self.pos - 1].offset,
                msg: format!("expected literal, found {other:?}"),
            }),
        }
    }

    fn query_stmt(&mut self) -> Result<QueryStmt, GqlError> {
        Err(self.err("MATCH not implemented yet")) // Task 4
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::*;
    use crate::parse;

    #[test]
    fn parses_insert_node() {
        let stmt = parse("INSERT (:Person {_id: 1, name: 'Ada'})").unwrap();
        assert_eq!(
            stmt,
            Statement::Insert(InsertStmt {
                nodes: vec![InsertNode {
                    labels: vec!["Person".into()],
                    props: vec![
                        ("_id".into(), Literal::Int(1)),
                        ("name".into(), Literal::Str("Ada".into())),
                    ],
                }]
            })
        );
    }

    #[test]
    fn parses_insert_two_nodes_comma_separated() {
        let stmt = parse("INSERT (:A {x: 1}), (:B {x: 2})").unwrap();
        let Statement::Insert(ins) = stmt else {
            panic!()
        };
        assert_eq!(ins.nodes.len(), 2);
    }

    #[test]
    fn insert_without_label_or_props_errors_helpfully() {
        let err = parse("INSERT ()").unwrap_err();
        assert!(err.to_string().contains("label"), "{err}");
    }
}
