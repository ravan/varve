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
        self.expect(&TokenKind::LParen, "'('")?;
        let var = self.ident("pattern variable")?;
        let label = if *self.peek() == TokenKind::Colon {
            self.pos += 1;
            Some(self.ident("label name")?)
        } else {
            None
        };
        self.expect(&TokenKind::RParen, "')'")?;

        let where_clause = if *self.peek() == TokenKind::Kw(Keyword::Where) {
            self.pos += 1;
            Some(self.prop_eq_expr()?)
        } else {
            None
        };

        self.expect(&TokenKind::Kw(Keyword::Return), "RETURN")?;
        let mut return_items = vec![self.return_item()?];
        while *self.peek() == TokenKind::Comma {
            self.pos += 1;
            return_items.push(self.return_item()?);
        }
        self.expect(&TokenKind::Eof, "end of statement")?;
        Ok(QueryStmt {
            pattern: NodePattern { var, label },
            where_clause,
            return_items,
        })
    }

    fn prop_eq_expr(&mut self) -> Result<Expr, GqlError> {
        let var = self.ident("variable")?;
        self.expect(&TokenKind::Dot, "'.'")?;
        let prop = self.ident("property name")?;
        self.expect(&TokenKind::Eq, "'='")?;
        Ok(Expr::PropEq {
            var,
            prop,
            value: self.literal()?,
        })
    }

    fn return_item(&mut self) -> Result<ReturnItem, GqlError> {
        let var = self.ident("variable")?;
        self.expect(&TokenKind::Dot, "'.'")?;
        let prop = self.ident("property name")?;
        let alias = if *self.peek() == TokenKind::Kw(Keyword::As) {
            self.pos += 1;
            Some(self.ident("alias")?)
        } else {
            None
        };
        Ok(ReturnItem { var, prop, alias })
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

    #[test]
    fn parses_match_where_return() {
        let stmt =
            parse("MATCH (p:Person) WHERE p.name = 'Ada' RETURN p.name AS n, p.age").unwrap();
        assert_eq!(
            stmt,
            Statement::Query(QueryStmt {
                pattern: NodePattern {
                    var: "p".into(),
                    label: Some("Person".into())
                },
                where_clause: Some(Expr::PropEq {
                    var: "p".into(),
                    prop: "name".into(),
                    value: Literal::Str("Ada".into()),
                }),
                return_items: vec![
                    ReturnItem {
                        var: "p".into(),
                        prop: "name".into(),
                        alias: Some("n".into())
                    },
                    ReturnItem {
                        var: "p".into(),
                        prop: "age".into(),
                        alias: None
                    },
                ],
            })
        );
    }

    #[test]
    fn match_without_where() {
        let stmt = parse("MATCH (p:Person) RETURN p.name").unwrap();
        let Statement::Query(q) = stmt else { panic!() };
        assert!(q.where_clause.is_none());
        assert_eq!(q.return_items.len(), 1);
    }

    #[test]
    fn match_without_return_errors() {
        assert!(parse("MATCH (p:Person)").is_err());
    }
}
