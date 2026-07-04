use crate::ast::*;
use crate::token::{tokenize, GqlError, Keyword, Token, TokenKind};
use varve_types::{Instant, TemporalDimension};

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
            TokenKind::Kw(Keyword::Match) | TokenKind::Kw(Keyword::For) => {
                let temporal = self.for_clauses()?;
                self.expect(&TokenKind::Kw(Keyword::Match), "MATCH")?;
                self.query_stmt(temporal).map(Statement::Query)
            }
            _ => Err(self.err("expected INSERT, MATCH, or FOR")),
        }
    }

    /// Parses zero or more `FOR VALID_TIME …` / `FOR SYSTEM_TIME …` clauses.
    /// Each axis may appear at most once in this run; a repeat is a parse
    /// error rather than a silent overwrite.
    fn for_clauses(&mut self) -> Result<TemporalClauses, GqlError> {
        let mut clauses = TemporalClauses::default();
        while *self.peek() == TokenKind::Kw(Keyword::For) {
            self.pos += 1;
            let offset = self.offset();
            match self.bump() {
                TokenKind::Kw(Keyword::ValidTime) => {
                    let dim = self.temporal_spec()?;
                    if clauses.valid.replace(dim).is_some() {
                        return Err(GqlError::Parse {
                            offset,
                            msg: "duplicate FOR VALID_TIME clause".into(),
                        });
                    }
                }
                TokenKind::Kw(Keyword::SystemTime) => {
                    let dim = self.temporal_spec()?;
                    if clauses.system.replace(dim).is_some() {
                        return Err(GqlError::Parse {
                            offset,
                            msg: "duplicate FOR SYSTEM_TIME clause".into(),
                        });
                    }
                }
                other => {
                    return Err(GqlError::Parse {
                        offset,
                        msg: format!(
                            "expected VALID_TIME or SYSTEM_TIME after FOR, found {other:?}"
                        ),
                    })
                }
            }
        }
        Ok(clauses)
    }

    /// Parses the value after `FOR VALID_TIME`/`FOR SYSTEM_TIME`: `AS OF …`,
    /// `FROM … TO …`, `BETWEEN … AND …`, or `ALL`.
    fn temporal_spec(&mut self) -> Result<TemporalDimension, GqlError> {
        let offset = self.offset();
        match self.peek().clone() {
            TokenKind::Kw(Keyword::As) => {
                self.pos += 1;
                self.expect(&TokenKind::Kw(Keyword::Of), "OF")?;
                Ok(TemporalDimension::at(self.datetime()?))
            }
            TokenKind::Kw(Keyword::From) => {
                self.pos += 1;
                let from = self.datetime()?;
                self.expect(&TokenKind::Kw(Keyword::To), "TO")?;
                let to = self.datetime()?;
                if from >= to {
                    return Err(GqlError::Parse {
                        offset,
                        msg: "FROM must be earlier than TO".into(),
                    });
                }
                Ok(TemporalDimension::in_range(from, to))
            }
            TokenKind::Kw(Keyword::Between) => {
                self.pos += 1;
                let from = self.datetime()?;
                self.expect(&TokenKind::Kw(Keyword::And), "AND")?;
                let to = self.datetime()?;
                if from > to {
                    return Err(GqlError::Parse {
                        offset,
                        msg: "BETWEEN start must be earlier than or equal to AND end".into(),
                    });
                }
                Ok(TemporalDimension::between(from, to))
            }
            TokenKind::Kw(Keyword::All) => {
                self.pos += 1;
                Ok(TemporalDimension::all())
            }
            other => Err(GqlError::Parse {
                offset,
                msg: format!("expected AS OF, FROM, BETWEEN, or ALL, found {other:?}"),
            }),
        }
    }

    /// Parses `TIMESTAMP '<rfc3339>'` or `DATE '<yyyy-mm-dd>'`.
    fn datetime(&mut self) -> Result<Instant, GqlError> {
        let offset = self.offset();
        let parse_str = |parser: fn(&str) -> Result<Instant, varve_types::TypeError>,
                         token: TokenKind|
         -> Result<Instant, GqlError> {
            match token {
                TokenKind::Str(s) => parser(&s).map_err(|e| GqlError::Parse {
                    offset,
                    msg: e.to_string(),
                }),
                other => Err(GqlError::Parse {
                    offset,
                    msg: format!("expected a quoted datetime literal, found {other:?}"),
                }),
            }
        };
        match self.bump() {
            TokenKind::Kw(Keyword::Timestamp) => parse_str(Instant::parse_rfc3339, self.bump()),
            TokenKind::Kw(Keyword::Date) => parse_str(Instant::parse_date, self.bump()),
            other => Err(GqlError::Parse {
                offset,
                msg: format!("expected TIMESTAMP '…' or DATE '…', found {other:?}"),
            }),
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

    fn query_stmt(&mut self, temporal: TemporalClauses) -> Result<QueryStmt, GqlError> {
        self.expect(&TokenKind::LParen, "'('")?;
        let var = self.ident("pattern variable")?;
        let label = if *self.peek() == TokenKind::Colon {
            self.pos += 1;
            Some(self.ident("label name")?)
        } else {
            None
        };
        self.expect(&TokenKind::RParen, "')'")?;

        let match_temporal = self.for_clauses()?;

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
            temporal,
            pattern: NodePattern { var, label },
            match_temporal,
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

    /// A `RETURN` item is either `var.prop [AS alias]` or a temporal function
    /// call `valid_from(var) [AS alias]` / `valid_to(var) [AS alias]` /
    /// `system_from(var) [AS alias]` — disambiguated by whether `(` follows
    /// the leading identifier.
    fn return_item(&mut self) -> Result<ReturnItem, GqlError> {
        let offset = self.offset();
        let name = self.ident("variable or temporal function")?;
        if *self.peek() == TokenKind::LParen {
            let func = match name.as_str() {
                "valid_from" => TemporalFnKind::ValidFrom,
                "valid_to" => TemporalFnKind::ValidTo,
                "system_from" => TemporalFnKind::SystemFrom,
                other => {
                    return Err(GqlError::Parse {
                        offset,
                        msg: format!(
                            "unknown function '{other}' — expected valid_from, valid_to, or system_from"
                        ),
                    })
                }
            };
            self.pos += 1; // '('
            let var = self.ident("variable")?;
            self.expect(&TokenKind::RParen, "')'")?;
            let alias = self.alias()?;
            return Ok(ReturnItem::TemporalFn { func, var, alias });
        }
        self.expect(&TokenKind::Dot, "'.'")?;
        let prop = self.ident("property name")?;
        let alias = self.alias()?;
        Ok(ReturnItem::Prop {
            var: name,
            prop,
            alias,
        })
    }

    fn alias(&mut self) -> Result<Option<String>, GqlError> {
        if *self.peek() == TokenKind::Kw(Keyword::As) {
            self.pos += 1;
            Ok(Some(self.ident("alias")?))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::*;
    use crate::parse;
    use varve_types::{Instant, TemporalDimension};

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
                temporal: TemporalClauses::default(),
                pattern: NodePattern {
                    var: "p".into(),
                    label: Some("Person".into())
                },
                match_temporal: TemporalClauses::default(),
                where_clause: Some(Expr::PropEq {
                    var: "p".into(),
                    prop: "name".into(),
                    value: Literal::Str("Ada".into()),
                }),
                return_items: vec![
                    ReturnItem::Prop {
                        var: "p".into(),
                        prop: "name".into(),
                        alias: Some("n".into())
                    },
                    ReturnItem::Prop {
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

    fn ts(s: &str) -> Instant {
        Instant::parse_rfc3339(s).unwrap()
    }

    fn query(src: &str) -> QueryStmt {
        match parse(src).unwrap() {
            Statement::Query(q) => q,
            other => panic!("not a query: {other:?}"),
        }
    }

    #[test]
    fn parses_query_level_for_clauses() {
        let q = query(
            "FOR VALID_TIME AS OF TIMESTAMP '2024-01-01T00:00:00Z' \
             FOR SYSTEM_TIME AS OF TIMESTAMP '2025-01-01T00:00:00Z' \
             MATCH (p:Person) RETURN p.name",
        );
        assert_eq!(
            q.temporal.valid,
            Some(TemporalDimension::at(ts("2024-01-01T00:00:00Z")))
        );
        assert_eq!(
            q.temporal.system,
            Some(TemporalDimension::at(ts("2025-01-01T00:00:00Z")))
        );
        assert_eq!(q.match_temporal, TemporalClauses::default());
    }

    #[test]
    fn parses_per_match_for_clause() {
        let q = query("MATCH (p:Person) FOR VALID_TIME AS OF DATE '2024-01-01' RETURN p.name");
        assert_eq!(q.temporal, TemporalClauses::default());
        assert_eq!(
            q.match_temporal.valid,
            Some(TemporalDimension::at(ts("2024-01-01T00:00:00Z")))
        );
    }

    #[test]
    fn parses_range_and_all_specs() {
        let q = query(
            "FOR VALID_TIME FROM TIMESTAMP '2020-01-01T00:00:00Z' TO TIMESTAMP '2021-01-01T00:00:00Z' \
             FOR SYSTEM_TIME ALL MATCH (p:Person) RETURN p.name",
        );
        assert_eq!(
            q.temporal.valid,
            Some(TemporalDimension::in_range(
                ts("2020-01-01T00:00:00Z"),
                ts("2021-01-01T00:00:00Z")
            ))
        );
        assert_eq!(q.temporal.system, Some(TemporalDimension::all()));

        let q = query(
            "FOR VALID_TIME BETWEEN DATE '2020-01-01' AND DATE '2021-01-01' \
             MATCH (p:Person) RETURN p.name",
        );
        assert_eq!(
            q.temporal.valid,
            Some(TemporalDimension::between(
                ts("2020-01-01T00:00:00Z"),
                ts("2021-01-01T00:00:00Z")
            ))
        );
    }

    #[test]
    fn parses_temporal_functions_in_return() {
        let q =
            query("MATCH (p:Person) RETURN valid_from(p) AS since, valid_to(p), system_from(p)");
        assert_eq!(
            q.return_items,
            vec![
                ReturnItem::TemporalFn {
                    func: TemporalFnKind::ValidFrom,
                    var: "p".into(),
                    alias: Some("since".into())
                },
                ReturnItem::TemporalFn {
                    func: TemporalFnKind::ValidTo,
                    var: "p".into(),
                    alias: None
                },
                ReturnItem::TemporalFn {
                    func: TemporalFnKind::SystemFrom,
                    var: "p".into(),
                    alias: None
                },
            ]
        );
    }

    #[test]
    fn temporal_clause_errors() {
        // duplicate axis
        let err =
            parse("FOR VALID_TIME ALL FOR VALID_TIME ALL MATCH (p:P) RETURN p.x").unwrap_err();
        assert!(err.to_string().contains("duplicate"), "{err}");
        // inverted range
        let err = parse(
            "FOR VALID_TIME FROM TIMESTAMP '2021-01-01T00:00:00Z' TO TIMESTAMP '2020-01-01T00:00:00Z' \
             MATCH (p:P) RETURN p.x",
        )
        .unwrap_err();
        assert!(err.to_string().contains("earlier"), "{err}");
        // bad timestamp literal
        let err =
            parse("FOR VALID_TIME AS OF TIMESTAMP 'nope' MATCH (p:P) RETURN p.x").unwrap_err();
        assert!(err.to_string().contains("invalid timestamp"), "{err}");
        // unknown function in RETURN
        let err = parse("MATCH (p:P) RETURN nonsense(p)").unwrap_err();
        assert!(err.to_string().contains("valid_from"), "{err}");
    }
}
