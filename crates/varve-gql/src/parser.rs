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

    fn peek_at(&self, n: usize) -> &TokenKind {
        self.tokens
            .get(self.pos + n)
            .map(|t| &t.kind)
            .unwrap_or(&TokenKind::Eof)
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
                self.match_tail(temporal)
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
        let mut paths = Vec::new();
        loop {
            let path = self.path_pattern()?;
            if path.start.var.is_none()
                && path.start.labels.is_empty()
                && path.start.props.is_empty()
                && path.hops.is_empty()
            {
                return Err(self.err("INSERT node needs a label or properties"));
            }
            paths.push(path);
            if *self.peek() == TokenKind::Comma {
                self.pos += 1;
                continue;
            }
            break;
        }

        let (mut valid_from, mut valid_to) = (None, None);
        if *self.peek() == TokenKind::Kw(Keyword::Valid) {
            self.pos += 1;
            let offset = self.offset();
            match self.bump() {
                TokenKind::Kw(Keyword::From) => {
                    valid_from = Some(self.datetime()?);
                    if *self.peek() == TokenKind::Kw(Keyword::To) {
                        self.pos += 1;
                        valid_to = Some(self.datetime()?);
                    }
                }
                TokenKind::Kw(Keyword::To) => {
                    valid_to = Some(self.datetime()?);
                }
                other => {
                    return Err(GqlError::Parse {
                        offset,
                        msg: format!("expected FROM or TO after VALID, found {other:?}"),
                    })
                }
            }
            if let (Some(from), Some(to)) = (valid_from, valid_to) {
                if from >= to {
                    return Err(GqlError::Parse {
                        offset,
                        msg: "VALID FROM must be earlier than VALID TO".into(),
                    });
                }
            }
        }
        self.expect(&TokenKind::Eof, "end of statement")?;
        Ok(InsertStmt {
            match_part: None,
            paths,
            valid_from,
            valid_to,
        })
    }

    /// '(' [var] (':' label)* [props] ')'
    fn node_pattern(&mut self) -> Result<NodePattern, GqlError> {
        self.expect(&TokenKind::LParen, "'('")?;
        let var = if matches!(self.peek(), TokenKind::Ident(_)) {
            Some(self.ident("pattern variable")?)
        } else {
            None
        };
        let mut labels = Vec::new();
        while *self.peek() == TokenKind::Colon {
            self.pos += 1;
            labels.push(self.ident("label name")?);
        }
        let props = if *self.peek() == TokenKind::LBrace {
            self.props_block()?
        } else {
            Vec::new()
        };
        self.expect(&TokenKind::RParen, "')'")?;
        Ok(NodePattern { var, labels, props })
    }

    /// '{' [ident ':' literal (',' ident ':' literal)*] '}'
    fn props_block(&mut self) -> Result<Vec<(String, Literal)>, GqlError> {
        self.expect(&TokenKind::LBrace, "'{'")?;
        let mut props = Vec::new();
        if *self.peek() != TokenKind::RBrace {
            loop {
                let key = self.ident("property name")?;
                self.expect(&TokenKind::Colon, "':'")?;
                props.push((key, self.literal()?));
                if *self.peek() == TokenKind::Comma {
                    self.pos += 1;
                } else {
                    break;
                }
            }
        }
        self.expect(&TokenKind::RBrace, "'}'")?;
        Ok(props)
    }

    /// '-[' body ']->' | '<-[' body ']-'  with optional postfix quantifier;
    /// body = [var] ':' label [props]. Label is REQUIRED in v1 (decision 15).
    fn edge_pattern(&mut self) -> Result<EdgePattern, GqlError> {
        let offset = self.offset();
        let direction = match self.peek() {
            TokenKind::Minus => Direction::Out,
            TokenKind::Lt => Direction::In,
            _ => return Err(self.err("expected '-[' or '<-[' edge pattern")),
        };
        if direction == Direction::In {
            self.expect(&TokenKind::Lt, "'<'")?;
        }
        self.expect(&TokenKind::Minus, "'-'")?;
        self.expect(&TokenKind::LBracket, "'['")?;
        let var = if matches!(self.peek(), TokenKind::Ident(_)) {
            Some(self.ident("edge variable")?)
        } else {
            None
        };
        if *self.peek() != TokenKind::Colon {
            return Err(GqlError::Parse {
                offset,
                msg: "edge patterns require a label in v1: -[:LABEL]->".into(),
            });
        }
        self.pos += 1;
        let label = self.ident("edge label")?;
        let props = if *self.peek() == TokenKind::LBrace {
            self.props_block()?
        } else {
            Vec::new()
        };
        self.expect(&TokenKind::RBracket, "']'")?;
        self.expect(&TokenKind::Minus, "'-'")?;
        if direction == Direction::Out {
            self.expect(&TokenKind::Gt, "'>'")?;
        }
        let quantifier = self.quantifier()?;
        if quantifier.is_some() && var.is_some() {
            return Err(GqlError::Parse {
                offset,
                msg: "edge variables on quantified edges (group variables) land in slice 7".into(),
            });
        }
        Ok(EdgePattern {
            var,
            label,
            props,
            direction,
            quantifier,
        })
    }

    /// Postfix '{n}' | '{m,n}' | '{m,}' | '*' — or nothing.
    fn quantifier(&mut self) -> Result<Option<Quantifier>, GqlError> {
        match self.peek() {
            TokenKind::Star => {
                self.pos += 1;
                Ok(Some(Quantifier { min: 0, max: None }))
            }
            TokenKind::LBrace => {
                let offset = self.offset();
                self.pos += 1;
                let min = self.quantifier_bound()?;
                let quant = if *self.peek() == TokenKind::Comma {
                    self.pos += 1;
                    if *self.peek() == TokenKind::RBrace {
                        Quantifier { min, max: None }
                    } else {
                        let max = self.quantifier_bound()?;
                        if max < min {
                            return Err(GqlError::Parse {
                                offset,
                                msg: format!("quantifier min {min} exceeds max {max}"),
                            });
                        }
                        Quantifier {
                            min,
                            max: Some(max),
                        }
                    }
                } else {
                    Quantifier {
                        min,
                        max: Some(min),
                    }
                };
                self.expect(&TokenKind::RBrace, "'}'")?;
                Ok(Some(quant))
            }
            _ => Ok(None),
        }
    }

    fn quantifier_bound(&mut self) -> Result<u32, GqlError> {
        let offset = self.offset();
        match self.bump() {
            TokenKind::Int(n) if (0..=u32::MAX as i64).contains(&n) => Ok(n as u32),
            other => Err(GqlError::Parse {
                offset,
                msg: format!("expected quantifier bound, found {other:?}"),
            }),
        }
    }

    /// [pvar '='] node (edge node)*
    fn path_pattern(&mut self) -> Result<PathPattern, GqlError> {
        let var = if matches!(self.peek(), TokenKind::Ident(_)) && *self.peek_at(1) == TokenKind::Eq
        {
            let v = self.ident("path variable")?;
            self.pos += 1; // '='
            Some(v)
        } else {
            None
        };
        let start = self.node_pattern()?;
        let mut hops = Vec::new();
        while matches!(self.peek(), TokenKind::Minus | TokenKind::Lt) {
            let edge = self.edge_pattern()?;
            let node = self.node_pattern()?;
            hops.push((edge, node));
        }
        Ok(PathPattern { var, start, hops })
    }

    fn literal(&mut self) -> Result<Literal, GqlError> {
        let offset = self.offset();
        match self.bump() {
            TokenKind::Int(i) => Ok(Literal::Int(i)),
            TokenKind::Float(f) => Ok(Literal::Float(f)),
            TokenKind::Str(s) => Ok(Literal::Str(s)),
            TokenKind::Kw(Keyword::True) => Ok(Literal::Bool(true)),
            TokenKind::Kw(Keyword::False) => Ok(Literal::Bool(false)),
            TokenKind::Kw(Keyword::Null) => Ok(Literal::Null),
            TokenKind::Minus => match self.bump() {
                TokenKind::Int(i) => Ok(Literal::Int(-i)),
                TokenKind::Float(f) => Ok(Literal::Float(-f)),
                other => Err(GqlError::Parse {
                    offset,
                    msg: format!("expected number after '-', found {other:?}"),
                }),
            },
            other => Err(GqlError::Parse {
                offset,
                msg: format!("expected literal, found {other:?}"),
            }),
        }
    }

    /// Shared tail after `MATCH <path> (',' <path>)*`: per-MATCH FOR
    /// clauses, an optional WHERE, then either `RETURN …` (a query) or
    /// `[DETACH] DELETE <var>` (a mutation).
    fn match_tail(&mut self, temporal: TemporalClauses) -> Result<Statement, GqlError> {
        let mut paths = vec![self.path_pattern()?];
        while *self.peek() == TokenKind::Comma {
            self.pos += 1;
            paths.push(self.path_pattern()?);
        }

        let match_temporal = self.for_clauses()?;

        let where_clause = if *self.peek() == TokenKind::Kw(Keyword::Where) {
            self.pos += 1;
            Some(self.prop_eq_expr()?)
        } else {
            None
        };

        let offset = self.offset();
        match self.peek().clone() {
            TokenKind::Kw(Keyword::Return) => {
                self.pos += 1;
                let mut return_items = vec![self.return_item()?];
                while *self.peek() == TokenKind::Comma {
                    self.pos += 1;
                    return_items.push(self.return_item()?);
                }
                self.expect(&TokenKind::Eof, "end of statement")?;
                Ok(Statement::Query(Box::new(QueryStmt {
                    temporal,
                    paths,
                    match_temporal,
                    where_clause,
                    return_items,
                })))
            }
            TokenKind::Kw(Keyword::Delete) | TokenKind::Kw(Keyword::Detach) => {
                let detach = *self.peek() == TokenKind::Kw(Keyword::Detach);
                self.pos += 1;
                if detach {
                    self.expect(&TokenKind::Kw(Keyword::Delete), "DELETE")?;
                }
                if temporal != TemporalClauses::default()
                    || match_temporal != TemporalClauses::default()
                {
                    return Err(GqlError::Parse {
                        offset,
                        msg: "DELETE reads current state — temporal clauses are not supported"
                            .into(),
                    });
                }
                if paths.len() != 1 || !paths[0].hops.is_empty() || paths[0].var.is_some() {
                    return Err(GqlError::Parse {
                        offset,
                        msg: "DELETE supports a single node pattern in v1 (edge deletion lands \
                              in slice 7)"
                            .into(),
                    });
                }
                let path = paths.remove(0);
                let target = self.ident("variable to delete")?;
                if path.start.var.as_deref() != Some(target.as_str()) {
                    return Err(GqlError::Parse {
                        offset,
                        msg: format!(
                            "DELETE target '{target}' is not bound (pattern variable is {:?})",
                            path.start.var
                        ),
                    });
                }
                self.expect(&TokenKind::Eof, "end of statement")?;
                Ok(Statement::Delete(DeleteStmt {
                    pattern: path.start,
                    where_clause,
                    target,
                    detach,
                }))
            }
            other => Err(GqlError::Parse {
                offset,
                msg: format!("expected RETURN, DELETE, or DETACH DELETE, found {other:?}"),
            }),
        }
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

    /// A `RETURN` item is either `var.prop [AS alias]`, a temporal function
    /// call `valid_from(var) [AS alias]` / `valid_to(var) [AS alias]` /
    /// `system_from(var) [AS alias]`, or a bare path variable `p [AS alias]`
    /// — disambiguated by whether `(` or `.` follows the leading identifier.
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
        if *self.peek() != TokenKind::Dot {
            let alias = self.alias()?;
            return Ok(ReturnItem::Var { var: name, alias });
        }
        self.pos += 1; // '.'
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

    fn node(var: Option<&str>, labels: &[&str]) -> NodePattern {
        NodePattern {
            var: var.map(String::from),
            labels: labels.iter().map(|s| s.to_string()).collect(),
            props: vec![],
        }
    }

    #[test]
    fn parses_insert_node() {
        let stmt = parse("INSERT (:Person {_id: 1, name: 'Ada'})").unwrap();
        assert_eq!(
            stmt,
            Statement::Insert(InsertStmt {
                match_part: None,
                paths: vec![PathPattern {
                    var: None,
                    start: NodePattern {
                        var: None,
                        labels: vec!["Person".into()],
                        props: vec![
                            ("_id".into(), Literal::Int(1)),
                            ("name".into(), Literal::Str("Ada".into())),
                        ],
                    },
                    hops: vec![],
                }],
                valid_from: None,
                valid_to: None,
            })
        );
    }

    #[test]
    fn parses_insert_two_nodes_comma_separated() {
        let stmt = parse("INSERT (:A {x: 1}), (:B {x: 2})").unwrap();
        let Statement::Insert(ins) = stmt else {
            panic!()
        };
        assert_eq!(ins.paths.len(), 2);
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
            Statement::Query(Box::new(QueryStmt {
                temporal: TemporalClauses::default(),
                paths: vec![PathPattern {
                    var: None,
                    start: node(Some("p"), &["Person"]),
                    hops: vec![],
                }],
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
            }))
        );
    }

    #[test]
    fn parses_single_node_match_as_one_path() {
        let q = query("MATCH (p:Person) WHERE p.name = 'Ada' RETURN p.name");
        assert_eq!(q.paths.len(), 1);
        assert_eq!(q.paths[0].start, node(Some("p"), &["Person"]));
        assert!(q.paths[0].hops.is_empty());
        assert_eq!(q.single_node(), Some(&q.paths[0].start));
    }

    #[test]
    fn parses_two_hop_path() {
        let q = query("MATCH (a:Person)-[:KNOWS]->(b)-[k:KNOWS]->(c:Person) RETURN c.name");
        let p = &q.paths[0];
        assert_eq!(p.start, node(Some("a"), &["Person"]));
        assert_eq!(p.hops.len(), 2);
        let (e0, n1) = &p.hops[0];
        assert_eq!(
            *e0,
            EdgePattern {
                var: None,
                label: "KNOWS".into(),
                props: vec![],
                direction: Direction::Out,
                quantifier: None,
            }
        );
        assert_eq!(*n1, node(Some("b"), &[]));
        let (e1, n2) = &p.hops[1];
        assert_eq!(e1.var.as_deref(), Some("k"));
        assert_eq!(*n2, node(Some("c"), &["Person"]));
    }

    #[test]
    fn parses_reverse_direction_and_props() {
        let q = query("MATCH (a)<-[:KNOWS {since: 2020}]-(b) RETURN a.name");
        let (e, _) = &q.paths[0].hops[0];
        assert_eq!(e.direction, Direction::In);
        assert_eq!(e.props, vec![("since".into(), Literal::Int(2020))]);
    }

    #[test]
    fn parses_quantifiers() {
        let q = query("MATCH (a)-[:KNOWS]->{1,3}(b) RETURN b.name");
        assert_eq!(
            q.paths[0].hops[0].0.quantifier,
            Some(Quantifier {
                min: 1,
                max: Some(3)
            })
        );
        let q = query("MATCH (a)-[:KNOWS]->{2}(b) RETURN b.name");
        assert_eq!(
            q.paths[0].hops[0].0.quantifier,
            Some(Quantifier {
                min: 2,
                max: Some(2)
            })
        );
        let q = query("MATCH (a)-[:KNOWS]->{2,}(b) RETURN b.name");
        assert_eq!(
            q.paths[0].hops[0].0.quantifier,
            Some(Quantifier { min: 2, max: None })
        );
        let q = query("MATCH (a)-[:KNOWS]->*(b) RETURN b.name");
        assert_eq!(
            q.paths[0].hops[0].0.quantifier,
            Some(Quantifier { min: 0, max: None })
        );
    }

    #[test]
    fn parses_path_variable_and_bare_return() {
        let q = query("MATCH p = (a)-[:KNOWS]->{1,3}(b) RETURN p");
        assert_eq!(q.paths[0].var.as_deref(), Some("p"));
        assert_eq!(
            q.return_items,
            vec![ReturnItem::Var {
                var: "p".into(),
                alias: None
            }]
        );
    }

    #[test]
    fn parses_node_props_and_multi_labels_in_match() {
        let q = query("MATCH (a:Person:Admin {name: 'Ada', age: -1}) RETURN a.name");
        let n = &q.paths[0].start;
        assert_eq!(n.labels, vec!["Person".to_string(), "Admin".to_string()]);
        assert_eq!(
            n.props,
            vec![
                ("name".into(), Literal::Str("Ada".into())),
                ("age".into(), Literal::Int(-1)),
            ]
        );
    }

    #[test]
    fn parses_comma_separated_paths() {
        let q = query("MATCH (a:Person), (b:Person) RETURN a.name");
        assert_eq!(q.paths.len(), 2);
    }

    #[test]
    fn edge_without_label_errors() {
        let err = parse("MATCH (a)-[]->(b) RETURN a.name").unwrap_err();
        assert!(err.to_string().contains("label"));
    }

    #[test]
    fn edge_var_on_quantified_edge_errors() {
        let err = parse("MATCH (a)-[k:KNOWS]->{1,3}(b) RETURN a.name").unwrap_err();
        assert!(err.to_string().contains("quantified"));
    }

    #[test]
    fn quantifier_min_above_max_errors() {
        let err = parse("MATCH (a)-[:KNOWS]->{3,1}(b) RETURN a.name").unwrap_err();
        assert!(err.to_string().contains("quantifier"));
    }

    #[test]
    fn parses_match_delete_with_new_shapes() {
        let stmt = parse("MATCH (p:Person) WHERE p.name = 'Ada' DELETE p").unwrap();
        let Statement::Delete(del) = stmt else {
            panic!("expected delete")
        };
        assert_eq!(del.pattern, node(Some("p"), &["Person"]));
        assert!(!del.detach);
    }

    #[test]
    fn delete_rejects_paths_with_hops() {
        let err = parse("MATCH (a)-[:KNOWS]->(b) DELETE a").unwrap_err();
        assert!(err.to_string().contains("DELETE"));
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
            Statement::Query(q) => *q,
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

    #[test]
    fn parses_insert_valid_clause() {
        let Statement::Insert(ins) =
            parse("INSERT (:P {_id: 1}) VALID FROM DATE '2020-01-01' TO DATE '2021-01-01'")
                .unwrap()
        else {
            panic!()
        };
        assert_eq!(ins.valid_from, Some(ts("2020-01-01T00:00:00Z")));
        assert_eq!(ins.valid_to, Some(ts("2021-01-01T00:00:00Z")));

        let Statement::Insert(ins) =
            parse("INSERT (:P {_id: 1}) VALID TO DATE '2021-01-01'").unwrap()
        else {
            panic!()
        };
        assert_eq!(ins.valid_from, None);
        assert_eq!(ins.valid_to, Some(ts("2021-01-01T00:00:00Z")));
    }

    #[test]
    fn insert_valid_range_must_be_ordered() {
        let err = parse("INSERT (:P {_id: 1}) VALID FROM DATE '2021-01-01' TO DATE '2020-01-01'")
            .unwrap_err();
        assert!(err.to_string().contains("earlier"), "{err}");
    }

    #[test]
    fn delete_target_must_be_bound() {
        let err = parse("MATCH (p:Person) DELETE q").unwrap_err();
        assert!(err.to_string().contains("not bound"), "{err}");
    }

    #[test]
    fn delete_rejects_temporal_clauses() {
        let err = parse("FOR VALID_TIME ALL MATCH (p:Person) DELETE p").unwrap_err();
        assert!(err.to_string().contains("DELETE"), "{err}");
    }
}
