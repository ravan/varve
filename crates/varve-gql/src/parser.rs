use crate::ast::*;
use crate::token::{tokenize, GqlError, Keyword, Token, TokenKind};
use varve_types::{Instant, TemporalDimension};

pub fn parse(src: &str) -> Result<Statement, GqlError> {
    let mut parser = Parser {
        tokens: tokenize(src)?,
        pos: 0,
    };
    let stmt = parser.statement()?;
    parser.expect(&TokenKind::Eof, "end of statement")?;
    Ok(stmt)
}

pub fn parse_program(gql: &str) -> Result<Program, GqlError> {
    let mut parser = Parser {
        tokens: tokenize(gql)?,
        pos: 0,
    };
    let mut use_graph = None;
    let mut statements = Vec::new();
    while *parser.peek() != TokenKind::Eof {
        if *parser.peek() == TokenKind::Kw(Keyword::Use) {
            parser.pos += 1;
            let graph = parser.ident("graph name")?;
            if use_graph.replace(graph).is_some() {
                return Err(parser.err("duplicate USE graph clause"));
            }
        } else {
            statements.push(parser.statement()?);
        }
        if *parser.peek() == TokenKind::Semicolon {
            parser.pos += 1;
            while *parser.peek() == TokenKind::Semicolon {
                parser.pos += 1;
            }
        } else if *parser.peek() != TokenKind::Eof {
            return Err(parser.err(format!(
                "expected ';' or end of program, found {:?}",
                parser.peek()
            )));
        }
    }
    Ok(Program {
        use_graph,
        statements,
    })
}

#[cfg(test)]
mod expression_tests {
    use crate::ast::*;
    use crate::parse;

    fn query(src: &str) -> QueryStmt {
        match parse(src).unwrap() {
            Statement::Query(q) => *q,
            other => panic!("not query: {other:?}"),
        }
    }

    fn where_expr(src: &str) -> Expr {
        let q = query(src);
        match &q.first.clauses[0] {
            Clause::Match {
                where_clause: Some(expr),
                ..
            } => expr.clone(),
            other => panic!("expected match where clause, got {other:?}"),
        }
    }

    fn return_exprs(src: &str) -> Vec<Expr> {
        query(src)
            .first
            .ret
            .items
            .into_iter()
            .map(|(expr, _alias)| expr)
            .collect()
    }

    #[test]
    fn parses_precedence_or_xor_and_not() {
        let expr = where_expr(
            "MATCH (a:A), (b:B), (c:C) \
             WHERE a.x = 1 OR NOT b.y = 2 AND c.z = 3 RETURN a.x",
        );

        assert_eq!(
            expr,
            Expr::Binary {
                op: BinaryOp::Or,
                lhs: Box::new(Expr::Binary {
                    op: BinaryOp::Eq,
                    lhs: Box::new(Expr::Prop {
                        var: "a".into(),
                        prop: "x".into(),
                    }),
                    rhs: Box::new(Expr::Literal(Literal::Int(1))),
                }),
                rhs: Box::new(Expr::Binary {
                    op: BinaryOp::And,
                    lhs: Box::new(Expr::Unary {
                        op: UnaryOp::Not,
                        expr: Box::new(Expr::Binary {
                            op: BinaryOp::Eq,
                            lhs: Box::new(Expr::Prop {
                                var: "b".into(),
                                prop: "y".into(),
                            }),
                            rhs: Box::new(Expr::Literal(Literal::Int(2))),
                        }),
                    }),
                    rhs: Box::new(Expr::Binary {
                        op: BinaryOp::Eq,
                        lhs: Box::new(Expr::Prop {
                            var: "c".into(),
                            prop: "z".into(),
                        }),
                        rhs: Box::new(Expr::Literal(Literal::Int(3))),
                    }),
                }),
            }
        );

        let expr = where_expr(
            "MATCH (a:A), (b:B), (c:C) \
             WHERE a.x = 1 XOR b.y = 2 OR c.z = 3 RETURN a.x",
        );
        assert!(matches!(
            expr,
            Expr::Binary {
                op: BinaryOp::Or,
                lhs,
                ..
            } if matches!(*lhs, Expr::Binary { op: BinaryOp::Xor, .. })
        ));
    }

    #[test]
    fn parses_arithmetic_and_postfix_is_null() {
        let expr =
            where_expr("MATCH (a:A) WHERE (a.total + 2 * -a.delta) IS NOT NULL RETURN a.total");

        assert_eq!(
            expr,
            Expr::Unary {
                op: UnaryOp::IsNotNull,
                expr: Box::new(Expr::Binary {
                    op: BinaryOp::Add,
                    lhs: Box::new(Expr::Prop {
                        var: "a".into(),
                        prop: "total".into(),
                    }),
                    rhs: Box::new(Expr::Binary {
                        op: BinaryOp::Mul,
                        lhs: Box::new(Expr::Literal(Literal::Int(2))),
                        rhs: Box::new(Expr::Unary {
                            op: UnaryOp::Neg,
                            expr: Box::new(Expr::Prop {
                                var: "a".into(),
                                prop: "delta".into(),
                            }),
                        }),
                    }),
                }),
            }
        );
    }

    #[test]
    fn parses_case_cast_list_param_and_fn_distinct() {
        let exprs = return_exprs(
            "MATCH (x:X) RETURN $param, [1, x.y], \
             CASE x.kind WHEN 'a' THEN 1 ELSE 2 END, \
             CAST(x.y AS INT), count(DISTINCT x.y)",
        );

        assert_eq!(
            exprs,
            vec![
                Expr::Param("param".into()),
                Expr::List(vec![
                    Expr::Literal(Literal::Int(1)),
                    Expr::Prop {
                        var: "x".into(),
                        prop: "y".into(),
                    },
                ]),
                Expr::Case {
                    operand: Some(Box::new(Expr::Prop {
                        var: "x".into(),
                        prop: "kind".into(),
                    })),
                    whens: vec![(
                        Expr::Literal(Literal::Str("a".into())),
                        Expr::Literal(Literal::Int(1)),
                    )],
                    otherwise: Some(Box::new(Expr::Literal(Literal::Int(2)))),
                },
                Expr::Cast {
                    expr: Box::new(Expr::Prop {
                        var: "x".into(),
                        prop: "y".into(),
                    }),
                    ty: CastType::Int,
                },
                Expr::FnCall {
                    name: "count".into(),
                    args: vec![Expr::Prop {
                        var: "x".into(),
                        prop: "y".into(),
                    }],
                    distinct: true,
                },
            ]
        );
    }

    #[test]
    fn parses_exists_subquery_with_where() {
        let expr =
            where_expr("MATCH (a:A) WHERE EXISTS { (a)-[:R]->(b) WHERE b.x = 1 } RETURN a.x");

        assert_eq!(
            expr,
            Expr::Exists {
                paths: vec![PathPattern {
                    var: None,
                    start: NodePattern {
                        var: Some("a".into()),
                        labels: LabelSpec::All(vec![]),
                        props: vec![],
                    },
                    hops: vec![(
                        EdgePattern {
                            var: None,
                            label: "R".into(),
                            props: vec![],
                            direction: Direction::Out,
                            quantifier: None,
                        },
                        NodePattern {
                            var: Some("b".into()),
                            labels: LabelSpec::All(vec![]),
                            props: vec![],
                        },
                    )],
                }],
                where_clause: Some(Box::new(Expr::Binary {
                    op: BinaryOp::Eq,
                    lhs: Box::new(Expr::Prop {
                        var: "b".into(),
                        prop: "x".into(),
                    }),
                    rhs: Box::new(Expr::Literal(Literal::Int(1))),
                })),
            }
        );
    }

    #[test]
    fn prop_block_can_be_empty_and_keywords_work_as_property_names() {
        let q = query("MATCH (n:L {}) WHERE n.match = 1 RETURN n.return");
        let Clause::Match {
            paths,
            where_clause,
            ..
        } = &q.first.clauses[0]
        else {
            panic!("expected match clause")
        };

        assert_eq!(paths[0].start.props, Vec::<(String, Expr)>::new());
        assert_eq!(
            where_clause,
            &Some(Expr::Binary {
                op: BinaryOp::Eq,
                lhs: Box::new(Expr::Prop {
                    var: "n".into(),
                    prop: "match".into(),
                }),
                rhs: Box::new(Expr::Literal(Literal::Int(1))),
            })
        );
        assert_eq!(
            q.first.ret.items,
            vec![(
                Expr::Prop {
                    var: "n".into(),
                    prop: "return".into(),
                },
                None
            )]
        );

        let q = query("MATCH (n:L {match: 1, return: 2}) RETURN n.match");
        let Clause::Match { paths, .. } = &q.first.clauses[0] else {
            panic!("expected match clause")
        };
        assert_eq!(
            paths[0].start.props,
            vec![
                ("match".into(), Expr::Literal(Literal::Int(1))),
                ("return".into(), Expr::Literal(Literal::Int(2))),
            ]
        );
    }

    #[test]
    fn rejects_reserved_double_underscore_variable_names() {
        let err = parse("MATCH (bad__name:L) RETURN bad__name").unwrap_err();
        assert!(err.to_string().contains("__"), "{err}");
    }

    #[test]
    fn rejects_chained_comparisons() {
        let err = parse("MATCH (a:A) WHERE a.x = 1 = 1 RETURN a.x").unwrap_err();
        assert!(err.to_string().contains("chained comparison"), "{err}");
    }

    #[test]
    fn star_only_valid_inside_count_star() {
        parse("MATCH (a:A) RETURN count(*)").unwrap();
        parse("MATCH (a:A) RETURN count(DISTINCT x.y)").unwrap();

        let err = parse("MATCH (a:A) RETURN *").unwrap_err();
        assert!(err.to_string().contains("count(*)"), "{err}");

        let err = parse("MATCH (a:A) RETURN sum(*)").unwrap_err();
        assert!(err.to_string().contains("count(*)"), "{err}");

        let err = parse("MATCH (a:A) RETURN count(DISTINCT *)").unwrap_err();
        assert!(err.to_string().contains("count(*)"), "{err}");
    }
}

fn keyword_text(keyword: Keyword) -> &'static str {
    match keyword {
        Keyword::Insert => "INSERT",
        Keyword::Match => "MATCH",
        Keyword::Where => "WHERE",
        Keyword::Return => "RETURN",
        Keyword::As => "AS",
        Keyword::True => "TRUE",
        Keyword::False => "FALSE",
        Keyword::Null => "NULL",
        Keyword::For => "FOR",
        Keyword::ValidTime => "VALID_TIME",
        Keyword::SystemTime => "SYSTEM_TIME",
        Keyword::Of => "OF",
        Keyword::All => "ALL",
        Keyword::From => "FROM",
        Keyword::To => "TO",
        Keyword::Between => "BETWEEN",
        Keyword::And => "AND",
        Keyword::Valid => "VALID",
        Keyword::Delete => "DELETE",
        Keyword::Timestamp => "TIMESTAMP",
        Keyword::Date => "DATE",
        Keyword::Detach => "DETACH",
        Keyword::Not => "NOT",
        Keyword::Or => "OR",
        Keyword::Xor => "XOR",
        Keyword::Is => "IS",
        Keyword::Case => "CASE",
        Keyword::When => "WHEN",
        Keyword::Then => "THEN",
        Keyword::Else => "ELSE",
        Keyword::End => "END",
        Keyword::Exists => "EXISTS",
        Keyword::Cast => "CAST",
        Keyword::In => "IN",
        Keyword::Starts => "STARTS",
        Keyword::Ends => "ENDS",
        Keyword::With => "WITH",
        Keyword::Contains => "CONTAINS",
        Keyword::Optional => "OPTIONAL",
        Keyword::Filter => "FILTER",
        Keyword::Let => "LET",
        Keyword::Set => "SET",
        Keyword::Remove => "REMOVE",
        Keyword::Erase => "ERASE",
        Keyword::Union => "UNION",
        Keyword::Distinct => "DISTINCT",
        Keyword::Order => "ORDER",
        Keyword::By => "BY",
        Keyword::Asc => "ASC",
        Keyword::Ascending => "ASCENDING",
        Keyword::Desc => "DESC",
        Keyword::Descending => "DESCENDING",
        Keyword::Skip => "SKIP",
        Keyword::Limit => "LIMIT",
        Keyword::Offset => "OFFSET",
        Keyword::Create => "CREATE",
        Keyword::Drop => "DROP",
        Keyword::Graph => "GRAPH",
        Keyword::Use => "USE",
    }
}

fn cast_type(name: &str) -> Result<CastType, GqlError> {
    match name.to_ascii_uppercase().as_str() {
        "INT" => Ok(CastType::Int),
        "FLOAT" => Ok(CastType::Float),
        "STRING" | "STR" => Ok(CastType::Str),
        "BOOL" | "BOOLEAN" => Ok(CastType::Bool),
        other => Err(GqlError::Parse {
            offset: 0,
            msg: format!("unknown cast type {other}"),
        }),
    }
}

fn is_comparison_op(op: &BinaryOp) -> bool {
    matches!(
        op,
        BinaryOp::Eq
            | BinaryOp::Neq
            | BinaryOp::Lt
            | BinaryOp::Lte
            | BinaryOp::Gt
            | BinaryOp::Gte
            | BinaryOp::In
            | BinaryOp::StartsWith
            | BinaryOp::EndsWith
            | BinaryOp::Contains
    )
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum LabelJoin {
    All,
    Any,
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

fn path_binds_var(path: &PathPattern, target: &str) -> bool {
    path.var.as_deref() == Some(target)
        || path.start.var.as_deref() == Some(target)
        || path.hops.iter().any(|(edge, node)| {
            edge.var.as_deref() == Some(target) || node.var.as_deref() == Some(target)
        })
}

impl Parser {
    fn peek(&self) -> &TokenKind {
        self.tokens
            .get(self.pos)
            .map(|t| &t.kind)
            .unwrap_or(&TokenKind::Eof)
    }

    fn peek_at(&self, n: usize) -> &TokenKind {
        self.tokens
            .get(self.pos + n)
            .map(|t| &t.kind)
            .unwrap_or(&TokenKind::Eof)
    }

    fn offset(&self) -> usize {
        self.tokens
            .get(self.pos)
            .or_else(|| self.tokens.last())
            .map(|t| t.offset)
            .unwrap_or(0)
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

    fn variable_ident(&mut self, what: &str) -> Result<String, GqlError> {
        let offset = self.offset();
        let name = self.ident(what)?;
        if name.contains("__") {
            return Err(GqlError::Parse {
                offset,
                msg: "variable names containing '__' are reserved".into(),
            });
        }
        Ok(name)
    }

    fn property_name(&mut self, what: &str) -> Result<String, GqlError> {
        match self.bump() {
            TokenKind::Ident(s) => Ok(s),
            TokenKind::Kw(kw) => Ok(keyword_text(kw).to_ascii_lowercase()),
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
                self.insert_stmt(None).map(Statement::Insert)
            }
            TokenKind::Kw(Keyword::Match) | TokenKind::Kw(Keyword::For) => {
                let temporal = self.for_clauses()?;
                self.expect(&TokenKind::Kw(Keyword::Match), "MATCH")?;
                self.match_prefixed_statement(temporal)
            }
            TokenKind::Kw(Keyword::Optional) => {
                self.pos += 1;
                self.expect(&TokenKind::Kw(Keyword::Match), "MATCH")?;
                let first = self.match_clause(true)?;
                let first = self.query_body_tail(TemporalClauses::default(), vec![first])?;
                let unions = self.union_tail()?;
                Ok(Statement::Query(Box::new(QueryStmt { first, unions })))
            }
            TokenKind::Kw(Keyword::Create) => {
                self.pos += 1;
                self.expect(&TokenKind::Kw(Keyword::Graph), "GRAPH")?;
                Ok(Statement::Graph(GraphStmt::Create(
                    self.ident("graph name")?,
                )))
            }
            TokenKind::Kw(Keyword::Drop) => {
                self.pos += 1;
                self.expect(&TokenKind::Kw(Keyword::Graph), "GRAPH")?;
                Ok(Statement::Graph(GraphStmt::Drop(self.ident("graph name")?)))
            }
            _ => Err(self.err("expected INSERT, MATCH, FOR, CREATE GRAPH, or DROP GRAPH")),
        }
    }

    /// Parses zero or more `FOR VALID_TIME …` / `FOR SYSTEM_TIME …` clauses.
    /// Each axis may appear at most once in this run; a repeat is a parse
    /// error rather than a silent overwrite.
    fn for_clauses(&mut self) -> Result<TemporalClauses, GqlError> {
        let mut clauses = TemporalClauses::default();
        while *self.peek() == TokenKind::Kw(Keyword::For)
            && matches!(
                self.peek_at(1),
                TokenKind::Kw(Keyword::ValidTime) | TokenKind::Kw(Keyword::SystemTime)
            )
        {
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

    fn insert_stmt(&mut self, match_part: Option<MatchPart>) -> Result<InsertStmt, GqlError> {
        let mut paths = Vec::new();
        loop {
            let offset = self.offset();
            let path = self.path_pattern()?;
            if path.var.is_some() {
                return Err(GqlError::Parse {
                    offset,
                    msg: "path variables (`p = …`) on INSERT patterns land in slice 7: bare \
                          node/edge patterns only for INSERT"
                        .into(),
                });
            }
            if path.hops.iter().any(|(edge, _)| edge.quantifier.is_some()) {
                return Err(GqlError::Parse {
                    offset,
                    msg: "quantifiers (`{m,n}`, `*`) are not supported in INSERT patterns".into(),
                });
            }
            if path.start.var.is_none()
                && path.start.labels.is_empty()
                && path.start.props.is_empty()
                && path.hops.is_empty()
            {
                return Err(GqlError::Parse {
                    offset,
                    msg: "INSERT node needs a label or properties".into(),
                });
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
        Ok(InsertStmt {
            match_part,
            paths,
            valid_from,
            valid_to,
        })
    }

    /// '(' [var] (':' label)* [props] ')'
    fn node_pattern(&mut self) -> Result<NodePattern, GqlError> {
        self.expect(&TokenKind::LParen, "'('")?;
        let var = if matches!(self.peek(), TokenKind::Ident(_)) {
            Some(self.variable_ident("pattern variable")?)
        } else {
            None
        };
        let labels = self.label_spec()?;
        let props = if *self.peek() == TokenKind::LBrace {
            self.props_block()?
        } else {
            Vec::new()
        };
        self.expect(&TokenKind::RParen, "')'")?;
        Ok(NodePattern { var, labels, props })
    }

    fn label_spec(&mut self) -> Result<LabelSpec, GqlError> {
        if *self.peek() != TokenKind::Colon {
            return Ok(LabelSpec::All(vec![]));
        }

        self.pos += 1;
        let mut labels = vec![self.label_name()?];
        let mut join = None;
        loop {
            let next_join = match self.peek() {
                TokenKind::Colon | TokenKind::Amp => Some(LabelJoin::All),
                TokenKind::Pipe => Some(LabelJoin::Any),
                TokenKind::Bang => return Err(self.label_expression_error()),
                _ => None,
            };
            let Some(next_join) = next_join else {
                break;
            };
            if let Some(existing) = join {
                if existing != next_join {
                    return Err(self.label_expression_error());
                }
            } else {
                join = Some(next_join);
            }
            self.pos += 1;
            labels.push(self.label_name()?);
        }

        Ok(match join {
            Some(LabelJoin::Any) => LabelSpec::Any(labels),
            _ => LabelSpec::All(labels),
        })
    }

    fn label_name(&mut self) -> Result<String, GqlError> {
        if *self.peek() == TokenKind::Bang {
            return Err(self.label_expression_error());
        }
        self.ident("label name")
    }

    fn label_expression_error(&self) -> GqlError {
        GqlError::Parse {
            offset: self.offset(),
            msg: "label expression nesting post-v1".into(),
        }
    }

    /// '{' [ident ':' expr (',' ident ':' expr)*] '}'
    fn props_block(&mut self) -> Result<Vec<(String, Expr)>, GqlError> {
        self.expect(&TokenKind::LBrace, "'{'")?;
        let mut props = Vec::new();
        if *self.peek() != TokenKind::RBrace {
            loop {
                let key = self.property_name("property name")?;
                self.expect(&TokenKind::Colon, "':'")?;
                props.push((key, self.expr()?));
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
            Some(self.variable_ident("edge variable")?)
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
            let v = self.variable_ident("path variable")?;
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

    /// Shared tail after `MATCH <path> (',' <path>)*`: per-MATCH FOR
    /// clauses, an optional WHERE, then either `RETURN …` (a query) or
    /// `[DETACH] DELETE <var>` (a mutation).
    fn match_prefixed_statement(
        &mut self,
        temporal: TemporalClauses,
    ) -> Result<Statement, GqlError> {
        let (paths, match_temporal, where_clause) = self.match_parts_after_keyword()?;
        let first_clause = Clause::Match {
            optional: false,
            paths: paths.clone(),
            temporal: match_temporal,
            where_clause: where_clause.clone(),
        };
        let match_part = MatchPart {
            paths,
            where_clause,
        };
        let temporal_mutation =
            temporal != TemporalClauses::default() || match_temporal != TemporalClauses::default();

        match self.peek() {
            TokenKind::Kw(Keyword::Delete) => {
                if temporal_mutation {
                    return Err(
                        self.err("DELETE reads current state - temporal clauses not supported")
                    );
                }
                self.mutate_stmt(match_part, MutKind::Delete, false)
            }
            TokenKind::Kw(Keyword::Erase) => self.mutate_stmt(match_part, MutKind::Erase, false),
            TokenKind::Kw(Keyword::Detach) => {
                self.pos += 1;
                match self.peek() {
                    TokenKind::Kw(Keyword::Delete) => {
                        if temporal_mutation {
                            return Err(self.err(
                                "DELETE reads current state - temporal clauses not supported",
                            ));
                        }
                        self.mutate_stmt(match_part, MutKind::Delete, true)
                    }
                    TokenKind::Kw(Keyword::Erase) => {
                        self.mutate_stmt(match_part, MutKind::Erase, true)
                    }
                    other => Err(GqlError::Parse {
                        offset: self.offset(),
                        msg: format!("expected DELETE or ERASE after DETACH, found {other:?}"),
                    }),
                }
            }
            TokenKind::Kw(Keyword::Set) => self.set_stmt(match_part),
            TokenKind::Kw(Keyword::Remove) => self.remove_stmt(match_part),
            TokenKind::Kw(Keyword::Insert) => {
                if temporal != TemporalClauses::default()
                    || match_temporal != TemporalClauses::default()
                {
                    return Err(self.err(
                        "MATCH ... INSERT reads current state - temporal clauses not supported",
                    ));
                }
                self.validate_match_insert_part(&match_part)?;
                self.pos += 1;
                self.insert_stmt(Some(match_part)).map(Statement::Insert)
            }
            _ => self
                .query_stmt_from_first(temporal, first_clause)
                .map(|stmt| Statement::Query(Box::new(stmt))),
        }
    }

    fn match_parts_after_keyword(
        &mut self,
    ) -> Result<(Vec<PathPattern>, TemporalClauses, Option<Expr>), GqlError> {
        let mut paths = Vec::new();
        if self.can_start_path() {
            paths.push(self.path_pattern()?);
            while *self.peek() == TokenKind::Comma {
                self.pos += 1;
                paths.push(self.path_pattern()?);
            }
        }
        let temporal = self.for_clauses()?;
        let where_clause = if *self.peek() == TokenKind::Kw(Keyword::Where) {
            self.pos += 1;
            Some(self.expr()?)
        } else {
            None
        };
        Ok((paths, temporal, where_clause))
    }

    fn can_start_path(&self) -> bool {
        matches!(self.peek(), TokenKind::LParen)
            || (matches!(self.peek(), TokenKind::Ident(_)) && *self.peek_at(1) == TokenKind::Eq)
    }

    fn query_stmt_from_first(
        &mut self,
        temporal: TemporalClauses,
        first_clause: Clause,
    ) -> Result<QueryStmt, GqlError> {
        let first = self.query_body_tail(temporal, vec![first_clause])?;
        let unions = self.union_tail()?;
        Ok(QueryStmt { first, unions })
    }

    fn query_body(&mut self) -> Result<QueryBody, GqlError> {
        let temporal = self.for_clauses()?;
        self.expect(&TokenKind::Kw(Keyword::Match), "MATCH")?;
        let first = self.match_clause(false)?;
        self.query_body_tail(temporal, vec![first])
    }

    fn query_body_tail(
        &mut self,
        temporal: TemporalClauses,
        mut clauses: Vec<Clause>,
    ) -> Result<QueryBody, GqlError> {
        loop {
            match self.peek() {
                TokenKind::Kw(Keyword::Return) => break,
                TokenKind::Kw(Keyword::Match) => {
                    self.pos += 1;
                    clauses.push(self.match_clause(false)?);
                }
                TokenKind::Kw(Keyword::Optional) => {
                    self.pos += 1;
                    self.expect(&TokenKind::Kw(Keyword::Match), "MATCH")?;
                    clauses.push(self.match_clause(true)?);
                }
                TokenKind::Kw(Keyword::Filter) => {
                    self.pos += 1;
                    clauses.push(Clause::Filter(self.expr()?));
                }
                TokenKind::Kw(Keyword::Let) => {
                    self.pos += 1;
                    clauses.push(Clause::Let(self.let_items()?));
                }
                TokenKind::Kw(Keyword::For)
                    if !matches!(
                        self.peek_at(1),
                        TokenKind::Kw(Keyword::ValidTime) | TokenKind::Kw(Keyword::SystemTime)
                    ) =>
                {
                    self.pos += 1;
                    clauses.push(self.for_clause()?);
                }
                other => {
                    return Err(GqlError::Parse {
                        offset: self.offset(),
                        msg: format!("expected pipeline clause or RETURN, found {other:?}"),
                    });
                }
            }
        }
        let ret = self.return_clause()?;
        Ok(QueryBody {
            temporal,
            clauses,
            ret,
        })
    }

    fn match_clause(&mut self, optional: bool) -> Result<Clause, GqlError> {
        let (paths, temporal, where_clause) = self.match_parts_after_keyword()?;
        Ok(Clause::Match {
            optional,
            paths,
            temporal,
            where_clause,
        })
    }

    fn union_tail(&mut self) -> Result<Vec<(UnionKind, QueryBody)>, GqlError> {
        let mut unions = Vec::new();
        while *self.peek() == TokenKind::Kw(Keyword::Union) {
            self.pos += 1;
            let kind = match self.peek() {
                TokenKind::Kw(Keyword::All) => {
                    self.pos += 1;
                    UnionKind::All
                }
                TokenKind::Kw(Keyword::Distinct) => {
                    self.pos += 1;
                    UnionKind::Distinct
                }
                _ => UnionKind::Distinct,
            };
            unions.push((kind, self.query_body()?));
        }
        Ok(unions)
    }

    fn mutate_stmt(
        &mut self,
        match_part: MatchPart,
        kind: MutKind,
        detach: bool,
    ) -> Result<Statement, GqlError> {
        self.pos += 1;
        let target = self.variable_ident("mutation target")?;
        let kind_name = match kind {
            MutKind::Delete => "DELETE",
            MutKind::Erase => "ERASE",
        };
        self.validate_mutation_target(&match_part, &target, kind_name)?;
        Ok(Statement::Mutate(MutateStmt {
            match_part,
            kind,
            target,
            detach,
        }))
    }

    fn validate_mutation_target(
        &self,
        match_part: &MatchPart,
        target: &str,
        kind_name: &str,
    ) -> Result<(), GqlError> {
        if !match_part
            .paths
            .iter()
            .any(|path| path_binds_var(path, target))
        {
            return Err(self.err(format!("{kind_name} target '{target}' not bound by MATCH")));
        }
        Ok(())
    }

    fn validate_match_insert_part(&self, match_part: &MatchPart) -> Result<(), GqlError> {
        for path in &match_part.paths {
            if path.start.var.is_none()
                && path
                    .hops
                    .iter()
                    .all(|(edge, node)| edge.var.is_none() && node.var.is_none())
            {
                return Err(self.err("MATCH ... INSERT patterns must bind at least one variable"));
            }
        }
        Ok(())
    }

    fn set_stmt(&mut self, match_part: MatchPart) -> Result<Statement, GqlError> {
        self.pos += 1;
        let mut items = vec![self.set_item()?];
        while *self.peek() == TokenKind::Comma {
            self.pos += 1;
            items.push(self.set_item()?);
        }
        Ok(Statement::Set(SetStmt { match_part, items }))
    }

    fn set_item(&mut self) -> Result<SetItem, GqlError> {
        let var = self.variable_ident("SET variable")?;
        match self.peek() {
            TokenKind::Dot => {
                self.pos += 1;
                let key = self.property_name("property name")?;
                self.expect(&TokenKind::Eq, "'='")?;
                Ok(SetItem::Prop {
                    var,
                    prop: key,
                    value: self.expr()?,
                })
            }
            TokenKind::Colon => {
                self.pos += 1;
                Ok(SetItem::Label {
                    var,
                    label: self.ident("label name")?,
                })
            }
            other => Err(GqlError::Parse {
                offset: self.offset(),
                msg: format!("expected property or label SET item, found {other:?}"),
            }),
        }
    }

    fn remove_stmt(&mut self, match_part: MatchPart) -> Result<Statement, GqlError> {
        self.pos += 1;
        let mut items = vec![self.remove_item()?];
        while *self.peek() == TokenKind::Comma {
            self.pos += 1;
            items.push(self.remove_item()?);
        }
        Ok(Statement::Remove(RemoveStmt { match_part, items }))
    }

    fn remove_item(&mut self) -> Result<RemoveItem, GqlError> {
        let var = self.variable_ident("REMOVE variable")?;
        match self.peek() {
            TokenKind::Dot => {
                self.pos += 1;
                Ok(RemoveItem::Prop {
                    var,
                    prop: self.property_name("property name")?,
                })
            }
            TokenKind::Colon => {
                self.pos += 1;
                Ok(RemoveItem::Label {
                    var,
                    label: self.ident("label name")?,
                })
            }
            other => Err(GqlError::Parse {
                offset: self.offset(),
                msg: format!("expected property or label REMOVE item, found {other:?}"),
            }),
        }
    }

    fn let_items(&mut self) -> Result<Vec<(String, Expr)>, GqlError> {
        let mut items = vec![self.let_item()?];
        while *self.peek() == TokenKind::Comma {
            self.pos += 1;
            items.push(self.let_item()?);
        }
        Ok(items)
    }

    fn let_item(&mut self) -> Result<(String, Expr), GqlError> {
        let var = self.variable_ident("LET variable")?;
        self.expect(&TokenKind::Eq, "'='")?;
        Ok((var, self.expr()?))
    }

    fn for_clause(&mut self) -> Result<Clause, GqlError> {
        let var = self.variable_ident("FOR variable")?;
        self.expect(&TokenKind::Kw(Keyword::In), "IN")?;
        Ok(Clause::For {
            var,
            list: self.expr()?,
        })
    }

    fn return_clause(&mut self) -> Result<ReturnClause, GqlError> {
        self.expect(&TokenKind::Kw(Keyword::Return), "RETURN")?;
        let distinct = if *self.peek() == TokenKind::Kw(Keyword::Distinct) {
            self.pos += 1;
            true
        } else {
            false
        };
        let mut items = vec![self.return_item()?];
        while *self.peek() == TokenKind::Comma {
            self.pos += 1;
            items.push(self.return_item()?);
        }

        let mut order_by = Vec::new();
        let mut skip = None;
        let mut limit = None;
        loop {
            match self.peek() {
                TokenKind::Kw(Keyword::Order) => {
                    self.pos += 1;
                    self.expect(&TokenKind::Kw(Keyword::By), "BY")?;
                    order_by.push(self.sort_item()?);
                    while *self.peek() == TokenKind::Comma {
                        self.pos += 1;
                        order_by.push(self.sort_item()?);
                    }
                }
                TokenKind::Kw(Keyword::Skip) | TokenKind::Kw(Keyword::Offset) => {
                    self.pos += 1;
                    skip = Some(self.u64_literal("skip amount")?);
                }
                TokenKind::Kw(Keyword::Limit) => {
                    self.pos += 1;
                    limit = Some(self.u64_literal("limit amount")?);
                }
                _ => break,
            }
        }
        Ok(ReturnClause {
            distinct,
            items,
            order_by,
            skip,
            limit,
        })
    }

    fn sort_item(&mut self) -> Result<SortItem, GqlError> {
        let expr = self.expr()?;
        let asc = match self.peek() {
            TokenKind::Kw(Keyword::Asc) | TokenKind::Kw(Keyword::Ascending) => {
                self.pos += 1;
                true
            }
            TokenKind::Kw(Keyword::Desc) | TokenKind::Kw(Keyword::Descending) => {
                self.pos += 1;
                false
            }
            _ => true,
        };
        Ok(SortItem { expr, asc })
    }

    fn u64_literal(&mut self, what: &str) -> Result<u64, GqlError> {
        let offset = self.offset();
        match self.bump() {
            TokenKind::Int(n) if n >= 0 => Ok(n as u64),
            other => Err(GqlError::Parse {
                offset,
                msg: format!("expected {what}, found {other:?}"),
            }),
        }
    }

    fn expr(&mut self) -> Result<Expr, GqlError> {
        self.expr_bp(0)
    }

    fn expr_bp(&mut self, min_bp: u8) -> Result<Expr, GqlError> {
        let mut lhs = match self.peek() {
            TokenKind::Kw(Keyword::Not) => {
                self.pos += 1;
                Expr::Unary {
                    op: UnaryOp::Not,
                    expr: Box::new(self.expr_bp(4)?),
                }
            }
            TokenKind::Minus => {
                self.pos += 1;
                Expr::Unary {
                    op: UnaryOp::Neg,
                    expr: Box::new(self.expr_bp(8)?),
                }
            }
            _ => self.primary()?,
        };

        loop {
            if min_bp <= 9 && *self.peek() == TokenKind::Kw(Keyword::Is) {
                self.pos += 1;
                let op = if *self.peek() == TokenKind::Kw(Keyword::Not) {
                    self.pos += 1;
                    UnaryOp::IsNotNull
                } else {
                    UnaryOp::IsNull
                };
                self.expect(&TokenKind::Kw(Keyword::Null), "NULL")?;
                lhs = Expr::Unary {
                    op,
                    expr: Box::new(lhs),
                };
                continue;
            }

            let before_op = self.pos;
            let Some((op, left_bp, right_bp)) = self.infix_op()? else {
                break;
            };
            if left_bp < min_bp {
                self.pos = before_op;
                break;
            }
            if is_comparison_op(&op)
                && matches!(
                    lhs,
                    Expr::Binary {
                        op: ref lhs_op,
                        ..
                    } if is_comparison_op(lhs_op)
                )
            {
                return Err(self.err("chained comparison operators are not supported"));
            }
            let rhs = self.expr_bp(right_bp)?;
            lhs = Expr::Binary {
                op,
                lhs: Box::new(lhs),
                rhs: Box::new(rhs),
            };
        }

        Ok(lhs)
    }

    fn infix_op(&mut self) -> Result<Option<(BinaryOp, u8, u8)>, GqlError> {
        let op = match self.peek() {
            TokenKind::Kw(Keyword::Or) => BinaryOp::Or,
            TokenKind::Kw(Keyword::Xor) => BinaryOp::Xor,
            TokenKind::Kw(Keyword::And) => BinaryOp::And,
            TokenKind::Eq => BinaryOp::Eq,
            TokenKind::Neq => BinaryOp::Neq,
            TokenKind::Lt => BinaryOp::Lt,
            TokenKind::Lte => BinaryOp::Lte,
            TokenKind::Gt => BinaryOp::Gt,
            TokenKind::Gte => BinaryOp::Gte,
            TokenKind::Kw(Keyword::In) => BinaryOp::In,
            TokenKind::Kw(Keyword::Starts) if *self.peek_at(1) == TokenKind::Kw(Keyword::With) => {
                self.pos += 1;
                BinaryOp::StartsWith
            }
            TokenKind::Kw(Keyword::Ends) if *self.peek_at(1) == TokenKind::Kw(Keyword::With) => {
                self.pos += 1;
                BinaryOp::EndsWith
            }
            TokenKind::Kw(Keyword::Contains) => BinaryOp::Contains,
            TokenKind::Plus => BinaryOp::Add,
            TokenKind::Minus => BinaryOp::Sub,
            TokenKind::Star => BinaryOp::Mul,
            TokenKind::Slash => BinaryOp::Div,
            TokenKind::Percent => BinaryOp::Mod,
            _ => return Ok(None),
        };

        let bp = match op {
            BinaryOp::Or => 1,
            BinaryOp::Xor => 2,
            BinaryOp::And => 3,
            BinaryOp::Eq
            | BinaryOp::Neq
            | BinaryOp::Lt
            | BinaryOp::Lte
            | BinaryOp::Gt
            | BinaryOp::Gte
            | BinaryOp::In
            | BinaryOp::StartsWith
            | BinaryOp::EndsWith
            | BinaryOp::Contains => 5,
            BinaryOp::Add | BinaryOp::Sub => 6,
            BinaryOp::Mul | BinaryOp::Div | BinaryOp::Mod => 7,
        };
        self.pos += 1;
        Ok(Some((op, bp, bp + 1)))
    }

    fn primary(&mut self) -> Result<Expr, GqlError> {
        match self.bump() {
            TokenKind::Int(i) => Ok(Expr::Literal(Literal::Int(i))),
            TokenKind::Float(f) => Ok(Expr::Literal(Literal::Float(f))),
            TokenKind::Str(s) => Ok(Expr::Literal(Literal::Str(s))),
            TokenKind::Kw(Keyword::True) => Ok(Expr::Literal(Literal::Bool(true))),
            TokenKind::Kw(Keyword::False) => Ok(Expr::Literal(Literal::Bool(false))),
            TokenKind::Kw(Keyword::Null) => Ok(Expr::Literal(Literal::Null)),
            TokenKind::Dollar => {
                let name = self.ident("parameter name")?;
                Ok(Expr::Param(name))
            }
            TokenKind::Ident(name) => self.ident_primary(name),
            TokenKind::Kw(Keyword::Case) => self.case_expr(),
            TokenKind::Kw(Keyword::Exists) => self.exists_expr(),
            TokenKind::Kw(Keyword::Cast) => self.cast_expr(),
            TokenKind::LBracket => self.list_expr(),
            TokenKind::LParen => {
                let expr = self.expr()?;
                self.expect(&TokenKind::RParen, "')'")?;
                Ok(expr)
            }
            TokenKind::Star => Err(GqlError::Parse {
                offset: self.tokens[self.pos - 1].offset,
                msg: "'*' is only valid inside count(*)".into(),
            }),
            other => Err(GqlError::Parse {
                offset: self.tokens[self.pos - 1].offset,
                msg: format!("expected expression, found {other:?}"),
            }),
        }
    }

    fn ident_primary(&mut self, name: String) -> Result<Expr, GqlError> {
        if name.contains("__") {
            return Err(GqlError::Parse {
                offset: self.tokens[self.pos - 1].offset,
                msg: "variable names containing '__' are reserved".into(),
            });
        }
        match self.peek() {
            TokenKind::Dot => {
                self.pos += 1;
                let prop = self.property_name("property name")?;
                Ok(Expr::Prop { var: name, prop })
            }
            TokenKind::LParen => {
                self.pos += 1;
                let (args, distinct) = self.fn_args(&name)?;
                self.expect(&TokenKind::RParen, "')'")?;
                Ok(Expr::FnCall {
                    name,
                    args,
                    distinct,
                })
            }
            _ => Ok(Expr::Var(name)),
        }
    }

    fn fn_args(&mut self, fn_name: &str) -> Result<(Vec<Expr>, bool), GqlError> {
        let distinct = if *self.peek() == TokenKind::Kw(Keyword::Distinct) {
            self.pos += 1;
            true
        } else {
            false
        };
        if *self.peek() == TokenKind::RParen {
            return Ok((Vec::new(), distinct));
        }
        let mut args = if *self.peek() == TokenKind::Star {
            self.pos += 1;
            vec![Expr::Star]
        } else {
            vec![self.expr()?]
        };
        while *self.peek() == TokenKind::Comma {
            self.pos += 1;
            if *self.peek() == TokenKind::Star {
                self.pos += 1;
                args.push(Expr::Star);
            } else {
                args.push(self.expr()?);
            }
        }
        if args.iter().any(|arg| matches!(arg, Expr::Star))
            && !(fn_name == "count"
                && !distinct
                && args.len() == 1
                && matches!(args[0], Expr::Star))
        {
            return Err(self.err("'*' is only valid inside count(*)"));
        }
        Ok((args, distinct))
    }

    fn list_expr(&mut self) -> Result<Expr, GqlError> {
        if *self.peek() == TokenKind::RBracket {
            self.pos += 1;
            return Ok(Expr::List(Vec::new()));
        }
        let mut items = vec![self.expr()?];
        while *self.peek() == TokenKind::Comma {
            self.pos += 1;
            items.push(self.expr()?);
        }
        self.expect(&TokenKind::RBracket, "']'")?;
        Ok(Expr::List(items))
    }

    fn case_expr(&mut self) -> Result<Expr, GqlError> {
        let operand = if *self.peek() == TokenKind::Kw(Keyword::When) {
            None
        } else {
            Some(Box::new(self.expr()?))
        };
        let mut whens = Vec::new();
        while *self.peek() == TokenKind::Kw(Keyword::When) {
            self.pos += 1;
            let cond = self.expr()?;
            self.expect(&TokenKind::Kw(Keyword::Then), "THEN")?;
            let value = self.expr()?;
            whens.push((cond, value));
        }
        let otherwise = if *self.peek() == TokenKind::Kw(Keyword::Else) {
            self.pos += 1;
            Some(Box::new(self.expr()?))
        } else {
            None
        };
        self.expect(&TokenKind::Kw(Keyword::End), "END")?;
        Ok(Expr::Case {
            operand,
            whens,
            otherwise,
        })
    }

    fn exists_expr(&mut self) -> Result<Expr, GqlError> {
        self.expect(&TokenKind::LBrace, "'{'")?;
        let mut paths = vec![self.path_pattern()?];
        while *self.peek() == TokenKind::Comma {
            self.pos += 1;
            paths.push(self.path_pattern()?);
        }
        let where_clause = if *self.peek() == TokenKind::Kw(Keyword::Where) {
            self.pos += 1;
            Some(Box::new(self.expr()?))
        } else {
            None
        };
        self.expect(&TokenKind::RBrace, "'}'")?;
        Ok(Expr::Exists {
            paths,
            where_clause,
        })
    }

    fn cast_expr(&mut self) -> Result<Expr, GqlError> {
        self.expect(&TokenKind::LParen, "'('")?;
        let expr = self.expr()?;
        self.expect(&TokenKind::Kw(Keyword::As), "AS")?;
        let ty = match self.bump() {
            TokenKind::Ident(s) => cast_type(&s),
            TokenKind::Kw(kw) => cast_type(keyword_text(kw)),
            other => {
                return Err(GqlError::Parse {
                    offset: self.tokens[self.pos - 1].offset,
                    msg: format!("expected cast type, found {other:?}"),
                })
            }
        }?;
        self.expect(&TokenKind::RParen, "')'")?;
        Ok(Expr::Cast {
            expr: Box::new(expr),
            ty,
        })
    }

    fn return_item(&mut self) -> Result<(Expr, Option<String>), GqlError> {
        let expr = self.expr()?;
        let alias = self.alias()?;
        Ok((expr, alias))
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
            labels: LabelSpec::All(labels.iter().map(|s| s.to_string()).collect()),
            props: vec![],
        }
    }

    fn first_match(q: &QueryStmt) -> (&[PathPattern], &TemporalClauses, Option<&Expr>) {
        match &q.first.clauses[0] {
            Clause::Match {
                paths,
                temporal,
                where_clause,
                ..
            } => (paths, temporal, where_clause.as_ref()),
            other => panic!("expected match clause, got {other:?}"),
        }
    }

    fn paths(q: &QueryStmt) -> &[PathPattern] {
        first_match(q).0
    }

    fn match_temporal(q: &QueryStmt) -> &TemporalClauses {
        first_match(q).1
    }

    fn where_clause(q: &QueryStmt) -> Option<&Expr> {
        first_match(q).2
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
                        labels: LabelSpec::All(vec!["Person".into()]),
                        props: vec![
                            ("_id".into(), Expr::Literal(Literal::Int(1))),
                            ("name".into(), Expr::Literal(Literal::Str("Ada".into()))),
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
                first: QueryBody {
                    temporal: TemporalClauses::default(),
                    clauses: vec![Clause::Match {
                        optional: false,
                        paths: vec![PathPattern {
                            var: None,
                            start: node(Some("p"), &["Person"]),
                            hops: vec![],
                        }],
                        temporal: TemporalClauses::default(),
                        where_clause: Some(Expr::Binary {
                            op: BinaryOp::Eq,
                            lhs: Box::new(Expr::Prop {
                                var: "p".into(),
                                prop: "name".into(),
                            }),
                            rhs: Box::new(Expr::Literal(Literal::Str("Ada".into()))),
                        }),
                    }],
                    ret: ReturnClause {
                        distinct: false,
                        items: vec![
                            (
                                Expr::Prop {
                                    var: "p".into(),
                                    prop: "name".into(),
                                },
                                Some("n".into()),
                            ),
                            (
                                Expr::Prop {
                                    var: "p".into(),
                                    prop: "age".into(),
                                },
                                None,
                            )
                        ],
                        order_by: vec![],
                        skip: None,
                        limit: None,
                    },
                },
                unions: vec![],
            }))
        );
    }

    #[test]
    fn parses_single_node_match_as_one_path() {
        let q = query("MATCH (p:Person) WHERE p.name = 'Ada' RETURN p.name");
        assert_eq!(paths(&q).len(), 1);
        assert_eq!(paths(&q)[0].start, node(Some("p"), &["Person"]));
        assert!(paths(&q)[0].hops.is_empty());
        assert_eq!(q.single_node(), Some(&paths(&q)[0].start));
    }

    #[test]
    fn parses_two_hop_path() {
        let q = query("MATCH (a:Person)-[:KNOWS]->(b)-[k:KNOWS]->(c:Person) RETURN c.name");
        let p = &paths(&q)[0];
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
        let (e, _) = &paths(&q)[0].hops[0];
        assert_eq!(e.direction, Direction::In);
        assert_eq!(
            e.props,
            vec![("since".into(), Expr::Literal(Literal::Int(2020)))]
        );
    }

    #[test]
    fn parses_quantifiers() {
        let q = query("MATCH (a)-[:KNOWS]->{1,3}(b) RETURN b.name");
        assert_eq!(
            paths(&q)[0].hops[0].0.quantifier,
            Some(Quantifier {
                min: 1,
                max: Some(3)
            })
        );
        let q = query("MATCH (a)-[:KNOWS]->{2}(b) RETURN b.name");
        assert_eq!(
            paths(&q)[0].hops[0].0.quantifier,
            Some(Quantifier {
                min: 2,
                max: Some(2)
            })
        );
        let q = query("MATCH (a)-[:KNOWS]->{2,}(b) RETURN b.name");
        assert_eq!(
            paths(&q)[0].hops[0].0.quantifier,
            Some(Quantifier { min: 2, max: None })
        );
        let q = query("MATCH (a)-[:KNOWS]->*(b) RETURN b.name");
        assert_eq!(
            paths(&q)[0].hops[0].0.quantifier,
            Some(Quantifier { min: 0, max: None })
        );
    }

    #[test]
    fn parses_path_variable_and_bare_return() {
        let q = query("MATCH p = (a)-[:KNOWS]->{1,3}(b) RETURN p");
        assert_eq!(paths(&q)[0].var.as_deref(), Some("p"));
        assert_eq!(q.first.ret.items, vec![(Expr::Var("p".into()), None)]);
    }

    #[test]
    fn parses_node_props_and_multi_labels_in_match() {
        let q = query("MATCH (a:Person:Admin {name: 'Ada', age: -1}) RETURN a.name");
        let n = &paths(&q)[0].start;
        assert_eq!(
            n.labels,
            LabelSpec::All(vec!["Person".to_string(), "Admin".to_string()])
        );
        assert_eq!(
            n.props,
            vec![
                ("name".into(), Expr::Literal(Literal::Str("Ada".into()))),
                (
                    "age".into(),
                    Expr::Unary {
                        op: UnaryOp::Neg,
                        expr: Box::new(Expr::Literal(Literal::Int(1))),
                    },
                ),
            ]
        );
    }

    #[test]
    fn parses_comma_separated_paths() {
        let q = query("MATCH (a:Person), (b:Person) RETURN a.name");
        assert_eq!(paths(&q).len(), 2);
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
        let Statement::Mutate(del) = stmt else {
            panic!("expected delete")
        };
        assert_eq!(del.match_part.paths[0].start, node(Some("p"), &["Person"]));
        assert_eq!(
            del.match_part.where_clause,
            Some(Expr::Binary {
                op: BinaryOp::Eq,
                lhs: Box::new(Expr::Prop {
                    var: "p".into(),
                    prop: "name".into(),
                }),
                rhs: Box::new(Expr::Literal(Literal::Str("Ada".into()))),
            })
        );
        assert_eq!(del.target, "p");
        assert!(!del.detach);
        assert_eq!(del.kind, MutKind::Delete);
    }

    #[test]
    fn delete_accepts_paths_with_hops() {
        let stmt = parse("MATCH (a)-[:KNOWS]->(b) DELETE b").unwrap();
        let Statement::Mutate(del) = stmt else {
            panic!()
        };
        assert_eq!(del.target, "b");
        assert_eq!(del.match_part.paths[0].hops[0].0.label, "KNOWS");
    }

    #[test]
    fn match_without_where() {
        let stmt = parse("MATCH (p:Person) RETURN p.name").unwrap();
        let Statement::Query(q) = stmt else { panic!() };
        assert!(where_clause(&q).is_none());
        assert_eq!(q.first.ret.items.len(), 1);
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
            q.first.temporal.valid,
            Some(TemporalDimension::at(ts("2024-01-01T00:00:00Z")))
        );
        assert_eq!(
            q.first.temporal.system,
            Some(TemporalDimension::at(ts("2025-01-01T00:00:00Z")))
        );
        assert_eq!(*match_temporal(&q), TemporalClauses::default());
    }

    #[test]
    fn parses_per_match_for_clause() {
        let q = query("MATCH (p:Person) FOR VALID_TIME AS OF DATE '2024-01-01' RETURN p.name");
        assert_eq!(q.first.temporal, TemporalClauses::default());
        assert_eq!(
            match_temporal(&q).valid,
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
            q.first.temporal.valid,
            Some(TemporalDimension::in_range(
                ts("2020-01-01T00:00:00Z"),
                ts("2021-01-01T00:00:00Z")
            ))
        );
        assert_eq!(q.first.temporal.system, Some(TemporalDimension::all()));

        let q = query(
            "FOR VALID_TIME BETWEEN DATE '2020-01-01' AND DATE '2021-01-01' \
             MATCH (p:Person) RETURN p.name",
        );
        assert_eq!(
            q.first.temporal.valid,
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
            q.first.ret.items,
            vec![
                (
                    Expr::FnCall {
                        name: "valid_from".into(),
                        args: vec![Expr::Var("p".into())],
                        distinct: false,
                    },
                    Some("since".into()),
                ),
                (
                    Expr::FnCall {
                        name: "valid_to".into(),
                        args: vec![Expr::Var("p".into())],
                        distinct: false,
                    },
                    None,
                ),
                (
                    Expr::FnCall {
                        name: "system_from".into(),
                        args: vec![Expr::Var("p".into())],
                        distinct: false,
                    },
                    None,
                ),
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
        let q = query("MATCH (p:P) RETURN nonsense(p)");
        assert_eq!(
            q.first.ret.items,
            vec![(
                Expr::FnCall {
                    name: "nonsense".into(),
                    args: vec![Expr::Var("p".into())],
                    distinct: false,
                },
                None,
            )]
        );
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

    #[test]
    fn parses_insert_edge_with_inline_nodes() {
        let stmt = parse(
            "INSERT (:Person {_id: 1, name: 'Ada'})-[:KNOWS {since: 2020}]->(:Person {_id: 2})",
        )
        .unwrap();
        let Statement::Insert(ins) = stmt else {
            panic!("expected insert")
        };
        assert!(ins.match_part.is_none());
        assert_eq!(ins.paths.len(), 1);
        let p = &ins.paths[0];
        assert_eq!(p.start.labels, LabelSpec::All(vec!["Person".to_string()]));
        assert_eq!(p.hops.len(), 1);
        let (e, end) = &p.hops[0];
        assert_eq!(e.label, "KNOWS");
        assert_eq!(
            e.props,
            vec![("since".into(), Expr::Literal(Literal::Int(2020)))]
        );
        assert_eq!(end.props[0], ("_id".into(), Expr::Literal(Literal::Int(2))));
    }

    #[test]
    fn parses_insert_var_reuse_across_paths() {
        let stmt = parse("INSERT (a:Person {_id: 1}), (a)-[:KNOWS]->(b:Person {_id: 2})").unwrap();
        let Statement::Insert(ins) = stmt else {
            panic!("expected insert")
        };
        assert_eq!(ins.paths.len(), 2);
        assert_eq!(
            ins.paths[1].start,
            NodePattern {
                var: Some("a".into()),
                labels: LabelSpec::All(vec![]),
                props: vec![]
            }
        );
        assert_eq!(ins.paths[1].hops[0].1.var.as_deref(), Some("b"));
    }

    #[test]
    fn parses_match_insert() {
        let stmt = parse(
            "MATCH (a:Person {name: 'Ada'}), (b:Person) WHERE b.name = 'Bob' INSERT (a)-[:KNOWS]->(b)",
        )
        .unwrap();
        let Statement::Insert(ins) = stmt else {
            panic!("expected insert")
        };
        let mp = ins.match_part.as_ref().unwrap();
        assert_eq!(mp.paths.len(), 2);
        assert_eq!(
            mp.paths[0].start.props,
            vec![("name".into(), Expr::Literal(Literal::Str("Ada".into())))]
        );
        assert!(mp.where_clause.is_some());
        assert_eq!(ins.paths[0].hops[0].0.label, "KNOWS");
    }

    #[test]
    fn match_insert_accepts_hops_in_match_part() {
        let stmt = parse("MATCH (a)-[:KNOWS]->(b) INSERT (a)-[:LIKES]->(b)").unwrap();
        let Statement::Insert(ins) = stmt else {
            panic!()
        };
        let mp = ins.match_part.unwrap();
        assert_eq!(mp.paths[0].hops[0].0.label, "KNOWS");
        assert_eq!(ins.paths[0].hops[0].0.label, "LIKES");
    }

    #[test]
    fn match_insert_requires_vars_and_rejects_temporal() {
        let err = parse("MATCH (:Person) INSERT (:X {_id: 1})").unwrap_err();
        assert!(err.to_string().contains("variable"));
        let err = parse("FOR VALID_TIME ALL MATCH (a:Person) INSERT (a)-[:K]->(a)").unwrap_err();
        assert!(err.to_string().contains("INSERT"));
    }

    #[test]
    fn parses_insert_edge_valid_clause() {
        let stmt = parse(
            "INSERT (:P {_id: 1})-[:K]->(:P {_id: 2}) VALID FROM TIMESTAMP '2020-01-01T00:00:00Z'",
        )
        .unwrap();
        let Statement::Insert(ins) = stmt else {
            panic!("expected insert")
        };
        assert!(ins.valid_from.is_some());
    }

    #[test]
    fn parses_detach_delete() {
        let stmt = parse("MATCH (p:Person) WHERE p.name = 'Ada' DETACH DELETE p").unwrap();
        let Statement::Mutate(del) = stmt else {
            panic!("expected delete")
        };
        assert!(del.detach);
        assert_eq!(del.target, "p");
        assert_eq!(del.kind, MutKind::Delete);
    }

    #[test]
    fn insert_paths_reject_quantifiers_and_path_vars() {
        let err = parse("INSERT (a:P {_id: 1})-[:K]->{1,3}(b:P {_id: 2})").unwrap_err();
        assert!(err.to_string().contains("quantifier"));
        let err = parse("INSERT p = (a:P {_id: 1})-[:K]->(b:P {_id: 2})").unwrap_err();
        assert!(err.to_string().contains("path variable"));
    }

    fn all(labels: &[&str]) -> LabelSpec {
        LabelSpec::All(labels.iter().map(|label| (*label).into()).collect())
    }

    fn any(labels: &[&str]) -> LabelSpec {
        LabelSpec::Any(labels.iter().map(|label| (*label).into()).collect())
    }

    fn gql_node(var: Option<&str>, labels: LabelSpec) -> NodePattern {
        NodePattern {
            var: var.map(String::from),
            labels,
            props: vec![],
        }
    }

    fn gql_path(start: NodePattern, hops: Vec<(EdgePattern, NodePattern)>) -> PathPattern {
        PathPattern {
            var: None,
            start,
            hops,
        }
    }

    fn edge(label: &str, direction: Direction) -> EdgePattern {
        EdgePattern {
            var: None,
            label: label.into(),
            props: vec![],
            direction,
            quantifier: None,
        }
    }

    fn match_clause(paths: Vec<PathPattern>) -> Clause {
        Clause::Match {
            optional: false,
            paths,
            temporal: TemporalClauses::default(),
            where_clause: None,
        }
    }

    fn ret(items: Vec<(Expr, Option<&str>)>) -> ReturnClause {
        ReturnClause {
            distinct: false,
            items: items
                .into_iter()
                .map(|(expr, alias)| (expr, alias.map(String::from)))
                .collect(),
            order_by: vec![],
            skip: None,
            limit: None,
        }
    }

    fn body(clauses: Vec<Clause>, ret: ReturnClause) -> QueryBody {
        QueryBody {
            temporal: TemporalClauses::default(),
            clauses,
            ret,
        }
    }

    fn var(name: &str) -> Expr {
        Expr::Var(name.into())
    }

    fn prop(var: &str, key: &str) -> Expr {
        Expr::Prop {
            var: var.into(),
            prop: key.into(),
        }
    }

    #[test]
    fn parses_multi_clause_pipeline() {
        let stmt = parse(
            "MATCH (a:Person) OPTIONAL MATCH (a)-[:KNOWS]->(b:Person) \
             FILTER b.age > 18 LET name = b.name FOR friend IN [b] RETURN name AS n, friend",
        )
        .unwrap();

        assert_eq!(
            stmt,
            Statement::Query(Box::new(QueryStmt {
                first: body(
                    vec![
                        match_clause(vec![gql_path(
                            gql_node(Some("a"), all(&["Person"])),
                            vec![],
                        )]),
                        Clause::Match {
                            optional: true,
                            paths: vec![gql_path(
                                gql_node(Some("a"), all(&[])),
                                vec![(
                                    edge("KNOWS", Direction::Out),
                                    gql_node(Some("b"), all(&["Person"])),
                                )],
                            )],
                            temporal: TemporalClauses::default(),
                            where_clause: None,
                        },
                        Clause::Filter(Expr::Binary {
                            op: BinaryOp::Gt,
                            lhs: Box::new(prop("b", "age")),
                            rhs: Box::new(Expr::Literal(Literal::Int(18))),
                        }),
                        Clause::Let(vec![("name".into(), prop("b", "name"))]),
                        Clause::For {
                            var: "friend".into(),
                            list: Expr::List(vec![var("b")]),
                        },
                    ],
                    ret(vec![(var("name"), Some("n")), (var("friend"), None)]),
                ),
                unions: vec![],
            }))
        );
    }

    #[test]
    fn parses_comma_separated_match_paths() {
        let stmt = parse("MATCH (a:A), (b:B)-[:R]->(c:C) RETURN a, c").unwrap();

        assert_eq!(
            stmt,
            Statement::Query(Box::new(QueryStmt {
                first: body(
                    vec![match_clause(vec![
                        gql_path(gql_node(Some("a"), all(&["A"])), vec![]),
                        gql_path(
                            gql_node(Some("b"), all(&["B"])),
                            vec![(edge("R", Direction::Out), gql_node(Some("c"), all(&["C"])))],
                        ),
                    ])],
                    ret(vec![(var("a"), None), (var("c"), None)]),
                ),
                unions: vec![],
            }))
        );
    }

    #[test]
    fn parses_return_distinct_order_skip_limit_offset_aliases() {
        let stmt = parse(
            "MATCH (n:Person) RETURN DISTINCT n.name AS name, n.age AS age \
             ORDER BY n.age DESC, n.name ASC SKIP 5 LIMIT 10",
        )
        .unwrap();

        assert_eq!(
            stmt,
            Statement::Query(Box::new(QueryStmt {
                first: body(
                    vec![match_clause(vec![gql_path(
                        gql_node(Some("n"), all(&["Person"])),
                        vec![],
                    )])],
                    ReturnClause {
                        distinct: true,
                        items: vec![
                            (prop("n", "name"), Some("name".into())),
                            (prop("n", "age"), Some("age".into())),
                        ],
                        order_by: vec![
                            SortItem {
                                expr: prop("n", "age"),
                                asc: false,
                            },
                            SortItem {
                                expr: prop("n", "name"),
                                asc: true,
                            },
                        ],
                        skip: Some(5),
                        limit: Some(10),
                    },
                ),
                unions: vec![],
            }))
        );

        let stmt = parse("MATCH (n) RETURN n OFFSET 7 LIMIT 8").unwrap();
        assert_eq!(
            stmt,
            Statement::Query(Box::new(QueryStmt {
                first: body(
                    vec![match_clause(vec![gql_path(
                        gql_node(Some("n"), all(&[])),
                        vec![]
                    )])],
                    ReturnClause {
                        distinct: false,
                        items: vec![(var("n"), None)],
                        order_by: vec![],
                        skip: Some(7),
                        limit: Some(8),
                    },
                ),
                unions: vec![],
            }))
        );
    }

    #[test]
    fn parses_union_and_union_all_chain() {
        let stmt =
            parse("MATCH (a:A) RETURN a UNION MATCH (b:B) RETURN b UNION ALL MATCH (c:C) RETURN c")
                .unwrap();

        assert_eq!(
            stmt,
            Statement::Query(Box::new(QueryStmt {
                first: body(
                    vec![match_clause(vec![gql_path(
                        gql_node(Some("a"), all(&["A"])),
                        vec![]
                    )])],
                    ret(vec![(var("a"), None)]),
                ),
                unions: vec![
                    (
                        UnionKind::Distinct,
                        body(
                            vec![match_clause(vec![gql_path(
                                gql_node(Some("b"), all(&["B"])),
                                vec![],
                            )])],
                            ret(vec![(var("b"), None)]),
                        ),
                    ),
                    (
                        UnionKind::All,
                        body(
                            vec![match_clause(vec![gql_path(
                                gql_node(Some("c"), all(&["C"])),
                                vec![],
                            )])],
                            ret(vec![(var("c"), None)]),
                        ),
                    ),
                ],
            }))
        );
    }

    #[test]
    fn parses_label_conjunction_and_alternation() {
        let stmt = parse("MATCH (a:A:B), (b:A&B), (c:A|B) RETURN a, b, c").unwrap();

        assert_eq!(
            stmt,
            Statement::Query(Box::new(QueryStmt {
                first: body(
                    vec![match_clause(vec![
                        gql_path(gql_node(Some("a"), all(&["A", "B"])), vec![]),
                        gql_path(gql_node(Some("b"), all(&["A", "B"])), vec![]),
                        gql_path(gql_node(Some("c"), any(&["A", "B"])), vec![]),
                    ])],
                    ret(vec![(var("a"), None), (var("b"), None), (var("c"), None)]),
                ),
                unions: vec![],
            }))
        );
    }

    #[test]
    fn label_mixing_rejected() {
        let err = parse("MATCH (n:A&B|C) RETURN n").unwrap_err();
        assert!(
            err.to_string().contains("label expression nesting post-v1"),
            "{err}"
        );

        let err = parse("MATCH (n:!A) RETURN n").unwrap_err();
        assert!(
            err.to_string().contains("label expression nesting post-v1"),
            "{err}"
        );
    }

    #[test]
    fn parses_set_remove_statements() {
        let match_part = MatchPart {
            paths: vec![gql_path(gql_node(Some("n"), all(&["Person"])), vec![])],
            where_clause: Some(Expr::Binary {
                op: BinaryOp::Eq,
                lhs: Box::new(prop("n", "_id")),
                rhs: Box::new(Expr::Literal(Literal::Int(1))),
            }),
        };

        assert_eq!(
            parse("MATCH (n:Person) WHERE n._id = 1 SET n.name = 'Ada', n:Employee").unwrap(),
            Statement::Set(SetStmt {
                match_part: match_part.clone(),
                items: vec![
                    SetItem::Prop {
                        var: "n".into(),
                        prop: "name".into(),
                        value: Expr::Literal(Literal::Str("Ada".into())),
                    },
                    SetItem::Label {
                        var: "n".into(),
                        label: "Employee".into(),
                    },
                ],
            })
        );

        assert_eq!(
            parse("MATCH (n:Person) WHERE n._id = 1 REMOVE n.name, n:Employee").unwrap(),
            Statement::Remove(RemoveStmt {
                match_part,
                items: vec![
                    RemoveItem::Prop {
                        var: "n".into(),
                        prop: "name".into(),
                    },
                    RemoveItem::Label {
                        var: "n".into(),
                        label: "Employee".into(),
                    },
                ],
            })
        );
    }

    #[test]
    fn parses_erase_and_detach_erase() {
        let match_part = MatchPart {
            paths: vec![gql_path(gql_node(Some("n"), all(&["Person"])), vec![])],
            where_clause: None,
        };

        assert_eq!(
            parse("MATCH (n:Person) ERASE n").unwrap(),
            Statement::Mutate(MutateStmt {
                match_part: match_part.clone(),
                kind: MutKind::Erase,
                target: "n".into(),
                detach: false,
            })
        );

        assert_eq!(
            parse("MATCH (n:Person) DETACH ERASE n").unwrap(),
            Statement::Mutate(MutateStmt {
                match_part,
                kind: MutKind::Erase,
                target: "n".into(),
                detach: true,
            })
        );
    }

    #[test]
    fn parses_create_drop_graph_statements() {
        assert_eq!(
            parse("CREATE GRAPH people").unwrap(),
            Statement::Graph(GraphStmt::Create("people".into()))
        );
        assert_eq!(
            parse("DROP GRAPH people").unwrap(),
            Statement::Graph(GraphStmt::Drop("people".into()))
        );
    }

    #[test]
    fn parses_program_use_graph_prefix() {
        let program = super::parse_program("USE g; MATCH (n:P) RETURN n").unwrap();

        assert_eq!(
            program,
            Program {
                use_graph: Some("g".into()),
                statements: vec![Statement::Query(Box::new(QueryStmt {
                    first: body(
                        vec![match_clause(vec![gql_path(
                            gql_node(Some("n"), all(&["P"])),
                            vec![],
                        )])],
                        ret(vec![(var("n"), None)]),
                    ),
                    unions: vec![],
                }))],
            }
        );
    }

    #[test]
    fn parses_program_semicolon_split() {
        let program =
            super::parse_program("CREATE GRAPH g; USE g; MATCH (n) RETURN n; DROP GRAPH g;")
                .unwrap();

        assert_eq!(
            program,
            Program {
                use_graph: Some("g".into()),
                statements: vec![
                    Statement::Graph(GraphStmt::Create("g".into())),
                    Statement::Query(Box::new(QueryStmt {
                        first: body(
                            vec![match_clause(vec![gql_path(
                                gql_node(Some("n"), all(&[])),
                                vec![],
                            )])],
                            ret(vec![(var("n"), None)]),
                        ),
                        unions: vec![],
                    })),
                    Statement::Graph(GraphStmt::Drop("g".into())),
                ],
            }
        );
    }

    #[test]
    fn for_temporal_vs_unwind_disambiguation() {
        let stmt = parse("FOR VALID_TIME ALL MATCH (n) RETURN n").unwrap();
        assert_eq!(
            stmt,
            Statement::Query(Box::new(QueryStmt {
                first: QueryBody {
                    temporal: TemporalClauses {
                        valid: Some(TemporalDimension::all()),
                        system: None,
                    },
                    clauses: vec![match_clause(vec![gql_path(
                        gql_node(Some("n"), all(&[])),
                        vec![]
                    )])],
                    ret: ret(vec![(var("n"), None)]),
                },
                unions: vec![],
            }))
        );

        let stmt = parse("MATCH (n) FOR VALID_TIME ALL RETURN n").unwrap();
        assert_eq!(
            stmt,
            Statement::Query(Box::new(QueryStmt {
                first: body(
                    vec![Clause::Match {
                        optional: false,
                        paths: vec![gql_path(gql_node(Some("n"), all(&[])), vec![])],
                        temporal: TemporalClauses {
                            valid: Some(TemporalDimension::all()),
                            system: None,
                        },
                        where_clause: None,
                    }],
                    ret(vec![(var("n"), None)]),
                ),
                unions: vec![],
            }))
        );

        let stmt = parse("MATCH (n) FOR item IN [1, 2] RETURN item").unwrap();
        assert_eq!(
            stmt,
            Statement::Query(Box::new(QueryStmt {
                first: body(
                    vec![
                        match_clause(vec![gql_path(gql_node(Some("n"), all(&[])), vec![])]),
                        Clause::For {
                            var: "item".into(),
                            list: Expr::List(vec![
                                Expr::Literal(Literal::Int(1)),
                                Expr::Literal(Literal::Int(2)),
                            ]),
                        },
                    ],
                    ret(vec![(var("item"), None)]),
                ),
                unions: vec![],
            }))
        );
    }
}
