//! SQL parser.

use super::ast::*;
use super::lexer::{Lexer, Token, LexerError};

/// Parser error.
#[derive(Debug, Clone, PartialEq)]
pub struct ParseError {
    pub message: String,
}

impl ParseError {
    fn new(message: impl Into<String>) -> Self {
        Self { message: message.into() }
    }
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for ParseError {}

impl From<LexerError> for ParseError {
    fn from(e: LexerError) -> Self {
        ParseError::new(e.message)
    }
}

/// SQL parser.
pub struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    /// Create a new parser from SQL input.
    pub fn new(input: &str) -> Result<Self, ParseError> {
        let mut lexer = Lexer::new(input);
        let tokens = lexer.tokenize()?;
        Ok(Self { tokens, pos: 0 })
    }

    /// Current token.
    fn current(&self) -> &Token {
        self.tokens.get(self.pos).unwrap_or(&Token::Eof)
    }

    /// Peek at next token.
    fn peek(&self) -> &Token {
        self.tokens.get(self.pos + 1).unwrap_or(&Token::Eof)
    }

    /// Advance to next token.
    fn advance(&mut self) -> &Token {
        if self.pos < self.tokens.len() {
            self.pos += 1;
        }
        self.tokens.get(self.pos - 1).unwrap_or(&Token::Eof)
    }

    /// Check if current token matches expected.
    fn check(&self, expected: &Token) -> bool {
        std::mem::discriminant(self.current()) == std::mem::discriminant(expected)
    }

    /// Consume token if it matches, error otherwise.
    fn expect(&mut self, expected: Token) -> Result<(), ParseError> {
        if self.check(&expected) {
            self.advance();
            Ok(())
        } else {
            Err(ParseError::new(format!("expected {:?}, got {:?}", expected, self.current())))
        }
    }

    /// Parse a statement.
    pub fn parse(&mut self) -> Result<Statement, ParseError> {
        let stmt = match self.current() {
            Token::Select => self.parse_select(),
            Token::Insert => self.parse_insert(),
            Token::Update => self.parse_update(),
            Token::Delete => self.parse_delete(),
            Token::Create => self.parse_create(),
            _ => Err(ParseError::new(format!("unexpected token {:?}", self.current()))),
        }?;

        // Optional semicolon
        if self.check(&Token::Semicolon) {
            self.advance();
        }

        Ok(stmt)
    }

    /// Parse SELECT statement.
    fn parse_select(&mut self) -> Result<Statement, ParseError> {
        self.expect(Token::Select)?;

        // Parse columns
        let columns = self.parse_select_columns()?;

        // Parse FROM
        self.expect(Token::From)?;
        let from = self.parse_table_ref()?;

        // Parse optional WHERE
        let where_clause = if self.check(&Token::Where) {
            self.advance();
            Some(self.parse_expr()?)
        } else {
            None
        };

        // Parse optional GROUP BY
        let group_by = if self.check(&Token::GroupBy) {
            self.advance();
            // Consume BY if present
            if let Token::Ident(s) = self.current() {
                if s.to_uppercase() == "BY" {
                    self.advance();
                }
            }
            self.parse_ident_list()?
        } else {
            vec![]
        };

        // Parse optional HAVING
        let having = if self.check(&Token::Having) {
            self.advance();
            Some(self.parse_expr()?)
        } else {
            None
        };

        // Parse optional ORDER BY
        let order_by = if self.check(&Token::OrderBy) {
            self.advance();
            // Consume BY if present
            if let Token::Ident(s) = self.current() {
                if s.to_uppercase() == "BY" {
                    self.advance();
                }
            }
            self.parse_order_by_list()?
        } else {
            vec![]
        };

        // Parse optional LIMIT
        let limit = if self.check(&Token::Limit) {
            self.advance();
            match self.current() {
                Token::IntLit(n) => {
                    let n = *n;
                    self.advance();
                    Some(n)
                }
                _ => return Err(ParseError::new("expected integer after LIMIT")),
            }
        } else {
            None
        };

        Ok(Statement::Select(SelectStatement {
            ctes: vec![],
            columns,
            from,
            joins: vec![],
            where_clause,
            order_by,
            limit,
            group_by,
            having,
        }))
    }

    /// Parse select columns.
    fn parse_select_columns(&mut self) -> Result<Vec<SelectColumn>, ParseError> {
        let mut columns = vec![];

        loop {
            if self.check(&Token::Star) {
                self.advance();
                columns.push(SelectColumn::Star);
            } else {
                let expr = self.parse_expr()?;
                let alias = if self.check(&Token::As) {
                    self.advance();
                    Some(self.parse_ident()?)
                } else {
                    None
                };
                columns.push(SelectColumn::Expr { expr, alias });
            }

            if self.check(&Token::Comma) {
                self.advance();
            } else {
                break;
            }
        }

        Ok(columns)
    }

    /// Parse table reference.
    fn parse_table_ref(&mut self) -> Result<TableRef, ParseError> {
        let name = self.parse_ident()?;
        let alias = if self.check(&Token::As) {
            self.advance();
            Some(self.parse_ident()?)
        } else if let Token::Ident(_) = self.current() {
            // Implicit alias
            if !self.check(&Token::Where) && !self.check(&Token::OrderBy) && 
               !self.check(&Token::GroupBy) && !self.check(&Token::Limit) {
                Some(self.parse_ident()?)
            } else {
                None
            }
        } else {
            None
        };

        Ok(TableRef { name, alias })
    }

    /// Parse ORDER BY list.
    fn parse_order_by_list(&mut self) -> Result<Vec<OrderBy>, ParseError> {
        let mut list = vec![];

        loop {
            let column = self.parse_ident()?;
            let descending = if self.check(&Token::Desc) {
                self.advance();
                true
            } else if self.check(&Token::Asc) {
                self.advance();
                false
            } else {
                false
            };

            list.push(OrderBy { column, descending });

            if self.check(&Token::Comma) {
                self.advance();
            } else {
                break;
            }
        }

        Ok(list)
    }

    /// Parse INSERT statement.
    fn parse_insert(&mut self) -> Result<Statement, ParseError> {
        self.expect(Token::Insert)?;
        self.expect(Token::Into)?;

        let table = self.parse_ident()?;

        // Optional column list
        let columns = if self.check(&Token::LParen) {
            self.advance();
            let cols = self.parse_ident_list()?;
            self.expect(Token::RParen)?;
            Some(cols)
        } else {
            None
        };

        self.expect(Token::Values)?;

        // Parse value lists
        let mut values = vec![];
        loop {
            self.expect(Token::LParen)?;
            let row = self.parse_expr_list()?;
            self.expect(Token::RParen)?;
            values.push(row);

            if self.check(&Token::Comma) {
                self.advance();
            } else {
                break;
            }
        }

        Ok(Statement::Insert(InsertStatement { table, columns, values }))
    }

    /// Parse UPDATE statement.
    fn parse_update(&mut self) -> Result<Statement, ParseError> {
        self.expect(Token::Update)?;
        let table = self.parse_ident()?;
        self.expect(Token::Set)?;

        let assignments = self.parse_assignments()?;

        let where_clause = if self.check(&Token::Where) {
            self.advance();
            Some(self.parse_expr()?)
        } else {
            None
        };

        Ok(Statement::Update(UpdateStatement { table, assignments, where_clause }))
    }

    /// Parse SET assignments.
    fn parse_assignments(&mut self) -> Result<Vec<Assignment>, ParseError> {
        let mut assignments = vec![];

        loop {
            let column = self.parse_ident()?;
            self.expect(Token::Eq)?;
            let value = self.parse_expr()?;
            assignments.push(Assignment { column, value });

            if self.check(&Token::Comma) {
                self.advance();
            } else {
                break;
            }
        }

        Ok(assignments)
    }

    /// Parse DELETE statement.
    fn parse_delete(&mut self) -> Result<Statement, ParseError> {
        self.expect(Token::Delete)?;
        self.expect(Token::From)?;
        let table = self.parse_ident()?;

        let where_clause = if self.check(&Token::Where) {
            self.advance();
            Some(self.parse_expr()?)
        } else {
            None
        };

        Ok(Statement::Delete(DeleteStatement { table, where_clause }))
    }

    /// Parse CREATE TABLE statement.
    fn parse_create(&mut self) -> Result<Statement, ParseError> {
        self.expect(Token::Create)?;
        self.expect(Token::Table)?;
        let name = self.parse_ident()?;

        self.expect(Token::LParen)?;
        let columns = self.parse_column_defs()?;
        self.expect(Token::RParen)?;

        Ok(Statement::CreateTable(CreateTableStatement { name, columns }))
    }

    /// Parse column definitions.
    fn parse_column_defs(&mut self) -> Result<Vec<ColumnDef>, ParseError> {
        let mut columns = vec![];

        loop {
            let col = self.parse_column_def()?;
            columns.push(col);

            if self.check(&Token::Comma) {
                self.advance();
            } else {
                break;
            }
        }

        Ok(columns)
    }

    /// Parse single column definition.
    fn parse_column_def(&mut self) -> Result<ColumnDef, ParseError> {
        let name = self.parse_ident()?;
        let data_type = self.parse_data_type()?;

        let mut primary_key = false;
        let mut not_null = false;

        // Parse constraints
        loop {
            if self.check(&Token::Primary) {
                self.advance();
                self.expect(Token::Key)?;
                primary_key = true;
            } else if self.check(&Token::Not) {
                self.advance();
                self.expect(Token::Null)?;
                not_null = true;
            } else {
                break;
            }
        }

        Ok(ColumnDef { name, data_type, primary_key, not_null })
    }

    /// Parse data type.
    fn parse_data_type(&mut self) -> Result<DataType, ParseError> {
        let dt = match self.current() {
            Token::Integer => DataType::Integer,
            Token::Real => DataType::Real,
            Token::Text => DataType::Text,
            Token::Boolean => DataType::Boolean,
            Token::Blob => DataType::Blob,
            _ => return Err(ParseError::new(format!("expected data type, got {:?}", self.current()))),
        };
        self.advance();
        Ok(dt)
    }

    /// Parse identifier.
    fn parse_ident(&mut self) -> Result<String, ParseError> {
        match self.current().clone() {
            Token::Ident(s) => {
                self.advance();
                Ok(s)
            }
            _ => Err(ParseError::new(format!("expected identifier, got {:?}", self.current()))),
        }
    }

    /// Parse identifier list.
    fn parse_ident_list(&mut self) -> Result<Vec<String>, ParseError> {
        let mut list = vec![];

        loop {
            list.push(self.parse_ident()?);

            if self.check(&Token::Comma) {
                self.advance();
            } else {
                break;
            }
        }

        Ok(list)
    }

    /// Parse expression list.
    fn parse_expr_list(&mut self) -> Result<Vec<Expr>, ParseError> {
        let mut list = vec![];

        loop {
            list.push(self.parse_expr()?);

            if self.check(&Token::Comma) {
                self.advance();
            } else {
                break;
            }
        }

        Ok(list)
    }

    /// Parse expression (with precedence).
    fn parse_expr(&mut self) -> Result<Expr, ParseError> {
        self.parse_or_expr()
    }

    /// Parse OR expression.
    fn parse_or_expr(&mut self) -> Result<Expr, ParseError> {
        let mut left = self.parse_and_expr()?;

        while self.check(&Token::Or) {
            self.advance();
            let right = self.parse_and_expr()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op: BinaryOp::Or,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    /// Parse AND expression.
    fn parse_and_expr(&mut self) -> Result<Expr, ParseError> {
        let mut left = self.parse_not_expr()?;

        while self.check(&Token::And) {
            self.advance();
            let right = self.parse_not_expr()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op: BinaryOp::And,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    /// Parse NOT expression.
    fn parse_not_expr(&mut self) -> Result<Expr, ParseError> {
        if self.check(&Token::Not) {
            self.advance();
            let expr = self.parse_not_expr()?;
            Ok(Expr::UnaryOp {
                op: UnaryOp::Not,
                expr: Box::new(expr),
            })
        } else {
            self.parse_comparison()
        }
    }

    /// Parse comparison expression.
    fn parse_comparison(&mut self) -> Result<Expr, ParseError> {
        let left = self.parse_additive()?;

        let op = match self.current() {
            Token::Eq => Some(BinaryOp::Eq),
            Token::Ne => Some(BinaryOp::Ne),
            Token::Lt => Some(BinaryOp::Lt),
            Token::Gt => Some(BinaryOp::Gt),
            Token::Le => Some(BinaryOp::Le),
            Token::Ge => Some(BinaryOp::Ge),
            _ => None,
        };

        if let Some(op) = op {
            self.advance();
            let right = self.parse_additive()?;
            Ok(Expr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            })
        } else {
            Ok(left)
        }
    }

    /// Parse additive expression.
    fn parse_additive(&mut self) -> Result<Expr, ParseError> {
        let mut left = self.parse_multiplicative()?;

        loop {
            let op = match self.current() {
                Token::Plus => Some(BinaryOp::Add),
                Token::Minus => Some(BinaryOp::Sub),
                _ => None,
            };

            if let Some(op) = op {
                self.advance();
                let right = self.parse_multiplicative()?;
                left = Expr::BinaryOp {
                    left: Box::new(left),
                    op,
                    right: Box::new(right),
                };
            } else {
                break;
            }
        }

        Ok(left)
    }

    /// Parse multiplicative expression.
    fn parse_multiplicative(&mut self) -> Result<Expr, ParseError> {
        let mut left = self.parse_unary()?;

        loop {
            let op = match self.current() {
                Token::Star => Some(BinaryOp::Mul),
                Token::Slash => Some(BinaryOp::Div),
                _ => None,
            };

            if let Some(op) = op {
                self.advance();
                let right = self.parse_unary()?;
                left = Expr::BinaryOp {
                    left: Box::new(left),
                    op,
                    right: Box::new(right),
                };
            } else {
                break;
            }
        }

        Ok(left)
    }

    /// Parse unary expression.
    fn parse_unary(&mut self) -> Result<Expr, ParseError> {
        if self.check(&Token::Minus) {
            self.advance();
            let expr = self.parse_unary()?;
            Ok(Expr::UnaryOp {
                op: UnaryOp::Neg,
                expr: Box::new(expr),
            })
        } else {
            self.parse_primary()
        }
    }

    /// Parse primary expression.
    fn parse_primary(&mut self) -> Result<Expr, ParseError> {
        match self.current().clone() {
            Token::IntLit(n) => {
                self.advance();
                Ok(Expr::Literal(Literal::Integer(n)))
            }
            Token::FloatLit(n) => {
                self.advance();
                Ok(Expr::Literal(Literal::Float(n)))
            }
            Token::StringLit(s) => {
                self.advance();
                Ok(Expr::Literal(Literal::String(s)))
            }
            Token::True => {
                self.advance();
                Ok(Expr::Literal(Literal::Boolean(true)))
            }
            Token::False => {
                self.advance();
                Ok(Expr::Literal(Literal::Boolean(false)))
            }
            Token::Null => {
                self.advance();
                Ok(Expr::Literal(Literal::Null))
            }
            Token::LParen => {
                self.advance();
                let expr = self.parse_expr()?;
                self.expect(Token::RParen)?;
                Ok(expr)
            }
            Token::Count | Token::Sum | Token::Avg | Token::Min | Token::Max => {
                let name = match self.current() {
                    Token::Count => "COUNT",
                    Token::Sum => "SUM",
                    Token::Avg => "AVG",
                    Token::Min => "MIN",
                    Token::Max => "MAX",
                    _ => unreachable!(),
                }.to_string();
                self.advance();
                self.expect(Token::LParen)?;
                let args = if self.check(&Token::Star) {
                    self.advance();
                    vec![] // COUNT(*) has no args
                } else {
                    self.parse_expr_list()?
                };
                self.expect(Token::RParen)?;
                Ok(Expr::Function { name, args })
            }
            Token::Ident(name) => {
                self.advance();
                // Check for function call
                if self.check(&Token::LParen) {
                    self.advance();
                    let args = if self.check(&Token::RParen) {
                        vec![]
                    } else {
                        self.parse_expr_list()?
                    };
                    self.expect(Token::RParen)?;
                    Ok(Expr::Function { name, args })
                } else if self.check(&Token::Dot) {
                    // Qualified column
                    self.advance();
                    let column = self.parse_ident()?;
                    Ok(Expr::QualifiedColumn { table: name, column })
                } else {
                    Ok(Expr::Column(name))
                }
            }
            _ => Err(ParseError::new(format!("unexpected token in expression: {:?}", self.current()))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_select() {
        let mut parser = Parser::new("SELECT * FROM users").unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.columns.len(), 1);
                assert_eq!(s.from.name, "users");
                assert!(s.where_clause.is_none());
            }
            _ => panic!("expected SELECT"),
        }
    }

    #[test]
    fn test_parse_select_with_columns() {
        let mut parser = Parser::new("SELECT id, name FROM users").unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.columns.len(), 2);
            }
            _ => panic!("expected SELECT"),
        }
    }

    #[test]
    fn test_parse_select_with_where() {
        let mut parser = Parser::new("SELECT * FROM users WHERE id = 42").unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::Select(s) => {
                assert!(s.where_clause.is_some());
            }
            _ => panic!("expected SELECT"),
        }
    }

    #[test]
    fn test_parse_insert() {
        let mut parser = Parser::new("INSERT INTO users VALUES (1, 'alice')").unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::Insert(i) => {
                assert_eq!(i.table, "users");
                assert_eq!(i.values.len(), 1);
                assert_eq!(i.values[0].len(), 2);
            }
            _ => panic!("expected INSERT"),
        }
    }

    #[test]
    fn test_parse_insert_with_columns() {
        let mut parser = Parser::new("INSERT INTO users (id, name) VALUES (1, 'alice')").unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::Insert(i) => {
                assert!(i.columns.is_some());
                assert_eq!(i.columns.unwrap().len(), 2);
            }
            _ => panic!("expected INSERT"),
        }
    }

    #[test]
    fn test_parse_update() {
        let mut parser = Parser::new("UPDATE users SET name = 'bob' WHERE id = 1").unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::Update(u) => {
                assert_eq!(u.table, "users");
                assert_eq!(u.assignments.len(), 1);
                assert!(u.where_clause.is_some());
            }
            _ => panic!("expected UPDATE"),
        }
    }

    #[test]
    fn test_parse_delete() {
        let mut parser = Parser::new("DELETE FROM users WHERE id = 1").unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::Delete(d) => {
                assert_eq!(d.table, "users");
                assert!(d.where_clause.is_some());
            }
            _ => panic!("expected DELETE"),
        }
    }

    #[test]
    fn test_parse_create_table() {
        let mut parser = Parser::new(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)"
        ).unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::CreateTable(c) => {
                assert_eq!(c.name, "users");
                assert_eq!(c.columns.len(), 2);
                assert!(c.columns[0].primary_key);
                assert!(c.columns[1].not_null);
            }
            _ => panic!("expected CREATE TABLE"),
        }
    }

    #[test]
    fn test_parse_select_with_order_limit() {
        let mut parser = Parser::new("SELECT * FROM users ORDER BY name DESC LIMIT 10").unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.order_by.len(), 1);
                assert!(s.order_by[0].descending);
                assert_eq!(s.limit, Some(10));
            }
            _ => panic!("expected SELECT"),
        }
    }

    #[test]
    fn test_parse_complex_where() {
        let mut parser = Parser::new(
            "SELECT * FROM users WHERE age > 18 AND (name = 'alice' OR name = 'bob')"
        ).unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::Select(s) => {
                assert!(s.where_clause.is_some());
            }
            _ => panic!("expected SELECT"),
        }
    }

    #[test]
    fn test_parse_aggregate() {
        let mut parser = Parser::new("SELECT COUNT(*), SUM(amount) FROM orders").unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.columns.len(), 2);
            }
            _ => panic!("expected SELECT"),
        }
    }
}
