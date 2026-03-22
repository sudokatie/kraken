//! SQL lexer.

use std::iter::Peekable;
use std::str::Chars;

/// SQL token.
#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // Keywords
    Select,
    From,
    Where,
    Insert,
    Into,
    Values,
    Update,
    Set,
    Delete,
    Create,
    Table,
    And,
    Or,
    Not,
    Null,
    True,
    False,
    Primary,
    Key,
    Integer,
    Text,
    Real,
    Boolean,
    Blob,
    OrderBy,
    Asc,
    Desc,
    Limit,
    GroupBy,
    Having,
    As,
    Join,
    On,
    Left,
    Right,
    Inner,
    Outer,
    Count,
    Sum,
    Avg,
    Min,
    Max,

    // Identifiers and literals
    Ident(String),
    IntLit(i64),
    FloatLit(f64),
    StringLit(String),

    // Operators
    Eq,      // =
    Ne,      // <> or !=
    Lt,      // <
    Gt,      // >
    Le,      // <=
    Ge,      // >=
    Plus,    // +
    Minus,   // -
    Star,    // *
    Slash,   // /

    // Punctuation
    LParen,    // (
    RParen,    // )
    Comma,     // ,
    Semicolon, // ;
    Dot,       // .

    // End
    Eof,
}

/// Lexer error.
#[derive(Debug, Clone, PartialEq)]
pub struct LexerError {
    pub message: String,
    pub position: usize,
}

impl LexerError {
    fn new(message: impl Into<String>, position: usize) -> Self {
        Self {
            message: message.into(),
            position,
        }
    }
}

/// SQL lexer.
pub struct Lexer<'a> {
    input: Peekable<Chars<'a>>,
    pos: usize,
}

impl<'a> Lexer<'a> {
    /// Create a new lexer.
    pub fn new(input: &'a str) -> Self {
        Self {
            input: input.chars().peekable(),
            pos: 0,
        }
    }

    /// Advance to next character.
    fn advance(&mut self) -> Option<char> {
        let c = self.input.next();
        if c.is_some() {
            self.pos += 1;
        }
        c
    }

    /// Peek at current character.
    fn peek(&mut self) -> Option<&char> {
        self.input.peek()
    }

    /// Skip whitespace.
    fn skip_whitespace(&mut self) {
        while let Some(&c) = self.peek() {
            if c.is_whitespace() {
                self.advance();
            } else {
                break;
            }
        }
    }

    /// Read an identifier or keyword.
    fn read_ident(&mut self, first: char) -> Token {
        let mut s = String::new();
        s.push(first);

        while let Some(&c) = self.peek() {
            if c.is_alphanumeric() || c == '_' {
                s.push(c);
                self.advance();
            } else {
                break;
            }
        }

        // Check for keywords (case-insensitive)
        match s.to_uppercase().as_str() {
            "SELECT" => Token::Select,
            "FROM" => Token::From,
            "WHERE" => Token::Where,
            "INSERT" => Token::Insert,
            "INTO" => Token::Into,
            "VALUES" => Token::Values,
            "UPDATE" => Token::Update,
            "SET" => Token::Set,
            "DELETE" => Token::Delete,
            "CREATE" => Token::Create,
            "TABLE" => Token::Table,
            "AND" => Token::And,
            "OR" => Token::Or,
            "NOT" => Token::Not,
            "NULL" => Token::Null,
            "TRUE" => Token::True,
            "FALSE" => Token::False,
            "PRIMARY" => Token::Primary,
            "KEY" => Token::Key,
            "INTEGER" | "INT" => Token::Integer,
            "TEXT" | "VARCHAR" => Token::Text,
            "REAL" | "FLOAT" | "DOUBLE" => Token::Real,
            "BOOLEAN" | "BOOL" => Token::Boolean,
            "BLOB" => Token::Blob,
            "ORDER" => Token::OrderBy, // Will need to handle "ORDER BY" specially
            "ASC" => Token::Asc,
            "DESC" => Token::Desc,
            "LIMIT" => Token::Limit,
            "GROUP" => Token::GroupBy, // Will need to handle "GROUP BY" specially
            "HAVING" => Token::Having,
            "AS" => Token::As,
            "JOIN" => Token::Join,
            "ON" => Token::On,
            "LEFT" => Token::Left,
            "RIGHT" => Token::Right,
            "INNER" => Token::Inner,
            "OUTER" => Token::Outer,
            "COUNT" => Token::Count,
            "SUM" => Token::Sum,
            "AVG" => Token::Avg,
            "MIN" => Token::Min,
            "MAX" => Token::Max,
            "BY" => Token::Ident("BY".into()), // Part of ORDER BY / GROUP BY
            _ => Token::Ident(s),
        }
    }

    /// Read a number.
    fn read_number(&mut self, first: char) -> Result<Token, LexerError> {
        let mut s = String::new();
        s.push(first);
        let mut has_dot = false;

        while let Some(&c) = self.peek() {
            if c.is_ascii_digit() {
                s.push(c);
                self.advance();
            } else if c == '.' && !has_dot {
                has_dot = true;
                s.push(c);
                self.advance();
            } else {
                break;
            }
        }

        if has_dot {
            s.parse::<f64>()
                .map(Token::FloatLit)
                .map_err(|_| LexerError::new("invalid float literal", self.pos))
        } else {
            s.parse::<i64>()
                .map(Token::IntLit)
                .map_err(|_| LexerError::new("invalid integer literal", self.pos))
        }
    }

    /// Read a string literal.
    fn read_string(&mut self, quote: char) -> Result<Token, LexerError> {
        let start_pos = self.pos;
        let mut s = String::new();

        loop {
            match self.advance() {
                Some(c) if c == quote => {
                    // Check for escaped quote
                    if self.peek() == Some(&quote) {
                        s.push(quote);
                        self.advance();
                    } else {
                        break;
                    }
                }
                Some(c) => s.push(c),
                None => return Err(LexerError::new("unterminated string", start_pos)),
            }
        }

        Ok(Token::StringLit(s))
    }

    /// Get next token.
    pub fn next_token(&mut self) -> Result<Token, LexerError> {
        self.skip_whitespace();

        let c = match self.advance() {
            Some(c) => c,
            None => return Ok(Token::Eof),
        };

        match c {
            // Single-character tokens
            '(' => Ok(Token::LParen),
            ')' => Ok(Token::RParen),
            ',' => Ok(Token::Comma),
            ';' => Ok(Token::Semicolon),
            '.' => Ok(Token::Dot),
            '+' => Ok(Token::Plus),
            '-' => Ok(Token::Minus),
            '*' => Ok(Token::Star),
            '/' => Ok(Token::Slash),

            // Two-character operators
            '=' => Ok(Token::Eq),
            '<' => {
                if self.peek() == Some(&'=') {
                    self.advance();
                    Ok(Token::Le)
                } else if self.peek() == Some(&'>') {
                    self.advance();
                    Ok(Token::Ne)
                } else {
                    Ok(Token::Lt)
                }
            }
            '>' => {
                if self.peek() == Some(&'=') {
                    self.advance();
                    Ok(Token::Ge)
                } else {
                    Ok(Token::Gt)
                }
            }
            '!' => {
                if self.peek() == Some(&'=') {
                    self.advance();
                    Ok(Token::Ne)
                } else {
                    Err(LexerError::new("unexpected character '!'", self.pos))
                }
            }

            // String literals
            '\'' | '"' => self.read_string(c),

            // Numbers
            c if c.is_ascii_digit() => self.read_number(c),

            // Identifiers and keywords
            c if c.is_alphabetic() || c == '_' => Ok(self.read_ident(c)),

            _ => Err(LexerError::new(format!("unexpected character '{}'", c), self.pos)),
        }
    }

    /// Tokenize entire input.
    pub fn tokenize(&mut self) -> Result<Vec<Token>, LexerError> {
        let mut tokens = Vec::new();

        loop {
            let token = self.next_token()?;
            if token == Token::Eof {
                tokens.push(token);
                break;
            }
            tokens.push(token);
        }

        Ok(tokens)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lexer_new() {
        let lexer = Lexer::new("SELECT * FROM users");
        assert_eq!(lexer.pos, 0);
    }

    #[test]
    fn test_simple_select() {
        let mut lexer = Lexer::new("SELECT * FROM users");
        let tokens = lexer.tokenize().unwrap();

        assert_eq!(tokens, vec![
            Token::Select,
            Token::Star,
            Token::From,
            Token::Ident("users".into()),
            Token::Eof,
        ]);
    }

    #[test]
    fn test_select_with_columns() {
        let mut lexer = Lexer::new("SELECT id, name FROM users");
        let tokens = lexer.tokenize().unwrap();

        assert_eq!(tokens, vec![
            Token::Select,
            Token::Ident("id".into()),
            Token::Comma,
            Token::Ident("name".into()),
            Token::From,
            Token::Ident("users".into()),
            Token::Eof,
        ]);
    }

    #[test]
    fn test_select_with_where() {
        let mut lexer = Lexer::new("SELECT * FROM users WHERE id = 42");
        let tokens = lexer.tokenize().unwrap();

        assert_eq!(tokens, vec![
            Token::Select,
            Token::Star,
            Token::From,
            Token::Ident("users".into()),
            Token::Where,
            Token::Ident("id".into()),
            Token::Eq,
            Token::IntLit(42),
            Token::Eof,
        ]);
    }

    #[test]
    fn test_insert_statement() {
        let mut lexer = Lexer::new("INSERT INTO users VALUES (1, 'alice')");
        let tokens = lexer.tokenize().unwrap();

        assert_eq!(tokens, vec![
            Token::Insert,
            Token::Into,
            Token::Ident("users".into()),
            Token::Values,
            Token::LParen,
            Token::IntLit(1),
            Token::Comma,
            Token::StringLit("alice".into()),
            Token::RParen,
            Token::Eof,
        ]);
    }

    #[test]
    fn test_create_table() {
        let mut lexer = Lexer::new("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");
        let tokens = lexer.tokenize().unwrap();

        assert_eq!(tokens, vec![
            Token::Create,
            Token::Table,
            Token::Ident("users".into()),
            Token::LParen,
            Token::Ident("id".into()),
            Token::Integer,
            Token::Primary,
            Token::Key,
            Token::Comma,
            Token::Ident("name".into()),
            Token::Text,
            Token::RParen,
            Token::Eof,
        ]);
    }

    #[test]
    fn test_comparison_operators() {
        let mut lexer = Lexer::new("a < b <= c > d >= e <> f != g");
        let tokens = lexer.tokenize().unwrap();

        assert_eq!(tokens, vec![
            Token::Ident("a".into()),
            Token::Lt,
            Token::Ident("b".into()),
            Token::Le,
            Token::Ident("c".into()),
            Token::Gt,
            Token::Ident("d".into()),
            Token::Ge,
            Token::Ident("e".into()),
            Token::Ne,
            Token::Ident("f".into()),
            Token::Ne,
            Token::Ident("g".into()),
            Token::Eof,
        ]);
    }

    #[test]
    fn test_float_literal() {
        let mut lexer = Lexer::new("SELECT 3.14, 2.718");
        let tokens = lexer.tokenize().unwrap();

        assert_eq!(tokens, vec![
            Token::Select,
            Token::FloatLit(3.14),
            Token::Comma,
            Token::FloatLit(2.718),
            Token::Eof,
        ]);
    }

    #[test]
    fn test_case_insensitive_keywords() {
        let mut lexer = Lexer::new("select FROM where");
        let tokens = lexer.tokenize().unwrap();

        assert_eq!(tokens, vec![
            Token::Select,
            Token::From,
            Token::Where,
            Token::Eof,
        ]);
    }

    #[test]
    fn test_escaped_string() {
        let mut lexer = Lexer::new("'it''s a test'");
        let tokens = lexer.tokenize().unwrap();

        assert_eq!(tokens, vec![
            Token::StringLit("it's a test".into()),
            Token::Eof,
        ]);
    }

    #[test]
    fn test_update_statement() {
        let mut lexer = Lexer::new("UPDATE users SET name = 'bob' WHERE id = 1");
        let tokens = lexer.tokenize().unwrap();

        assert_eq!(tokens, vec![
            Token::Update,
            Token::Ident("users".into()),
            Token::Set,
            Token::Ident("name".into()),
            Token::Eq,
            Token::StringLit("bob".into()),
            Token::Where,
            Token::Ident("id".into()),
            Token::Eq,
            Token::IntLit(1),
            Token::Eof,
        ]);
    }

    #[test]
    fn test_delete_statement() {
        let mut lexer = Lexer::new("DELETE FROM users WHERE id = 1");
        let tokens = lexer.tokenize().unwrap();

        assert_eq!(tokens, vec![
            Token::Delete,
            Token::From,
            Token::Ident("users".into()),
            Token::Where,
            Token::Ident("id".into()),
            Token::Eq,
            Token::IntLit(1),
            Token::Eof,
        ]);
    }

    #[test]
    fn test_aggregate_functions() {
        let mut lexer = Lexer::new("SELECT COUNT(*), SUM(x), AVG(y), MIN(z), MAX(w) FROM t");
        let tokens = lexer.tokenize().unwrap();

        assert_eq!(tokens, vec![
            Token::Select,
            Token::Count,
            Token::LParen,
            Token::Star,
            Token::RParen,
            Token::Comma,
            Token::Sum,
            Token::LParen,
            Token::Ident("x".into()),
            Token::RParen,
            Token::Comma,
            Token::Avg,
            Token::LParen,
            Token::Ident("y".into()),
            Token::RParen,
            Token::Comma,
            Token::Min,
            Token::LParen,
            Token::Ident("z".into()),
            Token::RParen,
            Token::Comma,
            Token::Max,
            Token::LParen,
            Token::Ident("w".into()),
            Token::RParen,
            Token::From,
            Token::Ident("t".into()),
            Token::Eof,
        ]);
    }
}
