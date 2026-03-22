//! SQL lexer.

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

    // Identifiers and literals
    Ident(String),
    Integer(i64),
    Float(f64),
    String(String),

    // Operators
    Eq,
    Ne,
    Lt,
    Gt,
    Le,
    Ge,
    Plus,
    Minus,
    Star,
    Slash,

    // Punctuation
    LParen,
    RParen,
    Comma,
    Semicolon,

    // End
    Eof,
}

/// SQL lexer.
pub struct Lexer {
    input: Vec<char>,
    pos: usize,
}

impl Lexer {
    /// Create a new lexer.
    pub fn new(input: &str) -> Self {
        Self {
            input: input.chars().collect(),
            pos: 0,
        }
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
}
