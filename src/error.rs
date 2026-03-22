//! Error types for Kraken.

use thiserror::Error;

/// Result type for Kraken operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Kraken error types.
#[derive(Debug, Error)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Page not found: {0}")]
    PageNotFound(u32),

    #[error("Page full")]
    PageFull,

    #[error("Buffer pool full")]
    BufferPoolFull,

    #[error("Invalid page format")]
    InvalidPage,

    #[error("Checksum mismatch")]
    ChecksumMismatch,

    #[error("SQL syntax error: {0}")]
    SyntaxError(String),

    #[error("Unknown table: {0}")]
    UnknownTable(String),

    #[error("Table already exists: {0}")]
    TableExists(String),

    #[error("Table not found: {0}")]
    TableNotFound(String),

    #[error("Unknown column: {0}")]
    UnknownColumn(String),

    #[error("Type mismatch: expected {expected}, got {actual}")]
    TypeMismatch { expected: String, actual: String },

    #[error("Transaction error: {0}")]
    TransactionError(String),

    #[error("Raft error: {0}")]
    RaftError(String),

    #[error("Network error: {0}")]
    NetworkError(String),

    #[error("Internal error: {0}")]
    Internal(String),
}
