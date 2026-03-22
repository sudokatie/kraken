//! Table schema.

/// Table schema.
#[derive(Debug, Clone)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnSchema>,
}

/// Column schema.
#[derive(Debug, Clone)]
pub struct ColumnSchema {
    pub name: String,
    pub ordinal: usize,
}
