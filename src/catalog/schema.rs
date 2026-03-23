//! Table schema definitions.

use crate::sql::ast::DataType;
use serde::{Deserialize, Serialize};

/// Table schema.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableSchema {
    /// Table name.
    pub name: String,
    /// Table ID (assigned by catalog).
    pub table_id: u32,
    /// Column definitions.
    pub columns: Vec<ColumnDef>,
    /// Heap file header page ID (for storage).
    #[serde(default)]
    pub heap_page_id: Option<u32>,
}

impl TableSchema {
    /// Create a new table schema.
    pub fn new(name: impl Into<String>, table_id: u32, columns: Vec<ColumnDef>) -> Self {
        Self {
            name: name.into(),
            table_id,
            columns,
            heap_page_id: None,
        }
    }
    
    /// Set the heap page ID.
    pub fn with_heap_page_id(mut self, page_id: u32) -> Self {
        self.heap_page_id = Some(page_id);
        self
    }

    /// Get column by name.
    pub fn column(&self, name: &str) -> Option<&ColumnDef> {
        self.columns.iter().find(|c| c.name == name)
    }

    /// Get column index by name.
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }

    /// Get primary key column.
    pub fn primary_key(&self) -> Option<&ColumnDef> {
        self.columns.iter().find(|c| c.primary_key)
    }

    /// Get primary key column index.
    pub fn primary_key_index(&self) -> Option<usize> {
        self.columns.iter().position(|c| c.primary_key)
    }

    /// Number of columns.
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }
}

/// Column definition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnDef {
    /// Column name.
    pub name: String,
    /// Column ordinal (position in table).
    pub ordinal: usize,
    /// Data type.
    pub data_type: ColumnType,
    /// Is this column nullable?
    pub nullable: bool,
    /// Is this the primary key?
    pub primary_key: bool,
}

impl ColumnDef {
    /// Create a new column definition.
    pub fn new(name: impl Into<String>, ordinal: usize, data_type: ColumnType) -> Self {
        Self {
            name: name.into(),
            ordinal,
            data_type,
            nullable: true,
            primary_key: false,
        }
    }

    /// Set nullable.
    pub fn nullable(mut self, nullable: bool) -> Self {
        self.nullable = nullable;
        self
    }

    /// Set primary key.
    pub fn primary_key(mut self, pk: bool) -> Self {
        self.primary_key = pk;
        if pk {
            self.nullable = false;
        }
        self
    }
}

/// Column data type (serializable version of ast::DataType).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ColumnType {
    Integer,
    Real,
    Text,
    Boolean,
    Blob,
}

impl From<DataType> for ColumnType {
    fn from(dt: DataType) -> Self {
        match dt {
            DataType::Integer => ColumnType::Integer,
            DataType::Real => ColumnType::Real,
            DataType::Text => ColumnType::Text,
            DataType::Boolean => ColumnType::Boolean,
            DataType::Blob => ColumnType::Blob,
        }
    }
}

impl From<ColumnType> for DataType {
    fn from(ct: ColumnType) -> Self {
        match ct {
            ColumnType::Integer => DataType::Integer,
            ColumnType::Real => DataType::Real,
            ColumnType::Text => DataType::Text,
            ColumnType::Boolean => DataType::Boolean,
            ColumnType::Blob => DataType::Blob,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_schema() {
        let cols = vec![
            ColumnDef::new("id", 0, ColumnType::Integer).primary_key(true),
            ColumnDef::new("name", 1, ColumnType::Text).nullable(false),
            ColumnDef::new("email", 2, ColumnType::Text),
        ];
        let schema = TableSchema::new("users", 1, cols);

        assert_eq!(schema.name, "users");
        assert_eq!(schema.column_count(), 3);
        assert!(schema.column("id").is_some());
        assert_eq!(schema.column_index("name"), Some(1));
        assert!(schema.primary_key().is_some());
        assert_eq!(schema.primary_key_index(), Some(0));
    }

    #[test]
    fn test_column_def() {
        let col = ColumnDef::new("age", 0, ColumnType::Integer)
            .nullable(false)
            .primary_key(false);

        assert_eq!(col.name, "age");
        assert!(!col.nullable);
        assert!(!col.primary_key);
    }

    #[test]
    fn test_primary_key_not_nullable() {
        let col = ColumnDef::new("id", 0, ColumnType::Integer).primary_key(true);
        assert!(!col.nullable);
    }
}
