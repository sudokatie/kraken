//! Catalog management.

pub mod schema;
pub mod catalog;

pub use schema::{TableSchema, ColumnDef, ColumnType};
pub use catalog::Catalog;
