//! Catalog management.

pub mod catalog;
pub mod schema;
pub mod statistics;

pub use catalog::Catalog;
pub use schema::{ColumnDef, ColumnType, TableSchema};
pub use statistics::{ColumnStatistics, StatisticsManager, TableStatistics};
