//! Execution operators.
//!
//! Implements the Volcano/iterator model for query execution.

pub mod scan;
pub mod filter;
pub mod project;
pub mod join;
pub mod aggregate;

use crate::Result;
use crate::sql::types::Value;

/// A row of values.
pub type Row = Vec<Value>;

/// Operator trait (Volcano/iterator model).
///
/// Each operator produces a stream of rows via next().
pub trait Operator {
    /// Get the next row, or None if exhausted.
    fn next(&mut self) -> Result<Option<Row>>;

    /// Get column names.
    fn columns(&self) -> &[String];

    /// Reset operator to beginning.
    fn reset(&mut self) -> Result<()>;
}

/// Box type for operators.
pub type BoxedOperator = Box<dyn Operator>;

pub use scan::TableScan;
pub use filter::Filter;
pub use project::Project;
pub use join::NestedLoopJoin;
pub use aggregate::Aggregate;
