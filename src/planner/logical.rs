//! Logical plan.

/// Logical plan node.
#[derive(Debug, Clone)]
pub enum LogicalPlan {
    Scan { table: String },
    Filter { input: Box<LogicalPlan> },
    Project { input: Box<LogicalPlan> },
}
