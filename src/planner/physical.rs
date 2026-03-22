//! Physical plan.

/// Physical plan node.
#[derive(Debug, Clone)]
pub enum PhysicalPlan {
    SeqScan { table: String },
    IndexScan { table: String, index: String },
    Filter { input: Box<PhysicalPlan> },
}
