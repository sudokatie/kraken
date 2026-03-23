//! Physical plan.

use crate::sql::ast::{Expr, OrderBy};
use super::logical::{JoinType, AggregateExpr, ProjectColumn};

/// Physical plan node.
#[derive(Debug, Clone)]
pub enum PhysicalPlan {
    /// Sequential table scan.
    SeqScan {
        table: String,
    },
    /// Index scan.
    IndexScan {
        table: String,
        index: String,
        key: Expr,
    },
    /// Filter operator.
    Filter {
        input: Box<PhysicalPlan>,
        predicate: Expr,
    },
    /// Projection operator.
    Project {
        input: Box<PhysicalPlan>,
        columns: Vec<ProjectColumn>,
    },
    /// Nested loop join.
    NestedLoopJoin {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        condition: Option<Expr>,
        join_type: JoinType,
    },
    /// Hash join.
    HashJoin {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        left_key: Expr,
        right_key: Expr,
        join_type: JoinType,
    },
    /// Hash aggregate.
    HashAggregate {
        input: Box<PhysicalPlan>,
        group_by: Vec<String>,
        aggregates: Vec<AggregateExpr>,
    },
    /// Sort operator.
    Sort {
        input: Box<PhysicalPlan>,
        order_by: Vec<OrderBy>,
    },
    /// Limit operator.
    Limit {
        input: Box<PhysicalPlan>,
        limit: i64,
    },
    /// Insert operator.
    Insert {
        table: String,
        columns: Option<Vec<String>>,
        values: Vec<Vec<Expr>>,
    },
    /// Update operator.
    Update {
        table: String,
        assignments: Vec<(String, Expr)>,
        filter: Option<Box<PhysicalPlan>>,
    },
    /// Delete operator.
    Delete {
        table: String,
        filter: Option<Box<PhysicalPlan>>,
    },
    /// Create table operator.
    CreateTable {
        name: String,
        columns: Vec<ColumnDef>,
    },
}

/// Column definition for physical plan.
#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: String,
    pub primary_key: bool,
    pub not_null: bool,
}

impl PhysicalPlan {
    /// Estimate the cost of this plan (simple heuristic).
    pub fn estimated_cost(&self) -> f64 {
        match self {
            PhysicalPlan::SeqScan { .. } => 100.0,
            PhysicalPlan::IndexScan { .. } => 10.0,
            PhysicalPlan::Filter { input, .. } => input.estimated_cost() * 0.5,
            PhysicalPlan::Project { input, .. } => input.estimated_cost() * 1.0,
            PhysicalPlan::NestedLoopJoin { left, right, .. } => {
                left.estimated_cost() * right.estimated_cost()
            }
            PhysicalPlan::HashJoin { left, right, .. } => {
                left.estimated_cost() + right.estimated_cost() * 2.0
            }
            PhysicalPlan::HashAggregate { input, .. } => input.estimated_cost() * 1.5,
            PhysicalPlan::Sort { input, .. } => {
                let n = input.estimated_cost();
                n * n.log2().max(1.0)
            }
            PhysicalPlan::Limit { input, limit } => {
                input.estimated_cost().min(*limit as f64)
            }
            PhysicalPlan::Insert { values, .. } => values.len() as f64,
            PhysicalPlan::Update { .. } => 50.0,
            PhysicalPlan::Delete { .. } => 50.0,
            PhysicalPlan::CreateTable { .. } => 1.0,
        }
    }

    /// Get the output columns of this plan.
    pub fn output_columns(&self) -> Vec<String> {
        match self {
            PhysicalPlan::SeqScan { table } => vec![format!("{}.*", table)],
            PhysicalPlan::IndexScan { table, .. } => vec![format!("{}.*", table)],
            PhysicalPlan::Filter { input, .. } => input.output_columns(),
            PhysicalPlan::Project { columns, .. } => {
                columns.iter()
                    .map(|c| c.alias.clone().unwrap_or_else(|| format!("{:?}", c.expr)))
                    .collect()
            }
            PhysicalPlan::NestedLoopJoin { left, right, .. } |
            PhysicalPlan::HashJoin { left, right, .. } => {
                let mut cols = left.output_columns();
                cols.extend(right.output_columns());
                cols
            }
            PhysicalPlan::HashAggregate { group_by, aggregates, .. } => {
                let mut cols: Vec<String> = group_by.clone();
                cols.extend(aggregates.iter().map(|a| a.alias.clone()));
                cols
            }
            PhysicalPlan::Sort { input, .. } => input.output_columns(),
            PhysicalPlan::Limit { input, .. } => input.output_columns(),
            PhysicalPlan::Insert { .. } => vec!["rows_affected".into()],
            PhysicalPlan::Update { .. } => vec!["rows_affected".into()],
            PhysicalPlan::Delete { .. } => vec!["rows_affected".into()],
            PhysicalPlan::CreateTable { name, .. } => vec![format!("created:{}", name)],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_seq_scan_cost() {
        let plan = PhysicalPlan::SeqScan { table: "users".into() };
        assert_eq!(plan.estimated_cost(), 100.0);
    }

    #[test]
    fn test_index_scan_cost() {
        use crate::sql::ast::Literal;
        let plan = PhysicalPlan::IndexScan {
            table: "users".into(),
            index: "pk_users".into(),
            key: Expr::Literal(Literal::Integer(1)),
        };
        assert_eq!(plan.estimated_cost(), 10.0);
    }

    #[test]
    fn test_join_cost() {
        let left = PhysicalPlan::SeqScan { table: "users".into() };
        let right = PhysicalPlan::SeqScan { table: "orders".into() };

        let nl_join = PhysicalPlan::NestedLoopJoin {
            left: Box::new(left.clone()),
            right: Box::new(right.clone()),
            condition: None,
            join_type: JoinType::Inner,
        };

        let hash_join = PhysicalPlan::HashJoin {
            left: Box::new(left),
            right: Box::new(right),
            left_key: Expr::Column("id".into()),
            right_key: Expr::Column("user_id".into()),
            join_type: JoinType::Inner,
        };

        // Hash join should be cheaper than nested loop for large tables
        assert!(hash_join.estimated_cost() < nl_join.estimated_cost());
    }

    #[test]
    fn test_output_columns() {
        let plan = PhysicalPlan::SeqScan { table: "users".into() };
        let cols = plan.output_columns();
        assert_eq!(cols, vec!["users.*"]);
    }
}
