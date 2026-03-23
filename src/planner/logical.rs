//! Logical plan.

use crate::sql::ast::{Expr, OrderBy};

/// Logical plan node.
#[derive(Debug, Clone)]
pub enum LogicalPlan {
    /// Table scan.
    Scan {
        table: String,
        alias: Option<String>,
    },
    /// Filter (WHERE clause).
    Filter {
        input: Box<LogicalPlan>,
        predicate: Expr,
    },
    /// Projection (SELECT columns).
    Project {
        input: Box<LogicalPlan>,
        columns: Vec<ProjectColumn>,
    },
    /// Join two tables.
    Join {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        condition: Option<Expr>,
        join_type: JoinType,
    },
    /// Aggregation (GROUP BY + aggregates).
    Aggregate {
        input: Box<LogicalPlan>,
        group_by: Vec<String>,
        aggregates: Vec<AggregateExpr>,
    },
    /// Sort (ORDER BY).
    Sort {
        input: Box<LogicalPlan>,
        order_by: Vec<OrderBy>,
    },
    /// Limit (LIMIT clause).
    Limit {
        input: Box<LogicalPlan>,
        limit: i64,
    },
    /// Insert rows.
    Insert {
        table: String,
        columns: Option<Vec<String>>,
        values: Vec<Vec<Expr>>,
    },
    /// Update rows.
    Update {
        table: String,
        assignments: Vec<(String, Expr)>,
        filter: Option<Expr>,
    },
    /// Delete rows.
    Delete {
        table: String,
        filter: Option<Expr>,
    },
    /// Create table.
    CreateTable {
        name: String,
        columns: Vec<ColumnSpec>,
    },
}

/// Join type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Cross,
}

/// Projected column.
#[derive(Debug, Clone)]
pub struct ProjectColumn {
    pub expr: Expr,
    pub alias: Option<String>,
}

/// Aggregate expression.
#[derive(Debug, Clone)]
pub struct AggregateExpr {
    pub func: AggregateFunc,
    pub column: Option<String>,
    pub alias: String,
}

/// Aggregate function type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

/// Column specification for CREATE TABLE.
#[derive(Debug, Clone)]
pub struct ColumnSpec {
    pub name: String,
    pub data_type: String,
    pub primary_key: bool,
    pub not_null: bool,
}

impl LogicalPlan {
    /// Create a simple table scan.
    pub fn scan(table: impl Into<String>) -> Self {
        LogicalPlan::Scan {
            table: table.into(),
            alias: None,
        }
    }

    /// Add a filter to this plan.
    pub fn filter(self, predicate: Expr) -> Self {
        LogicalPlan::Filter {
            input: Box::new(self),
            predicate,
        }
    }

    /// Add a projection to this plan.
    pub fn project(self, columns: Vec<ProjectColumn>) -> Self {
        LogicalPlan::Project {
            input: Box::new(self),
            columns,
        }
    }

    /// Add a sort to this plan.
    pub fn sort(self, order_by: Vec<OrderBy>) -> Self {
        LogicalPlan::Sort {
            input: Box::new(self),
            order_by,
        }
    }

    /// Add a limit to this plan.
    pub fn limit(self, limit: i64) -> Self {
        LogicalPlan::Limit {
            input: Box::new(self),
            limit,
        }
    }

    /// Join with another plan.
    pub fn join(self, right: LogicalPlan, condition: Option<Expr>, join_type: JoinType) -> Self {
        LogicalPlan::Join {
            left: Box::new(self),
            right: Box::new(right),
            condition,
            join_type,
        }
    }

    /// Add aggregation.
    pub fn aggregate(self, group_by: Vec<String>, aggregates: Vec<AggregateExpr>) -> Self {
        LogicalPlan::Aggregate {
            input: Box::new(self),
            group_by,
            aggregates,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::ast::{Literal, BinaryOp};

    #[test]
    fn test_scan() {
        let plan = LogicalPlan::scan("users");
        match plan {
            LogicalPlan::Scan { table, .. } => assert_eq!(table, "users"),
            _ => panic!("expected scan"),
        }
    }

    #[test]
    fn test_filter_chain() {
        let plan = LogicalPlan::scan("users")
            .filter(Expr::BinaryOp {
                left: Box::new(Expr::Column("id".into())),
                op: BinaryOp::Eq,
                right: Box::new(Expr::Literal(Literal::Integer(1))),
            });

        match plan {
            LogicalPlan::Filter { input, .. } => {
                match *input {
                    LogicalPlan::Scan { table, .. } => assert_eq!(table, "users"),
                    _ => panic!("expected scan"),
                }
            }
            _ => panic!("expected filter"),
        }
    }

    #[test]
    fn test_join() {
        let left = LogicalPlan::scan("users");
        let right = LogicalPlan::scan("orders");
        let plan = left.join(right, None, JoinType::Inner);

        match plan {
            LogicalPlan::Join { join_type, .. } => {
                assert_eq!(join_type, JoinType::Inner);
            }
            _ => panic!("expected join"),
        }
    }
}
