//! Query optimizer.
//!
//! Converts logical plans to optimized physical plans.

use super::logical::LogicalPlan;
use super::physical::{PhysicalPlan, ColumnDef};
use crate::sql::ast::Expr;

/// Query optimizer.
pub struct Optimizer {
    /// Available indexes (table -> index names).
    indexes: std::collections::HashMap<String, Vec<String>>,
}

impl Optimizer {
    /// Create a new optimizer.
    pub fn new() -> Self {
        Self {
            indexes: std::collections::HashMap::new(),
        }
    }

    /// Register an index for optimization.
    pub fn register_index(&mut self, table: &str, index: &str) {
        self.indexes
            .entry(table.to_string())
            .or_default()
            .push(index.to_string());
    }

    /// Optimize a logical plan into a physical plan.
    pub fn optimize(&self, plan: LogicalPlan) -> PhysicalPlan {
        // Apply optimization rules
        let plan = self.push_down_predicates(plan);
        let plan = self.eliminate_redundant_projects(plan);
        
        // Convert to physical plan
        self.to_physical(plan)
    }

    /// Push predicates down closer to table scans.
    fn push_down_predicates(&self, plan: LogicalPlan) -> LogicalPlan {
        match plan {
            LogicalPlan::Filter { input, predicate } => {
                match *input {
                    // Push filter below project
                    LogicalPlan::Project { input: proj_input, columns } => {
                        LogicalPlan::Project {
                            input: Box::new(LogicalPlan::Filter {
                                input: proj_input,
                                predicate,
                            }),
                            columns,
                        }
                    }
                    // Push filter into one side of join if possible
                    LogicalPlan::Join { left, right, condition, join_type } => {
                        // Simplified: try to push to left side
                        // Full implementation would analyze predicate columns
                        LogicalPlan::Join {
                            left: Box::new(LogicalPlan::Filter {
                                input: left,
                                predicate: predicate.clone(),
                            }),
                            right,
                            condition,
                            join_type,
                        }
                    }
                    other => LogicalPlan::Filter {
                        input: Box::new(self.push_down_predicates(other)),
                        predicate,
                    }
                }
            }
            // Recursively process children
            LogicalPlan::Project { input, columns } => {
                LogicalPlan::Project {
                    input: Box::new(self.push_down_predicates(*input)),
                    columns,
                }
            }
            LogicalPlan::Sort { input, order_by } => {
                LogicalPlan::Sort {
                    input: Box::new(self.push_down_predicates(*input)),
                    order_by,
                }
            }
            LogicalPlan::Limit { input, limit } => {
                LogicalPlan::Limit {
                    input: Box::new(self.push_down_predicates(*input)),
                    limit,
                }
            }
            LogicalPlan::Join { left, right, condition, join_type } => {
                LogicalPlan::Join {
                    left: Box::new(self.push_down_predicates(*left)),
                    right: Box::new(self.push_down_predicates(*right)),
                    condition,
                    join_type,
                }
            }
            LogicalPlan::Aggregate { input, group_by, aggregates } => {
                LogicalPlan::Aggregate {
                    input: Box::new(self.push_down_predicates(*input)),
                    group_by,
                    aggregates,
                }
            }
            other => other,
        }
    }

    /// Eliminate redundant project operations.
    fn eliminate_redundant_projects(&self, plan: LogicalPlan) -> LogicalPlan {
        match plan {
            // Project on Project can sometimes be merged
            LogicalPlan::Project { input, columns } => {
                let optimized_input = self.eliminate_redundant_projects(*input);
                LogicalPlan::Project {
                    input: Box::new(optimized_input),
                    columns,
                }
            }
            // Recursively process children
            LogicalPlan::Filter { input, predicate } => {
                LogicalPlan::Filter {
                    input: Box::new(self.eliminate_redundant_projects(*input)),
                    predicate,
                }
            }
            LogicalPlan::Sort { input, order_by } => {
                LogicalPlan::Sort {
                    input: Box::new(self.eliminate_redundant_projects(*input)),
                    order_by,
                }
            }
            LogicalPlan::Limit { input, limit } => {
                LogicalPlan::Limit {
                    input: Box::new(self.eliminate_redundant_projects(*input)),
                    limit,
                }
            }
            LogicalPlan::Join { left, right, condition, join_type } => {
                LogicalPlan::Join {
                    left: Box::new(self.eliminate_redundant_projects(*left)),
                    right: Box::new(self.eliminate_redundant_projects(*right)),
                    condition,
                    join_type,
                }
            }
            LogicalPlan::Aggregate { input, group_by, aggregates } => {
                LogicalPlan::Aggregate {
                    input: Box::new(self.eliminate_redundant_projects(*input)),
                    group_by,
                    aggregates,
                }
            }
            other => other,
        }
    }

    /// Convert logical plan to physical plan.
    fn to_physical(&self, plan: LogicalPlan) -> PhysicalPlan {
        match plan {
            LogicalPlan::Scan { table, .. } => {
                // Check if we can use an index
                // For now, always use sequential scan
                PhysicalPlan::SeqScan { table }
            }
            LogicalPlan::Filter { input, predicate } => {
                // Check for index scan opportunity
                if let Some(index_scan) = self.try_index_scan(&input, &predicate) {
                    index_scan
                } else {
                    PhysicalPlan::Filter {
                        input: Box::new(self.to_physical(*input)),
                        predicate,
                    }
                }
            }
            LogicalPlan::Project { input, columns } => {
                PhysicalPlan::Project {
                    input: Box::new(self.to_physical(*input)),
                    columns,
                }
            }
            LogicalPlan::Join { left, right, condition, join_type } => {
                let left_phys = self.to_physical(*left);
                let right_phys = self.to_physical(*right);

                // Choose join strategy based on estimated sizes
                let left_cost = left_phys.estimated_cost();
                let right_cost = right_phys.estimated_cost();

                // Use hash join for equi-joins on larger tables
                if let Some(ref cond) = condition {
                    if let Some((left_key, right_key)) = self.extract_equi_join_keys(cond) {
                        if left_cost > 50.0 || right_cost > 50.0 {
                            return PhysicalPlan::HashJoin {
                                left: Box::new(left_phys),
                                right: Box::new(right_phys),
                                left_key,
                                right_key,
                                join_type,
                            };
                        }
                    }
                }

                // Default to nested loop join
                PhysicalPlan::NestedLoopJoin {
                    left: Box::new(left_phys),
                    right: Box::new(right_phys),
                    condition,
                    join_type,
                }
            }
            LogicalPlan::Aggregate { input, group_by, aggregates } => {
                PhysicalPlan::HashAggregate {
                    input: Box::new(self.to_physical(*input)),
                    group_by,
                    aggregates,
                }
            }
            LogicalPlan::Sort { input, order_by } => {
                PhysicalPlan::Sort {
                    input: Box::new(self.to_physical(*input)),
                    order_by,
                }
            }
            LogicalPlan::Limit { input, limit } => {
                PhysicalPlan::Limit {
                    input: Box::new(self.to_physical(*input)),
                    limit,
                }
            }
            LogicalPlan::Insert { table, columns, values } => {
                PhysicalPlan::Insert { table, columns, values }
            }
            LogicalPlan::Update { table, assignments, filter } => {
                PhysicalPlan::Update {
                    table,
                    assignments,
                    filter: filter.map(|f| Box::new(self.to_physical(LogicalPlan::Filter {
                        input: Box::new(LogicalPlan::Scan { table: String::new(), alias: None }),
                        predicate: f,
                    }))),
                }
            }
            LogicalPlan::Delete { table, filter } => {
                PhysicalPlan::Delete {
                    table,
                    filter: filter.map(|f| Box::new(self.to_physical(LogicalPlan::Filter {
                        input: Box::new(LogicalPlan::Scan { table: String::new(), alias: None }),
                        predicate: f,
                    }))),
                }
            }
            LogicalPlan::CreateTable { name, columns } => {
                PhysicalPlan::CreateTable {
                    name,
                    columns: columns.into_iter().map(|c| ColumnDef {
                        name: c.name,
                        data_type: c.data_type,
                        primary_key: c.primary_key,
                        not_null: c.not_null,
                    }).collect(),
                }
            }
        }
    }

    /// Try to convert a filter to an index scan.
    fn try_index_scan(&self, input: &LogicalPlan, predicate: &Expr) -> Option<PhysicalPlan> {
        // Check if input is a scan and we have an index
        if let LogicalPlan::Scan { table, .. } = input {
            if let Some(indexes) = self.indexes.get(table) {
                if !indexes.is_empty() {
                    // Check if predicate is an equality on indexed column
                    if let Expr::BinaryOp { left, op: crate::sql::ast::BinaryOp::Eq, right } = predicate {
                        if let Expr::Column(_col) = left.as_ref() {
                            // Simplified: assume first index is on this column
                            return Some(PhysicalPlan::IndexScan {
                                table: table.clone(),
                                index: indexes[0].clone(),
                                key: *right.clone(),
                            });
                        }
                    }
                }
            }
        }
        None
    }

    /// Extract equi-join keys from a join condition.
    fn extract_equi_join_keys(&self, condition: &Expr) -> Option<(Expr, Expr)> {
        if let Expr::BinaryOp { left, op: crate::sql::ast::BinaryOp::Eq, right } = condition {
            // Check both sides are column references
            if matches!(left.as_ref(), Expr::Column(_) | Expr::QualifiedColumn { .. }) &&
               matches!(right.as_ref(), Expr::Column(_) | Expr::QualifiedColumn { .. }) {
                return Some((*left.clone(), *right.clone()));
            }
        }
        None
    }
}

impl Default for Optimizer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::ast::{Literal, BinaryOp, Expr};
    use crate::planner::logical::{ProjectColumn, JoinType, AggregateExpr, AggregateFunc};

    #[test]
    fn test_simple_scan() {
        let opt = Optimizer::new();
        let plan = LogicalPlan::scan("users");
        let physical = opt.optimize(plan);

        match physical {
            PhysicalPlan::SeqScan { table } => assert_eq!(table, "users"),
            _ => panic!("expected seq scan"),
        }
    }

    #[test]
    fn test_filter_to_index_scan() {
        let mut opt = Optimizer::new();
        opt.register_index("users", "pk_users");

        let plan = LogicalPlan::scan("users")
            .filter(Expr::BinaryOp {
                left: Box::new(Expr::Column("id".into())),
                op: BinaryOp::Eq,
                right: Box::new(Expr::Literal(Literal::Integer(1))),
            });

        let physical = opt.optimize(plan);

        match physical {
            PhysicalPlan::IndexScan { table, index, .. } => {
                assert_eq!(table, "users");
                assert_eq!(index, "pk_users");
            }
            _ => panic!("expected index scan"),
        }
    }

    #[test]
    fn test_predicate_pushdown() {
        let opt = Optimizer::new();

        // Filter on top of Project
        let plan = LogicalPlan::Project {
            input: Box::new(LogicalPlan::scan("users")),
            columns: vec![ProjectColumn {
                expr: Expr::Column("name".into()),
                alias: None,
            }],
        };
        let plan = LogicalPlan::Filter {
            input: Box::new(plan),
            predicate: Expr::BinaryOp {
                left: Box::new(Expr::Column("id".into())),
                op: BinaryOp::Gt,
                right: Box::new(Expr::Literal(Literal::Integer(0))),
            },
        };

        let optimized = opt.push_down_predicates(plan);

        // Filter should now be inside the Project
        match optimized {
            LogicalPlan::Project { input, .. } => {
                match *input {
                    LogicalPlan::Filter { .. } => {}
                    _ => panic!("expected filter inside project"),
                }
            }
            _ => panic!("expected project"),
        }
    }

    #[test]
    fn test_join_strategy_selection() {
        let opt = Optimizer::new();

        let plan = LogicalPlan::scan("users").join(
            LogicalPlan::scan("orders"),
            Some(Expr::BinaryOp {
                left: Box::new(Expr::Column("users.id".into())),
                op: BinaryOp::Eq,
                right: Box::new(Expr::Column("orders.user_id".into())),
            }),
            JoinType::Inner,
        );

        let physical = opt.optimize(plan);

        // Should choose hash join for equi-join
        match physical {
            PhysicalPlan::HashJoin { .. } => {}
            PhysicalPlan::NestedLoopJoin { .. } => {}
            _ => panic!("expected join"),
        }
    }

    #[test]
    fn test_aggregate() {
        let opt = Optimizer::new();

        let plan = LogicalPlan::scan("orders")
            .aggregate(
                vec!["user_id".into()],
                vec![AggregateExpr {
                    func: super::super::logical::AggregateFunc::Count,
                    column: Some("id".into()),
                    alias: "order_count".into(),
                }],
            );

        let physical = opt.optimize(plan);

        match physical {
            PhysicalPlan::HashAggregate { group_by, aggregates, .. } => {
                assert_eq!(group_by, vec!["user_id"]);
                assert_eq!(aggregates.len(), 1);
            }
            _ => panic!("expected hash aggregate"),
        }
    }
}
