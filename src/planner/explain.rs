//! EXPLAIN output for query plans.

use super::logical::LogicalPlan;
use super::physical::PhysicalPlan;
use std::fmt::Write;

/// Format for EXPLAIN output.
#[derive(Debug, Clone, Copy, Default)]
pub enum ExplainFormat {
    /// Human-readable text format.
    #[default]
    Text,
    /// JSON format.
    Json,
    /// Tree format with indentation.
    Tree,
}

/// Options for EXPLAIN.
#[derive(Debug, Clone, Default)]
pub struct ExplainOptions {
    /// Output format.
    pub format: ExplainFormat,
    /// Include cost estimates.
    pub costs: bool,
    /// Include row estimates.
    pub rows: bool,
    /// Verbose output (include all details).
    pub verbose: bool,
}

impl ExplainOptions {
    /// Create new explain options.
    pub fn new() -> Self {
        Self::default()
    }

    /// Enable cost estimates.
    pub fn with_costs(mut self) -> Self {
        self.costs = true;
        self
    }

    /// Enable row estimates.
    pub fn with_rows(mut self) -> Self {
        self.rows = true;
        self
    }

    /// Enable verbose output.
    pub fn with_verbose(mut self) -> Self {
        self.verbose = true;
        self
    }

    /// Set format.
    pub fn with_format(mut self, format: ExplainFormat) -> Self {
        self.format = format;
        self
    }
}

/// Explain a logical plan.
pub fn explain_logical(plan: &LogicalPlan, options: &ExplainOptions) -> String {
    match options.format {
        ExplainFormat::Text => explain_logical_text(plan, 0),
        ExplainFormat::Tree => explain_logical_tree(plan, 0),
        ExplainFormat::Json => explain_logical_json(plan),
    }
}

/// Explain a physical plan.
pub fn explain_physical(plan: &PhysicalPlan, options: &ExplainOptions) -> String {
    match options.format {
        ExplainFormat::Text => explain_physical_text(plan, 0, options),
        ExplainFormat::Tree => explain_physical_tree(plan, 0, options),
        ExplainFormat::Json => explain_physical_json(plan),
    }
}

fn explain_logical_text(plan: &LogicalPlan, indent: usize) -> String {
    let prefix = "  ".repeat(indent);
    let mut out = String::new();

    match plan {
        LogicalPlan::Scan { table, alias } => {
            writeln!(out, "{}Scan: {} (alias: {:?})", prefix, table, alias).ok();
        }
        LogicalPlan::Project { input, columns } => {
            writeln!(out, "{}Project: {:?}", prefix, columns).ok();
            out.push_str(&explain_logical_text(input, indent + 1));
        }
        LogicalPlan::Filter { input, predicate } => {
            writeln!(out, "{}Filter: {:?}", prefix, predicate).ok();
            out.push_str(&explain_logical_text(input, indent + 1));
        }
        LogicalPlan::Join { left, right, condition, join_type } => {
            writeln!(out, "{}Join ({:?}): {:?}", prefix, join_type, condition).ok();
            out.push_str(&explain_logical_text(left, indent + 1));
            out.push_str(&explain_logical_text(right, indent + 1));
        }
        LogicalPlan::Aggregate { input, group_by, aggregates } => {
            writeln!(out, "{}Aggregate: group_by={:?}, aggs={:?}", prefix, group_by, aggregates).ok();
            out.push_str(&explain_logical_text(input, indent + 1));
        }
        LogicalPlan::Sort { input, order_by } => {
            writeln!(out, "{}Sort: {:?}", prefix, order_by).ok();
            out.push_str(&explain_logical_text(input, indent + 1));
        }
        LogicalPlan::Limit { input, limit } => {
            writeln!(out, "{}Limit: {}", prefix, limit).ok();
            out.push_str(&explain_logical_text(input, indent + 1));
        }
        LogicalPlan::Insert { table, values, .. } => {
            writeln!(out, "{}Insert: {} ({} rows)", prefix, table, values.len()).ok();
        }
        LogicalPlan::Update { table, .. } => {
            writeln!(out, "{}Update: {}", prefix, table).ok();
        }
        LogicalPlan::Delete { table, .. } => {
            writeln!(out, "{}Delete: {}", prefix, table).ok();
        }
        LogicalPlan::CreateTable { name, columns } => {
            writeln!(out, "{}CreateTable: {} ({} columns)", prefix, name, columns.len()).ok();
        }
    }

    out
}

fn explain_logical_tree(plan: &LogicalPlan, indent: usize) -> String {
    let connector = if indent == 0 { "" } else { "└─ " };
    let prefix = if indent == 0 {
        String::new()
    } else {
        "   ".repeat(indent - 1) + connector
    };

    let mut out = String::new();

    match plan {
        LogicalPlan::Scan { table, .. } => {
            writeln!(out, "{}Scan[{}]", prefix, table).ok();
        }
        LogicalPlan::Project { input, columns } => {
            writeln!(out, "{}Project[{}]", prefix, columns.len()).ok();
            out.push_str(&explain_logical_tree(input, indent + 1));
        }
        LogicalPlan::Filter { input, .. } => {
            writeln!(out, "{}Filter", prefix).ok();
            out.push_str(&explain_logical_tree(input, indent + 1));
        }
        LogicalPlan::Join { left, right, join_type, .. } => {
            writeln!(out, "{}{:?}Join", prefix, join_type).ok();
            out.push_str(&explain_logical_tree(left, indent + 1));
            out.push_str(&explain_logical_tree(right, indent + 1));
        }
        LogicalPlan::Aggregate { input, aggregates, .. } => {
            writeln!(out, "{}Aggregate[{}]", prefix, aggregates.len()).ok();
            out.push_str(&explain_logical_tree(input, indent + 1));
        }
        LogicalPlan::Sort { input, .. } => {
            writeln!(out, "{}Sort", prefix).ok();
            out.push_str(&explain_logical_tree(input, indent + 1));
        }
        LogicalPlan::Limit { input, limit } => {
            writeln!(out, "{}Limit[{}]", prefix, limit).ok();
            out.push_str(&explain_logical_tree(input, indent + 1));
        }
        LogicalPlan::Insert { table, values, .. } => {
            writeln!(out, "{}Insert[{}x{}]", prefix, table, values.len()).ok();
        }
        LogicalPlan::Update { table, .. } => {
            writeln!(out, "{}Update[{}]", prefix, table).ok();
        }
        LogicalPlan::Delete { table, .. } => {
            writeln!(out, "{}Delete[{}]", prefix, table).ok();
        }
        LogicalPlan::CreateTable { name, .. } => {
            writeln!(out, "{}CreateTable[{}]", prefix, name).ok();
        }
    }

    out
}

fn explain_logical_json(plan: &LogicalPlan) -> String {
    serde_json::to_string_pretty(&logical_to_json(plan)).unwrap_or_else(|_| "{}".to_string())
}

fn logical_to_json(plan: &LogicalPlan) -> serde_json::Value {
    use serde_json::json;

    match plan {
        LogicalPlan::Scan { table, alias } => {
            json!({
                "type": "Scan",
                "table": table,
                "alias": alias
            })
        }
        LogicalPlan::Project { input, columns } => {
            json!({
                "type": "Project",
                "columns": columns.len(),
                "input": logical_to_json(input)
            })
        }
        LogicalPlan::Filter { input, predicate } => {
            json!({
                "type": "Filter",
                "predicate": format!("{:?}", predicate),
                "input": logical_to_json(input)
            })
        }
        LogicalPlan::Join { left, right, join_type, .. } => {
            json!({
                "type": "Join",
                "join_type": format!("{:?}", join_type),
                "left": logical_to_json(left),
                "right": logical_to_json(right)
            })
        }
        LogicalPlan::Aggregate { input, group_by, aggregates } => {
            json!({
                "type": "Aggregate",
                "group_by": group_by.len(),
                "aggregates": aggregates.len(),
                "input": logical_to_json(input)
            })
        }
        LogicalPlan::Sort { input, order_by } => {
            json!({
                "type": "Sort",
                "order_by": order_by.len(),
                "input": logical_to_json(input)
            })
        }
        LogicalPlan::Limit { input, limit } => {
            json!({
                "type": "Limit",
                "limit": limit,
                "input": logical_to_json(input)
            })
        }
        LogicalPlan::Insert { table, values, .. } => {
            json!({
                "type": "Insert",
                "table": table,
                "rows": values.len()
            })
        }
        LogicalPlan::Update { table, .. } => {
            json!({
                "type": "Update",
                "table": table
            })
        }
        LogicalPlan::Delete { table, .. } => {
            json!({
                "type": "Delete",
                "table": table
            })
        }
        LogicalPlan::CreateTable { name, columns } => {
            json!({
                "type": "CreateTable",
                "name": name,
                "columns": columns.len()
            })
        }
    }
}

fn explain_physical_text(plan: &PhysicalPlan, indent: usize, options: &ExplainOptions) -> String {
    let prefix = "  ".repeat(indent);
    let mut out = String::new();

    match plan {
        PhysicalPlan::SeqScan { table } => {
            write!(out, "{}SeqScan: {}", prefix, table).ok();
            if options.costs {
                write!(out, " [cost: {:.1}]", plan.estimated_cost()).ok();
            }
            writeln!(out).ok();
        }
        PhysicalPlan::IndexScan { table, index, key } => {
            write!(out, "{}IndexScan: {} using {} on {:?}", prefix, table, index, key).ok();
            if options.costs {
                write!(out, " [cost: {:.1}]", plan.estimated_cost()).ok();
            }
            writeln!(out).ok();
        }
        PhysicalPlan::Filter { input, predicate } => {
            writeln!(out, "{}Filter: {:?}", prefix, predicate).ok();
            out.push_str(&explain_physical_text(input, indent + 1, options));
        }
        PhysicalPlan::Project { input, columns } => {
            let col_names: Vec<_> = columns.iter().map(|c| c.alias.as_ref().unwrap_or(&"?".to_string()).clone()).collect();
            writeln!(out, "{}Project: {:?}", prefix, col_names).ok();
            out.push_str(&explain_physical_text(input, indent + 1, options));
        }
        PhysicalPlan::NestedLoopJoin { left, right, condition, join_type } => {
            write!(out, "{}NestedLoopJoin ({:?}): {:?}", prefix, join_type, condition).ok();
            if options.costs {
                write!(out, " [cost: {:.1}]", plan.estimated_cost()).ok();
            }
            writeln!(out).ok();
            out.push_str(&explain_physical_text(left, indent + 1, options));
            out.push_str(&explain_physical_text(right, indent + 1, options));
        }
        PhysicalPlan::HashJoin { left, right, left_key, right_key, join_type } => {
            write!(out, "{}HashJoin ({:?}): {:?} = {:?}", prefix, join_type, left_key, right_key).ok();
            if options.costs {
                write!(out, " [cost: {:.1}]", plan.estimated_cost()).ok();
            }
            writeln!(out).ok();
            out.push_str(&explain_physical_text(left, indent + 1, options));
            out.push_str(&explain_physical_text(right, indent + 1, options));
        }
        PhysicalPlan::HashAggregate { input, group_by, aggregates } => {
            write!(out, "{}HashAggregate: group_by={}, aggs={}", prefix, group_by.len(), aggregates.len()).ok();
            if options.costs {
                write!(out, " [cost: {:.1}]", plan.estimated_cost()).ok();
            }
            writeln!(out).ok();
            out.push_str(&explain_physical_text(input, indent + 1, options));
        }
        PhysicalPlan::Sort { input, order_by } => {
            write!(out, "{}Sort: {:?}", prefix, order_by).ok();
            if options.costs {
                write!(out, " [cost: {:.1}]", plan.estimated_cost()).ok();
            }
            writeln!(out).ok();
            out.push_str(&explain_physical_text(input, indent + 1, options));
        }
        PhysicalPlan::Limit { input, limit } => {
            writeln!(out, "{}Limit: {}", prefix, limit).ok();
            out.push_str(&explain_physical_text(input, indent + 1, options));
        }
        PhysicalPlan::Insert { table, values, .. } => {
            writeln!(out, "{}Insert: {} ({} rows)", prefix, table, values.len()).ok();
        }
        PhysicalPlan::Update { table, .. } => {
            writeln!(out, "{}Update: {}", prefix, table).ok();
        }
        PhysicalPlan::Delete { table, .. } => {
            writeln!(out, "{}Delete: {}", prefix, table).ok();
        }
        PhysicalPlan::CreateTable { name, columns } => {
            writeln!(out, "{}CreateTable: {} ({} columns)", prefix, name, columns.len()).ok();
        }
    }

    out
}

fn explain_physical_tree(plan: &PhysicalPlan, indent: usize, options: &ExplainOptions) -> String {
    let connector = if indent == 0 { "" } else { "└─ " };
    let prefix = if indent == 0 {
        String::new()
    } else {
        "   ".repeat(indent - 1) + connector
    };

    let mut out = String::new();

    match plan {
        PhysicalPlan::SeqScan { table } => {
            write!(out, "{}SeqScan[{}]", prefix, table).ok();
            if options.costs {
                write!(out, " cost={:.0}", plan.estimated_cost()).ok();
            }
            writeln!(out).ok();
        }
        PhysicalPlan::IndexScan { table, index, .. } => {
            write!(out, "{}IndexScan[{}.{}]", prefix, table, index).ok();
            if options.costs {
                write!(out, " cost={:.0}", plan.estimated_cost()).ok();
            }
            writeln!(out).ok();
        }
        PhysicalPlan::Filter { input, .. } => {
            writeln!(out, "{}Filter", prefix).ok();
            out.push_str(&explain_physical_tree(input, indent + 1, options));
        }
        PhysicalPlan::Project { input, columns } => {
            writeln!(out, "{}Project[{}]", prefix, columns.len()).ok();
            out.push_str(&explain_physical_tree(input, indent + 1, options));
        }
        PhysicalPlan::NestedLoopJoin { left, right, .. } => {
            write!(out, "{}NLJoin", prefix).ok();
            if options.costs {
                write!(out, " cost={:.0}", plan.estimated_cost()).ok();
            }
            writeln!(out).ok();
            out.push_str(&explain_physical_tree(left, indent + 1, options));
            out.push_str(&explain_physical_tree(right, indent + 1, options));
        }
        PhysicalPlan::HashJoin { left, right, .. } => {
            write!(out, "{}HashJoin", prefix).ok();
            if options.costs {
                write!(out, " cost={:.0}", plan.estimated_cost()).ok();
            }
            writeln!(out).ok();
            out.push_str(&explain_physical_tree(left, indent + 1, options));
            out.push_str(&explain_physical_tree(right, indent + 1, options));
        }
        PhysicalPlan::HashAggregate { input, .. } => {
            write!(out, "{}HashAgg", prefix).ok();
            if options.costs {
                write!(out, " cost={:.0}", plan.estimated_cost()).ok();
            }
            writeln!(out).ok();
            out.push_str(&explain_physical_tree(input, indent + 1, options));
        }
        PhysicalPlan::Sort { input, .. } => {
            writeln!(out, "{}Sort", prefix).ok();
            out.push_str(&explain_physical_tree(input, indent + 1, options));
        }
        PhysicalPlan::Limit { input, limit } => {
            writeln!(out, "{}Limit[{}]", prefix, limit).ok();
            out.push_str(&explain_physical_tree(input, indent + 1, options));
        }
        PhysicalPlan::Insert { table, values, .. } => {
            writeln!(out, "{}Insert[{}x{}]", prefix, table, values.len()).ok();
        }
        PhysicalPlan::Update { table, .. } => {
            writeln!(out, "{}Update[{}]", prefix, table).ok();
        }
        PhysicalPlan::Delete { table, .. } => {
            writeln!(out, "{}Delete[{}]", prefix, table).ok();
        }
        PhysicalPlan::CreateTable { name, .. } => {
            writeln!(out, "{}CreateTable[{}]", prefix, name).ok();
        }
    }

    out
}

fn explain_physical_json(plan: &PhysicalPlan) -> String {
    serde_json::to_string_pretty(&physical_to_json(plan)).unwrap_or_else(|_| "{}".to_string())
}

fn physical_to_json(plan: &PhysicalPlan) -> serde_json::Value {
    use serde_json::json;

    match plan {
        PhysicalPlan::SeqScan { table } => {
            json!({
                "type": "SeqScan",
                "table": table,
                "estimated_cost": plan.estimated_cost()
            })
        }
        PhysicalPlan::IndexScan { table, index, .. } => {
            json!({
                "type": "IndexScan",
                "table": table,
                "index": index,
                "estimated_cost": plan.estimated_cost()
            })
        }
        PhysicalPlan::Filter { input, .. } => {
            json!({
                "type": "Filter",
                "input": physical_to_json(input)
            })
        }
        PhysicalPlan::Project { input, columns } => {
            json!({
                "type": "Project",
                "columns": columns.iter().map(|c| c.alias.clone().unwrap_or_default()).collect::<Vec<_>>(),
                "input": physical_to_json(input)
            })
        }
        PhysicalPlan::NestedLoopJoin { left, right, join_type, .. } => {
            json!({
                "type": "NestedLoopJoin",
                "join_type": format!("{:?}", join_type),
                "estimated_cost": plan.estimated_cost(),
                "left": physical_to_json(left),
                "right": physical_to_json(right)
            })
        }
        PhysicalPlan::HashJoin { left, right, join_type, .. } => {
            json!({
                "type": "HashJoin",
                "join_type": format!("{:?}", join_type),
                "estimated_cost": plan.estimated_cost(),
                "left": physical_to_json(left),
                "right": physical_to_json(right)
            })
        }
        PhysicalPlan::HashAggregate { input, group_by, aggregates } => {
            json!({
                "type": "HashAggregate",
                "group_by": group_by,
                "aggregates": aggregates.len(),
                "input": physical_to_json(input)
            })
        }
        PhysicalPlan::Sort { input, .. } => {
            json!({
                "type": "Sort",
                "input": physical_to_json(input)
            })
        }
        PhysicalPlan::Limit { input, limit } => {
            json!({
                "type": "Limit",
                "limit": limit,
                "input": physical_to_json(input)
            })
        }
        PhysicalPlan::Insert { table, values, .. } => {
            json!({
                "type": "Insert",
                "table": table,
                "rows": values.len()
            })
        }
        PhysicalPlan::Update { table, .. } => {
            json!({
                "type": "Update",
                "table": table
            })
        }
        PhysicalPlan::Delete { table, .. } => {
            json!({
                "type": "Delete",
                "table": table
            })
        }
        PhysicalPlan::CreateTable { name, columns } => {
            json!({
                "type": "CreateTable",
                "name": name,
                "columns": columns.len()
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_logical_plan() -> LogicalPlan {
        LogicalPlan::Limit {
            input: Box::new(LogicalPlan::Filter {
                input: Box::new(LogicalPlan::Scan {
                    table: "users".to_string(),
                    alias: None,
                }),
                predicate: crate::sql::ast::Expr::Literal(crate::sql::ast::Literal::Integer(1)),
            }),
            limit: 10,
        }
    }

    #[test]
    fn test_explain_logical_text() {
        let plan = sample_logical_plan();
        let options = ExplainOptions::new();
        let out = explain_logical(&plan, &options);

        assert!(out.contains("Limit"));
        assert!(out.contains("Filter"));
        assert!(out.contains("Scan"));
    }

    #[test]
    fn test_explain_logical_tree() {
        let plan = sample_logical_plan();
        let options = ExplainOptions::new().with_format(ExplainFormat::Tree);
        let out = explain_logical(&plan, &options);

        assert!(out.contains("Limit[10]"));
        assert!(out.contains("Filter"));
        assert!(out.contains("Scan[users]"));
    }

    #[test]
    fn test_explain_logical_json() {
        let plan = sample_logical_plan();
        let options = ExplainOptions::new().with_format(ExplainFormat::Json);
        let out = explain_logical(&plan, &options);

        assert!(out.contains("\"type\": \"Limit\""));
        assert!(out.contains("\"type\": \"Scan\""));
    }

    #[test]
    fn test_explain_options() {
        let options = ExplainOptions::new()
            .with_costs()
            .with_rows()
            .with_verbose()
            .with_format(ExplainFormat::Tree);

        assert!(options.costs);
        assert!(options.rows);
        assert!(options.verbose);
    }
}
