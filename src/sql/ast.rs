//! SQL Abstract Syntax Tree.

/// SQL statement.
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Select(SelectStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
    CreateTable(CreateTableStatement),
}

/// SELECT statement.
#[derive(Debug, Clone, PartialEq)]
pub struct SelectStatement {
    /// WITH clause (CTEs)
    pub ctes: Vec<CommonTableExpr>,
    pub columns: Vec<SelectColumn>,
    pub from: TableRef,
    pub joins: Vec<JoinClause>,
    pub where_clause: Option<Expr>,
    pub order_by: Vec<OrderBy>,
    pub limit: Option<i64>,
    pub group_by: Vec<String>,
    pub having: Option<Expr>,
}

/// Common Table Expression (CTE) from WITH clause
#[derive(Debug, Clone, PartialEq)]
pub struct CommonTableExpr {
    pub name: String,
    pub columns: Option<Vec<String>>,
    pub query: Box<SelectStatement>,
}

/// JOIN clause
#[derive(Debug, Clone, PartialEq)]
pub struct JoinClause {
    pub join_type: JoinType,
    pub table: TableRef,
    pub condition: Option<Expr>,
}

/// JOIN type
#[derive(Debug, Clone, PartialEq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

/// Column in SELECT list.
#[derive(Debug, Clone, PartialEq)]
pub enum SelectColumn {
    /// All columns (*)
    Star,
    /// Expression with optional alias
    Expr { expr: Expr, alias: Option<String> },
}

/// Table reference.
#[derive(Debug, Clone, PartialEq)]
pub struct TableRef {
    pub name: String,
    pub alias: Option<String>,
}

/// ORDER BY clause.
#[derive(Debug, Clone, PartialEq)]
pub struct OrderBy {
    pub column: String,
    pub descending: bool,
}

/// INSERT statement.
#[derive(Debug, Clone, PartialEq)]
pub struct InsertStatement {
    pub table: String,
    pub columns: Option<Vec<String>>,
    pub values: Vec<Vec<Expr>>,
}

/// UPDATE statement.
#[derive(Debug, Clone, PartialEq)]
pub struct UpdateStatement {
    pub table: String,
    pub assignments: Vec<Assignment>,
    pub where_clause: Option<Expr>,
}

/// SET assignment.
#[derive(Debug, Clone, PartialEq)]
pub struct Assignment {
    pub column: String,
    pub value: Expr,
}

/// DELETE statement.
#[derive(Debug, Clone, PartialEq)]
pub struct DeleteStatement {
    pub table: String,
    pub where_clause: Option<Expr>,
}

/// CREATE TABLE statement.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTableStatement {
    pub name: String,
    pub columns: Vec<ColumnDef>,
}

/// Column definition.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub primary_key: bool,
    pub not_null: bool,
}

/// Data type.
#[derive(Debug, Clone, PartialEq)]
pub enum DataType {
    Integer,
    Real,
    Text,
    Boolean,
    Blob,
}

/// Expression.
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// Literal value
    Literal(Literal),
    /// Column reference
    Column(String),
    /// Table.column reference
    QualifiedColumn { table: String, column: String },
    /// Binary operation
    BinaryOp { left: Box<Expr>, op: BinaryOp, right: Box<Expr> },
    /// Unary operation
    UnaryOp { op: UnaryOp, expr: Box<Expr> },
    /// Function call
    Function { name: String, args: Vec<Expr> },
    /// IS NULL / IS NOT NULL
    IsNull { expr: Box<Expr>, negated: bool },
    /// Scalar subquery (returns single value)
    Subquery(Box<SelectStatement>),
    /// EXISTS subquery
    Exists { subquery: Box<SelectStatement>, negated: bool },
    /// IN subquery: expr IN (SELECT ...)
    InSubquery { expr: Box<Expr>, subquery: Box<SelectStatement>, negated: bool },
    /// CASE expression
    Case {
        operand: Option<Box<Expr>>,
        when_clauses: Vec<WhenClause>,
        else_result: Option<Box<Expr>>,
    },
    /// Window function
    WindowFunction {
        function: WindowFunc,
        partition_by: Vec<Expr>,
        order_by: Vec<OrderBy>,
        frame: Option<WindowFrame>,
    },
}

/// WHEN clause for CASE expression
#[derive(Debug, Clone, PartialEq)]
pub struct WhenClause {
    pub condition: Expr,
    pub result: Expr,
}

/// Window function types
#[derive(Debug, Clone, PartialEq)]
pub enum WindowFunc {
    RowNumber,
    Rank,
    DenseRank,
    NTile(i64),
    Lead { expr: Box<Expr>, offset: Option<i64>, default: Option<Box<Expr>> },
    Lag { expr: Box<Expr>, offset: Option<i64>, default: Option<Box<Expr>> },
    FirstValue(Box<Expr>),
    LastValue(Box<Expr>),
    NthValue { expr: Box<Expr>, n: i64 },
    /// Aggregate used as window function
    Aggregate { name: String, args: Vec<Expr> },
}

/// Window frame specification
#[derive(Debug, Clone, PartialEq)]
pub struct WindowFrame {
    pub mode: FrameMode,
    pub start: FrameBound,
    pub end: Option<FrameBound>,
}

/// Frame mode (ROWS or RANGE)
#[derive(Debug, Clone, PartialEq)]
pub enum FrameMode {
    Rows,
    Range,
    Groups,
}

/// Frame bound
#[derive(Debug, Clone, PartialEq)]
pub enum FrameBound {
    UnboundedPreceding,
    Preceding(i64),
    CurrentRow,
    Following(i64),
    UnboundedFollowing,
}

/// Literal value.
#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    Null,
    Integer(i64),
    Float(f64),
    String(String),
    Boolean(bool),
}

/// Binary operator.
#[derive(Debug, Clone, PartialEq)]
pub enum BinaryOp {
    // Comparison
    Eq,
    Ne,
    Lt,
    Gt,
    Le,
    Ge,
    // Logical
    And,
    Or,
    // Arithmetic
    Add,
    Sub,
    Mul,
    Div,
}

/// Unary operator.
#[derive(Debug, Clone, PartialEq)]
pub enum UnaryOp {
    Not,
    Neg,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_select_statement() {
        let stmt = Statement::Select(SelectStatement {
            ctes: vec![],
            columns: vec![SelectColumn::Star],
            from: TableRef { name: "users".into(), alias: None },
            joins: vec![],
            where_clause: None,
            order_by: vec![],
            limit: None,
            group_by: vec![],
            having: None,
        });

        if let Statement::Select(s) = stmt {
            assert_eq!(s.from.name, "users");
        } else {
            panic!("expected select");
        }
    }

    #[test]
    fn test_cte() {
        let cte = CommonTableExpr {
            name: "recent_orders".into(),
            columns: Some(vec!["id".into(), "total".into()]),
            query: Box::new(SelectStatement {
                ctes: vec![],
                columns: vec![SelectColumn::Star],
                from: TableRef { name: "orders".into(), alias: None },
                joins: vec![],
                where_clause: None,
                order_by: vec![],
                limit: Some(10),
                group_by: vec![],
                having: None,
            }),
        };

        assert_eq!(cte.name, "recent_orders");
        assert_eq!(cte.columns, Some(vec!["id".into(), "total".into()]));
    }

    #[test]
    fn test_window_function() {
        let expr = Expr::WindowFunction {
            function: WindowFunc::RowNumber,
            partition_by: vec![Expr::Column("dept".into())],
            order_by: vec![OrderBy { column: "salary".into(), descending: true }],
            frame: None,
        };

        if let Expr::WindowFunction { function, .. } = expr {
            assert_eq!(function, WindowFunc::RowNumber);
        } else {
            panic!("expected window function");
        }
    }

    #[test]
    fn test_case_expression() {
        let expr = Expr::Case {
            operand: None,
            when_clauses: vec![
                WhenClause {
                    condition: Expr::BinaryOp {
                        left: Box::new(Expr::Column("status".into())),
                        op: BinaryOp::Eq,
                        right: Box::new(Expr::Literal(Literal::String("active".into()))),
                    },
                    result: Expr::Literal(Literal::Integer(1)),
                },
            ],
            else_result: Some(Box::new(Expr::Literal(Literal::Integer(0)))),
        };

        if let Expr::Case { when_clauses, .. } = expr {
            assert_eq!(when_clauses.len(), 1);
        } else {
            panic!("expected case expression");
        }
    }
}
