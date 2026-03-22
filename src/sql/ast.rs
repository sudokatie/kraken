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
    pub columns: Vec<SelectColumn>,
    pub from: TableRef,
    pub where_clause: Option<Expr>,
    pub order_by: Vec<OrderBy>,
    pub limit: Option<i64>,
    pub group_by: Vec<String>,
    pub having: Option<Expr>,
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
            columns: vec![SelectColumn::Star],
            from: TableRef { name: "users".into(), alias: None },
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
}
