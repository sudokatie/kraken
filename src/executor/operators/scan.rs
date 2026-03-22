//! Table scan operator.

use crate::Result;
use super::{Operator, Row};

/// Table scan operator.
///
/// Scans all rows from a table (or in-memory data).
pub struct TableScan {
    /// Column names.
    columns: Vec<String>,
    /// Data rows.
    data: Vec<Row>,
    /// Current position.
    pos: usize,
}

impl TableScan {
    /// Create a new table scan with column names and data.
    pub fn new(columns: Vec<String>, data: Vec<Row>) -> Self {
        Self {
            columns,
            data,
            pos: 0,
        }
    }

    /// Create an empty scan.
    pub fn empty(columns: Vec<String>) -> Self {
        Self::new(columns, vec![])
    }
}

impl Operator for TableScan {
    fn next(&mut self) -> Result<Option<Row>> {
        if self.pos >= self.data.len() {
            return Ok(None);
        }

        let row = self.data[self.pos].clone();
        self.pos += 1;
        Ok(Some(row))
    }

    fn columns(&self) -> &[String] {
        &self.columns
    }

    fn reset(&mut self) -> Result<()> {
        self.pos = 0;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::types::Value;

    #[test]
    fn test_table_scan() {
        let columns = vec!["id".into(), "name".into()];
        let data = vec![
            vec![Value::Integer(1), Value::Text("alice".into())],
            vec![Value::Integer(2), Value::Text("bob".into())],
        ];

        let mut scan = TableScan::new(columns, data);

        let row1 = scan.next().unwrap().unwrap();
        assert_eq!(row1[0], Value::Integer(1));

        let row2 = scan.next().unwrap().unwrap();
        assert_eq!(row2[0], Value::Integer(2));

        assert!(scan.next().unwrap().is_none());
    }

    #[test]
    fn test_scan_reset() {
        let columns = vec!["x".into()];
        let data = vec![vec![Value::Integer(42)]];

        let mut scan = TableScan::new(columns, data);

        scan.next().unwrap();
        assert!(scan.next().unwrap().is_none());

        scan.reset().unwrap();
        let row = scan.next().unwrap().unwrap();
        assert_eq!(row[0], Value::Integer(42));
    }

    #[test]
    fn test_empty_scan() {
        let mut scan = TableScan::empty(vec!["a".into()]);
        assert!(scan.next().unwrap().is_none());
    }
}
