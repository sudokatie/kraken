//! Join operators.

use crate::Result;
use super::{Operator, Row, BoxedOperator};

/// Nested loop join operator.
///
/// Performs a nested loop join between left and right children.
pub struct NestedLoopJoin {
    /// Left child operator.
    left: BoxedOperator,
    /// Right child operator.
    right: BoxedOperator,
    /// Join predicate: (left_col_idx, right_col_idx)
    on: (usize, usize),
    /// Output column names.
    columns: Vec<String>,
    /// Current left row.
    current_left: Option<Row>,
    /// Whether we've exhausted the left side.
    left_exhausted: bool,
}

impl NestedLoopJoin {
    /// Create a new nested loop join.
    pub fn new(left: BoxedOperator, right: BoxedOperator, on: (usize, usize)) -> Self {
        let mut columns = left.columns().to_vec();
        columns.extend(right.columns().iter().cloned());

        Self {
            left,
            right,
            on,
            columns,
            current_left: None,
            left_exhausted: false,
        }
    }
}

impl Operator for NestedLoopJoin {
    fn next(&mut self) -> Result<Option<Row>> {
        loop {
            // Get current left row
            if self.current_left.is_none() {
                if self.left_exhausted {
                    return Ok(None);
                }

                match self.left.next()? {
                    Some(row) => {
                        self.current_left = Some(row);
                        self.right.reset()?;
                    }
                    None => {
                        self.left_exhausted = true;
                        return Ok(None);
                    }
                }
            }

            // Scan right for matches
            let left_row = self.current_left.as_ref().unwrap();

            match self.right.next()? {
                Some(right_row) => {
                    // Check join condition
                    let left_val = &left_row[self.on.0];
                    let right_val = &right_row[self.on.1];

                    if left_val == right_val {
                        // Match! Combine rows
                        let mut combined = left_row.clone();
                        combined.extend(right_row);
                        return Ok(Some(combined));
                    }
                    // No match, continue scanning right
                }
                None => {
                    // Right exhausted, move to next left row
                    self.current_left = None;
                }
            }
        }
    }

    fn columns(&self) -> &[String] {
        &self.columns
    }

    fn reset(&mut self) -> Result<()> {
        self.left.reset()?;
        self.right.reset()?;
        self.current_left = None;
        self.left_exhausted = false;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::types::Value;
    use crate::executor::operators::TableScan;

    #[test]
    fn test_nested_loop_join() {
        let left_cols = vec!["id".into(), "name".into()];
        let left_data = vec![
            vec![Value::Integer(1), Value::Text("alice".into())],
            vec![Value::Integer(2), Value::Text("bob".into())],
        ];

        let right_cols = vec!["user_id".into(), "order".into()];
        let right_data = vec![
            vec![Value::Integer(1), Value::Text("order1".into())],
            vec![Value::Integer(1), Value::Text("order2".into())],
            vec![Value::Integer(3), Value::Text("order3".into())],
        ];

        let left = Box::new(TableScan::new(left_cols, left_data));
        let right = Box::new(TableScan::new(right_cols, right_data));

        let mut join = NestedLoopJoin::new(left, right, (0, 0));

        // Alice's orders
        let row1 = join.next().unwrap().unwrap();
        assert_eq!(row1[0], Value::Integer(1));
        assert_eq!(row1[3], Value::Text("order1".into()));

        let row2 = join.next().unwrap().unwrap();
        assert_eq!(row2[0], Value::Integer(1));
        assert_eq!(row2[3], Value::Text("order2".into()));

        // No more matches (bob has no orders, order3 has no user)
        assert!(join.next().unwrap().is_none());
    }

    #[test]
    fn test_empty_join() {
        let left = Box::new(TableScan::empty(vec!["a".into()]));
        let right = Box::new(TableScan::empty(vec!["b".into()]));

        let mut join = NestedLoopJoin::new(left, right, (0, 0));
        assert!(join.next().unwrap().is_none());
    }
}
