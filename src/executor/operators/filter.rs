//! Filter operator.

use crate::Result;
use crate::sql::types::Value;
use super::{Operator, Row, BoxedOperator};

/// Predicate function type.
pub type Predicate = Box<dyn Fn(&Row) -> bool>;

/// Filter operator.
///
/// Filters rows from child operator based on a predicate.
pub struct Filter {
    /// Child operator.
    child: BoxedOperator,
    /// Predicate function.
    predicate: Predicate,
}

impl Filter {
    /// Create a new filter operator.
    pub fn new(child: BoxedOperator, predicate: Predicate) -> Self {
        Self { child, predicate }
    }

    /// Create a filter that compares a column to a value.
    pub fn column_eq(child: BoxedOperator, col_idx: usize, value: Value) -> Self {
        Self::new(child, Box::new(move |row| {
            row.get(col_idx).map(|v| v == &value).unwrap_or(false)
        }))
    }

    /// Create a filter that checks column > value (for integers).
    pub fn column_gt(child: BoxedOperator, col_idx: usize, value: i64) -> Self {
        Self::new(child, Box::new(move |row| {
            match row.get(col_idx) {
                Some(Value::Integer(n)) => *n > value,
                _ => false,
            }
        }))
    }

    /// Create a filter that checks column < value (for integers).
    pub fn column_lt(child: BoxedOperator, col_idx: usize, value: i64) -> Self {
        Self::new(child, Box::new(move |row| {
            match row.get(col_idx) {
                Some(Value::Integer(n)) => *n < value,
                _ => false,
            }
        }))
    }
}

impl Operator for Filter {
    fn next(&mut self) -> Result<Option<Row>> {
        loop {
            match self.child.next()? {
                Some(row) => {
                    if (self.predicate)(&row) {
                        return Ok(Some(row));
                    }
                    // Row filtered out, continue to next
                }
                None => return Ok(None),
            }
        }
    }

    fn columns(&self) -> &[String] {
        self.child.columns()
    }

    fn reset(&mut self) -> Result<()> {
        self.child.reset()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::operators::TableScan;

    fn test_data() -> (Vec<String>, Vec<Row>) {
        let columns = vec!["id".into(), "age".into()];
        let data = vec![
            vec![Value::Integer(1), Value::Integer(20)],
            vec![Value::Integer(2), Value::Integer(30)],
            vec![Value::Integer(3), Value::Integer(25)],
        ];
        (columns, data)
    }

    #[test]
    fn test_filter_eq() {
        let (columns, data) = test_data();
        let scan = Box::new(TableScan::new(columns, data));
        let mut filter = Filter::column_eq(scan, 0, Value::Integer(2));

        let row = filter.next().unwrap().unwrap();
        assert_eq!(row[0], Value::Integer(2));
        assert!(filter.next().unwrap().is_none());
    }

    #[test]
    fn test_filter_gt() {
        let (columns, data) = test_data();
        let scan = Box::new(TableScan::new(columns, data));
        let mut filter = Filter::column_gt(scan, 1, 22);

        let row1 = filter.next().unwrap().unwrap();
        assert_eq!(row1[1], Value::Integer(30));

        let row2 = filter.next().unwrap().unwrap();
        assert_eq!(row2[1], Value::Integer(25));

        assert!(filter.next().unwrap().is_none());
    }

    #[test]
    fn test_filter_custom() {
        let (columns, data) = test_data();
        let scan = Box::new(TableScan::new(columns, data));

        // Custom predicate: even IDs only
        let mut filter = Filter::new(scan, Box::new(|row| {
            match &row[0] {
                Value::Integer(n) => n % 2 == 0,
                _ => false,
            }
        }));

        let row = filter.next().unwrap().unwrap();
        assert_eq!(row[0], Value::Integer(2));
        assert!(filter.next().unwrap().is_none());
    }

    #[test]
    fn test_filter_none() {
        let (columns, data) = test_data();
        let scan = Box::new(TableScan::new(columns, data));
        let mut filter = Filter::column_eq(scan, 0, Value::Integer(999));

        assert!(filter.next().unwrap().is_none());
    }
}
