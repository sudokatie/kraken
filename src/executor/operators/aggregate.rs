//! Aggregate operator.

use crate::Result;
use crate::sql::types::Value;
use super::{Operator, Row, BoxedOperator};

/// Aggregate function type.
#[derive(Debug, Clone, Copy)]
pub enum AggregateFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

/// Aggregate operator.
///
/// Computes aggregate functions over all input rows.
/// Returns a single row with the aggregate results.
pub struct Aggregate {
    /// Child operator.
    child: BoxedOperator,
    /// Aggregates to compute: (function, column_index).
    aggregates: Vec<(AggregateFunc, usize)>,
    /// Output column names.
    columns: Vec<String>,
    /// Whether we've returned the result.
    done: bool,
}

impl Aggregate {
    /// Create a new aggregate operator.
    pub fn new(
        child: BoxedOperator,
        aggregates: Vec<(AggregateFunc, usize)>,
        columns: Vec<String>,
    ) -> Self {
        Self {
            child,
            aggregates,
            columns,
            done: false,
        }
    }

    /// Compute aggregates over all rows.
    fn compute(&mut self) -> Result<Row> {
        let mut accumulators: Vec<Accumulator> = self.aggregates.iter()
            .map(|(func, _)| Accumulator::new(*func))
            .collect();

        // Process all rows
        while let Some(row) = self.child.next()? {
            for (i, (_, col_idx)) in self.aggregates.iter().enumerate() {
                if let Some(val) = row.get(*col_idx) {
                    accumulators[i].add(val);
                }
            }
        }

        // Finalize
        let result: Row = accumulators.into_iter()
            .map(|acc| acc.finalize())
            .collect();

        Ok(result)
    }
}

impl Operator for Aggregate {
    fn next(&mut self) -> Result<Option<Row>> {
        if self.done {
            return Ok(None);
        }

        self.done = true;
        let result = self.compute()?;
        Ok(Some(result))
    }

    fn columns(&self) -> &[String] {
        &self.columns
    }

    fn reset(&mut self) -> Result<()> {
        self.child.reset()?;
        self.done = false;
        Ok(())
    }
}

/// Accumulator for aggregate computation.
struct Accumulator {
    func: AggregateFunc,
    count: i64,
    sum: f64,
    min: Option<Value>,
    max: Option<Value>,
}

impl Accumulator {
    fn new(func: AggregateFunc) -> Self {
        Self {
            func,
            count: 0,
            sum: 0.0,
            min: None,
            max: None,
        }
    }

    fn add(&mut self, val: &Value) {
        if matches!(val, Value::Null) {
            return;
        }

        self.count += 1;

        // Sum
        if let Some(n) = val.as_number() {
            self.sum += n;
        }

        // Min
        if self.min.is_none() || val < self.min.as_ref().unwrap() {
            self.min = Some(val.clone());
        }

        // Max
        if self.max.is_none() || val > self.max.as_ref().unwrap() {
            self.max = Some(val.clone());
        }
    }

    fn finalize(self) -> Value {
        match self.func {
            AggregateFunc::Count => Value::Integer(self.count),
            AggregateFunc::Sum => Value::Real(self.sum),
            AggregateFunc::Avg => {
                if self.count > 0 {
                    Value::Real(self.sum / self.count as f64)
                } else {
                    Value::Null
                }
            }
            AggregateFunc::Min => self.min.unwrap_or(Value::Null),
            AggregateFunc::Max => self.max.unwrap_or(Value::Null),
        }
    }
}

// Helper methods on Value
impl Value {
    fn as_number(&self) -> Option<f64> {
        match self {
            Value::Integer(n) => Some(*n as f64),
            Value::Real(n) => Some(*n),
            _ => None,
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Value::Integer(a), Value::Integer(b)) => a.partial_cmp(b),
            (Value::Real(a), Value::Real(b)) => a.partial_cmp(b),
            (Value::Text(a), Value::Text(b)) => a.partial_cmp(b),
            (Value::Boolean(a), Value::Boolean(b)) => a.partial_cmp(b),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::operators::TableScan;

    fn test_data() -> (Vec<String>, Vec<Row>) {
        let columns = vec!["id".into(), "value".into()];
        let data = vec![
            vec![Value::Integer(1), Value::Integer(10)],
            vec![Value::Integer(2), Value::Integer(20)],
            vec![Value::Integer(3), Value::Integer(30)],
        ];
        (columns, data)
    }

    #[test]
    fn test_count() {
        let (columns, data) = test_data();
        let scan = Box::new(TableScan::new(columns, data));

        let mut agg = Aggregate::new(
            scan,
            vec![(AggregateFunc::Count, 0)],
            vec!["count".into()],
        );

        let row = agg.next().unwrap().unwrap();
        assert_eq!(row[0], Value::Integer(3));

        assert!(agg.next().unwrap().is_none());
    }

    #[test]
    fn test_sum() {
        let (columns, data) = test_data();
        let scan = Box::new(TableScan::new(columns, data));

        let mut agg = Aggregate::new(
            scan,
            vec![(AggregateFunc::Sum, 1)],
            vec!["sum".into()],
        );

        let row = agg.next().unwrap().unwrap();
        assert_eq!(row[0], Value::Real(60.0));
    }

    #[test]
    fn test_avg() {
        let (columns, data) = test_data();
        let scan = Box::new(TableScan::new(columns, data));

        let mut agg = Aggregate::new(
            scan,
            vec![(AggregateFunc::Avg, 1)],
            vec!["avg".into()],
        );

        let row = agg.next().unwrap().unwrap();
        assert_eq!(row[0], Value::Real(20.0));
    }

    #[test]
    fn test_min_max() {
        let (columns, data) = test_data();
        let scan = Box::new(TableScan::new(columns, data));

        let mut agg = Aggregate::new(
            scan,
            vec![
                (AggregateFunc::Min, 1),
                (AggregateFunc::Max, 1),
            ],
            vec!["min".into(), "max".into()],
        );

        let row = agg.next().unwrap().unwrap();
        assert_eq!(row[0], Value::Integer(10));
        assert_eq!(row[1], Value::Integer(30));
    }

    #[test]
    fn test_multiple_aggregates() {
        let (columns, data) = test_data();
        let scan = Box::new(TableScan::new(columns, data));

        let mut agg = Aggregate::new(
            scan,
            vec![
                (AggregateFunc::Count, 0),
                (AggregateFunc::Sum, 1),
                (AggregateFunc::Avg, 1),
            ],
            vec!["count".into(), "sum".into(), "avg".into()],
        );

        let row = agg.next().unwrap().unwrap();
        assert_eq!(row[0], Value::Integer(3));
        assert_eq!(row[1], Value::Real(60.0));
        assert_eq!(row[2], Value::Real(20.0));
    }

    #[test]
    fn test_empty_aggregate() {
        let scan = Box::new(TableScan::empty(vec!["x".into()]));

        let mut agg = Aggregate::new(
            scan,
            vec![(AggregateFunc::Count, 0)],
            vec!["count".into()],
        );

        let row = agg.next().unwrap().unwrap();
        assert_eq!(row[0], Value::Integer(0));
    }
}
