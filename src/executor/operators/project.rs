//! Project operator.

use crate::Result;
use super::{Operator, Row, BoxedOperator};

/// Project operator.
///
/// Projects (selects) specific columns from child rows.
pub struct Project {
    /// Child operator.
    child: BoxedOperator,
    /// Column indices to project.
    indices: Vec<usize>,
    /// Output column names.
    columns: Vec<String>,
}

impl Project {
    /// Create a new project operator.
    pub fn new(child: BoxedOperator, indices: Vec<usize>, columns: Vec<String>) -> Self {
        Self {
            child,
            indices,
            columns,
        }
    }

    /// Create a project that selects columns by index.
    pub fn by_indices(child: BoxedOperator, indices: Vec<usize>) -> Self {
        let columns: Vec<String> = indices.iter()
            .map(|&i| child.columns().get(i).cloned().unwrap_or_else(|| format!("col{}", i)))
            .collect();
        Self::new(child, indices, columns)
    }

    /// Create a project that selects columns by name.
    pub fn by_names(child: BoxedOperator, names: &[&str]) -> Result<Self> {
        let mut indices = Vec::with_capacity(names.len());
        let child_cols = child.columns();

        for name in names {
            let idx = child_cols.iter()
                .position(|c| c == *name)
                .ok_or_else(|| crate::Error::UnknownColumn((*name).to_string()))?;
            indices.push(idx);
        }

        let columns = names.iter().map(|s| (*s).to_string()).collect();
        Ok(Self::new(child, indices, columns))
    }
}

impl Operator for Project {
    fn next(&mut self) -> Result<Option<Row>> {
        match self.child.next()? {
            Some(row) => {
                let projected: Row = self.indices.iter()
                    .map(|&i| row.get(i).cloned().unwrap_or(crate::sql::types::Value::Null))
                    .collect();
                Ok(Some(projected))
            }
            None => Ok(None),
        }
    }

    fn columns(&self) -> &[String] {
        &self.columns
    }

    fn reset(&mut self) -> Result<()> {
        self.child.reset()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::types::Value;
    use crate::executor::operators::TableScan;

    fn test_data() -> (Vec<String>, Vec<Row>) {
        let columns = vec!["id".into(), "name".into(), "age".into()];
        let data = vec![
            vec![Value::Integer(1), Value::Text("alice".into()), Value::Integer(30)],
            vec![Value::Integer(2), Value::Text("bob".into()), Value::Integer(25)],
        ];
        (columns, data)
    }

    #[test]
    fn test_project_by_indices() {
        let (columns, data) = test_data();
        let scan = Box::new(TableScan::new(columns, data));
        let mut project = Project::by_indices(scan, vec![1, 2]);

        let row1 = project.next().unwrap().unwrap();
        assert_eq!(row1.len(), 2);
        assert_eq!(row1[0], Value::Text("alice".into()));
        assert_eq!(row1[1], Value::Integer(30));

        let row2 = project.next().unwrap().unwrap();
        assert_eq!(row2[0], Value::Text("bob".into()));
    }

    #[test]
    fn test_project_by_names() {
        let (columns, data) = test_data();
        let scan = Box::new(TableScan::new(columns, data));
        let mut project = Project::by_names(scan, &["name", "id"]).unwrap();

        let row = project.next().unwrap().unwrap();
        assert_eq!(row[0], Value::Text("alice".into()));
        assert_eq!(row[1], Value::Integer(1));

        assert_eq!(project.columns(), &["name", "id"]);
    }

    #[test]
    fn test_project_single_column() {
        let (columns, data) = test_data();
        let scan = Box::new(TableScan::new(columns, data));
        let mut project = Project::by_indices(scan, vec![0]);

        let row = project.next().unwrap().unwrap();
        assert_eq!(row.len(), 1);
        assert_eq!(row[0], Value::Integer(1));
    }

    #[test]
    fn test_project_unknown_column() {
        let (columns, data) = test_data();
        let scan = Box::new(TableScan::new(columns, data));
        let result = Project::by_names(scan, &["nonexistent"]);
        assert!(result.is_err());
    }
}
