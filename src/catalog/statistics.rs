//! Table and column statistics for query optimization.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Statistics for a single table.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TableStatistics {
    /// Table name.
    pub table_name: String,
    /// Estimated row count.
    pub row_count: u64,
    /// Statistics per column (column name -> stats).
    pub columns: HashMap<String, ColumnStatistics>,
    /// Last update timestamp (Unix seconds).
    pub last_updated: u64,
}

impl TableStatistics {
    /// Create new statistics for a table.
    pub fn new(table_name: impl Into<String>) -> Self {
        Self {
            table_name: table_name.into(),
            row_count: 0,
            columns: HashMap::new(),
            last_updated: 0,
        }
    }

    /// Set row count.
    pub fn with_row_count(mut self, count: u64) -> Self {
        self.row_count = count;
        self
    }

    /// Add column statistics.
    pub fn with_column(mut self, name: impl Into<String>, stats: ColumnStatistics) -> Self {
        self.columns.insert(name.into(), stats);
        self
    }

    /// Get column statistics.
    pub fn column(&self, name: &str) -> Option<&ColumnStatistics> {
        self.columns.get(name)
    }

    /// Estimate selectivity of an equality predicate.
    pub fn selectivity_eq(&self, column: &str) -> f64 {
        self.columns
            .get(column)
            .map(|c| 1.0 / c.distinct_count.max(1) as f64)
            .unwrap_or(0.1) // Default 10% selectivity
    }

    /// Estimate selectivity of a range predicate.
    pub fn selectivity_range(&self, column: &str) -> f64 {
        // Default: assume 33% of rows match a range predicate
        self.columns
            .get(column)
            .map(|_| 0.33)
            .unwrap_or(0.33)
    }

    /// Estimate output rows after applying selectivity.
    pub fn estimate_rows(&self, selectivity: f64) -> u64 {
        ((self.row_count as f64) * selectivity).max(1.0) as u64
    }
}

/// Statistics for a single column.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ColumnStatistics {
    /// Column name.
    pub column_name: String,
    /// Number of distinct values.
    pub distinct_count: u64,
    /// Number of null values.
    pub null_count: u64,
    /// Minimum value (as string for simplicity).
    pub min_value: Option<String>,
    /// Maximum value (as string for simplicity).
    pub max_value: Option<String>,
    /// Average value length (for text columns).
    pub avg_length: Option<f64>,
}

impl ColumnStatistics {
    /// Create new column statistics.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            column_name: name.into(),
            distinct_count: 0,
            null_count: 0,
            min_value: None,
            max_value: None,
            avg_length: None,
        }
    }

    /// Set distinct count.
    pub fn with_distinct_count(mut self, count: u64) -> Self {
        self.distinct_count = count;
        self
    }

    /// Set null count.
    pub fn with_null_count(mut self, count: u64) -> Self {
        self.null_count = count;
        self
    }

    /// Set min/max values.
    pub fn with_range(mut self, min: Option<String>, max: Option<String>) -> Self {
        self.min_value = min;
        self.max_value = max;
        self
    }
}

/// Statistics manager for the database.
#[derive(Debug, Default)]
pub struct StatisticsManager {
    /// Statistics per table (table name -> stats).
    tables: HashMap<String, TableStatistics>,
}

impl StatisticsManager {
    /// Create a new statistics manager.
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    /// Update statistics for a table.
    pub fn update(&mut self, stats: TableStatistics) {
        self.tables.insert(stats.table_name.clone(), stats);
    }

    /// Get statistics for a table.
    pub fn get(&self, table_name: &str) -> Option<&TableStatistics> {
        self.tables.get(table_name)
    }

    /// Remove statistics for a table.
    pub fn remove(&mut self, table_name: &str) {
        self.tables.remove(table_name);
    }

    /// Get all table names with statistics.
    pub fn tables(&self) -> Vec<&str> {
        self.tables.keys().map(|s| s.as_str()).collect()
    }

    /// Estimate join cardinality using statistics.
    pub fn estimate_join_rows(
        &self,
        left_table: &str,
        right_table: &str,
        left_col: &str,
        right_col: &str,
    ) -> u64 {
        let left_stats = self.get(left_table);
        let right_stats = self.get(right_table);

        match (left_stats, right_stats) {
            (Some(l), Some(r)) => {
                let left_rows = l.row_count;
                let right_rows = r.row_count;
                let left_distinct = l.column(left_col).map(|c| c.distinct_count).unwrap_or(left_rows);
                let right_distinct = r.column(right_col).map(|c| c.distinct_count).unwrap_or(right_rows);

                // Estimate: (left_rows * right_rows) / max(left_distinct, right_distinct)
                let max_distinct = left_distinct.max(right_distinct).max(1);
                (left_rows * right_rows) / max_distinct
            }
            _ => 1000, // Default estimate
        }
    }

    /// Serialize statistics to JSON.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(&self.tables)
    }

    /// Load statistics from JSON.
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        let tables: HashMap<String, TableStatistics> = serde_json::from_str(json)?;
        Ok(Self { tables })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_statistics_new() {
        let stats = TableStatistics::new("users").with_row_count(1000);
        assert_eq!(stats.table_name, "users");
        assert_eq!(stats.row_count, 1000);
    }

    #[test]
    fn test_column_statistics() {
        let col = ColumnStatistics::new("id")
            .with_distinct_count(1000)
            .with_null_count(0)
            .with_range(Some("1".to_string()), Some("1000".to_string()));

        assert_eq!(col.distinct_count, 1000);
        assert_eq!(col.null_count, 0);
        assert_eq!(col.min_value, Some("1".to_string()));
    }

    #[test]
    fn test_selectivity_eq() {
        let mut stats = TableStatistics::new("users").with_row_count(1000);
        stats.columns.insert(
            "country".to_string(),
            ColumnStatistics::new("country").with_distinct_count(10),
        );

        // 10 distinct values -> 10% selectivity
        let sel = stats.selectivity_eq("country");
        assert!((sel - 0.1).abs() < 0.001);
    }

    #[test]
    fn test_estimate_rows() {
        let stats = TableStatistics::new("users").with_row_count(1000);
        let estimated = stats.estimate_rows(0.1);
        assert_eq!(estimated, 100);
    }

    #[test]
    fn test_statistics_manager() {
        let mut mgr = StatisticsManager::new();

        let users_stats = TableStatistics::new("users")
            .with_row_count(1000)
            .with_column("id", ColumnStatistics::new("id").with_distinct_count(1000));

        mgr.update(users_stats);

        assert!(mgr.get("users").is_some());
        assert_eq!(mgr.get("users").unwrap().row_count, 1000);
    }

    #[test]
    fn test_estimate_join_rows() {
        let mut mgr = StatisticsManager::new();

        let users = TableStatistics::new("users")
            .with_row_count(1000)
            .with_column("id", ColumnStatistics::new("id").with_distinct_count(1000));

        let orders = TableStatistics::new("orders")
            .with_row_count(5000)
            .with_column("user_id", ColumnStatistics::new("user_id").with_distinct_count(800));

        mgr.update(users);
        mgr.update(orders);

        let estimate = mgr.estimate_join_rows("users", "orders", "id", "user_id");
        // 1000 * 5000 / 1000 = 5000
        assert_eq!(estimate, 5000);
    }

    #[test]
    fn test_statistics_serialization() {
        let mut mgr = StatisticsManager::new();
        mgr.update(TableStatistics::new("users").with_row_count(100));

        let json = mgr.to_json().unwrap();
        let loaded = StatisticsManager::from_json(&json).unwrap();

        assert_eq!(loaded.get("users").unwrap().row_count, 100);
    }

    #[test]
    fn test_remove_statistics() {
        let mut mgr = StatisticsManager::new();
        mgr.update(TableStatistics::new("users").with_row_count(100));

        assert!(mgr.get("users").is_some());
        mgr.remove("users");
        assert!(mgr.get("users").is_none());
    }

    #[test]
    fn test_default_selectivity() {
        let stats = TableStatistics::new("users").with_row_count(1000);
        // No column stats -> default selectivity
        let sel = stats.selectivity_eq("unknown_col");
        assert!((sel - 0.1).abs() < 0.001);
    }
}
