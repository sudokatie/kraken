//! System catalog for schema management.

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter};
use std::path::{Path, PathBuf};

use crate::Result;
use crate::sql::ast::CreateTableStatement;
use super::schema::{TableSchema, ColumnDef, ColumnType};

/// System catalog.
/// 
/// Manages table schemas and provides lookup operations.
/// Persists schemas to a JSON file in the data directory.
pub struct Catalog {
    /// Path to catalog file.
    path: PathBuf,
    /// Table schemas by name.
    tables: HashMap<String, TableSchema>,
    /// Next table ID.
    next_table_id: u32,
}

impl Catalog {
    /// Create or load a catalog from the given directory.
    pub fn open(data_dir: &Path) -> Result<Self> {
        let path = data_dir.join("catalog.json");

        if path.exists() {
            Self::load(&path)
        } else {
            // Create data directory if needed
            fs::create_dir_all(data_dir)?;
            Ok(Self {
                path,
                tables: HashMap::new(),
                next_table_id: 1,
            })
        }
    }

    /// Load catalog from file.
    fn load(path: &Path) -> Result<Self> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let data: CatalogData = serde_json::from_reader(reader)
            .map_err(|e| crate::Error::Internal(e.to_string()))?;

        let mut tables = HashMap::new();
        for schema in data.tables {
            tables.insert(schema.name.clone(), schema);
        }

        Ok(Self {
            path: path.to_path_buf(),
            tables,
            next_table_id: data.next_table_id,
        })
    }

    /// Save catalog to file.
    pub fn save(&self) -> Result<()> {
        let data = CatalogData {
            tables: self.tables.values().cloned().collect(),
            next_table_id: self.next_table_id,
        };

        let file = File::create(&self.path)?;
        let writer = BufWriter::new(file);
        serde_json::to_writer_pretty(writer, &data)
            .map_err(|e| crate::Error::Internal(e.to_string()))?;

        Ok(())
    }

    /// Create a new table from a CREATE TABLE statement.
    pub fn create_table(&mut self, stmt: &CreateTableStatement) -> Result<&TableSchema> {
        if self.tables.contains_key(&stmt.name) {
            return Err(crate::Error::TableExists(stmt.name.clone()));
        }

        let table_id = self.next_table_id;
        self.next_table_id += 1;

        let columns: Vec<ColumnDef> = stmt.columns.iter().enumerate()
            .map(|(i, col)| {
                ColumnDef {
                    name: col.name.clone(),
                    ordinal: i,
                    data_type: ColumnType::from(col.data_type.clone()),
                    nullable: !col.not_null && !col.primary_key,
                    primary_key: col.primary_key,
                }
            })
            .collect();

        let schema = TableSchema::new(&stmt.name, table_id, columns);
        self.tables.insert(stmt.name.clone(), schema);
        self.save()?;

        Ok(self.tables.get(&stmt.name).unwrap())
    }

    /// Get a table schema by name.
    pub fn get_table(&self, name: &str) -> Option<&TableSchema> {
        self.tables.get(name)
    }

    /// Check if a table exists.
    pub fn table_exists(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }

    /// Drop a table by name.
    pub fn drop_table(&mut self, name: &str) -> Result<()> {
        if self.tables.remove(name).is_none() {
            return Err(crate::Error::TableNotFound(name.to_string()));
        }
        self.save()?;
        Ok(())
    }
    
    /// Update heap page ID for a table.
    pub fn set_heap_page_id(&mut self, table_name: &str, page_id: u32) -> Result<()> {
        if let Some(schema) = self.tables.get_mut(table_name) {
            schema.heap_page_id = Some(page_id);
            self.save()?;
            Ok(())
        } else {
            Err(crate::Error::TableNotFound(table_name.to_string()))
        }
    }

    /// List all table names.
    pub fn list_tables(&self) -> Vec<&str> {
        self.tables.keys().map(|s| s.as_str()).collect()
    }

    /// Get all table schemas.
    pub fn all_tables(&self) -> impl Iterator<Item = &TableSchema> {
        self.tables.values()
    }
}

/// Serializable catalog data.
#[derive(serde::Serialize, serde::Deserialize)]
struct CatalogData {
    tables: Vec<TableSchema>,
    next_table_id: u32,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::ast::{ColumnDef as AstColumnDef, DataType};
    use tempfile::TempDir;

    fn create_test_catalog() -> (Catalog, TempDir) {
        let tmp = TempDir::new().unwrap();
        let catalog = Catalog::open(tmp.path()).unwrap();
        (catalog, tmp)
    }

    #[test]
    fn test_create_table() {
        let (mut catalog, _tmp) = create_test_catalog();

        let stmt = CreateTableStatement {
            name: "users".into(),
            columns: vec![
                AstColumnDef {
                    name: "id".into(),
                    data_type: DataType::Integer,
                    primary_key: true,
                    not_null: false,
                },
                AstColumnDef {
                    name: "name".into(),
                    data_type: DataType::Text,
                    primary_key: false,
                    not_null: true,
                },
            ],
        };

        let schema = catalog.create_table(&stmt).unwrap();
        assert_eq!(schema.name, "users");
        assert_eq!(schema.columns.len(), 2);
        assert!(schema.columns[0].primary_key);
        assert!(!schema.columns[1].nullable);
    }

    #[test]
    fn test_get_table() {
        let (mut catalog, _tmp) = create_test_catalog();

        let stmt = CreateTableStatement {
            name: "test".into(),
            columns: vec![AstColumnDef {
                name: "id".into(),
                data_type: DataType::Integer,
                primary_key: true,
                not_null: false,
            }],
        };

        catalog.create_table(&stmt).unwrap();

        assert!(catalog.get_table("test").is_some());
        assert!(catalog.get_table("nonexistent").is_none());
    }

    #[test]
    fn test_list_tables() {
        let (mut catalog, _tmp) = create_test_catalog();

        for name in ["users", "orders", "products"] {
            let stmt = CreateTableStatement {
                name: name.into(),
                columns: vec![AstColumnDef {
                    name: "id".into(),
                    data_type: DataType::Integer,
                    primary_key: true,
                    not_null: false,
                }],
            };
            catalog.create_table(&stmt).unwrap();
        }

        let tables = catalog.list_tables();
        assert_eq!(tables.len(), 3);
    }

    #[test]
    fn test_drop_table() {
        let (mut catalog, _tmp) = create_test_catalog();

        let stmt = CreateTableStatement {
            name: "temp".into(),
            columns: vec![AstColumnDef {
                name: "id".into(),
                data_type: DataType::Integer,
                primary_key: true,
                not_null: false,
            }],
        };

        catalog.create_table(&stmt).unwrap();
        assert!(catalog.table_exists("temp"));

        catalog.drop_table("temp").unwrap();
        assert!(!catalog.table_exists("temp"));
    }

    #[test]
    fn test_duplicate_table_error() {
        let (mut catalog, _tmp) = create_test_catalog();

        let stmt = CreateTableStatement {
            name: "dup".into(),
            columns: vec![AstColumnDef {
                name: "id".into(),
                data_type: DataType::Integer,
                primary_key: true,
                not_null: false,
            }],
        };

        catalog.create_table(&stmt).unwrap();
        assert!(catalog.create_table(&stmt).is_err());
    }

    #[test]
    fn test_persistence() {
        let tmp = TempDir::new().unwrap();

        // Create catalog and add table
        {
            let mut catalog = Catalog::open(tmp.path()).unwrap();
            let stmt = CreateTableStatement {
                name: "persistent".into(),
                columns: vec![AstColumnDef {
                    name: "id".into(),
                    data_type: DataType::Integer,
                    primary_key: true,
                    not_null: false,
                }],
            };
            catalog.create_table(&stmt).unwrap();
        }

        // Reopen and verify
        {
            let catalog = Catalog::open(tmp.path()).unwrap();
            assert!(catalog.table_exists("persistent"));
            let schema = catalog.get_table("persistent").unwrap();
            assert_eq!(schema.columns.len(), 1);
        }
    }

    #[test]
    fn test_column_lookup() {
        let (mut catalog, _tmp) = create_test_catalog();

        let stmt = CreateTableStatement {
            name: "lookup".into(),
            columns: vec![
                AstColumnDef {
                    name: "id".into(),
                    data_type: DataType::Integer,
                    primary_key: true,
                    not_null: false,
                },
                AstColumnDef {
                    name: "value".into(),
                    data_type: DataType::Real,
                    primary_key: false,
                    not_null: false,
                },
            ],
        };

        catalog.create_table(&stmt).unwrap();
        let schema = catalog.get_table("lookup").unwrap();

        assert!(schema.column("id").is_some());
        assert!(schema.column("value").is_some());
        assert!(schema.column("missing").is_none());
        assert_eq!(schema.column_index("value"), Some(1));
    }
}
