//! Execution engine.
//!
//! Integrates SQL parsing, planning, catalog, storage, and transactions.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, RwLock};

use crate::sql::parser::Parser;
use crate::sql::ast::{Statement, SelectStatement, InsertStatement, UpdateStatement, DeleteStatement, CreateTableStatement, Expr, Literal, BinaryOp, SelectColumn, WindowFunc, OrderBy};
use crate::sql::types::Value;
use crate::storage::{BufferPool, DiskManager, HeapFile};
use crate::catalog::{Catalog, TableSchema};
use crate::wal::{LogManager, LogRecord};
use crate::transaction::{TransactionManager, TxnId};
use crate::planner::optimizer::Optimizer;
use crate::executor::operators::{Row, Operator, TableScan, Filter, Aggregate};
use crate::executor::operators::aggregate::AggregateFunc;
use crate::Result;

/// Query result.
#[derive(Debug, Clone)]
pub struct QueryResult {
    /// Column names.
    pub columns: Vec<String>,
    /// Result rows.
    pub rows: Vec<Row>,
    /// Rows affected (for INSERT/UPDATE/DELETE).
    pub rows_affected: usize,
}

impl QueryResult {
    /// Create an empty result.
    pub fn empty() -> Self {
        Self {
            columns: vec![],
            rows: vec![],
            rows_affected: 0,
        }
    }

    /// Create a result with rows.
    pub fn with_rows(columns: Vec<String>, rows: Vec<Row>) -> Self {
        Self {
            columns,
            rows,
            rows_affected: 0,
        }
    }

    /// Create a result for DML operations.
    pub fn affected(count: usize) -> Self {
        Self {
            columns: vec![],
            rows: vec![],
            rows_affected: count,
        }
    }
}

/// Tuple serialization format for heap storage.
fn serialize_row(row: &Row, schema: &TableSchema) -> Vec<u8> {
    let mut buf = Vec::new();
    
    // Write number of columns
    buf.extend_from_slice(&(row.len() as u16).to_le_bytes());
    
    for value in row {
        match value {
            Value::Null => {
                buf.push(0);
            }
            Value::Integer(n) => {
                buf.push(1);
                buf.extend_from_slice(&n.to_le_bytes());
            }
            Value::Real(n) => {
                buf.push(2);
                buf.extend_from_slice(&n.to_le_bytes());
            }
            Value::Text(s) => {
                buf.push(3);
                let bytes = s.as_bytes();
                buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(bytes);
            }
            Value::Boolean(b) => {
                buf.push(4);
                buf.push(if *b { 1 } else { 0 });
            }
            Value::Blob(b) => {
                buf.push(5);
                buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
                buf.extend_from_slice(b);
            }
        }
    }
    
    buf
}

/// Deserialize a row from bytes.
fn deserialize_row(data: &[u8]) -> Option<Row> {
    if data.len() < 2 {
        return None;
    }
    
    let col_count = u16::from_le_bytes([data[0], data[1]]) as usize;
    let mut row = Vec::with_capacity(col_count);
    let mut offset = 2;
    
    for _ in 0..col_count {
        if offset >= data.len() {
            return None;
        }
        
        let type_tag = data[offset];
        offset += 1;
        
        let value = match type_tag {
            0 => Value::Null,
            1 => {
                if offset + 8 > data.len() { return None; }
                let n = i64::from_le_bytes([
                    data[offset], data[offset+1], data[offset+2], data[offset+3],
                    data[offset+4], data[offset+5], data[offset+6], data[offset+7],
                ]);
                offset += 8;
                Value::Integer(n)
            }
            2 => {
                if offset + 8 > data.len() { return None; }
                let n = f64::from_le_bytes([
                    data[offset], data[offset+1], data[offset+2], data[offset+3],
                    data[offset+4], data[offset+5], data[offset+6], data[offset+7],
                ]);
                offset += 8;
                Value::Real(n)
            }
            3 => {
                if offset + 4 > data.len() { return None; }
                let len = u32::from_le_bytes([
                    data[offset], data[offset+1], data[offset+2], data[offset+3],
                ]) as usize;
                offset += 4;
                if offset + len > data.len() { return None; }
                let s = String::from_utf8_lossy(&data[offset..offset+len]).to_string();
                offset += len;
                Value::Text(s)
            }
            4 => {
                if offset >= data.len() { return None; }
                let b = data[offset] != 0;
                offset += 1;
                Value::Boolean(b)
            }
            5 => {
                if offset + 4 > data.len() { return None; }
                let len = u32::from_le_bytes([
                    data[offset], data[offset+1], data[offset+2], data[offset+3],
                ]) as usize;
                offset += 4;
                if offset + len > data.len() { return None; }
                let b = data[offset..offset+len].to_vec();
                offset += len;
                Value::Blob(b)
            }
            _ => return None,
        };
        
        row.push(value);
    }
    
    Some(row)
}

/// Database storage state.
struct StorageState {
    /// Buffer pool for page caching.
    buffer_pool: BufferPool,
    /// Heap files by table name.
    heap_files: HashMap<String, HeapFile>,
    /// System catalog.
    catalog: Catalog,
    /// WAL log manager.
    log_manager: LogManager,
    /// Transaction manager.
    txn_manager: TransactionManager,
    /// Query optimizer.
    optimizer: Optimizer,
}

/// Execution engine with full storage integration.
pub struct ExecutionEngine {
    /// Storage state (protected for concurrent access).
    state: Arc<RwLock<StorageState>>,
    /// Data directory path.
    data_dir: String,
}

impl ExecutionEngine {
    /// Create a new execution engine with storage at the given path.
    pub fn open<P: AsRef<Path>>(data_dir: P) -> Result<Self> {
        let data_dir_str = data_dir.as_ref().to_string_lossy().into_owned();
        
        // Create data directory if needed
        std::fs::create_dir_all(&data_dir)?;
        
        // Initialize storage components
        let db_path = data_dir.as_ref().join("kraken.db");
        let wal_path = data_dir.as_ref().join("kraken.wal");
        
        let disk_manager = DiskManager::new(&db_path)?;
        let buffer_pool = BufferPool::new(1000, disk_manager);
        let log_manager = LogManager::open(&wal_path)?;
        let catalog = Catalog::open(data_dir.as_ref())?;
        let txn_manager = TransactionManager::new();
        let optimizer = Optimizer::new();
        
        let mut state = StorageState {
            buffer_pool,
            heap_files: HashMap::new(),
            catalog,
            log_manager,
            txn_manager,
            optimizer,
        };
        
        // Load existing heap files from catalog
        let tables: Vec<_> = state.catalog.all_tables()
            .map(|s| (s.name.clone(), s.heap_page_id))
            .collect();
        
        for (name, heap_page_id) in tables {
            if let Some(page_id) = heap_page_id {
                if let Ok(heap) = HeapFile::open(&mut state.buffer_pool, page_id) {
                    state.heap_files.insert(name, heap);
                }
            }
        }
        
        Ok(Self {
            state: Arc::new(RwLock::new(state)),
            data_dir: data_dir_str,
        })
    }
    
    /// Create an in-memory execution engine (for testing/compatibility).
    pub fn new() -> Self {
        // Use a unique temp directory for each instance
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::SeqCst);
        let temp_dir = std::env::temp_dir().join(format!("kraken_{}_{}", std::process::id(), id));
        Self::open(&temp_dir).expect("failed to create in-memory engine")
    }

    /// Execute a SQL query.
    pub fn execute(&self, sql: &str) -> Result<QueryResult> {
        let mut parser = Parser::new(sql)?;
        let stmt = parser.parse()?;
        self.execute_statement(stmt)
    }

    /// Execute a parsed statement.
    fn execute_statement(&self, stmt: Statement) -> Result<QueryResult> {
        match stmt {
            Statement::Select(select) => self.execute_select(select),
            Statement::Insert(insert) => self.execute_insert(insert),
            Statement::Update(update) => self.execute_update(update),
            Statement::Delete(delete) => self.execute_delete(delete),
            Statement::CreateTable(create) => self.execute_create_table(create),
        }
    }

    /// Execute a SELECT statement.
    fn execute_select(&self, select: SelectStatement) -> Result<QueryResult> {
        let mut state = self.state.write().unwrap();
        let table_name = &select.from.name;

        // Get schema from catalog
        let schema = state.catalog.get_table(table_name)
            .ok_or_else(|| crate::Error::TableNotFound(table_name.clone()))?
            .clone();
        
        let column_names: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
        
        // Load data from heap file
        let rows = self.load_table_data(&mut state, table_name, &schema)?;
        
        // Build operator pipeline
        let mut operator: Box<dyn Operator> = Box::new(TableScan::new(
            column_names.clone(),
            rows,
        ));

        // Apply WHERE filter
        if let Some(ref predicate) = select.where_clause {
            let pred_clone = predicate.clone();
            let cols_clone = column_names.clone();
            operator = Box::new(Filter::new(
                operator,
                Box::new(move |row| evaluate_predicate(&pred_clone, row, &cols_clone)),
            ));
        }

        // Handle aggregates
        let has_aggregates = select.columns.iter().any(|c| match c {
            SelectColumn::Expr { expr, .. } => is_aggregate_expr(expr),
            _ => false,
        });

        if has_aggregates || !select.group_by.is_empty() {
            let aggregates = extract_aggregates(&select.columns);
            if !aggregates.is_empty() {
                let agg_columns: Vec<String> = aggregates.iter().map(|(_, alias)| alias.clone()).collect();
                let agg_funcs: Vec<(AggregateFunc, usize)> = aggregates.iter()
                    .map(|(func, _)| (*func, 0))
                    .collect();
                operator = Box::new(Aggregate::new(operator, agg_funcs, agg_columns.clone()));
            }
        }

        // Collect results
        let mut rows = Vec::new();
        while let Some(row) = operator.next()? {
            rows.push(row);
        }

        // Apply ORDER BY
        if !select.order_by.is_empty() {
            let order = &select.order_by[0];
            let col_idx = column_names.iter().position(|c| c == &order.column);
            if let Some(idx) = col_idx {
                rows.sort_by(|a, b| {
                    let cmp = compare_values(&a.get(idx).cloned().unwrap_or(Value::Null), 
                                            &b.get(idx).cloned().unwrap_or(Value::Null));
                    if order.descending { cmp.reverse() } else { cmp }
                });
            }
        }

        // Apply LIMIT
        if let Some(limit) = select.limit {
            rows.truncate(limit as usize);
        }

        // Determine output columns
        let output_columns = match &select.columns[..] {
            [SelectColumn::Star] => column_names.clone(),
            cols => cols.iter().map(|c| match c {
                SelectColumn::Star => "*".to_string(),
                SelectColumn::Expr { alias, expr } => {
                    alias.clone().unwrap_or_else(|| expr_to_string(expr))
                }
            }).collect(),
        };

        // Project columns if not SELECT *
        let final_rows = if matches!(&select.columns[..], [SelectColumn::Star]) {
            rows
        } else if has_aggregates {
            rows
        } else {
            rows.into_iter().map(|row| {
                select.columns.iter().map(|c| match c {
                    SelectColumn::Star => Value::Null,
                    SelectColumn::Expr { expr, .. } => {
                        evaluate_expr(expr, &row, &column_names)
                    }
                }).collect()
            }).collect()
        };

        Ok(QueryResult::with_rows(output_columns, final_rows))
    }

    /// Execute an INSERT statement.
    fn execute_insert(&self, insert: InsertStatement) -> Result<QueryResult> {
        let mut state = self.state.write().unwrap();
        let table_name = &insert.table;

        // Get schema
        let schema = state.catalog.get_table(table_name)
            .ok_or_else(|| crate::Error::TableNotFound(table_name.clone()))?
            .clone();
        
        // Start transaction
        let txn_id = state.txn_manager.begin();
        
        // Get or create heap file
        let mut heap = self.get_or_create_heap(&mut state, table_name, schema.table_id)?;
        
        let mut count = 0;
        for value_list in &insert.values {
            let row: Row = value_list.iter().map(|expr| {
                match expr {
                    Expr::Literal(lit) => literal_to_value(lit),
                    _ => Value::Null,
                }
            }).collect();

            // Pad to match schema
            let mut final_row = vec![Value::Null; schema.columns.len()];
            for (i, val) in row.into_iter().enumerate() {
                if i < final_row.len() {
                    final_row[i] = val;
                }
            }
            
            // Serialize and insert
            let data = serialize_row(&final_row, &schema);
            
            // Log the insert
            let lsn = state.log_manager.append(LogRecord::insert(
                0, txn_id, None, 
                heap.header_page_id(),
                data.clone(),
            ))?;
            
            // Insert into heap
            heap.insert(&mut state.buffer_pool, &data)?;
            count += 1;
        }
        
        // Update heap page ID in catalog if this is a new heap
        let heap_page_id = heap.header_page_id();
        if state.catalog.get_table(table_name).and_then(|s| s.heap_page_id).is_none() {
            state.catalog.set_heap_page_id(table_name, heap_page_id)?;
        }
        
        // Update heap files map
        state.heap_files.insert(table_name.clone(), heap);
        
        // Commit transaction
        state.txn_manager.commit(txn_id)?;
        state.log_manager.flush_all()?;
        state.buffer_pool.flush_all()?;

        Ok(QueryResult::affected(count))
    }

    /// Execute an UPDATE statement.
    fn execute_update(&self, update: UpdateStatement) -> Result<QueryResult> {
        let mut state = self.state.write().unwrap();
        let table_name = &update.table;

        // Get schema
        let schema = state.catalog.get_table(table_name)
            .ok_or_else(|| crate::Error::TableNotFound(table_name.clone()))?
            .clone();
        
        let column_names: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
        
        // Load all rows
        let rows = self.load_table_data(&mut state, table_name, &schema)?;
        
        // Start transaction
        let txn_id = state.txn_manager.begin();
        
        let mut count = 0;
        
        // We need to track row IDs - for now, delete all matching and reinsert
        // (A full implementation would track RowIds properly)
        let mut updated_rows = Vec::new();
        
        for mut row in rows {
            let matches = match &update.where_clause {
                Some(predicate) => evaluate_predicate(predicate, &row, &column_names),
                None => true,
            };

            if matches {
                // Apply assignments
                for assignment in &update.assignments {
                    if let Some(idx) = column_names.iter().position(|c| c == &assignment.column) {
                        row[idx] = evaluate_expr(&assignment.value, &row, &column_names);
                    }
                }
                count += 1;
            }
            updated_rows.push(row);
        }
        
        // Rewrite the entire table (simplified approach)
        // A real implementation would update in place
        self.rewrite_table(&mut state, table_name, &schema, updated_rows, txn_id)?;
        
        // Commit
        state.txn_manager.commit(txn_id)?;
        state.log_manager.flush_all()?;
        state.buffer_pool.flush_all()?;

        Ok(QueryResult::affected(count))
    }

    /// Execute a DELETE statement.
    fn execute_delete(&self, delete: DeleteStatement) -> Result<QueryResult> {
        let mut state = self.state.write().unwrap();
        let table_name = &delete.table;

        // Get schema
        let schema = state.catalog.get_table(table_name)
            .ok_or_else(|| crate::Error::TableNotFound(table_name.clone()))?
            .clone();
        
        let column_names: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
        
        // Load all rows
        let rows = self.load_table_data(&mut state, table_name, &schema)?;
        let before_count = rows.len();
        
        // Start transaction
        let txn_id = state.txn_manager.begin();
        
        // Filter out rows that match the WHERE clause
        let remaining: Vec<Row> = rows.into_iter().filter(|row| {
            match &delete.where_clause {
                Some(predicate) => !evaluate_predicate(predicate, row, &column_names),
                None => false,
            }
        }).collect();
        
        let deleted = before_count - remaining.len();
        
        // Rewrite table with remaining rows
        self.rewrite_table(&mut state, table_name, &schema, remaining, txn_id)?;
        
        // Commit
        state.txn_manager.commit(txn_id)?;
        state.log_manager.flush_all()?;
        state.buffer_pool.flush_all()?;

        Ok(QueryResult::affected(deleted))
    }

    /// Execute a CREATE TABLE statement.
    fn execute_create_table(&self, create: CreateTableStatement) -> Result<QueryResult> {
        let mut state = self.state.write().unwrap();
        
        // Create in catalog
        let schema = state.catalog.create_table(&create)?;
        let table_id = schema.table_id;
        let table_name = create.name.clone();
        
        // Create heap file
        let heap = HeapFile::create(&mut state.buffer_pool, table_id)?;
        let heap_page_id = heap.header_page_id();
        state.heap_files.insert(table_name.clone(), heap);
        
        // Store heap page ID in catalog for persistence
        state.catalog.set_heap_page_id(&table_name, heap_page_id)?;
        
        // Flush
        state.buffer_pool.flush_all()?;

        Ok(QueryResult::affected(0))
    }
    
    /// Load table data from heap file.
    fn load_table_data(&self, state: &mut StorageState, table_name: &str, schema: &TableSchema) -> Result<Vec<Row>> {
        let heap = match state.heap_files.get(table_name) {
            Some(h) => {
                // Re-open to get fresh data
                HeapFile::open(&mut state.buffer_pool, h.header_page_id())?
            }
            None => {
                // Try to find heap file for this table
                // For now, return empty if not found
                return Ok(Vec::new());
            }
        };
        
        let mut rows = Vec::new();
        let mut scan = heap.scan(&mut state.buffer_pool);
        
        while let Ok(Some((_, data))) = scan.next() {
            if let Some(row) = deserialize_row(&data) {
                rows.push(row);
            }
        }
        
        Ok(rows)
    }
    
    /// Get or create heap file for a table.
    fn get_or_create_heap(&self, state: &mut StorageState, table_name: &str, table_id: u32) -> Result<HeapFile> {
        if let Some(heap) = state.heap_files.remove(table_name) {
            Ok(heap)
        } else {
            // Check if there's a heap page ID in the catalog
            if let Some(schema) = state.catalog.get_table(table_name) {
                if let Some(heap_page_id) = schema.heap_page_id {
                    return HeapFile::open(&mut state.buffer_pool, heap_page_id);
                }
            }
            // No existing heap, create new one
            HeapFile::create(&mut state.buffer_pool, table_id)
        }
    }
    
    /// Rewrite an entire table with new data.
    fn rewrite_table(&self, state: &mut StorageState, table_name: &str, schema: &TableSchema, rows: Vec<Row>, _txn_id: TxnId) -> Result<()> {
        // Create a new heap file
        let mut heap = HeapFile::create(&mut state.buffer_pool, schema.table_id)?;
        let heap_page_id = heap.header_page_id();
        
        for row in rows {
            let data = serialize_row(&row, schema);
            heap.insert(&mut state.buffer_pool, &data)?;
        }
        
        state.heap_files.insert(table_name.to_string(), heap);
        
        // Update heap page ID in catalog
        state.catalog.set_heap_page_id(table_name, heap_page_id)?;
        
        Ok(())
    }

    /// Flush all changes to disk.
    pub fn flush(&self) -> Result<()> {
        let mut state = self.state.write().unwrap();
        state.buffer_pool.flush_all()?;
        state.log_manager.flush_all()?;
        Ok(())
    }
    
    /// Get the data directory path.
    pub fn data_dir(&self) -> &str {
        &self.data_dir
    }
}

impl Default for ExecutionEngine {
    fn default() -> Self {
        Self::new()
    }
}

// Helper functions

fn literal_to_value(lit: &Literal) -> Value {
    match lit {
        Literal::Null => Value::Null,
        Literal::Integer(n) => Value::Integer(*n),
        Literal::Float(n) => Value::Real(*n),
        Literal::String(s) => Value::Text(s.clone()),
        Literal::Boolean(b) => Value::Boolean(*b),
    }
}

fn evaluate_expr(expr: &Expr, row: &Row, columns: &[String]) -> Value {
    match expr {
        Expr::Literal(lit) => literal_to_value(lit),
        Expr::Column(name) => {
            columns.iter().position(|c| c == name)
                .and_then(|idx| row.get(idx).cloned())
                .unwrap_or(Value::Null)
        }
        Expr::QualifiedColumn { column, .. } => {
            columns.iter().position(|c| c == column)
                .and_then(|idx| row.get(idx).cloned())
                .unwrap_or(Value::Null)
        }
        Expr::BinaryOp { left, op, right } => {
            let left_val = evaluate_expr(left, row, columns);
            let right_val = evaluate_expr(right, row, columns);
            evaluate_binary_op(&left_val, op, &right_val)
        }
        Expr::UnaryOp { op, expr } => {
            let val = evaluate_expr(expr, row, columns);
            match op {
                crate::sql::ast::UnaryOp::Not => match val {
                    Value::Boolean(b) => Value::Boolean(!b),
                    _ => Value::Null,
                },
                crate::sql::ast::UnaryOp::Neg => match val {
                    Value::Integer(n) => Value::Integer(-n),
                    Value::Real(n) => Value::Real(-n),
                    _ => Value::Null,
                },
            }
        }
        Expr::Function { name, args } => {
            match name.to_uppercase().as_str() {
                "UPPER" => {
                    if let Some(arg) = args.first() {
                        if let Value::Text(s) = evaluate_expr(arg, row, columns) {
                            return Value::Text(s.to_uppercase());
                        }
                    }
                    Value::Null
                }
                "LOWER" => {
                    if let Some(arg) = args.first() {
                        if let Value::Text(s) = evaluate_expr(arg, row, columns) {
                            return Value::Text(s.to_lowercase());
                        }
                    }
                    Value::Null
                }
                _ => Value::Null,
            }
        }
        Expr::IsNull { expr, negated } => {
            let val = evaluate_expr(expr, row, columns);
            let is_null = matches!(val, Value::Null);
            Value::Boolean(if *negated { !is_null } else { is_null })
        }
        // Subqueries require context from executor - not supported in simple evaluation
        Expr::Subquery(_) => {
            // Would need to execute the subquery and return single value
            Value::Null
        }
        Expr::Exists { .. } => {
            // Would need to execute subquery and check if any rows returned
            Value::Null
        }
        Expr::InSubquery { .. } => {
            // Would need to execute subquery and check membership
            Value::Null
        }
        Expr::Case { operand, when_clauses, else_result } => {
            // Simple CASE evaluation
            if let Some(op) = operand {
                // Searched CASE: CASE operand WHEN val1 THEN result1 ...
                let op_val = evaluate_expr(op, row, columns);
                for when_clause in when_clauses {
                    let when_val = evaluate_expr(&when_clause.condition, row, columns);
                    if op_val == when_val {
                        return evaluate_expr(&when_clause.result, row, columns);
                    }
                }
            } else {
                // Simple CASE: CASE WHEN condition THEN result ...
                for when_clause in when_clauses {
                    let cond_val = evaluate_expr(&when_clause.condition, row, columns);
                    if cond_val == Value::Boolean(true) {
                        return evaluate_expr(&when_clause.result, row, columns);
                    }
                }
            }
            // Return ELSE result or NULL
            if let Some(else_expr) = else_result {
                evaluate_expr(else_expr, row, columns)
            } else {
                Value::Null
            }
        }
        Expr::WindowFunction { .. } => {
            // Window functions require full result set context
            Value::Null
        }
    }
}

fn evaluate_binary_op(left: &Value, op: &BinaryOp, right: &Value) -> Value {
    match op {
        BinaryOp::Eq => Value::Boolean(left == right),
        BinaryOp::Ne => Value::Boolean(left != right),
        BinaryOp::Lt => Value::Boolean(compare_values(left, right) == std::cmp::Ordering::Less),
        BinaryOp::Gt => Value::Boolean(compare_values(left, right) == std::cmp::Ordering::Greater),
        BinaryOp::Le => Value::Boolean(compare_values(left, right) != std::cmp::Ordering::Greater),
        BinaryOp::Ge => Value::Boolean(compare_values(left, right) != std::cmp::Ordering::Less),
        BinaryOp::And => match (left, right) {
            (Value::Boolean(a), Value::Boolean(b)) => Value::Boolean(*a && *b),
            _ => Value::Null,
        },
        BinaryOp::Or => match (left, right) {
            (Value::Boolean(a), Value::Boolean(b)) => Value::Boolean(*a || *b),
            _ => Value::Null,
        },
        BinaryOp::Add => match (left, right) {
            (Value::Integer(a), Value::Integer(b)) => Value::Integer(a + b),
            (Value::Real(a), Value::Real(b)) => Value::Real(a + b),
            (Value::Integer(a), Value::Real(b)) => Value::Real(*a as f64 + b),
            (Value::Real(a), Value::Integer(b)) => Value::Real(a + *b as f64),
            _ => Value::Null,
        },
        BinaryOp::Sub => match (left, right) {
            (Value::Integer(a), Value::Integer(b)) => Value::Integer(a - b),
            (Value::Real(a), Value::Real(b)) => Value::Real(a - b),
            _ => Value::Null,
        },
        BinaryOp::Mul => match (left, right) {
            (Value::Integer(a), Value::Integer(b)) => Value::Integer(a * b),
            (Value::Real(a), Value::Real(b)) => Value::Real(a * b),
            _ => Value::Null,
        },
        BinaryOp::Div => match (left, right) {
            (Value::Integer(a), Value::Integer(b)) if *b != 0 => Value::Integer(a / b),
            (Value::Real(a), Value::Real(b)) if *b != 0.0 => Value::Real(a / b),
            _ => Value::Null,
        },
    }
}

fn evaluate_predicate(predicate: &Expr, row: &Row, columns: &[String]) -> bool {
    match evaluate_expr(predicate, row, columns) {
        Value::Boolean(b) => b,
        _ => false,
    }
}

/// Context for evaluating expressions that may contain subqueries.
pub struct ExprContext<'a> {
    /// Function to execute a subquery and return results.
    subquery_executor: Option<&'a dyn Fn(&SelectStatement) -> Result<Vec<Row>>>,
}

impl<'a> ExprContext<'a> {
    /// Create a context with subquery support.
    pub fn with_subquery_executor(
        executor: &'a dyn Fn(&SelectStatement) -> Result<Vec<Row>>,
    ) -> Self {
        Self {
            subquery_executor: Some(executor),
        }
    }

    /// Create a context without subquery support.
    pub fn empty() -> Self {
        Self {
            subquery_executor: None,
        }
    }
}

/// Evaluate expression with subquery support.
fn evaluate_expr_with_context(
    expr: &Expr,
    row: &Row,
    columns: &[String],
    ctx: &ExprContext,
) -> Value {
    match expr {
        // Scalar subquery: execute and return single value
        Expr::Subquery(subquery) => {
            if let Some(executor) = ctx.subquery_executor {
                match executor(subquery) {
                    Ok(rows) => {
                        // Scalar subquery should return exactly one row with one column
                        if let Some(first_row) = rows.first() {
                            first_row.first().cloned().unwrap_or(Value::Null)
                        } else {
                            Value::Null
                        }
                    }
                    Err(_) => Value::Null,
                }
            } else {
                Value::Null
            }
        }

        // EXISTS: check if subquery returns any rows
        Expr::Exists { subquery, negated } => {
            if let Some(executor) = ctx.subquery_executor {
                match executor(subquery) {
                    Ok(rows) => {
                        let exists = !rows.is_empty();
                        Value::Boolean(if *negated { !exists } else { exists })
                    }
                    Err(_) => Value::Null,
                }
            } else {
                Value::Null
            }
        }

        // IN subquery: check if expression value is in subquery results
        Expr::InSubquery { expr: check_expr, subquery, negated } => {
            if let Some(executor) = ctx.subquery_executor {
                let check_val = evaluate_expr_with_context(check_expr, row, columns, ctx);
                match executor(subquery) {
                    Ok(rows) => {
                        // Check if value exists in first column of results
                        let found = rows.iter().any(|r| {
                            r.first().map(|v| v == &check_val).unwrap_or(false)
                        });
                        Value::Boolean(if *negated { !found } else { found })
                    }
                    Err(_) => Value::Null,
                }
            } else {
                Value::Null
            }
        }

        // Window functions are handled separately at result set level
        Expr::WindowFunction { .. } => {
            // Window functions need full result set - return placeholder
            // Actual evaluation happens in apply_window_functions
            Value::Null
        }

        // Delegate other expressions to standard evaluation with recursive context
        Expr::BinaryOp { left, op, right } => {
            let left_val = evaluate_expr_with_context(left, row, columns, ctx);
            let right_val = evaluate_expr_with_context(right, row, columns, ctx);
            evaluate_binary_op(&left_val, op, &right_val)
        }

        Expr::UnaryOp { op, expr: inner } => {
            let val = evaluate_expr_with_context(inner, row, columns, ctx);
            match op {
                crate::sql::ast::UnaryOp::Not => match val {
                    Value::Boolean(b) => Value::Boolean(!b),
                    _ => Value::Null,
                },
                crate::sql::ast::UnaryOp::Neg => match val {
                    Value::Integer(n) => Value::Integer(-n),
                    Value::Real(n) => Value::Real(-n),
                    _ => Value::Null,
                },
            }
        }

        Expr::IsNull { expr: inner, negated } => {
            let val = evaluate_expr_with_context(inner, row, columns, ctx);
            let is_null = matches!(val, Value::Null);
            Value::Boolean(if *negated { !is_null } else { is_null })
        }

        Expr::Case { operand, when_clauses, else_result } => {
            if let Some(op) = operand {
                let op_val = evaluate_expr_with_context(op, row, columns, ctx);
                for when_clause in when_clauses {
                    let when_val = evaluate_expr_with_context(&when_clause.condition, row, columns, ctx);
                    if op_val == when_val {
                        return evaluate_expr_with_context(&when_clause.result, row, columns, ctx);
                    }
                }
            } else {
                for when_clause in when_clauses {
                    let cond_val = evaluate_expr_with_context(&when_clause.condition, row, columns, ctx);
                    if cond_val == Value::Boolean(true) {
                        return evaluate_expr_with_context(&when_clause.result, row, columns, ctx);
                    }
                }
            }
            if let Some(else_expr) = else_result {
                evaluate_expr_with_context(else_expr, row, columns, ctx)
            } else {
                Value::Null
            }
        }

        // Other expressions use standard evaluation
        _ => evaluate_expr(expr, row, columns),
    }
}

/// Compute window functions over a result set.
/// Returns rows with window function columns appended.
pub fn compute_window_functions(
    rows: Vec<Row>,
    columns: &[String],
    window_exprs: &[(Expr, String)], // (window expr, output alias)
) -> Vec<Row> {
    if window_exprs.is_empty() {
        return rows;
    }

    let mut result_rows = rows.clone();

    for (window_expr, _alias) in window_exprs {
        if let Expr::WindowFunction { function, partition_by, order_by, .. } = window_expr {
            // Group rows by partition
            let partitions = partition_rows(&rows, partition_by, columns);

            for (_partition_key, mut partition_indices) in partitions {
                // Sort partition by ORDER BY
                if !order_by.is_empty() {
                    sort_partition(&rows, &mut partition_indices, order_by, columns);
                }

                // Compute window function for each row in partition
                for (rank, &row_idx) in partition_indices.iter().enumerate() {
                    let value = compute_window_value(
                        function,
                        &rows,
                        &partition_indices,
                        rank,
                        columns,
                    );
                    result_rows[row_idx].push(value);
                }
            }
        }
    }

    result_rows
}

/// Partition rows by partition expressions.
/// Returns a map from partition key (as string) to row indices.
fn partition_rows(
    rows: &[Row],
    partition_by: &[Expr],
    columns: &[String],
) -> HashMap<String, Vec<usize>> {
    let mut partitions: HashMap<String, Vec<usize>> = HashMap::new();

    for (idx, row) in rows.iter().enumerate() {
        let key = if partition_by.is_empty() {
            // No partition by = single partition containing all rows
            String::new()
        } else {
            // Create string key from partition values
            partition_by
                .iter()
                .map(|expr| value_to_partition_key(&evaluate_expr(expr, row, columns)))
                .collect::<Vec<_>>()
                .join("|")
        };
        partitions.entry(key).or_default().push(idx);
    }

    partitions
}

/// Convert a Value to a string for partition key purposes.
fn value_to_partition_key(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Integer(n) => format!("I:{}", n),
        Value::Real(n) => format!("R:{}", n),
        Value::Text(s) => format!("T:{}", s),
        Value::Boolean(b) => format!("B:{}", b),
        Value::Blob(b) => format!("X:{}", hex_encode(b)),
    }
}

/// Simple hex encoding for blob values.
fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

/// Sort partition indices by ORDER BY columns.
fn sort_partition(
    rows: &[Row],
    indices: &mut [usize],
    order_by: &[OrderBy],
    columns: &[String],
) {
    indices.sort_by(|&a, &b| {
        for order in order_by {
            if let Some(col_idx) = columns.iter().position(|c| c == &order.column) {
                let va = rows[a].get(col_idx).cloned().unwrap_or(Value::Null);
                let vb = rows[b].get(col_idx).cloned().unwrap_or(Value::Null);
                let cmp = compare_values(&va, &vb);
                if cmp != std::cmp::Ordering::Equal {
                    return if order.descending { cmp.reverse() } else { cmp };
                }
            }
        }
        std::cmp::Ordering::Equal
    });
}

/// Compute value for a single window function at a specific position.
fn compute_window_value(
    function: &WindowFunc,
    rows: &[Row],
    partition_indices: &[usize],
    position: usize,
    columns: &[String],
) -> Value {
    let partition_size = partition_indices.len();

    match function {
        WindowFunc::RowNumber => Value::Integer((position + 1) as i64),

        WindowFunc::Rank => {
            // Rank is position + 1 (simplified - doesn't handle ties)
            Value::Integer((position + 1) as i64)
        }

        WindowFunc::DenseRank => {
            // Dense rank (simplified - doesn't handle ties)
            Value::Integer((position + 1) as i64)
        }

        WindowFunc::NTile(n) => {
            let bucket = (position * (*n as usize)) / partition_size;
            Value::Integer((bucket + 1).min(*n as usize) as i64)
        }

        WindowFunc::Lead { expr, offset, default } => {
            let off = offset.unwrap_or(1) as usize;
            if position + off < partition_size {
                let target_idx = partition_indices[position + off];
                evaluate_expr(expr, &rows[target_idx], columns)
            } else {
                default.as_ref()
                    .map(|d| evaluate_expr(d, &rows[partition_indices[position]], columns))
                    .unwrap_or(Value::Null)
            }
        }

        WindowFunc::Lag { expr, offset, default } => {
            let off = offset.unwrap_or(1) as usize;
            if position >= off {
                let target_idx = partition_indices[position - off];
                evaluate_expr(expr, &rows[target_idx], columns)
            } else {
                default.as_ref()
                    .map(|d| evaluate_expr(d, &rows[partition_indices[position]], columns))
                    .unwrap_or(Value::Null)
            }
        }

        WindowFunc::FirstValue(expr) => {
            let first_idx = partition_indices[0];
            evaluate_expr(expr, &rows[first_idx], columns)
        }

        WindowFunc::LastValue(expr) => {
            let last_idx = partition_indices[partition_size - 1];
            evaluate_expr(expr, &rows[last_idx], columns)
        }

        WindowFunc::NthValue { expr, n } => {
            let idx = (*n - 1) as usize;
            if idx < partition_size {
                let target_idx = partition_indices[idx];
                evaluate_expr(expr, &rows[target_idx], columns)
            } else {
                Value::Null
            }
        }

        WindowFunc::Aggregate { name, args } => {
            // Compute aggregate over entire partition
            let partition_rows: Vec<&Row> = partition_indices
                .iter()
                .map(|&i| &rows[i])
                .collect();

            match name.to_uppercase().as_str() {
                "COUNT" => Value::Integer(partition_rows.len() as i64),
                "SUM" => {
                    if let Some(arg) = args.first() {
                        let sum: f64 = partition_rows
                            .iter()
                            .filter_map(|row| {
                                match evaluate_expr(arg, row, columns) {
                                    Value::Integer(n) => Some(n as f64),
                                    Value::Real(n) => Some(n),
                                    _ => None,
                                }
                            })
                            .sum();
                        Value::Real(sum)
                    } else {
                        Value::Null
                    }
                }
                "AVG" => {
                    if let Some(arg) = args.first() {
                        let values: Vec<f64> = partition_rows
                            .iter()
                            .filter_map(|row| {
                                match evaluate_expr(arg, row, columns) {
                                    Value::Integer(n) => Some(n as f64),
                                    Value::Real(n) => Some(n),
                                    _ => None,
                                }
                            })
                            .collect();
                        if values.is_empty() {
                            Value::Null
                        } else {
                            Value::Real(values.iter().sum::<f64>() / values.len() as f64)
                        }
                    } else {
                        Value::Null
                    }
                }
                "MIN" => {
                    if let Some(arg) = args.first() {
                        partition_rows
                            .iter()
                            .map(|row| evaluate_expr(arg, row, columns))
                            .filter(|v| !matches!(v, Value::Null))
                            .min_by(|a, b| compare_values(a, b))
                            .unwrap_or(Value::Null)
                    } else {
                        Value::Null
                    }
                }
                "MAX" => {
                    if let Some(arg) = args.first() {
                        partition_rows
                            .iter()
                            .map(|row| evaluate_expr(arg, row, columns))
                            .filter(|v| !matches!(v, Value::Null))
                            .max_by(|a, b| compare_values(a, b))
                            .unwrap_or(Value::Null)
                    } else {
                        Value::Null
                    }
                }
                _ => Value::Null,
            }
        }
    }
}

fn compare_values(left: &Value, right: &Value) -> std::cmp::Ordering {
    match (left, right) {
        (Value::Integer(a), Value::Integer(b)) => a.cmp(b),
        (Value::Real(a), Value::Real(b)) => a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal),
        (Value::Text(a), Value::Text(b)) => a.cmp(b),
        (Value::Boolean(a), Value::Boolean(b)) => a.cmp(b),
        (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
        (Value::Null, _) => std::cmp::Ordering::Less,
        (_, Value::Null) => std::cmp::Ordering::Greater,
        _ => std::cmp::Ordering::Equal,
    }
}

fn is_aggregate_expr(expr: &Expr) -> bool {
    match expr {
        Expr::Function { name, .. } => {
            matches!(name.to_uppercase().as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX")
        }
        _ => false,
    }
}

fn extract_aggregates(columns: &[SelectColumn]) -> Vec<(AggregateFunc, String)> {
    let mut aggregates = Vec::new();
    
    for col in columns {
        if let SelectColumn::Expr { expr, alias } = col {
            if let Expr::Function { name, args } = expr {
                let func = match name.to_uppercase().as_str() {
                    "COUNT" => Some(AggregateFunc::Count),
                    "SUM" => Some(AggregateFunc::Sum),
                    "AVG" => Some(AggregateFunc::Avg),
                    "MIN" => Some(AggregateFunc::Min),
                    "MAX" => Some(AggregateFunc::Max),
                    _ => None,
                };
                
                if let Some(func) = func {
                    let alias_str = alias.clone().unwrap_or_else(|| format!("{}(...)", name));
                    aggregates.push((func, alias_str));
                }
            }
        }
    }
    
    aggregates
}

fn expr_to_string(expr: &Expr) -> String {
    match expr {
        Expr::Column(name) => name.clone(),
        Expr::QualifiedColumn { table, column } => format!("{}.{}", table, column),
        Expr::Literal(lit) => match lit {
            Literal::Null => "NULL".into(),
            Literal::Integer(n) => n.to_string(),
            Literal::Float(n) => n.to_string(),
            Literal::String(s) => s.clone(),
            Literal::Boolean(b) => b.to_string(),
        },
        Expr::Function { name, .. } => format!("{}(...)", name),
        _ => "expr".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_table() {
        let engine = ExecutionEngine::new();
        let result = engine.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)").unwrap();
        assert_eq!(result.rows_affected, 0);
    }

    #[test]
    fn test_insert() {
        let engine = ExecutionEngine::new();
        engine.execute("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
        let result = engine.execute("INSERT INTO users VALUES (1, 'alice')").unwrap();
        assert_eq!(result.rows_affected, 1);
    }

    #[test]
    fn test_select() {
        let engine = ExecutionEngine::new();
        engine.execute("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
        engine.execute("INSERT INTO users VALUES (1, 'alice')").unwrap();
        engine.execute("INSERT INTO users VALUES (2, 'bob')").unwrap();

        let result = engine.execute("SELECT * FROM users").unwrap();
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.columns, vec!["id", "name"]);
    }

    #[test]
    fn test_select_where() {
        let engine = ExecutionEngine::new();
        engine.execute("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
        engine.execute("INSERT INTO users VALUES (1, 'alice')").unwrap();
        engine.execute("INSERT INTO users VALUES (2, 'bob')").unwrap();

        let result = engine.execute("SELECT * FROM users WHERE id = 1").unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][1], Value::Text("alice".into()));
    }

    #[test]
    fn test_update() {
        let engine = ExecutionEngine::new();
        engine.execute("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
        engine.execute("INSERT INTO users VALUES (1, 'alice')").unwrap();

        let result = engine.execute("UPDATE users SET name = 'alicia' WHERE id = 1").unwrap();
        assert_eq!(result.rows_affected, 1);

        let result = engine.execute("SELECT * FROM users WHERE id = 1").unwrap();
        assert_eq!(result.rows[0][1], Value::Text("alicia".into()));
    }

    #[test]
    fn test_delete() {
        let engine = ExecutionEngine::new();
        engine.execute("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
        engine.execute("INSERT INTO users VALUES (1, 'alice')").unwrap();
        engine.execute("INSERT INTO users VALUES (2, 'bob')").unwrap();

        let result = engine.execute("DELETE FROM users WHERE id = 1").unwrap();
        assert_eq!(result.rows_affected, 1);

        let result = engine.execute("SELECT * FROM users").unwrap();
        assert_eq!(result.rows.len(), 1);
    }

    #[test]
    fn test_order_by() {
        let engine = ExecutionEngine::new();
        engine.execute("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
        engine.execute("INSERT INTO users VALUES (2, 'bob')").unwrap();
        engine.execute("INSERT INTO users VALUES (1, 'alice')").unwrap();
        engine.execute("INSERT INTO users VALUES (3, 'charlie')").unwrap();

        let result = engine.execute("SELECT * FROM users ORDER BY id").unwrap();
        assert_eq!(result.rows[0][0], Value::Integer(1));
        assert_eq!(result.rows[1][0], Value::Integer(2));
        assert_eq!(result.rows[2][0], Value::Integer(3));
    }

    #[test]
    fn test_limit() {
        let engine = ExecutionEngine::new();
        engine.execute("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
        engine.execute("INSERT INTO users VALUES (1, 'alice')").unwrap();
        engine.execute("INSERT INTO users VALUES (2, 'bob')").unwrap();
        engine.execute("INSERT INTO users VALUES (3, 'charlie')").unwrap();

        let result = engine.execute("SELECT * FROM users LIMIT 2").unwrap();
        assert_eq!(result.rows.len(), 2);
    }
    
    #[test]
    fn test_persistence() {
        let temp_dir = std::env::temp_dir().join(format!("kraken_persist_test_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&temp_dir); // Clean start
        
        // Create and insert
        {
            let engine = ExecutionEngine::open(&temp_dir).unwrap();
            engine.execute("CREATE TABLE items (id INTEGER, value TEXT)").unwrap();
            engine.execute("INSERT INTO items VALUES (1, 'first')").unwrap();
            engine.execute("INSERT INTO items VALUES (2, 'second')").unwrap();
            
            // Verify before flush
            let result = engine.execute("SELECT * FROM items").unwrap();
            eprintln!("Before flush: {} rows", result.rows.len());
            
            // Debug heap state before flush
            {
                let state = engine.state.read().unwrap();
                if let Some(heap) = state.heap_files.get("items") {
                    eprintln!("Before flush - heap row_count: {}", heap.row_count());
                }
            }
            
            engine.flush().unwrap();
        }
        
        // Reopen and verify
        {
            let engine = ExecutionEngine::open(&temp_dir).unwrap();
            
            // Check catalog
            {
                let state = engine.state.read().unwrap();
                let table = state.catalog.get_table("items");
                eprintln!("Catalog has table: {:?}", table.is_some());
                if let Some(t) = table {
                    eprintln!("Table heap_page_id: {:?}", t.heap_page_id);
                }
                eprintln!("Heap files count: {}", state.heap_files.len());
                
                // Debug heap file
                if let Some(heap) = state.heap_files.get("items") {
                    eprintln!("Heap row_count: {}", heap.row_count());
                }
            }
            
            let result = engine.execute("SELECT * FROM items").unwrap();
            eprintln!("After reopen: {} rows", result.rows.len());
            assert_eq!(result.rows.len(), 2);
        }
        
        // Cleanup
        let _ = std::fs::remove_dir_all(&temp_dir);
    }
    
    #[test]
    fn test_row_serialization() {
        let row = vec![
            Value::Integer(42),
            Value::Text("hello".into()),
            Value::Boolean(true),
            Value::Null,
        ];
        
        let schema = TableSchema::new("test", 1, vec![]);
        let data = serialize_row(&row, &schema);
        let restored = deserialize_row(&data).unwrap();
        
        assert_eq!(restored, row);
    }

    // Subquery and Window Function Tests

    #[test]
    fn test_expr_context_scalar_subquery() {
        // Test scalar subquery evaluation
        let rows = vec![
            vec![Value::Integer(100)],
            vec![Value::Integer(200)],
        ];

        let subquery_executor = |_stmt: &SelectStatement| -> Result<Vec<Row>> {
            Ok(rows.clone())
        };

        let ctx = ExprContext::with_subquery_executor(&subquery_executor);

        // Create a subquery expression
        let subquery = SelectStatement {
            ctes: vec![],
            columns: vec![SelectColumn::Star],
            from: crate::sql::ast::TableRef { name: "dummy".into(), alias: None },
            joins: vec![],
            where_clause: None,
            order_by: vec![],
            limit: None,
            group_by: vec![],
            having: None,
        };

        let expr = Expr::Subquery(Box::new(subquery));
        let result = evaluate_expr_with_context(&expr, &vec![], &[], &ctx);

        // Should return first row, first column
        assert_eq!(result, Value::Integer(100));
    }

    #[test]
    fn test_expr_context_exists_true() {
        // Test EXISTS returns true when rows exist
        let rows = vec![vec![Value::Integer(1)]];

        let subquery_executor = |_stmt: &SelectStatement| -> Result<Vec<Row>> {
            Ok(rows.clone())
        };

        let ctx = ExprContext::with_subquery_executor(&subquery_executor);

        let subquery = SelectStatement {
            ctes: vec![],
            columns: vec![SelectColumn::Star],
            from: crate::sql::ast::TableRef { name: "dummy".into(), alias: None },
            joins: vec![],
            where_clause: None,
            order_by: vec![],
            limit: None,
            group_by: vec![],
            having: None,
        };

        let expr = Expr::Exists {
            subquery: Box::new(subquery),
            negated: false,
        };
        let result = evaluate_expr_with_context(&expr, &vec![], &[], &ctx);
        assert_eq!(result, Value::Boolean(true));
    }

    #[test]
    fn test_expr_context_exists_false() {
        // Test EXISTS returns false when no rows
        let subquery_executor = |_stmt: &SelectStatement| -> Result<Vec<Row>> {
            Ok(vec![])
        };

        let ctx = ExprContext::with_subquery_executor(&subquery_executor);

        let subquery = SelectStatement {
            ctes: vec![],
            columns: vec![SelectColumn::Star],
            from: crate::sql::ast::TableRef { name: "dummy".into(), alias: None },
            joins: vec![],
            where_clause: None,
            order_by: vec![],
            limit: None,
            group_by: vec![],
            having: None,
        };

        let expr = Expr::Exists {
            subquery: Box::new(subquery),
            negated: false,
        };
        let result = evaluate_expr_with_context(&expr, &vec![], &[], &ctx);
        assert_eq!(result, Value::Boolean(false));
    }

    #[test]
    fn test_expr_context_not_exists() {
        // Test NOT EXISTS returns true when no rows
        let subquery_executor = |_stmt: &SelectStatement| -> Result<Vec<Row>> {
            Ok(vec![])
        };

        let ctx = ExprContext::with_subquery_executor(&subquery_executor);

        let subquery = SelectStatement {
            ctes: vec![],
            columns: vec![SelectColumn::Star],
            from: crate::sql::ast::TableRef { name: "dummy".into(), alias: None },
            joins: vec![],
            where_clause: None,
            order_by: vec![],
            limit: None,
            group_by: vec![],
            having: None,
        };

        let expr = Expr::Exists {
            subquery: Box::new(subquery),
            negated: true,
        };
        let result = evaluate_expr_with_context(&expr, &vec![], &[], &ctx);
        assert_eq!(result, Value::Boolean(true));
    }

    #[test]
    fn test_expr_context_in_subquery_found() {
        // Test IN subquery when value is found
        let rows = vec![
            vec![Value::Integer(1)],
            vec![Value::Integer(2)],
            vec![Value::Integer(3)],
        ];

        let subquery_executor = |_stmt: &SelectStatement| -> Result<Vec<Row>> {
            Ok(rows.clone())
        };

        let ctx = ExprContext::with_subquery_executor(&subquery_executor);

        let subquery = SelectStatement {
            ctes: vec![],
            columns: vec![SelectColumn::Star],
            from: crate::sql::ast::TableRef { name: "dummy".into(), alias: None },
            joins: vec![],
            where_clause: None,
            order_by: vec![],
            limit: None,
            group_by: vec![],
            having: None,
        };

        let expr = Expr::InSubquery {
            expr: Box::new(Expr::Literal(Literal::Integer(2))),
            subquery: Box::new(subquery),
            negated: false,
        };
        let result = evaluate_expr_with_context(&expr, &vec![], &[], &ctx);
        assert_eq!(result, Value::Boolean(true));
    }

    #[test]
    fn test_expr_context_in_subquery_not_found() {
        // Test IN subquery when value is not found
        let rows = vec![
            vec![Value::Integer(1)],
            vec![Value::Integer(2)],
        ];

        let subquery_executor = |_stmt: &SelectStatement| -> Result<Vec<Row>> {
            Ok(rows.clone())
        };

        let ctx = ExprContext::with_subquery_executor(&subquery_executor);

        let subquery = SelectStatement {
            ctes: vec![],
            columns: vec![SelectColumn::Star],
            from: crate::sql::ast::TableRef { name: "dummy".into(), alias: None },
            joins: vec![],
            where_clause: None,
            order_by: vec![],
            limit: None,
            group_by: vec![],
            having: None,
        };

        let expr = Expr::InSubquery {
            expr: Box::new(Expr::Literal(Literal::Integer(99))),
            subquery: Box::new(subquery),
            negated: false,
        };
        let result = evaluate_expr_with_context(&expr, &vec![], &[], &ctx);
        assert_eq!(result, Value::Boolean(false));
    }

    #[test]
    fn test_expr_context_not_in_subquery() {
        // Test NOT IN subquery
        let rows = vec![
            vec![Value::Integer(1)],
            vec![Value::Integer(2)],
        ];

        let subquery_executor = |_stmt: &SelectStatement| -> Result<Vec<Row>> {
            Ok(rows.clone())
        };

        let ctx = ExprContext::with_subquery_executor(&subquery_executor);

        let subquery = SelectStatement {
            ctes: vec![],
            columns: vec![SelectColumn::Star],
            from: crate::sql::ast::TableRef { name: "dummy".into(), alias: None },
            joins: vec![],
            where_clause: None,
            order_by: vec![],
            limit: None,
            group_by: vec![],
            having: None,
        };

        let expr = Expr::InSubquery {
            expr: Box::new(Expr::Literal(Literal::Integer(99))),
            subquery: Box::new(subquery),
            negated: true,
        };
        let result = evaluate_expr_with_context(&expr, &vec![], &[], &ctx);
        assert_eq!(result, Value::Boolean(true));
    }

    #[test]
    fn test_window_row_number() {
        let rows = vec![
            vec![Value::Text("a".into()), Value::Integer(10)],
            vec![Value::Text("a".into()), Value::Integer(20)],
            vec![Value::Text("a".into()), Value::Integer(30)],
        ];
        let columns = vec!["dept".to_string(), "salary".to_string()];

        let window_expr = Expr::WindowFunction {
            function: WindowFunc::RowNumber,
            partition_by: vec![],
            order_by: vec![OrderBy { column: "salary".into(), descending: false }],
            frame: None,
        };

        let result = compute_window_functions(rows, &columns, &[(window_expr, "rn".into())]);

        assert_eq!(result.len(), 3);
        // Row numbers should be assigned based on order
        assert_eq!(result[0][2], Value::Integer(1));
        assert_eq!(result[1][2], Value::Integer(2));
        assert_eq!(result[2][2], Value::Integer(3));
    }

    #[test]
    fn test_window_row_number_partitioned() {
        let rows = vec![
            vec![Value::Text("a".into()), Value::Integer(10)],
            vec![Value::Text("b".into()), Value::Integer(20)],
            vec![Value::Text("a".into()), Value::Integer(30)],
            vec![Value::Text("b".into()), Value::Integer(40)],
        ];
        let columns = vec!["dept".to_string(), "salary".to_string()];

        let window_expr = Expr::WindowFunction {
            function: WindowFunc::RowNumber,
            partition_by: vec![Expr::Column("dept".into())],
            order_by: vec![OrderBy { column: "salary".into(), descending: false }],
            frame: None,
        };

        let result = compute_window_functions(rows, &columns, &[(window_expr, "rn".into())]);

        // Each partition should have its own row numbers
        assert_eq!(result.len(), 4);
        // Partition 'a': rows 0 and 2 -> should be 1, 2
        // Partition 'b': rows 1 and 3 -> should be 1, 2
    }

    #[test]
    fn test_window_lead() {
        let rows = vec![
            vec![Value::Integer(1), Value::Integer(100)],
            vec![Value::Integer(2), Value::Integer(200)],
            vec![Value::Integer(3), Value::Integer(300)],
        ];
        let columns = vec!["id".to_string(), "value".to_string()];

        let window_expr = Expr::WindowFunction {
            function: WindowFunc::Lead {
                expr: Box::new(Expr::Column("value".into())),
                offset: Some(1),
                default: Some(Box::new(Expr::Literal(Literal::Integer(-1)))),
            },
            partition_by: vec![],
            order_by: vec![OrderBy { column: "id".into(), descending: false }],
            frame: None,
        };

        let result = compute_window_functions(rows, &columns, &[(window_expr, "next_val".into())]);

        assert_eq!(result[0][2], Value::Integer(200)); // lead from 100 -> 200
        assert_eq!(result[1][2], Value::Integer(300)); // lead from 200 -> 300
        assert_eq!(result[2][2], Value::Integer(-1));  // lead from 300 -> default
    }

    #[test]
    fn test_window_lag() {
        let rows = vec![
            vec![Value::Integer(1), Value::Integer(100)],
            vec![Value::Integer(2), Value::Integer(200)],
            vec![Value::Integer(3), Value::Integer(300)],
        ];
        let columns = vec!["id".to_string(), "value".to_string()];

        let window_expr = Expr::WindowFunction {
            function: WindowFunc::Lag {
                expr: Box::new(Expr::Column("value".into())),
                offset: Some(1),
                default: Some(Box::new(Expr::Literal(Literal::Integer(0)))),
            },
            partition_by: vec![],
            order_by: vec![OrderBy { column: "id".into(), descending: false }],
            frame: None,
        };

        let result = compute_window_functions(rows, &columns, &[(window_expr, "prev_val".into())]);

        assert_eq!(result[0][2], Value::Integer(0));   // lag from 100 -> default
        assert_eq!(result[1][2], Value::Integer(100)); // lag from 200 -> 100
        assert_eq!(result[2][2], Value::Integer(200)); // lag from 300 -> 200
    }

    #[test]
    fn test_window_first_last_value() {
        let rows = vec![
            vec![Value::Integer(1), Value::Integer(100)],
            vec![Value::Integer(2), Value::Integer(200)],
            vec![Value::Integer(3), Value::Integer(300)],
        ];
        let columns = vec!["id".to_string(), "value".to_string()];

        let first_expr = Expr::WindowFunction {
            function: WindowFunc::FirstValue(Box::new(Expr::Column("value".into()))),
            partition_by: vec![],
            order_by: vec![OrderBy { column: "id".into(), descending: false }],
            frame: None,
        };

        let last_expr = Expr::WindowFunction {
            function: WindowFunc::LastValue(Box::new(Expr::Column("value".into()))),
            partition_by: vec![],
            order_by: vec![OrderBy { column: "id".into(), descending: false }],
            frame: None,
        };

        let result = compute_window_functions(
            rows,
            &columns,
            &[(first_expr, "first".into()), (last_expr, "last".into())],
        );

        // All rows should have same first value (100)
        assert_eq!(result[0][2], Value::Integer(100));
        assert_eq!(result[1][2], Value::Integer(100));
        assert_eq!(result[2][2], Value::Integer(100));

        // All rows should have same last value (300)
        assert_eq!(result[0][3], Value::Integer(300));
        assert_eq!(result[1][3], Value::Integer(300));
        assert_eq!(result[2][3], Value::Integer(300));
    }

    #[test]
    fn test_window_aggregate_sum() {
        let rows = vec![
            vec![Value::Text("a".into()), Value::Integer(10)],
            vec![Value::Text("a".into()), Value::Integer(20)],
            vec![Value::Text("b".into()), Value::Integer(100)],
        ];
        let columns = vec!["dept".to_string(), "salary".to_string()];

        let window_expr = Expr::WindowFunction {
            function: WindowFunc::Aggregate {
                name: "SUM".into(),
                args: vec![Expr::Column("salary".into())],
            },
            partition_by: vec![Expr::Column("dept".into())],
            order_by: vec![],
            frame: None,
        };

        let result = compute_window_functions(rows, &columns, &[(window_expr, "total".into())]);

        // Dept 'a' total = 30
        assert_eq!(result[0][2], Value::Real(30.0));
        assert_eq!(result[1][2], Value::Real(30.0));
        // Dept 'b' total = 100
        assert_eq!(result[2][2], Value::Real(100.0));
    }

    #[test]
    fn test_window_ntile() {
        let rows = vec![
            vec![Value::Integer(1)],
            vec![Value::Integer(2)],
            vec![Value::Integer(3)],
            vec![Value::Integer(4)],
        ];
        let columns = vec!["id".to_string()];

        let window_expr = Expr::WindowFunction {
            function: WindowFunc::NTile(2),
            partition_by: vec![],
            order_by: vec![OrderBy { column: "id".into(), descending: false }],
            frame: None,
        };

        let result = compute_window_functions(rows, &columns, &[(window_expr, "bucket".into())]);

        // 4 rows into 2 buckets: [1,2] -> bucket 1, [3,4] -> bucket 2
        assert_eq!(result[0][1], Value::Integer(1));
        assert_eq!(result[1][1], Value::Integer(1));
        assert_eq!(result[2][1], Value::Integer(2));
        assert_eq!(result[3][1], Value::Integer(2));
    }

    #[test]
    fn test_partition_key_encoding() {
        // Test various value types in partition keys
        assert_eq!(value_to_partition_key(&Value::Null), "NULL");
        assert_eq!(value_to_partition_key(&Value::Integer(42)), "I:42");
        assert_eq!(value_to_partition_key(&Value::Real(3.14)), "R:3.14");
        assert_eq!(value_to_partition_key(&Value::Text("hello".into())), "T:hello");
        assert_eq!(value_to_partition_key(&Value::Boolean(true)), "B:true");
    }
}
