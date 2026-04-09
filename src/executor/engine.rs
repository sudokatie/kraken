//! Execution engine.
//!
//! Integrates SQL parsing, planning, catalog, storage, and transactions.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, RwLock};

use crate::sql::parser::Parser;
use crate::sql::ast::{Statement, SelectStatement, InsertStatement, UpdateStatement, DeleteStatement, CreateTableStatement, Expr, Literal, BinaryOp, SelectColumn};
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
}
