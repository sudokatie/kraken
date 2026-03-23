//! Transaction manager.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use crate::wal::{LogManager, LogRecord};
use crate::storage::page::PageId;

/// Transaction ID.
pub type TxnId = u64;

/// Transaction state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnState {
    Active,
    Committed,
    Aborted,
}

/// Transaction.
#[derive(Debug)]
pub struct Transaction {
    /// Transaction ID.
    pub id: TxnId,
    /// Current state.
    pub state: TxnState,
    /// Last LSN for this transaction.
    pub last_lsn: Option<u64>,
    /// Pages modified by this transaction.
    pub modified_pages: Vec<PageId>,
}

impl Transaction {
    /// Create a new transaction.
    pub fn new(id: TxnId) -> Self {
        Self {
            id,
            state: TxnState::Active,
            last_lsn: None,
            modified_pages: Vec::new(),
        }
    }

    /// Check if transaction is active.
    pub fn is_active(&self) -> bool {
        self.state == TxnState::Active
    }
}

/// Transaction manager.
pub struct TransactionManager {
    /// Next transaction ID.
    next_txn_id: AtomicU64,
    /// Active transactions.
    active_txns: RwLock<HashMap<TxnId, Transaction>>,
    /// WAL log manager.
    log_manager: Option<Arc<RwLock<LogManager>>>,
}

impl TransactionManager {
    /// Create a new transaction manager.
    pub fn new() -> Self {
        Self {
            next_txn_id: AtomicU64::new(1),
            active_txns: RwLock::new(HashMap::new()),
            log_manager: None,
        }
    }

    /// Create with a log manager for durability.
    pub fn with_log_manager(log_manager: Arc<RwLock<LogManager>>) -> Self {
        Self {
            next_txn_id: AtomicU64::new(1),
            active_txns: RwLock::new(HashMap::new()),
            log_manager: Some(log_manager),
        }
    }

    /// Begin a new transaction.
    pub fn begin(&self) -> TxnId {
        let txn_id = self.next_txn_id.fetch_add(1, Ordering::SeqCst);
        let txn = Transaction::new(txn_id);

        // Log BEGIN record
        if let Some(ref lm) = self.log_manager {
            let lm = lm.write().unwrap();
            let lsn = lm.next_lsn();
            let record = LogRecord::begin(lsn, txn_id);
            let _ = lm.append(record);
        }

        self.active_txns.write().unwrap().insert(txn_id, txn);
        txn_id
    }

    /// Commit a transaction.
    pub fn commit(&self, txn_id: TxnId) -> crate::Result<()> {
        let mut txns = self.active_txns.write().unwrap();
        let txn = txns.get_mut(&txn_id)
            .ok_or_else(|| crate::Error::Internal("transaction not found".into()))?;

        if txn.state != TxnState::Active {
            return Err(crate::Error::Internal("transaction not active".into()));
        }

        // Log COMMIT record
        if let Some(ref lm) = self.log_manager {
            let lm = lm.write().unwrap();
            let lsn = lm.next_lsn();
            let prev_lsn = txn.last_lsn.unwrap_or(0);
            let record = LogRecord::commit(lsn, txn_id, prev_lsn);
            let _ = lm.append(record);
            lm.flush(lsn)?;
        }

        txn.state = TxnState::Committed;
        txns.remove(&txn_id);
        Ok(())
    }

    /// Abort a transaction.
    pub fn abort(&self, txn_id: TxnId) -> crate::Result<()> {
        let mut txns = self.active_txns.write().unwrap();
        let txn = txns.get_mut(&txn_id)
            .ok_or_else(|| crate::Error::Internal("transaction not found".into()))?;

        if txn.state != TxnState::Active {
            return Err(crate::Error::Internal("transaction not active".into()));
        }

        // Log ABORT record
        if let Some(ref lm) = self.log_manager {
            let lm = lm.write().unwrap();
            let lsn = lm.next_lsn();
            let prev_lsn = txn.last_lsn.unwrap_or(0);
            let record = LogRecord::abort(lsn, txn_id, prev_lsn);
            let _ = lm.append(record);
        }

        txn.state = TxnState::Aborted;
        txns.remove(&txn_id);
        Ok(())
    }

    /// Log an insert operation.
    pub fn log_insert(&self, txn_id: TxnId, page_id: PageId, data: Vec<u8>) -> crate::Result<u64> {
        let mut txns = self.active_txns.write().unwrap();
        let txn = txns.get_mut(&txn_id)
            .ok_or_else(|| crate::Error::Internal("transaction not found".into()))?;

        let lsn = if let Some(ref lm) = self.log_manager {
            let lm = lm.write().unwrap();
            let lsn = lm.next_lsn();
            let record = LogRecord::insert(lsn, txn_id, txn.last_lsn, page_id, data);
            lm.append(record)?;
            lsn
        } else {
            0
        };

        txn.last_lsn = Some(lsn);
        txn.modified_pages.push(page_id);
        Ok(lsn)
    }

    /// Log an update operation.
    pub fn log_update(
        &self,
        txn_id: TxnId,
        page_id: PageId,
        before: Vec<u8>,
        after: Vec<u8>,
    ) -> crate::Result<u64> {
        let mut txns = self.active_txns.write().unwrap();
        let txn = txns.get_mut(&txn_id)
            .ok_or_else(|| crate::Error::Internal("transaction not found".into()))?;

        let lsn = if let Some(ref lm) = self.log_manager {
            let lm = lm.write().unwrap();
            let lsn = lm.next_lsn();
            let record = LogRecord::update(lsn, txn_id, txn.last_lsn, page_id, before, after);
            lm.append(record)?;
            lsn
        } else {
            0
        };

        txn.last_lsn = Some(lsn);
        if !txn.modified_pages.contains(&page_id) {
            txn.modified_pages.push(page_id);
        }
        Ok(lsn)
    }

    /// Log a delete operation.
    pub fn log_delete(&self, txn_id: TxnId, page_id: PageId, data: Vec<u8>) -> crate::Result<u64> {
        let mut txns = self.active_txns.write().unwrap();
        let txn = txns.get_mut(&txn_id)
            .ok_or_else(|| crate::Error::Internal("transaction not found".into()))?;

        let lsn = if let Some(ref lm) = self.log_manager {
            let lm = lm.write().unwrap();
            let lsn = lm.next_lsn();
            let record = LogRecord::delete(lsn, txn_id, txn.last_lsn, page_id, data);
            lm.append(record)?;
            lsn
        } else {
            0
        };

        txn.last_lsn = Some(lsn);
        if !txn.modified_pages.contains(&page_id) {
            txn.modified_pages.push(page_id);
        }
        Ok(lsn)
    }

    /// Get active transaction count.
    pub fn active_count(&self) -> usize {
        self.active_txns.read().unwrap().len()
    }

    /// Check if transaction exists and is active.
    pub fn is_active(&self, txn_id: TxnId) -> bool {
        self.active_txns.read().unwrap()
            .get(&txn_id)
            .map(|t| t.is_active())
            .unwrap_or(false)
    }
}

impl Default for TransactionManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_begin_transaction() {
        let tm = TransactionManager::new();
        let txn1 = tm.begin();
        let txn2 = tm.begin();

        assert_eq!(txn1, 1);
        assert_eq!(txn2, 2);
        assert_eq!(tm.active_count(), 2);
    }

    #[test]
    fn test_commit_transaction() {
        let tm = TransactionManager::new();
        let txn = tm.begin();

        assert!(tm.is_active(txn));
        tm.commit(txn).unwrap();
        assert!(!tm.is_active(txn));
        assert_eq!(tm.active_count(), 0);
    }

    #[test]
    fn test_abort_transaction() {
        let tm = TransactionManager::new();
        let txn = tm.begin();

        tm.abort(txn).unwrap();
        assert!(!tm.is_active(txn));
    }

    #[test]
    fn test_commit_nonexistent() {
        let tm = TransactionManager::new();
        assert!(tm.commit(999).is_err());
    }

    #[test]
    fn test_double_commit() {
        let tm = TransactionManager::new();
        let txn = tm.begin();

        tm.commit(txn).unwrap();
        assert!(tm.commit(txn).is_err());
    }
}
