//! Crash recovery using write-ahead logging.
//!
//! Implements a simplified ARIES-style recovery:
//! 1. Analysis pass: identify transactions in progress
//! 2. Redo pass: replay committed changes
//! 3. Undo pass: rollback uncommitted changes

use std::collections::{HashMap, HashSet};

use crate::Result;
use super::log_record::{LogRecord, LogRecordType, Lsn, TxnId};
use super::log_manager::LogManager;

/// Recovery action to apply.
#[derive(Debug, Clone)]
pub enum RecoveryAction {
    /// Redo an insert.
    RedoInsert { page_id: u32, data: Vec<u8> },
    /// Redo an update.
    RedoUpdate { page_id: u32, data: Vec<u8> },
    /// Redo a delete.
    RedoDelete { page_id: u32 },
    /// Undo an insert (delete it).
    UndoInsert { page_id: u32 },
    /// Undo an update (restore before image).
    UndoUpdate { page_id: u32, data: Vec<u8> },
    /// Undo a delete (restore before image).
    UndoDelete { page_id: u32, data: Vec<u8> },
}

/// Transaction state during recovery.
#[derive(Debug, Clone, Copy, PartialEq)]
enum TxnState {
    InProgress,
    Committed,
    Aborted,
}

/// Recovery manager.
pub struct RecoveryManager {
    /// Transaction states.
    txn_states: HashMap<TxnId, TxnState>,
    /// Last LSN for each transaction.
    txn_last_lsn: HashMap<TxnId, Lsn>,
    /// Actions to redo.
    redo_actions: Vec<RecoveryAction>,
    /// Actions to undo.
    undo_actions: Vec<RecoveryAction>,
}

impl RecoveryManager {
    /// Create a new recovery manager.
    pub fn new() -> Self {
        Self {
            txn_states: HashMap::new(),
            txn_last_lsn: HashMap::new(),
            redo_actions: Vec::new(),
            undo_actions: Vec::new(),
        }
    }

    /// Perform recovery from the log.
    pub fn recover(&mut self, log: &LogManager) -> Result<()> {
        let records = log.read_all()?;

        // Analysis pass
        self.analysis_pass(&records);

        // Redo pass
        self.redo_pass(&records);

        // Undo pass
        self.undo_pass(&records);

        Ok(())
    }

    /// Analysis pass: determine transaction states.
    fn analysis_pass(&mut self, records: &[LogRecord]) {
        for record in records {
            match record.record_type {
                LogRecordType::Begin => {
                    self.txn_states.insert(record.txn_id, TxnState::InProgress);
                }
                LogRecordType::Commit => {
                    self.txn_states.insert(record.txn_id, TxnState::Committed);
                }
                LogRecordType::Abort => {
                    self.txn_states.insert(record.txn_id, TxnState::Aborted);
                }
                _ => {
                    // Track last LSN for each transaction
                    self.txn_last_lsn.insert(record.txn_id, record.lsn);
                }
            }
        }
    }

    /// Redo pass: replay committed transactions.
    fn redo_pass(&mut self, records: &[LogRecord]) {
        for record in records {
            let state = self.txn_states.get(&record.txn_id).copied();

            // Only redo committed transactions
            if state != Some(TxnState::Committed) {
                continue;
            }

            match record.record_type {
                LogRecordType::Insert => {
                    if let (Some(page_id), Some(data)) = (record.page_id, &record.after_image) {
                        self.redo_actions.push(RecoveryAction::RedoInsert {
                            page_id,
                            data: data.clone(),
                        });
                    }
                }
                LogRecordType::Update => {
                    if let (Some(page_id), Some(data)) = (record.page_id, &record.after_image) {
                        self.redo_actions.push(RecoveryAction::RedoUpdate {
                            page_id,
                            data: data.clone(),
                        });
                    }
                }
                LogRecordType::Delete => {
                    if let Some(page_id) = record.page_id {
                        self.redo_actions.push(RecoveryAction::RedoDelete { page_id });
                    }
                }
                _ => {}
            }
        }
    }

    /// Undo pass: rollback uncommitted transactions.
    fn undo_pass(&mut self, records: &[LogRecord]) {
        // Find uncommitted transactions
        let uncommitted: HashSet<TxnId> = self.txn_states.iter()
            .filter(|(_, state)| **state == TxnState::InProgress)
            .map(|(txn_id, _)| *txn_id)
            .collect();

        // Process records in reverse order
        for record in records.iter().rev() {
            if !uncommitted.contains(&record.txn_id) {
                continue;
            }

            match record.record_type {
                LogRecordType::Insert => {
                    if let Some(page_id) = record.page_id {
                        self.undo_actions.push(RecoveryAction::UndoInsert { page_id });
                    }
                }
                LogRecordType::Update => {
                    if let (Some(page_id), Some(data)) = (record.page_id, &record.before_image) {
                        self.undo_actions.push(RecoveryAction::UndoUpdate {
                            page_id,
                            data: data.clone(),
                        });
                    }
                }
                LogRecordType::Delete => {
                    if let (Some(page_id), Some(data)) = (record.page_id, &record.before_image) {
                        self.undo_actions.push(RecoveryAction::UndoDelete {
                            page_id,
                            data: data.clone(),
                        });
                    }
                }
                _ => {}
            }
        }
    }

    /// Get redo actions.
    pub fn redo_actions(&self) -> &[RecoveryAction] {
        &self.redo_actions
    }

    /// Get undo actions.
    pub fn undo_actions(&self) -> &[RecoveryAction] {
        &self.undo_actions
    }

    /// Get committed transaction IDs.
    pub fn committed_txns(&self) -> Vec<TxnId> {
        self.txn_states.iter()
            .filter(|(_, state)| **state == TxnState::Committed)
            .map(|(txn_id, _)| *txn_id)
            .collect()
    }

    /// Get uncommitted transaction IDs.
    pub fn uncommitted_txns(&self) -> Vec<TxnId> {
        self.txn_states.iter()
            .filter(|(_, state)| **state == TxnState::InProgress)
            .map(|(txn_id, _)| *txn_id)
            .collect()
    }
}

impl Default for RecoveryManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    fn create_log() -> (LogManager, NamedTempFile) {
        let tmp = NamedTempFile::new().unwrap();
        let log = LogManager::open(tmp.path()).unwrap();
        (log, tmp)
    }

    #[test]
    fn test_committed_transaction() {
        let (log, _tmp) = create_log();

        // Committed transaction
        log.append(LogRecord::begin(0, 1)).unwrap();
        log.append(LogRecord::insert(0, 1, Some(1), 10, vec![1, 2, 3])).unwrap();
        log.append(LogRecord::commit(0, 1, 2)).unwrap();

        let mut recovery = RecoveryManager::new();
        recovery.recover(&log).unwrap();

        assert_eq!(recovery.committed_txns().len(), 1);
        assert_eq!(recovery.uncommitted_txns().len(), 0);
        assert_eq!(recovery.redo_actions().len(), 1);
        assert_eq!(recovery.undo_actions().len(), 0);
    }

    #[test]
    fn test_uncommitted_transaction() {
        let (log, _tmp) = create_log();

        // Uncommitted transaction
        log.append(LogRecord::begin(0, 1)).unwrap();
        log.append(LogRecord::insert(0, 1, Some(1), 10, vec![1, 2, 3])).unwrap();
        // No commit!

        let mut recovery = RecoveryManager::new();
        recovery.recover(&log).unwrap();

        assert_eq!(recovery.committed_txns().len(), 0);
        assert_eq!(recovery.uncommitted_txns().len(), 1);
        assert_eq!(recovery.redo_actions().len(), 0);
        assert_eq!(recovery.undo_actions().len(), 1);
    }

    #[test]
    fn test_mixed_transactions() {
        let (log, _tmp) = create_log();

        // Transaction 1: committed
        log.append(LogRecord::begin(0, 1)).unwrap();
        log.append(LogRecord::insert(0, 1, Some(1), 10, vec![1])).unwrap();
        log.append(LogRecord::commit(0, 1, 2)).unwrap();

        // Transaction 2: uncommitted
        log.append(LogRecord::begin(0, 2)).unwrap();
        log.append(LogRecord::insert(0, 2, Some(4), 20, vec![2])).unwrap();

        // Transaction 3: aborted
        log.append(LogRecord::begin(0, 3)).unwrap();
        log.append(LogRecord::insert(0, 3, Some(6), 30, vec![3])).unwrap();
        log.append(LogRecord::abort(0, 3, 7)).unwrap();

        let mut recovery = RecoveryManager::new();
        recovery.recover(&log).unwrap();

        assert_eq!(recovery.committed_txns().len(), 1);
        assert_eq!(recovery.uncommitted_txns().len(), 1);
        assert_eq!(recovery.redo_actions().len(), 1);
        assert_eq!(recovery.undo_actions().len(), 1);
    }

    #[test]
    fn test_update_recovery() {
        let (log, _tmp) = create_log();

        // Committed update
        log.append(LogRecord::begin(0, 1)).unwrap();
        log.append(LogRecord::update(0, 1, Some(1), 10, vec![1], vec![2])).unwrap();
        log.append(LogRecord::commit(0, 1, 2)).unwrap();

        let mut recovery = RecoveryManager::new();
        recovery.recover(&log).unwrap();

        assert_eq!(recovery.redo_actions().len(), 1);
        match &recovery.redo_actions()[0] {
            RecoveryAction::RedoUpdate { page_id, data } => {
                assert_eq!(*page_id, 10);
                assert_eq!(data, &vec![2]);
            }
            _ => panic!("expected RedoUpdate"),
        }
    }

    #[test]
    fn test_undo_update() {
        let (log, _tmp) = create_log();

        // Uncommitted update
        log.append(LogRecord::begin(0, 1)).unwrap();
        log.append(LogRecord::update(0, 1, Some(1), 10, vec![1], vec![2])).unwrap();
        // No commit

        let mut recovery = RecoveryManager::new();
        recovery.recover(&log).unwrap();

        assert_eq!(recovery.undo_actions().len(), 1);
        match &recovery.undo_actions()[0] {
            RecoveryAction::UndoUpdate { page_id, data } => {
                assert_eq!(*page_id, 10);
                assert_eq!(data, &vec![1]); // Before image
            }
            _ => panic!("expected UndoUpdate"),
        }
    }

    #[test]
    fn test_empty_log() {
        let (log, _tmp) = create_log();

        let mut recovery = RecoveryManager::new();
        recovery.recover(&log).unwrap();

        assert!(recovery.committed_txns().is_empty());
        assert!(recovery.uncommitted_txns().is_empty());
        assert!(recovery.redo_actions().is_empty());
        assert!(recovery.undo_actions().is_empty());
    }
}
