//! WAL record types.

use serde::{Deserialize, Serialize};
use crate::storage::page::PageId;

/// Log Sequence Number.
pub type Lsn = u64;

/// Transaction ID.
pub type TxnId = u64;

/// Log record types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LogRecordType {
    /// Transaction begin.
    Begin,
    /// Transaction commit.
    Commit,
    /// Transaction abort.
    Abort,
    /// Checkpoint record.
    Checkpoint,
    /// Page insert.
    Insert,
    /// Page update.
    Update,
    /// Page delete.
    Delete,
}

/// Log record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogRecord {
    /// Log sequence number.
    pub lsn: Lsn,
    /// Transaction ID.
    pub txn_id: TxnId,
    /// Previous LSN for this transaction.
    pub prev_lsn: Option<Lsn>,
    /// Record type.
    pub record_type: LogRecordType,
    /// Page ID (for data records).
    pub page_id: Option<PageId>,
    /// Before image (for undo).
    pub before_image: Option<Vec<u8>>,
    /// After image (for redo).
    pub after_image: Option<Vec<u8>>,
}

impl LogRecord {
    /// Create a BEGIN record.
    pub fn begin(lsn: Lsn, txn_id: TxnId) -> Self {
        Self {
            lsn,
            txn_id,
            prev_lsn: None,
            record_type: LogRecordType::Begin,
            page_id: None,
            before_image: None,
            after_image: None,
        }
    }

    /// Create a COMMIT record.
    pub fn commit(lsn: Lsn, txn_id: TxnId, prev_lsn: Lsn) -> Self {
        Self {
            lsn,
            txn_id,
            prev_lsn: Some(prev_lsn),
            record_type: LogRecordType::Commit,
            page_id: None,
            before_image: None,
            after_image: None,
        }
    }

    /// Create an ABORT record.
    pub fn abort(lsn: Lsn, txn_id: TxnId, prev_lsn: Lsn) -> Self {
        Self {
            lsn,
            txn_id,
            prev_lsn: Some(prev_lsn),
            record_type: LogRecordType::Abort,
            page_id: None,
            before_image: None,
            after_image: None,
        }
    }

    /// Create an INSERT record.
    pub fn insert(
        lsn: Lsn,
        txn_id: TxnId,
        prev_lsn: Option<Lsn>,
        page_id: PageId,
        after_image: Vec<u8>,
    ) -> Self {
        Self {
            lsn,
            txn_id,
            prev_lsn,
            record_type: LogRecordType::Insert,
            page_id: Some(page_id),
            before_image: None,
            after_image: Some(after_image),
        }
    }

    /// Create an UPDATE record.
    pub fn update(
        lsn: Lsn,
        txn_id: TxnId,
        prev_lsn: Option<Lsn>,
        page_id: PageId,
        before_image: Vec<u8>,
        after_image: Vec<u8>,
    ) -> Self {
        Self {
            lsn,
            txn_id,
            prev_lsn,
            record_type: LogRecordType::Update,
            page_id: Some(page_id),
            before_image: Some(before_image),
            after_image: Some(after_image),
        }
    }

    /// Create a DELETE record.
    pub fn delete(
        lsn: Lsn,
        txn_id: TxnId,
        prev_lsn: Option<Lsn>,
        page_id: PageId,
        before_image: Vec<u8>,
    ) -> Self {
        Self {
            lsn,
            txn_id,
            prev_lsn,
            record_type: LogRecordType::Delete,
            page_id: Some(page_id),
            before_image: Some(before_image),
            after_image: None,
        }
    }

    /// Create a CHECKPOINT record.
    pub fn checkpoint(lsn: Lsn) -> Self {
        Self {
            lsn,
            txn_id: 0,
            prev_lsn: None,
            record_type: LogRecordType::Checkpoint,
            page_id: None,
            before_image: None,
            after_image: None,
        }
    }

    /// Serialize to bytes.
    pub fn serialize(&self) -> Vec<u8> {
        bincode::serialize(self).expect("serialization failed")
    }

    /// Deserialize from bytes.
    pub fn deserialize(data: &[u8]) -> Option<Self> {
        bincode::deserialize(data).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_begin_record() {
        let record = LogRecord::begin(1, 100);
        assert_eq!(record.lsn, 1);
        assert_eq!(record.txn_id, 100);
        assert_eq!(record.record_type, LogRecordType::Begin);
    }

    #[test]
    fn test_commit_record() {
        let record = LogRecord::commit(5, 100, 4);
        assert_eq!(record.prev_lsn, Some(4));
        assert_eq!(record.record_type, LogRecordType::Commit);
    }

    #[test]
    fn test_insert_record() {
        let data = vec![1, 2, 3, 4];
        let record = LogRecord::insert(2, 100, Some(1), 42, data.clone());
        assert_eq!(record.page_id, Some(42));
        assert_eq!(record.after_image, Some(data));
    }

    #[test]
    fn test_serialize_deserialize() {
        let record = LogRecord::begin(1, 100);
        let bytes = record.serialize();
        let restored = LogRecord::deserialize(&bytes).unwrap();

        assert_eq!(restored.lsn, 1);
        assert_eq!(restored.txn_id, 100);
    }

    #[test]
    fn test_update_record() {
        let before = vec![1, 2, 3];
        let after = vec![4, 5, 6];
        let record = LogRecord::update(3, 100, Some(2), 10, before.clone(), after.clone());

        assert_eq!(record.before_image, Some(before));
        assert_eq!(record.after_image, Some(after));
    }
}
