//! MVCC (Multi-Version Concurrency Control) implementation.
//!
//! Provides snapshot isolation for concurrent readers and writers.

use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;

/// Version timestamp.
pub type Timestamp = u64;

/// Row identifier.
pub type RowId = u64;

/// A versioned value.
#[derive(Debug, Clone)]
pub struct Version<T> {
    /// Transaction that created this version.
    pub created_by: Timestamp,
    /// Transaction that deleted this version (None if active).
    pub deleted_by: Option<Timestamp>,
    /// The actual value.
    pub value: T,
}

impl<T> Version<T> {
    /// Create a new version.
    pub fn new(created_by: Timestamp, value: T) -> Self {
        Self {
            created_by,
            deleted_by: None,
            value,
        }
    }

    /// Check if this version is visible to a snapshot.
    pub fn is_visible(&self, snapshot: &Snapshot) -> bool {
        // Created before snapshot and not deleted (or deleted after snapshot)
        if self.created_by > snapshot.timestamp {
            return false;
        }
        if snapshot.active_txns.contains(&self.created_by) {
            return false;
        }
        match self.deleted_by {
            None => true,
            Some(deleted) => {
                deleted > snapshot.timestamp || snapshot.active_txns.contains(&deleted)
            }
        }
    }
}

/// A snapshot of the database at a point in time.
#[derive(Debug, Clone)]
pub struct Snapshot {
    /// Snapshot timestamp.
    pub timestamp: Timestamp,
    /// Transactions that were active when snapshot was taken.
    pub active_txns: HashSet<Timestamp>,
}

impl Snapshot {
    /// Create a new snapshot.
    pub fn new(timestamp: Timestamp, active_txns: HashSet<Timestamp>) -> Self {
        Self { timestamp, active_txns }
    }
}

/// MVCC storage for a single table.
pub struct MvccTable<T: Clone> {
    /// Row versions: row_id -> list of versions (newest first).
    versions: RwLock<BTreeMap<RowId, Vec<Version<T>>>>,
    /// Next row ID.
    next_row_id: AtomicU64,
}

impl<T: Clone> MvccTable<T> {
    /// Create a new MVCC table.
    pub fn new() -> Self {
        Self {
            versions: RwLock::new(BTreeMap::new()),
            next_row_id: AtomicU64::new(1),
        }
    }

    /// Insert a new row, returning its row ID.
    pub fn insert(&self, txn_ts: Timestamp, value: T) -> RowId {
        let row_id = self.next_row_id.fetch_add(1, Ordering::SeqCst);
        let version = Version::new(txn_ts, value);

        let mut versions = self.versions.write().unwrap();
        versions.insert(row_id, vec![version]);

        row_id
    }

    /// Read a row at a given snapshot.
    pub fn read(&self, row_id: RowId, snapshot: &Snapshot) -> Option<T> {
        let versions = self.versions.read().unwrap();
        let row_versions = versions.get(&row_id)?;

        // Find the first visible version
        for version in row_versions {
            if version.is_visible(snapshot) {
                return Some(version.value.clone());
            }
        }

        None
    }

    /// Update a row, creating a new version.
    pub fn update(&self, row_id: RowId, txn_ts: Timestamp, value: T) -> bool {
        let mut versions = self.versions.write().unwrap();

        if let Some(row_versions) = versions.get_mut(&row_id) {
            // Mark old version as deleted
            if let Some(current) = row_versions.first_mut() {
                if current.deleted_by.is_none() {
                    current.deleted_by = Some(txn_ts);
                }
            }

            // Add new version at front
            let new_version = Version::new(txn_ts, value);
            row_versions.insert(0, new_version);
            true
        } else {
            false
        }
    }

    /// Delete a row (mark current version as deleted).
    pub fn delete(&self, row_id: RowId, txn_ts: Timestamp) -> bool {
        let mut versions = self.versions.write().unwrap();

        if let Some(row_versions) = versions.get_mut(&row_id) {
            if let Some(current) = row_versions.first_mut() {
                if current.deleted_by.is_none() {
                    current.deleted_by = Some(txn_ts);
                    return true;
                }
            }
        }

        false
    }

    /// Scan all visible rows at a snapshot.
    pub fn scan(&self, snapshot: &Snapshot) -> Vec<(RowId, T)> {
        let versions = self.versions.read().unwrap();
        let mut results = Vec::new();

        for (&row_id, row_versions) in versions.iter() {
            for version in row_versions {
                if version.is_visible(snapshot) {
                    results.push((row_id, version.value.clone()));
                    break;
                }
            }
        }

        results
    }

    /// Garbage collect old versions that are no longer needed.
    /// Keeps versions visible to any transaction >= min_active_ts.
    pub fn gc(&self, min_active_ts: Timestamp) {
        let mut versions = self.versions.write().unwrap();

        for row_versions in versions.values_mut() {
            // Keep versions that might still be visible
            row_versions.retain(|v| {
                // Keep if created after min_active or not yet deleted
                v.created_by >= min_active_ts || v.deleted_by.map(|d| d >= min_active_ts).unwrap_or(true)
            });
        }

        // Remove empty rows
        versions.retain(|_, v| !v.is_empty());
    }
}

impl<T: Clone> Default for MvccTable<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// MVCC coordinator managing snapshots and transactions.
pub struct MvccCoordinator {
    /// Current timestamp (monotonically increasing).
    current_ts: AtomicU64,
    /// Active transaction timestamps.
    active_txns: RwLock<HashSet<Timestamp>>,
}

impl MvccCoordinator {
    /// Create a new coordinator.
    pub fn new() -> Self {
        Self {
            current_ts: AtomicU64::new(1),
            active_txns: RwLock::new(HashSet::new()),
        }
    }

    /// Begin a new transaction, returning its timestamp.
    pub fn begin(&self) -> Timestamp {
        let ts = self.current_ts.fetch_add(1, Ordering::SeqCst);
        self.active_txns.write().unwrap().insert(ts);
        ts
    }

    /// Commit a transaction.
    pub fn commit(&self, ts: Timestamp) {
        self.active_txns.write().unwrap().remove(&ts);
    }

    /// Abort a transaction.
    pub fn abort(&self, ts: Timestamp) {
        self.active_txns.write().unwrap().remove(&ts);
    }

    /// Take a snapshot for read operations.
    /// The snapshot sees all committed transactions with timestamps < current_ts.
    pub fn snapshot(&self) -> Snapshot {
        // Use current_ts - 1 so we see all committed transactions but not
        // any future transactions that will get current_ts as their ID
        let ts = self.current_ts.load(Ordering::SeqCst).saturating_sub(1);
        let active = self.active_txns.read().unwrap().clone();
        Snapshot::new(ts, active)
    }

    /// Get minimum active transaction timestamp for GC.
    pub fn min_active_ts(&self) -> Timestamp {
        self.active_txns.read().unwrap()
            .iter()
            .copied()
            .min()
            .unwrap_or(self.current_ts.load(Ordering::SeqCst))
    }
}

impl Default for MvccCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version_visibility() {
        let version = Version::new(5, "hello");
        
        // Visible to snapshot at ts=10 with no active txns
        let snap1 = Snapshot::new(10, HashSet::new());
        assert!(version.is_visible(&snap1));

        // Not visible to snapshot at ts=3 (before creation)
        let snap2 = Snapshot::new(3, HashSet::new());
        assert!(!version.is_visible(&snap2));

        // Not visible if creator is still active
        let mut active = HashSet::new();
        active.insert(5);
        let snap3 = Snapshot::new(10, active);
        assert!(!version.is_visible(&snap3));
    }

    #[test]
    fn test_deleted_version_visibility() {
        let mut version = Version::new(5, "hello");
        version.deleted_by = Some(8);

        // Visible at ts=7 (before deletion)
        let snap1 = Snapshot::new(7, HashSet::new());
        assert!(version.is_visible(&snap1));

        // Not visible at ts=10 (after deletion)
        let snap2 = Snapshot::new(10, HashSet::new());
        assert!(!version.is_visible(&snap2));
    }

    #[test]
    fn test_mvcc_table_insert_read() {
        let table: MvccTable<String> = MvccTable::new();
        let coord = MvccCoordinator::new();

        let ts = coord.begin();
        let row_id = table.insert(ts, "value1".into());
        coord.commit(ts);

        let snapshot = coord.snapshot();
        let value = table.read(row_id, &snapshot);
        assert_eq!(value, Some("value1".into()));
    }

    #[test]
    fn test_mvcc_table_update() {
        let table: MvccTable<String> = MvccTable::new();
        let coord = MvccCoordinator::new();

        // Insert
        let ts1 = coord.begin();
        let row_id = table.insert(ts1, "v1".into());
        coord.commit(ts1);

        // Take snapshot before update
        let snap_before = coord.snapshot();

        // Update
        let ts2 = coord.begin();
        table.update(row_id, ts2, "v2".into());
        coord.commit(ts2);

        // Snapshot before sees old value
        assert_eq!(table.read(row_id, &snap_before), Some("v1".into()));

        // New snapshot sees new value
        let snap_after = coord.snapshot();
        assert_eq!(table.read(row_id, &snap_after), Some("v2".into()));
    }

    #[test]
    fn test_mvcc_table_delete() {
        let table: MvccTable<String> = MvccTable::new();
        let coord = MvccCoordinator::new();

        // Insert
        let ts1 = coord.begin();
        let row_id = table.insert(ts1, "value".into());
        coord.commit(ts1);

        // Take snapshot before delete
        let snap_before = coord.snapshot();

        // Delete
        let ts2 = coord.begin();
        assert!(table.delete(row_id, ts2));
        coord.commit(ts2);

        // Snapshot before still sees value
        assert_eq!(table.read(row_id, &snap_before), Some("value".into()));

        // New snapshot sees nothing
        let snap_after = coord.snapshot();
        assert_eq!(table.read(row_id, &snap_after), None);
    }

    #[test]
    fn test_mvcc_table_scan() {
        let table: MvccTable<i32> = MvccTable::new();
        let coord = MvccCoordinator::new();

        let ts = coord.begin();
        table.insert(ts, 1);
        table.insert(ts, 2);
        table.insert(ts, 3);
        coord.commit(ts);

        let snapshot = coord.snapshot();
        let rows = table.scan(&snapshot);
        
        assert_eq!(rows.len(), 3);
        let values: Vec<i32> = rows.iter().map(|(_, v)| *v).collect();
        assert!(values.contains(&1));
        assert!(values.contains(&2));
        assert!(values.contains(&3));
    }

    #[test]
    fn test_mvcc_isolation() {
        let table: MvccTable<String> = MvccTable::new();
        let coord = MvccCoordinator::new();

        // Transaction 1 inserts
        let ts1 = coord.begin();
        let row_id = table.insert(ts1, "from_tx1".into());

        // Transaction 2 takes a snapshot (tx1 still active)
        let ts2 = coord.begin();
        let snap2 = Snapshot::new(coord.current_ts.load(Ordering::SeqCst), {
            let mut active = HashSet::new();
            active.insert(ts1);
            active.insert(ts2);
            active
        });

        // Transaction 2 cannot see tx1's uncommitted write
        assert_eq!(table.read(row_id, &snap2), None);

        // Commit tx1
        coord.commit(ts1);

        // New snapshot can see it
        let snap3 = coord.snapshot();
        assert_eq!(table.read(row_id, &snap3), Some("from_tx1".into()));
    }

    #[test]
    fn test_coordinator_begin_commit() {
        let coord = MvccCoordinator::new();

        let ts1 = coord.begin();
        let ts2 = coord.begin();

        assert_ne!(ts1, ts2);
        assert!(ts2 > ts1);

        coord.commit(ts1);
        assert_eq!(coord.min_active_ts(), ts2);

        coord.commit(ts2);
    }
}
