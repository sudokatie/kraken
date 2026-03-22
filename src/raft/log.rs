//! Raft log.

use serde::{Deserialize, Serialize};

/// Log entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    /// Term when entry was received.
    pub term: u64,
    /// Log index (1-indexed).
    pub index: u64,
    /// Command data.
    pub data: Vec<u8>,
}

/// Raft log.
pub struct RaftLog {
    /// Log entries.
    entries: Vec<LogEntry>,
}

impl RaftLog {
    /// Create a new log.
    pub fn new() -> Self {
        Self { entries: Vec::new() }
    }

    /// Get the last log index (0 if empty).
    pub fn last_index(&self) -> u64 {
        self.entries.last().map(|e| e.index).unwrap_or(0)
    }

    /// Get the last log term (0 if empty).
    pub fn last_term(&self) -> u64 {
        self.entries.last().map(|e| e.term).unwrap_or(0)
    }

    /// Get number of entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Append entry.
    pub fn append(&mut self, entry: LogEntry) {
        self.entries.push(entry);
    }

    /// Append or replace entry (for conflict resolution).
    pub fn append_or_replace(&mut self, entry: LogEntry) {
        // Find position by index
        if let Some(pos) = self.entries.iter().position(|e| e.index == entry.index) {
            // Check for conflict
            if self.entries[pos].term != entry.term {
                // Delete this and all following entries
                self.entries.truncate(pos);
                self.entries.push(entry);
            }
            // If same term, entry is already there
        } else if entry.index == self.last_index() + 1 {
            // Append new entry
            self.entries.push(entry);
        }
        // Otherwise ignore (gap in indices)
    }

    /// Get entry by index.
    pub fn get(&self, index: u64) -> Option<&LogEntry> {
        self.entries.iter().find(|e| e.index == index)
    }

    /// Get entries from index onwards.
    pub fn entries_from(&self, start_index: u64) -> Vec<LogEntry> {
        self.entries.iter()
            .filter(|e| e.index >= start_index)
            .cloned()
            .collect()
    }

    /// Get term for entry at index.
    pub fn term_at(&self, index: u64) -> Option<u64> {
        self.get(index).map(|e| e.term)
    }

    /// Truncate log to given index.
    pub fn truncate_after(&mut self, index: u64) {
        self.entries.retain(|e| e.index <= index);
    }

    /// Get all entries.
    pub fn entries(&self) -> &[LogEntry] {
        &self.entries
    }
}

impl Default for RaftLog {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_new() {
        let log = RaftLog::new();
        assert!(log.is_empty());
        assert_eq!(log.last_index(), 0);
        assert_eq!(log.last_term(), 0);
    }

    #[test]
    fn test_log_append() {
        let mut log = RaftLog::new();
        log.append(LogEntry { term: 1, index: 1, data: vec![1, 2, 3] });

        assert_eq!(log.len(), 1);
        assert_eq!(log.last_index(), 1);
        assert_eq!(log.last_term(), 1);
        assert!(log.get(1).is_some());
    }

    #[test]
    fn test_log_multiple() {
        let mut log = RaftLog::new();
        log.append(LogEntry { term: 1, index: 1, data: vec![] });
        log.append(LogEntry { term: 1, index: 2, data: vec![] });
        log.append(LogEntry { term: 2, index: 3, data: vec![] });

        assert_eq!(log.len(), 3);
        assert_eq!(log.last_index(), 3);
        assert_eq!(log.last_term(), 2);
    }

    #[test]
    fn test_entries_from() {
        let mut log = RaftLog::new();
        log.append(LogEntry { term: 1, index: 1, data: vec![] });
        log.append(LogEntry { term: 1, index: 2, data: vec![] });
        log.append(LogEntry { term: 2, index: 3, data: vec![] });

        let entries = log.entries_from(2);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].index, 2);
        assert_eq!(entries[1].index, 3);
    }

    #[test]
    fn test_truncate() {
        let mut log = RaftLog::new();
        log.append(LogEntry { term: 1, index: 1, data: vec![] });
        log.append(LogEntry { term: 1, index: 2, data: vec![] });
        log.append(LogEntry { term: 2, index: 3, data: vec![] });

        log.truncate_after(1);

        assert_eq!(log.len(), 1);
        assert_eq!(log.last_index(), 1);
    }

    #[test]
    fn test_append_or_replace_conflict() {
        let mut log = RaftLog::new();
        log.append(LogEntry { term: 1, index: 1, data: vec![1] });
        log.append(LogEntry { term: 1, index: 2, data: vec![2] });
        log.append(LogEntry { term: 1, index: 3, data: vec![3] });

        // Conflicting entry at index 2 with different term
        log.append_or_replace(LogEntry { term: 2, index: 2, data: vec![20] });

        assert_eq!(log.len(), 2);
        assert_eq!(log.get(2).unwrap().term, 2);
        assert_eq!(log.get(2).unwrap().data, vec![20]);
        assert!(log.get(3).is_none());
    }

    #[test]
    fn test_term_at() {
        let mut log = RaftLog::new();
        log.append(LogEntry { term: 1, index: 1, data: vec![] });
        log.append(LogEntry { term: 2, index: 2, data: vec![] });

        assert_eq!(log.term_at(1), Some(1));
        assert_eq!(log.term_at(2), Some(2));
        assert_eq!(log.term_at(3), None);
    }
}
