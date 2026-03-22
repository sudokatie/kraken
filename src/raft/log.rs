//! Raft log.

/// Log entry.
#[derive(Debug, Clone)]
pub struct LogEntry {
    pub term: u64,
    pub index: u64,
    pub data: Vec<u8>,
}

/// Raft log.
pub struct RaftLog {
    entries: Vec<LogEntry>,
}

impl RaftLog {
    /// Create a new log.
    pub fn new() -> Self {
        Self { entries: Vec::new() }
    }

    /// Append entry.
    pub fn append(&mut self, entry: LogEntry) {
        self.entries.push(entry);
    }

    /// Get entry by index.
    pub fn get(&self, index: u64) -> Option<&LogEntry> {
        self.entries.iter().find(|e| e.index == index)
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
    fn test_log_append() {
        let mut log = RaftLog::new();
        log.append(LogEntry { term: 1, index: 1, data: vec![1, 2, 3] });
        assert!(log.get(1).is_some());
    }
}
