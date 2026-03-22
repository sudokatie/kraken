//! State machine interface for Raft.

use serde::{Deserialize, Serialize};

/// State machine trait.
///
/// Defines how committed log entries are applied to produce state changes.
pub trait StateMachine: Send + Sync {
    /// Apply a command and return the result.
    fn apply(&mut self, command: &[u8]) -> Result<Vec<u8>, String>;

    /// Take a snapshot of current state.
    fn snapshot(&self) -> Vec<u8>;

    /// Restore from a snapshot.
    fn restore(&mut self, snapshot: &[u8]) -> Result<(), String>;
}

/// Command types for SQL state machine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SqlCommand {
    /// Execute a write query (INSERT, UPDATE, DELETE, CREATE TABLE).
    Write { sql: String },
    /// Execute a read query (SELECT) - for linearizable reads.
    Read { sql: String },
}

/// Result of applying a command.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CommandResult {
    /// Success with optional result data.
    Success { data: Option<String> },
    /// Error with message.
    Error { message: String },
}

impl SqlCommand {
    /// Create a write command.
    pub fn write(sql: impl Into<String>) -> Self {
        SqlCommand::Write { sql: sql.into() }
    }

    /// Create a read command.
    pub fn read(sql: impl Into<String>) -> Self {
        SqlCommand::Read { sql: sql.into() }
    }

    /// Serialize to bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(self).expect("serialization failed")
    }

    /// Deserialize from bytes.
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        bincode::deserialize(data).ok()
    }
}

impl CommandResult {
    /// Create a success result.
    pub fn success(data: Option<String>) -> Self {
        CommandResult::Success { data }
    }

    /// Create an error result.
    pub fn error(message: impl Into<String>) -> Self {
        CommandResult::Error { message: message.into() }
    }

    /// Serialize to bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(self).expect("serialization failed")
    }

    /// Deserialize from bytes.
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        bincode::deserialize(data).ok()
    }
}

/// Simple in-memory key-value state machine for testing.
#[derive(Default)]
pub struct KvStateMachine {
    data: std::collections::HashMap<String, String>,
}

impl KvStateMachine {
    /// Create a new KV state machine.
    pub fn new() -> Self {
        Self::default()
    }

    /// Get a value.
    pub fn get(&self, key: &str) -> Option<&String> {
        self.data.get(key)
    }

    /// Set a value.
    pub fn set(&mut self, key: String, value: String) {
        self.data.insert(key, value);
    }

    /// Delete a value.
    pub fn delete(&mut self, key: &str) -> Option<String> {
        self.data.remove(key)
    }
}

/// KV command.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum KvCommand {
    Get { key: String },
    Set { key: String, value: String },
    Delete { key: String },
}

impl StateMachine for KvStateMachine {
    fn apply(&mut self, command: &[u8]) -> Result<Vec<u8>, String> {
        let cmd: KvCommand = bincode::deserialize(command)
            .map_err(|e| e.to_string())?;

        let result = match cmd {
            KvCommand::Get { key } => {
                let value = self.get(&key).cloned();
                CommandResult::success(value)
            }
            KvCommand::Set { key, value } => {
                self.set(key, value);
                CommandResult::success(None)
            }
            KvCommand::Delete { key } => {
                let removed = self.delete(&key);
                CommandResult::success(removed)
            }
        };

        Ok(result.to_bytes())
    }

    fn snapshot(&self) -> Vec<u8> {
        bincode::serialize(&self.data).expect("serialization failed")
    }

    fn restore(&mut self, snapshot: &[u8]) -> Result<(), String> {
        self.data = bincode::deserialize(snapshot)
            .map_err(|e| e.to_string())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sql_command_serialize() {
        let cmd = SqlCommand::write("INSERT INTO users VALUES (1, 'alice')");
        let bytes = cmd.to_bytes();
        let restored = SqlCommand::from_bytes(&bytes).unwrap();

        match restored {
            SqlCommand::Write { sql } => assert!(sql.contains("INSERT")),
            _ => panic!("wrong command type"),
        }
    }

    #[test]
    fn test_command_result() {
        let result = CommandResult::success(Some("OK".into()));
        let bytes = result.to_bytes();
        let restored = CommandResult::from_bytes(&bytes).unwrap();

        match restored {
            CommandResult::Success { data } => assert_eq!(data, Some("OK".into())),
            _ => panic!("wrong result type"),
        }
    }

    #[test]
    fn test_kv_state_machine() {
        let mut sm = KvStateMachine::new();

        // Set
        let cmd = KvCommand::Set { key: "foo".into(), value: "bar".into() };
        let bytes = bincode::serialize(&cmd).unwrap();
        sm.apply(&bytes).unwrap();

        assert_eq!(sm.get("foo"), Some(&"bar".to_string()));

        // Get
        let cmd = KvCommand::Get { key: "foo".into() };
        let bytes = bincode::serialize(&cmd).unwrap();
        let result_bytes = sm.apply(&bytes).unwrap();
        let result = CommandResult::from_bytes(&result_bytes).unwrap();

        match result {
            CommandResult::Success { data } => assert_eq!(data, Some("bar".into())),
            _ => panic!("expected success"),
        }

        // Delete
        let cmd = KvCommand::Delete { key: "foo".into() };
        let bytes = bincode::serialize(&cmd).unwrap();
        sm.apply(&bytes).unwrap();

        assert_eq!(sm.get("foo"), None);
    }

    #[test]
    fn test_snapshot_restore() {
        let mut sm1 = KvStateMachine::new();
        sm1.set("a".into(), "1".into());
        sm1.set("b".into(), "2".into());

        let snapshot = sm1.snapshot();

        let mut sm2 = KvStateMachine::new();
        sm2.restore(&snapshot).unwrap();

        assert_eq!(sm2.get("a"), Some(&"1".to_string()));
        assert_eq!(sm2.get("b"), Some(&"2".to_string()));
    }
}
