//! WAL record types.

/// Log record types.
#[derive(Debug, Clone)]
pub enum LogRecordType {
    Begin,
    Commit,
    Abort,
    Insert,
    Update,
    Delete,
}
