//! Wire protocol for client-server communication.

use serde::{Deserialize, Serialize};

/// Client request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Request {
    /// Execute SQL query.
    Query(String),
    /// Ping server.
    Ping,
    /// Get cluster status.
    Status,
}

/// Server response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Response {
    /// Query result with rows.
    QueryResult(QueryResult),
    /// Pong response.
    Pong,
    /// Status information.
    Status(StatusInfo),
    /// Error message.
    Error(String),
    /// Redirect to leader.
    Redirect(String),
}

/// Query result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    /// Column names.
    pub columns: Vec<String>,
    /// Rows of values.
    pub rows: Vec<Vec<String>>,
    /// Number of rows affected (for INSERT/UPDATE/DELETE).
    pub rows_affected: u64,
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

    /// Create a result for affected rows.
    pub fn affected(count: u64) -> Self {
        Self {
            columns: vec![],
            rows: vec![],
            rows_affected: count,
        }
    }
}

/// Cluster status info.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusInfo {
    /// Node ID.
    pub node_id: u64,
    /// Current term.
    pub term: u64,
    /// Node state (follower/candidate/leader).
    pub state: String,
    /// Leader ID if known.
    pub leader_id: Option<u64>,
    /// Number of peers.
    pub peer_count: usize,
    /// Commit index.
    pub commit_index: u64,
}

/// Message framing: 4-byte length prefix + data.
pub const HEADER_SIZE: usize = 4;

/// Encode a message with length prefix.
pub fn encode_message<T: Serialize>(msg: &T) -> Vec<u8> {
    let data = bincode::serialize(msg).expect("serialization failed");
    let len = data.len() as u32;
    let mut buf = Vec::with_capacity(HEADER_SIZE + data.len());
    buf.extend_from_slice(&len.to_le_bytes());
    buf.extend_from_slice(&data);
    buf
}

/// Decode message length from header.
pub fn decode_length(header: &[u8; HEADER_SIZE]) -> usize {
    u32::from_le_bytes(*header) as usize
}

/// Decode a message from bytes.
pub fn decode_message<T: for<'de> Deserialize<'de>>(data: &[u8]) -> Option<T> {
    bincode::deserialize(data).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_request() {
        let req = Request::Query("SELECT * FROM users".into());
        let encoded = encode_message(&req);

        let len = decode_length(&encoded[..4].try_into().unwrap());
        let decoded: Request = decode_message(&encoded[4..4 + len]).unwrap();

        match decoded {
            Request::Query(sql) => assert_eq!(sql, "SELECT * FROM users"),
            _ => panic!("wrong request type"),
        }
    }

    #[test]
    fn test_query_result() {
        let result = QueryResult {
            columns: vec!["id".into(), "name".into()],
            rows: vec![
                vec!["1".into(), "alice".into()],
                vec!["2".into(), "bob".into()],
            ],
            rows_affected: 0,
        };

        let resp = Response::QueryResult(result);
        let encoded = encode_message(&resp);
        let len = decode_length(&encoded[..4].try_into().unwrap());
        let decoded: Response = decode_message(&encoded[4..4 + len]).unwrap();

        match decoded {
            Response::QueryResult(r) => {
                assert_eq!(r.columns.len(), 2);
                assert_eq!(r.rows.len(), 2);
            }
            _ => panic!("wrong response type"),
        }
    }
}
