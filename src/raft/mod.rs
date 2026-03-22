//! Raft consensus protocol.

pub mod log;
pub mod node;
pub mod rpc;
pub mod state_machine;

pub use log::{RaftLog, LogEntry};
pub use node::{RaftNode, NodeState};
pub use rpc::{RequestVoteRequest, RequestVoteResponse, AppendEntriesRequest, AppendEntriesResponse};
