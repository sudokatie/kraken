//! Raft RPCs.

/// RequestVote RPC.
#[derive(Debug, Clone)]
pub struct RequestVote {
    pub term: u64,
    pub candidate_id: u64,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

/// RequestVote response.
#[derive(Debug, Clone)]
pub struct RequestVoteResponse {
    pub term: u64,
    pub vote_granted: bool,
}

/// AppendEntries RPC.
#[derive(Debug, Clone)]
pub struct AppendEntries {
    pub term: u64,
    pub leader_id: u64,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<Vec<u8>>,
    pub leader_commit: u64,
}

/// AppendEntries response.
#[derive(Debug, Clone)]
pub struct AppendEntriesResponse {
    pub term: u64,
    pub success: bool,
}
