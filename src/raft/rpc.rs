//! Raft RPCs.

use serde::{Deserialize, Serialize};
use super::log::LogEntry;
use super::node::RaftNode;

/// RequestVote RPC request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteRequest {
    /// Candidate's term.
    pub term: u64,
    /// Candidate requesting vote.
    pub candidate_id: u64,
    /// Index of candidate's last log entry.
    pub last_log_index: u64,
    /// Term of candidate's last log entry.
    pub last_log_term: u64,
}

/// RequestVote RPC response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteResponse {
    /// Current term, for candidate to update itself.
    pub term: u64,
    /// True if candidate received vote.
    pub vote_granted: bool,
}

/// AppendEntries RPC request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesRequest {
    /// Leader's term.
    pub term: u64,
    /// Leader ID so follower can redirect clients.
    pub leader_id: u64,
    /// Index of log entry immediately preceding new ones.
    pub prev_log_index: u64,
    /// Term of prev_log_index entry.
    pub prev_log_term: u64,
    /// Log entries to store (empty for heartbeat).
    pub entries: Vec<LogEntry>,
    /// Leader's commit index.
    pub leader_commit: u64,
}

/// AppendEntries RPC response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesResponse {
    /// Current term, for leader to update itself.
    pub term: u64,
    /// True if follower contained entry matching prev_log_index and prev_log_term.
    pub success: bool,
    /// Hint for faster log reconciliation (optional optimization).
    pub match_index: Option<u64>,
}

impl RaftNode {
    /// Handle RequestVote RPC.
    pub fn handle_request_vote(&mut self, req: RequestVoteRequest) -> RequestVoteResponse {
        // Check for higher term
        self.handle_term(req.term);

        let vote_granted = self.can_grant_vote(
            req.candidate_id,
            req.term,
            req.last_log_index,
            req.last_log_term,
        );

        if vote_granted {
            self.grant_vote(req.candidate_id, req.term);
        }

        RequestVoteResponse {
            term: self.term,
            vote_granted,
        }
    }

    /// Handle AppendEntries RPC.
    pub fn handle_append_entries(&mut self, req: AppendEntriesRequest) -> AppendEntriesResponse {
        let success = self.append_entries(
            req.term,
            req.leader_id,
            req.prev_log_index,
            req.prev_log_term,
            req.entries,
            req.leader_commit,
        );

        let match_index = if success {
            Some(self.log.last_index())
        } else {
            None
        };

        AppendEntriesResponse {
            term: self.term,
            success,
            match_index,
        }
    }

    /// Create a RequestVote request for this node.
    pub fn create_request_vote(&self) -> RequestVoteRequest {
        let (last_log_index, last_log_term) = self.last_log_info();
        RequestVoteRequest {
            term: self.term,
            candidate_id: self.id,
            last_log_index,
            last_log_term,
        }
    }

    /// Create an AppendEntries request (heartbeat or with entries).
    pub fn create_append_entries(&self, prev_log_index: u64, entries: Vec<LogEntry>) -> AppendEntriesRequest {
        let prev_log_term = self.log.term_at(prev_log_index).unwrap_or(0);
        AppendEntriesRequest {
            term: self.term,
            leader_id: self.id,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit: self.commit_index,
        }
    }

    /// Create a heartbeat (empty AppendEntries).
    pub fn create_heartbeat(&self) -> AppendEntriesRequest {
        self.create_append_entries(self.log.last_index(), vec![])
    }

    /// Process RequestVote response.
    pub fn process_request_vote_response(&mut self, resp: RequestVoteResponse) {
        if resp.term > self.term {
            self.step_down(resp.term);
            return;
        }

        self.receive_vote(0, resp.term, resp.vote_granted);
    }

    /// Process AppendEntries response.
    pub fn process_append_entries_response(&mut self, _follower_id: u64, resp: AppendEntriesResponse) {
        if resp.term > self.term {
            self.step_down(resp.term);
        }
        // Match index tracking would go here for full implementation
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::node::NodeState;

    #[test]
    fn test_request_vote_granted() {
        let mut node = RaftNode::new(1);

        let req = RequestVoteRequest {
            term: 1,
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
        };

        let resp = node.handle_request_vote(req);

        assert!(resp.vote_granted);
        assert_eq!(node.voted_for, Some(2));
    }

    #[test]
    fn test_request_vote_rejected_old_term() {
        let mut node = RaftNode::new(1);
        node.term = 5;

        let req = RequestVoteRequest {
            term: 3,
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
        };

        let resp = node.handle_request_vote(req);

        assert!(!resp.vote_granted);
        assert_eq!(resp.term, 5);
    }

    #[test]
    fn test_request_vote_rejected_already_voted() {
        let mut node = RaftNode::new(1);
        node.voted_for = Some(3);
        node.term = 1;

        let req = RequestVoteRequest {
            term: 1,
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
        };

        let resp = node.handle_request_vote(req);

        assert!(!resp.vote_granted);
    }

    #[test]
    fn test_append_entries_heartbeat() {
        let mut node = RaftNode::new(1);

        let req = AppendEntriesRequest {
            term: 1,
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        };

        let resp = node.handle_append_entries(req);

        assert!(resp.success);
        assert_eq!(node.leader_id, Some(2));
    }

    #[test]
    fn test_append_entries_with_data() {
        let mut node = RaftNode::new(1);

        let entries = vec![
            LogEntry { term: 1, index: 1, data: vec![1, 2, 3] },
        ];

        let req = AppendEntriesRequest {
            term: 1,
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries,
            leader_commit: 1,
        };

        let resp = node.handle_append_entries(req);

        assert!(resp.success);
        assert_eq!(resp.match_index, Some(1));
        assert_eq!(node.commit_index, 1);
    }

    #[test]
    fn test_append_entries_rejected_stale() {
        let mut node = RaftNode::new(1);
        node.term = 5;

        let req = AppendEntriesRequest {
            term: 3,
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        };

        let resp = node.handle_append_entries(req);

        assert!(!resp.success);
    }

    #[test]
    fn test_create_request_vote() {
        let mut node = RaftNode::new(1);
        node.term = 3;
        node.log.append(LogEntry { term: 2, index: 1, data: vec![] });

        let req = node.create_request_vote();

        assert_eq!(req.term, 3);
        assert_eq!(req.candidate_id, 1);
        assert_eq!(req.last_log_index, 1);
        assert_eq!(req.last_log_term, 2);
    }

    #[test]
    fn test_create_heartbeat() {
        let mut node = RaftNode::new(1);
        node.term = 2;

        let req = node.create_heartbeat();

        assert_eq!(req.term, 2);
        assert_eq!(req.leader_id, 1);
        assert!(req.entries.is_empty());
    }

    #[test]
    fn test_step_down_on_higher_term_response() {
        let mut node = RaftNode::new(1);
        node.start_election();
        assert_eq!(node.state, NodeState::Candidate);

        let resp = RequestVoteResponse {
            term: 10,
            vote_granted: false,
        };

        node.process_request_vote_response(resp);

        assert_eq!(node.state, NodeState::Follower);
        assert_eq!(node.term, 10);
    }
}
