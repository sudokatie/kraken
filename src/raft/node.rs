//! Raft node state machine.

use std::time::{Duration, Instant};
use std::collections::HashSet;

use super::log::{RaftLog, LogEntry};

/// Node state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeState {
    Follower,
    Candidate,
    Leader,
}

/// Election timeout range (milliseconds).
const ELECTION_TIMEOUT_MIN: u64 = 150;
const ELECTION_TIMEOUT_MAX: u64 = 300;

/// Heartbeat interval (milliseconds).
const HEARTBEAT_INTERVAL: u64 = 50;

/// Raft node.
pub struct RaftNode {
    /// Node ID.
    pub id: u64,
    /// Current state.
    pub state: NodeState,
    /// Current term.
    pub term: u64,
    /// Candidate voted for in current term.
    pub voted_for: Option<u64>,
    /// Replicated log.
    pub log: RaftLog,
    /// Commit index (highest log entry known to be committed).
    pub commit_index: u64,
    /// Last applied (highest log entry applied to state machine).
    pub last_applied: u64,
    /// Known peer IDs.
    peers: HashSet<u64>,
    /// Votes received (when candidate).
    votes_received: HashSet<u64>,
    /// Last heartbeat received.
    last_heartbeat: Instant,
    /// Election timeout for this node.
    election_timeout: Duration,
    /// Current leader (if known).
    pub leader_id: Option<u64>,
}

impl RaftNode {
    /// Create a new Raft node.
    pub fn new(id: u64) -> Self {
        Self {
            id,
            state: NodeState::Follower,
            term: 0,
            voted_for: None,
            log: RaftLog::new(),
            commit_index: 0,
            last_applied: 0,
            peers: HashSet::new(),
            votes_received: HashSet::new(),
            last_heartbeat: Instant::now(),
            election_timeout: random_election_timeout(),
            leader_id: None,
        }
    }

    /// Add a peer.
    pub fn add_peer(&mut self, peer_id: u64) {
        if peer_id != self.id {
            self.peers.insert(peer_id);
        }
    }

    /// Get all peers.
    pub fn peers(&self) -> &HashSet<u64> {
        &self.peers
    }

    /// Get cluster size (including self).
    pub fn cluster_size(&self) -> usize {
        self.peers.len() + 1
    }

    /// Get majority size.
    pub fn majority(&self) -> usize {
        self.cluster_size() / 2 + 1
    }

    /// Check if election timeout has elapsed.
    pub fn election_timeout_elapsed(&self) -> bool {
        self.last_heartbeat.elapsed() > self.election_timeout
    }

    /// Reset election timer (called on receiving heartbeat or granting vote).
    pub fn reset_election_timer(&mut self) {
        self.last_heartbeat = Instant::now();
        self.election_timeout = random_election_timeout();
    }

    /// Start an election.
    pub fn start_election(&mut self) {
        self.state = NodeState::Candidate;
        self.term += 1;
        self.voted_for = Some(self.id);
        self.votes_received.clear();
        self.votes_received.insert(self.id);
        self.reset_election_timer();
    }

    /// Receive a vote.
    pub fn receive_vote(&mut self, from: u64, term: u64, granted: bool) {
        if term != self.term || self.state != NodeState::Candidate {
            return;
        }

        if granted {
            self.votes_received.insert(from);

            // Check if we have majority
            if self.votes_received.len() >= self.majority() {
                self.become_leader();
            }
        }
    }

    /// Become leader.
    pub fn become_leader(&mut self) {
        self.state = NodeState::Leader;
        self.leader_id = Some(self.id);
    }

    /// Step down to follower.
    pub fn step_down(&mut self, term: u64) {
        self.state = NodeState::Follower;
        self.term = term;
        self.voted_for = None;
        self.votes_received.clear();
        self.reset_election_timer();
    }

    /// Handle a higher term (step down if necessary).
    pub fn handle_term(&mut self, term: u64) -> bool {
        if term > self.term {
            self.step_down(term);
            true
        } else {
            false
        }
    }

    /// Check if we can grant a vote.
    pub fn can_grant_vote(&self, candidate_id: u64, term: u64, last_log_index: u64, last_log_term: u64) -> bool {
        // Must be same or higher term
        if term < self.term {
            return false;
        }

        // Already voted for someone else this term
        if let Some(voted) = self.voted_for {
            if voted != candidate_id {
                return false;
            }
        }

        // Candidate's log must be at least as up-to-date
        let our_last_term = self.log.last_term();
        let our_last_index = self.log.last_index();

        if last_log_term > our_last_term {
            return true;
        }
        if last_log_term == our_last_term && last_log_index >= our_last_index {
            return true;
        }

        false
    }

    /// Grant a vote to a candidate.
    pub fn grant_vote(&mut self, candidate_id: u64, term: u64) {
        self.term = term;
        self.voted_for = Some(candidate_id);
        self.reset_election_timer();
    }

    /// Append entries to log.
    pub fn append_entries(
        &mut self,
        term: u64,
        leader_id: u64,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<LogEntry>,
        leader_commit: u64,
    ) -> bool {
        // Reply false if term < currentTerm
        if term < self.term {
            return false;
        }

        // Update term if necessary
        self.handle_term(term);

        // Recognize leader
        self.leader_id = Some(leader_id);
        self.reset_election_timer();

        // Step down if candidate
        if self.state == NodeState::Candidate {
            self.state = NodeState::Follower;
        }

        // Check previous log entry
        if prev_log_index > 0 {
            match self.log.get(prev_log_index) {
                Some(entry) if entry.term == prev_log_term => {}
                _ => return false,
            }
        }

        // Append new entries (with conflict resolution)
        for entry in entries {
            self.log.append_or_replace(entry);
        }

        // Update commit index
        if leader_commit > self.commit_index {
            self.commit_index = std::cmp::min(leader_commit, self.log.last_index());
        }

        true
    }

    /// Get last log index and term for RequestVote.
    pub fn last_log_info(&self) -> (u64, u64) {
        (self.log.last_index(), self.log.last_term())
    }

    /// Check if heartbeat should be sent (leaders only).
    pub fn should_send_heartbeat(&self) -> bool {
        self.state == NodeState::Leader &&
            self.last_heartbeat.elapsed() > Duration::from_millis(HEARTBEAT_INTERVAL)
    }

    /// Mark heartbeat sent.
    pub fn heartbeat_sent(&mut self) {
        self.last_heartbeat = Instant::now();
    }
}

/// Generate a random election timeout.
fn random_election_timeout() -> Duration {
    use std::time::SystemTime;
    let seed = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    let range = ELECTION_TIMEOUT_MAX - ELECTION_TIMEOUT_MIN;
    let timeout = ELECTION_TIMEOUT_MIN + (seed % range);
    Duration::from_millis(timeout)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_node() {
        let node = RaftNode::new(1);
        assert_eq!(node.id, 1);
        assert_eq!(node.state, NodeState::Follower);
        assert_eq!(node.term, 0);
    }

    #[test]
    fn test_add_peers() {
        let mut node = RaftNode::new(1);
        node.add_peer(2);
        node.add_peer(3);
        node.add_peer(1); // Self, should be ignored

        assert_eq!(node.peers().len(), 2);
        assert_eq!(node.cluster_size(), 3);
        assert_eq!(node.majority(), 2);
    }

    #[test]
    fn test_start_election() {
        let mut node = RaftNode::new(1);
        node.add_peer(2);
        node.add_peer(3);

        node.start_election();

        assert_eq!(node.state, NodeState::Candidate);
        assert_eq!(node.term, 1);
        assert_eq!(node.voted_for, Some(1));
        assert!(node.votes_received.contains(&1));
    }

    #[test]
    fn test_win_election() {
        let mut node = RaftNode::new(1);
        node.add_peer(2);
        node.add_peer(3);

        node.start_election();
        node.receive_vote(2, 1, true);

        assert_eq!(node.state, NodeState::Leader);
    }

    #[test]
    fn test_step_down_on_higher_term() {
        let mut node = RaftNode::new(1);
        node.start_election();
        assert_eq!(node.state, NodeState::Candidate);
        assert_eq!(node.term, 1);

        node.handle_term(5);

        assert_eq!(node.state, NodeState::Follower);
        assert_eq!(node.term, 5);
        assert_eq!(node.voted_for, None);
    }

    #[test]
    fn test_grant_vote() {
        let mut node = RaftNode::new(1);

        assert!(node.can_grant_vote(2, 1, 0, 0));
        node.grant_vote(2, 1);

        assert_eq!(node.voted_for, Some(2));
        assert_eq!(node.term, 1);
    }

    #[test]
    fn test_cant_vote_twice() {
        let mut node = RaftNode::new(1);
        node.grant_vote(2, 1);

        assert!(!node.can_grant_vote(3, 1, 0, 0));
    }

    #[test]
    fn test_append_entries_updates_commit() {
        let mut node = RaftNode::new(1);
        node.log.append(LogEntry { term: 1, index: 1, data: vec![] });

        let success = node.append_entries(1, 2, 0, 0, vec![], 1);

        assert!(success);
        assert_eq!(node.commit_index, 1);
        assert_eq!(node.leader_id, Some(2));
    }

    #[test]
    fn test_reject_old_term() {
        let mut node = RaftNode::new(1);
        node.term = 5;

        let success = node.append_entries(3, 2, 0, 0, vec![], 0);

        assert!(!success);
    }
}
