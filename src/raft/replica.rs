//! Raft replica - integrates node, log, and state machine.

use std::sync::Arc;
use tokio::sync::Mutex;

use super::node::{RaftNode, NodeState};
use super::log::LogEntry;
use super::state_machine::StateMachine;
use super::rpc::{AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest, RequestVoteResponse};

/// Raft replica combining node state and state machine.
pub struct Replica<S: StateMachine> {
    /// Raft node state.
    node: Arc<Mutex<RaftNode>>,
    /// State machine.
    state_machine: Arc<Mutex<S>>,
}

impl<S: StateMachine + 'static> Replica<S> {
    /// Create a new replica.
    pub fn new(node_id: u64, state_machine: S) -> Self {
        Self {
            node: Arc::new(Mutex::new(RaftNode::new(node_id))),
            state_machine: Arc::new(Mutex::new(state_machine)),
        }
    }

    /// Get node ID.
    pub async fn node_id(&self) -> u64 {
        self.node.lock().await.id
    }

    /// Get current state.
    pub async fn state(&self) -> NodeState {
        self.node.lock().await.state
    }

    /// Get current term.
    pub async fn term(&self) -> u64 {
        self.node.lock().await.term
    }

    /// Check if this node is the leader.
    pub async fn is_leader(&self) -> bool {
        self.node.lock().await.state == NodeState::Leader
    }

    /// Get leader ID.
    pub async fn leader_id(&self) -> Option<u64> {
        self.node.lock().await.leader_id
    }

    /// Add a peer.
    pub async fn add_peer(&self, peer_id: u64) {
        self.node.lock().await.add_peer(peer_id);
    }

    /// Propose a command (leader only).
    /// Returns the log index if successful, or error if not leader.
    pub async fn propose(&self, command: Vec<u8>) -> Result<u64, String> {
        let mut node = self.node.lock().await;

        if node.state != NodeState::Leader {
            return Err("not leader".into());
        }

        let index = node.log.last_index() + 1;
        let entry = LogEntry {
            term: node.term,
            index,
            data: command,
        };

        node.log.append(entry);
        Ok(index)
    }

    /// Apply committed entries to state machine.
    pub async fn apply_committed(&self) -> Vec<(u64, Vec<u8>)> {
        let mut node = self.node.lock().await;
        let mut sm = self.state_machine.lock().await;
        let mut results = Vec::new();

        while node.last_applied < node.commit_index {
            node.last_applied += 1;
            let index = node.last_applied;

            if let Some(entry) = node.log.get(index) {
                match sm.apply(&entry.data) {
                    Ok(result) => results.push((index, result)),
                    Err(e) => {
                        eprintln!("Failed to apply entry {}: {}", index, e);
                    }
                }
            }
        }

        results
    }

    /// Handle RequestVote RPC.
    pub async fn handle_request_vote(&self, req: RequestVoteRequest) -> RequestVoteResponse {
        self.node.lock().await.handle_request_vote(req)
    }

    /// Handle AppendEntries RPC.
    pub async fn handle_append_entries(&self, req: AppendEntriesRequest) -> AppendEntriesResponse {
        let resp = self.node.lock().await.handle_append_entries(req);

        // Apply any newly committed entries
        if resp.success {
            self.apply_committed().await;
        }

        resp
    }

    /// Start an election (called on timeout).
    pub async fn start_election(&self) {
        self.node.lock().await.start_election();
    }

    /// Check if election timeout has elapsed.
    pub async fn should_start_election(&self) -> bool {
        let node = self.node.lock().await;
        node.state != NodeState::Leader && node.election_timeout_elapsed()
    }

    /// Create RequestVote request.
    pub async fn create_request_vote(&self) -> RequestVoteRequest {
        self.node.lock().await.create_request_vote()
    }

    /// Create heartbeat.
    pub async fn create_heartbeat(&self) -> AppendEntriesRequest {
        self.node.lock().await.create_heartbeat()
    }

    /// Process RequestVote response.
    pub async fn process_request_vote_response(&self, resp: RequestVoteResponse) {
        self.node.lock().await.process_request_vote_response(resp);
    }

    /// Check if should send heartbeat (leader only).
    pub async fn should_send_heartbeat(&self) -> bool {
        self.node.lock().await.should_send_heartbeat()
    }

    /// Mark heartbeat sent.
    pub async fn heartbeat_sent(&self) {
        self.node.lock().await.heartbeat_sent();
    }

    /// Get commit index.
    pub async fn commit_index(&self) -> u64 {
        self.node.lock().await.commit_index
    }

    /// Get last applied index.
    pub async fn last_applied(&self) -> u64 {
        self.node.lock().await.last_applied
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::state_machine::KvStateMachine;

    #[tokio::test]
    async fn test_replica_new() {
        let sm = KvStateMachine::new();
        let replica = Replica::new(1, sm);

        assert_eq!(replica.node_id().await, 1);
        assert_eq!(replica.state().await, NodeState::Follower);
    }

    #[tokio::test]
    async fn test_replica_propose_not_leader() {
        let sm = KvStateMachine::new();
        let replica = Replica::new(1, sm);

        let result = replica.propose(vec![1, 2, 3]).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_replica_become_leader_propose() {
        let sm = KvStateMachine::new();
        let replica = Replica::new(1, sm);

        // Become leader
        {
            let mut node = replica.node.lock().await;
            node.state = NodeState::Leader;
        }

        let index = replica.propose(vec![1, 2, 3]).await.unwrap();
        assert_eq!(index, 1);
    }

    #[tokio::test]
    async fn test_replica_apply_committed() {
        use super::super::state_machine::KvCommand;

        let sm = KvStateMachine::new();
        let replica = Replica::new(1, sm);

        // Add an entry and commit it
        {
            let mut node = replica.node.lock().await;
            node.state = NodeState::Leader;

            let cmd = KvCommand::Set { key: "x".into(), value: "42".into() };
            let data = bincode::serialize(&cmd).unwrap();

            node.log.append(LogEntry {
                term: 1,
                index: 1,
                data,
            });
            node.commit_index = 1;
        }

        // Apply
        let results = replica.apply_committed().await;
        assert_eq!(results.len(), 1);
        assert_eq!(replica.last_applied().await, 1);
    }

    #[tokio::test]
    async fn test_replica_handle_append_entries() {
        let sm = KvStateMachine::new();
        let replica = Replica::new(1, sm);

        let req = AppendEntriesRequest {
            term: 1,
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        };

        let resp = replica.handle_append_entries(req).await;
        assert!(resp.success);
        assert_eq!(replica.leader_id().await, Some(2));
    }
}
