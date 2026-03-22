//! Raft node state machine.

/// Node state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeState {
    Follower,
    Candidate,
    Leader,
}

/// Raft node.
pub struct RaftNode {
    pub id: u64,
    pub state: NodeState,
    pub term: u64,
    pub voted_for: Option<u64>,
}

impl RaftNode {
    /// Create a new Raft node.
    pub fn new(id: u64) -> Self {
        Self {
            id,
            state: NodeState::Follower,
            term: 0,
            voted_for: None,
        }
    }
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
}
