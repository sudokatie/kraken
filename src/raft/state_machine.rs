//! State machine interface.

/// State machine trait.
pub trait StateMachine {
    /// Apply a command.
    fn apply(&mut self, command: &[u8]) -> Vec<u8>;
}
