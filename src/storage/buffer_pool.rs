//! Buffer pool manager.
//!
//! LRU cache of pages in memory.

use crate::Result;
use super::page::{Page, PageId};

/// Buffer pool manager.
pub struct BufferPool {
    capacity: usize,
}

impl BufferPool {
    /// Create a new buffer pool.
    pub fn new(capacity: usize) -> Self {
        Self { capacity }
    }

    /// Get capacity.
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_buffer_pool() {
        let pool = BufferPool::new(100);
        assert_eq!(pool.capacity(), 100);
    }
}
