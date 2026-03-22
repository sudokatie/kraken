//! B-tree index implementation.

/// B-tree index.
pub struct BTree {
    root_page: Option<u32>,
}

impl BTree {
    /// Create a new B-tree.
    pub fn new() -> Self {
        Self { root_page: None }
    }
}

impl Default for BTree {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_btree() {
        let tree = BTree::new();
        assert!(tree.root_page.is_none());
    }
}
