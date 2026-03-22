//! B-tree index implementation.
//!
//! A B+ tree for indexing with all values in leaf nodes.

use crate::Result;
use super::page::{Page, PageId, PageType, PAGE_SIZE};
use super::buffer_pool::BufferPool;

/// Maximum keys per node (branching factor).
/// With 4KB pages and 8-byte keys + 8-byte values + 4-byte child pointers,
/// we can fit roughly 200 keys per node.
const MAX_KEYS: usize = 200;
const MIN_KEYS: usize = MAX_KEYS / 2;

/// B-tree key type.
pub type Key = i64;

/// B-tree value type (typically a RowId or PageId).
pub type Value = u64;

/// B-tree node.
#[derive(Debug, Clone)]
pub struct BTreeNode {
    /// Page ID of this node.
    pub page_id: PageId,
    /// Is this a leaf node?
    pub is_leaf: bool,
    /// Number of keys in this node.
    pub key_count: usize,
    /// Keys.
    pub keys: Vec<Key>,
    /// Values (leaf) or child page IDs (internal).
    pub children: Vec<u64>,
    /// Parent page ID.
    pub parent: Option<PageId>,
    /// Next leaf (for range scans).
    pub next_leaf: Option<PageId>,
}

impl BTreeNode {
    /// Create a new leaf node.
    pub fn new_leaf(page_id: PageId) -> Self {
        Self {
            page_id,
            is_leaf: true,
            key_count: 0,
            keys: Vec::with_capacity(MAX_KEYS),
            children: Vec::with_capacity(MAX_KEYS),
            parent: None,
            next_leaf: None,
        }
    }

    /// Create a new internal node.
    pub fn new_internal(page_id: PageId) -> Self {
        Self {
            page_id,
            is_leaf: false,
            key_count: 0,
            keys: Vec::with_capacity(MAX_KEYS),
            children: Vec::with_capacity(MAX_KEYS + 1),
            parent: None,
            next_leaf: None,
        }
    }

    /// Check if node is full.
    pub fn is_full(&self) -> bool {
        self.key_count >= MAX_KEYS
    }

    /// Check if node is underfull (needs merge/redistribute).
    pub fn is_underfull(&self) -> bool {
        self.key_count < MIN_KEYS
    }

    /// Find the position where a key should be inserted.
    pub fn find_key_position(&self, key: Key) -> usize {
        self.keys.iter().position(|&k| k >= key).unwrap_or(self.key_count)
    }

    /// Insert a key-value pair into a leaf node.
    pub fn insert_leaf(&mut self, key: Key, value: Value) -> Result<()> {
        if !self.is_leaf {
            return Err(crate::Error::Internal("not a leaf node".into()));
        }

        let pos = self.find_key_position(key);

        // Check for duplicate
        if pos < self.key_count && self.keys[pos] == key {
            // Update existing
            self.children[pos] = value;
        } else {
            // Insert new
            self.keys.insert(pos, key);
            self.children.insert(pos, value);
            self.key_count += 1;
        }

        Ok(())
    }

    /// Search for a key in a leaf node.
    pub fn search_leaf(&self, key: Key) -> Option<Value> {
        if !self.is_leaf {
            return None;
        }

        let pos = self.find_key_position(key);
        if pos < self.key_count && self.keys[pos] == key {
            Some(self.children[pos])
        } else {
            None
        }
    }

    /// Find child index for a key in an internal node.
    pub fn find_child(&self, key: Key) -> usize {
        self.find_key_position(key)
    }

    /// Delete a key from a leaf node.
    pub fn delete_leaf(&mut self, key: Key) -> Option<Value> {
        if !self.is_leaf {
            return None;
        }

        let pos = self.find_key_position(key);
        if pos < self.key_count && self.keys[pos] == key {
            self.keys.remove(pos);
            let value = self.children.remove(pos);
            self.key_count -= 1;
            Some(value)
        } else {
            None
        }
    }

    /// Serialize node to bytes.
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(PAGE_SIZE);

        // Header: is_leaf (1) + key_count (2) + parent (4) + next_leaf (4)
        buf.push(if self.is_leaf { 1 } else { 0 });
        buf.extend_from_slice(&(self.key_count as u16).to_le_bytes());
        buf.extend_from_slice(&self.parent.unwrap_or(u32::MAX).to_le_bytes());
        buf.extend_from_slice(&self.next_leaf.unwrap_or(u32::MAX).to_le_bytes());

        // Keys
        for key in &self.keys {
            buf.extend_from_slice(&key.to_le_bytes());
        }

        // Pad keys to MAX_KEYS
        for _ in self.keys.len()..MAX_KEYS {
            buf.extend_from_slice(&0i64.to_le_bytes());
        }

        // Children/values
        for child in &self.children {
            buf.extend_from_slice(&child.to_le_bytes());
        }

        // Pad children
        let max_children = if self.is_leaf { MAX_KEYS } else { MAX_KEYS + 1 };
        for _ in self.children.len()..max_children {
            buf.extend_from_slice(&0u64.to_le_bytes());
        }

        buf
    }

    /// Deserialize node from bytes.
    pub fn deserialize(page_id: PageId, data: &[u8]) -> Result<Self> {
        if data.is_empty() {
            return Err(crate::Error::InvalidPage);
        }

        let is_leaf = data[0] == 1;
        let key_count = u16::from_le_bytes([data[1], data[2]]) as usize;
        let parent_raw = u32::from_le_bytes([data[3], data[4], data[5], data[6]]);
        let parent = if parent_raw == u32::MAX { None } else { Some(parent_raw) };
        let next_leaf_raw = u32::from_le_bytes([data[7], data[8], data[9], data[10]]);
        let next_leaf = if next_leaf_raw == u32::MAX { None } else { Some(next_leaf_raw) };

        // Keys start at offset 11
        let mut keys = Vec::with_capacity(key_count);
        let mut offset = 11;
        for _ in 0..key_count {
            let key = i64::from_le_bytes([
                data[offset], data[offset + 1], data[offset + 2], data[offset + 3],
                data[offset + 4], data[offset + 5], data[offset + 6], data[offset + 7],
            ]);
            keys.push(key);
            offset += 8;
        }

        // Skip to children (after MAX_KEYS * 8 bytes from key start)
        offset = 11 + MAX_KEYS * 8;

        let child_count = if is_leaf { key_count } else { key_count + 1 };
        let mut children = Vec::with_capacity(child_count);
        for _ in 0..child_count {
            let child = u64::from_le_bytes([
                data[offset], data[offset + 1], data[offset + 2], data[offset + 3],
                data[offset + 4], data[offset + 5], data[offset + 6], data[offset + 7],
            ]);
            children.push(child);
            offset += 8;
        }

        Ok(Self {
            page_id,
            is_leaf,
            key_count,
            keys,
            children,
            parent,
            next_leaf,
        })
    }
}

/// B-tree index.
pub struct BTree {
    /// Root page ID.
    root: Option<PageId>,
}

impl BTree {
    /// Create a new empty B-tree.
    pub fn new() -> Self {
        Self { root: None }
    }

    /// Create a B-tree with an existing root.
    pub fn with_root(root: PageId) -> Self {
        Self { root: Some(root) }
    }

    /// Get the root page ID.
    pub fn root(&self) -> Option<PageId> {
        self.root
    }

    /// Check if tree is empty.
    pub fn is_empty(&self) -> bool {
        self.root.is_none()
    }

    /// Insert a key-value pair.
    pub fn insert(&mut self, pool: &mut BufferPool, key: Key, value: Value) -> Result<()> {
        match self.root {
            None => {
                // Create first leaf node
                let page_id = pool.new_page(PageType::BTreeLeaf)?;
                let mut node = BTreeNode::new_leaf(page_id);
                node.insert_leaf(key, value)?;

                // Write to page
                let page = pool.fetch_page_mut(page_id)?;
                let data = node.serialize();
                page.data[..data.len()].copy_from_slice(&data);
                page.dirty = true;

                self.root = Some(page_id);
                Ok(())
            }
            Some(root_id) => {
                // Find leaf for insertion
                let leaf_id = self.find_leaf(pool, root_id, key)?;

                // Load leaf node
                let page = pool.fetch_page(leaf_id)?;
                let mut node = BTreeNode::deserialize(leaf_id, &page.data)?;

                // Insert into leaf
                node.insert_leaf(key, value)?;

                // Write back
                let page = pool.fetch_page_mut(leaf_id)?;
                let data = node.serialize();
                page.data[..data.len()].copy_from_slice(&data);
                page.dirty = true;

                // TODO: Handle splits when node is full
                Ok(())
            }
        }
    }

    /// Search for a key.
    pub fn search(&self, pool: &mut BufferPool, key: Key) -> Result<Option<Value>> {
        match self.root {
            None => Ok(None),
            Some(root_id) => {
                let leaf_id = self.find_leaf(pool, root_id, key)?;
                let page = pool.fetch_page(leaf_id)?;
                let node = BTreeNode::deserialize(leaf_id, &page.data)?;
                Ok(node.search_leaf(key))
            }
        }
    }

    /// Delete a key.
    pub fn delete(&mut self, pool: &mut BufferPool, key: Key) -> Result<Option<Value>> {
        match self.root {
            None => Ok(None),
            Some(root_id) => {
                let leaf_id = self.find_leaf(pool, root_id, key)?;

                let page = pool.fetch_page(leaf_id)?;
                let mut node = BTreeNode::deserialize(leaf_id, &page.data)?;

                let result = node.delete_leaf(key);

                if result.is_some() {
                    let page = pool.fetch_page_mut(leaf_id)?;
                    let data = node.serialize();
                    page.data[..data.len()].copy_from_slice(&data);
                    page.dirty = true;
                }

                // TODO: Handle underflow

                Ok(result)
            }
        }
    }

    /// Find the leaf node that should contain a key.
    fn find_leaf(&self, pool: &mut BufferPool, start: PageId, key: Key) -> Result<PageId> {
        let page = pool.fetch_page(start)?;
        let node = BTreeNode::deserialize(start, &page.data)?;

        if node.is_leaf {
            return Ok(start);
        }

        // Internal node: follow child pointer
        let child_idx = node.find_child(key);
        let child_id = node.children[child_idx] as PageId;
        self.find_leaf(pool, child_id, key)
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
    use crate::storage::DiskManager;
    use tempfile::NamedTempFile;

    fn create_pool() -> BufferPool {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();
        std::mem::forget(tmp);
        let dm = DiskManager::new(&path).unwrap();
        BufferPool::new(100, dm)
    }

    #[test]
    fn test_new_btree() {
        let tree = BTree::new();
        assert!(tree.is_empty());
        assert!(tree.root().is_none());
    }

    #[test]
    fn test_leaf_node_insert() {
        let mut node = BTreeNode::new_leaf(0);
        node.insert_leaf(10, 100).unwrap();
        node.insert_leaf(5, 50).unwrap();
        node.insert_leaf(15, 150).unwrap();

        assert_eq!(node.key_count, 3);
        assert_eq!(node.search_leaf(10), Some(100));
        assert_eq!(node.search_leaf(5), Some(50));
        assert_eq!(node.search_leaf(15), Some(150));
        assert_eq!(node.search_leaf(20), None);
    }

    #[test]
    fn test_leaf_node_delete() {
        let mut node = BTreeNode::new_leaf(0);
        node.insert_leaf(10, 100).unwrap();
        node.insert_leaf(20, 200).unwrap();

        assert_eq!(node.delete_leaf(10), Some(100));
        assert_eq!(node.key_count, 1);
        assert_eq!(node.search_leaf(10), None);
        assert_eq!(node.search_leaf(20), Some(200));
    }

    #[test]
    fn test_node_serialize_deserialize() {
        let mut node = BTreeNode::new_leaf(42);
        node.insert_leaf(10, 100).unwrap();
        node.insert_leaf(20, 200).unwrap();
        node.parent = Some(1);

        let data = node.serialize();
        let restored = BTreeNode::deserialize(42, &data).unwrap();

        assert!(restored.is_leaf);
        assert_eq!(restored.key_count, 2);
        assert_eq!(restored.parent, Some(1));
        assert_eq!(restored.search_leaf(10), Some(100));
        assert_eq!(restored.search_leaf(20), Some(200));
    }

    #[test]
    fn test_btree_insert_search() {
        let mut pool = create_pool();
        let mut tree = BTree::new();

        tree.insert(&mut pool, 10, 100).unwrap();
        tree.insert(&mut pool, 20, 200).unwrap();
        tree.insert(&mut pool, 5, 50).unwrap();

        assert!(!tree.is_empty());
        assert_eq!(tree.search(&mut pool, 10).unwrap(), Some(100));
        assert_eq!(tree.search(&mut pool, 20).unwrap(), Some(200));
        assert_eq!(tree.search(&mut pool, 5).unwrap(), Some(50));
        assert_eq!(tree.search(&mut pool, 15).unwrap(), None);
    }

    #[test]
    fn test_btree_delete() {
        let mut pool = create_pool();
        let mut tree = BTree::new();

        tree.insert(&mut pool, 10, 100).unwrap();
        tree.insert(&mut pool, 20, 200).unwrap();

        assert_eq!(tree.delete(&mut pool, 10).unwrap(), Some(100));
        assert_eq!(tree.search(&mut pool, 10).unwrap(), None);
        assert_eq!(tree.search(&mut pool, 20).unwrap(), Some(200));
    }
}
