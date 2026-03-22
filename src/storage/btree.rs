//! B-tree index implementation.
//!
//! A B+ tree for indexing with all values in leaf nodes.

use crate::Result;
use super::page::{PageId, PageType, PAGE_SIZE};
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
    /// In B+ tree: keys[i] is separator, go to children[i] if key < keys[i],
    /// otherwise continue to the right.
    pub fn find_child(&self, key: Key) -> usize {
        // Find first key > target, then follow child at that index
        self.keys.iter().position(|&k| k > key).unwrap_or(self.key_count)
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

                // Check if split needed
                if node.is_full() {
                    // Write the updated node first
                    let page = pool.fetch_page_mut(leaf_id)?;
                    let data = node.serialize();
                    page.data[..data.len()].copy_from_slice(&data);
                    page.dirty = true;

                    // Split upward
                    self.split_node(pool, leaf_id)?;
                } else {
                    // Write back
                    let page = pool.fetch_page_mut(leaf_id)?;
                    let data = node.serialize();
                    page.data[..data.len()].copy_from_slice(&data);
                    page.dirty = true;
                }

                Ok(())
            }
        }
    }

    /// Split a full node and propagate up.
    fn split_node(&mut self, pool: &mut BufferPool, node_id: PageId) -> Result<()> {
        let page = pool.fetch_page(node_id)?;
        let node = BTreeNode::deserialize(node_id, &page.data)?;

        if !node.is_full() {
            return Ok(());
        }

        let mid = node.key_count / 2;

        if node.is_leaf {
            // Create new leaf with right half
            let new_page_id = pool.new_page(PageType::BTreeLeaf)?;
            let mut new_node = BTreeNode::new_leaf(new_page_id);
            new_node.keys = node.keys[mid..].to_vec();
            new_node.children = node.children[mid..].to_vec();
            new_node.key_count = node.key_count - mid;
            new_node.next_leaf = node.next_leaf;
            new_node.parent = node.parent;

            // Update old node (left half)
            let mut left_node = node.clone();
            left_node.keys.truncate(mid);
            left_node.children.truncate(mid);
            left_node.key_count = mid;
            left_node.next_leaf = Some(new_page_id);

            // Key to promote
            let promote_key = new_node.keys[0];

            // Write both nodes
            let page = pool.fetch_page_mut(node_id)?;
            let data = left_node.serialize();
            page.data[..data.len()].copy_from_slice(&data);
            page.dirty = true;

            let page = pool.fetch_page_mut(new_page_id)?;
            let data = new_node.serialize();
            page.data[..data.len()].copy_from_slice(&data);
            page.dirty = true;

            // Insert into parent
            self.insert_into_parent(pool, node_id, promote_key, new_page_id)?;
        } else {
            // Internal node split
            let new_page_id = pool.new_page(PageType::BTreeInternal)?;
            let mut new_node = BTreeNode::new_internal(new_page_id);
            new_node.keys = node.keys[mid + 1..].to_vec();
            new_node.children = node.children[mid + 1..].to_vec();
            new_node.key_count = node.key_count - mid - 1;
            new_node.parent = node.parent;

            // Key to promote (the middle key)
            let promote_key = node.keys[mid];

            // Update old node (left half)
            let mut left_node = node.clone();
            left_node.keys.truncate(mid);
            left_node.children.truncate(mid + 1);
            left_node.key_count = mid;

            // Update parent pointers in new node's children
            for &child_id in &new_node.children {
                let child_page = pool.fetch_page(child_id as PageId)?;
                let mut child = BTreeNode::deserialize(child_id as PageId, &child_page.data)?;
                child.parent = Some(new_page_id);
                let child_page = pool.fetch_page_mut(child_id as PageId)?;
                let data = child.serialize();
                child_page.data[..data.len()].copy_from_slice(&data);
                child_page.dirty = true;
            }

            // Write both nodes
            let page = pool.fetch_page_mut(node_id)?;
            let data = left_node.serialize();
            page.data[..data.len()].copy_from_slice(&data);
            page.dirty = true;

            let page = pool.fetch_page_mut(new_page_id)?;
            let data = new_node.serialize();
            page.data[..data.len()].copy_from_slice(&data);
            page.dirty = true;

            // Insert into parent
            self.insert_into_parent(pool, node_id, promote_key, new_page_id)?;
        }

        Ok(())
    }

    /// Insert a key and right child into parent after split.
    fn insert_into_parent(
        &mut self,
        pool: &mut BufferPool,
        left_id: PageId,
        key: Key,
        right_id: PageId,
    ) -> Result<()> {
        let page = pool.fetch_page(left_id)?;
        let left_node = BTreeNode::deserialize(left_id, &page.data)?;

        match left_node.parent {
            None => {
                // Create new root
                let new_root_id = pool.new_page(PageType::BTreeInternal)?;
                let mut new_root = BTreeNode::new_internal(new_root_id);
                new_root.keys.push(key);
                new_root.children.push(left_id as u64);
                new_root.children.push(right_id as u64);
                new_root.key_count = 1;

                // Write new root
                let page = pool.fetch_page_mut(new_root_id)?;
                let data = new_root.serialize();
                page.data[..data.len()].copy_from_slice(&data);
                page.dirty = true;

                // Update children's parent pointers
                self.update_parent(pool, left_id, new_root_id)?;
                self.update_parent(pool, right_id, new_root_id)?;

                self.root = Some(new_root_id);
                Ok(())
            }
            Some(parent_id) => {
                // Insert into existing parent
                let page = pool.fetch_page(parent_id)?;
                let mut parent = BTreeNode::deserialize(parent_id, &page.data)?;

                let pos = parent.find_key_position(key);
                parent.keys.insert(pos, key);
                parent.children.insert(pos + 1, right_id as u64);
                parent.key_count += 1;

                // Update right child's parent
                self.update_parent(pool, right_id, parent_id)?;

                // Check if parent needs split
                if parent.is_full() {
                    let page = pool.fetch_page_mut(parent_id)?;
                    let data = parent.serialize();
                    page.data[..data.len()].copy_from_slice(&data);
                    page.dirty = true;

                    self.split_node(pool, parent_id)
                } else {
                    let page = pool.fetch_page_mut(parent_id)?;
                    let data = parent.serialize();
                    page.data[..data.len()].copy_from_slice(&data);
                    page.dirty = true;
                    Ok(())
                }
            }
        }
    }

    /// Update a node's parent pointer.
    fn update_parent(&self, pool: &mut BufferPool, node_id: PageId, parent_id: PageId) -> Result<()> {
        let page = pool.fetch_page(node_id)?;
        let mut node = BTreeNode::deserialize(node_id, &page.data)?;
        node.parent = Some(parent_id);

        let page = pool.fetch_page_mut(node_id)?;
        let data = node.serialize();
        page.data[..data.len()].copy_from_slice(&data);
        page.dirty = true;
        Ok(())
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

                    // Handle underflow if not root
                    if node.is_underfull() && node.parent.is_some() {
                        self.handle_underflow(pool, leaf_id)?;
                    }

                    // Handle empty root
                    if self.root == Some(leaf_id) && node.key_count == 0 {
                        self.root = None;
                    }
                }

                Ok(result)
            }
        }
    }

    /// Handle underflow after deletion by redistributing or merging.
    fn handle_underflow(&mut self, pool: &mut BufferPool, node_id: PageId) -> Result<()> {
        let page = pool.fetch_page(node_id)?;
        let node = BTreeNode::deserialize(node_id, &page.data)?;

        if !node.is_underfull() || node.parent.is_none() {
            return Ok(());
        }

        let parent_id = node.parent.unwrap();
        let page = pool.fetch_page(parent_id)?;
        let parent = BTreeNode::deserialize(parent_id, &page.data)?;

        // Find our position in parent
        let pos = parent.children.iter()
            .position(|&c| c == node_id as u64)
            .ok_or_else(|| crate::Error::Internal("node not found in parent".into()))?;

        // Try to borrow from left sibling
        if pos > 0 {
            let left_id = parent.children[pos - 1] as PageId;
            if self.try_redistribute_from_left(pool, parent_id, pos, left_id, node_id)? {
                return Ok(());
            }
        }

        // Try to borrow from right sibling
        if pos < parent.key_count {
            let right_id = parent.children[pos + 1] as PageId;
            if self.try_redistribute_from_right(pool, parent_id, pos, node_id, right_id)? {
                return Ok(());
            }
        }

        // Must merge
        if pos > 0 {
            // Merge with left sibling
            let left_id = parent.children[pos - 1] as PageId;
            self.merge_nodes(pool, parent_id, pos - 1, left_id, node_id)?;
        } else {
            // Merge with right sibling
            let right_id = parent.children[pos + 1] as PageId;
            self.merge_nodes(pool, parent_id, pos, node_id, right_id)?;
        }

        Ok(())
    }

    /// Try to redistribute keys from left sibling.
    fn try_redistribute_from_left(
        &self,
        pool: &mut BufferPool,
        parent_id: PageId,
        key_idx: usize,
        left_id: PageId,
        right_id: PageId,
    ) -> Result<bool> {
        let page = pool.fetch_page(left_id)?;
        let left = BTreeNode::deserialize(left_id, &page.data)?;

        // Can't borrow if left would become underfull
        if left.key_count <= MIN_KEYS {
            return Ok(false);
        }

        let page = pool.fetch_page(right_id)?;
        let mut right = BTreeNode::deserialize(right_id, &page.data)?;

        let page = pool.fetch_page(parent_id)?;
        let mut parent = BTreeNode::deserialize(parent_id, &page.data)?;

        let mut left = left;

        if left.is_leaf {
            // Move last key from left to front of right
            let move_key = left.keys.pop().unwrap();
            let move_val = left.children.pop().unwrap();
            left.key_count -= 1;

            right.keys.insert(0, move_key);
            right.children.insert(0, move_val);
            right.key_count += 1;

            // Update parent key
            parent.keys[key_idx - 1] = right.keys[0];
        } else {
            // Internal node redistribution
            let parent_key = parent.keys[key_idx - 1];
            let move_child = left.children.pop().unwrap();
            let move_key = left.keys.pop().unwrap();
            left.key_count -= 1;

            right.keys.insert(0, parent_key);
            right.children.insert(0, move_child);
            right.key_count += 1;

            parent.keys[key_idx - 1] = move_key;

            // Update moved child's parent
            self.update_parent(pool, move_child as PageId, right_id)?;
        }

        // Write all nodes back
        let page = pool.fetch_page_mut(left_id)?;
        let data = left.serialize();
        page.data[..data.len()].copy_from_slice(&data);
        page.dirty = true;

        let page = pool.fetch_page_mut(right_id)?;
        let data = right.serialize();
        page.data[..data.len()].copy_from_slice(&data);
        page.dirty = true;

        let page = pool.fetch_page_mut(parent_id)?;
        let data = parent.serialize();
        page.data[..data.len()].copy_from_slice(&data);
        page.dirty = true;

        Ok(true)
    }

    /// Try to redistribute keys from right sibling.
    fn try_redistribute_from_right(
        &self,
        pool: &mut BufferPool,
        parent_id: PageId,
        key_idx: usize,
        left_id: PageId,
        right_id: PageId,
    ) -> Result<bool> {
        let page = pool.fetch_page(right_id)?;
        let right = BTreeNode::deserialize(right_id, &page.data)?;

        // Can't borrow if right would become underfull
        if right.key_count <= MIN_KEYS {
            return Ok(false);
        }

        let page = pool.fetch_page(left_id)?;
        let mut left = BTreeNode::deserialize(left_id, &page.data)?;

        let page = pool.fetch_page(parent_id)?;
        let mut parent = BTreeNode::deserialize(parent_id, &page.data)?;

        let mut right = right;

        if left.is_leaf {
            // Move first key from right to end of left
            let move_key = right.keys.remove(0);
            let move_val = right.children.remove(0);
            right.key_count -= 1;

            left.keys.push(move_key);
            left.children.push(move_val);
            left.key_count += 1;

            // Update parent key
            parent.keys[key_idx] = right.keys[0];
        } else {
            // Internal node redistribution
            let parent_key = parent.keys[key_idx];
            let move_child = right.children.remove(0);
            let move_key = right.keys.remove(0);
            right.key_count -= 1;

            left.keys.push(parent_key);
            left.children.push(move_child);
            left.key_count += 1;

            parent.keys[key_idx] = move_key;

            // Update moved child's parent
            self.update_parent(pool, move_child as PageId, left_id)?;
        }

        // Write all nodes back
        let page = pool.fetch_page_mut(left_id)?;
        let data = left.serialize();
        page.data[..data.len()].copy_from_slice(&data);
        page.dirty = true;

        let page = pool.fetch_page_mut(right_id)?;
        let data = right.serialize();
        page.data[..data.len()].copy_from_slice(&data);
        page.dirty = true;

        let page = pool.fetch_page_mut(parent_id)?;
        let data = parent.serialize();
        page.data[..data.len()].copy_from_slice(&data);
        page.dirty = true;

        Ok(true)
    }

    /// Merge two sibling nodes.
    fn merge_nodes(
        &mut self,
        pool: &mut BufferPool,
        parent_id: PageId,
        key_idx: usize,
        left_id: PageId,
        right_id: PageId,
    ) -> Result<()> {
        let page = pool.fetch_page(left_id)?;
        let mut left = BTreeNode::deserialize(left_id, &page.data)?;

        let page = pool.fetch_page(right_id)?;
        let right = BTreeNode::deserialize(right_id, &page.data)?;

        let page = pool.fetch_page(parent_id)?;
        let mut parent = BTreeNode::deserialize(parent_id, &page.data)?;

        if left.is_leaf {
            // Merge leaf nodes: append right to left
            left.keys.extend(right.keys.iter());
            left.children.extend(right.children.iter());
            left.key_count += right.key_count;
            left.next_leaf = right.next_leaf;
        } else {
            // Merge internal nodes: include separator key from parent
            left.keys.push(parent.keys[key_idx]);
            left.keys.extend(right.keys.iter());
            left.children.extend(right.children.iter());
            left.key_count += 1 + right.key_count;

            // Update parent pointers for merged children
            for &child_id in &right.children {
                self.update_parent(pool, child_id as PageId, left_id)?;
            }
        }

        // Write merged node
        let page = pool.fetch_page_mut(left_id)?;
        let data = left.serialize();
        page.data[..data.len()].copy_from_slice(&data);
        page.dirty = true;

        // Remove key and right child from parent
        parent.keys.remove(key_idx);
        parent.children.remove(key_idx + 1);
        parent.key_count -= 1;

        // Write parent
        let page = pool.fetch_page_mut(parent_id)?;
        let data = parent.serialize();
        page.data[..data.len()].copy_from_slice(&data);
        page.dirty = true;

        // Handle parent underflow or empty root
        if Some(parent_id) == self.root && parent.key_count == 0 {
            // Root became empty, left child is new root
            self.root = Some(left_id);
            left.parent = None;
            let page = pool.fetch_page_mut(left_id)?;
            let data = left.serialize();
            page.data[..data.len()].copy_from_slice(&data);
            page.dirty = true;
        } else if parent.is_underfull() && parent.parent.is_some() {
            self.handle_underflow(pool, parent_id)?;
        }

        Ok(())
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

    #[test]
    fn test_btree_many_inserts() {
        let mut pool = create_pool();
        let mut tree = BTree::new();

        // Insert enough keys to trigger splits
        for i in 0..500 {
            tree.insert(&mut pool, i, (i * 10) as u64).unwrap();
        }

        // Verify all keys present
        for i in 0..500 {
            assert_eq!(tree.search(&mut pool, i).unwrap(), Some((i * 10) as u64));
        }
    }

    #[test]
    fn test_btree_many_deletes() {
        let mut pool = create_pool();
        let mut tree = BTree::new();

        // Insert keys
        for i in 0..100 {
            tree.insert(&mut pool, i, (i * 10) as u64).unwrap();
        }

        // Delete half
        for i in (0..100).step_by(2) {
            assert_eq!(tree.delete(&mut pool, i).unwrap(), Some((i * 10) as u64));
        }

        // Verify remaining
        for i in 0..100 {
            if i % 2 == 0 {
                assert_eq!(tree.search(&mut pool, i).unwrap(), None);
            } else {
                assert_eq!(tree.search(&mut pool, i).unwrap(), Some((i * 10) as u64));
            }
        }
    }

    #[test]
    fn test_btree_delete_all() {
        let mut pool = create_pool();
        let mut tree = BTree::new();

        // Insert and delete all
        for i in 0..50 {
            tree.insert(&mut pool, i, (i * 10) as u64).unwrap();
        }

        for i in 0..50 {
            tree.delete(&mut pool, i).unwrap();
        }

        // Tree should be empty
        for i in 0..50 {
            assert_eq!(tree.search(&mut pool, i).unwrap(), None);
        }
    }

    #[test]
    fn test_btree_reverse_order_insert() {
        let mut pool = create_pool();
        let mut tree = BTree::new();

        // Insert in reverse order
        for i in (0..200).rev() {
            tree.insert(&mut pool, i, (i * 10) as u64).unwrap();
        }

        // Verify all present
        for i in 0..200 {
            assert_eq!(tree.search(&mut pool, i).unwrap(), Some((i * 10) as u64));
        }
    }

    #[test]
    fn test_btree_duplicate_insert() {
        let mut pool = create_pool();
        let mut tree = BTree::new();

        tree.insert(&mut pool, 10, 100).unwrap();
        tree.insert(&mut pool, 10, 200).unwrap(); // Update

        assert_eq!(tree.search(&mut pool, 10).unwrap(), Some(200));
    }
}
