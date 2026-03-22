//! Buffer pool manager.
//!
//! LRU cache of pages in memory with pin/unpin semantics.

use std::collections::HashMap;

use crate::Result;
use super::page::{Page, PageId, PageType};
use super::disk_manager::DiskManager;

/// Frame in the buffer pool.
struct Frame {
    /// The cached page.
    page: Page,
    /// Pin count (number of users).
    pin_count: u32,
    /// Position in LRU list (lower = older).
    lru_timestamp: u64,
}

/// Buffer pool manager.
pub struct BufferPool {
    /// Maximum number of pages to cache.
    capacity: usize,
    /// Page frames indexed by page ID.
    frames: HashMap<PageId, Frame>,
    /// Disk manager for I/O.
    disk_manager: DiskManager,
    /// LRU timestamp counter.
    lru_counter: u64,
}

impl BufferPool {
    /// Create a new buffer pool.
    pub fn new(capacity: usize, disk_manager: DiskManager) -> Self {
        Self {
            capacity,
            frames: HashMap::with_capacity(capacity),
            disk_manager,
            lru_counter: 0,
        }
    }

    /// Get the buffer pool capacity.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Get the number of pages currently cached.
    pub fn size(&self) -> usize {
        self.frames.len()
    }

    /// Fetch a page, loading from disk if necessary.
    pub fn fetch_page(&mut self, page_id: PageId) -> Result<&Page> {
        // Check if already in cache
        if self.frames.contains_key(&page_id) {
            // Update LRU
            self.lru_counter += 1;
            if let Some(frame) = self.frames.get_mut(&page_id) {
                frame.lru_timestamp = self.lru_counter;
            }
            return Ok(&self.frames.get(&page_id).unwrap().page);
        }

        // Need to load from disk
        self.make_room()?;

        let page = self.disk_manager.read_page(page_id)?;
        self.lru_counter += 1;

        self.frames.insert(
            page_id,
            Frame {
                page,
                pin_count: 0,
                lru_timestamp: self.lru_counter,
            },
        );

        Ok(&self.frames.get(&page_id).unwrap().page)
    }

    /// Fetch a page mutably.
    pub fn fetch_page_mut(&mut self, page_id: PageId) -> Result<&mut Page> {
        // Ensure page is loaded
        if !self.frames.contains_key(&page_id) {
            self.make_room()?;
            let page = self.disk_manager.read_page(page_id)?;
            self.lru_counter += 1;
            self.frames.insert(
                page_id,
                Frame {
                    page,
                    pin_count: 0,
                    lru_timestamp: self.lru_counter,
                },
            );
        }

        // Update LRU
        self.lru_counter += 1;
        if let Some(frame) = self.frames.get_mut(&page_id) {
            frame.lru_timestamp = self.lru_counter;
        }

        Ok(&mut self.frames.get_mut(&page_id).unwrap().page)
    }

    /// Create a new page.
    pub fn new_page(&mut self, page_type: PageType) -> Result<PageId> {
        self.make_room()?;

        let page = self.disk_manager.allocate_page(page_type)?;
        let page_id = page.id;

        self.lru_counter += 1;
        self.frames.insert(
            page_id,
            Frame {
                page,
                pin_count: 0,
                lru_timestamp: self.lru_counter,
            },
        );

        Ok(page_id)
    }

    /// Pin a page (prevent eviction).
    pub fn pin_page(&mut self, page_id: PageId) -> bool {
        if let Some(frame) = self.frames.get_mut(&page_id) {
            frame.pin_count += 1;
            true
        } else {
            false
        }
    }

    /// Unpin a page.
    pub fn unpin_page(&mut self, page_id: PageId) -> bool {
        if let Some(frame) = self.frames.get_mut(&page_id) {
            if frame.pin_count > 0 {
                frame.pin_count -= 1;
                return true;
            }
        }
        false
    }

    /// Flush a specific page to disk.
    pub fn flush_page(&mut self, page_id: PageId) -> Result<()> {
        if let Some(frame) = self.frames.get_mut(&page_id) {
            if frame.page.dirty {
                self.disk_manager.write_page(&frame.page)?;
                frame.page.dirty = false;
            }
        }
        Ok(())
    }

    /// Flush all dirty pages to disk.
    pub fn flush_all(&mut self) -> Result<()> {
        let dirty_pages: Vec<PageId> = self
            .frames
            .iter()
            .filter(|(_, f)| f.page.dirty)
            .map(|(id, _)| *id)
            .collect();

        for page_id in dirty_pages {
            self.flush_page(page_id)?;
        }

        self.disk_manager.sync()?;
        Ok(())
    }

    /// Make room for a new page by evicting if necessary.
    fn make_room(&mut self) -> Result<()> {
        if self.frames.len() < self.capacity {
            return Ok(());
        }

        // Find LRU unpinned page
        let victim = self
            .frames
            .iter()
            .filter(|(_, f)| f.pin_count == 0)
            .min_by_key(|(_, f)| f.lru_timestamp)
            .map(|(id, _)| *id);

        match victim {
            Some(page_id) => {
                // Flush if dirty
                self.flush_page(page_id)?;
                self.frames.remove(&page_id);
                Ok(())
            }
            None => Err(crate::Error::BufferPoolFull),
        }
    }

    /// Check if a page is in the cache.
    pub fn is_cached(&self, page_id: PageId) -> bool {
        self.frames.contains_key(&page_id)
    }

    /// Check if a page is dirty.
    pub fn is_dirty(&self, page_id: PageId) -> bool {
        self.frames
            .get(&page_id)
            .map(|f| f.page.dirty)
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    fn create_pool(capacity: usize) -> BufferPool {
        let tmp = NamedTempFile::new().unwrap();
        // Keep file alive by leaking it (test only)
        let path = tmp.path().to_path_buf();
        std::mem::forget(tmp);
        let dm = DiskManager::new(&path).unwrap();
        BufferPool::new(capacity, dm)
    }

    #[test]
    fn test_new_buffer_pool() {
        let pool = create_pool(10);
        assert_eq!(pool.capacity(), 10);
        assert_eq!(pool.size(), 0);
    }

    #[test]
    fn test_new_page() {
        let mut pool = create_pool(10);
        let page_id = pool.new_page(PageType::Heap).unwrap();
        assert_eq!(page_id, 0);
        assert!(pool.is_cached(0));
        assert_eq!(pool.size(), 1);
    }

    #[test]
    fn test_fetch_page() {
        let mut pool = create_pool(10);

        // Create a page
        let page_id = pool.new_page(PageType::Heap).unwrap();

        // Modify it
        {
            let page = pool.fetch_page_mut(page_id).unwrap();
            page.insert_tuple(b"test").unwrap();
        }

        // Flush and verify dirty flag
        assert!(pool.is_dirty(page_id));
        pool.flush_page(page_id).unwrap();
        assert!(!pool.is_dirty(page_id));
    }

    #[test]
    fn test_pin_unpin() {
        let mut pool = create_pool(10);
        let page_id = pool.new_page(PageType::Heap).unwrap();

        assert!(pool.pin_page(page_id));
        assert!(pool.unpin_page(page_id));
        assert!(!pool.unpin_page(page_id)); // Already 0
    }

    #[test]
    fn test_eviction() {
        let mut pool = create_pool(2);

        // Fill the pool
        let p0 = pool.new_page(PageType::Heap).unwrap();
        let p1 = pool.new_page(PageType::Heap).unwrap();

        assert!(pool.is_cached(p0));
        assert!(pool.is_cached(p1));

        // Adding a third should evict the LRU (p0)
        let _p2 = pool.new_page(PageType::Heap).unwrap();

        assert!(!pool.is_cached(p0)); // Evicted
        assert!(pool.is_cached(p1));
    }

    #[test]
    fn test_pinned_not_evicted() {
        let mut pool = create_pool(2);

        let p0 = pool.new_page(PageType::Heap).unwrap();
        let p1 = pool.new_page(PageType::Heap).unwrap();

        // Pin p0
        pool.pin_page(p0);

        // Adding a third should evict p1 (p0 is pinned)
        let _p2 = pool.new_page(PageType::Heap).unwrap();

        assert!(pool.is_cached(p0)); // Still there (pinned)
        assert!(!pool.is_cached(p1)); // Evicted
    }
}
