//! Heap file for table storage.
//!
//! A heap file stores unordered table rows across multiple pages.
//! Each table has one heap file. Rows are inserted into the first
//! page with enough space.

use crate::Result;
use super::buffer_pool::BufferPool;
use super::page::{Page, PageId, PageType, PAGE_SIZE};

/// Row identifier: (page_id, slot_id).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RowId {
    pub page_id: PageId,
    pub slot_id: u16,
}

impl RowId {
    pub fn new(page_id: PageId, slot_id: u16) -> Self {
        Self { page_id, slot_id }
    }
}

/// Heap file header stored in first page.
#[derive(Debug, Clone)]
pub struct HeapFileHeader {
    /// Table ID.
    pub table_id: u32,
    /// First data page ID.
    pub first_page: Option<PageId>,
    /// Last data page ID (for fast inserts).
    pub last_page: Option<PageId>,
    /// Total row count.
    pub row_count: u64,
    /// Total page count.
    pub page_count: u32,
}

impl HeapFileHeader {
    pub fn new(table_id: u32) -> Self {
        Self {
            table_id,
            first_page: None,
            last_page: None,
            row_count: 0,
            page_count: 0,
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(32);
        buf.extend_from_slice(&self.table_id.to_le_bytes());
        buf.extend_from_slice(&self.first_page.unwrap_or(u32::MAX).to_le_bytes());
        buf.extend_from_slice(&self.last_page.unwrap_or(u32::MAX).to_le_bytes());
        buf.extend_from_slice(&self.row_count.to_le_bytes());
        buf.extend_from_slice(&self.page_count.to_le_bytes());
        buf
    }

    pub fn deserialize(data: &[u8]) -> Option<Self> {
        if data.len() < 24 {
            return None;
        }
        let table_id = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
        let first_raw = u32::from_le_bytes([data[4], data[5], data[6], data[7]]);
        let last_raw = u32::from_le_bytes([data[8], data[9], data[10], data[11]]);
        let row_count = u64::from_le_bytes([
            data[12], data[13], data[14], data[15],
            data[16], data[17], data[18], data[19],
        ]);
        let page_count = u32::from_le_bytes([data[20], data[21], data[22], data[23]]);

        Some(Self {
            table_id,
            first_page: if first_raw == u32::MAX { None } else { Some(first_raw) },
            last_page: if last_raw == u32::MAX { None } else { Some(last_raw) },
            row_count,
            page_count,
        })
    }
}

/// Heap file manages table data across pages.
pub struct HeapFile {
    /// Table ID.
    table_id: u32,
    /// Header page ID.
    header_page_id: PageId,
    /// Cached header.
    header: HeapFileHeader,
}

/// Offset where heap file header is stored in page data (after page header).
const HEAP_HEADER_OFFSET: usize = super::page::PAGE_HEADER_SIZE;

impl HeapFile {
    /// Create a new heap file for a table.
    pub fn create(pool: &mut BufferPool, table_id: u32) -> Result<Self> {
        // Allocate header page
        let header_page_id = pool.new_page(PageType::Heap)?;
        let header = HeapFileHeader::new(table_id);

        // Write header to page (after page header region)
        {
            let page = pool.fetch_page_mut(header_page_id)?;
            let data = header.serialize();
            page.data[HEAP_HEADER_OFFSET..HEAP_HEADER_OFFSET + data.len()].copy_from_slice(&data);
            page.dirty = true;
        }

        Ok(Self {
            table_id,
            header_page_id,
            header,
        })
    }

    /// Open an existing heap file.
    pub fn open(pool: &mut BufferPool, header_page_id: PageId) -> Result<Self> {
        let page = pool.fetch_page(header_page_id)?;
        let header = HeapFileHeader::deserialize(&page.data[HEAP_HEADER_OFFSET..])
            .ok_or_else(|| crate::Error::InvalidPage)?;

        Ok(Self {
            table_id: header.table_id,
            header_page_id,
            header,
        })
    }

    /// Get table ID.
    pub fn table_id(&self) -> u32 {
        self.table_id
    }

    /// Get header page ID.
    pub fn header_page_id(&self) -> PageId {
        self.header_page_id
    }

    /// Get row count.
    pub fn row_count(&self) -> u64 {
        self.header.row_count
    }

    /// Insert a tuple, returning its RowId.
    pub fn insert(&mut self, pool: &mut BufferPool, data: &[u8]) -> Result<RowId> {
        // Try to insert into last page first
        if let Some(last_page_id) = self.header.last_page {
            let page = pool.fetch_page_mut(last_page_id)?;
            if page.free_space() >= data.len() + 4 {
                let slot_id = page.insert_tuple(data)?;
                self.header.row_count += 1;
                self.save_header(pool)?;
                return Ok(RowId::new(last_page_id, slot_id));
            }
        }

        // Need a new page
        let new_page_id = pool.new_page(PageType::Heap)?;
        {
            let page = pool.fetch_page_mut(new_page_id)?;
            let slot_id = page.insert_tuple(data)?;

            // Update linked list
            if self.header.first_page.is_none() {
                self.header.first_page = Some(new_page_id);
            }
            self.header.last_page = Some(new_page_id);
            self.header.page_count += 1;
            self.header.row_count += 1;
            self.save_header(pool)?;

            Ok(RowId::new(new_page_id, slot_id))
        }
    }

    /// Read a tuple by RowId.
    pub fn read(&self, pool: &mut BufferPool, row_id: RowId) -> Result<Option<Vec<u8>>> {
        let page = pool.fetch_page(row_id.page_id)?;
        Ok(page.read_tuple(row_id.slot_id).map(|s| s.to_vec()))
    }

    /// Update a tuple by RowId.
    pub fn update(&mut self, pool: &mut BufferPool, row_id: RowId, data: &[u8]) -> Result<bool> {
        // For simplicity, delete + insert if size changed
        // A real implementation would handle in-place updates
        let page = pool.fetch_page_mut(row_id.page_id)?;
        
        if let Some(old_data) = page.read_tuple(row_id.slot_id) {
            if old_data.len() == data.len() {
                // In-place update
                let slot = &page.slots[row_id.slot_id as usize];
                let offset = slot.offset as usize;
                page.data[offset..offset + data.len()].copy_from_slice(data);
                page.dirty = true;
                return Ok(true);
            }
        }

        // Delete and reinsert
        if self.delete(pool, row_id)? {
            self.insert(pool, data)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Delete a tuple by RowId.
    pub fn delete(&mut self, pool: &mut BufferPool, row_id: RowId) -> Result<bool> {
        let page = pool.fetch_page_mut(row_id.page_id)?;
        if page.delete_tuple(row_id.slot_id) {
            self.header.row_count = self.header.row_count.saturating_sub(1);
            self.save_header(pool)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Scan all tuples in the heap file.
    pub fn scan<'a>(&'a self, pool: &'a mut BufferPool) -> HeapScan<'a> {
        HeapScan::new(self, pool)
    }

    /// Save header to disk.
    fn save_header(&self, pool: &mut BufferPool) -> Result<()> {
        let page = pool.fetch_page_mut(self.header_page_id)?;
        let data = self.header.serialize();
        page.data[HEAP_HEADER_OFFSET..HEAP_HEADER_OFFSET + data.len()].copy_from_slice(&data);
        page.dirty = true;
        Ok(())
    }

    /// Get all page IDs in this heap file.
    pub fn page_ids(&self, pool: &mut BufferPool) -> Result<Vec<PageId>> {
        let mut ids = Vec::new();
        let mut current = self.header.first_page;

        // Simple approach: iterate from first to last
        // A full implementation would maintain a linked list in page headers
        while let Some(page_id) = current {
            ids.push(page_id);
            if Some(page_id) == self.header.last_page {
                break;
            }
            // For now, assume contiguous pages after first
            current = Some(page_id + 1);
            if ids.len() >= self.header.page_count as usize {
                break;
            }
        }

        Ok(ids)
    }
}

/// Iterator over heap file tuples.
pub struct HeapScan<'a> {
    heap: &'a HeapFile,
    pool: &'a mut BufferPool,
    current_page: Option<PageId>,
    current_slot: u16,
    page_ids: Vec<PageId>,
    page_idx: usize,
}

impl<'a> HeapScan<'a> {
    fn new(heap: &'a HeapFile, pool: &'a mut BufferPool) -> Self {
        // Get all page IDs upfront
        let page_ids = heap.page_ids(pool).unwrap_or_default();
        let current_page = page_ids.first().copied();

        Self {
            heap,
            pool,
            current_page,
            current_slot: 0,
            page_ids,
            page_idx: 0,
        }
    }

    /// Get next tuple.
    pub fn next(&mut self) -> Result<Option<(RowId, Vec<u8>)>> {
        loop {
            let page_id = match self.current_page {
                Some(id) => id,
                None => return Ok(None),
            };

            let page = self.pool.fetch_page(page_id)?;

            // Find next valid slot
            while (self.current_slot as usize) < page.slots.len() {
                let slot_id = self.current_slot;
                self.current_slot += 1;

                if let Some(data) = page.read_tuple(slot_id) {
                    let row_id = RowId::new(page_id, slot_id);
                    return Ok(Some((row_id, data.to_vec())));
                }
            }

            // Move to next page
            self.page_idx += 1;
            if self.page_idx < self.page_ids.len() {
                self.current_page = Some(self.page_ids[self.page_idx]);
                self.current_slot = 0;
            } else {
                self.current_page = None;
            }
        }
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
    fn test_create_heap_file() {
        let mut pool = create_pool();
        let heap = HeapFile::create(&mut pool, 1).unwrap();
        assert_eq!(heap.table_id(), 1);
        assert_eq!(heap.row_count(), 0);
    }

    #[test]
    fn test_insert_read() {
        let mut pool = create_pool();
        let mut heap = HeapFile::create(&mut pool, 1).unwrap();

        let row_id = heap.insert(&mut pool, b"hello world").unwrap();
        assert_eq!(heap.row_count(), 1);

        let data = heap.read(&mut pool, row_id).unwrap();
        assert_eq!(data, Some(b"hello world".to_vec()));
    }

    #[test]
    fn test_insert_multiple() {
        let mut pool = create_pool();
        let mut heap = HeapFile::create(&mut pool, 1).unwrap();

        let row1 = heap.insert(&mut pool, b"row 1").unwrap();
        let row2 = heap.insert(&mut pool, b"row 2").unwrap();
        let row3 = heap.insert(&mut pool, b"row 3").unwrap();

        assert_eq!(heap.row_count(), 3);
        assert_eq!(heap.read(&mut pool, row1).unwrap(), Some(b"row 1".to_vec()));
        assert_eq!(heap.read(&mut pool, row2).unwrap(), Some(b"row 2".to_vec()));
        assert_eq!(heap.read(&mut pool, row3).unwrap(), Some(b"row 3".to_vec()));
    }

    #[test]
    fn test_delete() {
        let mut pool = create_pool();
        let mut heap = HeapFile::create(&mut pool, 1).unwrap();

        let row_id = heap.insert(&mut pool, b"delete me").unwrap();
        assert_eq!(heap.row_count(), 1);

        assert!(heap.delete(&mut pool, row_id).unwrap());
        assert_eq!(heap.row_count(), 0);
        assert_eq!(heap.read(&mut pool, row_id).unwrap(), None);
    }

    #[test]
    fn test_update() {
        let mut pool = create_pool();
        let mut heap = HeapFile::create(&mut pool, 1).unwrap();

        let row_id = heap.insert(&mut pool, b"original").unwrap();
        heap.update(&mut pool, row_id, b"updated!").unwrap();

        // Note: update might change row_id if size changed
        // For same-size update, row_id stays the same
        let data = heap.read(&mut pool, row_id).unwrap();
        assert!(data.is_some());
    }

    #[test]
    fn test_scan() {
        let mut pool = create_pool();
        let mut heap = HeapFile::create(&mut pool, 1).unwrap();

        heap.insert(&mut pool, b"row 1").unwrap();
        heap.insert(&mut pool, b"row 2").unwrap();
        heap.insert(&mut pool, b"row 3").unwrap();

        let mut scan = heap.scan(&mut pool);
        let mut count = 0;
        while let Ok(Some(_)) = scan.next() {
            count += 1;
        }
        assert_eq!(count, 3);
    }

    #[test]
    fn test_open_existing() {
        let mut pool = create_pool();
        
        let header_page_id;
        {
            let mut heap = HeapFile::create(&mut pool, 42).unwrap();
            header_page_id = heap.header_page_id();
            heap.insert(&mut pool, b"persistent data").unwrap();
        }

        // Reopen
        let heap = HeapFile::open(&mut pool, header_page_id).unwrap();
        assert_eq!(heap.table_id(), 42);
        assert_eq!(heap.row_count(), 1);
    }

    #[test]
    fn test_large_inserts() {
        let mut pool = create_pool();
        let mut heap = HeapFile::create(&mut pool, 1).unwrap();

        // Insert enough to span multiple pages
        for i in 0..100 {
            let data = format!("row number {} with some extra data to make it larger", i);
            heap.insert(&mut pool, data.as_bytes()).unwrap();
        }

        assert_eq!(heap.row_count(), 100);
    }

    #[test]
    fn test_row_id() {
        let row_id = RowId::new(5, 10);
        assert_eq!(row_id.page_id, 5);
        assert_eq!(row_id.slot_id, 10);
    }

    #[test]
    fn test_header_serialization() {
        let mut header = HeapFileHeader::new(123);
        header.first_page = Some(1);
        header.last_page = Some(5);
        header.row_count = 1000;
        header.page_count = 5;

        let data = header.serialize();
        let restored = HeapFileHeader::deserialize(&data).unwrap();

        assert_eq!(restored.table_id, 123);
        assert_eq!(restored.first_page, Some(1));
        assert_eq!(restored.last_page, Some(5));
        assert_eq!(restored.row_count, 1000);
        assert_eq!(restored.page_count, 5);
    }
}
