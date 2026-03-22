//! Page structure and operations.
//!
//! Fixed 4KB pages with slotted page format.

/// Page size in bytes (4KB).
pub const PAGE_SIZE: usize = 4096;

/// Page header size in bytes.
pub const PAGE_HEADER_SIZE: usize = 24;

/// Page identifier.
pub type PageId = u32;

/// Slot in the page directory.
#[derive(Debug, Clone, Copy)]
pub struct Slot {
    /// Offset from start of page.
    pub offset: u16,
    /// Length of tuple data.
    pub length: u16,
}

/// Page types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PageType {
    /// Heap page for table data.
    Heap = 0,
    /// B-tree internal node.
    BTreeInternal = 1,
    /// B-tree leaf node.
    BTreeLeaf = 2,
    /// Overflow page for large tuples.
    Overflow = 3,
}

/// A database page.
#[derive(Debug)]
pub struct Page {
    /// Unique page identifier.
    pub id: PageId,
    /// Type of page.
    pub page_type: PageType,
    /// Offset to start of free space.
    pub free_space_start: u16,
    /// Offset to end of free space.
    pub free_space_end: u16,
    /// Number of slots.
    pub slot_count: u16,
    /// Checksum for integrity.
    pub checksum: u32,
    /// Log sequence number.
    pub lsn: u64,
    /// Slot directory.
    pub slots: Vec<Slot>,
    /// Raw page data.
    pub data: [u8; PAGE_SIZE],
    /// Dirty flag.
    pub dirty: bool,
}

impl Page {
    /// Create a new empty page.
    pub fn new(id: PageId, page_type: PageType) -> Self {
        Self {
            id,
            page_type,
            free_space_start: PAGE_HEADER_SIZE as u16,
            free_space_end: PAGE_SIZE as u16,
            slot_count: 0,
            checksum: 0,
            lsn: 0,
            slots: Vec::new(),
            data: [0u8; PAGE_SIZE],
            dirty: false,
        }
    }

    /// Calculate free space in page.
    pub fn free_space(&self) -> usize {
        if self.free_space_end <= self.free_space_start {
            return 0;
        }
        (self.free_space_end - self.free_space_start) as usize
    }

    /// Insert a tuple into the page.
    pub fn insert_tuple(&mut self, data: &[u8]) -> crate::Result<u16> {
        let slot_size = 4; // offset + length
        let required = data.len() + slot_size;

        if self.free_space() < required {
            return Err(crate::Error::PageFull);
        }

        // Allocate space at end
        self.free_space_end -= data.len() as u16;
        let offset = self.free_space_end;

        // Copy tuple data
        self.data[offset as usize..offset as usize + data.len()].copy_from_slice(data);

        // Add slot
        let slot = Slot {
            offset,
            length: data.len() as u16,
        };
        self.slots.push(slot);
        self.slot_count += 1;
        self.dirty = true;

        Ok(self.slot_count - 1)
    }

    /// Read a tuple from the page.
    pub fn read_tuple(&self, slot_id: u16) -> Option<&[u8]> {
        let slot = self.slots.get(slot_id as usize)?;
        if slot.length == 0 {
            return None; // Deleted
        }
        Some(&self.data[slot.offset as usize..slot.offset as usize + slot.length as usize])
    }

    /// Delete a tuple (mark as deleted).
    pub fn delete_tuple(&mut self, slot_id: u16) -> bool {
        if let Some(slot) = self.slots.get_mut(slot_id as usize) {
            slot.length = 0; // Mark as deleted
            self.dirty = true;
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_page() {
        let page = Page::new(1, PageType::Heap);
        assert_eq!(page.id, 1);
        assert_eq!(page.slot_count, 0);
        assert!(page.free_space() > 0);
    }

    #[test]
    fn test_insert_tuple() {
        let mut page = Page::new(1, PageType::Heap);
        let data = b"hello world";
        let slot = page.insert_tuple(data).unwrap();
        assert_eq!(slot, 0);
        assert_eq!(page.slot_count, 1);
    }

    #[test]
    fn test_read_tuple() {
        let mut page = Page::new(1, PageType::Heap);
        let data = b"test data";
        let slot = page.insert_tuple(data).unwrap();
        let read = page.read_tuple(slot).unwrap();
        assert_eq!(read, data);
    }

    #[test]
    fn test_delete_tuple() {
        let mut page = Page::new(1, PageType::Heap);
        let data = b"delete me";
        let slot = page.insert_tuple(data).unwrap();
        assert!(page.delete_tuple(slot));
        assert!(page.read_tuple(slot).is_none());
    }
}
