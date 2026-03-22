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

    /// Serialize page to bytes.
    ///
    /// Layout:
    /// - [0..4]: page_id (u32 LE)
    /// - [4..5]: page_type (u8)
    /// - [5..7]: free_space_start (u16 LE)
    /// - [7..9]: free_space_end (u16 LE)
    /// - [9..11]: slot_count (u16 LE)
    /// - [11..15]: checksum (u32 LE)
    /// - [15..23]: lsn (u64 LE)
    /// - [23..24]: reserved
    /// - [24..24+slot_count*4]: slot array
    /// - [free_space_end..PAGE_SIZE]: tuple data
    pub fn serialize(&self) -> [u8; PAGE_SIZE] {
        let mut buf = [0u8; PAGE_SIZE];

        // Header
        buf[0..4].copy_from_slice(&self.id.to_le_bytes());
        buf[4] = self.page_type as u8;
        buf[5..7].copy_from_slice(&self.free_space_start.to_le_bytes());
        buf[7..9].copy_from_slice(&self.free_space_end.to_le_bytes());
        buf[9..11].copy_from_slice(&self.slot_count.to_le_bytes());
        buf[11..15].copy_from_slice(&self.checksum.to_le_bytes());
        buf[15..23].copy_from_slice(&self.lsn.to_le_bytes());
        // buf[23] reserved

        // Slot array
        for (i, slot) in self.slots.iter().enumerate() {
            let offset = PAGE_HEADER_SIZE + i * 4;
            buf[offset..offset + 2].copy_from_slice(&slot.offset.to_le_bytes());
            buf[offset + 2..offset + 4].copy_from_slice(&slot.length.to_le_bytes());
        }

        // Tuple data (already in self.data at correct offsets)
        let data_start = self.free_space_end as usize;
        buf[data_start..PAGE_SIZE].copy_from_slice(&self.data[data_start..PAGE_SIZE]);

        buf
    }

    /// Deserialize page from bytes.
    pub fn deserialize(buf: &[u8; PAGE_SIZE]) -> crate::Result<Self> {
        // Header
        let id = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
        let page_type = match buf[4] {
            0 => PageType::Heap,
            1 => PageType::BTreeInternal,
            2 => PageType::BTreeLeaf,
            3 => PageType::Overflow,
            _ => return Err(crate::Error::InvalidPage),
        };
        let free_space_start = u16::from_le_bytes([buf[5], buf[6]]);
        let free_space_end = u16::from_le_bytes([buf[7], buf[8]]);
        let slot_count = u16::from_le_bytes([buf[9], buf[10]]);
        let checksum = u32::from_le_bytes([buf[11], buf[12], buf[13], buf[14]]);
        let lsn = u64::from_le_bytes([
            buf[15], buf[16], buf[17], buf[18], buf[19], buf[20], buf[21], buf[22],
        ]);

        // Slot array
        let mut slots = Vec::with_capacity(slot_count as usize);
        for i in 0..slot_count as usize {
            let offset = PAGE_HEADER_SIZE + i * 4;
            let slot_offset = u16::from_le_bytes([buf[offset], buf[offset + 1]]);
            let slot_length = u16::from_le_bytes([buf[offset + 2], buf[offset + 3]]);
            slots.push(Slot {
                offset: slot_offset,
                length: slot_length,
            });
        }

        // Copy raw data
        let mut data = [0u8; PAGE_SIZE];
        data.copy_from_slice(buf);

        Ok(Self {
            id,
            page_type,
            free_space_start,
            free_space_end,
            slot_count,
            checksum,
            lsn,
            slots,
            data,
            dirty: false,
        })
    }

    /// Compute and update checksum.
    pub fn update_checksum(&mut self) {
        // Zero out checksum field before computing
        let mut buf = self.serialize();
        buf[11..15].copy_from_slice(&[0, 0, 0, 0]);
        self.checksum = crc32fast::hash(&buf);
    }

    /// Verify checksum.
    pub fn verify_checksum(&self) -> bool {
        let mut buf = self.serialize();
        let stored = self.checksum;
        buf[11..15].copy_from_slice(&[0, 0, 0, 0]);
        let computed = crc32fast::hash(&buf);
        stored == computed
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

    #[test]
    fn test_serialize_deserialize() {
        let mut page = Page::new(42, PageType::Heap);
        page.lsn = 12345;
        page.insert_tuple(b"first tuple").unwrap();
        page.insert_tuple(b"second tuple").unwrap();

        let bytes = page.serialize();
        let restored = Page::deserialize(&bytes).unwrap();

        assert_eq!(restored.id, 42);
        assert_eq!(restored.page_type, PageType::Heap);
        assert_eq!(restored.lsn, 12345);
        assert_eq!(restored.slot_count, 2);
        assert_eq!(restored.read_tuple(0).unwrap(), b"first tuple");
        assert_eq!(restored.read_tuple(1).unwrap(), b"second tuple");
    }

    #[test]
    fn test_checksum() {
        let mut page = Page::new(1, PageType::Heap);
        page.insert_tuple(b"data").unwrap();
        page.update_checksum();
        assert!(page.verify_checksum());

        // Corrupt tuple data (in the actual data area)
        let tuple_offset = page.free_space_end as usize;
        page.data[tuple_offset] = 0xFF;
        assert!(!page.verify_checksum());
    }
}
