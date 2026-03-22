//! Disk manager for page I/O.
//!
//! Handles reading and writing pages to the database file.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::Result;
use super::page::{Page, PageId, PageType, PAGE_SIZE};

/// File header size (first page is metadata).
const FILE_HEADER_SIZE: usize = PAGE_SIZE;

/// Disk manager handles all file I/O for the database.
pub struct DiskManager {
    /// Database file.
    file: File,
    /// Path to database file.
    path: String,
    /// Next page ID to allocate.
    next_page_id: PageId,
}

impl DiskManager {
    /// Create or open a database file.
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_str = path.as_ref().to_string_lossy().into_owned();

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;

        let metadata = file.metadata()?;
        let file_size = metadata.len() as usize;

        // Calculate next page ID from file size
        let next_page_id = if file_size <= FILE_HEADER_SIZE {
            0 // New database
        } else {
            ((file_size - FILE_HEADER_SIZE) / PAGE_SIZE) as PageId
        };

        Ok(Self {
            file,
            path: path_str,
            next_page_id,
        })
    }

    /// Get the database file path.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Get the next page ID that will be allocated.
    pub fn next_page_id(&self) -> PageId {
        self.next_page_id
    }

    /// Calculate file offset for a page.
    fn page_offset(page_id: PageId) -> u64 {
        FILE_HEADER_SIZE as u64 + (page_id as u64 * PAGE_SIZE as u64)
    }

    /// Read a page from disk.
    pub fn read_page(&mut self, page_id: PageId) -> Result<Page> {
        if page_id >= self.next_page_id {
            return Err(crate::Error::PageNotFound(page_id));
        }

        let offset = Self::page_offset(page_id);
        self.file.seek(SeekFrom::Start(offset))?;

        let mut buf = [0u8; PAGE_SIZE];
        self.file.read_exact(&mut buf)?;

        Page::deserialize(&buf)
    }

    /// Write a page to disk.
    pub fn write_page(&mut self, page: &Page) -> Result<()> {
        let offset = Self::page_offset(page.id);
        self.file.seek(SeekFrom::Start(offset))?;

        let buf = page.serialize();
        self.file.write_all(&buf)?;

        Ok(())
    }

    /// Allocate a new page.
    pub fn allocate_page(&mut self, page_type: PageType) -> Result<Page> {
        let page_id = self.next_page_id;
        self.next_page_id += 1;

        let page = Page::new(page_id, page_type);

        // Extend file
        let offset = Self::page_offset(page_id);
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(&[0u8; PAGE_SIZE])?;

        Ok(page)
    }

    /// Sync file to disk.
    pub fn sync(&mut self) -> Result<()> {
        self.file.sync_all()?;
        Ok(())
    }

    /// Get total number of pages.
    pub fn page_count(&self) -> PageId {
        self.next_page_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_new_database() {
        let tmp = NamedTempFile::new().unwrap();
        let dm = DiskManager::new(tmp.path()).unwrap();
        assert_eq!(dm.page_count(), 0);
    }

    #[test]
    fn test_allocate_page() {
        let tmp = NamedTempFile::new().unwrap();
        let mut dm = DiskManager::new(tmp.path()).unwrap();

        let page = dm.allocate_page(PageType::Heap).unwrap();
        assert_eq!(page.id, 0);
        assert_eq!(dm.page_count(), 1);

        let page2 = dm.allocate_page(PageType::Heap).unwrap();
        assert_eq!(page2.id, 1);
        assert_eq!(dm.page_count(), 2);
    }

    #[test]
    fn test_write_read_page() {
        let tmp = NamedTempFile::new().unwrap();
        let mut dm = DiskManager::new(tmp.path()).unwrap();

        // Allocate and modify page
        let mut page = dm.allocate_page(PageType::Heap).unwrap();
        page.insert_tuple(b"test data").unwrap();
        page.lsn = 42;

        // Write to disk
        dm.write_page(&page).unwrap();

        // Read back
        let restored = dm.read_page(0).unwrap();
        assert_eq!(restored.id, 0);
        assert_eq!(restored.lsn, 42);
        assert_eq!(restored.read_tuple(0).unwrap(), b"test data");
    }

    #[test]
    fn test_read_nonexistent_page() {
        let tmp = NamedTempFile::new().unwrap();
        let mut dm = DiskManager::new(tmp.path()).unwrap();

        let result = dm.read_page(999);
        assert!(result.is_err());
    }

    #[test]
    fn test_persistence() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        // Create database and write data
        {
            let mut dm = DiskManager::new(&path).unwrap();
            let mut page = dm.allocate_page(PageType::Heap).unwrap();
            page.insert_tuple(b"persistent data").unwrap();
            dm.write_page(&page).unwrap();
            dm.sync().unwrap();
        }

        // Reopen and verify
        {
            let mut dm = DiskManager::new(&path).unwrap();
            assert_eq!(dm.page_count(), 1);
            let page = dm.read_page(0).unwrap();
            assert_eq!(page.read_tuple(0).unwrap(), b"persistent data");
        }
    }
}
