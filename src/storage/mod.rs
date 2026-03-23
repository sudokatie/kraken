//! Storage engine components.
//!
//! Page-based storage with B-tree indexes.

pub mod page;
pub mod buffer_pool;
pub mod disk_manager;
pub mod btree;
pub mod heap;

pub use page::{Page, PageId, PageType, PAGE_SIZE};
pub use buffer_pool::BufferPool;
pub use disk_manager::DiskManager;
pub use btree::BTree;
pub use heap::{HeapFile, RowId};
