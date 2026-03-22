//! Kraken - A distributed SQL database
//!
//! A database built from scratch implementing:
//! - Storage engine with B-tree indexes
//! - SQL parser and query planner
//! - Query execution engine
//! - Write-ahead logging
//! - Raft consensus for replication

pub mod storage;
pub mod wal;
pub mod sql;
pub mod catalog;
pub mod planner;
pub mod executor;
pub mod transaction;
pub mod raft;
pub mod network;
pub mod cli;
pub mod error;

pub use error::{Error, Result};
