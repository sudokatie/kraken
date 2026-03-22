//! Disk manager for page I/O.

use std::path::Path;
use crate::Result;

/// Disk manager handles file I/O.
pub struct DiskManager {
    path: String,
}

impl DiskManager {
    /// Create a new disk manager.
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        Ok(Self {
            path: path.as_ref().to_string_lossy().into_owned(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_disk_manager_new() {
        // Just test construction
    }
}
