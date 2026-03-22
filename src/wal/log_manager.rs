//! WAL manager.

use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use crate::Result;
use super::log_record::{LogRecord, Lsn};

/// Log manager.
///
/// Manages the write-ahead log with append-only writes
/// and fsync for durability.
pub struct LogManager {
    /// Path to log file.
    #[allow(dead_code)]
    path: PathBuf,
    /// Log file for writing.
    file: Mutex<File>,
    /// Next LSN to assign.
    next_lsn: Mutex<Lsn>,
    /// Flushed LSN (all records up to this LSN are durable).
    flushed_lsn: Mutex<Lsn>,
}

impl LogManager {
    /// Open or create a log file.
    pub fn open(path: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        let mut manager = Self {
            path: path.to_path_buf(),
            file: Mutex::new(file),
            next_lsn: Mutex::new(1),
            flushed_lsn: Mutex::new(0),
        };

        // Recover next LSN from existing records
        manager.recover_lsn()?;

        Ok(manager)
    }

    /// Recover the next LSN by scanning existing records.
    fn recover_lsn(&mut self) -> Result<()> {
        let records = self.read_all()?;
        if let Some(last) = records.last() {
            *self.next_lsn.get_mut().unwrap() = last.lsn + 1;
            *self.flushed_lsn.get_mut().unwrap() = last.lsn;
        }
        Ok(())
    }

    /// Get the next LSN (without incrementing).
    pub fn next_lsn(&self) -> Lsn {
        *self.next_lsn.lock().unwrap()
    }

    /// Get the flushed LSN.
    pub fn flushed_lsn(&self) -> Lsn {
        *self.flushed_lsn.lock().unwrap()
    }

    /// Append a log record and return its LSN.
    pub fn append(&self, mut record: LogRecord) -> Result<Lsn> {
        let mut next_lsn = self.next_lsn.lock().unwrap();
        let mut file = self.file.lock().unwrap();

        // Assign LSN
        record.lsn = *next_lsn;
        let lsn = *next_lsn;
        *next_lsn += 1;

        // Serialize record
        let data = record.serialize();
        let len = data.len() as u32;

        // Seek to end
        file.seek(SeekFrom::End(0))?;

        // Write length prefix + data + checksum
        let checksum = crc32fast::hash(&data);
        file.write_all(&len.to_le_bytes())?;
        file.write_all(&data)?;
        file.write_all(&checksum.to_le_bytes())?;

        Ok(lsn)
    }

    /// Force all log records to disk up to the given LSN.
    pub fn flush(&self, lsn: Lsn) -> Result<()> {
        let mut flushed = self.flushed_lsn.lock().unwrap();
        
        if lsn > *flushed {
            let file = self.file.lock().unwrap();
            file.sync_all()?;
            *flushed = lsn;
        }

        Ok(())
    }

    /// Force all pending records to disk.
    pub fn flush_all(&self) -> Result<()> {
        let next = *self.next_lsn.lock().unwrap();
        if next > 1 {
            self.flush(next - 1)?;
        }
        Ok(())
    }

    /// Read all log records.
    pub fn read_all(&self) -> Result<Vec<LogRecord>> {
        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start(0))?;

        let mut reader = BufReader::new(&*file);
        let mut records = Vec::new();

        loop {
            // Read length
            let mut len_buf = [0u8; 4];
            match reader.read_exact(&mut len_buf) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }
            let len = u32::from_le_bytes(len_buf) as usize;

            // Read data
            let mut data = vec![0u8; len];
            reader.read_exact(&mut data)?;

            // Read checksum
            let mut checksum_buf = [0u8; 4];
            reader.read_exact(&mut checksum_buf)?;
            let stored_checksum = u32::from_le_bytes(checksum_buf);

            // Verify checksum
            let computed_checksum = crc32fast::hash(&data);
            if stored_checksum != computed_checksum {
                return Err(crate::Error::ChecksumMismatch);
            }

            // Deserialize
            if let Some(record) = LogRecord::deserialize(&data) {
                records.push(record);
            }
        }

        Ok(records)
    }

    /// Read records starting from a given LSN.
    pub fn read_from(&self, start_lsn: Lsn) -> Result<Vec<LogRecord>> {
        let all = self.read_all()?;
        Ok(all.into_iter().filter(|r| r.lsn >= start_lsn).collect())
    }

    /// Truncate the log (for testing/maintenance).
    pub fn truncate(&self) -> Result<()> {
        let mut file = self.file.lock().unwrap();
        file.set_len(0)?;
        file.seek(SeekFrom::Start(0))?;

        *self.next_lsn.lock().unwrap() = 1;
        *self.flushed_lsn.lock().unwrap() = 0;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::log_record::LogRecordType;
    use tempfile::NamedTempFile;

    fn create_log_manager() -> (LogManager, NamedTempFile) {
        let tmp = NamedTempFile::new().unwrap();
        let manager = LogManager::open(tmp.path()).unwrap();
        (manager, tmp)
    }

    #[test]
    fn test_append_read() {
        let (manager, _tmp) = create_log_manager();

        let record = LogRecord::begin(0, 100);
        let lsn = manager.append(record).unwrap();
        assert_eq!(lsn, 1);

        let records = manager.read_all().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].lsn, 1);
        assert_eq!(records[0].txn_id, 100);
    }

    #[test]
    fn test_multiple_records() {
        let (manager, _tmp) = create_log_manager();

        manager.append(LogRecord::begin(0, 1)).unwrap();
        manager.append(LogRecord::insert(0, 1, Some(1), 10, vec![1, 2, 3])).unwrap();
        manager.append(LogRecord::commit(0, 1, 2)).unwrap();

        let records = manager.read_all().unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].record_type, LogRecordType::Begin);
        assert_eq!(records[1].record_type, LogRecordType::Insert);
        assert_eq!(records[2].record_type, LogRecordType::Commit);
    }

    #[test]
    fn test_lsn_monotonic() {
        let (manager, _tmp) = create_log_manager();

        let lsn1 = manager.append(LogRecord::begin(0, 1)).unwrap();
        let lsn2 = manager.append(LogRecord::begin(0, 2)).unwrap();
        let lsn3 = manager.append(LogRecord::begin(0, 3)).unwrap();

        assert_eq!(lsn1, 1);
        assert_eq!(lsn2, 2);
        assert_eq!(lsn3, 3);
    }

    #[test]
    fn test_flush() {
        let (manager, _tmp) = create_log_manager();

        manager.append(LogRecord::begin(0, 1)).unwrap();
        assert_eq!(manager.flushed_lsn(), 0);

        manager.flush(1).unwrap();
        assert_eq!(manager.flushed_lsn(), 1);
    }

    #[test]
    fn test_persistence() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        // Write records
        {
            let manager = LogManager::open(&path).unwrap();
            manager.append(LogRecord::begin(0, 100)).unwrap();
            manager.append(LogRecord::commit(0, 100, 1)).unwrap();
            manager.flush_all().unwrap();
        }

        // Reopen and verify
        {
            let manager = LogManager::open(&path).unwrap();
            let records = manager.read_all().unwrap();
            assert_eq!(records.len(), 2);
            assert_eq!(manager.next_lsn(), 3);
        }
    }

    #[test]
    fn test_read_from_lsn() {
        let (manager, _tmp) = create_log_manager();

        manager.append(LogRecord::begin(0, 1)).unwrap();
        manager.append(LogRecord::begin(0, 2)).unwrap();
        manager.append(LogRecord::begin(0, 3)).unwrap();

        let records = manager.read_from(2).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].lsn, 2);
        assert_eq!(records[1].lsn, 3);
    }

    #[test]
    fn test_truncate() {
        let (manager, _tmp) = create_log_manager();

        manager.append(LogRecord::begin(0, 1)).unwrap();
        manager.append(LogRecord::begin(0, 2)).unwrap();

        manager.truncate().unwrap();

        let records = manager.read_all().unwrap();
        assert!(records.is_empty());
        assert_eq!(manager.next_lsn(), 1);
    }
}
