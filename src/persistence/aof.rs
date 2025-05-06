use std::fs::{File,OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;
use crate::util::crc64::calculate_crc;
use crate::storage::memory::MemTable;


/// Command types for AOF entries
#[derive(Debug, Clone, Copy, PartialEq)]
enum CommandType {
  Set = 1,
  Delete = 2,
  // Future command types
}

// AOF entry header - fixed size for easy seeking
#[repr(C, packed)]
struct EntryHeader {
  // CRC64 for data integrity
  crc: u64,
  // total entry size including header
  size: u32,
  // command type (Set/Delete/etc)
  cmd_type: u8,
  // timestamp for entry creation (ms since epoch)
  timestamp: u64,
  // Size of key in bytes
  key_size: u16,
  // size of value in bytes (0 for Delete)
  value_size: u32,
  // TTL duration in milliseconds (0 for no TTL)
  ttl_ms: u64,
}

/// AppendOnlyFile - Durability persistence layer
pub struct AppendOnlyFile {
  // Path to AOF file
  path: PathBuf,
  // Open file handle
  file: File,
  // Write buffer for batching
  writer: BufWriter<File>,
  // Current file position
  position: u64,
  // Count of records replayed during recovery
  replay_count: usize,
}

impl AppendOnlyFile {
  /// Create or open AOF file
  pub fn new<P: AsRef<Path>>(path: P) -> io::Result<Self> {
    let path_buf = Self::resolve_aof_path(path)?;
    
    // Create directories if needed
    if let Some(parent) = path_buf.parent() {
        std::fs::create_dir_all(parent)?;
    }

    // Open or create file
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&path_buf)?;

    let position = file.metadata()?.len();
    let writer = BufWriter::new(file.try_clone()?);

    Ok(Self {
        path: path_buf,
        file,
        writer,
        position,
        replay_count: 0,
    })
  }
  
  /// Get count of records replayed during recovery
  pub fn replay_count(&self) -> usize {
      self.replay_count
  }
  
  /// Append SET command to AOF
  pub fn append_set(&mut self, key: &[u8], value: &[u8], ttl: Option<Duration>) -> io::Result<u64> {
      // Validate inputs
      if key.len() > u16::MAX as usize {
          return Err(io::Error::new(
              io::ErrorKind::InvalidInput,
              "Key too large"
          ));
      }
      
      if value.len() > u32::MAX as usize {
          return Err(io::Error::new(
              io::ErrorKind::InvalidInput,
              "Value too large"
          ));
      }
      
      // Convert TTL to milliseconds
      let ttl_ms = ttl.map(|d| d.as_millis() as u64).unwrap_or(0);
      
      // Create entry header (without CRC for now)
      let header_size = std::mem::size_of::<EntryHeader>();
      let total_size = header_size + key.len() + value.len();
      
      let mut header = EntryHeader {
          crc: 0, // Will calculate after preparing full entry
          size: total_size as u32,
          cmd_type: CommandType::Set as u8,
          timestamp: Self::current_timestamp_ms(),
          key_size: key.len() as u16,
          value_size: value.len() as u32,
          ttl_ms,
      };
      
      // Prepare full entry in memory for CRC calculation
      let mut entry_buf = Vec::with_capacity(total_size);
      
      // Write header placeholder
      let header_bytes = unsafe {
          std::slice::from_raw_parts(
              &header as *const EntryHeader as *const u8,
              header_size
          )
      };
      entry_buf.extend_from_slice(header_bytes);
      
      // Write key and value
      entry_buf.extend_from_slice(key);
      entry_buf.extend_from_slice(value);
      
      // Calculate CRC over the entry (excluding CRC field itself)
      let crc = calculate_crc(&entry_buf[8..]); // Skip CRC field
      
      // Update header with CRC
      header.crc = crc;
      
      // Update entry buffer with correct header
      let header_bytes = unsafe {
          std::slice::from_raw_parts(
              &header as *const EntryHeader as *const u8,
              header_size
          )
      };
      entry_buf[..header_size].copy_from_slice(header_bytes);
      
      // Append to file
      self.writer.write_all(&entry_buf)?;
      self.writer.flush()?;
      
      // Update position and return entry position
      let entry_pos = self.position;
      self.position += total_size as u64;
      
      Ok(entry_pos)
  }
  
  /// Append DELETE command to AOF
  pub fn append_delete(&mut self, key: &[u8]) -> io::Result<u64> {
      // Validate inputs
      if key.len() > u16::MAX as usize {
          return Err(io::Error::new(
              io::ErrorKind::InvalidInput,
              "Key too large"
          ));
      }
      
      // Create entry header (without CRC for now)
      let header_size = std::mem::size_of::<EntryHeader>();
      let total_size = header_size + key.len();
      
      let mut header = EntryHeader {
          crc: 0, // Will calculate after preparing full entry
          size: total_size as u32,
          cmd_type: CommandType::Delete as u8,
          timestamp: Self::current_timestamp_ms(),
          key_size: key.len() as u16,
          value_size: 0,
          ttl_ms: 0,
      };
      
      // Prepare full entry in memory for CRC calculation
      let mut entry_buf = Vec::with_capacity(total_size);
      
      // Write header placeholder
      let header_bytes = unsafe {
          std::slice::from_raw_parts(
              &header as *const EntryHeader as *const u8,
              header_size
          )
      };
      entry_buf.extend_from_slice(header_bytes);
      
      // Write key
      entry_buf.extend_from_slice(key);
      
      // Calculate CRC over the entry (excluding CRC field itself)
      let crc = calculate_crc(&entry_buf[8..]); // Skip CRC field
      
      // Update header with CRC
      header.crc = crc;
      
      // Update entry buffer with correct header
      let header_bytes = unsafe {
          std::slice::from_raw_parts(
              &header as *const EntryHeader as *const u8,
              header_size
          )
      };
      entry_buf[..header_size].copy_from_slice(header_bytes);
      
      // Append to file
      self.writer.write_all(&entry_buf)?;
      self.writer.flush()?;
      
      // Update position and return entry position
      let entry_pos = self.position;
      self.position += total_size as u64;
      
      Ok(entry_pos)
  }
  
  /// Replay existing entries from file for recovery
  pub fn replay_existing_entries(&mut self, mem_table: &MemTable) -> io::Result<()> {
    if self.position == 0 {
        return Ok(());
    }

    let mut reader = BufReader::new(&self.file);
    reader.seek(SeekFrom::Start(0))?;
    
    let header_size = std::mem::size_of::<EntryHeader>();
    let mut position = 0;

    while position < self.position {
        // Read and parse header
        let mut header_buf = [0u8; std::mem::size_of::<EntryHeader>()];
        reader.read_exact(&mut header_buf)?;
        let header: EntryHeader = unsafe { 
            std::ptr::read_unaligned(header_buf.as_ptr() as *const EntryHeader)
        };

        // Validate entry
        if header.size < header_size as u32 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData, 
                "Corrupted AOF entry"
            ));
        }

        // Read key and value
        let mut key = vec![0u8; header.key_size as usize];
        reader.read_exact(&mut key)?;
        
        let mut value = Vec::new();
        if header.value_size > 0 {
            value.resize(header.value_size as usize, 0);
            reader.read_exact(&mut value)?;
        }

        // Verify CRC
        let mut crc_data = Vec::with_capacity(header.size as usize);
        crc_data.extend_from_slice(&header_buf[8..]);
        crc_data.extend_from_slice(&key);
        crc_data.extend_from_slice(&value);
        
        if calculate_crc(&crc_data) != header.crc {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("CRC mismatch at position {}", position)
            ));
        }

        // Apply to MemTable
        match header.cmd_type {
            x if x == CommandType::Set as u8 => {
                let ttl = Duration::from_millis(header.ttl_ms);
                mem_table.recover_set(
                    &key,
                    value,
                    if header.ttl_ms > 0 { Some(ttl) } else { None }
                ).map_err(|e| io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Replay failed: {}", e)
                ))?;
                self.replay_count += 1;
            }
            x if x == CommandType::Delete as u8 => {
                mem_table.recover_delete(&key)
                    .map_err(|e| io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Replay failed: {}", e)
                    ))?;
                self.replay_count += 1;
            }
            _ => return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Unknown command type"
            ))
        }

        position += header.size as u64;
    }

    // Reset file position for new writes
    self.file.seek(SeekFrom::End(0))?;
    Ok(())
  }
  /// Resolve AOF file path
  fn resolve_aof_path<P: AsRef<Path>>(path: P) -> io::Result<PathBuf> {
      let path_ref = path.as_ref();
      
      // If path is a directory, append default filename
      if path_ref.is_dir() {
          Ok(path_ref.join("workingdb.aof"))
      } else {
          Ok(path_ref.to_path_buf())
      }
  }
  
  /// Get current timestamp in milliseconds
  fn current_timestamp_ms() -> u64 {
      use std::time::{SystemTime, UNIX_EPOCH};
      
      SystemTime::now()
          .duration_since(UNIX_EPOCH)
          .unwrap_or_else(|_| Duration::from_secs(0))
          .as_millis() as u64
  }
}
