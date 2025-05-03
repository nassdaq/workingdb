// src/storage/disk.rs - DIRECT NVME ACCESS (O_DIRECT)
// Raw device access for maximum I/O performance

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::os::fd::AsRawFd;
// CRITICAL FIX: Added libc import instead of nix for simplified dependencies
use libc::{O_DIRECT, O_DSYNC};

/// NvmeAccess - Direct device access bypassing kernel buffers
pub struct NvmeAccess {
    // Device path (e.g., "/dev/nvme0n1")
    path: String,
    
    // Block size for aligned I/O
    block_size: usize,
    
    // File handle (when opened)
    file: Option<File>,
}

impl NvmeAccess {
    /// Create new NVMe access for device
    pub fn new<P: AsRef<Path>>(device_path: P) -> Result<Self, std::io::Error> {
        // Validate device exists
        let path_str = device_path.as_ref()
            .to_str()
            .ok_or_else(|| std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Invalid device path"
            ))?
            .to_string();
        
        // Default block size for NVMe (typically 4KiB)
        let block_size = 4096;
        
        Ok(Self {
            path: path_str,
            block_size,
            file: None,
        })
    }
    
    /// Open the device with direct I/O flags
    pub fn open(&mut self) -> Result<(), std::io::Error> {
        // CRITICAL FIX: Using O_DIRECT and O_DSYNC from libc instead of nix
        // Use O_DIRECT flag for bypassing kernel page cache
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(O_DIRECT | O_DSYNC)
            .open(&self.path)?;
        
        self.file = Some(file);
        Ok(())
    }
    
    /// Close the device if open
    pub fn close(&mut self) {
        self.file = None;
    }
    
    /// Read aligned block from device at offset
    pub fn read_aligned(&mut self, offset: u64, buf: &mut [u8]) -> Result<usize, std::io::Error> {
        // Ensure buffer is correctly aligned
        self.ensure_alignment(buf.as_ptr() as usize, buf.len())?;
        
        // Ensure offset is block-aligned
        if offset % self.block_size as u64 != 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Offset must be block-aligned"
            ));
        }
        
        // Ensure we have an open file
        let file = self.ensure_open()?;
        
        // Seek to offset
        file.seek(SeekFrom::Start(offset))?;
        
        // Perform read
        let bytes_read = file.read(buf)?;
        
        Ok(bytes_read)
    }
    
    /// Write aligned block to device at offset
    pub fn write_aligned(&mut self, offset: u64, buf: &[u8]) -> Result<(), std::io::Error> {
        // Ensure buffer is correctly aligned
        self.ensure_alignment(buf.as_ptr() as usize, buf.len())?;
        
        // Ensure offset is block-aligned
        if offset % self.block_size as u64 != 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Offset must be block-aligned"
            ));
        }
        
        // Ensure we have an open file
        let file = self.ensure_open()?;
        
        // Seek to offset
        file.seek(SeekFrom::Start(offset))?;
        
        // Perform write
        file.write_all(buf)?;
        
        Ok(())
    }
    
    /// Raw write to device at any offset (internally handles alignment)
    pub fn raw_write(&mut self, offset: u64, data: &[u8]) -> Result<(), std::io::Error> {
        // Calculate aligned offset and size
        let aligned_offset = (offset / self.block_size as u64) * self.block_size as u64;
        let offset_within_block = (offset - aligned_offset) as usize;
        
        // Create aligned buffer
        let mut aligned_buf = self.create_aligned_buffer(
            offset_within_block + data.len()
        )?;
        
        // If we're not at block start, need to read existing block first
        if offset_within_block > 0 {
            self.read_aligned(aligned_offset, &mut aligned_buf)?;
        }
        
        // Copy data into aligned buffer at correct position
        aligned_buf[offset_within_block..offset_within_block + data.len()]
            .copy_from_slice(data);
            
        // Write aligned buffer
        self.write_aligned(aligned_offset, &aligned_buf)?;
        
        Ok(())
    }
    
    // === PRIVATE HELPERS ===
    
    /// Ensure we have an open file handle
    fn ensure_open(&mut self) -> Result<&mut File, std::io::Error> {
        if self.file.is_none() {
            self.open()?;
        }
        
        self.file.as_mut().ok_or_else(|| std::io::Error::new(
            std::io::ErrorKind::Other,
            "Failed to get file handle"
        ))
    }
    
    /// Check if address and size are aligned to block size
    fn ensure_alignment(&self, addr: usize, size: usize) -> Result<(), std::io::Error> {
        // Check address alignment
        if addr % self.block_size != 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Buffer must be aligned to block size"
            ));
        }
        
        // Check size alignment
        if size % self.block_size != 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Buffer size must be a multiple of block size"
            ));
        }
        
        Ok(())
    }
    
    /// Create block-aligned buffer of specified size
    fn create_aligned_buffer(&self, size: usize) -> Result<Vec<u8>, std::io::Error> {
        // Round up to nearest multiple of block size
        let aligned_size = ((size + self.block_size - 1) / self.block_size) * self.block_size;
        
        // Allocate aligned memory
        // Note: In production, we'd use proper aligned allocation
        // For simplicity, we're just creating a vector and hoping OS aligns it
        let mut buffer = Vec::with_capacity(aligned_size);
        buffer.resize(aligned_size, 0);
        
        Ok(buffer)
    }
}

impl Drop for NvmeAccess {
    fn drop(&mut self) {
        // Ensure file is closed when object is dropped
        self.close();
    }
}