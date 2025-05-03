// src/lib.rs - CORE WORKINGDB PRIMITIVES
// Core database engine exports and feature flags

// SYSTEM RUNTIME FLAGS - FEATURE ACTIVATION
#![feature(box_patterns)]      // Enable box pattern matching
#![allow(unused_variables)]    // Silence warnings during development
#![allow(dead_code)]           // Silence warnings during development

// Core module exports
pub mod core;
pub mod storage;
pub mod query;
pub mod network;
pub mod persistence;
pub mod util;

// Re-export primary public interface
pub use core::state::GlobalState;
pub use storage::memory::MemTable;
pub use persistence::aof::AppendOnlyFile;
pub use network::tcp::TcpServer;

/// WorkingDB - High-performance database engine
/// 
/// WorkingDB is a multi-protocol database engine designed for extreme performance,
/// resilience, and compatibility with existing ecosystems.
pub struct WorkingDB {
    // Global state reference
    state: std::sync::Arc<GlobalState>,
    
    // TCP server for network connections
    server: Option<TcpServer>,
    
    // Database configuration
    config: Config,
}

/// Database configuration
#[derive(Clone, Debug)]
pub struct Config {
    // Host to bind to
    pub host: String,
    
    // Port to listen on
    pub port: u16,
    
    // Data directory path
    pub data_path: std::path::PathBuf,
    
    // Memory limit in bytes (0 = no limit)
    pub memory_limit: usize,
    
    // Enable persistence
    pub persistence_enabled: bool,
    
    // GC interval in milliseconds
    pub gc_interval_ms: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 6379,
            data_path: std::path::PathBuf::from("./data"),
            memory_limit: 0,
            persistence_enabled: true,
            gc_interval_ms: 1000,
        }
    }
}

impl WorkingDB {
    /// Create new WorkingDB instance with default configuration
    pub fn new() -> Self {
        Self::with_config(Config::default())
    }
    
    /// Create new WorkingDB instance with custom configuration
    pub fn with_config(config: Config) -> Self {
        // Initialize with config, but don't start network server yet
        Self {
            state: std::sync::Arc::new(GlobalState::new(
                std::sync::Arc::new(MemTable::new()),
                AppendOnlyFile::new(&config.data_path).unwrap_or_else(|e| {
                    eprintln!("Failed to initialize AOF: {}", e);
                    std::process::exit(1);
                }),
            )),
            server: None,
            config,
        }
    }
    
    /// Start the database server
    // CRITICAL FIX: Changed error handling to use specific error type
    pub async fn start(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        // Create TCP server
        let server = TcpServer::new(
            self.config.host.clone(),
            self.config.port,
            self.state.clone(),
        );
        
        // Start server
        println!("Starting WorkingDB on {}:{}", self.config.host, self.config.port);
        self.server = Some(server.clone());
        
        // Run server (this will block until shutdown)
        // CRITICAL FIX: Fixed error handling by extracting result before using ?
        match server.run().await {
            Ok(_) => Ok(()),
            Err(e) => {
                // Convert boxed error to a regular error
                let err_str = format!("Server error: {}", e);
                Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, err_str)))
            }
        }
    }
    
    /// Shutdown the database server
    pub fn shutdown(&mut self) {
        println!("Shutting down WorkingDB");
        self.server = None;
    }
    
    /// Get key from database
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.state.get(key)
    }
    
    /// Set key in database
    pub fn set(&self, key: &[u8], value: Vec<u8>) -> Result<(), String> {
        // CRITICAL FIX: No change needed as we've updated GlobalState to use interior mutability
        self.state.set(key, value, None)
    }
    
    /// Delete key from database
    pub fn delete(&self, key: &[u8]) -> Result<bool, String> {
        self.state.delete(key)
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_basic_operations() {
        // Create in-memory database for testing
        let config = Config {
            persistence_enabled: false,
            ..Config::default()
        };
        
        let db = WorkingDB::with_config(config);
        
        // Test SET operation
        let key = b"test_key";
        let value = b"test_value".to_vec();
        db.set(key, value.clone()).unwrap();
        
        // Test GET operation
        let result = db.get(key);
        assert_eq!(result, Some(value));
        
        // Test DELETE operation
        let deleted = db.delete(key).unwrap();
        assert!(deleted);
        
        // Key should be gone
        let result = db.get(key);
        assert_eq!(result, None);
    }
}