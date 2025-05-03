// src/network/memcached.rs - BINARY PROTOCOL SHIM
// Memcached protocol implementation for legacy compatibility

use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::core::state::GlobalState;
use crate::network::tcp::{TcpConnection, ProtocolHandler};

/// Memcached protocol handler
pub struct MemcachedHandler {
    // Shared database state
    state: Arc<GlobalState>,
}

/// Memcached command parsed from text protocol
enum MemcachedCommand {
    // get <key>
    Get(String),
    
    // set <key> <flags> <exptime> <bytes> [noreply]\r\n<data>\r\n
    Set(String, u32, u32, Vec<u8>, bool),
    
    // delete <key> [noreply]
    Delete(String, bool),
    
    // stats
    Stats,
    
    // version
    Version,
}

impl MemcachedHandler {
    /// Create new Memcached protocol handler
    pub fn new(state: Arc<GlobalState>) -> Self {
        Self { state }
    }
    
    /// Parse Memcached text command line
    async fn parse_command_line(
        conn: &mut TcpConnection
    ) -> Result<Option<String>, Box<dyn std::error::Error + Send + Sync>> {
        let mut line = Vec::new();
        let mut buf = [0u8; 1];
        
        // Read until \r\n
        loop {
            let n = conn.read(&mut buf).await?;
            if n == 0 {
                // Connection closed
                if line.is_empty() {
                    return Ok(None);
                } else {
                    return Err("Unexpected end of stream".into());
                }
            }
            
            line.push(buf[0]);
            
            // Check for end of line
            if line.len() >= 2 && line[line.len() - 2] == b'\r' && line[line.len() - 1] == b'\n' {
                // Remove \r\n
                line.truncate(line.len() - 2);
                break;
            }
        }
        
        // Convert to string
        let cmd_line = String::from_utf8(line)
            .map_err(|_| "Invalid UTF-8 in command")?;
            
        Ok(Some(cmd_line))
    }
    
    /// Parse full command including data for SET
    async fn parse_command(
        conn: &mut TcpConnection
    ) -> Result<Option<MemcachedCommand>, Box<dyn std::error::Error + Send + Sync>> {
        // Read command line
        let line = match Self::parse_command_line(conn).await? {
            Some(l) => l,
            None => return Ok(None),
        };
        
        // Split into parts
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.is_empty() {
            return Err("Empty command".into());
        }
        
        // Parse based on command
        match parts[0].to_lowercase().as_str() {
            "get" if parts.len() >= 2 => {
                Ok(Some(MemcachedCommand::Get(parts[1].to_string())))
            }
            "set" if parts.len() >= 5 => {
                // Parse flags, exptime, bytes
                let flags = parts[2].parse::<u32>()
                    .map_err(|_| format!("Invalid flags: {}", parts[2]))?;
                    
                let exptime = parts[3].parse::<u32>()
                    .map_err(|_| format!("Invalid exptime: {}", parts[3]))?;
                    
                let bytes = parts[4].parse::<usize>()
                    .map_err(|_| format!("Invalid bytes: {}", parts[4]))?;
                
                // Check for noreply
                let noreply = parts.len() >= 6 && parts[5] == "noreply";
                
                // Read data
                let mut data = vec![0u8; bytes];
                conn.read_exact(&mut data).await?;
                
                // Read trailing \r\n
                let mut crlf = [0u8; 2];
                conn.read_exact(&mut crlf).await?;
                
                if crlf != [b'\r', b'\n'] {
                    return Err("Expected CRLF after data".into());
                }
                
                Ok(Some(MemcachedCommand::Set(
                    parts[1].to_string(),
                    flags,
                    exptime,
                    data,
                    noreply
                )))
            }
            "delete" if parts.len() >= 2 => {
                // Check for noreply
                let noreply = parts.len() >= 3 && parts[2] == "noreply";
                
                Ok(Some(MemcachedCommand::Delete(
                    parts[1].to_string(),
                    noreply
                )))
            }
            "stats" => {
                Ok(Some(MemcachedCommand::Stats))
            }
            "version" => {
                Ok(Some(MemcachedCommand::Version))
            }
            _ => {
                Err(format!("Unknown command: {}", parts[0]).into())
            }
        }
    }
}

impl ProtocolHandler for MemcachedHandler {
    async fn handle_connection(
        &mut self,
        conn: &mut TcpConnection
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        println!("Handling Memcached protocol connection");
        
        // Process commands in a loop
        loop {
            // Parse command
            let cmd = match Self::parse_command(conn).await {
                Ok(Some(cmd)) => cmd,
                Ok(None) => {
                    // Client disconnected
                    println!("Client disconnected");
                    break;
                }
                Err(e) => {
                    eprintln!("Error parsing command: {}", e);
                    conn.write_all(format!("ERROR {}\r\n", e).as_bytes()).await?;
                    continue;
                }
            };
            
            // Execute command
            match cmd {
                MemcachedCommand::Get(key) => {
                    // Get value from storage
                    match self.state.get(key.as_bytes()) {
                        Some(value) => {
                            // Format: VALUE <key> <flags> <bytes>\r\n<data>\r\nEND\r\n
                            conn.write_all(format!("VALUE {} 0 {}\r\n", key, value.len()).as_bytes()).await?;
                            conn.write_all(&value).await?;
                            conn.write_all(b"\r\n").await?;
                            conn.write_all(b"END\r\n").await?;
                        }
                        None => {
                            // Item not found - just return END
                            conn.write_all(b"END\r\n").await?;
                        }
                    }
                }
                MemcachedCommand::Set(key, _flags, exptime, value, noreply) => {
                    // Convert exptime to Duration if non-zero
                    let ttl = if exptime > 0 {
                        Some(Duration::from_secs(exptime as u64))
                    } else {
                        None
                    };
                    
                    // Set value in storage
                    // CRITICAL FIX: No change needed as we've updated GlobalState to use interior mutability
                    match self.state.set(key.as_bytes(), value, ttl) {
                        Ok(_) => {
                            if !noreply {
                                conn.write_all(b"STORED\r\n").await?;
                            }
                        }
                        Err(e) => {
                            if !noreply {
                                conn.write_all(format!("SERVER_ERROR {}\r\n", e).as_bytes()).await?;
                            }
                        }
                    }
                }
                MemcachedCommand::Delete(key, noreply) => {
                    // Delete value from storage
                    match self.state.delete(key.as_bytes()) {
                        Ok(true) => {
                            if !noreply {
                                conn.write_all(b"DELETED\r\n").await?;
                            }
                        }
                        Ok(false) => {
                            if !noreply {
                                conn.write_all(b"NOT_FOUND\r\n").await?;
                            }
                        }
                        Err(e) => {
                            if !noreply {
                                conn.write_all(format!("SERVER_ERROR {}\r\n", e).as_bytes()).await?;
                            }
                        }
                    }
                }
                MemcachedCommand::Stats => {
                    // Get system stats
                    let (uptime, reads, writes, deletes, read_lat, write_lat) = 
                        self.state.get_stats();
                        
                    // Format stats response
                    let stats = [
                        format!("STAT pid {}\r\n", std::process::id()),
                        format!("STAT uptime {}\r\n", uptime.as_secs()),
                        format!("STAT version 0.1.0\r\n"),
                        format!("STAT cmd_get {}\r\n", reads),
                        format!("STAT cmd_set {}\r\n", writes),
                        format!("STAT cmd_delete {}\r\n", deletes),
                        format!("STAT read_latency_ns {}\r\n", read_lat),
                        format!("STAT write_latency_ns {}\r\n", write_lat),
                        "END\r\n".to_string(),
                    ];
                    
                    // Send stats
                    for stat in stats {
                        conn.write_all(stat.as_bytes()).await?;
                    }
                }
                MemcachedCommand::Version => {
                    // Send version
                    conn.write_all(b"VERSION 0.1.0\r\n").await?;
                }
            }
        }
        
        Ok(())
    }
}