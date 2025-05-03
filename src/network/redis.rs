// src/network/redis.rs - RESP PROTOCOL ADAPTER
// Redis protocol implementation for compatibility with Redis clients

use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt}; // CRITICAL FIX: Added AsyncWriteExt

use crate::core::state::GlobalState;
use crate::network::tcp::{TcpConnection, ProtocolHandler};

/// Redis protocol handler
pub struct RedisHandler {
    // Shared database state
    state: Arc<GlobalState>,
}

/// Redis command parsed from RESP protocol
#[derive(Debug)]
enum RedisCommand {
    // GET key
    Get(Vec<u8>),
    
    // SET key value [EX seconds]
    Set(Vec<u8>, Vec<u8>, Option<Duration>),
    
    // DEL key
    Del(Vec<u8>),
    
    // PING
    Ping,
    
    // INFO
    Info,
}

impl RedisHandler {
    /// Create new Redis protocol handler
    pub fn new(state: Arc<GlobalState>) -> Self {
        Self { state }
    }
    
    /// Parse Redis command from buffer
    async fn parse_command(
        conn: &mut TcpConnection
    ) -> Result<Option<RedisCommand>, Box<dyn std::error::Error + Send + Sync>> {
        // Read first byte to determine RESP type
        let mut type_buf = [0u8; 1];
        let n = conn.read(&mut type_buf).await?;
        
        if n == 0 {
            // EOF - client disconnected
            return Ok(None);
        }
        
        match type_buf[0] {
            b'*' => {
                // Array - typical for Redis commands
                let array_len = Self::parse_integer(conn).await?;
                
                // Read array elements
                let mut parts = Vec::with_capacity(array_len as usize);
                for _ in 0..array_len {
                    // Each element is a bulk string
                    let mut bulk_type = [0u8; 1];
                    conn.read_exact(&mut bulk_type).await?;
                    
                    // CRITICAL FIX: Fixed incorrect character check
                    if bulk_type[0] != b'$' {
                        return Err(format!("Expected bulk string in array, got: {}", bulk_type[0] as char).into());
                    }
                    
                    // Parse bulk string
                    let bulk = Self::parse_bulk_string(conn).await?;
                    parts.push(bulk);
                }
                
                // Parse command based on parts
                if parts.is_empty() {
                    return Err("Empty command".into());
                }
                
                // Convert first part to uppercase for command name
                let cmd = parts[0].to_ascii_uppercase();
                
                // Parse different commands
                match cmd.as_slice() {
                    b"GET" if parts.len() == 2 => {
                        Ok(Some(RedisCommand::Get(parts[1].clone())))
                    }
                    b"SET" if parts.len() >= 3 => {
                        // Check for EX option
                        let mut ttl = None;
                        if parts.len() >= 5 && parts[3].to_ascii_uppercase() == b"EX" {
                            // Parse seconds
                            if let Ok(secs) = std::str::from_utf8(&parts[4])
                                .map_err(|_| "Invalid TTL value")?
                                .parse::<u64>() {
                                ttl = Some(Duration::from_secs(secs));
                            }
                        }
                        
                        Ok(Some(RedisCommand::Set(
                            parts[1].clone(),
                            parts[2].clone(),
                            ttl
                        )))
                    }
                    b"DEL" if parts.len() == 2 => {
                        Ok(Some(RedisCommand::Del(parts[1].clone())))
                    }
                    b"PING" => {
                        Ok(Some(RedisCommand::Ping))
                    }
                    b"INFO" => {
                        Ok(Some(RedisCommand::Info))
                    }
                    _ => {
                        Err(format!("Unsupported command: {:?}", 
                            String::from_utf8_lossy(&cmd)).into())
                    }
                }
            }
            _ => {
                Err(format!("Unsupported RESP type: {}", type_buf[0] as char).into())
            }
        }
    }
    
    /// Parse integer from RESP protocol
    async fn parse_integer(
        conn: &mut TcpConnection
    ) -> Result<i64, Box<dyn std::error::Error + Send + Sync>> {
        // Read until CRLF
        let mut buf = Vec::new();
        let mut byte = [0u8; 1];
        
        loop {
            conn.read_exact(&mut byte).await?;
            
            if byte[0] == b'\r' {
                // Expect LF next
                let mut lf = [0u8; 1];
                conn.read_exact(&mut lf).await?;
                
                if lf[0] != b'\n' {
                    return Err("Expected LF after CR".into());
                }
                
                break;
            }
            
            buf.push(byte[0]);
        }
        
        // Convert to string and parse
        let num_str = std::str::from_utf8(&buf)
            .map_err(|_| "Invalid UTF-8 in integer")?;
            
        let num = num_str.parse::<i64>()
            .map_err(|_| format!("Invalid integer: {}", num_str))?;
            
        Ok(num)
    }
    
    /// Parse bulk string from RESP protocol
    async fn parse_bulk_string(
        conn: &mut TcpConnection
    ) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
        // Read length
        let length = Self::parse_integer(conn).await?;
        
        if length < 0 {
            // NULL string
            return Ok(Vec::new());
        }
        
        // Read string content
        let mut buf = vec![0u8; length as usize];
        conn.read_exact(&mut buf).await?;
        
        // Read trailing CRLF
        let mut crlf = [0u8; 2];
        conn.read_exact(&mut crlf).await?;
        
        if crlf != [b'\r', b'\n'] {
            return Err("Expected CRLF after bulk string".into());
        }
        
        Ok(buf)
    }
    
    /// Write simple string response
    async fn write_simple_string(
        conn: &mut TcpConnection, 
        s: &str
    ) -> Result<(), std::io::Error> {
        let mut response = Vec::with_capacity(s.len() + 3);
        response.push(b'+');
        response.extend_from_slice(s.as_bytes());
        response.extend_from_slice(b"\r\n");
        
        conn.write_all(&response).await
    }
    
    /// Write error response
    async fn write_error(
        conn: &mut TcpConnection, 
        err: &str
    ) -> Result<(), std::io::Error> {
        let mut response = Vec::with_capacity(err.len() + 3);
        response.push(b'-');
        response.extend_from_slice(err.as_bytes());
        response.extend_from_slice(b"\r\n");
        
        conn.write_all(&response).await
    }
    
    /// Write bulk string response
    async fn write_bulk_string(
        conn: &mut TcpConnection, 
        data: Option<&[u8]>
    ) -> Result<(), std::io::Error> {
        match data {
            Some(bytes) => {
                // Format: $<length>\r\n<data>\r\n
                let header = format!("${}\r\n", bytes.len());
                
                conn.write_all(header.as_bytes()).await?;
                conn.write_all(bytes).await?;
                conn.write_all(b"\r\n").await
            }
            None => {
                // Null bulk string: $-1\r\n
                conn.write_all(b"$-1\r\n").await
            }
        }
    }
    
    /// Write integer response
    async fn write_integer(
        conn: &mut TcpConnection, 
        n: i64
    ) -> Result<(), std::io::Error> {
        let response = format!(":{}\r\n", n);
        conn.write_all(response.as_bytes()).await
    }
}

impl ProtocolHandler for RedisHandler {
    async fn handle_connection(
        &mut self,
        conn: &mut TcpConnection
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> { // CRITICAL FIX: Added Send + Sync
        println!("Handling Redis protocol connection");
        
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
                    Self::write_error(conn, &format!("ERR {}", e)).await?;
                    continue;
                }
            };
            
            // Execute command
            match cmd {
                RedisCommand::Get(key) => {
                    // Get value from storage
                    let value = self.state.get(&key);
                    
                    // Send response
                    match value {
                        Some(v) => Self::write_bulk_string(conn, Some(&v)).await?,
                        None => Self::write_bulk_string(conn, None).await?,
                    }
                }
                RedisCommand::Set(key, value, ttl) => {
                    // Set value in storage
                    match self.state.set(&key, value, ttl) {
                        Ok(_) => Self::write_simple_string(conn, "OK").await?,
                        Err(e) => Self::write_error(conn, &format!("ERR {}", e)).await?,
                    }
                }
                RedisCommand::Del(key) => {
                    // Delete value from storage
                    match self.state.delete(&key) {
                        Ok(true) => Self::write_integer(conn, 1).await?,
                        Ok(false) => Self::write_integer(conn, 0).await?,
                        Err(e) => Self::write_error(conn, &format!("ERR {}", e)).await?,
                    }
                }
                RedisCommand::Ping => {
                    // Simple ping-pong
                    Self::write_simple_string(conn, "PONG").await?
                }
                RedisCommand::Info => {
                    // Get system info
                    let (uptime, reads, writes, deletes, read_lat, write_lat) = 
                        self.state.get_stats();
                        
                    let info = format!(
                        "# Server\r\nworkingdb_version:0.1.0\r\nuptime_seconds:{}\r\n\
                         # Stats\r\ntotal_reads:{}\r\ntotal_writes:{}\r\n\
                         total_deletes:{}\r\navg_read_latency_ns:{}\r\n\
                         avg_write_latency_ns:{}\r\n",
                        uptime.as_secs(), reads, writes, deletes, read_lat, write_lat
                    );
                    
                    Self::write_bulk_string(conn, Some(info.as_bytes())).await?
                }
            }
        }
        
        Ok(())
    }
}