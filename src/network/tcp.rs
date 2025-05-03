// src/network/tcp.rs - TOKIO + IO_URING NETWORK STACK
use std::sync::Arc;
use std::task::{Context, Poll};
use std::pin::Pin;
use std::io::{self};
use tokio::io::ReadBuf;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncRead, AsyncWrite, AsyncReadExt, AsyncWriteExt};

use crate::core::state::GlobalState;

/// Protocol detection result
pub enum Protocol {
    Redis,
    Memcached,
    SQLite,
    Unknown,
}

/// TCP server for handling client connections
#[derive(Clone)]
pub struct TcpServer {
    // Bind address
    host: String,
    
    // Listen port
    port: u16,
    
    // Shared database state
    state: Arc<GlobalState>,
}

impl TcpServer {
    /// Create new TCP server
    pub fn new(host: String, port: u16, state: Arc<GlobalState>) -> Self {
        Self { host, port, state }
    }
    
    /// Run the server - listen for connections
    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Bind to address
        let addr = format!("{}:{}", self.host, self.port);
        let listener = TcpListener::bind(&addr).await?;
        
        println!("Listening on {}", addr);
        
        // Accept connections
        loop {
            match listener.accept().await {
                Ok((socket, addr)) => {
                    println!("New connection from {}", addr);
                    
                    // Clone reference to state for the handler task
                    let state = self.state.clone();
                    
                    // Spawn task for this connection
                    tokio::spawn(async move {
                        if let Err(e) = Self::handle_connection(socket, state).await {
                            eprintln!("Connection error: {}", e);
                        }
                    });
                }
                Err(e) => {
                    eprintln!("Accept error: {}", e);
                }
            }
        }
    }
    
    /// Handle a single client connection
    async fn handle_connection(
        socket: TcpStream, 
        state: Arc<GlobalState>
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Create connection handler
        let mut conn = TcpConnection::new(socket);
        
        // Read initial bytes to detect protocol
        let protocol = conn.detect_protocol().await?;
        
        // Handle based on protocol
        match protocol {
            Protocol::Redis => {
                // Use Redis protocol handler
                use crate::network::redis::RedisHandler;
                let mut handler = RedisHandler::new(state);
                handler.handle_connection(&mut conn).await?;
            }
            Protocol::Memcached => {
                // Use Memcached protocol handler
                use crate::network::memcached::MemcachedHandler;
                let mut handler = MemcachedHandler::new(state);
                handler.handle_connection(&mut conn).await?;
            }
            Protocol::SQLite => {
                // Use SQLite protocol handler
                // (Placeholder - would be implemented in a real version)
                conn.write_all(b"SQLite protocol not implemented yet\r\n").await?;
            }
            Protocol::Unknown => {
                // Unknown protocol - send error
                conn.write_all(b"ERROR: Unknown protocol\r\n").await?;
            }
        }
        
        Ok(())
    }
}

/// TCP connection wrapper
pub struct TcpConnection {
    // Socket for this connection
    socket: TcpStream,
    
    // Read buffer
    buffer: Vec<u8>,
}

impl TcpConnection {
    /// Create new TCP connection
    pub fn new(socket: TcpStream) -> Self {
        Self {
            socket,
            buffer: vec![0; 4096], // 4KB initial buffer
        }
    }
    
    /// Connect to server
    pub async fn connect(host: &str, port: u16) -> Result<Self, std::io::Error> {
        let addr = format!("{}:{}", host, port);
        let socket = TcpStream::connect(addr).await?;
        
        Ok(Self::new(socket))
    }
    
    /// Read bytes from connection
    pub async fn read(&mut self, buf: &mut [u8]) -> Result<usize, std::io::Error> {
        self.socket.read(buf).await
    }
    
    /// Write bytes to connection
    pub async fn write_all(&mut self, buf: &[u8]) -> Result<(), std::io::Error> {
        self.socket.write_all(buf).await
    }
    
    /// Detect protocol based on initial bytes
    pub async fn detect_protocol(&mut self) -> Result<Protocol, std::io::Error> {
        // Read initial bytes
        let n = self.socket.peek(&mut self.buffer).await?;
        
        if n == 0 {
            return Ok(Protocol::Unknown);
        }
        
        // Check for Redis protocol
        if self.buffer[0] == b'*' || self.buffer[0] == b'$' || 
           self.buffer[0] == b'+' || self.buffer[0] == b'-' || 
           self.buffer[0] == b':' {
            return Ok(Protocol::Redis);
        }
        
        // Check for Memcached protocol (text-based)
        // FIXED: Array size mismatch
        let commands: [&[u8]; 5] = [b"get ", b"set ", b"add ", b"replace ", b"delete "];
        for cmd in &commands {
            if self.buffer.starts_with(cmd) {
                return Ok(Protocol::Memcached);
            }
        }
        
        // Check for SQLite protocol
        // Note: SQLite wire protocol detection would be more complex
        // This is a placeholder
        if self.buffer.len() >= 16 && self.buffer[0] == 0x53 && self.buffer[1] == 0x51 {
            return Ok(Protocol::SQLite);
        }
        
        // Default to unknown
        Ok(Protocol::Unknown)
    }
}

// CRITICAL FIX: Implement AsyncRead trait for TcpConnection
// This allows using read_exact and other AsyncReadExt methods
impl AsyncRead for TcpConnection {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.socket).poll_read(cx, buf)
    }
}

// CRITICAL FIX: Implement AsyncWrite trait for TcpConnection
// This allows using write_all and other AsyncWriteExt methods
impl AsyncWrite for TcpConnection {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        Pin::new(&mut self.socket).poll_write(cx, buf)
    }
    
    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.socket).poll_flush(cx)
    }
    
    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.socket).poll_shutdown(cx)
    }
}

/// Protocol handler trait
pub trait ProtocolHandler {
    /// Handle a client connection with this protocol
    async fn handle_connection(
        &mut self, 
        conn: &mut TcpConnection
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}