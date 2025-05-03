// WorkingDB - A database engine for the modern infrastructure era
// MAIN PROGRAM ENTRY POINT - SYSTEM INITIALIZATION

use std::path::PathBuf;
use std::process::exit;
use std::sync::Arc;

// Import core modules from lib.rs
use workingdb::core::state::GlobalState;
use workingdb::network::tcp::TcpServer;
use workingdb::storage::memory::MemTable; // CRITICAL FIX: Fixed casing
use workingdb::persistence::aof::AppendOnlyFile;
use workingdb::util::panic::init_panic_handler;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize custom panic handler
    init_panic_handler();
    
    // SYSTEM HEADER - IDENTITY SIGNATURE
    println!("🔥 WorkingDB v0.1.0 - DATABASE INITIALIZATION SEQUENCE 🔥");
    
    // PARSE CLI ARGUMENTS - EXECUTION PARAMETERS
    let args = parse_args();
    
    // PRINT SYSTEM CONFIG - DEPLOYMENT PARAMETERS
    println!("🔌 Storage Path: {}", args.data_path.display());
    println!("🌐 Listening on: {}:{}", args.host, args.port);
    
    // INITIALIZE CORE STORAGE ENGINE - MEMORY SUBSTRATE
    let mem_table = Arc::new(MemTable::new()); // CRITICAL FIX: Fixed casing
    println!("💾 Memory table initialized with {} partitions", mem_table.partition_count());
    
    // INITIALIZE PERSISTENCE LAYER - DURABILITY ENGINE
    let aof = AppendOnlyFile::new(&args.data_path)?;
    println!("📝 Persistence layer active, {} records recovered", aof.replay_count());
    
    // CREATE GLOBAL STATE - SHARED CONTEXT
    let state = Arc::new(GlobalState::new(mem_table, aof));
    
    // INITIALIZE NETWORK STACK - PROTOCOL INTERFACE
    let server = TcpServer::new(args.host, args.port, state.clone());
    println!("🚀 Server initialized, ready to process requests");
    
    // START MAIN EXECUTION LOOP - CONNECTION PROCESSING
    println!("⚡ WorkingDB online - ACCEPTING CONNECTIONS");
    if let Err(e) = server.run().await {
        eprintln!("💥 Fatal error: {}", e);
        exit(1);
    }
    
    Ok(())
}

// CLI ARGUMENT STRUCTURE - EXECUTION CONFIG
struct Args {
    host: String,
    port: u16,
    data_path: PathBuf,
}

// PARSE COMMAND LINE ARGS - CONFIG EXTRACTION
fn parse_args() -> Args {
    // Basic arg parsing - EXPAND LATER WITH CLAP
    let host = std::env::var("WORKINGDB_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let port = std::env::var("WORKINGDB_PORT")
        .map(|p| p.parse::<u16>().unwrap_or(6379))
        .unwrap_or(6379);
    let data_path = std::env::var("WORKINGDB_DATA")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("./data"));
    
    Args { host, port, data_path }
}