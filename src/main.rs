use std::path::PathBuf;
use std::process::exit;
use std::sync::Arc;
mod core;

mod storage;
mod query;
mod network;
mod persistence;
mod util;

use core::state::GlobalState;
use network::tcp::TcpServer;
use storage::memory::MemTable;
use persistance::aof::AppendOnlyFile;


#[tokio::main]
async fn main() -> Result<(),Box<dyn std::error::Error>>{
    println!("🔥 WorkingDB v0.1.0 - DATABASE INITIALIZATION SEQUENCE 🔥");

    let args = parse_args();

    println!("🔌 Storage Path: {}", args.data_path.display());
    println!("🌐 Listening on: {}:{}", args.host, args.port);

    let mem_table = Arc::new(memTable::new());
    println!("💾 Memory table initialized with {} partitions", mem_table.partition_count());

    let aof = AppendOnlyFile::new(&args.data_path)?;
    println!("📝 Persistence layer active, {} records recovered", aof.replay_count());


    let state = Arc::new(GlobalState::new(mem_table,aof));

    let server = TcpServer::new(args.host,args.port,state.clone());
    println!("🚀 Server initialized, ready to process requests");

    println!("⚡ WorkingDB online - ACCEPTING CONNECTIONS");

    if let Err(e) = server.run().await {
        eprintln!("💥 Fatal error: {}", e);
        exit(1);
    }
    Ok(())
   
      
}

struct Args {
    host: String,
    port: u16,
    data_path: PathBuf,

}

fn parse_args() -> Args {

    let host = std::env::var("WORKINGDB_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let port = std::env::var("WORKINGDB_PORT")
        .map(|p| p.parse::<u16>().unwrap_or(7777))
        .unwrap_or(7777);
    let data_path = std::env::var("WORKING_DATA")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("./data"));


    Args {host,port,data_path}

}

