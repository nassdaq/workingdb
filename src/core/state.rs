use std::sync::Arc;
use std::time::{Duration,Instant};

use crate::storage::memory::MemTable;
use crate::persistence::aof::AppendOnlyFile;

pub struct GlobalState{
  mem_table: Arch<MemTable>,
  aof: AppendOnlyFile,
  stats: Statistics,

}

struct Statistics{
  start_time: Instant,

  reads: std::sync::atomic::AtomicU64,
  writes: std::sync::atomic::AtomicU64,
  deletes: std::sync::atomic::AtomicU64,

  write_latency_ns: std::sync::atomic::AtomicU64,
  read_latency_ns: std::sync::atomic::AtomicU64,


}

impl GlobalState {
  pub fn new(mem_table: Arc<MemTable>, aof: AppendOnlyFile) -> Self{
    Self {
      mem_table,
      aof,
      stats: Statistics { 
        start_time: Instant::now(),
        reads: std::sync::atomic::AtomicU64::new(0),
        writes: std::sync::atomic::AtomicU64::new(0),
        deletes: std::sync::atomic::AtomicU64::new(0),
        write_latency_ns: std::sync::atomic::AtomicU64::new(0),
        read_latency_ns: std::sync::atomic::AtomicU64::new(0), 
      },

    }
  }
  pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
    let start = Instant::now();
    let result = self.mem_table.get(key);

    let elapsed = start.elapsed().as_nanos() as u64;
    self.stats.reads.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    self.stats.read_latency_ns.fetch_add(elapsed,std::sync::atomic::Ordering::Relaxed);

    result
  }
  
  pub fn set(&self,key: &[u8],value: Vec<u8>,ttl: Option<Duration>) -> Result<(),String> {
    let start = Instant::now();

    let result = match self.mem_table.set(key,value.clone(),ttl){
      Ok(_) => {
        if let Err(e) = self.aof.append_set(key,&value,ttl) {
          return Err(format!("AOF write failed: {}",e));
        }
        Ok(())
      }
      Err(e) => Err(format!("Memory write failed: {}",e)),
    };

    let elapsed = start.elapsed().as_nanos() as u64;
    self.stats.writes.fetch_add(1, std::sync::atomic::Relaxed);
    self.stats.write_latency_ns.fetch_add(elapsed, std::sync::atomic::Ordering::Relaxed);


    result
  }

  pub fn delete(&self,key: [u8]) -> Result<bool,String>{
    let exists = match self.mem_table.delete(key){
      Ok(exists) => {
        if let Err(e) = self.aof.append_delete(key){
          return Err(format!("AOF delete failed: {}",e));

        }
        exists
      }
      Err(e) => Err(format!("Memory delete failed: {}",e)),
    };

    self.stats.deletes.fetch_add(1,std::sync::atomic::Ordering::Relaxed);

    Ok(exists)
  }

  pub fn get_stats(&self) -> (Duration,u64,u64,u64,u64,u64) {
    let uptime = self.stats.start_time.elapsed();
    let reads = self.stats.reads.load(std::sync::atomic::Ordering::Relaxed);
    let writes = self.stats.deletes.load(std::sync::atomic::Ordering::Relaxed);
    let deletes = self.stats.load(std::sync::atomic::Ordering::Relaxed);


    let avg_write_latency =if writes > 0 {
      self.stats.write_latency_ns.load(std::sync::atomic::Ordering::Relaxed) / writes
    } else {
      0
    };

    let avg_read_latency = if reads > 0 {
      self.stats.read_latency_ns.load(std::sync::atomic::Ordering::Relaxed) / reads
    } else {
      0
    };

    (uptime,reads,writes,deletes,avg_read_latency,avg_write_latency)
  }



}

