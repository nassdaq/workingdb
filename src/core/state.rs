// src/core/state.rs - GLOBAL DATABASE STATE - LOCK-FREE ARCHITECTURE
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::storage::memory::MemTable;
use crate::persistence::aof::AppendOnlyFile;

/// GlobalState - Central database state manager
/// Core abstraction maintaining atomic consistency across components
pub struct GlobalState {
    // Core storage engine - primary data substrate
    mem_table: Arc<MemTable>,
    
    // Persistence layer - durability mechanism
    // CRITICAL FIX: Change to interior mutability pattern with Arc<Mutex<>>
    aof: std::sync::Mutex<AppendOnlyFile>,
    
    // System statistics - performance telemetry
    stats: Statistics,
}

/// Statistical counters for system monitoring
struct Statistics {
    // System start time - uptime tracking
    start_time: Instant,
    
    // Operation counters - throughput metrics
    reads: AtomicU64,
    writes: AtomicU64,
    deletes: AtomicU64,
    
    // Performance metrics - latency tracking
    write_latency_ns: AtomicU64,
    read_latency_ns: AtomicU64,
}

impl GlobalState {
    /// Create new global state with provided storage components
    pub fn new(mem_table: Arc<MemTable>, aof: AppendOnlyFile) -> Self {
        Self {
            mem_table,
            // CRITICAL FIX: Wrap in Mutex for interior mutability
            aof: std::sync::Mutex::new(aof),
            stats: Statistics {
                start_time: Instant::now(),
                reads: AtomicU64::new(0),
                writes: AtomicU64::new(0),
                deletes: AtomicU64::new(0),
                write_latency_ns: AtomicU64::new(0),
                read_latency_ns: AtomicU64::new(0),
            },
        }
    }
    
    /// Get value from storage
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let start = Instant::now();
        
        // Core read operation
        let result = self.mem_table.get(key);
        
        // Update metrics
        let elapsed = start.elapsed().as_nanos() as u64;
        self.stats.reads.fetch_add(1, Ordering::Relaxed);
        self.stats.read_latency_ns.fetch_add(elapsed, Ordering::Relaxed);
        
        result
    }
    
    /// Set value in storage with optional TTL
    // CRITICAL FIX: Same signature, using interior mutability
    pub fn set(&self, key: &[u8], value: Vec<u8>, ttl: Option<Duration>) -> Result<(), String> {
        let start = Instant::now();
        
        // Core write operation
        let result = match self.mem_table.set(key, value.clone(), ttl) {
            Ok(_) => {
                // Log to AOF for durability - use Mutex lock
                // CRITICAL FIX: Access AOF through mutex
                if let Ok(mut aof_guard) = self.aof.lock() {
                    if let Err(e) = aof_guard.append_set(key, &value, ttl) {
                        return Err(format!("AOF write failed: {}", e));
                    }
                } else {
                    return Err("Failed to acquire AOF lock".to_string());
                }
                Ok(())
            }
            Err(e) => Err(format!("Memory write failed: {}", e)),
        };
        
        // Update metrics
        let elapsed = start.elapsed().as_nanos() as u64;
        self.stats.writes.fetch_add(1, Ordering::Relaxed);
        self.stats.write_latency_ns.fetch_add(elapsed, Ordering::Relaxed);
        
        result
    }
    
    /// Delete value from storage
    // CRITICAL FIX: Same signature, using interior mutability
    pub fn delete(&self, key: &[u8]) -> Result<bool, String> {
        // Core delete operation
        let exists = match self.mem_table.delete(key) {
            Ok(exists) => {
                // Log to AOF for durability
                // CRITICAL FIX: Access AOF through mutex
                if let Ok(mut aof_guard) = self.aof.lock() {
                    if let Err(e) = aof_guard.append_delete(key) {
                        return Err(format!("AOF delete failed: {}", e));
                    }
                } else {
                    return Err("Failed to acquire AOF lock".to_string());
                }
                exists
            }
            Err(e) => return Err(format!("Memory delete failed: {}", e)),
        };
        
        // Update metrics
        self.stats.deletes.fetch_add(1, Ordering::Relaxed);
        
        Ok(exists)
    }
    
    /// Get system statistics
    pub fn get_stats(&self) -> (Duration, u64, u64, u64, u64, u64) {
        let uptime = self.stats.start_time.elapsed();
        
        let reads = self.stats.reads.load(Ordering::Relaxed);
        let writes = self.stats.writes.load(Ordering::Relaxed);
        let deletes = self.stats.deletes.load(Ordering::Relaxed);
        
        let avg_write_latency = if writes > 0 {
            self.stats.write_latency_ns.load(Ordering::Relaxed) / writes
        } else {
            0
        };
        
        let avg_read_latency = if reads > 0 {
            self.stats.read_latency_ns.load(Ordering::Relaxed) / reads
        } else {
            0
        };
        
        (uptime, reads, writes, deletes, avg_read_latency, avg_write_latency)
    }
}