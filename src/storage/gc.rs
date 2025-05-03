// src/storage/gc.rs - EPOCH-BASED REAPER (NO-STW)
// Lock-free garbage collection for memory management

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use std::thread;

use crate::storage::memory::MemTable;

/// GarbageCollector - Manages memory cleanup and expired entries
pub struct GarbageCollector {
    // Memory table reference
    mem_table: Arc<MemTable>,
    
    // GC thread control - signal to stop
    should_stop: Arc<AtomicUsize>,
    
    // GC statistics
    stats: GcStats,
}

/// Statistics for garbage collection
// CRITICAL FIX: Removed Clone derive since AtomicUsize doesn't implement Clone
#[derive(Debug, Default)]
pub struct GcStats {
    // Total number of GC cycles run
    pub cycles: AtomicUsize,
    
    // Total number of objects collected
    pub collected: AtomicUsize,
    
    // Last run timestamp
    pub last_run: Option<Instant>,
    
    // Average cycle duration in milliseconds
    pub avg_duration_ms: AtomicUsize,
}

// CRITICAL FIX: Manual Clone implementation for GcStats
impl Clone for GcStats {
    fn clone(&self) -> Self {
        Self {
            cycles: AtomicUsize::new(self.cycles.load(Ordering::Relaxed)),
            collected: AtomicUsize::new(self.collected.load(Ordering::Relaxed)),
            last_run: self.last_run.clone(),
            avg_duration_ms: AtomicUsize::new(self.avg_duration_ms.load(Ordering::Relaxed)),
        }
    }
}

impl GarbageCollector {
    /// Create new garbage collector for memory table
    pub fn new(mem_table: Arc<MemTable>) -> Self {
        Self {
            mem_table,
            should_stop: Arc::new(AtomicUsize::new(0)),
            stats: GcStats::default(),
        }
    }
    
    /// Start background GC thread
    pub fn start_background_gc(&self, interval: Duration) -> thread::JoinHandle<()> {
        // Clone references for the GC thread
        let mem_table = self.mem_table.clone();
        let should_stop = self.should_stop.clone();
        let stats = self.stats.clone();
        
        // Spawn GC thread
        thread::spawn(move || {
            println!("Starting background GC thread");
            
            while should_stop.load(Ordering::Relaxed) == 0 {
                // Sleep for interval
                thread::sleep(interval);
                
                // Run GC cycle
                let start = Instant::now();
                let collected = mem_table.gc();
                let duration = start.elapsed();
                
                // Update statistics
                stats.cycles.fetch_add(1, Ordering::Relaxed);
                stats.collected.fetch_add(collected, Ordering::Relaxed);
                
                // Update average duration using exponential moving average
                let current_avg = stats.avg_duration_ms.load(Ordering::Relaxed);
                let new_duration_ms = duration.as_millis() as usize;
                
                if current_avg == 0 {
                    stats.avg_duration_ms.store(new_duration_ms, Ordering::Relaxed);
                } else {
                    // 90% old value, 10% new value
                    let new_avg = (current_avg * 9 + new_duration_ms) / 10;
                    stats.avg_duration_ms.store(new_avg, Ordering::Relaxed);
                }
                
                if collected > 0 {
                    println!("GC cycle complete: {} objects collected in {:?}", 
                        collected, duration);
                }
            }
            
            println!("Background GC thread stopped");
        })
    }
    
    /// Stop background GC thread
    pub fn stop(&self) {
        self.should_stop.store(1, Ordering::Relaxed);
    }
    
    /// Run a single GC cycle manually
    // CRITICAL FIX: Changed to &mut self to allow modifying last_run
    pub fn run_now(&mut self) -> usize {
        let start = Instant::now();
        let collected = self.mem_table.gc();
        
        // Update statistics
        self.stats.cycles.fetch_add(1, Ordering::Relaxed);
        self.stats.collected.fetch_add(collected, Ordering::Relaxed);
        self.stats.last_run = Some(start);
        
        collected
    }
    
    /// Get current GC statistics
    pub fn get_stats(&self) -> GcStatsSnapshot {
        GcStatsSnapshot {
            cycles: self.stats.cycles.load(Ordering::Relaxed),
            collected: self.stats.collected.load(Ordering::Relaxed),
            last_run: self.stats.last_run,
            avg_duration_ms: self.stats.avg_duration_ms.load(Ordering::Relaxed),
        }
    }
}

/// Immutable snapshot of GC statistics
#[derive(Clone, Debug)]
pub struct GcStatsSnapshot {
    pub cycles: usize,
    pub collected: usize,
    pub last_run: Option<Instant>,
    pub avg_duration_ms: usize,
}

impl Drop for GarbageCollector {
    fn drop(&mut self) {
        // Ensure GC thread is stopped when GC is dropped
        self.stop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_gc_basic() {
        // Create memory table with short-lived entries
        let mem = Arc::new(MemTable::new());
        
        // Add entries with TTL
        for i in 0..100 {
            let key = format!("key_{}", i).into_bytes();
            let value = format!("value_{}", i).into_bytes();
            
            // Half with 100ms TTL, half permanent
            let ttl = if i % 2 == 0 {
                Some(Duration::from_millis(100))
            } else {
                None
            };
            
            mem.set(&key, value, ttl).unwrap();
        }
        
        // Create GC
        let mut gc = GarbageCollector::new(mem.clone());
        
        // Sleep to allow TTLs to expire
        thread::sleep(Duration::from_millis(150));
        
        // Run GC
        let collected = gc.run_now();
        
        // Should have collected about 50 entries
        assert!(collected >= 45, "Expected at least 45 collected, got {}", collected);
        assert!(collected <= 55, "Expected at most 55 collected, got {}", collected);
        
        // Check stats
        let stats = gc.get_stats();
        assert_eq!(stats.cycles, 1);
        assert_eq!(stats.collected, collected);
        assert!(stats.last_run.is_some());
    }
    
    // CRITICAL FIX: Removed dangling }.run_now(); syntax error
}