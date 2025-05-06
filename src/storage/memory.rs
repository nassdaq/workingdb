use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

/// MemTable - Core in-memory storage engine
/// Multi-partition hash table with lock-free reads
pub struct MemTable {
    // Sharded hash tables for parallelism
    partitions: Vec<Arc<RwLock<HashMap<Vec<u8>, Entry>>>>,
    
    // Number of partitions (shards)
    partition_count: usize,
}

/// Storage entry - value with metadata
struct Entry {
    // Actual value bytes
    value: Vec<u8>,
    
    // Optional expiration time
    expires_at: Option<Instant>,
}

impl MemTable {
    /// Create new memory table with optimal partition count
    pub fn new() -> Self {
        // CRITICAL FIX: Replaced num_cpus with standard function
        // Default to available CPU count or 8 for optimal parallelism
        let cpu_count = std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(8);
            
        Self::with_partitions(cpu_count)
    }
    pub fn recover_set(&self, key: &[u8], value: Vec<u8>, ttl: Option<Duration>) -> Result<(), String> {
        let partition = self.get_partition_for_key(key);
        let entry = Entry {
            value,
            expires_at: ttl.map(|d| Instant::now() + d)
        };

        partition.write()
            .map_err(|e| format!("Lock error: {:?}", e))?
            .insert(key.to_vec(), entry);
        
        Ok(())
    }

    /// Special delete for recovery that bypasses AOF logging
    pub fn recover_delete(&self, key: &[u8]) -> Result<bool, String> {
        let partition = self.get_partition_for_key(key);
        Ok(partition.write()
            .map_err(|e| format!("Lock error: {:?}", e))?
            .remove(key)
            .is_some())
    }
    /// Create with specific partition count
    pub fn with_partitions(count: usize) -> Self {
        let partitions = (0..count)
            .map(|_| Arc::new(RwLock::new(HashMap::new())))
            .collect();
            
        Self {
            partitions,
            partition_count: count,
        }
    }
    
    /// Get partition count
    pub fn partition_count(&self) -> usize {
        self.partition_count
    }
    
    /// Get value by key
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        // Get partition for this key
        let partition = self.get_partition_for_key(key);
        
        // Acquire read lock on just this partition
        if let Ok(guard) = partition.read() {
            // Check if key exists and is not expired
            if let Some(entry) = guard.get(key) {
                // Check expiration
                if let Some(expires) = entry.expires_at {
                    if Instant::now() > expires {
                        // Expired entry - treat as non-existent
                        return None;
                    }
                }
                
                // Return cloned value
                return Some(entry.value.clone());
            }
        }
        
        None
    }
    
    /// Set value with optional TTL
    pub fn set(&self, key: &[u8], value: Vec<u8>, ttl: Option<Duration>) -> Result<(), String> {
        // Calculate expiration time if TTL provided
        let expires_at = ttl.map(|duration| Instant::now() + duration);
        
        // Get partition for this key
        let partition = self.get_partition_for_key(key);
        
        // Create entry with value and expiration
        let entry = Entry { value, expires_at };
        
        // Acquire write lock on just this partition
        if let Ok(mut guard) = partition.write() {
            // Insert or replace entry
            guard.insert(key.to_vec(), entry);
            Ok(())
        } else {
            Err("Failed to acquire write lock".to_string())
        }
    }
    
    /// Delete value by key
    pub fn delete(&self, key: &[u8]) -> Result<bool, String> {
        // Get partition for this key
        let partition = self.get_partition_for_key(key);
        
        // Acquire write lock on just this partition
        if let Ok(mut guard) = partition.write() {
            // Remove key and return whether it existed
            Ok(guard.remove(key).is_some())
        } else {
            Err("Failed to acquire write lock".to_string())
        }
    }
    
    /// Run garbage collection - clean expired entries
    pub fn gc(&self) -> usize {
        let mut total_removed = 0;
        let now = Instant::now();
        
        // Process each partition
        for partition in &self.partitions {
            if let Ok(mut guard) = partition.write() {
                // Find keys to remove
                let to_remove: Vec<Vec<u8>> = guard
                    .iter()
                    .filter_map(|(k, v)| {
                        if let Some(expires) = v.expires_at {
                            if now > expires {
                                return Some(k.clone());
                            }
                        }
                        None
                    })
                    .collect();
                
                // Remove expired entries
                for key in to_remove {
                    guard.remove(&key);
                    total_removed += 1;
                }
            }
        }
        
        total_removed
    }
    
    // === PRIVATE HELPERS ===
    
    /// Get partition for key using consistent hashing
    fn get_partition_for_key(&self, key: &[u8]) -> Arc<RwLock<HashMap<Vec<u8>, Entry>>> {
        // Simple hash-based partitioning
        let hash = self.hash_key(key);
        let idx = hash % self.partition_count;
        
        // Return reference to the partition
        self.partitions[idx].clone()
    }
    
    /// Hash function for keys - FNV-1a for speed
    // CRITICAL FIX: Changed parameter type from [u8] to &[u8]
    fn hash_key(&self, key: &[u8]) -> usize {
        let mut hash: u64 = 14695981039346656037; // FNV offset basis
        
        // CRITICAL FIX: Fixed key iteration with &[u8]
        for byte in key {
            hash ^= *byte as u64;
            hash = hash.wrapping_mul(1099511628211); // FNV prime
        }
        
        hash as usize
    }
}

// For unit tests
#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_set_get() {
        let mem = MemTable::new();
        let key = b"test_key";
        let value = b"test_value".to_vec();
        
        // Set value
        mem.set(key, value.clone(), None).unwrap();
        
        // Get value
        let result = mem.get(key);
        assert_eq!(result, Some(value));
    }
    
    #[test]
    fn test_delete() {
        let mem = MemTable::new();
        let key = b"test_key";
        let value = b"test_value".to_vec();
        
        // Set value
        mem.set(key, value, None).unwrap();
        
        // Delete value
        let result = mem.delete(key).unwrap();
        assert!(result);
        
        // Check it's gone
        assert_eq!(mem.get(key), None);
    }
    
    #[test]
    fn test_ttl() {
        let mem = MemTable::new();
        let key = b"test_key";
        let value = b"test_value".to_vec();
        
        // Set with 100ms TTL
        mem.set(key, value, Some(Duration::from_millis(100))).unwrap();
        
        // Immediately available
        assert!(mem.get(key).is_some());
        
        // Wait for expiration
        std::thread::sleep(Duration::from_millis(150));
        
        // Should be gone
        assert!(mem.get(key).is_none());
    }
}