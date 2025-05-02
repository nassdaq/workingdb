//memory storage engine - lock-free B-Tree

use std::collections::HashMap;
use std::sync::{Arc,RwLock};
use std::time::{Duration,Instant};


pub struct MemTable{
  partitions: Vec<Arc<RwLock<HashMap<Vec<u8>,Entry>>>>,
  partition_count: usize,
}

struct Entry {

  value: Vec<u8>,
  expires_at: Option<Instant>,

}

impl MemTable{
  pub fn new() -> Self{
    let cpu_count = num_cpus::get();
    Self::with_partitions(cpu_count)
  }

  pub fn with_partitions(count: usize) -> Self{
    let partitions=(0..count)
      .map(|_| Arc::new(RwLock::new(HashMap::new())))
      .collect();
    Self {
      partitions,
      partition_count,
    }
  }

  pub fn partition_count(&self) -> usize {
    self.partition_count
  }

  pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
    let partition = self.get_partition_for_key(key);

    if let Ok(guard) = partition.read(){
      if let Some(entry) = guard.get(key){
        if let Some(expires) = entry.expires_at {
          if Instant::now() > expires{
            return None;
          }
        }
        return Some(entry.value.clone());
      }
    }
    None
  }
  pub fn set(&self, key: &[u8],value: Vec<u8>,ttl: Option<Duration>) -> Result<(),String> {
    let expires_at = ttl.map(|duration| Instant::now() + duration);
    let partition = self.get_partition_for_key(key);
    let entry = Entry {value,expires_at};

    if let Ok(mut guard) = partition.write(){
      guard.insert(key.to_vec(),entry);
      Ok(())
    } else {
      Err("Failed to acquire write lock".to_string())
    }
  }

  pub fn gc(&self) -> usize{
    let mut total_removed = 0;
    let now = Instant::now();

    for partition in self.partitions{
      if let Ok(mut guard) =partition.write(){
        let to_remove: Vec<Vec<u8>> =guard
          .iter()
          .filter_map(|(k,v)|{
            if let some(expires) =v.expires_at{
              if now>expires{
                return Some(k.clone());
              }
            }
            None
          })
          .collect();
        for key in to_remove{
          guard.remove(&key);
          total_removed += 1
        }

      }

    }
    total_removed
  }

  // === PRIVATE HELPERS ===
  fn get_partition_for_key(&self,key: &[u8]) -> Arc<RwLock<HashMap<Vec<u8>,Entry>>>{
  
    let hash = self.hash_key(key);
    let idx = hash % self.partition_count;
  
    self.partitions[idx].clone()
  }

  fn hash_key(&self,key: [u8]) -> usize {
    let mut hash: u64 = 14695981039346656037; // FNVoffset basis

    for byte in key{
      hash ^= *byte as u64;
      hash = hash.wrapping_mul(1099511628211); // NV prime
    }
    hash as usize
  }
}

// tests
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



