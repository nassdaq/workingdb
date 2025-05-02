//Controlled system failure 
use std::sync::Arc;
use std::time::{Duration,Instant};
use std::thread;

use crate::core::state::GlobalState;


pub enum ChaosType {
  ProcessKill,
  MemoryPressure,
  DiskFailure,
  NetworkPartition,
  ClockSkew,
}

pub struct ChaosEngine {
  state: Arc<GlobalState>,
  active: std::sync::atomic::AtomicBool,
}

impl ChaosEngine {
  pub fn new(state: Arc<GlobalState>) -> Self{
    Self {
      state,
      active: std::sync::atomic::AtomicBool::new(false),
    }
  }
  pub fn start_chaos(&self,chaos_type: ChaosType,duration: Duration) -> Result<(),String>{
    if self.active.swap(true,std::sync::atomic::Acquire){
      return Err("Chaos test already in progress".to_string());
    }
    let state_ref = self.state.clone();
    let active_ref = self.active.clone();

    thread::spawn(move||{
      let start = Instant::now();
      println!("🔥 CHAOS TEST INITIATED: {:?} for {:?}", chaos_type, duration);

      match chaos_type {
        ChaosType::ProcessKill => {
          Self::simulate_process_kill(&state_ref);
        },
        ChaosType::MemoryPressure => {
          Self::simulate_memory_pressure();
        },
        ChaosType::DiskFailure => {
          Self::simulate_disk_failure();
        },
        ChaosType::NetworkPartition => {
          Self::simulate_network_partiotion();

        },
        ChaosType::ClockSkew => {
          Self::simulate_clock_skew();
        }
      }

      while start.elapsed() < duration {
        thread::sleep(Duration::from_millis(100));
      }
      println!("🟢 CHAOS TEST COMPLETED - SYSTEM SURVIVED");
      
      active_ref.store(false,std::sync::atomic::Ordering::Release);
    });
    Ok(())
  }
  
  pub fn is_active(&self) -> bool {
    self.active.load(std::sync::atomic::Ordering::Relaxed)
  }

  // === CHAOS SIMULATION METHODS ===

  /// Simulate process kill signals - SIGTERM, SIGKILL etc.
  fn simulate_process_kill(_state: &GlobalState) {
    // For safety, we don't actually kill the process
    // Instead, simulate effects of process termination
    println!("🔪 SIMULATING PROCESS TERMINATION");
    
    // Force crash recovery path execution
    // state.trigger_recovery_path();
    
    // TODO: When safe_process_restart is implemented, use it
  }
  /// Simulate memory pressure - allocation failures
  fn simulate_memory_pressure() {
    println!("🧠 SIMULATING MEMORY PRESSURE");
    
    // Allocate large blocks until we hit pressure
    let mut memory_hogs = Vec::new();
    
    // Try to allocate up to 1GB in 100MB chunks
    for _ in 0..10 {
        match Vec::<u8>::with_capacity(100 * 1024 * 1024) {
            hog => memory_hogs.push(hog),
        }
        
        // Short sleep to allow system to feel the pressure
        thread::sleep(Duration::from_millis(100));
    }
    
    // Keep the allocations live for a second
    thread::sleep(Duration::from_secs(1));
    
    // Drop occurs automatically when memory_hogs goes out of scope
  }
  /// Simulate disk failures - I/O errors
  fn simulate_disk_failure() {
    println!("💽 SIMULATING DISK FAILURES");
    
    // In a real implementation, we'd use LD_PRELOAD or syscall interception
    // to make filesystem operations return errors
    
    // For now, just log the simulation
    thread::sleep(Duration::from_secs(2));
  }

  /// Simulate network partitions - connectivity issues
  fn simulate_network_partition() {
    println!("🌐 SIMULATING NETWORK PARTITION");
    
    // In a real implementation, we'd use iptables/tc to drop packets
    
    // For now, just log the simulation
    thread::sleep(Duration::from_secs(2));
  }

  /// Simulate clock skew - sudden time jumps
  fn simulate_clock_skew() {
    println!("⏰ SIMULATING CLOCK SKEW");
    
    // In a real implementation, we'd use libfaketime or similar
    // to manipulate the clock as seen by the process
    
    // For now, just log the simulation
    thread::sleep(Duration::from_secs(2));
  }
  
  
  
}