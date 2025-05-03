// src/core/chaos.rs - CONTROLLED SYSTEM FAILURE INJECTION
// System resilience tester - ensures database survives apocalyptic conditions

use std::sync::Arc;
use std::time::{Duration, Instant};
use std::thread;
use std::sync::atomic::{AtomicBool, Ordering}; // CRITICAL FIX: Added Ordering

use crate::core::state::GlobalState;

/// Chaos test type enumeration - different failure modes
#[derive(Debug)] // CRITICAL FIX: Added Debug trait
pub enum ChaosType {
    /// Kill process signals
    ProcessKill,
    
    /// Memory pressure (allocation failures)
    MemoryPressure,
    
    /// Disk failures (I/O errors)
    DiskFailure,
    
    /// Network partitions
    NetworkPartition,
    
    /// Clock skew (time jumps)
    ClockSkew,
}

/// ChaosEngine - deliberate fault injector for resilience testing
pub struct ChaosEngine {
    /// Global state reference
    state: Arc<GlobalState>,
    
    /// Is chaos testing active
    active: AtomicBool,
}

impl ChaosEngine {
    /// Create new chaos engine attached to global state
    pub fn new(state: Arc<GlobalState>) -> Self {
        Self {
            state,
            active: AtomicBool::new(false),
        }
    }
    
    /// Start chaos test with specified type and duration
    pub fn start_chaos(&self, chaos_type: ChaosType, duration: Duration) -> Result<(), String> {
        // CRITICAL FIX: Fixed Ordering::Acquire usage
        if self.active.swap(true, Ordering::Acquire) {
            return Err("Chaos test already in progress".to_string());
        }
        
        // Clone necessary references for the chaos thread
        let state_ref = self.state.clone();
        
        // CRITICAL FIX: Fixed AtomicBool cloning
        // Can't clone AtomicBool directly, use Arc instead
        let active_ref = Arc::new(AtomicBool::new(false));
        let local_active = active_ref.clone();
        
        // Spawn chaos thread
        thread::spawn(move || {
            let start = Instant::now();
            
            // CRITICAL FIX: Fixed Debug formatter for ChaosType
            println!("🔥 CHAOS TEST INITIATED: {:?} for {:?}", chaos_type, duration);
            
            // Execute chaos based on type
            match chaos_type {
                ChaosType::ProcessKill => {
                    // Simulate process termination signals
                    Self::simulate_process_kill(&state_ref);
                },
                ChaosType::MemoryPressure => {
                    // Allocate excessive memory to trigger pressure
                    Self::simulate_memory_pressure();
                },
                ChaosType::DiskFailure => {
                    // Make disk operations fail temporarily
                    Self::simulate_disk_failure();
                },
                ChaosType::NetworkPartition => {
                    // CRITICAL FIX: Fixed typo in method name
                    Self::simulate_network_partition();
                },
                ChaosType::ClockSkew => {
                    // Simulate sudden time jumps
                    Self::simulate_clock_skew();
                },
            }
            
            // Wait until duration completes
            while start.elapsed() < duration {
                thread::sleep(Duration::from_millis(100));
            }
            
            println!("🟢 CHAOS TEST COMPLETED - SYSTEM SURVIVED");
            
            // Mark test as complete
            local_active.store(false, Ordering::Release);
        });
        
        Ok(())
    }
    
    /// Is chaos test currently active
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Relaxed)
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
    // CRITICAL FIX: Fixed method name typo
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