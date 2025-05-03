// KERNEL PANIC → GRACEFUL SHUTDOWN
// Custom panic handler for crash recovery

use std::panic;
use std::process;
use std::sync::Once;

/// Initialize custom panic handler
// TODO: Replaces default panic handler with our recovery-oriented version
pub fn init_panic_handler() {
    static INIT: Once = Once::new();
    
    INIT.call_once(|| {
        // Set custom panic hook
        panic::set_hook(Box::new(|panic_info| {
            // Log panic information
            eprintln!("!!! CRITICAL ERROR - RECOVERY SEQUENCE INITIATED !!!");
            eprintln!("Panic details: {:?}", panic_info);
            
            if let Some(location) = panic_info.location() {
                eprintln!("Panic occurred in file '{}' at line {}", 
                    location.file(), location.line());
            }
            
            if let Some(payload) = panic_info.payload().downcast_ref::<&str>() {
                eprintln!("Panic message: {}", payload);
            }
            
            // Print stack trace if available
            #[cfg(feature = "backtrace")]
            {
                eprintln!("Stack trace:");
                let backtrace = backtrace::Backtrace::new();
                eprintln!("{:?}", backtrace);
            }
            
            // In a real implementation, we would:
            // 1. Attempt to flush all pending writes to disk
            // 2. Signal any cluster peers about our impending shutdown
            // 3. Write crash marker file for recovery on next start
            
            eprintln!("Attempting graceful shutdown...");
            
            // Exit with error code
            process::exit(1);
        }));
    });
    
    // Log initialization
    println!("Custom panic handler initialized");
}

/// Try to execute a function, recovering from panic if possible
pub fn try_recover<F, R>(f: F) -> Result<R, String>
where
    F: FnOnce() -> R + panic::UnwindSafe,
{
    match panic::catch_unwind(f) {
        Ok(result) => Ok(result),
        Err(e) => {
            // Convert panic payload to error message
            let err_msg = if let Some(s) = e.downcast_ref::<&str>() {
                format!("Panic occurred: {}", s)
            } else if let Some(s) = e.downcast_ref::<String>() {
                format!("Panic occurred: {}", s)
            } else {
                "Unknown panic occurred".to_string()
            };
            
            Err(err_msg)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_try_recover() {
        // Function that doesn't panic
        let result = try_recover(|| {
            42
        });
        assert_eq!(result, Ok(42));
        
        // Function that panics
        let result = try_recover(|| {
            panic!("Test panic");
            #[allow(unreachable_code)]
            0
        });
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Test panic"));
    }
}