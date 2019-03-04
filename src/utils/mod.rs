pub mod atomic_lock;
#[macro_use]
pub mod event;
pub mod append_list;
pub mod append_list_ext;
pub mod fifo_cache;
pub mod map_lock;
pub mod once_option;

pub use self::append_list::AppendList;
pub use self::append_list_ext::AppendListExt;
pub use self::atomic_lock::{AtomicLock, AtomicLockGuard};
pub use self::fifo_cache::FifoCache;
pub use self::map_lock::{MapLock, MapLockGuard};
pub use self::once_option::OnceOption;

use std::io::{Error, ErrorKind};
use std::time::{Duration, Instant};

use may::coroutine;

// timely wait condition
pub fn wait_cond<F: Fn() -> bool>(timeout: Option<Duration>, f: F) -> Result<(), Error> {
    if let Some(timeout) = timeout {
        let now = Instant::now();

        while !f() {
            if now.elapsed() >= timeout {
                return Err(Error::from(ErrorKind::TimedOut));
            }
            // every one second check again
            coroutine::sleep(Duration::from_millis(1));
        }
    } else {
        while !f() {
            // every one second check again
            coroutine::sleep(Duration::from_millis(1));
        }
    }

    Ok(())
}
