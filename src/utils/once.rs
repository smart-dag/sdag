use std::sync::atomic::{AtomicBool, Ordering};

use may::sync::SyncFlag;

#[derive(Debug)]
pub struct Once<T> {
    data: Option<T>,
    b_init: AtomicBool,
    waiter: SyncFlag,
}

unsafe impl<T: Send> Send for Once<T> {}
unsafe impl<T: Sync> Sync for Once<T> {}

impl<T> Default for Once<T> {
    fn default() -> Self {
        Once {
            data: None,
            b_init: AtomicBool::new(false),
            waiter: SyncFlag::new(),
        }
    }
}

impl<T> std::ops::Deref for Once<T> {
    type Target = T;
    fn deref(&self) -> &T {
        self.get()
    }
}

impl<T> Once<T> {
    pub fn new(data: Option<T>) -> Self {
        if data.is_none() {
            return Self::default();
        }

        let waiter = SyncFlag::new();
        waiter.fire();

        Once {
            data,
            b_init: AtomicBool::new(true),
            waiter,
        }
    }

    /// set the OnceOption data, if it's already set, return false
    pub fn call_once<F: FnOnce() -> T>(&self, f: F) -> bool {
        if self.b_init.load(Ordering::Relaxed) {
            return false;
        }

        if !self.b_init.swap(true, Ordering::AcqRel) {
            let data_ptr = &self.data as *const _ as *mut Option<T>;
            unsafe { data_ptr.replace(Some(f())) };
            self.waiter.fire();
            return true;
        }

        false
    }

    /// get the data, if it's not initialized, block until data is set
    pub fn get(&self) -> &T {
        self.waiter.wait();
        self.data.as_ref().expect("no data in Once")
    }

    /// Returns true if some `call_once` call has completed successfully.
    /// Specifically, `is_completed` will return false in
    /// the following situations:
    ///   * `call_once` was not called at all,
    ///   * `call_once` was called, but has not yet completed,
    pub fn is_completed(&self) -> bool {
        self.waiter.is_fired()
    }
}
