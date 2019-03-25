use std::sync::atomic::{AtomicBool, Ordering};

pub struct OnceOption<T> {
    b_init: AtomicBool,
    data: Option<T>,
}

unsafe impl<T: Send> Send for OnceOption<T> {}
unsafe impl<T: Sync> Sync for OnceOption<T> {}

/// `OnceOption` can only be init once
impl<T> OnceOption<T> {
    pub fn new() -> Self {
        Default::default()
    }

    /// set the OnceOption data, if it's already set, then just return the data back
    pub fn set(&self, data: T) -> Option<T> {
        if self.get().is_none() {
            let data_ptr = &self.data as *const _ as *mut Option<T>;
            unsafe { data_ptr.replace(Some(data)) };
            self.b_init.store(true, Ordering::Release);
            None
        } else {
            Some(data)
        }
    }

    /// get the data, if it's not initialized, return None
    pub fn get(&self) -> Option<&T> {
        // fast check and check again with strict ordering
        if self.b_init.load(Ordering::Relaxed) || self.b_init.load(Ordering::Acquire) {
            let ret = self.data.as_ref();
            if ret.is_none() {
                panic!("there is no data set, but init set to true");
            }
            ret
        } else {
            None
        }
    }
}

impl<T> Default for OnceOption<T> {
    fn default() -> Self {
        OnceOption {
            b_init: AtomicBool::new(false),
            data: None,
        }
    }
}
