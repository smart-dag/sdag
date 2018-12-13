/// AppendListExt is a low-level primitive supporting two safe operations:
/// `push`, which appends a node to the list, and `iter` which iterates the list
/// The list cannot be shrunk whilst in use.
use std::sync::atomic::{AtomicPtr, Ordering};
use std::{mem, ptr};

use rcu_cell::{RcuCell, RcuReader};

type NodePtr<T> = Option<Box<Node<T>>>;

trait IntoRaw<T> {
    fn into_raw(self) -> *mut T;
}

impl<T> IntoRaw<Node<T>> for NodePtr<T> {
    fn into_raw(self) -> *mut Node<T> {
        match self {
            Some(b) => Box::into_raw(b),
            None => ptr::null_mut(),
        }
    }
}

#[derive(Debug)]
struct Node<T> {
    value: RcuCell<T>,
    next: AppendListExt<T>,
}

#[derive(Debug)]
pub struct AppendListExt<T>(AtomicPtr<Node<T>>);

impl<T> AppendListExt<T> {
    unsafe fn from_raw(ptr: *mut Node<T>) -> NodePtr<T> {
        if ptr.is_null() {
            None
        } else {
            Some(Box::from_raw(ptr))
        }
    }

    fn new_internal(node: NodePtr<T>) -> Self {
        AppendListExt(AtomicPtr::new(node.into_raw()))
    }

    pub fn new() -> Self {
        Self::new_internal(None)
    }

    pub fn append(&self, value: T) {
        self.append_list(AppendListExt::new_internal(Some(Box::new(Node {
            value: RcuCell::new(Some(value)),
            next: AppendListExt::new(),
        }))));
    }

    unsafe fn append_ptr(&self, p: *mut Node<T>) {
        loop {
            match self.0.compare_exchange_weak(
                ptr::null_mut(),
                p,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(head) => {
                    if !head.is_null() {
                        return (*head).next.append_ptr(p);
                    }
                }
            }
        }
    }

    pub fn append_list(&self, other: AppendListExt<T>) {
        let p = other.0.load(Ordering::Acquire);
        mem::forget(other);
        unsafe { self.append_ptr(p) };
    }

    pub fn iter(&self) -> AppendListIterator<T> {
        AppendListIterator(&self.0)
    }

    /// Returns true if the AppendListExt contains no data
    pub fn is_empty(&self) -> bool {
        self.iter().next().is_none()
    }

    /// get the length of the list, this is O(n)
    pub fn len(&self) -> usize {
        let mut l = 0;
        for _ in self.iter() {
            l += 1;
        }
        l
    }

    // Note: this just clear the item, but not remove it
    pub fn remove_with<F: Fn(&T) -> bool>(&self, f: F) {
        let mut ptr = &self.0;

        loop {
            let p = ptr.load(Ordering::Acquire);
            // reach the end
            if p.is_null() {
                return;
            }
            let value = unsafe {
                ptr = &(*p).next.0;
                &(*p).value
            };
            // skip those removed items
            if let Some(r) = value.read() {
                if f(&*r) {
                    // clear the item
                    loop {
                        match value.try_lock() {
                            None => {} // try lock until success
                            Some(mut g) => {
                                g.update(None);
                                return;
                            }
                        }
                    }
                }
            }
        }
    }
}

impl<'a, T> IntoIterator for &'a AppendListExt<T> {
    type Item = RcuReader<T>;
    type IntoIter = AppendListIterator<'a, T>;

    fn into_iter(self) -> AppendListIterator<'a, T> {
        self.iter()
    }
}

impl<T> ::std::iter::FromIterator<T> for AppendListExt<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let l = AppendListExt::new();
        for i in iter {
            l.append(i);
        }
        l
    }
}

impl<T> Default for AppendListExt<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Drop for AppendListExt<T> {
    fn drop(&mut self) {
        unsafe { Self::from_raw(mem::replace(self.0.get_mut(), ptr::null_mut())) };
    }
}

#[derive(Debug)]
pub struct AppendListIterator<'a, T: 'a>(&'a AtomicPtr<Node<T>>);

impl<'a, T: 'a> Iterator for AppendListIterator<'a, T> {
    type Item = RcuReader<T>;

    fn next(&mut self) -> Option<RcuReader<T>> {
        loop {
            let p = self.0.load(Ordering::Acquire);
            if p.is_null() {
                return None;
            }
            let value = unsafe {
                self.0 = &(*p).next.0;
                &(*p).value
            };

            // skip those removed items
            if let Some(r) = value.read() {
                return Some(r);
            }
        }
    }
}
