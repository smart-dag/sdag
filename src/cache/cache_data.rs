use std::sync::Arc;

use error::Result;
use kv_store::LoadFromKv;
use may::coroutine;
use rcu_cell::{RcuCell, RcuReader};

//---------------------------------------------------------------------------------------
// HashKey
//---------------------------------------------------------------------------------------
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct HashKey(pub Arc<String>);
impl HashKey {
    pub fn new(key: &str) -> Self {
        HashKey(Arc::new(key.to_owned()))
    }
}

impl ::std::ops::Deref for HashKey {
    type Target = String;
    #[inline]
    fn deref(&self) -> &String {
        self.0.as_ref()
    }
}

impl ::std::borrow::Borrow<str> for HashKey {
    #[inline]
    fn borrow(&self) -> &str {
        self.0.as_str()
    }
}

impl ::std::borrow::Borrow<String> for HashKey {
    #[inline]
    fn borrow(&self) -> &String {
        self.0.as_ref()
    }
}

//---------------------------------------------------------------------------------------
// CachedData
//---------------------------------------------------------------------------------------
#[derive(Debug)]
pub struct CachedData<K, V> {
    pub key: Arc<K>,
    pub data: RcuCell<V>,
}

impl<K, V> Clone for CachedData<K, V> {
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            data: self.data.clone(),
        }
    }
}

impl<K: PartialEq, V> PartialEq for CachedData<K, V> {
    fn eq(&self, other: &Self) -> bool {
        // Arc::ptr_eq(&self.key, &other.key)
        self.key == other.key
    }
}

impl<K, V> CachedData<K, V> {
    pub fn empty(key: Arc<K>) -> Self {
        CachedData {
            key,
            data: Default::default(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.data.read().is_none()
    }

    pub(super) fn raw_read(&self) -> RcuReader<V> {
        self.data.read().expect("raw_read expect data!")
    }

    pub fn set(&self, data: V) {
        loop {
            if let Some(mut g) = self.data.try_lock() {
                assert_eq!(g.as_ref().is_none(), true);
                return g.update(Some(data));
            }
            coroutine::yield_now();
        }
    }

    pub fn clear(&self) {
        loop {
            if let Some(mut g) = self.data.try_lock() {
                if g.as_ref().is_some() {
                    g.update(None);
                }
                return;
            }
            coroutine::yield_now();
        }
    }
}

impl<K, V: LoadFromKv<K>> CachedData<K, V> {
    // read from mem or else form db
    pub fn read(&self) -> Result<RcuReader<V>> {
        match self.data.read() {
            None => {
                // we should read from KV store and
                // return update self with the correct data
                self.read_from_db()
            }
            Some(r) => Ok(r),
        }
    }

    // if the value is not set, read form database
    fn read_from_db(&self) -> Result<RcuReader<V>> {
        loop {
            if let Some(mut g) = self.data.try_lock() {
                if g.as_ref().is_none() {
                    // try read from kv store with the key and update self
                    let joint = V::load_from_kv(&self.key)?;
                    g.update(Some(joint));
                }
                // the data is already set
                return Ok(self.data.read().unwrap());
            }
            coroutine::yield_now();
        }
    }

    // save the value to database and clear the data memory
    pub fn save_to_db(&self) -> Result<()> {
        loop {
            if let Some(mut g) = self.data.try_lock() {
                match g.as_ref() {
                    Some(v) => {
                        v.save_to_kv(&self.key)?;
                    }
                    None => bail!("no data found to save to db"),
                }
                g.update(None);
                return Ok(());
            }
            coroutine::yield_now();
        }
    }
}
