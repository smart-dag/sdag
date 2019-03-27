extern crate rocksdb;

use self::rocksdb::{DBVector, DB};
use error::Result;
use lru_cache::LruCache;
use std::hash::Hash;
use std::borrow::Borrow;

pub struct KvStore {
    db: DB,
}

impl KvStore {
    pub fn new(path: &str) -> Self {
        let db = DB::open_default(path).unwrap();
        KvStore { db }
    }

    fn put<K, V>(&mut self, key: K, value: V) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.db.put(key.as_ref(), value.as_ref())?;
        Ok(())
    }

    fn remove<K: AsRef<[u8]>>(&mut self, key: &K) -> Result<()> {
        self.db.delete(key.as_ref())?;
        Ok(())
    }

    fn get<K, V>(&self, key: &K) -> Result<DBVector>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let v = self
            .db
            .get(key.as_ref())?
            .ok_or_else(|| format_err!("read last mci from kv failed"))?;

        Ok(v)
    }
}

#[allow(dead_code)]

pub struct LruSafeCache<K, V>
where
    K: Eq + Hash + Clone,
    V: Eq + Clone,
{
    cache: LruCache<K, V>,
    local: KvStore,
}

#[allow(dead_code)]

impl<K, V> LruSafeCache<K, V>
where
    K: Eq + Hash + Clone,

    V: Eq + Clone,
{
    pub fn new(&self, size: usize, store: KvStore) -> Self {
        LruSafeCache {
            cache: LruCache::new(size),
            local: store,
        }
    }

    pub fn with_capacity(&mut self, capacity: usize) {
        self.cache.set_capacity(capacity)
    }

    pub fn capacity(&self) -> usize {
        self.cache.capacity()
    }

    pub fn get_mut(&mut self, key: &K) -> Option<&[u8]>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        match self.cache.get_mut(key) {
            Some(v) => {
                return Some(v.as_ref());
            }

            None => {
                let v = self.local.get(key);

                if let Ok(value) = v {
                    return Some(value.to_utf8().unwrap().as_bytes());
                }
            }
        };

        None
    }

    pub fn insert(&mut self, key: K, value: V) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.cache.insert(key.clone(), value.clone());
        self.local.put(key, value)
    }

    pub fn remove(&mut self, key: &K)
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.cache.remove(key);
        self.local.remove(key);
    }

    pub fn contains_key<Q: ?Sized>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        self.cache.contains_key(key)
    }

    // pub fn iter(&self) -> LruCache::Iter {

    //     self.cache.iter()

    // }

    // pub fn iter_mute(&mut self) -> LruCache::IterMut {

    //     self.cache.iter_mute()

    // }
}
