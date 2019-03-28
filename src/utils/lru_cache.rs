extern crate rocksdb;
use self::rocksdb::{DBVector, DB};
use error::Result;
use lru_cache::LruCache;
use std::borrow::Borrow;
use std::hash::Hash;

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
        K: Eq + Hash + AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.db.put(key.as_ref(), value.as_ref())?;
        Ok(())
    }

    fn remove<Q: ?Sized>(&mut self, key: &Q) -> Result<()>
    where
        Q: Hash + Eq + AsRef<[u8]>,
    {
        self.db.delete(key.as_ref())?;
        Ok(())
    }

    fn get<Q: ?Sized>(&self, key: &Q) -> Result<DBVector>
    where
        Q: Hash + Eq + AsRef<[u8]>,
    {
        let v = self
            .db
            .get(key.as_ref())?
            .ok_or_else(|| format_err!("read last mci from kv failed"))?;
        Ok(v)
    }
}



#[allow(dead_code)]
pub struct Cache<K, V>
where
    K: Eq + Hash + Clone,
    V: Eq + Clone,
{
    cache: LruCache<K, V>,
    local: KvStore,
}

#[allow(dead_code)]
impl<K, V> Cache<K, V>
where
    K: Eq + Hash + Clone,
    V: Eq + Clone + AsRef<[u8]>,
{
    pub fn new(size: usize, path: &str) -> Self {
        Cache {
            cache: LruCache::new(size),
            local: KvStore::new(path),
        }
    }

    pub fn with_capacity(&mut self, capacity: usize) {
        self.cache.set_capacity(capacity)
    }

    pub fn capacity(&self) -> usize {
        self.cache.capacity()
    }
    pub fn len(&self) -> usize {
        self.cache.len()
    }

    pub fn get_mut<Q: ?Sized>(&mut self, key: &Q) -> Option<Vec<u8>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + AsRef<[u8]>,
    {
        match self.cache.get_mut(key) {
            Some(v) => {
                let value = v.clone();
                return Some(value.as_ref().to_vec());
            }
            None => {
                let v = self.local.get(key);
                if let Ok(value) = v {
                    return Some(value.to_utf8().unwrap().as_bytes().to_vec());
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

    pub fn remove<Q: ?Sized>(&mut self, key: &Q) -> Result<()>
    where
        K: Borrow<Q> + AsRef<[u8]>,
        Q: Eq + Hash + AsRef<[u8]>,
    {
        self.cache.remove(key);
        self.local.remove(key)?;
        Ok(())
    }

    pub fn contains_key<Q: ?Sized>(&mut self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        self.cache.contains_key(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_capaticy() {
        let cache: Cache<u32, String> = Cache::new(100, "./aaa");
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.capacity(), 100);
    }

    #[test]
    fn test_insert() -> Result<()> {
        let mut cache: Cache<&str, String> = Cache::new(100, "./b");
        cache.insert("2", "a".to_string())?;
        cache.insert("3", "b".to_string())?;
        cache.insert("4", "c".to_string())?;
        let n = "a".as_bytes().to_vec();
        assert_eq!(cache.len(), 3);
        assert_eq!(cache.get_mut("2"), Some(n));
        assert_eq!(cache.get_mut("3"), Some("b".as_bytes().to_vec()));
        assert_eq!(cache.get_mut("4"), Some("c".as_bytes().to_vec()));
        Ok(())
    }

    #[test]
    fn test_remove() -> Result<()> {
        let mut cache: Cache<&str, String> = Cache::new(100, "./c");
        cache.insert("2", "a".to_string())?;
        cache.insert("3", "b".to_string())?;
        cache.insert("4", "c".to_string())?;
        cache.remove("2")?;
        assert_eq!(cache.len(), 2);
        assert!(cache.get_mut("2").is_none());
        cache.remove("3")?;
        assert_eq!(cache.len(), 1);
        assert!(cache.get_mut("3").is_none());
        assert_eq!(cache.get_mut("4"), Some("c".as_bytes().to_vec()));
        Ok(())
    }

    #[test]
    fn test_contains_key() -> Result<()> {
        let mut cache: Cache<&str, String> = Cache::new(100, "./d");
        cache.insert("2", "a".to_string())?;
        cache.insert("3", "b".to_string())?;
        cache.insert("4", "c".to_string())?;
        assert_eq!(cache.len(), 3);
        assert!(cache.contains_key("2"));
        cache.remove("3")?;
        assert!(!cache.contains_key("3"));
        assert_eq!(cache.get_mut("4"), Some("c".as_bytes().to_vec()));
        Ok(())
    }
}
