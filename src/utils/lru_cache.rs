use lru_cache::LruCache;
use std::hash::Hash;

pub trait LocalStore<K: Eq + Hash, V: Eq> {
    fn insert(&mut self, key: K, value: V) -> Option<V>;

    fn remove(&mut self, key: &K);

    fn get(&self, key: &K) -> Option<&V>;

    fn contains_key(&self);
}

pub struct KvStore {}

impl<K: Eq + Hash, V: Eq> LocalStore<K, V> for KvStore {
    fn insert(&mut self, _key: K, _value: V) -> Option<V> {
        unimplemented!()
    }

    fn remove(&mut self, _key: &K) {
        unimplemented!()
    }

    fn get(&self, _key: &K) -> Option<&V> {
        unimplemented!()
    }

    fn contains_key(&self) {
        unimplemented!()
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

    pub fn get_mut(&mut self, key: &K) -> Option<&V> {
        match self.cache.get_mut(key) {
            Some(v) => Some(&*v),
            None => self.local.get(key),
        }
    }

    pub fn insert(&mut self, key: K, value: V) {
        self.cache.insert(key.clone(), value.clone());
        self.local.insert(key, value);
    }

    pub fn remove(&mut self, _key: &K) {
        // self.cache.remove(key);
        // self.local.remove(key);
        unimplemented!()
    }
}
