use std::collections::HashSet;
use std::sync::RwLock;

use hashbrown::HashMap;
use sdag_object_base::object_hash;

lazy_static! {
    // stored watchers info
    static ref WATCHERS: Watcher = Watcher::default();
}

#[derive(Default)]
struct Watcher {
    // key is watched address, value is notify peer address
    watchers: RwLock<HashMap<String, HashSet<String>>>,
}

impl Watcher {
    fn insert(&self, watch_info: &WatchInfo) {
        if watch_info.self_address.is_empty() || watch_info.watch_address.is_empty() {
            return;
        }
        for watch in watch_info.watch_address.iter() {
            if !object_hash::is_chash_valid(watch) {
                continue;
            }

            let mut new_watch = HashSet::new();
            let mut w_g = self.watchers.write().unwrap();
            match w_g.get_mut(watch) {
                Some(v) => v.insert(watch_info.self_address.to_string()),
                None => new_watch.insert(watch_info.self_address.to_string()),
            };

            if !new_watch.is_empty() {
                w_g.insert(watch.to_string(), new_watch);
            }
        }
    }

    /// if notify address is empty, drop the record from hashmap
    fn remove(&self, self_address: &str, watch_address: &str) {
        let mut is_remove = false;
        let mut w_g = self.watchers.write().unwrap();
        if let Some(v) = w_g.get_mut(watch_address) {
            if v.remove(self_address) && v.is_empty() {
                is_remove = true;
            }
        }
        if is_remove {
            w_g.remove(watch_address);
        }
    }

    /// return notify peer addresses
    fn get(&self, watch_address: &str) -> Option<HashSet<String>> {
        let r_g = self.watchers.read().unwrap();
        match r_g.get(watch_address) {
            Some(v) => Some(v.to_owned()),
            None => None,
        }
    }
}

pub fn watcher_insert(watch_info: &WatchInfo) {
    WATCHERS.insert(watch_info);
}

pub fn watcher_remove(self_address: &str, watch_address: &str) {
    WATCHERS.remove(self_address, watch_address);
}

pub fn get_watchers(watch_address: &str) -> Option<HashSet<String>> {
    WATCHERS.get(watch_address)
}

#[derive(Serialize, Deserialize)]
pub struct WatchInfo {
    pub self_address: String,
    pub watch_address: Vec<String>,
}
