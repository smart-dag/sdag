use std::collections::HashSet;
use std::sync::{Arc, RwLock};

use cache::JointData;
use hashbrown::HashMap;
use rcu_cell::RcuReader;
use sdag_object_base::object_hash;
use spec::{Payload, Unit};

//---------------------------------------------------------------------------------------
// NotifyEvent
//---------------------------------------------------------------------------------------
pub struct NotifyEvent {
    pub joint: RcuReader<JointData>,
}

impl_event!(NotifyEvent);

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
    fn insert(&self, self_address: &str, watch_addresses: &[String]) {
        if self_address.is_empty() || watch_addresses.is_empty() {
            return;
        }
        if !object_hash::is_chash_valid(self_address) {
            return;
        }

        for watch in watch_addresses.iter() {
            if !object_hash::is_chash_valid(watch) {
                continue;
            }

            let mut new_watch = HashSet::new();
            let mut w_g = self.watchers.write().unwrap();
            match w_g.get_mut(watch) {
                Some(v) => v.insert(self_address.to_string()),
                None => new_watch.insert(self_address.to_string()),
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

pub fn watcher_insert(self_address: &str, watch_addresses: &[String]) {
    WATCHERS.insert(self_address, watch_addresses);
}

/// network interface struct
/// include all messages, except changes
#[derive(Default, Serialize, Deserialize, Clone)]
struct NotifyMessage {
    from: String,
    to_msg: Vec<(String, u64)>, // 0 is address, 1 is amount
    text: String,
    time: u64,
    unit: String,
}

/// notify messages to watchers
/// - 1) authors: send all messages, just send message to watchers which watch first author;
/// - 2) output: send output[index], text;
pub fn notify_watchers(joint: RcuReader<JointData>) {
    let unit = &joint.unit;
    let first_author = &unit.authors[0].address;
    let output_addresses = get_output_addresses(unit);

    let mut is_watched = false;
    if WATCHERS.get(first_author).is_none() {
        for addr in &output_addresses {
            if WATCHERS.get(addr).is_some() {
                is_watched = true;
                break;
            }
        }
        if !is_watched {
            return;
        }
    }

    let notify_message = get_notify_message(unit);

    if let Some(dst_address) = WATCHERS.get(first_author) {
        let msg_value = serde_json::to_value(notify_message.clone()).unwrap();
        for addr in &dst_address {
            if let Ok(false) =
                ::network::hub::WSS.notify_watcher(Arc::new(addr.to_string()), msg_value.clone())
            {
                WATCHERS.remove(first_author, addr);
            }
        }
    }

    // watch output address
    for addr in &output_addresses {
        if let Some(dst_addr) = WATCHERS.get(addr) {
            let mut new_msg = notify_message.clone();
            for i in 0..new_msg.to_msg.len() {
                if &new_msg.to_msg[i].0 != addr {
                    new_msg.to_msg.remove(i);
                }
            }
            let new_msg_value = serde_json::to_value(new_msg).unwrap();

            for dst in dst_addr {
                if let Ok(false) = ::network::hub::WSS
                    .notify_watcher(Arc::new(dst.to_string()), new_msg_value.clone())
                {
                    WATCHERS.remove(first_author, addr);
                }
            }
        }
    }
}

fn get_notify_message(unit: &Unit) -> NotifyMessage {
    let first_author = &unit.authors[0].address;
    let mut notify_message = NotifyMessage {
        from: first_author.to_string(),
        to_msg: Vec::new(),
        text: String::new(),
        time: unit.timestamp.unwrap_or_else(::time::now),
        unit: unit.unit.to_string(),
    };

    for msg in &unit.messages {
        match msg.payload {
            Some(Payload::Payment(ref payment)) => {
                for output in &payment.outputs {
                    // except changes
                    if &output.address == first_author {
                        continue;
                    }
                    notify_message
                        .to_msg
                        .push((output.address.clone(), output.amount));
                }
            }
            Some(Payload::Text(ref txt)) => {
                notify_message.text = txt.to_string();
            }
            _ => {
                warn!("not support payload type");
            }
        }
    }
    notify_message
}

/// get distinct output addresses, except changes
fn get_output_addresses(unit: &Unit) -> HashSet<String> {
    let mut output_addresses = HashSet::new();
    for msg in &unit.messages {
        if let Some(Payload::Payment(ref payment)) = msg.payload {
            for output in &payment.outputs {
                if output.address == unit.authors[0].address {
                    continue;
                }
                output_addresses.insert(output.address.clone());
            }
        }
    }

    output_addresses
}
