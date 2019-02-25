use std::sync::atomic::{AtomicBool, Ordering};

use error::Result;

#[cfg(feature = "kv_store_none")]
use self::kv_store_none::KvStore;

#[cfg(feature = "kv_store_sled")]
use self::kv_store_sled::KvStore;

lazy_static! {
    pub static ref KV_STORE: KvStore = KvStore::default();

    // avoid overwriting when rebuilding everything from kv
    static ref IS_REBUILDING_FROM_KV: AtomicBool = AtomicBool::new(true);
}

pub fn is_rebuilding_from_kv() -> bool {
    IS_REBUILDING_FROM_KV.load(Ordering::Relaxed)
}

//---------------------------------------------------------------------------------------
// LoadFromKv trait
//---------------------------------------------------------------------------------------
pub trait LoadFromKv<K: ?Sized>: Sized {
    // can load data from kv store
    fn load_from_kv<T: ::std::borrow::Borrow<K>>(key: &T) -> Result<Self>;
    fn save_to_kv<T: ::std::borrow::Borrow<K>>(&self, key: &T) -> Result<()>;
}

#[cfg(feature = "kv_store_none")]
mod kv_store_none {
    use cache::CachedJoint;
    use error::Result;
    use joint::{Joint, JointProperty, Level};

    pub struct KvStore {}

    impl Default for KvStore {
        fn default() -> Self {
            KvStore::load("./sdag_kv").expect("init KvStore failed")
        }
    }

    impl KvStore {
        pub fn load(_path: &str) -> Result<Self> {
            Ok(KvStore {})
        }

        pub fn is_joint_exist(&self, _key: &str) -> Result<bool> {
            Ok(false)
        }

        pub fn read_joint(&self, key: &str) -> Result<Joint> {
            bail!("joint {} not exist in KV", key)
        }

        pub fn read_joint_children(&self, key: &str) -> Result<Vec<String>> {
            bail!("joint children {} not exist in KV", key)
        }

        pub fn read_joint_property(&self, key: &str) -> Result<JointProperty> {
            bail!("joint property {} not exist in KV", key)
        }

        pub fn save_joint(&self, _key: &str, _joint: &Joint) -> Result<()> {
            Ok(())
        }

        pub fn save_joint_children(&self, _key: &str, _children: Vec<String>) -> Result<()> {
            Ok(())
        }

        pub fn save_joint_property(&self, _key: &str, _property: &JointProperty) -> Result<()> {
            Ok(())
        }

        pub fn rebuild_from_kv(&self) -> Result<()> {
            Ok(())
        }

        pub fn save_unstable_joints(&self) -> Result<()> {
            Ok(())
        }

        pub fn save_last_mci(&self, _mci: Level) -> Result<()> {
            Ok(())
        }

        pub fn delete_joint(&self, key: &str) -> Result<()> {
            bail!("joint {} not exist in KV", key)
        }

        pub fn delete_joint_property(&self, key: &str) -> Result<()> {
            bail!("joint {} not exist in KV", key)
        }

        pub fn save_cache_async(&self, data: CachedJoint) -> Result<()> {
            Ok(())
        }

        pub fn finish(self) -> Result<()> {
            Ok(())
        }
    }
}

#[cfg(feature = "kv_store_sled")]
mod kv_store_sled {
    extern crate sled;
    use self::sled::{Db, Tree};

    use super::*;
    use cache::{CachedJoint, SDAG_CACHE};
    use crossbeam::crossbeam_channel::{unbounded, Receiver, Sender};
    use error::Result;
    use failure::ResultExt;
    use joint::{Joint, JointProperty, Level};
    use serde_json;
    use std::sync::Arc;
    use std::thread::JoinHandle;

    pub struct KvStore {
        pub joints: Arc<Tree>,
        pub properties: Arc<Tree>,
        pub children: Arc<Tree>,
        pub misc: Arc<Tree>,
        sender: Sender<CachedJoint>,
        _handlers: Vec<JoinHandle<()>>,
    }

    impl Default for KvStore {
        fn default() -> Self {
            KvStore::load("./sdag_kv").expect("init KvStore failed")
        }
    }

    impl KvStore {
        pub fn load(path: &str) -> Result<Self> {
            let db = Db::start_default(path).context("Failed to read file for KvStore")?;
            let joints = db
                .open_tree(b"joints".to_vec())
                .context("Failed to init joints KvStore")?;
            let properties = db
                .open_tree(b"properties".to_vec())
                .context("Failed to init properties KvStore")?;
            let children = db
                .open_tree(b"children".to_vec())
                .context("Failed to init children KvStore")?;
            let misc = db
                .open_tree(b"misc".to_vec())
                .context("Failed to init misc KvStore")?;

            let (sender, receiver): (Sender<CachedJoint>, Receiver<CachedJoint>) = unbounded();
            let mut handlers = Vec::new();

            for i in 1..9 {
                let rx = receiver.clone();
                handlers.push(std::thread::spawn(move || {
                    while let Ok(cached_joint) = rx.recv() {
                        info!(
                            "Thread{}: Saving cached joint with key {}",
                            i, cached_joint.key
                        );
                        t_c!(cached_joint.save_to_db());
                    }
                }));
            }

            Ok(KvStore {
                joints,
                properties,
                children,
                misc,
                sender,
                _handlers: handlers,
            })
        }

        pub fn is_joint_exist(&self, _key: &str) -> Result<bool> {
            Ok(false)
        }

        pub fn read_joint(&self, key: &str) -> Result<Joint> {
            if let Some(value) = self.joints.get(key)? {
                return Ok(serde_json::from_slice(&value)?);
            }

            bail!("joint {} not exist in KV", key)
        }

        pub fn read_joint_children(&self, key: &str) -> Result<Vec<String>> {
            if let Some(value) = self.children.get(key)? {
                return Ok(serde_json::from_slice(&value)?);
            }

            bail!("joint property {} not exist in KV", key)
        }

        pub fn read_joint_property(&self, key: &str) -> Result<JointProperty> {
            if let Some(value) = self.properties.get(key)? {
                return Ok(serde_json::from_slice(&value)?);
            }

            bail!("joint property {} not exist in KV", key)
        }

        pub fn save_joint(&self, key: &str, joint: &Joint) -> Result<()> {
            self.joints.set(key, serde_json::to_vec(joint)?)?;
            Ok(())
        }

        pub fn save_joint_children(&self, key: &str, children: Vec<String>) -> Result<()> {
            self.children.set(key, serde_json::to_vec(&children)?)?;
            Ok(())
        }

        pub fn save_joint_property(&self, key: &str, property: &JointProperty) -> Result<()> {
            self.properties.set(key, serde_json::to_vec(property)?)?;
            Ok(())
        }

        pub fn rebuild_from_kv(&self) -> Result<()> {
            use main_chain::{self, MciStableEvent};
            use may::sync::Semphore;
            use std::time::Duration;
            use utils::event::Event;

            info!("Rebuild from KV start!");

            let last_mci = self.read_last_mci().unwrap_or(Level::INVALID);

            let sem = Arc::new(Semphore::new(0));
            if last_mci.is_valid() {
                let post_sem = sem.clone();
                MciStableEvent::add_handler(move |v| {
                    if v.mci == last_mci {
                        post_sem.post();
                    }
                });
            }

            for item in self.joints.iter() {
                let (_, value) = item.unwrap();
                let joint: Joint = serde_json::from_slice(&value)?;
                handle_kv_joint(joint)?
            }

            if last_mci.is_valid() {
                while !sem.wait_timeout(Duration::from_secs(1)) {
                    info!("current mci={:?}", main_chain::get_last_stable_mci());
                }
            }

            info!("Rebuild from KV done!");
            IS_REBUILDING_FROM_KV.store(false, Ordering::Relaxed);

            Ok(())
        }

        pub fn save_unstable_joints(&self) -> Result<()> {
            let joints = SDAG_CACHE.get_unstable_joints()?;

            for joint in joints {
                joint.save_to_db()?;
            }

            // FIXME: now rebuild everything, will redesign the restore process later
            self.save_last_mci(::main_chain::get_last_stable_mci())?;

            Ok(())
        }

        pub fn delete_joint(&self, key: &str) -> Result<()> {
            self.joints.del(key)?;
            Ok(())
        }

        pub fn delete_joint_property(&self, key: &str) -> Result<()> {
            self.properties.del(key)?;
            Ok(())
        }

        pub fn save_last_mci(&self, mci: Level) -> Result<()> {
            self.misc.set(b"last_mci", serde_json::to_vec(&mci)?)?;
            Ok(())
        }

        fn read_last_mci(&self) -> Result<Level> {
            let v = self
                .misc
                .get(b"last_mci")?
                .ok_or_else(|| format_err!("read last mci from kv failed"))?;

            Ok(serde_json::from_slice(&v)?)
        }

        pub fn save_cache_async(&self, data: CachedJoint) -> Result<()> {
            self.sender.send(data)?;
            Ok(())
        }

        pub fn finish(&self) -> Result<()> {
            self.joints.flush()?;
            self.children.flush()?;
            self.properties.flush()?;
            self.misc.flush()?;

            info!("kv store finished");

            Ok(())
        }
    }

    fn handle_kv_joint(joint: Joint) -> Result<()> {
        use joint::JointSequence;
        use validation;

        try_go!(move || {
            // check content_hash or unit_hash first!
            validation::validate_unit_hash(&joint.unit)?;
            let cached_joint = match SDAG_CACHE.add_new_joint(joint, None) {
                Ok(j) => j,
                Err(e) => {
                    bail!("add_new_joint: err = {}", e);
                }
            };

            let joint_data = cached_joint.read().unwrap();
            if let Some(ref hash) = joint_data.unit.content_hash {
                error!("unit {} content hash = {}", cached_joint.key, hash);
                joint_data.set_sequence(JointSequence::FinalBad);
            }

            if joint_data.is_ready() {
                validation::validate_ready_joint(cached_joint)?;
            }

            Ok(())
        });

        Ok(())
    }

    #[test]
    fn kv_store_joint_test() -> Result<()> {
        use super::*;

        let joint = r#"{
        "unit":{
            "alt":"1",
            "authors":[
                {
                    "address":"LWFAESN3EB5E5VFXJ7JWIJB7K5MDQCZE",
                    "authentifiers":{
                        "r":"l412FzG4ZMESwMASqNdNfXhj2XvSGhOblud5DuKhbc8mnNJFFxpTLUU0s3SuDL8ONLQ1OaWQHN7lTx8B53Ofqw=="
                    }
                }
            ],
            "headers_commission":344,
            "last_ball":"n/7WqfyUwX14nS/+Iw2O4LvivSqwVecPkSpl8qbUvJM=",
            "last_ball_unit":"Gz0nOu5Utp3WtCZwlfG5+TbqRMGvF8fDsAVWh9BJc7Q=",
            "messages":[
                {
                    "app":"payment",
                    "payload":{
                        "inputs":[
                            {
                                "message_index":1,
                                "output_index":41,
                                "unit":"Gz0nOu5Utp3WtCZwlfG5+TbqRMGvF8fDsAVWh9BJc7Q="
                            }
                        ],
                        "outputs":[
                            {
                                "address":"LWFAESN3EB5E5VFXJ7JWIJB7K5MDQCZE",
                                "amount":999499
                            }
                        ]
                    },
                    "payload_hash":"LRsKHh5DMb30BLrPBlY81vLdFcIr0JboraHoN15pjfM=",
                    "payload_location":"inline"
                }
            ],
            "parent_units":[
                "BQFT9TpXhXbxd0b+rqGeBvehuJjnrV+NjA7Alp4IkHM="
            ],
            "payload_commission":157,
            "timestamp":1547396486,
            "unit":"MHBF65OZbRHOEVyicHo7DUfUjxt41ILtQ7f7QAwBPGc=",
            "version":"1.0",
            "witness_list_unit":"Gz0nOu5Utp3WtCZwlfG5+TbqRMGvF8fDsAVWh9BJc7Q="
        }
    }"#;
        let joint: Joint = serde_json::from_str(&joint)?;

        KV_STORE.save_joint(&joint.unit.unit, &joint)?;
        let read_joint = KV_STORE.read_joint(&joint.unit.unit)?;

        assert_eq!(
            serde_json::to_string(&joint)?,
            serde_json::to_string(&read_joint)?
        );

        Ok(())
    }

    #[test]
    fn kv_store_property_test() -> Result<()> {
        use super::*;

        let key = "MHBF65OZbRHOEVyicHo7DUfUjxt41ILtQ7f7QAwBPGc=";
        let property: JointProperty = JointProperty::default();

        KV_STORE.save_joint_property(&key, &property)?;
        let read_property = KV_STORE.read_joint_property(&key)?;

        assert_eq!(
            serde_json::to_string(&property)?,
            serde_json::to_string(&read_property)?
        );

        Ok(())
    }

    #[test]
    fn kv_store_children_test() -> Result<()> {
        use super::*;

        let key = "MHBF65OZbRHOEVyicHo7DUfUjxt41ILtQ7f7QAwBPGc=";
        let mut children = vec![];
        for i in 0..20 {
            children.push(i.to_string());
        }

        KV_STORE.save_joint_children(&key, children.clone())?;
        let read_children = KV_STORE.read_joint_children(&key)?;

        assert_eq!(children, read_children);

        Ok(())
    }

    #[test]
    fn kv_store_delete_test() -> Result<()> {
        use super::*;

        let key = "MHBF65OZbRHOEVyicHo7DUfUjxt41ILtQ7f7QAwBPGc=";
        let property: JointProperty = JointProperty::default();

        KV_STORE.save_joint_property(&key, &property)?;
        let read_property = KV_STORE.read_joint_property(&key)?;

        assert_eq!(
            serde_json::to_string(&property)?,
            serde_json::to_string(&read_property)?
        );

        KV_STORE.delete_joint_property(&key)?;

        assert_eq!(KV_STORE.read_joint_property(&key).is_err(), true);

        Ok(())
    }
}
