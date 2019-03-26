use std::sync::atomic::{AtomicBool, Ordering};

use error::Result;

#[cfg(feature = "kv_store_sled")]
mod sled;

#[cfg(feature = "kv_store_rocksdb")]
mod rocksdb;

#[cfg(feature = "kv_store_none")]
use self::kv_store_none::KvStore;

#[cfg(feature = "kv_store_sled")]
use self::sled::KvStore;

#[cfg(feature = "kv_store_rocksdb")]
use self::rocksdb::KvStore;

lazy_static! {
    pub static ref KV_STORE: KvStore = KvStore::default();

    // avoid overwriting when rebuilding everything from kv
    static ref IS_REBUILDING_FROM_KV: AtomicBool = AtomicBool::new(false);
}

pub fn is_rebuilding_from_kv() -> bool {
    IS_REBUILDING_FROM_KV.load(Ordering::Acquire)
}

//---------------------------------------------------------------------------------------
// LoadFromKv trait
//---------------------------------------------------------------------------------------
pub trait LoadFromKv<K: ?Sized>: Sized {
    // can load data from kv store
    fn load_from_kv<T: ::std::borrow::Borrow<K>>(key: &T) -> Result<Self>;
    fn save_to_kv<T: ::std::borrow::Borrow<K>>(&self, key: &T) -> Result<()>;
    fn should_reclaim(&self) -> bool;
    fn set_should_reclaim(&self, should_reclaim: bool);
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

        pub fn update_joint(&self, _key: &str, _joint: &Joint) -> Result<()> {
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

        pub fn save_cache_async(&self, _data: CachedJoint) -> Result<()> {
            Ok(())
        }

        pub fn finish(&self) -> Result<()> {
            Ok(())
        }
    }
}

#[cfg(not(feature = "kv_store_none"))]
mod kv_store_common {
    extern crate crossbeam;

    use std::thread::JoinHandle;

    use self::crossbeam::crossbeam_channel::{unbounded, Receiver, Sender};
    use super::*;
    use cache::{CachedJoint, SDAG_CACHE};

    pub fn handle_kv_joint(joint: crate::joint::Joint) -> Result<()> {
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

    pub fn create_thread_pool(size: usize) -> (Sender<CachedJoint>, Vec<JoinHandle<()>>) {
        let (sender, receiver): (Sender<CachedJoint>, Receiver<CachedJoint>) = unbounded();
        let mut handlers = Vec::new();

        for i in 1..size + 1 {
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

        (sender, handlers)
    }
}

#[cfg(all(test, not(feature = "kv_store_none")))]
mod tests {
    use super::*;
    use cache::CachedJoint;
    use joint::{Joint, JointProperty};
    use serde_json;

    #[test]
    fn kv_store_joint_test() -> Result<()> {
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

    #[test]
    fn kv_store_save_empty_joint_test() -> Result<()> {
        use cache::CachedData;
        use std::sync::Arc;

        let key = Arc::new(::std::string::String::from(
            "MHBF65OZbRHOEVyicHo7DUfUjxt41ILtQ7f7QAwBPGc=",
        ));
        let joint: CachedJoint = CachedData::empty(key);

        assert!(joint.save_to_db().is_err());

        Ok(())
    }
}
