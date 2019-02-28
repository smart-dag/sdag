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

        pub fn save_cache_async(&self, _data: CachedJoint) -> Result<()> {
            Ok(())
        }

        pub fn finish(&self) -> Result<()> {
            Ok(())
        }
    }
}

#[cfg(all(test, not(feature = "kv_store_none")))]
mod tests {
    use super::*;
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
}
