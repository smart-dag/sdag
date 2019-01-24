use error::Result;

#[cfg(feature = "kv_store_none")]
use self::kv_store_none::KvStore;

#[cfg(feature = "kv_store_sled")]
use self::kv_store_sled::KvStore;

lazy_static! {
    pub static ref KV_STORE: KvStore = KvStore::default();
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
    use error::Result;
    use joint::{Joint, JointProperty};

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
    }
}

#[cfg(feature = "kv_store_sled")]
mod kv_store_sled {
    extern crate sled;

    use self::sled::{Db, Tree};
    use error::Result;
    use failure::ResultExt;
    use joint::{Joint, JointProperty};
    use serde_json;
    use std::sync::Arc;

    pub struct KvStore {
        pub joints: Arc<Tree>,
        pub properties: Arc<Tree>,
        pub children: Arc<Tree>,
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
            Ok(KvStore {
                joints,
                properties,
                children,
            })
        }

        pub fn is_joint_exist(&self, _key: &str) -> Result<bool> {
            Ok(false)
        }

        pub fn read_joint(&self, key: &str) -> Result<Joint> {
            if let Some(value) = self.joints.get(key)? {
                return Ok(serde_json::from_str(::std::str::from_utf8(&value)?)?);
            }

            bail!("joint {} not exist in KV", key)
        }

        pub fn read_joint_children(&self, key: &str) -> Result<Vec<String>> {
            if let Some(v) = self.children.get(key)? {
                return Ok(serde_json::from_str(::std::str::from_utf8(&v)?)?);
            }

            bail!("joint property {} not exist in KV", key)
        }

        pub fn read_joint_property(&self, key: &str) -> Result<JointProperty> {
            if let Some(value) = self.properties.get(key)? {
                return Ok(serde_json::from_str(::std::str::from_utf8(&value)?)?);
            }

            bail!("joint property {} not exist in KV", key)
        }

        // TODO: save a joint
        pub fn save_joint(&self, key: &str, joint: &Joint) -> Result<()> {
            self.joints
                .set(key, serde_json::to_string(joint)?.into_bytes())?;
            self.joints.flush()?;

            Ok(())
        }

        pub fn save_joint_children(&self, key: &str, children: Vec<String>) -> Result<()> {
            self.children
                .set(key, serde_json::to_string(&children)?.into_bytes())?;
            self.children.flush()?;
            Ok(())
        }

        pub fn save_joint_property(&self, key: &str, property: &JointProperty) -> Result<()> {
            self.properties
                .set(key, serde_json::to_string(property)?.into_bytes())?;
            self.properties.flush()?;

            Ok(())
        }
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
}
