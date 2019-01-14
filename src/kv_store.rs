use error::Result;
use joint::{Joint, JointProperty};
use serde_json;
use sled::{Db, Tree};
use std::sync::Arc;

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

//---------------------------------------------------------------------------------------
// KvStore
//---------------------------------------------------------------------------------------
pub struct KvStore {
    joints: Arc<Tree>,
}

impl Default for KvStore {
    fn default() -> Self {
        let db = Db::start_default("./sdag_kv").expect("Failed to init KvStore");
        let joints = db
            .open_tree(b"joints".to_vec())
            .expect("Failed to init joints KvStore");
        KvStore { joints }
    }
}

impl KvStore {
    pub fn is_joint_exist(&self, _key: &str) -> Result<bool> {
        Ok(false)
    }

    pub fn read_joint(&self, key: &str) -> Result<Joint> {
        if let Some(value) = self.joints.get(key)? {
            return Ok(serde_json::from_str(::std::str::from_utf8(&value)?)?);
        }

        bail!("joint {} not exist in KV", key)
    }

    pub fn read_joint_children(&self, _key: &str) -> Result<Vec<String>> {
        unimplemented!("read_joint_children")
    }

    pub fn read_joint_property(&self, _key: &str) -> Result<JointProperty> {
        unimplemented!("read_joint_property")
    }

    // TODO: save a joint
    pub fn save_joint(&self, key: &str, joint: &Joint) -> Result<()> {
        self.joints
            .set(key, serde_json::to_string(joint)?.into_bytes())?;
        self.joints.flush()?;

        Ok(())
    }

    pub fn save_joint_children(&self, _key: &str, _children: Vec<String>) -> Result<()> {
        unimplemented!("save_joint_children")
    }

    pub fn save_joint_property(&self, _key: &str, _property: &JointProperty) -> Result<()> {
        unimplemented!("save_joint_property")
    }
}

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
