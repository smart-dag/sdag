use cache::JointData;
use error::Result;
use joint::{Joint, JointProperty};

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
#[derive(Default)]
pub struct KvStore {}

impl KvStore {
    pub fn is_joint_exist(&self, _key: &str) -> Result<bool> {
        Ok(false)
    }

    pub fn read_joint(&self, key: &str) -> Result<Joint> {
        bail!("read_joint from kv not supported, key={}", key)
    }

    pub fn read_joint_children(&self, _key: &str) -> Result<Vec<String>> {
        unimplemented!("read_joint_children")
    }

    pub fn read_joint_property(&self, _key: &str) -> Result<JointProperty> {
        unimplemented!("read_joint_property")
    }

    // TODO: save a joint
    pub fn save_joint(&self, _joint: &JointData) -> Result<()> {
        Ok(())
    }
}
