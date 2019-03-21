extern crate crossbeam;
extern crate sled;

use self::crossbeam::crossbeam_channel::Sender;
use self::sled::{Db, Tree};

use super::*;
use cache::{CachedJoint, SDAG_CACHE};
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

        let (sender, handlers) = kv_store_common::create_thread_pool(8);

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
        info!("Rebuild from KV start!");
        IS_REBUILDING_FROM_KV.store(true, Ordering::Release);

        let mut handle_joint_count = 0;
        for item in self.joints.iter() {
            let (_, value) = item.unwrap();
            let joint: Joint = serde_json::from_slice(&value)?;
            kv_store_common::handle_kv_joint(joint)?;
            handle_joint_count += 1;
        }
        ::utils::wait_cond(None, || {
            handle_joint_count == SDAG_CACHE.get_num_of_normal_joints()
        })?;

        info!("Rebuild from KV done!");
        IS_REBUILDING_FROM_KV.store(false, Ordering::Release);

        Ok(())
    }

    #[allow(dead_code)]
    pub fn save_unstable_joints(&self) -> Result<()> {
        let joints = SDAG_CACHE.get_unstable_joints()?;

        for joint in joints {
            joint.save_to_db()?;
        }

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

    #[allow(dead_code)]
    pub fn save_last_mci(&self, mci: Level) -> Result<()> {
        self.misc.set(b"last_mci", serde_json::to_vec(&mci)?)?;
        Ok(())
    }

    #[allow(dead_code)]
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
