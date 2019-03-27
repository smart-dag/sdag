extern crate crossbeam;
extern crate rocksdb;

use self::crossbeam::crossbeam_channel::Sender;
use self::rocksdb::{ColumnFamilyDescriptor, IteratorMode, Options, DB};

use super::*;
use cache::{CachedJoint, SDAG_CACHE};
use error::Result;
use failure::ResultExt;
use joint::{Joint, JointProperty, Level};
use serde_json;
use std::thread::JoinHandle;

pub struct KvStore {
    pub joints: DB,
    pub properties: DB,
    pub children: DB,
    pub misc: DB,
    sender: Sender<(CachedJoint, bool)>,
    _handlers: Vec<JoinHandle<()>>,
}

impl Default for KvStore {
    fn default() -> Self {
        KvStore::load("./sdag_kv").expect("init KvStore failed")
    }
}

impl KvStore {
    pub fn load(path: &str) -> Result<Self> {
        // Some column family for ball and skiplist, now skiplist is not used
        // Both updated ball and skiplist are saved under ball cf
        let ball = ColumnFamilyDescriptor::new("ball", Options::default());
        let skiplist = ColumnFamilyDescriptor::new("skiplist", Options::default());

        let mut joint_opts = Options::default();
        joint_opts.create_missing_column_families(true);
        joint_opts.create_if_missing(true);
        let joints = DB::open_cf_descriptors(
            &joint_opts,
            format!("{}/joints", path),
            vec![ball, skiplist],
        )
        .context("Failed to init joints KvStore")?;

        let properties = DB::open_default(format!("{}/properties", path))
            .context("Failed to init properties KvStore")?;
        let children = DB::open_default(format!("{}/children", path))
            .context("Failed to init children KvStore")?;
        let misc =
            DB::open_default(format!("{}/misc", path)).context("Failed to init misc KvStore")?;

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
        if let Some(value) = self.joints.get(key.as_bytes())? {
            let mut joint: Joint = serde_json::from_slice(&value)?;

            if let Some(ball_cf) = self.joints.cf_handle("ball") {
                if let Some(value) = self.joints.get_cf(ball_cf, key.as_bytes())? {
                    let (ball, skiplist) = serde_json::from_slice(&value)?;
                    joint.ball = ball;
                    joint.skiplist_units = skiplist;
                }
            }

            return Ok(joint);
        }

        bail!("joint {} not exist in KV", key)
    }

    pub fn read_joint_children(&self, key: &str) -> Result<Vec<String>> {
        if let Some(value) = self.children.get(key.as_bytes())? {
            return Ok(serde_json::from_slice(&value)?);
        }

        bail!("joint children {} not exist in KV", key)
    }

    pub fn read_joint_property(&self, key: &str) -> Result<JointProperty> {
        if let Some(value) = self.properties.get(key.as_bytes())? {
            return Ok(serde_json::from_slice(&value)?);
        }

        bail!("joint property {} not exist in KV", key)
    }

    pub fn save_joint(&self, key: &str, joint: &Joint) -> Result<()> {
        self.joints
            .put(key.as_bytes(), &serde_json::to_vec(joint)?)?;
        Ok(())
    }

    pub fn update_joint(&self, key: &str, joint: &Joint) -> Result<()> {
        if let Some(ball_cf) = self.joints.cf_handle("ball") {
            self.joints.put_cf(
                ball_cf,
                key.as_bytes(),
                &serde_json::to_vec(&(&joint.ball, &joint.skiplist_units))?,
            )?;
        }

        Ok(())
    }

    pub fn save_joint_children(&self, key: &str, children: Vec<String>) -> Result<()> {
        self.children
            .put(key.as_bytes(), &serde_json::to_vec(&children)?)?;
        Ok(())
    }

    pub fn save_joint_property(&self, key: &str, property: &JointProperty) -> Result<()> {
        self.properties
            .put(key.as_bytes(), &serde_json::to_vec(property)?)?;
        Ok(())
    }

    pub fn rebuild_from_kv(&self) -> Result<()> {
        info!("Rebuild from KV start!");
        IS_REBUILDING_FROM_KV.store(true, Ordering::Release);

        let mut handle_joint_count = 0;
        for (_key, value) in self.joints.iterator(IteratorMode::Start) {
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
        self.joints.delete(key.as_bytes())?;
        Ok(())
    }

    pub fn delete_joint_property(&self, key: &str) -> Result<()> {
        self.properties.delete(key.as_bytes())?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn save_last_mci(&self, mci: Level) -> Result<()> {
        self.misc.put(b"last_mci", &serde_json::to_vec(&mci)?)?;
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
        self.sender.send((data, false))?;
        Ok(())
    }

    pub fn update_cache_async(&self, data: CachedJoint) -> Result<()> {
        self.sender.send((data, true))?;
        Ok(())
    }

    pub fn finish(&self) -> Result<()> {
        info!("kv store finished");

        Ok(())
    }
}
