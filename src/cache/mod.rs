mod cache_data;
mod cache_impl;
mod joint_data;

use std::collections::VecDeque;
use std::sync::Arc;

use config;
use error::Result;
use hashbrown::{HashMap, HashSet};
use joint::{Joint, Level};
use kv_store::{LoadFromKv, KV_STORE};
use may::sync::RwLock;
use serde_json::Value;
use smallvec::SmallVec;
use statistics;
use validation;

pub use self::{
    cache_data::{CachedData, HashKey},
    joint_data::{JointData, UnitProps},
};

lazy_static! {
    pub static ref SDAG_CACHE: SDagCache = SDagCache::default();
}

//---------------------------------------------------------------------------------------
// CachedJoint
//---------------------------------------------------------------------------------------
pub type CachedJoint = CachedData<String, JointData>;

//---------------------------------------------------------------------------------------
// SDagCache
//---------------------------------------------------------------------------------------
// this is the unit mem cache
#[derive(Default)]
pub struct SDagCache {
    // keep all joints here, the source of every thing
    joints: RwLock<cache_impl::SDagCacheInner>,
    // hash tree balls <ball, unit>
    hash_tree_balls: RwLock<HashMap<String, String>>,
    mc_units: RwLock<HashMap<Level, String>>,
    // ball cache
    ball_units: RwLock<HashMap<String, String>>,
    // definitions<address, (unit_hash, definition)>
    definitions: RwLock<HashMap<String, (String, Value)>>,
}

impl SDagCache {
    /// insert a valid joint into the cache
    /// the joint data can be from internet or load from kv store
    fn insert_joint(&self, hash_key: HashKey, data: JointData) -> CachedJoint {
        self.joints
            .write()
            .unwrap()
            .add_normal_joint(hash_key, data)
    }

    /// remove a joint from cache completely
    /// clear a joint need two steps:
    ///  1. free the data heap memory
    ///  2. remove the cache entry for it
    /// this function perform the second step
    #[allow(dead_code)]
    fn remove_joint(&self, joint: CachedJoint) {
        // before remove the joint make sure it's empty
        assert_eq!(joint.is_empty(), true);
        self.joints.write().unwrap().del_joint(&joint.key);
    }

    /// load a joint from kv store directly
    /// usually we don't need to call this directly
    #[allow(dead_code)]
    fn load_joint_from_kv(&self, key: &str) -> Result<CachedJoint> {
        let key = HashKey::new(key);
        let joint = JointData::load_from_kv(&key)?;
        Ok(self.insert_joint(key, joint))
    }

    /// get a joint form the hashmap, if not exist just insert one with none
    /// this is used when there are some (parents) refs that need to create
    fn get_joint_or_none(&self, key: &str) -> CachedJoint {
        let mut g = self.joints.write().unwrap();
        match g.get_joint(key) {
            None => g.add_empty_joint(key),
            Some(j) => j,
        }
    }

    /// get a joint from hashmap
    pub fn try_get_joint(&self, key: &str) -> Option<CachedJoint> {
        self.joints.read().unwrap().get_joint(key)
    }

    /// get a joint from the hashmap, if not exist try load from kv store
    pub fn get_joint(&self, key: &str) -> Result<CachedJoint> {
        let g = self.joints.read().unwrap();
        match g.get_joint(key) {
            None => {
                if g.is_known_unhandled_joint(key) {
                    bail!("unit={} is unhandled", key);
                }
                if g.is_known_bad_joint(key) {
                    bail!("unit={} is known bad", key);
                }

                // now we are rebuilding everything, joint read from kv can not be used directly
                // since the cache and business state are not rebuild accordingly
                bail!("unit={} does not exist", key);

                // loading joint from kv needs the write guard
                //drop(g);
                //self.load_joint_from_kv(key)
            }
            Some(j) => Ok(j),
        }
    }

    /// get all good free joints
    pub fn get_good_free_joints(&self) -> Result<Vec<CachedJoint>> {
        self.joints.read().unwrap().get_good_free_joints()
    }

    /// get all the free joints
    pub fn get_all_free_joints(&self) -> Result<Vec<CachedJoint>> {
        self.joints.read().unwrap().get_all_free_joints()
    }

    pub fn get_bad_joints(&self) -> Vec<String> {
        self.joints.read().unwrap().get_known_bad_joints()
    }

    pub fn get_num_of_bad_joints(&self) -> usize {
        self.joints.read().unwrap().get_num_of_known_bad_joints()
    }

    pub fn get_temp_bad_joints(&self) -> Vec<String> {
        self.get_all_free_joints()
            .map(|v| {
                v.into_iter()
                    .filter_map(|j| {
                        if let Ok(joint) = j.read() {
                            if joint.get_sequence().is_temp_bad() {
                                Some(joint.unit.unit.clone())
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_else(|_| Vec::new())
    }

    pub fn get_num_of_temp_bad_joints(&self) -> usize {
        self.get_temp_bad_joints().len()
    }

    pub fn get_num_of_unhandled_joints(&self) -> usize {
        self.joints.read().unwrap().get_num_of_unhandled_joints()
    }

    pub fn get_num_of_normal_joints(&self) -> usize {
        self.joints.read().unwrap().get_num_of_normal_joints()
    }

    /// get all unstable joints
    pub fn get_unstable_joints(&self) -> Result<Vec<CachedJoint>> {
        let mut queue = VecDeque::new();
        let mut visited = HashSet::new();
        let mut joints = Vec::new();

        for joint in self.get_all_free_joints()? {
            if visited.insert(joint.key.clone()) {
                queue.push_back(joint);
            }
        }

        while let Some(joint) = queue.pop_front() {
            let joint_data = joint.read()?;

            if joint_data.is_stable() {
                continue;
            }

            joints.push(joint);

            for parent in joint_data.parents.iter() {
                if visited.insert(parent.key.clone()) {
                    queue.push_back(parent.clone());
                }
            }
        }

        // reverse
        joints.reverse();

        Ok(joints)
    }

    /// check if the joint is new, only new joint will be handled
    pub fn check_new_joint(&self, joint: &str) -> Result<()> {
        let cache = self.joints.read().unwrap();
        if cache.get_joint(joint).is_some() {
            bail!("joint is already known in cache, unit = {}", joint);
        }

        if cache.is_known_unhandled_joint(joint) {
            bail!("joint is already known unhandled, unit = {}", joint);
        }

        if cache.is_known_bad_joint(joint) {
            bail!("joint is known bad, unit = {}", joint);
        }

        drop(cache);

        if KV_STORE.is_joint_exist(joint)? {
            bail!("joint is already known in kv, unit = {}", joint);
        }

        Ok(())
    }

    /// purge a bad joint
    pub fn purge_bad_joint(&self, key: Arc<String>, err: String) {
        let mut g = self.joints.write().unwrap();
        // then we need to purge all the child that depends on it
        g.purge_bad_joint(key, err);
    }

    /// add a new joint into the unhandled memory cache
    /// we use the returned cached joint for further validation
    pub fn add_new_joint(&self, joint: Joint, peer_id: Option<Arc<String>>) -> Result<CachedJoint> {
        // first check if joint is already known
        let key = HashKey::new(&joint.unit.unit);
        self.check_new_joint(&key)?;

        let joint_data = JointData::from_joint(joint, peer_id.clone());

        if let Err(e) = validation::basic_validate(&joint_data) {
            // need to record as known bad joint
            self.purge_bad_joint(key.0, e.to_string());
            let peer_id = peer_id.unwrap_or_else(|| Arc::new(String::from("unknown")));
            statistics::increase_stats(peer_id, true, false);
            bail!("base validation failed, err={}", e);
        }

        let mut valid_parents = SmallVec::<[CachedJoint; config::MAX_PARENT_PER_UNIT]>::new();
        let mut missing_parents = SmallVec::<[String; config::MAX_PARENT_PER_UNIT]>::new();

        let mut is_bad_parent = false;
        {
            let reader = self.joints.read().unwrap();
            for parent in &joint_data.unit.parent_units {
                // check if it's a known bad joint
                if reader.is_known_bad_joint(parent) {
                    is_bad_parent = true;
                    break;
                }
            }
        }

        if is_bad_parent {
            self.purge_bad_joint(key.0, String::from("bad parent"));
            bail!("joint parents contains known bad joint");
        }

        let mut g = self.joints.write().unwrap();
        for parent in &joint_data.unit.parent_units {
            // check if it's already ok
            match g.get_joint(parent) {
                None => {
                    if KV_STORE.is_joint_exist(parent)? {
                        let j = g.add_empty_joint(parent);
                        valid_parents.push(j);
                    } else {
                        missing_parents.push(parent.to_owned());
                    }
                }
                Some(j) => valid_parents.push(j),
            }
        }

        // at this stage we construct the unhandled joint in cache
        for valid_parent in valid_parents {
            valid_parent.read()?.inc_unhandled_refs();
            joint_data.add_parent(valid_parent);
        }

        let cached_joint = g.add_unhandled_joint(key, joint_data);

        // add the missing parent
        for missing_parent in missing_parents {
            g.add_missing_parent(missing_parent, cached_joint.clone());
        }

        Ok(cached_joint)
    }

    /// normalize a joint: move the joint from unhandled to normal
    pub fn normalize_joint(&self, joint: CachedJoint) {
        let mut g = self.joints.write().unwrap();
        g.transfer_joint_to_normal(joint.clone());
        g.update_parent_and_child(joint);
    }

    /// judge if a ball is exit in the hash tree ball
    /// a ball either exist in joint.ball or in hash tree ball
    pub fn is_ball_in_hash_tree(&self, ball: &str) -> bool {
        let g = self.hash_tree_balls.read().unwrap();
        g.get(ball).is_some()
    }

    /// return true if hash tree balls is empty
    pub fn get_hash_tree_ball_len(&self) -> usize {
        let g = self.hash_tree_balls.read().unwrap();
        g.len()
    }

    /// get unit from hash_tree_balls by ball
    pub fn get_hash_tree_unit(&self, ball: &str) -> Option<String> {
        self.hash_tree_balls.read().unwrap().get(ball).cloned()
    }

    /// get ball from hash_tree_balls by unit
    pub fn get_hash_tree_ball(&self, unit: &str) -> Option<String> {
        for (ball, saved_unit) in self.hash_tree_balls.read().unwrap().iter() {
            if unit == saved_unit {
                return Some(ball.clone());
            }
        }
        None
    }

    /// add a ball into hash tree balls
    pub fn add_hash_tree_ball(&self, ball: String, unit: String) {
        let mut g = self.hash_tree_balls.write().unwrap();
        g.insert(ball, unit);
    }

    /// remove the ball entry in hash tree balls when a joint got stable
    pub fn del_hash_tree_ball(&self, ball: &str) -> Option<String> {
        let mut g = self.hash_tree_balls.write().unwrap();
        g.remove(ball)
    }

    /// clear all the  hash tree balls
    pub fn clear_hash_tree_ball(&self) {
        let mut g = self.hash_tree_balls.write().unwrap();
        if !g.is_empty() {
            error!("clear hash tree");
            g.clear()
        }
    }

    /// get last ball mci of mci
    pub fn get_last_ball_mci_of_mci(&self, mci: Level) -> Result<Level> {
        let unit = self
            .get_mc_unit_hash(mci)?
            .ok_or_else(|| format_err!("mc unit not found, mci={:?}", mci))?;
        let joint = self.get_joint(&unit)?.read()?;
        let last_ball_unit = joint
            .unit
            .last_ball_unit
            .as_ref()
            .ok_or_else(|| format_err!("last ball unit is none"))?;
        let last_ball_joint = self.get_joint(&last_ball_unit)?.read()?;
        Ok(last_ball_joint.get_mci())
    }

    /// get all joints that have the same mci
    pub fn get_joints_by_mci(&self, mci: Level) -> Result<Vec<CachedJoint>> {
        let joint = match self.get_mc_unit_hash(mci)? {
            None => return Ok(Vec::new()),
            Some(unit) => SDAG_CACHE.get_joint(&unit)?,
        };

        let mut queue = VecDeque::new();
        let mut visited = HashSet::new();
        let mut joints = Vec::new();
        if visited.insert(joint.key.clone()) {
            queue.push_back(joint);
        }

        while let Some(joint) = queue.pop_front() {
            let joint_data = joint.read()?;

            if joint_data.get_mci() != mci {
                continue;
            }

            let sub_mci = joint_data.get_sub_mci();
            joints.push((sub_mci, joint));
            for parent in joint_data.parents.iter() {
                if visited.insert(parent.key.clone()) {
                    queue.push_back(parent.clone());
                }
            }
        }

        // order by sub_mci
        joints.sort_by_key(|v| v.0.value());

        Ok(joints.into_iter().map(|v| v.1).collect())
    }

    /// get all missing joints
    pub fn get_missing_joints(&self) -> Vec<String> {
        let g = self.joints.read().unwrap();
        g.get_all_missing_joints()
    }

    /// get main chain unit by mci
    pub fn get_mc_unit_hash(&self, mci: Level) -> Result<Option<String>> {
        // TODO: since this is a cache, read out from database if necessary
        Ok(self.mc_units.read().unwrap().get(&mci).cloned())
    }

    /// set main chain unit by mci
    pub fn set_mc_unit_hash(&self, mci: Level, unit: String) -> Result<()> {
        // TODO: update the database
        self.mc_units.write().unwrap().insert(mci, unit);
        Ok(())
    }

    /// get unit hash by ball
    pub fn get_ball_unit_hash(&self, ball: &str) -> Result<Option<String>> {
        Ok(self.ball_units.read().unwrap().get(ball).cloned())
    }

    /// set unit hash by ball
    pub fn set_ball_unit_hash(&self, ball: String, unit: String) -> Result<()> {
        info!("set ball, unit = {}, ball = {}", unit, ball);
        self.ball_units.write().unwrap().insert(ball, unit);
        Ok(())
    }

    // insert entry <address, (unit, definition)> into definitions
    pub fn insert_definition(&self, addr: String, unit: String, def: Value) {
        use hashbrown::hash_map::Entry;
        match self.definitions.write().unwrap().entry(addr) {
            Entry::Occupied(mut o) => {
                o.insert((unit, def));
            }
            Entry::Vacant(v) => {
                v.insert((unit, def));
            }
        }
        //TODO: save definitions into KV-Store
    }

    // get definition by address from definitions
    pub fn get_definition(&self, addr: &str) -> Option<(String, Value)> {
        self.definitions.read().unwrap().get(addr).cloned()
        //TODO: if not found try to read from database
    }

    // purge unhandled joints that are old enough
    // now: is the current time in ms
    // timeout: is the timeout value in ms
    pub fn purge_old_unhandled_joints(&self, now: u64, timeout: u64) {
        self.joints
            .write()
            .unwrap()
            .purge_old_unhandled_joints(now, timeout);
    }

    // purge temp-bad free joints that are old enough
    // now: is the current time in ms
    // timeout: is the timeout value in ms
    pub fn purge_old_temp_bad_free_joints(&self, now: u64, timeout: u64) -> Result<()> {
        self.joints
            .write()
            .unwrap()
            .purge_old_temp_bad_free_joints(now, timeout)
    }

    pub fn get_joints_len(&self) -> usize {
        self.joints.read().unwrap().get_normal_joints_len()
    }
}
