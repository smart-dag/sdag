use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use cache::{CachedData, CachedJoint, HashKey, JointData};
use error::Result;
use joint::JointSequence;
use rcu_cell::RcuCell;

//---------------------------------------------------------------------------------------
// SDagCacheInner
//---------------------------------------------------------------------------------------
// this is the inner mem cache data struct
#[derive(Default)]
pub struct SDagCacheInner {
    // keep all joints here, the source of every thing
    normal_joints: HashMap<HashKey, CachedJoint>,
    // this is graph entry that contains all the free joint
    free_joints: HashMap<HashKey, CachedJoint>,
    // joints that missing parents
    unhandled_joints: HashMap<HashKey, CachedJoint>,
    // dependency that missing
    missing_parents: HashMap<String, Vec<CachedJoint>>,
    // known bad joints: unit_hash, error message
    known_bad_joints: HashMap<String, String>,
}

impl SDagCacheInner {
    /// insert a valid joint into the cache
    pub fn add_normal_joint(&mut self, hash_key: HashKey, data: JointData) -> CachedJoint {
        let key = hash_key.0.clone();
        self.normal_joints
            .entry(hash_key)
            .or_insert_with(|| CachedData {
                key,
                data: RcuCell::new(Some(data)),
            })
            .clone()
    }

    /// insert a valid joint into the cache
    pub fn add_unhandled_joint(&mut self, hash_key: HashKey, data: JointData) -> CachedJoint {
        let key = hash_key.0.clone();
        self.unhandled_joints
            .entry(hash_key)
            .or_insert_with(|| CachedData {
                key,
                data: RcuCell::new(Some(data)),
            })
            .clone()
    }

    /// remove a joint entry from cache
    pub fn del_joint(&mut self, key: &str) -> Option<(HashKey, CachedJoint)> {
        self.normal_joints.remove_entry(key)
    }

    /// get a joint from cache
    pub fn get_joint(&self, key: &str) -> Option<CachedJoint> {
        self.normal_joints.get(key).cloned()
    }

    pub fn get_num_of_normal_joints(&self) -> usize {
        self.normal_joints.len()
    }

    /// add empty joint into the cache
    /// this is used when there are some (parents) refs that need to create
    pub fn add_empty_joint(&mut self, key: &str) -> CachedJoint {
        let key = Arc::new(key.to_owned());
        let hash_key = HashKey(key.clone());
        self.normal_joints
            .entry(hash_key)
            .or_insert_with(|| CachedData {
                key,
                data: RcuCell::default(),
            })
            .clone()
    }

    /// get all the good free joints
    pub fn get_free_joints(&self) -> Result<Vec<CachedJoint>> {
        // judge if the joint has all bad children
        fn is_all_children_bad(joint: &JointData) -> Result<bool> {
            for child in joint.children.iter() {
                let child_data = child.read()?;
                if child_data.get_sequence() == JointSequence::Good
                    || child_data.unit.is_authored_by_witness()
                {
                    return Ok(false);
                }
            }
            Ok(true)
        }

        let mut free_joints = Vec::new();
        let mut joints = self.free_joints.values().cloned().collect::<VecDeque<_>>();
        let mut visited = HashSet::new();

        while let Some(joint) = joints.pop_front() {
            let joint_data = joint.read()?;
            let key = joint.key.clone();

            if !visited.insert(key) {
                continue;
            }

            if joint_data.get_sequence() == JointSequence::Good
                || joint_data.unit.is_authored_by_witness()
            {
                if is_all_children_bad(&joint_data)? {
                    free_joints.push(joint);
                }
                continue;
            }

            // the joint is now temp-bad
            for parent in joint_data.parents.iter() {
                joints.push_back(parent.clone());
            }
        }

        Ok(free_joints)
    }

    /// query if joint is in unhandled joints
    pub fn is_known_unhandled_joint(&self, key: &str) -> bool {
        self.unhandled_joints.contains_key(key)
    }

    pub fn get_num_of_unhandled_joints(&self) -> usize {
        self.unhandled_joints.len()
    }

    /// move a joint from unhandled to normal
    pub fn transfer_joint_to_normal(&mut self, key: &str) {
        if let Some((k, v)) = self.unhandled_joints.remove_entry(key) {
            self.normal_joints.entry(k).or_insert_with(|| v.clone());
        }
    }

    /// query if joint is known bad
    pub fn is_known_bad_joint(&self, key: &str) -> bool {
        self.known_bad_joints.contains_key(key)
    }

    pub fn get_num_of_known_bad_joints(&self) -> usize {
        self.known_bad_joints.len()
    }

    /// remove the missing parent entry if the parent is validate good
    /// and trigger dependent children that are satisfied
    /// append the joint as child for all it's parents
    pub fn update_parent_and_child(&mut self, key: &str) {
        let joint = self.get_joint(key).expect("not found ready joint");

        // add parents for my children
        if let Some((_k, v)) = self.missing_parents.remove_entry(key) {
            for child in v {
                let child_data = child.raw_read();
                child_data.add_parent(joint.clone());
                if !child_data.is_missing_parent() {
                    // trigger the child ready here, start validate, save and so on
                    try_go!(|| ::validation::validate_ready_joint(child));
                }
            }
        }

        // add child for my parents and free joints
        let joint_data = joint.raw_read();
        for parent in joint_data.parents.iter() {
            let parent_data = parent.raw_read();
            // remove parent from free joints
            self.free_joints.remove(&*parent.key);
            // add child for parents
            parent_data.add_child(joint.clone());
        }

        // add the new free joints
        self.free_joints.insert(HashKey(joint.key.clone()), joint);
    }

    /// remove the bad parent and all it's desendants
    pub fn purge_bad_joint(&mut self, key: &str, err: String) {
        let mut stack = vec![key.to_owned()];
        let mut error = Some(err);

        // recursiely remove the bad joint along the child of the graph
        // we use deep search without a revisited hashmap
        while let Some(key) = stack.pop() {
            // remove from unhandled
            self.unhandled_joints.remove(&key);
            // remove all it's desendants
            if let Some(v) = self.missing_parents.remove(&key) {
                for child in v {
                    stack.push(child.key.as_ref().to_owned());
                }
            }
            // insert into known bad
            let err = error.take().unwrap_or_else(|| String::from("bad parent"));
            error!("add known bad joint = {}, err={}", key, err);
            self.known_bad_joints.entry(key).or_insert(err);
        }
    }

    /// add a missing parent dependent
    pub fn add_missing_parent(&mut self, missing_parent: String, child: CachedJoint) {
        self.missing_parents
            .entry(missing_parent)
            .and_modify(|v| {
                assert_eq!(v.contains(&child), false);
                v.push(child.clone());
            })
            .or_insert_with(|| vec![child]);
    }

    /// return all the missing joints
    pub fn get_all_missing_joints(&self) -> Vec<String> {
        self.missing_parents
            .keys()
            .filter(|key| !self.unhandled_joints.contains_key(*key))
            .cloned()
            .collect()
    }

    /// remove the parent and all it's descendants
    fn purge_unhandled_joint(&mut self, key: Arc<String>) {
        let mut stack = vec![key];

        // recursively remove the joint along the child of the graph
        // we use deep search without a revisited hashmap
        while let Some(key) = stack.pop() {
            // remove from unhandled
            self.unhandled_joints.remove(&*key);
            // remove all it's descendants
            if let Some(v) = self.missing_parents.remove(&*key) {
                for child in v {
                    stack.push(child.key);
                }
            }
        }
    }

    // purge unhandled joints that are old enough
    // now: is the current time in ms
    // timeout: is the timeout value in ms
    pub fn purge_old_unhandled_joints(&mut self, now: u64, timeout: u64) {
        let mut old_joints = Vec::new();

        // collect those old enough joints
        for (k, j) in self.unhandled_joints.iter() {
            // since all unhandled joints are in cache, we can safely unwrap here
            let joint_data = j.read().unwrap();

            if now - joint_data.get_create_time() >= timeout {
                old_joints.push(k.clone());
            }
        }

        // purge all the old joints
        for k in old_joints {
            self.purge_unhandled_joint(k.0);
        }
    }

    // remove the free joint from normal joints
    // remove the free joint from free joints
    // add it's parent back to free joints if possible
    fn purge_free_joint(&mut self, joint: &str) -> Result<()> {
        let mut stack = vec![joint.to_owned()];

        while let Some(ref joint) = stack.pop() {
            self.free_joints.remove(joint);
            let joint = self
                .normal_joints
                .remove(joint)
                .expect("purge_free_joint not found")
                .raw_read();

            let unit = &joint.unit.unit;
            for parent in joint.parents.iter() {
                // remove the child for the parent
                // if the parent becomes free just add back to free list
                let parent_joint = parent.read()?;
                parent_joint.children.remove_with(|j| &*j.key == unit);
                if parent_joint.is_free() {
                    if parent_joint.get_sequence() != JointSequence::Good {
                        // remove this "bad" "free" parent
                        stack.push(parent_joint.unit.unit.to_owned());
                    } else {
                        self.free_joints
                            .insert(HashKey(parent.key.clone()), parent.clone());
                    }
                }
            }
        }

        Ok(())
    }

    // purge temp-bad free joints that are old enough
    // now: is the current time in ms
    // timeout: is the timeout value in ms
    pub fn purge_old_temp_bad_free_joints(&mut self, now: u64, timeout: u64) -> Result<()> {
        // collect those bad joints
        let joints = self
            .free_joints
            .iter()
            .filter_map(|(k, j)| {
                // free joints must be in cache so that we can safely unwrap it
                let joint = j.raw_read();
                if joint.get_sequence() != JointSequence::TempBad {
                    return None;
                }

                if now - joint.get_create_time() < timeout {
                    return None;
                }

                Some(k.clone())
            })
            .collect::<Vec<_>>();

        // remove from normal joints and free joints
        for joint in joints {
            self.purge_free_joint(&joint)?;
        }
        Ok(())
    }

    pub fn get_normal_joints_len(&self) -> usize {
        self.normal_joints.len()
    }
}
