use std::collections::HashSet;

use cache::{CachedJoint, SDAG_CACHE};
use config;
use error::Result;
use joint::JointSequence;

#[allow(dead_code)]
pub struct ParentsAndBall {
    parents: Vec<String>,
    last_ball: String,
}

pub fn pick_parents() -> Result<ParentsAndBall> {
    let mut good_cache_joints = SDAG_CACHE.get_free_joints();
    let bad_cache_joints = SDAG_CACHE.get_free_bad_joints();

    let last_stable_joint = ::main_chain::get_last_stable_joint();

    if !bad_cache_joints.is_empty() {
        good_cache_joints.append(&mut pick_parents_from_free_bad_joints(bad_cache_joints)?);
    }

    if good_cache_joints.len() <= config::MAX_PARENT_PER_UNIT {
        return Ok(ParentsAndBall {
            parents: good_cache_joints
                .iter()
                .map(|v| v.read().unwrap().unit.unit.clone())
                .collect::<Vec<_>>(),
            last_ball: last_stable_joint.read()?.unit.unit.clone(),
        });
    }

    for group in good_cache_joints.chunks(16) {
        if !::main_chain::is_stable_in_later_joints(&last_stable_joint, &group)? {
            continue;
        }

        for cache_joint in group {
            let joint = cache_joint.read().expect("reading cache_joint is error");
            if joint.unit.version != config::VERSION || joint.unit.alt != config::ALT {
                bail!("wrong network");
            }
        }

        return Ok(ParentsAndBall {
            parents: group
                .iter()
                .map(|v| v.read().unwrap().unit.unit.clone())
                .collect::<Vec<_>>(),
            last_ball: last_stable_joint.read()?.unit.unit.clone(),
        });
    }

    bail!("fail to choose parents")
}

fn pick_parents_from_free_bad_joints(mut bad_joints: Vec<CachedJoint>) -> Result<Vec<CachedJoint>> {
    let mut parents: Vec<CachedJoint> = Vec::new();
    let mut visited_set: HashSet<String> = HashSet::new();

    for cache_joint in bad_joints.pop() {
        let joint = cache_joint.read()?;

        for cache_parent in joint.parents.iter() {
            let parent = cache_parent.read()?;
            if parent.get_sequence() != JointSequence::Good {
                bad_joints.push(cache_parent.clone());
                continue;
            }

            if !visited_set.contains(&parent.unit.unit) {
                parents.push(cache_parent.clone());
                visited_set.insert(parent.unit.unit.clone());
            }
        }
    }

    Ok(parents)
}
