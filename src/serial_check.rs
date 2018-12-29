use std::collections::{HashSet, VecDeque};
use std::sync::Arc;

use cache::{CachedJoint, SDAG_CACHE};
use error::Result;
use joint::{Joint, JointSequence};

#[inline]
fn is_authored_by_any_addr(joint: &Joint, addresses: &[&String]) -> bool {
    0 != joint
        .unit
        .authors
        .iter()
        .filter(|a| addresses.contains(&&a.address))
        .count()
}

/// check if non_serial with unstable units
/// return true when non_serial, return false when serial
pub fn is_unstable_joint_non_serial(joint: CachedJoint) -> Result<bool> {
    let joint_data = joint.read()?;

    let unstable_ancestors = get_unstable_ancestor_units(vec![joint.clone()], HashSet::new())?; // A2 (contain self)

    // A3 =  A1 - A2 ( the set of which P can't see) (not contain self)
    let free_joints = SDAG_CACHE.get_free_joints()?;
    let no_see_units = get_unstable_ancestor_units(free_joints, unstable_ancestors)?;

    // A4 (the set of joints who can see P)
    let descendants = if joint_data.is_free() {
        Default::default()
    } else {
        get_descendant_units(joint)?
    };

    // A5 = A3 -A4 (the set of joints that can't see p each other)
    let no_include_relationship_units = no_see_units.difference(&descendants);

    let addresses = joint_data
        .unit
        .authors
        .iter()
        .map(|a| &a.address)
        .collect::<Vec<_>>();

    for u in no_include_relationship_units {
        let j_data = SDAG_CACHE.get_joint(&u)?.read()?;
        if is_authored_by_any_addr(&j_data, &addresses)
            && j_data.get_sequence() != JointSequence::FinalBad
        {
            info!(
                "joint [{}] detect non serial with unit [{}]",
                joint_data.unit.unit, j_data.unit.unit
            );
            return Ok(true);
        }
    }

    Ok(false)
}

/// get unstable units which included by joints
fn get_unstable_ancestor_units(
    joints: Vec<CachedJoint>,
    mut visited: HashSet<Arc<String>>,
) -> Result<HashSet<Arc<String>>> {
    let mut queue = VecDeque::new();
    let mut result = HashSet::new();

    for joint in joints {
        if visited.insert(joint.key.clone()) {
            queue.push_back(joint);
        }
    }

    while let Some(joint) = queue.pop_front() {
        let joint_data = joint.read()?;

        if joint_data.is_stable() {
            continue;
        }

        result.insert(joint.key.clone());

        for p in joint_data.parents.iter() {
            if visited.insert(p.key.clone()) {
                queue.push_back(p.clone());
            }
        }
    }

    Ok(result)
}

/// get all joint's descendants
fn get_descendant_units(joint: CachedJoint) -> Result<HashSet<Arc<String>>> {
    let mut queue = VecDeque::new();
    let mut visited = HashSet::new();

    queue.push_back(joint);

    // the set include starting joint
    while let Some(joint) = queue.pop_front() {
        for child in joint.read()?.children.iter() {
            let child = &*child;
            if visited.insert(child.key.clone()) {
                queue.push_back(child.clone());
            }
        }
    }

    Ok(visited)
}
