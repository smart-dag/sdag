use std::sync::atomic::{AtomicIsize, Ordering};
use std::time::Duration;

use hashbrown::HashSet;
use rcu_cell::RcuReader;
use sdag::business::BUSINESS_CACHE;
use sdag::cache::{CachedJoint, JointData, SDAG_CACHE};
use sdag::error::Result;
use sdag::joint::JointSequence;
use sdag::joint::Level;
use sdag::my_witness::MY_WITNESSES;
use sdag::wallet_info::MY_WALLET;
use sdag_wallet_base::Base64KeyExt;

lazy_static! {
    static ref WALLET_PUBK: String = MY_WALLET._00_address_pubk.to_base64_key();
    static ref SELF_LEVEL: AtomicIsize = AtomicIsize::new(-6); // set -6 to meet from free level to self level more than 6 when start chain
}

pub fn witness_timer_check() -> Result<Duration> {
    if is_need_witnessing()? {
        witness()?;
    }

    use rand::{thread_rng, Rng};
    let mut rng = thread_rng();
    let time = rng.gen_range(200, 1000);

    // random delay between 0.5 ~ 2
    Ok(Duration::from_millis(time))
}

/// witnessing condition:
/// 1) last self unstable joint is relative stable to free joint, that means the path from free joint to my last unstable joint have more than 6 diff witnesses
/// 2) non witness joint mci > min retrievable mci, min retrievable is last_stable_joint's last_stable_unit mci
/// 3) last self unstable joint support current main chain, that means current main chain include my last unstable joint (cancel)
fn is_need_witnessing() -> Result<(bool)> {
    info!("witnessing: if need post witness joint?");
    let free_joints = SDAG_CACHE.get_all_free_joints();

    if free_joints.is_empty() {
        return Ok(false);
    }

    // distance from max_level_free to SELF_LEVEL should more than 6
    if !is_more_than_six_to_last_self(&free_joints)? {
        return Ok(false);
    }

    let best_joint = sdag::main_chain::find_best_joint(free_joints.iter())?
        .ok_or_else(|| format_err!("empty best joint among free joints"))?;

    let (need_witness, has_normal_joint) = is_relative_stable(best_joint.clone())?;

    if !need_witness {
        return Ok(false);
    }
    info!("witnessing: more than 6 witness on path of best parents");

    if has_normal_joint {
        return Ok(true);
    }

    is_need_witness_normal_joint(&free_joints, best_joint)
}

/// return true if more than six joints from free joints to last_self
fn is_more_than_six_to_last_self(free_joints: &[CachedJoint]) -> Result<(bool)> {
    let self_level = SELF_LEVEL.load(Ordering::Relaxed);
    for unit in free_joints {
        let level = unit.read()?.get_level();
        if level.value() as isize - self_level >= sdag::config::MAJORITY_OF_WITNESSES as isize - 1 {
            return Ok(true);
        }
    }

    Ok(false)
}

/// return true if more than 6 different other witnesses from best free joints until stable
/// return true if has unstable normal joints
fn is_relative_stable(mut best_free_parent: RcuReader<JointData>) -> Result<(bool, bool)> {
    let mut has_normal_joints = false;

    let mut diff_witnesses = HashSet::new();
    while !(best_free_parent.is_stable() || best_free_parent.unit.is_genesis_unit()) {
        for author in &best_free_parent.unit.authors {
            if MY_WALLET._00_address == author.address {
                return Ok((false, has_normal_joints));
            }

            if MY_WITNESSES.contains(&author.address) {
                diff_witnesses.insert(author.address.clone());
            } else {
                has_normal_joints = true;
            }
        }
        // need at least half other witnesses
        if diff_witnesses.len() >= sdag::config::MAJORITY_OF_WITNESSES - 1 {
            break;
        }
        best_free_parent = best_free_parent.get_best_parent().read()?;
    }

    Ok((true, has_normal_joints))
}

/// return lastest unstable normal joint ordered by level
fn get_unstable_latest_normal_joint(
    free_joints: &[CachedJoint],
) -> Result<Option<RcuReader<JointData>>> {
    use std::cmp;
    use std::collections::BinaryHeap;
    #[derive(PartialOrd, PartialEq)]
    struct OrdJoint(RcuReader<JointData>);
    impl cmp::Eq for OrdJoint {}
    impl cmp::Ord for OrdJoint {
        fn cmp(&self, other: &OrdJoint) -> cmp::Ordering {
            self.0.get_level().value().cmp(&other.0.get_level().value())
        }
    }

    impl From<RcuReader<JointData>> for OrdJoint {
        fn from(joint: RcuReader<JointData>) -> Self {
            OrdJoint(joint)
        }
    }

    let mut queue = BinaryHeap::<OrdJoint>::new();
    let mut visited = HashSet::new();
    for joint in free_joints {
        if visited.insert(joint.key.clone()) {
            queue.push(joint.read()?.into());
        }
    }

    while let Some(joint) = queue.pop() {
        let joint_data = joint.0;
        if joint_data.is_stable() {
            continue;
        }

        // only good normal transaction joints need be witnessed;
        // non serial is not consensus between diff hubs or witnesses, example: a joint is good in hub, but non serial in witness, witness will witnessing forever until the non serial joint is not free;
        // finalbad is not need.
        if !joint_data.unit.is_authored_by_witness()
            && joint_data.get_sequence() == JointSequence::Good
        {
            return Ok(Some(joint_data));
        }

        for p in joint_data.parents.iter() {
            if visited.insert(p.key.clone()) {
                queue.push(p.read()?.into());
            }
        }
    }

    Ok(None)
}

/// find the oldest mc joint that include the normal joint
fn find_best_include_mc_joint(
    mut best_joint: RcuReader<JointData>,
    normal_joint: RcuReader<JointData>,
) -> Result<Option<RcuReader<JointData>>> {
    let mut stack = Vec::new();
    let normal_level = normal_joint.get_level();
    while best_joint.get_level() >= normal_level {
        let next_best_joint = best_joint.get_best_parent().read()?;
        stack.push(best_joint);
        best_joint = next_best_joint;
    }

    while let Some(joint) = stack.pop() {
        let is_include = joint >= normal_joint;
        if is_include {
            return Ok(Some(joint));
        }
    }

    Ok(None)
}

/// return true if we need to witness the normal joint
fn is_need_witness_normal_joint(
    free_joints: &[CachedJoint],
    best_joint: RcuReader<JointData>,
) -> Result<bool> {
    if let Some(joint) = get_unstable_latest_normal_joint(free_joints)? {
        if let Some(joint) = find_best_include_mc_joint(best_joint.clone(), joint)? {
            if sdag::main_chain::is_stable_to_joint(&joint, &best_joint)? {
                return Ok(false);
            }
        }
        return Ok(true);
    }
    Ok(false)
}

/// witness compose and post joint
fn witness() -> Result<()> {
    info!("witnessing: will compose and post a witness joint");
    for i in 0..10 {
        match compose_and_normalize() {
            Ok(_) => break,
            Err(e) => error!("compose witness joint failed, times {}, err = [{:?}]", i, e),
        }

        may::coroutine::sleep(Duration::from_millis(100));
    }

    Ok(())
}

/// compose, validation, normalize, post
fn compose_and_normalize() -> Result<()> {
    let sdag::composer::ParentsAndLastBall {
        parents,
        last_ball,
        last_ball_unit,
    } = sdag::composer::pick_parents_and_last_ball(&MY_WALLET._00_address)?;

    // at most we need another 1000 sdg (usually 431 + 197)
    let (inputs, amount) = BUSINESS_CACHE.get_inputs_for_amount(
        &MY_WALLET._00_address,
        1_000 as u64,
        false,
        &last_ball_unit,
    )?;

    let light_props = sdag::light::LightProps {
        last_ball,
        last_ball_unit,
        parent_units: parents,
        witness_list_unit: sdag::spec::GENESIS_UNIT.to_string(),
        has_definition: SDAG_CACHE.get_definition(&MY_WALLET._00_address).is_some(),
    };

    let compose_info = sdag::composer::ComposeInfo {
        paid_address: MY_WALLET._00_address.clone(),
        change_address: MY_WALLET._00_address.clone(),
        outputs: Vec::new(),
        inputs: sdag::light::InputsResponse { inputs, amount },
        transaction_amount: 0,
        text_message: None,
        light_props,
        pubk: WALLET_PUBK.clone(),
    };

    // if sdag::config::get_need_post_timestamp() {
    //     let time_stamp = TimeStamp {
    //         timestamp: sdag::time::now() / 1_000,
    //     };
    //     let data_feed_msg = sdag::spec::Message {
    //         app: "data_feed".to_string(),
    //         payload_location: "inline".to_string(),
    //         payload_hash: object_hash::get_base64_hash(&time_stamp)?,
    //         payload: Some(sdag::spec::Payload::Other(serde_json::to_value(
    //             time_stamp,
    //         )?)),
    //         payload_uri: None,
    //         payload_uri_hash: None,
    //         spend_proofs: Vec::new(),
    //     };

    //     compose_info.text_message = Some(data_feed_msg);
    // }

    let joint = sdag::composer::compose_joint(compose_info, &*MY_WALLET)?;

    let cached_joint = SDAG_CACHE.add_new_joint(joint, None)?;

    let joint_data = cached_joint.read()?;
    sdag::validation::validate_ready_joint(cached_joint)?;
    let sequence = joint_data.get_sequence();
    if sequence != JointSequence::Good {
        // purge the bad composed joint
        SDAG_CACHE.purge_free_joint(&joint_data.unit.unit)?;
        bail!(
            "only good joint is allowed to post for witness, unit={}, sequence={:?}",
            joint_data.unit.unit,
            sequence
        );
    }

    let mut max_parent_level = Level::MINIMUM;
    for parent in joint_data.parents.iter() {
        let level = parent.read()?.get_level();
        assert_eq!(level.is_valid(), true);
        if max_parent_level < level {
            max_parent_level = level;
        }
    }
    SELF_LEVEL.store(max_parent_level.value() as isize + 1, Ordering::Relaxed);
    info!(
        "witnessing: compose and validate success, will post [{}]",
        joint_data.unit.unit
    );

    // we just post the joint to one hub
    if let Some(ws) = sdag::network::hub::WSS.get_next_peer() {
        ws.post_joint(&joint_data)?;
    }

    Ok(())
}
