extern crate sdag_wallet_base;

use std::collections::VecDeque;
use std::sync::atomic::{AtomicIsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use hashbrown::HashSet;
use may::sync::RwLock;
use rcu_cell::RcuReader;
use sdag::business::BUSINESS_CACHE;
use sdag::cache::{CachedJoint, JointData, SDAG_CACHE};
use sdag::error::Result;
use sdag::joint::JointSequence;
use sdag::joint::Level;
use sdag::my_witness::MY_WITNESSES;
use sdag::utils::AtomicLock;
use sdag_wallet_base::Base64KeyExt;
use serde_json;
use WALLET_INFO;

lazy_static! {
    static ref IS_WITNESSING: AtomicLock = AtomicLock::new();
    static ref EVENT_TIMER: Arc<RwLock<Option<Instant>>> = Arc::new(RwLock::new(None));
    static ref WALLET_PUBK: String = WALLET_INFO._00_address_pubk.to_base64_key();
    static ref SELF_LEVEL: AtomicIsize = AtomicIsize::new(-2);
}

const THRESHOLD_DISTANCE: isize = sdag::config::COUNT_WITNESSES as isize * 2 / 3;

pub fn witness_timer_check() -> Result<Duration> {
    match check_timeout() {
        None => {
            if is_need_witnessing()? {
                witness()?;
            }
            *EVENT_TIMER.write().unwrap() = None;
            Ok(Duration::from_secs(1))
        }
        Some(dur) => Ok(dur),
    }
}

fn set_timeout(sleep_time_ms: u64) {
    let next_expire = Instant::now() + Duration::from_millis(sleep_time_ms);
    let mut g = EVENT_TIMER.write().unwrap();
    if Some(next_expire) > *g {
        *g = Some(next_expire);
    }
}

// when check_timeout return None means we need to take action
// when return Some(duration) means we need to sleep duration for next check
#[inline]
fn check_timeout() -> Option<Duration> {
    let g = EVENT_TIMER.read().unwrap();

    match *g {
        None => Some(Duration::from_secs(1)),

        Some(time) => {
            let now = Instant::now();

            if now >= time {
                None
            } else {
                Some(time - now)
            }
        }
    }
}

pub fn check_and_witness() {
    info!("check and witness");
    let _g = match IS_WITNESSING.try_lock() {
        Some(g) => g,
        None => {
            info!("witnessing under way");
            return;
        }
    };

    if adjust_witnessing_speed().is_err() {
        error!("adjust_witnessing_speed failed");
    };
}

/// adjust witnessing speed
fn adjust_witnessing_speed() -> Result<()> {
    use rand::{thread_rng, Rng};
    let mut rng = thread_rng();
    let time;
    let self_level = SELF_LEVEL.load(Ordering::Relaxed);
    let mut distance: isize = -1;
    if self_level < 0 {
        time = (rng.gen_range(0.0, 1.0) * 2_000.0) as u64;
    } else {
        let free_joints = SDAG_CACHE.get_all_free_joints()?;
        let free_joint_level = sdag::main_chain::find_best_joint(free_joints.iter())?
            .ok_or_else(|| format_err!("empty best joint among free joints"))?
            .get_level()
            .value() as isize;

        // free_joint_level may less than self_level, so distance and SELF_LEVEL can not be usize
        distance = free_joint_level - self_level;
        if distance < THRESHOLD_DISTANCE {
            time = ((THRESHOLD_DISTANCE - distance) * 200) as u64;
        } else if distance < 25 {
            time = ((THRESHOLD_DISTANCE as f64 / distance as f64) * 200.0) as u64;
        } else if distance < 100 {
            time = (rng.gen_range(0.0, 1.0) * 200.0) as u64;
        } else if distance < 1000 {
            time = (rng.gen_range(0.0, 1.0) * 50.0) as u64;
        } else {
            time = (rng.gen_range(0.0, 1.0) * 10.0) as u64;
        }
    }
    info!(
        "will post a witness joint after {} ms unless a new joint arrives, the distance from max_mc_level to max_my_level is {}.",
        time, distance
    );
    set_timeout(time);

    Ok(())
}

/// witnessing condition:
/// 1) last self unstable joint is relative stable to free joint, that means the path from free joint to my last unstable joint have more than 6 diff witnesses
/// 2) non witness joint mci > min retrievable mci, min retrievable is last_stable_joint's last_stable_unit mci
/// 3) last self unstable joint support current main chain, that means current main chain include my last unstable joint (cancel)
fn is_need_witnessing() -> Result<(bool)> {
    info!("if need post witness joint?");
    let _g = match IS_WITNESSING.try_lock() {
        Some(g) => g,
        None => {
            info!("witness_before_threshold under way");
            return Ok(false);
        }
    };

    let free_joints = SDAG_CACHE.get_all_free_joints()?;

    if free_joints.is_empty() {
        return Ok(false);
    }

    let best_joint = sdag::main_chain::find_best_joint(free_joints.iter())?
        .ok_or_else(|| format_err!("empty best joint among free joints"))?;

    let (need_witness, has_normal_joint) = is_relative_stable(best_joint)?;

    if !need_witness {
        return Ok(false);
    }
    info!("more than 6 witness on path of best parents");

    if has_normal_joint {
        return Ok(true);
    }

    // if is_successive_witnesses(&best_joint)? {
    //     return Ok(false);
    // }

    is_normal_joint_behind_min_retrievable(&free_joints)
}

/// return true if more than 6 different other witnesses from best free joints until stable
/// return true if has unstable normal joints
fn is_relative_stable(mut best_free_parent: RcuReader<JointData>) -> Result<(bool, bool)> {
    let mut has_normal_joints = false;

    let mut diff_witnesses = HashSet::new();
    while !(best_free_parent.is_stable() || best_free_parent.unit.is_genesis_unit()) {
        for author in &best_free_parent.unit.authors {
            if WALLET_INFO._00_address == author.address {
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

/// return true if successive witnessing (contains no normal joint)
#[allow(dead_code)]
fn is_successive_witnesses(best_joint: &CachedJoint) -> Result<bool> {
    let mut best_free_parent = best_joint.read()?;
    let mut diff_witnesses = HashSet::new();
    while !(best_free_parent.is_stable() || best_free_parent.unit.is_genesis_unit()) {
        for author in &best_free_parent.unit.authors {
            if WALLET_INFO._00_address == author.address {
                return Ok(true);
            }

            if MY_WITNESSES.contains(&author.address) {
                diff_witnesses.insert(author.address.clone());
            } else {
                return Ok(false);
            }
        }
        // need at least half other witnesses
        if diff_witnesses.len() >= sdag::config::COUNT_WITNESSES - 3 {
            break;
        }
        best_free_parent = best_free_parent.get_best_parent().read()?;
    }
    Ok(false)
}

/// return true if non witness joint behind min retrievable mci, it is very heavy!!!
fn is_normal_joint_behind_min_retrievable(free_joints: &[CachedJoint]) -> Result<bool> {
    let min_retrievable_mci = get_min_retrievable_unit()?.get_mci();
    let mut queue = VecDeque::new();
    let mut visited = HashSet::new();
    for joint in free_joints {
        if visited.insert(joint.key.clone()) {
            queue.push_back(joint.clone());
        }
    }

    while let Some(joint) = queue.pop_front() {
        let joint_data = joint.read()?;
        let mci = joint_data.get_mci();
        if mci <= min_retrievable_mci {
            continue;
        }
        for author in &joint_data.unit.authors {
            if !MY_WITNESSES.contains(&author.address)
                && joint_data.get_sequence() != JointSequence::FinalBad
            {
                return Ok(true);
            }
        }
        for p in joint_data.parents.iter() {
            if visited.insert(p.key.clone()) {
                queue.push_back(p.clone());
            }
        }
    }

    Ok(false)
}

/// get min retrievable unit: last stable unit's last stable unit
fn get_min_retrievable_unit() -> Result<RcuReader<JointData>> {
    // we can unwrap here because free joints is not empty
    let last_stable_joint = sdag::main_chain::get_last_stable_joint();
    if let Some(ref unit) = last_stable_joint.unit.last_ball_unit {
        return SDAG_CACHE.get_joint(unit)?.read();
    }
    // only genesis has no last ball unit
    Ok(last_stable_joint)
}

#[derive(Serialize)]
struct TimeStamp {
    timestamp: u64,
}

/// compose witness joint and validate, save, post
fn witness() -> Result<()> {
    // as we can not completely avoid the case that witness joints concurrency
    // so we will cancel the limit that witness cancel post a new joint when it's joint is free joint.
    // let free_joints = SDAG_CACHE.get_all_free_joints()?;
    // for joint in &free_joints {
    //     let joint_data = joint.read()?;
    //     for author in &joint_data.unit.authors {
    //         if author.address == WALLET_INFO._00_address {
    //             warn!(
    //                 "my witness unit [{:?}] is free joint, post the block joint again, and cancel post a new joint",
    //                 joint_data.unit.unit
    //             );

    //             if let Some(ws) = sdag::network::hub::WSS.get_next_peer() {
    //                 return ws.post_joint(&joint_data);
    //             }
    //         }
    //     }
    // }

    // divide one output into two outputs, to increase witnessing concurrent performance
    // let amount = divide_money(&WALLET_INFO._00_address)?;
    info!("will compose and post a witness joint");
    for i in 0..10 {
        match compose_and_normalize() {
            Ok(_) => break,
            Err(e) => error!("compose witness joint failed, times {}, err = [{:?}]", i, e),
        }

        may::coroutine::sleep(Duration::from_millis(10));
    }

    Ok(())
}

fn compose_and_normalize() -> Result<()> {
    // get inputs first, get last ball second, for meeting the limit that input units must before last ball
    // at most we need another 1000 sdg (usually 431 + 197)
    let (inputs, amount) =
        BUSINESS_CACHE.get_inputs_for_amount(&WALLET_INFO._00_address, 1_000 as u64, false)?;

    let sdag::composer::ParentsAndLastBall {
        parents,
        last_ball,
        last_ball_unit,
    } = sdag::composer::pick_parents_and_last_ball(&WALLET_INFO._00_address)?;

    let light_props = sdag::light::LightProps {
        last_ball,
        last_ball_unit,
        parent_units: parents,
        witness_list_unit: sdag::config::get_genesis_unit(),
        has_definition: SDAG_CACHE
            .get_definition(&WALLET_INFO._00_address)
            .is_some(),
    };

    let mut compose_info = sdag::composer::ComposeInfo {
        paid_address: WALLET_INFO._00_address.clone(),
        change_address: WALLET_INFO._00_address.clone(),
        outputs: Vec::new(),
        inputs: sdag::light::InputsResponse { inputs, amount },
        transaction_amount: 0,
        text_message: None,
        light_props,
        pubk: WALLET_PUBK.clone(),
    };

    if sdag::config::get_need_post_timestamp() {
        let time_stamp = TimeStamp {
            timestamp: sdag::time::now() / 1_000,
        };
        let data_feed_msg = sdag::spec::Message {
            app: "data_feed".to_string(),
            payload_location: "inline".to_string(),
            payload_hash: sdag::object_hash::get_base64_hash(&time_stamp)?,
            payload: Some(sdag::spec::Payload::Other(serde_json::to_value(
                time_stamp,
            )?)),
            payload_uri: None,
            payload_uri_hash: None,
            spend_proofs: Vec::new(),
        };

        compose_info.text_message = Some(data_feed_msg);
    }

    let joint = sdag::composer::compose_joint(compose_info, &*WALLET_INFO)?;

    let cached_joint = SDAG_CACHE.add_new_joint(joint, None)?;

    let joint_data = cached_joint.read()?;
    sdag::validation::validate_ready_joint(cached_joint)?;
    let sequence = joint_data.get_sequence();
    if sequence != JointSequence::Good {
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
        "comose and validate success, will post [{}]",
        joint_data.unit.unit
    );
    // we just post the joint to one hub
    if let Some(ws) = sdag::network::hub::WSS.get_next_peer() {
        ws.post_joint(&joint_data)?;
    }

    Ok(())
}
