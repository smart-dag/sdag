use std::collections::VecDeque;
use std::sync::Arc;

use cache::{CachedJoint, JointData, SDAG_CACHE};
use error::Result;
use failure::ResultExt;
use hashbrown::HashSet;
use joint::Level;
use may::coroutine::JoinHandle;
use may::sync::mpsc;
use rcu_cell::{RcuCell, RcuReader};

lazy_static! {
    pub static ref MAIN_CHAIN_WORKER: MainChainWorker = MainChainWorker::default();
    static ref LAST_STABLE_JOINT: RcuCell<RcuReader<JointData>> = {
        match calc_last_stable_joint() {
            Ok(joint) => RcuCell::new(Some(joint)),
            Err(e) => {
                warn!("init LAST_STABLE_JOINT, err={}", e);
                RcuCell::new(None)
            }
        }
    };
}

//---------------------------------------------------------------------------------------
// MciStableEvent
//---------------------------------------------------------------------------------------
pub struct MciStableEvent {
    pub mci: Level,
}
impl_event!(MciStableEvent);

//---------------------------------------------------------------------------------------
// MainChainWorker
//---------------------------------------------------------------------------------------
pub struct MainChainWorker {
    tx: mpsc::Sender<RcuReader<JointData>>,
    _handler: JoinHandle<()>,
}

impl Default for MainChainWorker {
    fn default() -> Self {
        let (tx, rx) = mpsc::channel();
        let _handler = start_main_chain_worker(rx);

        MainChainWorker { tx, _handler }
    }
}

impl MainChainWorker {
    // the validation would call this API to push ready joint
    pub fn push_ready_joint(&self, joint: RcuReader<JointData>) -> Result<()> {
        self.tx.send(joint)?;
        Ok(())
    }
}

fn start_main_chain_worker(rx: mpsc::Receiver<RcuReader<JointData>>) -> JoinHandle<()> {
    go!(move || {
        // init it as -1 then the genesis min_wl = 0 can go forward
        let mut last_stable_level = Level::MINIMUM;

        // let mut last_stable_level = match LAST_STABLE_JOINT.read() {
        //     Some(j) => j.get_mci(),
        //     None => Level::MINIMUM,
        // };

        info!(
            "main chain worker started, last_stable_level = {:?}",
            last_stable_level
        );

        while let Ok(joint) = rx.recv() {
            if joint.get_min_wl() <= last_stable_level {
                continue;
            }

            let max_stable_joint = t_c!(joint.get_max_stable_unit());
            if max_stable_joint.get_level() > last_stable_level {
                info!(
                    "main chain worker get a valid joint, unit={}",
                    joint.unit.unit
                );
                last_stable_level = t_c!(update_main_chain(max_stable_joint));
            }
        }
        error!("main chain worker stopped!");
        ::std::process::abort();
    })
}

fn update_main_chain(joint: RcuReader<JointData>) -> Result<Level> {
    let mut valid_mc_joints = build_unstable_main_chain_from_joint(joint)?;
    let mut stable_joint = valid_mc_joints.pop().expect("no stable joint found!");
    // directly update to longest max stable unit since we have already verified
    stable_joint = update_stable_main_chain_joints(stable_joint, valid_mc_joints)?;
    Ok(stable_joint.get_level())
}

fn build_unstable_main_chain_from_joint(
    mut joint: RcuReader<JointData>,
) -> Result<(Vec<RcuReader<JointData>>)> {
    let mut mc_joints = Vec::new();

    while !joint.is_on_main_chain() {
        mc_joints.push(joint.clone());
        joint = joint.get_best_parent().read()?;
    }

    // add the stable joint
    mc_joints.push(joint);

    Ok(mc_joints)
}

fn mark_main_chain_joint_stable(main_chain_joint: &RcuReader<JointData>, mci: Level) -> Result<()> {
    main_chain_joint.set_limci(mci);

    let mut joints = VecDeque::new();
    let mut sorted = Vec::new();
    joints.push_back(main_chain_joint.clone());

    while let Some(joint) = joints.pop_front() {
        //Mci has already set but not stable yet, still waiting for balls
        if sorted.contains(&joint) || joint.get_mci().is_valid() {
            continue;
        }

        // parent_units is ordered, joint.parents is not ordered
        for parent in joint.parents.iter() {
            joints.push_back(parent.read()?);
        }

        sorted.push(joint);
    }

    // first sort by level, then unit hash
    sorted.sort_by(|a, b| {
        use std::cmp::Ordering;
        match PartialOrd::partial_cmp(&a.get_level(), &b.get_level()) {
            Some(Ordering::Equal) => Ord::cmp(&a.unit.unit, &b.unit.unit),
            Some(r) => r,
            None => unreachable!("invalid level cmp"),
        }
    });

    let mut sub_mci = Level::ZERO;
    for joint in sorted {
        // set sub_mci
        joint.set_sub_mci(sub_mci);
        sub_mci += 1;

        //limci on main chain joint is already set, do not overwrite it
        if !joint.get_limci().is_valid() {
            let mut limci = Level::ZERO;
            for parent in joint.parents.iter() {
                let parent_data = parent.read()?;

                //limci
                let parent_limci = parent_data.get_limci();
                if parent_limci > limci {
                    limci = parent_limci;
                }
            }
            joint.set_limci(limci);
        }

        // set mci
        joint.set_mci(mci);

        // push it to the business logic
        ::business::BUSINESS_WORKER.push_stable_joint(joint)?;
    }

    // update the global property
    SDAG_CACHE.set_mc_unit_hash(mci, main_chain_joint.unit.unit.clone())?;

    info!(
        "main chain update: last_stable_joint = {:?}",
        main_chain_joint.get_props()
    );

    ::utils::event::emit_event(MciStableEvent { mci });

    Ok(())
}

// update to last stable ball unit and pop from unstable_mc_joints
fn update_stable_main_chain_joints(
    mut stable_joint: RcuReader<JointData>,
    mut unstable_mc_joints: Vec<RcuReader<JointData>>,
) -> Result<RcuReader<JointData>> {
    let mut stable_level = stable_joint.get_mci();
    while let Some(joint) = unstable_mc_joints.pop() {
        stable_level += 1;
        mark_main_chain_joint_stable(&joint, stable_level)?;
        stable_joint = joint;
    }

    Ok(stable_joint)
}

// TODO: get last stable joint from db
fn calc_last_stable_joint() -> Result<RcuReader<JointData>> {
    let free_joints = SDAG_CACHE.get_all_free_joints();

    if free_joints.is_empty() {
        bail!("calc_last_stable_joint free_joints empty");
    }

    //Any free joint should connect to stable main chain
    let mut joint_data = free_joints[0].read()?;

    //Go up with best parent to reach the stable main chain
    while !joint_data.is_on_main_chain() {
        joint_data = joint_data.get_best_parent().read()?;
    }

    //Go down to find the last one
    loop {
        let mut child_on_mc = None;
        for child in joint_data.children.iter() {
            let child_data = child.read()?;

            if child_data.is_on_main_chain() {
                child_on_mc = Some(child_data);
                break;
            }
        }

        if let Some(child_data) = child_on_mc {
            joint_data = child_data;
        } else {
            return Ok(joint_data);
        }
    }
}

// get the lastest unit that makes the min_wl increased
fn get_lastest_effective_unit(joint: &RcuReader<JointData>) -> Result<RcuReader<JointData>> {
    let mut joint = joint.clone();
    while !joint.is_min_wl_increased() {
        joint = joint.get_best_parent().read()?;
    }
    Ok(joint)
}

// build the mc from joint until it's min_wl
fn build_unstable_main_chain_from_joint_to_min_wl(
    joint: &RcuReader<JointData>,
    min_wl: Level,
) -> Result<(Vec<RcuReader<JointData>>)> {
    let mut joint = joint.clone();
    let mut mc_joints = Vec::new();

    while joint.get_level() < min_wl {
        mc_joints.push(joint.clone());
        joint = joint.get_best_parent().read()?;
    }

    // put the last one
    mc_joints.push(joint);

    Ok(mc_joints)
}

// find all witness units form the joint until a level
// the witness unit must not on the main chain
fn get_all_alt_witness_units(
    joint: &RcuReader<JointData>,
    mc: &[RcuReader<JointData>],
    until_level: Level,
) -> Result<Vec<RcuReader<JointData>>> {
    let mut ret = Vec::new();
    // Limit the max_alt_level to the history in end joint's perspective
    let mut joints = VecDeque::new();
    let mut visited = HashSet::new();

    joints.push_back(joint.clone());
    while let Some(joint) = joints.pop_front() {
        let joint_level = joint.get_level();
        if joint_level <= until_level {
            continue;
        }

        for parent in joint.parents.iter() {
            let parent_data = parent.read()?;
            if visited.insert(parent.key.clone()) {
                joints.push_back(parent_data);
            }
        }

        if joint.unit.is_authored_by_witness() && !mc.contains(&joint) {
            // find another different witness along the bp
            let mut bp = joint.get_best_parent().read()?;
            let mut bp_level = bp.get_level();
            while bp_level > Level::ZERO {
                if bp_level < until_level {
                    break;
                }
                // here we ignore the same witness to speed up
                if bp.unit.is_authored_by_witness() && !mc.contains(&bp) {
                    if !ret.contains(&bp) {
                        ret.push(bp);
                    }
                    break;
                }

                bp = bp.get_best_parent().read()?;
                bp_level = bp.get_level();
            }
        }
    }

    Ok(ret)
}

// update the max intersect point of main chains
fn update_mc_to_intersect(
    mc: &mut Vec<RcuReader<JointData>>,
    visited: &mut HashSet<Arc<String>>,
    mut joint: RcuReader<JointData>,
) -> Result<()> {
    let mut last_mc_unit = mc
        .last()
        .cloned()
        .ok_or_else(|| format_err!("can't update empty mc"))?;

    let mut to_level = joint.get_level();
    while to_level > Level::ZERO {
        // extend the mc until the same level
        while last_mc_unit.get_level() > to_level {
            last_mc_unit = last_mc_unit.get_best_parent().read()?;
            mc.push(last_mc_unit.clone());
        }

        // reverse visit to speed up
        if mc.iter().rev().any(|j| j == &joint) {
            break;
        }

        let cached_joint = joint.get_best_parent();
        joint = cached_joint.read()?;
        visited.insert(cached_joint.key);
        to_level = joint.get_level();
    }

    Ok(())
}

// check if the last stable is best ancestor and on main chain
fn is_best_ancestor(
    last_ball: &RcuReader<JointData>,
    joint: &RcuReader<JointData>,
) -> Result<bool> {
    let stable_point = get_last_stable_joint();
    let stable_point_level = stable_point.get_level();
    let last_ball_level = last_ball.get_level();

    // last_ball joint is on main chain and before the stable point
    if last_ball_level <= stable_point_level {
        // last_ball joint must be stable if it on main chain
        if !last_ball.is_on_main_chain() {
            // last_ball joint must not no main chain
            info!(
                "is_best_ancestor return false, last_ball {} is not on main chain and before stable point",
                last_ball.unit.unit
            );
            return Ok(false);
        }

        // last_ball unit must be ancestor of joint
        let mut is_ancestor = false;
        let mut best_parent = joint.clone();
        while best_parent.get_level() >= last_ball_level {
            if best_parent.is_on_main_chain() {
                is_ancestor = true;
                break;
            }
            best_parent = best_parent.get_best_parent().read()?;
        }

        if !is_ancestor {
            info!(
                "is_best_ancestor return false, unit={}, last_ball_unit={} can't lead to last stable unit",
                joint.unit.unit, last_ball.unit.unit
            );
            return Ok(false);
        }
    } else {
        // last ball joint is after stable point
        let mut best_parent = last_ball.clone();
        while best_parent.get_level() > stable_point_level {
            best_parent = best_parent.get_best_parent().read()?;
        }

        if stable_point != best_parent {
            info!(
                "is_best_ancestor return false, last_ball {} is not on main chain, can't pass to stable point {}",
                last_ball.unit.unit,
                stable_point.unit.unit
            );
            return Ok(false);
        }

        // last_ball unit must be ancestor of joint
        let mut best_parent = joint.clone();
        while best_parent.get_level() > last_ball_level {
            best_parent = best_parent.get_best_parent().read()?;
        }

        if *last_ball != best_parent {
            info!(
                "is_best_ancestor return false, unit={}, last_ball_unit={} can't lead to last ball unit",
                joint.unit.unit, last_ball.unit.unit
            );
            return Ok(false);
        }
    }

    Ok(true)
}

//---------------------------------------------------------------------------------------
// pub APIs
//---------------------------------------------------------------------------------------

/// find the best joint among set of joints
/// Sort by max(wl), min(level), min(unit_hash)
pub fn find_best_joint<'a, I: IntoIterator<Item = &'a CachedJoint>>(
    iter: I,
) -> Result<Option<RcuReader<JointData>>> {
    let mut p_it = iter.into_iter();
    // at least one parent
    let mut best_joint_data = match p_it.next() {
        None => return Ok(None),
        Some(p) => p.read().context("find best joint")?,
    };

    for joint in p_it {
        let cur_joint_data = joint.read().context("find_best_joint")?;
        if cur_joint_data.is_precedence_than(&*best_joint_data) {
            best_joint_data = cur_joint_data;
        }
    }

    Ok(Some(best_joint_data))
}

// calc the max stable unit
pub fn calc_max_stable_unit(joint: RcuReader<JointData>) -> Result<CachedJoint> {
    let min_wl = joint.get_min_wl();
    let mut mc_joints = build_unstable_main_chain_from_joint_to_min_wl(&joint, min_wl)?;
    let alt_witnesses = get_all_alt_witness_units(&joint, &mc_joints, min_wl)?;
    // if we already visit the witness, we can skip it
    let mut visited = HashSet::new();
    for joint in alt_witnesses {
        if visited.insert(Arc::new(joint.unit.unit.to_owned())) {
            update_mc_to_intersect(&mut mc_joints, &mut visited, joint)?;
        }
    }
    let max_stable_unit = mc_joints
        .pop()
        .ok_or_else(|| format_err!("calc max stable unit failed, unit={}", joint.unit.unit))?;
    Ok(SDAG_CACHE.get_joint(&max_stable_unit.unit.unit)?)
}

/// judge if earlier_joint is relative stable to later_joint
pub fn is_stable_to_joint(
    last_ball: &RcuReader<JointData>,
    joint: &RcuReader<JointData>,
) -> Result<bool> {
    if last_ball.unit.is_genesis_unit() {
        return Ok(true);
    }

    let min_wl = joint.get_min_wl();
    let last_ball_level = last_ball.get_level();
    if min_wl < last_ball_level {
        info!(
            "is_stable_to_joint return false, min_wl={:?}, last_ball_level={:?}",
            min_wl, last_ball_level
        );
        return Ok(false);
    }

    let last_wl_increased_joint = get_lastest_effective_unit(&joint)?;
    let max_stable_unit = last_wl_increased_joint.get_max_stable_unit()?;
    is_best_ancestor(last_ball, &max_stable_unit)
}

/// Returns current unstable main chain from the best free joint
pub fn build_unstable_main_chain() -> Result<Vec<RcuReader<JointData>>> {
    let free_joints = SDAG_CACHE.get_good_free_joints()?;
    match find_best_joint(free_joints.iter())? {
        Some(best_free_joint) => {
            let mut mc_joints = build_unstable_main_chain_from_joint(best_free_joint)?;
            // remove the last stable one
            mc_joints.pop();
            Ok(mc_joints)
        }
        None => Ok(Vec::new()),
    }
}

/// get stable point mci
pub fn get_last_stable_mci() -> Level {
    match LAST_STABLE_JOINT.read() {
        Some(j) => j.get_mci(),
        None => Level::ZERO,
    }
}

/// get the stable point joint
pub fn get_last_stable_joint() -> RcuReader<JointData> {
    use std::time::Duration;
    loop {
        match LAST_STABLE_JOINT.read() {
            Some(j) => return j.as_ref().clone(),
            None => ::may::coroutine::sleep(Duration::from_millis(1)),
        }
    }
}

/// set the last stable joint
pub fn set_last_stable_joint(joint: RcuReader<JointData>) {
    let mut g = loop {
        match LAST_STABLE_JOINT.try_lock() {
            None => error!("failed to lock last stable ball"),
            Some(g) => break g,
        }
    };

    g.update(Some(joint));
}
