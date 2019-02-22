use std::collections::VecDeque;

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

        let mut best_joint: Option<RcuReader<JointData>> = None;

        while let Ok(joint) = rx.recv() {
            if let Some(old_joint) = best_joint.take() {
                if !joint.is_precedence_than(&old_joint) {
                    continue;
                }
            }
            best_joint = Some(joint.clone());

            if joint.get_min_wl() > last_stable_level {
                info!(
                    "main chain worker get a valid joint, unit={}",
                    joint.unit.unit
                );
                last_stable_level = t_c!(update_main_chain(joint));
            }
        }
        error!("main chain worker stopped!");
        ::std::process::abort();
    })
}

fn update_main_chain(joint: RcuReader<JointData>) -> Result<Level> {
    let mc_joints = build_unstable_main_chain_from_joint(joint)?;

    let stable_mci = mc_joints[mc_joints.len() - 1].get_mci();
    if Level::ZERO < stable_mci && stable_mci < get_last_stable_mci() {
        error!("your chain is diversing!!!!, stable_level={:?}", stable_mci);
        ::std::process::abort();
    }

    update_stable_main_chain(mc_joints)
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

// param: end is the view point joint
fn calc_max_alt_level(last_ball: &JointData, end: &RcuReader<JointData>) -> Result<Level> {
    let stable_point = last_ball.get_best_parent().read()?;
    let stable_point_level = stable_point.get_level();

    //Alternative roots are last stable mc joint's best children
    //but not on current main chain
    let mut alternative_branch = Vec::new();
    for child in stable_point.children.iter() {
        let child = &*child;
        let child_data = child.read()?;

        if child_data.get_best_parent().key.as_str() == stable_point.unit.unit
            && child_data.unit.unit != last_ball.unit.unit
        {
            alternative_branch.push(child.clone().read()?);
        }
    }

    let end_level = end.get_level();
    let mut alt_candidates = Vec::new();
    let mut max_alt_level = stable_point_level;
    let mut min_alt_level = end_level;

    //Go down to collect best children
    //Best children should never intersect, no need to check revisit
    while let Some(joint_data) = alternative_branch.pop() {
        // End joint would never include this joint, done
        if joint_data.get_level() >= end_level {
            continue;
        }

        if joint_data.is_wl_increased() {
            alt_candidates.push(joint_data.clone());
            let level = joint_data.get_level();
            if level > max_alt_level {
                max_alt_level = level;
            }
            if level < min_alt_level {
                min_alt_level = level;
            }
        }

        for child in joint_data.children.iter() {
            let child_data = child.read()?;

            if child_data.get_best_parent().key.as_str() == joint_data.unit.unit {
                alternative_branch.push(child_data);
            }
        }
    }

    // Fast return if min_wl is already greater than max_alt_level_possible
    // this max_alt_level_possible is not the real max_alt_level
    // but it's fine to return since it can't affect the stable result
    let min_wl = end.get_min_wl();
    if min_wl > max_alt_level {
        return Ok(max_alt_level);
    }

    // Limit the max_alt_level to the history in end joint's perspective
    let mut joints = VecDeque::new();
    let mut visited = HashSet::new();
    let to_min_wl = if min_wl < min_alt_level {
        min_alt_level
    } else {
        min_wl
    };

    joints.push_back(end.clone());
    while let Some(joint) = joints.pop_front() {
        let joint_level = joint.get_level();
        if joint_level < to_min_wl {
            continue;
        }

        // find a candidate level that is bigger than min_wl
        if alt_candidates.contains(&joint) {
            return Ok(joint_level);
        }

        for parent in joint.parents.iter() {
            let parent_data = parent.read()?;
            if visited.insert(parent.key.clone()) {
                joints.push_back(parent_data);
            }
        }
    }

    // we didn't find any valid alt level, return the default one
    Ok(stable_point_level)
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
fn update_stable_main_chain_to_joint(
    mut stable_joint: RcuReader<JointData>,
    unstable_mc_joints: &mut Vec<RcuReader<JointData>>,
) -> Result<RcuReader<JointData>> {
    let last_ball_joint = unstable_mc_joints[0].get_last_ball_joint()?;

    let mut stable_level = stable_joint.get_level();
    let last_ball_level = last_ball_joint.get_level();
    while last_ball_level > stable_level {
        let joint = match unstable_mc_joints.pop() {
            None => break,
            Some(joint) => joint,
        };

        stable_level += 1;
        mark_main_chain_joint_stable(&joint, stable_level)?;
        stable_joint = joint;
    }

    Ok(stable_joint)
}

fn update_stable_main_chain(mut unstable_mc_joints: Vec<RcuReader<JointData>>) -> Result<Level> {
    let mut stable_joint = unstable_mc_joints.pop().expect("no stable joint found!");
    let end_joint = unstable_mc_joints[0].clone();

    // directly update to longest last ball unit since we have already verified
    stable_joint = update_stable_main_chain_to_joint(stable_joint, &mut unstable_mc_joints)?;

    // find valid end points in order
    let mut last_stable_level = stable_joint.get_level();
    let min_wl = end_joint.get_min_wl();

    // forward main chain in order
    while let Some(unstable_mc_joint) = unstable_mc_joints.pop() {
        //Alternative roots are last stable mc joint's best children but not on current main chain
        if min_wl >= unstable_mc_joint.get_level()
            && min_wl > calc_max_alt_level(&unstable_mc_joint, &end_joint)?
        {
            mark_main_chain_joint_stable(&unstable_mc_joint, stable_joint.get_mci() + 1)?;
            stable_joint = unstable_mc_joint;
            last_stable_level = stable_joint.get_level();
        } else {
            unstable_mc_joints.push(unstable_mc_joint);
            break;
        }
    }

    Ok(last_stable_level)
}

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
            error!(
                "is_stable_to_joint return false, last_ball {} is not on main chain and before stable point",
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
            error!(
                "is_stable_to_joint return false, unit={}, last_ball_unit={} can't lead to last stable unit",
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
            error!(
                "is_stable_to_joint return false, last_ball {} is not on main chain, can't pass to stable point {}",
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
            error!(
                "is_stable_to_joint return false, unit={}, last_ball_unit={} can't lead to last ball unit",
                joint.unit.unit, last_ball.unit.unit
            );
            return Ok(false);
        }
    }

    Ok(true)
}

// if any joint along main chain that is relative stable to stable_joint
// this is somewhat like calc_max_alt_level
// but avoid calc alt branch multiple times
fn is_stable_to_joints(
    last_ball: &RcuReader<JointData>,
    joint: &RcuReader<JointData>,
) -> Result<bool> {
    let stable_point = last_ball.get_best_parent().read()?;
    let stable_point_level = stable_point.get_level();

    //Alternative roots are last stable mc joint's best children
    //but not on current main chain
    let mut alternative_branch = Vec::new();
    for child in stable_point.children.iter() {
        let child = &*child;
        let child_data = child.read()?;

        if child_data.get_best_parent().key.as_str() == stable_point.unit.unit
            && child_data.unit.unit != last_ball.unit.unit
        {
            alternative_branch.push(child.clone().read()?);
        }
    }

    let end_level = joint.get_level();
    let mut alt_candidates = Vec::new();
    let mut max_alt_level = stable_point_level;
    let mut min_alt_level = end_level;

    //Go down to collect best children
    //Best children should never intersect, no need to check revisit
    while let Some(joint_data) = alternative_branch.pop() {
        // End joint would never include this joint, done
        if joint_data.get_level() >= end_level {
            continue;
        }

        if joint_data.is_wl_increased() {
            alt_candidates.push(joint_data.clone());
            let level = joint_data.get_level();
            if level > max_alt_level {
                max_alt_level = level;
            }
            if level < min_alt_level {
                min_alt_level = level;
            }
        }

        for child in joint_data.children.iter() {
            let child_data = child.read()?;

            if child_data.get_best_parent().key.as_str() == joint_data.unit.unit {
                alternative_branch.push(child_data);
            }
        }
    }

    if joint.get_min_wl() > max_alt_level {
        return Ok(true);
    }

    // search along the best parent path
    // test those unit whoes min_wl increased
    let mut end_joint = joint.clone();
    loop {
        let min_wl = end_joint.get_min_wl();
        if min_wl <= stable_point_level {
            break;
        }

        if end_joint.is_min_wl_increased() {
            let to_min_level = if min_wl < min_alt_level {
                min_alt_level
            } else {
                min_wl
            };
            if is_stable_to_end_joint(to_min_level, &end_joint, &alt_candidates)? {
                return Ok(true);
            }
        }

        end_joint = end_joint.get_best_parent().read()?;
    }

    fn is_stable_to_end_joint(
        to_min_level: Level,
        end_joint: &RcuReader<JointData>,
        alt_candidates: &[RcuReader<JointData>],
    ) -> Result<bool> {
        // Limit the max_alt_level to the history in end joint's perspective
        let mut joints = VecDeque::new();
        let mut visited = HashSet::new();

        joints.push_back(end_joint.clone());
        while let Some(joint) = joints.pop_front() {
            let joint_level = joint.get_level();
            if joint_level < to_min_level {
                continue;
            }

            // find a candidate level that is bigger than to_min_level
            // continue to check with the next end joint
            if alt_candidates.contains(&joint) {
                return Ok(false);
            }

            for parent in joint.parents.iter() {
                let parent_data = parent.read()?;
                if visited.insert(parent.key.clone()) {
                    joints.push_back(parent_data);
                }
            }
        }

        // No alt candidates found, it is stable
        Ok(true)
    }

    error!("not stable exhausted!!");
    Ok(false)
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
        error!(
            "is_stable_to_joint return false, min_wl={:?}, last_ball_level={:?}",
            min_wl, last_ball_level
        );
        return Ok(false);
    }

    if !is_best_ancestor(last_ball, joint)? {
        return Ok(false);
    }

    is_stable_to_joints(last_ball, joint)
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

    if ::kv_store::KV_STORE.save_last_mci(joint.get_mci()).is_err() {
        error!("Save last mci to kv failed");
    }

    g.update(Some(joint));
}
