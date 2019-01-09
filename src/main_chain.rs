use std::collections::VecDeque;

use cache::{CachedJoint, JointData, SDAG_CACHE};
use error::Result;
use failure::ResultExt;
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
                error!("init LAST_STABLE_JOINT, err={}", e);
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
        let mut last_stable_level = match LAST_STABLE_JOINT.read() {
            Some(j) => j.get_mci(),
            None => Level::MINIMUM,
        };

        info!(
            "main chain worker started, last_stable_level = {:?}",
            last_stable_level
        );

        let mut last_min_wl = Level::MINIMUM;

        while let Ok(joint) = rx.recv() {
            let min_wl = joint.get_min_wl();

            if min_wl > last_min_wl {
                last_min_wl = min_wl;
                if min_wl > last_stable_level {
                    info!(
                        "main chain worker get a valid joint, min_wl = {:?}, unit={}",
                        min_wl, joint.unit.unit
                    );
                    last_stable_level = t_c!(update_main_chain(joint));
                }
            }
        }
        error!("main chain worker stopped!");
        ::std::process::abort();
    })
}

fn update_main_chain(joint: RcuReader<JointData>) -> Result<Level> {
    let stable_level;

    if joint.unit.is_genesis_unit() {
        stable_level = Level::ZERO;
        mark_main_chain_joint_stable(&joint, stable_level)?;
    } else {
        let (stable_joint, unstable_mc_joints) = build_unstable_main_chain_from_joint(joint)?;
        stable_level = update_stable_main_chain(stable_joint, unstable_mc_joints)?;
    }

    Ok(stable_level)
}

fn build_unstable_main_chain_from_joint(
    mut joint: RcuReader<JointData>,
) -> Result<(RcuReader<JointData>, Vec<RcuReader<JointData>>)> {
    let mut unstable_mc_joints = Vec::new();

    while !joint.is_on_main_chain() {
        unstable_mc_joints.push(joint.clone());
        joint = joint.get_best_parent().read()?;
    }

    Ok((joint, unstable_mc_joints))
}

// fn get_max_alt_level(last_ball: &JointData, _best_parent: &JointData) -> Result<Level> {
//     let stable_point = last_ball.get_best_parent().read()?;
//     let max_alt_level = stable_point.get_level();
//     //Alternative roots are last stable mc joint's best children
//     //but not on current main chain
//     let mut alternative_roots = Vec::new();
//     for child in stable_point.children.iter() {
//         let child = &*child;
//         let child_data = child.read()?;

//         if child_data.get_best_parent().key.as_str() == stable_point.unit.unit
//             && child_data.unit.unit != last_ball.unit.unit
//         {
//             alternative_roots.push(child.clone());
//         }
//     }

//     Ok(max_alt_level)
// }

fn calc_max_alt_level(last_ball: &JointData, best_parent: &JointData) -> Result<Level> {
    let stable_point = last_ball.get_best_parent().read()?;
    let mut max_alt_level = stable_point.get_level();

    //Alternative roots are last stable mc joint's best children
    //but not on current main chain
    let mut alternative_roots = Vec::new();
    for child in stable_point.children.iter() {
        let child = &*child;
        let child_data = child.read()?;

        if child_data.get_best_parent().key.as_str() == stable_point.unit.unit
            && child_data.unit.unit != last_ball.unit.unit
        {
            alternative_roots.push(child.clone());
        }
    }

    let mut joints = Vec::new();
    for j in alternative_roots {
        joints.push(j.read()?);
    }

    //Go down to collect best children
    //Best children should never intersect, no need to check revisit
    while let Some(joint_data) = joints.pop() {
        let is_include = best_parent > &*joint_data;
        if !is_include {
            continue;
        }

        if joint_data.is_wl_increased() {
            let level = joint_data.get_level();
            if level > max_alt_level {
                max_alt_level = level;
            }
        }

        for child in joint_data.children.iter() {
            let child = &*child;
            let child_data = child.read()?;

            if child_data.get_best_parent().key.as_str() == joint_data.unit.unit {
                joints.push(child_data);
            }
        }
    }

    Ok(max_alt_level)
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

fn update_stable_main_chain(
    mut stable_joint: RcuReader<JointData>,
    mut unstable_mc_joints: Vec<RcuReader<JointData>>,
) -> Result<Level> {
    ensure!(!unstable_mc_joints.is_empty(), "Empty unstable main chain");

    let mut last_stable_level = stable_joint.get_level();

    let best_parent = unstable_mc_joints[0].clone();
    let min_wl = best_parent.get_min_wl();

    while let Some(unstable_mc_joint) = unstable_mc_joints.pop() {
        //Alternative roots are last stable mc joint's best children but not on current main chain
        let max_alt_level = calc_max_alt_level(&unstable_mc_joint, &best_parent)?;

        if min_wl > max_alt_level {
            mark_main_chain_joint_stable(&unstable_mc_joint, stable_joint.get_mci() + 1)?;
            stable_joint = unstable_mc_joint;
            last_stable_level = stable_joint.get_level();
        } else {
            break;
        }
    }

    Ok(last_stable_level)
}

fn calc_last_stable_joint() -> Result<RcuReader<JointData>> {
    let free_joints = SDAG_CACHE.get_free_joints()?;

    if free_joints.is_empty() {
        bail!("calc_last_stable_joint free_joints empty");
    }

    //Any free joint should connect to stable main chain
    let mut joint_data = free_joints.into_iter().nth(0).unwrap().read()?;

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
pub fn is_stable_to_joint(earlier_joint: &CachedJoint, joint: &JointData) -> Result<bool> {
    let earlier_joint_data = earlier_joint.read()?;
    let mut is_ancestor = false;
    let mut best_parent = joint.get_best_parent().read()?;

    if earlier_joint_data.unit.is_genesis_unit() {
        return Ok(true);
    }

    // min_wl must bigger that earlier unit level
    let min_wl = best_parent.get_min_wl();
    let level = earlier_joint_data.get_best_parent().read()?.get_level();
    // let max_alt_level = calc_max_alt_level(&earlier_joint_data, &best_parent)?;
    // if min_wl <= max_alt_level {
    if min_wl <= level {
        error!(
            "is_stable_to_joint return false, min_wl={:?}, max_alt_level={:?}",
            min_wl, level
        );
        return Ok(false);
    }

    // earlier unit must be ancestor of joint on main chain
    while best_parent.get_level() > level {
        if earlier_joint.key.as_str() == best_parent.unit.unit {
            is_ancestor = true;
            break;
        }
        best_parent = best_parent.get_best_parent().read()?;
    }

    if !is_ancestor {
        error!(
            "is_stable_to_joint return false, unit={}, last_ball_unit={} is not on main chain",
            joint.unit.unit, earlier_joint.key
        );
    }

    Ok(is_ancestor)
}

/// Returns current unstable main chain from the best free joint
pub fn build_unstable_main_chain() -> Result<Vec<RcuReader<JointData>>> {
    let free_joints = SDAG_CACHE.get_free_joints()?;
    match find_best_joint(free_joints.iter())? {
        Some(main_chain_free_joint) => {
            let (_, unsatable_mc_joints) =
                build_unstable_main_chain_from_joint(main_chain_free_joint)?;
            Ok(unsatable_mc_joints)
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
    let joint = LAST_STABLE_JOINT.read().expect("no last stable joint");
    joint.as_ref().clone()
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
