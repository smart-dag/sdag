use std::collections::HashSet;
use std::collections::VecDeque;

use cache::{CachedJoint, SDagCache, SDAG_CACHE};
use error::Result;
use failure::ResultExt;
use joint::Level;
use may::coroutine::JoinHandle;
use may::sync::{mpsc, RwLock};

lazy_static! {
    pub static ref MAIN_CHAIN_WORKER: MainChainWorker = MainChainWorker::default();
    static ref LAST_STABLE_JOINT: RwLock<CachedJoint> =
        RwLock::new(calc_last_stable_joint(&SDAG_CACHE).expect("failed to read last stable joint"));
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
    tx: mpsc::Sender<CachedJoint>,
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
    pub fn push_ready_joint(&self, joint: CachedJoint) -> Result<()> {
        self.tx.send(joint)?;
        Ok(())
    }
}

fn start_main_chain_worker(rx: mpsc::Receiver<CachedJoint>) -> JoinHandle<()> {
    go!(move || {
        // init it as -1 then the genesis min_wl = 0 can go forward
        let mut last_stable_level = LAST_STABLE_JOINT
            .read()
            .unwrap()
            .read()
            .map(|j| j.get_level())
            .unwrap_or(Level::minimum());

        info!(
            "main chain worker started, last_stable_level = {:?}",
            last_stable_level
        );

        while let Ok(joint) = rx.recv() {
            let joint_data = t_c!(joint.read());
            let min_wl = joint_data.get_min_wl();

            info!("main chain worker get a new joint, min_wl = {:?}", min_wl);
            if min_wl > last_stable_level {
                last_stable_level = t_c!(update_main_chain(joint));
            }
        }
        error!("main chain worker stopped!");
        ::std::process::abort();
    })
}

fn update_main_chain(joint: CachedJoint) -> Result<Level> {
    let joint_data = joint.read()?;
    let stable_level;

    if joint_data.unit.is_genesis_unit() {
        stable_level = Level::zero();
        mark_main_chain_joint_stable(joint, stable_level)?;
    } else {
        let (last_stable_mc_joint, unstable_mc) = build_unstable_main_chain_from_joint(joint)?;
        stable_level = update_stable_main_chain(last_stable_mc_joint, unstable_mc)?;
    }

    Ok(stable_level)
}

fn build_unstable_main_chain_from_joint(
    joint: CachedJoint,
) -> Result<(CachedJoint, Vec<CachedJoint>)> {
    let mut unstable_main_chain = Vec::new();
    let last_stable_joint;

    let mut joint = joint;
    loop {
        let joint_data = joint.read()?;

        if joint_data.is_on_main_chain() {
            last_stable_joint = joint.clone();
            break;
        }

        unstable_main_chain.push(joint.clone());

        joint = joint_data.get_best_parent();
    }

    Ok((last_stable_joint, unstable_main_chain))
}

fn calc_max_alt_level(alternative_roots: Vec<CachedJoint>) -> Result<Option<Level>> {
    let mut max_alt_level = None;
    let mut joints = alternative_roots;

    //Go down to collect best children
    //Best children should never intersect, no need to check revisit
    while let Some(joint) = joints.pop() {
        let joint_data = joint.read()?;

        if joint_data.is_wl_increased() {
            let level = joint_data.get_level();
            if level > max_alt_level.unwrap_or(Level::zero()) {
                max_alt_level = Some(level);
            }
        }

        for child in joint_data.children.iter() {
            let child = &*child;
            let child_data = child.read()?;

            if child_data.get_best_parent() == joint {
                joints.push(child.clone());
            }
        }
    }

    Ok(max_alt_level)
}

fn mark_main_chain_joint_stable(joint: CachedJoint, mci: Level) -> Result<()> {
    let main_chain_joint_data = joint.read()?;
    main_chain_joint_data.set_limci(mci);

    let mut joints = VecDeque::new();
    let mut sorted = Vec::new();
    joints.push_back(joint.clone());

    while let Some(joint) = joints.pop_front() {
        let joint_data = joint.read()?;

        //Mci has been already set but not stable yet, still waiting for balls
        if joint_data.get_mci().is_valid() {
            continue;
        }

        sorted.push(joint.clone());

        // parent_units is ordered, joint.parents is not ordered
        for parent_unit in &joint_data.unit.parent_units {
            joints.push_back(SDAG_CACHE.get_joint(parent_unit)?);
        }
    }

    let mut visited = HashSet::new();
    let mut sub_mci = Level::zero();

    while let Some(joint) = sorted.pop() {
        //Ignore the second visit to keep the order right
        if !visited.contains(&joint.key) {
            visited.insert(joint.key.clone());
        } else {
            continue;
        }

        let joint_data = joint.read()?;

        joint_data.set_sub_mci(sub_mci);
        sub_mci += 1;

        //limci on main chain joint is already set, do not overwrite it
        if joint_data.get_limci().is_none() {
            let mut limci = Level::zero();
            for parent in joint_data.parents.iter() {
                let parent_data = parent.read()?;

                //limci
                let parent_limci = parent_data.get_limci();
                if parent_limci > limci {
                    limci = parent_limci;
                }
            }
            joint_data.set_limci(limci);
        }

        joint_data.set_mci(mci);

        if joint_data.is_on_main_chain() {
            SDAG_CACHE.set_mc_unit_hash(mci, joint_data.unit.unit.clone())?;
        }

        ::business::BUSINESS_WORKER.push_stable_joint(joint)?;
    }

    info!(
        "main chain update: last_stable_joint = {:?}",
        main_chain_joint_data.get_props()
    );
    *LAST_STABLE_JOINT.write().unwrap() = joint;

    Ok(())
}

fn update_stable_main_chain(
    last_stable_main_chain_joint: CachedJoint,
    mut unstable_main_chain: Vec<CachedJoint>,
) -> Result<Level> {
    ensure!(!unstable_main_chain.is_empty(), "Empty unstable main chain");

    let mut last_stable_mc_joint = last_stable_main_chain_joint;
    let mut last_stable_level = last_stable_mc_joint.read()?.get_level();

    let min_wl = unstable_main_chain.first().unwrap().read()?.get_min_wl();

    while let Some(first_unstable_mc_joint) = unstable_main_chain.pop() {
        let last_stable_mc_joint_data = last_stable_mc_joint.read()?;
        let first_unstable_mc_joint_data = first_unstable_mc_joint.read()?;
        let first_unstable_mc_level = first_unstable_mc_joint_data.get_level();

        //Alternative roots are last stable mc joint's best children
        //but not on current main chain
        let mut alternative_roots = Vec::new();
        for child in last_stable_mc_joint_data.children.iter() {
            let child = &*child;
            let child_data = child.read()?;

            if child_data.get_best_parent() == last_stable_mc_joint
                && child != &first_unstable_mc_joint
            {
                alternative_roots.push(child.clone());
            }
        }
        let max_alt_level = calc_max_alt_level(alternative_roots)?;

        let stable = min_wl > max_alt_level.unwrap_or(last_stable_level);

        if stable {
            last_stable_level = first_unstable_mc_level;
            last_stable_mc_joint = first_unstable_mc_joint.clone();
            mark_main_chain_joint_stable(
                first_unstable_mc_joint,
                last_stable_mc_joint_data.get_mci() + 1,
            )?;
        } else {
            break;
        }
    }

    Ok(last_stable_level)
}

fn calc_last_stable_joint(cache: &SDagCache) -> Result<CachedJoint> {
    let free_joints = cache.get_free_joints()?;

    if free_joints.is_empty() {
        // here we create a fake joint
        let joint = CachedJoint {
            key: Default::default(),
            data: Default::default(),
        };
        return Ok(joint);
    }

    //Any free joint should connect to stable main chain
    let mut joint = free_joints.into_iter().nth(0).unwrap();

    //Go up with best parent to reach the stable main chain
    loop {
        let joint_data = joint.read()?;

        if joint_data.is_on_main_chain() {
            break;
        }

        joint = joint_data.get_best_parent();
    }

    //Go down to find the last one
    loop {
        let joint_data = joint.read()?;

        let mut child_on_mc = None;
        for child in joint_data.children.iter() {
            let child = &*child;
            let child_data = child.read()?;

            if child_data.is_on_main_chain() {
                child_on_mc = Some(child.clone());
                break;
            }
        }

        if let Some(child) = child_on_mc {
            joint = child;
        } else {
            return Ok(joint);
        }
    }
}

fn calc_max_alt_level_included_by_later_joints(
    alternative_roots: Vec<CachedJoint>,
    later_joints: &[CachedJoint],
) -> Result<Option<Level>> {
    let mut max_alt_level = None;
    let mut joints = alternative_roots;

    //Go down to collect best children
    //Best children should never intersect, no need to check revisit
    while let Some(joint) = joints.pop() {
        let joint_data = joint.read()?;

        if !joint_data.get_props().is_ancestor(later_joints.iter())? {
            continue;
        }

        if joint_data.is_wl_increased() {
            let level = joint_data.get_level();
            if level > max_alt_level.unwrap_or(Level::minimum()) {
                max_alt_level = Some(level);
            }
        }

        for child in joint_data.children.iter() {
            let child = &*child;
            let child_data = child.read()?;

            if child_data.get_best_parent() == joint {
                joints.push(child.clone());
            }
        }
    }

    Ok(max_alt_level)
}

fn calc_min_wl_included_by_later_joints(
    first_unstable_joint: &CachedJoint,
    later_joints: &[CachedJoint],
) -> Result<Level> {
    let mut joints = vec![first_unstable_joint.clone()];
    let mut witness_joints = Vec::new();

    //Go down to collect best children
    //Best children should never intersect, no need to check revisit
    while let Some(joint) = joints.pop() {
        let joint_data = joint.read()?;

        if !joint_data.get_props().is_ancestor(later_joints.iter())? {
            continue;
        }

        for child in joint_data.children.iter() {
            let child = &*child;
            let child_data = child.read()?;

            if child_data.get_best_parent() == joint {
                joints.push(child.clone());
            }
        }

        //Collect witness joints
        if joint_data.is_authored_by_witness() {
            witness_joints.push(joint_data);
        }
    }

    witness_joints.sort_by_key(|j| j.get_wl().value());

    let mut collected_witnesses = HashSet::new();
    while let Some(joint_data) = witness_joints.pop() {
        for author in &joint_data.unit.authors {
            if collected_witnesses.contains(&author.address) {
                continue;
            }

            collected_witnesses.insert(author.address.clone());

            if collected_witnesses.len() >= ::config::MAJORITY_OF_WITNESSES {
                return Ok(joint_data.get_wl());
            }
        }
    }

    Ok(Level::minimum())
}

//---------------------------------------------------------------------------------------
// pub APIs
//---------------------------------------------------------------------------------------

/// find the best joint among set of joints
/// Sort by max(wl), min(level), min(unit_hash)
pub fn find_best_joint<'a, I: IntoIterator<Item = &'a CachedJoint>>(
    iter: I,
) -> Result<Option<CachedJoint>> {
    let mut p_it = iter.into_iter();
    // at least one parent
    let mut best_joint = match p_it.next() {
        None => return Ok(None),
        Some(p) => p,
    };

    for joint in p_it {
        let best_j = best_joint.read().context("find best joint")?;
        let j = joint.read().context("find_best_joint")?;

        if j.is_precedence_than(&*best_j) {
            best_joint = joint;
        }
    }

    Ok(Some(best_joint.clone()))
}

/// judge if earlier_joint is relative stable to later_joints
pub fn is_stable_in_later_joints(
    earlier_joint: &CachedJoint,
    later_joints: &[CachedJoint],
) -> Result<bool> {
    let earlier_joint_data = earlier_joint.read()?;

    //Genesis
    if earlier_joint_data.unit.is_genesis_unit() {
        return Ok(true);
    }

    //Free joint
    if earlier_joint_data.is_free() {
        return Ok(false);
    }

    let best_parent = earlier_joint_data.get_best_parent().read()?;

    let mut alt_branches_roots = Vec::new();
    for child in best_parent.children.iter() {
        let child = &*child;
        if child.key == earlier_joint.key {
            continue;
        }

        if child.read()?.get_props().is_ancestor(later_joints.iter())? {
            alt_branches_roots.push(child.clone());
        }
    }
    let max_alt_level =
        calc_max_alt_level_included_by_later_joints(alt_branches_roots, &later_joints)?;

    let min_wl = calc_min_wl_included_by_later_joints(earlier_joint, &later_joints)?;

    let stable = min_wl > max_alt_level.unwrap_or(best_parent.get_level());

    Ok(stable)
}

/// Returns current unstable main chain from the best free joint
pub fn build_unstable_main_chain() -> Result<Vec<CachedJoint>> {
    let free_joints = SDAG_CACHE.get_free_joints()?;
    if let Some(main_chain_free_joint) = find_best_joint(free_joints.iter())? {
        let (_, unstable_main_chain) = build_unstable_main_chain_from_joint(main_chain_free_joint)?;
        Ok(unstable_main_chain)
    } else {
        Ok(Vec::new())
    }
}

/// get stable point mci
pub fn get_last_stable_mci() -> Level {
    get_last_stable_joint()
        .read()
        .map(|j| j.get_mci())
        .unwrap_or(Level::zero())
}

/// get the stable point joint
pub fn get_last_stable_joint() -> CachedJoint {
    LAST_STABLE_JOINT.read().unwrap().clone()
}
