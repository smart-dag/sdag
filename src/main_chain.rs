use std::collections::VecDeque;

use cache::{CachedJoint, JointData, SDagCache, SDAG_CACHE};
use error::Result;
use failure::ResultExt;
use joint::Level;
use may::coroutine::JoinHandle;
use may::sync::mpsc;
use rcu_cell::RcuCell;

lazy_static! {
    pub static ref MAIN_CHAIN_WORKER: MainChainWorker = MainChainWorker::default();
    static ref LAST_STABLE_JOINT: RcuCell<CachedJoint> = RcuCell::new(Some(
        calc_last_stable_joint(&SDAG_CACHE).expect("failed to read last stable joint")
    ));
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
            .unwrap_or(Level::MINIMUM);

        info!(
            "main chain worker started, last_stable_level = {:?}",
            last_stable_level
        );

        let mut last_min_wl = Level::MINIMUM;

        while let Ok(joint) = rx.recv() {
            let joint_data = t_c!(joint.read());
            let min_wl = joint_data.get_min_wl();

            info!("main chain worker get a new joint, min_wl = {:?}", min_wl);
            if min_wl > last_min_wl {
                last_min_wl = min_wl;
                if min_wl > last_stable_level {
                    last_stable_level = t_c!(update_main_chain(joint));
                }
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
        stable_level = Level::ZERO;
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
    let mut joints = Vec::new();

    for j in alternative_roots {
        joints.push(j.read()?);
    }

    //Go down to collect best children
    //Best children should never intersect, no need to check revisit
    while let Some(joint_data) = joints.pop() {
        if joint_data.is_wl_increased() {
            let level = joint_data.get_level();
            if level > max_alt_level.unwrap_or(Level::MINIMUM) {
                max_alt_level = Some(level);
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

fn mark_main_chain_joint_stable(main_chain_joint: CachedJoint, mci: Level) -> Result<()> {
    use rcu_cell::RcuReader;

    let main_chain_joint_data = main_chain_joint.read()?;
    main_chain_joint_data.set_limci(mci);

    struct VisitedJoint {
        joint: CachedJoint,
        joint_data: RcuReader<JointData>,
    }

    impl PartialEq for VisitedJoint {
        fn eq(&self, other: &Self) -> bool {
            self.joint == other.joint
        }
    }

    let mut joints = VecDeque::new();
    let mut sorted = Vec::new();
    joints.push_back(main_chain_joint.clone());

    while let Some(joint) = joints.pop_front() {
        let joint_data = joint.read()?;

        let visit_joint = VisitedJoint { joint_data, joint };

        //Mci has been already set but not stable yet, still waiting for balls
        if sorted.contains(&visit_joint) || visit_joint.joint_data.get_mci().is_valid() {
            continue;
        }

        // parent_units is ordered, joint.parents is not ordered
        for parent in visit_joint.joint_data.parents.iter() {
            joints.push_back(parent.clone());
        }

        sorted.push(visit_joint);
    }

    // first sort by level, then unit hash
    sorted.sort_by(|a, b| {
        use std::cmp::Ordering;
        match PartialOrd::partial_cmp(&a.joint_data.get_level(), &b.joint_data.get_level()) {
            Some(Ordering::Equal) => Ord::cmp(&a.joint.key, &b.joint.key),
            Some(r) => r,
            None => unreachable!("invalid level cmp"),
        }
    });

    let mut sub_mci = Level::ZERO;
    for VisitedJoint { joint_data, joint } in sorted {
        // set sub_mci
        joint_data.set_sub_mci(sub_mci);
        sub_mci += 1;

        //limci on main chain joint is already set, do not overwrite it
        if !joint_data.get_limci().is_valid() {
            let mut limci = Level::ZERO;
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

        // set mci
        joint_data.set_mci(mci);

        // push it to the business logic
        ::business::BUSINESS_WORKER.push_stable_joint(joint)?;
    }

    // update the global property
    SDAG_CACHE.set_mc_unit_hash(mci, main_chain_joint.key.to_string())?;

    info!(
        "main chain update: last_stable_joint = {:?}",
        main_chain_joint_data.get_props()
    );

    ::utils::event::emit_event(MciStableEvent { mci });

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
    if min_wl <= level {
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

    Ok(is_ancestor)
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
        .unwrap_or(Level::ZERO)
}

/// get the stable point joint
pub fn get_last_stable_joint() -> CachedJoint {
    let joint = LAST_STABLE_JOINT.read().expect("no last stable joint");
    joint.as_ref().clone()
}

/// set the last stable joint
pub fn set_last_stable_joint(joint: &CachedJoint) {
    let mut g = loop {
        match LAST_STABLE_JOINT.try_lock() {
            None => error!("failed to lock last stable ball"),
            Some(g) => break g,
        }
    };
    g.update(Some(joint.clone()));
}
