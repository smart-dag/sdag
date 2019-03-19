use cache::{CachedJoint, JointData, SDAG_CACHE};
use error::Result;
use joint::JointSequence;
use may::coroutine::JoinHandle;
use may::sync::mpsc;
use statistics::final_joints_increase;

lazy_static! {
    pub static ref FINALIZATION_WORKER: FinalizationWorker = FinalizationWorker::default();
}

//---------------------------------------------------------------------------------------
// FinalizationWorker
//---------------------------------------------------------------------------------------
pub struct FinalizationWorker {
    tx: mpsc::Sender<CachedJoint>,
    _handler: JoinHandle<()>,
}

impl Default for FinalizationWorker {
    fn default() -> Self {
        let (tx, rx) = mpsc::channel();

        let _handler = start_finalization_worker(rx);

        FinalizationWorker { tx, _handler }
    }
}

impl FinalizationWorker {
    // the main chain logic would call this API to push stable joint in order
    pub fn push_final_joint(&self, joint: CachedJoint) -> Result<()> {
        self.tx.send(joint)?;
        Ok(())
    }
}

// this would start the global thread to process the final joints
fn start_finalization_worker(rx: mpsc::Receiver<CachedJoint>) -> JoinHandle<()> {
    go!(move || {
        while let Ok(joint) = rx.recv() {
            t_c!(finalize_joint(joint));
            final_joints_increase();
        }
        error!("Finalization worker stopped!");
        ::std::process::abort();
    })
}

fn finalize_joint(cached_joint: CachedJoint) -> Result<()> {
    info!("finalize_joint, unit={}", cached_joint.key);
    let joint_data = cached_joint.read()?;

    let skiplist_units = calc_skiplist(&joint_data)?;

    let ball = calc_ball(&joint_data, &skiplist_units)?;
    SDAG_CACHE.set_ball_unit_hash(ball.clone(), joint_data.unit.unit.clone())?;
    SDAG_CACHE.del_hash_tree_ball(&ball);

    joint_data.update_ball(ball);
    joint_data.update_skiplist(skiplist_units);

    // clear the message content if it has no commission payed
    if joint_data.get_sequence() == JointSequence::NoCommission
        && joint_data.unit.content_hash.is_none()
    {
        let content_hash = joint_data.unit.get_unit_content_hash();
        joint_data.clear_content(content_hash);
    }

    joint_data.set_stable();
    if joint_data.is_on_main_chain() {
        ::main_chain::set_last_stable_joint(joint_data.clone());
    }

    cached_joint.save_to_db_async()?;

    Ok(())
}

fn calc_ball(joint_data: &JointData, skiplist: &[String]) -> Result<(String)> {
    use sdag_object_base::object_hash;
    let unit = &joint_data.unit.unit;

    //Parent balls
    let mut parent_balls = Vec::new();
    for parent in joint_data.parents.iter() {
        let parent_data = parent.read()?;

        if let Some(parent_ball) = &parent_data.ball {
            parent_balls.push(parent_ball.clone());
        } else {
            bail!(
                "no ball for unit {} in parents of unit {}",
                parent_data.unit.unit,
                unit
            );
        }
    }

    //Skip list balls
    let mut skiplist_balls = Vec::new();
    for skiplist_unit in skiplist.iter() {
        let joint = SDAG_CACHE.get_joint(&skiplist_unit)?;
        let skiplist_ball = &joint.read()?.ball;

        if let Some(skiplist_ball) = skiplist_ball {
            skiplist_balls.push(skiplist_ball.clone());
        } else {
            bail!("no ball for unit {} in skiplist of {}", skiplist_unit, unit);
        }
    }

    //Calculate ball
    parent_balls.sort();
    skiplist_balls.sort();
    let ball = object_hash::calc_ball_hash(
        unit,
        &parent_balls,
        &skiplist_balls,
        joint_data.get_sequence() != JointSequence::Good,
    );

    if let Some(stored_ball) = &joint_data.ball {
        // we should not bail out here, just use a warning should be fine
        // any way we should save the correct ball value
        if stored_ball != &ball {
            error!(
                "stored and calculated ball hashes do not match, stored_ball={} calc_ball={} unit={}",
                stored_ball, ball, unit
            );
        }
    }

    Ok(ball)
}

fn get_similar_mcis(mci: usize) -> Vec<usize> {
    let mut similar_mcis = Vec::new();
    let mut devisor = 10;
    loop {
        if mci % devisor != 0 || mci == 0 {
            return similar_mcis;
        } else {
            similar_mcis.push(mci - devisor);
            devisor *= 10;
        }
    }
}

fn calc_skiplist(joint_data: &JointData) -> Result<Vec<String>> {
    let mut skiplist = Vec::new();

    if !joint_data.is_on_main_chain() {
        return Ok(skiplist);
    }

    let mci = joint_data.get_mci().value();

    let target_mcis = get_similar_mcis(mci);

    for target_mci in target_mcis.into_iter() {
        let skiplist_unit = SDAG_CACHE
            .get_mc_unit_hash(target_mci.into())?
            .ok_or_else(|| format_err!("no unit hash for mci {}", target_mci))?;
        skiplist.push(skiplist_unit);
    }
    skiplist.sort();

    Ok(skiplist)
}
