use cache::SDAG_CACHE;
use error::Result;
use joint::{Joint, JointSequence};
use main_chain;
use witness_proof;

#[derive(Serialize, Deserialize)]
pub struct CatchupReq {
    last_stable_mci: usize,
    last_known_mci: usize,
    witnesses: Vec<String>,
}

#[derive(Serialize, Deserialize)]
pub struct CatchupChain {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    pub unstable_mc_joints: Vec<Joint>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    pub stable_last_ball_joints: Vec<Joint>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    pub witness_change_and_definition_joints: Vec<Joint>,
}

pub fn prepare_catchup_chain(catchup_req: CatchupReq) -> Result<CatchupChain> {
    let CatchupReq {
        last_stable_mci,
        last_known_mci,
        witnesses,
    } = catchup_req;

    let mut stable_last_ball_joints = Vec::new();

    ensure!(
        last_stable_mci <= last_known_mci,
        "CatchupReq last_stable_mci={} > last_known_mci={}",
        last_stable_mci,
        last_known_mci,
    );
    ensure!(witnesses.len() == 12, "invalid witness list");

    if SDAG_CACHE
        .get_mc_unit_hash(last_stable_mci.into())?
        .is_none()
    {
        return Ok(CatchupChain {
            // already current
            status: Some("current".to_owned()),
            unstable_mc_joints: Vec::new(),
            stable_last_ball_joints: Vec::new(),
            witness_change_and_definition_joints: Vec::new(),
        });
    }

    let witness_proof = witness_proof::prepare_witness_proof(&witnesses, last_stable_mci)?;
    let mut last_ball_unit = witness_proof.last_ball_unit;

    loop {
        let joint = SDAG_CACHE.get_joint(&last_ball_unit)?.read()?;
        stable_last_ball_joints.push((**joint).clone());

        if joint.get_mci().value() <= last_stable_mci {
            break;
        }

        // goup
        last_ball_unit = joint
            .unit
            .last_ball_unit
            .clone()
            .expect("missing last ball unit for unit");
    }

    Ok(CatchupChain {
        status: None,
        stable_last_ball_joints,
        unstable_mc_joints: witness_proof.unstable_mc_joints,
        witness_change_and_definition_joints: witness_proof.witness_change_and_definition,
    })
}

pub fn process_catchup_chain(catchup_chain: CatchupChain) -> Result<Vec<String>> {
    if let Some(s) = catchup_chain.status {
        if s.as_str() == "current" {
            return Ok(Vec::new());
        }
    }

    ensure!(
        !catchup_chain.stable_last_ball_joints.is_empty(),
        "stable_last_ball_joints is empty"
    );

    let witness_proof::ProcessWitnessProof {
        last_ball_units,
        assoc_last_ball_by_last_ball_unit,
    } = witness_proof::process_witness_proof(
        &catchup_chain.unstable_mc_joints,
        &catchup_chain.witness_change_and_definition_joints,
        true,
    )?;

    let first_stable_joint = &catchup_chain.stable_last_ball_joints[0];

    let mut last_ball_unit = &first_stable_joint.unit.unit;
    ensure!(
        last_ball_units.contains(last_ball_unit),
        "first stable unit is not last ball unit of any unstable unit"
    );

    let mut last_ball = &assoc_last_ball_by_last_ball_unit[last_ball_unit];
    ensure!(
        first_stable_joint.ball.as_ref() == Some(last_ball),
        "last ball and last ball unit do not match"
    );

    let mut chain_balls = Vec::<String>::new();
    for joint in &catchup_chain.stable_last_ball_joints {
        ensure!(joint.ball.is_some(), "stable but no ball");
        ensure!(joint.unit.has_valid_hashes(), "invalid hash");
        ensure!(&joint.unit.unit == last_ball_unit, "not the last ball unit");
        ensure!(joint.ball.as_ref() == Some(last_ball), "not the last ball");

        let unit = &joint.unit;

        // genesis has no last ball unit and last ball
        if let Some(ref lbu) = unit.last_ball_unit {
            last_ball = unit.last_ball.as_ref().expect("missing last ball");
            last_ball_unit = lbu;
        }

        chain_balls.push(joint.ball.as_ref().unwrap().clone());
    }

    // adjust first chain ball if necessary and make sure it is the only stable unit in the entire chain
    || -> Result<()> {
        let len = chain_balls.len();
        ensure!(len >= 1, "chain_balls length is not bigger enough");

        let joint = match SDAG_CACHE.get_ball_unit_hash(&chain_balls[len - 1])? {
            Some(unit) => SDAG_CACHE.get_joint(&unit)?.read()?,
            None => bail!("first chain ball {} is not known", chain_balls[len - 1]),
        };

        ensure!(
            joint.is_stable(),
            "first chain ball {} is not stable",
            chain_balls[len - 1]
        );
        ensure!(
            joint.is_on_main_chain(),
            "first chain ball {} is not on mc",
            chain_balls[len - 1]
        );
        let main_chain_index = joint.get_mci();

        let last_stable_mci = main_chain::get_last_stable_mci();

        if main_chain_index > last_stable_mci {
            bail!("first chain ball {} mci is too large", chain_balls[len - 1]);
        }

        if last_stable_mci == main_chain_index {
            return Ok(());
        }

        // replace to avoid receiving duplicates, no need to catchup already knowns
        chain_balls[len - 1] = main_chain::get_last_stable_joint()
            .read()?
            .ball
            .clone()
            .ok_or_else(|| format_err!("no ball found for last_stable_joint"))?;

        // check the second ball
        if len == 1 {
            return Ok(());
        }

        let second_ball_joint = match SDAG_CACHE.get_ball_unit_hash(&chain_balls[len - 2])? {
            None => return Ok(()),
            Some(unit) => SDAG_CACHE.get_joint(&unit)?.read()?,
        };

        ensure!(
            !second_ball_joint.is_stable(),
            "second chain ball {} must not be stable",
            chain_balls[len - 2]
        );
        Ok(())
    }()?;

    // validation complete, now write the chain for future downloading of hash trees
    Ok(chain_balls)
}

#[derive(Serialize, Deserialize)]
pub struct HashTreeReq {
    pub from_ball: String,
    pub to_ball: String,
}

#[derive(Serialize, Deserialize)]
pub struct BallProps {
    pub unit: String,
    pub ball: String,
    #[serde(default)]
    is_nonserial: bool,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    parent_balls: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    skiplist_balls: Vec<String>,
}

pub fn prepare_hash_tree(hash_tree_req: HashTreeReq) -> Result<Vec<BallProps>> {
    let HashTreeReq { from_ball, to_ball } = hash_tree_req;

    // this is only for main chain balls
    let from_joint = match SDAG_CACHE.get_ball_unit_hash(&from_ball)? {
        Some(unit) => SDAG_CACHE.get_joint(&unit)?.read()?,
        None => bail!("from_ball {} is not known", from_ball),
    };

    let to_joint = match SDAG_CACHE.get_ball_unit_hash(&to_ball)? {
        Some(unit) => SDAG_CACHE.get_joint(&unit)?.read()?,
        None => bail!("to_ball {} is not known", from_ball),
    };

    let mut from_mci = from_joint.get_mci();
    let to_mci = to_joint.get_mci();
    ensure!(from_mci < to_mci, "from is after to");
    // no need to catchup the already known joints
    from_mci += 1;

    let mut balls = Vec::new();
    while from_mci <= to_mci {
        let joints = SDAG_CACHE.get_joints_by_mci(from_mci)?;
        for joint in joints {
            let joint_data = joint.read()?;

            let mut parent_balls = joint_data
                .parents
                .iter()
                .map(|p| {
                    Ok(p.read()?
                        .ball
                        .clone()
                        .ok_or_else(|| format_err!("no ball for parent joint"))?)
                })
                .collect::<Result<Vec<_>>>()?;
            parent_balls.sort();

            let mut skiplist_balls = joint_data
                .skiplist_units
                .iter()
                .map(|unit| {
                    Ok(SDAG_CACHE
                        .get_joint(unit)?
                        .read()?
                        .ball
                        .clone()
                        .ok_or_else(|| format_err!("no ball for skiplist joint"))?)
                })
                .collect::<Result<Vec<_>>>()?;
            skiplist_balls.sort();

            let unit = joint_data.unit.unit.clone();
            let ball = joint_data
                .ball
                .clone()
                .ok_or_else(|| format_err!("no ball for skiplist joint"))?;
            let is_nonserial = joint_data.get_sequence() != JointSequence::Good;

            balls.push(BallProps {
                unit,
                ball,
                is_nonserial,
                parent_balls,
                skiplist_balls,
            });
        }
        from_mci += 1;
    }

    Ok(balls)
}

pub fn process_hash_tree(balls: &[BallProps]) -> Result<()> {
    for ball_prop in balls {
        // skip the already known ones
        if SDAG_CACHE.get_ball_unit_hash(&ball_prop.ball)?.is_some() {
            continue;
        }

        let ball = ::object_hash::calc_ball_hash(
            &ball_prop.unit,
            &ball_prop.parent_balls,
            &ball_prop.skiplist_balls,
            ball_prop.is_nonserial,
        );

        if ball_prop.ball != ball {
            bail!(
                "wrong ball hash, ball {}, unit {}",
                ball_prop.unit,
                ball_prop.ball
            );
        }

        SDAG_CACHE.add_hash_tree_ball(ball, ball_prop.unit.clone());
    }

    Ok(())
}
