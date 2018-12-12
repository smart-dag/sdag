use std::collections::HashMap;

use cache::SDAG_CACHE;
use joint::{Joint, JointSequence};
use main_chain;
use my_witness::MY_WITNESSES;
use serde_json::Value;
use spec::Unit;
use {error::Result, SdagError};

pub struct PrepareWitnessProof {
    pub unstable_mc_joints: Vec<Joint>,
    pub witness_change_and_definition: Vec<Joint>,
    pub last_ball_unit: String,
    pub last_ball_mci: usize,
}

pub fn prepare_witness_proof(
    witnesses: &[String],
    last_stable_mci: usize,
) -> Result<PrepareWitnessProof> {
    // if storage::determine_if_witness_and_address_definition_have_refs(db, witnesses)? {
    //     return Err(SdagError::WitnessChanged.into());
    // }

    // collect all unstable MC units
    let unstable_mc_cached_joints = main_chain::build_unstable_main_chain()?;
    let mut unstable_mc_joints = Vec::new();

    // Find all last ball unit from unstable mc joints
    let mut found_witnesses = Vec::new();
    let mut last_ball_units = Vec::new();
    for joint in unstable_mc_cached_joints {
        let mut joint = (**joint.read()?).clone();

        // the unit might get stabilized while we were reading other units
        joint.ball = None;

        for author in &joint.unit.authors {
            let address = &author.address;
            if witnesses.contains(address) && !found_witnesses.contains(address) {
                found_witnesses.push(address.clone());
            }
        }

        if joint.unit.last_ball_unit.is_some()
            && found_witnesses.len() >= ::config::MAJORITY_OF_WITNESSES
        {
            let last_ball_unit = joint.unit.last_ball_unit.as_ref().unwrap().clone();
            let last_ball_mci = SDAG_CACHE
                .get_joint(&last_ball_unit)?
                .read()?
                .get_mci()
                .value();
            last_ball_units.push((last_ball_unit, last_ball_mci));
        }

        unstable_mc_joints.push(joint);
    }

    if last_ball_units.is_empty() {
        bail!("your witness list might be too much off, too few witness authored units");
    }

    // select the newest last ball unit
    last_ball_units.sort_by_key(|u| u.1);
    let (last_ball_unit, last_ball_mci) = last_ball_units.pop().unwrap();

    if last_stable_mci >= last_ball_mci {
        if last_stable_mci > 0 {
            return Err(SdagError::CatchupAlreadyCurrent.into());
        }
    }

    let witness_change_and_definition =
        prepare_witness_change_and_definition(witnesses, last_stable_mci)?;

    Ok(PrepareWitnessProof {
        unstable_mc_joints,
        witness_change_and_definition,
        last_ball_unit,
        last_ball_mci,
    })
}

///Read the witness definition after laster stable mci
fn prepare_witness_change_and_definition(
    witnesses: &[String],
    last_stable_mci: usize,
) -> Result<Vec<Joint>> {
    let mut witness_change_and_definition = Vec::new();

    //Definition change is not handled by now
    for witness in witnesses {
        let (unit, _) = SDAG_CACHE
            .get_definition(witness)
            .ok_or_else(|| format_err!("No definition found for witness {}", witness))?;

        let joint_data = SDAG_CACHE.get_joint(&unit)?.read()?;

        if joint_data.is_stable()
            && joint_data.get_sequence() == JointSequence::Good
            && joint_data.get_limci().value() >= last_stable_mci
        {
            witness_change_and_definition.push((**joint_data).clone());
        }
    }

    Ok(witness_change_and_definition)
}

#[derive(Debug)]
pub struct ProcessWitnessProof {
    pub last_ball_units: Vec<String>,
    pub assoc_last_ball_by_last_ball_unit: HashMap<String, String>,
}

pub fn process_witness_proof(
    unstable_mc_joints: &[Joint],
    witness_change_and_definition: &[Joint],
    from_current: bool,
) -> Result<ProcessWitnessProof> {
    let mut parent_units = Vec::new();
    let mut found_witnesses = Vec::new();
    let mut last_ball_units = Vec::new();
    let mut assoc_last_ball_by_last_ball_unit = HashMap::<String, String>::new();
    let mut witness_joints = Vec::new();

    for joint in unstable_mc_joints {
        let unit = &joint.unit;
        let unit_hash = &joint.unit.unit;
        ensure!(joint.ball.is_none(), "unstable mc but has ball");
        ensure!(joint.unit.has_valid_hashes(), "invalid hash");
        if !parent_units.is_empty() {
            ensure!(parent_units.contains(unit_hash), "not in parents");
        }

        let mut added_joint = false;
        for author in &unit.authors {
            let address = &author.address;
            if MY_WITNESSES.contains(address) {
                if !found_witnesses.contains(address) {
                    found_witnesses.push(address.clone());
                }
                if !added_joint {
                    witness_joints.push(joint);
                }
                added_joint = true;
            }
        }

        parent_units = unit.parent_units.clone();
        if unit.last_ball_unit.is_some() && found_witnesses.len() >= ::config::MAJORITY_OF_WITNESSES
        {
            let last_ball_unit = unit.last_ball_unit.as_ref().unwrap().clone();
            let last_ball = unit.last_ball.as_ref().unwrap().clone();
            last_ball_units.push(last_ball_unit.clone());
            assoc_last_ball_by_last_ball_unit.insert(last_ball_unit, last_ball);
        }
    }

    ensure!(
        found_witnesses.len() >= ::config::MAJORITY_OF_WITNESSES,
        "not enough witnesses"
    );
    ensure!(
        !last_ball_units.is_empty(),
        "processWitnessProof: no last ball units"
    );

    process_witness_change_and_definition(
        &witness_joints,
        witness_change_and_definition,
        from_current,
    )?;

    Ok(ProcessWitnessProof {
        last_ball_units,
        assoc_last_ball_by_last_ball_unit,
    })
}

fn process_witness_change_and_definition(
    witness_joints: &[&Joint],
    witness_change_and_definition: &[Joint],
    from_current: bool,
) -> Result<()> {
    for joint in witness_change_and_definition {
        ensure!(
            joint.ball.is_some(),
            "witness_change_and_definition_joints: joint without ball"
        );
        ensure!(
            joint.unit.has_valid_hashes(),
            "witness_change_and_definition_joints: invalid hash"
        );

        if !joint.unit.is_authored_by_witness() {
            bail!("not authored by my witness");
        }
    }

    let mut definitions = HashMap::<String, Value>::new();

    // Not handling definition change, so use address as key to find definition
    for address in MY_WITNESSES.iter() {
        if let Some((_, definition)) = SDAG_CACHE.get_definition(address) {
            definitions.insert(address.clone(), definition);
        }
    }

    for joint in witness_change_and_definition {
        let unit_hash = &joint.unit.unit;
        if from_current {
            // already known and stable - skip it
            if let Ok(joint) = SDAG_CACHE.get_joint(unit_hash) {
                let joint_data = joint.read()?;
                if joint_data.is_stable() {
                    continue;
                }
            }
        }
        validate_witness_unit(&joint.unit, &mut definitions, true)?;
    }

    // check signatures of unstable witness joints
    for joint in witness_joints {
        validate_witness_unit(&joint.unit, &mut definitions, false)?;
    }

    Ok(())
}

fn validate_witness_unit(
    unit: &Unit,
    definitions: &mut HashMap<String, Value>,
    require_definition_or_change: bool,
) -> Result<()> {
    use object_hash;
    use validation;

    let mut b_found = false;
    for author in &unit.authors {
        let address = &author.address;
        if !MY_WITNESSES.contains(address) {
            // not a witness - skip it
            continue;
        }

        if !author.definition.is_null() {
            let chash = object_hash::get_chash(&author.definition)?;
            ensure!(
                address == &chash,
                "definition doesn't hash to the expected value"
            );
            definitions.insert(address.clone(), author.definition.clone());
            b_found = true;
        }

        // handle author
        validation::validate_authentifiers(
            &Value::Null,
            definitions
                .get(address)
                .ok_or_else(|| format_err!("failed to find definition, address={}", address))?,
            &unit.calc_unit_hash_to_sign(),
            &author.authentifiers,
        )?;
    }

    if require_definition_or_change && !b_found {
        bail!("neither definition nor change");
    }
    Ok(())
}
