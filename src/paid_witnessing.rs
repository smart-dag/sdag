use std::collections::{HashSet, VecDeque};

use cache::CachedJoint;
use config;
use error::Result;
use joint::Level;

#[allow(dead_code)]
pub fn get_max_spendable_mci_for_last_ball_mci(last_ball_mci: u32) -> Option<u32> {
    last_ball_mci.checked_sub(1 + config::COUNT_MC_BALLS_FOR_PAID_WITNESSING)
}

pub fn get_max_spendable_joint_for_last_ball(_last_ball: CachedJoint) -> Result<CachedJoint> {
    unimplemented!()
}

fn get_next_unpaid_joint(_joint: &CachedJoint) -> Result<CachedJoint> {
    unimplemented!()
}

fn save_to_db() -> Result<()> {
    unimplemented!()
}

fn get_paying_witnesses() -> Result<Vec<CachedJoint>> {
    unimplemented!()
}

fn get_witness_list(_joint: &CachedJoint) -> Result<Vec<String>> {
    unimplemented!()
}

fn verify_if_build(_from_joint: &CachedJoint) -> Result<()> {
    unimplemented!()
}

fn get_min_mci_joint_unpaid_witness() -> Result<CachedJoint> {
    unimplemented!()
}

fn build_paid_witnesses(to_joint: &CachedJoint) -> Result<()> {
    verify_if_build(&to_joint)?;
    let witnesses_list = get_witness_list(&to_joint)?;

    let joint_data = to_joint.read()?;
    // let _joint_props = graph::UnitProps {
    //     unit: (*to_joint.key).clone(),
    //     level: joint_data.get_level(),
    //     limci: joint_data.get_limci(),
    //     mci: joint_data.get_mci(),
    //     is_on_main_chain: joint_data.is_on_main_chain(),
    //     is_free: joint_data.is_free(),
    //     is_stable: joint_data.is_stable(),
    //     wl: joint_data.get_wl() as usize,
    // };

    let to_main_chain_index =
        joint_data.get_mci() + config::COUNT_MC_BALLS_FOR_PAID_WITNESSING as usize;
    let _units = read_descendant_units_by_authors_before_mc_index(
        to_joint,
        &witnesses_list,
        to_main_chain_index,
    )?;

    let _paying_witnesses = get_paying_witnesses()?;

    save_to_db()?;

    unimplemented!()
}

//key is last-stable_joint
pub fn update_paid_witnesses(key: CachedJoint) -> Result<()> {
    let max_spendable_mci = get_max_spendable_joint_for_last_ball(key)?
        .read()?
        .get_mci();
    let mut min_mci_unpaid_joint = get_min_mci_joint_unpaid_witness()?;

    while min_mci_unpaid_joint.read().unwrap().get_mci() <= max_spendable_mci {
        build_paid_witnesses(&min_mci_unpaid_joint)?;

        if let Ok(tmp) = get_next_unpaid_joint(&min_mci_unpaid_joint) {
            min_mci_unpaid_joint = tmp;
        } else {
            break;
        }
    }

    Ok(())
}

// get descendant joints whose author in author_addresses
// start from earlier_joint until mci <= to_mci
fn read_descendant_units_by_authors_before_mc_index(
    earlier_joint: &CachedJoint,
    author_addresses: &[String],
    to_mci: Level,
) -> Result<Vec<CachedJoint>> {
    let mut joints = VecDeque::new();
    let mut visited = HashSet::new();

    let mut result = Vec::new();

    joints.push_back(earlier_joint.read()?);

    while let Some(joint) = joints.pop_front() {
        for child in joint.children.iter() {
            let child_data = child.read()?;
            let child_mci = child_data.get_mci();

            if child_mci > to_mci {
                continue;
            }

            for author in &child_data.unit.authors {
                if author_addresses.contains(&author.address) {
                    result.push((&*child).clone());
                }
            }

            if !visited.contains(&child.key) {
                visited.insert(child.key.clone());
                joints.push_back(child_data);
            }
        }
    }

    Ok(result)
}
