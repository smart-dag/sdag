use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use cache::{CachedJoint, JointData, SDAG_CACHE};
use error::Result;
use joint::{JointSequence, Level};
use utils::AppendList;

struct ChildInfo {
    child_unit: String,
    next_mc_unit: String,
}

fn get_winner_info<'a>(children: &'a mut Vec<ChildInfo>) -> Result<&'a ChildInfo> {
    if children.len() == 1 {
        return Ok(&children[0]);
    }

    use sha1::Sha1;
    children.sort_by_key(|child| {
        let mut m = Sha1::new();
        m.update(child.child_unit.as_bytes());
        m.update(child.next_mc_unit.as_bytes());
        m.digest().to_string()
    });

    Ok(&children[0])
}

// find stable main chain joint whose mci increment by one
fn find_next_mci_joint(joint: &JointData) -> Result<CachedJoint> {
    let mci = joint.get_mci();
    match SDAG_CACHE.get_mc_unit_hash(mci + 1)? {
        None => bail!("not found next stable unit on main chain"),
        Some(unit) => SDAG_CACHE.get_joint(&unit),
    }
}

fn find_all_hc_payer(
    earner_joint_cache: &CachedJoint,
    since_mc_index: Level,
) -> Result<AppendList<CachedJoint>> {
    let earner_joint = earner_joint_cache.read()?;
    let hc_payers = &earner_joint.parents;
    let valid_payers = AppendList::new();
    for payer_cache in hc_payers.iter() {
        let payer = payer_cache.read()?;
        if payer.get_mci() > since_mc_index
            && payer.get_sequence() == JointSequence::Good
            && earner_joint.get_mci() - payer.get_mci() <= 1
            && payer.is_stable()
        {
            valid_payers.append(payer_cache.clone());
        }
    }
    Ok(valid_payers)
}

// TODO: this function is some wrong, need to more check and code review
fn find_hc_recipient(payer_joint: &CachedJoint) -> Result<CachedJoint> {
    let payer = payer_joint.read()?;
    let next_mc_unit = find_next_mci_joint(&payer)?;
    let mut children_info: Vec<ChildInfo> = Vec::new();
    for maybe_earner_data in payer.children.iter() {
        let maybe_earner = maybe_earner_data.read()?;
        if maybe_earner.get_mci() - payer.get_mci() <= 1
            && maybe_earner.is_stable()
            && maybe_earner.get_sequence() == JointSequence::Good
        {
            children_info.push(ChildInfo {
                child_unit: (*maybe_earner_data.key).clone(),
                next_mc_unit: (*next_mc_unit.key).clone(),
            });
        }
    }

    SDAG_CACHE.get_joint(get_winner_info(&mut children_info)?.child_unit.as_str())
}

#[allow(dead_code)]
// key had better to be the last stable unit
fn earner_and_payer_of_hc(
    key: CachedJoint,
    since_mc_index: Level,
) -> Result<HashMap<String, HashMap<String, u32>>> {
    let mut joints_cache = VecDeque::new();
    //find joint that is to pay header commission and push joints_cache
    joints_cache.push_back(key);

    //first key is pay headers commission
    //second key is earn headers commission and amount
    let mut payers_and_earners: HashMap<String, HashMap<String, u32>> = HashMap::new();
    let mut visited_joint: HashMap<Arc<String>, bool> = HashMap::new();

    while let Some(payer_joint_data) = joints_cache.pop_front() {
        let earner_joint_cache = find_hc_recipient(&payer_joint_data)?;
        let earner_joint = earner_joint_cache.read()?;
        let hc_payers = find_all_hc_payer(&earner_joint_cache, since_mc_index)?;

        for payer_cache in hc_payers.iter() {
            let payer = payer_cache.read()?;
            let payer_children = &payer.children;

            for next_payer in payer_children.iter() {
                if visited_joint.get(&next_payer.key).is_none() && next_payer.read()?.is_stable() {
                    joints_cache.push_back((&*next_payer).clone());
                    visited_joint.insert(next_payer.key.clone(), true);
                }
            }

            let earner_shared_addresses = &earner_joint.unit.earned_headers_commission_recipients;

            for address in earner_shared_addresses {
                let pay_amount = payer.unit.headers_commission.unwrap();
                let amount = if address.earned_headers_commission_share == 100 {
                    pay_amount
                } else {
                    (f64::from(pay_amount) * address.earned_headers_commission_share as f64 / 100.0)
                        .round() as u32
                };
                payers_and_earners
                    .entry((*payer_cache.key).clone())
                    .or_insert_with(HashMap::<String, u32>::new)
                    .entry(address.address.clone())
                    .or_insert(amount);
            }
        }
    }
    Ok(payers_and_earners)
}

// #[allow(dead_code)]
// fn insert_to_db(
//     db: &Connection,
//     since_mc_index: u32,
//     hc_data: HashMap<String, HashMap<String, u32>>,
// ) -> Result<()> {
//     let mut payer_and_earner: Vec<String> = Vec::new();
//     for payer in hc_data.keys() {
//         let earners = hc_data.get(payer).unwrap();
//         for earner in earners.keys() {
//             payer_and_earner.push(format!(
//                 "('{}','{}',{})",
//                 payer,
//                 earner,
//                 earners.get(earner).unwrap()
//             ));
//         }
//     }

//     let value_list = payer_and_earner.join(", ");

//     let sql = format!(
//         "INSERT INTO headers_commission_contributions (unit, address, amount) VALUES {}",
//         value_list
//     );
//     let mut stmt = db.prepare(&sql)?;
//     stmt.execute(&[])?;

//     let mut stmt = db.prepare_cached(
//             "INSERT INTO headers_commission_outputs (main_chain_index, address, amount) \
//                 SELECT main_chain_index, address, SUM(amount) FROM headers_commission_contributions JOIN units USING(unit) \
//                 WHERE main_chain_index>? \
//                 GROUP BY main_chain_index, address")?;
//     stmt.execute(&[&since_mc_index])?;

//     Ok(())
// }

//since_joint is last stable joint
#[allow(dead_code)]
pub fn calc_headers_commissions(_since_joint: CachedJoint) -> Result<()> {
    // let since_mc_index = since_joint.read()?.get_mci() - 1;

    // let payer_and_earner = earner_and_payer_of_hc(since_joint, since_mc_index)?;
    // let db = db::DB_POOL.get_connection();
    // insert_to_db(&db, since_mc_index, payer_and_earner)?;

    // Ok(())
    unimplemented!()
}
