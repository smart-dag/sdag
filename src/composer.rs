use std::collections::{HashMap, HashSet};

use cache::{CachedJoint, JointData, SDAG_CACHE};
use config;
use error::Result;
use joint::Joint;
use light::*;
use object_hash;
use rcu_cell::RcuReader;
use serde_json::Value;
use signature::Signer;
use spec::*;

#[derive(Serialize, Deserialize)]
pub struct ParentsAndLastBall {
    pub parents: Vec<String>,
    pub last_ball: String,
    pub last_ball_unit: String,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ComposeInfo {
    pub paid_address: String,
    pub change_address: String,
    pub outputs: Vec<Output>,
    pub inputs: InputsResponse,
    pub transaction_amount: u64,
    pub text_message: Option<Message>,
    pub light_props: LightProps,
    pub pubk: String,
}

/// we should pick last stable ball firstly.
/// if we pick parents firstly, last ball we picked may not be last ball in the view of parents
/// the last ball belong to the newer unit coming on main chain after parents
pub fn pick_parents_and_last_ball(_address: &str) -> Result<ParentsAndLastBall> {
    let mut lsj_data = ::main_chain::get_last_stable_joint();
    let mut free_joints = SDAG_CACHE.get_good_free_joints()?;

    // detect same author joints
    let mut authors = HashMap::new();
    for joint in free_joints.iter() {
        let joint = joint.read()?;
        let address = joint.unit.authors[0].address.clone();

        if let Some(old_unit) = authors.insert(address, joint.unit.unit.clone()) {
            bail!(
                "there are same author joints: [{}],[{}]",
                joint.unit.unit,
                old_unit
            )
        }
    }

    //cached joint must include a value, this read.unwrap can't panic
    free_joints.sort_by_key(|a| a.read().unwrap().get_level().value());
    // must include best joint, last stable point is sure stable to it
    let best_joint = ::main_chain::find_best_joint(free_joints.iter())?
        .ok_or_else(|| format_err!("free joints is empty now"))?;

    let best_min_wl = best_joint.get_min_wl();

    // FIXME: last ball could retreat!
    // usually the last stable joint is stable to best_joint
    while best_min_wl < lsj_data.get_level() {
        // adjust the last stable unit
        error!("adjust last stable joint when compose unit");
        lsj_data = lsj_data.get_best_parent().read()?;
    }

    // pick other joints freely
    let mut parents = vec![best_joint.unit.unit.clone()];
    if free_joints.len() > config::MAX_PARENT_PER_UNIT {
        // get the free joint which include last my unstable joint first
        if let Some(unit) = get_include_self_free_joint(&free_joints, _address)? {
            parents.push(unit);
        }
    }

    for joint in free_joints {
        if parents.contains(&*joint.key) {
            continue;
        }

        parents.push(joint.key.to_string());
        if parents.len() >= config::MAX_PARENT_PER_UNIT {
            break;
        }
    }

    parents.sort();

    Ok(ParentsAndLastBall {
        parents,
        last_ball: lsj_data.ball.clone().expect("ball in joint is none"),
        last_ball_unit: lsj_data.unit.unit.clone(),
    })
}

/// if my joint is unstable, get the free joint which is the descendant of my unstable joint
fn get_include_self_free_joint(
    free_joints: &[CachedJoint],
    address: &str,
) -> Result<Option<String>> {
    if let Some(joint) = get_last_my_unstable_joint(free_joints, address)? {
        let mut first_child = joint;
        while !first_child.is_free() {
            if let Some(child) = first_child.children.iter().nth(0) {
                first_child = child.read()?;
            }
        }
        return Ok(Some(first_child.unit.unit.clone()));
    }

    Ok(None)
}

/// get last my joint from unstable joints
fn get_last_my_unstable_joint(
    free_joints: &[CachedJoint],
    address: &str,
) -> Result<Option<RcuReader<JointData>>> {
    let mut joints = Vec::new();
    let mut visited = HashSet::new();
    for joint in free_joints {
        if visited.insert(joint.key.clone()) {
            joints.push(joint.read()?);
        }
    }

    while let Some(joint) = joints.pop() {
        if joint.is_stable() {
            continue;
        }

        for author in &joint.unit.authors {
            if author.address == address {
                return Ok(Some(joint.clone()));
            }
        }

        for p in joint.parents.iter() {
            if visited.insert(p.key.clone()) {
                joints.push(p.read()?);
            }
        }
    }

    Ok(None)
}

/// create a pure text message
pub fn create_text_message(text: &str) -> Result<Message> {
    Ok(Message {
        app: String::from("text"),
        payload_location: String::from("inline"),
        payload_hash: object_hash::get_base64_hash(text)?,
        payload: Some(Payload::Text(text.to_string())),
        ..Default::default()
    })
}

pub fn compose_joint<T: Signer>(composer_info: ComposeInfo, signer: &T) -> Result<Joint> {
    let ComposeInfo {
        paid_address,
        change_address,
        transaction_amount,
        mut inputs,
        mut outputs,
        light_props,
        text_message,
        pubk,
    } = composer_info;

    let mut new_outputs = vec![Output {
        address: change_address.clone(),
        amount: 0,
    }];
    new_outputs.append(&mut outputs);

    let mut unit = Unit {
        messages: text_message.into_iter().collect::<Vec<_>>(),
        ..Default::default()
    };

    unit.last_ball = Some(light_props.last_ball);
    unit.last_ball_unit = Some(light_props.last_ball_unit);
    unit.witness_list_unit = Some(light_props.witness_list_unit);
    unit.parent_units = light_props.parent_units;

    let definition = if light_props.has_definition {
        Value::Null
    } else {
        json!(["sig", { "pubkey": pubk }])
    };
    let authors = vec![Author {
        address: paid_address,
        authentifiers: {
            // here we use a dummy signature to calc the correct header size
            let mut sign = HashMap::new();
            sign.insert("r".to_string(), "-".repeat(config::SIG_LENGTH));
            sign
        },
        definition,
    }];

    unit.authors = authors;

    let payment_message = Message {
        app: "payment".to_string(),
        payload_location: "inline".to_string(),
        payload_hash: "-".repeat(config::HASH_LENGTH),
        payload: Some(Payload::Payment(Payment {
            address: None,
            asset: None,
            definition_chash: None,
            denomination: None,
            inputs: vec![],
            outputs: new_outputs,
        })),
        payload_uri: None,
        payload_uri_hash: None,
        spend_proofs: vec![],
    };

    unit.messages.push(payment_message);
    unit.headers_commission = Some(unit.calc_header_size());

    if let Some(Payload::Payment(ref mut x)) = unit.messages.last_mut().unwrap().payload {
        x.inputs.append(&mut inputs.inputs);
    }

    unit.payload_commission = Some(unit.calc_payload_size());
    info!(
        "inputs increased payload by {}",
        unit.payload_commission.unwrap()
    );

    let change = inputs.amount as i64
        - transaction_amount as i64
        - i64::from(unit.headers_commission.unwrap())
        - i64::from(unit.payload_commission.unwrap());

    if change < 0 {
        bail!(
            "NOT_ENOUGH_FUNDS: address {} not enough spendable funds for fees",
            unit.authors[0].address
        );
    }

    {
        let payment_message = unit.messages.last_mut().unwrap();
        if let Some(Payload::Payment(ref mut x)) = payment_message.payload {
            if let Some(change_output) = x.outputs.first_mut() {
                change_output.amount = change as u64;
            } else {
                bail!("compose output error")
            }

            x.outputs.sort_by(|a, b| {
                if a.address == b.address {
                    a.amount.cmp(&b.amount)
                } else {
                    a.address.cmp(&b.address)
                }
            });

            payment_message.payload_hash = object_hash::get_base64_hash(&x)?;
        }
    }

    let unit_hash = unit.calc_unit_hash_to_sign();
    for mut author in &mut unit.authors {
        let signature = signer.sign(&unit_hash, &author.address)?;
        author.authentifiers.insert("r".to_string(), signature);
    }

    unit.timestamp = Some(::time::now() / 1000);
    unit.unit = unit.calc_unit_hash();

    Ok(Joint {
        ball: None,
        skiplist_units: Vec::new(),
        unsigned: None,
        unit,
    })
}
