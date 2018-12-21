use std::collections::HashMap;

use business;
use cache::{CachedJoint, JointData, SDAG_CACHE};
use config;
use error::Result;
use failure::ResultExt;
use joint::{Joint, JointSequence, Level};
use main_chain;
use object_hash;
use serde::Deserialize;
use serde_json::Value;
use signature;
use spec::{Definition, Unit};

/// validate unit
pub fn validate_unit_hash(unit: &Unit) -> Result<()> {
    // check content_hash or unit_hash first!
    let unit_hash = match unit.content_hash {
        Some(ref hash) => hash,
        None => &unit.unit,
    };

    if unit_hash != &unit.calc_unit_hash() {
        bail!("wrong unit hash calculated");
    }
    Ok(())
}

/// validate joint when it get ready
pub fn validate_ready_joint(joint: CachedJoint) -> Result<()> {
    // TODO: if validation failed we should sent error message to the corresponding connection
    let joint_data = joint.read()?;

    // FIXME: what if it failed, should we purge the joint or just leaving it?
    // if we just return the error, the joint would still kept in unhandled, and never
    // got triggered, unless setup a timer to seek those valid joints that are ready
    joint_data.cacl_static_props()?;

    match normal_validate(&joint_data) {
        Ok(_) => {
            // save the unhandled joint to normal
            SDAG_CACHE.normalize_joint(&joint.key);
        }
        Err(e) => {
            // validation failed, purge the bad joint
            error!(
                "normal_validate, unit={}, err={}",
                &joint.key,
                e.to_string()
            );
            SDAG_CACHE.purge_bad_joint(&joint.key, e.to_string());
            return Err(e);
        }
    }

    // validate messages after joint transfer to normal joints
    // we need a complete graph data to check the non-serial joint
    if joint_data.unit.content_hash.is_none() {
        validate_messages(&joint_data);
    }

    if joint_data.is_min_wl_increased() {
        main_chain::MAIN_CHAIN_WORKER.push_ready_joint(joint)?;
    }

    // broadcast the good joint
    if joint_data.get_sequence() == JointSequence::Good {
        try_go!(|| ::network::hub::WSS.broadcast_joint(joint_data));
    }

    Ok(())
}

// validation before move the joint to normal joints
fn normal_validate(joint: &JointData) -> Result<()> {
    let unit = &joint.unit;

    if !unit.parent_units.is_empty() {
        validate_parents(joint)?;
        // validate_ball(joint)?;
    }

    validate_witnesses(joint).context("validate witnesses failed")?;

    if !joint.skiplist_units.is_empty() {
        validate_skip_list(&joint.skiplist_units)?;
    }

    validate_authors(joint)?;

    // check if include last self unit
    business::BUSINESS_CACHE.is_include_last_stable_self_joint(joint)?;
    // check sub businesses
    business::check_business(joint)?;

    // save definition after validate success
    for author in joint.unit.authors.iter() {
        if !author.definition.is_null() {
            SDAG_CACHE.insert_definition(
                author.address.to_owned(),
                joint.unit.unit.to_owned(),
                author.definition.to_owned(),
            );
        }
    }

    Ok(())
}

// validate_base: unit hash, content hash, size, len, version, alt, and so on;
pub fn basic_validate(joint: &Joint) -> Result<()> {
    let unit = &joint.unit;
    info!("basic validating joint identified by unit {}", unit.unit);

    // basic info checks
    if unit.version != config::VERSION {
        bail!("wrong version");
    }

    if unit.alt != config::ALT {
        bail!("wrong alt");
    }

    // basic parent check
    validate_parent_basic(unit)?;

    // basic author check
    validate_author_basic(unit)?;

    // basic message check
    validate_message_basic(unit)?;

    // basic ball check
    validate_ball_basic(joint)?;

    Ok(())
}

// check if joint.ball correct
#[allow(dead_code)]
fn validate_ball(joint: &JointData) -> Result<()> {
    if joint.ball.is_none() {
        return Ok(());
    }

    let ball = joint.ball.as_ref().unwrap();
    let unit_hash = &joint.unit.unit;

    // at this point the ball should only exist in the hash tree ball
    let hash_tree_unit = SDAG_CACHE.get_hash_tree_unit(ball);
    if hash_tree_unit.is_none() {
        bail!("ball {} is not known in hash tree", ball);
    }
    if &hash_tree_unit.unwrap() != unit_hash {
        bail!("ball {} unit {} contradicts hash tree", ball, unit_hash);
    }

    // this is already done in process hash tree balls
    let mut parent_balls = Vec::new();
    for parent in joint.parents.iter() {
        let parent_joint = parent.read()?;
        // FIXME: it's not protected for the two source data, we may get bail!
        let parent_ball = match parent_joint.ball {
            None => match SDAG_CACHE.get_hash_tree_ball(&parent_joint.unit.unit) {
                None => bail!("some parents ball not found"),
                Some(ball) => ball,
            },
            Some(ref ball) => ball.clone(),
        };

        parent_balls.push(parent_ball);
    }

    // skiplist must be stable already?
    let mut skiplist_balls = Vec::new();
    for unit in &joint.skiplist_units {
        let skiplist_joint = SDAG_CACHE.get_joint(unit)?.read()?;
        // FIXME: it's not protected for the two source data, we may get bail!
        let skiplist_ball = match skiplist_joint.ball {
            None => match SDAG_CACHE.get_hash_tree_ball(&skiplist_joint.unit.unit) {
                None => bail!("some skiplist ball not found"),
                Some(ball) => ball,
            },
            Some(ref ball) => ball.clone(),
        };

        skiplist_balls.push(skiplist_ball);
    }

    parent_balls.sort();
    skiplist_balls.sort();
    let ball_hash = object_hash::calc_ball_hash(
        unit_hash,
        &parent_balls,
        &skiplist_balls,
        // TODO: how to judge a joint is bad?
        // | message | sequence | content_hash |
        // |---------|----------|--------------|
        // | Vec     | Good     | None         |
        // | Vec     | Bad      | ?            |
        // | Empty   | NoCm     | hash         |
        joint.unit.content_hash.is_some(),
    );

    if &ball_hash != ball {
        bail!(
            "ball hash is wrong, calc ball [{}], joint.ball [{}]",
            &ball_hash,
            &ball
        );
    }

    Ok(())
}

fn validate_parent_basic(unit: &Unit) -> Result<()> {
    if unit.is_genesis_unit() {
        return Ok(());
    }

    // non_genesis joint must has at least one parent
    if unit.parent_units.is_empty() {
        bail!("joint contains no parents");
    }

    // the parents must less than MAX_PARENT_PER_UNIT
    if unit.parent_units.len() > config::MAX_PARENT_PER_UNIT {
        bail!("joint parents must less than MAX_PARENT_PER_UNIT");
    }

    // genesis last_ball is none
    if unit.last_ball.as_ref().map(|s| s.len()).unwrap_or(0) != config::HASH_LENGTH {
        bail!("wrong length of last ball");
    }
    // genesis last_ball_unit is none
    if unit.last_ball_unit.as_ref().map(|s| s.len()).unwrap_or(0) != config::HASH_LENGTH {
        bail!("wrong length of last ball unit");
    }

    // the parent unit must be unique and sorted
    for pair in unit.parent_units.windows(2) {
        if pair[0] >= pair[1] {
            bail!("joint parents must be unique and sorted");
        }
    }

    Ok(())
}

fn validate_author_basic(unit: &Unit) -> Result<()> {
    if unit.authors.is_empty() {
        bail!("missing or empty authors array");
    }

    if unit.authors.len() > config::MAX_AUTHORS_PER_UNIT {
        bail!("too many authors");
    }

    let mut prev_address = String::new();
    for author in &unit.authors {
        if author.address <= prev_address {
            bail!("author addresses not sorted");
        }
        prev_address = author.address.clone();

        if author.address.len() != 32 {
            bail!("wrong address length");
        }

        if author.authentifiers.is_empty() && unit.content_hash.is_none() {
            bail!("no authentifiers");
        }

        for auth in author.authentifiers.values() {
            if auth.is_empty() {
                bail!("authentifiers must be nonempty strings");
            }
            if auth.len() > config::MAX_AUTHENTIFIER_LENGTH {
                bail!("authentifier too long");
            }
        }

        if !object_hash::is_chash_valid(&author.address) {
            bail!("address checksum invalid");
        }
    }

    Ok(())
}

fn validate_message_basic(unit: &Unit) -> Result<()> {
    // only non_serial unit has content_hash
    if unit.content_hash.is_some() {
        let content_hash = unit.content_hash.as_ref().unwrap();
        if content_hash.len() != config::HASH_LENGTH {
            bail!("wrong content_hash length");
        }

        if !unit.earned_headers_commission_recipients.is_empty()
            || unit.headers_commission.is_some()
            || unit.payload_commission.is_some()
            || unit.main_chain_index.is_some() // TODO: this could be removed
            || !unit.messages.is_empty()
        {
            bail!("unknown fields in nonserial unit");
        }
    } else {
        // serial
        if unit.messages.is_empty() {
            bail!("missing or empty messages array");
        }

        if unit.messages.len() > config::MAX_MESSAGES_PER_UNIT {
            bail!("too many messages");
        }

        let header_size = unit.calc_header_size();
        if unit.headers_commission != Some(header_size) {
            bail!("wrong headers commission, expected {}", header_size);
        }

        let payload_size = unit.calc_payload_size();
        if payload_size > config::MAX_PAYLOAD_SIZE {
            bail!(
                "payload size more than max limit, payload_size is {}",
                payload_size
            );
        }
        if unit.payload_commission != Some(payload_size) {
            bail!("wrong payload commission, expected {}", payload_size);
        }
    }

    // validate each sub business format
    business::validate_business_basic(unit)?;

    Ok(())
}

fn validate_ball_basic(joint: &Joint) -> Result<()> {
    if joint.unsigned == Some(true) {
        if joint.ball.is_some() || !joint.skiplist_units.is_empty() {
            bail!("unknown fields in unsigned unit-joint");
        }
    } else if joint.ball.is_some() {
        let ball = joint.ball.as_ref().unwrap();
        if ball.len() != config::HASH_LENGTH {
            bail!("wrong ball length");
        }
    }

    if joint.unit.content_hash.is_some() && joint.ball.is_none() {
        bail!("content_hash allowed only in finished ball");
    }
    Ok(())
}

// 1)if joint has ball, parents should have ball
// 2)no include relationship between parents,
// 3)an address(author) can not appear twice in parents
// 4)last_ball_unit must be stable in view of parents(at least one parent)
// 5)last_ball_unit must be on_main_chain, joint.last_ball = last_ball from DB
// 6)max_parent_last_ball_mci <= last_ball_mci (last ball unit should not retreat)
fn validate_parents(joint: &JointData) -> Result<()> {
    // should never happen
    if joint.parents.len() != joint.unit.parent_units.len() {
        bail!("missing parents");
    }

    let joint_ball = &joint.ball;

    let mut parent_authors = Vec::new();
    let mut parents = Vec::new();

    for parent in joint.parents.iter() {
        let parent_joint = parent.read()?;

        // the ball value is from network data, we are not stable yet!
        if joint_ball.is_some() && parent_joint.ball.is_none() {
            bail!("joint has ball, and parent [{}] has not ball", parent.key);
        }

        // check no same author/address for more than one parents
        for author in &parent_joint.unit.authors {
            let addr = &author.address;
            if parent_authors.contains(addr) {
                bail!("some addresses [{}] found more than once in parents", addr);
            }
            parent_authors.push(addr.to_owned());
        }
        //type: (RcuReader<JointData>, CachedJoint)
        parents.push((parent_joint, parent.clone()));
    }

    // ensure no include relationship between parents
    // descendent sorting
    let mut new_parents = Vec::new();
    parents.sort_by(|a, b| Ord::cmp(&b.0.get_level().value(), &a.0.get_level().value()));
    while let Some(p) = parents.pop() {
        if p.0.get_props().is_ancestor(parents.iter().map(|v| &v.1))? {
            bail!("have include relationship between parents");
        }
        new_parents.push(p.1);
    }

    let last_ball_unit_hash = match joint.unit.last_ball_unit {
        None => return Ok(()), // no last ball here
        Some(ref unit) => unit,
    };

    let last_ball_joint = match SDAG_CACHE.get_joint(last_ball_unit_hash) {
        Ok(j) => j,
        Err(e) => bail!(
            "last ball unit {} not found, err={}",
            last_ball_unit_hash,
            e
        ),
    };

    // Check if it is stable in view of the parents
    if !main_chain::is_stable_in_later_joints(&last_ball_joint, &new_parents)? {
        bail!(
            "{}: last ball unit {} is not stable in view of your parents {:?}",
            joint.unit.unit,
            last_ball_unit_hash,
            joint.parents
        );
    }

    // Last ball may not stable in our view, need to wait until it got stable
    last_ball_joint.read()?.wait_stable();

    let last_ball_joint_data = last_ball_joint.read()?;
    // last_ball_unit is on main chain
    if !last_ball_joint_data.is_on_main_chain() {
        bail!("last ball {} is not on MC", last_ball_unit_hash);
    }

    if last_ball_joint_data.ball.is_none() {
        bail!(
            "last ball unit {} is stable but has no ball",
            last_ball_unit_hash
        );
    }

    if last_ball_joint_data.ball != joint.unit.last_ball {
        bail!(
            "finalize_joint.ball {:?} and joint.unit.last_ball {:?} do not match",
            last_ball_joint_data.ball,
            joint.unit.last_ball
        );
    }

    // check last ball should not retreat
    let mut max_parent_last_ball_mci = Level::MINIMUM;
    for parent in new_parents {
        let parent_joint = parent.read()?;
        let parent_last_ball_mci = parent_joint.get_last_ball_mci()?;
        if parent_last_ball_mci > max_parent_last_ball_mci {
            max_parent_last_ball_mci = parent_last_ball_mci;
        }
    }

    let last_ball_mci = last_ball_joint_data.get_mci();
    if max_parent_last_ball_mci > last_ball_mci {
        bail!(
            "last ball mci must not retreat, max_parent_last_ball_mci:{:?}, last_ball_mci:{:?}",
            max_parent_last_ball_mci,
            last_ball_mci
        );
    }

    Ok(())
}

fn validate_skip_list(skip_list: &[String]) -> Result<()> {
    let mut prev = &String::new();
    for skip_unit in skip_list {
        if skip_unit <= prev {
            bail!("skiplist units not ordered");
        }

        // skip_unit must exit, else bail!
        let joint_prop = SDAG_CACHE.get_joint(&skip_unit)?.read()?.get_props();
        if joint_prop.is_stable {
            //skip unit must on main chain after stable
            if joint_prop.mci != joint_prop.limci {
                bail!("skiplist unit {} is not on MC", skip_unit);
            }
            if joint_prop.mci.value() % 10 != 0 {
                bail!("skiplist unit {} MCI is not divisible by 10", skip_unit);
            }
        }
        prev = skip_unit;
    }

    Ok(())
}

// 1) if unit has witness_list_unit, then witness_list_unit must be exit, and is stable, sequence is 'good', mci <= unit.last_ball_mci
// 2) unit_witnesses table must has the record(unit = witness_list_unit), and len is 12
// 3) if unit.witnesses is 12, then witnesses.address must valid and ordering
// 4) unit.witnesses or unit.witnesses_list_unit must exist one;
// 5) the count of witness_unit (mci <= last_ball_mci, and is_stable, and sequence is 'good', and address distinct) must be 12 (cancel this check)
// 6) allow witnesses change one between last_ball_unit and current unit, but not allow changes more than one(cancel this check)
// Note: in future we would read all witnesses from the chain itself, thus we don't have to validate witnesses for a joint
fn validate_witnesses(joint: &JointData) -> Result<()> {
    let unit = &joint.unit;

    if unit.witness_list_unit.is_some() && !unit.witnesses.is_empty() {
        bail!("ambiguous witnesses");
    }

    if let Some(witness_list_unit) = &unit.witness_list_unit {
        let witness_joint = SDAG_CACHE.get_joint(witness_list_unit)?.read()?;
        let witness_joint_props = witness_joint.get_props();

        if witness_joint_props.sequence != ::joint::JointSequence::Good {
            bail!("witness list unit is not serial");
        }
        if !witness_joint_props.is_stable {
            bail!("witness list unit is not stable");
        }
        // Note: the witness unit should be ahead of last ball unit
        if witness_joint_props.mci > joint.get_last_ball_mci()? {
            bail!("witness list unit must come before last ball");
        }

        // Note: this not necessary, because we have verify the witness unit previously
        let witnesses = &witness_joint.unit.witnesses;
        if witnesses.len() != config::COUNT_WITNESSES {
            bail!("wrong number of witnesses: {}", witnesses.len());
        }
    } else if unit.witnesses.len() == config::COUNT_WITNESSES {
        let mut witness_iter = unit.witnesses.iter();
        let mut prev_witness = witness_iter.next();
        for curr_witness in witness_iter {
            if !object_hash::is_chash_valid(curr_witness) {
                bail!("witness address is invalid")
            }

            if Some(curr_witness) <= prev_witness {
                bail!("wrong order of witnesses, or duplicates")
            }
            prev_witness = Some(curr_witness);
        }
    } else {
        bail!("no witnesses or not enough witnesses")
    }

    Ok(())
}

fn validate_authors(joint: &JointData) -> Result<()> {
    for author in &joint.unit.authors {
        if !author.definition.is_null() {
            // only first joint need take definition
            if SDAG_CACHE.get_definition(&author.address).is_some() {
                bail!("duplicate definition");
            }

            let address = object_hash::get_chash(&author.definition)?;
            if author.address != address {
                bail!(
                    "address and definition are not match!, address = {}, definition = {:?}",
                    author.address,
                    author.definition
                );
            }

            let definition = &author.definition;
            let unit_hash = joint.unit.calc_unit_hash_to_sign();
            validate_authentifiers(&Value::Null, definition, &unit_hash, &author.authentifiers)?;
        } else {
            // when content is cleared, unit_hash is not correct
            if joint.unit.content_hash.is_some() {
                joint.set_sequence(JointSequence::FinalBad);
                return Ok(());
            }

            let (_, definition) = SDAG_CACHE.get_definition(&author.address).ok_or_else(|| {
                format_err!(
                    "definition bound to address {} is not defined",
                    author.address
                )
            })?;

            let unit_hash = joint.unit.calc_unit_hash_to_sign();
            validate_authentifiers(&Value::Null, &definition, &unit_hash, &author.authentifiers)?;
        };
    }
    Ok(())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SigValue<'a> {
    algo: Option<&'a str>,
    pubkey: &'a str,
}

fn validate_definition(definition: &Value, is_asset: bool) -> Result<()> {
    fn evaluate(
        definition: &Value,
        is_in_negation: bool,
        is_asset: bool,
        complexity: &mut usize,
    ) -> Result<bool> {
        *complexity += 1;
        if *complexity > config::MAX_COMPLEXITY {
            bail!("complexity exceeded");
        }

        let definition = Definition::from_value(definition)?;

        match definition.op {
            "sig" => {
                if is_in_negation {
                    bail!("sig cannot be negated");
                }
                if is_asset {
                    bail!("asset condition cannot have sig");
                }

                let sig_value =
                    SigValue::deserialize(definition.args).context("can't convert to SigValue")?;

                if let Some(algo) = sig_value.algo {
                    ensure!(algo == "secp256k1", "unsupported sig algo");
                }

                ensure!(
                    sig_value.pubkey.len() == config::HASH_LENGTH,
                    "wrong pubkey length"
                );
            }
            op => unimplemented!("unsupported op: {}", op),
        }
        Ok(true)
    }

    let mut complexity = 0;
    let has_sig = evaluate(definition, false, is_asset, &mut complexity)?;

    if !has_sig && !is_asset {
        bail!("each branch must have a signature");
    }

    Ok(())
}

pub fn validate_authentifiers<S: std::hash::BuildHasher>(
    asset: &Value,
    definition: &Value,
    unit_hash: &[u8],
    authentifiers: &HashMap<String, String, S>,
) -> Result<()> {
    let evaluate = |definition: &Value, path: &str, used_path: &mut Vec<String>| -> Result<()> {
        let definition = Definition::from_value(definition)?;
        match definition.op {
            "sig" => {
                let sig = authentifiers
                    .get(path)
                    .ok_or_else(|| format_err!("authentifier path not found: {}", path))?;
                used_path.push(path.to_owned());

                let sig_value =
                    SigValue::deserialize(definition.args).context("can't convert to SigValue")?;

                signature::verify(unit_hash, sig, sig_value.pubkey)
                    .context(format!("bad signature at path: {:?}", path))?;
            }
            op => unimplemented!("unsupported op: {}", op),
        }
        Ok(())
    };

    let is_asset = authentifiers.is_empty();
    if is_asset && !asset.is_null() {
        bail!("incompatible params");
    }
    validate_definition(definition, is_asset)?;
    let mut used_path = Vec::new();
    evaluate(definition, "r", &mut used_path)?;
    if !is_asset && used_path.len() != authentifiers.len() {
        bail!(
            "some authentifiers are not used, used={:?}, passed={:?}",
            used_path,
            authentifiers
        );
    }
    Ok(())
}

/// after normalization
fn validate_messages(joint: &JointData) {
    info!("validateMessages {:?}", joint.unit.unit);

    if joint.unit.content_hash.is_some() {
        info!(
            "payload has been cleared, content_hash is [{:?}]",
            joint.unit.content_hash
        );
        return;
    }

    // validate if have enough balance to pay commission first
    match business::BUSINESS_CACHE.validate_unstable_joint(joint) {
        Ok(s) => joint.set_sequence(s),
        Err(e) => {
            error!("validate_unstable_joint failed, err={}", e);
            joint.set_sequence(JointSequence::FinalBad);
        }
    }
}
