mod data_feed;
mod text;
mod utxo;

use std::collections::BTreeMap;

use self::utxo::{UtxoData, UtxoKey};
use cache::{CachedJoint, JointData, SDAG_CACHE};
use config;
use error::Result;
use hashbrown::HashMap;
use joint::{JointSequence, Level};
use may::coroutine::JoinHandle;
use may::sync::{mpsc, RwLock};
use rcu_cell::RcuReader;
use sdag_object_base::object_hash;
use spec::*;

lazy_static! {
    pub static ref BUSINESS_WORKER: BusinessWorker = BusinessWorker::default();
    pub static ref BUSINESS_CACHE: BusinessCache =
        BusinessCache::rebuild_from_db().expect("failed to rebuild business state");
}

//---------------------------------------------------------------------------------------
// Business Trait (for different sub business)
//---------------------------------------------------------------------------------------
// TODO: use this trait in dynamic business registration
pub trait SubBusiness {
    /// validate business basics like format before put joint into cache
    fn validate_message_basic(message: &Message) -> Result<()>;
    /// check sub business before normalize
    fn check_business(joint: &JointData, message_idx: usize) -> Result<()>;
    /// validate if the message/action is valid in current state after joint got stable
    fn validate_message(&self, joint: &JointData, message_idx: usize) -> Result<()>;
    /// apply the message/action to the current business state
    /// this is a specific business state transition
    /// return an error means that something should never happen since we validate first
    /// and you should make sure that the state is rolled back before return error
    fn apply_message(&mut self, joint: &JointData, message_idx: usize) -> Result<()>;
    /// revert temp change if stable validation failed, only for temp state
    fn revert_message(&mut self, joint: &JointData, message_idx: usize) -> Result<()>;
}

//---------------------------------------------------------------------------------------
// BusinessWorker
//---------------------------------------------------------------------------------------
pub struct BusinessWorker {
    tx: mpsc::Sender<RcuReader<JointData>>,
    _handler: JoinHandle<()>,
}

impl Default for BusinessWorker {
    fn default() -> Self {
        let (tx, rx) = mpsc::channel();

        let _handler = start_business_worker(rx);

        BusinessWorker { tx, _handler }
    }
}

impl BusinessWorker {
    // the main chain logic would call this API to push stable joint in order
    pub fn push_stable_joint(&self, joint: RcuReader<JointData>) -> Result<()> {
        self.tx.send(joint)?;
        Ok(())
    }
}

// this would start the global thread to process the stable joints
fn start_business_worker(rx: mpsc::Receiver<RcuReader<JointData>>) -> JoinHandle<()> {
    go!(move || {
        while let Ok(joint) = rx.recv() {
            // TODO: spend the commissions first
            // if not enough we should set a special state and skip business validate and apply
            // and the final_stage would clear the content

            // TODO: add state transfer table

            match BUSINESS_CACHE.validate_stable_joint(&joint) {
                Ok(_) => {
                    match joint.get_sequence() {
                        JointSequence::NonserialBad | JointSequence::TempBad => {
                            // apply the message to temp business state
                            let mut temp_business_state =
                                BUSINESS_CACHE.temp_business_state.write().unwrap();
                            for i in 0..joint.unit.messages.len() {
                                if let Err(e) = temp_business_state.apply_message(&joint, i) {
                                    warn!("apply temp state failed, err = {}", e);
                                }
                            }
                        }
                        _ => {}
                    }

                    if let Err(e) = BUSINESS_CACHE.apply_stable_joint(&joint) {
                        // apply joint failed which should never happen
                        // but we have to save it as a bad joint
                        // we hope that the global state is still correct
                        // like transactions
                        error!(
                            "apply_joint failed, unit = {}, err = {}",
                            joint.unit.unit, e
                        );
                        joint.set_sequence(JointSequence::FinalBad);
                    }

                    if joint.get_sequence() != JointSequence::Good {
                        joint.set_sequence(JointSequence::Good);
                    }
                }
                Err(e) => {
                    error!(
                        "validate_joint failed, unit = {}, err = {}",
                        joint.unit.unit, e
                    );
                    if let JointSequence::Good = joint.get_sequence() {
                        let mut temp_business_state =
                            BUSINESS_CACHE.temp_business_state.write().unwrap();
                        for i in 0..joint.unit.messages.len() {
                            if let Ok(true) = BUSINESS_CACHE.stable_utxo_contains(&joint, i) {
                                if let Err(e) = temp_business_state.revert_message(&joint, i) {
                                    error!("revert temp state failed, err = {}", e);
                                }
                            }
                        }
                    }

                    joint.set_sequence(JointSequence::FinalBad);
                }
            }

            // FIXME: the joint may not exist due to purge temp-bad
            let joint = t_c!(SDAG_CACHE.get_joint(&joint.unit.unit));
            t_c!(::finalization::FINALIZATION_WORKER.push_final_joint(joint));
        }
        error!("business worker stopped!");
        ::std::process::abort();
    })
}

//---------------------------------------------------------------------------------------
// GlobalState
//---------------------------------------------------------------------------------------
#[derive(Default)]
pub struct GlobalState {
    // FIXME: this read lock is some what too heavy, we are only care about one address
    // record author own last stable self joint that he last send
    // HashMap<Address, UnitHash>
    last_stable_self_joint: RwLock<HashMap<String, String>>,

    // HahsMap<Address, output_unit_hash>
    related_joints: RwLock<HashMap<String, Vec<String>>>,
}

impl GlobalState {
    // return value: first is last_stable_self_unit, second is related_units
    fn get_global_state(&self, address: &str) -> (Option<String>, Vec<String>) {
        (
            self.get_last_stable_self_joint(address),
            self.get_related_joints(address),
        )
    }

    pub fn get_last_stable_self_joint(&self, address: &str) -> Option<String> {
        self.last_stable_self_joint
            .read()
            .unwrap()
            .get(address)
            .cloned()
    }

    pub fn get_related_joints(&self, address: &str) -> Vec<String> {
        match self.related_joints.read().unwrap().get(address) {
            Some(joints) => joints.clone(),
            None => Vec::new(),
        }
    }

    // note: just support one author currently
    fn update_global_state(&self, joint: &JointData) {
        self.update_last_stable_self_joint(joint);

        // clear self related joints
        self.remove_related_joints(&joint.unit.authors[0].address);
        // push other related joints
        self.update_related_joints(joint);
    }

    fn update_last_stable_self_joint(&self, joint: &JointData) {
        let unit_hash = &joint.unit.unit;
        // only genesis has multi authors currently
        // joints which have multi authors should not belong any author
        if joint.unit.authors.len() == 1 {
            self.last_stable_self_joint
                .write()
                .unwrap()
                .entry(joint.unit.authors[0].address.clone())
                .and_modify(|v| *v = unit_hash.clone())
                .or_insert_with(|| unit_hash.clone());
        }
    }

    /// get <to_addr, unit_has> from outputs, then update related_joints[to_addr]
    fn update_related_joints(&self, joint: &JointData) {
        let address = if joint.unit.authors.len() > 1 {
            "multi_address" // never match
        } else {
            &joint.unit.authors[0].address
        };

        let unit_hash = &joint.unit.unit;
        for msg in &joint.unit.messages {
            if let Some(Payload::Payment(ref payment)) = msg.payload {
                for output in &payment.outputs {
                    // related_joints should not include changes
                    if output.address != address {
                        self.related_joints
                            .write()
                            .unwrap()
                            .entry(output.address.clone())
                            .and_modify(|v| {
                                if !v.contains(unit_hash) {
                                    v.push(unit_hash.clone())
                                }
                            })
                            .or_insert_with(|| vec![unit_hash.clone()]);
                    }
                }
            }
        }
    }

    fn remove_related_joints(&self, addr: &str) {
        self.related_joints.write().unwrap().remove(addr);
    }

    /// get balance from stable joints
    pub fn get_stable_balance(&self, address: &str) -> Result<u64> {
        let (last_stable_self_unit, related_units) = self.get_global_state(address);

        let mut balance = match last_stable_self_unit {
            Some(ref unit) => SDAG_CACHE.get_joint(unit)?.read()?.get_balance(),
            None => 0,
        };

        // add those related payment to us
        for unit in &related_units {
            let related_joint_date = SDAG_CACHE.get_joint(unit)?.read()?;
            for msg in &related_joint_date.unit.messages {
                if let Some(Payload::Payment(ref payment)) = msg.payload {
                    // note: no mater what kind we should add output for balance
                    for output in &payment.outputs {
                        if output.address == address {
                            balance += output.amount;
                        }
                    }
                }
            }
        }

        Ok(balance)
    }

    /// rebuild from database
    /// TODO: rebuild from database
    /// NOTE: need also update global state and temp business state
    pub fn rebuild_from_db() -> Result<Self> {
        Ok(GlobalState::default())
    }
}

//---------------------------------------------------------------------------------------
// BusinessState
//---------------------------------------------------------------------------------------
#[derive(Default)]
pub struct BusinessState {
    // below is sub business
    utxo: utxo::UtxoCache,
    text: text::TextCache,
    data_feed: data_feed::TimerCache,
    // TODO: dynamic business (use Anymap?)
}

impl BusinessState {
    //check if the joint contains spending utxo
    fn utxo_contains(&self, joint: &JointData, msg_index: usize) -> Result<bool> {
        if joint.unit.messages.len() <= msg_index {
            bail!(
                "unknown message, max index : {}, error index: {}",
                joint.unit.messages.len() - 1,
                msg_index
            )
        }

        let message = &joint.unit.messages[msg_index];
        let outputs = self.get_utxos_by_address(&joint.unit.authors[0].address)?;

        match message.payload {
            Some(Payload::Payment(ref payment)) => {
                for Input {
                    unit,
                    output_index,
                    message_index,
                    ..
                } in &payment.inputs
                {
                    let unit = unit.clone().unwrap();
                    let output_index = output_index.unwrap() as usize;
                    let message_index = message_index.unwrap() as usize;
                    let output = utxo::get_output_by_unit(&unit, output_index, message_index)?;

                    if !outputs.contains_key(&UtxoKey {
                        unit,
                        output_index,
                        message_index,
                        amount: output.amount,
                    }) {
                        return Ok(false);
                    }
                }
            }
            _ => bail!("payload is not a payment"),
        }

        Ok(true)
    }

    fn get_utxos_by_address(&self, address: &str) -> Result<&BTreeMap<UtxoKey, UtxoData>> {
        self.utxo
            .get_utxos_by_address(address)
            .ok_or_else(|| format_err!("there is no output for address {}", address))
    }

    fn validate_message_basic(message: &Message) -> Result<()> {
        // each sub business format check
        match message.app.as_str() {
            "payment" => utxo::UtxoCache::validate_message_basic(message)?,
            "text" => text::TextCache::validate_message_basic(message)?,
            "data_feed" => data_feed::TimerCache::validate_message_basic(message)?,
            _ => bail!("unsupported business"),
        }
        Ok(())
    }

    fn check_business(joint: &JointData, message_idx: usize) -> Result<()> {
        let message = &joint.unit.messages[message_idx];
        match message.app.as_str() {
            "payment" => utxo::UtxoCache::check_business(joint, message_idx)?,
            "text" => text::TextCache::check_business(joint, message_idx)?,
            "data_feed" => data_feed::TimerCache::check_business(joint, message_idx)?,
            _ => bail!("unsupported business"),
        }
        Ok(())
    }

    fn validate_message(&self, joint: &JointData, message_idx: usize) -> Result<()> {
        let message = &joint.unit.messages[message_idx];
        match message.app.as_str() {
            "payment" => self.utxo.validate_message(joint, message_idx)?,
            "text" => self.text.validate_message(joint, message_idx)?,
            "data_feed" => self.data_feed.validate_message(joint, message_idx)?,
            _ => bail!("unsupported business"),
        }
        Ok(())
    }

    fn apply_message(&mut self, joint: &JointData, message_idx: usize) -> Result<()> {
        let message = &joint.unit.messages[message_idx];
        match message.app.as_str() {
            "payment" => self.utxo.apply_message(joint, message_idx)?,
            "text" => self.text.apply_message(joint, message_idx)?,
            "data_feed" => self.data_feed.apply_message(joint, message_idx)?,
            _ => bail!("unsupported business"),
        }
        Ok(())
    }

    // only temp state would call this api
    fn revert_message(&mut self, joint: &JointData, message_idx: usize) -> Result<()> {
        let message = &joint.unit.messages[message_idx];
        match message.app.as_str() {
            "payment" => self.utxo.revert_message(joint, message_idx)?,
            "text" => self.text.revert_message(joint, message_idx)?,
            "data_feed" => self.data_feed.revert_message(joint, message_idx)?,
            _ => bail!("unsupported business"),
        }
        Ok(())
    }
}

//---------------------------------------------------------------------------------------
// BusinessCache
//---------------------------------------------------------------------------------------
#[derive(Default)]
pub struct BusinessCache {
    // TODO: lock global is not necessary for each address
    pub global_state: GlobalState,
    business_state: RwLock<BusinessState>,
    temp_business_state: RwLock<BusinessState>,
}

impl BusinessCache {
    pub fn stable_utxo_contains(&self, joint: &JointData, msg_index: usize) -> Result<bool> {
        self.business_state
            .read()
            .unwrap()
            .utxo_contains(joint, msg_index)
    }

    /// select unspent outputs from temp output
    /// determine if units related with selected outputs is stable
    /// if no, calculate unstable outputs' amount
    /// pick amount whose value equals that amount until total amount >= required_ament
    pub fn get_inputs_for_amount(
        &self,
        paying_address: &str,
        required_amount: u64,
        send_all: bool,
        last_stable_unit: &str,
    ) -> Result<(Vec<Input>, u64)> {
        let last_ball_joint = SDAG_CACHE.get_joint(last_stable_unit)?.read()?;

        let temp_state = self.temp_business_state.read().unwrap();
        let temp_outputs = temp_state.get_utxos_by_address(paying_address)?;

        let stable_state = self.business_state.read().unwrap();
        let stable_outputs = stable_state.get_utxos_by_address(paying_address)?;

        let mut inputs = vec![];
        let mut total_amount: u64 = 0;
        for v in temp_outputs.keys() {
            // we can't use unit.is_stable() here, it's may not stable yet
            if !stable_outputs.contains_key(v) {
                continue;
            }

            // input unit must before last ball
            let input_joint = SDAG_CACHE.get_joint(&v.unit)?.read()?;
            let is_include = *input_joint <= *last_ball_joint;
            if !is_include {
                warn!(
                    "input unit {} is not ancestor of last stable unit {:?}",
                    &v.unit, last_ball_joint.unit.unit
                );
                continue;
            }

            total_amount += v.amount;
            inputs.push(Input {
                unit: Some(v.unit.clone()),
                message_index: Some(v.message_index as u32),
                output_index: Some(v.output_index as u32),
                ..Default::default()
            });

            if !send_all && total_amount >= required_amount {
                break;
            }
        }

        if total_amount < required_amount {
            bail!("there is not enough balance, address: {}", paying_address);
        }

        Ok((inputs, total_amount))
    }

    /// build the state from genesis
    /// TODO: also need to rebuild temp state (same as state)
    pub fn rebuild_from_genesis() -> Result<Self> {
        let business_cache = BusinessCache::default();
        let mut mci = Level::new(0);

        while let Ok(next_joints) = SDAG_CACHE.get_joints_by_mci(mci) {
            if next_joints.is_empty() {
                break;
            }

            for joint in next_joints.into_iter() {
                let joint = joint.read()?;

                if joint.get_sequence() == JointSequence::Good {
                    business_cache.apply_stable_joint(&joint)?;
                }
            }
            mci += 1;
        }

        Ok(business_cache)
    }

    /// rebuild from database
    /// TODO: rebuild from database
    /// NOTE: need also update global state and temp business state
    pub fn rebuild_from_db() -> Result<Self> {
        Ok(BusinessCache::default())
    }

    /// validate if contains last stable self unit
    pub fn is_include_last_stable_self_joint(&self, joint: &JointData) -> Result<()> {
        for author in &joint.unit.authors {
            let last_stable_self_unit = self
                .global_state
                .get_last_stable_self_joint(&author.address);

            if let Some(ref unit) = last_stable_self_unit {
                let author_joint = SDAG_CACHE.get_joint(unit)?.read()?;
                // joint is not include author joint
                let included = joint > &*author_joint;
                if !included {
                    bail!("joint not include last stable self unit {}", unit);
                }
            }
        }

        Ok(())
    }

    /// validate unstable joint with no global order
    pub fn validate_unstable_joint(&self, cached_joint: CachedJoint) -> Result<JointSequence> {
        let joint = cached_joint.read()?;
        // global check
        let state = validate_unstable_joint_serial(cached_joint)?;
        if state != JointSequence::Good {
            return Ok(state);
        }

        // for each message do business related validation
        let mut g = self.temp_business_state.write().unwrap();
        for i in 0..joint.unit.messages.len() {
            let state = g.validate_message(&joint, i);
            if let Err(e) = state {
                error!(
                    "validate_unstable_joint, unit = {}, err = {}",
                    joint.unit.unit, e
                );
                // now we only support one message for a unit
                return Ok(JointSequence::TempBad);
            } else {
                // unordered validate pass, apply it
                g.apply_message(&joint, i)?;
            }
        }

        Ok(JointSequence::Good)
    }

    /// validate stable joint with global order
    fn validate_stable_joint(&self, joint: &JointData) -> Result<()> {
        info!("validate_stable_joint, unit={}", joint.unit.unit);
        // TODO: check if enough commission here
        // for each message do business related validation
        if joint.get_sequence() == JointSequence::FinalBad {
            bail!("joint is already set to finalbad, unit={}", joint.unit.unit);
        }

        let business_state = self.business_state.read().unwrap();
        for i in 0..joint.unit.messages.len() {
            business_state.validate_message(joint, i)?;
        }

        Ok(())
    }

    // set joint properties {prev_self_unit, related_units, balance}
    // note: just support one author currently
    // note: genesis {prev_self_unit = None, related_units = vec![], balance = 0}
    fn update_joint_balance_props(&self, joint: &JointData) -> Result<()> {
        let addr = &joint.unit.authors[0].address;

        let (last_stable_self_unit, related_units) = self.global_state.get_global_state(&addr);

        let mut balance = self.global_state.get_stable_balance(addr)?;
        // reduce spend amount
        if !joint.unit.is_genesis_unit() {
            for msg in &joint.unit.messages {
                if let Some(Payload::Payment(ref payment)) = msg.payload {
                    for output in &payment.outputs {
                        if addr != &output.address {
                            balance -= output.amount;
                        }
                    }
                }
            }
            balance -= u64::from(joint.unit.headers_commission.unwrap_or(0));
            balance -= u64::from(joint.unit.payload_commission.unwrap_or(0));
        }

        if let Some(unit) = last_stable_self_unit {
            joint.set_stable_prev_self_unit(unit);
        }
        joint.set_related_units(related_units);
        joint.set_balance(balance);

        // note: the spendable utxo always before the last_stable_unit
        // maybe not all of the balance can be used right now
        Ok(())
    }

    /// apply changes, save the new state
    fn apply_stable_joint(&self, joint: &JointData) -> Result<()> {
        // TODO: deduce the commission

        self.update_joint_balance_props(joint)?;

        // update global state {last_stable_self_joint, related_joints}
        self.global_state.update_global_state(joint);

        let mut business_state = self.business_state.write().unwrap();

        for i in 0..joint.unit.messages.len() {
            business_state.apply_message(joint, i)?;
        }

        Ok(())
    }
}

//---------------------------------------------------------------------------------------
// Global functions
//---------------------------------------------------------------------------------------
pub fn validate_business_basic(unit: &Unit) -> Result<()> {
    validate_headers_commission_recipients(unit)?;

    for i in 0..unit.messages.len() {
        let message = &unit.messages[i];
        validate_message_format(message)?;
        validate_message_payload(message)?;
        BusinessState::validate_message_basic(message)?;
    }

    Ok(())
}

pub fn check_business(joint: &JointData) -> Result<()> {
    // for each message do business related validation
    for i in 0..joint.unit.messages.len() {
        BusinessState::check_business(joint, i)?;
    }
    Ok(())
}

// 1) if has multi authors , unit.earned_headers_commission_recipients must not be empty;
// 2) address of unit.earned_headers_commission_recipients should ordered by address
// 3) total earned_headers_commission_share of unit.earned_headers_commission_recipients must be 100
fn validate_headers_commission_recipients(unit: &Unit) -> Result<()> {
    if unit.authors.len() > 1 && unit.earned_headers_commission_recipients.is_empty() {
        bail!("must specify earned_headers_commission_recipients when more than 1 author");
    }

    if unit.earned_headers_commission_recipients.is_empty() {
        return Ok(());
    }

    let mut total_earned_headers_commission_share = 0;
    let mut prev_address = "".to_owned();
    for recipient in &unit.earned_headers_commission_recipients {
        if recipient.address <= prev_address {
            bail!("recipient list must be sorted by address");
        }
        if !object_hash::is_chash_valid(&recipient.address) {
            bail!("invalid recipient address checksum");
        }
        total_earned_headers_commission_share += recipient.earned_headers_commission_share;
        prev_address = recipient.address.clone();
    }

    if total_earned_headers_commission_share != 100 {
        bail!("sum of earned_headers_commission_share is not 100");
    }

    Ok(())
}

fn validate_message_payload(message: &Message) -> Result<()> {
    if message.payload_hash.len() != config::HASH_LENGTH {
        bail!("wrong payload hash size");
    }

    if message.payload.is_none() {
        bail!("no inline payload");
    }

    let payload_hash = object_hash::get_base64_hash(message.payload.as_ref().unwrap())?;
    if payload_hash != message.payload_hash {
        bail!(
            "wrong payload hash: expected {}, got {}",
            payload_hash,
            message.payload_hash
        );
    }

    Ok(())
}

fn validate_message_format(msg: &Message) -> Result<()> {
    if msg.payload_location != "inline"
        && msg.payload_location != "uri"
        && msg.payload_location != "none"
    {
        bail!("wrong payload location: {}", msg.payload_location);
    }

    if msg.payload_location != "uri" && msg.payload_uri.is_some() && msg.payload_uri_hash.is_some()
    {
        bail!("must not contain payload_uri and payload_uri_hash");
    }

    Ok(())
}

fn validate_unstable_joint_serial(joint: CachedJoint) -> Result<JointSequence> {
    if ::serial_check::is_unstable_joint_non_serial(joint)? {
        return Ok(JointSequence::NonserialBad);
    }

    Ok(JointSequence::Good)
}
