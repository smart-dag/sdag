use business::SubBusiness;
use cache::JointData;
use cache::SDAG_CACHE;
use config;
use error::Result;
use failure::ResultExt;
use hashbrown::{hash_map::Entry, HashMap, HashSet};
use joint::{JointSequence, Level};
use sdag_object_base::object_hash;
use spec::*;
use std::cmp::Ordering;
use std::collections::BTreeMap;

//---------------------------------------------------------------------------------------
// UtxoCache
//---------------------------------------------------------------------------------------
#[derive(Default)]
pub struct UtxoCache {
    //record money that address can spend
    pub output: HashMap<String, BTreeMap<UtxoKey, UtxoData>>,
    // save payload commission earnings  <Key, Amount> NOT USED YET
    pub payload_commission_output: HashMap<PayloadCommissionOutputKey, usize>,
    // save header commission earnings <Key, Amount> NOT USED YET
    pub headers_commission_output: HashMap<HeadersCommissionOutputKey, usize>,
}

pub(super) fn get_output_by_unit(
    unit: &str,
    output_index: usize,
    message_index: usize,
) -> Result<Output> {
    let joint = SDAG_CACHE.get_joint(unit)?.read()?;
    if message_index >= joint.unit.messages.len() {
        bail!(
            "invlide message index for the input, unit={}, msg_idx={}",
            unit,
            message_index
        );
    }
    let message = &joint.unit.messages[message_index];

    match message.payload {
        Some(Payload::Payment(ref payment)) => {
            if output_index >= payment.outputs.len() {
                bail!(
                    "invlide output index for the input, unit={}, msg_idx={}, output_idx={}",
                    unit,
                    message_index,
                    output_index
                );
            }
            Ok(payment.outputs[output_index].clone())
        }

        _ => bail!("address can't find from non payment message"),
    }
}

// basic function about output
impl UtxoCache {
    pub fn revert_output(
        &mut self,
        message: &Message,
        message_index: usize,
        unit: &str,
        utxo_value: UtxoData,
    ) -> Result<()> {
        match message.payload {
            Some(Payload::Payment(ref payment)) => {
                // remove output that have already received
                for (output_index, output) in payment.outputs.iter().enumerate() {
                    self.remove_output(
                        output.address.clone(),
                        &UtxoKey {
                            unit: unit.to_owned(),
                            output_index,
                            message_index,
                            amount: output.amount,
                        },
                    )?;
                }

                // recovery output that have already spent
                for input in &payment.inputs {
                    let output = get_output_by_unit(
                        unit,
                        input.output_index.unwrap() as usize,
                        input.message_index.unwrap() as usize,
                    )?;

                    self.insert_output(
                        output.address.clone(),
                        UtxoKey {
                            unit: input.unit.clone().unwrap(),
                            output_index: input.output_index.unwrap() as usize,
                            message_index: input.message_index.unwrap() as usize,
                            amount: output.amount,
                        },
                        utxo_value,
                    )?;
                }
            }
            _ => bail!("payload is not a payment"),
        }

        Ok(())
    }

    pub fn apply_payment(
        &mut self,
        message: &Message,
        message_index: usize,
        unit: &str,
        utxo_value: UtxoData,
    ) -> Result<()> {
        match message.payload {
            Some(Payload::Payment(ref payment)) => {
                self.decrease_output(&payment.inputs)
                    .context("apply_payment decrease_output failed")?;
                self.increase_output(unit, &payment.outputs, message_index, utxo_value)
                    .context("apply_payment increase_output failed")?;
            }
            _ => bail!("payload is not a payment"),
        }

        Ok(())
    }

    fn decrease_output(&mut self, inputs: &[Input]) -> Result<()> {
        for input in inputs.iter() {
            match input.kind {
                Some(ref kind) if kind == "issue" => continue,
                _ => {}
            }

            let unit = input.unit.as_ref().unwrap();
            let output_index = input.output_index.unwrap() as usize;
            let message_index = input.message_index.unwrap() as usize;

            let (address, amount, _) =
                self.get_output_by_input(unit, output_index, message_index)?;

            let address_key = UtxoKey {
                unit: unit.to_owned(),
                output_index,
                message_index,
                amount,
            };

            self.remove_output(address, &address_key)?;
        }
        Ok(())
    }

    fn increase_output(
        &mut self,
        unit_hash: &str,
        outputs: &[Output],
        message_index: usize,
        utxo_value: UtxoData,
    ) -> Result<()> {
        for (output_index, output) in outputs.iter().enumerate() {
            let address_key = UtxoKey {
                unit: unit_hash.to_owned(),
                output_index,
                message_index,
                amount: output.amount,
            };

            self.insert_output(output.address.clone(), address_key, utxo_value)?;
        }

        Ok(())
    }

    fn remove_output(&mut self, pay_address: String, address_key: &UtxoKey) -> Result<()> {
        match self.output.entry(pay_address) {
            Entry::Occupied(mut utxo) => {
                let is_empty = {
                    let utxo_set = utxo.get_mut();
                    if utxo_set.remove(address_key).is_none() {
                        bail!("no utxo found!");
                    };
                    utxo_set.is_empty()
                };

                if is_empty {
                    // We delete the empty set from the map.
                    utxo.remove();
                }
            }
            _ => bail!("remove_output: invalid paied address"),
        }

        Ok(())
    }

    fn insert_output(
        &mut self,
        earned_address: String,
        utxo_key: UtxoKey,
        utxo_value: UtxoData,
    ) -> Result<()> {
        match self.output.entry(earned_address) {
            Entry::Occupied(mut output) => {
                output.get_mut().insert(utxo_key, utxo_value);
            }
            Entry::Vacant(output) => {
                let mut map = BTreeMap::new();
                map.insert(utxo_key, utxo_value);
                output.insert(map);
            }
        }
        Ok(())
    }

    /// return all available utxo for an address
    pub fn get_utxos_by_address(
        &self,
        paying_address: &str,
    ) -> Option<&BTreeMap<UtxoKey, UtxoData>> {
        self.output.get(paying_address)
    }

    fn get_output_by_input(
        &self,
        unit: &str,
        output_index: usize,
        message_index: usize,
    ) -> Result<(String, u64, Level)> {
        let output = get_output_by_unit(unit, output_index, message_index)?;

        let utxo_data = self
            .output
            .get(&output.address)
            .ok_or_else(|| format_err!("not found address in output: {:?}", output.address))?
            .get(&UtxoKey {
                unit: unit.to_string(),
                output_index,
                message_index,
                amount: output.amount,
            })
            .ok_or_else(|| format_err!("not found utxo about output: unit-{}", unit))?;

        Ok((output.address, output.amount, utxo_data.mci))
    }

    fn verify_transfer_of_input(
        &self,
        input: &Input,
        author_addresses: &[&String],
        input_keys: &mut HashSet<String>,
    ) -> Result<u64> {
        if input.address.is_some()
            || input.amount.is_some()
            || input.from_main_chain_index.is_some()
            || input.serial_number.is_some()
            || input.to_main_chain_index.is_some()
        {
            bail!("unknown fields in payment input")
        }

        match input.unit {
            Some(ref unit) => {
                if unit.len() != config::HASH_LENGTH {
                    bail!("wrong unit length in payment input");
                }
            }
            None => {
                bail!("no unit in payment input");
            }
        }

        if input.message_index.is_none() {
            bail!("no message_index in payment input")
        }

        if input.output_index.is_none() {
            bail!("no output_index in payment input")
        }

        let input_unit = input.unit.as_ref().unwrap();
        let input_message_index = input.message_index.unwrap();
        let input_output_index = input.output_index.unwrap();

        // duplication detection
        let input_key = format!(
            "base-{}-{}-{}",
            input_unit, input_message_index, input_output_index
        );

        if input_keys.contains(&input_key) {
            bail!("input {:?} already used", input_key)
        }
        input_keys.insert(input_key);

        let (output_address, output_amount, _output_mci) = self.get_output_by_input(
            &input_unit.clone(),
            input_output_index as usize,
            input_message_index as usize,
        )?;

        let joint = SDAG_CACHE.get_joint(input_unit)?.read()?;
        if joint.get_sequence() != JointSequence::Good {
            bail!("input unit {} is not serial", input_unit)
        }

        if !joint.is_stable() {
            bail!("input unit {} is not stable", input_unit)
        }

        if !author_addresses.contains(&&output_address) {
            bail!("output owner is not among authors")
        }

        Ok(output_amount)
    }

    fn verify_issue_of_input(
        &self,
        input: &Input,
        index: usize,
        author_addresses: &[&String],
        unit: &Unit,
        input_keys: &mut HashSet<String>,
    ) -> Result<u64> {
        if index != 0 {
            bail!("issue must come first")
        }

        if !unit.is_genesis_unit() {
            bail!("only genesis can issue base asset")
        }

        if input.unit.is_some()
            || input.message_index.is_some()
            || input.output_index.is_some()
            || input.from_main_chain_index.is_some()
            || input.to_main_chain_index.is_some()
        {
            bail!("verify_issue_of_input: unknown fields in payment input")
        }

        if input.amount <= Some(0) {
            bail!("amount must be positive")
        }

        if input.serial_number != Some(1) {
            bail!("only one issue per message allowed")
        }

        // Note: we already validate author, so it must not empty
        let address = if author_addresses.len() == 1 {
            match input.address {
                Some(_) => bail!(
                    "when single-authored,\
                     must not put address in issue input"
                ),
                None => &author_addresses[0],
            }
        } else {
            match input.address {
                None => bail!("when multi-authored, must put address in issue input"),
                Some(ref input_address) => {
                    if !author_addresses.contains(&input_address) {
                        bail!("issue input address {} is not an author", input_address)
                    }

                    input_address
                }
            }
        };

        if input.amount != Some(config::TOTAL_WHITEBYTES) {
            bail!("issue must be equal to cap")
        }

        // duplication detection
        let input_key = format!("base-{}-{}", address, input.serial_number.unwrap_or(0),);

        if input_keys.contains(&input_key) {
            bail!("input {} already used", input_key)
        }
        input_keys.insert(input_key);

        Ok(input.amount.unwrap())
    }
}

impl UtxoCache {
    // TODO: impl spend header commission #57
    // TODO: impl spend header commission #58
    // TODO: refine Payment structure
    // Note: in future we would use account model to record one usize balance for each address
    // thus we don't need to save that in this big table
    #[allow(dead_code)]
    fn save_payload_commission(
        &mut self,
        address: String,
        mci: Level,
        amount: usize,
    ) -> Result<()> {
        let key = PayloadCommissionOutputKey { mci, address };
        if self.payload_commission_output.get(&key).is_some() {
            bail!("already have key={:?} in payload commission output", key);
        }
        self.payload_commission_output.insert(key, amount);
        Ok(())
    }

    #[allow(dead_code)]
    fn save_header_commission(&mut self, address: String, mci: Level, amount: usize) -> Result<()> {
        let key = HeadersCommissionOutputKey { mci, address };
        if self.headers_commission_output.get(&key).is_some() {
            bail!("already have key={:?} in headers commission output", key);
        }
        self.headers_commission_output.insert(key, amount);
        Ok(())
    }

    fn verify_output(&self, outputs: &[Output]) -> Result<u64> {
        let mut total_output = 0;
        let mut prev_address = String::new();
        let mut prev_amount = 0;

        for output in outputs {
            if output.amount == 0 {
                bail!("amount must be positive integer, found {:?}", output.amount)
            }

            let amount = output.amount;
            let address = &output.address;

            if !object_hash::is_chash_valid(address) {
                bail!("output address {} invalid", address)
            }

            if prev_address > *address {
                bail!("output addresses not sorted");
            } else if &prev_address == address && prev_amount > amount {
                bail!("output amounts for same address not sorted");
            }

            prev_address = address.clone();
            prev_amount = amount;

            total_output += amount;
        }
        Ok(total_output)
    }

    //returned value: (output_address, output_amount, output_mci)
    fn verify_input(
        &self,
        inputs: &[Input],
        author_addresses: Vec<&String>,
        unit: &Unit,
    ) -> Result<u64> {
        let transfer = String::from("transfer");
        let mut input_keys = HashSet::new();
        let mut total_input: u64 = 0;

        for (index, input) in inputs.iter().enumerate() {
            let kind = input.kind.as_ref().unwrap_or(&transfer);

            match kind.as_str() {
                "transfer" => {
                    let amount =
                        self.verify_transfer_of_input(input, &author_addresses, &mut input_keys)?;
                    total_input += amount;
                }

                "issue" => {
                    let amount = self.verify_issue_of_input(
                        input,
                        index,
                        &author_addresses,
                        unit,
                        &mut input_keys,
                    )?;

                    total_input += amount;
                }
                _ => unimplemented!(),
            }
        }
        Ok(total_input)
    }

    fn validate_payment_inputs_and_outputs(&self, payment: &Payment, unit: &Unit) -> Result<()> {
        let author_addresses = unit.authors.iter().map(|a| &a.address).collect::<Vec<_>>();

        let total_output = self.verify_output(&payment.outputs)?;
        let total_input = self.verify_input(&payment.inputs, author_addresses, unit)?;

        if total_input
            != total_output
                + u64::from(unit.headers_commission.unwrap_or(0))
                + u64::from(unit.payload_commission.unwrap_or(0))
        {
            bail!(
                "inputs and outputs do not balance: {} != {} + {} + {}",
                total_input,
                total_output,
                unit.headers_commission.unwrap_or(0),
                unit.payload_commission.unwrap_or(0)
            )
        }

        Ok(())
    }
}

impl SubBusiness for UtxoCache {
    fn validate_message_basic(message: &Message) -> Result<()> {
        validate_payment_format(message)
    }

    fn check_business(joint: &JointData, message_idx: usize) -> Result<()> {
        let last_ball_unit = match joint.unit.last_ball_unit {
            Some(ref unit) => unit,
            None => return Ok(()), // genesis
        };

        let last_ball = SDAG_CACHE.get_joint(last_ball_unit)?.read()?;

        let message = &joint.unit.messages[message_idx];

        match message.payload {
            Some(Payload::Payment(ref payment)) => {
                for input in &payment.inputs {
                    if input.kind.as_ref().unwrap_or(&"transfer".to_string()) == "transfer" {
                        let src_joint =
                            SDAG_CACHE.get_joint(input.unit.as_ref().unwrap())?.read()?;

                        let is_included = *src_joint <= *last_ball;
                        if !is_included {
                            bail!("src output must be before last ball")
                        }
                    }
                }
            }
            Some(_) => {}
            _ => unreachable!(),
        }
        Ok(())
    }

    fn validate_message(&self, joint: &JointData, message_idx: usize) -> Result<()> {
        let message = &joint.unit.messages[message_idx];

        match message.payload {
            Some(Payload::Payment(ref payment)) => {
                self.validate_payment_inputs_and_outputs(payment, &joint.unit)
            }
            _ => bail!("validate_message end\npayload is not a payment"),
        }
    }

    fn apply_message(&mut self, joint: &JointData, message_idx: usize) -> Result<()> {
        let unit_hash = &joint.unit.unit;
        let message = &joint.unit.messages[message_idx];
        let utxo_value = UtxoData {
            mci: joint.get_mci(),
            sub_mci: joint.get_sub_mci(),
        };
        self.apply_payment(message, message_idx, &unit_hash, utxo_value)?;

        Ok(())
    }

    fn revert_message(&mut self, joint: &JointData, message_idx: usize) -> Result<()> {
        let utxo_value = UtxoData {
            mci: joint.get_mci(),
            sub_mci: joint.get_sub_mci(),
        };
        let unit_hash = &joint.unit.unit;
        let message = &joint.unit.messages[message_idx];
        self.revert_output(message, message_idx, unit_hash, utxo_value)
    }
}

//---------------------------------------------------------------------------------------
// UtxoKey
//---------------------------------------------------------------------------------------
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct UtxoKey {
    pub unit: String,
    pub output_index: usize,
    pub message_index: usize,
    pub amount: u64,
}

impl Ord for UtxoKey {
    fn cmp(&self, other: &UtxoKey) -> Ordering {
        match Ord::cmp(&self.amount, &other.amount) {
            Ordering::Equal => {}
            r => return r,
        }
        match Ord::cmp(&self.unit, &other.unit) {
            Ordering::Equal => {}
            r => return r,
        }
        match Ord::cmp(&self.message_index, &other.message_index) {
            Ordering::Equal => {}
            r => return r,
        }
        Ord::cmp(&self.output_index, &other.output_index)
    }
}

impl PartialOrd for UtxoKey {
    fn partial_cmp(&self, other: &UtxoKey) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

//---------------------------------------------------------------------------------------
// UtxoData
//---------------------------------------------------------------------------------------
#[derive(Clone, Debug, Copy)]
pub struct UtxoData {
    pub mci: Level,
    pub sub_mci: Level,
}

//---------------------------------------------------------------------------------------
// HeadersCommissionOutputKey
//---------------------------------------------------------------------------------------
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct HeadersCommissionOutputKey {
    pub mci: Level,
    pub address: String,
}

//---------------------------------------------------------------------------------------
// PayloadCommissionOutputKey
//---------------------------------------------------------------------------------------
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct PayloadCommissionOutputKey {
    pub mci: Level,
    pub address: String,
}

//---------------------------------------------------------------------------------------
// Global functions
//---------------------------------------------------------------------------------------

fn validate_payment_format(message: &Message) -> Result<()> {
    if message.payload_location != "inline" {
        bail!("payment location must be inline");
    }

    if !message.spend_proofs.is_empty() {
        bail!("private payment not supported");
    }

    match message.payload {
        Some(Payload::Payment(ref payment)) => {
            if payment.asset.is_some() {
                bail!("We do not handle assets for now")
            }

            if payment.address.is_some()
                || payment.definition_chash.is_some()
                || payment.denomination.is_some()
            {
                bail!("validate_payment_format: unknown fields in payment message")
            }

            if payment.inputs.len() > config::MAX_INPUTS_PER_PAYMENT_MESSAGE
                || payment.outputs.len() > config::MAX_OUTPUTS_PER_PAYMENT_MESSAGE
            {
                bail!(
                    "too many inputs {} or output {}",
                    payment.inputs.len(),
                    payment.outputs.len()
                )
            }
        }
        _ => bail!("validate_payment_format: not payment"),
    }

    Ok(())
}
