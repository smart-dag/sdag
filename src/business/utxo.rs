use std::cmp::Ordering;
use std::collections::HashMap;

use business::SubBusiness;
use cache::JointData;
use cache::SDAG_CACHE;
use config;
use error::Result;
use joint::Level;
use spec::*;

use super::output::UtxOutput;
//---------------------------------------------------------------------------------------
// UtxoCache
//---------------------------------------------------------------------------------------
#[derive(Default)]
pub struct UtxoCache {
    //record money that address can spend
    output: UtxOutput,
    // save payload commission earnings  <Key, Amount> NOT USED YET
    payload_commission_output: HashMap<PayloadCommissionOutputKey, usize>,
    // save header commission earnings <Key, Amount> NOT USED YET
    headers_commission_output: HashMap<HeadersCommissionOutputKey, usize>,
}

pub trait OutputOperation {
    fn verify_output(&self, outputs: &Vec<Output>) -> Result<usize>;

    fn verify_input(
        &self,
        inputs: &Vec<Input>,
        author_addresses: Vec<&String>,
        unit: &Unit,
    ) -> Result<usize>;
}

pub fn get_output_by_unit(utxo_key: &UtxoKey) -> Result<Output> {
    let joint = SDAG_CACHE.get_joint(&utxo_key.unit)?.read()?;
    let message = &joint.unit.messages[utxo_key.message_index];

    match message.payload {
        Some(Payload::Payment(ref payment)) => Ok(payment.outputs[utxo_key.output_index].clone()),

        _ => bail!("address can't find from non payment message"),
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
        if let Some(_) = self.payload_commission_output.get(&key) {
            bail!("already have key={:?} in payload commission output", key);
        }
        self.payload_commission_output.insert(key, amount);
        Ok(())
    }

    #[allow(dead_code)]
    fn save_header_commission(&mut self, address: String, mci: Level, amount: usize) -> Result<()> {
        let key = HeadersCommissionOutputKey { mci, address };
        if let Some(_) = self.headers_commission_output.get(&key) {
            bail!("already have key={:?} in headers commission output", key);
        }
        self.headers_commission_output.insert(key, amount);
        Ok(())
    }

    fn validate_payment_inputs_and_outputs(&self, payment: &Payment, unit: &Unit) -> Result<()> {
        let author_addresses = unit.authors.iter().map(|a| &a.address).collect::<Vec<_>>();

        let total_output = self.output.verify_output(&payment.outputs)?;
        let total_input = self
            .output
            .verify_input(&payment.inputs, author_addresses, unit)?;

        if total_input
            != total_output
                + unit.headers_commission.unwrap_or(0) as usize
                + unit.payload_commission.unwrap_or(0) as usize
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
            denomination: 1,
            amount: None,
        };
        self.output
            .apply_payment(message, message_idx, &unit_hash, utxo_value)?;

        Ok(())
    }

    fn revert_message(&mut self, joint: &JointData, message_idx: usize) -> Result<()> {
        let utxo_value = UtxoData {
            mci: joint.get_mci(),
            sub_mci: joint.get_sub_mci(),
            denomination: 1,
            amount: None,
        };
        let unit_hash = &joint.unit.unit;
        let message = &joint.unit.messages[message_idx];
        self.output
            .revert_output(message, message_idx, unit_hash, utxo_value)
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
}

impl Ord for UtxoKey {
    fn cmp(&self, other: &UtxoKey) -> Ordering {
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
    pub denomination: u32,
    pub amount: Option<usize>,
}

//---------------------------------------------------------------------------------------
// HeadersCommissionOutputKey
//---------------------------------------------------------------------------------------
#[derive(Debug, PartialEq, Eq, Hash)]
struct HeadersCommissionOutputKey {
    mci: Level,
    address: String,
}

//---------------------------------------------------------------------------------------
// PayloadCommissionOutputKey
//---------------------------------------------------------------------------------------
#[derive(Debug, PartialEq, Eq, Hash)]
struct PayloadCommissionOutputKey {
    mci: Level,
    address: String,
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
                bail!("too many inputs or output")
            }
        }
        _ => bail!("validate_payment_format: not payment"),
    }

    Ok(())
}
