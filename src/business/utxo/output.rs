use std::collections::{hash_map::Entry, BTreeMap, HashMap, HashSet};

use cache::SDAG_CACHE;
use config;
use error::Result;
use failure::ResultExt;
use joint::{JointSequence, Level};
use object_hash;
use spec::*;

use super::*;

#[derive(Default)]
pub struct UtxOutput {
    output: HashMap<String, BTreeMap<UtxoKey, UtxoData>>,
}

impl UtxOutput {
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
                            amount: output.amount as usize,
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
                            amount: output.amount as usize,
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

    fn decrease_output(&mut self, inputs: &Vec<Input>) -> Result<()> {
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
                unit: input
                    .unit
                    .clone()
                    .ok_or_else(|| format_err!("decrease_output: input.unit is none"))?,
                output_index: input
                    .output_index
                    .ok_or_else(|| format_err!("decrease_output: input.output_index is none"))?
                    as usize,
                message_index: input
                    .message_index
                    .ok_or_else(|| format_err!("decrease_output: input.message_index is none"))?
                    as usize,
                amount: amount,
            };

            self.remove_output(address, &address_key)?;
        }
        Ok(())
    }

    fn increase_output(
        &mut self,
        unit_hash: &str,
        outputs: &Vec<Output>,
        message_index: usize,
        utxo_value: UtxoData,
    ) -> Result<()> {
        for (output_index, output) in outputs.iter().enumerate() {
            let address_key = UtxoKey {
                unit: unit_hash.to_owned(),
                output_index,
                message_index,
                amount: output.amount as usize,
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
                    if let None = utxo_set.remove(address_key) {
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

    fn get_output_by_input(
        &self,
        unit: &str,
        output_index: usize,
        message_index: usize,
    ) -> Result<(String, usize, Level)> {
        let output = get_output_by_unit(unit, output_index, message_index)?;

        let utxo_data = self
            .output
            .get(&output.address)
            .ok_or_else(|| format_err!("not found address in output: {:?}", output.address))?
            .get(&UtxoKey {
                unit: unit.to_string(),
                output_index,
                message_index,
                amount: output.amount as usize,
            })
            .ok_or_else(|| format_err!("not found utxo about output: unit-{}", unit))?;

        Ok((output.address, output.amount as usize, utxo_data.mci))
    }

    fn verify_transfer_of_input(
        &self,
        input: &Input,
        author_addresses: &Vec<&String>,
        input_keys: &mut HashSet<String>,
    ) -> Result<(usize)> {
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
        author_addresses: &Vec<&String>,
        unit: &Unit,
        input_keys: &mut HashSet<String>,
    ) -> Result<usize> {
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

        Ok(input.amount.unwrap() as usize)
    }
}

impl OutputOperation for UtxOutput {
    fn verify_output(&self, outputs: &Vec<Output>) -> Result<usize> {
        let mut total_output = 0;
        let mut prev_address = String::new();
        let mut prev_amount = 0;

        for output in outputs {
            if output.amount <= 0 {
                bail!("amount must be positive integer, found {:?}", output.amount)
            }

            let amount = output.amount;
            let address = &output.address;

            if !object_hash::is_chash_valid(address) {
                bail!("output address {} invalid", address)
            }

            if &prev_address > address {
                bail!("output addresses not sorted");
            } else if &prev_address == address && prev_amount > amount {
                bail!("output amounts for same address not sorted");
            }

            prev_address = address.clone();
            prev_amount = amount;

            total_output += amount;
        }
        Ok(total_output as usize)
    }

    //returned value: (output_address, output_amount, output_mci)

    fn verify_input(
        &self,
        inputs: &Vec<Input>,
        author_addresses: Vec<&String>,
        unit: &Unit,
    ) -> Result<usize> {
        let transfer = String::from("transfer");
        let mut input_keys = HashSet::new();
        let mut total_input: usize = 0;

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
}
