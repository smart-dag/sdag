use error::Result;

use business::BUSINESS_CACHE;
use cache::SDAG_CACHE;
use spec::{Input, Payload, Unit};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LightProps {
    pub last_ball: String,
    pub last_ball_unit: String,
    pub parent_units: Vec<String>,
    pub witness_list_unit: String,
    pub has_definition: bool,
}

#[derive(Serialize, Deserialize)]
pub struct NumOfUnit {
    pub valid_unit: usize,
    pub known_bad: usize,
    pub temp_bad: usize,
    pub unhandled: usize,
    pub last_stable_mci: ::joint::Level,
}

#[derive(Serialize, Deserialize)]
pub struct HistoryRequest {
    pub address: String,
    #[serde(default)]
    pub num: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TransactionInfo {
    pub unit_hash: String,
    pub from_addr: String,
    pub to_addr: String,
    pub amount: i64,
    pub time: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HistoryResponse {
    pub transactions: Vec<TransactionInfo>,
}

#[derive(Serialize, Deserialize)]
pub struct InputsRequest {
    pub paid_address: String,
    pub total_amount: u64,
    pub is_spend_all: bool,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct InputsResponse {
    pub inputs: Vec<Input>,
    pub amount: u64,
}
pub fn get_inputs_for_amount(input_request: InputsRequest) -> Result<InputsResponse> {
    let InputsRequest {
        paid_address,
        total_amount,
        is_spend_all,
    } = input_request;

    let (inputs, amount) =
        BUSINESS_CACHE.get_inputs_for_amount(&paid_address, total_amount, is_spend_all)?;

    Ok(InputsResponse { inputs, amount })
}

/// get history by address, return transactions
pub fn get_latest_history(history_request: &HistoryRequest) -> Result<HistoryResponse> {
    // note: just support get stable history currently
    // let mut unstable_txs = get_unstable_history(history_request, history_request.num);
    // let mut stable_txs = get_stable_history(history_request, limit - unstable_txs.len());
    // unstable_txs.append(&mut stable_txs);

    Ok(HistoryResponse {
        transactions: get_stable_history(history_request)?,
    })
}

/// get transactions from unstable joints
fn _get_unstable_history(
    _history_request: &HistoryRequest,
    _need_tx_count: usize,
) -> Vec<TransactionInfo> {
    unimplemented!()
}

/// get transactions from stable joints
fn get_stable_history(history_request: &HistoryRequest) -> Result<Vec<TransactionInfo>> {
    let address = &history_request.address;
    let num = history_request.num;

    let mut transactions = Vec::new();

    // receive money from others
    // history range (last_stable_self_joint, last_stable_joint]
    for unit in BUSINESS_CACHE.global_state.get_related_joints(address) {
        let related_joint_data = SDAG_CACHE.get_joint(&unit)?.read()?;
        if get_receive_tx(&related_joint_data.unit, address, num, &mut transactions) {
            return Ok(transactions);
        }
    }

    // history range (known_stable_self_units, last_stable_self_joint]
    // last_stable_self_joint must is not None if the address has sent a joint
    let mut self_unit = BUSINESS_CACHE
        .global_state
        .get_last_stable_self_joint(&address);

    while let Some(last_self_unit) = self_unit {
        let self_joint_data = SDAG_CACHE.get_joint(&last_self_unit)?.read()?;

        fn is_authored_by_address(unit: &Unit, address: &str) -> bool {
            for author in unit.authors.iter() {
                if author.address == address {
                    return true;
                }
            }
            false
        }

        if !is_authored_by_address(&self_joint_data.unit, address) {
            panic!("last self unit first author is not address {}", address);
        }

        // send money to others
        for msg in &self_joint_data.unit.messages {
            if let Some(Payload::Payment(ref payment)) = msg.payload {
                for output in &payment.outputs {
                    // skip ourself change
                    if &output.address == address {
                        continue;
                    }

                    transactions.push(TransactionInfo {
                        unit_hash: last_self_unit.clone(),
                        from_addr: address.clone(),
                        to_addr: output.address.clone(),
                        amount: output.amount as i64,
                        time: self_joint_data.unit.timestamp,
                    });

                    if transactions.len() >= num {
                        return Ok(transactions);
                    }
                }
            }
        }

        // receive money from others
        let related_units = self_joint_data.get_related_units();
        for unit in related_units {
            let related_joint_data = SDAG_CACHE.get_joint(&unit)?.read()?;
            if get_receive_tx(&related_joint_data.unit, address, num, &mut transactions) {
                return Ok(transactions);
            }
        }

        self_unit = self_joint_data.get_stable_prev_self_unit();
    }

    Ok(transactions)
}

/// get Transactions from outputs of unit
/// return true if find all needed tx
fn get_receive_tx(
    unit: &Unit,
    address: &str,
    need_tx_count: usize,
    txs: &mut Vec<TransactionInfo>,
) -> bool {
    for msg in &unit.messages {
        if let Some(Payload::Payment(ref payment)) = msg.payload {
            for output in &payment.outputs {
                if output.address == address {
                    txs.push(TransactionInfo {
                        unit_hash: unit.unit.clone(),
                        from_addr: unit.authors[0].address.clone(), // just support one author currently
                        to_addr: address.to_owned(),
                        amount: output.amount as i64,
                        time: unit.timestamp,
                    });

                    if txs.len() >= need_tx_count {
                        return true;
                    }
                }
            }
        }
    }

    false
}
