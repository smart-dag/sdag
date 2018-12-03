use error::Result;
use may::sync::Mutex;

#[allow(dead_code)]
const MAX_HISTORY_ITEMS: usize = 1000;

lazy_static! {
    static ref LIGHT_JOINTS: Mutex<()> = Mutex::new(());
}

#[derive(Serialize, Deserialize)]
pub struct HistoryRequest {
    pub address: String,
    #[serde(default)]
    pub known_stable_units: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Transaction {
    pub unit_hash: String,
    pub from_addr: String,
    pub to_addr: String,
    pub amount: u64,
    pub time: u64,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct HistoryResponse {
    pub transaction_history: Vec<Transaction>,
}

#[derive(Serialize, Deserialize)]
pub struct InputsRequest {
    pub address: String,
    pub amount: usize,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct LightProps {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_stable_mc_ball: Option<String>,
    pub last_stable_mc_ball_mci: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_stable_mc_ball_unit: Option<String>,
    pub parent_units: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub witness_list_unit: Option<String>,
}
// #[derive(Debug, Serialize, Deserialize)]
// struct ProofBalls {
//     ball: String,
//     unit: String,
//     #[serde(default)]
//     parent_balls: Vec<String>,
//     #[serde(skip_serializing_if = "Option::is_none")]
//     content_hash: Option<String>,
//     #[serde(skip_serializing_if = "Option::is_none")]
//     is_nonserial: Option<bool>,
//     #[serde(default)]
//     #[serde(skip_serializing_if = "Vec::is_empty")]
//     skiplist_balls: Vec<String>,
// }

pub fn prepare_history(_history_request: &HistoryRequest) -> Result<HistoryResponse> {
    unimplemented!()
}
