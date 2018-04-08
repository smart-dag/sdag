use error::Result;
use joint::Joint;
use may::sync::Mutex;

#[allow(dead_code)]
const MAX_HISTORY_ITEMS: usize = 1000;

lazy_static! {
    static ref LIGHT_JOINTS: Mutex<()> = Mutex::new(());
}

#[derive(Serialize, Deserialize)]
pub struct HistoryRequest {
    pub witnesses: Vec<String>,
    #[serde(default)]
    pub addresses: Vec<String>,
    #[serde(default)]
    pub known_stable_units: Vec<String>,
    #[serde(default)]
    pub requested_joints: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HistoryResponse {
    #[serde(default)]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    unstable_mc_joints: Vec<Joint>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    witness_change_and_definition_joints: Vec<Joint>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    joints: Vec<Joint>,
    // #[serde(default)]
    // #[serde(skip_serializing_if = "Vec::is_empty")]
    // proofchain_balls: Vec<ProofBalls>,
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
