use std::collections::HashMap as StdHashMap;
use std::net::ToSocketAddrs;
use std::sync::Arc;
use std::time::Duration;

use super::network_base::{Sender, Server, WsConnection};
use config;
use error::Result;
use joint::Joint;
use joint::JointProperty;
use light;
use may::coroutine;
use may::net::TcpStream;
use may::sync::Semphore;
use serde_json::{self, Value};
use tungstenite::client::client;
use tungstenite::handshake::client::Request;
use tungstenite::protocol::Role;
use url::Url;

//---------------------------------------------------------------------------------------
// WalletData
//---------------------------------------------------------------------------------------
pub struct WalletData {
    init_done: Semphore,
}

impl WalletData {
    fn wait_init_done(&self) -> Result<()> {
        if !self.init_done.wait_timeout(Duration::from_secs(5)) {
            bail!("wait_init_done timeout")
        }
        Ok(())
    }

    fn trigger_init_done(&self) {
        self.init_done.post();
    }
}

impl Default for WalletData {
    fn default() -> Self {
        WalletData {
            init_done: Semphore::new(0),
        }
    }
}
impl Server<WalletData> for WalletData {
    fn on_message(ws: Arc<WalletConn>, subject: String, body: Value) -> Result<()> {
        match subject.as_str() {
            "version" => ws.on_version(body)?,
            subject => error!("on_message unknown subject: {}", subject),
        }
        Ok(())
    }

    fn on_request(ws: Arc<WalletConn>, command: String, params: Value) -> Result<Value> {
        let response = match command.as_str() {
            "heartbeat" => ws.on_heartbeat(params)?,
            "subscribe" => ws.on_subscribe(params)?,
            command => bail!("on_request unknown command: {}", command),
        };
        Ok(response)
    }

    fn close(_ws: Arc<WalletConn>) {}
}

//---------------------------------------------------------------------------------------
// WalletConn
//---------------------------------------------------------------------------------------
pub type WalletConn = WsConnection<WalletData>;

impl WalletConn {
    fn send_version(&self) -> Result<()> {
        self.send_just_saying(
            "version",
            json!({
                "protocol_version": config::VERSION,
                "alt": config::ALT,
                "library": config::LIBRARY,
                "library_version": config::LIBRARY_VERSION,
                "program": "rust-sdag-sdg",
                "program_version": "0.1.0"
            }),
        )
    }

    fn send_heartbeat(&self) -> Result<()> {
        self.send_request("heartbeat", &Value::Null)?;
        Ok(())
    }

    pub fn post_joint(&self, joint: &Joint) -> Result<()> {
        self.send_request("post_joint", &serde_json::to_value(joint)?)?;
        Ok(())
    }

    pub fn get_inputs_from_hub(
        &self,
        paid_address: &str,
        total_amount: u64,
        is_spend_all: bool,
        last_stable_unit: &str,
    ) -> Result<light::InputsResponse> {
        let inputs_response = self.send_request(
            "light/inputs",
            &serde_json::to_value(light::InputsRequest {
                paid_address: paid_address.to_owned(),
                total_amount,
                is_spend_all,
                last_stable_unit: last_stable_unit.to_owned(),
            })?,
        )?;

        Ok(serde_json::from_value(inputs_response)?)
    }

    //returned spendable the number of coins
    pub fn get_balance(&self, address: &str) -> Result<u64> {
        let response = self.send_request("get_balance", &serde_json::to_value(address)?)?;
        let balance = response["balance"]
            .as_u64()
            .ok_or_else(|| format_err!("get balance failed"))?;

        Ok(balance)
    }

    // get the network status
    pub fn get_net_state(&self) -> Result<super::hub::HubNetState> {
        let response = self.send_request("net_state", &Value::Null)?;
        Ok(serde_json::from_value(response)?)
    }

    // get the network statistics
    pub fn get_net_statistics(&self) -> Result<StdHashMap<String, ::statistics::LastConnStat>> {
        let response = self.send_request("net_statistics", &Value::Null)?;
        Ok(serde_json::from_value(response)?)
    }

    //returned joint and joint property
    pub fn get_joint_by_unit_hash(&self, unit: &str) -> Result<(Joint, JointProperty)> {
        let mut response =
            self.send_request("get_joint_by_unit_hash", &serde_json::to_value(unit)?)?;

        let joint: Joint = serde_json::from_value(response["joint"].take())?;

        let property: JointProperty = serde_json::from_value(response["property"].take())?;

        Ok((joint, property))
    }

    //returned free joint list
    pub fn get_free_joints(&self) -> Result<Vec<String>> {
        let response = self.send_request("get_free_joints", &Value::Null)?;

        Ok(serde_json::from_value(response)?)
    }

    //returned missing joint list
    pub fn get_missing_joints(&self) -> Result<Vec<String>> {
        let response = self.send_request("get_missing_joints", &Value::Null)?;

        Ok(serde_json::from_value(response)?)
    }

    /// get those knows bad joints which are not in normal joints
    pub fn get_bad_joints(&self) -> Result<Vec<String>> {
        let response = self.send_request("get_bad_joints", &Value::Null)?;

        Ok(serde_json::from_value(response)?)
    }

    /// get those bad joints which are in normal joints
    /// including temp-bad | nonserial-bad
    pub fn get_temp_bad_joints(&self) -> Result<Vec<String>> {
        let response = self.send_request("get_temp_bad_joints", &Value::Null)?;

        Ok(serde_json::from_value(response)?)
    }

    // return all children of a unit
    pub fn get_children(&self, unit: &str) -> Result<Vec<String>> {
        let response = self.send_request("get_children", &serde_json::to_value(unit)?)?;

        Ok(serde_json::from_value(response)?)
    }

    pub fn get_joints_info(&self) -> Result<light::NumOfUnit> {
        let response = self.send_request("get_joints_info", &Value::Null)?;

        Ok(serde_json::from_value(response)?)
    }

    //returned joints by mci, -1 would return all free joints
    pub fn get_joints_by_mci(&self, mci: isize) -> Result<Vec<Joint>> {
        let mut response = self.send_request("get_joints_by_mci", &serde_json::to_value(mci)?)?;

        let joints: Vec<Joint> = serde_json::from_value(response["joints"].take())?;

        Ok(joints)
    }

    //returned joints by level
    pub fn get_joints_by_level(&self, min_level: usize, max_level: usize) -> Result<Vec<String>> {
        let response = self.send_request(
            "get_joints_by_level",
            &json!({"min_level": min_level, "max_level": max_level}),
        )?;

        let joints: Vec<Vec<::explore::DisplayUnit>> = serde_json::from_value(response)?;

        Ok(joints
            .into_iter()
            .flat_map(|v| v.into_iter().map(|j| j.unit))
            .collect())
    }

    pub fn get_latest_history(
        &self,
        address: String,
        num: usize,
    ) -> Result<light::HistoryResponse> {
        let response = self.send_request(
            "light/get_history",
            &serde_json::to_value(light::HistoryRequest { address, num })?,
        )?;

        Ok(serde_json::from_value(response)?)
    }

    pub fn get_light_props(&self, address: &str) -> Result<light::LightProps> {
        let light_prop = self.send_request("light/light_props", &serde_json::to_value(address)?)?;

        Ok(serde_json::from_value(light_prop)?)
    }

    pub fn get_witnesses(&self) -> Result<Vec<String>> {
        let witnesses = self.send_request("get_witnesses", &Value::Null)?;
        Ok(serde_json::from_value(witnesses)?)
    }
}

// the server side impl
impl WalletConn {
    fn on_version(&self, version: Value) -> Result<()> {
        if version["protocol_version"].as_str() != Some(config::VERSION) {
            error!("Incompatible versions, mine {}", config::VERSION);
        }

        if version["alt"].as_str() != Some(config::ALT) {
            error!("Incompatible alt, mine {}", config::ALT);
        }

        info!("got peer version: {}", version);
        Ok(())
    }

    fn on_heartbeat(&self, _: Value) -> Result<Value> {
        Ok(Value::Null)
    }

    fn on_subscribe(&self, _param: Value) -> Result<Value> {
        self.get_data().trigger_init_done();
        // TODO: use wallet address as the peer_id
        let self_id = ::object_hash::gen_random_string(30);
        Ok(json!({ "peer_id": self_id }))
    }
}

//---------------------------------------------------------------------------------------
// Global Functions
//--------------------------------------------------------------------------------------
fn init_connection(ws: &Arc<WalletConn>) -> Result<()> {
    use rand::{thread_rng, Rng};
    // wait for some time for server ready
    coroutine::sleep(Duration::from_millis(1));
    ws.send_version()?;

    let mut rng = thread_rng();
    let n: u64 = rng.gen_range(0, 1000);
    let ws_c = Arc::downgrade(ws);

    // start the heartbeat timer for each connection
    go!(move || loop {
        coroutine::sleep(Duration::from_millis(3000 + n));
        let ws = match ws_c.upgrade() {
            Some(ws) => ws,
            None => return,
        };
        if ws.get_last_recv_tm().elapsed() < Duration::from_secs(5) {
            continue;
        }
        // heartbeat failed so just close the connection
        let rsp = ws.send_heartbeat();
        if rsp.is_err() {
            error!("heartbeat err= {}", rsp.unwrap_err());
            return;
        }
    });

    // wait for hub/challenge done for hand shake
    ws.get_data().wait_init_done()
}

pub fn create_outbound_conn<A: ToSocketAddrs>(address: A) -> Result<Arc<WalletConn>> {
    let stream = TcpStream::connect(address)?;
    let peer = match stream.peer_addr() {
        Ok(addr) => addr.to_string(),
        Err(_) => "unknown peer".to_owned(),
    };
    let url = Url::parse("wss://localhost/")?;
    let req = Request::from(url);
    let (conn, _) = client(req, stream)?;

    let ws = WsConnection::new(conn, WalletData::default(), peer, Role::Client)?;

    init_connection(&ws)?;
    Ok(ws)
}
