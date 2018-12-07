use std::net::ToSocketAddrs;
use std::sync::Arc;
use std::time::Duration;

use super::network::{Sender, Server, WsConnection};
use config;
use error::Result;
use joint::Joint;
use may::coroutine;
use may::net::TcpStream;
use spec::Input;

use light::*;
use serde_json::{self, Value};
use tungstenite::client::client;
use tungstenite::handshake::client::Request;
use tungstenite::protocol::Role;
use url::Url;

#[derive(Default)]
pub struct WalletData {}

pub type WalletConn = WsConnection<WalletData>;

fn init_connection(ws: &Arc<WalletConn>) -> Result<()> {
    use rand::{thread_rng, Rng};

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
        }
    });

    Ok(())
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

    pub fn get_inputs_from_hub(&self, address: String, amount: u64) -> Result<Vec<Input>> {
        let inputs = self.send_request(
            "light/inputs",
            &serde_json::to_value(InputsRequest {
                address,
                amount,
                is_spend_all: false,
            })?,
        )?;

        Ok(serde_json::from_value(inputs).unwrap())
    }
    //returned spendable the number of coins
    pub fn get_balance(&self, address: &String) -> Result<u64> {
        let response = self.send_request("get_balance", &serde_json::to_value(address)?)?;
        let balance = response["balance"]
            .as_u64()
            .ok_or_else(|| format_err!("get balance failed"))?;

        Ok(balance)
    }

    pub fn get_latest_history(&self, address: String, num: usize) -> Result<HistoryResponse> {
        let response = self.send_request(
            "light/get_history",
            &serde_json::to_value(HistoryRequest { address, num })?,
        )?;

        Ok(serde_json::from_value(response).unwrap())
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

    fn on_hub_challenge(&self, param: Value) -> Result<()> {
        // TODO: add special wallet logic here
        // this is hub, we do nothing here
        // only wallet would save the challenge and save the challenge
        // for next login and match
        info!("peer is a hub, challenge = {}", param);
        Ok(())
    }

    fn on_heartbeat(&self, _: Value) -> Result<Value> {
        Ok(Value::Null)
    }

    fn on_subscribe(&self, _param: Value) -> Result<Value> {
        bail!("I'm light, cannot subscribe you to updates");
    }
}

impl Server<WalletData> for WalletData {
    fn on_message(ws: Arc<WalletConn>, subject: String, body: Value) -> Result<()> {
        match subject.as_str() {
            "version" => ws.on_version(body)?,
            "hub/challenge" => ws.on_hub_challenge(body)?,
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
}
