use std::net::ToSocketAddrs;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use super::network_base::{Sender, Server, WsConnection};
use business::BUSINESS_CACHE;
use cache::{JointData, SDAG_CACHE};
use catchup;
use composer::*;
use config;
use crossbeam::atomic::ArcCell;
use error::Result;
use failure::ResultExt;
use joint::{Joint, JointSequence, Level};
use light;
use main_chain;
use may::coroutine;
use may::net::TcpStream;
use may::sync::RwLock;
use object_hash;
use serde_json::{self, Value};
use signature;
use tungstenite::client::client;
use tungstenite::handshake::client::Request;
use tungstenite::protocol::Role;
use url::Url;
use utils::{AtomicLock, FifoCache, MapLock};
use validation;

#[derive(Serialize, Deserialize)]
pub struct HubNetState {
    in_bounds: Vec<String>,
    out_bounds: Vec<String>,
}

#[derive(Serialize, Deserialize)]
pub struct Login {
    pub challenge: String,
    pub pubkey: String,
    #[serde(skip_serializing)]
    pub signature: String,
}

#[derive(Serialize, Deserialize)]
pub struct TempPubkey {
    pub pubkey: String,
    pub temp_pubkey: String,
    #[serde(skip_serializing)]
    pub signature: String,
}

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
pub enum DeviceMessage {
    Login(Login),
    TempPubkey(TempPubkey),
}

impl DeviceMessage {
    // prefix device addresses with 0 to avoid confusion with payment addresses
    // Note that 0 is not a member of base32 alphabet, which makes device addresses easily distinguishable from payment addresses
    // but still selectable by double-click.  Stripping the leading 0 will not produce a payment address that the device owner knows a private key for,
    // because payment address is derived by c-hashing the definition object, while device address is produced from raw public key.
    fn get_device_address(&self) -> Result<String> {
        let mut address = match *self {
            DeviceMessage::Login(ref login) => object_hash::get_chash(&login.pubkey)?,
            DeviceMessage::TempPubkey(ref temp_pubkey) => {
                object_hash::get_chash(&temp_pubkey.pubkey)?
            }
        };

        address.insert(0, '0');
        Ok(address)
    }

    fn get_device_message_hash_to_sign(&self) -> Vec<u8> {
        use sha2::{Digest, Sha256};

        let source_string = ::obj_ser::to_string(self).expect("DeviceMessage to string failed");
        Sha256::digest(source_string.as_bytes()).to_vec()
    }
}

pub struct HubData {
    // indicate if this connection is a subscribed peer
    is_subscribed: AtomicBool,
    is_source: AtomicBool,
    is_inbound: AtomicBool,
    is_login_completed: AtomicBool,
    challenge: ArcCell<String>,
    device_address: ArcCell<Option<String>>,
}

pub type HubConn = WsConnection<HubData>;

// global data that record the internal state
lazy_static! {
    // global Ws connections
    pub static ref WSS: WsConnections = WsConnections::new();
    // maybe this is too heavy, could use an optimized hashset<AtomicBool>
    static ref UNIT_IN_WORK: MapLock<String> = MapLock::new();
    static ref JOINT_IN_REQ: MapLock<String> = MapLock::new();
    static ref IS_CATCHING_UP: AtomicLock = AtomicLock::new();
    static ref CHALLENGE_ID: String = object_hash::gen_random_string(30);
    static ref BAD_CONNECTION: FifoCache<String, ()> = FifoCache::with_capacity(10);
}

pub struct NormalizeEvent;
impl_event!(NormalizeEvent);

fn init_connection(ws: &Arc<HubConn>) -> Result<()> {
    use rand::{thread_rng, Rng};

    // wait for some time for server ready
    coroutine::sleep(Duration::from_millis(1));

    ws.send_version()?;
    ws.send_subscribe()?;
    ws.send_hub_challenge()?;

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
            ws.close();
            return;
        }
    });

    Ok(())
}

fn add_peer_host(_bound: Arc<HubConn>) -> Result<()> {
    // TODO: impl save peer host to database
    Ok(())
}

// global request has no specific ws connections, just find a proper one should be fine
pub struct WsConnections {
    inbound: RwLock<Vec<Arc<HubConn>>>,
    outbound: RwLock<Vec<Arc<HubConn>>>,
    next_inbound: AtomicUsize,
    next_outbound: AtomicUsize,
}

impl WsConnections {
    fn new() -> Self {
        WsConnections {
            inbound: RwLock::new(Vec::new()),
            outbound: RwLock::new(Vec::new()),
            next_inbound: AtomicUsize::new(0),
            next_outbound: AtomicUsize::new(0),
        }
    }

    pub fn add_inbound(&self, inbound: Arc<HubConn>) -> Result<()> {
        self.inbound.write().unwrap().push(inbound.clone());
        inbound.set_inbound();
        init_connection(&inbound)?;
        add_peer_host(inbound)
    }

    pub fn add_outbound(&self, outbound: Arc<HubConn>) -> Result<()> {
        self.outbound.write().unwrap().push(outbound.clone());
        init_connection(&outbound)?;
        add_peer_host(outbound)
    }

    pub fn close_all(&self) {
        let mut g = self.outbound.write().unwrap();
        g.clear();
        let mut g = self.inbound.write().unwrap();
        g.clear();
    }

    fn get_ws(&self, conn: &HubConn) -> Arc<HubConn> {
        let g = self.outbound.read().unwrap();
        for c in &*g {
            if c.conn_eq(&conn) {
                return c.clone();
            }
        }
        drop(g);

        let g = self.inbound.read().unwrap();
        for c in &*g {
            if c.conn_eq(&conn) {
                return c.clone();
            }
        }

        unreachable!("can't find a ws connection from global wss!")
    }

    fn close(&self, conn: &HubConn) {
        // find out the actor and remove it
        let mut g = self.outbound.write().unwrap();
        for i in 0..g.len() {
            if g[i].conn_eq(&conn) {
                g.swap_remove(i);
                return;
            }
        }
        drop(g);

        let mut g = self.inbound.write().unwrap();
        for i in 0..g.len() {
            if g[i].conn_eq(&conn) {
                g.swap_remove(i);
                return;
            }
        }
    }

    pub fn get_next_inbound(&self) -> Option<Arc<HubConn>> {
        let g = self.inbound.read().unwrap();
        let len = g.len();
        if len == 0 {
            return None;
        }
        let idx = self.next_inbound.fetch_add(1, Ordering::Relaxed) % len;
        Some(g[idx].clone())
    }

    pub fn get_next_outbound(&self) -> Option<Arc<HubConn>> {
        let g = self.outbound.read().unwrap();
        let len = g.len();
        if len == 0 {
            return None;
        }
        let idx = self.next_outbound.fetch_add(1, Ordering::Relaxed) % len;
        Some(g[idx].clone())
    }

    pub fn get_next_peer(&self) -> Option<Arc<HubConn>> {
        self.get_next_outbound().or_else(|| self.get_next_inbound())
    }

    fn get_peers_from_remote(&self) -> Vec<String> {
        let mut peers: Vec<String> = Vec::new();
        let challenge = Value::from(CHALLENGE_ID.as_str());
        let out_bound_peers = self.outbound.read().unwrap().to_vec();
        for out_bound_peer in out_bound_peers {
            if let Ok(value) = out_bound_peer.send_request("get_peers", &challenge) {
                if let Ok(mut tmp) = serde_json::from_value(value) {
                    peers.append(&mut tmp);
                }
            }
        }

        let in_bound_peers = self.inbound.read().unwrap().to_vec();
        for in_bound_peer in in_bound_peers {
            if let Ok(value) = in_bound_peer.send_request("get_peers", &challenge) {
                if let Ok(mut tmp) = serde_json::from_value(value) {
                    peers.append(&mut tmp);
                }
            }
        }

        peers.sort();
        peers.dedup();

        peers
    }

    pub fn get_connection_by_name(&self, peer: &str) -> Option<Arc<HubConn>> {
        let g = self.outbound.read().unwrap();
        for c in &*g {
            if c.get_peer() == peer {
                return Some(c.clone());
            }
        }
        drop(g);

        let g = self.inbound.read().unwrap();
        for c in &*g {
            if c.get_peer() == peer {
                return Some(c.clone());
            }
        }

        None
    }

    #[allow(dead_code)]
    fn forward_joint(&self, cur_ws: &HubConn, joint: &Joint) -> Result<()> {
        for c in &*self.outbound.read().unwrap() {
            if c.is_subscribed() && !c.conn_eq(cur_ws) {
                c.send_joint(joint)?;
            }
        }

        for c in &*self.inbound.read().unwrap() {
            if c.is_subscribed() && !c.conn_eq(cur_ws) {
                c.send_joint(joint)?;
            }
        }
        Ok(())
    }

    pub fn broadcast_joint(&self, joint: &Joint) -> Result<()> {
        for c in &*self.outbound.read().unwrap() {
            // we should check if the outbound is subscribed
            // ref issue #28
            c.send_joint(joint)?;
        }

        for c in &*self.inbound.read().unwrap() {
            if c.is_subscribed() {
                c.send_joint(joint)?;
            }
        }
        Ok(())
    }

    pub fn request_free_joints_from_all_outbound_peers(&self) -> Result<()> {
        let out_bound_peers = self.outbound.read().unwrap().to_vec();
        for out_bound_peer in out_bound_peers {
            out_bound_peer.send_just_saying("refresh", Value::Null)?;
        }
        Ok(())
    }

    pub fn get_outbound_peers(&self, challenge: &str) -> Vec<String> {
        // filter out the connection with the same challenge
        self.outbound
            .read()
            .unwrap()
            .iter()
            .filter(|c| c.get_challenge() != challenge)
            .map(|c| c.get_peer().to_owned())
            .collect()
    }

    pub fn get_inbound_peers(&self) -> Vec<String> {
        self.inbound
            .read()
            .unwrap()
            .iter()
            .map(|c| c.get_peer().to_owned())
            .collect()
    }

    fn get_net_state(&self) -> HubNetState {
        HubNetState {
            in_bounds: self.get_inbound_peers(),
            out_bounds: self.get_outbound_peers(""),
        }
    }

    fn get_needed_outbound_peers(&self) -> usize {
        let outbound_connecions = self.outbound.read().unwrap().len();
        if config::MAX_OUTBOUND_CONNECTIONS > outbound_connecions {
            return config::MAX_OUTBOUND_CONNECTIONS - outbound_connecions;
        }
        0
    }

    fn contains(&self, addr: &str) -> bool {
        let out_contains = self
            .outbound
            .read()
            .unwrap()
            .iter()
            .any(|v| v.get_peer() == addr);
        let in_contains = self
            .inbound
            .read()
            .unwrap()
            .iter()
            .any(|v| v.get_peer() == addr);
        out_contains || in_contains
    }
}

fn get_unconnected_peers_in_config() -> Vec<String> {
    config::get_remote_hub_url()
        .into_iter()
        .filter(|peer| !WSS.contains(peer))
        .collect::<Vec<_>>()
}

fn get_unconnected_peers_in_db() -> Vec<String> {
    // TODO: impl
    Vec::new()
}

pub fn get_unconnected_remote_peers() -> Vec<String> {
    WSS.get_peers_from_remote()
        .into_iter()
        .filter(|peer| !WSS.contains(peer))
        .collect::<Vec<_>>()
}

pub fn auto_connection() {
    let mut counts = WSS.get_needed_outbound_peers();
    if counts == 0 {
        return;
    }

    let peers = get_unconnected_peers_in_config();
    for peer in peers {
        if BAD_CONNECTION.get(&peer).is_some() {
            continue;
        }
        if create_outbound_conn(peer).is_ok() {
            counts -= 1;
            if counts == 0 {
                return;
            }
        }
    }

    let peers = get_unconnected_remote_peers();
    for peer in peers {
        if BAD_CONNECTION.get(&peer).is_some() {
            continue;
        }
        if create_outbound_conn(peer).is_ok() {
            counts -= 1;
            if counts == 0 {
                return;
            }
        }
    }

    let peers = get_unconnected_peers_in_db();
    for peer in peers {
        if BAD_CONNECTION.get(&peer).is_some() {
            continue;
        }
        if create_outbound_conn(peer).is_ok() {
            counts -= 1;
            if counts == 0 {
                return;
            }
        }
    }
}

impl Default for HubData {
    fn default() -> Self {
        HubData {
            is_subscribed: AtomicBool::new(false),
            is_source: AtomicBool::new(false),
            is_inbound: AtomicBool::new(false),
            is_login_completed: AtomicBool::new(false),
            challenge: ArcCell::new(Arc::new("unknown".to_owned())),
            device_address: ArcCell::new(Arc::new(None)),
        }
    }
}

impl Server<HubData> for HubData {
    fn on_message(ws: Arc<HubConn>, subject: String, body: Value) -> Result<()> {
        match subject.as_str() {
            "version" => ws.on_version(body)?,
            "hub/challenge" => ws.on_hub_challenge(body)?,
            "free_joints_end" => {} // not handled
            "error" => error!("receive error: {}", body),
            "info" => info!("receive info: {}", body),
            "result" => info!("receive result: {}", body),
            "joint" => ws.on_joint(body)?,
            "refresh" => ws.on_refresh(body)?,
            "light/new_address_to_watch" => ws.on_new_address_to_watch(body)?,
            "hub/login" => ws.on_hub_login(body)?,
            subject => bail!(
                "on_message unknown subject: {} body {}",
                subject,
                body.to_string()
            ),
        }
        Ok(())
    }

    fn on_request(ws: Arc<HubConn>, command: String, params: Value) -> Result<Value> {
        let response = match command.as_str() {
            "heartbeat" => ws.on_heartbeat(params)?,
            "subscribe" => ws.on_subscribe(params)?,
            "get_joint" => ws.on_get_joint(params)?,
            "catchup" => ws.on_catchup(params)?,
            "get_hash_tree" => ws.on_get_hash_tree(params)?,
            "hub/temp_pubkey" => ws.on_hub_temp_pubkey(params)?,
            "get_peers" => ws.on_get_peers(params)?,
            "get_witnesses" => ws.on_get_witnesses(params)?,
            "post_joint" => ws.on_post_joint(params)?,
            "light/get_history" => ws.on_get_history(params)?,
            "light/get_link_proofs" => ws.on_get_link_proofs(params)?,
            "light/inputs" => ws.on_get_inputs(params)?,
            "get_balance" => ws.on_get_balance(params)?,
            "net_state" => ws.on_get_net_state(params)?,
            "light/light_props" => ws.on_get_light_props(params)?,
            // apis for explorer
            "get_network_info" => ws.on_get_network_info(params)?,
            "get_joints_by_mci" => ws.on_get_joints_by_mci(params)?,
            "get_joint_by_unit_hash" => ws.on_get_joint_by_unit_hash(params)?,
            command => bail!("on_request unknown command: {}", command),
        };
        Ok(response)
    }

    fn close(ws: Arc<HubConn>) {
        ws.close()
    }
}

// internal state access
impl HubConn {
    pub fn is_subscribed(&self) -> bool {
        let data = self.get_data();
        data.is_subscribed.load(Ordering::Relaxed)
    }

    fn set_subscribed(&self) {
        let data = self.get_data();
        data.is_subscribed.store(true, Ordering::Relaxed);
    }

    pub fn is_source(&self) -> bool {
        let data = self.get_data();
        data.is_source.load(Ordering::Relaxed)
    }

    fn set_source(&self) {
        let data = self.get_data();
        data.is_source.store(true, Ordering::Relaxed);
    }

    pub fn is_inbound(&self) -> bool {
        let data = self.get_data();
        data.is_inbound.load(Ordering::Relaxed)
    }

    pub fn set_inbound(&self) {
        let data = self.get_data();
        data.is_inbound.store(true, Ordering::Relaxed);
    }

    pub fn is_login_completed(&self) -> bool {
        let data = self.get_data();
        data.is_login_completed.load(Ordering::Relaxed)
    }

    pub fn set_login_completed(&self) {
        let data = self.get_data();
        data.is_login_completed.store(true, Ordering::Relaxed);
    }

    pub fn get_challenge(&self) -> String {
        let data = self.get_data();
        data.challenge.get().to_string()
    }

    pub fn set_challenge(&self, challenge: &str) {
        let data = self.get_data();
        data.challenge.set(Arc::new(challenge.to_owned()));
    }

    pub fn get_device_address(&self) -> Arc<Option<String>> {
        let data = self.get_data();
        data.device_address.get()
    }

    pub fn set_device_address(&self, device_address: &str) {
        info!("set_device_address {}", device_address);
        let data = self.get_data();
        data.device_address
            .set(Arc::new(Some(device_address.to_owned())));
    }
}

// the server side impl
impl HubConn {
    fn on_version(&self, version: Value) -> Result<()> {
        if version["protocol_version"].as_str() != Some(config::VERSION) {
            error!("Incompatible versions, mine {}", config::VERSION);
            self.close();
        }

        if version["alt"].as_str() != Some(config::ALT) {
            error!("Incompatible alt, mine {}", config::ALT);
            self.close();
        }

        info!("got peer version: {}", version);
        Ok(())
    }

    fn on_get_balance(&self, param: Value) -> Result<Value> {
        let addr = param
            .as_str()
            .ok_or_else(|| format_err!("no address for get_balance"))?;
        let balance = BUSINESS_CACHE.global_state.get_stable_balance(addr)?;

        Ok(json!({"address": addr, "balance": balance}))
    }

    fn on_get_inputs(&self, param: Value) -> Result<Value> {
        let inputs_request: light::InputsRequest = serde_json::from_value(param)?;

        let ret = light::get_inputs_for_amount(inputs_request)?;

        Ok(serde_json::to_value(ret)?)
    }

    fn on_get_light_props(&self, param: Value) -> Result<Value> {
        if !self.is_inbound() {
            bail!("light clients have to be inbound");
        }
        let address: String = serde_json::from_value(param)?;
        let ParentsAndLastBall {
            parents,
            last_ball,
            last_ball_unit,
        } = pick_parents_and_last_ball(&address)?;

        let light_props = light::LightProps {
            last_ball,
            last_ball_unit,
            parent_units: parents,
            witness_list_unit: config::get_genesis_unit(),
            has_definition: SDAG_CACHE.get_definition(&address).is_some(),
        };

        Ok(serde_json::to_value(light_props)?)
    }

    fn on_heartbeat(&self, _: Value) -> Result<Value> {
        Ok(Value::Null)
    }

    fn on_subscribe(&self, param: Value) -> Result<Value> {
        let subscription_id = param["subscription_id"]
            .as_str()
            .ok_or_else(|| format_err!("no subscription_id"))?;
        if subscription_id == *CHALLENGE_ID {
            self.close();
            return Err(format_err!("self-connect"));
        }
        self.set_subscribed();
        // send some joint in a background task
        let ws = WSS.get_ws(self);
        let last_mci = param["last_mci"].as_u64();
        try_go!(move || -> Result<()> {
            if let Some(last_mci) = last_mci {
                ws.send_joints_since_mci(Level::from(last_mci as usize))?;
            }
            ws.send_free_joints()?;
            ws.send_just_saying("free_joints_end", Value::Null)?;
            Ok(())
        });

        Ok(Value::from("subscribed"))
    }

    fn on_hub_challenge(&self, param: Value) -> Result<()> {
        // this is hub, we do nothing here
        // only wallet would save the challenge
        // for next login and match
        info!("peer is a hub, challenge = {}", param);
        let challenge = param
            .as_str()
            .ok_or_else(|| format_err!("no challenge id"))?;
        // we save the peer's challenge to distingue who it is
        self.set_challenge(challenge);
        Ok(())
    }

    fn on_get_joint(&self, param: Value) -> Result<Value> {
        let unit: String = serde_json::from_value(param)?;

        match SDAG_CACHE.get_joint(&unit).and_then(|j| j.read()) {
            Ok(joint) => Ok(json!({ "joint": clear_ball_after_min_retrievable_mci(&joint)?})),

            Err(e) => {
                error!("read joint {} failed, err={}", unit, e);
                Ok(json!({ "joint_not_found": unit }))
            }
        }
    }

    fn on_joint(&self, param: Value) -> Result<()> {
        let joint: Joint = serde_json::from_value(param)?;
        info!("receive a joint: {:?}", joint);
        ensure!(!joint.unit.unit.is_empty(), "no unit");
        self.handle_online_joint(joint)
    }

    fn on_catchup(&self, param: Value) -> Result<Value> {
        let catchup_req: catchup::CatchupReq = serde_json::from_value(param)?;
        let catchup_chain = catchup::prepare_catchup_chain(catchup_req)?;
        Ok(serde_json::to_value(catchup_chain)?)
    }

    fn on_get_hash_tree(&self, param: Value) -> Result<Value> {
        let hash_tree_req: catchup::HashTreeReq = serde_json::from_value(param)?;
        let hash_tree = catchup::prepare_hash_tree(hash_tree_req)?;
        Ok(json!({ "balls": hash_tree }))
    }

    fn on_refresh(&self, param: Value) -> Result<()> {
        let _g = match IS_CATCHING_UP.try_lock() {
            Some(g) => g,
            None => return Ok(()),
        };

        let mci = param.as_u64();
        if let Some(mci) = mci {
            self.send_joints_since_mci(Level::from(mci as usize))?;
        }
        self.send_free_joints()?;
        self.send_just_saying("free_joints_end", Value::Null)?;

        Ok(())
    }

    fn on_new_address_to_watch(&self, param: Value) -> Result<()> {
        if !self.is_inbound() {
            return self.send_error(Value::from("light clients have to be inbound"));
        }

        let address: String = serde_json::from_value(param).context("not an address string")?;
        if !::object_hash::is_chash_valid(&address) {
            return self.send_error(Value::from("address not valid"));
        }

        // let db = db::DB_POOL.get_connection();
        // let mut stmt = db.prepare_cached(
        //     "INSERT OR IGNORE INTO watched_light_addresses (peer, address) VALUES (?,?)",
        // )?;
        // stmt.execute(&[self.get_peer(), &address])?;
        // self.send_info(Value::from(format!("now watching {}", address)))?;

        // let mut stmt = db.prepare_cached(
        //     "SELECT unit, is_stable FROM unit_authors JOIN units USING(unit) WHERE address=? \
        //      UNION \
        //      SELECT unit, is_stable FROM outputs JOIN units USING(unit) WHERE address=? \
        //      ORDER BY is_stable LIMIT 10",
        // )?;

        // struct TempUnit {
        //     unit: String,
        //     is_stable: u32,
        // }

        // let rows = stmt
        //     .query_map(&[&address, &address], |row| TempUnit {
        //         unit: row.get(0),
        //         is_stable: row.get(1),
        //     })?
        //     .collect::<::std::result::Result<Vec<_>, _>>()?;

        // if rows.is_empty() {
        //     return Ok(());
        // }

        // if rows.len() == 10 || rows.iter().any(|r| r.is_stable == 1) {
        //     self.send_just_saying("light/have_updates", Value::Null)?;
        // }

        // for row in rows {
        //     if row.is_stable == 1 {
        //         continue;
        //     }
        //     let joint = storage::read_joint(&db, &row.unit)
        //         .context(format!("watched unit {} not found", row.unit))?;
        //     self.send_joint(&joint)?;
        // }

        // Ok(())
        unimplemented!()
    }

    fn on_get_peers(&self, param: Value) -> Result<Value> {
        let challenge = param.as_str();
        let peers = WSS.get_outbound_peers(challenge.unwrap_or("unknown"));
        Ok(serde_json::to_value(peers)?)
    }

    fn on_get_net_state(&self, _param: Value) -> Result<Value> {
        let net_state = WSS.get_net_state();
        Ok(serde_json::to_value(net_state)?)
    }

    fn on_get_witnesses(&self, _: Value) -> Result<Value> {
        use my_witness::MY_WITNESSES;
        Ok(serde_json::to_value(&*MY_WITNESSES)?)
    }

    fn on_post_joint(&self, param: Value) -> Result<Value> {
        let joint: Joint = serde_json::from_value(param)?;
        info!("receive a posted joint: {:?}", joint);

        let unit = joint.clone();
        self.handle_online_joint(joint)?;

        // TODO: we should only broadcast the joint after normalize
        // and wati until it comes?
        WSS.broadcast_joint(&unit)?;
        Ok(Value::from("accepted"))
    }

    fn on_get_history(&self, param: Value) -> Result<Value> {
        if !self.is_inbound() {
            bail!("light clients have to be inbound");
        }

        let history_request: light::HistoryRequest = serde_json::from_value(param)?;

        let ret = light::get_latest_history(&history_request)?;

        Ok(serde_json::to_value(ret)?)
    }

    fn on_get_link_proofs(&self, _params: Value) -> Result<Value> {
        if !self.is_inbound() {
            bail!("light clients have to be inbound");
        }
        // let units: Vec<String> =
        //     serde_json::from_value(params).context("prepare_Link_proofs.params is error")?;
        // Ok(serde_json::to_value(light::prepare_link_proofs(
        //     &units,
        // )?)?)
        Ok(json![null])
    }

    fn on_hub_login(&self, body: Value) -> Result<()> {
        match serde_json::from_value::<DeviceMessage>(body) {
            Err(e) => {
                error!("hub_login: serde err= {}", e);
                return self.send_error(Value::from("no login params"));
            }
            Ok(device_message) => {
                if let DeviceMessage::Login(ref login) = &device_message {
                    if login.challenge != *CHALLENGE_ID {
                        return self.send_error(Value::from("wrong challenge"));
                    }

                    if login.pubkey.len() != ::config::PUBKEY_LENGTH {
                        return self.send_error(Value::from("wrong pubkey length"));
                    }

                    if login.signature.len() != ::config::SIG_LENGTH {
                        return self.send_error(Value::from("wrong signature length"));
                    };

                    if signature::verify(
                        &device_message.get_device_message_hash_to_sign(),
                        &login.signature,
                        &login.pubkey,
                    )
                    .is_err()
                    {
                        return self.send_error(Value::from("wrong signature"));
                    }

                    let device_address = device_message.get_device_address()?;
                    self.set_device_address(&device_address);

                    self.send_just_saying("hub/push_project_number", json!({"projectNumber": 0}))?;

                    // TODO: send out saved messages to device
                    // after this point the device is authenticated and can send further commands
                    // let db = db::DB_POOL.get_connection();
                    // let mut stmt =
                    //     db.prepare_cached("SELECT 1 FROM devices WHERE device_address=?")?;
                    // if !stmt.exists(&[&device_address])? {
                    //     let mut stmt = db.prepare_cached(
                    //         "INSERT INTO devices (device_address, pubkey) VALUES (?,?)",
                    //     )?;
                    //     stmt.execute(&[&device_address, &login.pubkey])?;
                    //     self.send_info(json!("address created"))?;
                    // } else {
                    //     self.send_stored_device_messages(&db, &device_address)?;
                    // }

                    //finishLogin
                    self.set_login_completed();
                //TODO: Seems to handle the temp_pubkey message before the login happen
                } else {
                    return self.send_error(Value::from("not a valid login DeviceMessage"));
                }
            }
        }

        Ok(())
    }

    fn on_hub_temp_pubkey(&self, param: Value) -> Result<Value> {
        let mut try_limit = 20;
        while try_limit > 0 && self.get_device_address().is_none() {
            try_limit -= 1;
            coroutine::sleep(Duration::from_millis(100));
        }

        let device_address = self.get_device_address();
        ensure!(device_address.is_some(), "please log in first");

        match serde_json::from_value::<DeviceMessage>(param) {
            Err(e) => {
                error!("temp_pubkey serde err={}", e);
                bail!("wrong temp_pubkey params");
            }

            Ok(device_message) => {
                if let DeviceMessage::TempPubkey(ref temp_pubkey) = &device_message {
                    ensure!(
                        temp_pubkey.temp_pubkey.len() == ::config::PUBKEY_LENGTH,
                        "wrong temp_pubkey length"
                    );
                    ensure!(
                        Some(device_message.get_device_address()?) == *self.get_device_address(),
                        "signed by another pubkey"
                    );

                    if signature::verify(
                        &device_message.get_device_message_hash_to_sign(),
                        &temp_pubkey.signature,
                        &temp_pubkey.pubkey,
                    )
                    .is_err()
                    {
                        bail!("wrong signature");
                    }

                    // TODO: save temp pubkey into db
                    // let db = db::DB_POOL.get_connection();
                    // let mut stmt = db.prepare_cached(
                    //     "UPDATE devices SET temp_pubkey_package=? WHERE device_address=?",
                    // )?;
                    // // TODO: here need to add signature back
                    // stmt.execute(&[&serde_json::to_string(temp_pubkey)?, &*device_address])?;

                    return Ok(Value::from("updated"));
                } else {
                    bail!("not a valid temp_pubkey params");
                }
            }
        }
    }

    fn on_get_network_info(&self, _param: Value) -> Result<Value> {
        let version = config::VERSION;
        let peers = WSS.get_inbound_peers().len();
        let tps = 1050;
        let last_mci = main_chain::get_last_stable_mci().value();
        let total_units = SDAG_CACHE.get_joints_len();

        Ok(json!({
            "version": version,
            "peers": peers,
            "tps": tps,
            "last_mci": last_mci,
            "total_units": total_units,
        }))
    }

    fn on_get_joints_by_mci(&self, param: Value) -> Result<Value> {
        let mci = param
            .as_i64()
            .ok_or_else(|| format_err!("not a valid mci"))?;

        let joints: Vec<Joint> = if mci < 0 {
            SDAG_CACHE
                .get_unstable_joints()?
                .into_iter()
                .map(|j| j.read())
                // Skip those failed to read
                .filter(|j| j.is_ok())
                .map(|j| (**j.unwrap()).clone())
                .collect()
        } else {
            SDAG_CACHE
                .get_joints_by_mci(Level::from(mci as usize))?
                .into_iter()
                .map(|j| j.read())
                // Skip those failed to read
                .filter(|j| j.is_ok())
                .map(|j| (**j.unwrap()).clone())
                .collect()
        };

        Ok(json!({ "joints": joints }))
    }

    fn on_get_joint_by_unit_hash(&self, param: Value) -> Result<Value> {
        let unit: String = serde_json::from_value(param)?;

        SDAG_CACHE
            .get_joint(&unit)
            .and_then(|j| j.read())
            .and_then(|j| Ok(json!({ "joint": (**j).clone(), "property": j.get_props()})))
    }
}

impl HubConn {
    fn handle_online_joint(&self, joint: Joint) -> Result<()> {
        // clear the main chain index, main chain index is used by light only
        // joint.unit.main_chain_index = None;

        // check content_hash or unit_hash first!
        validation::validate_unit_hash(&joint.unit)?;

        let ball = joint.ball.clone();

        // check if unit is in work, when g is dropped unlock the unit
        let g = UNIT_IN_WORK.try_lock(vec![joint.unit.unit.to_owned()]);
        if g.is_none() {
            // the unit is in work, do nothing
            return Ok(());
        }

        let cached_joint = match SDAG_CACHE.add_new_joint(joint) {
            Ok(j) => j,
            Err(e) => {
                warn!("add_new_joint: {}", e);
                return Ok(());
            }
        };
        let joint_data = cached_joint.read().unwrap();
        if joint_data.unit.content_hash.is_some() {
            joint_data.set_sequence(JointSequence::FinalBad);
        }

        if !joint_data.is_missing_parent() {
            return validation::validate_ready_joint(cached_joint);
        }

        // trigger catchup
        if let Some(ball) = ball {
            if !SDAG_CACHE.is_ball_in_hash_tree(&ball) {
                // need to catchup and keep the joint in unhandled till timeout
                let ws = WSS.get_ws(self);
                try_go!(move || {
                    // if we already in catchup mode, just return
                    let _g = match IS_CATCHING_UP.try_lock() {
                        Some(g) => g,
                        None => return Ok(()),
                    };

                    let ret = start_catchup(ws);
                    // after the catchup done, clear the hash tree ball
                    SDAG_CACHE.clear_hash_tree_ball();

                    ret
                });
                return Ok(());
            }
        } else {
            // missing parent, ask for them
            let missing_parents = joint_data.get_missing_parents()?;
            self.request_new_missing_joints(missing_parents)?;
        }
        Ok(())
    }

    // record peer event in database
    #[allow(dead_code)]
    fn write_event(&self, _event: &str) -> Result<()> {
        // TODO: record peer event
        // if event.contains("invalid") || event.contains("nonserial") {
        //     let host = self.get_peer();
        //     let event_string: String = event.to_string();
        //     let column = format!("count_{}_joints", &event_string);
        //     let sql = format!(
        //         "UPDATE peer_hosts SET {}={}+1 WHERE peer_host=?",
        //         column, column
        //     );
        //     let mut stmt = db.prepare_cached(&sql)?;
        //     stmt.execute(&[host])?;

        //     let mut stmt =
        //         db.prepare_cached("INSERT INTO peer_events (peer_host, event) VALUES (?, ?)")?;
        //     stmt.execute(&[host, &event_string])?;
        // }

        Ok(())
    }

    fn request_catchup(&self) -> Result<Vec<String>> {
        info!("will request catchup from {}", self.get_peer());

        // here we send out the real catchup request
        let last_stable_mci = main_chain::get_last_stable_mci();
        // TODO: what's this used for?
        // let last_known_mci = storage::read_last_main_chain_index(db)?;
        let witnesses: &[String] = &::my_witness::MY_WITNESSES;
        let param = json!({
            "witnesses": witnesses,
            "last_stable_mci": last_stable_mci.value(),
            "last_known_mci": last_stable_mci.value()
        });

        let ret = self.send_request("catchup", &param)?;
        if !ret["error"].is_null() {
            bail!("catchup request got error response: {:?}", ret["error"]);
        }

        let catchup_chain: catchup::CatchupChain = serde_json::from_value(ret)?;
        catchup::process_catchup_chain(catchup_chain)
    }

    fn request_new_missing_joints<'a>(
        &self,
        units: impl Iterator<Item = &'a String>,
    ) -> Result<()> {
        let mut new_units = Vec::new();

        for unit in units {
            let g = UNIT_IN_WORK.try_lock(vec![unit.clone()]);
            if g.is_none() {
                // other thread is working on the unit, skip it
                debug!("request unit in working. unit={}", unit);
                continue;
            }

            // re-check if this is necessary
            if let Err(e) = SDAG_CACHE.check_new_joint(unit) {
                info!("unnecessary request unit: {} ", e);
                continue;
            }

            new_units.push(unit.clone());
        }

        self.request_joints(new_units.iter())?;
        Ok(())
    }

    fn request_next_hash_tree(
        &self,
        from_ball: &str,
        to_ball: &str,
    ) -> Result<Vec<catchup::BallProps>> {
        // TODO: need reroute if failed to send
        let mut hash_tree = self.send_request(
            "get_hash_tree",
            &json!({
                "from_ball": from_ball,
                "to_ball": to_ball,
            }),
        )?;

        if !hash_tree["error"].is_null() {
            bail!("get_hash_tree got error response: {}", hash_tree["error"]);
        }

        Ok(serde_json::from_value(hash_tree["balls"].take())?)
    }

    #[inline]
    fn send_joint(&self, joint: &Joint) -> Result<()> {
        self.send_just_saying("joint", serde_json::to_value(joint)?)
    }

    /// send stable joints to trigger peer catchup
    fn send_joints_since_mci(&self, mci: Level) -> Result<()> {
        let last_stable_mci = main_chain::get_last_stable_mci();
        // peer no need catchup
        if mci >= last_stable_mci {
            return Ok(());
        }

        // only send latest stable joints
        for joint in SDAG_CACHE.get_joints_by_mci(last_stable_mci)? {
            self.send_joint(&clear_ball_after_min_retrievable_mci(&*joint.read()?)?)?;
        }

        Ok(())
    }

    fn send_free_joints(&self) -> Result<()> {
        let joints = SDAG_CACHE.get_free_joints()?;
        for joint in joints {
            let joint = joint.read()?;
            self.send_joint(&**joint)?;
        }
        self.send_just_saying("free_joints_end", Value::Null)?;
        Ok(())
    }

    #[allow(dead_code)]
    fn send_stored_device_messages(&self, _device_address: &str) -> Result<()> {
        //TODO: save and send device messages
        Ok(())
    }
}

// the client side impl
impl HubConn {
    fn send_version(&self) -> Result<()> {
        self.send_just_saying(
            "version",
            json!({
                "protocol_version": config::VERSION,
                "alt": config::ALT,
                "library": config::LIBRARY,
                "library_version": config::LIBRARY_VERSION,
                "program": "rust-sdag-hub",
                // TODO: read from Cargo.toml
                "program_version": "0.1.0"
            }),
        )
    }

    fn send_hub_challenge(&self) -> Result<()> {
        self.send_just_saying("hub/challenge", Value::from(CHALLENGE_ID.to_owned()))?;
        Ok(())
    }

    fn send_subscribe(&self) -> Result<()> {
        let last_mci = main_chain::get_last_stable_mci();

        match self.send_request(
            "subscribe",
            &json!({ "subscription_id": *CHALLENGE_ID, "last_mci": last_mci.value()}),
        ) {
            Ok(_) => self.set_source(),
            Err(e) => {
                warn!("send subscribe failed, err={}, peer={}", e, self.get_peer());
                // save the peer address to avoid connect to it again
                BAD_CONNECTION.insert(self.get_peer().clone(), ());
            }
        }

        Ok(())
    }

    fn send_heartbeat(&self) -> Result<()> {
        self.send_request("heartbeat", &Value::Null)?;
        Ok(())
    }

    pub fn post_joint(&self, joint: &Joint) -> Result<()> {
        self.send_request("post_joint", &serde_json::to_value(joint)?)?;
        Ok(())
    }

    // remove self from global
    fn close(&self) {
        info!("close connection: {}", self.get_peer());
        // we hope that when all related joints are resolved
        // the connection could drop automatically
        WSS.close(self);
    }

    fn request_joints<'a>(&self, units: impl Iterator<Item = &'a String>) -> Result<()> {
        fn request_joint(ws: Arc<HubConn>, unit: &str) -> Result<()> {
            // if the joint is in request, just ignore
            let g = JOINT_IN_REQ.try_lock(vec![unit.to_owned()]);
            if g.is_none() {
                debug!("already request_joint: {}", unit);
                return Ok(());
            }

            let mut v = ws.send_request("get_joint", &Value::from(unit))?;
            if v["joint_not_found"].as_str() == Some(&unit) {
                // TODO: if self connection failed to request joint, should
                // let available ws to try a again here. see #72
                bail!(
                    "unit {} not found with the connection: {}",
                    unit,
                    ws.get_peer()
                );
            }

            let joint: Joint = serde_json::from_value(v["joint"].take())?;
            info!("receive a requested joint: {:?}", joint);

            if joint.unit.unit != unit {
                let err = format!("I didn't request this unit from you: {}", joint.unit.unit);
                return ws.send_error(Value::from(err));
            }
            drop(g);

            ws.handle_online_joint(joint)
        }

        for unit in units {
            let unit = unit.clone();
            let ws = WSS.get_ws(self);
            try_go!(move || request_joint(ws, &unit));
        }
        Ok(())
    }

    pub fn get_witnesses(&self) -> Result<Vec<String>> {
        let witnesses = self
            .send_request("get_witnesses", &Value::Null)
            .context("failed to get witnesses")?;
        let witnesses: Vec<String> =
            serde_json::from_value(witnesses).context("failed to parse witnesses")?;
        if witnesses.len() != config::COUNT_WITNESSES {
            bail!(
                "witnesses must contains {} addresses, but we got {}",
                config::COUNT_WITNESSES,
                witnesses.len()
            );
        }
        Ok(witnesses)
    }
}

pub fn create_outbound_conn<A: ToSocketAddrs>(address: A) -> Result<Arc<HubConn>> {
    let stream = TcpStream::connect(address)?;
    let peer = match stream.peer_addr() {
        Ok(addr) => addr.to_string(),
        Err(_) => "unknown peer".to_owned(),
    };
    let url = Url::parse("wss://localhost/")?;
    let req = Request::from(url);
    let (conn, _) = client(req, stream)?;

    let ws = WsConnection::new(conn, HubData::default(), peer, Role::Client)?;

    WSS.add_outbound(ws.clone())?;
    Ok(ws)
}

/// remove those long time not ready joints
pub fn purge_junk_unhandled_joints(timeout: u64) {
    let now = crate::time::now();

    // maybe we are catching up the missing parents
    if IS_CATCHING_UP.is_locked() {
        return;
    }

    // remove those joints that stay in unhandled more that 10min
    SDAG_CACHE.purge_old_unhandled_joints(now, timeout);
}

/// remove those long time temp-bad free joints
pub fn purge_temp_bad_free_joints(timeout: u64) -> Result<()> {
    let now = crate::time::now();
    SDAG_CACHE.purge_old_temp_bad_free_joints(now, timeout)
}

pub fn start_catchup(ws: Arc<HubConn>) -> Result<()> {
    error!("catchup started");

    // before a catchup the hash_tree_ball should be clear
    assert_eq!(SDAG_CACHE.get_hash_tree_ball_len(), 0);
    let mut catchup_chain_balls = ws.request_catchup()?;
    catchup_chain_balls.reverse();

    for batch in catchup_chain_balls.windows(2) {
        let start = batch[0].clone();
        let end = batch[1].clone();

        let batch_balls = ws.request_next_hash_tree(&start, &end)?;

        // check last ball is next item
        if batch_balls.last().map(|p| &p.ball) != Some(&end) {
            bail!("batch last ball not match to ball!");
        }
        catchup::process_hash_tree(&batch_balls)?;

        ws.request_new_missing_joints(batch_balls.iter().map(|j| &j.unit))?;

        // wait the batch number below a value and then start another batch
        ::utils::wait_cond(Some(Duration::from_secs(10)), || {
            SDAG_CACHE.get_hash_tree_ball_len() < 300
        })
        .context("catchup wait hash tree batch timeout")?;
    }

    // wait all the catchup done
    ::utils::wait_cond(Some(Duration::from_secs(10)), || {
        SDAG_CACHE.get_hash_tree_ball_len() == 0
    })
    .context("catchup wait last ball timeout")?;
    error!("catchup done");

    // wait until there is no more working
    ::utils::wait_cond(None, || UNIT_IN_WORK.get_waiter_num() == 0).ok();

    WSS.request_free_joints_from_all_outbound_peers()?;

    Ok(())
}

/// this fn will be called every 8s in a timer
pub fn re_request_lost_joints() -> Result<()> {
    let _g = match IS_CATCHING_UP.try_lock() {
        Some(g) => g,
        None => return Ok(()),
    };

    let units = SDAG_CACHE.get_missing_joints();
    if units.is_empty() {
        return Ok(());
    }
    info!("lost units {:?}", units);

    let ws = match WSS.get_next_peer() {
        None => bail!("failed to find next peer"),
        Some(c) => c,
    };
    info!("found next peer {}", ws.get_peer());

    // this is not an atomic operation, but it's fine to request the unit in working
    let new_units = units
        .iter()
        .filter(|x| UNIT_IN_WORK.try_lock(vec![(*x).to_owned()]).is_none());

    ws.request_joints(new_units)
}

#[allow(dead_code)]
fn notify_watchers(joint: &Joint, cur_ws: &HubConn) -> Result<()> {
    let unit = &joint.unit;
    if unit.messages.is_empty() {
        return Ok(());
    }

    // already stable, light clients will require a proof
    if joint.ball.is_some() {
        return Ok(());
    }

    let mut addresses = unit.authors.iter().map(|a| &a.address).collect::<Vec<_>>();
    for message in &unit.messages {
        use spec::Payload;
        if message.app != "payment" || message.payload.is_none() {
            continue;
        }
        match message.payload {
            Some(Payload::Payment(ref payment)) => {
                for output in &payment.outputs {
                    let address = &output.address;
                    if !addresses.contains(&address) {
                        addresses.push(address);
                    }
                }
            }
            _ => unreachable!("payload should be a payment"),
        }
    }

    // let addresses_str = addresses
    //     .into_iter()
    //     .map(|s| format!("'{}'", s))
    //     .collect::<Vec<_>>()
    //     .join(", ");
    // let sql = format!(
    //     "SELECT peer FROM watched_light_addresses WHERE address IN({})",
    //     addresses_str
    // );

    // let mut stmt = db.prepare(&sql)?;
    // let rows = stmt
    //     .query_map(&[], |row| row.get(0))?
    //     .collect::<::std::result::Result<Vec<String>, _>>()?;
    // TODO: find out peers and send the message to them
    let rows: Vec<String> = Vec::new();

    if rows.is_empty() {
        return Ok(());
    }

    // light clients need timestamp
    let mut joint = joint.clone();
    joint.unit.timestamp = Some(::time::now() / 1000);

    for peer in rows {
        if let Some(ws) = WSS.get_connection_by_name(&peer) {
            if !ws.conn_eq(cur_ws) {
                ws.send_joint(&joint)?;
            }
        }
    }

    Ok(())
}

fn notify_light_clients_about_stable_joints(_from_mci: Level, _to_mci: Level) -> Result<()> {
    // let mut stmt = db.prepare_cached(
    // 	"SELECT peer FROM units JOIN unit_authors USING(unit) JOIN watched_light_addresses USING(address) \
    // 	WHERE main_chain_index>? AND main_chain_index<=? \
    // 	UNION \
    // 	SELECT peer FROM units JOIN outputs USING(unit) JOIN watched_light_addresses USING(address) \
    // 	WHERE main_chain_index>? AND main_chain_index<=? \
    // 	UNION \
    // 	SELECT peer FROM units JOIN watched_light_units USING(unit) \
    // 	WHERE main_chain_index>? AND main_chain_index<=?")?;

    // let rows = stmt
    //     .query_map(
    //         &[&from_mci, &to_mci, &from_mci, &to_mci, &from_mci, &to_mci],
    //         |row| row.get(0),
    //     )?
    //     .collect::<::std::result::Result<Vec<String>, _>>()?;
    // for peer in rows {
    //     if let Some(ws) = WSS.get_connection_by_name(&peer) {
    //         ws.send_just_saying("light/have_updates", Value::Null)?;
    //     }
    // }

    // let mut stmt = db.prepare_cached(
    //     "DELETE FROM watched_light_units \
    //      WHERE unit IN (SELECT unit FROM units WHERE main_chain_index>? AND main_chain_index<=?)",
    // )?;

    // stmt.execute(&[&from_mci, &to_mci])?;

    // Ok(())
    unimplemented!()
}

pub fn notify_watchers_about_stable_joints(mci: Level) -> Result<()> {
    use joint::WRITER_MUTEX;
    // the event was emitted from inside mysql transaction, make sure it completes so that the changes are visible
    // If the mci became stable in determineIfStableInLaterUnitsAndUpdateStableMcFlag (rare), write lock is released before the validation commits,
    // so we might not see this mci as stable yet. Hopefully, it'll complete before light/have_updates roundtrip
    let g = WRITER_MUTEX.lock().unwrap();
    // we don't need to block writes, we requested the lock just to wait that the current write completes
    drop(g);
    info!("notify_watchers_about_stable_joints, mci={:?} ", mci);
    if mci.value() <= 1 {
        return Ok(());
    }

    let last_ball_mci = SDAG_CACHE.get_last_ball_mci_of_mci(mci)?;
    let prev_last_ball_mci = SDAG_CACHE.get_last_ball_mci_of_mci((mci.value() - 1).into())?;

    if last_ball_mci == prev_last_ball_mci {
        return Ok(());
    }

    notify_light_clients_about_stable_joints(prev_last_ball_mci, last_ball_mci)
}

fn clear_ball_after_min_retrievable_mci(joint_data: &JointData) -> Result<Joint> {
    let mut joint = (**joint_data).clone();

    // min_retrievable mci is the mci of the last ball of the last stable joint
    if joint_data.get_mci()
        >= SDAG_CACHE
            .get_last_ball_mci_of_mci(::main_chain::get_last_stable_mci())
            .unwrap_or(Level::default())
    {
        joint.ball = None;
        joint.skiplist_units = Vec::new();
    }

    Ok(joint)
}
