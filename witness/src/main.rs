#[macro_use]
extern crate log;
#[macro_use]
extern crate may;
#[macro_use]
extern crate failure;
#[macro_use]
extern crate sdag;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate lazy_static;

extern crate chrono;
extern crate fern;
extern crate rcu_cell;
extern crate may_signal;
extern crate rand;
extern crate sdag_wallet_base;
extern crate serde_json;

mod timer;
mod witness;

use sdag::config;
use sdag::error::Result;
use sdag::network;
use sdag_wallet_base::{ExtendedPrivKey, ExtendedPubKey, Mnemonic};

lazy_static! {
    pub static ref WALLET_INFO: WalletInfo = {
        let mnemonic = config::get_mnemonic().expect("failed to read mnemonic form settings");
        WalletInfo::from_mnemonic(&mnemonic).expect("failed to generate wallet info")
    };
}

pub struct WalletInfo {
    #[allow(dead_code)]
    master_prvk: ExtendedPrivKey,
    _wallet_pubk: ExtendedPubKey,
    _device_address: String,
    _wallet_0_id: String,
    _00_address: String,
    _00_address_pubk: ExtendedPubKey,
    _00_address_prvk: ExtendedPrivKey,
}

impl WalletInfo {
    fn from_mnemonic(mnemonic: &str) -> Result<WalletInfo> {
        let wallet = 0;
        let mnemonic = Mnemonic::from(&mnemonic)?;
        let master_prvk = sdag_wallet_base::master_private_key(&mnemonic, "")?;
        let _device_address = sdag_wallet_base::device_address(&master_prvk)?;
        let _wallet_pubk = sdag_wallet_base::wallet_pubkey(&master_prvk, wallet)?;
        let _wallet_0_id = sdag_wallet_base::wallet_id(&_wallet_pubk);
        let _00_address = sdag_wallet_base::wallet_address(&_wallet_pubk, false, 0)?;
        let _00_address_prvk = sdag_wallet_base::wallet_address_prvkey(&master_prvk, 0, false, 0)?;
        let _00_address_pubk = sdag_wallet_base::wallet_address_pubkey(&_wallet_pubk, false, 0)?;

        Ok(WalletInfo {
            master_prvk,
            _wallet_pubk,
            _device_address,
            _wallet_0_id,
            _00_address,
            _00_address_pubk,
            _00_address_prvk,
        })
    }
}

impl sdag::signature::Signer for WalletInfo {
    fn sign(&self, hash: &[u8], address: &str) -> Result<String> {
        if address != self._00_address {
            bail!("invalid address for wallet to sign");
        }

        sdag_wallet_base::sign(hash, &self._00_address_prvk)
    }
}

// register global event handlers
fn register_event_handlers() {
    use sdag::utils::event::Event;
    use sdag::validation::NewJointEvent;

    // hook the actual handler here
    NewJointEvent::add_handler(move |_v| witness::check_and_witness());
}

fn init_log() {
    // TODO: need to implement async logs
    let log_lvl = if cfg!(debug_assertions) {
        log::LevelFilter::Debug
    } else {
        log::LevelFilter::Warn
    };

    fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "{}[{}][{}] {}",
                chrono::Local::now().format("[%Y-%m-%d][%H:%M:%S%.3f]"),
                record.level(),
                record.target(),
                message
            ))
        })
        .level(log_lvl)
        .chain(std::io::stdout())
        .apply()
        .unwrap();

    debug!("log init done!");
}

fn init() -> Result<()> {
    // init default coroutine settings
    let stack_size = if cfg!(debug_assertions) {
        0x4000
    } else {
        0x2000
    };
    may::config().set_stack_size(stack_size);

    init_log();

    Ok(())
}

fn connect_to_remote() -> Result<()> {
    let peers = sdag::config::get_remote_hub_url();

    for peer in peers {
        if let Err(e) = sdag::network::hub::create_outbound_conn(&peer) {
            error!(" fail to connected: {}, err={}", peer, e);
        }
    }

    witness::check_and_witness();
    Ok(())
}

fn network_cleanup() {
    network::hub::WSS.close_all();
}

fn main() -> Result<()> {
    init()?;

    connect_to_remote()?;

    timer::start_global_timers();

    // add new_joint event
    register_event_handlers();

    // at least wait for genesis got stable
    sdag::utils::wait_cond(None, || {
        let genesis = match sdag::cache::SDAG_CACHE.get_joint(&*sdag::spec::GENESIS_UNIT) {
            Ok(j) => j,
            _ => return false,
        };

        match genesis.read() {
            Ok(data) => data.is_stable(),
            _ => false,
        }
    })?;

    if !sdag::my_witness::MY_WITNESSES.contains(&WALLET_INFO._00_address) {
        bail!("address {} is not witness");
    }

    // wait user input a ctrl_c to exit
    may_signal::ctrl_c().recv().unwrap();
    network_cleanup();
    info!("bye from main!\n\n");
    Ok(())
}
