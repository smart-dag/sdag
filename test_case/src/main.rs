#[macro_use]
extern crate log;
#[macro_use]
extern crate failure;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate clap;

extern crate chrono;
extern crate env_logger;
extern crate hashbrown;
extern crate may;
extern crate sdag;
extern crate sdag_object_base;
extern crate sdag_wallet_base;
extern crate serde;
#[macro_use]
extern crate serde_json;
#[macro_use]
extern crate lazy_static;

use std::fs::File;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::sync::RwLock;

use clap::App;

use hashbrown::HashSet;
use serde::ser::Serialize;

mod genesis;
mod local_cmd;
mod net_cmd;
mod send_payment;
mod wallet;

use self::wallet::WalletInfo;
use sdag::error::Result;

lazy_static! {
    pub static ref TRANSANTION_NUM: AtomicUsize = AtomicUsize::new(1);
    pub static ref REGISTERED_WALLETS: RwLock<HashSet<usize>> = RwLock::new(HashSet::new());
}

const WALLET_ADDRESSES: &str = "wallets.json";

fn main() -> Result<()> {
    let yml = load_yaml!("test.yml");
    let m = App::from_yaml(yml).get_matches();
    let verbosity = m.occurrences_of("verbose");
    init(verbosity)?;

    let settings = sdag::config::get_settings();

    let arg_local_vec = vec!["init", "genesis", "wallets", "to", "parse"];

    for arg in arg_local_vec {
        if m.is_present(arg) {
            local_cmd::local_cmd(&m)?;
            return Ok(());
        }
    }

    net_cmd::net_cmd(&m, &settings)
}

fn save_results<T>(result: &T, path: &str) -> Result<()>
where
    T: Serialize + ?Sized,
{
    let mut results_path = ::std::env::current_dir()?;
    results_path.push(path);

    let file = ::std::fs::File::create(results_path)?;

    serde_json::to_writer_pretty(file, result)?;

    Ok(())
}

fn init_log(verbosity: u64) {
    let log_lvl = match verbosity {
        0 => log::LevelFilter::Off,
        1 => log::LevelFilter::Error,
        2 => log::LevelFilter::Info,
        _ => log::LevelFilter::Debug,
    };

    let mut builder = env_logger::Builder::from_default_env();
    builder.filter(None, log_lvl).init();

    debug!("log init done!");
}

fn init(verbosity: u64) -> Result<()> {
    // init default coroutine settings
    let stack_size = if cfg!(debug_assertions) {
        0x4000
    } else {
        0x2000
    };
    may::config().set_stack_size(stack_size);

    init_log(verbosity);

    Ok(())
}
