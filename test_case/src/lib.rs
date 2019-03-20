#[macro_use]
extern crate log;
#[macro_use]
extern crate failure;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate clap;
#[macro_use]
extern crate serde_json;
#[macro_use]
extern crate lazy_static;

extern crate chrono;
extern crate env_logger;
extern crate hashbrown;
extern crate may;
extern crate sdag;
extern crate sdag_object_base;
extern crate sdag_wallet_base;
extern crate serde;

pub mod genesis;
pub mod local_cmd;
pub mod net_cmd;
pub mod transaction;
pub mod wallet;

pub use sdag::error::Result;

use hashbrown::HashSet;
use serde::Serialize;
use std::sync::atomic::AtomicUsize;
use std::sync::RwLock;

lazy_static! {
    pub static ref TRANSANTION_NUM: AtomicUsize = AtomicUsize::new(1);
    pub static ref REGISTERED_WALLETS: RwLock<HashSet<usize>> = RwLock::new(HashSet::new());
}

pub fn save_results<T>(result: &T, path: &str) -> Result<()>
where
    T: Serialize + ?Sized,
{
    let mut results_path = ::std::env::current_dir()?;
    results_path.push(path);

    let file = ::std::fs::File::create(results_path)?;

    serde_json::to_writer_pretty(file, result)?;

    Ok(())
}

pub fn connect_to_remote(
    peers: &[String],
) -> Result<std::sync::Arc<sdag::network::wallet::WalletConn>> {
    for peer in peers {
        match sdag::network::wallet::create_outbound_conn(&peer) {
            Err(e) => {
                error!(" fail to connected: {}, err={}", peer, e);
                continue;
            }
            Ok(c) => return Ok(c),
        }
    }
    bail!("failed to connect remote hub");
}
