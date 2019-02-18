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
extern crate env_logger;
extern crate hashbrown;
extern crate may_signal;
extern crate num_cpus;
extern crate rand;
extern crate rcu_cell;
extern crate sdag_object_base;
extern crate sdag_wallet_base;
extern crate serde_json;

mod timer;
mod witness;

use sdag::error::Result;
use sdag::kv_store;
use sdag::network;
use sdag::wallet_info::MY_WALLET;

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

    let mut builder = env_logger::Builder::from_default_env();
    builder.filter(None, log_lvl).init();

    info!("log init done!");
}

fn init() -> Result<()> {
    // init default coroutine settings
    let stack_size = if cfg!(debug_assertions) {
        0x4000
    } else {
        0x2000
    };
    may::config()
        .set_stack_size(stack_size)
        .set_io_workers(num_cpus::get_physical())
        .set_workers(num_cpus::get_physical());;

    init_log();

    kv_store::KV_STORE.rebuild_from_kv()?;

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

    if !sdag::my_witness::MY_WITNESSES.contains(&MY_WALLET._00_address) {
        bail!("address {} is not witness");
    }

    // wait user input a ctrl_c to exit
    may_signal::ctrl_c().recv().unwrap();

    kv_store::KV_STORE.save_unstable_joints()?;

    network_cleanup();
    info!("bye from main!\n\n");
    Ok(())
}
