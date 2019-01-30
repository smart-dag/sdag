#[macro_use]
extern crate log;
extern crate chrono;
extern crate env_logger;
#[macro_use]
extern crate sdag;
#[macro_use]
extern crate may;
extern crate may_signal;
extern crate num_cpus;
extern crate serde_json;

mod timer;
use sdag::error::Result;
use sdag::*;

fn log_init() {
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

fn start_ws_server() -> Result<::may::coroutine::JoinHandle<()>> {
    use network::hub::WSS;
    use network::WsServer;

    let port = config::get_hub_server_port();

    let server = WsServer::start(("0.0.0.0", port), |c| {
        t!(WSS.add_p2p_conn(c, true));
    })?;
    println!("Websocket server running on ws://0.0.0.0:{}", port);

    Ok(server)
}

fn connect_to_remote() -> Result<()> {
    let peers = config::get_remote_hub_url();

    for peer in peers {
        if let Err(e) = network::hub::create_outbound_conn(&peer) {
            error!(" fail to connected: {}, err={}", peer, e);
        }
    }

    Ok(())
}

fn network_cleanup() {
    network::hub::WSS.close_all();
}

// register global event handlers
fn register_event_handlers() {
    // use main_chain::MciStableEvent;
    use utils::event::Event;
    use validation::NewJointEvent;

    // MciStableEvent::add_handler(|v| t!(network::hub::notify_watchers_about_stable_joints(v.mci)));
    NewJointEvent::add_handler(|e| network::hub::WSS.broadcast_joint(e.joint.clone()));
}

// the hub server logic that run in coroutine context
fn run_hub_server() -> Result<()> {
    register_event_handlers();
    let _server = start_ws_server()?;
    connect_to_remote()?;
    timer::start_global_timers();
    Ok(())
}

fn main() -> Result<()> {
    // init default coroutine settings
    let stack_size = if cfg!(debug_assertions) {
        0x4000
    } else {
        0x2000
    };
    may::config()
        .set_stack_size(stack_size)
        .set_io_workers(num_cpus::get())
        .set_workers(2);

    log_init();
    config::show_config();

    // uncomment it to test read joint from db
    go!(run_hub_server)
        .join()
        .expect("panic inside run_hub_server")?;
    // wait user input a ctrl_c to exit
    may_signal::ctrl_c().recv().unwrap();

    // close all the connections
    network_cleanup();
    info!("bye from main!\n\n");
    Ok(())
}
