#[macro_use]
extern crate log;
#[macro_use]
extern crate may;
#[macro_use]
extern crate clap;
#[macro_use]
extern crate failure;
#[macro_use]
extern crate serde_derive;

extern crate chrono;
extern crate env_logger;
extern crate sdag;
extern crate sdag_object_base;
extern crate sdag_wallet_base;
extern crate serde;
extern crate serde_json;

mod config;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{Local, TimeZone};
use clap::App;
use failure::ResultExt;
use may::sync::Semphore;
use sdag::cache::SDAG_CACHE;
use sdag::error::Result;
use sdag::joint::{Joint, JointSequence};
use sdag::network::wallet::WalletConn;
use sdag::statistics::{LastConnStat, StatsPerPeriod};
use sdag::try_go;
use sdag::validation;
use sdag::wallet_info::{WalletInfo, MY_WALLET};
use sdag_object_base::object_hash;
use sdag_wallet_base::Base64KeyExt;

fn init_log(verbosity: u64) {
    let log_lvl = match verbosity {
        0 => log::LevelFilter::Off,
        1 => log::LevelFilter::Error,
        2 => log::LevelFilter::Info,
        _ => log::LevelFilter::Debug,
    };

    let mut builder = env_logger::Builder::from_default_env();
    builder.filter(None, log_lvl).init();

    info!("log init done!");
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

fn connect_to_remote(peers: &[String]) -> Result<Arc<WalletConn>> {
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

fn info(ws: &Arc<WalletConn>, wallet_info: &WalletInfo, is_json: bool) -> Result<()> {
    let address_pubk = wallet_info._00_address_pubk.to_base64_key();

    let stable = ws.get_balance(&wallet_info._00_address)? as f64 / 1_000_000.0;

    if is_json {
        #[derive(Serialize, Debug)]
        struct Info {
            device_address: String,
            wallet_public_key: String,
            wallet_id: String,
            address: String,
            path: String,
            pubkey: String,
            balance: String,
        }

        let info = Info {
            device_address: wallet_info.device_address.clone(),
            wallet_public_key: wallet_info.wallet_pubk.to_string(),
            wallet_id: wallet_info.wallet_0_id.clone(),
            address: wallet_info._00_address.clone(),
            path: "/m/44'/0'/0'/0/0".to_string(),
            pubkey: address_pubk.clone(),
            balance: stable.to_string(),
        };
        serde_json::to_writer_pretty(std::io::stdout(), &info)?;
        println!("\n");
    } else {
        println!("\ncurrent wallet info:\n");
        println!("device_address: {}", wallet_info.device_address);
        println!("wallet_public_key: {}", wallet_info.wallet_pubk.to_string());
        println!("└──wallet_id(0): {}", wallet_info.wallet_0_id);
        println!("   └──address(0/0): {}", wallet_info._00_address);
        println!("      ├── path: /m/44'/0'/0'/0/0");
        println!("      ├── pubkey: {}", address_pubk);
        println!("      └── balance: {:.6}", stable);
    }

    Ok(())
}

fn net_state(ws: &Arc<WalletConn>) -> Result<()> {
    let net_state = ws.get_net_state()?;
    println!("{}", serde_json::to_string_pretty(&net_state)?);
    Ok(())
}

fn print_stats_matrix(stat: LastConnStat) {
    println!("|           |  RX_GOOD |  RX_BAD  |    TX    |");
    println!("|-----------|----------|----------|----------|");

    println!(
        "| LAST_SEC  | {:>8} | {:>8} | {:>8} |",
        stat.sec.rx_good, stat.sec.rx_bad, stat.sec.tx_total
    );

    println!(
        "| LAST_MIN  | {:>8} | {:>8} | {:>8} |",
        stat.min.rx_good, stat.min.rx_bad, stat.min.tx_total
    );

    println!(
        "| LAST_HOUR | {:>8} | {:>8} | {:>8} |",
        stat.hour.rx_good, stat.hour.rx_bad, stat.hour.tx_total
    );

    println!(
        "| LAST_DAY  | {:>8} | {:>8} | {:>8} |",
        stat.day.rx_good, stat.day.rx_bad, stat.day.tx_total
    );
}

fn calc_overall_stats(stats: &HashMap<String, LastConnStat>) -> LastConnStat {
    let mut total_sec = StatsPerPeriod::default();
    let mut total_min = StatsPerPeriod::default();
    let mut total_hour = StatsPerPeriod::default();
    let mut total_day = StatsPerPeriod::default();

    for stat in stats.values() {
        total_sec.rx_good += stat.sec.rx_good;
        total_sec.rx_bad += stat.sec.rx_bad;
        total_sec.tx_total += stat.sec.tx_total;

        total_min.rx_good += stat.min.rx_good;
        total_min.rx_bad += stat.min.rx_bad;
        total_min.tx_total += stat.min.tx_total;

        total_hour.rx_good += stat.hour.rx_good;
        total_hour.rx_bad += stat.hour.rx_bad;
        total_hour.tx_total += stat.hour.tx_total;

        total_day.rx_good += stat.day.rx_good;
        total_day.rx_bad += stat.day.rx_bad;
        total_day.tx_total += stat.day.tx_total;
    }

    LastConnStat {
        sec: total_sec,
        min: total_min,
        hour: total_hour,
        day: total_day,
        ..Default::default()
    }
}

fn net_statistics(ws: &Arc<WalletConn>) -> Result<()> {
    let net_stats = ws.get_net_statistics()?;
    let overall_stats = calc_overall_stats(&net_stats);
    // Overall Stats
    println!("---\n");
    println!("- OVERALL\n");
    print_stats_matrix(overall_stats);

    for (id, stat) in net_stats {
        if stat.is_connected {
            println!("---\n");
            println!("- PEER_ID   : {}", id);
            println!("- PEER_ADDR : {}", stat.peer_addr);
            // println!("- IS_CONN   : {}\n", stat.is_connected);
            print_stats_matrix(stat);
        }
    }

    Ok(())
}

fn net_state_info(ws: &Arc<WalletConn>) -> Result<()> {
    let net_state = ws.get_net_state()?;

    let inbound_num = net_state.in_bounds.len();
    let outbound_num = net_state.out_bounds.len();

    println!("Inbound  : {:>4}", inbound_num);
    println!("Outbound : {:>4}", outbound_num);
    Ok(())
}

fn show_history(
    ws: &Arc<WalletConn>,
    address: &str,
    index: Option<usize>,
    num: usize,
) -> Result<()> {
    let history = ws.get_latest_history(address.to_owned(), num)?;

    if let Some(index) = index {
        // show special unit's detail information
        if index == 0 || index > history.transactions.len() {
            bail!("invalid transaction index");
        }

        let history = &history.transactions[index - 1];
        let amount = if history.to_addr == address {
            println!("FROM     : {}", history.from_addr);
            history.amount
        } else {
            println!("TO       : {}", history.to_addr);
            0 - history.amount
        };
        println!("UNIT     : {}", history.unit_hash);
        println!("AMOUNT   : {:.6} MN", amount as f64 / 1_000_000.0);
        println!(
            "DATE     : {}",
            Local
                .timestamp(history.time.unwrap_or(0) as i64, 0)
                .naive_local()
        );
    } else {
        for (id, transaction) in history.transactions.iter().enumerate() {
            if id > num - 1 {
                break;
            }
            let amount = if transaction.to_addr == address {
                transaction.amount
            } else {
                0 - transaction.amount
            };

            println!(
                "#{:<4} {:>10.6} MN  \t{}",
                id + 1,
                amount as f64 / 1_000_000.0,
                Local
                    .timestamp(transaction.time.unwrap_or(0) as i64, 0)
                    .naive_local()
            );
        }
    }

    Ok(())
}

fn send_payment(
    ws: &Arc<WalletConn>,
    text: Option<&str>,
    address_amount: Vec<(String, f64)>,
    wallet_info: &WalletInfo,
) -> Result<()> {
    let text_message = match text {
        Some(msg) => Some(sdag::composer::create_text_message(msg)?),
        None => None,
    };

    let light_props = ws.get_light_props(&wallet_info._00_address)?;

    let outputs = address_amount
        .iter()
        .map(|(address, amount)| sdag::spec::Output {
            address: address.clone(),
            amount: (amount * 1_000_000.0).round() as u64,
        })
        .collect::<Vec<_>>();

    let total_amount = outputs.iter().fold(0, |acc, x| acc + x.amount);

    let inputs: sdag::light::InputsResponse = ws.get_inputs_from_hub(
        &wallet_info._00_address,
        total_amount + 1000, // we need another 1000 sdg (usually 431 + 197)
        false,               // is_spend_all
        &light_props.last_ball_unit,
    )?;

    let compose_info = sdag::composer::ComposeInfo {
        paid_address: wallet_info._00_address.clone(),
        change_address: wallet_info._00_address.clone(),
        outputs,
        text_message,
        inputs,
        transaction_amount: total_amount,
        light_props,
        pubk: wallet_info._00_address_pubk.to_base64_key(),
    };

    let joint = sdag::composer::compose_joint(compose_info, wallet_info)?;

    if let Err(e) = ws.post_joint(&joint) {
        eprintln!("post_joint err={}", e);
        return Err(e);
    }

    println!("FROM  : {}", wallet_info._00_address);
    println!("TO    : ");
    for (address, amount) in address_amount {
        println!("      address : {}, amount : {}", address, amount);
    }
    println!("UNIT  : {}", joint.unit.unit);

    if text.is_some() {
        println!("TEXT  : {}", text.unwrap_or(""));
    }

    println!(
        "DATE  : {}",
        Local
            .timestamp_millis(sdag::time::now() as i64)
            .naive_local()
    );

    Ok(())
}

fn verify_joints(joints: Vec<Joint>, last_mci: usize) -> Result<()> {
    if joints.is_empty() {
        return Ok(());
    }

    println!("\n===================");
    println!("start to verify");

    let now = Instant::now();
    let sem = Arc::new(Semphore::new(0));
    let total_joints = joints.len();
    register_event_handlers(last_mci, sem.clone());
    for joint in joints {
        try_go!(move || {
            // check content_hash or unit_hash first!
            validation::validate_unit_hash(&joint.unit)?;
            let cached_joint = match SDAG_CACHE.add_new_joint(joint, None) {
                Ok(j) => j,
                Err(e) => {
                    bail!("add_new_joint: err = {}", e);
                }
            };
            let joint_data = cached_joint.read().unwrap();
            if let Some(ref hash) = joint_data.unit.content_hash {
                error!("unit {} content hash = {}", cached_joint.key, hash);
                joint_data.set_sequence(JointSequence::FinalBad);
            }

            if joint_data.is_ready() {
                validation::validate_ready_joint(cached_joint)?;
            }

            Ok(())
        });
    }

    while !sem.wait_timeout(Duration::from_secs(1)) {
        println!("current mci={:?}", sdag::main_chain::get_last_stable_mci());
    }

    let dur = now.elapsed();
    println!("\ntotal mci={:?}", sdag::main_chain::get_last_stable_mci());
    println!("time_used={:?}", dur);
    let sec = dur.as_secs() as f64 + f64::from(dur.subsec_nanos()) / 1_000_000_000.0;
    let tps = total_joints as f64 / sec;
    println!("TPS = {}", tps);
    Ok(())
}

// register global event handlers
fn register_event_handlers(last_mci: usize, sem: Arc<Semphore>) {
    use sdag::main_chain::MciStableEvent;
    use sdag::utils::event::Event;

    MciStableEvent::add_handler(move |v| {
        if v.mci.value() == last_mci {
            sem.post();
        }
    });
}

fn main() -> Result<()> {
    // init default coroutine settings
    let stack_size = if cfg!(debug_assertions) {
        0x4000
    } else {
        0x2000
    };
    may::config().set_stack_size(stack_size).set_workers(4);

    let yml = load_yaml!("sdg.yml");
    let m = App::from_yaml(yml).get_matches();

    let verbosity = m.occurrences_of("verbose");
    init(verbosity)?;

    // init command
    if let Some(init_arg) = m.subcommand_matches("init") {
        if let Some(mnemonic) = init_arg.value_of("MNEMONIC") {
            config::update_mnemonic(mnemonic)?;
        }
        // create settings
        let settings = config::get_settings();
        settings.show_config();
        // every init would remove the local database
        ::std::fs::remove_file(sdag::config::get_database_path(true)).ok();
        return Ok(());
    }

    let settings = config::get_settings();
    let ws = connect_to_remote(&settings.hub_url)?;

    //raw_post
    if let Some(raw_post) = m.subcommand_matches("raw_post") {
        if let Some(file) = raw_post.value_of("JOINT_FILE") {
            println!("raw_post file = {}", file);

            let file = ::std::fs::File::open(file)?;
            let joint = serde_json::from_reader(file)?;

            println!("joint = {:#?}", joint);
            ws.post_joint(&joint)?;
            return Ok(());
        }
        unreachable!("must have a joint json file");
    }

    let wallet_info = &MY_WALLET;

    //info
    if let Some(info_args) = m.subcommand_matches("info") {
        let is_json = info_args.values_of("j").is_some();
        return info(&ws, wallet_info, is_json);
    }

    //net
    if let Some(net) = m.subcommand_matches("net") {
        if net.values_of("info").is_some() {
            return net_state_info(&ws);
        }

        if net.values_of("stats").is_some() {
            return net_statistics(&ws);
        }

        return net_state(&ws);
    }

    //Log
    if let Some(log) = m.subcommand_matches("log") {
        let index = value_t!(log.value_of("v"), usize).ok();

        match value_t!(log.value_of("n"), usize) {
            Ok(num) => {
                return show_history(&ws, &wallet_info._00_address, index, num);
            }
            Err(clap::Error {
                kind: clap::ErrorKind::ArgumentNotFound,
                ..
            }) => {
                return show_history(&ws, &wallet_info._00_address, index, 5);
            }
            Err(e) => e.exit(),
        }
    }

    //Send
    if let Some(send) = m.subcommand_matches("send") {
        let witnesses = ws.get_witnesses()?;
        if witnesses.contains(&wallet_info._00_address) {
            bail!("witness can not send payment by sdg");
        }

        let mut address_amount = Vec::new();
        if let Some(pay) = send.values_of("pay") {
            let v = pay.collect::<Vec<_>>();
            for arg in v.chunks(2) {
                if !object_hash::is_chash_valid(arg[0]) {
                    eprintln!("invalid address, please check");
                    return Ok(());
                }
                let amount = arg[1].parse::<f64>().context("invalid amount arg")?;
                if amount > std::u64::MAX as f64 || amount < 0.000_001 {
                    eprintln!("invalid amount, please check");
                    return Ok(());
                }
                address_amount.push((arg[0].to_string(), amount));
            }
        }

        let text = send.value_of("text");

        return send_payment(&ws, text, address_amount, wallet_info);
    }

    //balance
    if m.subcommand_matches("balance").is_some() {
        println!(
            "{:.6}",
            ws.get_balance(&wallet_info._00_address)? as f64 / 1_000_000.0
        );

        return Ok(());
    }

    //show joint and properties
    if let Some(unit_args) = m.subcommand_matches("unit") {
        return handle_subcommand_unit(unit_args, &ws);
    }

    if let Some(dump_args) = m.subcommand_matches("dump") {
        let is_verbose = dump_args.values_of("verbose").is_some();

        let mut joints = Vec::new();
        let mut last_mci = 0;
        println!("===================");
        println!("get all data from hub");
        for i in 0.. {
            let mut stable_joints = ws.get_joints_by_mci(i)?;
            if stable_joints.is_empty() {
                last_mci = i as usize - 1;
                println!("last mci = {}", last_mci);
                break;
            }
            if is_verbose {
                println!("mci={}, joints_num={}", i, stable_joints.len());
            }
            joints.append(&mut stable_joints);
        }
        let mut unstable_joints = ws.get_joints_by_mci(-1)?;
        joints.append(&mut unstable_joints);
        println!("total unit num = {}", joints.len());

        if let Some(file) = dump_args.value_of("FILE") {
            use std::fs::File;
            // save all the joints
            println!("\n===================");
            println!("write data to file: {}", file);
            let file = File::create(file)?;
            serde_json::to_writer_pretty(&file, &joints)?;
        }

        // verify the joints
        verify_joints(joints, last_mci)?;

        return Ok(());
    }

    Ok(())
}

#[inline]
fn print_unit_hash_list(list: Vec<String>, item_type: &str) {
    for (index, hash) in list.iter().enumerate() {
        println!("{:>4}. unit -> {}", index + 1, hash);
    }

    if list.is_empty() {
        println!("\nthere is no {} joints\n", item_type);
    }
}

fn handle_subcommand_unit(unit_args: &clap::ArgMatches, ws: &Arc<WalletConn>) -> Result<()> {
    // show all valid free joints
    if unit_args.values_of("free").is_some() {
        print_unit_hash_list(ws.get_free_joints()?, "free");
        return Ok(());
    }

    // show the miss joints
    if unit_args.values_of("lost").is_some() {
        print_unit_hash_list(ws.get_missing_joints()?, "missing");
        return Ok(());
    }

    // show the bad joints
    if unit_args.values_of("bad").is_some() {
        print_unit_hash_list(ws.get_bad_joints()?, "bad");
        return Ok(());
    }

    // show the temp-bad joints
    if unit_args.values_of("temp-bad").is_some() {
        print_unit_hash_list(ws.get_temp_bad_joints()?, "temp-bad");
        return Ok(());
    }

    // joints of a specified mci
    if let Ok(mci) = value_t!(unit_args.value_of("mci"), isize) {
        print_unit_hash_list(
            ws.get_joints_by_mci(mci)?
                .into_iter()
                .map(|j| j.unit.unit)
                .collect::<Vec<_>>(),
            &format!("mci={}", mci),
        );
        return Ok(());
    }

    // joints of a specified level
    // let files: Vec<_> = unit_args.values_of("level").unwrap().collect();
    if let Ok(level) = values_t!(unit_args.values_of("level"), usize) {
        let min_level = level[0];
        let max_level = if level.len() == 1 {
            min_level
        } else {
            level[1]
        };
        print_unit_hash_list(
            ws.get_joints_by_level(min_level, max_level)?,
            &format!("min_level={} max_level={}", min_level, max_level),
        );
        return Ok(());
    }

    // overall statistics
    if unit_args.values_of("info").is_some() {
        let sdag::light::NumOfUnit {
            valid_unit,
            known_bad,
            temp_bad,
            unhandled,
            last_stable_mci,
        } = ws.get_joints_info()?;

        println!("the number of various joint\n");
        println!("normal joint      : {}", valid_unit);
        println!("known bad joints  : {}", known_bad);
        println!("temp bad joints   : {}", temp_bad);
        println!("unhandled joints  : {}", unhandled);
        println!("last stable mci   : {:?}", last_stable_mci);
        return Ok(());
    }

    // show joint and properties of a specified unit hash
    if let Some(hash) = unit_args.value_of("show") {
        let resp = ws.get_joint_by_unit_hash(hash)?;

        println!("joint = {:#?}", resp.0);
        println!("property = {:#?}", resp.1);
        return Ok(());
    }

    // show all children of a specified unit hash
    if let Some(hash) = unit_args.value_of("children") {
        print_unit_hash_list(ws.get_children(hash)?, &format!("{}'s children", hash));
        return Ok(());
    }

    bail!("invalid argument value")
}
