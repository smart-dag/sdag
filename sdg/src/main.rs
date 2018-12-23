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
extern crate fern;
extern crate sdag;
extern crate sdag_wallet_base;
extern crate serde;
extern crate serde_json;

mod config;

use std::sync::Arc;
use std::time::Instant;

use chrono::{Local, TimeZone};
use clap::App;
use failure::ResultExt;
use may::sync::Semphore;
use sdag::cache::SDAG_CACHE;
use sdag::error::Result;
use sdag::joint::{Joint, JointSequence};
use sdag::network::wallet::WalletConn;
use sdag::try_go;
use sdag::validation;
use sdag_wallet_base::{Base64KeyExt, ExtendedPrivKey, ExtendedPubKey, Mnemonic};

struct WalletInfo {
    #[allow(dead_code)]
    master_prvk: ExtendedPrivKey,
    wallet_pubk: ExtendedPubKey,
    device_address: String,
    wallet_0_id: String,
    _00_address: String,
    _00_address_pubk: ExtendedPubKey,
    _00_address_prvk: ExtendedPrivKey,
}

impl WalletInfo {
    fn from_mnemonic(mnemonic: &str) -> Result<WalletInfo> {
        let wallet = 0;
        let mnemonic = Mnemonic::from(&mnemonic)?;
        let master_prvk = sdag_wallet_base::master_private_key(&mnemonic, "")?;
        let device_address = sdag_wallet_base::device_address(&master_prvk)?;
        let wallet_pubk = sdag_wallet_base::wallet_pubkey(&master_prvk, wallet)?;
        let wallet_0_id = sdag_wallet_base::wallet_id(&wallet_pubk);
        let _00_address = sdag_wallet_base::wallet_address(&wallet_pubk, false, 0)?;
        let _00_address_prvk = sdag_wallet_base::wallet_address_prvkey(&master_prvk, 0, false, 0)?;
        let _00_address_pubk = sdag_wallet_base::wallet_address_pubkey(&wallet_pubk, false, 0)?;

        Ok(WalletInfo {
            master_prvk,
            wallet_pubk,
            device_address,
            wallet_0_id,
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

fn init_log(verbosity: u64) {
    let log_lvl = match verbosity {
        0 => log::LevelFilter::Off,
        1 => log::LevelFilter::Error,
        2 => log::LevelFilter::Info,
        _ => log::LevelFilter::Debug,
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
        println!("");
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
            if joint_data.unit.content_hash.is_some() {
                joint_data.set_sequence(JointSequence::FinalBad);
            }

            if !joint_data.is_missing_parent() {
                validation::validate_ready_joint(cached_joint)?;
            }

            Ok(())
        });
    }

    sem.wait();
    let dur = now.elapsed();
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
    may::config().set_stack_size(stack_size).set_workers(2);

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

    let wallet_info = WalletInfo::from_mnemonic(&settings.mnemonic)?;

    //info
    if let Some(info_args) = m.subcommand_matches("info") {
        let is_json = info_args.values_of("j").is_some();
        return info(&ws, &wallet_info, is_json);
    }

    //net
    if m.subcommand_matches("net").is_some() {
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
                if !sdag::object_hash::is_chash_valid(arg[0]) {
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

        return send_payment(&ws, text, address_amount, &wallet_info);
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
        //show the list of free joint
        if unit_args.values_of("free").is_some() {
            let units_hash = ws.get_free_joints_list()?;

            for (index, hash) in units_hash.iter().enumerate() {
                println!("{}. unit -> {}", index + 1, hash);
            }

            if units_hash.is_empty() {
                println!("\nthere is no free joints\n");
            }
        }

        if unit_args.values_of("bad").is_some() {
            println!(
                "\nthe number of bad joints = {}\n",
                ws.get_bad_joints_list()?
            );
        }

        if unit_args.values_of("unhandled").is_some() {
            println!(
                "\nthe number of unhandled joints = {}\n",
                ws.get_unhandled_joints_list()?
            );
        }

        //show joint and properties
        if let Some(hash) = unit_args.value_of("show") {
            let resp = ws.get_joint_by_unit_hash(hash)?;

            println!("joint = {:#?}", resp.0);
            println!("property = {:#?}", resp.1);
        }

        if let Ok(mci) = value_t!(unit_args.value_of("mci"), isize) {
            let joints = ws.get_joints_by_mci(mci)?;

            for (index, joint) in joints.iter().enumerate() {
                println!("{}. unit -> {}", index + 1, joint.unit.unit);
            }

            if joints.is_empty() {
                println!("\nthere is no joints with mci={}\n", mci);
            }
        }
    }

    if let Some(dump_args) = m.subcommand_matches("dump") {
        if let Some(file) = dump_args.value_of("FILE") {
            use std::fs::File;
            let mut joints = Vec::new();
            let mut last_mci = 0;
            for i in 0.. {
                let mut stable_joints = ws.get_joints_by_mci(i)?;
                if stable_joints.is_empty() {
                    last_mci = i as usize - 1;
                    println!("last mci = {}", last_mci);
                    break;
                }
                joints.append(&mut stable_joints);
            }
            let mut unstable_joints = ws.get_joints_by_mci(-1)?;
            joints.append(&mut unstable_joints);
            println!("total unit num = {}", joints.len());

            // save all the joints
            let file = File::create(file)?;
            serde_json::to_writer_pretty(&file, &joints)?;
            // verify the joints
            verify_joints(joints, last_mci)?;
        }
        return Ok(());
    }

    Ok(())
}
