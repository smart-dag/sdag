#[macro_use]
extern crate log;
#[macro_use]
extern crate failure;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate clap;

extern crate chrono;
extern crate fern;
extern crate may;
extern crate sdag;
extern crate sdag_wallet_base;
extern crate serde;
#[macro_use]
extern crate serde_json;

use chrono::{Local, TimeZone};
use clap::App;
use failure::ResultExt;
use may::*;
use sdag_wallet_base::Base64KeyExt;

use rand::{thread_rng, Rng};
use std::sync::Arc;
use std::{thread, time::Duration};

mod config;
mod genesis;
mod wallet;

use sdag::error::Result;
use sdag::network::wallet::WalletConn;

use self::wallet::WalletInfo;

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

fn info(ws: &Arc<WalletConn>, wallet_info: &WalletInfo) -> Result<()> {
    let address_pubk = wallet_info._00_address_pubk.to_base64_key();

    let stable = ws.get_balance(&wallet_info._00_address)? as f64 / 1000_000.0;

    println!("\ncurrent wallet info:\n");
    println!("wallet_public_key: {}", wallet_info.wallet_pubk.to_string());
    println!("   └──address(0/0): {}", wallet_info._00_address);
    println!("      ├── path: /m/44'/0'/0'/0/0");
    println!("      ├── pubkey: {}", address_pubk);
    println!("      └── balance: {:.6}", stable);

    Ok(())
}

fn show_history(
    ws: &Arc<WalletConn>,
    address: &String,
    index: Option<usize>,
    num: usize,
) -> Result<()> {
    let history = ws.get_latest_history(address.clone(), num)?;

    if let Some(index) = index {
        // show special unit's detail information
        if index == 0 || index > history.transactions.len() {
            bail!("invalid transaction index");
        }

        let history = &history.transactions[index - 1];
        let amount;
        if &history.to_addr == address {
            println!("FROM     : {}", history.from_addr);
            amount = history.amount;
        } else {
            println!("TO       : {}", history.to_addr);
            amount = 0 - history.amount;
        }
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
            let amount = if &transaction.to_addr == address {
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
    address_amount: Vec<(String, f64)>,
    wallet_info: &WalletInfo,
) -> Result<()> {
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
        text_message: None,
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

    println!(
        "DATE  : {}",
        Local
            .timestamp_millis(sdag::time::now() as i64)
            .naive_local()
    );

    Ok(())
}

fn is_witness(ws: &Arc<WalletConn>, address: &String) -> Result<bool> {
    let witnesses = ws.get_witnesses()?;
    Ok(witnesses.contains(address))
}

fn continue_sending(
    ws: Arc<WalletConn>,
    wallets_info: &Vec<wallet::WalletInfo>,
    index: usize,
) -> Result<()> {
    let mut rng = thread_rng();

    let n1: usize = rng.gen_range(0, 100);

    let address = wallets_info[n1]._00_address.clone();

    send_payment(&ws, vec![(address, 0.001)], &wallets_info[index])?;
    Ok(())
}

fn genesis_init() -> Result<()> {
    // TODO: get total amount and msg from args
    let total = 500_000_000_000_000;
    let msg = "hello sdag";
    let wallets = genesis::gen_all_wallets()?;

    let genesis_joint = genesis::gen_genesis_joint(&wallets, total, msg)?;

    config::save_results(&genesis_joint, genesis::GENESIS_FILE)?;
    config::save_results(
        &genesis::gen_first_payment(&wallets.sdag_org, 20, &genesis_joint)?,
        genesis::FIRST_PAYMENT,
    )?;

    #[derive(Serialize)]
    struct Tmp<'a> {
        wallets: Vec<&'a String>,
        sdag_org: String,
    };

    let result = Tmp {
        wallets: wallets
            .witnesses
            .iter()
            .map(|v| &v.mnemonic)
            .collect::<Vec<_>>(),

        sdag_org: wallets.sdag_org.mnemonic,
    };

    config::save_results(&result, genesis::INIT_MNEMONIC)
}

fn main() -> Result<()> {
    let yml = load_yaml!("test.yml");
    let m = App::from_yaml(yml).get_matches();
    let verbosity = m.occurrences_of("verbose");
    init(verbosity)?;

    let settings = config::get_settings();
    println!("{:?}", settings);
    let ws = connect_to_remote(&settings.hub_url).context("sdfsd")?;

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

    if m.subcommand_matches("genesis").is_some() {
        genesis_init()?;
        return Ok(());
    }

    //raw_post
    if let Some(raw_post) = m.subcommand_matches("raw_post") {
        if raw_post.values_of("genesis").is_some() {
            let genesis_file = config::open_file(genesis::GENESIS_FILE)?;
            ws.post_joint(&serde_json::from_reader(genesis_file)?)?;
            return Ok(());
        }

        if raw_post.values_of("first_pay").is_some() {
            let first_paid_file = config::open_file(genesis::FIRST_PAYMENT)?;
            ws.post_joint(&serde_json::from_reader(first_paid_file)?)?;
            return Ok(());
        }
    }

    if let Some(n) = m.subcommand_matches("wallets") {
        match value_t!(n.value_of("n"), u64) {
            Ok(num) => {
                let wallets_info = wallet::gen_wallets(num)?;
                let wallets = wallets_info
                    .iter()
                    .map(|v| (v.mnemonic.clone(), v._00_address.clone()))
                    .collect::<Vec<_>>();
                config::save_results(&wallets, config::WALLET_ADDRESSES)?;
            }

            Err(e) => e.exit(),
        }
        return Ok(());
    }

    //transfer
    if let Some(send) = m.subcommand_matches("send") {
        let test_wallets = match wallet::get_wallets() {
            Ok(wallets) => Arc::new(wallets),
            Err(_) => {
                let wallets_info = wallet::gen_wallets(100)?;
                let wallets = wallets_info
                    .iter()
                    .map(|v| (v.mnemonic.clone(), v._00_address.clone()))
                    .collect::<Vec<_>>();
                config::save_results(&wallets, config::WALLET_ADDRESSES)?;
                Arc::new(wallets_info)
            }
        };

        if let Ok(num) = value_t!(send.value_of("continue"), usize) {
            info!("continuously");

            for i in 0..num {
                let tmp_ws = Arc::clone(&ws);
                let tmp_test_wallets = Arc::clone(&test_wallets);

                thread::spawn(move || loop {
                    let tmp_ws = Arc::clone(&tmp_ws);
                    match continue_sending(tmp_ws, &tmp_test_wallets, i) {
                        Err(e) => error!("{}", e),
                        _ => {}
                    };
                    thread::yield_now();
                    thread::sleep(Duration::from_millis(10));
                });
            }

            loop {
                thread::sleep(Duration::from_secs(1));
            }
        }

        if let Ok(num) = value_t!(send.value_of("pay"), u64) {
            let wallet_info = WalletInfo::from_mnemonic(&settings.mnemonic)?;

            if is_witness(&ws, &wallet_info._00_address)? {
                bail!("witness can not send payment by test");
            }

            let address_amount = test_wallets
                .iter()
                .map(|w| (w._00_address.clone(), num as f64))
                .collect::<Vec<(String, f64)>>();

            return send_payment(&ws, address_amount, &wallet_info);
        }
    }

    //balance
    if let Some(arg) = m.subcommand_matches("balance") {
        if let Some(address) = arg.value_of("ADDRESS") {
            println!("{:.6}", ws.get_balance(&address)? as f64 / 1_000_000.0);
        }

        return Ok(());
    }

    let mut wallets_info = wallet::get_wallets()?;
    wallets_info.push(WalletInfo::from_mnemonic(&settings.mnemonic)?);

    //info
    if let Some(arg) = m.subcommand_matches("info") {
        if let Some(address) = arg.value_of("ADDRESS") {
            for wallet in wallets_info {
                if wallet._00_address == address {
                    return info(&ws, &wallet);
                }
            }
        }
        return Ok(());
    }

    //log
    if let Some(log) = m.subcommand_matches("log") {
        let wallet = || -> Result<WalletInfo> {
            if let Some(address) = log.value_of("ADDRESS") {
                for wallet in wallets_info {
                    if wallet._00_address == address {
                        return Ok(wallet);
                    }
                }
            }
            bail!("not found wallet")
        }()?;

        let index = value_t!(log.value_of("v"), usize).ok();
        match value_t!(log.value_of("n"), usize) {
            Ok(num) => {
                println!("num = {}, index = {:?}", num, index);
                return show_history(&ws, &wallet._00_address, index, num);
            }
            Err(clap::Error {
                kind: clap::ErrorKind::ArgumentNotFound,
                ..
            }) => {
                println!("3");
                return show_history(&ws, &wallet._00_address, index, 5);
            }
            Err(e) => e.exit(),
        }
    }

    //show joint and properties
    if let Some(show_args) = m.subcommand_matches("show") {
        if let Some(unit) = show_args.value_of("UNIT") {
            let resp = ws.get_joint_by_unit_hash(unit)?;

            println!("joint = {:#?}", resp.0);
            println!("property = {:#?}", resp.1);
        }
    }

    Ok(())
}
