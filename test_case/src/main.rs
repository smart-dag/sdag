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
#[macro_use]
extern crate lazy_static;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chrono::{Local, TimeZone};
use clap::App;
use failure::ResultExt;
use may::*;
use rand::{thread_rng, Rng};
use sdag_wallet_base::Base64KeyExt;

mod config;
mod genesis;
mod wallet;

use sdag::error::Result;
use sdag::network::wallet::WalletConn;

use self::wallet::WalletInfo;

lazy_static! {
    pub static ref TRANSANTION_NUM: AtomicUsize = AtomicUsize::new(1);
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

fn info(ws: &Arc<WalletConn>, wallet_info: &WalletInfo) -> Result<()> {
    let address_pubk = wallet_info._00_address_pubk.to_base64_key();

    let stable = ws.get_balance(&wallet_info._00_address)? as f64 / 1_000_000.0;

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
    address: &str,
    index: Option<usize>,
    num: usize,
) -> Result<()> {
    let history = ws.get_latest_history(address.to_string(), num)?;

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
    address_amount: Vec<(String, f64)>,
    wallet_info: &WalletInfo,
    flag: &str,
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

    let mut compose_info = sdag::composer::ComposeInfo {
        paid_address: wallet_info._00_address.clone(),
        change_address: wallet_info._00_address.clone(),
        outputs,
        text_message: None,
        inputs,
        transaction_amount: total_amount,
        light_props,
        pubk: wallet_info._00_address_pubk.to_base64_key(),
    };

    let joint = sdag::composer::compose_joint(compose_info.clone(), wallet_info)?;

    if let Err(e) = ws.post_joint(&joint) {
        eprintln!("post_joint err={}", e);
        return Err(e);
    }

    println!("FROM  : {}", wallet_info._00_address);
    println!("TO    : ");
    for (address, amount) in address_amount.clone() {
        println!("      address : {}, amount : {}", address, amount);
    }
    println!("UNIT  : {}", joint.unit.unit);

    println!(
        "DATE  : {}",
        Local
            .timestamp_millis(sdag::time::now() as i64)
            .naive_local()
    );
    println!("Total :{}", TRANSANTION_NUM.fetch_add(1, Ordering::SeqCst));

    println!("\n the original joint: \n [{:#?}] \n", joint);

    match flag {
        "good" => return Ok(()),
        "nonserial" => {
            compose_info.inputs = ws.get_inputs_from_hub(
                &wallet_info._00_address,
                total_amount + 1000, // we need another 1000 sdg (usually 431 + 197)
                false,               // is_spend_all
            )?;

            let joint = sdag::composer::compose_joint(compose_info, wallet_info)?;

            if let Err(e) = ws.post_joint(&joint) {
                eprintln!("post_joint err={}", e);
                return Err(e);
            }

            println!("\n non serial joint: \n [{:#?}] \n", joint);
        }
        "doublespend" => {
            compose_info.light_props.parent_units = vec![joint.unit.unit];
            let joint = sdag::composer::compose_joint(compose_info, wallet_info)?;

            if let Err(e) = ws.post_joint(&joint) {
                eprintln!("post_joint err={}", e);
                return Err(e);
            }

            println!("\n double spend joint: \n [{:#?}] \n", joint);
        }
        "samejoint" => {
            let joint = sdag::composer::compose_joint(compose_info, wallet_info)?;

            if let Err(e) = ws.post_joint(&joint) {
                eprintln!("post_joint err={}", e);
                return Err(e);
            }

            println!("\n the same joint: \n [{:#?}] \n", joint);
        }
        _ => bail!("flag is invalid"),
    }

    Ok(())
}

fn continue_sending(ws: Arc<WalletConn>, wallets_info: &[wallet::WalletInfo]) -> Result<()> {
    let mut rng = thread_rng();

    let n1: usize = rng.gen_range(0, 100);
    let n2: usize = rng.gen_range(0, 100);

    let wallets = vec![(wallets_info[n1]._00_address.clone(), 0.001)];

    if let Err(e) = send_payment(&ws, wallets, &wallets_info[n2], "good") {
        coroutine::sleep(Duration::from_secs(10));
        eprintln!("{}", e);
    }

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
    struct MNEMONIC<'a> {
        wallets: Vec<&'a String>,
        sdag_org: &'a String,
    };

    let result = MNEMONIC {
        wallets: wallets
            .witnesses
            .iter()
            .map(|v| &v.mnemonic)
            .collect::<Vec<_>>(),

        sdag_org: &wallets.sdag_org.mnemonic,
    };

    config::save_results(&result, genesis::INIT_MNEMONIC)?;

    use sdag::joint::Joint;
    #[derive(Serialize)]
    struct GENESIS<'a> {
        wallets: Vec<&'a String>,
        sdag_org: &'a String,
        first_payment: Joint,
        genesis_joint: Joint,
    }
    let result = GENESIS {
        wallets: wallets
            .witnesses
            .iter()
            .map(|v| &v.mnemonic)
            .collect::<Vec<_>>(),
        sdag_org: &wallets.sdag_org.mnemonic,
        first_payment: genesis::gen_first_payment(&wallets.sdag_org, 20, &genesis_joint)?,
        genesis_joint,
    };
    config::save_results(&result, "result.json")
}

fn main() -> Result<()> {
    let yml = load_yaml!("test.yml");
    let m = App::from_yaml(yml).get_matches();
    let verbosity = m.occurrences_of("verbose");
    init(verbosity)?;

    let settings = config::get_settings();
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

        let wallet_info = WalletInfo::from_mnemonic(&settings.mnemonic)?;
        let witnesses = ws.get_witnesses()?;

        if witnesses.contains(&wallet_info._00_address) {
            bail!("witness can not send payment by test");
        }

        let cycle_index = value_t!(send.value_of("continue"), usize).ok();
        let amount = value_t!(send.value_of("pay"), f64).ok();

        if cycle_index.is_some() && amount.is_none() {
            info!("continuously");

            for _ in 0..cycle_index.unwrap() {
                let tmp_ws = Arc::clone(&ws);
                let tmp_test_wallets = Arc::clone(&test_wallets);

                go!(move || loop {
                    let tmp_ws = Arc::clone(&tmp_ws);
                    if let Err(e) = continue_sending(tmp_ws, &tmp_test_wallets) {
                        error!("continue_sending err={}", e);
                    };
                    coroutine::yield_now();
                    coroutine::sleep(Duration::from_millis(10));
                });
            }

            loop {
                coroutine::sleep(Duration::from_secs(1));
            }
        }

        if let Some(num) = amount {
            let address_amount = test_wallets
                .iter()
                .map(|w| (w._00_address.clone(), num as f64))
                .collect::<Vec<(String, f64)>>();

            if let Some(index) = cycle_index {
                for _ in 0..index {
                    while let Err(e) =
                        send_payment(&ws, address_amount.clone(), &wallet_info, "good")
                    {
                        coroutine::sleep(Duration::from_secs(10));
                        eprintln!("{}", e);
                    }
                }
            } else {
                send_payment(&ws, address_amount, &wallet_info, "good")?;
            }
            return Ok(());
        }
    }

    //Send one payment to address
    // Example:
    // 1) pay LWFAESN3EB5E5VFXJ7JWIJB7K5MDQCZE 1
    // 2) pay LWFAESN3EB5E5VFXJ7JWIJB7K5MDQCZE 1 -ns
    // 3) pay LWFAESN3EB5E5VFXJ7JWIJB7K5MDQCZE 1 -ds
    // 4) pay LWFAESN3EB5E5VFXJ7JWIJB7K5MDQCZE 1 -sa
    if let Some(pay) = m.subcommand_matches("pay") {
        let wallet_info = WalletInfo::from_mnemonic(&settings.mnemonic)?;
        let witnesses = ws.get_witnesses()?;
        if witnesses.contains(&wallet_info._00_address) {
            bail!("witness can not send payment by sdg");
        }

        let mut address_amount = Vec::new();
        if let Some(address) = pay.value_of("ADDRESS") {
            if !sdag::object_hash::is_chash_valid(address) {
                eprintln!("invalid address, please check");
                return Ok(());
            }
            if let Some(v) = pay.value_of("AMOUNT") {
                let amount = v.parse::<f64>().context("invalid amount arg")?;
                if amount > std::u64::MAX as f64 || amount < 0.000_001 {
                    eprintln!("invalid amount, please check");
                    return Ok(());
                }
                address_amount.push((address.to_string(), amount));
            }
        }

        if address_amount.is_empty() {
            eprintln!("address or amount is none");
            return Ok(());
        }

        let mut flag = "good";
        if pay.values_of("ns").is_some() {
            flag = "nonserial";
        }
        if pay.values_of("ds").is_some() {
            flag = "doublespend";
        }
        if pay.values_of("sa").is_some() {
            flag = "samejoint";
        }
        let flags = vec!["good", "nonserial", "doublespend", "samejoint"];
        if !flags.contains(&flag) {
            eprintln!("flag is invalid, valid flag [{:#?}]", flags);
            return Ok(());
        }

        send_payment(&ws, address_amount, &wallet_info, flag)?;
    }

    //balance
    if let Some(arg) = m.subcommand_matches("balance") {
        if let Some(address) = arg.value_of("ADDRESS") {
            println!("\naddress: {}", address);
            println!(
                "balance: {:.6}\n",
                ws.get_balance(&address)? as f64 / 1_000_000.0
            );
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
