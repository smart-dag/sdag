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
extern crate serde_json;

use chrono::{Local, TimeZone};
use clap::App;
use failure::ResultExt;
use may::*;

use rand::{thread_rng, Rng};
use std::sync::Arc;
use std::{thread, time::Duration};

mod config;
mod wallet;

use sdag::error::Result;
use sdag::network::wallet::WalletConn;
use sdag_wallet_base::{Base64KeyExt, ExtendedPrivKey, ExtendedPubKey, Mnemonic};

#[derive(Debug, Clone)]
struct WalletInfo {
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

#[allow(dead_code)]
fn info(ws: &Arc<WalletConn>, wallet_info: &WalletInfo) -> Result<()> {
    let address_pubk = wallet_info._00_address_pubk.to_base64_key();

    let stable = ws.get_balance(&wallet_info._00_address)? as f64 / 1000_000.0;

    println!("\ncurrent wallet info:\n");
    println!("device_address: {}", wallet_info.device_address);
    println!("wallet_public_key: {}", wallet_info.wallet_pubk.to_string());
    println!("└──wallet_id(0): {}", wallet_info.wallet_0_id);

    println!("   └──address(0/0): {}", wallet_info._00_address);
    println!("      ├── path: /m/44'/0'/0'/0/0");
    println!("      ├── pubkey: {}", address_pubk);
    println!("      └── balance: {:.6}", stable);

    Ok(())
}

#[allow(dead_code)]
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

fn continue_sending(ws: &Arc<WalletConn>, wallets: &Vec<wallet::Wallets>) -> Result<()> {
    let mut wallets_info: Vec<WalletInfo> = vec![];
    for w in wallets.iter() {
        wallets_info.push(WalletInfo::from_mnemonic(&w.mnemonic)?);
    }

    thread::sleep(Duration::from_secs(2));
    let mut rng = thread_rng();

    let n1: usize = rng.gen_range(0, 100);
    let n2: usize = rng.gen_range(0, 100);

    let address = wallets_info[n1]._00_address.clone();
    send_payment(&ws, vec![(address, 0.001)], &wallets_info[n2])?;

    Ok(())
}

fn main() -> Result<()> {
    let yml = load_yaml!("test.yml");
    let m = App::from_yaml(yml).get_matches();
    let verbosity = m.occurrences_of("verbose");
    init(verbosity)?;

    let settings = config::get_settings();
    println!("{:?}", settings);
    let ws = connect_to_remote(&settings.hub_url).context("sdfsd")?;

    let wallet_info = WalletInfo::from_mnemonic(&settings.mnemonic)?;
    let test_wallets = Arc::new(wallet::get_wallets()?);
    let test_address = wallet::get_wallets_address(&test_wallets)?;

    if is_witness(&ws, &wallet_info._00_address)? {
        bail!("witness can not send payment by test");
    }

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

    if let Some(n) = m.subcommand_matches("wallets") {
        match value_t!(n.value_of("n"), u64) {
            Ok(num) => {
                let wallets = wallet::gen_wallets(num)?;
                config::save_results(&wallets, config::WALLET_ADDRESSES)?;
            }

            Err(e) => e.exit(),
        }
        return Ok(());
    }

    if let Some(send) = m.subcommand_matches("send") {
        if let Some(_c) = send.values_of("continue") {
            info!("continuously");

            for _ in 0..20 {
                let tmp_ws = Arc::clone(&ws);
                let tmp_test_wallets = Arc::clone(&test_wallets);

                thread::spawn(move || loop {
                    match continue_sending(&tmp_ws, &tmp_test_wallets) {
                        Err(e) => error!("{}", e),
                        _ => {}
                    };
                });
            }

            loop {
                thread::sleep(Duration::from_secs(10));
            }
        } else {
            match value_t!(send.value_of("pay"), u64) {
                Ok(num) => {
                    let address_amount = test_address
                        .iter()
                        .map(|w| (w.clone(), num as f64))
                        .collect::<Vec<(String, f64)>>();

                    return send_payment(&ws, address_amount, &wallet_info);;
                }

                Err(e) => e.exit(),
            }
        }
    }

    Ok(())
}
