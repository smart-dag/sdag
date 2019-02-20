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

use chrono::{Local, TimeZone};
use clap::App;
use failure::ResultExt;
use hashbrown::HashSet;
use serde::ser::Serialize;

use sdag_object_base::object_hash;
use sdag_wallet_base::Base64KeyExt;

mod genesis;
mod send_payment;
mod wallet;

use sdag::error::Result;
use sdag::network::wallet::WalletConn;

use self::wallet::WalletInfo;

lazy_static! {
    pub static ref TRANSANTION_NUM: AtomicUsize = AtomicUsize::new(1);
    pub static ref REGISTERED_WALLETS: RwLock<HashSet<usize>> = RwLock::new(HashSet::new());
}

const WALLET_ADDRESSES: &str = "wallets.json";

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

fn genesis_init() -> Result<()> {
    // TODO: get total amount and msg from args
    let total = 500_000_000_000_000;
    let msg = "hello sdag";
    let wallets = genesis::gen_all_wallets()?;

    let genesis_joint = genesis::gen_genesis_joint(&wallets, total, msg)?;

    save_results(&genesis_joint, genesis::GENESIS_FILE)?;
    save_results(
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

    save_results(&result, genesis::INIT_MNEMONIC)?;

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
    save_results(&result, "result.json")
}

fn main() -> Result<()> {
    let yml = load_yaml!("test.yml");
    let m = App::from_yaml(yml).get_matches();
    let verbosity = m.occurrences_of("verbose");
    init(verbosity)?;

    let settings = sdag::config::get_settings();
    let ws = connect_to_remote(&settings.hub_url).context("sdfsd")?;
    let witnesses = ws.get_witnesses()?;

    // init command
    if let Some(init_arg) = m.subcommand_matches("init") {
        if let Some(mnemonic) = init_arg.value_of("MNEMONIC") {
            sdag::config::update_mnemonic(mnemonic)?;
        }
        // create settings
        let settings = sdag::config::get_settings();
        settings.show_config();

        return Ok(());
    }

    if m.subcommand_matches("genesis").is_some() {
        genesis_init()?;
        return Ok(());
    }

    //raw_post
    if let Some(raw_post) = m.subcommand_matches("raw_post") {
        if raw_post.values_of("genesis").is_some() {
            let genesis_file = File::open(genesis::GENESIS_FILE)?;
            ws.post_joint(&serde_json::from_reader(genesis_file)?)?;
            return Ok(());
        }

        if raw_post.values_of("first_pay").is_some() {
            let first_paid_file = File::open(genesis::FIRST_PAYMENT)?;
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
                save_results(&wallets, WALLET_ADDRESSES)?;
            }

            Err(e) => e.exit(),
        }
        return Ok(());
    }

    let wallet_info = WalletInfo::from_mnemonic(&settings.mnemonic)?;
    //transfer
    if let Some(send) = m.subcommand_matches("send") {
        send_payment::distrubite_coins_and_cocurrency(&ws, &send, &wallet_info, &witnesses)?;
    }

    //Send one payment to address
    // Example:
    // 1) pay LWFAESN3EB5E5VFXJ7JWIJB7K5MDQCZE 1
    // 2) pay LWFAESN3EB5E5VFXJ7JWIJB7K5MDQCZE 1 -ns
    // 3) pay LWFAESN3EB5E5VFXJ7JWIJB7K5MDQCZE 1 -ds
    // 4) pay LWFAESN3EB5E5VFXJ7JWIJB7K5MDQCZE 1 -sa
    if let Some(pay) = m.subcommand_matches("pay") {
        let witnesses = ws.get_witnesses()?;
        if witnesses.contains(&wallet_info._00_address) {
            bail!("witness can not send payment by sdg");
        }

        let mut address_amount = Vec::new();
        if let Some(address) = pay.value_of("ADDRESS") {
            if !object_hash::is_chash_valid(address) {
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

        let flag = if pay.values_of("ns").is_some() {
            "nonserial"
        } else if pay.values_of("ds").is_some() {
            "doublespend"
        } else if pay.values_of("sa").is_some() {
            "samejoint"
        } else {
            "good"
        };
        let flags = vec!["good", "nonserial", "doublespend", "samejoint"];
        if !flags.contains(&flag) {
            eprintln!("flag is invalid, valid flag [{:#?}]", flags);
            return Ok(());
        }

        send_payment::send_payment(&ws, address_amount, &wallet_info, flag)?;
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
