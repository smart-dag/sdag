use chrono::{Local, TimeZone};
use clap::ArgMatches;
use failure::ResultExt;

use crate::*;
use sdag::error::Result;
use sdag::network::wallet::WalletConn;
use sdag_object_base::object_hash;
use sdag_wallet_base::Base64KeyExt;

pub(super) fn net_cmd(m: &ArgMatches, settings: &sdag::config::Settings) -> Result<()> {
    let ws = connect_to_remote(&settings.hub_url)?;
    let witnesses = ws.get_witnesses()?;
    //raw_post
    if let Some(_) = m.subcommand_matches("raw_post") {
        info!("unimpliment")
    }

    let wallet_info = WalletInfo::from_mnemonic(&settings.get_mnemonic())?;
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
    wallets_info.push(WalletInfo::from_mnemonic(&settings.get_mnemonic())?);

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
