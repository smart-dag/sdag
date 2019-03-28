use chrono::{Local, TimeZone};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use sdag::error::Result;
use sdag::network::wallet::WalletConn;
use sdag_wallet_base::Base64KeyExt;

use crate::{
    wallet::{self, WalletInfo},
    REGISTERED_WALLETS, TRANSANTION_NUM,
};

pub fn send_payment(
    ws: &Arc<WalletConn>,
    address_amount: Vec<(String, f64)>,
    wallet_info: &WalletInfo,
    flag: &str,
) -> Result<String> {
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

    let normal_joint = sdag::composer::compose_joint(compose_info.clone(), wallet_info)?;

    if let Err(e) = ws.post_joint(&normal_joint) {
        eprintln!("post_joint err={}", e);
        return Err(e);
    }

    println!("FROM  : {}", wallet_info._00_address);
    println!("TO    : ");
    for (index, (address, amount)) in address_amount.clone().iter().enumerate() {
        if index < 20 {
            println!("      address : {}, amount : {}", address, amount);
        } else {
            println!("      ......");
            println!("      {} outputs", address_amount.len() + 1);
            break;
        }
    }
    println!("UNIT  : {}", normal_joint.unit.unit);

    println!(
        "DATE  : {}",
        Local
            .timestamp_millis(sdag::time::now() as i64)
            .naive_local()
    );
    println!("Total :{}", TRANSANTION_NUM.fetch_add(1, Ordering::SeqCst));

    //println!("\n the original joint: \n [{:#?}] \n", joint);
    match flag {
        "good" => {}
        "nonserial" => {
            compose_info.inputs = ws.get_inputs_from_hub(
                &wallet_info._00_address,
                total_amount + 1000, // we need another 1000 sdg (usually 431 + 197)
                false,               // is_spend_all
                &compose_info.light_props.last_ball_unit,
            )?;

            let joint = sdag::composer::compose_joint(compose_info, wallet_info)?;

            if let Err(e) = ws.post_joint(&joint) {
                error!("post_joint err={}", e);
                return Err(e);
            }

            println!("\n non serial joint: \n [{:#?}] \n", joint);
        }
        "doublespend" => {
            compose_info.light_props.parent_units = vec![normal_joint.unit.unit.clone()];
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

    Ok(normal_joint.unit.unit)
}

// choose a wallet whose index is cur_wallet in test_wallets
pub fn choose_wallet(
    cur_wallet: usize,
    test_wallets: &[wallet::WalletInfo],
) -> Result<&wallet::WalletInfo> {
    let mut registered_wallets = REGISTERED_WALLETS.write().unwrap();

    let mut index = if let Some(v) = registered_wallets.iter().max() {
        *v
    } else {
        0
    };

    while index < test_wallets.len() {
        if !registered_wallets.contains(&index) && index != cur_wallet {
            registered_wallets.insert(index);
            return Ok(&test_wallets[index]);
        }
        index += 1;
    }
    registered_wallets.clear();
    registered_wallets.insert(0);
    Ok(&test_wallets[0])
}

pub fn distribute_token(
    ws: &Arc<WalletConn>,
    paid_wallet: &wallet::WalletInfo,
    token: f64,
    count: usize,
    witnesses: &[String],
    test_wallets: &Vec<wallet::WalletInfo>,
) {
    let mut address_amount = test_wallets
        .iter()
        .map(|w| (w._00_address.clone(), token))
        .collect::<Vec<(String, f64)>>();

    address_amount.append(
        &mut witnesses
            .iter()
            .map(|w| (w.clone(), token))
            .collect::<Vec<(String, f64)>>(),
    );

    // When to compose,it will add a change_address automatically,
    // and each transaction only supports 128 outputs,
    // so MAX_OUTPUTS_PER_PAYMENT_MESSAGE has to sub 1
    for _ in 0..count {
        for chunk in address_amount.chunks(sdag::config::MAX_OUTPUTS_PER_PAYMENT_MESSAGE - 1) {
            if let Ok(hash) = send_payment(&ws, chunk.to_vec(), &paid_wallet, "good") {
                wait_stable(ws, &hash);
            }
        }
    }
}

fn wait_stable(ws: &Arc<WalletConn>, unit: &str) {
    while let Ok(resp) = ws.get_joint_by_unit_hash(&unit) {
        if resp.1.is_stable {
            break;
        }
        may::coroutine::sleep(Duration::from_millis(500));
    }
}
