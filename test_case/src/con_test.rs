use clap::ArgMatches;
use may::*;
use rand::{thread_rng, Rng};
use std::sync::Arc;
use std::time::Duration;

use sdag::error::Result;
use sdag::network::wallet::WalletConn;

use super::{
    config,
    wallet::{self, WalletInfo},
    REGISTERED_WALLETS,
};

// choose a wallet more than all trading wallets's index if max index equals test_wallets.len() -1
// other case, choose from usize::min_value()
pub fn choose_wallet(cur_wallet: usize, test_wallets: &[wallet::WalletInfo]) -> Result<usize> {
    if REGISTERED_WALLETS.is_poisoned() {
        bail!("lock poisoned")
    }

    let mut registered_wallets = REGISTERED_WALLETS.write().unwrap();

    let max_wallet = match registered_wallets.iter().max() {
        Some(&max) => {
            if max >= test_wallets.len() - 1 {
                usize::min_value()
            } else {
                max
            }
        }
        _ => usize::min_value(),
    };

    for index in max_wallet..test_wallets.len() {
        if registered_wallets.contains(&index) {
            continue;
        } else {
            if index > max_wallet || max_wallet == 0 {
                registered_wallets.remove(&cur_wallet);
                registered_wallets.insert(index);

                return Ok(index);
            }
        }
    }

    Ok(0)
}

pub fn continue_sending(
    ws: Arc<WalletConn>,
    index: usize,
    test_wallets: &[wallet::WalletInfo],
) -> Result<()> {
    let mut rng = thread_rng();
    let len = test_wallets.len();

    let w1: usize = rng.gen_range(0, len);
    let wallets_info = if index == usize::max_value() {
        &test_wallets[rng.gen_range(0, len)]
    } else {
        &test_wallets[index]
    };

    let paid_wallets = vec![(test_wallets[w1]._00_address.clone(), 0.1)];

    super::send_payment(&ws, paid_wallets, wallets_info, "good")?;

    Ok(())
}

pub fn distrubite_coins_and_cocurrency(
    ws: &Arc<WalletConn>,
    send: &ArgMatches,
    wallet_info: &WalletInfo,
    witnesses: &[String],
) -> Result<()> {
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

    if witnesses.contains(&wallet_info._00_address) {
        bail!("witness can not send payment by test");
    }

    let cycle_index = value_t!(send.value_of("continue"), usize).ok();
    let paid_amount = value_t!(send.value_of("pay"), f64).ok();

    if cycle_index.is_some() && paid_amount.is_none() {
        for i in 0..cycle_index.unwrap() {
            let tmp_ws = Arc::clone(&ws);
            let tmp_test_wallets = Arc::clone(&test_wallets);

            go!(move || {
                let mut index = i;

                REGISTERED_WALLETS.write().unwrap().insert(i);
                loop {
                    let tmp_ws = Arc::clone(&tmp_ws);

                    if let Err(e1) = continue_sending(tmp_ws, index, &tmp_test_wallets) {
                        eprintln!("wallet {} send payment error {} ", index, e1);
                        index = match choose_wallet(index, &tmp_test_wallets) {
                            Ok(v) => {
                                println!("rechoose wallet {} ", v);
                                v
                            }
                            Err(e2) => {
                                coroutine::sleep(Duration::from_secs(10));
                                eprintln!("fail to choose wallet {} ", e2);
                                index
                            }
                        };
                    };

                    coroutine::yield_now();
                    coroutine::sleep(Duration::from_millis(100));
                }
            });
        }

        loop {
            coroutine::sleep(Duration::from_secs(10));
        }
    }

    if let Some(num) = paid_amount {
        let mut address_amount = test_wallets
            .iter()
            .map(|w| (w._00_address.clone(), num as f64))
            .collect::<Vec<(String, f64)>>();

        address_amount.append(
            &mut witnesses
                .iter()
                .map(|w| (w.clone(), num as f64))
                .collect::<Vec<(String, f64)>>(),
        );

        // When to compose,it will add a change_address automatically,
        // and each transaction only supports 128 outputs,
        // so MAX_OUTPUTS_PER_PAYMENT_MESSAGE has to sub 1
        for _ in 0..cycle_index.unwrap_or(1) {
            for chunk in address_amount.chunks(sdag::config::MAX_OUTPUTS_PER_PAYMENT_MESSAGE - 1) {
                while let Err(e) = super::send_payment(&ws, chunk.to_vec(), &wallet_info, "good") {
                    coroutine::sleep(Duration::from_secs(10));
                    eprintln!("{}", e);
                }
                coroutine::sleep(Duration::from_secs(7));
            }
        }
    }

    Ok(())
}
