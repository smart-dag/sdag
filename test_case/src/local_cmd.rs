use clap::ArgMatches;

use crate::*;
use sdag::error::Result;

pub(super) fn local_cmd(m: &ArgMatches) -> Result<()> {
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

    if let Some(n) = m.subcommand_matches("genesis") {
        match value_t!(n.value_of("n"), u32) {
            Ok(num) => genesis_init(num)?,

            Err(e) => {
                error!("{}", e);
                e.exit()
            }
        }

        return Ok(());
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
    }
    Ok(())
}

fn genesis_init(witness_counts: u32) -> Result<()> {
    // TODO: get total amount and msg from args
    let total = 500_000_000_000_000;
    let msg = "hello sdag";
    let wallets = genesis::gen_all_wallets(witness_counts)?;

    let (genesis_joint, balance) = genesis::gen_genesis_joint(&wallets, total, msg)?;
    let first_joint = genesis::gen_first_payment(&wallets.sdag_org, 20, &genesis_joint, balance)?;

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
        first_payment: first_joint,
        genesis_joint,
    };

    save_results(&result, "result.json")
}
