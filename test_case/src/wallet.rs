use std::fs::File;

use super::config;
use sdag::error::Result;

#[derive(Debug, Serialize, Deserialize)]
pub struct Wallets {
    pub address: String,
    pub mnemonic: String,
}

impl Wallets {
    // generate a random wallet
    fn new() -> Result<Wallets> {
        let wallet = 0;
        let mnemonic = sdag_wallet_base::mnemonic("")?;
        let master_prvk = sdag_wallet_base::master_private_key(&mnemonic, "")?;
        let wallet_pubk = sdag_wallet_base::wallet_pubkey(&master_prvk, wallet)?;
        let _00_address = sdag_wallet_base::wallet_address(&wallet_pubk, false, 0)?;
        let _00_address_prvk = sdag_wallet_base::wallet_address_prvkey(&master_prvk, 0, false, 0)?;
        let _00_address_pubk = sdag_wallet_base::wallet_address_pubkey(&wallet_pubk, false, 0)?;

        Ok(Wallets {
            mnemonic: mnemonic.to_string(),
            address: _00_address,
        })
    }
}

pub fn gen_wallets(num: u64) -> Result<Vec<Wallets>> {
    let mut wallets: Vec<Wallets> = Vec::new();
    for _ in 0..num {
        wallets.push(Wallets::new()?);
    }
    Ok(wallets)
}

pub fn get_wallets() -> Result<Vec<Wallets>> {
    let mut settings_path = ::std::env::current_dir()?;
    settings_path.push(config::WALLET_ADDRESSES);
    let file = File::open(settings_path)?;
    let settings = serde_json::from_reader(file)?;
    Ok(settings)
}

pub fn get_wallets_address(s: &Vec<Wallets>) -> Result<Vec<String>> {
    Ok(s.iter().map(|v| v.address.clone()).collect::<Vec<_>>())
}
