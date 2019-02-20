use sdag_wallet_base::{ExtendedPrivKey, ExtendedPubKey, Mnemonic};
use std::fs::File;

use super::WALLET_ADDRESSES;
use sdag::error::Result;

#[derive(Debug, Serialize, Deserialize)]
pub struct Wallets {
    pub address: String,
    pub mnemonic: String,
}

#[derive(Debug, Clone)]
pub struct WalletInfo {
    pub mnemonic: String,
    pub master_prvk: ExtendedPrivKey,
    pub wallet_pubk: ExtendedPubKey,
    pub _00_address: String,
    pub _00_address_pubk: ExtendedPubKey,
    pub _00_address_prvk: ExtendedPrivKey,
}

impl WalletInfo {
    pub fn from_mnemonic(mnemonic: &str) -> Result<WalletInfo> {
        let wallet = 0;

        let mnemonic = if mnemonic.is_empty() {
            sdag_wallet_base::mnemonic(&mnemonic)?
        } else {
            Mnemonic::from(&mnemonic)?
        };

        let master_prvk = sdag_wallet_base::master_private_key(&mnemonic, "")?;
        let wallet_pubk = sdag_wallet_base::wallet_pubkey(&master_prvk, wallet)?;
        let _00_address = sdag_wallet_base::wallet_address(&wallet_pubk, false, 0)?;
        let _00_address_prvk = sdag_wallet_base::wallet_address_prvkey(&master_prvk, 0, false, 0)?;
        let _00_address_pubk = sdag_wallet_base::wallet_address_pubkey(&wallet_pubk, false, 0)?;

        Ok(WalletInfo {
            mnemonic: mnemonic.to_string(),
            master_prvk,
            wallet_pubk,
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

pub fn gen_wallets(num: u64) -> Result<Vec<WalletInfo>> {
    let mut wallets: Vec<WalletInfo> = Vec::new();
    for _ in 0..num {
        wallets.push(WalletInfo::from_mnemonic("")?);
    }
    Ok(wallets)
}

pub fn get_wallets() -> Result<Vec<WalletInfo>> {
    let mut settings_path = ::std::env::current_dir()?;
    settings_path.push(WALLET_ADDRESSES);
    let file = File::open(settings_path)?;
    let wallets: Vec<(String, String)> = serde_json::from_reader(file)?;
    let mut wallets_info: Vec<WalletInfo> = vec![];
    for m in wallets {
        wallets_info.push(WalletInfo::from_mnemonic(&m.0)?);
    }
    Ok(wallets_info)
}
