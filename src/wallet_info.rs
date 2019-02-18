extern crate sdag_wallet_base;

use self::sdag_wallet_base::{ExtendedPrivKey, ExtendedPubKey, Mnemonic};
use config;
use error::Result;

lazy_static! {
    pub static ref MY_WALLET: WalletInfo = {
        let mnemonic = config::get_mnemonic().expect("failed to read mnemonic form settings");
        WalletInfo::from_mnemonic(&mnemonic).expect("failed to generate wallet info")
    };
}

pub struct WalletInfo {
    #[allow(dead_code)]
    pub master_prvk: ExtendedPrivKey,
    pub wallet_pubk: ExtendedPubKey,
    pub device_address: String,
    pub wallet_0_id: String,
    pub _00_address: String,
    pub _00_address_pubk: ExtendedPubKey,
    pub _00_address_prvk: ExtendedPrivKey,
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

impl ::signature::Signer for WalletInfo {
    fn sign(&self, hash: &[u8], address: &str) -> Result<String> {
        if address != self._00_address {
            bail!("invalid address for wallet to sign");
        }

        sdag_wallet_base::sign(hash, &self._00_address_prvk)
    }
}
