use std::fs::File;

use error::Result;
use log;
use sdag_wallet_base::{mnemonic, Mnemonic};
use serde_json;
use wallet_info::MY_WALLET;

pub const HASH_LENGTH: usize = 44;
pub const PUBKEY_LENGTH: usize = 44;
pub const SIG_LENGTH: usize = 88;
pub const MAX_COMPLEXITY: usize = 100;
pub const TOTAL_WHITEBYTES: u64 = 500_000_000_000_000;
pub const COUNT_WITNESSES: usize = 12;
pub const MAJORITY_OF_WITNESSES: usize = 7;
pub const VERSION: &str = "1.0";
pub const ALT: &str = "1";
pub const LIBRARY: &str = "rust-sdag";
// TODO: how to read version from Cargo.toml?
pub const LIBRARY_VERSION: &str = "0.1.0";
pub const STALLED_TIMEOUT: usize = 10;
pub const MAX_MESSAGES_PER_UNIT: usize = 128;
pub const MAX_PARENT_PER_UNIT: usize = 16;
pub const MAX_AUTHORS_PER_UNIT: usize = 16;
pub const MAX_SPEND_PROOFS_PER_MESSAGE: usize = 128;
pub const MAX_INPUTS_PER_PAYMENT_MESSAGE: usize = 128;
pub const MAX_OUTPUTS_PER_PAYMENT_MESSAGE: usize = 128;
pub const MAX_AUTHENTIFIER_LENGTH: usize = 4096;
pub const COUNT_MC_BALLS_FOR_PAID_WITNESSING: u32 = 100;
pub const MAX_DATA_FEED_NAME_LENGTH: usize = 64;
pub const MAX_DATA_FEED_VALUE_LENGTH: usize = 64;
pub const MAX_ITEMS_IN_CACHE: usize = 1_000;
pub const MAX_OUTBOUND_CONNECTIONS: usize = 5;
pub const TRANSFER_INPUT_SIZE: u32 = 60;
pub const ADDRESS_SIZE: u32 = 32;
pub const HEADERS_COMMISSION_INPUT_SIZE: u32 = 18;
pub const WITNESSING_INPUT_SIZE: u32 = 26;
pub const MAX_PAYLOAD_SIZE: u32 = 16384; //16k

const SETTINGS_FILE: &str = "settings.json";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Settings {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub log_level: Option<String>, // ["OFF", "ERROR", "WARN", "INFO", "DEBUG", "TRACE"];
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker_thread_num: Option<usize>, // may set_workers()
    pub hub_url: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub listen_address: Option<String>,
    mnemonic: Option<String>,
    pub genesis_unit: Option<String>,
}

impl Default for Settings {
    fn default() -> Self {
        Settings {
            log_level: Some(String::from("WARN")),
            worker_thread_num: Some(4),
            listen_address: Some(String::from("127.0.0.1:6615")),
            hub_url: vec![String::from("127.0.0.1:6615")],
            genesis_unit: Some(String::from("9AXarZlxv7/CgumgfLEmd1tQjyEnyW9JYPXFZUBWrJg=")),
            mnemonic: Some(
                mnemonic("")
                    .expect("failed to generate mnemonic")
                    .to_string(),
            ),
        }
    }
}

fn open_settings() -> Result<Settings> {
    let mut settings_path = ::std::env::current_dir()?;
    settings_path.push(SETTINGS_FILE);
    let file = File::open(settings_path)?;
    let settings = serde_json::from_reader(file)?;
    Ok(settings)
}

impl Settings {
    pub fn show_config(&self) {
        use std::io::stdout;
        println!("settings:");
        serde_json::to_writer_pretty(stdout(), self).unwrap();
        println!("\n");
    }

    fn save_settings(&self) -> Result<()> {
        let mut settings_path = ::std::env::current_dir()?;
        settings_path.push(SETTINGS_FILE);

        let file = File::create(settings_path)?;
        serde_json::to_writer_pretty(file, self)?;
        Ok(())
    }

    fn update_mnemonic(&mut self, mnemonic: &str) -> Result<()> {
        let mnemonic = Some(Mnemonic::from(mnemonic)?.to_string());
        if self.mnemonic != mnemonic {
            info!("will update mnemonic to: {:?}", mnemonic);
            self.mnemonic = mnemonic;
            self.save_settings()?;
        }
        Ok(())
    }

    pub fn get_mnemonic(&self) -> String {
        if let Some(ref v) = self.mnemonic {
            v.clone()
        } else {
            warn!("no mnemonic in settings, will generate one");
            let mnemonic = mnemonic("")
                .expect("failed to generate mnemonic")
                .to_string();
            let mut settings = self.clone();
            settings.mnemonic = Some(mnemonic);
            settings.save_settings().ok();
            settings.mnemonic.unwrap()
        }
    }
}

pub fn update_mnemonic(mnemonic: &str) -> Result<()> {
    let mut settings = get_settings();
    settings.update_mnemonic(mnemonic)
}

pub fn get_settings() -> Settings {
    match open_settings() {
        Ok(s) => s,
        Err(_) => {
            warn!("can't open settings.json, will use default settings");
            let settings = Settings::default();
            settings.save_settings().ok();
            settings
        }
    }
}

pub fn show_config() {
    let cfg = get_settings();
    println!("\nconfig:");
    println!("\tpeer_id = {:?}", MY_WALLET._00_address);
    println!("\thub_url = {:?}", cfg.hub_url);
    println!("\tlisten_address = {:?}", cfg.listen_address);
    println!("\tlog_level = {:?}", cfg.log_level);
    println!(
        "\tworker_thread_num = {:?}",
        cfg.worker_thread_num.unwrap_or(4)
    );
    println!("\n");
}

pub fn get_genesis_unit() -> String {
    let mut settings = get_settings();
    match settings.genesis_unit {
        Some(v) => v,
        None => {
            let genesis_unit = String::from("9AXarZlxv7/CgumgfLEmd1tQjyEnyW9JYPXFZUBWrJg=");
            settings.genesis_unit = Some(genesis_unit);
            settings.save_settings().ok();
            settings.genesis_unit.unwrap()
        }
    }
}

pub fn get_remote_hub_url() -> Vec<String> {
    get_settings().hub_url
}

pub fn get_listen_address() -> Option<String> {
    get_settings().listen_address
}

pub fn get_log_level() -> log::LevelFilter {
    use std::str::FromStr;
    if let Some(v) = get_settings().log_level {
        log::LevelFilter::from_str(&v).unwrap_or(log::LevelFilter::Warn)
    } else if cfg!(debug_assertions) {
        log::LevelFilter::Debug
    } else {
        log::LevelFilter::Warn
    }
}

pub fn get_worker_thread_num() -> usize {
    get_settings().worker_thread_num.unwrap_or(4)
}

pub fn get_mnemonic() -> String {
    let settings = get_settings();
    settings.get_mnemonic()
}
