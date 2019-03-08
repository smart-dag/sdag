use std::fs::File;
use std::path::PathBuf;

use super::Result;

const SETTINGS_FILE: &str = "settings.json";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Settings {
    pub port: String,
    pub root_dir: PathBuf,
}

impl Default for Settings {
    fn default() -> Self {
        Settings {
            port: String::from("8080"),
            root_dir: PathBuf::new(),
        }
    }
}

fn open_settings() -> Result<Settings> {
    let mut settings_path = ::std::env::current_dir()?;
    settings_path.push(SETTINGS_FILE);
    let file = File::open(settings_path)?;

    Ok(serde_json::from_reader(file)?)
}

impl Settings {
    fn save_settings(&self) -> Result<()> {
        let mut settings_path = ::std::env::current_dir()?;
        settings_path.push(SETTINGS_FILE);
        let file = File::create(settings_path)?;
        serde_json::to_writer_pretty(file, self)?;
        Ok(())
    }
}

pub fn get_settings() -> Result<Settings> {
    match open_settings() {
        Ok(s) => Ok(s),
        _ => {
            let settings = Settings::default();
            settings.save_settings().ok();
            bail!("not found settings.json")
        }
    }
}

pub fn show_config() -> Result<()> {
    let cfg = get_settings()?;
    println!("\n config:");
    if let Some(root_dir) = cfg.root_dir.to_str() {
        if root_dir.is_empty() {
            bail!("you must set valid root dir")
        }
        println!("\t root_dir = {:?}", root_dir);
    } else {
        bail!("you must set valid root dir for")
    }

    println!("\t port = {:?}", cfg.port);
    println!("\n");
    Ok(())
}

pub fn get_port() -> String {
    get_settings().expect("not fount valid").port
}

pub fn get_root_dir() -> Result<PathBuf> {
    let root_dir = get_settings()?.root_dir;
    if root_dir.exists() {
        return Ok(root_dir);
    }

    bail!(" you must set valid root dir")
}
