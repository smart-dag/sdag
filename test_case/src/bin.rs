use clap::App;
use tests::*;

fn main() -> Result<()> {
    let yml = clap::load_yaml!("../test.yml");
    let m = App::from_yaml(yml).get_matches();
    let verbosity = m.occurrences_of("verbose");
    init(verbosity)?;

    let settings = sdag::config::get_settings();

    let arg_local_vec = vec!["init", "genesis", "wallets"];

    for arg in arg_local_vec {
        if m.is_present(arg) {
            local_cmd::local_cmd(&m)?;
            return Ok(());
        }
    }

    net_cmd::net_cmd(&m, &settings)
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

    log::debug!("log init done!");
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
