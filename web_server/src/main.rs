extern crate lazy_static;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate failure;
extern crate sdag;

use may_minihttp::HttpServer;
use sdag::error::Result;

mod config;
mod server;

fn main() -> Result<()> {
    config::show_config()?;

    let port = config::get_port();
    let ip = String::from("127.0.0.1");

    let server = HttpServer(server::Server).start(ip + ":" + &port)?;
    server.wait();
    Ok(())
}
