extern crate may_minihttp;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate failure;

mod config;
mod server;

use failure::Error;
use may_minihttp::HttpServer;

pub type Result<T> = ::std::result::Result<T, Error>;

fn main() -> Result<()> {
    config::show_config()?;

    let port = config::get_port();
    let ip = String::from("127.0.0.1");

    let server = HttpServer(server::Server).start(ip + ":" + &port)?;
    server.wait();
    Ok(())
}
