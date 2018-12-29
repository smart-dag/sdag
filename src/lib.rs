#[macro_use]
extern crate log;
#[macro_use]
extern crate may;
extern crate may_waiter;
extern crate serde;
extern crate smallvec;
#[macro_use]
extern crate serde_json;
#[macro_use]
extern crate serde_derive;
extern crate rcu_cell;
extern crate tungstenite;
extern crate url;

#[macro_use]
extern crate failure;

#[macro_use]
extern crate lazy_static;
extern crate base32;
extern crate base64;
extern crate bit_vec;
extern crate crossbeam;
extern crate rand;
extern crate ripemd160;
extern crate secp256k1;
extern crate sha1;
extern crate sha2;

#[macro_export]
macro_rules! t {
    ($e:expr) => {
        match $e {
            Ok(val) => val,
            Err(err) => {
                error!("call = {:?}\nerr = {:?}", stringify!($e), err);
            }
        }
    };
}

#[macro_export]
macro_rules! t_c {
    ($e:expr) => {
        match $e {
            Ok(val) => val,
            Err(err) => {
                error!("call = {:?}\nerr = {:?}", stringify!($e), err);
                continue;
            }
        }
    };
}

// this is a special go macro that can return Result and print the error and backtrace
#[macro_export]
macro_rules! try_go {
    ($func:expr) => {{
        fn _go_check<F, E>(f: F) -> F
        where
            F: FnOnce() -> ::std::result::Result<(), E> + Send + 'static,
            E: Send + 'static,
        {
            f
        }
        let f = _go_check($func);
        go!(move || if let Err(e) = f() {
            error!("coroutine error: {}", e);
            error!("back_trace={}", e.backtrace());
        })
    }};
}

#[macro_use]
pub mod utils;

pub mod cache;
pub mod kv_store;

pub mod business;
pub mod config;
pub mod error;

pub mod my_witness;
pub mod network;
pub mod paid_witnessing;

pub mod serial_check;
pub mod spec;

pub mod catchup;
pub mod composer;
pub mod finalization;
pub mod joint;
pub mod light;
pub mod main_chain;
mod obj_ser;
pub mod object_hash;
pub mod signature;
pub mod time;
pub mod validation;
pub mod witness_proof;
pub use error::{Result, SdagError};
