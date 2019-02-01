#[macro_use]
extern crate log;
#[macro_use]
extern crate may;
#[macro_use]
extern crate failure;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate serde_json;
#[macro_use]
extern crate serde_derive;

extern crate base32;
extern crate base64;
extern crate bit_vec;
extern crate crossbeam;
extern crate hashbrown;
extern crate may_waiter;
extern crate rand;
extern crate rcu_cell;
extern crate ripemd160;
extern crate secp256k1;
extern crate serde;
extern crate sha1;
extern crate sha2;
extern crate smallvec;
extern crate tungstenite;
extern crate url;

#[macro_export]
macro_rules! t {
    ($e:expr) => {
        match $e {
            Ok(val) => val,
            Err(err) => {
                error!("call = {:?} err = {}", stringify!($e), err);
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
                error!("call = {:?} err = {}", stringify!($e), err);
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
        })
    }};

    // for builder/scope spawn
    ($builder:expr, $func:expr) => {{
        fn _go_check<F, E>(f: F) -> F
        where
            F: FnOnce() -> ::std::result::Result<(), E> + Send,
            E: Send,
        {
            f
        }
        let f = _go_check($func);
        go!($builder, move || if let Err(e) = f() {
            error!("coroutine error: {}", e);
        })
    }};
}

#[macro_use]
pub mod utils;

pub mod business;
pub mod cache;
pub mod catchup;
pub mod composer;
pub mod config;
pub mod error;
pub mod finalization;
pub mod joint;
pub mod kv_store;
pub mod light;
pub mod main_chain;
pub mod my_witness;
pub mod network;
mod obj_ser;
pub mod object_hash;
pub mod paid_witnessing;
pub mod serial_check;
pub mod signature;
pub mod spec;
pub mod statistics;
pub mod time;
pub mod validation;
pub mod witness_proof;
