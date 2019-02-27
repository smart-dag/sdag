use std::sync::{Condvar, Mutex};

use config;

lazy_static! {
    // temp init bridge
    static ref INIT_WITNESSES: Mutex<Vec<String>> = Mutex::new(Vec::new());
    static ref COND_VAR: Condvar = Condvar::new();
    // actual witness data
    pub static ref MY_WITNESSES: [String; config::COUNT_WITNESSES] = {
        let mut result: [String; config::COUNT_WITNESSES] = Default::default();
        let mut g = INIT_WITNESSES.lock().unwrap();
        while g.is_empty() {
            g = COND_VAR.wait(g).unwrap();
        }

        if g.len() == config::COUNT_WITNESSES {
            result.clone_from_slice(&g);
        } else {
            error!("witnesses not init yet!");
            ::std::process::exit(1);
        }
        result
    };
}

/// set my witnesses
pub fn init_my_witnesses(witnesses: &[String]) {
    {
        let mut g = INIT_WITNESSES.lock().unwrap();
        *g = witnesses.to_vec();
        COND_VAR.notify_all();
    }
    assert_eq!(MY_WITNESSES.len(), config::COUNT_WITNESSES);
}
