use config;
use rcu_cell::RcuCell;

lazy_static! {
    // temp init bridge
    static ref INIT_WITNESSES: RcuCell<Vec<String>> = RcuCell::new(None);
    // acutal witness data
    pub static ref MY_WITNESSES: [String; config::COUNT_WITNESSES] = {
        let mut result: [String; config::COUNT_WITNESSES] = Default::default();
        let mut g = INIT_WITNESSES.try_lock().expect("failed to lock init witnesses");
        {
            if let Some(witnesses) = g.as_ref() {
                result.clone_from_slice(witnesses);
            } else {
                error!("witnesses not init yet!");
                ::std::process::abort();
            }
        }
        g.update(None);
        result
    };
}

/// set my witnesses
pub fn init_my_witnesses(witnesses: &[String]) {
    {
        let mut g = INIT_WITNESSES
            .try_lock()
            .expect("failed to lock init witnesses");
        g.update(Some(witnesses.to_vec()));
    }
    assert_eq!(MY_WITNESSES.len(), config::COUNT_WITNESSES);
}
