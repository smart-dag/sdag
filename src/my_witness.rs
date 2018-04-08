use config;

lazy_static! {
    pub static ref MY_WITNESSES: Vec<String> = config::get_witnesses().to_vec();
}
