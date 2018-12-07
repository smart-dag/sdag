use std::time::Duration;

use may::coroutine;
use sdag::network::hub;

pub fn start_global_timers() {
    // request needed joints that were not received during the previous session
    go!(move || loop {
        info!("re_request_lost_joints");
        t!(hub::re_request_lost_joints());
        coroutine::sleep(Duration::from_secs(8));
    });

    // remove those junk joints
    go!(move || loop {
        const TIMEOUT: u64 = 10 * 60 * 1000; // 10min
        coroutine::sleep(Duration::from_secs(TIMEOUT / 2));
        info!("purge_junk_unhandled_joints");
        hub::purge_junk_unhandled_joints(TIMEOUT);
    });

    // remove those temp-bad free joints
    go!(move || loop {
        const TIMEOUT: u64 = 4 * 60 * 1000; // 4min
        coroutine::sleep(Duration::from_secs(TIMEOUT / 2));
        info!("purge_junk_unhandled_joints");
        t!(hub::purge_temp_bad_free_joints(TIMEOUT));
    });

    // auto connection if peers count is under threshold
    go!(move || loop {
        coroutine::sleep(Duration::from_secs(30));
        info!("auto connect to other peers");
        hub::auto_connection();
    });
}
