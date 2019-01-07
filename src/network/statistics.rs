use super::hub;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

lazy_static! {
// Overall stats
    pub static ref STATS: ConnectionStats = ConnectionStats::default();
}

// internal data
#[derive(Debug)]
pub struct ConnectionStats {
    sec: ConnectionStatsPerPeriod,
    min: ConnectionStatsPerPeriod,
    hour: ConnectionStatsPerPeriod,
    day: ConnectionStatsPerPeriod,
}

#[derive(Debug)]
struct ConnectionStatsPerPeriod {
    rx_good: AtomicUsize,
    rx_bad: AtomicUsize,
    tx_total: AtomicUsize,
}

// network interface data
#[derive(Default, Debug, Serialize, Deserialize)]
pub struct ConnStats {
    pub peer_id: String,
    pub peer_addr: String,
    pub last_sec: ConnStatsPerPeriod,
    pub last_min: ConnStatsPerPeriod,
    pub last_hour: ConnStatsPerPeriod,
    pub last_day: ConnStatsPerPeriod,
}

#[derive(Default, Debug, Serialize, Deserialize)]
pub struct ConnStatsPerPeriod {
    pub rx_good: usize,
    pub rx_bad: usize,
    pub tx_total: usize,
}

impl Default for ConnectionStatsPerPeriod {
    fn default() -> Self {
        ConnectionStatsPerPeriod {
            rx_good: AtomicUsize::new(0),
            rx_bad: AtomicUsize::new(0),
            tx_total: AtomicUsize::new(0),
        }
    }
}

impl Default for ConnectionStats {
    fn default() -> Self {
        ConnectionStats {
            sec: ConnectionStatsPerPeriod::default(),
            min: ConnectionStatsPerPeriod::default(),
            hour: ConnectionStatsPerPeriod::default(),
            day: ConnectionStatsPerPeriod::default(),
        }
    }
}

impl ConnectionStatsPerPeriod {
    fn reset(&self) {
        self.rx_good.store(0, Ordering::Relaxed);
        self.rx_bad.store(0, Ordering::Relaxed);
        self.tx_total.store(0, Ordering::Relaxed);
    }

    fn get(&self) -> ConnStatsPerPeriod {
        ConnStatsPerPeriod {
            rx_good: self.rx_good.load(Ordering::Relaxed),
            rx_bad: self.rx_bad.load(Ordering::Relaxed),
            tx_total: self.tx_total.load(Ordering::Relaxed),
        }
    }
}

impl ConnectionStats {
    fn increase_rx_good(&self) {
        self.sec.rx_good.fetch_add(1, Ordering::Relaxed);
        self.min.rx_good.fetch_add(1, Ordering::Relaxed);
        self.hour.rx_good.fetch_add(1, Ordering::Relaxed);
        self.day.rx_good.fetch_add(1, Ordering::Relaxed);
    }

    fn increase_rx_bad(&self) {
        self.sec.rx_bad.fetch_add(1, Ordering::Relaxed);
        self.min.rx_bad.fetch_add(1, Ordering::Relaxed);
        self.hour.rx_bad.fetch_add(1, Ordering::Relaxed);
        self.day.rx_bad.fetch_add(1, Ordering::Relaxed);
    }

    fn increase_tx_total(&self) {
        self.sec.tx_total.fetch_add(1, Ordering::Relaxed);
        self.min.tx_total.fetch_add(1, Ordering::Relaxed);
        self.hour.tx_total.fetch_add(1, Ordering::Relaxed);
        self.day.tx_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn reset_sec(&self) {
        self.sec.reset();
    }

    pub fn reset_min(&self) {
        self.min.reset();
    }

    pub fn reset_hour(&self) {
        self.hour.reset();
    }

    pub fn reset_day(&self) {
        self.day.reset();
    }

    pub fn get_sec(&self) -> ConnStatsPerPeriod {
        self.sec.get()
    }

    pub fn get_min(&self) -> ConnStatsPerPeriod {
        self.min.get()
    }

    pub fn get_hour(&self) -> ConnStatsPerPeriod {
        self.hour.get()
    }

    pub fn get_day(&self) -> ConnStatsPerPeriod {
        self.day.get()
    }
}

pub fn update_statistics(peer_id: Option<&str>, is_rx: bool, is_good: bool) {
    if let Some(peer_id) = peer_id {
        if let Some(conn) = hub::WSS.get_connection(Arc::new(String::from(peer_id))) {
            let stats = conn.get_stats();
            if is_rx {
                if is_good {
                    stats.increase_rx_good();
                    STATS.increase_rx_good();
                } else {
                    stats.increase_rx_bad();
                    STATS.increase_rx_bad();
                }
            } else {
                stats.increase_tx_total();
                STATS.increase_tx_total();
            }
        }
    }
}

pub fn get_overall() -> ConnStats {
    ConnStats {
        peer_id: String::new(),
        peer_addr: String::new(),
        last_sec: STATS.get_sec(),
        last_min: STATS.get_min(),
        last_hour: STATS.get_hour(),
        last_day: STATS.get_day(),
    }
}

pub fn reset_stats_last_sec() {
    hub::WSS.reset_stats_last_sec();
    STATS.sec.reset();
}

pub fn reset_stats_last_min() {
    hub::WSS.reset_stats_last_min();
    STATS.min.reset();
}

pub fn reset_stats_last_hour() {
    hub::WSS.reset_stats_last_hour();
    STATS.hour.reset();
}

pub fn reset_stats_last_day() {
    hub::WSS.reset_stats_last_day();
    STATS.sec.reset();
}
