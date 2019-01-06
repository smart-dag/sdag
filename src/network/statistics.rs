use super::hub;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

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
        self.sec.rx_good.store(0, Ordering::Relaxed);
        self.sec.rx_bad.store(0, Ordering::Relaxed);
        self.sec.tx_total.store(0, Ordering::Relaxed);
    }

    pub fn reset_min(&self) {
        self.min.rx_good.store(0, Ordering::Relaxed);
        self.min.rx_bad.store(0, Ordering::Relaxed);
        self.min.tx_total.store(0, Ordering::Relaxed);
    }

    pub fn reset_hour(&self) {
        self.hour.rx_good.store(0, Ordering::Relaxed);
        self.hour.rx_bad.store(0, Ordering::Relaxed);
        self.hour.tx_total.store(0, Ordering::Relaxed);
    }

    pub fn reset_day(&self) {
        self.day.rx_good.store(0, Ordering::Relaxed);
        self.day.rx_bad.store(0, Ordering::Relaxed);
        self.day.tx_total.store(0, Ordering::Relaxed);
    }

    pub fn get_sec(&self) -> ConnStatsPerPeriod {
        ConnStatsPerPeriod {
            rx_good: self.sec.rx_good.load(Ordering::Relaxed),
            rx_bad: self.sec.rx_bad.load(Ordering::Relaxed),
            tx_total: self.sec.tx_total.load(Ordering::Relaxed),
        }
    }

    pub fn get_min(&self) -> ConnStatsPerPeriod {
        ConnStatsPerPeriod {
            rx_good: self.min.rx_good.load(Ordering::Relaxed),
            rx_bad: self.min.rx_bad.load(Ordering::Relaxed),
            tx_total: self.min.tx_total.load(Ordering::Relaxed),
        }
    }

    pub fn get_hour(&self) -> ConnStatsPerPeriod {
        ConnStatsPerPeriod {
            rx_good: self.hour.rx_good.load(Ordering::Relaxed),
            rx_bad: self.hour.rx_bad.load(Ordering::Relaxed),
            tx_total: self.hour.tx_total.load(Ordering::Relaxed),
        }
    }

    pub fn get_day(&self) -> ConnStatsPerPeriod {
        ConnStatsPerPeriod {
            rx_good: self.day.rx_good.load(Ordering::Relaxed),
            rx_bad: self.day.rx_bad.load(Ordering::Relaxed),
            tx_total: self.day.tx_total.load(Ordering::Relaxed),
        }
    }
}

pub fn update_statistics(peer_id: Option<&str>, is_rx: bool, is_good: bool) {
    if let Some(peer_id) = peer_id {
        if let Some(conn) = hub::WSS.get_connection(Arc::new(String::from(peer_id))) {
            let stats = conn.get_stats();
            if is_rx {
                if is_good {
                    stats.increase_rx_good();
                } else {
                    stats.increase_rx_bad();
                }
            } else {
                stats.increase_tx_total();
            }
        }
    }
}
