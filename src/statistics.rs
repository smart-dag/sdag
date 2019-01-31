use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use network::hub;

lazy_static! {
    // stored all connection statistics
    static ref ALL_CONN_STATS: AllConnStats = AllConnStats::default();
}

//---------------------------------------------------------------------------------------
// AllConnStats
//---------------------------------------------------------------------------------------
#[derive(Default)]
pub struct AllConnStats {
    // key is peer_id, val is ConnStats
    conn_stats: RwLock<HashMap<Arc<String>, ConnStats>>,
}

impl AllConnStats {
    /// hub timer call update every secs
    /// 1) if timestamp % 60 == 0, collect all secs and set mins[timestamp/60], then reset all secs to None
    /// 2) if timestamp % 3600 == 0, collect all mins and set hours[(timestamp/3600)%24], then reset all mins to None
    /// 3) if timestamp % 86400 == 0, collect all hours and set days[(timestamp/86400)%30], then reset all hours to None
    fn update(&self) {
        let timestamp = (::time::now() / 1000) as usize;
        let is_mins = timestamp % 60 == 0;
        if !is_mins {
            return;
        }

        let mins_index = (timestamp / 60) % 60;
        let is_hours = timestamp % 3600 == 0;
        let hours_index = (timestamp / 3600) % 24;
        let is_days = timestamp % 86400 == 0;
        let days_index = (timestamp / 86400) % 30;

        let mut to_remove = Vec::new();

        let mut w_g = self.conn_stats.write().unwrap();

        for (id, stat) in w_g.iter_mut() {
            if is_mins {
                let total_secs = sum_all_stats(&stat.secs);
                stat.mins[mins_index] = total_secs;
                stat.secs = [StatsPerPeriod::default(); 60];
            }
            if is_hours {
                let total_mins = sum_all_stats(&stat.mins);
                stat.hours[hours_index] = total_mins;
                stat.mins = [StatsPerPeriod::default(); 60];
            }
            if is_days {
                let total_hours = sum_all_stats(&stat.hours);
                stat.days[days_index] = total_hours;
                stat.hours = [StatsPerPeriod::default(); 24];

                // if the connection has no any stats in last 1 day, then remove it
                if total_hours.is_zero() {
                    to_remove.push(id.clone());
                }
            }
        }

        for id in to_remove {
            w_g.remove(&id);
        }
    }

    /// get all last stats, last stats may less than the real stats
    /// example: if now is 01:30:30
    /// 1) last_min is stats of (01:30:00, 01:30:30]
    /// 2) last_hour is stats of (01:00:00, 01:30:30]
    /// 3) last_day is stats of (00:30:00, 01:30:30]
    fn get_all_last_stats(&self) -> HashMap<String, LastConnStat> {
        let now = (::time::now() / 1000) as usize;
        let mut all_stats = HashMap::new();
        let r_g = self.conn_stats.read().unwrap();

        for (id, stat) in r_g.iter() {
            let total_sec = (*stat).secs[now % 60];
            let total_min = sum_all_stats(&stat.secs);
            let total_hour = sum_all_stats(&[sum_all_stats(&stat.mins), total_min]);
            let total_day = sum_all_stats(&[sum_all_stats(&stat.hours), total_hour]);

            let last_stat = LastConnStat {
                peer_addr: stat.peer_addr.to_string(),
                sec: total_sec,
                min: total_min,
                hour: total_hour,
                day: total_day,
                is_connected: false,
            };
            all_stats.insert(id.to_string(), last_stat);
        }

        all_stats
    }

    fn increase_sec(&self, peer_id: Arc<String>, is_rx: bool, is_good: bool) {
        let index = (::time::now() / 1000 % 60) as usize;

        let mut w_g = self.conn_stats.write().unwrap();

        w_g.entry(peer_id.clone())
            .and_modify(|stat| {
                stat.secs[index].increase(is_rx, is_good);
            })
            .or_insert_with(|| {
                // init a new conn_stat and insert
                let mut new_stat = StatsPerPeriod::default();
                new_stat.increase(is_rx, is_good);

                let peer_addr = match hub::WSS.get_connection(peer_id) {
                    Some(conn) => conn.get_peer_addr().to_string(),
                    None => String::from("unknown"),
                };

                let mut new_stats = ConnStats::new(peer_addr);
                new_stats.secs[index] = new_stat;
                new_stats
            });
    }

    fn get_peer_id_by_address(&self, peer_addr: &str) -> Option<String> {
        let r_g = self.conn_stats.read().unwrap();
        for (key, val) in r_g.iter() {
            if peer_addr == val.peer_addr {
                return Some(key.to_string());
            }
        }

        None
    }
}

//---------------------------------------------------------------------------------------
// ConnStats
//---------------------------------------------------------------------------------------

pub struct ConnStats {
    peer_addr: String,
    secs: [StatsPerPeriod; 60],  // capacity 60
    mins: [StatsPerPeriod; 60],  // capacity 60
    hours: [StatsPerPeriod; 24], // capacity 24
    days: [StatsPerPeriod; 30],  // capacity 30
}

impl ConnStats {
    fn new(peer_addr: String) -> Self {
        ConnStats {
            peer_addr,
            secs: [StatsPerPeriod::default(); 60],
            mins: [StatsPerPeriod::default(); 60],
            hours: [StatsPerPeriod::default(); 24],
            days: [StatsPerPeriod::default(); 30],
        }
    }
}

//---------------------------------------------------------------------------------------
// StatsPerPeriod
//---------------------------------------------------------------------------------------
#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize)]
pub struct StatsPerPeriod {
    pub rx_good: usize,
    pub rx_bad: usize,
    pub tx_total: usize,
}

impl StatsPerPeriod {
    fn increase(&mut self, is_rx: bool, is_good: bool) {
        if is_rx {
            if is_good {
                self.rx_good += 1;
            } else {
                self.rx_bad += 1;
            }
        } else {
            self.tx_total += 1;
        }
    }

    fn is_zero(&self) -> bool {
        self.rx_good == 0 && self.rx_bad == 0 && self.tx_total == 0
    }
}

//---------------------------------------------------------------------------------------
// LastConnStat
//---------------------------------------------------------------------------------------
/// network interface struct
#[derive(Default, Serialize, Deserialize)]
pub struct LastConnStat {
    pub peer_addr: String,
    pub sec: StatsPerPeriod,
    pub min: StatsPerPeriod,
    pub hour: StatsPerPeriod,
    pub day: StatsPerPeriod,
    pub is_connected: bool,
}

//---------------------------------------------------------------------------------------
// Global Functions
//---------------------------------------------------------------------------------------
#[inline]
fn sum_all_stats(stats: &[StatsPerPeriod]) -> StatsPerPeriod {
    let mut total_state = StatsPerPeriod::default();

    for stat in stats {
        total_state.rx_good += stat.rx_good;
        total_state.rx_bad += stat.rx_bad;
        total_state.tx_total += stat.tx_total;
    }

    total_state
}

/// hub timer call the func every secs, to update all connection statistics
pub fn update_stats() {
    ALL_CONN_STATS.update()
}

/// only increase secs, mins/hours/days will update by timer
pub fn increase_stats(peer_id: Arc<String>, is_rx: bool, is_good: bool) {
    ALL_CONN_STATS.increase_sec(peer_id, is_rx, is_good);
}

/// network interface: get all last statistics
pub fn get_all_last_stats() -> HashMap<String, LastConnStat> {
    ALL_CONN_STATS.get_all_last_stats()
}

pub fn get_peer_id_by_address(peer_addr: &str) -> Option<String> {
    ALL_CONN_STATS.get_peer_id_by_address(peer_addr)
}
