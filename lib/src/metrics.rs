use std::sync::{
    atomic::{AtomicU64, Ordering::Relaxed},
    Arc,
};
use serde::Serialize;

#[derive(Debug, Default)]
pub struct Metrics {
    write_count:    AtomicU64,
    write_total_us: AtomicU64,
    write_max_us:   AtomicU64,
    read_count:     AtomicU64,
    read_total_us:  AtomicU64,
    read_max_us:    AtomicU64,
}

impl Metrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn record_write(&self, us: u64) {
        self.write_count.fetch_add(1, Relaxed);
        self.write_total_us.fetch_add(us, Relaxed);
        self.write_max_us.fetch_max(us, Relaxed);
    }

    pub fn record_read(&self, us: u64) {
        self.read_count.fetch_add(1, Relaxed);
        self.read_total_us.fetch_add(us, Relaxed);
        self.read_max_us.fetch_max(us, Relaxed);
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        let wc = self.write_count.load(Relaxed);
        let rc = self.read_count.load(Relaxed);
        MetricsSnapshot {
            write_count:  wc,
            write_avg_us: if wc > 0 { self.write_total_us.load(Relaxed) / wc } else { 0 },
            write_max_us: self.write_max_us.load(Relaxed),
            read_count:   rc,
            read_avg_us:  if rc > 0 { self.read_total_us.load(Relaxed) / rc } else { 0 },
            read_max_us:  self.read_max_us.load(Relaxed),
        }
    }
}

#[derive(Debug, Serialize, Clone)]
pub struct MetricsSnapshot {
    pub write_count:  u64,
    pub write_avg_us: u64,
    pub write_max_us: u64,
    pub read_count:   u64,
    pub read_avg_us:  u64,
    pub read_max_us:  u64,
}
