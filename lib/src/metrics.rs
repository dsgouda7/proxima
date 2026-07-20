use hdrhistogram::Histogram;
use serde::Serialize;
use std::sync::{Arc, Mutex};

/// Per-instance latency tracker backed by HDR histograms.
///
/// Records every `persist_trie`, `query_region`, and `query_nearby` call
/// duration so callers can observe full latency distributions
/// (p50/p95/p99/p99.9) rather than lossy avg/max summaries.
///
/// Thread-safe — all methods take `&self` and use internal locking that does
/// not span async await points.
pub struct Metrics {
    /// Histogram for `persist_trie` call durations (microseconds).
    write_hist: Mutex<Histogram<u64>>,
    /// Histogram for `query_region` call durations (microseconds).
    read_hist: Mutex<Histogram<u64>>,
    /// Histogram for `query_nearby` call durations (microseconds).
    /// Tracks the full pipeline: cap covering → Redis fetch → haversine filter → sort.
    nearby_hist: Mutex<Histogram<u64>>,
}

impl std::fmt::Debug for Metrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Metrics").finish_non_exhaustive()
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            // 1 µs low bound, 60 s high bound, 3 significant figures
            write_hist: Mutex::new(
                Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).expect("HDR histogram init"),
            ),
            read_hist: Mutex::new(
                Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).expect("HDR histogram init"),
            ),
            nearby_hist: Mutex::new(
                Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).expect("HDR histogram init"),
            ),
        }
    }
}

impl Metrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Record a single write (persist_trie) duration in microseconds.
    #[inline]
    pub fn record_write(&self, us: u64) {
        if let Ok(mut h) = self.write_hist.lock() {
            h.record(us.max(1)).ok();
        }
    }

    /// Record a single read (query_region) duration in microseconds.
    #[inline]
    pub fn record_read(&self, us: u64) {
        if let Ok(mut h) = self.read_hist.lock() {
            h.record(us.max(1)).ok();
        }
    }

    /// Record a single nearby (query_nearby) duration in microseconds.
    ///
    /// This covers the full pipeline: S2 cap covering, Redis SUNION + pipelined
    /// GET, haversine post-filter, and sort. Compare against `record_read` to
    /// quantify the overhead of the post-filter and sort at your entity density.
    #[inline]
    pub fn record_nearby(&self, us: u64) {
        if let Ok(mut h) = self.nearby_hist.lock() {
            h.record(us.max(1)).ok();
        }
    }

    /// Returns a point-in-time snapshot of all latency percentiles.
    pub fn snapshot(&self) -> MetricsSnapshot {
        let wh = self.write_hist.lock().unwrap();
        let rh = self.read_hist.lock().unwrap();
        let nh = self.nearby_hist.lock().unwrap();
        MetricsSnapshot {
            write_count: wh.len(),
            write_p50_us: wh.value_at_quantile(0.50),
            write_p95_us: wh.value_at_quantile(0.95),
            write_p99_us: wh.value_at_quantile(0.99),
            write_p999_us: wh.value_at_quantile(0.999),
            write_max_us: wh.max(),
            read_count: rh.len(),
            read_p50_us: rh.value_at_quantile(0.50),
            read_p95_us: rh.value_at_quantile(0.95),
            read_p99_us: rh.value_at_quantile(0.99),
            read_p999_us: rh.value_at_quantile(0.999),
            read_max_us: rh.max(),
            nearby_count: nh.len(),
            nearby_p50_us: nh.value_at_quantile(0.50),
            nearby_p95_us: nh.value_at_quantile(0.95),
            nearby_p99_us: nh.value_at_quantile(0.99),
            nearby_p999_us: nh.value_at_quantile(0.999),
            nearby_max_us: nh.max(),
        }
    }
}

/// Snapshot of latency distributions at a point in time.
///
/// All latency values are in **microseconds**.  Use `to_ms()` helpers for
/// human-readable output.
#[derive(Debug, Serialize, Clone)]
pub struct MetricsSnapshot {
    // ── Write (persist_trie) ─────────────────────────────────────────────
    pub write_count: u64,
    pub write_p50_us: u64,
    pub write_p95_us: u64,
    pub write_p99_us: u64,
    pub write_p999_us: u64,
    pub write_max_us: u64,
    // ── Read (query_region) ──────────────────────────────────────────────
    pub read_count: u64,
    pub read_p50_us: u64,
    pub read_p95_us: u64,
    pub read_p99_us: u64,
    pub read_p999_us: u64,
    pub read_max_us: u64,
    // ── Nearby (query_nearby) ────────────────────────────────────────────
    /// Number of `query_nearby` calls recorded since process start.
    pub nearby_count: u64,
    /// p50 latency of the full nearby pipeline (µs): cap covering + Redis +
    /// haversine filter + sort.
    pub nearby_p50_us: u64,
    pub nearby_p95_us: u64,
    pub nearby_p99_us: u64,
    pub nearby_p999_us: u64,
    pub nearby_max_us: u64,
}

impl MetricsSnapshot {
    /// Format a microsecond value as a human-readable string (µs or ms).
    pub fn fmt_us(us: u64) -> String {
        if us >= 1_000 {
            format!("{:.2}ms", us as f64 / 1_000.0)
        } else {
            format!("{us}µs")
        }
    }
}
