//! geo-redis-loadtest
//!
//! Fires parallel Tokio tasks to simulate production-scale Redis load:
//!   - N writer tasks each pipeline-writing a full aircraft batch every cycle
//!   - M reader tasks each doing SUNION + batch-GET for a random map viewport
//!
//! Latencies are captured in HDR histograms (p50/p95/p99/p99.9/max).
//!
//! Usage:
//!   cargo run --release -p geo-redis-loadtest -- --help
//!   cargo run --release -p geo-redis-loadtest -- --writers 4 --readers 16 --duration-secs 60

use std::{
    f64::consts::PI,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering::Relaxed},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

use anyhow::Result;
use clap::Parser;
use hdrhistogram::Histogram;
use proxima::{GeoEntry, GeoTrie};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use redis::{aio::ConnectionManager, AsyncCommands};
use s2::{cap::Cap, latlng::LatLng, point::Point, region::RegionCoverer, s1};
use serde_json::json;

// ── CLI ───────────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(
    name = "geo-redis-loadtest",
    about = "Production-scale load test: parallel writers + readers against a live Redis"
)]
struct Args {
    /// Redis connection URL
    #[arg(long, env = "REDIS_URL", default_value = "redis://127.0.0.1:6379")]
    redis_url: String,

    /// Number of concurrent writer tasks
    #[arg(long, default_value_t = 4)]
    writers: usize,

    /// Number of concurrent reader tasks
    #[arg(long, default_value_t = 16)]
    readers: usize,

    /// Test duration in seconds
    #[arg(long, default_value_t = 60)]
    duration_secs: u64,

    /// Aircraft entries per writer batch (5000 ≈ one regional feed)
    #[arg(long, default_value_t = 5_000)]
    batch_size: usize,

    /// S2 cell level (9 ≈ 70 km cells, 12 ≈ 2 km)
    #[arg(long, default_value_t = 9)]
    s2_level: u8,

    /// Pipeline chunk size (commands per Redis round-trip)
    #[arg(long, default_value_t = 500)]
    chunk_size: usize,

    /// Optional: comma-separated shard specs for distributed mode.
    /// Format: prefix_start:prefix_end:redis_url  (repeat per shard)
    /// Example: ":5:redis://localhost:6379,5:a:redis://localhost:6380,a::redis://localhost:6381"
    /// When set, writers route each aircraft to the correct shard by S2 token.
    #[arg(long)]
    shards: Option<String>,
}

// ── Shard config ──────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
struct ShardSpec {
    prefix_start: String,
    prefix_end: String,
    redis_url: String,
}

impl ShardSpec {
    /// Parse "prefix_start:prefix_end:redis_url"
    fn parse(s: &str) -> anyhow::Result<Self> {
        let parts: Vec<&str> = s.splitn(3, ':').collect();
        if parts.len() < 3 {
            anyhow::bail!("shard spec must be prefix_start:prefix_end:redis_url, got: {s}");
        }
        // Handle redis:// URLs that contain colons — rejoin from index 2
        let url_start = s.find(':').unwrap() + 1;
        let url_start = s[url_start..]
            .find(':')
            .map(|i| url_start + i + 1)
            .unwrap_or(s.len());
        Ok(Self {
            prefix_start: parts[0].to_string(),
            prefix_end: parts[1].to_string(),
            redis_url: s[url_start..].to_string(),
        })
    }

    fn owns(&self, token: &str) -> bool {
        let ge = self.prefix_start.is_empty() || token >= self.prefix_start.as_str();
        let lt = self.prefix_end.is_empty() || token < self.prefix_end.as_str();
        ge && lt
    }
}

// ── Shared metrics ────────────────────────────────────────────────────────

struct Metrics {
    write_hist: Mutex<Histogram<u64>>,
    read_hist: Mutex<Histogram<u64>>,
    write_ops: AtomicU64,
    write_aircraft: AtomicU64,
    read_ops: AtomicU64,
    read_misses: AtomicU64,
    /// Per-shard write counts (index matches shards Vec or 0 for single-shard mode)
    shard_writes: Vec<AtomicU64>,
}

impl Metrics {
    fn new(num_shards: usize) -> Arc<Self> {
        Arc::new(Self {
            write_hist: Mutex::new(Histogram::new(3).expect("histogram")),
            read_hist: Mutex::new(Histogram::new(3).expect("histogram")),
            write_ops: AtomicU64::new(0),
            write_aircraft: AtomicU64::new(0),
            read_ops: AtomicU64::new(0),
            read_misses: AtomicU64::new(0),
            shard_writes: (0..num_shards.max(1)).map(|_| AtomicU64::new(0)).collect(),
        })
    }

    fn record_write(&self, us: u64, aircraft: u64) {
        self.write_hist.lock().unwrap().record(us.max(1)).ok();
        self.write_ops.fetch_add(1, Relaxed);
        self.write_aircraft.fetch_add(aircraft, Relaxed);
    }

    fn record_write_shard(&self, shard_idx: usize, count: u64) {
        if let Some(counter) = self.shard_writes.get(shard_idx) {
            counter.fetch_add(count, Relaxed);
        }
    }

    fn record_read(&self, us: u64, count: usize) {
        self.read_hist.lock().unwrap().record(us.max(1)).ok();
        self.read_ops.fetch_add(1, Relaxed);
        if count == 0 {
            self.read_misses.fetch_add(1, Relaxed);
        }
    }
}

// ── Entry point ───────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let args = Args::parse();

    // Parse optional multi-shard configuration
    let shards: Vec<ShardSpec> = args
        .shards
        .as_deref()
        .map(|s| {
            s.split(',')
                .filter(|p| !p.is_empty())
                .map(|spec| ShardSpec::parse(spec).expect("invalid shard spec"))
                .collect()
        })
        .unwrap_or_default();

    let is_sharded = !shards.is_empty();

    print_header(&args, &shards);

    let metrics = Metrics::new(shards.len());
    let stop_flag = Arc::new(AtomicBool::new(false));
    let mut handles = Vec::new();

    if is_sharded {
        // ── Distributed mode: writers route to correct shard by S2 token ──
        let shard_conns: Vec<ConnectionManager> = {
            let mut v = Vec::new();
            for s in &shards {
                let client = redis::Client::open(s.redis_url.as_str())?;
                v.push(
                    ConnectionManager::new(client)
                        .await
                        .map_err(|e| anyhow::anyhow!("Shard Redis {}: {e}", s.redis_url))?,
                );
            }
            v
        };
        let shard_conns = Arc::new(shard_conns);

        for worker_id in 0..args.writers {
            let conns = Arc::clone(&shard_conns);
            let shards_c = shards.clone();
            let m = Arc::clone(&metrics);
            let stop = Arc::clone(&stop_flag);
            let batch_sz = args.batch_size;
            let chunk_sz = args.chunk_size;
            let s2_level = args.s2_level;
            handles.push(tokio::spawn(async move {
                sharded_writer_task(
                    worker_id, conns, shards_c, m, stop, batch_sz, chunk_sz, s2_level,
                )
                .await;
            }));
        }

        // Readers query a random shard
        for _ in 0..args.readers {
            let conns = Arc::clone(&shard_conns);
            let m = Arc::clone(&metrics);
            let stop = Arc::clone(&stop_flag);
            let s2_level = args.s2_level;
            handles.push(tokio::spawn(async move {
                sharded_reader_task(conns, m, stop, s2_level).await;
            }));
        }
    } else {
        // ── Single-node mode (original behaviour) ─────────────────────────
        let client = redis::Client::open(args.redis_url.as_str())?;
        let conn_mgr = ConnectionManager::new(client).await.map_err(|e| {
            anyhow::anyhow!("Redis: {e}\nStart Redis: docker run -d -p 6379:6379 redis:7-alpine")
        })?;

        for worker_id in 0..args.writers {
            let conn = conn_mgr.clone();
            let m = Arc::clone(&metrics);
            let stop = Arc::clone(&stop_flag);
            let batch_sz = args.batch_size;
            let chunk_sz = args.chunk_size;
            let s2_level = args.s2_level;
            handles.push(tokio::spawn(async move {
                writer_task(worker_id, conn, m, stop, batch_sz, chunk_sz, s2_level).await;
            }));
        }
        for _ in 0..args.readers {
            let conn = conn_mgr.clone();
            let m = Arc::clone(&metrics);
            let stop = Arc::clone(&stop_flag);
            let s2_level = args.s2_level;
            handles.push(tokio::spawn(async move {
                reader_task(conn, m, stop, s2_level).await;
            }));
        }
    }

    // Progress ticker
    {
        let m = Arc::clone(&metrics);
        let stop = Arc::clone(&stop_flag);
        let t0 = Instant::now();
        let sh = shards.clone();
        handles.push(tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(5));
            ticker.tick().await;
            loop {
                ticker.tick().await;
                if stop.load(Relaxed) {
                    break;
                }
                let secs = t0.elapsed().as_secs().max(1);
                let wo = m.write_ops.load(Relaxed);
                let ro = m.read_ops.load(Relaxed);
                let wa = m.write_aircraft.load(Relaxed);
                print!(
                    "[{:3}s] writes {:5} ({:.1}/s, {:>9} aircraft/s)  reads {:6} ({:.1}/s)",
                    secs,
                    wo,
                    wo as f64 / secs as f64,
                    fmt_num(wa / secs),
                    ro,
                    ro as f64 / secs as f64
                );
                if !sh.is_empty() {
                    print!("  shards [");
                    for (i, s) in sh.iter().enumerate() {
                        let sw = m.shard_writes.get(i).map(|c| c.load(Relaxed)).unwrap_or(0);
                        let range = format!(
                            "{}-{}",
                            s.prefix_start.chars().next().unwrap_or('∅'),
                            s.prefix_end.chars().next().unwrap_or('∞')
                        );
                        print!(" {range}:{sw}");
                    }
                    print!(" ]");
                }
                println!();
            }
        }));
    }

    tokio::time::sleep(Duration::from_secs(args.duration_secs)).await;
    stop_flag.store(true, Relaxed);
    tokio::time::sleep(Duration::from_millis(300)).await;
    for h in handles {
        h.abort();
    }

    print_results(&args, &metrics, &shards);
    Ok(())
}

// ── Sharded writer task ───────────────────────────────────────────────────
// Routes each aircraft to the correct Redis shard based on its S2 token.
// This proves geographic routing: North American aircraft → shard-0,
// European aircraft → shard-1, etc.

#[allow(clippy::too_many_arguments)]
async fn sharded_writer_task(
    id: usize,
    conns: Arc<Vec<ConnectionManager>>,
    shards: Vec<ShardSpec>,
    metrics: Arc<Metrics>,
    stop: Arc<AtomicBool>,
    batch_sz: usize,
    chunk_sz: usize,
    s2_level: u8,
) {
    let mut rng = StdRng::from_entropy();
    let prefix = "geo-redis";
    const TTL: u64 = 120;
    const EXPIRE: i64 = 120;

    while !stop.load(Relaxed) {
        // Generate batch and group by shard
        let mut by_shard: Vec<Vec<GeoEntry>> = vec![vec![]; shards.len()];
        let mut trie = GeoTrie::new(s2_level);

        for i in 0..batch_sz {
            let lat = rng.gen_range(-85.0_f64..85.0);
            let lon = rng.gen_range(-180.0_f64..180.0);
            let entry = GeoEntry {
                id: format!("sw{id}-{i}"),
                lat,
                lon,
                payload: json!({ "callsign": format!("SW{id}{i:04}"),
                                 "altitude": rng.gen_range(0.0_f64..12_000.0) }),
                written_at: 0,
            };
            let token = trie.cell_token(lat, lon);
            // Find the shard that owns this token
            let shard_idx = shards.iter().position(|s| s.owns(&token)).unwrap_or(0);
            by_shard[shard_idx].push(entry.clone());
            trie.insert(entry);
        }

        let start = Instant::now();
        let mut total_ok = 0u64;

        for (shard_idx, entries) in by_shard.iter().enumerate() {
            if entries.is_empty() {
                continue;
            }
            let mut conn = conns[shard_idx].clone();
            let mut ok = true;

            for chunk in entries.chunks(chunk_sz) {
                let mut pipe = redis::pipe();
                pipe.atomic();
                for entry in chunk {
                    let token = trie.cell_token(entry.lat, entry.lon);
                    let ak = format!("{prefix}:entity:{}", entry.id);
                    let ck = format!("{prefix}:cell:{token}");
                    let js = serde_json::to_string(entry).unwrap_or_default();
                    pipe.set_ex(&ak, &js, TTL).ignore();
                    pipe.sadd(&ck, &entry.id).ignore();
                    pipe.expire(&ck, EXPIRE).ignore();
                }
                if pipe.query_async::<()>(&mut conn).await.is_err() {
                    ok = false;
                    break;
                }
            }

            if ok {
                total_ok += entries.len() as u64;
                metrics.record_write_shard(shard_idx, entries.len() as u64);
            }
        }

        if total_ok > 0 {
            metrics.record_write(start.elapsed().as_micros() as u64, total_ok);
        }
    }
}

// ── Sharded reader task ───────────────────────────────────────────────────
// Queries a random shard — a real viewport query would fan-out to 1–3 shards.

async fn sharded_reader_task(
    conns: Arc<Vec<ConnectionManager>>,
    metrics: Arc<Metrics>,
    stop: Arc<AtomicBool>,
    s2_level: u8,
) {
    let mut rng = StdRng::from_entropy();
    let prefix = "geo-redis";

    while !stop.load(Relaxed) {
        let tokens = random_viewport_tokens(&mut rng, s2_level);
        if tokens.is_empty() {
            continue;
        }

        // Route to a random shard connection (demonstrates fan-out)
        let shard_idx = rng.gen_range(0..conns.len());
        let start = Instant::now();
        let mut conn = conns[shard_idx].clone();

        let cell_keys: Vec<String> = tokens
            .iter()
            .map(|t| format!("{prefix}:cell:{t}"))
            .collect();
        let ids: Vec<String> = match conn.sunion(cell_keys).await {
            Ok(v) => v,
            Err(_) => continue,
        };
        if !ids.is_empty() {
            let mut pipe = redis::pipe();
            for id in &ids {
                pipe.get(format!("{prefix}:aircraft:{id}"));
            }
            let _: Vec<Option<String>> = match pipe.query_async(&mut conn).await {
                Ok(v) => v,
                Err(_) => continue,
            };
        }
        metrics.record_read(start.elapsed().as_micros() as u64, ids.len());
    }
}

// ── Writer task ───────────────────────────────────────────────────────────

async fn writer_task(
    id: usize,
    mut conn: ConnectionManager,
    metrics: Arc<Metrics>,
    stop: Arc<AtomicBool>,
    batch_sz: usize,
    chunk_sz: usize,
    s2_level: u8,
) {
    let mut rng = StdRng::from_entropy();
    let prefix = "geo-redis";
    const TTL: u64 = 120;
    const EXPIRE: i64 = 120;

    while !stop.load(Relaxed) {
        // Build a trie with `batch_sz` random aircraft
        let mut trie = GeoTrie::new(s2_level);
        for i in 0..batch_sz {
            let lat = rng.gen_range(-85.0_f64..85.0);
            let lon = rng.gen_range(-180.0_f64..180.0);
            trie.insert(GeoEntry {
                id: format!("w{id}-{i}"),
                lat,
                lon,
                payload: json!({
                    "callsign": format!("W{id}{i:04}"),
                    "altitude": rng.gen_range(0.0_f64..12_000.0),
                    "velocity": rng.gen_range(0.0_f64..950.0),
                    "heading":  rng.gen_range(0.0_f64..360.0),
                }),
                written_at: 0,
            });
        }

        let entries = trie.all_entries();
        let start = Instant::now();
        let mut ok = true;

        for chunk in entries.chunks(chunk_sz) {
            let mut pipe = redis::pipe();
            pipe.atomic();
            for entry in chunk {
                let token = trie.cell_token(entry.lat, entry.lon);
                let ak = format!("{prefix}:aircraft:{}", entry.id);
                let ck = format!("{prefix}:cell:{token}");
                let json = serde_json::to_string(entry).unwrap_or_default();
                pipe.set_ex(&ak, &json, TTL).ignore();
                pipe.sadd(&ck, &entry.id).ignore();
                pipe.expire(&ck, EXPIRE).ignore();
            }
            if pipe.query_async::<()>(&mut conn).await.is_err() {
                ok = false;
                break;
            }
        }

        if ok {
            metrics.record_write(start.elapsed().as_micros() as u64, batch_sz as u64);
        }
    }
}

// ── Reader task ───────────────────────────────────────────────────────────

async fn reader_task(
    mut conn: ConnectionManager,
    metrics: Arc<Metrics>,
    stop: Arc<AtomicBool>,
    s2_level: u8,
) {
    let mut rng = StdRng::from_entropy();
    let prefix = "geo-redis";

    while !stop.load(Relaxed) {
        let tokens = random_viewport_tokens(&mut rng, s2_level);
        if tokens.is_empty() {
            continue;
        }

        let start = Instant::now();
        let cell_keys: Vec<String> = tokens
            .iter()
            .map(|t| format!("{prefix}:cell:{t}"))
            .collect();

        // SUNION → set of aircraft IDs in the viewport
        let ids: Vec<String> = match conn.sunion(cell_keys).await {
            Ok(v) => v,
            Err(_) => continue,
        };

        let result_count = ids.len();

        // Pipeline GET for each aircraft
        if !ids.is_empty() {
            let mut pipe = redis::pipe();
            for id in &ids {
                pipe.get(format!("{prefix}:aircraft:{id}"));
            }
            let _: Vec<Option<String>> = match pipe.query_async(&mut conn).await {
                Ok(v) => v,
                Err(_) => continue,
            };
        }

        metrics.record_read(start.elapsed().as_micros() as u64, result_count);
    }
}

// ── S2 helpers ────────────────────────────────────────────────────────────

/// Returns S2 cell tokens covering a random ~800 km radius viewport.
fn random_viewport_tokens(rng: &mut impl Rng, s2_level: u8) -> Vec<String> {
    let lat = rng.gen_range(-70.0_f64..70.0);
    let lon = rng.gen_range(-180.0_f64..180.0);
    let radius_rad = (800_000.0_f64 / 6_371_000.0).min(PI); // ~800 km

    let center = Point::from(LatLng::new(s1::Deg(lat).into(), s1::Deg(lon).into()));
    let cap_angle: s1::angle::Angle = s1::Rad(radius_rad).into();
    let cap = Cap::from_center_angle(&center, &cap_angle);
    let coverer = RegionCoverer {
        min_level: s2_level,
        max_level: s2_level,
        level_mod: 1,
        max_cells: 50,
    };

    coverer
        .covering(&cap)
        .0
        .iter()
        .map(|c| {
            let h = format!("{:016x}", c.0);
            h.trim_end_matches('0').to_string()
        })
        .collect()
}

// ── Output helpers ────────────────────────────────────────────────────────

fn print_header(a: &Args, shards: &[ShardSpec]) {
    println!();
    println!("╔══════════════════════════════════════════════════════════╗");
    println!("║             geo-redis  PRODUCTION  LOAD  TEST             ║");
    println!("╚══════════════════════════════════════════════════════════╝");
    println!();
    if shards.is_empty() {
        println!("  Mode       : single-node");
        println!("  Redis      : {}", a.redis_url);
    } else {
        println!("  Mode       : DISTRIBUTED ({} shards)", shards.len());
        for (i, s) in shards.iter().enumerate() {
            let range = format!("[{}, {})", s.prefix_start, s.prefix_end);
            println!("  Shard {i}    : {range:16} → {}", s.redis_url);
        }
    }
    println!(
        "  Writers    : {} tasks × {} aircraft/batch",
        a.writers, a.batch_size
    );
    println!("  Readers    : {} concurrent viewport queries", a.readers);
    println!("  Duration   : {}s", a.duration_secs);
    println!(
        "  S2 level   : {} (cell ≈ {})",
        a.s2_level,
        s2_level_desc(a.s2_level)
    );
    println!();
    println!("  [Progress every 5s]\n");
}

fn print_results(a: &Args, m: &Metrics, shards: &[ShardSpec]) {
    let dur = a.duration_secs as f64;
    let wo = m.write_ops.load(Relaxed);
    let wa = m.write_aircraft.load(Relaxed);
    let ro = m.read_ops.load(Relaxed);
    let miss = m.read_misses.load(Relaxed);
    let wh = m.write_hist.lock().unwrap();
    let rh = m.read_hist.lock().unwrap();

    println!();
    println!("╔══════════════════════════════════════════════════════════╗");
    println!(
        "║  RESULTS  ({} writers / {} readers / {}s)",
        a.writers, a.readers, a.duration_secs
    );
    println!("╠══════════════════════════════════════════════════════════╣");
    println!("║  WRITES");
    println!("║    batches    : {}  ({:.1} batch/s)", wo, wo as f64 / dur);
    println!(
        "║    aircraft   : {}  ({} aircraft/s)",
        fmt_num(wa),
        fmt_num((wa as f64 / dur) as u64)
    );
    if !wh.is_empty() {
        println!(
            "║    p50 {:.1}ms  p95 {:.1}ms  p99 {:.1}ms  max {:.1}ms",
            pct(&wh, 0.50),
            pct(&wh, 0.95),
            pct(&wh, 0.99),
            wh.max() as f64 / 1000.0
        );
    } else {
        println!("║    (no samples)");
    }

    if !shards.is_empty() {
        println!("║");
        println!("║  GEOGRAPHIC ROUTING (proves distributed tree)");
        let total = m
            .shard_writes
            .iter()
            .map(|c| c.load(Relaxed))
            .sum::<u64>()
            .max(1);
        for (i, s) in shards.iter().enumerate() {
            let sw = m.shard_writes.get(i).map(|c| c.load(Relaxed)).unwrap_or(0);
            let pct = sw as f64 / total as f64 * 100.0;
            let bar = "█".repeat((pct / 2.5) as usize);
            let rng = format!("[{}, {})", s.prefix_start, s.prefix_end);
            println!(
                "║    shard-{i} {rng:12}: {:>8}  ({pct:5.1}%)  {bar}",
                fmt_num(sw)
            );
        }
    }

    println!("║");
    println!("║  READS");
    println!("║    queries    : {}  ({:.1} query/s)", ro, ro as f64 / dur);
    println!(
        "║    cache miss : {} ({:.1}%)",
        miss,
        if ro > 0 {
            miss as f64 / ro as f64 * 100.0
        } else {
            0.0
        }
    );
    if !rh.is_empty() {
        println!(
            "║    p50 {:.2}ms  p95 {:.2}ms  p99 {:.2}ms  max {:.2}ms",
            pct(&rh, 0.50),
            pct(&rh, 0.95),
            pct(&rh, 0.99),
            rh.max() as f64 / 1000.0
        );
    }
    println!("╚══════════════════════════════════════════════════════════╝");
    println!();
    if shards.is_empty() {
        println!("  Distributed mode example:");
        println!("  --shards ':5:redis://localhost:6379,5:a:redis://localhost:6380,a::redis://localhost:6381'");
    }
    println!();
}

fn pct(h: &Histogram<u64>, q: f64) -> f64 {
    h.value_at_quantile(q) as f64 / 1000.0
}

fn fmt_num(n: u64) -> String {
    // Insert thousands separators
    let s = n.to_string();
    let chars: Vec<char> = s.chars().rev().collect();
    let grouped: String = chars
        .chunks(3)
        .map(|c| c.iter().collect::<String>())
        .collect::<Vec<_>>()
        .join(",");
    grouped.chars().rev().collect()
}

fn s2_level_desc(level: u8) -> &'static str {
    match level {
        1..=5 => "continent scale",
        6..=8 => "~200–800 km",
        9..=10 => "~50–200 km",
        11..=13 => "~5–50 km",
        14..=16 => "~1–5 km",
        _ => "sub-km",
    }
}
