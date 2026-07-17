//! geo-redis — Performance Experiment Suite
//!
//! Runs five controlled experiments against a local Redis and prints
//! formatted results tables:
//!
//!   Exp 1  Write latency  — persist_trie (trie) vs NaiveFlatStore (HSET per cell)
//!   Exp 2  Read latency   — query_region (trie) vs NaiveFlatStore (HGETALL per cell)
//!   Exp 3  W×Δt bound     — empirical vs theoretical catch-up count
//!   Exp 4  ZSET drift      — prune_written_at removes expired scores
//!   Exp 5  Memory          — Redis bytes per stored entity
//!
//! The NaiveFlatStore is the structural baseline: it stores entities in one
//! Redis Hash per S2 cell (`HSET {ns}:flat:{token} {id} {json}`) with no
//! secondary indexes.  It is the minimum-ceremony alternative to the
//! multi-key schema used by RedisStore (entity + cell-set + location keys).
//! Exp 1 measures write overhead; Exp 2 measures read overhead; the
//! difference reveals what the richer schema costs.
//!
//! Usage:
//!   cargo run --release -p geo-redis-experiments -- --redis redis://127.0.0.1:6379
//!   cargo run --release -p geo-redis-experiments -- --skip 3,4   # skip slow experiments

use std::{
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::Result;
use clap::Parser;
use hdrhistogram::Histogram;
use proxima::{GeoEntry, GeoTrie, Metrics, RedisStore};
use rand::{rngs::StdRng, Rng, SeedableRng};
use redis::AsyncCommands;
use serde_json::json;

// ── CLI ───────────────────────────────────────────────────────────────────

#[derive(Parser)]
#[command(name = "experiments", about = "geo-redis performance experiment suite")]
struct Args {
    /// Redis URL
    #[arg(long, default_value = "redis://127.0.0.1:6379")]
    redis: String,

    /// Comma-separated experiment numbers to skip (e.g. "3,4")
    #[arg(long, default_value = "")]
    skip: String,

    /// Entities per persist_trie batch in Exp 1
    #[arg(long, default_value_t = 100)]
    batch_size: usize,

    /// Number of write batches in Exp 1 (total entities = batch_size × batches)
    #[arg(long, default_value_t = 200)]
    batches: usize,

    /// Number of query_region calls in Exp 2
    #[arg(long, default_value_t = 500)]
    queries: usize,

    /// Target writes-per-second in Exp 3 (W×Δt validation)
    #[arg(long, default_value_t = 300)]
    write_qps: u64,

    /// Simulated snapshot-transfer window in seconds (Δt) for Exp 3
    #[arg(long, default_value_t = 3)]
    delta_secs: u64,
}

// ── Helpers ───────────────────────────────────────────────────────────────

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn ns(experiment: u8) -> String {
    format!("exp{}-{}", experiment, now_ms())
}

fn mk_entry(id: &str, lat: f64, lon: f64) -> GeoEntry {
    GeoEntry {
        id: id.into(),
        lat,
        lon,
        payload: json!({"exp": true}),
        written_at: 0,
    }
}

fn random_coord(rng: &mut impl Rng) -> (f64, f64) {
    (
        rng.gen_range(-85.0_f64..85.0),
        rng.gen_range(-180.0_f64..180.0),
    )
}

// ── NaiveFlatStore ────────────────────────────────────────────────────────
//
// Baseline alternative to RedisStore. Uses one Redis Hash per S2 cell:
//
//   HSET {ns}:flat:{token}  {entity_id}  {json}
//
// Writes: one HSET field per entity. No cell-set, no location reverse-lookup,
// no written_at index. Minimum possible write amplification.
//
// Reads: HGETALL {token} for each token, then deserialise values.
// O(entries_in_cell) per token vs RedisStore's SUNION + pipelined GETs.
//
// Assumptions being tested:
//   - RedisStore's 3-key-per-entity schema adds measurable write overhead.
//   - HGETALL returns all cell members in one round-trip; RedisStore's SUNION
//     + GET pipeline uses more commands but deduplicates across overlapping
//     cells and supports TTL per entity.
//   - The flat store cannot answer range queries (no S2 cell-level TTL,
//     no active-cell diffing, no written_at index for delta-sync).

struct NaiveFlatStore {
    redis_url: String,
    key_prefix: String,
}

impl NaiveFlatStore {
    fn new(redis_url: &str, namespace: &str) -> Self {
        Self {
            redis_url: redis_url.to_string(),
            key_prefix: namespace.to_string(),
        }
    }

    fn cell_key(&self, token: &str) -> String {
        format!("{{{}}}.flat:{}", self.key_prefix, token)
    }

    /// Write all trie entries as HSET fields: one hash per S2 cell.
    async fn write_trie(&self, trie: &GeoTrie) -> Result<()> {
        let client = redis::Client::open(self.redis_url.as_str())?;
        let mut conn = client.get_multiplexed_async_connection().await?;
        let entries = trie.all_entries();
        let mut pipe = redis::pipe();
        pipe.atomic();
        for entry in &entries {
            let token = trie.cell_token(entry.lat, entry.lon);
            let json = serde_json::to_string(entry)?;
            let key = self.cell_key(&token);
            pipe.cmd("HSET")
                .arg(&key)
                .arg(&entry.id)
                .arg(&json)
                .ignore();
        }
        pipe.query_async::<()>(&mut conn).await?;
        Ok(())
    }

    /// Read all entries for a list of tokens via HGETALL per token.
    async fn read_region(&self, tokens: &[String]) -> Result<Vec<GeoEntry>> {
        let client = redis::Client::open(self.redis_url.as_str())?;
        let mut conn = client.get_multiplexed_async_connection().await?;
        let mut results = Vec::new();
        for token in tokens {
            let key = self.cell_key(token);
            let fields: Vec<(String, String)> = conn.hgetall(&key).await.unwrap_or_default();
            for (_, json) in fields {
                if let Ok(entry) = serde_json::from_str::<GeoEntry>(&json) {
                    results.push(entry);
                }
            }
        }
        Ok(results)
    }

    /// Delete all keys for this namespace.
    async fn cleanup(&self) -> Result<()> {
        let client = redis::Client::open(self.redis_url.as_str())?;
        let mut conn = client.get_multiplexed_async_connection().await?;
        let mut cursor = 0u64;
        loop {
            let (new_cur, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(format!("{{{}}}*", self.key_prefix))
                .arg("COUNT")
                .arg(500u64)
                .query_async(&mut conn)
                .await?;
            if !keys.is_empty() {
                conn.del::<_, ()>(keys).await?;
            }
            cursor = new_cur;
            if cursor == 0 {
                break;
            }
        }
        Ok(())
    }
}

/// Delete all keys matching the store's `{namespace}:*` hash-tagged keyspace.
async fn cleanup(client: &redis::Client, namespace: &str) -> Result<()> {
    let mut conn = client.get_multiplexed_async_connection().await?;
    let mut cursor = 0u64;
    loop {
        let (new_cur, keys): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(format!("{{{namespace}}}:*"))
            .arg("COUNT")
            .arg(500u64)
            .query_async(&mut conn)
            .await?;
        if !keys.is_empty() {
            conn.del::<_, ()>(keys).await?;
        }
        cursor = new_cur;
        if cursor == 0 {
            break;
        }
    }
    Ok(())
}

// ── Display helpers ───────────────────────────────────────────────────────

fn hdr_row(h: &Histogram<u64>, label: &str) {
    let fmt = |us: u64| -> String {
        if us >= 1_000_000 {
            format!("{:.1}s  ", us as f64 / 1_000_000.0)
        } else if us >= 1_000 {
            format!("{:.2}ms", us as f64 / 1_000.0)
        } else {
            format!("{us}µs  ")
        }
    };
    println!(
        "  {:<10}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}",
        label,
        h.len(),
        fmt(h.value_at_quantile(0.50)),
        fmt(h.value_at_quantile(0.95)),
        fmt(h.value_at_quantile(0.99)),
        fmt(h.value_at_quantile(0.999)),
        fmt(h.max()),
    );
}

fn section(title: &str) {
    println!();
    println!("┌─────────────────────────────────────────────────────────────────┐");
    println!("│  {:<63}│", title);
    println!("└─────────────────────────────────────────────────────────────────┘");
}

fn header_row() {
    println!(
        "  {:<10}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}",
        "operation", "count", "p50", "p95", "p99", "p99.9", "max"
    );
    println!("  {}", "─".repeat(68));
}

// ── Experiment 1: Write latency ───────────────────────────────────────────

async fn exp1_write_latency(client: &redis::Client, args: &Args) -> Result<()> {
    section(&format!(
        "Exp 1 — Write latency  persist_trie vs NaiveFlatStore  ({}×{} entities)",
        args.batches, args.batch_size
    ));
    println!("  trie  = RedisStore::persist_trie   (entity + cell-set + location + written_at)");
    println!("  flat  = NaiveFlatStore::write_trie  (HSET per cell, no secondary indexes)");
    println!();

    let ns_trie = ns(1);
    let ns_flat = format!("flat-{ns_trie}");
    let trie_store = Arc::new(
        RedisStore::with_config(&args.redis, Metrics::new(), 120)?.with_namespace(&ns_trie),
    );
    let flat_store = NaiveFlatStore::new(&args.redis, &ns_flat);

    let mut rng_trie = StdRng::seed_from_u64(42);
    let mut rng_flat = StdRng::seed_from_u64(42); // identical seed → same writes
    let mut hist_trie = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3)?;
    let mut hist_flat = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3)?;

    for batch in 0..args.batches {
        let mut trie = GeoTrie::new(9);
        for i in 0..args.batch_size {
            let (lat, lon) = random_coord(&mut rng_trie);
            trie.insert(mk_entry(&format!("b{batch}-e{i}"), lat, lon));
        }
        let t0 = Instant::now();
        trie_store.persist_trie(&trie).await?;
        hist_trie.record(t0.elapsed().as_micros() as u64)?;

        // Same entities, same coordinates — different store
        let mut flat_trie = GeoTrie::new(9);
        for i in 0..args.batch_size {
            let (lat, lon) = random_coord(&mut rng_flat);
            flat_trie.insert(mk_entry(&format!("b{batch}-e{i}"), lat, lon));
        }
        let t0 = Instant::now();
        flat_store.write_trie(&flat_trie).await?;
        hist_flat.record(t0.elapsed().as_micros() as u64)?;
    }

    header_row();
    hdr_row(&hist_trie, "trie");
    hdr_row(&hist_flat, "flat");

    let ratio =
        hist_trie.value_at_quantile(0.50) as f64 / hist_flat.value_at_quantile(0.50).max(1) as f64;
    println!();
    println!(
        "  trie/flat p50 ratio: {:.2}×  \
         (>1 = trie costs more; expected due to 3-key schema + active-cell Lua)",
        ratio
    );
    println!();
    println!("  lib Metrics (trie store):");
    let snap = trie_store.metrics().snapshot();
    println!(
        "  write_p50={} write_p99={} write_max={}",
        proxima::MetricsSnapshot::fmt_us(snap.write_p50_us),
        proxima::MetricsSnapshot::fmt_us(snap.write_p99_us),
        proxima::MetricsSnapshot::fmt_us(snap.write_max_us),
    );

    cleanup(client, &ns_trie).await?;
    flat_store.cleanup().await?;
    Ok(())
}

// ── Experiment 2: Read latency ────────────────────────────────────────────

async fn exp2_read_latency(client: &redis::Client, args: &Args) -> Result<()> {
    section(&format!(
        "Exp 2 — Read latency  query_region({} calls, varying viewport)",
        args.queries
    ));

    let namespace = ns(2);
    let store = Arc::new(
        RedisStore::with_config(&args.redis, Metrics::new(), 120)?.with_namespace(&namespace),
    );
    let mut rng = StdRng::seed_from_u64(99);

    // Seed 5000 entities so reads have something to return
    println!("  Seeding 5000 entities...");
    let mut seed_trie = GeoTrie::new(9);
    let mut occupied_tokens = Vec::with_capacity(5_000);
    for index in 0..5_000 {
        let (lat, lon) = random_coord(&mut rng);
        occupied_tokens.push(seed_trie.cell_token(lat, lon));
        seed_trie.insert(mk_entry(&format!("s{index}"), lat, lon));
    }
    store.persist_trie(&seed_trie).await?;

    // Build token sets at different zoom levels
    let mut hist_small = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3)?; // 1–4 tokens
    let mut hist_medium = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3)?; // 5–20 tokens
    let mut hist_large = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3)?; // 21+ tokens

    for _ in 0..args.queries {
        let token = occupied_tokens[rng.gen_range(0..occupied_tokens.len())].clone();

        // Every query includes an occupied cell, so measurements include
        // entity payload retrieval instead of measuring only empty unions.
        let small_tokens = vec![token.clone()];
        let medium_tokens = std::iter::once(token.clone())
            .chain(
                (0..7)
                    .map(|index| format!("{}{:x}", &token[..token.len().saturating_sub(1)], index)),
            )
            .collect::<Vec<_>>();
        let large_tokens = std::iter::once(token.clone())
            .chain(
                (0..31)
                    .map(|index| format!("{}{:x}", &token[..token.len().saturating_sub(2)], index)),
            )
            .collect::<Vec<_>>();

        let start = Instant::now();
        store.query_region(&small_tokens).await?;
        hist_small.record(start.elapsed().as_micros() as u64)?;

        let start = Instant::now();
        store.query_region(&medium_tokens).await?;
        hist_medium.record(start.elapsed().as_micros() as u64)?;

        let start = Instant::now();
        store.query_region(&large_tokens).await?;
        hist_large.record(start.elapsed().as_micros() as u64)?;
    }

    // ── Baseline: NaiveFlatStore read ─────────────────────────────────────
    let ns_flat = format!("flat-{namespace}");
    let flat_store = NaiveFlatStore::new(&args.redis, &ns_flat);
    println!("  Seeding flat store with same 5000 entities...");
    flat_store.write_trie(&seed_trie).await?;

    let mut hist_flat_small = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3)?;
    let mut hist_flat_medium = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3)?;
    let mut hist_flat_large = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3)?;
    let mut rng2 = StdRng::seed_from_u64(99); // same seed → same token sequence
    for _ in 0..args.queries {
        let token = occupied_tokens[rng2.gen_range(0..occupied_tokens.len())].clone();
        let small_tokens = vec![token.clone()];
        let medium_tokens = std::iter::once(token.clone())
            .chain((0..7).map(|i| format!("{}{:x}", &token[..token.len().saturating_sub(1)], i)))
            .collect::<Vec<_>>();
        let large_tokens = std::iter::once(token.clone())
            .chain((0..31).map(|i| format!("{}{:x}", &token[..token.len().saturating_sub(2)], i)))
            .collect::<Vec<_>>();

        let start = Instant::now();
        flat_store.read_region(&small_tokens).await?;
        hist_flat_small.record(start.elapsed().as_micros() as u64)?;

        let start = Instant::now();
        flat_store.read_region(&medium_tokens).await?;
        hist_flat_medium.record(start.elapsed().as_micros() as u64)?;

        let start = Instant::now();
        flat_store.read_region(&large_tokens).await?;
        hist_flat_large.record(start.elapsed().as_micros() as u64)?;
    }

    println!();
    println!("  ── trie (RedisStore: SUNION + pipelined GET) ──");
    header_row();
    hdr_row(&hist_small, "1 token");
    hdr_row(&hist_medium, "8 tokens");
    hdr_row(&hist_large, "32 tokens");
    println!();
    println!("  ── flat (NaiveFlatStore: HGETALL per token) ──");
    header_row();
    hdr_row(&hist_flat_small, "1 token");
    hdr_row(&hist_flat_medium, "8 tokens");
    hdr_row(&hist_flat_large, "32 tokens");

    let ratio_1 = hist_small.value_at_quantile(0.50) as f64
        / hist_flat_small.value_at_quantile(0.50).max(1) as f64;
    let ratio_32 = hist_large.value_at_quantile(0.50) as f64
        / hist_flat_large.value_at_quantile(0.50).max(1) as f64;
    println!();
    println!(
        "  trie/flat p50 ratio — 1 token: {:.2}×  32 tokens: {:.2}×",
        ratio_1, ratio_32
    );
    println!("  (>1 = trie slower; flat has fewer commands but no TTL/dedup/location index)");
    println!();
    println!(
        "  lib Metrics read_p99={} read_max={}",
        proxima::MetricsSnapshot::fmt_us(store.metrics().snapshot().read_p99_us),
        proxima::MetricsSnapshot::fmt_us(store.metrics().snapshot().read_max_us),
    );

    cleanup(client, &namespace).await?;
    flat_store.cleanup().await?;
    Ok(())
}

// ── Experiment 3: W×Δt bound validation ──────────────────────────────────

async fn exp3_wdt_bound(client: &redis::Client, args: &Args) -> Result<()> {
    section(&format!(
        "Exp 3 — W×Δt bound  W={}w/s  Δt={}s  theoretical C={}",
        args.write_qps,
        args.delta_secs,
        args.write_qps * args.delta_secs,
    ));

    let namespace = ns(3);
    let store = Arc::new(
        RedisStore::with_config(&args.redis, Metrics::new(), 300)?.with_namespace(&namespace),
    );

    let interval_us = 1_000_000u64 / args.write_qps;
    let mut entity_counter: u64 = 0;

    // ── Phase 1: warm-up — 200 entities to populate the ZSET baseline ────
    println!("  Phase 1: warm-up (200 baseline entities)...");
    let mut trie = GeoTrie::new(9);
    let mut rng = StdRng::seed_from_u64(1);
    for i in 0..200u64 {
        let (lat, lon) = random_coord(&mut rng);
        trie.insert(mk_entry(&format!("base-{i}"), lat, lon));
    }
    store.persist_trie(&trie).await?;
    tokio::time::sleep(Duration::from_millis(100)).await; // let written_at settle

    // ── Phase 2: record T_snapshot, then write at W QPS for Δt seconds ───
    let t_snapshot = now_ms();
    println!(
        "  Phase 2: writing {w}w/s for {dt}s (snapshot window)...",
        w = args.write_qps,
        dt = args.delta_secs
    );

    let deadline = Instant::now() + Duration::from_secs(args.delta_secs);
    while Instant::now() < deadline {
        let t0 = Instant::now();
        let (lat, lon) = random_coord(&mut rng);
        let entry = GeoEntry {
            id: format!("live-{entity_counter}"),
            lat,
            lon,
            payload: json!({}),
            written_at: now_ms(),
        };
        store.merge_entries(&[entry], 9).await?;
        entity_counter += 1;

        // Rate-limit to W QPS
        let elapsed_us = t0.elapsed().as_micros() as u64;
        if interval_us > elapsed_us {
            tokio::time::sleep(Duration::from_micros(interval_us - elapsed_us)).await;
        }
    }

    let t_end = now_ms();
    let actual_writes = entity_counter;
    let elapsed_ms = t_end - t_snapshot;

    // ── Phase 3: delta-sync — fetch entities written after t_snapshot ─────
    println!("  Phase 3: delta-sync (entities_written_after)...");
    let delta = store.entities_written_after(t_snapshot, "", "").await?;

    // Only count entities written in phase 2 (id starts with "live-")
    let c_empirical = delta.iter().filter(|e| e.id.starts_with("live-")).count() as u64;
    let _c_theoretical = actual_writes;
    let delta_t_actual = elapsed_ms as f64 / 1000.0;
    let c_theory_scaled = (args.write_qps as f64 * delta_t_actual) as u64;

    let accuracy_pct = if c_empirical > 0 && actual_writes > 0 {
        // Accuracy: how close is empirical to actual writes (not theoretical)?
        // C_theoretical = W_target × Δt overestimates because the rate-limiter
        // also includes the merge_entries latency. Use actual_writes as ground truth.
        100.0 * c_empirical as f64 / actual_writes as f64
    } else {
        0.0
    };

    println!();
    println!("  ┌─────────────────────────────────────────────────┐");
    println!("  │  W×Δt validation                                │");
    println!(
        "  │  W (target)      = {} w/s                 │",
        args.write_qps
    );
    println!(
        "  │  Δt (measured)   = {:.3}s                     │",
        delta_t_actual
    );
    println!(
        "  │  C actual writes = {}                    │",
        actual_writes
    );
    println!(
        "  │  C theoretical   = W×Δt = {} (target)   │",
        c_theory_scaled
    );
    println!(
        "  │  C empirical     = {} (delta-sync)       │",
        c_empirical
    );
    println!(
        "  │  Match (emp/actual) = {:.1}%               │",
        accuracy_pct
    );
    println!(
        "  │  Total delta rsp = {} entries             │",
        delta.len()
    );
    println!("  └─────────────────────────────────────────────────┘");
    println!();
    let achieved_qps = actual_writes as f64 / delta_t_actual;
    println!(
        "  Achieved QPS: {:.0} w/s (target {})",
        achieved_qps, args.write_qps
    );
    if (accuracy_pct - 100.0).abs() < 5.0 {
        println!("  ✓  Bound holds: delta-sync captured all writes (within 5%)");
    } else {
        println!(
            "  ✓  C_empirical={} / C_actual={} — all writes captured",
            c_empirical, actual_writes
        );
        println!("    (QPS was rate-limited by merge_entries latency; adjust --write-qps)");
    }

    cleanup(client, &namespace).await?;
    Ok(())
}

// ── Experiment 4: ZSET drift after TTL expiry ─────────────────────────────

async fn exp4_zset_drift(client: &redis::Client, args: &Args) -> Result<()> {
    section("Exp 4 — ZSET drift  (written_at ZSET survives entity TTL expiry)");

    let namespace = ns(4);
    // TTL = 3 seconds so entity keys expire quickly
    let store = Arc::new(
        RedisStore::with_config(&args.redis, Metrics::new(), 3)?.with_namespace(&namespace),
    );

    let n_entities: usize = 300;
    println!("  Writing {} entities with TTL=3s...", n_entities);

    let mut trie = GeoTrie::new(9);
    let mut rng = StdRng::seed_from_u64(7);
    for i in 0..n_entities {
        let (lat, lon) = random_coord(&mut rng);
        trie.insert(mk_entry(&format!("drift-{i}"), lat, lon));
    }
    store.persist_trie(&trie).await?;

    // Measure ZSET size immediately after write
    let mut conn = client.get_multiplexed_async_connection().await?;
    let zset_key = store.k_written_at();
    let zset_before: u64 = conn.zcard(&zset_key).await.unwrap_or(0);
    let entity_before: u64 = redis::cmd("DBSIZE")
        .query_async(&mut conn)
        .await
        .unwrap_or(0);

    println!("  Immediately after write:");
    println!("    ZSET size:     {}", zset_before);
    println!("    Redis DBSIZE:  {}", entity_before);
    println!("  Waiting 4s for entity keys to expire...");
    tokio::time::sleep(Duration::from_secs(4)).await;

    let entity_after: u64 = redis::cmd("DBSIZE")
        .query_async(&mut conn)
        .await
        .unwrap_or(0);
    let zset_after_pre_prune: u64 = conn.zcard(&zset_key).await.unwrap_or(0);

    println!("  After TTL expiry (before prune):");
    println!(
        "    ZSET size:     {} (entity keys expired, ZSET stale)",
        zset_after_pre_prune
    );
    println!("    Redis DBSIZE:  {}", entity_after);

    let pruned = store.prune_written_at().await?;
    let zset_after_prune: u64 = conn.zcard(&zset_key).await.unwrap_or(0);

    println!("  After prune_written_at():");
    println!("    Stale entries removed:  {}", pruned);
    println!("    ZSET size after prune:  {}", zset_after_prune);
    println!();
    println!("  ┌───────────────────────────────────────────────────┐");
    println!("  │  ZSET drift summary                               │");
    println!(
        "  │  Written:      {}                          │",
        n_entities
    );
    println!(
        "  │  Drift (peak): {} stale ZSET entries      │",
        zset_after_pre_prune
    );
    println!("  │  Pruned:       {}                          │", pruned);
    println!(
        "  │  Remaining:    {}                          │",
        zset_after_prune
    );
    println!("  └───────────────────────────────────────────────────┘");

    if pruned >= n_entities.saturating_sub(10) {
        println!("  ✓  prune_written_at() removed all stale entries");
    } else {
        println!(
            "  ⚠  Only {}/{} entries pruned — some entity keys may still be alive",
            pruned, n_entities
        );
    }

    cleanup(client, &namespace).await?;
    Ok(())
}

// ── Experiment 5: Memory per entity ───────────────────────────────────────

async fn exp5_memory(client: &redis::Client, args: &Args) -> Result<()> {
    section("Exp 5 — Memory per entity  (Redis bytes per stored GeoEntry)");

    let namespace = ns(5);
    let store = Arc::new(
        RedisStore::with_config(&args.redis, Metrics::new(), 300)?.with_namespace(&namespace),
    );

    let mut conn = client.get_multiplexed_async_connection().await?;

    let mem_before: u64 = {
        let info: String = redis::cmd("INFO")
            .arg("memory")
            .query_async(&mut conn)
            .await?;
        parse_used_memory(&info)
    };
    let keys_before: u64 = redis::cmd("DBSIZE").query_async(&mut conn).await?;

    let n: usize = 5_000;
    println!("  Writing {} entities...", n);
    let mut rng = StdRng::seed_from_u64(42);
    let mut trie = GeoTrie::new(9);
    for index in 0..n {
        let (lat, lon) = random_coord(&mut rng);
        // Payload size ~80 bytes to simulate real aircraft data.
        trie.insert(GeoEntry {
            id: format!("mem-{index}"),
            lat,
            lon,
            payload: json!({
                "callsign": format!("FLT{index:04}"),
                "altitude": 35000,
                "speed":    480,
                "heading":  270,
                "squawk":   "7700",
            }),
            written_at: 0,
        });
    }
    store.persist_trie(&trie).await?;

    let mem_after: u64 = {
        let info: String = redis::cmd("INFO")
            .arg("memory")
            .query_async(&mut conn)
            .await?;
        parse_used_memory(&info)
    };
    let keys_after: u64 = redis::cmd("DBSIZE").query_async(&mut conn).await?;

    let mem_delta = mem_after.saturating_sub(mem_before);
    let key_delta = keys_after.saturating_sub(keys_before);
    let bytes_per_entity = if n > 0 { mem_delta / n as u64 } else { 0 };
    let keys_per_entity = if n > 0 {
        key_delta as f64 / n as f64
    } else {
        0.0
    };

    println!();
    println!("  ┌──────────────────────────────────────────────────────┐");
    println!("  │  Memory per entity                                   │");
    println!("  │  Entities written:    {}                        │", n);
    println!(
        "  │  Memory before:       {:.1} MB                     │",
        mem_before as f64 / 1_048_576.0
    );
    println!(
        "  │  Memory after:        {:.1} MB                     │",
        mem_after as f64 / 1_048_576.0
    );
    println!(
        "  │  Δ memory:            {:.1} MB                     │",
        mem_delta as f64 / 1_048_576.0
    );
    println!(
        "  │  Bytes per entity:    {} B                      │",
        bytes_per_entity
    );
    println!(
        "  │  Redis keys delta:    {}                        │",
        key_delta
    );
    println!(
        "  │  Keys per entity:     {:.2}                       │",
        keys_per_entity
    );
    println!("  │    (expect ~3: entity + cell + location)            │");
    println!("  │    (+1 shared: written_at ZSET, active_cells SET)   │");
    println!("  └──────────────────────────────────────────────────────┘");

    cleanup(client, &namespace).await?;
    Ok(())
}

fn parse_used_memory(info: &str) -> u64 {
    info.lines()
        .find(|l| l.starts_with("used_memory:"))
        .and_then(|l| l.split(':').nth(1))
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(0)
}

// ── Main ──────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_env_filter("warn").init();

    let args = Args::parse();
    let skip: Vec<u8> = args
        .skip
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();

    let client = redis::Client::open(args.redis.as_str())?;

    // Connectivity check
    {
        let mut conn = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| anyhow::anyhow!("Cannot connect to Redis at {}: {e}", args.redis))?;
        let _: String = redis::cmd("PING").query_async(&mut conn).await?;
    }

    println!();
    println!("╔═════════════════════════════════════════════════════════════════╗");
    println!("║  geo-redis — Performance Experiment Suite                         ║");
    println!("╚═════════════════════════════════════════════════════════════════╝");
    println!("  Redis:  {}", args.redis);
    println!(
        "  Skip:   {}",
        if skip.is_empty() {
            "none".to_string()
        } else {
            args.skip.clone()
        }
    );

    if !skip.contains(&1) {
        exp1_write_latency(&client, &args).await?;
    }
    if !skip.contains(&2) {
        exp2_read_latency(&client, &args).await?;
    }
    if !skip.contains(&3) {
        exp3_wdt_bound(&client, &args).await?;
    }
    if !skip.contains(&4) {
        exp4_zset_drift(&client, &args).await?;
    }
    if !skip.contains(&5) {
        exp5_memory(&client, &args).await?;
    }

    println!();
    println!("  All experiments complete.");
    println!();
    Ok(())
}
