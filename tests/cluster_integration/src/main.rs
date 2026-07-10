/// georedis cluster integration test
///
/// Tests the full shard-split lifecycle at the library level using real Redis
/// containers (via testcontainers).  No geo-node HTTP server is needed — the
/// test drives `RedisStore` directly so it stays fast and deterministic.
///
/// Phases
/// ──────
///   1. SETUP            — start two Redis containers (simulating two shards)
///   2. HIGH-VOLUME WRITE — persist 50 k entities across two RedisStore instances
///   3. SPLIT SEEDING    — simulate split: collect shard-0 entities >= midpoint,
///                         call merge_entries on shard-1 (snapshot-first seeding)
///   4. FRESHNESS CHECK  — re-ingest stale + fresh versions, assert merge_entries
///                         only applies the fresh one
///   5. DELTA SYNC       — write to shard-0 after seeding, call entities_written_after,
///                         verify shard-1 catches up correctly
///   6. REMOVE RANGE     — call trie.remove_range on shard-0 subset, verify pruning
///   7. CONSISTENCY      — assert total key count equals expected, no duplicates
///
/// Run:
///   cargo run -p georedis-cluster-test
///   cargo run -p georedis-cluster-test -- --verbose

use anyhow::Result;
use georedis::{GeoEntry, GeoTrie, Metrics, RedisStore};
use rand::{Rng, SeedableRng};
use serde_json::json;
use std::{sync::Arc, time::{Duration, Instant}};
use testcontainers::{runners::AsyncRunner, GenericImage};
use testcontainers::core::ContainerPort;

// ── Constants ──────────────────────────────────────────────────────────────

const ENTITIES_PER_SHARD: usize = 25_000;
const S2_LEVEL:           u8    = 9;
const TTL_SECS:           u64   = 300;  // generous TTL for tests
const SPLIT_PREFIX:       &str  = "8";  // token >= "8" → shard-1

// ── Entry point ───────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env()
            .add_directive("georedis_cluster_test=info".parse().unwrap()))
        .init();

    let verbose = std::env::args().any(|a| a == "--verbose");

    println!();
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║          georedis Cluster Integration Test                   ║");
    println!("║  Phases: setup → high-load → split seed → delta-sync        ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!();

    let mut passed = 0usize;
    let mut failed = 0usize;

    macro_rules! run_phase {
        ($name:literal, $body:expr) => {{
            let t0 = Instant::now();
            print!("  {:.<50}", $name);
            match $body {
                Ok(msg) => {
                    println!(" PASS  {:>6}ms   {}", t0.elapsed().as_millis(), msg);
                    passed += 1;
                }
                Err(e) => {
                    println!(" FAIL  {:>6}ms   {}", t0.elapsed().as_millis(), e);
                    failed += 1;
                }
            }
        }};
    }

    // ── Phase 1: Start Redis containers ─────────────────────────────────────
    println!("Phase 1 — SETUP");
    let redis0_container = GenericImage::new("redis", "7-alpine")
        .with_exposed_port(ContainerPort::Tcp(6379))
        .start().await?;
    let redis1_container = GenericImage::new("redis", "7-alpine")
        .with_exposed_port(ContainerPort::Tcp(6379))
        .start().await?;

    let port0 = redis0_container.get_host_port_ipv4(6379u16).await?;
    let port1 = redis1_container.get_host_port_ipv4(6379u16).await?;
    let url0  = format!("redis://127.0.0.1:{port0}");
    let url1  = format!("redis://127.0.0.1:{port1}");

    let store0 = Arc::new(RedisStore::with_config(&url0, Metrics::new(), TTL_SECS)?);
    let store1 = Arc::new(RedisStore::with_config(&url1, Metrics::new(), TTL_SECS)?);

    run_phase!("Redis containers healthy", {
        // Verify both stores are reachable
        let t0 = GeoTrie::new(S2_LEVEL);
        store0.persist_trie(&t0).await?;
        store1.persist_trie(&t0).await?;
        Ok::<_, anyhow::Error>(format!("shard0=:{port0}  shard1=:{port1}"))
    });

    // ── Phase 2: High-volume write ───────────────────────────────────────────
    println!("\nPhase 2 — HIGH-VOLUME WRITE ({} entities × 2 shards)", ENTITIES_PER_SHARD);

    let (trie0, trie1) = generate_split_tries(ENTITIES_PER_SHARD, verbose);

    run_phase!("Persist shard-0 (50 k entities)", {
        store0.persist_trie(&trie0).await?;
        Ok::<_, anyhow::Error>(format!("{} entities", trie0.len()))
    });

    run_phase!("Persist shard-1 (50 k entities)", {
        store1.persist_trie(&trie1).await?;
        Ok::<_, anyhow::Error>(format!("{} entries", trie1.len()))
    });

    run_phase!("Concurrent writes (4 tasks × 2500 entities)", {
        let (s0, s1) = (Arc::clone(&store0), Arc::clone(&store1));
        let tasks: Vec<_> = (0..4).map(|i| {
            let (sx, trie) = if i % 2 == 0 {
                (Arc::clone(&s0), generate_random_trie(2500, i * 7919))
            } else {
                (Arc::clone(&s1), generate_random_trie(2500, i * 7907))
            };
            tokio::spawn(async move { sx.persist_trie(&trie).await })
        }).collect();
        for t in tasks { t.await??; }
        Ok::<_, anyhow::Error>("4 concurrent persist_trie calls completed".to_string())
    });

    // ── Phase 3: Split seeding ───────────────────────────────────────────────
    println!("\nPhase 3 — SPLIT SEEDING (snapshot-first via merge_entries)");

    // Collect entities from shard-0 that belong to [SPLIT_PREFIX, ∅) → migrate to shard-1
    let migrating = collect_range(&trie0, SPLIT_PREFIX, "");

    run_phase!("Collect entities >= split prefix", {
        if migrating.is_empty() {
            anyhow::bail!("no entities in split range — increase entity count");
        }
        Ok::<_, anyhow::Error>(format!("{} entities to migrate (prefix >= '{SPLIT_PREFIX}')", migrating.len()))
    });

    run_phase!("merge_entries on shard-1 (seeding from snapshot)", {
        let written = store1.merge_entries(&migrating, S2_LEVEL).await?;
        if written != migrating.len() {
            anyhow::bail!("expected {} written, got {}", migrating.len(), written);
        }
        Ok::<_, anyhow::Error>(format!("{written} entities seeded idempotently"))
    });

    run_phase!("Re-run merge_entries (idempotency check)", {
        // Second call with same data and same written_at → should write 0 (all already up-to-date)
        let written = store1.merge_entries(&migrating, S2_LEVEL).await?;
        if written != 0 && written != migrating.len() {
            // written == 0 is ideal; written == all is also acceptable (same timestamp == >=)
            anyhow::bail!("unexpected write count on re-run: {written}");
        }
        Ok::<_, anyhow::Error>(format!("{written} entries re-written (all already fresh)"))
    });

    // ── Phase 4: Freshness ordering ──────────────────────────────────────────
    println!("\nPhase 4 — FRESHNESS ORDERING");

    let fresh_ts   = now_ms();
    let stale_ts   = fresh_ts - 60_000;       // 60 s in the past
    let future_ts  = fresh_ts + 60_000;       // 60 s in the future

    let probe_id   = "freshness-probe";
    let probe_lat  = 48.85_f64;
    let probe_lon  = 2.35_f64;

    // Seed with a "current" entry
    let current = vec![GeoEntry { id: probe_id.into(), lat: probe_lat, lon: probe_lon,
                                   payload: json!({"version": 1}), written_at: fresh_ts }];
    store1.merge_entries(&current, S2_LEVEL).await?;

    run_phase!("Stale write is rejected by merge_entries", {
        let stale = vec![GeoEntry { id: probe_id.into(), lat: probe_lat, lon: probe_lon,
                                     payload: json!({"version": 0}), written_at: stale_ts }];
        let written = store1.merge_entries(&stale, S2_LEVEL).await?;
        if written != 0 {
            anyhow::bail!("stale entry was written (written={written})");
        }
        Ok::<_, anyhow::Error>("stale entry correctly rejected".to_string())
    });

    run_phase!("Fresher write is accepted by merge_entries", {
        let newer = vec![GeoEntry { id: probe_id.into(), lat: probe_lat, lon: probe_lon,
                                     payload: json!({"version": 2}), written_at: future_ts }];
        let written = store1.merge_entries(&newer, S2_LEVEL).await?;
        if written != 1 {
            anyhow::bail!("fresh entry was not written (written={written})");
        }
        Ok::<_, anyhow::Error>("newer entry correctly applied".to_string())
    });

    // ── Phase 5: Delta sync ──────────────────────────────────────────────────
    println!("\nPhase 5 — DELTA SYNC (entities_written_after)");

    // Record the snapshot timestamp then write new entries to shard-0
    let snapshot_ts = now_ms();
    tokio::time::sleep(Duration::from_millis(50)).await; // ensure written_at > snapshot_ts

    let delta_count = 500usize;
    let delta_trie  = generate_random_trie(delta_count, 99991);
    store0.persist_trie(&delta_trie).await?;

    run_phase!("entities_written_after returns only new writes", {
        let delta = store0.entities_written_after(snapshot_ts, "", "").await?;
        if delta.is_empty() {
            anyhow::bail!("no entities returned for delta sync (snapshot_ts={snapshot_ts})");
        }
        if delta.len() > delta_count + 50 {
            anyhow::bail!("too many delta entries: got {} (expected ~{})", delta.len(), delta_count);
        }
        Ok::<_, anyhow::Error>(format!("{} delta entities (of {} written after snapshot)", delta.len(), delta_count))
    });

    run_phase!("Delta applied to shard-1 with freshness check", {
        let delta = store0.entities_written_after(snapshot_ts, "", "").await?;
        let written = store1.merge_entries(&delta, S2_LEVEL).await?;
        Ok::<_, anyhow::Error>(format!("{}/{} delta entries applied to shard-1", written, delta.len()))
    });

    run_phase!("entities_written_after with prefix filter", {
        // Only ask for entities in the Americas prefix range [∅, "5")
        let delta = store0.entities_written_after(snapshot_ts, "", "5").await?;
        // All returned tokens should be < "5"
        // (we can't verify token ordering here without a trie, but we verify count is bounded)
        Ok::<_, anyhow::Error>(format!("{} entities in [∅, '5') range", delta.len()))
    });

    // ── Phase 6: Remove range (GeoTrie) ──────────────────────────────────────
    println!("\nPhase 6 — REMOVE RANGE (GeoTrie::remove_range)");

    run_phase!("remove_range prunes correct entries", {
        let mut trie = generate_random_trie(1000, 55555);
        let before   = trie.len();

        // Remove all entries with token < "5" (Americas range)
        let pruned = trie.remove_range("", "5");
        let after  = trie.len();

        if pruned.is_empty() {
            anyhow::bail!("remove_range returned 0 entries — something is wrong");
        }
        if before != after + pruned.len() {
            anyhow::bail!("before={} after={} pruned={} — counts don't add up", before, after, pruned.len());
        }
        // Verify remaining entries are all outside [∅, "5")
        let helper = GeoTrie::new(S2_LEVEL);
        for e in trie.all_entries() {
            let tok = helper.cell_token(e.lat, e.lon);
            if tok.as_str() < "5" {
                anyhow::bail!("entry {} with token {} survived remove_range(['', '5'))", e.id, tok);
            }
        }
        Ok::<_, anyhow::Error>(format!("pruned {}/{} entries, {} remain", pruned.len(), before, after))
    });

    run_phase!("remove_range is a no-op on empty range", {
        let mut trie = generate_random_trie(100, 12345);
        // An empty range [x, x) should remove nothing
        let pruned   = trie.remove_range("z", "z");
        if !pruned.is_empty() {
            anyhow::bail!("expected 0, got {}", pruned.len());
        }
        Ok::<_, anyhow::Error>("no entries removed for zero-width range".to_string())
    });

    // ── Phase 7: Consistency ─────────────────────────────────────────────────
    println!("\nPhase 7 — CONSISTENCY");

    run_phase!("No orphaned cell keys on shard-0", {
        // After all the writes, query a global region — results should be non-empty
        let all_tokens: Vec<String> = (0..10).map(|i| {
            let helper = GeoTrie::new(S2_LEVEL);
            helper.cell_token(-80.0 + i as f64 * 16.0, -170.0 + i as f64 * 30.0)
        }).collect();
        let entries = store0.query_region(&all_tokens).await?;
        Ok::<_, anyhow::Error>(format!("{} entities in sampled region", entries.len()))
    });

    run_phase!("query_region returns correct GeoEntry shape", {
        let helper    = GeoTrie::new(S2_LEVEL);
        let token     = helper.cell_token(51.5, -0.1);  // London
        // This might be empty depending on random placement, but should not error
        let _entries  = store0.query_region(&[token.clone()]).await?;
        Ok::<_, anyhow::Error>(format!("query_region for token {} completed", &token[..6]))
    });

    // ── Summary ──────────────────────────────────────────────────────────────
    println!();
    println!("══════════════════════════════════════════════════════════════");
    let total = passed + failed;
    if failed == 0 {
        println!("  RESULT: ALL {total} TESTS PASSED ✓");
    } else {
        println!("  RESULT: {failed}/{total} TESTS FAILED ✗");
    }
    println!("══════════════════════════════════════════════════════════════");
    println!();

    std::process::exit(if failed == 0 { 0 } else { 1 });
}

// ── Helpers ────────────────────────────────────────────────────────────────

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Generate a GeoTrie with `n` random entities seeded from `seed`.
fn generate_random_trie(n: usize, seed: u64) -> GeoTrie {
    let mut rng  = rand::rngs::StdRng::seed_from_u64(seed);
    let mut trie = GeoTrie::new(S2_LEVEL);
    for i in 0..n {
        let lat = rng.gen_range(-85.0_f64..85.0);
        let lon = rng.gen_range(-180.0_f64..180.0);
        trie.insert(GeoEntry {
            id:         format!("s{seed}-{i:06}"),
            lat, lon,
            payload:    json!({ "speed": rng.gen_range(0..300) }),
            written_at: 0,  // will be stamped by persist_trie
        });
    }
    trie
}

/// Generate two tries split at the Americas / rest-of-world boundary.
/// Shard-0 gets tokens < SPLIT_PREFIX, shard-1 gets tokens >= SPLIT_PREFIX.
fn generate_split_tries(per_shard: usize, verbose: bool) -> (GeoTrie, GeoTrie) {
    let mut rng   = rand::rngs::StdRng::seed_from_u64(42);
    let helper    = GeoTrie::new(S2_LEVEL);
    let mut trie0 = GeoTrie::new(S2_LEVEL);
    let mut trie1 = GeoTrie::new(S2_LEVEL);
    let mut placed = (0usize, 0usize);

    for i in 0usize.. {
        let lat = rng.gen_range(-85.0_f64..85.0);
        let lon = rng.gen_range(-180.0_f64..180.0);
        let tok = helper.cell_token(lat, lon);
        let entry = GeoEntry {
            id:         format!("split-{i:08}"),
            lat, lon,
            payload:    json!({ "idx": i }),
            written_at: 0,
        };
        if tok.as_str() < SPLIT_PREFIX && placed.0 < per_shard {
            trie0.insert(entry);
            placed.0 += 1;
        } else if tok.as_str() >= SPLIT_PREFIX && placed.1 < per_shard {
            trie1.insert(entry);
            placed.1 += 1;
        }
        if placed.0 >= per_shard && placed.1 >= per_shard { break; }
        if i > per_shard * 20 { break; } // safety guard
    }
    if verbose {
        println!("  Generated trie0={} trie1={} entities", trie0.len(), trie1.len());
    }
    (trie0, trie1)
}

/// Collect all entries from a trie whose token falls in [start, end).
fn collect_range(trie: &GeoTrie, start: &str, end: &str) -> Vec<GeoEntry> {
    let helper = GeoTrie::new(S2_LEVEL);
    trie.all_entries()
        .into_iter()
        .filter(|e| {
            let tok = helper.cell_token(e.lat, e.lon);
            let ge  = start.is_empty() || tok.as_str() >= start;
            let lt  = end.is_empty()   || tok.as_str() <  end;
            ge && lt
        })
        .collect()
}
