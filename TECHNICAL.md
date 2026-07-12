# Proxima — Technical Design Document

**Status:** Draft v0.1 — benchmark harnesses are checked in; no published results have been independently reproduced
**Scope:** Core library (`proxima`), distributed geo-node daemon, split/merge protocol

> **Product framing:** Proxima is a distributed geospatial cache for sub-millisecond reads and multi-million entity storage, backed by any managed Redis instance. Each shard is a stateless Rust service with its own dedicated Redis — a $50/month managed Redis per region supports ~6 million entities. Shards split without downtime as load grows.

---

## 1. Problem Statement

Standard distributed databases partition data by consistent hash (Redis Cluster) or ordered row key (HBase, CockroachDB). For geospatial workloads, consistent hashing creates an irreconcilable tension:

- **Fast range queries** require that geographically adjacent entities live on the same shard, because a viewport query is answered by a `SUNION` over a small set of cell keys — one network round-trip.
- **Consistent hashing** deliberately distributes adjacent keys across shards to balance load, which breaks locality and forces the `SUNION` to fan out across every shard.

Redis Cluster's own geo commands (`GEOADD`/`GEORADIUS`) are single-node only for this reason.

Tile38 solves the single-node problem with Raft replication but has no horizontal split protocol — a single node must hold all data for a geographic region.

**Proxima's thesis:** S2 cell token strings form a total order that respects geographic locality. Using the token string as both the Redis key suffix and the shard routing key allows shard boundaries to be pure lexicographic prefix comparisons. Splits require no data reshuffle — only a bounded catch-up window, and each shard's Redis instance is completely independent.

### One Redis per shard — not shared

This is the fundamental topology decision. Shards do **not** share a Redis instance. When a split happens, entities are HTTP-transferred from the source's Redis to the target's Redis via `/ingest-snapshot`, then deleted from the source. There is no cross-shard Redis operation during steady-state reads or writes.

In Docker (`demo/cluster-compose.yml`), each geo-node container has a dedicated `redis:7-alpine` sidecar. In Kubernetes (`demo/k8s/`), Redis runs as a sidecar in each shard pod on the loopback interface (<0.1 ms). In production, replace the sidecar `REDIS_URL` with a managed instance (Azure Cache for Redis, AWS ElastiCache, Redis Cloud) in the same region as the geo-node.

---

## 2. The S2 Trie Index

### 2.1 Cell tokens

Google's S2 geometry library divides the sphere into a hierarchical grid of cells identified by 64-bit `CellID` values. When formatted as a hex string with trailing zeros stripped, adjacent cells share a common prefix:

```
4          ← coarse European cell (level 1)
48         ← Western Europe (level 2)
487        ← England/France (level 3)
487a       ← London area (level 4)
487a3      ← Central London (level 5)
```

A viewport covering London generates S2 tokens `487a`, `487b`, `487c`, … — all share the `487` prefix. A viewport covering Tokyo generates tokens starting with `a3f`. A single shard holds `[487, 48c)` — all Western European cells — and never touches Tokyo data.

### 2.2 Trie structure

```
Root ∅
├── "4"  (Europe)
│   └── "48" (Western Europe)
│       └── "487" → {23 entities: UAL123, BAW456, …}
├── "8"  (Americas)
│   └── "89c" → {31 entities}
└── "a"  (Asia-Pacific)
    └── "a3f" → {18 entities}
```

- **Insert:** O(token_length) ≈ O(1) — S2 level 9 produces 5-character tokens.
- **Viewport query:** O(covering_size) — a 200×200 km viewport at zoom 10 requires ≤ 8 token lookups, each resolving to a Redis `SET` of entity IDs.
- **Memory:** depends on token distribution, payload size, allocator behavior, and the active index sets; profile the target workload before capacity planning.

Redis storage use has not yet been published from a reproducible benchmark. The
per-entity `entity`, `cell`, and `location` indexes are documented below, but
their memory overhead depends on Redis encoding thresholds and workload shape.

All keys are namespaced under a configurable prefix (default `proxima`):

| Key pattern | Type | Content | TTL |
|---|---|---|---|
| `{ns}:entity:{id}` | STRING | JSON `GeoEntry` | `entity_ttl_secs` |
| `{ns}:cell:{token}` | SET | entity IDs in this cell | `entity_ttl_secs` |
| `{ns}:location:{id}` | STRING | current cell token | `entity_ttl_secs` |
| `{ns}:written_at` | ZSET | score=ms, member=id | none (pruned by `prune_written_at`) |
| `{ns}:active_cells` | SET | all occupied cell tokens | `entity_ttl_secs` |
| `/proxima/{ns}/range-claims/{prefix}` | etcd key | durable range owner | released by an explicit merge |

The `written_at` sorted set is the only key without a TTL — it is pruned periodically by `prune_written_at()` which removes members whose backing entity key has expired. In steady state its size equals the live entity count.

---

## 3. Shard Split Protocol

### 3.1 Correctness invariants

1. **At most one active owner** for any token at any time. Enforced by the range claim CAS (`SET NX EX 120` on `{ns}:range_claim:{prefix_start}`).
2. **No lost writes** during split. The source node stays active for the range until the target transitions to Active. Writes to the split-off range during bootstrapping are served by the source and captured in the `written_at` sorted set.
3. **Freshness ordering** (`merge_entries`). A snapshot entry never overwrites a live write. Score comparison in the ZSET ensures `incoming.written_at ≥ existing.written_at` before any write.

### 3.2 Protocol sequence

```
Source (node-0)                         Target (node-1, was Standby)
────────────────────────────────────────────────────────────────────
1. status → Splitting
2. Scan entity keys ≥ split_point P
   Phase 1: collect (read-only)
   Phase 2: POST /ingest-snapshot        → Persist to SQLite (durable write-ahead)
            (100-entry chunks)           → merge_entries() into Redis
            Record snapshot_ts = T
3. PUT /assign-range {                   → SET NX range_claim:{P}  ← CAS guard
     prefix_start: P,                    → if conflict → 409, abort
     prefix_end:   old_end,             → status → Bootstrapping
     source_addr,                        → spawn bootstrap_delta_sync(src, T)
     snapshot_timestamp: T              }
4. Own prefix_end → P                    ┌── GET /delta-sync?since_ms=T
5. status → Active                       │   (pipelined location lookups)
                                         │   Returns entries with written_at > T
                                         └── merge_entries(delta)
                                             del range_claim:{P}
                                             status → Active
```

### 3.3 Latency bound on split

Let:
- $W$ = write QPS at split time (writes/s)
- $\Delta t$ = snapshot transfer time (s) = $\frac{N \cdot E}{B}$ where $N$ = entity count, $E$ = avg entry bytes, $B$ = network bandwidth
- $\delta$ = delta-sync round-trip latency (typically 20–100ms)

**Catch-up entry count:**
$$C = W \cdot \Delta t$$

**Total split duration for target to reach Active:**
$$T_{split} = \Delta t + \delta$$

**Key property:** $T_{split}$ is independent of shard size. A 10M-entity shard and a 1k-entity shard have the same $\delta$ — only $\Delta t$ scales with size, and $\Delta t$ is bounded by bandwidth, not by key count as in slot-based reshuffling.

**Example:** At $W = 5{,}000$ writes/s, $\Delta t = 2\text{s}$, $E = 200$ bytes:
- Catch-up entries: $C = 10{,}000$
- Network overhead: $2\text{ MB}$ (single HTTP call)
- Total split time: $\approx 2.05\text{ s}$

Compare Redis Cluster slot migration at 500k keys × 200 bytes = **100 MB** transfer with continuous MIGRATE overhead and client-visible MOVED errors throughout.

### 3.4 Required split validation

The experiment harness should write entities at a controlled rate during a
split, call `entities_written_after(T_snapshot)`, and compare the result with
an independent source-of-truth write log. No outcome has been recorded as a
published result.

The split protocol remains experimental until endpoint-level fault injection
demonstrates the behavior for target `409`, target `401`, request timeout,
target crash during bootstrap, and source crash during cleanup. Validation must
also establish no data loss or unowned range under concurrent writes, retries,
and network partitions.

---

## 4. Merge Protocol

Merge is the inverse of split with freshness safety:

1. Absorbing node marks itself `Merging`.
2. `GET /delta-sync?since_ms=0` from target — fetches all entities.
3. `merge_entries(all_target_entities)` — freshness check ensures source's live writes are never overwritten.
4. Extend own `prefix_end` to target's `prefix_end`.
5. `PUT /assign-range { prefix_start: "", prefix_end: "" }` on target → resets to Standby.

---

## 5. Gossip and Failure Detection

### 5.1 Base protocol

- **Period:** `gossip_interval_secs` (default 2s)
- **Fanout:** 2 random peers per cycle
- **Merge rule:** higher `generation` wins; tie broken by `last_seen_secs`
- **State machine:** Active → Suspect (age > `suspect_secs`) → Dead (age > `dead_secs`)

### 5.2 SWIM indirect pinging

Before escalating a node to Suspect/Dead, the observer asks 2 other Active nodes to probe the target via `POST /probe { target }`. Only if all indirect probes fail does escalation proceed. This eliminates false positives from one-hop network blips — the key insight from the 2002 SWIM paper.

```
Observer              Proxy A          Proxy B          Target
   │── direct gossip ──────────────────────────────────► FAIL
   │── POST /probe { target } ──────► GET /health ──────► OK?
   │── POST /probe { target } ───────────────────────► GET /health ──► FAIL?
   │
   └── ALL proxies failed → escalate to Suspect
```

### 5.3 Known gap: consensus on range metadata

Range assignments use a Redis CAS lock (`SET NX EX 120`) which prevents two nodes from simultaneously claiming the same prefix — but this lock is not replicated. In a network partition where the lock-holding Redis becomes unreachable, a new node on the other partition side could claim the same range. Full correctness requires a Raft-based range assignment log (future work).

---

## 6. API Reference

### Library (`proxima` crate)

```rust
// Core trait — implement for mocking in tests
pub trait GeoStore: Send + Sync {
    async fn merge_entries(&self, entries: &[GeoEntry], s2_level: u8) -> Result<usize>;
    async fn entities_written_after(&self, since_ms: u64, start: &str, end: &str) -> Result<Vec<GeoEntry>>;
    async fn prune_written_at(&self) -> Result<usize>;
    async fn persist_trie(&self, trie: &GeoTrie) -> Result<()>;
    async fn query_region(&self, tokens: &[String]) -> Result<Vec<GeoEntry>>;
    fn metrics(&self) -> &Arc<Metrics>;
}

// Concrete Redis implementation
RedisStore::new(redis_url, metrics)            // default namespace "proxima"
    .with_namespace("tenant-acme")             // multi-tenant isolation
    .with_config(url, metrics, ttl_secs)       // explicit TTL

// S2 trie (in-process, no I/O)
GeoTrie::new(s2_level: u8)
trie.insert(GeoEntry { id, lat, lon, payload, written_at })
trie.query_token(token: &str) -> Vec<&GeoEntry>
trie.cell_token(lat, lon) -> String
trie.all_entries() -> Vec<GeoEntry>
trie.remove_range(start, end) -> Vec<GeoEntry>
```

### geo-node HTTP endpoints

| Method | Path | Auth | Description |
|---|---|---|---|
| `GET` | `/health` | — | `{"ok": true}` |
| `GET` | `/cluster` | — | All nodes in the gossip ring |
| `GET` | `/state` | — | This node's `NodeInfo` |
| `GET` | `/delta-sync?since_ms=T` | — | Entities written after T in this shard's range |
| `GET` | `/metrics` | — | JSON metrics snapshot |
| `GET` | `/metrics/prom` | — | Prometheus text format |
| `GET` | `/trace?lat=N&lon=E` | — | Routing trace for a coordinate |
| `POST` | `/gossip` | — | Receive gossip push, return own state |
| `POST` | `/probe` | — | SWIM indirect probe relay |
| `POST` | `/ingest` | API key | Batch entity upsert |
| `POST` | `/ingest-snapshot` | API key | Receive split seed (snapshot entries) |
| `POST` | `/split` | API key | Trigger shard split |
| `POST` | `/merge` | API key | Absorb adjacent shard |
| `PUT` | `/assign-range` | API key | Assign prefix range (called by splitting node) |
| `DELETE` | `/entity/:id` | API key | Immediate entity removal |

---

## 7. Metrics Architecture

### 7.1 What is instrumented

The `Metrics` struct (per `RedisStore` instance) now uses **HDR histograms** backed by the `hdrhistogram` crate, replacing the previous avg/max counters. The full latency distribution is captured at sub-microsecond resolution:

| Metric | Type | Description |
|---|---|---|
| `write_count` | counter | Total `persist_trie` calls |
| `write_p50/p95/p99/p99.9_us` | histogram | Write latency percentiles (µs) |
| `write_max_us` | gauge | Max write latency observed |
| `read_count` | counter | Total `query_region` calls |
| `read_p50/p95/p99/p99.9_us` | histogram | Read latency percentiles (µs) |
| `read_max_us` | gauge | Max read latency observed |

The geo-node exposes these plus Redis `DBSIZE` and `INFO memory` at `GET /metrics/prom` in Prometheus text format under the `proxima_*` namespace.

### 7.2 Additional metrics to add for production

**Split/bootstrap duration**    // total split time
proxima_bootstrap_duration_ms{node_id}                // snapshot + delta-sync time
proxima_delta_sync_entries{node_id}                   // entries in last delta-sync
proxima_snapshot_transfer_ms{node_id}                 // phase 2 transfer time
```

**ZSET health**

```
proxima_written_at_zset_size{node_id}   // live ZSET cardinality (should ≈ key_count)
proxima_prune_removed_total{node_id}    // cumulative entries pruned (should stay near 0)
```

**S2-level breakdown**

```
proxima_query_cells{node_id, s2_level}  // avg cells per viewport query
proxima_entities_per_cell{node_id}      // distribution: how many entities per occupied cell
```

### 7.3 Roll-up: cluster-wide view

Scrape all geo-nodes from a single Prometheus instance. Aggregate labels to get cluster-wide metrics:

```promql
# Total write QPS across all shards
sum(rate(proxima_write_count[1m]))

# p99 read latency worst shard
max(proxima_query_latency_us{quantile="0.99"})

# Total entities in cluster
sum(proxima_key_count)

# ZSET drift (writes at-risk of loss if node crashes)
sum(proxima_written_at_zset_size) - sum(proxima_key_count)

# Split frequency over 24h
increase(proxima_split_duration_ms_count[24h])
```

### 7.4 Drill-in: per-shard / per-prefix analysis

```promql
# Single shard latency over time
proxima_query_latency_us{quantile="0.99", node_id="node-0"}

# Bootstrap catch-up vs. write rate (validate the W×Δt bound)
proxima_delta_sync_entries{node_id="node-1"} /
  rate(proxima_write_count{node_id="node-0"}[30s])
# Should equal Δt (snapshot transfer duration)

# Shard balance: flag shards with > 2× average key count
proxima_key_count / avg(proxima_key_count)
```

### 7.5 Recommended dashboard layout

```
┌─────────────────────────────────────────────────────────────────┐
│  CLUSTER HEALTH (roll-up row)                                   │
│  Total keys │ Write QPS │ p99 read latency │ Active splits       │
├─────────────────────────────────────────────────────────────────┤
│  PER-SHARD (one panel per node_id)                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐  │
│  │ Key count    │  │ Write latency│  │ ZSET size vs keys    │  │
│  │ over time    │  │ p50/p95/p99  │  │ (drift = data at     │  │
│  │ bar chart    │  │ line chart   │  │  risk on crash)      │  │
│  └──────────────┘  └──────────────┘  └──────────────────────┘  │
├─────────────────────────────────────────────────────────────────┤
│  SPLIT / MERGE EVENTS                                           │
│  Timeline of splits with duration + delta_sync_entries          │
│  Overlay: write QPS at split time (validate W×Δt bound)         │
└─────────────────────────────────────────────────────────────────┘
```

### 7.6 Criterion micro-benchmarks (in-process)

For latency without network noise, use the Criterion suite in `lib/benches/`:

```bash
cargo bench -p proxima                        # run all benches
cargo bench -p proxima -- insert_10k          # single bench
cargo bench -p proxima -- --save-baseline v1  # save baseline
# ... make changes ...
cargo bench -p proxima -- --baseline v1       # compare
```

Current benchmarks:

| Benchmark | Description |
|---|---|
| `insert_10k` | Insert 10k entries into a fresh GeoTrie |
| `query_token` | Single token lookup on 10k-entry trie |

**Benchmarks to add:**

| Benchmark | What it measures |
|---|---|
| `persist_trie_10k` | Full Redis write cycle (requires running Redis) |
| `query_region_viewport` | SUNION + 100-entity GET pipeline |
| `merge_entries_1k` | Freshness check + write cycle |
| `entities_written_after_1k` | ZRANGEBYSCORE + pipelined location lookups |
| `split_10k` | Full split protocol end-to-end (two in-process nodes) |

---

## 8. Local Docker Experiment Results

The following is one reproducible direct library-to-Redis run. It is not an
HTTP, geo-node, Kubernetes, cross-host, managed-Redis, or production-capacity
benchmark.

| Input | Value |
|---|---|
| Date | 2026-07-11 |
| Host | Windows 11 Enterprise 10.0.26100; AMD EPYC 7763, 8 cores / 16 logical processors; 64 GB RAM |
| Toolchain | Rust 1.97.0; release build |
| Redis | Redis 7.4.9, `redis:7-alpine`, Docker Desktop 29.6.1, `noeviction`, AOF disabled, snapshots disabled |
| Topology | One Docker Compose Redis container at `127.0.0.1:6379`; loopback connection; isolated logical database 15 was empty before the run |
| Command | `.\scripts\run-experiments.ps1 -Redis 'redis://127.0.0.1:6379/15'` |
| Raw output | Local `target/experiment-results-20260711-232333.txt` |

### 8.1 Results

| Experiment | Workload | Result |
|---|---|---|
| Write latency | 200 sequential `persist_trie` calls, 100-entry full snapshot each | 4.19 ms p50; 6.50 ms p95; 7.17 ms p99; 9.15 ms max |
| Read latency | 5,000-entry full snapshot; 500 queries containing one occupied token | 1 token: 1.14 ms p50, 1.89 ms p99; 8 tokens: 1.14 ms p50, 2.05 ms p99; 32 tokens: 1.15 ms p50, 2.09 ms p99 |
| Criterion insert | `insert_10k`, 100 samples | 13.176 ms estimate; 12.767–13.624 ms confidence interval |
| Criterion lookup | `query_token`, 100 samples | 94.783 ns estimate; 93.755–96.133 ns confidence interval |
| Split delta probe | 3.014 s window, 300 writes/s target | 201 writes attempted, 200 returned (99.5%); achieved 67 writes/s |
| ZSET pruning | 300 entries, 3 s entity TTL, 4 s wait | 300 stale ZSET members before pruning; 300 removed; 0 remaining |
| Redis memory | 5,000-entry full snapshot, workload payload | 1,076 B/entity; 14,990 keys, or 3.00 keys/entity |

The split delta probe missed one write and did not reach its requested rate.
It is therefore neither a zero-loss proof nor a high-throughput split result.
The endpoint-level failure-injection scenarios remain required validation.

### 8.2 Reproduction And Publication Requirements

The available harnesses are:

```powershell
# In-process Criterion micro-benchmarks; no Redis or HTTP involved.
cargo bench -p proxima

# Redis-backed experiment harness; requires a locally reachable Redis instance.
.\scripts\run-experiments.ps1

# Sustained concurrent traffic; configure the Redis endpoint before running.
cargo run --release -p proxima-loadtest -- --writers 4 --readers 16 --duration-secs 60
```

Before adding a result, commit or attach the raw output and record:

1. Git revision, operating system, CPU model, memory, Rust version, and build profile.
2. Redis version, deployment topology, network path, persistence configuration, and memory policy.
3. Entity count, S2 level, payload-size distribution, request mix, concurrency, warm-up, and duration.
4. Latency distribution (`p50`, `p95`, `p99`, maximum), throughput, failures, and the exact command line.

The Redis-backed work that still needs broader benchmark coverage is
`merge_entries`, `entities_written_after`, end-to-end split behavior under
concurrent writes, and throughput across multiple shards. Results from a local
loopback Redis must not be described as HTTP, cross-host, Kubernetes, or
managed-service latency.

---

## 9. Comparison with Related Systems

| System | Geo sharding | Split protocol | Sub-10ms reads | Written in |
|---|---|---|---|---|
| **Proxima** | S2 token prefix | snapshot + bounded delta-sync | ✓ | Rust |
| Redis Cluster | Consistent hash (keyslot) | MIGRATE (blocking) | ✓ | C |
| Tile38 | None (single-node Raft) | N/A | ✓ | Go |
| PostGIS | None | N/A | ✗ (10–100ms) | C |
| MongoDB geo | Zone sharding | Chunk migration | ✗ | C++ |
| H3/S2 libs | Index only, no runtime | N/A | N/A | Various |

**Proxima's unique position:** the only system where the spatial index key *is* the shard routing key, making shard boundaries metadata-only operations and bounding split downtime to `snapshot_transfer_time + one_network_RTT`.

---

## 10. Known Gaps and Future Work

| Gap | Impact | Mitigation today |
|---|---|---|
| External etcd metadata dependency | Split/merge cannot proceed when the quorum is unavailable | `METADATA_ETCD_ENDPOINTS` is required; range changes fail closed |
| `written_at` ZSET is per-shard | Cross-shard delta-sync needs two queries | Each shard's ZSET covers its own range; merge absorbs via `since_ms=0` |
| SWIM: no indirect-ack piggybacking | Slight false-positive rate under load | Threshold tuning via `suspect_secs`/`dead_secs` |
| No multi-level S2 indexing | Single S2 level per store | Use `with_config` to create stores at different levels for different zoom tiers |

### Bug postmortem: `zadd` argument inversion

During experiment development, Exp 3 (W×Δt validation) revealed that `entities_written_after` was returning empty results. Root cause: `redis-rs 0.26` exposes `zadd(key, member, score)` — **member before score** — but the code had `zadd(key, score, member)`. Inside `MULTI/EXEC` atomic pipelines, per-command errors are deferred; combined with `.ignore()` on the failing call, the error was completely silent. The `written_at` sorted set had entity IDs stored as scores (rejected by Redis, silently swallowed) and timestamps stored as members — making all delta-sync queries return nothing.

**Fix:** swap to `zadd(key, member=id, score=timestamp_f64)` in all three call sites (`persist_trie`, `merge_entries`, `route_ingest_batch`). The experiment suite now validates the correct behaviour.

**Lesson:** `.ignore()` inside atomic pipelines is a footgun for commands that produce data depended on by other code paths. Future write pipelines should use explicit error checking or separate non-ignored commands for critical index updates.
