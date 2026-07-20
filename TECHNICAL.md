# geo-redis — Technical Design Document

**Status:** Draft v0.1 — benchmark harnesses are checked in; no published results have been independently reproduced
**Scope:** Core library (`geo-redis`), distributed geo-node daemon, split/merge protocol

> **Product framing:** geo-redis is a distributed geospatial cache for sub-millisecond reads and multi-million entity storage, backed by any managed Redis instance. Each shard is a stateless Rust service with its own dedicated Redis — a $50/month managed Redis per region supports ~6 million entities. Shards split without downtime as load grows.

---

## 1. Problem Statement

Standard distributed databases partition data by consistent hash (Redis Cluster) or ordered row key (HBase, CockroachDB). For geospatial workloads, consistent hashing creates an irreconcilable tension:

- **Fast range queries** require that geographically adjacent entities live on the same shard, because a viewport query is answered by a `SUNION` over a small set of cell keys — one network round-trip.
- **Consistent hashing** deliberately distributes adjacent keys across shards to balance load, which breaks locality and forces the `SUNION` to fan out across every shard.

Redis Cluster's own geo commands (`GEOADD`/`GEORADIUS`) are single-node only for this reason.

Tile38 solves the single-node problem with Raft replication but has no horizontal split protocol — a single node must hold all data for a geographic region.

**geo-redis's thesis:** S2 cell token strings form a total order that respects geographic locality. Using the token string as both the Redis key suffix and the shard routing key allows shard boundaries to be pure lexicographic prefix comparisons. Splits require no data reshuffle — only a bounded catch-up window, and each shard's Redis instance is completely independent.

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

All keys are namespaced under a configurable prefix (default `geo-redis`):

| Key pattern | Type | Content | TTL |
|---|---|---|---|
| `{ns}:entity:{id}` | STRING | JSON `GeoEntry` | `entity_ttl_secs` |
| `{ns}:cell:{token}` | SET | entity IDs in this cell | `entity_ttl_secs` |
| `{ns}:location:{id}` | STRING | current cell token | `entity_ttl_secs` |
| `{ns}:written_at` | ZSET | score=ms, member=id | none (pruned by `prune_written_at`) |
| `{ns}:active_cells` | SET | all occupied cell tokens | `entity_ttl_secs` |
| `/geo-redis/{ns}/range-claims/{prefix}` | etcd key | durable range owner | released by an explicit merge |

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

### Library (`geo-redis` crate)

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
RedisStore::new(redis_url, metrics)            // default namespace "geo-redis"
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
| `nearby_count` | counter | Total `query_nearby` calls |
| `nearby_p50/p95/p99/p99.9_us` | histogram | Nearby pipeline latency (µs): S2 cap covering + Redis SUNION/GET + haversine filter + sort |
| `nearby_max_us` | gauge | Peak nearby query latency observed |

**Interpreting `nearby_p50` vs `read_p50` (Redis-backed):** For `RedisStore`, `query_nearby` calls `query_region` internally, so the nearby histogram captures the full pipeline — Redis SUNION/GET plus haversine filter and sort. Expect `nearby_p50 ≈ read_p50 + cap_covering_cost + sort_cost`. At typical candidate counts (< 200) the haversine and sort are < 5 µs; the S2 cap covering computation is the dominant addition (~1 ms at level 9). If `nearby_p99` diverges sharply from `read_p99`, suspect a very large candidate set — reduce `radius_m` or increase `s2_level` to use finer cells. See §8.7 for measured S2-vs-naive results.

The geo-node exposes these plus Redis `DBSIZE` and `INFO memory` at `GET /metrics/prom` in Prometheus text format under the `geo-redis_*` namespace.

### 7.2 Additional metrics to add for production

**Split/bootstrap duration**    // total split time
geo-redis_bootstrap_duration_ms{node_id}                // snapshot + delta-sync time
geo-redis_delta_sync_entries{node_id}                   // entries in last delta-sync
geo-redis_snapshot_transfer_ms{node_id}                 // phase 2 transfer time
```

**ZSET health**

```
geo-redis_written_at_zset_size{node_id}   // live ZSET cardinality (should ≈ key_count)
geo-redis_prune_removed_total{node_id}    // cumulative entries pruned (should stay near 0)
```

**S2-level breakdown**

```
geo-redis_query_cells{node_id, s2_level}  // avg cells per viewport query
geo-redis_entities_per_cell{node_id}      // distribution: how many entities per occupied cell
```

### 7.3 Roll-up: cluster-wide view

Scrape all geo-nodes from a single Prometheus instance. Aggregate labels to get cluster-wide metrics:

```promql
# Total write QPS across all shards
sum(rate(geo-redis_write_count[1m]))

# p99 read latency worst shard
max(geo-redis_query_latency_us{quantile="0.99"})

# p99 nearby latency (includes query_region + S2 cap covering + haversine + sort)
max(geo-redis_nearby_latency_us{quantile="0.99"})

# Nearby overhead above plain read (dominated by S2 cap computation, not haversine)
# At level-9, ~1ms extra vs read; see §8.7 for measured breakdown
max(geo-redis_nearby_latency_us{quantile="0.99"}) - max(geo-redis_query_latency_us{quantile="0.99"})

# Total entities in cluster
sum(geo-redis_key_count)

# ZSET drift (writes at-risk of loss if node crashes)
sum(geo-redis_written_at_zset_size) - sum(geo-redis_key_count)

# Split frequency over 24h
increase(geo-redis_split_duration_ms_count[24h])
```

### 7.4 Drill-in: per-shard / per-prefix analysis

```promql
# Single shard latency over time
geo-redis_query_latency_us{quantile="0.99", node_id="node-0"}

# Bootstrap catch-up vs. write rate (validate the W×Δt bound)
geo-redis_delta_sync_entries{node_id="node-1"} /
  rate(geo-redis_write_count{node_id="node-0"}[30s])
# Should equal Δt (snapshot transfer duration)

# Shard balance: flag shards with > 2× average key count
geo-redis_key_count / avg(geo-redis_key_count)
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
cargo bench -p geo-redis                        # run all benches
cargo bench -p geo-redis -- --save-baseline v1  # save baseline
cargo bench -p geo-redis -- --baseline v1       # compare to baseline
```

---

## 8. Baseline Methodology And Results

### 8.0 Why a baseline is needed

Measuring latency in isolation says how fast a thing runs; it does not say
whether the structural choice is justified. The baseline chosen here is the
simplest alternative with equivalent retrieval semantics at single S2-level
granularity.

**Naive flat HashMap** (in-process Criterion):

```
HashMap<String, Vec<GeoEntry>>   keyed by S2 token at level 9
```

- Insert: O(1) amortised hash vs trie's O(token_length) pointer walk.
- Exact-token query: O(1) hash lookup vs O(token_length) descent.
- Prefix/coarse-cell query: O(N) full key scan, no shortcut.

**NaiveFlatStore** (Redis experiment harness):

```
write: HSET {ns}.flat:{token}  {id}  {json}   # one hash per S2 cell
read:  HGETALL {ns}.flat:{token}               # all entities in a cell
```

One command per entity on write vs four in `RedisStore` (`SET entity` +
`SADD cell-set` + `SET location` + `ZADD written_at`).
One sequential `HGETALL` per token on read vs `SUNION` + N×`GET` pipeline.

What the flat store **cannot do**: per-entity TTL expiry, move detection
(no `location:id` reverse lookup), delta-sync for shard splits (no
`written_at` ZSET), or active-cell diffing (no Lua vacated-cell cleanup).
It is a lower bound on Redis I/O cost, not a drop-in production alternative.

### 8.1 Environment

| Input | Value |
|---|---|
| Date | 2026-07-12 |
| Host | Windows 11 Enterprise 10.0.26100; AMD EPYC 7763, 8 cores / 16 logical processors; 64 GB RAM |
| Toolchain | Rust 1.97.0; release build |
| Redis | Redis 7.4.9, `redis:7-alpine`, Docker Desktop 29.6.1, `noeviction`, AOF disabled, snapshots disabled |
| Topology | One Docker Compose Redis container at `127.0.0.1:6379`; loopback; database 15 flushed before the run |
| Commands | `cargo bench -p geo-redis` · `.\scripts\run-experiments.ps1 -Redis 'redis://127.0.0.1:6379/15'` |
| Raw outputs | `target/experiment-results-20260712-175628.txt`; Criterion HTML in `target/criterion/` |

### 8.2 In-process Criterion results

10,000-entry synthetic dataset; uniform random lat/lon; empty payload.

| Benchmark | Structure | Estimate | vs baseline |
|---|---|---|---|
| `insert_10k` | GeoTrie | 11.667 ms | baseline: 5.654 ms flat |
| `insert_10k_flat` | HashMap | 5.654 ms | **2.1× faster than trie** |
| `query_token` | GeoTrie | 97.788 ns | baseline: 16.343 ns flat |
| `query_token_flat` | HashMap | 16.343 ns | **6× faster than trie** |
| `query_prefix_coarse` | GeoTrie | 35.160 ns | baseline: 13.388 µs flat |
| `query_prefix_coarse_flat` | HashMap | 13.388 µs | **381× slower than trie** |

The trie pays on insert (2.1×) and on exact single-token lookup (6×). Both
structures are sub-microsecond for queries; neither is the bottleneck —
Redis round-trip time dominates by three orders of magnitude.

The decisive difference is the prefix query. The trie answers a country-scale
viewport query (2-character S2 prefix, covering hundreds of level-9 cells) in
**35 ns**. The HashMap requires a full key scan: **13 µs** — 381× slower. This
gap scales linearly with dataset size and is the structural reason the trie
was chosen over a flat map for multi-resolution spatial indexing.

### 8.3 Redis experiment results

| Experiment | Trie (RedisStore) | Flat (NaiveFlatStore) | Ratio trie/flat | Notes |
|---|---|---|---|---|
| Write p50 (100-entity snapshot) | 4.11 ms | 1.88 ms | **2.18×** | Trie writes 4 keys/entity; flat writes 1 |
| Write p99 | 7.72 ms | 2.99 ms | 2.58× | — |
| Read p50, 1 token | 1.14 ms | 1.24 ms | 0.92× | Near parity; SUNION+pipeline vs HGETALL |
| Read p50, 8 tokens | 1.13 ms | 2.19 ms | 0.52× | Trie batches all tokens; flat loops N×HGETALL |
| Read p50, 32 tokens | 1.16 ms | 5.81 ms | **0.20×** | **Trie is 5× faster** at viewport scale |
| Read p99, 32 tokens | 1.89 ms | 10.70 ms | 0.18× | — |

The 3-key-per-entity schema plus active-cell Lua makes writes **2.2× more
expensive** than the flat alternative. This is the known, accepted cost: the
secondary indexes are what enable per-entity TTL, move detection, and
delta-sync. At a typical Leaflet viewport at zoom 8 the query covers 30–80
S2 tokens; at 32 tokens the trie is **5× faster** because it issues one
pipelined `SUNION` where the flat store issues 32 sequential `HGETALL` calls.

### 8.4 Split delta probe and ZSET

| Experiment | Result |
|---|---|
| Split delta probe | 201 writes attempted; 200 captured (99.5%); achieved 67 writes/s |
| ZSET pruning | 300 entries written; 300 removed after TTL expiry |
| Redis memory (trie, 5k entities) | 1,077 B/entity; 3.00 keys/entity |

The split probe does not reach its requested rate and is not a zero-loss proof.
Endpoint-level failure-injection remains required validation.

### 8.5 gRPC over-the-wire cache benchmark

The Criterion and Redis experiment harnesses measure the store in-process. The
`demo/grpc-bench` harness instead measures the two cache strategies **end-to-end
over a real gRPC (HTTP/2 loopback) connection**, so the numbers include
serialization, transport, and async scheduling — the cost an actual client pays.

Both backends sit behind the *same* hand-rolled gRPC service (wire-compatible
with `demo/geo-node`), differing only in the region-query implementation:

- **naive-redis** — `SUNION` over the covering S2 cell keys, then a pipelined
  `GET` of every entity. One or more Redis round-trips per query.
- **trie** — geo-redis's in-memory `GeoTrie` walked by the S2 viewport tokens.
  No Redis round-trip.

40,000 entities (uniform random lat/lon), 1,500 timed queries, ±5° viewports,
S2 level 9; both backends return an identical 184.5 entries/query on average,
confirming result parity.

| Backend | p50 | p95 | p99 | max | QPS |
|---|---|---|---|---|---|
| naive-redis | 7.89 ms | 11.08 ms | 22.99 ms | 35.17 ms | 120 |
| **trie** | **3.68 ms** | **5.35 ms** | **6.13 ms** | 13.95 ms | **261** |

The trie cache is **2.1× faster at the median** and, more importantly, holds a
far tighter tail (p99 6.1 ms vs 23.0 ms) because it never leaves the process to
answer a query. The naive Redis cache pays a `SUNION` plus a pipelined multi-key
`GET` per request; that round-trip dominates its latency and drives the p99
blow-up under viewport-scale fan-out. This is the over-the-wire complement to
§8.3: the trie's structural advantage on multi-token viewport queries survives
the addition of a real gRPC transport.

Reproduce (requires a local Redis; the Redis backend is skipped with a warning
if none is reachable):

```powershell
cargo run -p geo-redis-grpc-bench --release -- --entities 40000 --queries 1500
```

### 8.6 Reproduction

```powershell
cargo bench -p geo-redis
.\scripts\run-experiments.ps1 -Redis 'redis://127.0.0.1:6379/15'
```

Record CPU, OS, Redis version and topology, payload shape, concurrency, and
warm-up before comparing runs. Loopback Docker results must not be described
as HTTP, cross-host, or managed-service latency.

### 8.7 `query_nearby` vs naive full scan

**What the naive baseline does:** iterate every entity in the dataset, compute
haversine distance to the query point, keep those within `radius_m`, sort, and
truncate to `top_k`. This is the cost of the simplest possible "find nearby"
strategy — fetch all entities from Redis (SCAN + GET-all), filter client-side.
No spatial index, no pruning.

**Environment:** `geo-redis-radio` demo server; 12,648 geo-tagged stations held
in a level-9 `GeoTrie` entirely in process; Windows 11 / AMD EPYC 7763; Rust
release build; loopback HTTP (timers measure server-side query time only, after
lock acquisition). 30 queries per approach across 6 cities at radii 150–400 km.
Reproduced via `GET /api/nearby` and `GET /api/nearby-naive`.

| Approach | p50 | p95 | max | Complexity |
|---|---|---|---|---|
| **S2 spatial index** (`/api/nearby`) | **1.34 ms** | **2.36 ms** | 2.50 ms | O(covering\_size + candidates) |
| Naive full scan (`/api/nearby-naive`) | 43.8 ms | 56.6 ms | 61.2 ms | O(N) |
| **Speedup** | **33×** | **24×** | — | — |

**Cost breakdown of the 1.34 ms S2 path:**

| Step | Approx. cost | Notes |
|---|---|---|
| S2 cap covering computation | ~1.1 ms | Converts `(lat, lon, radius_m)` → ~50–80 level-9 tokens |
| Trie traversal (matching cells only) | ~0.2 ms | Walks only the cells in the covering |
| Haversine filter (~20 candidates) | < 5 µs | O(candidates), not O(N) |
| Sort + truncate | < 5 µs | O(k log k), k ≤ 20 |

The dominant cost is S2 cap covering, **not** haversine. The 43.8 ms naive
cost is almost entirely haversine × 12,648.

**Scaling property:** the S2 covering size is a function of `radius_m` and
`s2_level`, not of the total entity count. Entities outside the query region
are never touched. At 1 million entities the naive approach extrapolates to
~3.5 s; the S2 path stays bounded by the covering size and candidate count.

---

## 9. Comparison with Related Systems

| System | Geo sharding | Split protocol | Sub-10ms reads | Written in |
|---|---|---|---|---|
| **geo-redis** | S2 token prefix | snapshot + bounded delta-sync | ✓ | Rust |
| Redis Cluster | Consistent hash (keyslot) | MIGRATE (blocking) | ✓ | C |
| Tile38 | None (single-node Raft) | N/A | ✓ | Go |
| PostGIS | None | N/A | ✗ (10–100ms) | C |
| MongoDB geo | Zone sharding | Chunk migration | ✗ | C++ |
| H3/S2 libs | Index only, no runtime | N/A | N/A | Various |

**geo-redis's unique position:** the only system where the spatial index key *is* the shard routing key, making shard boundaries metadata-only operations and bounding split downtime to `snapshot_transfer_time + one_network_RTT`.

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
