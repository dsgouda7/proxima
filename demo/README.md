# geo-redis — demo suite

Four live demo applications that exercise every layer of the geo-redis stack:  
real-time ingest, S2 spatial indexing, SSE streaming, distributed shard management, and split/merge orchestration.

---

## Quick start

```powershell
# Windows — builds everything and opens all UIs
.\scripts\run-demo.ps1

# With the 4-node distributed cluster
.\scripts\run-demo.ps1 -WithCluster

# Skip rebuild (reuse existing binaries)
.\scripts\run-demo.ps1 -SkipBuild
```

```bash
# Linux / macOS
./scripts/run-demo.sh
./scripts/run-demo.sh --with-cluster
./scripts/run-demo.sh --skip-build
```

The script starts Redis, both backend servers, and all three Vite dev servers.  
Press **Ctrl+C** to stop everything cleanly.

---

## What's running

| URL | App | Backend |
|-----|-----|---------|
| http://localhost:5173 | OpenSky aircraft tracker | `geo-redis-demo` :3000 |
| http://localhost:5174 | Live METAR weather map | `geo-redis-weather` :3001 |
| http://localhost:5175 | USGS earthquake tracker | `earthquake-server` (.NET) :3003 |
| http://localhost:5176 | Cluster monitor | geo-node cluster :4000–4003 |
| http://localhost:4000–4003 | Distributed geo-nodes | Docker (with `-WithCluster`) |

---

## Demo 1 — OpenSky aircraft tracker (`demo/ui` · `demo/server`)

A live world map of commercial aviation updated every 30 seconds via [OpenSky Network](https://opensky-network.org/).

**What it shows:**
- 10,000 + aircraft ingested into the S2 trie and persisted to Redis on each poll cycle
- Viewport query returning only aircraft visible in the current map bounds, served in 2–10 ms
- Rotating plane icons oriented by true heading
- Redis read-latency panel (p50 / p95 / p99) updated in real time
- Zoom in/out demonstrates how the S2 cell coverage adapts to viewport size

**Key paths:** `demo/server/src/` (REST API + OpenSky poller) · `demo/ui/src/` (React + Leaflet)

---

## Demo 2 — Live METAR weather map (`demo/ui` · `demo/weather-server`)

Global weather conditions streamed from the [FAA/NWS METAR bulk feed](https://aviationweather.gov/data/cache/metars.cache.csv.gz) — no API key required.

**What it shows:**
- ~5,000 METAR weather stations downloaded and decoded every 60 seconds
- Automatic S2 spatial aggregation: 5,000 raw stations → 77 regional clusters (auto-selects S2 levels 2–5 to keep ≤100 clusters visible)
- Zoom-aware drill-down: zoom in and the map switches to finer S2 levels, showing denser station detail
- Server-Sent Events (SSE) stream: each refresh cycle fires one event per cluster (77 events over ~400 ms), visible in the cluster monitor's weather ticker
- WMO weather code → emoji mapping: ⛅ 🌧 ❄ 🌩 etc. with median temperature per cluster
- Persisted to SQLite + Redis for delta queries

**Key paths:** `demo/weather-server/src/` (METAR download, S2 aggregation, SSE) · `demo/ui/src/` (weather Vite config)

---

## Demo 3 — USGS Earthquake Tracker (`demo/earthquake-server` · `demo/ui`)

Real-time earthquake visualization using USGS GeoJSON feeds — **powered by .NET 8 + gRPC** to showcase cross-platform geo-redis integration.

**What it shows:**
- 100–500 recent earthquakes (past 24 hours, magnitude ≥ 2.5) ingested via **gRPC `InsertBatch`**
- **Cross-platform .NET client** demonstrating Protobuf code generation from `georedis.proto`
- Magnitude-based circle sizing and color coding (minor/yellow → great/dark red)
- USGS alert levels (green/yellow/orange/red border rings) and tsunami warnings
- Metrics panel with magnitude distribution and recent large quakes (M ≥ 5.0)
- Poll frequency: **5 minutes** (matches USGS update cycle — no API key required)

**Key paths:** `demo/earthquake-server/` (.NET gRPC client + USGS poller) · `demo/ui/src/AppEarthquake.tsx` (React + Leaflet)

**Start the demo:**
```bash
cd demo/earthquake-server
dotnet run                       # → http://localhost:3003
cd ../ui
npm run dev:earthquake           # → http://localhost:5175
```

---

## Demo 4 — Distributed cluster monitor (`demo/cluster-ui` · `demo/geo-node`)

An operations dashboard for the live 4-node geo-node cluster.  
Requires `.\scripts\run-demo.ps1 -WithCluster`.

**What it shows:**

### Topology view
- SVG ring diagram with four geo-nodes (Americas, Europe, Asia-Pacific, Standby)
- Each node shows: status badge, key count, memory usage, S2 prefix range
- Animated flow arrows — yellow for split seed propagation, blue for delta-sync
- Pulsing rings for `splitting` and `bootstrapping` transitions

### Shard split (auto-orchestrated)
1. Click **Trigger Split** — the active node (e.g. Americas) seizes the `splitting` status
2. The standby node transitions to `bootstrapping` and receives a full snapshot via `/ingest/snapshot`
3. Delta-sync catches up writes that arrived during the snapshot transfer
4. Both nodes become `active` with non-overlapping S2 prefix ranges
5. The topology diagram redraws with the new shard boundary

### Shard merge
1. Click **Trigger Merge** — the monitor auto-detects the adjacent standby shard
2. The absorbing node fetches all keys from the target via delta-sync (`/delta-sync?since_ms=0`)
3. Freshness-ordered upsert (`merge_entries`) ensures no stale write wins
4. Target resets to `standby`; absorbing node extends its prefix range

### Charts
- Rolling 90-point throughput chart (2 s ticks)
- Horizontal key-distribution bars per shard (animated width)
- Live event log: color-coded by event kind (split ⟿, bootstrap ↻, ok ✓, warn ⚠)

### Weather panel (embedded)
- Polls `geo-redis-weather` metrics every 3 seconds
- Subscribes to the weather SSE stream live — shows streaming progress `⚡ Streaming 12/77…` during each METAR cycle
- Event ticker: WMO emoji + ICAO station ID + temperature + condition label

**Key paths:** `demo/cluster-ui/src/` (React + Vite) · `demo/geo-node/src/` (distributed node daemon)

---

## Demo 5 — Cluster integration test (`demo/cluster-test`)

A headless Rust test that spins up two Redis containers via testcontainers-rs and runs the full distributed protocol automatically.

```powershell
cargo run -p geo-redis-cluster-test
```

**Seven phases tested:**
1. Setup — two ephemeral Redis containers
2. High-volume write (10k entries)
3. Split seeding with `merge_entries` idempotency check
4. Freshness ordering — older `written_at` must not overwrite newer
5. Delta-sync — only entries newer than `since_ms` returned
6. `remove_range` — key removal within S2 prefix bounds
7. Consistency — all surviving keys are in the correct shard's range

---

## Load test (`demo/loadtest`)

Drives sustained write and read QPS at the geo-node cluster.

```powershell
cargo run -p geo-redis-loadtest --release -- --target http://localhost:4000 --rps 5000
```

---

## gRPC cache benchmark (`demo/grpc-bench`)

Head-to-head comparison of two geo-cache strategies measured **over real gRPC**:
a naive Redis cache (`SUNION` over S2 cell sets + pipelined `GET`) versus
geo-redis's in-memory trie-based cache. Both backends sit behind the same
hand-rolled gRPC service, so the latency delta reflects the cache strategy, not
the transport. Requires a local Redis (`docker compose up -d` in `demo/`); the
Redis backend is skipped with a warning if none is reachable.

```powershell
cargo run -p geo-redis-grpc-bench --release -- --entities 40000 --queries 1500
```

Sample output:

```
===================== gRPC region-query latency =====================
  backend            p50       p95       p99       max       QPS
  --------------------------------------------------------------------
  naive-redis    7.89 ms  11.08 ms  22.99 ms  35.17 ms       120
  trie           3.68 ms   5.35 ms   6.13 ms  13.95 ms       261
  ====================================================================
  -> trie is 2.1x faster than naive-redis at the median (p50).
```

---

## Architecture of this demo suite

```
demo/
├── server/             REST backend — OpenSky poller, Redis persistence, SQLite
├── weather-server/     METAR download, S2 aggregation (auto-level), SSE stream
├── earthquake-server/  .NET gRPC client — USGS poller, Protobuf code gen
├── geo-node/           Distributed node daemon — gossip, split, merge, bootstrap
├── ui/                 React + Leaflet — aircraft + weather + earthquake maps
├── cluster-ui/         React — cluster monitor + topology + charts + weather panel
├── cluster-test/       Rust integration tests (testcontainers-rs)
├── loadtest/           Rust load generator
├── grpc-bench/         Rust gRPC benchmark — naive Redis vs trie cache
├── docker-compose.yml        single Redis for local demos
└── cluster-compose.yml       4-node geo-node cluster with sidecar Redis instances
```

---

## Prerequisites

| Tool | Version |
|------|---------|
| [Rust](https://rustup.rs) | stable (≥ 1.87) |
| [.NET SDK](https://dotnet.microsoft.com) | ≥ 8.0 |
| [Node.js](https://nodejs.org) | ≥ 24 |
| [Docker Desktop](https://docker.com) | any recent |

No API keys required for any demo.
