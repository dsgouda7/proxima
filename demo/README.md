# georedis — demo suite

Three live demo applications that exercise every layer of the georedis stack:  
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
| http://localhost:5173 | OpenSky aircraft tracker | `georedis-demo` :3000 |
| http://localhost:5174 | Live METAR weather map | `georedis-weather` :3001 |
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

## Demo 3 — Distributed cluster monitor (`demo/cluster-ui` · `demo/geo-node`)

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
- Polls `georedis-weather` metrics every 3 seconds
- Subscribes to the weather SSE stream live — shows streaming progress `⚡ Streaming 12/77…` during each METAR cycle
- Event ticker: WMO emoji + ICAO station ID + temperature + condition label

**Key paths:** `demo/cluster-ui/src/` (React + Vite) · `demo/geo-node/src/` (distributed node daemon)

---

## Demo 4 — Cluster integration test (`demo/cluster-test`)

A headless Rust test that spins up two Redis containers via testcontainers-rs and runs the full distributed protocol automatically.

```powershell
cargo test -p georedis-cluster-test -- --nocapture
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
cargo run -p georedis-loadtest --release -- --target http://localhost:4000 --rps 5000
```

---

## Architecture of this demo suite

```
demo/
├── server/          REST backend — OpenSky poller, Redis persistence, SQLite
├── weather-server/  METAR download, S2 aggregation (auto-level), SSE stream
├── geo-node/        Distributed node daemon — gossip, split, merge, bootstrap
├── ui/              React + Leaflet — aircraft tracker + weather map (two Vite configs)
├── cluster-ui/      React — cluster monitor + topology + charts + weather panel
├── cluster-test/    Rust integration tests (testcontainers-rs)
├── loadtest/        Rust load generator
├── docker-compose.yml        single Redis for local demos
└── cluster-compose.yml       4-node geo-node cluster with sidecar Redis instances
```

---

## Prerequisites

| Tool | Version |
|------|---------|
| [Rust](https://rustup.rs) | stable (≥ 1.87) |
| [Node.js](https://nodejs.org) | ≥ 20 |
| [Docker Desktop](https://docker.com) | any recent |

No API keys required for any demo.
