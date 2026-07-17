use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::Result;
use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    middleware::{self, Next},
    routing::{delete, get, post, put},
    Json, Router,
};

mod grpc;
mod metadata;
mod snapshot;
use metadata::{ClaimResult, EtcdRangeAuthority};
use proxima::{
    cluster::{ClusterRing, NodeInfo, NodeStatus},
    GeoEntry,
};
use proxima::{Metrics, RedisStore};
use rand::seq::SliceRandom;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tower_http::cors::CorsLayer;
use tracing::info;

// ── Config ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct Config {
    node_id: String,
    http_addr: String,
    http_port: u16,
    redis_url: String,
    prefix_start: String,
    prefix_end: String,
    seed_peers: Vec<String>,
    s2_level: u8,
    // Auto-split/merge thresholds — documented in README + K8s configmap.
    // Read at startup and stored for a future automatic-split trigger;
    // splits are currently initiated via POST /split.
    #[allow(dead_code)]
    split_threshold_keys: u64,
    #[allow(dead_code)]
    split_threshold_write_qps: f64,
    #[allow(dead_code)]
    merge_threshold_keys: u64,
    // ── Gossip timing ─────────────────────────────────────────────────────
    suspect_secs: u64,
    dead_secs: u64,
    gossip_interval_secs: u64,
    // ── Snapshot / recovery ───────────────────────────────────────────────
    /// Path for the SQLite snapshot DB. Empty string = disabled.
    snapshot_path: String,
    snapshot_interval_secs: u64,
    /// Redis TTL for entity keys. Set to 2× your write interval so stale
    /// cross-shard data expires promptly after an entity moves regions.
    entity_ttl_secs: u64,
    /// If non-empty, all write endpoints require `X-API-Key: <value>`.
    /// Leave empty in dev. Set via API_KEY env var in production.
    api_key: String,
    /// Redis key namespace prefix. Defaults to "geo-redis".
    /// Override via KEY_NAMESPACE env var for multi-tenant isolation
    /// (multiple logical datasets on the same Redis instance).
    key_namespace: String,
    /// Comma-separated Redis Cluster node URLs.
    /// When non-empty, the store uses redis::cluster::ClusterClient.
    /// All keys use {namespace} hash tags, so SUNION/Lua/pipelines work.
    /// Example: redis://node1:6379,redis://node2:6379,redis://node3:6379
    redis_cluster_urls: Vec<String>,
    /// Port for the gRPC server. Defaults to http_port + 10.
    grpc_port: u16,
    /// Comma-separated etcd v3 endpoints used for consensus-backed range
    /// ownership. Required for production split/merge deployments.
    metadata_etcd_endpoints: Vec<String>,
}

impl Config {
    fn from_env() -> Self {
        let port: u16 = env("HTTP_PORT", "4000").parse().unwrap_or(4000);
        Self {
            node_id: env("NODE_ID", "node-0"),
            http_addr: env("NODE_ADDR", &format!("localhost:{port}")),
            http_port: port,
            redis_url: env("REDIS_URL", "redis://127.0.0.1:6379"),
            prefix_start: env("PREFIX_START", ""),
            prefix_end: env("PREFIX_END", ""),
            seed_peers: env("SEED_PEERS", "")
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(String::from)
                .collect(),
            s2_level: env("S2_LEVEL", "9").parse().unwrap_or(9),
            // Thresholds — override in cluster-compose.yml or K8s ConfigMap
            split_threshold_keys: env_parse("SPLIT_THRESHOLD_KEYS", 500_000u64),
            split_threshold_write_qps: env_parse("SPLIT_THRESHOLD_WRITE_QPS", 50_000f64),
            merge_threshold_keys: env_parse("MERGE_THRESHOLD_KEYS", 25_000u64),
            suspect_secs: env_parse("SUSPECT_SECS", 10u64),
            dead_secs: env_parse("DEAD_SECS", 30u64),
            gossip_interval_secs: env_parse("GOSSIP_INTERVAL_SECS", 2u64),
            // Snapshot
            snapshot_path: env("SNAPSHOT_PATH", ""),
            snapshot_interval_secs: env_parse("SNAPSHOT_INTERVAL_SECS", 300u64),
            entity_ttl_secs: env_parse("ENTITY_TTL_SECS", 120u64),
            api_key: env("API_KEY", ""),
            key_namespace: env("KEY_NAMESPACE", "geo-redis"),
            redis_cluster_urls: env("REDIS_CLUSTER_URLS", "")
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(String::from)
                .collect(),
            grpc_port: env_parse("GRPC_PORT", port + 10),
            metadata_etcd_endpoints: env("METADATA_ETCD_ENDPOINTS", "")
                .split(',')
                .map(str::trim)
                .filter(|endpoint| !endpoint.is_empty())
                .map(String::from)
                .collect(),
        }
    }
}

fn env(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_parse<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

// ── Shared application state ───────────────────────────────────────────────

#[derive(Clone)]
struct AppState {
    cfg: Config,
    ring: Arc<RwLock<ClusterRing>>,
    my_info: Arc<RwLock<NodeInfo>>,
    redis: redis::Client,
    http: reqwest::Client,
    /// None when SNAPSHOT_PATH is empty (snapshotting disabled)
    snapshot: Option<Arc<snapshot::Snapshot>>,
    /// RedisStore wrapping the same Redis connection.
    /// Used for the lib-level `entities_written_after` delta-sync query.
    store: Arc<RedisStore>,
    metadata: EtcdRangeAuthority,
}

impl AppState {
    fn new(cfg: Config, redis: redis::Client) -> anyhow::Result<Self> {
        let now = unix_now();
        let snap = if cfg.snapshot_path.is_empty() {
            None
        } else {
            Some(Arc::new(snapshot::Snapshot::open(&cfg.snapshot_path)?))
        };
        let my = NodeInfo {
            node_id: cfg.node_id.clone(),
            addr: cfg.http_addr.clone(),
            redis_url: cfg.redis_url.clone(),
            prefix_start: cfg.prefix_start.clone(),
            prefix_end: cfg.prefix_end.clone(),
            key_count: 0,
            mem_bytes: 0,
            generation: 1,
            status: if cfg.prefix_start.is_empty() && cfg.prefix_end.is_empty() {
                NodeStatus::Standby
            } else {
                NodeStatus::Active
            },
            last_seen_secs: now,
        };
        let mut ring = ClusterRing::default();
        ring.merge(my.clone());
        let store = Arc::new(if cfg.redis_cluster_urls.is_empty() {
            RedisStore::with_config(cfg.redis_url.as_str(), Metrics::new(), cfg.entity_ttl_secs)
                .expect("RedisStore init")
                .with_namespace(&cfg.key_namespace)
        } else {
            tracing::info!(
                "Using Redis Cluster mode: {} nodes",
                cfg.redis_cluster_urls.len()
            );
            RedisStore::new_cluster(
                cfg.redis_cluster_urls.clone(),
                Metrics::new(),
                cfg.entity_ttl_secs,
            )
            .expect("RedisStore cluster init")
            .with_namespace(&cfg.key_namespace)
        });
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(3))
            .build()
            .unwrap();
        let metadata = EtcdRangeAuthority::new(
            cfg.metadata_etcd_endpoints.clone(),
            http.clone(),
            &cfg.key_namespace,
        );
        Ok(Self {
            cfg,
            ring: Arc::new(RwLock::new(ring)),
            my_info: Arc::new(RwLock::new(my)),
            redis: redis.clone(),
            http,
            snapshot: snap,
            store,
            metadata,
        })
    }
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

// ── Main ───────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cfg = Config::from_env();
    let redis = redis::Client::open(cfg.redis_url.as_str())?;

    info!(
        "Node {} starting — prefix [{}, {}), redis: {}",
        cfg.node_id, cfg.prefix_start, cfg.prefix_end, cfg.redis_url
    );
    info!("Seed peers: {:?}", cfg.seed_peers);
    if !cfg.snapshot_path.is_empty() {
        info!(
            "Snapshot store: {} (every {}s)",
            cfg.snapshot_path, cfg.snapshot_interval_secs
        );
    }

    let state = AppState::new(cfg.clone(), redis)?;

    // ── Restore from snapshot if Redis is empty (e.g. new node after failure)
    if let Some(snap) = &state.snapshot {
        match restore_from_snapshot(&state, snap).await {
            Ok(true) => {}
            Ok(false) => info!("No snapshot restore needed (Redis has data or snapshot is empty)"),
            Err(e) => tracing::warn!("Snapshot restore failed (continuing cold): {e}"),
        }
    }

    // Gossip loop
    let gossip_state = state.clone();
    tokio::spawn(async move { gossip_loop(gossip_state).await });

    // Metrics loop
    let metrics_state = state.clone();
    tokio::spawn(async move { metrics_loop(metrics_state).await });

    // Snapshot loop — disabled when SNAPSHOT_PATH is empty
    if state.snapshot.is_some() {
        let snap_state = state.clone();
        tokio::spawn(async move { snapshot_loop(snap_state).await });
    }

    // written_at ZSET pruning loop — cleans up scores for expired entity keys
    let prune_state = state.clone();
    tokio::spawn(async move { prune_loop(prune_state).await });

    // ── gRPC server (dedicated port = http_port + 10) ─────────────────────
    {
        let grpc_state = state.clone();
        let grpc_port = cfg.grpc_port;
        tokio::spawn(async move {
            if let Err(e) = grpc::serve(grpc_state, grpc_port).await {
                tracing::error!("gRPC server error: {e}");
            }
        });
    }

    // ── HTTP server ───────────────────────────────────────────────────────
    // Write endpoints are protected by optional API-key auth.
    let write_routes = Router::new()
        .route("/ingest", post(route_ingest_batch))
        .route("/ingest-snapshot", post(route_ingest_snapshot))
        .route("/split", post(route_trigger_split))
        .route("/merge", post(route_trigger_merge))
        .route("/assign-range", put(route_assign_range))
        .route("/entity/:id", delete(route_delete_entity))
        .route_layer(middleware::from_fn_with_state(state.clone(), api_key_guard));

    let app = Router::new()
        .route("/state", get(route_get_state))
        .route("/cluster", get(route_get_cluster))
        .route("/health", get(route_health))
        .route("/gossip", post(route_receive_gossip))
        .route("/probe", post(route_probe))
        .route("/metrics", get(route_metrics))
        .route("/metrics/prom", get(route_metrics_prometheus))
        .route("/trace", get(route_trace))
        .route("/delta-sync", get(route_delta_sync)) // read-only, no auth
        .merge(write_routes)
        .layer(CorsLayer::permissive())
        .with_state(state);

    let addr = format!("0.0.0.0:{}", cfg.http_port);
    info!("Listening on http://{addr}");
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

// ── written_at prune loop ─────────────────────────────────────────────────

/// Periodically removes entries from the `written_at` sorted set whose
/// entity keys have expired in Redis.  Prevents unbounded ZSET growth.
async fn prune_loop(state: AppState) {
    // Wait two full TTL cycles on startup so we don't race with initial ingest.
    let interval = Duration::from_secs(state.cfg.entity_ttl_secs * 2);
    tokio::time::sleep(interval).await;
    loop {
        match state.store.prune_written_at().await {
            Ok(n) if n > 0 => tracing::info!("prune_written_at: removed {} stale entries", n),
            Ok(_) => {}
            Err(e) => tracing::warn!("prune_written_at failed: {e}"),
        }
        tokio::time::sleep(interval).await;
    }
}

// ── Snapshot loop ──────────────────────────────────────────────────────────

async fn snapshot_loop(state: AppState) {
    loop {
        tokio::time::sleep(Duration::from_secs(state.cfg.snapshot_interval_secs)).await;
        if let Some(snap) = &state.snapshot {
            match take_snapshot(&state, snap).await {
                Ok(n) => tracing::info!("Snapshot: {} entities saved", n),
                Err(e) => tracing::error!("Snapshot failed: {e}"),
            }
        }
    }
}

/// Scan the local Redis and persist everything to SQLite.
async fn take_snapshot(state: &AppState, snap: &snapshot::Snapshot) -> anyhow::Result<u64> {
    use redis::AsyncCommands;
    let mut conn = state.redis.get_multiplexed_async_connection().await?;
    let mut entries = Vec::new();
    let mut cursor = 0u64;

    loop {
        let (new_cur, keys): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(state.store.k_entity_pattern())
            .arg("COUNT")
            .arg(200)
            .query_async(&mut conn)
            .await?;

        for key in keys {
            if let Ok(Some(json)) = conn.get::<_, Option<String>>(&key).await {
                if let Ok(entry) = serde_json::from_str::<proxima::GeoEntry>(&json) {
                    let token = cell_token(entry.lat, entry.lon, state.cfg.s2_level);
                    entries.push(snapshot::SnapshotEntry {
                        id: entry.id,
                        json,
                        token,
                        snapshotted: unix_now() as i64,
                    });
                }
            }
        }

        cursor = new_cur;
        if cursor == 0 {
            break;
        }
    }

    snap.save(entries).await
}

/// If Redis is empty AND we have a snapshot, restore from it.
/// Returns true if a restore was performed.
async fn restore_from_snapshot(
    state: &AppState,
    snap: &snapshot::Snapshot,
) -> anyhow::Result<bool> {
    let snap_count = snap.count().await?;
    if snap_count == 0 {
        tracing::info!("Snapshot store is empty — cold start");
        return Ok(false);
    }

    let mut conn = state.redis.get_multiplexed_async_connection().await?;
    let redis_count: u64 = redis::cmd("DBSIZE").query_async(&mut conn).await?;
    if redis_count > 0 {
        tracing::info!("Redis has {} keys — snapshot restore skipped", redis_count);
        return Ok(false);
    }

    let entries = snap.load().await?;
    let now = unix_now() as i64;
    let ttl_i = state.cfg.entity_ttl_secs as i64;

    // Filter out entries that would have expired under the configured TTL.
    // An entity snapshotted at T with TTL=600s should not be restored if
    // now > T + 600 — it would have been evicted from Redis by then anyway.
    let (valid, expired): (Vec<_>, Vec<_>) = entries
        .into_iter()
        .partition(|e| e.snapshotted + ttl_i > now);

    if !expired.is_empty() {
        tracing::info!(
            "Snapshot restore: skipping {} expired entries (TTL={}s)",
            expired.len(),
            ttl_i
        );
    }

    let n = valid.len();
    tracing::info!("Redis is empty — restoring {} entities from snapshot", n);

    // Convert SnapshotEntries → GeoEntries and use the lib's merge_entries
    // so the written_at index is maintained on restore too.
    let geo_entries: Vec<proxima::GeoEntry> = valid
        .iter()
        .filter_map(|e| serde_json::from_str::<proxima::GeoEntry>(&e.json).ok())
        .collect();
    state
        .store
        .merge_entries(&geo_entries, state.cfg.s2_level)
        .await
        .map_err(|e| anyhow::anyhow!("Snapshot restore merge failed: {e}"))?;

    tracing::info!(
        "Snapshot restore complete: {} entities loaded into Redis",
        n
    );
    Ok(true)
}

async fn gossip_loop(state: AppState) {
    loop {
        tokio::time::sleep(Duration::from_secs(state.cfg.gossip_interval_secs)).await;

        let my_info = state.my_info.read().await.clone();
        let suspect_secs = state.cfg.suspect_secs;
        let dead_secs = state.cfg.dead_secs;
        let peers: Vec<String> = {
            let ring = state.ring.read().await;
            ring.all_nodes()
                .filter(|n| n.node_id != my_info.node_id)
                .filter(|n| n.status != NodeStatus::Dead)
                .map(|n| n.addr.clone())
                .chain(state.cfg.seed_peers.iter().cloned())
                .collect::<std::collections::HashSet<_>>()
                .into_iter()
                .collect()
        };

        // Choose K random peers
        let targets: Vec<String> = {
            let mut rng = rand::thread_rng();
            let mut p = peers;
            p.shuffle(&mut rng);
            p.into_iter().take(2).collect() // fanout = 2 peers per cycle
        };

        for peer in targets {
            let url = format!("http://{peer}/gossip");
            match state.http.post(&url).json(&my_info).send().await {
                Ok(resp) => {
                    if let Ok(their_state) = resp.json::<NodeInfo>().await {
                        let now = unix_now();
                        let mut ring = state.ring.write().await;
                        ring.merge(their_state);
                        // Update last_seen in the same lock acquisition
                        for n in ring.all_nodes().cloned().collect::<Vec<_>>() {
                            if n.addr == peer {
                                let mut updated = n.clone();
                                updated.last_seen_secs = now;
                                ring.merge(updated);
                            }
                        }
                    }
                }
                Err(_) => {
                    // ── SWIM indirect ping ────────────────────────────────
                    // Direct gossip failed.  Before escalating to Suspect/Dead,
                    // ask up to 2 other Active nodes to probe the target.
                    // Only escalate if the indirect probes ALSO fail — this
                    // prevents false positives from transient one-hop drops.
                    let now = unix_now();

                    let probers: Vec<String> = {
                        let ring = state.ring.read().await;
                        ring.all_nodes()
                            .filter(|n| {
                                n.addr != peer
                                    && n.node_id != my_info.node_id
                                    && n.status == NodeStatus::Active
                            })
                            .map(|n| n.addr.clone())
                            .take(2)
                            .collect()
                    };

                    let mut reachable_via_proxy = false;
                    for prober in &probers {
                        if let Ok(resp) = state
                            .http
                            .post(format!("http://{prober}/probe"))
                            .json(&serde_json::json!({ "target": peer }))
                            .timeout(Duration::from_secs(2))
                            .send()
                            .await
                        {
                            if resp.json::<bool>().await.unwrap_or(false) {
                                reachable_via_proxy = true;
                                tracing::debug!(
                                    "Indirect probe: {} is reachable via {}",
                                    peer,
                                    prober
                                );
                                break;
                            }
                        }
                    }

                    if reachable_via_proxy {
                        // Peer reachable via proxy — likely a one-hop network
                        // blip, not a real failure.  Do not escalate this cycle.
                    } else {
                        // Neither direct nor indirect ping succeeded.
                        let mut ring = state.ring.write().await;
                        for n in ring.all_nodes().cloned().collect::<Vec<_>>() {
                            if n.addr == peer {
                                let age = now.saturating_sub(n.last_seen_secs);
                                if age > dead_secs && n.status != NodeStatus::Dead {
                                    let mut dead = n.clone();
                                    dead.status = NodeStatus::Dead;
                                    dead.generation += 1;
                                    tracing::warn!(
                                        "Node {} marked DEAD ({}s, direct+indirect probes failed)",
                                        n.node_id,
                                        age
                                    );
                                    ring.merge(dead);
                                } else if age > suspect_secs && n.status == NodeStatus::Active {
                                    let mut suspect = n.clone();
                                    suspect.status = NodeStatus::Suspect;
                                    suspect.generation += 1;
                                    tracing::warn!(
                                        "Node {} marked SUSPECT ({}s, direct+indirect probes failed)",
                                        n.node_id, age
                                    );
                                    ring.merge(suspect);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

// ── Metrics loop ───────────────────────────────────────────────────────────

async fn metrics_loop(state: AppState) {
    loop {
        tokio::time::sleep(Duration::from_secs(10)).await;

        if let Ok(mut conn) = state.redis.get_multiplexed_async_connection().await {
            // Count aircraft keys
            let key_count: u64 = redis::cmd("DBSIZE")
                .query_async(&mut conn)
                .await
                .unwrap_or(0);

            // Get memory usage
            let info: String = redis::cmd("INFO")
                .arg("memory")
                .query_async(&mut conn)
                .await
                .unwrap_or_default();
            let mem_bytes: u64 = info
                .lines()
                .find(|l| l.starts_with("used_memory:"))
                .and_then(|l| l.split(':').nth(1))
                .and_then(|v| v.trim().parse().ok())
                .unwrap_or(0);

            let mut my = state.my_info.write().await;
            my.key_count = key_count;
            my.mem_bytes = mem_bytes;
            my.last_seen_secs = unix_now();
            my.generation += 1;

            state.ring.write().await.merge(my.clone());
        }
    }
}

// ── HTTP handlers ──────────────────────────────────────────────────────────

async fn route_health() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "ok": true }))
}

async fn route_get_state(State(s): State<AppState>) -> Json<NodeInfo> {
    Json(s.my_info.read().await.clone())
}

async fn route_get_cluster(State(s): State<AppState>) -> Json<Vec<NodeInfo>> {
    Json(s.ring.read().await.as_vec())
}

async fn route_metrics(State(s): State<AppState>) -> Json<serde_json::Value> {
    let my = s.my_info.read().await.clone();

    let snap_info = if let Some(snap) = &s.snapshot {
        match snap.last_snapshot_info().await {
            Ok(Some((count, dur_ms, ts))) => serde_json::json!({
                "entities":    count,
                "duration_ms": dur_ms,
                "captured_at": ts,
                "path":        s.cfg.snapshot_path,
                "interval_secs": s.cfg.snapshot_interval_secs,
            }),
            _ => serde_json::json!({ "status": "no snapshot yet" }),
        }
    } else {
        serde_json::json!({ "status": "disabled — set SNAPSHOT_PATH to enable" })
    };

    Json(serde_json::json!({
        "node_id":      my.node_id,
        "prefix":       format!("[{}, {})", my.prefix_start, my.prefix_end),
        "key_count":    my.key_count,
        "mem_mb":       my.mem_bytes / 1_048_576,
        "status":       my.status,
        "entity_ttl_secs": s.cfg.entity_ttl_secs,
        "snapshot":     snap_info,
    }))
}

/// Receive a gossip push from another node.
/// Returns our own current state so the caller can merge it too.
async fn route_receive_gossip(
    State(s): State<AppState>,
    Json(node): Json<NodeInfo>,
) -> Json<NodeInfo> {
    s.ring.write().await.merge(node);
    Json(s.my_info.read().await.clone())
}

// ── SWIM indirect probe ───────────────────────────────────────────────────

#[derive(Deserialize)]
struct ProbeRequest {
    /// HTTP addr (host:port) of the node to health-check on behalf of the caller.
    target: String,
}

/// POST /probe  — SWIM indirect ping relay.
///
/// When a node fails its direct gossip to peer P it asks other nodes to probe P.
/// Returns `true` if this node can reach P, `false` otherwise.
/// No state is mutated — purely a reachability test for the caller.
async fn route_probe(State(s): State<AppState>, Json(req): Json<ProbeRequest>) -> Json<bool> {
    let reachable = s
        .http
        .get(format!("http://{}/health", req.target))
        .timeout(Duration::from_secs(2))
        .send()
        .await
        .map(|r| r.status().is_success())
        .unwrap_or(false);
    Json(reachable)
}

// ── Split ──────────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct SplitRequest {
    /// HTTP addr of the target node that will receive half the data
    target: String,
    /// Optional explicit split point. If absent, computed as median occupied prefix.
    split_point: Option<String>,
}

#[derive(Serialize)]
struct SplitResponse {
    migrated_keys: u64,
    split_point: String,
    new_prefix_end: String,
}

/// POST /split  — migrates keys >= split_point to the target node,
/// then updates both nodes' prefix ranges via gossip.
async fn route_trigger_split(
    State(s): State<AppState>,
    Json(req): Json<SplitRequest>,
) -> Result<Json<SplitResponse>, (StatusCode, String)> {
    let err = |e: anyhow::Error| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string());

    // Determine split point
    let split_point = match req.split_point {
        Some(sp) => sp,
        None => find_median_split(&s).await.map_err(err)?,
    };

    info!("Splitting at '{}' → target {}", split_point, req.target);

    // Mark ourselves as splitting so readers know to check both nodes
    {
        let mut my = s.my_info.write().await;
        my.status = NodeStatus::Splitting;
        my.generation += 1;
        s.ring.write().await.merge(my.clone());
    }

    // Record the delta watermark before copying. The source remains the sole
    // owner until the target has copied this snapshot and caught up from it.
    let snapshot_ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    let migrated = match migrate_keys(&s, &req.target, &split_point).await {
        Ok(migrated) => migrated,
        Err(error) => return Err(split_failed(&s, error).await),
    };

    let old_end = s.my_info.read().await.prefix_end.clone();

    // Tell the target its new range: [split_point, old_end)
    // while this node continues serving the original range. A failed
    // assignment therefore leaves no routing hole and no lost source data.
    let assignment = cluster_request(
        &s,
        s.http
            .put(format!("http://{}/assign-range", req.target))
            .json(&AssignRangeRequest {
                prefix_start: split_point.clone(),
                prefix_end: old_end.clone(),
                source_addr: Some(s.cfg.http_addr.clone()),
                snapshot_timestamp: Some(snapshot_ts),
            }),
    )
    .send()
    .await
    .and_then(|response| response.error_for_status());
    if let Err(error) = assignment {
        return Err(split_failed(&s, error.into()).await);
    }

    if let Err(error) = wait_for_target_active(&s, &req.target, &split_point, &old_end).await {
        return Err(split_failed(&s, error).await);
    }

    // The target has acknowledged its range and completed delta-sync. Cleanup
    // happens before contracting this node's route; failure leaves duplicate
    // data but preserves availability and can be retried safely.
    remove_migrated_keys(&s, &split_point).await.map_err(err)?;
    {
        let mut my = s.my_info.write().await;
        my.prefix_end = split_point.clone();
        my.status = NodeStatus::Active;
        my.generation += 1;
        s.ring.write().await.merge(my.clone());
    }

    info!(
        "Split complete: migrated {} keys to {}",
        migrated, req.target
    );

    Ok(Json(SplitResponse {
        migrated_keys: migrated,
        split_point,
        new_prefix_end: old_end,
    }))
}

/// Return the source to its pre-split serving state. This runs only before
/// cleanup starts; its entities and full range are still present at this point.
async fn split_failed(s: &AppState, error: anyhow::Error) -> (StatusCode, String) {
    let mut my = s.my_info.write().await;
    my.status = NodeStatus::Active;
    my.generation += 1;
    s.ring.write().await.merge(my.clone());
    (StatusCode::BAD_GATEWAY, error.to_string())
}

// ── Merge ──────────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct MergeRequest {
    /// HTTP address of the adjacent shard to absorb (e.g. "localhost:4003").
    absorb: String,
}

/// POST /merge — absorb an adjacent shard into this one.
///
/// Merge is the inverse of split:
///   1. Mark self as Merging.
///   2. Fetch ALL entities from the target shard via /delta-sync?since_ms=0.
///   3. Absorb them with store.merge_entries() (freshness-safe, idempotent).
///   4. Extend this shard's prefix range to cover the target's range too.
///   5. Tell the target to reset to Standby (empty prefix range).
async fn route_trigger_merge(
    State(s): State<AppState>,
    Json(req): Json<MergeRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let err = |e: anyhow::Error| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string());
    let target = req.absorb.trim_start_matches("http://").to_string();

    info!("Merging: absorbing {} into this shard", target);

    // 1. Mark self as Merging
    {
        let mut my = s.my_info.write().await;
        my.status = NodeStatus::Merging;
        my.generation += 1;
        s.ring.write().await.merge(my.clone());
    }

    // 2. Fetch all entities from target (since_ms=0 → everything)
    let delta_url = format!("http://{target}/delta-sync?since_ms=0");
    let entities: Vec<GeoEntry> = s
        .http
        .get(&delta_url)
        .timeout(Duration::from_secs(30))
        .send()
        .await
        .map_err(|e| err(e.into()))?
        .json()
        .await
        .map_err(|e| err(e.into()))?;

    let n = entities.len();
    info!("Absorbing {} entities from {}", n, target);

    // 3. Merge into this shard using the lib's freshness-ordered primitive
    s.store
        .merge_entries(&entities, s.cfg.s2_level)
        .await
        .map_err(|e| err(e.into()))?;

    let target_prefix_end = {
        // Collect first so the RwLockReadGuard can be dropped cleanly
        let all: Vec<NodeInfo> = s.ring.read().await.as_vec();
        all.into_iter()
            .find(|n| n.addr.trim_start_matches("http://") == target.as_str() || n.addr == target)
            .map(|n| n.prefix_end)
            .unwrap_or_default()
    };

    {
        let mut my = s.my_info.write().await;
        my.prefix_end = target_prefix_end.clone();
        my.status = NodeStatus::Active;
        my.generation += 1;
        s.ring.write().await.merge(my.clone());
    }

    info!(
        "Merged range now [{}, {})",
        s.my_info.read().await.prefix_start,
        target_prefix_end
    );

    // 5. Reset target to Standby (empty prefix = no responsibility)
    let _ = s
        .http
        .put(format!("http://{target}/assign-range"))
        .json(&AssignRangeRequest {
            prefix_start: String::new(),
            prefix_end: String::new(),
            source_addr: None,
            snapshot_timestamp: None,
        })
        .send()
        .await;

    info!("Merge complete: absorbed {n} entities, target {target} reset to Standby");

    Ok(Json(serde_json::json!({
        "absorbed_entities": n,
        "new_range":         format!("[{}, {})", s.my_info.read().await.prefix_start, target_prefix_end),
    })))
}

/// Find the token prefix that splits current keys roughly in half.
async fn find_median_split(s: &AppState) -> Result<String> {
    let mut conn = s.redis.get_multiplexed_async_connection().await?;
    let mut prefix_counts: std::collections::BTreeMap<String, u64> = Default::default();
    let mut cursor = 0u64;

    loop {
        let (new_cur, keys): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(s.store.k_entity_pattern())
            .arg("COUNT")
            .arg(200)
            .query_async(&mut conn)
            .await?;

        for key in keys {
            if let Ok(Some(json)) = conn.get::<_, Option<String>>(&key).await {
                if let Ok(entry) = serde_json::from_str::<GeoEntry>(&json) {
                    let token = cell_token(entry.lat, entry.lon, s.cfg.s2_level);
                    // Use first 2 chars as the partition key
                    let prefix = token.chars().take(2).collect::<String>();
                    *prefix_counts.entry(prefix).or_insert(0) += 1;
                }
            }
        }

        cursor = new_cur;
        if cursor == 0 {
            break;
        }
    }

    let total: u64 = prefix_counts.values().sum();
    let mut cumulative = 0u64;
    let mut split_at = String::new();
    for (prefix, count) in &prefix_counts {
        cumulative += count;
        if cumulative >= total / 2 {
            split_at = prefix.clone();
            break;
        }
    }

    Ok(split_at)
}

/// Migrate all entity + cell keys with token >= split_point to the target node.
///
/// Copying is idempotent and intentionally leaves the source intact:
///   Phase 1 — Scan and collect matching keys (read-only, nothing deleted yet).
///   Phase 2 — For each chunk: POST to target and confirm persistence.
///
/// The source is cleaned only after the target has completed bootstrap, so a
/// failed assignment or delta-sync cannot create an unowned routing range.
async fn migrate_keys(s: &AppState, target: &str, split_point: &str) -> Result<u64> {
    let mut conn = s.redis.get_multiplexed_async_connection().await?;

    // ── Phase 1: Collect matching entity keys (no mutations yet) ──────────
    let mut to_migrate: Vec<(String, GeoEntry)> = Vec::new();
    let mut cursor = 0u64;

    loop {
        let (new_cur, keys): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(s.store.k_entity_pattern())
            .arg("COUNT")
            .arg(200)
            .query_async(&mut conn)
            .await?;

        for key in keys {
            if let Ok(Some(json)) = conn.get::<_, Option<String>>(&key).await {
                if let Ok(entry) = serde_json::from_str::<GeoEntry>(&json) {
                    let token = cell_token(entry.lat, entry.lon, s.cfg.s2_level);
                    if token.as_str() >= split_point {
                        to_migrate.push((key, entry));
                    }
                }
            }
        }

        cursor = new_cur;
        if cursor == 0 {
            break;
        }
    }

    let total = to_migrate.len() as u64;
    info!(
        "Split: collected {} entities to migrate to {}",
        total, target
    );

    // ── Phase 2: For each chunk — build snapshot entries and deliver them.
    //
    //   /ingest-snapshot writes to the target's SQLite first, then Redis.
    //   If the target crashes after acknowledging, it can self-restore from
    //   its snapshot on restart — no re-split required.
    for chunk in to_migrate.chunks(100) {
        let snap_entries: Vec<snapshot::SnapshotEntry> = chunk
            .iter()
            .map(|(_, entry)| snapshot::SnapshotEntry {
                id: entry.id.clone(),
                json: serde_json::to_string(entry).unwrap_or_default(),
                token: cell_token(entry.lat, entry.lon, s.cfg.s2_level),
                snapshotted: unix_now() as i64,
            })
            .collect();

        // Prefer /ingest-snapshot; fall back to /ingest for older nodes.
        let resp = cluster_request(
            s,
            s.http
                .post(format!("http://{target}/ingest-snapshot"))
                .json(&snap_entries),
        )
        .send()
        .await?;

        if !resp.status().is_success() {
            anyhow::bail!(
                "Target {} rejected /ingest-snapshot ({}); split aborted — source keys intact",
                target,
                resp.status()
            );
        }
    }

    Ok(total)
}

fn cluster_request(state: &AppState, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
    if state.cfg.api_key.is_empty() {
        request
    } else {
        request.header("x-api-key", &state.cfg.api_key)
    }
}

async fn wait_for_target_active(
    state: &AppState,
    target: &str,
    prefix_start: &str,
    prefix_end: &str,
) -> Result<()> {
    const ATTEMPTS: usize = 60;
    for _ in 0..ATTEMPTS {
        if let Ok(response) = state
            .http
            .get(format!("http://{target}/state"))
            .send()
            .await
        {
            if response.status().is_success() {
                if let Ok(node) = response.json::<NodeInfo>().await {
                    if node.status == NodeStatus::Active
                        && node.prefix_start == prefix_start
                        && node.prefix_end == prefix_end
                    {
                        return Ok(());
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    anyhow::bail!("target {target} did not become Active for range [{prefix_start}, {prefix_end})")
}

async fn remove_migrated_keys(s: &AppState, split_point: &str) -> Result<()> {
    let mut conn = s.redis.get_multiplexed_async_connection().await?;
    let mut cursor = 0u64;
    loop {
        let (next_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(s.store.k_entity_pattern())
            .arg("COUNT")
            .arg(200)
            .query_async(&mut conn)
            .await?;
        for key in keys {
            if let Ok(Some(json)) = conn.get::<_, Option<String>>(&key).await {
                if let Ok(entry) = serde_json::from_str::<GeoEntry>(&json) {
                    if cell_token(entry.lat, entry.lon, s.cfg.s2_level).as_str() >= split_point {
                        conn.del::<_, ()>(key).await?;
                        conn.del::<_, ()>(s.store.k_location(&entry.id)).await?;
                    }
                }
            }
        }
        cursor = next_cursor;
        if cursor == 0 {
            break;
        }
    }

    cursor = 0;
    let cell_prefix = format!("{{{}}}:cell:", s.store.key_prefix());
    loop {
        let (next_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(s.store.k_cell_pattern())
            .arg("COUNT")
            .arg(200)
            .query_async(&mut conn)
            .await?;
        for key in keys {
            if key.trim_start_matches(&cell_prefix) >= split_point {
                conn.del::<_, ()>(key).await?;
            }
        }
        cursor = next_cursor;
        if cursor == 0 {
            break;
        }
    }
    Ok(())
}

/// Retained for backward compatibility with older nodes that don't support
/// /ingest-snapshot. New splits exclusively use /ingest-snapshot.
#[allow(dead_code)]
async fn post_ingest(s: &AppState, target: &str, entries: Vec<GeoEntry>) -> Result<()> {
    s.http
        .post(format!("http://{target}/ingest"))
        .json(&entries)
        .send()
        .await?;
    Ok(())
}

// ── Ingest (receive migrated keys) ────────────────────────────────────────
//
// Uniqueness guarantee: each entity ID exists in exactly ONE cell at all times.
// On every write, we check {prefix}:location:{id} for the entity's previous
// cell token. If it has moved to a new cell, we SREM it from the old cell
// immediately — no TTL dependency.

async fn route_ingest_batch(
    State(s): State<AppState>,
    Json(entries): Json<Vec<GeoEntry>>,
) -> StatusCode {
    use redis::AsyncCommands;

    // A Bootstrapping node is catching up from snapshot and must not accept
    // new writes until it has transitioned to Active.
    {
        let my = s.my_info.read().await;
        if my.status == NodeStatus::Bootstrapping {
            tracing::warn!("Rejected ingest — node is still Bootstrapping");
            return StatusCode::SERVICE_UNAVAILABLE;
        }
    }

    let Ok(mut conn) = s.redis.get_multiplexed_async_connection().await else {
        return StatusCode::SERVICE_UNAVAILABLE;
    };

    let ttl = s.cfg.entity_ttl_secs;
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    let written_at_key = s.store.k_written_at();

    // Snapshot this node's range once (avoid repeated lock acquisitions).
    let (pfx_start, pfx_end) = {
        let my = s.my_info.read().await;
        (my.prefix_start.clone(), my.prefix_end.clone())
    };

    for mut entry in entries.into_iter() {
        let new_token = cell_token(entry.lat, entry.lon, s.cfg.s2_level);

        let in_range = (pfx_start.is_empty() || new_token.as_str() >= pfx_start.as_str())
            && (pfx_end.is_empty() || new_token.as_str() < pfx_end.as_str());
        if !in_range {
            tracing::warn!(
                "Rejecting entity {} (token {}) — not in my range [{}, {})",
                entry.id,
                new_token,
                pfx_start,
                pfx_end
            );
            return StatusCode::CONFLICT;
        }

        if entry.written_at == 0 {
            entry.written_at = now_ms;
        }

        let ak = s.store.k_entity(&entry.id);
        let new_ck = s.store.k_cell(&new_token);
        let loc_key = s.store.k_location(&entry.id);
        let json = serde_json::to_string(&entry).unwrap_or_default();

        if let Ok(Some(old_token)) = conn.get::<_, Option<String>>(&loc_key).await {
            if old_token != new_token {
                let old_ck = s.store.k_cell(&old_token);
                let _: () = conn.srem(&old_ck, &entry.id).await.unwrap_or(());
                let remaining: u64 = conn.scard(&old_ck).await.unwrap_or(1);
                if remaining == 0 {
                    let _: () = conn.del(&old_ck).await.unwrap_or(());
                }
                tracing::debug!("Entity {} moved: cell {old_token} → {new_token}", entry.id);
            }
        }

        let mut pipe = redis::pipe();
        pipe.set_ex(&ak, &json, ttl)
            .ignore()
            .sadd(&new_ck, &entry.id)
            .ignore()
            .set_ex(&loc_key, &new_token, ttl)
            .ignore()
            .cmd("ZADD")
            .arg(&written_at_key)
            .arg(entry.written_at as f64)
            .arg(entry.id.as_str())
            .ignore();
        let _: () = pipe.query_async(&mut conn).await.unwrap_or(());
    }

    StatusCode::OK
}

#[derive(Deserialize, Serialize)]
struct IngestCellRequest {
    token: String,
    ids: Vec<String>,
}

// Route exists for cell index migration — just add to the cell set
#[allow(dead_code)]
async fn route_ingest_cell(
    State(s): State<AppState>,
    Json(req): Json<IngestCellRequest>,
) -> StatusCode {
    if let Ok(mut conn) = s.redis.get_multiplexed_async_connection().await {
        let key = s.store.k_cell(&req.token);
        for id in &req.ids {
            let _: () = conn.sadd(&key, id).await.unwrap_or(());
        }
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

/// POST /ingest-snapshot — receive migrated entities from a splitting shard.
///
/// The source shard calls this instead of `/ingest` during a split so that
/// the new branch is seeded from a durable snapshot rather than ephemeral
/// in-memory state.  The two-step sequence is:
///
///   1. **Persist first** — entries are appended to this node's SQLite
///      snapshot before touching Redis.  If this node crashes immediately
///      after this call, it will auto-restore from the snapshot on restart
///      without requiring a re-split.
///
///   2. **Restore to Redis** — entities are written to Redis so the shard
///      becomes queryable immediately without waiting for the next restart.
///
/// Falls back gracefully if snapshotting is disabled (`SNAPSHOT_PATH` not
/// set): entries are still written to Redis directly.
async fn route_ingest_snapshot(
    State(s): State<AppState>,
    Json(entries): Json<Vec<snapshot::SnapshotEntry>>,
) -> StatusCode {
    let n = entries.len();
    tracing::info!("ingest-snapshot: receiving {} migrated entities", n);

    // 1. Persist to SQLite snapshot (durable write-ahead)
    if let Some(snap) = &s.snapshot {
        if let Err(e) = snap
            .append(
                entries
                    .iter()
                    .map(|e| snapshot::SnapshotEntry {
                        id: e.id.clone(),
                        json: e.json.clone(),
                        token: e.token.clone(),
                        snapshotted: e.snapshotted,
                    })
                    .collect(),
            )
            .await
        {
            tracing::error!("Snapshot append failed during split ingest: {e}");
            return StatusCode::INTERNAL_SERVER_ERROR;
        }
    }

    // 2. Merge into Redis using the lib's freshness-ordered primitive.
    //    This is the same path used for delta-sync catch-up — idempotent
    //    and safe to retry.
    let geo_entries: Vec<proxima::GeoEntry> = entries
        .iter()
        .filter_map(|e| serde_json::from_str::<proxima::GeoEntry>(&e.json).ok())
        .collect();
    if let Err(e) = s.store.merge_entries(&geo_entries, s.cfg.s2_level).await {
        tracing::error!("Redis merge failed during split ingest: {e}");
        return StatusCode::INTERNAL_SERVER_ERROR;
    }

    tracing::info!(
        "ingest-snapshot: {} entities written to snapshot + Redis",
        n
    );
    StatusCode::OK
}

// ── Delta sync (for bootstrapping shards) ─────────────────────────────────

#[derive(Deserialize)]
struct DeltaSyncParams {
    since_ms: u64,
}

/// GET /delta-sync?since_ms=T
///
/// Returns all entities in this node's prefix range that were written
/// (or updated) after timestamp T (Unix milliseconds).
///
/// Called by a newly bootstrapped shard to catch up on writes that
/// happened AFTER its snapshot was captured, before it goes Active.
async fn route_delta_sync(
    State(s): State<AppState>,
    Query(p): Query<DeltaSyncParams>,
) -> Json<Vec<GeoEntry>> {
    let (prefix_start, prefix_end) = {
        let my = s.my_info.read().await;
        (my.prefix_start.clone(), my.prefix_end.clone())
    };
    match s
        .store
        .entities_written_after(p.since_ms, &prefix_start, &prefix_end)
        .await
    {
        Ok(entries) => {
            tracing::info!(
                "delta-sync: {} entities written after {}ms in [{}, {})",
                entries.len(),
                p.since_ms,
                prefix_start,
                prefix_end
            );
            Json(entries)
        }
        Err(e) => {
            tracing::error!("delta-sync query failed: {e}");
            Json(vec![])
        }
    }
}

// ── Assign range (from a splitting node) ─────────────────────────────────

#[derive(Deserialize, Serialize)]
struct AssignRangeRequest {
    prefix_start: String,
    prefix_end: String,
    /// HTTP address of the source shard — used for delta-sync catch-up.
    source_addr: Option<String>,
    /// Unix ms timestamp of the snapshot seed. Bounds the delta-sync query.
    snapshot_timestamp: Option<u64>,
}

/// PUT /assign-range — called by the splitting shard to hand off a token range.
///
/// Transitions to `Bootstrapping` and spawns a background task that:
///   1. Requests entities written after `snapshot_timestamp` from the source shard
///   2. Applies each with a freshness check (`written_at` comparison)
///   3. Transitions to `Active` and gossips the new status
async fn route_assign_range(
    State(s): State<AppState>,
    Json(req): Json<AssignRangeRequest>,
) -> StatusCode {
    // Empty prefix_start + prefix_end = release this shard back to Standby.
    // This is called by the absorbing node after a successful merge.
    if req.prefix_start.is_empty() && req.prefix_end.is_empty() {
        info!("Releasing range — transitioning to Standby");
        let old_start = s.my_info.read().await.prefix_start.clone();
        if !old_start.is_empty() {
            if !s.metadata.enabled() {
                tracing::error!("Cannot release range without configured etcd metadata authority");
                return StatusCode::SERVICE_UNAVAILABLE;
            }
            if let Err(error) = s.metadata.release(&old_start, &s.cfg.node_id).await {
                tracing::error!("Cannot release consensus range claim: {error}");
                return StatusCode::CONFLICT;
            }
        }
        let mut my = s.my_info.write().await;
        my.prefix_start = String::new();
        my.prefix_end = String::new();
        my.status = NodeStatus::Standby;
        my.generation += 1;
        s.ring.write().await.merge(my.clone());
        return StatusCode::OK;
    }

    // A range has one authoritative owner, decided by an etcd Raft transaction.
    // Redis replication and local locks cannot provide this guarantee under a
    // network partition, so split/merge is unavailable without this authority.
    if !s.metadata.enabled() {
        tracing::error!("Rejecting range assignment: METADATA_ETCD_ENDPOINTS is required");
        return StatusCode::SERVICE_UNAVAILABLE;
    }
    match s.metadata.claim(&req.prefix_start, &s.cfg.node_id).await {
        Ok(ClaimResult::Granted) => {}
        Ok(ClaimResult::HeldBy(holder)) if holder == s.cfg.node_id => {}
        Ok(ClaimResult::HeldBy(holder)) => {
            tracing::warn!(
                "Range claim conflict: prefix {} held by {}",
                req.prefix_start,
                holder
            );
            return StatusCode::CONFLICT;
        }
        Err(error) => {
            tracing::error!("Consensus range claim unavailable: {error}");
            return StatusCode::SERVICE_UNAVAILABLE;
        }
    }

    info!(
        "Assigned range [{}, {}); bootstrapping from {}",
        req.prefix_start,
        req.prefix_end,
        req.source_addr.as_deref().unwrap_or("unknown")
    );
    {
        let mut my = s.my_info.write().await;
        my.prefix_start = req.prefix_start.clone();
        my.prefix_end = req.prefix_end.clone();
        my.status = NodeStatus::Bootstrapping; // not yet ready for writes
        my.generation += 1;
        s.ring.write().await.merge(my.clone());
    }

    let (bs_state, src, ps, pe, ts) = (
        s.clone(),
        req.source_addr.clone(),
        req.prefix_start.clone(),
        req.prefix_end.clone(),
        req.snapshot_timestamp.unwrap_or(0),
    );
    tokio::spawn(async move {
        bootstrap_delta_sync(bs_state, src, ps, pe, ts).await;
    });
    StatusCode::OK
}

/// Background task: delta-sync catch-up then go Active.
async fn bootstrap_delta_sync(
    s: AppState,
    source_addr: Option<String>,
    prefix_start: String,
    prefix_end: String,
    since_ms: u64,
) {
    tokio::time::sleep(Duration::from_secs(3)).await; // let /ingest-snapshot settle

    let Some(src) = source_addr else {
        tracing::error!("Bootstrap: no source address; remaining Bootstrapping");
        return;
    };
    let url = format!("http://{src}/delta-sync?since_ms={since_ms}");
    tracing::info!("Bootstrap: requesting delta sync from {src} (since {since_ms} ms)");

    let delta = match s.http.get(&url).send().await {
        Ok(resp) if resp.status().is_success() => match resp.json::<Vec<GeoEntry>>().await {
            Ok(delta) => delta,
            Err(e) => {
                tracing::warn!("Bootstrap: invalid delta-sync response: {e}");
                return;
            }
        },
        Ok(resp) => {
            tracing::warn!("Bootstrap: delta sync returned {}", resp.status());
            return;
        }
        Err(e) => {
            tracing::warn!("Bootstrap: delta sync failed: {e}");
            return;
        }
    };

    tracing::info!("Bootstrap: received {} delta entries", delta.len());
    if let Err(e) = s.store.merge_entries(&delta, s.cfg.s2_level).await {
        tracing::warn!("Bootstrap: merge_entries failed: {e}");
        return;
    }

    let mut my = s.my_info.write().await;
    my.status = NodeStatus::Active;
    my.generation += 1;
    s.ring.write().await.merge(my.clone());
    info!(
        "Bootstrap complete — node [{}, {}) is Active",
        prefix_start, prefix_end
    );
}

// ── Cross-shard entity cleanup ─────────────────────────────────────────────
//
// When an entity moves to a different geographic region:
//   1. The new write lands on shard B (correct shard for new position).
//   2. The OLD entry on shard A expires via TTL (configurable via ENTITY_TTL_SECS).
//
// For most use cases, TTL-based expiry is sufficient:
//   - Aircraft update every 30s → set ENTITY_TTL_SECS=60, stale data gone in 60s.
//   - Couriers update every 5s  → set ENTITY_TTL_SECS=15.
//
// For immediate cleanup (zero-lag SLA), call DELETE /entity/:id on the old shard.
// The caller is responsible for knowing which shard held the stale data — typically
// by checking the entity's previous position from the position_history table.

#[derive(serde::Deserialize)]
struct DeleteEntityParams {
    token: Option<String>, // known S2 token for targeted SREM (faster)
}

/// DELETE /entity/:id?token=... — removes an entity from this shard's Redis immediately.
/// Used for explicit cross-shard cleanup when TTL-based expiry is too slow.
async fn route_delete_entity(
    State(s): State<AppState>,
    Path(id): Path<String>,
    Query(p): Query<DeleteEntityParams>,
) -> StatusCode {
    use redis::AsyncCommands;
    let Ok(mut conn) = s.redis.get_multiplexed_async_connection().await else {
        return StatusCode::SERVICE_UNAVAILABLE;
    };

    let entity_key = s.store.k_entity(id.as_str());
    let loc_key = s.store.k_location(id.as_str());

    if let Some(token) = p.token {
        let cell_key = s.store.k_cell(&token);
        let _: () = conn.del(&entity_key).await.unwrap_or(());
        let _: () = conn.del(&loc_key).await.unwrap_or(());
        let _: () = conn.srem(&cell_key, &id).await.unwrap_or(());
        tracing::info!("Deleted entity {id} from cell {token}");
    } else {
        let _: () = conn.del(&entity_key).await.unwrap_or(());
        let _: () = conn.del(&loc_key).await.unwrap_or(());
        let mut cursor = 0u64;
        loop {
            let (new_cur, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(s.store.k_cell_pattern())
                .arg("COUNT")
                .arg(200)
                .query_async(&mut conn)
                .await
                .unwrap_or((0, vec![]));
            for key in keys {
                let _: () = conn.srem(&key, &id).await.unwrap_or(());
            }
            cursor = new_cur;
            if cursor == 0 {
                break;
            }
        }
        tracing::info!("Deleted entity {id} (slow-path scan)");
    }

    StatusCode::NO_CONTENT
}
//
// GET /trace?lat=51.5&lon=-0.1
// Returns which shard owns that coordinate AND whether this node is it.
// Hit this endpoint on multiple nodes to see that only one claims ownership.

#[derive(Deserialize)]
struct TraceParams {
    lat: f64,
    lon: f64,
}

#[derive(Serialize)]
struct TraceResponse {
    lat: f64,
    lon: f64,
    s2_level: u8,
    s2_token: String,
    token_prefix_2: String,
    /// Which node the cluster ring says should own this token
    owning_node_id: String,
    owning_prefix_range: String,
    /// This node — proves request was answered by the right shard
    served_by: String,
    /// true only when this node is the correct owner
    is_local: bool,
    all_shards: Vec<ShardEntry>,
}

#[derive(Serialize)]
struct ShardEntry {
    node_id: String,
    prefix_range: String,
    owns_token: bool,
    status: String,
}

// ── API key guard ─────────────────────────────────────────────────────────

async fn api_key_guard(
    State(s): State<AppState>,
    req: axum::extract::Request,
    next: Next,
) -> Result<axum::response::Response, StatusCode> {
    if s.cfg.api_key.is_empty() {
        return Ok(next.run(req).await);
    }
    let provided = req
        .headers()
        .get("x-api-key")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    // Constant-time comparison prevents timing-oracle key enumeration.
    let valid: bool =
        subtle::ConstantTimeEq::ct_eq(s.cfg.api_key.as_bytes(), provided.as_bytes()).into();
    if valid {
        Ok(next.run(req).await)
    } else {
        tracing::warn!("Rejected: missing or invalid X-API-Key");
        Err(StatusCode::UNAUTHORIZED)
    }
}

// ── Prometheus text-format metrics ────────────────────────────────────────

async fn route_metrics_prometheus(State(s): State<AppState>) -> (HeaderMap, String) {
    let my = s.my_info.read().await.clone();
    let snap: Option<(u64, u64, u64)> = if let Some(snap) = &s.snapshot {
        snap.last_snapshot_info().await.ok().flatten()
    } else {
        None
    };

    let node = &my.node_id;
    let prefix = format!("[{}, {})", my.prefix_start, my.prefix_end);

    let mut out = format!(
        "# HELP geo-redis_key_count Entities in shard\n\
         # TYPE geo-redis_key_count gauge\n\
         geo-redis_key_count{{node_id=\"{node}\",prefix=\"{prefix}\"}} {}\n\
         # HELP geo-redis_mem_bytes Redis memory used\n\
         # TYPE geo-redis_mem_bytes gauge\n\
         geo-redis_mem_bytes{{node_id=\"{node}\"}} {}\n",
        my.key_count, my.mem_bytes
    );
    if let Some((count, dur_ms, ts)) = snap {
        out.push_str(&format!(
            "# TYPE geo-redis_snapshot_entities gauge\n\
             geo-redis_snapshot_entities{{node_id=\"{node}\"}} {count}\n\
             # TYPE geo-redis_snapshot_duration_ms gauge\n\
             geo-redis_snapshot_duration_ms{{node_id=\"{node}\"}} {dur_ms}\n\
             # TYPE geo-redis_snapshot_ts gauge\n\
             geo-redis_snapshot_ts{{node_id=\"{node}\"}} {ts}\n"
        ));
    }
    let mut headers = HeaderMap::new();
    headers.insert(
        "content-type",
        HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8"),
    );
    (headers, out)
}

// ── Routing trace ──────────────────────────────────────────────────────────

async fn route_trace(
    State(s): State<AppState>,
    Query(p): Query<TraceParams>,
) -> (HeaderMap, Json<TraceResponse>) {
    let token = cell_token(p.lat, p.lon, s.cfg.s2_level);
    let prefix = token.chars().take(2).collect::<String>();

    let ring = s.ring.read().await;
    let my = s.my_info.read().await;

    let (owning_id, owning_range) = ring
        .route(&token)
        .map(|n| {
            (
                n.node_id.clone(),
                format!("[{}, {})", n.prefix_start, n.prefix_end),
            )
        })
        .unwrap_or_else(|| ("unowned".into(), "—".into()));

    let is_local = my.owns(&token);

    let shards: Vec<ShardEntry> = ring
        .all_nodes()
        .map(|n| ShardEntry {
            node_id: n.node_id.clone(),
            prefix_range: format!("[{}, {})", n.prefix_start, n.prefix_end),
            owns_token: n.owns(&token),
            status: format!("{:?}", n.status),
        })
        .collect();

    let mut headers = HeaderMap::new();
    headers.insert(
        "x-served-by",
        HeaderValue::from_str(&s.cfg.node_id).unwrap(),
    );
    headers.insert("x-owning-node", HeaderValue::from_str(&owning_id).unwrap());
    headers.insert("x-s2-token", HeaderValue::from_str(&token).unwrap());
    headers.insert(
        "x-is-local",
        HeaderValue::from_static(if is_local { "true" } else { "false" }),
    );

    (
        headers,
        Json(TraceResponse {
            lat: p.lat,
            lon: p.lon,
            s2_level: s.cfg.s2_level,
            s2_token: token,
            token_prefix_2: prefix,
            owning_node_id: owning_id,
            owning_prefix_range: owning_range,
            served_by: s.cfg.node_id.clone(),
            is_local,
            all_shards: shards,
        }),
    )
}

// ── S2 helper ─────────────────────────────────────────────────────────────

pub(crate) fn cell_token(lat: f64, lon: f64, level: u8) -> String {
    use s2::{cellid::CellID, latlng::LatLng, s1};
    let ll = LatLng::new(s1::Deg(lat).into(), s1::Deg(lon).into());
    let cell = CellID::from(ll).parent(level as u64);
    let hex = format!("{:016x}", cell.0);
    hex.trim_end_matches('0').to_string()
}
pub(crate) fn viewport_tokens(
    south: f64,
    west: f64,
    north: f64,
    east: f64,
    level: u8,
) -> Vec<String> {
    use s2::{cap::Cap, latlng::LatLng, point::Point, region::RegionCoverer, s1};
    use std::f64::consts::PI;
    let clat = (south + north) / 2.0;
    let clon = (west + east) / 2.0;
    let dlat = (north - south).abs() / 2.0;
    let dlon = (east - west).abs() / 2.0;
    let rad = ((dlat * dlat + dlon * dlon).sqrt() * PI / 180.0).min(PI);
    let center = Point::from(LatLng::new(s1::Deg(clat).into(), s1::Deg(clon).into()));
    let angle: s1::angle::Angle = s1::Rad(rad).into();
    let cap = Cap::from_center_angle(&center, &angle);
    let coverer = RegionCoverer {
        min_level: level,
        max_level: level,
        level_mod: 1,
        max_cells: 500,
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
