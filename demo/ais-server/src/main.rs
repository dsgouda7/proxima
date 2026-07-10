mod aisstream;
mod config;
mod db;
mod routes;

use std::collections::HashMap;
use std::sync::Arc;
use axum::{routing::get, Router};
use tokio::sync::{mpsc, RwLock};
use tower_http::cors::CorsLayer;
use georedis::{GeoEntry, GeoTrie, Metrics, RedisStore};
use config::Config;

pub struct AppState {
    pub trie:         RwLock<GeoTrie>,
    pub store:        RedisStore,
    pub config:       Config,
    pub last_sync:    RwLock<Option<u64>>,
    pub db:           Arc<db::Db>,
    /// Latest position for every active vessel keyed by MMSI.
    /// Value is (vessel, unix_seconds_of_last_update).
    /// Entries older than entity_ttl_secs are evicted on each sync cycle.
    pub vessel_cache: RwLock<HashMap<String, (aisstream::Vessel, u64)>>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cfg = Config::from_env();

    if cfg.aisstream_api_key.is_empty() {
        tracing::warn!(
            "AISSTREAM_API_KEY is not set — vessel stream will not connect. \
             Get a free key at https://aisstream.io and add it to your .env file."
        );
    }

    let metrics  = Metrics::new();
    let store    = RedisStore::with_config(&cfg.redis_url, Arc::clone(&metrics), cfg.entity_ttl_secs)?;
    let database = Arc::new(db::Db::open(&cfg.sqlite_path)?);

    tracing::info!("Source: AISStream.io (WebSocket push)");
    tracing::info!("Redis: {}", cfg.redis_url);
    tracing::info!("SQLite: {}", cfg.sqlite_path);
    tracing::info!("S2 level: {}, trie sync: {}s", cfg.s2_level, cfg.sync_interval_secs);

    let state = Arc::new(AppState {
        trie:         RwLock::new(GeoTrie::new(cfg.s2_level)),
        store,
        config:       cfg.clone(),
        last_sync:    RwLock::new(None),
        db:           database,
        vessel_cache: RwLock::new(HashMap::new()),
    });

    // ── WebSocket receiver ────────────────────────────────────────────────
    // Channel capacity: 8 k messages — large enough to absorb burst
    // spikes while still dropping updates if the consumer falls behind.
    let (tx, mut rx) = mpsc::channel::<aisstream::Vessel>(8_192);

    // Task A: connect to AISStream and push vessels into the channel.
    // Reconnects automatically with exponential back-off on any error.
    let api_key = cfg.aisstream_api_key.clone();
    tokio::spawn(async move {
        if api_key.is_empty() {
            tracing::info!("WebSocket connector idle (no API key configured)");
            return;
        }
        let mut backoff_secs = 1u64;
        loop {
            match aisstream::stream_once(&api_key, &tx).await {
                Ok(())   => tracing::info!("AISStream disconnected cleanly — reconnecting"),
                Err(e)   => tracing::warn!("AISStream error: {e} — reconnecting in {backoff_secs}s"),
            }
            tokio::time::sleep(tokio::time::Duration::from_secs(backoff_secs)).await;
            backoff_secs = (backoff_secs * 2).min(60);
        }
    });

    // Task B: drain the channel and update the in-memory vessel cache.
    let cache_state = Arc::clone(&state);
    tokio::spawn(async move {
        while let Some(vessel) = rx.recv().await {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            cache_state.vessel_cache.write().await
                .insert(vessel.mmsi.clone(), (vessel, now));
        }
    });

    // Task C: every sync_interval_secs, snapshot the cache → trie + Redis + SQLite.
    let sync_state = Arc::clone(&state);
    tokio::spawn(async move {
        let mut prune_counter: u32 = 0;
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(
                sync_state.config.sync_interval_secs,
            ))
            .await;

            let now_secs = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            let ttl = sync_state.config.entity_ttl_secs;

            // Evict vessels not seen within entity_ttl_secs before snapshotting.
            {
                let mut cache = sync_state.vessel_cache.write().await;
                cache.retain(|_, (_, last_seen)| now_secs.saturating_sub(*last_seen) < ttl);
            }

            let snapshot: Vec<aisstream::Vessel> = sync_state
                .vessel_cache.read().await
                .values().map(|(v, _)| v.clone()).collect();

            if snapshot.is_empty() { continue; }

            let n = snapshot.len();

            // 1. Persist to SQLite
            let db_data: Vec<db::VesselData> = snapshot.iter().map(|v| db::VesselData {
                id:         v.mmsi.clone(),
                lat:        v.lat,
                lon:        v.lon,
                ship_name:  v.ship_name.clone(),
                sog:        v.sog,
                cog:        v.cog,
                heading:    v.heading,
                nav_status: v.nav_status,
            }).collect();
            if let Err(e) = sync_state.db.upsert_batch(db_data).await {
                tracing::error!("SQLite upsert failed: {e}");
            }

            // 2. Rebuild trie
            {
                let mut trie = sync_state.trie.write().await;
                trie.clear();
                for v in &snapshot {
                    trie.insert(GeoEntry {
                        id:  v.mmsi.clone(),
                        lat: v.lat,
                        lon: v.lon,
                        payload: serde_json::json!({
                            "ship_name":  v.ship_name,
                            "sog":        v.sog,
                            "cog":        v.cog,
                            "heading":    v.heading,
                            "nav_status": v.nav_status,
                        }),
                    });
                }
            }

            // 3. Persist trie to Redis
            {
                let trie = sync_state.trie.read().await;
                if let Err(e) = sync_state.store.persist_trie(&trie).await {
                    tracing::error!("Redis persist failed: {e}");
                }
            }

            let ts = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            *sync_state.last_sync.write().await = Some(ts);
            tracing::info!("Synced {n} vessels to trie + Redis (AISStream)");

            prune_counter += 1;
            if prune_counter % 12 == 0 {
                if let Err(e) = sync_state.db.prune_history().await {
                    tracing::warn!("History prune failed: {e}");
                }
            }
        }
    });

    // ── HTTP server ───────────────────────────────────────────────────────
    let app = Router::new()
        .route("/api/vessels",     get(routes::all_vessels))
        .route("/api/vessels/:id", get(routes::vessel_detail))
        .route("/api/metrics",     get(routes::get_metrics))
        .route("/health",          get(routes::health))
        // Aircraft-compatible aliases so the existing Leaflet UI can connect to this server
        .route("/api/aircraft",     get(routes::all_vessels_compat))
        .route("/api/aircraft/:id", get(routes::vessel_detail_compat))
        // /api/region returns aircraft-compat format (vessel fields mapped for the UI)
        .route("/api/region",       get(routes::region_vessels_compat))
        .layer(CorsLayer::permissive())
        .with_state(Arc::clone(&state));

    let addr = format!("{}:{}", state.config.server_host, state.config.server_port);
    tracing::info!("AISStream demo listening on http://{addr}");
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
