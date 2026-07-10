mod config;
mod db;
mod opensky;
mod routes;

use std::sync::Arc;
use axum::{routing::get, Router};
use tokio::sync::RwLock;
use tower_http::cors::CorsLayer;
use georedis::{GeoEntry, GeoTrie, Metrics, RedisStore};
use config::Config;

pub struct AppState {
    pub trie:      RwLock<GeoTrie>,
    pub store:     RedisStore,
    pub config:    Config,
    pub last_sync: RwLock<Option<u64>>,
    /// Full metadata + position history — queried on-demand for detail view
    pub db:        Arc<db::Db>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cfg     = Config::from_env();
    let metrics = Metrics::new();
    let store   = RedisStore::with_config(&cfg.redis_url, Arc::clone(&metrics), cfg.entity_ttl_secs)?;
    let database = Arc::new(db::Db::open(&cfg.sqlite_path)?);

    tracing::info!("Redis: {}", cfg.redis_url);
    tracing::info!("SQLite: {}", cfg.sqlite_path);
    tracing::info!("S2 level: {}, poll interval: {}s", cfg.s2_level, cfg.poll_interval_secs);

    let state = Arc::new(AppState {
        trie:      RwLock::new(GeoTrie::new(cfg.s2_level)),
        store,
        config:    cfg.clone(),
        last_sync: RwLock::new(None),
        db:        database,
    });

    // ── background poller ─────────────────────────────────────────────────
    let poll_state = Arc::clone(&state);
    tokio::spawn(async move {
        let http = reqwest::Client::new();
        loop {
            tracing::info!("Polling OpenSky Network…");
            match opensky::fetch_aircraft(&http).await {
                Ok(aircraft) => {
                    let n = aircraft.len();

                    // ── 1. Persist metadata + history to SQLite ────────────
                    let db_data: Vec<db::AircraftData> = aircraft.iter().map(|a| db::AircraftData {
                        id:             a.icao24.clone(),
                        lat:            a.lat,
                        lon:            a.lon,
                        callsign:       a.callsign.clone(),
                        origin_country: a.origin_country.clone(),
                        altitude:       a.altitude,
                        velocity:       a.velocity,
                        heading:        a.heading,
                        on_ground:      a.on_ground,
                    }).collect();
                    if let Err(e) = poll_state.db.upsert_batch(db_data).await {
                        tracing::error!("SQLite upsert failed: {e}");
                    }

                    // ── 2. Rebuild trie with minimal display payload (no history) ─
                    {
                        let mut trie = poll_state.trie.write().await;
                        trie.clear();
                        for a in &aircraft {
                            trie.insert(GeoEntry {
                                id:  a.icao24.clone(),
                                lat: a.lat,
                                lon: a.lon,
                                payload: serde_json::json!({
                                    "callsign":       a.callsign,
                                    "altitude":       a.altitude,
                                    "velocity":       a.velocity,
                                    "heading":        a.heading,
                                    "on_ground":      a.on_ground,
                                    "origin_country": a.origin_country,
                                    // history is intentionally omitted — fetch from SQLite on-demand
                                }),
                                written_at: 0,
                            });
                        }
                    }
                    // ── 3. Persist trie to Redis ───────────────────────────
                    {
                        let trie = poll_state.trie.read().await;
                        if let Err(e) = poll_state.store.persist_trie(&trie).await {
                            tracing::error!("Redis persist failed: {e}");
                        }
                    }
                    let ts = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs();
                    *poll_state.last_sync.write().await = Some(ts);
                    tracing::info!("Synced {n} aircraft to trie + Redis");
                }
                Err(e) => tracing::error!("OpenSky fetch failed: {e}"),
            }
            tokio::time::sleep(tokio::time::Duration::from_secs(
                poll_state.config.poll_interval_secs,
            ))
            .await;
        }
    });

    // ── HTTP server ───────────────────────────────────────────────────────
    let app = Router::new()
        .route("/api/aircraft",     get(routes::all_aircraft))
        .route("/api/aircraft/:id", get(routes::aircraft_detail))
        .route("/api/region",       get(routes::region_aircraft))
        .route("/api/metrics",      get(routes::get_metrics))
        .route("/api/health",       get(routes::health))
        .layer(CorsLayer::permissive())
        .with_state(state);

    let addr = format!("{}:{}", cfg.server_host, cfg.server_port);
    tracing::info!("Listening on http://{addr}");
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
