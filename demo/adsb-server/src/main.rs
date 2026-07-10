mod adsb;
mod config;
mod db;
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

    tracing::info!("Source: OpenSky Network (ADSB demo — separate Redis DB 1)");
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
        let mut poll_count: u32 = 0;
        loop {
            tracing::info!("Polling ADSB.fi…");
            match adsb::fetch_aircraft(&http).await {
                Ok(aircraft) => {
                    let n = aircraft.len();

                    let db_data: Vec<db::AircraftData> = aircraft.iter().map(|a| db::AircraftData {
                        id:            a.icao24.clone(),
                        lat:           a.lat,
                        lon:           a.lon,
                        callsign:      a.callsign.clone(),
                        aircraft_type: a.aircraft_type.clone(),
                        registration:  a.registration.clone(),
                        altitude:      a.altitude,
                        velocity:      a.velocity,
                        heading:       a.heading,
                        on_ground:     a.on_ground,
                    }).collect();
                    if let Err(e) = poll_state.db.upsert_batch(db_data).await {
                        tracing::error!("SQLite upsert failed: {e}");
                    }

                    {
                        let mut trie = poll_state.trie.write().await;
                        trie.clear();
                        for a in &aircraft {
                            trie.insert(GeoEntry {
                                id:  a.icao24.clone(),
                                lat: a.lat,
                                lon: a.lon,
                                payload: serde_json::json!({
                                    "callsign":      a.callsign,
                                    "altitude":      a.altitude,
                                    "velocity":      a.velocity,
                                    "heading":       a.heading,
                                    "on_ground":     a.on_ground,
                                    "aircraft_type": a.aircraft_type,
                                    "registration":  a.registration,
                                }),
                            });
                        }
                    }
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
                    tracing::info!("Synced {n} aircraft to trie + Redis (ADSB.fi)");

                    poll_count += 1;
                    if poll_count % 10 == 0 {
                        if let Err(e) = poll_state.db.prune_history().await {
                            tracing::warn!("History prune failed: {e}");
                        }
                    }
                }
                Err(e) => tracing::error!("ADSB.fi fetch failed: {e}"),
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
        .route("/health",           get(routes::health))
        .layer(CorsLayer::permissive())
        .with_state(Arc::clone(&state));

    let addr = format!("{}:{}", state.config.server_host, state.config.server_port);
    tracing::info!("ADSB.fi demo listening on http://{addr}");
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
