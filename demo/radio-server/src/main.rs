mod aggregate;
mod config;
mod radio_api;
mod routes;

use axum::{routing::get, Router};
use std::{collections::HashMap, sync::{atomic::AtomicUsize, Arc}};
use tokio::sync::RwLock;
use tower_http::cors::CorsLayer;

pub struct AppState {
    /// Pre-computed cluster tiers keyed by S2 level (2, 3, 4, 5).
    /// Level 5 entries also carry the full station list for the flyout.
    pub clusters: RwLock<HashMap<u8, Vec<aggregate::RadioCluster>>>,
    /// Total geo-tagged stations downloaded (for the metrics panel).
    pub total_stations: AtomicUsize,
    /// Unix timestamp (seconds) of the most recent Radio Browser refresh.
    pub last_refresh: RwLock<Option<u64>>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cfg = config::Config::from_env();

    tracing::info!("geo-redis-radio starting");
    tracing::info!("API base: {}", cfg.api_base);
    tracing::info!("Fetch limit: {}", cfg.fetch_limit);
    tracing::info!("Poll interval: {}s", cfg.poll_interval_secs);

    let state = Arc::new(AppState {
        clusters:       RwLock::new(HashMap::new()),
        total_stations: AtomicUsize::new(0),
        last_refresh:   RwLock::new(None),
    });

    // Initial load — block until we have data before accepting requests.
    let http = reqwest::Client::new();
    match radio_api::fetch_stations(&http, &cfg.api_base, cfg.fetch_limit).await {
        Ok(stations) => {
            rebuild_clusters(&state, &stations).await;
        }
        Err(e) => tracing::error!("Initial radio station fetch failed: {e}"),
    }

    // Background refresh.
    let poll_state = Arc::clone(&state);
    let poll_cfg   = cfg.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(
                poll_cfg.poll_interval_secs,
            ))
            .await;
            let http = reqwest::Client::new();
            match radio_api::fetch_stations(&http, &poll_cfg.api_base, poll_cfg.fetch_limit).await
            {
                Ok(stations) => rebuild_clusters(&poll_state, &stations).await,
                Err(e)       => tracing::error!("Radio station refresh failed: {e}"),
            }
        }
    });

    let app = Router::new()
        .route("/api/aircraft",   get(routes::all_clusters))
        .route("/api/region",     get(routes::region_clusters))
        .route("/api/metrics",    get(routes::get_metrics))
        .route("/health",         get(routes::health))
        .layer(CorsLayer::permissive())
        .with_state(Arc::clone(&state));

    let addr = format!("{}:{}", cfg.server_host, cfg.server_port);
    tracing::info!("Radio server listening on http://{addr}");
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn rebuild_clusters(
    state: &Arc<AppState>,
    stations: &[radio_api::RadioStation],
) {
    state
        .total_stations
        .store(stations.len(), std::sync::atomic::Ordering::Relaxed);

    let mut map: HashMap<u8, Vec<aggregate::RadioCluster>> = HashMap::new();
    for level in [2u8, 3, 4] {
        let clusters = aggregate::group_at_level(stations, level, false);
        tracing::info!(
            "Level {}: {} clusters from {} stations",
            level, clusters.len(), stations.len()
        );
        map.insert(level, clusters);
    }
    // Leaf level — include station lists so the flyout can render without an extra request.
    let leaf = aggregate::group_at_level(stations, aggregate::LEAF_LEVEL, true);
    tracing::info!(
        "Level {} (leaf): {} cells, station lists included",
        aggregate::LEAF_LEVEL, leaf.len()
    );
    map.insert(aggregate::LEAF_LEVEL, leaf);

    *state.clusters.write().await = map;

    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    *state.last_refresh.write().await = Some(ts);
}
