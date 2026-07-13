use crate::{aggregate, AppState};
use axum::{
    extract::{Query, State},
    Json,
};
use proxima::GeoEntry;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

// ── Response types ─────────────────────────────────────────────────────────

#[derive(Serialize)]
pub struct RadioResponse {
    count:    usize,
    aircraft: Vec<GeoEntry>,   // "aircraft" key keeps the UI schema unchanged
}

// ── Query param structs ────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct ZoomParam {
    zoom: Option<u8>,
}

#[derive(Deserialize)]
pub struct RegionParams {
    s: f64,
    w: f64,
    n: f64,
    e: f64,
    zoom: Option<u8>,
}

// ── Cluster → GeoEntry conversion ─────────────────────────────────────────

fn cluster_to_entry(c: &aggregate::RadioCluster, include_stations: bool) -> GeoEntry {
    let display = if c.count == 1 {
        c.top_name.clone()
    } else {
        format!("{} stations", c.count)
    };

    let stations_val: serde_json::Value = if include_stations {
        serde_json::to_value(&c.stations).unwrap_or(serde_json::Value::Null)
    } else {
        serde_json::Value::Null
    };

    GeoEntry {
        id: format!("radio:{}", c.token),
        lat: c.lat,
        lon: c.lon,
        written_at: 0,
        payload: serde_json::json!({
            "__is_radio":   true,
            "callsign":     display,
            "count":        c.count,
            "origin_country": c.top_country,
            "top_tags":     c.top_tags,
            "top_cc":       c.top_cc,
            "stations":     stations_val,
        }),
    }
}

// ── Handlers ───────────────────────────────────────────────────────────────

/// GET /api/aircraft?zoom=N
/// Returns global clusters at the resolution matching `zoom`.
pub async fn all_clusters(
    State(st): State<Arc<AppState>>,
    Query(p): Query<ZoomParam>,
) -> Json<RadioResponse> {
    let zoom = p.zoom.unwrap_or(3);
    let level = aggregate::zoom_to_s2_level(zoom);
    let is_leaf = zoom >= aggregate::LEAF_ZOOM;

    let clusters = st.clusters.read().await;
    let tier = clusters.get(&level).cloned().unwrap_or_default();
    drop(clusters);

    let aircraft: Vec<GeoEntry> = tier
        .iter()
        .map(|c| cluster_to_entry(c, is_leaf))
        .collect();
    let count = aircraft.len();
    Json(RadioResponse { count, aircraft })
}

/// GET /api/region?s=&w=&n=&e=&zoom=N
/// Returns clusters whose centroid falls within the viewport.
pub async fn region_clusters(
    State(st): State<Arc<AppState>>,
    Query(p): Query<RegionParams>,
) -> Json<RadioResponse> {
    let zoom = p.zoom.unwrap_or(6);
    let level = aggregate::zoom_to_s2_level(zoom);
    let is_leaf = zoom >= aggregate::LEAF_ZOOM;

    let clusters = st.clusters.read().await;
    let tier = clusters.get(&level).cloned().unwrap_or_default();
    drop(clusters);

    let aircraft: Vec<GeoEntry> = tier
        .iter()
        .filter(|c| c.lat >= p.s && c.lat <= p.n && c.lon >= p.w && c.lon <= p.e)
        .map(|c| cluster_to_entry(c, is_leaf))
        .collect();
    let count = aircraft.len();
    Json(RadioResponse { count, aircraft })
}

/// GET /api/metrics
pub async fn get_metrics(State(st): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let total = st.total_stations.load(std::sync::atomic::Ordering::Relaxed);
    let clusters = st.clusters.read().await;
    let leaf_count = clusters
        .get(&aggregate::LEAF_LEVEL)
        .map(|v| v.len())
        .unwrap_or(0);
    Json(serde_json::json!({
        "source":          "Radio Browser (radiobrowser.info)",
        "total_stations":  total,
        "leaf_cells":      leaf_count,
        "trie_size":       leaf_count,
        "metrics": {
            "write_count":  0,
            "write_avg_us": 0,
            "write_max_us": 0,
            "read_count":   0,
            "read_avg_us":  0,
            "read_max_us":  0,
        },
    }))
}

/// GET /health
pub async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "status": "ok", "source": "Radio Browser" }))
}
