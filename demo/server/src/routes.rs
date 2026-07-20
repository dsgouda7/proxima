use crate::AppState;
use axum::{
    extract::{Path, Query, State},
    Json,
};
use proxima::{GeoEntry, NearbyEntry};
use s2::{cap::Cap, latlng::LatLng, point::Point, region::RegionCoverer, s1};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Serialize)]
pub struct AircraftResponse {
    count: usize,
    aircraft: Vec<GeoEntry>,
}

#[derive(Deserialize)]
pub struct RegionParams {
    s: f64, // south lat
    w: f64, // west  lon
    n: f64, // north lat
    e: f64, // east  lon
}

pub async fn all_aircraft(State(st): State<Arc<AppState>>) -> Json<AircraftResponse> {
    let trie = st.trie.read().await;
    let aircraft = trie.all_entries();
    Json(AircraftResponse {
        count: aircraft.len(),
        aircraft,
    })
}

pub async fn region_aircraft(
    State(st): State<Arc<AppState>>,
    Query(p): Query<RegionParams>,
) -> Json<AircraftResponse> {
    let tokens = viewport_tokens(p.s, p.w, p.n, p.e, st.config.s2_level);
    match st.store.query_region(&tokens).await {
        Ok(aircraft) => Json(AircraftResponse {
            count: aircraft.len(),
            aircraft,
        }),
        Err(e) => {
            tracing::error!("region query: {e}");
            Json(AircraftResponse {
                count: 0,
                aircraft: vec![],
            })
        }
    }
}

pub async fn get_metrics(State(st): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let snapshot = st.store.metrics().snapshot();
    let trie_size = st.trie.read().await.len();
    let last_sync = *st.last_sync.read().await;
    Json(serde_json::json!({
        "metrics":   snapshot,
        "trie_size": trie_size,
        "last_sync": last_sync,
    }))
}

pub async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "status": "ok" }))
}

// ── Nearby query ───────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct NearbyParams {
    lat: f64,
    lon: f64,
    /// Search radius in metres (default 500 km).
    #[serde(default = "default_radius_m")]
    radius_m: f64,
    /// Maximum results to return (default 20).
    #[serde(default = "default_limit")]
    limit: usize,
}

fn default_radius_m() -> f64 { 500_000.0 }
fn default_limit()    -> usize { 20 }

#[derive(Serialize)]
pub struct NearbyResponse {
    count: usize,
    query_lat: f64,
    query_lon: f64,
    radius_m: f64,
    results: Vec<NearbyEntry>,
}

/// GET /api/nearby?lat=&lon=&radius_m=&limit=
///
/// Returns up to `limit` aircraft nearest to `(lat, lon)` within `radius_m`
/// metres, sorted closest-first with the great-circle distance included in
/// each result. Uses the Redis-backed S2 trie — no full dataset scan.
pub async fn nearby_aircraft(
    State(st): State<Arc<AppState>>,
    Query(p): Query<NearbyParams>,
) -> Json<NearbyResponse> {
    let top_k = Some(p.limit);
    match st.store.query_nearby(p.lat, p.lon, p.radius_m, st.config.s2_level, top_k).await {
        Ok(results) => Json(NearbyResponse {
            count: results.len(),
            query_lat: p.lat,
            query_lon: p.lon,
            radius_m: p.radius_m,
            results,
        }),
        Err(e) => {
            tracing::error!("nearby query: {e}");
            Json(NearbyResponse {
                count: 0,
                query_lat: p.lat,
                query_lon: p.lon,
                radius_m: p.radius_m,
                results: vec![],
            })
        }
    }
}

#[derive(Serialize)]
pub struct TrieNodeDto {
    id: String,
    token: String,
}

#[derive(Serialize)]
pub struct TrieSnapshot {
    s2_level: u8,
    count: usize,
    nodes: Vec<TrieNodeDto>,
}

/// GET /api/trie — flat list of {id, token} for every entry currently held
/// in the in-memory S2 trie. The frontend reconstructs the branch/leaf
/// structure client-side by grouping tokens on shared hex-character
/// prefixes — the same walk `GeoTrie::insert` performs internally — so no
/// new introspection method is needed on `GeoTrie` itself.
pub async fn trie_snapshot(State(st): State<Arc<AppState>>) -> Json<TrieSnapshot> {
    let trie = st.trie.read().await;
    let nodes: Vec<TrieNodeDto> = trie
        .all_entries()
        .into_iter()
        .map(|e| {
            let token = trie.cell_token(e.lat, e.lon);
            TrieNodeDto { id: e.id, token }
        })
        .collect();
    Json(TrieSnapshot {
        s2_level: trie.s2_level,
        count: nodes.len(),
        nodes,
    })
}

/// On-demand full metadata + last-3-positions history from SQLite.
/// Only called when zoomed in to < 5 aircraft — keeps Redis payload minimal.
pub async fn aircraft_detail(
    State(st): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Json<serde_json::Value> {
    match st.db.get_detail(&id).await {
        Ok(Some(detail)) => Json(serde_json::to_value(&detail).unwrap_or(serde_json::json!({}))),
        Ok(None) => Json(serde_json::json!({ "error": "not found" })),
        Err(e) => {
            tracing::error!("SQLite detail query failed: {e}");
            Json(serde_json::json!({ "error": "internal error" }))
        }
    }
}

// ── helper: S2 cap covering for a map viewport ────────────────────────────

fn viewport_tokens(south: f64, west: f64, north: f64, east: f64, level: u8) -> Vec<String> {
    use std::f64::consts::PI;
    let center_lat = (south + north) / 2.0;
    let center_lon = (west + east) / 2.0;
    let d_lat = (north - south).abs() / 2.0;
    let d_lon = (east - west).abs() / 2.0;
    // convert half-diagonal to radians; clamp to valid cap angle
    let radius_rad = ((d_lat * d_lat + d_lon * d_lon).sqrt() * PI / 180.0).min(PI);

    let center = Point::from(LatLng::new(
        s1::Deg(center_lat).into(),
        s1::Deg(center_lon).into(),
    ));
    let cap_angle: s1::angle::Angle = s1::Rad(radius_rad).into();
    let cap = Cap::from_center_angle(&center, &cap_angle);
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
            let hex = format!("{:016x}", c.0);
            hex.trim_end_matches('0').to_string()
        })
        .collect()
}
