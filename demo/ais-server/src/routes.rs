use axum::{
    extract::{Path, Query, State},
    Json,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use s2::{cap::Cap, latlng::LatLng, point::Point, region::RegionCoverer, s1};
use georedis::GeoEntry;
use crate::AppState;

#[derive(Serialize)]
pub struct VesselResponse {
    count:   usize,
    vessels: Vec<GeoEntry>,
}

/// Aircraft-compatible wrapper so the existing Leaflet UI can consume vessel data.
/// Maps: ship_name→callsign, sog→velocity, nav_status→on_ground+origin_country.
#[derive(Serialize)]
pub struct AircraftCompatResponse {
    count:    usize,
    aircraft: Vec<GeoEntry>,
}

#[derive(Deserialize)]
pub struct RegionParams {
    s: f64,
    w: f64,
    n: f64,
    e: f64,
}

pub async fn all_vessels(State(st): State<Arc<AppState>>) -> Json<VesselResponse> {
    let trie    = st.trie.read().await;
    let vessels = trie.all_entries();
    Json(VesselResponse { count: vessels.len(), vessels })
}

/// GET /api/vessels/region — native vessel format for non-UI API consumers.
#[allow(dead_code)]
pub async fn region_vessels(
    State(st): State<Arc<AppState>>,
    Query(p):  Query<RegionParams>,
) -> Json<VesselResponse> {
    let tokens = viewport_tokens(p.s, p.w, p.n, p.e, st.config.s2_level);
    match st.store.query_region(&tokens).await {
        Ok(vessels) => Json(VesselResponse { count: vessels.len(), vessels }),
        Err(e) => {
            tracing::error!("region query: {e}");
            Json(VesselResponse { count: 0, vessels: vec![] })
        }
    }
}

pub async fn get_metrics(State(st): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let snapshot  = st.store.metrics().snapshot();
    let trie_size = st.trie.read().await.len();
    let last_sync = *st.last_sync.read().await;
    let vessel_count = st.vessel_cache.read().await.len(); // counts (Vessel, timestamp) entries
    Json(serde_json::json!({
        "source":       "aisstream.io",
        "metrics":      snapshot,
        "trie_size":    trie_size,
        "cached_live":  vessel_count,
        "last_sync":    last_sync,
    }))
}

pub async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "status": "ok", "source": "aisstream.io" }))
}

pub async fn vessel_detail(
    State(st): State<Arc<AppState>>,
    Path(id):  Path<String>,
) -> Json<serde_json::Value> {
    match st.db.get_detail(&id).await {
        Ok(Some(detail)) => Json(serde_json::to_value(&detail).unwrap_or(serde_json::json!({}))),
        Ok(None)         => Json(serde_json::json!({ "error": "not found" })),
        Err(e) => {
            tracing::error!("SQLite detail query failed: {e}");
            Json(serde_json::json!({ "error": "internal error" }))
        }
    }
}

fn viewport_tokens(south: f64, west: f64, north: f64, east: f64, level: u8) -> Vec<String> {
    use std::f64::consts::PI;
    let center_lat = (south + north) / 2.0;
    let center_lon = (west  + east)  / 2.0;
    let d_lat = (north - south).abs() / 2.0;
    let d_lon = (east  - west).abs()  / 2.0;
    let radius_rad = ((d_lat * d_lat + d_lon * d_lon).sqrt() * PI / 180.0).min(PI);

    let center    = Point::from(LatLng::new(s1::Deg(center_lat).into(), s1::Deg(center_lon).into()));
    let cap_angle: s1::angle::Angle = s1::Rad(radius_rad).into();
    let cap       = Cap::from_center_angle(&center, &cap_angle);
    let coverer   = RegionCoverer { min_level: level, max_level: level, level_mod: 1, max_cells: 500 };

    coverer.covering(&cap)
        .0
        .iter()
        .map(|c| {
            let hex = format!("{:016x}", c.0);
            hex.trim_end_matches('0').to_string()
        })
        .collect()
}

// ── Aircraft-compatible alias routes (for the existing Leaflet UI) ─────────
//
// Maps vessel payload fields to the aircraft schema the UI expects:
//   ship_name  → callsign
//   sog        → velocity  (speed over ground, knots)
//   nav_status → on_ground (1=at anchor, 5=moored, 6=aground)
//   nav_status → origin_country (human-readable status label)

fn vessel_to_aircraft_entry(mut e: GeoEntry) -> GeoEntry {
    let callsign   = e.payload["ship_name"].as_str().unwrap_or("").to_string();
    let velocity   = e.payload["sog"].as_f64();
    let heading    = e.payload["heading"].as_u64().map(|h| h as f64);
    let nav_status = e.payload["nav_status"].as_u64().unwrap_or(0);
    let on_ground  = matches!(nav_status, 1 | 5 | 6); // anchored / moored / aground
    e.payload = serde_json::json!({
        "callsign":       if callsign.is_empty() { serde_json::Value::Null } else { callsign.into() },
        "velocity":       velocity,
        "heading":        heading,
        "on_ground":      on_ground,
        "origin_country": nav_status_label(nav_status),
    });
    e
}

fn nav_status_label(s: u64) -> &'static str {
    match s {
        0 => "Underway (engine)",
        1 => "At anchor",
        2 => "Not under command",
        3 => "Restricted manoeuvring",
        5 => "Moored",
        6 => "Aground",
        7 => "Fishing",
        8 => "Sailing",
        _ => "Unknown",
    }
}

/// GET /api/aircraft — vessel data wrapped in the aircraft-compatible envelope.
pub async fn all_vessels_compat(State(st): State<Arc<AppState>>) -> Json<AircraftCompatResponse> {
    let trie  = st.trie.read().await;
    let items = trie.all_entries().into_iter().map(vessel_to_aircraft_entry).collect::<Vec<_>>();
    Json(AircraftCompatResponse { count: items.len(), aircraft: items })
}

/// GET /api/region (aircraft-compat) — vessel region query for the Leaflet UI.
pub async fn region_vessels_compat(
    State(st): State<Arc<AppState>>,
    Query(p):  Query<RegionParams>,
) -> Json<AircraftCompatResponse> {
    let tokens = viewport_tokens(p.s, p.w, p.n, p.e, st.config.s2_level);
    let items  = match st.store.query_region(&tokens).await {
        Ok(v)  => v.into_iter().map(vessel_to_aircraft_entry).collect(),
        Err(e) => { tracing::error!("region query: {e}"); vec![] }
    };
    Json(AircraftCompatResponse { count: items.len(), aircraft: items })
}

/// GET /api/aircraft/:id — vessel detail mapped to the aircraft detail schema.
pub async fn vessel_detail_compat(
    State(st): State<Arc<AppState>>,
    Path(id):  Path<String>,
) -> Json<serde_json::Value> {
    match st.db.get_detail(&id).await {
        Ok(Some(d)) => Json(serde_json::json!({
            "id":             d.id,
            "callsign":       if d.ship_name.is_empty() { serde_json::Value::Null } else { d.ship_name.into() },
            "origin_country": nav_status_label(d.nav_status as u64),
            "altitude":       serde_json::Value::Null,
            "velocity":       d.sog,
            "heading":        d.heading,
            "on_ground":      matches!(d.nav_status, 1 | 5 | 6),
            "history":        d.history,
        })),
        Ok(None) => Json(serde_json::json!({ "error": "not found" })),
        Err(e)   => {
            tracing::error!("detail query failed: {e}");
            Json(serde_json::json!({ "error": "internal error" }))
        }
    }
}
