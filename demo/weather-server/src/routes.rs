use axum::{
    extract::{Path, Query, State},
    response::sse::{Event, Sse},
    Json,
};
use futures_util::stream;
use serde::{Deserialize, Serialize};
use std::{convert::Infallible, sync::Arc, time::Duration};
use s2::{cap::Cap, latlng::LatLng, point::Point, region::RegionCoverer, s1};
use georedis::GeoEntry;
use crate::{open_meteo, AppState};

#[derive(Serialize)]
pub struct AircraftResponse {
    count:    usize,
    aircraft: Vec<GeoEntry>,
}

#[derive(Deserialize)]
pub struct RegionParams {
    s: f64, w: f64, n: f64, e: f64,
}

// ── Payload mapping ────────────────────────────────────────────────────────
// Map weather grid point → aircraft-compatible schema so the existing Leaflet
// UI renders it. Extra fields (__is_weather, temp_c, wmo_code) are picked up
// by the new weather icon component in the UI.

fn station_to_aircraft(mut e: GeoEntry) -> GeoEntry {
    let name        = e.payload["name"].as_str().unwrap_or(&e.id).to_string();
    let temp_c      = e.payload["temp_c"].as_f64().unwrap_or(0.0);
    let feels_like  = e.payload["feels_like_c"].as_f64();
    let humidity    = e.payload["humidity_pct"].as_f64();
    let wspd        = e.payload["wspd_kt"].as_f64();
    let gust        = e.payload["gust_kt"].as_f64();
    let wdir        = e.payload["wdir"].as_u64().map(|d| d as f64);
    let cloud       = e.payload["cloud_pct"].as_f64();
    let pressure    = e.payload["pressure_hpa"].as_f64();
    let precip      = e.payload["precip"].as_f64();
    let wmo         = e.payload["wmo_code"].as_u64().unwrap_or(0) as u8;
    let count       = e.payload["count"].as_u64().unwrap_or(1);
    let flt_cat     = e.payload["flt_cat"].as_str().unwrap_or("").to_string();

    // Cluster nodes have ID like "wx:{s2_token}"; show readable count label
    let display_name = if name.starts_with("wx:") {
        format!("{} stations", count)
    } else {
        name
    };

    let condition_label = {
        let base = format!("{} {}", open_meteo::wmo_emoji(wmo), open_meteo::wmo_label(wmo));
        if flt_cat.is_empty() { base } else { format!("{base}  {flt_cat}") }
    };

    e.payload = serde_json::json!({
        "callsign":       display_name,
        "altitude":       open_meteo::temp_to_altitude_m(temp_c),
        "velocity":       wspd,
        "heading":        wdir,
        "on_ground":      false,
        "origin_country": condition_label,
        "__is_weather":   true,
        "temp_c":         temp_c,
        "feels_like_c":   feels_like,
        "humidity_pct":   humidity,
        "gust_kt":        gust,
        "cloud_pct":      cloud,
        "pressure_hpa":   pressure,
        "precip":         precip,
        "wmo_code":       wmo,
        "count":          count,
    });
    e
}

// ── Handlers ───────────────────────────────────────────────────────────────

pub async fn all_stations(State(st): State<Arc<AppState>>) -> Json<AircraftResponse> {
    let entries = st.trie.read().await.all_entries()
        .into_iter().map(station_to_aircraft).collect::<Vec<_>>();
    Json(AircraftResponse { count: entries.len(), aircraft: entries })
}

pub async fn region_stations(
    State(st): State<Arc<AppState>>,
    Query(p):  Query<RegionParams>,
) -> Json<AircraftResponse> {
    let tokens = viewport_tokens(p.s, p.w, p.n, p.e, st.config.s2_level);
    let entries = match st.store.query_region(&tokens).await {
        Ok(v)  => v.into_iter().map(station_to_aircraft).collect(),
        Err(e) => { tracing::error!("region query: {e}"); vec![] }
    };
    Json(AircraftResponse { count: entries.len(), aircraft: entries })
}

pub async fn station_detail(
    State(st): State<Arc<AppState>>,
    Path(id):  Path<String>,
) -> Json<serde_json::Value> {
    match st.db.get_detail(&id).await {
        Ok(Some(d)) => {
            let temp = d.temp_c.unwrap_or(0.0);
            Json(serde_json::json!({
                "id":             d.id,
                "callsign":       if d.name.is_empty() { &d.id } else { &d.name },
                "origin_country": d.wx_string,
                "altitude":       open_meteo::temp_to_altitude_m(temp),
                "velocity":       d.wspd_kt,
                "heading":        d.wdir,
                "on_ground":      false,
                "history":        d.history,
                "__is_weather":   true,
                "temp_c":         temp,
            }))
        }
        Ok(None) => Json(serde_json::json!({ "error": "not found" })),
        Err(e)   => { tracing::error!("detail: {e}"); Json(serde_json::json!({ "error": "internal error" })) }
    }
}

pub async fn get_metrics(State(st): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let snapshot  = st.store.metrics().snapshot();
    let trie_size = st.trie.read().await.len();
    let last_sync = *st.last_sync.read().await;
    Json(serde_json::json!({
        "source":    "aviationweather.gov METAR bulk dump (every 5 min)",
        "metrics":   snapshot,
        "trie_size": trie_size,
        "last_sync": last_sync,
    }))
}

pub async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "status": "ok", "source": "METAR bulk" }))
}

/// GET /api/stream — SSE endpoint.
/// Each METAR observation insertion fires a JSON StationEvent.
/// The browser subscribes once and receives live station-by-station updates
/// as the bulk dump is streamed into the georedis cache.
pub async fn sse_stream(
    State(st): State<Arc<AppState>>,
) -> Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>> {
    let rx = st.updates.subscribe();
    let s = stream::unfold(rx, |mut rx| async move {
        let event = match tokio::time::timeout(Duration::from_secs(30), rx.recv()).await {
            Ok(Ok(station_event)) => {
                let json = serde_json::to_string(&station_event).unwrap_or_default();
                Event::default().event("station").data(json)
            }
            _ => Event::default().event("keepalive").data(""),
        };
        Some((Ok::<Event, Infallible>(event), rx))
    });
    Sse::new(s).keep_alive(axum::response::sse::KeepAlive::new().interval(Duration::from_secs(25)))
}

// ── S2 viewport helper ─────────────────────────────────────────────────────

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
    coverer.covering(&cap).0.iter()
        .map(|c| { let h = format!("{:016x}", c.0); h.trim_end_matches('0').to_string() })
        .collect()
}
