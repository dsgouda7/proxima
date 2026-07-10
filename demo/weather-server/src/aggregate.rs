/// Spatial aggregation of raw METAR stations into at most `max_clusters`
/// representative nodes, equally distributed across the globe.
///
/// Algorithm:
///   1. Try S2 cell levels 5 → 4 → 3 (finest to coarsest).
///   2. Use the FINEST level whose occupied cell count is ≤ `max_clusters`.
///   3. Group every METAR station into its cell.
///   4. For each occupied cell compute:
///      - centroid  : mean lat/lon of member stations
///      - temp_c    : median temperature
///      - wind_spd  : median wind speed
///      - condition : most common wx_string (or sky cover if wx is empty)
///      - wmo_code  : derived from the winning condition
///      - count     : number of member stations
///
/// The resulting cluster IDs are `"wx:{s2_token}"` — ordinary strings that
/// the georedis lib handles without any changes (GeoEntry.id is already String).

use std::collections::HashMap;
use georedis::GeoTrie;
use crate::metar_bulk::{self, BulkMETAR};

/// One aggregated weather cluster ready to be inserted into the georedis trie.
#[derive(Debug, Clone)]
pub struct Cluster {
    /// Stable ID of the form `"wx:{s2_token_at_chosen_level}"`.
    pub id:       String,
    pub lat:      f64,
    pub lon:      f64,
    pub temp_c:   Option<f64>,
    pub wind_spd: Option<f64>,
    pub wind_dir: Option<u16>,
    pub wx:       String,
    pub sky:      String,
    pub wmo_code: u8,
    pub flt_cat:  String,
    /// Number of raw METAR stations that contributed to this cluster.
    pub count:    usize,
}

/// Aggregate `stations` into at most `max_clusters` spatial clusters.
///
/// If `force_level` is Some(n), that S2 level is used directly (useful for
/// tuning via the `CLUSTER_LEVEL` env var).  Otherwise the level is
/// auto-detected to keep occupied cells ≤ `max_clusters`.
pub fn aggregate(
    stations:    &[BulkMETAR],
    max_clusters: usize,
    force_level:  Option<u8>,
) -> Vec<Cluster> {
    if stations.is_empty() { return vec![]; }

    let level = force_level.unwrap_or_else(|| auto_level(stations, max_clusters));
    tracing::info!("Aggregating {} stations into S2 level-{} clusters (max {})",
        stations.len(), level, max_clusters);

    let helper = GeoTrie::new(level);

    // Group stations by S2 cell token at the chosen level.
    let mut cells: HashMap<String, Vec<&BulkMETAR>> = HashMap::new();
    for s in stations {
        let token = helper.cell_token(s.lat, s.lon);
        cells.entry(token).or_default().push(s);
    }

    let clusters: Vec<Cluster> = cells.into_iter()
        .map(|(token, members)| build_cluster(token, &members))
        .collect();

    tracing::info!("Produced {} clusters at S2 level {}", clusters.len(), level);
    clusters
}

// ── Helpers ────────────────────────────────────────────────────────────────

/// Find the finest S2 level (5→4→3) that gives ≤ `max_clusters` occupied cells.
fn auto_level(stations: &[BulkMETAR], max_clusters: usize) -> u8 {
    for level in [5u8, 4, 3, 2] {
        let helper = GeoTrie::new(level);
        let occupied: std::collections::HashSet<String> = stations
            .iter()
            .map(|s| helper.cell_token(s.lat, s.lon))
            .collect();
        if occupied.len() <= max_clusters {
            return level;
        }
    }
    2
}

fn build_cluster(token: String, members: &[&BulkMETAR]) -> Cluster {
    let n = members.len();

    // Centroid (mean lat/lon)
    let lat = members.iter().map(|s| s.lat).sum::<f64>() / n as f64;
    let lon = members.iter().map(|s| s.lon).sum::<f64>() / n as f64;

    // Median temperature
    let mut temps: Vec<f64> = members.iter().filter_map(|s| s.temp_c).collect();
    temps.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let temp_c = temps.get(temps.len() / 2).copied();

    // Median wind speed
    let mut winds: Vec<f64> = members.iter().filter_map(|s| s.wind_spd).collect();
    winds.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let wind_spd = winds.get(winds.len() / 2).copied();

    // Modal wind direction (median of circular data — simplified: numeric median)
    let mut dirs: Vec<u16> = members.iter().filter_map(|s| s.wind_dir).collect();
    dirs.sort();
    let wind_dir = dirs.get(dirs.len() / 2).copied();

    // Most common non-empty wx_string; fall back to sky cover
    let wx  = modal_string(members.iter().map(|s| s.wx.as_str()).filter(|s| !s.is_empty()));
    let sky = modal_string(members.iter().map(|s| s.sky.as_str()).filter(|s| !s.is_empty()));

    // Most common flight category
    let flt_cat = modal_string(members.iter().map(|s| s.flt_cat.as_str()).filter(|s| !s.is_empty()));

    let wmo_code = metar_bulk::wx_to_wmo(&wx, &sky);

    Cluster {
        id: format!("wx:{token}"),
        lat, lon,
        temp_c,
        wind_spd,
        wind_dir,
        wx,
        sky,
        wmo_code,
        flt_cat,
        count: n,
    }
}

/// Returns the most common non-empty string from an iterator.
fn modal_string<'a>(iter: impl Iterator<Item = &'a str>) -> String {
    let mut counts: HashMap<&str, usize> = HashMap::new();
    for s in iter { *counts.entry(s).or_insert(0) += 1; }
    counts.into_iter()
        .max_by_key(|(_, c)| *c)
        .map(|(k, _)| k.to_string())
        .unwrap_or_default()
}
