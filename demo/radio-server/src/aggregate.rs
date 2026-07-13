use crate::radio_api::RadioStation;
use proxima::GeoTrie;
use serde::Serialize;
use std::collections::HashMap;

// ── Zoom → S2 level ────────────────────────────────────────────────────────
//
//  Leaflet zoom  │ S2 level │ approx cells │ typical cell size
//  ──────────────┼──────────┼──────────────┼─────────────────
//      0 – 3     │    2     │   48         │ continent
//      4 – 5     │    3     │  192         │ country
//      6 – 7     │    4     │  768         │ region / state
//      8 +       │    5     │ 3 072        │ city area (leaf — includes station list)

pub const LEAF_ZOOM: u8 = 8;
pub const LEAF_LEVEL: u8 = 5;

pub fn zoom_to_s2_level(map_zoom: u8) -> u8 {
    match map_zoom {
        0..=3 => 2,
        4..=5 => 3,
        6..=7 => 4,
        _ => LEAF_LEVEL,
    }
}

// ── Station info included in leaf-level responses ──────────────────────────

/// Compact station record embedded in each leaf cluster's payload.
#[derive(Debug, Clone, Serialize)]
pub struct StationInfo {
    pub uuid:        String,
    pub name:        String,
    pub stream_url:  String,
    pub tags:        String,
    pub country:     String,
    pub countrycode: String,
    pub codec:       String,
    pub bitrate:     u32,
    pub votes:       i32,
    pub favicon:     String,
}

// ── Cluster ────────────────────────────────────────────────────────────────

/// One representative node for an S2 cell (centroid + statistics).
#[derive(Debug, Clone)]
pub struct RadioCluster {
    pub token:       String,   // S2 cell token used as part of the GeoEntry id
    pub lat:         f64,
    pub lon:         f64,
    pub count:       usize,
    /// Populated only at the leaf level.
    pub stations:    Vec<StationInfo>,
    /// Highest-voted station's name (used as the display label).
    pub top_name:    String,
    pub top_tags:    String,
    pub top_country: String,
    pub top_cc:      String,
}

/// Group `stations` into S2 cells at the given level.
///
/// `include_stations` controls whether `RadioCluster::stations` is populated
/// (only needed for leaf-level responses to keep coarse payloads compact).
pub fn group_at_level(
    stations: &[RadioStation],
    level: u8,
    include_stations: bool,
) -> Vec<RadioCluster> {
    if stations.is_empty() {
        return vec![];
    }

    let helper = GeoTrie::new(level);

    // Group by S2 token at this level.
    let mut cells: HashMap<String, Vec<&RadioStation>> = HashMap::new();
    for s in stations {
        let lat = s.geo_lat.unwrap_or(0.0);
        let lon = s.geo_long.unwrap_or(0.0);
        let token = helper.cell_token(lat, lon);
        cells.entry(token).or_default().push(s);
    }

    cells
        .into_iter()
        .map(|(token, members)| build_cluster(token, &members, include_stations))
        .collect()
}

fn build_cluster(
    token: String,
    members: &[&RadioStation],
    include_stations: bool,
) -> RadioCluster {
    let n = members.len() as f64;

    // Centroid — mean of member coordinates.
    let lat = members.iter().map(|s| s.geo_lat.unwrap_or(0.0)).sum::<f64>() / n;
    let lon = members.iter().map(|s| s.geo_long.unwrap_or(0.0)).sum::<f64>() / n;

    // Top station = highest vote count.
    let top = members.iter().max_by_key(|s| s.votes).copied().unwrap();

    let stations = if include_stations {
        let mut infos: Vec<StationInfo> = members
            .iter()
            .map(|s| StationInfo {
                uuid:        s.stationuuid.clone(),
                name:        s.name.clone(),
                stream_url:  s.stream_url().to_string(),
                tags:        s.tags.clone(),
                country:     s.country.clone(),
                countrycode: s.countrycode.to_uppercase(),
                codec:       s.codec.clone(),
                bitrate:     s.bitrate,
                votes:       s.votes,
                favicon:     s.favicon.clone(),
            })
            .collect();
        // Sort by votes descending so best stations appear first in the flyout.
        infos.sort_by(|a, b| b.votes.cmp(&a.votes));
        infos
    } else {
        vec![]
    };

    RadioCluster {
        token,
        lat,
        lon,
        count: members.len(),
        stations,
        top_name:    top.name.clone(),
        top_tags:    top.tags.clone(),
        top_country: top.country.clone(),
        top_cc:      top.countrycode.to_uppercase(),
    }
}
