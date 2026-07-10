use serde_json::Value;

#[derive(Debug, Clone)]
pub struct Aircraft {
    pub icao24:        String,
    pub callsign:      Option<String>,
    /// OpenSky origin_country stored here; named aircraft_type to match the db schema.
    pub aircraft_type: String,
    pub registration:  Option<String>,
    pub lat:           f64,
    pub lon:           f64,
    pub altitude:      Option<f64>,
    pub velocity:      Option<f64>,
    pub heading:       Option<f64>,
    pub on_ground:     bool,
}

#[derive(serde::Deserialize)]
struct OpenSkyResponse {
    states: Option<Vec<Vec<Value>>>,
}

/// Fetches live aircraft from the OpenSky Network public API.
/// (ADSB.fi switched to Cloudflare protection; OpenSky is the reliable open source.)
pub async fn fetch_aircraft(client: &reqwest::Client) -> anyhow::Result<Vec<Aircraft>> {
    let resp = client
        .get("https://opensky-network.org/api/states/all")
        .timeout(std::time::Duration::from_secs(25))
        .send()
        .await?
        .json::<OpenSkyResponse>()
        .await?;

    Ok(resp.states.unwrap_or_default().into_iter().filter_map(parse_state).collect())
}

fn parse_state(row: Vec<Value>) -> Option<Aircraft> {
    if row.len() < 17 { return None; }
    let lon = row[5].as_f64()?;
    let lat = row[6].as_f64()?;
    if !(-90.0..=90.0).contains(&lat) || !(-180.0..=180.0).contains(&lon) { return None; }

    Some(Aircraft {
        icao24:        row[0].as_str()?.to_string(),
        callsign:      row[1].as_str().map(str::trim).filter(|s| !s.is_empty()).map(String::from),
        aircraft_type: row[2].as_str().unwrap_or("").to_string(),
        registration:  None,
        lat, lon,
        altitude:  row[7].as_f64(),
        on_ground: row[8].as_bool().unwrap_or(false),
        velocity:  row[9].as_f64(),
        heading:   row[10].as_f64(),
    })
}
