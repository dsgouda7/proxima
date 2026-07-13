use anyhow::Context;
use serde::Deserialize;

/// Raw station record from Radio Browser API.
#[derive(Debug, Clone, Deserialize)]
pub struct RadioStation {
    pub stationuuid:  String,
    pub name:         String,
    /// Primary stream URL (may redirect).
    #[serde(default)]
    pub url:          String,
    /// Redirect-resolved stream URL (preferred for direct playback).
    #[serde(default)]
    pub url_resolved: String,
    #[serde(default)]
    pub favicon:      String,
    #[serde(default)]
    pub tags:         String,
    #[serde(default)]
    pub country:      String,
    #[serde(default)]
    pub countrycode:  String,
    #[serde(default)]
    pub state:        String,
    #[serde(default)]
    pub language:     String,
    #[serde(default)]
    pub votes:        i32,
    #[serde(default)]
    pub codec:        String,
    #[serde(default)]
    pub bitrate:      u32,
    pub geo_lat:      Option<f64>,
    pub geo_long:     Option<f64>,
}

impl RadioStation {
    /// Best available stream URL.
    pub fn stream_url(&self) -> &str {
        if !self.url_resolved.is_empty() {
            &self.url_resolved
        } else {
            &self.url
        }
    }
}

/// Download geo-tagged stations from the Radio Browser API.
pub async fn fetch_stations(
    http: &reqwest::Client,
    api_base: &str,
    limit: usize,
) -> anyhow::Result<Vec<RadioStation>> {
    let url = format!(
        "{api_base}/json/stations/search\
         ?limit={limit}\
         &hidebroken=true\
         &has_geo_info=true\
         &order=votes\
         &reverse=true"
    );

    tracing::info!("Fetching radio stations from {url}");

    let stations: Vec<RadioStation> = http
        .get(&url)
        .header("User-Agent", "proxima-radio/1.0")
        .send()
        .await
        .context("GET radio stations")?
        .error_for_status()
        .context("Radio Browser HTTP error")?
        .json()
        .await
        .context("deserialise radio stations")?;

    let geo_tagged: Vec<RadioStation> = stations
        .into_iter()
        .filter(|s| s.geo_lat.is_some() && s.geo_long.is_some())
        .collect();

    tracing::info!("Fetched {} geo-tagged radio stations", geo_tagged.len());
    Ok(geo_tagged)
}
