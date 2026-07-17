//! Open-Meteo integration — retained as a fallback data source.
//! Primary source is now `metar_bulk` (aviationweather.gov bulk CSV dump).
#![allow(dead_code)]

use serde::Deserialize;

/// One current weather observation from Open-Meteo.
/// Retained for potential future use; primary data source is now metar_bulk.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct WeatherObs {
    pub id: String,
    pub name: String,
    pub lat: f64,
    pub lon: f64,
    pub temp_c: f64,
    pub feels_like_c: f64,
    pub humidity_pct: f64,
    pub wspd_kt: f64,
    pub gust_kt: f64,
    pub wdir: u16,
    pub wmo_code: u8,
    pub precip: f64,
    pub cloud_pct: f64,
    pub pressure_hpa: f64,
}

// ── WMO codes ─────────────────────────────────────────────────────────────

pub fn wmo_emoji(code: u8) -> &'static str {
    match code {
        0 => "☀️",
        1 => "🌤️",
        2 => "⛅",
        3 => "☁️",
        45 | 48 => "🌫️",
        51..=57 => "🌦️",
        61..=65 => "🌧️",
        66 | 67 => "🌨️",
        71..=77 => "❄️",
        80..=82 => "🌦️",
        85 | 86 => "🌨️",
        95 => "⛈️",
        96 | 99 => "⛈️",
        _ => "🌡️",
    }
}

pub fn wmo_label(code: u8) -> &'static str {
    match code {
        0 => "Clear sky",
        1 => "Mainly clear",
        2 => "Partly cloudy",
        3 => "Overcast",
        45 => "Fog",
        48 => "Icy fog",
        51 => "Light drizzle",
        53 => "Moderate drizzle",
        55 => "Heavy drizzle",
        61 => "Light rain",
        63 => "Moderate rain",
        65 => "Heavy rain",
        66 | 67 => "Freezing rain",
        71 => "Light snow",
        73 => "Moderate snow",
        75 => "Heavy snow",
        77 => "Snow grains",
        80 => "Light showers",
        81 => "Moderate showers",
        82 => "Heavy showers",
        85 | 86 => "Snow showers",
        95 => "Thunderstorm",
        96 | 99 => "Thunderstorm + hail",
        _ => "Unknown",
    }
}

/// Map temperature (°C) to an "altitude" value (metres) for the existing
/// Leaflet colour scale — cold = purple/blue, hot = red/orange:
///
///   +35°C →      0 m  (red)
///   +20°C →  1 800 m  (yellow)
///     0°C →  4 200 m  (green)
///   -20°C →  6 600 m  (cyan)
///  -40°C+ → ≥9 000 m  (purple)
pub fn temp_to_altitude_m(temp_c: f64) -> f64 {
    ((35.0 - temp_c) * 120.0).max(0.0)
}

// ── Open-Meteo API shapes ──────────────────────────────────────────────────

#[derive(Deserialize)]
struct OmResponse {
    latitude: f64,
    longitude: f64,
    current: CurrentWeather,
}

#[derive(Deserialize)]
struct CurrentWeather {
    temperature_2m: f64,
    apparent_temperature: f64,
    relative_humidity_2m: f64,
    wind_speed_10m: f64,
    wind_gusts_10m: f64,
    wind_direction_10m: f64,
    weather_code: f64,
    precipitation: f64,
    cloud_cover: f64,
    surface_pressure: f64,
}

// ── Global 5° grid (2520 points, ~500 km spacing) ─────────────────────────

fn global_grid() -> Vec<(f64, f64)> {
    let mut pts = Vec::new();
    let mut lat = -85.0_f64;
    while lat <= 85.01 {
        let mut lon = -175.0_f64;
        while lon <= 175.01 {
            pts.push((lat, lon));
            lon += 5.0;
        }
        lat += 5.0;
    }
    pts
}

// ── Public API ─────────────────────────────────────────────────────────────

/// Fetches current weather for a global 5° grid (~2520 points) from Open-Meteo.
/// No API key required. Sends batches of 400 points (~7 requests) with a small
/// inter-request delay to stay well within Open-Meteo's per-minute rate limit.
pub async fn fetch_stations(client: &reqwest::Client) -> anyhow::Result<Vec<WeatherObs>> {
    let grid = global_grid();
    let mut all: Vec<WeatherObs> = Vec::with_capacity(grid.len());

    for (i, chunk) in grid.chunks(400).enumerate() {
        // Small courtesy delay after the first batch — keeps us well under
        // Open-Meteo's per-minute request limit (free tier: ~10 req/min).
        if i > 0 {
            tokio::time::sleep(std::time::Duration::from_secs(6)).await;
        }
        let lats: Vec<String> = chunk.iter().map(|(la, _)| format!("{la}")).collect();
        let lons: Vec<String> = chunk.iter().map(|(_, lo)| format!("{lo}")).collect();

        let body = client
            .get("https://api.open-meteo.com/v1/forecast")
            .query(&[
                ("latitude", lats.join(",")),
                ("longitude", lons.join(",")),
                (
                    "current",
                    "temperature_2m,apparent_temperature,relative_humidity_2m,\
                  precipitation,weather_code,cloud_cover,surface_pressure,\
                  wind_speed_10m,wind_direction_10m,wind_gusts_10m"
                        .to_string(),
                ),
                ("wind_speed_unit", "kn".to_string()),
            ])
            .timeout(std::time::Duration::from_secs(30))
            .header("User-Agent", "geo-redis-weather-demo/1.0")
            .send()
            .await?
            .json::<serde_json::Value>()
            .await?;

        let records: Vec<OmResponse> = match body {
            serde_json::Value::Array(arr) => {
                serde_json::from_value(serde_json::Value::Array(arr)).unwrap_or_default()
            }
            serde_json::Value::Object(ref obj) if obj.contains_key("error") => anyhow::bail!(
                "Open-Meteo error: {}",
                body["reason"].as_str().unwrap_or("unknown")
            ),
            single => match serde_json::from_value::<OmResponse>(single) {
                Ok(r) => vec![r],
                Err(_) => vec![],
            },
        };

        for r in records {
            let lat = (r.latitude * 10.0).round() / 10.0;
            let lon = (r.longitude * 10.0).round() / 10.0;
            let wmo = r.current.weather_code as u8;
            let lat_str = if lat >= 0.0 {
                format!("{lat}°N")
            } else {
                format!("{}°S", lat.abs())
            };
            let lon_str = if lon >= 0.0 {
                format!("{lon}°E")
            } else {
                format!("{}°W", lon.abs())
            };
            all.push(WeatherObs {
                id: format!("{lat}_{lon}"),
                name: format!("{lat_str} {lon_str}"),
                lat,
                lon,
                temp_c: r.current.temperature_2m,
                feels_like_c: r.current.apparent_temperature,
                humidity_pct: r.current.relative_humidity_2m,
                wspd_kt: r.current.wind_speed_10m,
                gust_kt: r.current.wind_gusts_10m,
                wdir: r.current.wind_direction_10m as u16,
                wmo_code: wmo,
                precip: r.current.precipitation,
                cloud_pct: r.current.cloud_cover,
                pressure_hpa: r.current.surface_pressure,
            });
        }
    }

    Ok(all)
}
