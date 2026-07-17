use flate2::read::GzDecoder;
use std::collections::HashMap;
/// Bulk METAR downloader.
///
/// aviationweather.gov publishes `metars.cache.csv.gz` — a gzip-compressed CSV
/// refreshed every **5 minutes** containing ALL current global METAR observations
/// (~5 000–8 000 stations).  A single file download = no per-request rate limiting.
///
/// This module downloads the file, decompresses it in-memory, and parses each row
/// into a `BulkMETAR` struct ready to be streamed into the geo-redis trie.
use std::io::Read;

const BULK_URL: &str = "https://aviationweather.gov/data/cache/metars.cache.csv.gz";

/// One METAR observation parsed from the bulk CSV.
#[derive(Debug, Clone)]
pub struct BulkMETAR {
    pub icao_id: String,
    pub lat: f64,
    pub lon: f64,
    pub temp_c: Option<f64>,
    pub dewp_c: Option<f64>,
    pub wind_dir: Option<u16>,
    pub wind_spd: Option<f64>,
    pub wind_gst: Option<f64>,
    pub wx: String,
    pub sky: String,
    pub flt_cat: String,
    pub _elev_m: f64,
}

// ── Download + parse ───────────────────────────────────────────────────────

/// Download the bulk METAR gzip, decompress, parse, and return all valid records.
pub async fn download_and_parse(client: &reqwest::Client) -> anyhow::Result<Vec<BulkMETAR>> {
    tracing::info!("Downloading bulk METAR dump from {BULK_URL}…");

    let bytes = client
        .get(BULK_URL)
        .timeout(std::time::Duration::from_secs(60))
        .header("User-Agent", "geo-redis-weather-demo/1.0")
        .send()
        .await?
        .bytes()
        .await?;

    tracing::info!("Downloaded {} KB — decompressing…", bytes.len() / 1024);

    let mut content = String::new();
    GzDecoder::new(bytes.as_ref()).read_to_string(&mut content)?;

    let records = parse_csv(&content);
    tracing::info!("Parsed {} METAR stations from bulk dump", records.len());
    Ok(records)
}

// ── CSV parser ─────────────────────────────────────────────────────────────

fn parse_csv(content: &str) -> Vec<BulkMETAR> {
    let mut lines = content.lines();

    // First non-comment line is the header
    let header = loop {
        match lines.next() {
            Some(l) if !l.starts_with('#') => break l,
            None => return vec![],
            _ => {}
        }
    };

    // Build column index map (handle duplicate names like sky_cover → sky_cover_0, _1…)
    let col_idx: HashMap<String, usize> = {
        let mut m = HashMap::new();
        let mut dup: HashMap<&str, usize> = HashMap::new();
        for (i, name) in header.split(',').enumerate() {
            let n = name.trim_matches('"');
            let cnt = dup.entry(n).or_insert(0);
            let key = if *cnt == 0 {
                n.to_string()
            } else {
                format!("{n}_{cnt}")
            };
            *cnt += 1;
            m.insert(key, i);
        }
        m
    };

    let get = |fields: &[&str], key: &str| -> String {
        col_idx
            .get(key)
            .and_then(|&i| fields.get(i))
            .map(|s| s.trim_matches('"').trim().to_string())
            .unwrap_or_default()
    };

    let mut records = Vec::new();

    for line in lines {
        if line.starts_with('#') || line.trim().is_empty() {
            continue;
        }

        // First field may be quoted (raw METAR text with commas); split carefully.
        let fields: Vec<&str> = split_row(line);

        let icao = get(&fields, "station_id");
        if icao.is_empty() {
            continue;
        }

        let lat = match get(&fields, "latitude").parse::<f64>() {
            Ok(v) if (-90.0..=90.0).contains(&v) => v,
            _ => continue,
        };
        let lon = match get(&fields, "longitude").parse::<f64>() {
            Ok(v) if (-180.0..=180.0).contains(&v) => v,
            _ => continue,
        };

        // Sky cover: use the most significant layer (OVC > BKN > SCT > FEW)
        let sky = {
            let layers = ["sky_cover", "sky_cover_1", "sky_cover_2", "sky_cover_3"];
            let priority = ["OVC", "BKN", "SCT", "FEW"];
            let mut best = String::new();
            for p in priority {
                if layers.iter().any(|k| get(&fields, k).starts_with(p)) {
                    best = p.to_string();
                    break;
                }
            }
            if best.is_empty() {
                get(&fields, "sky_cover")
            } else {
                best
            }
        };

        records.push(BulkMETAR {
            icao_id: icao,
            lat,
            lon,
            temp_c: get(&fields, "temp_c").parse().ok(),
            dewp_c: get(&fields, "dewpoint_c").parse().ok(),
            wind_dir: get(&fields, "wind_dir_degrees").parse().ok(),
            wind_spd: get(&fields, "wind_speed_kt").parse().ok(),
            wind_gst: get(&fields, "wind_gust_kt").parse().ok(),
            wx: get(&fields, "wx_string"),
            sky,
            flt_cat: get(&fields, "flight_category"),
            _elev_m: get(&fields, "elevation_m").parse().unwrap_or(0.0),
        });
    }

    records
}

/// Split a CSV row, handling a leading quoted field (raw_text).
fn split_row(line: &str) -> Vec<&str> {
    if line.starts_with('"') {
        // Scan for the closing quote (accounting for escaped "")
        let mut i = 1usize;
        let bytes = line.as_bytes();
        loop {
            if i >= bytes.len() {
                break;
            }
            if bytes[i] == b'"' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                    i += 2; // escaped quote ""
                } else {
                    break; // end of quoted field
                }
            } else {
                i += 1;
            }
        }
        // i now points to the closing quote; skip ',' after it
        let after = if i + 1 < line.len() {
            &line[i + 2..]
        } else {
            ""
        };
        let mut fields = vec![&line[1..i]];
        fields.extend(after.split(','));
        fields
    } else {
        line.split(',').collect()
    }
}

// ── METAR condition → WMO code ─────────────────────────────────────────────

/// Convert a METAR wx_string + sky_cover to the nearest WMO weather code
/// so the existing weather icon table in the UI can render the right emoji.
pub fn wx_to_wmo(wx: &str, sky: &str) -> u8 {
    // Thunderstorm (check TS first — it overrides everything)
    if wx.contains("TS") {
        return 95;
    }
    // Freezing precipitation
    if wx.contains("FZRA") || wx.contains("FZDZ") {
        return 67;
    }
    // Snow
    if wx.contains("SN") || wx.contains("SG") || wx.contains("PL") {
        return 71;
    }
    // Snow showers
    if wx.contains("SHSN") {
        return 85;
    }
    // Rain showers
    if wx.contains("SHRA") || wx.contains("SH") {
        return 80;
    }
    // Heavy rain
    if wx.contains("+RA") {
        return 65;
    }
    // Moderate rain
    if wx.contains("RA") {
        return 61;
    }
    // Drizzle
    if wx.contains("DZ") {
        return 51;
    }
    // Fog / mist
    if wx.contains("FG") {
        return 45;
    }
    if wx.contains("BR") {
        return 45;
    }
    // Haze / smoke → use "mainly clear" (no good WMO match)
    if wx.contains("HZ") || wx.contains("FU") {
        return 1;
    }
    // No precipitation — use cloud cover
    match sky {
        s if s.starts_with("OVC") || s.starts_with("BKN") => 3, // overcast
        s if s.starts_with("SCT") => 2,                         // partly cloudy
        s if s.starts_with("FEW") => 1,                         // mainly clear
        _ => 0,                                                 // clear
    }
}
