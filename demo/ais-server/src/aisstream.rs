use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};

const AISSTREAM_URL: &str = "wss://stream.aisstream.io/v0/stream";

/// A vessel position update parsed from an AISStream message.
#[derive(Debug, Clone)]
pub struct Vessel {
    pub mmsi:       String,
    pub ship_name:  String,
    pub lat:        f64,
    pub lon:        f64,
    /// Speed over ground in knots.
    pub sog:        Option<f64>,
    /// Course over ground in degrees (0–360).
    pub cog:        Option<f64>,
    /// True heading in degrees (0–359, 511 = not available).
    pub heading:    Option<u16>,
    /// AIS navigational status (0 = underway using engine, 1 = at anchor, …).
    pub nav_status: u8,
}

// ── AISStream message shapes ───────────────────────────────────────────────

#[derive(Deserialize)]
struct AisEnvelope {
    #[serde(rename = "MessageType")]
    message_type: String,
    #[serde(rename = "Message")]
    message:      serde_json::Value,
    #[serde(rename = "MetaData")]
    meta:         MetaData,
}

#[derive(Deserialize)]
struct MetaData {
    #[serde(rename = "MMSI_String")]
    mmsi:      String,
    #[serde(rename = "ShipName")]
    ship_name: String,
    latitude:  f64,
    longitude: f64,
}

// ── Public API ─────────────────────────────────────────────────────────────

/// Opens one WebSocket session to AISStream.io and forwards parsed vessels
/// to `tx` until the connection closes or errors out.
/// The caller should reconnect (with backoff) when this returns.
pub async fn stream_once(api_key: &str, tx: &mpsc::Sender<Vessel>) -> Result<()> {
    let (mut ws, _) = connect_async(AISSTREAM_URL).await?;
    tracing::info!("AISStream WebSocket connected");

    let sub = serde_json::json!({
        "APIKey":             api_key,
        "BoundingBoxes":      [[[-90.0, -180.0], [90.0, 180.0]]],
        "FilterMessageTypes": ["PositionReport"]
    });
    ws.send(Message::Text(sub.to_string().into())).await?;

    while let Some(msg) = ws.next().await {
        let msg = msg?;
        let text = match msg {
            Message::Text(t)  => t,
            Message::Close(_) => {
                tracing::warn!("AISStream WebSocket closed by server");
                break;
            }
            _ => continue,
        };

        if let Some(vessel) = parse_message(&text) {
            // Drop the update if the receiver is full; never block the WS reader.
            let _ = tx.try_send(vessel);
        }
    }

    Ok(())
}

fn parse_message(text: &str) -> Option<Vessel> {
    let env: AisEnvelope = serde_json::from_str(text).ok()?;
    if env.message_type != "PositionReport" {
        return None;
    }

    let meta = &env.meta;
    if !(-90.0..=90.0).contains(&meta.latitude) || !(-180.0..=180.0).contains(&meta.longitude) {
        return None;
    }

    let mmsi = meta.mmsi.trim().to_string();
    if mmsi.is_empty() {
        return None;
    }

    let pr = &env.message["PositionReport"];

    // TrueHeading 511 means "not available" in the AIS spec.
    let heading = pr.get("TrueHeading")
        .and_then(|v| v.as_u64())
        .map(|h| h as u16)
        .filter(|&h| h != 511);

    Some(Vessel {
        mmsi,
        ship_name:  meta.ship_name.trim().to_string(),
        lat:        meta.latitude,
        lon:        meta.longitude,
        sog:        pr.get("Sog").and_then(|v| v.as_f64()),
        cog:        pr.get("Cog").and_then(|v| v.as_f64()),
        heading,
        nav_status: pr.get("NavigationalStatus").and_then(|v| v.as_u64()).unwrap_or(0) as u8,
    })
}
