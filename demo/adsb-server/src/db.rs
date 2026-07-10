use std::sync::{Arc, Mutex};
use rusqlite::{params, Connection};
use serde::Serialize;

const SCHEMA: &str = r#"
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

CREATE TABLE IF NOT EXISTS aircraft (
    id             TEXT PRIMARY KEY,
    callsign       TEXT,
    aircraft_type  TEXT NOT NULL DEFAULT '',
    registration   TEXT,
    altitude       REAL,
    velocity       REAL,
    heading        REAL,
    on_ground      INTEGER NOT NULL DEFAULT 0,
    updated_at     INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS position_history (
    rowid       INTEGER PRIMARY KEY AUTOINCREMENT,
    aircraft_id TEXT NOT NULL,
    lat         REAL NOT NULL,
    lon         REAL NOT NULL,
    recorded_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_hist_ac_time
ON position_history(aircraft_id, recorded_at DESC);
"#;

pub struct AircraftData {
    pub id:            String,
    pub lat:           f64,
    pub lon:           f64,
    pub callsign:      Option<String>,
    pub aircraft_type: String,
    pub registration:  Option<String>,
    pub altitude:      Option<f64>,
    pub velocity:      Option<f64>,
    pub heading:       Option<f64>,
    pub on_ground:     bool,
}

#[derive(Serialize)]
pub struct AircraftDetail {
    pub id:            String,
    pub callsign:      Option<String>,
    pub aircraft_type: String,
    pub registration:  Option<String>,
    pub altitude:      Option<f64>,
    pub velocity:      Option<f64>,
    pub heading:       Option<f64>,
    pub on_ground:     bool,
    pub history:       Vec<[f64; 2]>,
}

#[derive(Clone)]
pub struct Db {
    conn: Arc<Mutex<Connection>>,
}

impl Db {
    pub fn open(path: &str) -> anyhow::Result<Self> {
        let conn = Connection::open(path)?;
        conn.execute_batch(SCHEMA)?;
        tracing::info!("SQLite opened at {path}");
        Ok(Self { conn: Arc::new(Mutex::new(conn)) })
    }

    pub async fn upsert_batch(&self, aircraft: Vec<AircraftData>) -> anyhow::Result<()> {
        let conn = Arc::clone(&self.conn);
        tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            let mut guard = conn.lock().map_err(|_| anyhow::anyhow!("db mutex poisoned"))?;
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64;

            let tx = guard.transaction()?;
            {
                let mut upsert = tx.prepare_cached(
                    "INSERT INTO aircraft
                         (id,callsign,aircraft_type,registration,altitude,velocity,heading,on_ground,updated_at)
                     VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)
                     ON CONFLICT(id) DO UPDATE SET
                         callsign=excluded.callsign,
                         aircraft_type=excluded.aircraft_type,
                         registration=excluded.registration,
                         altitude=excluded.altitude,
                         velocity=excluded.velocity,
                         heading=excluded.heading,
                         on_ground=excluded.on_ground,
                         updated_at=excluded.updated_at",
                )?;
                let mut hist = tx.prepare_cached(
                    "INSERT INTO position_history(aircraft_id,lat,lon,recorded_at) VALUES(?1,?2,?3,?4)",
                )?;
                for a in &aircraft {
                    upsert.execute(params![
                        a.id, a.callsign, a.aircraft_type, a.registration,
                        a.altitude, a.velocity, a.heading,
                        a.on_ground as i32, now
                    ])?;
                    hist.execute(params![a.id, a.lat, a.lon, now])?;
                }
            }
            tx.commit()?;
            Ok(())
        })
        .await?
    }

    pub async fn prune_history(&self) -> anyhow::Result<()> {
        let conn = Arc::clone(&self.conn);
        tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            let guard = conn.lock().map_err(|_| anyhow::anyhow!("db mutex poisoned"))?;
            let cutoff = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64 - 600;
            guard.execute("DELETE FROM position_history WHERE recorded_at < ?1", params![cutoff])?;
            Ok(())
        })
        .await?
    }

    pub async fn get_detail(&self, id: &str) -> anyhow::Result<Option<AircraftDetail>> {
        let conn = Arc::clone(&self.conn);
        let id   = id.to_string();
        tokio::task::spawn_blocking(move || -> anyhow::Result<Option<AircraftDetail>> {
            let guard = conn.lock().map_err(|_| anyhow::anyhow!("db mutex poisoned"))?;

            let result = guard.query_row(
                "SELECT id,callsign,aircraft_type,registration,altitude,velocity,heading,on_ground
                 FROM aircraft WHERE id=?1",
                params![id],
                |row| {
                    Ok(AircraftDetail {
                        id:            row.get(0)?,
                        callsign:      row.get(1)?,
                        aircraft_type: row.get::<_, Option<String>>(2)?.unwrap_or_default(),
                        registration:  row.get(3)?,
                        altitude:      row.get(4)?,
                        velocity:      row.get(5)?,
                        heading:       row.get(6)?,
                        on_ground:     row.get::<_, i32>(7)? != 0,
                        history:       vec![],
                    })
                },
            );

            let mut detail = match result {
                Ok(d)                                     => d,
                Err(rusqlite::Error::QueryReturnedNoRows) => return Ok(None),
                Err(e)                                    => return Err(e.into()),
            };

            let mut stmt = guard.prepare(
                "SELECT lat,lon FROM position_history
                 WHERE aircraft_id=?1 ORDER BY recorded_at ASC LIMIT 3",
            )?;
            detail.history = stmt
                .query_map(params![detail.id], |r| {
                    Ok([r.get::<_, f64>(0)?, r.get::<_, f64>(1)?])
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;

            Ok(Some(detail))
        })
        .await?
    }
}
