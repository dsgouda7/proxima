use std::sync::{Arc, Mutex};
use rusqlite::{params, Connection};
use serde::Serialize;

const SCHEMA: &str = r#"
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

CREATE TABLE IF NOT EXISTS vessels (
    id         TEXT PRIMARY KEY,
    ship_name  TEXT NOT NULL DEFAULT '',
    sog        REAL,
    cog        REAL,
    heading    INTEGER,
    nav_status INTEGER NOT NULL DEFAULT 0,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS position_history (
    rowid     INTEGER PRIMARY KEY AUTOINCREMENT,
    vessel_id TEXT NOT NULL,
    lat       REAL NOT NULL,
    lon       REAL NOT NULL,
    recorded_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_hist_vessel_time
ON position_history(vessel_id, recorded_at DESC);
"#;

pub struct VesselData {
    pub id:         String,
    pub lat:        f64,
    pub lon:        f64,
    pub ship_name:  String,
    pub sog:        Option<f64>,
    pub cog:        Option<f64>,
    pub heading:    Option<u16>,
    pub nav_status: u8,
}

#[derive(Serialize)]
pub struct VesselDetail {
    pub id:         String,
    pub ship_name:  String,
    pub sog:        Option<f64>,
    pub cog:        Option<f64>,
    pub heading:    Option<u16>,
    pub nav_status: u8,
    pub history:    Vec<[f64; 2]>,
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

    pub async fn upsert_batch(&self, vessels: Vec<VesselData>) -> anyhow::Result<()> {
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
                    "INSERT INTO vessels (id,ship_name,sog,cog,heading,nav_status,updated_at)
                     VALUES(?1,?2,?3,?4,?5,?6,?7)
                     ON CONFLICT(id) DO UPDATE SET
                         ship_name=excluded.ship_name,
                         sog=excluded.sog,
                         cog=excluded.cog,
                         heading=excluded.heading,
                         nav_status=excluded.nav_status,
                         updated_at=excluded.updated_at",
                )?;
                let mut hist = tx.prepare_cached(
                    "INSERT INTO position_history(vessel_id,lat,lon,recorded_at) VALUES(?1,?2,?3,?4)",
                )?;
                for v in &vessels {
                    upsert.execute(params![
                        v.id, v.ship_name,
                        v.sog, v.cog,
                        v.heading.map(|h| h as i32),
                        v.nav_status as i32,
                        now
                    ])?;
                    hist.execute(params![v.id, v.lat, v.lon, now])?;
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

    pub async fn get_detail(&self, id: &str) -> anyhow::Result<Option<VesselDetail>> {
        let conn = Arc::clone(&self.conn);
        let id   = id.to_string();
        tokio::task::spawn_blocking(move || -> anyhow::Result<Option<VesselDetail>> {
            let guard = conn.lock().map_err(|_| anyhow::anyhow!("db mutex poisoned"))?;

            let result = guard.query_row(
                "SELECT id,ship_name,sog,cog,heading,nav_status FROM vessels WHERE id=?1",
                params![id],
                |row| {
                    Ok(VesselDetail {
                        id:         row.get(0)?,
                        ship_name:  row.get::<_, Option<String>>(1)?.unwrap_or_default(),
                        sog:        row.get(2)?,
                        cog:        row.get(3)?,
                        heading:    row.get::<_, Option<i32>>(4)?.map(|h| h as u16),
                        nav_status: row.get::<_, i32>(5)? as u8,
                        history:    vec![],
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
                 WHERE vessel_id=?1 ORDER BY recorded_at ASC LIMIT 3",
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
