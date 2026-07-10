//! Periodic snapshot of this shard's Redis state to SQLite.
//!
//! # Why
//!
//! Redis has built-in RDB/AOF persistence, which handles the common case
//! of a node restart on the **same machine** with the same volume.
//!
//! This module handles the harder case: a **new pod/machine replacing a dead
//! node**, where the ephemeral Redis volume is gone. On startup, the node
//! checks whether Redis is empty and, if so, bootstraps from the latest
//! SQLite snapshot rather than waiting for the next write cycle.
//!
//! # Stale-data contract across shards
//!
//! When an entity moves from one geographic shard to another, the old shard
//! retains the stale entry until its TTL expires (configurable, default 120s).
//! For high-frequency writes (couriers, IoT) set ENTITY_TTL_SECS = 2× your
//! write interval so stale data expires within two missed updates.
//!
//! For immediate cross-shard cleanup, the `DELETE /entity/:id` HTTP endpoint
//! removes an entity explicitly from whichever shard it reaches.

use anyhow::Result;
use rusqlite::params;
use std::sync::Arc;
use tokio::sync::Mutex;

// ── Schema ─────────────────────────────────────────────────────────────────

const SCHEMA: &str = r#"
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

-- Full entity snapshot — atomically replaced each cycle.
-- token: the S2 cell token this entity belonged to at snapshot time.
CREATE TABLE IF NOT EXISTS entity_snapshot (
    id          TEXT PRIMARY KEY,
    json        TEXT NOT NULL,   -- full GeoEntry JSON (lat, lon, payload)
    token       TEXT NOT NULL,   -- S2 cell token for cell-key restoration
    snapshotted INTEGER NOT NULL -- unix timestamp of this snapshot cycle
);

-- Audit log: one row per completed snapshot
CREATE TABLE IF NOT EXISTS snapshot_log (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_count  INTEGER NOT NULL,
    duration_ms   INTEGER NOT NULL,
    captured_at   INTEGER NOT NULL
);
"#;

// ── Public API ─────────────────────────────────────────────────────────────

pub struct SnapshotEntry {
    pub id:          String,
    pub json:        String,
    pub token:       String,
    /// Unix timestamp when this entry was snapshotted.
    /// Used during restore to skip entries that would have already expired
    /// under the configured `entity_ttl_secs`.
    pub snapshotted: i64,
}

#[derive(Clone)]
pub struct Snapshot {
    conn: Arc<Mutex<rusqlite::Connection>>,
}

impl Snapshot {
    pub fn open(path: &str) -> Result<Self> {
        let conn = rusqlite::Connection::open(path)?;
        conn.execute_batch(SCHEMA)?;
        tracing::info!("Snapshot store opened at {path}");
        Ok(Self { conn: Arc::new(Mutex::new(conn)) })
    }

    /// Atomically replaces the entire snapshot with `entries`.
    /// Uses a single WAL transaction — ~11k entries completes in <200 ms.
    pub async fn save(&self, entries: Vec<SnapshotEntry>) -> Result<u64> {
        let conn   = Arc::clone(&self.conn);
        let count  = entries.len() as i64;
        let now    = unix_now();
        let t0     = std::time::Instant::now();

        tokio::task::spawn_blocking(move || -> Result<()> {
            let mut guard = conn.blocking_lock();
            let tx = guard.transaction()?;

            // Atomic replace: delete all then re-insert
            tx.execute("DELETE FROM entity_snapshot", [])?;
            {
                let mut ins = tx.prepare_cached(
                    "INSERT INTO entity_snapshot(id,json,token,snapshotted) VALUES(?1,?2,?3,?4)",
                )?;
                for e in &entries {
                    ins.execute(params![e.id, e.json, e.token, now])?;
                }
            }
            let dur_ms = t0.elapsed().as_millis() as i64;
            tx.execute(
                "INSERT INTO snapshot_log(entity_count,duration_ms,captured_at) VALUES(?1,?2,?3)",
                params![count, dur_ms, now],
            )?;
            tx.commit()?;
            Ok(())
        })
        .await??;

        tracing::info!("Snapshot saved: {} entities", count);
        Ok(count as u64)
    }

    /// Load snapshotted entries including their capture timestamps.
    /// Caller filters by TTL before restoring to Redis.
    pub async fn load(&self) -> Result<Vec<SnapshotEntry>> {
        let conn = Arc::clone(&self.conn);
        tokio::task::spawn_blocking(move || -> Result<Vec<SnapshotEntry>> {
            let guard = conn.blocking_lock();
            let mut stmt = guard.prepare(
                "SELECT id,json,token,snapshotted FROM entity_snapshot ORDER BY id",
            )?;
            let entries = stmt
                .query_map([], |r| Ok(SnapshotEntry {
                    id:          r.get(0)?,
                    json:        r.get(1)?,
                    token:       r.get(2)?,
                    snapshotted: r.get(3)?,
                }))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(entries)
        })
        .await?
    }

    /// Number of entities in the current snapshot.
    pub async fn count(&self) -> Result<u64> {
        let conn = Arc::clone(&self.conn);
        tokio::task::spawn_blocking(move || -> Result<u64> {
            let guard = conn.blocking_lock();
            let n: i64 = guard.query_row(
                "SELECT COUNT(*) FROM entity_snapshot", [], |r| r.get(0),
            )?;
            Ok(n as u64)
        })
        .await?
    }

    /// Most recent snapshot log entry (for metrics/observability).
    pub async fn last_snapshot_info(&self) -> Result<Option<(u64, u64, u64)>> {
        let conn = Arc::clone(&self.conn);
        tokio::task::spawn_blocking(move || -> Result<Option<(u64, u64, u64)>> {
            let guard = conn.blocking_lock();
            let result = guard.query_row(
                "SELECT entity_count,duration_ms,captured_at FROM snapshot_log ORDER BY id DESC LIMIT 1",
                [],
                |r| Ok((r.get::<_,i64>(0)? as u64, r.get::<_,i64>(1)? as u64, r.get::<_,i64>(2)? as u64)),
            );
            match result {
                Ok(row)                                        => Ok(Some(row)),
                Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
                Err(e)                                         => Err(e.into()),
            }
        })
        .await?
    }
}

// ── helpers ────────────────────────────────────────────────────────────────

fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}
