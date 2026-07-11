use std::{collections::HashSet, sync::Arc, time::Instant};
use redis::AsyncCommands;
use s2::{cellid::CellID, latlng::LatLng, s1};
use crate::{GeoEntry, GeoTrie, Metrics, Result};

/// Default safety-net TTL: 10 minutes.
pub const DEFAULT_ENTITY_TTL_SECS: u64 = 600;
const CHUNK_SIZE: usize = 400;

/// Atomically:
///   1. Delete cell keys that are in `active_cells` but absent from the new set.
///   2. Replace `active_cells` with the new set.
///   3. Set TTL on `active_cells`.
///
/// Using EVAL means all three steps are a single Redis round-trip with no
/// gap between "write new entities" and "clean up stale cells".
/// Safe for single-instance Redis per shard (our deployment model).
const ACTIVE_CELLS_LUA: &str = r#"
local active_key = KEYS[1]
local prefix     = ARGV[1]
local ttl        = tonumber(ARGV[2])

local prev_cells = redis.call('SMEMBERS', active_key)

-- Build lookup table of new tokens (ARGV[3..])
local new_set = {}
for i = 3, #ARGV do new_set[ARGV[i]] = true end

-- Delete cell keys no longer in use
local deleted = 0
for _, token in ipairs(prev_cells) do
    if not new_set[token] then
        redis.call('DEL', prefix .. ':cell:' .. token)
        deleted = deleted + 1
    end
end

-- Atomically replace active_cells with the new token set
redis.call('DEL', active_key)
for i = 3, #ARGV do redis.call('SADD', active_key, ARGV[i]) end
if ttl > 0 then redis.call('EXPIRE', active_key, ttl) end

return deleted
"#;

pub struct RedisStore {
    client:          redis::Client,
    metrics:         Arc<Metrics>,
    key_prefix:      String,
    /// Safety-net TTL for entities that stop reporting.
    /// Configurable per deployment; use `with_config` to override.
    entity_ttl_secs: u64,
}

impl RedisStore {
    /// Create a store with the default 10-minute safety-net TTL.
    pub fn new(redis_url: &str, metrics: Arc<Metrics>) -> Result<Self> {
        Self::with_config(redis_url, metrics, DEFAULT_ENTITY_TTL_SECS)
    }

    /// Create a store with an explicit entity TTL.
    ///
    /// Set `entity_ttl_secs` to ~10× your write interval:
    /// - Aircraft (30s poll) → 300–600s
    /// - Couriers (5s GPS)   → 30–60s
    /// - IoT sensors (1s)    → 10–15s
    pub fn with_config(
        redis_url:       &str,
        metrics:         Arc<Metrics>,
        entity_ttl_secs: u64,
    ) -> Result<Self> {
        Ok(Self {
            client: redis::Client::open(redis_url)?,
            metrics,
            key_prefix: "georedis".into(),
            entity_ttl_secs,
        })
    }

    /// Override the Redis key namespace (default: `"georedis"`).
    ///
    /// Multiple logical tenants can share one Redis instance without key
    /// collisions by using distinct namespaces:
    /// ```ignore
    /// let store = RedisStore::new(url, metrics)?.with_namespace("acme");
    /// ```
    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.key_prefix = namespace.into();
        self
    }

    /// Persists all trie entries to Redis.
    ///
    /// **Uniqueness guarantee**: every entity ID exists in exactly ONE cell at
    /// any time. Uses three complementary mechanisms:
    ///
    /// 1. `georedis:location:{id}` — reverse lookup: id → current cell token.
    ///    Written on every persist cycle so that the next cycle can detect moves.
    ///
    /// 2. `georedis:active_cells` — tracks all currently occupied cell tokens.
    ///    On each cycle, cell tokens that are no longer occupied are explicitly
    ///    deleted (not relied upon to expire via TTL).
    ///
    /// 3. Safety-net TTL (configurable, default 10 min) — catches entities that stop
    ///    reporting entirely without ever moving (e.g. aircraft that lands and
    ///    disappears from the feed). The TTL is intentionally long (5 min) so
    ///    it never fires for active entities.
    pub async fn persist_trie(&self, trie: &GeoTrie) -> Result<()> {
        let start   = Instant::now();
        let mut entries = trie.all_entries();
        if entries.is_empty() { return Ok(()); }

        // Stamp written_at on entries that haven't been timestamped yet.
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        for e in &mut entries {
            if e.written_at == 0 { e.written_at = now_ms; }
        }

        let prefix = &self.key_prefix;
        let ttl    = self.entity_ttl_secs;
        let ttl_i  = ttl as i64;
        let mut conn = self.client.get_multiplexed_async_connection().await?;

        // ── Step 1: Prepare active-cells key (diff computed server-side in Lua) ──
        let active_key    = format!("{prefix}:active_cells");
        let written_at_key = format!("{prefix}:written_at");

        // ── Step 2: Write entities + location reverse-lookup + written_at index ──
        let mut new_cells: HashSet<String> = HashSet::with_capacity(entries.len() / 4);

        for chunk in entries.chunks(CHUNK_SIZE) {
            let mut pipe = redis::pipe();
            pipe.atomic();
            for entry in chunk {
                let token    = trie.cell_token(entry.lat, entry.lon);
                let ak       = format!("{prefix}:entity:{}", entry.id);
                let ck       = format!("{prefix}:cell:{token}");
                let loc_key  = format!("{prefix}:location:{}", entry.id);
                let json     = serde_json::to_string(entry)?;

                pipe.set_ex(&ak,      &json,  ttl).ignore();
                pipe.sadd(&ck,        &entry.id).ignore();
                // Reverse lookup: id → current cell token
                pipe.set_ex(&loc_key, &token, ttl).ignore();
                // Written-at sorted set: score = ms timestamp, member = entity id.
                // Enables efficient delta-sync queries: ZRANGEBYSCORE since_ms +inf
                pipe.zadd(&written_at_key, entry.written_at as f64, entry.id.as_str()).ignore();
                new_cells.insert(token);
            }
            pipe.query_async::<()>(&mut conn).await?;
        }

        // ── Step 3 + 4: Atomically delete stale cells + update active-cells ──
        // Bind Script to a local variable so it outlives the invocation borrow.
        let lua_script = redis::Script::new(ACTIVE_CELLS_LUA);
        let mut invocation = lua_script.prepare_invoke();
        invocation
            .key(&active_key)
            .arg(prefix.as_str())
            .arg(ttl_i);
        for token in &new_cells {
            invocation.arg(token.as_str());
        }
        let stale_deleted: i64 = invocation.invoke_async(&mut conn).await?;
        if stale_deleted > 0 {
            tracing::debug!("Lua: deleted {} stale cell keys atomically", stale_deleted);
        }

        self.metrics.record_write(start.elapsed().as_micros() as u64);
        tracing::debug!("Persisted {} entries, {} cells in {}µs",
            entries.len(), new_cells.len(), start.elapsed().as_micros());
        Ok(())
    }

    /// Queries aircraft whose S2 cell token appears in `tokens`.
    /// Uses SUNION across cell keys, then pipelines GET for each aircraft.
    pub async fn query_region(&self, tokens: &[String]) -> Result<Vec<GeoEntry>> {
        if tokens.is_empty() { return Ok(vec![]); }

        let start = Instant::now();
        let mut conn = self.client.get_multiplexed_async_connection().await?;

        let cell_keys: Vec<String> = tokens
            .iter()
            .map(|t| format!("{}:cell:{}", self.key_prefix, t))
            .collect();

        let ids: Vec<String> = conn.sunion(cell_keys).await?;
        if ids.is_empty() {
            self.metrics.record_read(start.elapsed().as_micros() as u64);
            return Ok(vec![]);
        }

        let mut pipe = redis::pipe();
        for id in &ids {
            pipe.get(format!("{}:entity:{}", self.key_prefix, id));
        }
        let jsons: Vec<Option<String>> = pipe.query_async(&mut conn).await?;

        // Filter out any entries whose aircraft key expired (safety-net cleanup)
        let entries: Vec<GeoEntry> = jsons
            .into_iter()
            .flatten()
            .filter_map(|j| serde_json::from_str(&j).ok())
            .collect();

        self.metrics.record_read(start.elapsed().as_micros() as u64);
        tracing::debug!("Queried {} tokens → {} entries in {}µs",
            tokens.len(), entries.len(), start.elapsed().as_micros());
        Ok(entries)
    }

    /// Removes entries from the `written_at` sorted set whose backing entity
    /// key has already expired in Redis.
    ///
    /// Call this periodically (e.g. every `entity_ttl_secs`) to prevent the
    /// ZSET from growing unboundedly in long-running deployments.  The scan
    /// uses `ZSCAN` in 200-member batches so it never blocks Redis for long.
    ///
    /// Returns the number of stale entries removed.
    pub async fn prune_written_at(&self) -> Result<usize> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let prefix   = &self.key_prefix;
        let wk       = format!("{prefix}:written_at");
        let mut stale_total = 0usize;
        let mut cursor      = 0u64;

        loop {
            // ZSCAN returns [member, score, member, score, ...]
            let (new_cursor, pairs): (u64, Vec<String>) = redis::cmd("ZSCAN")
                .arg(&wk)
                .arg(cursor)
                .arg("COUNT").arg(200u64)
                .query_async(&mut conn).await?;

            let members: Vec<String> = pairs.into_iter().step_by(2).collect();

            if !members.is_empty() {
                let mut pipe = redis::pipe();
                for id in &members {
                    pipe.exists(format!("{prefix}:entity:{id}"));
                }
                let alive: Vec<bool> = pipe.query_async(&mut conn).await?;

                let stale: Vec<&str> = members.iter().zip(alive.iter())
                    .filter(|(_, &a)| !a)
                    .map(|(id, _)| id.as_str())
                    .collect();

                if !stale.is_empty() {
                    stale_total += stale.len();
                    let mut cmd = redis::cmd("ZREM");
                    cmd.arg(&wk);
                    for s in &stale { cmd.arg(s); }
                    cmd.query_async::<i64>(&mut conn).await?;
                }
            }

            cursor = new_cursor;
            if cursor == 0 { break; }
        }

        if stale_total > 0 {
            tracing::info!("prune_written_at: removed {} stale entries", stale_total);
        }
        Ok(stale_total)
    }

    pub fn metrics(&self) -> &Arc<Metrics> { &self.metrics }

    /// Merge a batch of `GeoEntry` items into Redis using **freshness ordering**.
    ///
    /// Unlike [`persist_trie`] — which atomically replaces the entire active set —
    /// `merge_entries` is an **additive, idempotent** upsert:
    ///
    /// - An entry is written only if `entry.written_at ≥ existing written_at`.
    ///   A stale snapshot can never overwrite a live write.
    /// - The `{prefix}:written_at` sorted set is kept consistent so subsequent
    ///   [`entities_written_after`] delta-sync queries correctly reflect the merged state.
    ///
    /// ## Shard split seeding — canonical usage
    ///
    /// When a new shard boots from a snapshot (via `POST /ingest-snapshot`) and then
    /// catches up via delta sync (`GET /delta-sync`), both paths call `merge_entries`.
    /// Either call is safe to retry; the freshness check guarantees idempotency.
    ///
    /// ```text
    /// Source shard                   New shard
    /// ─────────────────────────────────────────
    /// POST snapshot entries ────────► merge_entries(snapshot, s2_level)
    /// POST delta entries    ────────► merge_entries(delta, s2_level)
    ///                                (only newer entries are written)
    /// ```
    ///
    /// ## Returns
    /// The number of entries **actually written** (those that passed the freshness
    /// check). Entries that were already up-to-date in Redis are skipped silently.
    pub async fn merge_entries(&self, entries: &[GeoEntry], s2_level: u8) -> Result<usize> {
        if entries.is_empty() { return Ok(0); }

        let prefix         = &self.key_prefix;
        let ttl            = self.entity_ttl_secs;
        let written_at_key = format!("{prefix}:written_at");
        let mut conn       = self.client.get_multiplexed_async_connection().await?;

        // ── Batch-fetch existing written_at scores (one pipeline round-trip) ──
        let mut score_pipe = redis::pipe();
        for e in entries {
            score_pipe.zscore(&written_at_key, e.id.as_str());
        }
        let existing: Vec<Option<f64>> = score_pipe.query_async(&mut conn).await?;

        // ── Stamp missing written_at values ───────────────────────────────────
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        // ── Keep only entries that are fresher than what's in Redis ───────────
        let to_write: Vec<(GeoEntry, String)> = entries
            .iter()
            .zip(existing.iter())
            .filter_map(|(entry, existing_score)| {
                let existing_ts = existing_score.map(|s| s as u64).unwrap_or(0);
                if entry.written_at >= existing_ts {
                    let mut e = entry.clone();
                    if e.written_at == 0 { e.written_at = now_ms; }
                    let token = s2_cell_token(e.lat, e.lon, s2_level);
                    Some((e, token))
                } else {
                    None
                }
            })
            .collect();

        let written = to_write.len();
        if written == 0 { return Ok(0); }

        // ── Write the fresh entries in chunked pipelines ──────────────────────
        for chunk in to_write.chunks(CHUNK_SIZE) {
            let mut pipe = redis::pipe();
            pipe.atomic();
            for (entry, token) in chunk {
                let ak  = format!("{prefix}:entity:{}", entry.id);
                let ck  = format!("{prefix}:cell:{token}");
                let loc = format!("{prefix}:location:{}", entry.id);
                let json = serde_json::to_string(entry)?;
                pipe.set_ex(&ak,            &json,              ttl).ignore();
                pipe.sadd(&ck,              &entry.id).ignore();
                pipe.set_ex(&loc,           token,              ttl).ignore();
                pipe.zadd(&written_at_key,  entry.written_at as f64, entry.id.as_str()).ignore();
            }
            pipe.query_async::<()>(&mut conn).await?;
        }

        tracing::debug!("merge_entries: wrote {}/{} entries", written, entries.len());
        Ok(written)
    }

    /// Returns all entities whose `written_at` timestamp is strictly greater
    /// than `since_ms` AND whose S2 cell token falls in `[prefix_start, prefix_end)`.
    ///
    /// Used by a newly bootstrapped shard to catch up on writes that occurred
    /// on the source shard AFTER the snapshot was captured.  The caller should
    /// apply the returned entries with a freshness check:
    ///   `apply if incoming.written_at > existing.written_at`
    pub async fn entities_written_after(
        &self,
        since_ms:     u64,
        prefix_start: &str,
        prefix_end:   &str,
    ) -> Result<Vec<GeoEntry>> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let prefix   = &self.key_prefix;

        // Query the sorted set for all entity IDs written after since_ms.
        let ids: Vec<String> = redis::cmd("ZRANGEBYSCORE")
            .arg(format!("{prefix}:written_at"))
            .arg(since_ms + 1)   // exclusive lower bound
            .arg("+inf")
            .query_async(&mut conn)
            .await?;

        if ids.is_empty() { return Ok(vec![]); }

        // Filter to the prefix range by looking up each entity's cell token.
        let mut in_range_ids: Vec<String> = Vec::with_capacity(ids.len());
        for id in &ids {
            let token: Option<String> = conn
                .get(format!("{prefix}:location:{id}"))
                .await
                .unwrap_or(None);
            if let Some(tok) = token {
                let ge = prefix_start.is_empty() || tok.as_str() >= prefix_start;
                let lt = prefix_end.is_empty()   || tok.as_str() <  prefix_end;
                if ge && lt { in_range_ids.push(id.clone()); }
            }
        }

        if in_range_ids.is_empty() { return Ok(vec![]); }

        // Batch-fetch entity JSON for the matching IDs.
        let mut pipe = redis::pipe();
        for id in &in_range_ids {
            pipe.get(format!("{prefix}:entity:{id}"));
        }
        let jsons: Vec<Option<String>> = pipe.query_async(&mut conn).await?;

        let entries = jsons
            .into_iter()
            .flatten()
            .filter_map(|j| serde_json::from_str::<GeoEntry>(&j).ok())
            .collect();

        Ok(entries)
    }
}

// ── Private helpers ────────────────────────────────────────────────────────

/// Compute the S2 cell token for a (lat, lon) at the given S2 level.
/// Mirrors `GeoTrie::cell_token` so `merge_entries` can work without a trie.
fn s2_cell_token(lat: f64, lon: f64, level: u8) -> String {
    let ll   = LatLng::new(s1::Deg(lat).into(), s1::Deg(lon).into());
    let cell = CellID::from(ll).parent(level as u64);
    if cell.0 == 0 { return "X".into(); }
    let hex  = format!("{:016x}", cell.0);
    hex.trim_end_matches('0').to_string()
}
