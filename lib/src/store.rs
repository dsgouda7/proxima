use crate::{GeoEntry, GeoTrie, Metrics, Result};
use crate::trie::{haversine_m, s2_cap_covering, NearbyEntry};
use redis::AsyncCommands;
use s2::{cellid::CellID, latlng::LatLng, s1};
use std::{collections::HashSet, sync::Arc, time::Instant};

/// Default safety-net TTL: 10 minutes.
pub const DEFAULT_ENTITY_TTL_SECS: u64 = 600;
const CHUNK_SIZE: usize = 400;

// Compile-time proof that RedisStore is safe to share across async tasks.
const _: () = {
    fn assert_send_sync<T: Send + Sync>() {}
    fn check() {
        assert_send_sync::<RedisStore>();
    }
    let _ = check;
};

// ── Redis client kind (single-node vs cluster) ─────────────────────────────

/// Underlying Redis client — single-node or Cluster mode.
///
/// In **cluster mode** all keys MUST use a hash tag so they land on the same
/// slot (required for `SUNION`, Lua scripts, and atomic pipelines to work).
/// `RedisStore` automatically adds `{namespace}` hash tags to every key it
/// writes, so cluster mode is transparent to callers.
pub enum RedisClientKind {
    /// Single Redis instance or Sentinel (connection string in REDIS_URL).
    Single(redis::Client),
    /// Redis Cluster — pass the addresses of any/all cluster nodes.
    Cluster(redis::cluster::ClusterClient),
}

/// Dispatch macro: run the same body against either a single-node or cluster
/// connection.  Both `MultiplexedConnection` and `ClusterConnection` implement
/// `AsyncCommands + ConnectionLike`, so the body compiles for both.
macro_rules! with_conn {
    ($self:expr, $conn:ident, $body:block) => {
        match &$self.client {
            RedisClientKind::Single(c) => {
                let mut $conn = c.get_multiplexed_async_connection().await?;
                $body
            }
            RedisClientKind::Cluster(c) => {
                let mut $conn = c.get_async_connection().await?;
                $body
            }
        }
    };
}

/// Lua script for atomic active-cell diffing.
/// Keys and ARGV values already include the `{namespace}` hash tag so all
/// keys resolve to the same Redis Cluster slot.
const ACTIVE_CELLS_LUA: &str = r#"
local active_key = KEYS[1]
local cell_pfx   = ARGV[1]
local ttl        = tonumber(ARGV[2])

local prev_cells = redis.call('SMEMBERS', active_key)

local new_set = {}
for i = 3, #ARGV do new_set[ARGV[i]] = true end

local deleted = 0
for _, token in ipairs(prev_cells) do
    if not new_set[token] then
        redis.call('DEL', cell_pfx .. token)
        deleted = deleted + 1
    end
end

redis.call('DEL', active_key)
for i = 3, #ARGV do redis.call('SADD', active_key, ARGV[i]) end
if ttl > 0 then redis.call('EXPIRE', active_key, ttl) end

return deleted
"#;

pub struct RedisStore {
    client: RedisClientKind,
    metrics: Arc<Metrics>,
    key_prefix: String,
    entity_ttl_secs: u64,
}

impl RedisStore {
    /// Create a single-node store with the default 10-minute safety-net TTL.
    pub fn new(redis_url: &str, metrics: Arc<Metrics>) -> Result<Self> {
        Self::with_config(redis_url, metrics, DEFAULT_ENTITY_TTL_SECS)
    }

    /// Create a single-node store with an explicit entity TTL.
    pub fn with_config(
        redis_url: &str,
        metrics: Arc<Metrics>,
        entity_ttl_secs: u64,
    ) -> Result<Self> {
        Ok(Self {
            client: RedisClientKind::Single(redis::Client::open(redis_url)?),
            metrics,
            key_prefix: "geo-redis".into(),
            entity_ttl_secs,
        })
    }

    /// Create a **Redis Cluster** store.
    ///
    /// Pass the addresses of one or more cluster nodes (any or all); the client
    /// discovers the full topology via `CLUSTER SLOTS`.
    ///
    /// ```ignore
    /// let store = RedisStore::new_cluster(
    ///     vec!["redis://node1:6379", "redis://node2:6379"],
    ///     metrics,
    ///     120,
    /// )?;
    /// ```
    pub fn new_cluster(
        node_urls: Vec<String>,
        metrics: Arc<Metrics>,
        entity_ttl_secs: u64,
    ) -> Result<Self> {
        let client = redis::cluster::ClusterClient::new(node_urls).map_err(crate::Error::Redis)?;
        Ok(Self {
            client: RedisClientKind::Cluster(client),
            metrics,
            key_prefix: "geo-redis".into(),
            entity_ttl_secs,
        })
    }

    /// Override the Redis key namespace (default: `"geo-redis"`).
    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.key_prefix = namespace.into();
        self
    }

    /// Returns the Redis key namespace used by this store.
    pub fn key_prefix(&self) -> &str {
        &self.key_prefix
    }

    /// Returns `true` when operating in Redis Cluster mode.
    pub fn is_cluster(&self) -> bool {
        matches!(self.client, RedisClientKind::Cluster(_))
    }

    // ── Key builder helpers ─────────────────────────────────────────────────
    //
    // All keys use a `{namespace}` hash tag so every key for a given store
    // instance hashes to the SAME Redis Cluster slot.  This is the mandatory
    // requirement for multi-key operations (SUNION, Lua, pipelines) to work
    // in cluster mode.
    //
    // Key format:  {<namespace>}:<type>:<identifier>
    // Example:     {geo-redis}:cell:487a3

    /// Entity payload key.  `{ns}:entity:{id}`
    pub fn k_entity(&self, id: &str) -> String {
        format!("{{{}}}:entity:{id}", self.key_prefix)
    }
    /// Spatial cell index key.  `{ns}:cell:{token}`
    pub fn k_cell(&self, token: &str) -> String {
        format!("{{{}}}:cell:{token}", self.key_prefix)
    }
    /// Reverse-lookup key (id → current cell token).  `{ns}:location:{id}`
    pub fn k_location(&self, id: &str) -> String {
        format!("{{{}}}:location:{id}", self.key_prefix)
    }
    /// Write-timestamp sorted set.  `{ns}:written_at`
    pub fn k_written_at(&self) -> String {
        format!("{{{}}}:written_at", self.key_prefix)
    }
    /// Active-cells tracking set.  `{ns}:active_cells`
    pub fn k_active_cells(&self) -> String {
        format!("{{{}}}:active_cells", self.key_prefix)
    }
    /// Range-claim CAS lock.  `{ns}:range_claim:{prefix_start}`
    pub fn k_range_claim(&self, prefix_start: &str) -> String {
        format!("{{{}}}:range_claim:{prefix_start}", self.key_prefix)
    }
    /// SCAN/MATCH pattern for all entity keys.
    pub fn k_entity_pattern(&self) -> String {
        format!("{{{}}}:entity:*", self.key_prefix)
    }
    /// SCAN/MATCH pattern for all cell keys.
    pub fn k_cell_pattern(&self) -> String {
        format!("{{{}}}:cell:*", self.key_prefix)
    }
    /// Strip the cell key prefix to get just the token.
    pub fn strip_cell_prefix<'a>(&self, key: &'a str) -> &'a str {
        let pfx = format!("{{{}}}:cell:", self.key_prefix);
        key.strip_prefix(pfx.as_str()).unwrap_or(key)
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
        let start = Instant::now();
        let mut entries = trie.all_entries();
        if entries.is_empty() {
            return Ok(());
        }

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        for e in &mut entries {
            if e.written_at == 0 {
                e.written_at = now_ms;
            }
        }

        let ttl = self.entity_ttl_secs;
        let ttl_i = ttl as i64;
        let active_key = self.k_active_cells();
        let written_at_key = self.k_written_at();
        // Lua receives the full cell-key prefix (with hash tag) so it can construct
        // keys like `{geo-redis}:cell:{token}` without reformatting.
        let cell_pfx = format!("{{{}}}:cell:", self.key_prefix);

        let mut new_cells: HashSet<String> = HashSet::with_capacity(entries.len() / 4);

        with_conn!(self, conn, {
            for chunk in entries.chunks(CHUNK_SIZE) {
                let mut pipe = redis::pipe();
                pipe.atomic();
                for entry in chunk {
                    let token = trie.cell_token(entry.lat, entry.lon);
                    let json = serde_json::to_string(entry)?;
                    pipe.set_ex(self.k_entity(&entry.id), &json, ttl).ignore();
                    pipe.sadd(self.k_cell(&token), &entry.id).ignore();
                    pipe.set_ex(self.k_location(&entry.id), &token, ttl)
                        .ignore();
                    pipe.cmd("ZADD")
                        .arg(&written_at_key)
                        .arg(entry.written_at as f64)
                        .arg(entry.id.as_str())
                        .ignore();
                    new_cells.insert(token);
                }
                pipe.query_async::<()>(&mut conn).await?;
            }

            let lua_script = redis::Script::new(ACTIVE_CELLS_LUA);
            let mut inv = lua_script.prepare_invoke();
            inv.key(&active_key).arg(&cell_pfx).arg(ttl_i);
            for token in &new_cells {
                inv.arg(token.as_str());
            }
            let stale: i64 = inv.invoke_async(&mut conn).await?;
            if stale > 0 {
                tracing::debug!("Lua: removed {} stale cell keys", stale);
            }

            self.metrics
                .record_write(start.elapsed().as_micros() as u64);
            tracing::debug!(
                "Persisted {} entries in {}µs",
                entries.len(),
                start.elapsed().as_micros()
            );
            Ok(())
        })
    }

    pub async fn query_region(&self, tokens: &[String]) -> Result<Vec<GeoEntry>> {
        if tokens.is_empty() {
            return Ok(vec![]);
        }
        let start = Instant::now();

        with_conn!(self, conn, {
            let cell_keys: Vec<String> = tokens.iter().map(|t| self.k_cell(t)).collect();
            let ids: Vec<String> = conn.sunion(cell_keys).await?;
            if ids.is_empty() {
                self.metrics.record_read(start.elapsed().as_micros() as u64);
                return Ok(vec![]);
            }
            let mut pipe = redis::pipe();
            for id in &ids {
                pipe.get(self.k_entity(id));
            }
            let jsons: Vec<Option<String>> = pipe.query_async(&mut conn).await?;
            let entries: Vec<GeoEntry> = jsons
                .into_iter()
                .flatten()
                .filter_map(|j| serde_json::from_str(&j).ok())
                .collect();
            self.metrics.record_read(start.elapsed().as_micros() as u64);
            tracing::debug!(
                "Queried {} tokens → {} entries in {}µs",
                tokens.len(),
                entries.len(),
                start.elapsed().as_micros()
            );
            Ok(entries)
        })
    }

    /// Returns entities within `radius_m` metres of `(lat, lon)` from Redis,
    /// sorted nearest-first, optionally capped at `top_k` results.
    ///
    /// Computes an S2 cap covering for the search area, fetches candidate
    /// entities from Redis via `query_region`, then applies an exact haversine
    /// post-filter so only entities strictly inside the radius are returned.
    pub async fn query_nearby(
        &self,
        lat: f64,
        lon: f64,
        radius_m: f64,
        s2_level: u8,
        top_k: Option<usize>,
    ) -> Result<Vec<NearbyEntry>> {
        let start = Instant::now();
        let tokens = s2_cap_covering(lat, lon, radius_m, s2_level);
        let candidates = self.query_region(&tokens).await?;
        let mut results: Vec<NearbyEntry> = candidates
            .into_iter()
            .filter_map(|e| {
                let dist = haversine_m(lat, lon, e.lat, e.lon);
                if dist <= radius_m {
                    Some(NearbyEntry { distance_m: dist, entry: e })
                } else {
                    None
                }
            })
            .collect();
        results.sort_by(|a, b| a.distance_m.partial_cmp(&b.distance_m).unwrap());
        if let Some(k) = top_k {
            results.truncate(k);
        }
        self.metrics.record_nearby(start.elapsed().as_micros() as u64);
        Ok(results)
    }

    /// Returns the number of stale entries removed.
    pub async fn prune_written_at(&self) -> Result<usize> {
        let wk = self.k_written_at();
        let mut stale_total = 0usize;
        let mut cursor = 0u64;

        with_conn!(self, conn, {
            loop {
                let (new_cursor, pairs): (u64, Vec<String>) = redis::cmd("ZSCAN")
                    .arg(&wk)
                    .arg(cursor)
                    .arg("COUNT")
                    .arg(200u64)
                    .query_async(&mut conn)
                    .await?;
                let members: Vec<String> = pairs.into_iter().step_by(2).collect();
                if !members.is_empty() {
                    let mut pipe = redis::pipe();
                    for id in &members {
                        pipe.exists(self.k_entity(id));
                    }
                    let alive: Vec<bool> = pipe.query_async(&mut conn).await?;
                    let stale: Vec<&str> = members
                        .iter()
                        .zip(alive.iter())
                        .filter(|(_, &a)| !a)
                        .map(|(id, _)| id.as_str())
                        .collect();
                    if !stale.is_empty() {
                        stale_total += stale.len();
                        let mut cmd = redis::cmd("ZREM");
                        cmd.arg(&wk);
                        for s in &stale {
                            cmd.arg(s);
                        }
                        cmd.query_async::<i64>(&mut conn).await?;
                    }
                }
                cursor = new_cursor;
                if cursor == 0 {
                    break;
                }
            }
            if stale_total > 0 {
                tracing::info!("prune_written_at: removed {} stale entries", stale_total);
            }
            Ok(stale_total)
        })
    }

    pub fn metrics(&self) -> &Arc<Metrics> {
        &self.metrics
    }

    pub async fn merge_entries(&self, entries: &[GeoEntry], s2_level: u8) -> Result<usize> {
        if entries.is_empty() {
            return Ok(0);
        }
        let ttl = self.entity_ttl_secs;
        let written_at_key = self.k_written_at();
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        with_conn!(self, conn, {
            let mut score_pipe = redis::pipe();
            for e in entries {
                score_pipe.zscore(&written_at_key, e.id.as_str());
            }
            let existing: Vec<Option<f64>> = score_pipe.query_async(&mut conn).await?;

            let to_write: Vec<(GeoEntry, String)> = entries
                .iter()
                .zip(existing.iter())
                .filter_map(|(entry, existing_score)| {
                    let existing_ts = existing_score.map(|s| s as u64).unwrap_or(0);
                    if entry.written_at >= existing_ts {
                        let mut e = entry.clone();
                        if e.written_at == 0 {
                            e.written_at = now_ms;
                        }
                        let token = s2_cell_token(e.lat, e.lon, s2_level);
                        Some((e, token))
                    } else {
                        None
                    }
                })
                .collect();

            let written = to_write.len();
            if written == 0 {
                return Ok(0);
            }

            for chunk in to_write.chunks(CHUNK_SIZE) {
                let mut pipe = redis::pipe();
                pipe.atomic();
                for (entry, token) in chunk {
                    let json = serde_json::to_string(entry)?;
                    pipe.set_ex(self.k_entity(&entry.id), &json, ttl).ignore();
                    pipe.sadd(self.k_cell(token), &entry.id).ignore();
                    pipe.set_ex(self.k_location(&entry.id), token, ttl).ignore();
                    pipe.cmd("ZADD")
                        .arg(&written_at_key)
                        .arg(entry.written_at as f64)
                        .arg(entry.id.as_str())
                        .ignore();
                }
                pipe.query_async::<()>(&mut conn).await?;
            }
            tracing::debug!("merge_entries: wrote {}/{} entries", written, entries.len());
            Ok(written)
        })
    }

    pub async fn entities_written_after(
        &self,
        since_ms: u64,
        prefix_start: &str,
        prefix_end: &str,
    ) -> Result<Vec<GeoEntry>> {
        let wk = self.k_written_at();

        with_conn!(self, conn, {
            let ids: Vec<String> = redis::cmd("ZRANGEBYSCORE")
                .arg(&wk)
                .arg(since_ms + 1)
                .arg("+inf")
                .query_async(&mut conn)
                .await?;
            if ids.is_empty() {
                return Ok(vec![]);
            }

            let mut loc_pipe = redis::pipe();
            for id in &ids {
                loc_pipe.get(self.k_location(id));
            }
            let tokens: Vec<Option<String>> = loc_pipe.query_async(&mut conn).await?;

            let in_range_ids: Vec<String> = ids
                .iter()
                .zip(tokens.iter())
                .filter_map(|(id, tok)| {
                    tok.as_ref().and_then(|t| {
                        let ge = prefix_start.is_empty() || t.as_str() >= prefix_start;
                        let lt = prefix_end.is_empty() || t.as_str() < prefix_end;
                        if ge && lt {
                            Some(id.clone())
                        } else {
                            None
                        }
                    })
                })
                .collect();
            if in_range_ids.is_empty() {
                return Ok(vec![]);
            }

            let mut pipe = redis::pipe();
            for id in &in_range_ids {
                pipe.get(self.k_entity(id));
            }
            let jsons: Vec<Option<String>> = pipe.query_async(&mut conn).await?;

            let entries = jsons
                .into_iter()
                .flatten()
                .filter_map(|j| serde_json::from_str::<GeoEntry>(&j).ok())
                .collect();
            Ok(entries)
        })
    }
}

// ── GeoStore trait ─────────────────────────────────────────────────────────

/// Async abstraction over the geospatial persistence layer.
///
/// `RedisStore` implements this trait. Define your own implementation to
/// inject an in-memory mock in unit tests without requiring a running Redis:
///
/// ```ignore
/// use geo-redis::{GeoStore, GeoEntry, GeoTrie, Metrics, Result};
/// use std::sync::Arc;
///
/// struct MockStore;
/// impl GeoStore for MockStore {
///     async fn merge_entries(&self, _: &[GeoEntry], _: u8) -> Result<usize> { Ok(0) }
///     async fn entities_written_after(&self, ..) -> Result<Vec<GeoEntry>> { Ok(vec![]) }
///     async fn prune_written_at(&self) -> Result<usize> { Ok(0) }
///     async fn persist_trie(&self, _: &GeoTrie) -> Result<()> { Ok(()) }
///     async fn query_region(&self, _: &[String]) -> Result<Vec<GeoEntry>> { Ok(vec![]) }
///     fn metrics(&self) -> &Arc<Metrics> { unimplemented!() }
/// }
/// ```
///
/// ## `Send + Sync` contract
///
/// All implementations **must** be `Send + Sync` so they can be wrapped in
/// `Arc<dyn GeoStore + Send + Sync>` and shared across Tokio tasks.  The
/// generated async futures are also required to be `Send` — this is enforced
/// automatically when the implementor itself is `Send + Sync`.
pub trait GeoStore: Send + Sync {
    /// Idempotent, freshness-ordered upsert.
    fn merge_entries<'a>(
        &'a self,
        entries: &'a [GeoEntry],
        s2_level: u8,
    ) -> impl std::future::Future<Output = Result<usize>> + Send + 'a;

    /// Returns every entity in `[prefix_start, prefix_end)` whose `written_at`
    /// timestamp is strictly greater than `since_ms`.
    fn entities_written_after<'a>(
        &'a self,
        since_ms: u64,
        prefix_start: &'a str,
        prefix_end: &'a str,
    ) -> impl std::future::Future<Output = Result<Vec<GeoEntry>>> + Send + 'a;

    /// Scans the `written_at` sorted set and removes stale members.
    fn prune_written_at(&self) -> impl std::future::Future<Output = Result<usize>> + Send + '_;

    /// Bulk-replaces the entire active entity set from a `GeoTrie` snapshot.
    fn persist_trie<'a>(
        &'a self,
        trie: &'a GeoTrie,
    ) -> impl std::future::Future<Output = Result<()>> + Send + 'a;

    /// Returns all entities whose S2 cell tokens appear in `tokens`.
    fn query_region<'a>(
        &'a self,
        tokens: &'a [String],
    ) -> impl std::future::Future<Output = Result<Vec<GeoEntry>>> + Send + 'a;

    /// Access runtime read/write latency metrics.
    fn metrics(&self) -> &Arc<Metrics>;
}

impl GeoStore for RedisStore {
    async fn merge_entries(&self, entries: &[GeoEntry], s2_level: u8) -> Result<usize> {
        self.merge_entries(entries, s2_level).await
    }
    async fn entities_written_after(
        &self,
        since_ms: u64,
        prefix_start: &str,
        prefix_end: &str,
    ) -> Result<Vec<GeoEntry>> {
        self.entities_written_after(since_ms, prefix_start, prefix_end)
            .await
    }
    async fn prune_written_at(&self) -> Result<usize> {
        self.prune_written_at().await
    }
    async fn persist_trie(&self, trie: &GeoTrie) -> Result<()> {
        self.persist_trie(trie).await
    }
    async fn query_region(&self, tokens: &[String]) -> Result<Vec<GeoEntry>> {
        self.query_region(tokens).await
    }
    fn metrics(&self) -> &Arc<Metrics> {
        self.metrics()
    }
}

// ── Private helpers ────────────────────────────────────────────────────────

/// Compute the S2 cell token for a (lat, lon) at the given S2 level.
/// Mirrors `GeoTrie::cell_token` so `merge_entries` can work without a trie.
fn s2_cell_token(lat: f64, lon: f64, level: u8) -> String {
    let ll = LatLng::new(s1::Deg(lat).into(), s1::Deg(lon).into());
    let cell = CellID::from(ll).parent(level as u64);
    if cell.0 == 0 {
        return "X".into();
    }
    let hex = format!("{:016x}", cell.0);
    hex.trim_end_matches('0').to_string()
}
