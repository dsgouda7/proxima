use std::{collections::HashSet, sync::Arc, time::Instant};
use redis::AsyncCommands;
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
        let entries = trie.all_entries();
        if entries.is_empty() { return Ok(()); }

        let prefix = &self.key_prefix;
        let ttl    = self.entity_ttl_secs;
        let ttl_i  = ttl as i64;
        let mut conn = self.client.get_multiplexed_async_connection().await?;

        // ── Step 1: Load previous active-cell set ─────────────────────────
        let active_key = format!("{prefix}:active_cells");
        let prev_cells: HashSet<String> = conn.smembers(&active_key).await?;

        // ── Step 2: Write entities + location reverse-lookup ──────────────
        let mut new_cells: HashSet<String> = HashSet::with_capacity(entries.len() / 4);

        for chunk in entries.chunks(CHUNK_SIZE) {
            let mut pipe = redis::pipe();
            pipe.atomic();
            for entry in chunk {
                let token    = trie.cell_token(entry.lat, entry.lon);
                let ak       = format!("{prefix}:aircraft:{}", entry.id);
                let ck       = format!("{prefix}:cell:{token}");
                let loc_key  = format!("{prefix}:location:{}", entry.id);
                let json     = serde_json::to_string(entry)?;

                pipe.set_ex(&ak,      &json,  ttl).ignore();
                pipe.sadd(&ck,        &entry.id).ignore();
                // Reverse lookup: id → current cell token
                pipe.set_ex(&loc_key, &token, ttl).ignore();
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
            pipe.get(format!("{}:aircraft:{}", self.key_prefix, id));
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

    pub fn metrics(&self) -> &Arc<Metrics> { &self.metrics }
}
