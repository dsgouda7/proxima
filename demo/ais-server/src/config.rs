#[derive(Debug, Clone)]
pub struct Config {
    pub redis_url:           String,
    pub server_host:         String,
    pub server_port:         u16,
    pub s2_level:            u8,
    pub sqlite_path:         String,
    pub entity_ttl_secs:     u64,
    /// Free-tier API key from https://aisstream.io
    pub aisstream_api_key:   String,
    /// How often (seconds) the in-memory vessel cache is flushed to the trie + Redis.
    pub sync_interval_secs:  u64,
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            redis_url:          env("REDIS_URL",           "redis://127.0.0.1:6379"),
            server_host:        env("SERVER_HOST",         "0.0.0.0"),
            server_port:        env_parse("SERVER_PORT",   3002),
            s2_level:           env_parse("S2_LEVEL",      9),
            sqlite_path:        env("SQLITE_PATH",         "georedis-ais.db"),
            entity_ttl_secs:    env_parse("ENTITY_TTL_SECS", georedis::store::DEFAULT_ENTITY_TTL_SECS),
            aisstream_api_key:  env("AISSTREAM_API_KEY",   ""),
            sync_interval_secs: env_parse("AIS_SYNC_INTERVAL_SECS", 5),
        }
    }
}

fn env(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.into())
}

fn env_parse<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}
