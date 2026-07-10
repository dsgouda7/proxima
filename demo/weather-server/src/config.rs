#[derive(Debug, Clone)]
pub struct Config {
    pub redis_url:          String,
    pub server_host:        String,
    pub server_port:        u16,
    pub poll_interval_secs: u64,
    pub s2_level:           u8,
    pub sqlite_path:        String,
    pub entity_ttl_secs:    u64,
    /// Milliseconds between successive METAR event insertions (default 5ms →
    /// ~5 000 stations stream in ~25 s, map fills up visually in real-time).
    pub stream_rate_ms:     u64,
    /// Maximum number of spatial clusters to show on the map (default 100).
    pub max_clusters:       usize,
    /// Optional explicit S2 aggregation level (3-6). When None the level is
    /// auto-detected so that occupied cells ≤ max_clusters.
    pub cluster_level:      Option<u8>,
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            redis_url:          env("REDIS_URL",           "redis://127.0.0.1:6379/1"),
            server_host:        env("SERVER_HOST",         "0.0.0.0"),
            server_port:        env_parse("SERVER_PORT",   3001),
            poll_interval_secs: env_parse("WEATHER_POLL_SECS", 300u64),
            s2_level:           env_parse("S2_LEVEL",      9),
            sqlite_path:        env("SQLITE_PATH",         "georedis-weather.db"),
            entity_ttl_secs:    env_parse("ENTITY_TTL_SECS", georedis::store::DEFAULT_ENTITY_TTL_SECS),
            stream_rate_ms:     env_parse("STREAM_RATE_MS",  5u64),
            max_clusters:       env_parse("MAX_CLUSTERS",    100usize),
            cluster_level:      std::env::var("CLUSTER_LEVEL").ok()
                                    .and_then(|v| v.parse::<u8>().ok()),
        }
    }
}

fn env(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.into())
}

fn env_parse<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}
