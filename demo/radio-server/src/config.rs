#[derive(Debug, Clone)]
pub struct Config {
    pub server_host: String,
    pub server_port: u16,
    /// Radio Browser API base URL (supports any mirror).
    pub api_base: String,
    /// Maximum stations to fetch (Radio Browser returns ~30 k geo-tagged ones).
    pub fetch_limit: usize,
    /// Re-fetch interval in seconds (stations don't change often; default 6 h).
    pub poll_interval_secs: u64,
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            server_host: env("SERVER_HOST", "0.0.0.0"),
            server_port: env_parse("RADIO_PORT", 3002u16),
            api_base: env(
                "RADIO_API_BASE",
                "https://de1.api.radio-browser.info",
            ),
            fetch_limit: env_parse("RADIO_FETCH_LIMIT", 50_000usize),
            poll_interval_secs: env_parse("RADIO_POLL_SECS", 21_600u64), // 6 h
        }
    }
}

fn env(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.into())
}

fn env_parse<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}
