//! # geo-redis
//!
//! A distributed S2-geometry-keyed trie that persists to Redis.
//! Designed for sub-10 ms "what's near me" queries over dynamic,
//! frequently updated geographic objects — aircraft, couriers, IoT devices.
//!
//! ## Example
//! ```no_run
//! use geo_redis::{GeoTrie, GeoEntry, RedisStore, Metrics};
//! use serde_json::json;
//!
//! #[tokio::main]
//! async fn main() -> geo_redis::Result<()> {
//!     let metrics  = Metrics::new();
//!     let store    = RedisStore::new("redis://127.0.0.1:6379", metrics)?;
//!     let mut trie = GeoTrie::new(9);
//!
//!     trie.insert(GeoEntry {
//!         id:         "abc".into(),
//!         lat:        37.77,
//!         lon:        -122.41,
//!         payload:    json!({ "callsign": "UAL123" }),
//!         written_at: 0,
//!     });
//!
//!     store.persist_trie(&trie).await?;
//!     Ok(())
//! }
//! ```

pub mod cluster;
pub mod error;
pub mod metrics;
pub mod store;
pub mod trie;

pub use error::{Error, Result};
pub use metrics::{Metrics, MetricsSnapshot};
pub use store::{GeoStore, RedisStore, DEFAULT_ENTITY_TTL_SECS};
pub use trie::{GeoEntry, GeoTrie, NearbyEntry};
