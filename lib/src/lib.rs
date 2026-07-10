//! # georedis
//!
//! An S2-geometry-keyed trie that persists to Redis.
//! Designed for fast "what's near me" queries over dynamic,
//! frequently updated geographic objects.
//!
//! ## Example
//! ```no_run
//! use georedis::{GeoTrie, GeoEntry, RedisStore, Metrics};
//! use serde_json::json;
//!
//! #[tokio::main]
//! async fn main() -> georedis::Result<()> {
//!     let metrics  = Metrics::new();
//!     let store    = RedisStore::new("redis://127.0.0.1:6379", metrics)?;
//!     let mut trie = GeoTrie::new(9);
//!
//!     trie.insert(GeoEntry {
//!         id:      "abc".into(),
//!         lat:     37.77,
//!         lon:     -122.41,
//!         payload: json!({ "callsign": "UAL123" }),
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
pub use store::{RedisStore, DEFAULT_ENTITY_TTL_SECS};
pub use trie::{GeoEntry, GeoTrie};
