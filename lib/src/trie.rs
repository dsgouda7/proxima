use s2::{cap::Cap, cellid::CellID, latlng::LatLng, point::Point, region::RegionCoverer, s1};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A geographic entry stored in the trie.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeoEntry {
    pub id: String,
    pub lat: f64,
    pub lon: f64,
    pub payload: serde_json::Value,
    /// Unix milliseconds when this entry was last written to a shard.
    /// Set to `SystemTime::now()` by `RedisStore::persist_trie()` when 0.
    /// Used for freshness-ordered delta sync after a geographic split.
    #[serde(default)]
    pub written_at: u64,
}

impl GeoEntry {
    /// Creates an entry whose stable storage identity is an arbitrary JSON
    /// value. The value is canonically encoded into `id`, so equivalent object
    /// values produce the same Redis/trie key regardless of field order.
    ///
    /// The complete value remains available to callers in `payload`; Redis
    /// indexes continue to use the string `id` for efficient set membership
    /// and reverse lookups.
    pub fn from_json_identity(
        identity: serde_json::Value,
        lat: f64,
        lon: f64,
        payload: serde_json::Value,
    ) -> Self {
        Self {
            id: format!("json:{}", canonical_json(&identity)),
            lat,
            lon,
            payload,
            written_at: 0,
        }
    }
}

/// A query result from [`GeoTrie::query_nearby`] or [`RedisStore::query_nearby`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NearbyEntry {
    /// Great-circle distance from the query point to this entity, in metres.
    pub distance_m: f64,
    pub entry: GeoEntry,
}

#[derive(Default)]
struct TrieNode {
    children: HashMap<u8, Box<TrieNode>>,
    entries: Vec<GeoEntry>,
}

/// An S2-cell-keyed trie.
///
/// Each character of the S2 hex token is one trie level, giving
/// O(token_len) insert and exact-cell lookup regardless of dataset size.
/// Neighbour-cell queries are handled by passing multiple tokens to
/// [`GeoTrie::query_tokens`].
pub struct GeoTrie {
    root: TrieNode,
    pub s2_level: u8,
}

impl GeoTrie {
    pub fn new(s2_level: u8) -> Self {
        assert!((1..=30).contains(&s2_level), "S2 level must be 1–30");
        Self {
            root: TrieNode::default(),
            s2_level,
        }
    }

    /// S2 cell token (hex string, trailing zeros trimmed) for a coordinate.
    pub fn cell_token(&self, lat: f64, lon: f64) -> String {
        let ll = LatLng::new(s1::Deg(lat).into(), s1::Deg(lon).into());
        let cell = CellID::from(ll).parent(self.s2_level as u64);
        s2_token(cell)
    }

    pub fn insert(&mut self, entry: GeoEntry) {
        let token = self.cell_token(entry.lat, entry.lon);
        descend_mut(&mut self.root, token.as_bytes())
            .entries
            .push(entry);
    }

    pub fn query_token(&self, token: &str) -> Vec<&GeoEntry> {
        match descend(&self.root, token.as_bytes()) {
            Some(n) => n.entries.iter().collect(),
            None => vec![],
        }
    }

    pub fn query_tokens(&self, tokens: &[String]) -> Vec<GeoEntry> {
        tokens
            .iter()
            .flat_map(|t| self.query_token(t))
            .cloned()
            .collect()
    }

    /// Returns all entries (cloned) — used for Redis persistence.
    pub fn all_entries(&self) -> Vec<GeoEntry> {
        let mut out = Vec::new();
        collect_entries(&self.root, &mut out);
        out
    }

    pub fn clear(&mut self) {
        self.root = TrieNode::default();
    }
    pub fn len(&self) -> usize {
        count_entries(&self.root)
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Remove entity `id` from the node at the given S2 token, then
    /// **prune every ancestor branch node** that becomes empty as a result.
    ///
    /// A branch node is pruned when it has neither entries nor children.
    /// This keeps the trie compact after deletions and eliminates wasted
    /// memory for branches that were created for entities that have since
    /// moved to a different cell or gone offline.
    ///
    /// Returns `true` if the entry was found and removed.
    pub fn remove_at_token(&mut self, token: &str, id: &str) -> bool {
        prune_remove(&mut self.root, token.as_bytes(), id)
    }

    /// Convenience wrapper: compute the token from `(lat, lon)` then remove.
    /// Use this when you know the entity's current position.
    pub fn remove_entry(&mut self, lat: f64, lon: f64, id: &str) -> bool {
        let token = self.cell_token(lat, lon);
        self.remove_at_token(&token, id)
    }

    /// Remove and return all entries whose S2 cell token falls in
    /// `[prefix_start, prefix_end)`.  Empty string means "unbounded".
    ///
    /// Called on the **source shard** immediately after a split so it stops
    /// holding data it is no longer responsible for.  Returns the removed
    /// entries so the caller can confirm the count or log them.
    pub fn remove_range(&mut self, prefix_start: &str, prefix_end: &str) -> Vec<GeoEntry> {
        let in_range: Vec<GeoEntry> = self
            .all_entries()
            .into_iter()
            .filter(|e| {
                let token = self.cell_token(e.lat, e.lon);
                let ge = prefix_start.is_empty() || token.as_str() >= prefix_start;
                let lt = prefix_end.is_empty() || token.as_str() < prefix_end;
                ge && lt
            })
            .collect();
        for e in &in_range {
            self.remove_entry(e.lat, e.lon, &e.id);
        }
        in_range
    }

    /// Count the total number of internal nodes in the trie (entries + branch nodes).
    /// Useful for observing memory savings after bulk pruning.
    pub fn count_nodes(&self) -> usize {
        count_nodes(&self.root)
    }

    /// Returns all entities within `radius_m` metres of `(lat, lon)`, sorted
    /// nearest-first, optionally capped at `top_k` results.
    ///
    /// Uses an S2 cap covering to identify candidate cells (fast, no full scan),
    /// then applies an exact haversine post-filter at the cell boundary so only
    /// entities strictly inside the requested radius are returned.
    pub fn query_nearby(
        &self,
        lat: f64,
        lon: f64,
        radius_m: f64,
        top_k: Option<usize>,
    ) -> Vec<NearbyEntry> {
        let tokens = s2_cap_covering(lat, lon, radius_m, self.s2_level);
        let mut results: Vec<NearbyEntry> = self
            .query_tokens(&tokens)
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
        results
    }
}

// ── private helpers ────────────────────────────────────────────────────────

/// Standard S2 token: 64-bit ID as lowercase hex, trailing zeros stripped.
fn s2_token(cell: CellID) -> String {
    if cell.0 == 0 {
        return "X".into();
    }
    let hex = format!("{:016x}", cell.0);
    hex.trim_end_matches('0').to_string()
}

/// Produces a deterministic JSON representation for use as a stable entry
/// identity. Object keys are sorted recursively; strings are escaped by
/// serde_json so the result remains unambiguous and valid JSON.
fn canonical_json(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null
        | serde_json::Value::Bool(_)
        | serde_json::Value::Number(_)
        | serde_json::Value::String(_) => {
            serde_json::to_string(value).expect("JSON value serializes")
        }
        serde_json::Value::Array(values) => format!(
            "[{}]",
            values
                .iter()
                .map(canonical_json)
                .collect::<Vec<_>>()
                .join(",")
        ),
        serde_json::Value::Object(values) => {
            let mut keys: Vec<&String> = values.keys().collect();
            keys.sort_unstable();
            let fields = keys
                .into_iter()
                .map(|key| {
                    format!(
                        "{}:{}",
                        serde_json::to_string(key).expect("JSON object key serializes"),
                        canonical_json(&values[key])
                    )
                })
                .collect::<Vec<_>>();
            format!("{{{}}}", fields.join(","))
        }
    }
}

fn descend_mut<'a>(node: &'a mut TrieNode, bytes: &[u8]) -> &'a mut TrieNode {
    if bytes.is_empty() {
        return node;
    }
    let child = node
        .children
        .entry(bytes[0])
        .or_insert_with(|| Box::new(TrieNode::default()));
    descend_mut(child, &bytes[1..])
}

fn descend<'a>(node: &'a TrieNode, bytes: &[u8]) -> Option<&'a TrieNode> {
    if bytes.is_empty() {
        return Some(node);
    }
    node.children
        .get(&bytes[0])
        .and_then(|c| descend(c, &bytes[1..]))
}

fn collect_entries(node: &TrieNode, out: &mut Vec<GeoEntry>) {
    out.extend(node.entries.iter().cloned());
    for child in node.children.values() {
        collect_entries(child, out);
    }
}

fn count_entries(node: &TrieNode) -> usize {
    node.entries.len()
        + node
            .children
            .values()
            .map(|c| count_entries(c))
            .sum::<usize>()
}

/// Recursive remove-with-pruning.
///
/// Descends along `bytes`. At the target node, removes the entry with `id`.
/// On the way back up, if any node has become empty (no entries, no children),
/// the parent removes it from its `children` map — pruning the dead branch.
fn prune_remove(node: &mut TrieNode, bytes: &[u8], id: &str) -> bool {
    if bytes.is_empty() {
        // At target node — remove matching entry
        let before = node.entries.len();
        node.entries.retain(|e| e.id != id);
        return node.entries.len() < before;
    }

    let ch = bytes[0];
    if !node.children.contains_key(&ch) {
        return false; // entity not found on this path
    }

    // Recurse — mutable borrow of child is scoped to this block
    let removed = {
        let child = node.children.get_mut(&ch).unwrap();
        prune_remove(child, &bytes[1..], id)
    };

    // After the recursive call the borrow has ended — safe to inspect + prune
    if removed {
        let should_prune = {
            let child = &node.children[&ch];
            child.entries.is_empty() && child.children.is_empty()
        };
        if should_prune {
            node.children.remove(&ch);
        }
    }

    removed
}

fn count_nodes(node: &TrieNode) -> usize {
    1 + node
        .children
        .values()
        .map(|c| count_nodes(c))
        .sum::<usize>()
}

// ── Geospatial helpers ─────────────────────────────────────────────────────

/// Great-circle distance between two points using the Haversine formula.
/// Returns metres.
pub(crate) fn haversine_m(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    const R: f64 = 6_371_000.0;
    let dlat = (lat2 - lat1).to_radians();
    let dlon = (lon2 - lon1).to_radians();
    let a = (dlat / 2.0).sin().powi(2)
        + lat1.to_radians().cos() * lat2.to_radians().cos() * (dlon / 2.0).sin().powi(2);
    2.0 * R * a.sqrt().asin()
}

/// S2 cell tokens (at `level`) whose union covers a spherical cap centred on
/// `(lat, lon)` with radius `radius_m` metres.
pub(crate) fn s2_cap_covering(lat: f64, lon: f64, radius_m: f64, level: u8) -> Vec<String> {
    use std::f64::consts::PI;
    let center = Point::from(LatLng::new(s1::Deg(lat).into(), s1::Deg(lon).into()));
    let radius_rad = (radius_m / 6_371_000.0_f64).min(PI);
    let angle: s1::angle::Angle = s1::Rad(radius_rad).into();
    let cap = Cap::from_center_angle(&center, &angle);
    let coverer = RegionCoverer {
        min_level: level,
        max_level: level,
        level_mod: 1,
        max_cells: 500,
    };
    coverer
        .covering(&cap)
        .0
        .iter()
        .map(|c| {
            let hex = format!("{:016x}", c.0);
            hex.trim_end_matches('0').to_string()
        })
        .collect()
}
