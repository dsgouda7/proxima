use std::collections::HashMap;
use serde::{Deserialize, Serialize};

/// One shard node in the geo-cluster ring.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NodeInfo {
    pub node_id:      String,
    /// HTTP address used for gossip and client routing (host:port)
    pub addr:         String,
    /// Redis connection URL for this node's backing store
    pub redis_url:    String,
    /// Start of S2 token prefix range (inclusive). Empty string = ring start.
    pub prefix_start: String,
    /// End of S2 token prefix range (exclusive). Empty string = ring end.
    pub prefix_end:   String,
    pub key_count:    u64,
    pub mem_bytes:    u64,
    /// Monotonically increasing — higher generation wins merge conflicts
    pub generation:   u64,
    pub status:       NodeStatus,
    pub last_seen_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum NodeStatus {
    Active,
    Splitting,
    Merging,
    Suspect,
    Dead,
    Standby,
    /// Node has loaded its snapshot and is performing delta-sync catch-up.
    /// It accepts no new writes until it transitions to Active.
    Bootstrapping,
}

impl NodeInfo {
    /// Returns true if this node is responsible for `token`.
    pub fn owns(&self, token: &str) -> bool {
        let ge_start = self.prefix_start.is_empty() || token >= self.prefix_start.as_str();
        let lt_end   = self.prefix_end.is_empty()   || token <  self.prefix_end.as_str();
        ge_start && lt_end
    }
}

// ── Cluster ring ───────────────────────────────────────────────────────────

/// In-memory view of the entire cluster topology.
/// Every node maintains its own copy, kept fresh by gossip.
#[derive(Debug, Default, Clone)]
pub struct ClusterRing {
    nodes: HashMap<String, NodeInfo>,  // node_id → NodeInfo
}

impl ClusterRing {
    pub fn from_nodes(nodes: Vec<NodeInfo>) -> Self {
        Self { nodes: nodes.into_iter().map(|n| (n.node_id.clone(), n)).collect() }
    }

    /// O(N_shards) routing — N is tiny (3–64 shards), so this is effectively O(1).
    pub fn route(&self, token: &str) -> Option<&NodeInfo> {
        self.nodes.values()
            .filter(|n| matches!(n.status, NodeStatus::Active | NodeStatus::Splitting))
            .find(|n| n.owns(token))
    }

    /// Group tokens by their owning shard address.
    /// Enables parallel fan-out for viewport queries — a zoomed-in query
    /// typically hits exactly 1 shard; a global query hits at most 6.
    pub fn group_by_shard<'a>(
        &'a self,
        tokens: &'a [String],
    ) -> HashMap<String, Vec<&'a str>> {
        let mut groups: HashMap<String, Vec<&str>> = HashMap::new();
        for tok in tokens {
            if let Some(node) = self.route(tok) {
                groups.entry(node.addr.clone()).or_default().push(tok.as_str());
            }
        }
        groups
    }

    /// Merge a gossip update. Higher generation wins.
    pub fn merge(&mut self, incoming: NodeInfo) {
        let entry = self.nodes
            .entry(incoming.node_id.clone())
            .or_insert_with(|| incoming.clone());
        if incoming.generation > entry.generation
            || (incoming.generation == entry.generation
                && incoming.last_seen_secs > entry.last_seen_secs)
        {
            *entry = incoming;
        }
    }

    pub fn get(&self, node_id: &str) -> Option<&NodeInfo> {
        self.nodes.get(node_id)
    }

    pub fn all_nodes(&self) -> impl Iterator<Item = &NodeInfo> {
        self.nodes.values()
    }

    pub fn active_nodes(&self) -> impl Iterator<Item = &NodeInfo> {
        self.nodes.values()
            .filter(|n| matches!(n.status, NodeStatus::Active | NodeStatus::Splitting))
    }

    pub fn as_vec(&self) -> Vec<NodeInfo> {
        self.nodes.values().cloned().collect()
    }
}
