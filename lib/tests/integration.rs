use georedis::{GeoEntry, GeoTrie};
use serde_json::json;

// ── helpers ────────────────────────────────────────────────────────────────

fn entry(id: &str, lat: f64, lon: f64) -> GeoEntry {
    GeoEntry { id: id.into(), lat, lon, payload: json!({ "test": true }), written_at: 0 }
}

// ── basic correctness ──────────────────────────────────────────────────────

#[test]
fn insert_and_exact_query() {
    let mut trie = GeoTrie::new(9);
    trie.insert(entry("abc", 37.7749, -122.4194));
    let token   = trie.cell_token(37.7749, -122.4194);
    let results = trie.query_token(&token);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].id, "abc");
}

#[test]
fn query_nonexistent_token_returns_empty() {
    let trie = GeoTrie::new(9);
    assert!(trie.query_token("deadbeef").is_empty());
}

#[test]
fn multiple_entries_inserted_count_correctly() {
    let mut trie = GeoTrie::new(9);
    trie.insert(entry("a0", 37.77, -122.41));
    trie.insert(entry("a1", 37.78, -122.42));
    trie.insert(entry("a2", 37.76, -122.40));
    assert_eq!(trie.len(), 3);
}

#[test]
fn clear_empties_trie() {
    let mut trie = GeoTrie::new(9);
    trie.insert(entry("x", 0.0, 0.0));
    assert!(!trie.is_empty());
    trie.clear();
    assert!(trie.is_empty());
    assert_eq!(trie.len(), 0);
}

// ── multi-token query ─────────────────────────────────────────────────────

#[test]
fn query_tokens_aggregates_multiple_cells() {
    let mut trie = GeoTrie::new(9);
    trie.insert(entry("sf", 37.77, -122.41));
    trie.insert(entry("la", 34.05, -118.24));
    trie.insert(entry("ny", 40.71,  -74.01));

    let tok_sf = trie.cell_token(37.77, -122.41);
    let tok_la = trie.cell_token(34.05, -118.24);
    let results = trie.query_tokens(&[tok_sf, tok_la]);

    assert_eq!(results.len(), 2);
    assert!(results.iter().any(|e| e.id == "sf"));
    assert!(results.iter().any(|e| e.id == "la"));
    assert!(!results.iter().any(|e| e.id == "ny"));
}

#[test]
fn query_tokens_empty_slice_returns_empty() {
    let trie = GeoTrie::new(9);
    assert!(trie.query_tokens(&[]).is_empty());
}

// ── all_entries ────────────────────────────────────────────────────────────

#[test]
fn all_entries_round_trips_all_data() {
    let mut trie = GeoTrie::new(9);
    let coords = [
        ("sydney",   -33.87,  151.21),
        ("london",    51.51,   -0.13),
        ("newyork",   40.71,  -74.01),
        ("tokyo",     35.68,  139.69),
        ("capetown", -33.93,   18.42),
    ];
    for (id, lat, lon) in coords {
        trie.insert(entry(id, lat, lon));
    }
    assert_eq!(trie.len(), coords.len());
    let all = trie.all_entries();
    assert_eq!(all.len(), coords.len());
    for (id, _, _) in coords {
        assert!(all.iter().any(|e| e.id == id), "missing: {id}");
    }
}

// ── geographic edge cases ─────────────────────────────────────────────────

#[test]
fn near_south_pole() {
    let mut trie = GeoTrie::new(9);
    trie.insert(entry("amundsen", -89.9, 0.0));
    let tok = trie.cell_token(-89.9, 0.0);
    assert_eq!(trie.query_token(&tok).len(), 1);
}

#[test]
fn near_north_pole() {
    let mut trie = GeoTrie::new(9);
    trie.insert(entry("north", 89.9, 0.0));
    let tok = trie.cell_token(89.9, 0.0);
    assert_eq!(trie.query_token(&tok).len(), 1);
}

#[test]
fn antimeridian_east_side() {
    let mut trie = GeoTrie::new(9);
    trie.insert(entry("fiji-e", -17.71, 179.9));
    let tok = trie.cell_token(-17.71, 179.9);
    assert_eq!(trie.query_token(&tok).len(), 1);
}

#[test]
fn antimeridian_west_side() {
    let mut trie = GeoTrie::new(9);
    trie.insert(entry("fiji-w", -17.71, -179.9));
    let tok = trie.cell_token(-17.71, -179.9);
    assert_eq!(trie.query_token(&tok).len(), 1);
}

#[test]
fn prime_meridian_and_equator() {
    let mut trie = GeoTrie::new(9);
    trie.insert(entry("null-island", 0.0, 0.0));
    trie.insert(entry("greenwich", 51.48, 0.0));
    assert_eq!(trie.len(), 2);
    assert_eq!(trie.query_token(&trie.cell_token(0.0, 0.0)).len(), 1);
}

// ── different S2 levels ───────────────────────────────────────────────────

#[test]
fn level_12_fine_separates_distant_points() {
    let trie = GeoTrie::new(12); // ~2 km cells
    // Eiffel Tower vs Notre-Dame (~3.5 km apart)
    let tok_a = trie.cell_token(48.8584, 2.2945);
    let tok_b = trie.cell_token(48.8606, 2.3376);
    assert_ne!(tok_a, tok_b);
}

// ── payload round-trip ────────────────────────────────────────────────────

#[test]
fn payload_preserved_on_query() {
    let mut trie = GeoTrie::new(9);
    trie.insert(GeoEntry {
        id:      "ua123".into(),
        lat:     41.97,
        lon:    -87.91,
        payload: json!({ "callsign": "UAL123", "altitude": 10600, "on_ground": false }),
        written_at: 0,
    });
    let tok = trie.cell_token(41.97, -87.91);
    let r   = trie.query_token(&tok);
    assert_eq!(r.len(), 1);
    assert_eq!(r[0].payload["callsign"], "UAL123");
    assert_eq!(r[0].payload["altitude"], 10600);
    assert_eq!(r[0].payload["on_ground"], false);
}

// ── remove_at_token + branch pruning ─────────────────────────────────────

#[test]
fn remove_existing_entry_returns_true() {
    let mut trie = GeoTrie::new(9);
    trie.insert(entry("x", 51.5, -0.1));
    let tok = trie.cell_token(51.5, -0.1);
    assert!(trie.remove_at_token(&tok, "x"));
    assert!(trie.is_empty());
}

#[test]
fn remove_nonexistent_returns_false() {
    let mut trie = GeoTrie::new(9);
    assert!(!trie.remove_at_token("deadbeef", "nobody"));
}

#[test]
fn remove_prunes_empty_branch_nodes() {
    let mut trie = GeoTrie::new(9);
    trie.insert(entry("lone", 51.5, -0.1));
    let tok        = trie.cell_token(51.5, -0.1);
    let nodes_before = trie.count_nodes();
    assert!(nodes_before > 1, "should have branch nodes before removal");
    trie.remove_at_token(&tok, "lone");
    // After removing the only entry, the entire branch back to root must be pruned
    assert_eq!(trie.count_nodes(), 1, "only root node should remain after pruning");
    assert!(trie.is_empty());
}

#[test]
fn remove_does_not_prune_shared_branch() {
    let mut trie = GeoTrie::new(12);
    // Two close points — likely share several token prefix characters
    trie.insert(entry("a", 51.500, -0.100));
    trie.insert(entry("b", 51.501, -0.101));
    let tok_a = trie.cell_token(51.500, -0.100);
    let tok_b = trie.cell_token(51.501, -0.101);
    trie.remove_at_token(&tok_a, "a");
    // "b" must still be findable — its branch was not pruned
    let remaining = trie.query_token(&tok_b);
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].id, "b");
}

#[test]
fn remove_entry_convenience_method() {
    let mut trie = GeoTrie::new(9);
    trie.insert(entry("courier", 40.71, -74.01));
    assert!(trie.remove_entry(40.71, -74.01, "courier"));
    assert!(trie.is_empty());
}

#[test]
fn bulk_insert_10k_stable() {
    let mut trie = GeoTrie::new(9);
    for i in 0..10_000u32 {
        let lat = -85.0 + (i % 170) as f64;
        let lon = -175.0 + (i % 350) as f64;
        trie.insert(entry(&format!("a{i}"), lat, lon));
    }
    assert_eq!(trie.len(), 10_000);
    assert_eq!(trie.all_entries().len(), 10_000);
}

#[test]
fn cell_token_is_deterministic() {
    let trie = GeoTrie::new(9);
    assert_eq!(trie.cell_token(37.7749, -122.4194), trie.cell_token(37.7749, -122.4194));
}

#[test]
fn different_coords_different_tokens_at_fine_level() {
    let trie = GeoTrie::new(12);
    assert_ne!(trie.cell_token(48.85, 2.35), trie.cell_token(51.51, -0.13));
}
