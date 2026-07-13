use std::collections::HashMap;

use criterion::{criterion_group, criterion_main, Criterion};
use proxima::{GeoEntry, GeoTrie};
use serde_json::json;

// ── Shared data helpers ───────────────────────────────────────────────────

fn make_entry(i: u32) -> (f64, f64, GeoEntry) {
    let lat = -85.0 + (i % 170) as f64;
    let lon = -175.0 + (i % 350) as f64;
    let entry = GeoEntry {
        id: format!("a{i}"),
        lat,
        lon,
        payload: json!({}),
        written_at: 0,
    };
    (lat, lon, entry)
}

// ── Trie insert ───────────────────────────────────────────────────────────

fn bench_insert(c: &mut Criterion) {
    c.bench_function("insert_10k", |b| {
        b.iter(|| {
            let mut trie = GeoTrie::new(9);
            for i in 0..10_000u32 {
                let (lat, lon, entry) = make_entry(i);
                let _ = (lat, lon);
                trie.insert(entry);
            }
        })
    });
}

// ── Baseline: flat HashMap insert (same S2 token, no tree) ───────────────
//
// Hypothesis: a HashMap<String, Vec<GeoEntry>> keyed by the S2 token is the
// structurally simplest alternative. Its insert is O(1) amortised hash vs
// the trie's O(token_length) pointer walk. This bench establishes the ceiling
// for how fast a purely in-memory geo index can be without the prefix benefit.

fn bench_insert_flat(c: &mut Criterion) {
    let helper = GeoTrie::new(9);
    c.bench_function("insert_10k_flat", |b| {
        b.iter(|| {
            let mut map: HashMap<String, Vec<GeoEntry>> = HashMap::new();
            for i in 0..10_000u32 {
                let (lat, lon, entry) = make_entry(i);
                let token = helper.cell_token(lat, lon);
                map.entry(token).or_default().push(entry);
            }
        })
    });
}

// ── Trie exact-token query ────────────────────────────────────────────────

fn bench_query(c: &mut Criterion) {
    let mut trie = GeoTrie::new(9);
    for i in 0..10_000u32 {
        let (_, _, entry) = make_entry(i);
        trie.insert(entry);
    }
    let token = trie.cell_token(37.77, -122.41);
    c.bench_function("query_token", |b| b.iter(|| trie.query_token(&token)));
}

// ── Baseline: flat HashMap exact-token query ──────────────────────────────
//
// Hypothesis: a HashMap get() is O(1) vs the trie's O(token_length) descent.
// Expected outcome: HashMap wins or ties on exact-token lookups. The trie's
// structural advantage is not visible at single-level query granularity.

fn bench_query_flat(c: &mut Criterion) {
    let helper = GeoTrie::new(9);
    let mut map: HashMap<String, Vec<GeoEntry>> = HashMap::new();
    for i in 0..10_000u32 {
        let (lat, lon, entry) = make_entry(i);
        let token = helper.cell_token(lat, lon);
        map.entry(token).or_default().push(entry);
    }
    let token = helper.cell_token(37.77, -122.41);
    c.bench_function("query_token_flat", |b| b.iter(|| map.get(&token)));
}

// ── Trie prefix/coarse-cell query ─────────────────────────────────────────
//
// This is the operation the trie is *designed* to win. A viewport covering a
// country or region maps to a short S2 prefix (e.g. "89" covers the US
// Northeast at level-2). The trie descends to that node and collects the
// entire subtree in one pass. A flat HashMap has no concept of prefix; it
// would require iterating all keys and string-matching, which is O(N).
//
// Hypothesis: query_prefix is materially faster than a flat scan when there
// are many distinct tokens. The gap widens with dataset size.

fn bench_query_prefix(c: &mut Criterion) {
    let mut trie = GeoTrie::new(9);
    for i in 0..10_000u32 {
        let (_, _, entry) = make_entry(i);
        trie.insert(entry);
    }
    // "89" is a 2-char prefix — country/region scale S2 cell. All entries
    // whose level-9 token starts with "89" are returned by descending to that
    // node and collecting its subtree.
    c.bench_function("query_prefix_coarse", |b| b.iter(|| trie.query_token("89")));
}

// ── Baseline: flat HashMap prefix scan ────────────────────────────────────
//
// The structurally equivalent operation for a flat HashMap: iterate all keys,
// keep those that start with the 2-char prefix, return their values. This is
// the unavoidable cost the trie avoids.

fn bench_query_prefix_flat(c: &mut Criterion) {
    let helper = GeoTrie::new(9);
    let mut map: HashMap<String, Vec<GeoEntry>> = HashMap::new();
    for i in 0..10_000u32 {
        let (lat, lon, entry) = make_entry(i);
        let token = helper.cell_token(lat, lon);
        map.entry(token).or_default().push(entry);
    }
    c.bench_function("query_prefix_coarse_flat", |b| {
        b.iter(|| {
            map.iter()
                .filter(|(k, _)| k.starts_with("89"))
                .flat_map(|(_, v)| v.iter())
                .count()
        })
    });
}

criterion_group!(
    benches,
    bench_insert,
    bench_insert_flat,
    bench_query,
    bench_query_flat,
    bench_query_prefix,
    bench_query_prefix_flat,
);
criterion_main!(benches);
