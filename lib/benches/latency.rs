use criterion::{criterion_group, criterion_main, Criterion};
use georedis::{GeoEntry, GeoTrie};
use serde_json::json;

fn bench_insert(c: &mut Criterion) {
    c.bench_function("insert_10k", |b| {
        b.iter(|| {
            let mut trie = GeoTrie::new(9);
            for i in 0..10_000u32 {
                let lat = -85.0 + (i % 170) as f64;
                let lon = -175.0 + (i % 350) as f64;
                trie.insert(GeoEntry { id: format!("a{i}"), lat, lon, payload: json!({}), written_at: 0 });
            }
        })
    });
}

fn bench_query(c: &mut Criterion) {
    let mut trie = GeoTrie::new(9);
    for i in 0..10_000u32 {
        let lat = -85.0 + (i % 170) as f64;
        let lon = -175.0 + (i % 350) as f64;
        trie.insert(GeoEntry { id: format!("a{i}"), lat, lon, payload: json!({}), written_at: 0 });
    }
    let token = trie.cell_token(37.77, -122.41);
    c.bench_function("query_token", |b| b.iter(|| trie.query_token(&token)));
}

criterion_group!(benches, bench_insert, bench_query);
criterion_main!(benches);
