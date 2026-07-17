//! geo-redis-grpc-bench
//!
//! A head-to-head **gRPC** benchmark of two geo-cache strategies, measured
//! end-to-end over a real HTTP/2 loopback connection:
//!
//!   • naive-redis — entries live in Redis (one set per S2 cell + one JSON
//!     string per entity). A region query does `SUNION` over the covering
//!     cell keys, then a pipelined `GET` of every entity. Every query pays a
//!     network round-trip (or several) to Redis.
//!
//!   • trie        — entries live in geo-redis's in-memory `GeoTrie`. A region
//!     query computes the S2 viewport tokens and walks the prefix trie. No
//!     Redis round-trip.
//!
//! Both backends sit behind the *same* hand-rolled gRPC service (identical
//! wire format to `demo/geo-node`), so the measured latency difference is
//! attributable to the cache strategy, not the transport.
//!
//! Usage:
//!   cargo run --release -p geo-redis-grpc-bench
//!   cargo run --release -p geo-redis-grpc-bench -- --entities 100000 --queries 5000
//!   cargo run --release -p geo-redis-grpc-bench -- --redis redis://127.0.0.1:6379

use std::{
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::Result;
use clap::Parser;
use hdrhistogram::Histogram;
use proxima::{GeoEntry, GeoTrie};
use rand::{rngs::StdRng, Rng, SeedableRng};
use redis::AsyncCommands;
use tokio::sync::RwLock;
use tonic::{async_trait, codegen::*, Request, Response, Status};

// ── CLI ─────────────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(
    name = "geo-redis-grpc-bench",
    about = "gRPC benchmark: naive Redis cache vs geo-redis trie-based cache"
)]
struct Args {
    /// Redis connection URL for the naive backend.
    #[arg(long, default_value = "redis://127.0.0.1:6379")]
    redis: String,

    /// Total synthetic entities to seed into each backend.
    #[arg(long, default_value_t = 50_000)]
    entities: usize,

    /// Number of timed region queries fired per backend.
    #[arg(long, default_value_t = 2_000)]
    queries: usize,

    /// S2 cell level (9 ≈ 70 km cells, 12 ≈ 2 km).
    #[arg(long, default_value_t = 9)]
    s2_level: u8,

    /// Half-size of each square query viewport, in degrees.
    #[arg(long, default_value_t = 5.0)]
    viewport_deg: f64,

    /// Entities per InsertBatch gRPC call while seeding.
    #[arg(long, default_value_t = 1_000)]
    seed_chunk: usize,

    /// RNG seed for reproducible datasets and viewports.
    #[arg(long, default_value_t = 42)]
    seed: u64,
}

// ── gRPC messages (wire-compatible prost, mirrors demo/geo-node) ─────────────

#[derive(Clone, PartialEq, prost::Message)]
pub struct GeoEntryMsg {
    #[prost(string, tag = "1")]
    pub id: String,
    #[prost(double, tag = "2")]
    pub lat: f64,
    #[prost(double, tag = "3")]
    pub lon: f64,
    #[prost(string, tag = "4")]
    pub payload_json: String,
}
#[derive(Clone, PartialEq, prost::Message)]
pub struct InsertBatchRequest {
    #[prost(message, repeated, tag = "1")]
    pub entries: Vec<GeoEntryMsg>,
}
#[derive(Clone, PartialEq, prost::Message)]
pub struct InsertResponse {
    #[prost(uint32, tag = "1")]
    pub written: u32,
}
#[derive(Clone, PartialEq, prost::Message)]
pub struct RegionRequest {
    #[prost(double, tag = "1")]
    pub south: f64,
    #[prost(double, tag = "2")]
    pub west: f64,
    #[prost(double, tag = "3")]
    pub north: f64,
    #[prost(double, tag = "4")]
    pub east: f64,
}
#[derive(Clone, PartialEq, prost::Message)]
pub struct RegionResponse {
    #[prost(message, repeated, tag = "1")]
    pub entries: Vec<GeoEntryMsg>,
    #[prost(uint32, tag = "2")]
    pub count: u32,
}

// ── Service trait + generic tonic server wrapper ─────────────────────────────

#[async_trait]
pub trait BenchCache: Send + Sync + 'static {
    async fn insert_batch(
        &self,
        r: Request<InsertBatchRequest>,
    ) -> Result<Response<InsertResponse>, Status>;
    async fn query_region(
        &self,
        r: Request<RegionRequest>,
    ) -> Result<Response<RegionResponse>, Status>;
}

pub struct BenchServer<T: BenchCache> {
    inner: Arc<T>,
}
impl<T: BenchCache> BenchServer<T> {
    pub fn new(inner: T) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }
}
impl<T: BenchCache> Clone for BenchServer<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}
impl<T: BenchCache> tonic::server::NamedService for BenchServer<T> {
    const NAME: &'static str = "bench.Cache";
}
impl<T, B> Service<http::Request<B>> for BenchServer<T>
where
    T: BenchCache,
    B: Body + Send + 'static,
    B::Error: Into<StdError> + Send + 'static,
{
    type Response = http::Response<tonic::body::BoxBody>;
    type Error = std::convert::Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: http::Request<B>) -> Self::Future {
        let inner = self.inner.clone();
        match req.uri().path() {
            "/bench.Cache/InsertBatch" => {
                struct H<T>(Arc<T>);
                impl<T: BenchCache> tonic::server::UnaryService<InsertBatchRequest> for H<T> {
                    type Response = InsertResponse;
                    type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                    fn call(&mut self, r: tonic::Request<InsertBatchRequest>) -> Self::Future {
                        let i = self.0.clone();
                        Box::pin(async move { i.insert_batch(r).await })
                    }
                }
                Box::pin(async move {
                    Ok(tonic::server::Grpc::new(tonic::codec::ProstCodec::default())
                        .unary(H(inner), req)
                        .await)
                })
            }
            "/bench.Cache/QueryRegion" => {
                struct H<T>(Arc<T>);
                impl<T: BenchCache> tonic::server::UnaryService<RegionRequest> for H<T> {
                    type Response = RegionResponse;
                    type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                    fn call(&mut self, r: tonic::Request<RegionRequest>) -> Self::Future {
                        let i = self.0.clone();
                        Box::pin(async move { i.query_region(r).await })
                    }
                }
                Box::pin(async move {
                    Ok(tonic::server::Grpc::new(tonic::codec::ProstCodec::default())
                        .unary(H(inner), req)
                        .await)
                })
            }
            _ => Box::pin(async move {
                Ok(http::Response::builder()
                    .status(200)
                    .header("grpc-status", "12")
                    .header("content-type", "application/grpc")
                    .body(tonic::body::empty_body())
                    .unwrap())
            }),
        }
    }
}

// ── S2 helpers (kept local; mirrors demo/geo-node) ───────────────────────────

fn cell_token(lat: f64, lon: f64, level: u8) -> String {
    use s2::{cellid::CellID, latlng::LatLng, s1};
    let ll = LatLng::new(s1::Deg(lat).into(), s1::Deg(lon).into());
    let cell = CellID::from(ll).parent(level as u64);
    let hex = format!("{:016x}", cell.0);
    hex.trim_end_matches('0').to_string()
}

fn viewport_tokens(south: f64, west: f64, north: f64, east: f64, level: u8) -> Vec<String> {
    use s2::{cap::Cap, latlng::LatLng, point::Point, region::RegionCoverer, s1};
    use std::f64::consts::PI;
    let clat = (south + north) / 2.0;
    let clon = (west + east) / 2.0;
    let dlat = (north - south).abs() / 2.0;
    let dlon = (east - west).abs() / 2.0;
    let rad = ((dlat * dlat + dlon * dlon).sqrt() * PI / 180.0).min(PI);
    let center = Point::from(LatLng::new(s1::Deg(clat).into(), s1::Deg(clon).into()));
    let angle: s1::angle::Angle = s1::Rad(rad).into();
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
            let h = format!("{:016x}", c.0);
            h.trim_end_matches('0').to_string()
        })
        .collect()
}

fn msg_to_entry(m: &GeoEntryMsg) -> GeoEntry {
    GeoEntry {
        id: m.id.clone(),
        lat: m.lat,
        lon: m.lon,
        payload: serde_json::from_str(&m.payload_json).unwrap_or_default(),
        written_at: 0,
    }
}

fn entry_to_msg(e: &GeoEntry) -> GeoEntryMsg {
    GeoEntryMsg {
        id: e.id.clone(),
        lat: e.lat,
        lon: e.lon,
        payload_json: serde_json::to_string(&e.payload).unwrap_or_default(),
    }
}

// ── Backend 1: naive Redis cache ─────────────────────────────────────────────

struct RedisBackend {
    conn: redis::aio::ConnectionManager,
    level: u8,
    prefix: String,
}

impl RedisBackend {
    fn k_cell(&self, token: &str) -> String {
        format!("{}:cell:{}", self.prefix, token)
    }
    fn k_entity(&self, id: &str) -> String {
        format!("{}:ent:{}", self.prefix, id)
    }
}

#[async_trait]
impl BenchCache for RedisBackend {
    async fn insert_batch(
        &self,
        r: Request<InsertBatchRequest>,
    ) -> Result<Response<InsertResponse>, Status> {
        let entries = r.into_inner().entries;
        let mut conn = self.conn.clone();
        let mut pipe = redis::pipe();
        for e in &entries {
            let token = cell_token(e.lat, e.lon, self.level);
            let json = serde_json::to_string(&msg_to_entry(e)).unwrap_or_default();
            pipe.sadd(self.k_cell(&token), &e.id)
                .ignore()
                .set(self.k_entity(&e.id), json)
                .ignore();
        }
        let _: () = pipe
            .query_async(&mut conn)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(InsertResponse {
            written: entries.len() as u32,
        }))
    }

    async fn query_region(
        &self,
        r: Request<RegionRequest>,
    ) -> Result<Response<RegionResponse>, Status> {
        let q = r.into_inner();
        let mut conn = self.conn.clone();
        let tokens = viewport_tokens(q.south, q.west, q.north, q.east, self.level);
        let cell_keys: Vec<String> = tokens.iter().map(|t| self.k_cell(t)).collect();
        let ids: Vec<String> = conn.sunion(cell_keys).await.unwrap_or_default();
        let mut pipe = redis::pipe();
        for id in &ids {
            pipe.get(self.k_entity(id));
        }
        let jsons: Vec<Option<String>> = pipe.query_async(&mut conn).await.unwrap_or_default();
        let entries: Vec<GeoEntryMsg> = jsons
            .into_iter()
            .flatten()
            .filter_map(|j| serde_json::from_str::<GeoEntry>(&j).ok())
            .map(|e| entry_to_msg(&e))
            .collect();
        let count = entries.len() as u32;
        Ok(Response::new(RegionResponse { entries, count }))
    }
}

// ── Backend 2: geo-redis trie-based cache ──────────────────────────────────────

struct TrieBackend {
    trie: RwLock<GeoTrie>,
    level: u8,
}

#[async_trait]
impl BenchCache for TrieBackend {
    async fn insert_batch(
        &self,
        r: Request<InsertBatchRequest>,
    ) -> Result<Response<InsertResponse>, Status> {
        let entries = r.into_inner().entries;
        let mut trie = self.trie.write().await;
        for e in &entries {
            trie.insert(msg_to_entry(e));
        }
        Ok(Response::new(InsertResponse {
            written: entries.len() as u32,
        }))
    }

    async fn query_region(
        &self,
        r: Request<RegionRequest>,
    ) -> Result<Response<RegionResponse>, Status> {
        let q = r.into_inner();
        let tokens = viewport_tokens(q.south, q.west, q.north, q.east, self.level);
        let trie = self.trie.read().await;
        let entries: Vec<GeoEntryMsg> = trie.query_tokens(&tokens).iter().map(entry_to_msg).collect();
        let count = entries.len() as u32;
        Ok(Response::new(RegionResponse { entries, count }))
    }
}

// ── Generic gRPC client calls ────────────────────────────────────────────────

async fn call_insert_batch(
    client: &mut tonic::client::Grpc<tonic::transport::Channel>,
    entries: Vec<GeoEntryMsg>,
) -> Result<u32> {
    client
        .ready()
        .await
        .map_err(|e| anyhow::anyhow!("client not ready: {e}"))?;
    let codec = tonic::codec::ProstCodec::<InsertBatchRequest, InsertResponse>::default();
    let path = http::uri::PathAndQuery::from_static("/bench.Cache/InsertBatch");
    let resp = client
        .unary(Request::new(InsertBatchRequest { entries }), path, codec)
        .await?;
    Ok(resp.into_inner().written)
}

async fn call_query_region(
    client: &mut tonic::client::Grpc<tonic::transport::Channel>,
    req: RegionRequest,
) -> Result<u32> {
    client
        .ready()
        .await
        .map_err(|e| anyhow::anyhow!("client not ready: {e}"))?;
    let codec = tonic::codec::ProstCodec::<RegionRequest, RegionResponse>::default();
    let path = http::uri::PathAndQuery::from_static("/bench.Cache/QueryRegion");
    let resp = client.unary(Request::new(req), path, codec).await?;
    Ok(resp.into_inner().count)
}

// ── Bench result ─────────────────────────────────────────────────────────────

struct Stats {
    name: &'static str,
    queries: usize,
    total_returned: u64,
    elapsed: Duration,
    hist: Histogram<u64>,
}

impl Stats {
    fn qps(&self) -> f64 {
        self.queries as f64 / self.elapsed.as_secs_f64()
    }
    fn avg_returned(&self) -> f64 {
        self.total_returned as f64 / self.queries.max(1) as f64
    }
}

// ── Dataset + viewport generation ────────────────────────────────────────────

fn make_dataset(n: usize, seed: u64) -> Vec<GeoEntryMsg> {
    let mut rng = StdRng::seed_from_u64(seed);
    (0..n)
        .map(|i| {
            let lat = rng.gen_range(-85.0_f64..85.0);
            let lon = rng.gen_range(-180.0_f64..180.0);
            GeoEntryMsg {
                id: format!("e{i}"),
                lat,
                lon,
                payload_json: format!("{{\"i\":{i}}}"),
            }
        })
        .collect()
}

fn make_viewports(n: usize, half_deg: f64, seed: u64) -> Vec<RegionRequest> {
    let mut rng = StdRng::seed_from_u64(seed ^ 0x9E37_79B9);
    (0..n)
        .map(|_| {
            let clat = rng.gen_range(-80.0_f64..80.0);
            let clon = rng.gen_range(-175.0_f64..175.0);
            RegionRequest {
                south: (clat - half_deg).max(-85.0),
                west: (clon - half_deg).max(-180.0),
                north: (clat + half_deg).min(85.0),
                east: (clon + half_deg).min(180.0),
            }
        })
        .collect()
}

async fn connect(port: u16) -> Result<tonic::transport::Channel> {
    let endpoint = tonic::transport::Channel::from_shared(format!("http://127.0.0.1:{port}"))?;
    // Retry briefly while the server binds.
    for _ in 0..50 {
        if let Ok(ch) = endpoint.connect().await {
            return Ok(ch);
        }
        tokio::time::sleep(Duration::from_millis(40)).await;
    }
    Ok(endpoint.connect().await?)
}

async fn run_backend(
    name: &'static str,
    channel: tonic::transport::Channel,
    dataset: &[GeoEntryMsg],
    viewports: &[RegionRequest],
    seed_chunk: usize,
) -> Result<Stats> {
    let mut client = tonic::client::Grpc::new(channel);

    // Seed over gRPC in chunks.
    for chunk in dataset.chunks(seed_chunk) {
        call_insert_batch(&mut client, chunk.to_vec()).await?;
    }

    // Warmup (not measured): first 5% of viewports.
    let warm = (viewports.len() / 20).max(1);
    for v in viewports.iter().take(warm) {
        let _ = call_query_region(&mut client, v.clone()).await?;
    }

    // Timed run.
    let mut hist = Histogram::<u64>::new(3)?;
    let mut total_returned = 0u64;
    let start = Instant::now();
    for v in viewports {
        let t0 = Instant::now();
        let count = call_query_region(&mut client, v.clone()).await?;
        let micros = t0.elapsed().as_micros() as u64;
        hist.record(micros).ok();
        total_returned += count as u64;
    }
    let elapsed = start.elapsed();

    Ok(Stats {
        name,
        queries: viewports.len(),
        total_returned,
        elapsed,
        hist,
    })
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
}

fn print_report(stats: &[Stats]) {
    let us = |v: u64| format!("{:.2} ms", v as f64 / 1000.0);
    println!("\n===================== gRPC region-query latency =====================");
    println!(
        "  {:<12} {:>9} {:>9} {:>9} {:>9} {:>9}",
        "backend", "p50", "p95", "p99", "max", "QPS"
    );
    println!("  --------------------------------------------------------------------");
    for s in stats {
        println!(
            "  {:<12} {:>9} {:>9} {:>9} {:>9} {:>9.0}",
            s.name,
            us(s.hist.value_at_quantile(0.50)),
            us(s.hist.value_at_quantile(0.95)),
            us(s.hist.value_at_quantile(0.99)),
            us(s.hist.max()),
            s.qps(),
        );
    }
    println!("  ====================================================================");

    for s in stats {
        println!(
            "  {:<12}  {} queries * {:.1} entries/query avg * {:.2}s total",
            s.name,
            s.queries,
            s.avg_returned(),
            s.elapsed.as_secs_f64(),
        );
    }

    // Head-to-head speedup on median latency.
    if stats.len() == 2 {
        let p50_a = stats[0].hist.value_at_quantile(0.50) as f64;
        let p50_b = stats[1].hist.value_at_quantile(0.50) as f64;
        if p50_a > 0.0 && p50_b > 0.0 {
            let (fast, slow, factor) = if p50_a < p50_b {
                (stats[0].name, stats[1].name, p50_b / p50_a)
            } else {
                (stats[1].name, stats[0].name, p50_a / p50_b)
            };
            println!(
                "\n  -> {fast} is {factor:.1}x faster than {slow} at the median (p50)."
            );
        }
    }
    println!();
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    println!("geo-redis gRPC cache benchmark");
    println!(
        "  entities={}  queries={}  s2_level={}  viewport=+/-{} deg",
        args.entities, args.queries, args.s2_level, args.viewport_deg
    );

    let dataset = make_dataset(args.entities, args.seed);
    let viewports = make_viewports(args.queries, args.viewport_deg, args.seed);

    let mut results: Vec<Stats> = Vec::new();

    // ── naive-redis backend ──────────────────────────────────────────────
    match redis::Client::open(args.redis.as_str()) {
        Ok(client) => match redis::aio::ConnectionManager::new(client).await {
            Ok(conn) => {
                let prefix = format!("grpcbench:{}", now_ms());
                let backend = RedisBackend {
                    conn,
                    level: args.s2_level,
                    prefix: prefix.clone(),
                };
                let port = 50_071;
                let addr = format!("127.0.0.1:{port}").parse()?;
                let server =
                    tonic::transport::Server::builder().add_service(BenchServer::new(backend));
                let handle = tokio::spawn(async move { server.serve(addr).await });

                let channel = connect(port).await?;
                let stats =
                    run_backend("naive-redis", channel, &dataset, &viewports, args.seed_chunk)
                        .await?;
                results.push(stats);
                handle.abort();

                // Best-effort cleanup of the benchmark keyspace.
                if let Ok(mut c) = redis::Client::open(args.redis.as_str())
                    .unwrap()
                    .get_multiplexed_async_connection()
                    .await
                {
                    let keys: Vec<String> =
                        c.keys(format!("{prefix}:*")).await.unwrap_or_default();
                    if !keys.is_empty() {
                        let _: () = c.del(keys).await.unwrap_or(());
                    }
                }
            }
            Err(e) => eprintln!("⚠ skipping naive-redis backend: Redis unavailable ({e})"),
        },
        Err(e) => eprintln!("⚠ skipping naive-redis backend: bad Redis URL ({e})"),
    }

    // ── trie backend ─────────────────────────────────────────────────────
    {
        let backend = TrieBackend {
            trie: RwLock::new(GeoTrie::new(args.s2_level)),
            level: args.s2_level,
        };
        let port = 50_072;
        let addr = format!("127.0.0.1:{port}").parse()?;
        let server = tonic::transport::Server::builder().add_service(BenchServer::new(backend));
        let handle = tokio::spawn(async move { server.serve(addr).await });

        let channel = connect(port).await?;
        let stats = run_backend("trie", channel, &dataset, &viewports, args.seed_chunk).await?;
        results.push(stats);
        handle.abort();
    }

    print_report(&results);
    Ok(())
}
