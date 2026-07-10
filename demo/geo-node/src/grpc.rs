//! gRPC service for geo-node — fully functional without protoc or code generation.
//! Proto message types are defined with prost derives to match docs/proto/georedis.proto.
//! Serves on a dedicated port (default: HTTP_PORT + 10).

use std::sync::Arc;
use tonic::{async_trait, codegen::*, Request, Response, Status};
use georedis::GeoEntry;
use redis::AsyncCommands;
use crate::{cell_token, viewport_tokens, AppState};

// ── Proto messages ─────────────────────────────────────────────────────────

#[derive(Clone, PartialEq, prost::Message)]
pub struct GrpcGeoEntry {
    #[prost(string, tag="1")] pub id:           String,
    #[prost(double, tag="2")] pub lat:          f64,
    #[prost(double, tag="3")] pub lon:          f64,
    #[prost(string, tag="4")] pub payload_json: String,
}
#[derive(Clone, PartialEq, prost::Message)]
pub struct InsertBatchRequest {
    #[prost(message, repeated, tag="1")] pub entries: Vec<GrpcGeoEntry>,
}
#[derive(Clone, PartialEq, prost::Message)]
pub struct InsertResponse {
    #[prost(bool,   tag="1")] pub success:        bool,
    #[prost(uint32, tag="2")] pub entries_written: u32,
    #[prost(string, tag="3")] pub error:           String,
}
#[derive(Clone, PartialEq, prost::Message)]
pub struct RegionRequest {
    #[prost(double, tag="1")] pub south: f64,
    #[prost(double, tag="2")] pub west:  f64,
    #[prost(double, tag="3")] pub north: f64,
    #[prost(double, tag="4")] pub east:  f64,
}
#[derive(Clone, PartialEq, prost::Message)]
pub struct GeoEntriesResponse {
    #[prost(message, repeated, tag="1")] pub entries: Vec<GrpcGeoEntry>,
    #[prost(uint32,            tag="2")] pub count:   u32,
}
#[derive(Clone, PartialEq, prost::Message)] pub struct Empty {}
#[derive(Clone, PartialEq, prost::Message)]
pub struct TraceRequest {
    #[prost(double, tag="1")] pub lat: f64,
    #[prost(double, tag="2")] pub lon: f64,
}
#[derive(Clone, PartialEq, prost::Message)]
pub struct TraceResponse {
    #[prost(string, tag="1")] pub s2_token:            String,
    #[prost(string, tag="2")] pub owning_node_id:       String,
    #[prost(string, tag="3")] pub owning_prefix_range:  String,
    #[prost(string, tag="4")] pub served_by:            String,
    #[prost(bool,   tag="5")] pub is_local:             bool,
}
#[derive(Clone, PartialEq, prost::Message)]
pub struct ClusterNodeInfo {
    #[prost(string, tag="1")] pub node_id:      String,
    #[prost(string, tag="2")] pub addr:         String,
    #[prost(string, tag="3")] pub prefix_start: String,
    #[prost(string, tag="4")] pub prefix_end:   String,
    #[prost(uint64, tag="5")] pub key_count:    u64,
    #[prost(string, tag="6")] pub status:       String,
}
#[derive(Clone, PartialEq, prost::Message)]
pub struct ClusterResponse {
    #[prost(message, repeated, tag="1")] pub nodes: Vec<ClusterNodeInfo>,
}

// ── Service trait ──────────────────────────────────────────────────────────

#[async_trait]
pub trait GeoRedisGrpc: Send + Sync + 'static {
    async fn insert(&self, r: Request<GrpcGeoEntry>)         -> Result<Response<InsertResponse>,   Status>;
    async fn insert_batch(&self, r: Request<InsertBatchRequest>) -> Result<Response<InsertResponse>,   Status>;
    async fn query_region(&self, r: Request<RegionRequest>)  -> Result<Response<GeoEntriesResponse>, Status>;
    async fn get_cluster(&self,  r: Request<Empty>)          -> Result<Response<ClusterResponse>,   Status>;
    async fn trace_coordinate(&self, r: Request<TraceRequest>) -> Result<Response<TraceResponse>,  Status>;
}

// ── Implementation ─────────────────────────────────────────────────────────

pub struct GeoRedisService { pub state: crate::AppState }

#[async_trait]
impl GeoRedisGrpc for GeoRedisService {
    async fn insert(&self, req: Request<GrpcGeoEntry>) -> Result<Response<InsertResponse>, Status> {
        let e   = req.into_inner();
        let ttl = self.state.cfg.entity_ttl_secs as u64;
        let s2l = self.state.cfg.s2_level;
        let geo = GeoEntry {
            id: e.id.clone(), lat: e.lat, lon: e.lon,
            payload: serde_json::from_str(&e.payload_json).unwrap_or_default(),
        };
        let mut conn = self.state.redis.get_multiplexed_async_connection().await
            .map_err(|e| Status::internal(e.to_string()))?;
        let new_tok = cell_token(e.lat, e.lon, s2l);
        let ak  = format!("georedis:aircraft:{}", e.id);
        let ck  = format!("georedis:cell:{new_tok}");
        let loc = format!("georedis:location:{}", e.id);
        let js  = serde_json::to_string(&geo).unwrap_or_default();
        if let Ok(Some(old)) = conn.get::<_, Option<String>>(&loc).await {
            if old != new_tok {
                let _: () = conn.srem(format!("georedis:cell:{old}"), &e.id).await.unwrap_or(());
            }
        }
        let mut pipe = redis::pipe();
        pipe.set_ex(&ak, &js, ttl).ignore().sadd(&ck, &e.id).ignore().set_ex(&loc, &new_tok, ttl).ignore();
        let _: () = pipe.query_async(&mut conn).await.unwrap_or(());
        Ok(Response::new(InsertResponse { success: true, entries_written: 1, error: String::new() }))
    }

    async fn insert_batch(&self, req: Request<InsertBatchRequest>) -> Result<Response<InsertResponse>, Status> {
        let entries = req.into_inner().entries;
        let count   = entries.len() as u32;
        for e in entries { self.insert(Request::new(e)).await?; }
        Ok(Response::new(InsertResponse { success: true, entries_written: count, error: String::new() }))
    }

    async fn query_region(&self, req: Request<RegionRequest>) -> Result<Response<GeoEntriesResponse>, Status> {
        let r      = req.into_inner();
        let tokens = viewport_tokens(r.south, r.west, r.north, r.east, self.state.cfg.s2_level);
        let mut conn = self.state.redis.get_multiplexed_async_connection().await
            .map_err(|e| Status::internal(e.to_string()))?;
        let cell_keys: Vec<String> = tokens.iter().map(|t| format!("georedis:cell:{t}")).collect();
        let ids: Vec<String> = conn.sunion(cell_keys).await.unwrap_or_default();
        let mut pipe = redis::pipe();
        for id in &ids { pipe.get(format!("georedis:aircraft:{id}")); }
        let jsons: Vec<Option<String>> = pipe.query_async(&mut conn).await.unwrap_or_default();
        let entries: Vec<GrpcGeoEntry> = jsons.into_iter().flatten()
            .filter_map(|j| serde_json::from_str::<GeoEntry>(&j).ok())
            .map(|e| GrpcGeoEntry { id: e.id, lat: e.lat, lon: e.lon,
                payload_json: serde_json::to_string(&e.payload).unwrap_or_default() })
            .collect();
        let count = entries.len() as u32;
        Ok(Response::new(GeoEntriesResponse { entries, count }))
    }

    async fn get_cluster(&self, _: Request<Empty>) -> Result<Response<ClusterResponse>, Status> {
        let ring  = self.state.ring.read().await;
        let nodes = ring.all_nodes().map(|n| ClusterNodeInfo {
            node_id: n.node_id.clone(), addr: n.addr.clone(),
            prefix_start: n.prefix_start.clone(), prefix_end: n.prefix_end.clone(),
            key_count: n.key_count, status: format!("{:?}", n.status),
        }).collect();
        Ok(Response::new(ClusterResponse { nodes }))
    }

    async fn trace_coordinate(&self, req: Request<TraceRequest>) -> Result<Response<TraceResponse>, Status> {
        let r     = req.into_inner();
        let token = cell_token(r.lat, r.lon, self.state.cfg.s2_level);
        let ring  = self.state.ring.read().await;
        let my    = self.state.my_info.read().await;
        let (oid, orng) = ring.route(&token)
            .map(|n| (n.node_id.clone(), format!("[{}, {})", n.prefix_start, n.prefix_end)))
            .unwrap_or_else(|| ("unowned".into(), "—".into()));
        Ok(Response::new(TraceResponse {
            s2_token: token.clone(), owning_node_id: oid, owning_prefix_range: orng,
            served_by: self.state.cfg.node_id.clone(), is_local: my.owns(&token),
        }))
    }
}

// ── Server wrapper (mirrors tonic_build output) ────────────────────────────

pub struct GeoRedisServer<T: GeoRedisGrpc> { inner: Arc<T> }

impl<T: GeoRedisGrpc> GeoRedisServer<T> {
    pub fn new(inner: T) -> Self { Self { inner: Arc::new(inner) } }
}

impl<T: GeoRedisGrpc> Clone for GeoRedisServer<T> {
    fn clone(&self) -> Self { Self { inner: self.inner.clone() } }
}

impl<T: GeoRedisGrpc> tonic::server::NamedService for GeoRedisServer<T> {
    const NAME: &'static str = "georedis.v1.GeoRedis";
}

impl<T, B> Service<http::Request<B>> for GeoRedisServer<T>
where
    T: GeoRedisGrpc,
    B: Body + Send + 'static,
    B::Error: Into<StdError> + Send + 'static,
{
    type Response = http::Response<tonic::body::BoxBody>;
    type Error    = std::convert::Infallible;
    type Future   = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> { Poll::Ready(Ok(())) }

    fn call(&mut self, req: http::Request<B>) -> Self::Future {
        let inner = self.inner.clone();
        match req.uri().path() {
            "/georedis.v1.GeoRedis/Insert" => {
                struct H<T>(Arc<T>);
                impl<T: GeoRedisGrpc> tonic::server::UnaryService<GrpcGeoEntry> for H<T> {
                    type Response = InsertResponse;
                    type Future   = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                    fn call(&mut self, r: tonic::Request<GrpcGeoEntry>) -> Self::Future {
                        let i = self.0.clone(); Box::pin(async move { i.insert(r).await })
                    }
                }
                Box::pin(async move {
                    Ok(tonic::server::Grpc::new(tonic::codec::ProstCodec::default())
                        .unary(H(inner), req).await)
                })
            }
            "/georedis.v1.GeoRedis/InsertBatch" => {
                struct H<T>(Arc<T>);
                impl<T: GeoRedisGrpc> tonic::server::UnaryService<InsertBatchRequest> for H<T> {
                    type Response = InsertResponse;
                    type Future   = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                    fn call(&mut self, r: tonic::Request<InsertBatchRequest>) -> Self::Future {
                        let i = self.0.clone(); Box::pin(async move { i.insert_batch(r).await })
                    }
                }
                Box::pin(async move {
                    Ok(tonic::server::Grpc::new(tonic::codec::ProstCodec::default())
                        .unary(H(inner), req).await)
                })
            }
            "/georedis.v1.GeoRedis/QueryRegion" => {
                struct H<T>(Arc<T>);
                impl<T: GeoRedisGrpc> tonic::server::UnaryService<RegionRequest> for H<T> {
                    type Response = GeoEntriesResponse;
                    type Future   = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                    fn call(&mut self, r: tonic::Request<RegionRequest>) -> Self::Future {
                        let i = self.0.clone(); Box::pin(async move { i.query_region(r).await })
                    }
                }
                Box::pin(async move {
                    Ok(tonic::server::Grpc::new(tonic::codec::ProstCodec::default())
                        .unary(H(inner), req).await)
                })
            }
            "/georedis.v1.GeoRedis/GetCluster" => {
                struct H<T>(Arc<T>);
                impl<T: GeoRedisGrpc> tonic::server::UnaryService<Empty> for H<T> {
                    type Response = ClusterResponse;
                    type Future   = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                    fn call(&mut self, r: tonic::Request<Empty>) -> Self::Future {
                        let i = self.0.clone(); Box::pin(async move { i.get_cluster(r).await })
                    }
                }
                Box::pin(async move {
                    Ok(tonic::server::Grpc::new(tonic::codec::ProstCodec::default())
                        .unary(H(inner), req).await)
                })
            }
            "/georedis.v1.GeoRedis/TraceCoordinate" => {
                struct H<T>(Arc<T>);
                impl<T: GeoRedisGrpc> tonic::server::UnaryService<TraceRequest> for H<T> {
                    type Response = TraceResponse;
                    type Future   = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                    fn call(&mut self, r: tonic::Request<TraceRequest>) -> Self::Future {
                        let i = self.0.clone(); Box::pin(async move { i.trace_coordinate(r).await })
                    }
                }
                Box::pin(async move {
                    Ok(tonic::server::Grpc::new(tonic::codec::ProstCodec::default())
                        .unary(H(inner), req).await)
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

pub async fn serve(state: crate::AppState, grpc_port: u16) -> anyhow::Result<()> {
    let addr = format!("0.0.0.0:{grpc_port}").parse()?;
    tracing::info!("gRPC server on {addr}  (service: georedis.v1.GeoRedis)");
    tonic::transport::Server::builder()
        .add_service(GeoRedisServer::new(GeoRedisService { state }))
        .serve(addr)
        .await?;
    Ok(())
}
