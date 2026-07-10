"""
GeoRedis Python quickstart

Install dependencies:
    pip install grpcio grpcio-tools requests

Generate Python stubs from proto:
    python -m grpc_tools.protoc \\
        -I docs/proto \\
        --python_out=. \\
        --grpc_python_out=. \\
        docs/proto/georedis.proto
"""

import grpc
import json
import requests

# ── Option A: gRPC client (recommended for high throughput) ────────────────

# Import generated stubs (run grpc_tools.protoc first)
# import georedis_pb2
# import georedis_pb2_grpc

def grpc_example():
    """Courier position tracking via gRPC."""
    import georedis_pb2 as pb
    import georedis_pb2_grpc as pbgrpc

    channel = grpc.insecure_channel("geo-node-0:4000")
    stub    = pbgrpc.GeoRedisStub(channel)

    # Insert a courier position
    resp = stub.Insert(pb.GeoEntry(
        id="courier-42",
        lat=51.5074,
        lon=-0.1278,
        payload_json=json.dumps({
            "name": "Alice",
            "status": "delivering",
            "order_id": "ORD-8821"
        })
    ))
    print(f"Inserted: {resp.success}")

    # Batch insert — one RPC call for N position updates
    batch = pb.InsertBatchRequest(entries=[
        pb.GeoEntry(id="courier-43", lat=51.51, lon=-0.12, payload_json="{}"),
        pb.GeoEntry(id="courier-44", lat=51.49, lon=-0.14, payload_json="{}"),
    ])
    stub.InsertBatch(batch)

    # Query all couriers within a map viewport
    nearby = stub.QueryRegion(pb.RegionRequest(
        south=51.40, west=-0.30,
        north=51.60, east=0.10
    ))
    print(f"Found {nearby.count} couriers in viewport")
    print(f"Shards queried: {list(nearby.shards_queried)}")

    for entry in nearby.entries:
        data = json.loads(entry.payload_json)
        print(f"  {entry.id} @ ({entry.lat:.4f}, {entry.lon:.4f}) — {data.get('name')}")

    # Lazy hydration: get full detail + history only when needed (< 5 entities)
    detail = stub.GetDetail(pb.DetailRequest(id="courier-42"))
    if detail.found:
        print(f"\nFull detail: {detail.payload_json}")
        print(f"History ({len(detail.history)} points):")
        for pos in detail.history:
            print(f"  ({pos.lat:.4f}, {pos.lon:.4f})")

    # Prove geographic routing
    trace = stub.TraceCoordinate(pb.TraceRequest(lat=51.51, lon=-0.12))
    print(f"\nRouting trace for London:")
    print(f"  S2 token     : {trace.s2_token}")
    print(f"  Owning shard : {trace.owning_node_id} {trace.owning_prefix_range}")
    print(f"  Served by    : {trace.served_by}")
    print(f"  Is local     : {trace.is_local}")


# ── Option B: REST/HTTP client (no code generation needed) ─────────────────

BASE = "http://geo-node-0:4000"

def rest_example():
    """Same operations using the JSON HTTP API — no proto compilation needed."""

    # Insert via HTTP POST /ingest
    requests.post(f"{BASE}/ingest", json=[{
        "id": "courier-42",
        "lat": 51.5074,
        "lon": -0.1278,
        "payload": {"name": "Alice", "status": "delivering"}
    }])

    # Query a viewport
    resp = requests.get(f"{BASE}/api/region", params={
        "s": 51.40, "w": -0.30, "n": 51.60, "e": 0.10
    }).json()
    print(f"Found {resp['count']} couriers")

    # Get full detail for one courier
    detail = requests.get(f"{BASE}/api/aircraft/courier-42").json()
    print(f"History: {detail.get('history')}")

    # Prove routing — works on any node
    trace = requests.get(f"{BASE}/trace", params={"lat": 51.51, "lon": -0.12}).json()
    print(f"Coordinate routes to: {trace['owning_node_id']}")
    print(f"Served by           : {trace['served_by']}")

    # Get full cluster topology
    cluster = requests.get(f"{BASE}/cluster").json()
    for node in cluster:
        print(f"  {node['node_id']:12} [{node['prefix_start'] or '∅':6} → "
              f"{node['prefix_end'] or '∞':6})  {node['status']:10}  {node['key_count']} keys")


# ── FastAPI integration example ─────────────────────────────────────────────

def fastapi_integration():
    """
    Typical integration pattern for a delivery backend.
    Courier app calls this endpoint on every GPS update.
    """
    from fastapi import FastAPI
    app = FastAPI()

    @app.post("/courier/{courier_id}/position")
    async def update_position(courier_id: str, lat: float, lon: float, metadata: dict):
        requests.post(f"{BASE}/ingest", json=[{
            "id":      courier_id,
            "lat":     lat,
            "lon":     lon,
            "payload": metadata,
        }])
        return {"status": "ok"}

    @app.get("/dispatch/nearby")
    async def find_nearby_couriers(lat: float, lon: float, radius_km: float = 5):
        # Convert radius to approximate bounding box
        deg = radius_km / 111.0
        result = requests.get(f"{BASE}/api/region", params={
            "s": lat - deg, "n": lat + deg,
            "w": lon - deg, "e": lon + deg,
        }).json()
        return {"couriers": result["aircraft"], "count": result["count"]}

    return app


if __name__ == "__main__":
    rest_example()
