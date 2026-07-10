// GeoRedis .NET quickstart — NuGet packages required:
//   dotnet add package Grpc.Net.Client
//   dotnet add package Grpc.Tools
//   dotnet add package Google.Protobuf
//
// Add the proto to your .csproj:
//   <ItemGroup>
//     <Protobuf Include="georedis.proto" GrpcServices="Client" />
//   </ItemGroup>
//
// Or use the HTTP/REST API directly (no code generation needed):

using System;
using System.Net.Http;
using System.Net.Http.Json;
using System.Threading.Tasks;
using Grpc.Net.Client;
using Georedis.V1;   // generated from georedis.proto

// ── Option A: gRPC client (recommended for high throughput) ────────────────

class GeoRedisGrpcExample
{
    static async Task Main()
    {
        // Connect to any geo-node (they all know the full cluster topology)
        using var channel = GrpcChannel.ForAddress("http://geo-node-0:4000");
        var client = new GeoRedis.GeoRedisClient(channel);

        // Insert a courier position
        var insertResp = await client.InsertAsync(new GeoEntry {
            Id          = "courier-42",
            Lat         = 51.5074,
            Lon         = -0.1278,
            PayloadJson = """{"name":"Alice","status":"delivering","orderId":"ORD-8821"}"""
        });
        Console.WriteLine($"Inserted: {insertResp.Success}");

        // Batch insert (more efficient — one gRPC call per update cycle)
        var batchReq = new InsertBatchRequest();
        batchReq.Entries.AddRange(new[] {
            new GeoEntry { Id = "courier-43", Lat = 51.51, Lon = -0.12, PayloadJson = "{}" },
            new GeoEntry { Id = "courier-44", Lat = 51.49, Lon = -0.14, PayloadJson = "{}" },
        });
        await client.InsertBatchAsync(batchReq);

        // Query couriers within a map viewport (bounding box)
        var nearby = await client.QueryRegionAsync(new RegionRequest {
            South = 51.40, West  = -0.30,
            North = 51.60, East  =  0.10
        });
        Console.WriteLine($"Found {nearby.Count} couriers in viewport");
        Console.WriteLine($"Shards queried: {string.Join(", ", nearby.ShardsQueried)}");

        foreach (var entry in nearby.Entries) {
            Console.WriteLine($"  {entry.Id} @ ({entry.Lat:F4}, {entry.Lon:F4})");
        }

        // Get full detail for a specific courier (lazy hydration — only when needed)
        var detail = await client.GetDetailAsync(new DetailRequest { Id = "courier-42" });
        Console.WriteLine($"\nDetail for courier-42: {detail.PayloadJson}");
        Console.WriteLine($"Position history ({detail.History.Count} points):");
        foreach (var pos in detail.History) {
            Console.WriteLine($"  ({pos.Lat:F4}, {pos.Lon:F4})");
        }

        // Prove routing — which shard owns a given coordinate?
        var trace = await client.TraceCoordinateAsync(new TraceRequest { Lat = 51.51, Lon = -0.12 });
        Console.WriteLine($"\nRouting trace for London:");
        Console.WriteLine($"  S2 token     : {trace.S2Token}");
        Console.WriteLine($"  Owning shard : {trace.OwningNodeId} {trace.OwningPrefixRange}");
        Console.WriteLine($"  Served by    : {trace.ServedBy}");
        Console.WriteLine($"  Is local     : {trace.IsLocal}");
    }
}


// ── Option B: REST/JSON client (no code generation) ────────────────────────

class GeoRedisRestExample
{
    static async Task Main()
    {
        var http = new HttpClient { BaseAddress = new Uri("http://geo-node-0:4000") };

        // Insert via REST
        await http.PostAsJsonAsync("/ingest", new[] {
            new { id = "courier-42", lat = 51.5074, lon = -0.1278,
                  payload = new { name = "Alice", status = "delivering" } }
        });

        // Query region via REST
        var resp = await http.GetFromJsonAsync<dynamic>(
            "/api/region?s=51.4&w=-0.3&n=51.6&e=0.1");

        // Get cluster topology
        var cluster = await http.GetFromJsonAsync<dynamic>("/cluster");

        // Prove routing for a coordinate
        var trace = await http.GetFromJsonAsync<dynamic>("/trace?lat=51.51&lon=-0.12");
        Console.WriteLine($"Owning node: {trace?.owning_node_id}");
    }
}
