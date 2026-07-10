# GeoRedis distributed cluster demo — Windows
# Walks through: startup → gossip convergence → split → failover
param([switch]$SkipBuild)

$COMPOSE = "docker compose -f demo/cluster-compose.yml"
$C0 = "http://localhost:4000"
$C1 = "http://localhost:4001"
$C2 = "http://localhost:4002"
$C3 = "http://localhost:4003"

function Sep  { Write-Host "`n$('━' * 55)" -ForegroundColor Cyan }
function Hdr  { param($t) Sep; Write-Host $t -ForegroundColor White; Sep }
function Ok   { param($m) Write-Host "  ✓ $m" -ForegroundColor Green }
function Info { param($m) Write-Host "  $m" -ForegroundColor Gray }

function Show-Cluster {
    $nodes = Invoke-RestMethod "$C0/cluster" | Sort-Object prefix_start
    Write-Host ""
    Write-Host ("  {0,-12} {1,-22} {2,-10} {3}" -f "NODE","PREFIX RANGE","STATUS","KEYS") -ForegroundColor Cyan
    Write-Host "  $('-' * 60)" -ForegroundColor DarkGray
    foreach ($n in $nodes) {
        $ps = if ($n.prefix_start) { $n.prefix_start } else { "(start)" }
        $pe = if ($n.prefix_end)   { $n.prefix_end }   else { "(end)"   }
        $bar = "█" * [Math]::Min($n.key_count / 5, 30)
        Write-Host ("  {0,-12} [{1,-8} → {2,-8})  {3,-10} {4,5}  {5}" -f `
            $n.node_id, $ps, $pe, $n.status, $n.key_count, $bar)
    }
    Write-Host ""
}

function Push-Aircraft { param($node, $lat, $lon, $id, $callsign)
    $body = "[{`"id`":`"$id`",`"lat`":$lat,`"lon`":$lon,`"payload`":{`"callsign`":`"$callsign`"}}]"
    Invoke-RestMethod -Method POST -Uri "$node/ingest" -Body $body -ContentType "application/json" | Out-Null
}

Set-Location $PSScriptRoot\..

# ── STEP 1: Build & start ────────────────────────────────────────────────
Hdr "STEP 1 — Build and start the 3-shard cluster + standby"

if (-not $SkipBuild) {
    Info "Building geo-node Docker image (~3 min first time)..."
    Invoke-Expression "$COMPOSE build"
}

Info "Starting all 4 nodes..."
Invoke-Expression "$COMPOSE up -d"

Info "Waiting for nodes to become healthy..."
foreach ($port in 4000,4001,4002,4003) {
    for ($i = 0; $i -lt 30; $i++) {
        try {
            Invoke-RestMethod "http://localhost:$port/health" -TimeoutSec 2 | Out-Null
            Ok "geo-node on :$port is up"; break
        } catch { Start-Sleep 1 }
    }
}
Start-Sleep 4  # gossip convergence

# ── STEP 2: Show initial cluster state ──────────────────────────────────
Hdr "STEP 2 — Cluster state after gossip convergence"
Info "All nodes should now know each other via gossip:"
Show-Cluster

Read-Host "`nPress Enter to continue"

# ── STEP 3: Inject aircraft data ─────────────────────────────────────────
Hdr "STEP 3 — Inject ~300 aircraft across all shards"

Info "Writing 100 North America aircraft to shard 0 (prefix 0-5)..."
1..100 | ForEach-Object {
    $lat = Get-Random -Minimum 25 -Maximum 50
    $lon = Get-Random -Minimum -125 -Maximum -70
    Push-Aircraft $C0 $lat $lon "usa$('{0:d3}' -f $_)" "UAL$_"
}
Ok "100 NA aircraft → shard 0"

Info "Writing 100 Europe aircraft to shard 1 (prefix 5-a)..."
1..100 | ForEach-Object {
    $lat = Get-Random -Minimum 45 -Maximum 62
    $lon = Get-Random -Minimum -5 -Maximum 30
    Push-Aircraft $C1 $lat $lon "eur$('{0:d3}' -f $_)" "BAW$_"
}
Ok "100 EU aircraft → shard 1"

Info "Writing 100 Pacific aircraft to shard 2 (prefix a-end)..."
1..100 | ForEach-Object {
    $lat = Get-Random -Minimum 10 -Maximum 45
    $lon = Get-Random -Minimum 120 -Maximum 150
    Push-Aircraft $C2 $lat $lon "pac$('{0:d3}' -f $_)" "ANA$_"
}
Ok "100 Pacific aircraft → shard 2"

Start-Sleep 12  # metrics loop refresh
Info "Distribution after write:"
Show-Cluster

Read-Host "`nPress Enter to continue"

# ── STEP 4: Trigger a split ──────────────────────────────────────────────
Hdr "STEP 4 — Split shard 1 at prefix '7' → standby node-3"
Info "node-1 owns [5, a). Split at '7':"
Info "  node-1: [5, 7)  — western portion"
Info "  node-3: [7, a)  — eastern portion (was standby)"

$body = '{"target":"geo-node-3:4003","split_point":"7"}'
$result = Invoke-RestMethod -Method POST -Uri "$C1/split" -Body $body -ContentType "application/json"

Write-Host ""
Ok "Migrated $($result.migrated_keys) keys to node-3"
Info "node-1 now owns: [5, $($result.new_prefix_end))"
Info "node-3 now owns: [$($result.split_point), a)"

Start-Sleep 5
Info "Cluster state after split (4 active shards):"
Show-Cluster

Read-Host "`nPress Enter to continue"

# ── STEP 5: Simulate node failure ───────────────────────────────────────
Hdr "STEP 5 — Kill node-2 (Pacific shard) — watch gossip detect failure"
Invoke-Expression "$COMPOSE stop geo-node-2"
Info "node-2 stopped. Watching gossip detection..."

for ($i = 1; $i -le 8; $i++) {
    Start-Sleep 5
    $status = (Invoke-RestMethod "$C0/cluster" | Where-Object { $_.node_id -eq "node-2" }).status
    Write-Host "  [${i}×5s] node-2 status from node-0: $status"
    if ($status -eq "dead") { break }
}
Ok "Gossip detected node-2 as Dead — no central coordinator needed"

Read-Host "`nPress Enter to continue"

# ── STEP 6: Restore ──────────────────────────────────────────────────────
Hdr "STEP 6 — Bring node-2 back — watch it re-join"
Invoke-Expression "$COMPOSE start geo-node-2"
Start-Sleep 8
$status = (Invoke-RestMethod "$C0/cluster" | Where-Object { $_.node_id -eq "node-2" }).status
Ok "node-2 rejoined — status: $status"

# ── Final summary ────────────────────────────────────────────────────────
Hdr "DEMO COMPLETE — Final 4-shard topology"
Show-Cluster

Info "To tear down:  docker compose -f demo/cluster-compose.yml down -v"
Info "Node metrics:  curl http://localhost:4000/metrics  (and 4001, 4002, 4003)"
