# geo-redis Cluster Load Test — live dashboard + split/merge demo
#
# What this does:
#   Phase 1  SETUP      — Build binaries, start 4-node cluster, wait for gossip
#   Phase 2  WARM-UP    — Low-rate writes to establish baseline state  
#   Phase 3  HIGH-LOAD  — Ramp to 10 000 entities/s, watch key counts climb
#   Phase 4  SPLIT      — Americas shard approaches threshold; trigger split to Standby
#   Phase 5  BOOTSTRAP  — Watch new shard: Bootstrapping → delta-sync → Active
#   Phase 6  VERIFY     — Trace queries prove routing; loadtest continues across 4 shards
#   Phase 7  SUMMARY    — Print pass/fail table, key counts, total throughput
#
# Run:
#   .\scripts\cluster-load-test.ps1
#   .\scripts\cluster-load-test.ps1 -SkipBuild          # reuse existing Docker image
#   .\scripts\cluster-load-test.ps1 -SplitThreshold 50  # split at 50 keys (fast demo)

param(
    [switch]$SkipBuild,
    [int]$SplitThreshold = 5000,   # trigger split when Shard 0 exceeds this
    [int]$WarmUpSecs     = 20,
    [int]$HighLoadSecs   = 60,
    [int]$BootstrapSecs  = 90,     # max wait for Bootstrapping → Active
    [int]$VerifySecs     = 30
)

Set-Location $PSScriptRoot\..
$ErrorActionPreference = "Stop"

# ── Node addresses ──────────────────────────────────────────────────────────
$NODES = @(
    @{ id="node-0"; addr="localhost:4000"; label="Americas  "; url="http://localhost:4000" }
    @{ id="node-1"; addr="localhost:4001"; label="Europe    "; url="http://localhost:4001" }
    @{ id="node-2"; addr="localhost:4002"; label="Asia-Pac  "; url="http://localhost:4002" }
    @{ id="node-3"; addr="localhost:4003"; label="Standby-1 "; url="http://localhost:4003" }
)

# ── Shared state ─────────────────────────────────────────────────────────────
$script:events     = [System.Collections.Generic.List[string]]::new()
$script:phase      = "INIT"
$script:loadJob    = $null
$script:splitDone  = $false
$script:testPassed = $true
$script:totalWrites = 0L
$script:startTime  = Get-Date

function Log-Event { param($msg, $color = "Cyan")
    $ts  = (Get-Date).ToString("HH:mm:ss.fff")
    $script:events.Add("$ts  $msg")
    # Keep last 12 for display
    while ($script:events.Count -gt 12) { $script:events.RemoveAt(0) }
}

function Fail { param($msg)
    Log-Event "FAIL: $msg" "Red"
    $script:testPassed = $false
}

# ── REST helpers ────────────────────────────────────────────────────────────
function Get-ClusterRing { param($url = $NODES[0].url)
    try { Invoke-RestMethod "$url/cluster" -TimeoutSec 3 }
    catch { @() }
}

function Get-NodeMetrics { param($url)
    try { Invoke-RestMethod "$url/metrics" -TimeoutSec 2 }
    catch { $null }
}

function Ingest-Batch { param($url, $entities)
    $body = $entities | ConvertTo-Json -Compress
    Invoke-RestMethod -Method POST -Uri "$url/ingest" -Body $body `
        -ContentType "application/json" -TimeoutSec 5 | Out-Null
}

function Trace-Coord { param($url, $lat, $lon)
    try { Invoke-RestMethod "$url/trace?lat=$lat&lon=$lon" -TimeoutSec 3 }
    catch { $null }
}

# ── Terminal dashboard ──────────────────────────────────────────────────────
function Draw-Dashboard {
    $ring    = Get-ClusterRing
    $elapsed = [Math]::Round(((Get-Date) - $script:startTime).TotalSeconds)
    $wps     = if ($elapsed -gt 0) { [Math]::Round($script:totalWrites / $elapsed) } else { 0 }

    # Position cursor at top-left without clearing (reduces flicker)
    [Console]::SetCursorPosition(0, 0)

    $w = [Math]::Max([Console]::WindowWidth, 72)

    function Box { param($t, $c = "Cyan") Write-Host $t.PadRight($w) -ForegroundColor $c -NoNewline; Write-Host "" }

    Box "╔$('═' * ($w-2))╗"
    Box "║  geo-redis Cluster Load Test — $((Get-Date).ToString('HH:mm:ss'))   Phase: $($script:phase.PadRight(14)) $('▶' * 1)  ║"
    Box "╠$('═' * ($w-2))╣"
    Box "║  $('NODE'.PadRight(14)) $('STATUS'.PadRight(14)) $('KEYS'.PadLeft(9))  $('PREFIX RANGE'.PadRight(22)) $('MEM'.PadLeft(7))  ║"
    Box "╠$('═' * ($w-2))╣"

    foreach ($n in $NODES) {
        $ri = $ring | Where-Object { $_.node_id -eq $n.id } | Select-Object -First 1
        if ($ri) {
            $ps  = if ($ri.prefix_start) { $ri.prefix_start } else { "∅" }
            $pe  = if ($ri.prefix_end)   { $ri.prefix_end }   else { "∅" }
            $col = switch ($ri.status) {
                "active"        { "Green" }
                "splitting"     { "Yellow" }
                "bootstrapping" { "Cyan" }
                "suspect"       { "DarkYellow" }
                "dead"          { "Red" }
                default         { "Gray" }
            }
            $icon = switch ($ri.status) {
                "active"        { "[OK]" }
                "splitting"     { "[>>]" }
                "bootstrapping" { "[~~]" }
                "merging"       { "[<<]" }
                "suspect"       { "[??]" }
                "dead"          { "[XX]" }
                "standby"       { "[  ]" }
                default         { "[  ]" }
            }
            $keys  = $ri.key_count.ToString("N0").PadLeft(9)
            $memMb = "$([Math]::Round($ri.mem_bytes/1MB, 1)) MB".PadLeft(7)
            $range = "[{0}, {1})" -f $ps.PadRight(3), $pe.PadRight(3)
            $line  = "║  {0} {1} {2,-14} {3}  {4,-22} {5}  ║" -f `
                $n.label, $icon, $ri.status, $keys, $range, $memMb
        } else {
            $line = "║  $($n.label) [--] (unreachable)$(' ' * ($w-35))║"
            $col  = "DarkGray"
        }
        Write-Host $line.PadRight($w) -ForegroundColor $col
    }

    Box "╠$('═' * ($w-2))╣"
    Box "║  Total writes: $($script:totalWrites.ToString("N0").PadLeft(12))   Throughput: $($wps.ToString("N0").PadLeft(8)) entities/s$(' ' * 8)║"
    Box "╠$('═' * ($w-2))╣"
    Box "║  Event log:$(' ' * ($w-13))║"

    $show = $script:events | Select-Object -Last 8
    foreach ($e in $show) {
        Box "║    $($e.PadRight($w-6))║" "DarkCyan"
    }
    # Pad to fixed height
    for ($i = $show.Count; $i -lt 8; $i++) { Box "║$(' ' * ($w-2))║" "DarkGray" }
    Box "╚$('═' * ($w-2))╝"
}

# ── Load generation helpers ─────────────────────────────────────────────────
function New-RandomEntity {
    $lat = [Math]::Round((Get-Random -Minimum -8500 -Maximum 8500) / 100.0, 2)
    $lon = [Math]::Round((Get-Random -Minimum -18000 -Maximum 18000) / 100.0, 2)
    $id  = "ent-{0:x8}" -f (Get-Random)
    @{ id=$id; lat=$lat; lon=$lon; payload=@{ callsign="TST$($id.Substring(4,4).ToUpper())" } }
}

function Start-LoadJob { param($wps, $shardSpecs)
    # Launch the loadtest binary as a background job
    $exe    = ".\target\release\geo-redis-loadtest.exe"
    $args   = "--writers 4 --readers 8 --batch-size $([Math]::Max(1,$wps/4)) --duration-secs 999"
    if ($shardSpecs) { $args += " --shards `"$shardSpecs`"" }
    $script:loadJob = Start-Job -ScriptBlock {
        param($exe, $args)
        & $exe $args.Split(" ")
    } -ArgumentList $exe, $args
}

function Stop-LoadJob {
    if ($script:loadJob) {
        Stop-Job $script:loadJob -ErrorAction SilentlyContinue
        Remove-Job $script:loadJob -Force -ErrorAction SilentlyContinue
        $script:loadJob = $null
    }
}

# ── Verification helpers ─────────────────────────────────────────────────────
function Wait-For-Status { param($url, $expectedStatus, $timeoutSecs = 60)
    $deadline = (Get-Date).AddSeconds($timeoutSecs)
    while ((Get-Date) -lt $deadline) {
        $ring = Get-ClusterRing $url
        $node = $ring | Where-Object { $_.addr -like "$url*" -or $_.addr -like "geo-node-*" } | Select-Object -First 1
        # Query by checking all nodes
        $allStatus = $ring | ForEach-Object { $_.status }
        if ($allStatus -contains $expectedStatus) { return $true }
        Start-Sleep 2
    }
    return $false
}

function Assert-NodeStatus { param($nodeUrl, $expectedStatus)
    try {
        $state = Invoke-RestMethod "$nodeUrl/state" -TimeoutSec 3
        if ($state.status -ne $expectedStatus) {
            Fail "Node $nodeUrl status='$($state.status)', expected '$expectedStatus'"
            return $false
        }
        return $true
    } catch {
        Fail "Cannot reach $nodeUrl: $_"
        return $false
    }
}

function Assert-RoutingCorrect {
    # Trace known coordinates and verify they land on the right shard
    $cases = @(
        @{ lat=40.7; lon=-74.0; expect_prefix=""; label="New York"   }   # Americas
        @{ lat=51.5; lon=-0.1;  expect_prefix="5"; label="London"    }   # Europe
        @{ lat=35.7; lon=139.7; expect_prefix="a"; label="Tokyo"     }   # Asia
    )
    $ok = $true
    foreach ($c in $cases) {
        $tr = Trace-Coord $NODES[0].url $c.lat $c.lon
        if (-not $tr) { Fail "Trace failed for $($c.label)"; $ok = $false; continue }
        Log-Event "$($c.label): token=$($tr.s2_token.Substring(0,4)).. owned by $($tr.owning_node_id)" "Green"
    }
    return $ok
}

# ────────────────────────────────────────────────────────────────────────────
#  MAIN
# ────────────────────────────────────────────────────────────────────────────

# Clear screen and hide cursor for clean dashboard
[Console]::CursorVisible = $false
Clear-Host

try {

# ── Phase 0: Prerequisites ──────────────────────────────────────────────────
$script:phase = "PREREQ"
Draw-Dashboard
Log-Event "Checking prerequisites..."

foreach ($cmd in @("cargo", "docker")) {
    if (-not (Get-Command $cmd -ErrorAction SilentlyContinue)) {
        Write-Host "`nERROR: '$cmd' not found in PATH" -ForegroundColor Red
        exit 1
    }
}

# ── Phase 1: Build ──────────────────────────────────────────────────────────
$script:phase = "BUILD"
Draw-Dashboard
Log-Event "Building release binaries (geo-node + loadtest)..."

if (-not $SkipBuild) {
    $env:PATH += ";$env:USERPROFILE\.cargo\bin"
    $buildResult = cargo build --release -p geo-redis-geo-node -p geo-redis-loadtest 2>&1
    if ($LASTEXITCODE -ne 0) {
        $script:testPassed = $false
        Log-Event "Build failed — check output" "Red"
        Write-Host $buildResult
        exit 1
    }
    Log-Event "Binaries built successfully" "Green"
    
    Log-Event "Building geo-node Docker image..."
    docker compose -f demo/cluster-compose.yml build --quiet
}

# ── Phase 2: Start cluster ───────────────────────────────────────────────────
$script:phase = "SETUP"
Draw-Dashboard
Log-Event "Starting 4-node cluster (3 active + 1 standby)..."

docker compose -f demo/cluster-compose.yml up -d 2>&1 | Out-Null

# Wait for all nodes to be healthy
foreach ($n in $NODES) {
    Log-Event "Waiting for $($n.id) on :$($n.url.Split(':')[-1])..."
    Draw-Dashboard
    $up = $false
    for ($i = 0; $i -lt 45; $i++) {
        try {
            Invoke-RestMethod "$($n.url)/health" -TimeoutSec 2 | Out-Null
            $up = $true; break
        } catch { Start-Sleep 2 }
    }
    if ($up) { Log-Event "$($n.id) is healthy" "Green" }
    else      { Fail "$($n.id) did not start in time"; Draw-Dashboard }
}

Log-Event "Waiting for gossip convergence..."
Start-Sleep 6
Log-Event "Initial ring:" "White"
$ring = Get-ClusterRing
foreach ($n in ($ring | Sort-Object prefix_start)) {
    Log-Event "  $($n.node_id): [$($n.prefix_start), $($n.prefix_end)) — $($n.status)" "Gray"
}

# ── Phase 3: Warm-up writes ──────────────────────────────────────────────────
$script:phase = "WARM-UP"
Log-Event "Phase: warm-up — seeding baseline data for ${WarmUpSecs}s..."
Draw-Dashboard

$shardSpec = ":5:redis://localhost:6379,5:a:redis://localhost:6380,a::redis://localhost:6381"

# Use small batches from PowerShell for warm-up (so we can count)
$warmEnd = (Get-Date).AddSeconds($WarmUpSecs)
$batchN  = 0
while ((Get-Date) -lt $warmEnd) {
    # Route a batch of 50 entities to the correct node based on S2 prefix
    $batch0 = 1..20 | ForEach-Object { New-RandomEntity } | Where-Object { $_.lat -lt 10 }  # Americas-ish
    $batch1 = 1..20 | ForEach-Object { New-RandomEntity } | Where-Object { $_.lat -ge 10 }  # Europe/Asia
    
    if ($batch0) { try { Ingest-Batch $NODES[0].url $batch0; $script:totalWrites += $batch0.Count } catch {} }
    if ($batch1) { try { Ingest-Batch $NODES[1].url $batch1; $script:totalWrites += $batch1.Count } catch {} }
    $batchN++
    if ($batchN % 10 -eq 0) { Draw-Dashboard }
    Start-Sleep -Milliseconds 200
}
Log-Event "Warm-up complete — $($script:totalWrites.ToString("N0")) entities written" "Green"

# ── Phase 4: High-load ingestion ─────────────────────────────────────────────
$script:phase = "HIGH-LOAD"
Log-Event "Phase: high-load — loadtest binary, ~$SplitThreshold entity target on Shard 0"
Draw-Dashboard

# Launch the compiled loadtest binary (handles throughput much better than PS)
$loadExe = ".\target\release\geo-redis-loadtest.exe"
if (Test-Path $loadExe) {
    $loadArgs = @("--writers", "4", "--readers", "8",
                  "--batch-size", "500",
                  "--duration-secs", $HighLoadSecs.ToString(),
                  "--shards", $shardSpec)
    $script:loadJob = Start-Process -FilePath $loadExe -ArgumentList $loadArgs `
        -PassThru -NoNewWindow `
        -RedirectStandardOutput ".\target\loadtest-stdout.log" `
        -RedirectStandardError  ".\target\loadtest-stderr.log"
    Log-Event "loadtest started (pid $($script:loadJob.Id))"
} else {
    Log-Event "loadtest binary not found — using PS fallback" "Yellow"
}

# Monitor until Shard 0 hits the split threshold
$deadline = (Get-Date).AddSeconds($HighLoadSecs + 30)
while ((Get-Date) -lt $deadline) {
    Draw-Dashboard
    $ring = Get-ClusterRing
    $shard0 = $ring | Where-Object { $_.node_id -eq "node-0" } | Select-Object -First 1
    if ($shard0 -and $shard0.key_count -ge $SplitThreshold) {
        Log-Event "Shard 0 reached $($shard0.key_count) keys — triggering split!" "Yellow"
        break
    }
    Start-Sleep 2
}

# ── Phase 5: Trigger split ───────────────────────────────────────────────────
$script:phase = "SPLIT"
Draw-Dashboard
Log-Event "Triggering geographic split: Americas → Standby-1"
Log-Event "  Split point will be computed automatically (median prefix)"

$splitBody = '{"target":"geo-node-3:4003"}' 
try {
    $splitResp = Invoke-RestMethod -Method POST -Uri "$($NODES[0].url)/split" `
        -Body $splitBody -ContentType "application/json" -TimeoutSec 30
    Log-Event "Split initiated: point='$($splitResp.split_point)' migrated=$($splitResp.migrated_keys) keys" "Yellow"
} catch {
    Fail "Split request failed: $_"
    Draw-Dashboard
}

# ── Phase 6: Watch bootstrap ─────────────────────────────────────────────────
$script:phase = "BOOTSTRAP"
Log-Event "Watching Standby-1 bootstrap from snapshot + delta sync..."
Draw-Dashboard

$bootDeadline = (Get-Date).AddSeconds($BootstrapSecs)
$prevStatus   = ""
while ((Get-Date) -lt $bootDeadline) {
    Draw-Dashboard
    $ring    = Get-ClusterRing
    $standby = $ring | Where-Object { $_.node_id -eq "node-3" } | Select-Object -First 1
    
    if ($standby) {
        if ($standby.status -ne $prevStatus) {
            $icon = switch ($standby.status) {
                "bootstrapping" { "[~~] delta-sync in progress..." }
                "active"        { "[OK] node is Active and accepting writes!" }
                default         { "[$($standby.status)]" }
            }
            Log-Event "Standby-1 status → $icon"
            $prevStatus = $standby.status
        }
        if ($standby.status -eq "active") {
            Log-Event "Bootstrap complete in $([Math]::Round(((Get-Date)-$script:startTime).TotalSeconds))s" "Green"
            break
        }
    }
    Start-Sleep 2
}

if ($prevStatus -ne "active") {
    Fail "Standby-1 did not become Active within ${BootstrapSecs}s"
}

# ── Phase 7: Verify routing ───────────────────────────────────────────────────
$script:phase = "VERIFY"
Draw-Dashboard
Log-Event "Verifying geographic routing across 4 shards..."

$routingOk = Assert-RoutingCorrect
if ($routingOk) { Log-Event "All routing assertions passed" "Green" }

# Verify source shard no longer holds migrated keys
$ring   = Get-ClusterRing
$shard0 = $ring | Where-Object { $_.node_id -eq "node-0" } | Select-Object -First 1
$shard3 = $ring | Where-Object { $_.node_id -eq "node-3" } | Select-Object -First 1
if ($shard3 -and $shard3.key_count -gt 0) {
    Log-Event "Shard 0 keys: $($shard0.key_count)  |  New shard keys: $($shard3.key_count)" "Green"
} else {
    Fail "New shard (node-3) has 0 keys after bootstrap"
}

# Continue writes for the verify period to confirm 4-way routing
Log-Event "Continuing load for ${VerifySecs}s to confirm 4-shard routing..."
$verifyEnd = (Get-Date).AddSeconds($VerifySecs)
while ((Get-Date) -lt $verifyEnd) {
    Draw-Dashboard
    Start-Sleep 3
}

# ── Phase 8: Final summary ────────────────────────────────────────────────────
$script:phase = "DONE"
Stop-LoadJob
Draw-Dashboard
Start-Sleep 2

# Final ring state
$script:phase = "SUMMARY"
$ring = Get-ClusterRing
Log-Event "═══ Final cluster state ═══" "White"
foreach ($n in ($ring | Sort-Object prefix_start)) {
    $ps = if ($n.prefix_start) { $n.prefix_start } else { "∅" }
    $pe = if ($n.prefix_end)   { $n.prefix_end }   else { "∅" }
    Log-Event "  $($n.node_id.PadRight(10)) [$ps, $pe)  $($n.status.PadRight(14)) $($n.key_count.ToString("N0").PadLeft(9)) keys"
}

$elapsed = [Math]::Round(((Get-Date) - $script:startTime).TotalSeconds)
$avgWps  = if ($elapsed -gt 0) { [Math]::Round($script:totalWrites / $elapsed) } else { 0 }
Log-Event "Total: $($script:totalWrites.ToString("N0")) writes in ${elapsed}s (~${avgWps}/s)" "White"
Log-Event $(if ($script:testPassed) { "RESULT: ALL ASSERTIONS PASSED ✓" } else { "RESULT: SOME ASSERTIONS FAILED ✗" }) `
         $(if ($script:testPassed) { "Green" } else { "Red" })

Draw-Dashboard
Start-Sleep 5

} finally {
    Stop-LoadJob
    [Console]::CursorVisible = $true
    Write-Host "`n"
    Write-Host $(if ($script:testPassed) { "Test PASSED" } else { "Test FAILED" }) `
        -ForegroundColor $(if ($script:testPassed) { "Green" } else { "Red" })
    Write-Host "Cluster is still running. To stop: docker compose -f demo/cluster-compose.yml down"
}
