# geo-redis — experiment runner
#
# Starts Redis if needed, builds the experiment binary, runs all 5 experiments
# and saves output to target/experiment-results-{timestamp}.txt
#
# Usage:
#   .\scripts\run-experiments.ps1
#   .\scripts\run-experiments.ps1 -Skip "3,4"       # skip slow experiments
#   .\scripts\run-experiments.ps1 -SkipBuild         # reuse existing binary
#   .\scripts\run-experiments.ps1 -Redis "redis://remote:6379"

param(
    [string]$Redis    = "redis://127.0.0.1:6379",
    [string]$Skip     = "",
    [switch]$SkipBuild,
    [int]$WriteQps    = 300,
    [int]$DeltaSecs   = 3,
    [int]$Batches     = 200,
    [int]$BatchSize   = 100,
    [int]$Queries     = 500
)

Set-Location $PSScriptRoot\..
$ErrorActionPreference = "Stop"

$cargoBin = "$env:USERPROFILE\.cargo\bin"
if ($env:PATH -notlike "*$cargoBin*") { $env:PATH += ";$cargoBin" }

# ── Check prerequisites ───────────────────────────────────────────────────
foreach ($cmd in @("cargo", "docker")) {
    if (-not (Get-Command $cmd -ErrorAction SilentlyContinue)) {
        Write-Error "Required: '$cmd' not found"; exit 1
    }
}

# ── Start Redis if not reachable ──────────────────────────────────────────
$redisUp = $false
try {
    $pong = (Invoke-Expression "redis-cli -u $Redis ping" 2>$null)
    $redisUp = ($pong -eq "PONG")
} catch {}

if (-not $redisUp) {
    Write-Host "Redis not reachable — starting via Docker..." -ForegroundColor Yellow
    docker compose -f demo/docker-compose.yml up -d
    Start-Sleep -Seconds 3
}

# ── Build ─────────────────────────────────────────────────────────────────
if (-not $SkipBuild) {
    Write-Host "Building experiment binary..." -ForegroundColor Yellow
    cargo build --release -p geo-redis-experiments
    if ($LASTEXITCODE -ne 0) { Write-Error "Build failed"; exit 1 }
}

# ── Run experiments ───────────────────────────────────────────────────────
$ts      = Get-Date -Format "yyyyMMdd-HHmmss"
$outFile = "target\experiment-results-$ts.txt"

$runArgs = @(
    "--redis",       $Redis,
    "--write-qps",   $WriteQps,
    "--delta-secs",  $DeltaSecs,
    "--batches",     $Batches,
    "--batch-size",  $BatchSize,
    "--queries",     $Queries
)
if ($Skip) { $runArgs += "--skip", $Skip }

Write-Host ""
Write-Host "Running experiments..." -ForegroundColor Cyan
Write-Host "Output will be saved to: $outFile" -ForegroundColor DarkGray
Write-Host ""

$result = & ".\target\release\experiments.exe" @runArgs
$result | Tee-Object -FilePath $outFile

Write-Host ""
Write-Host "Results saved to $outFile" -ForegroundColor Green
