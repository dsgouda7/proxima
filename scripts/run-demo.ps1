# georedis — unified demo launcher
#
# Starts ALL demo components in one command:
#
#  Redis (Docker)
#  ├── georedis-demo      :3000  OpenSky aircraft tracker
#  └── georedis-weather   :3001  Live METAR weather (streams every 60 s)
#
#  Vite dev servers
#  ├── :5173  OpenSky tracker UI
#  ├── :5174  Weather map UI
#  └── :5176  Cluster monitor (tracks geo-node cluster + weather stream)
#
#  Geo-node cluster (Docker — optional, flag -WithCluster)
#  ├── :4000  node-0  Americas
#  ├── :4001  node-1  Europe/Asia
#  ├── :4002  node-2  Asia-Pacific
#  └── :4003  node-3  Standby
#
# Usage:
#   .\scripts\run-demo.ps1                  # starts all servers + UIs
#   .\scripts\run-demo.ps1 -WithCluster     # also spins up the 4-node geo cluster
#   .\scripts\run-demo.ps1 -SkipBuild       # reuse existing binaries

param(
    [switch]$WithCluster,
    [switch]$SkipBuild
)

Set-Location $PSScriptRoot\..
$ErrorActionPreference = "Stop"

# ── Ensure ~/.cargo/bin is on PATH (needed before prereq check) ───────────
$cargoBin = "$env:USERPROFILE\.cargo\bin"
if ($env:PATH -notlike "*$cargoBin*") { $env:PATH += ";$cargoBin" }

# ── Prerequisites ─────────────────────────────────────────────────────────
foreach ($cmd in @("cargo", "node", "npm", "docker")) {
    if (-not (Get-Command $cmd -ErrorAction SilentlyContinue)) {
        Write-Error "Required: '$cmd' not found in PATH"; exit 1
    }
}

# ── .env ──────────────────────────────────────────────────────────────────
if (-not (Test-Path .env)) {
    Copy-Item config\.env.example .env
    Write-Host "Created .env from config/.env.example" -ForegroundColor Green
}

# ── Redis (single-node demos) ─────────────────────────────────────────────
Write-Host "Starting Redis..." -ForegroundColor Yellow
docker compose -f demo/docker-compose.yml up -d
Start-Sleep -Seconds 2

# ── Optional: geo-node cluster ────────────────────────────────────────────
if ($WithCluster) {
    Write-Host "Building + starting 4-node geo cluster..." -ForegroundColor Yellow
    docker compose -f demo/cluster-compose.yml build --quiet
    docker compose -f demo/cluster-compose.yml up -d
    Start-Sleep -Seconds 6
}

# ── npm install ───────────────────────────────────────────────────────────
foreach ($dir in @("demo/ui", "demo/cluster-ui")) {
    if (-not (Test-Path "$dir/node_modules")) {
        Write-Host "Installing $dir dependencies..." -ForegroundColor Yellow
        Push-Location $dir; npm install --silent; Pop-Location
    }
}

# ── Build Rust binaries ───────────────────────────────────────────────────
if (-not $SkipBuild) {
    Write-Host "Building backends (first build ~60s)..." -ForegroundColor Yellow
    cargo build --release -p georedis-demo -p georedis-weather
    if ($LASTEXITCODE -ne 0) { Write-Error "Cargo build failed"; exit 1 }
}

# ── Load .env ─────────────────────────────────────────────────────────────
Get-Content .env | Where-Object { $_ -match "^\s*[^#]\S+=\S" } | ForEach-Object {
    $k, $v = $_ -split "=", 2
    [System.Environment]::SetEnvironmentVariable($k.Trim(), $v.Trim(), "Process")
}

# ── OpenSky demo server — :3000 ───────────────────────────────────────────
Write-Host "Starting OpenSky server    → :3000" -ForegroundColor Yellow
$env:SERVER_PORT = "3000"; $env:SQLITE_PATH = "georedis.db"
$env:REDIS_URL   = if ($env:REDIS_URL) { $env:REDIS_URL } else { "redis://127.0.0.1:6379" }
$p0 = Start-Process -FilePath ".\target\release\georedis-demo.exe" `
    -RedirectStandardOutput ".\target\demo-stdout.log" `
    -RedirectStandardError  ".\target\demo-stderr.log" -PassThru -NoNewWindow

# ── Weather server — :3001 ────────────────────────────────────────────────
Write-Host "Starting Weather server    → :3001" -ForegroundColor Yellow
$env:SERVER_PORT = "3001"; $env:SQLITE_PATH = "georedis-weather.db"
$env:REDIS_URL   = "redis://127.0.0.1:6379/1"
$p1 = Start-Process -FilePath ".\target\release\georedis-weather.exe" `
    -RedirectStandardOutput ".\target\weather-stdout.log" `
    -RedirectStandardError  ".\target\weather-stderr.log" -PassThru -NoNewWindow

Start-Sleep -Seconds 3

# ── Vite UI dev servers ───────────────────────────────────────────────────
Write-Host "Starting UI dev servers..." -ForegroundColor Yellow
$uiDir = Resolve-Path "demo/ui"
$clDir = Resolve-Path "demo/cluster-ui"

$ui0 = Start-Process "pwsh" -ArgumentList "-NoProfile","-Command",
    "Set-Location '$uiDir'; node_modules\.bin\vite.ps1" `
    -RedirectStandardOutput ".\target\ui-opensky.log" `
    -RedirectStandardError  ".\target\ui-opensky-err.log" -PassThru -NoNewWindow

$ui1 = Start-Process "pwsh" -ArgumentList "-NoProfile","-Command",
    "Set-Location '$uiDir'; node_modules\.bin\vite.ps1 --config vite.weather.config.ts" `
    -RedirectStandardOutput ".\target\ui-weather.log" `
    -RedirectStandardError  ".\target\ui-weather-err.log" -PassThru -NoNewWindow

$ui2 = Start-Process "pwsh" -ArgumentList "-NoProfile","-Command",
    "Set-Location '$clDir'; node_modules\.bin\vite.ps1" `
    -RedirectStandardOutput ".\target\ui-cluster.log" `
    -RedirectStandardError  ".\target\ui-cluster-err.log" -PassThru -NoNewWindow

Start-Sleep -Seconds 6

# ── Summary ───────────────────────────────────────────────────────────────
Write-Host ""
Write-Host "  ┌────────────────────────────────────────────────────────────┐" -ForegroundColor Cyan
Write-Host "  │  OpenSky aircraft tracker  →  http://localhost:5173        │" -ForegroundColor Cyan
Write-Host "  │  Live METAR weather map    →  http://localhost:5174        │" -ForegroundColor Cyan
Write-Host "  │  Cluster monitor           →  http://localhost:5176        │" -ForegroundColor Cyan
if ($WithCluster) {
Write-Host "  │  Geo-node cluster          →  http://localhost:4000-4003   │" -ForegroundColor Cyan
}
Write-Host "  └────────────────────────────────────────────────────────────┘" -ForegroundColor Cyan
Write-Host ""
Write-Host "  Logs in target/  |  Cluster test: cargo run -p georedis-cluster-test" -ForegroundColor DarkGray
Write-Host "  Press Ctrl+C to stop everything." -ForegroundColor Gray

try {
    Wait-Process -Id $p0.Id
} finally {
    foreach ($p in @($p0, $p1, $ui0, $ui1, $ui2)) {
        Stop-Process -Id $p.Id -ErrorAction SilentlyContinue
    }
    docker compose -f demo/docker-compose.yml down
    if ($WithCluster) { docker compose -f demo/cluster-compose.yml down }
    Write-Host "Stopped." -ForegroundColor Gray
}
