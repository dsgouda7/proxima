Write-Host "GeoRedis — starting demo..." -ForegroundColor Cyan

# ── prerequisites check ───────────────────────────────────────────────────
foreach ($cmd in @("cargo", "node", "npm", "docker")) {
    if (-not (Get-Command $cmd -ErrorAction SilentlyContinue)) {
        Write-Error "Required tool not found: $cmd"
        exit 1
    }
}

Set-Location $PSScriptRoot\..

# ── .env ─────────────────────────────────────────────────────────────────
if (-not (Test-Path .env)) {
    Copy-Item .env.example .env
    Write-Host "Created .env from .env.example" -ForegroundColor Green
}

# ── Redis via Docker ──────────────────────────────────────────────────────
Write-Host "Starting Redis..." -ForegroundColor Yellow
docker compose -f demo/docker-compose.yml up -d
Start-Sleep -Seconds 2

# ── UI node_modules ───────────────────────────────────────────────────────
if (-not (Test-Path demo/ui/node_modules)) {
    Write-Host "Installing UI dependencies (first run)..." -ForegroundColor Yellow
    Push-Location demo/ui
    npm install
    Pop-Location
}

# ── Rust backend ──────────────────────────────────────────────────────────
Write-Host "Building + starting backend (first build may take ~60s)..." -ForegroundColor Yellow
$backendEnv = Get-Content .env | Where-Object { $_ -match "^\s*[^#]" } |
    ForEach-Object { $k, $v = $_ -split "=", 2; [System.Environment]::SetEnvironmentVariable($k.Trim(), $v.Trim()) }

$backend = Start-Process cargo `
    -ArgumentList "run", "--release", "-p", "georedis-demo" `
    -PassThru -NoNewWindow
Write-Host "Backend PID: $($backend.Id)"

# ── Vite UI dev server ─────────────────────────────────────────────────────
Write-Host "Starting UI dev server..." -ForegroundColor Yellow
$ui = Start-Process npm `
    -WorkingDirectory "demo/ui" `
    -ArgumentList "run", "dev" `
    -PassThru -NoNewWindow
Write-Host "UI PID: $($ui.Id)"

Write-Host ""
Write-Host "  Open  →  http://localhost:5173" -ForegroundColor Green
Write-Host "  API   →  http://localhost:3000/api/health"
Write-Host "  Stats →  http://localhost:3000/api/metrics"
Write-Host ""
Write-Host "Press Ctrl+C to stop everything." -ForegroundColor Gray

try {
    Wait-Process -Id $backend.Id
} finally {
    Stop-Process -Id $backend.Id -ErrorAction SilentlyContinue
    Stop-Process -Id $ui.Id      -ErrorAction SilentlyContinue
    docker compose -f demo/docker-compose.yml down
    Write-Host "Stopped." -ForegroundColor Gray
}
