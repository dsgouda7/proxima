Write-Host "GeoRedis — one-time setup" -ForegroundColor Cyan

# ── Rust ──────────────────────────────────────────────────────────────────
if (-not (Get-Command cargo -ErrorAction SilentlyContinue)) {
    Write-Host "Installing Rust via rustup..." -ForegroundColor Yellow
    Invoke-WebRequest https://win.rustup.rs/x86_64 -OutFile rustup-init.exe
    .\rustup-init.exe -y --default-toolchain stable
    Remove-Item rustup-init.exe
    $env:PATH += ";$env:USERPROFILE\.cargo\bin"
}

# ── Node ──────────────────────────────────────────────────────────────────
if (-not (Get-Command node -ErrorAction SilentlyContinue)) {
    Write-Error "Node.js >= 20 is required. Download from https://nodejs.org"
    exit 1
}

# ── Docker ────────────────────────────────────────────────────────────────
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    Write-Error "Docker Desktop is required. Download from https://docker.com"
    exit 1
}

# ── UI deps ───────────────────────────────────────────────────────────────
Write-Host "Installing UI dependencies..." -ForegroundColor Yellow
Set-Location $PSScriptRoot\..
Push-Location demo/ui
npm install
Pop-Location

Write-Host ""
Write-Host "Setup complete. Run .\scripts\run-demo.ps1 to start." -ForegroundColor Green
