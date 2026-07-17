#!/usr/bin/env bash
# geo-redis — unified demo launcher (Linux / macOS)
#
# Usage:
#   ./scripts/run-demo.sh                   # starts all servers + UIs
#   ./scripts/run-demo.sh --with-cluster    # also spins up the 4-node geo cluster
#   ./scripts/run-demo.sh --skip-build      # reuse existing binaries

set -euo pipefail
cd "$(dirname "$0")/.."

WITH_CLUSTER=0; SKIP_BUILD=0
for arg in "$@"; do
  case $arg in
    --with-cluster) WITH_CLUSTER=1 ;;
    --skip-build)   SKIP_BUILD=1   ;;
  esac
done

info() { echo -e "\033[36m$*\033[0m"; }
ok()   { echo -e "\033[32m  ✓ $*\033[0m"; }

# ── Prerequisites ─────────────────────────────────────────────────────────
for cmd in cargo node npm docker; do
  command -v "$cmd" &>/dev/null || { echo "Required: '$cmd' not in PATH"; exit 1; }
done

# ── .env ──────────────────────────────────────────────────────────────────
[[ -f .env ]] || { cp config/.env.example .env; ok "Created .env"; }

# ── Redis ─────────────────────────────────────────────────────────────────
info "Starting Redis..."
docker compose -f demo/docker-compose.yml up -d
sleep 2

# ── Optional: geo-node cluster ────────────────────────────────────────────
if [[ $WITH_CLUSTER -eq 1 ]]; then
  info "Building + starting 4-node geo cluster..."
  docker compose -f demo/cluster-compose.yml build -q
  docker compose -f demo/cluster-compose.yml up -d
  sleep 6
fi

# ── npm install ───────────────────────────────────────────────────────────
for dir in demo/ui demo/cluster-ui; do
  [[ -d "$dir/node_modules" ]] || { info "Installing $dir deps..."; (cd "$dir" && npm install --silent); }
done

# ── Build Rust binaries ───────────────────────────────────────────────────
if [[ $SKIP_BUILD -eq 0 ]]; then
  info "Building backends (first build ~60s)..."
  cargo build --release -p geo-redis-demo -p geo-redis-weather
fi

mkdir -p target

# ── Servers ───────────────────────────────────────────────────────────────
info "Starting OpenSky server    → :3000"
SERVER_PORT=3000 SQLITE_PATH=geo-redis.db \
  REDIS_URL="${REDIS_URL:-redis://127.0.0.1:6379}" \
  ./target/release/geo-redis-demo \
  >target/demo-stdout.log 2>target/demo-stderr.log &
P_DEMO=$!

info "Starting Weather server    → :3001"
SERVER_PORT=3001 SQLITE_PATH=geo-redis-weather.db \
  REDIS_URL=redis://127.0.0.1:6379/1 \
  WEATHER_POLL_SECS=60 \
  ./target/release/geo-redis-weather \
  >target/weather-stdout.log 2>target/weather-stderr.log &
P_WEATHER=$!

sleep 3

# ── Vite UI dev servers ───────────────────────────────────────────────────
info "Starting UI dev servers (5173 / 5174 / 5176)..."
(cd demo/ui         && npx vite)                                    >target/ui-opensky.log  2>&1 & P_UI0=$!
(cd demo/ui         && npx vite --config vite.weather.config.ts)    >target/ui-weather.log  2>&1 & P_UI1=$!
(cd demo/cluster-ui && npx vite)                                    >target/ui-cluster.log  2>&1 & P_UI2=$!

sleep 5

echo ""
echo "  ┌────────────────────────────────────────────────────────────┐"
echo "  │  OpenSky aircraft tracker  →  http://localhost:5173        │"
echo "  │  Live METAR weather map    →  http://localhost:5174        │"
echo "  │  Cluster monitor           →  http://localhost:5176        │"
[[ $WITH_CLUSTER -eq 1 ]] && \
echo "  │  Geo-node cluster          →  http://localhost:4000-4003   │"
echo "  └────────────────────────────────────────────────────────────┘"
echo ""
echo "  Logs in target/  |  Cluster test: cargo run -p georedis-cluster-test"
echo "  Press Ctrl+C to stop everything."

cleanup() {
  echo ""; echo "Stopping..."
  kill "$P_DEMO" "$P_WEATHER" "$P_UI0" "$P_UI1" "$P_UI2" 2>/dev/null || true
  docker compose -f demo/docker-compose.yml down
  [[ $WITH_CLUSTER -eq 1 ]] && docker compose -f demo/cluster-compose.yml down
  echo "Done."
}
trap cleanup INT TERM EXIT
wait "$P_DEMO"
