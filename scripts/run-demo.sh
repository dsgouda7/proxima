#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

# ── prerequisites ─────────────────────────────────────────────────────────
for cmd in cargo node npm docker; do
    command -v "$cmd" >/dev/null 2>&1 || { echo "Error: $cmd not found"; exit 1; }
done

# ── .env ──────────────────────────────────────────────────────────────────
[ -f .env ] || cp .env.example .env

# ── Redis ─────────────────────────────────────────────────────────────────
echo "Starting Redis..."
docker compose -f demo/docker-compose.yml up -d
sleep 2

# ── UI deps ───────────────────────────────────────────────────────────────
[ -d demo/ui/node_modules ] || (cd demo/ui && npm install)

# ── source .env ───────────────────────────────────────────────────────────
set -a; source .env; set +a

# ── Rust backend ──────────────────────────────────────────────────────────
echo "Building + starting backend (first build may take ~60s)..."
cargo run --release -p georedis-demo &
BACKEND=$!

# ── Vite UI ───────────────────────────────────────────────────────────────
echo "Starting UI dev server..."
(cd demo/ui && npm run dev) &
UI=$!

echo ""
echo "  Open  →  http://localhost:5173"
echo "  API   →  http://localhost:3000/api/health"
echo "  Stats →  http://localhost:3000/api/metrics"
echo ""
echo "Press Ctrl+C to stop."

cleanup() {
    kill "$BACKEND" "$UI" 2>/dev/null || true
    docker compose -f demo/docker-compose.yml down
    echo "Stopped."
}
trap cleanup INT TERM
wait "$BACKEND"
