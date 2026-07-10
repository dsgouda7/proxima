#!/usr/bin/env bash
set -euo pipefail

echo "GeoRedis — one-time setup"

# Rust
if ! command -v cargo &>/dev/null; then
    echo "Installing Rust..."
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    source "$HOME/.cargo/env"
fi

# Node
if ! command -v node &>/dev/null; then
    echo "Error: Node.js >= 20 required. See https://nodejs.org"; exit 1
fi

# Docker
if ! command -v docker &>/dev/null; then
    echo "Error: Docker required. See https://docker.com"; exit 1
fi

# UI deps
cd "$(dirname "$0")/.."
echo "Installing UI dependencies..."
(cd demo/ui && npm install)

echo ""
echo "Setup complete. Run ./scripts/run-demo.sh to start."
