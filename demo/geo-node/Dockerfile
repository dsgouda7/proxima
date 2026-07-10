# ── Build stage ───────────────────────────────────────────────────────────
FROM rust:1.78-slim AS builder

RUN apt-get update && apt-get install -y pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy manifests first for layer caching
COPY Cargo.toml Cargo.lock ./
COPY lib/Cargo.toml lib/
COPY demo/geo-node/Cargo.toml demo/geo-node/
COPY demo/server/Cargo.toml demo/server/
COPY demo/loadtest/Cargo.toml demo/loadtest/

# Stub all source files so cargo can resolve deps
RUN mkdir -p lib/src demo/geo-node/src demo/server/src demo/loadtest/src && \
    echo "fn main(){}" > demo/geo-node/src/main.rs && \
    echo "fn main(){}" > demo/server/src/main.rs && \
    echo "fn main(){}" > demo/loadtest/src/main.rs && \
    echo "" > lib/src/lib.rs

# Cache dependencies only
RUN cargo build --release -p georedis-geo-node 2>/dev/null; true

# Copy real source
COPY lib/ lib/
COPY demo/geo-node/ demo/geo-node/

RUN touch lib/src/lib.rs demo/geo-node/src/main.rs && \
    cargo build --release -p georedis-geo-node

# ── Runtime stage ─────────────────────────────────────────────────────────
FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y ca-certificates libssl3 curl && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/georedis-geo-node /usr/local/bin/

EXPOSE 4000

HEALTHCHECK --interval=5s --timeout=3s --retries=5 \
    CMD curl -f http://localhost:${HTTP_PORT:-4000}/health || exit 1

CMD ["georedis-geo-node"]
