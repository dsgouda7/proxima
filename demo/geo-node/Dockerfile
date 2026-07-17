# ── Build stage ───────────────────────────────────────────────────────────
FROM rust:1.87-slim AS builder

RUN apt-get update && apt-get install -y pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy manifests first for layer caching
COPY Cargo.toml Cargo.lock ./
COPY lib/Cargo.toml lib/
COPY demo/geo-node/Cargo.toml    demo/geo-node/
COPY demo/server/Cargo.toml      demo/server/
COPY demo/loadtest/Cargo.toml    demo/loadtest/
COPY demo/weather-server/Cargo.toml demo/weather-server/
COPY demo/experiments/Cargo.toml demo/experiments/

# cluster-test is a workspace member — stub it so cargo resolves
COPY demo/cluster-test/Cargo.toml demo/cluster-test/

# Stub all source files so cargo can resolve deps
RUN mkdir -p lib/src demo/geo-node/src demo/server/src demo/loadtest/src \
             demo/weather-server/src demo/cluster-test/src demo/experiments/src && \
    echo "fn main(){}" > demo/geo-node/src/main.rs && \
    echo "fn main(){}" > demo/server/src/main.rs && \
    echo "fn main(){}" > demo/loadtest/src/main.rs && \
    echo "fn main(){}" > demo/weather-server/src/main.rs && \
    echo "fn main(){}" > demo/cluster-test/src/main.rs && \
    echo "fn main(){}" > demo/experiments/src/main.rs && \
    echo "" > lib/src/lib.rs

# Cache dependencies only
RUN cargo build --release -p geo-redis-geo-node 2>/dev/null; true

# Copy real source
COPY lib/ lib/
COPY demo/geo-node/ demo/geo-node/

# Verify the ZADD fix is present before compiling (fails loudly if COPY served stale cache)
RUN grep -q 'cmd("ZADD")' lib/src/store.rs && \
    echo "ZADD fix confirmed in store.rs" || \
    { echo "ERROR: COPY served stale store.rs — fix not found"; exit 1; }

# Remove pre-built artifacts for our own crates so Cargo always recompiles
# them from the current source, avoiding stale-cache bugs.
RUN cargo clean -p geo-redis -p geo-redis-geo-node && \
    cargo build --release -p geo-redis-geo-node

# ── Runtime stage ─────────────────────────────────────────────────────────
FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y ca-certificates libssl3 curl && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/geo-redis-geo-node /usr/local/bin/

EXPOSE 4000

HEALTHCHECK --interval=5s --timeout=3s --retries=5 \
    CMD curl -f http://localhost:${HTTP_PORT:-4000}/health || exit 1

CMD ["geo-redis-geo-node"]
