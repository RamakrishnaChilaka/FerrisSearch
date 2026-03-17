# ── Build stage ───────────────────────────────────────────────────────────────
FROM rust:1.85-bookworm AS builder

# Install protobuf compiler (required by tonic-build)
RUN apt-get update && apt-get install -y protobuf-compiler && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Cache dependency builds: copy manifests first, then build a dummy to warm cache
COPY Cargo.toml Cargo.lock build.rs ./
COPY proto/ proto/
RUN mkdir -p src && echo "fn main() {}" > src/main.rs && echo "" > src/lib.rs \
    && cargo build --release 2>/dev/null || true \
    && rm -rf src

# Copy full source and build for real
COPY src/ src/
COPY config/ config/
RUN cargo build --release

# ── Runtime stage ────────────────────────────────────────────────────────────
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/ferrissearch /usr/local/bin/ferrissearch
COPY config/ /etc/ferrissearch/config/

WORKDIR /data

ENV FERRISSEARCH_DATA_DIR=/data
ENV FERRISSEARCH_HTTP_PORT=9200
ENV FERRISSEARCH_TRANSPORT_PORT=9300

EXPOSE 9200 9300

ENTRYPOINT ["ferrissearch"]
