# Tachyon — single static binary in a small image.
# Target: <50MB runtime image.
#
# Build against Alpine so the binary links against musl.
# The runtime image contains only Alpine + required runtime utilities
# + the Tachyon binary.

# ============================================================
# Builder
# ============================================================

FROM rust:1-alpine AS builder

# musl-dev: C runtime / linker support for native dependencies
# curl: required by utoipa-swagger-ui's build script to download
#       the Swagger UI distribution during compilation.
RUN apk add --no-cache \
    musl-dev \
    curl

WORKDIR /src

# ------------------------------------------------------------
# Copy manifests first.
# This allows Cargo's dependency compilation to remain cached
# when only source files change.
# ------------------------------------------------------------

COPY Cargo.toml Cargo.lock ./

COPY crates/tachyon-core/Cargo.toml crates/tachyon-core/
COPY crates/tachyon-storage/Cargo.toml crates/tachyon-storage/
COPY crates/tachyon-index/Cargo.toml crates/tachyon-index/
COPY crates/tachyon-query/Cargo.toml crates/tachyon-query/
COPY crates/tachyon-engine/Cargo.toml crates/tachyon-engine/
COPY crates/tachyon-server/Cargo.toml crates/tachyon-server/
COPY crates/tachyon-bench/Cargo.toml crates/tachyon-bench/

# ------------------------------------------------------------
# Create minimal source stubs.
#
# Cargo needs source files to resolve/build the workspace
# dependencies before the real source is copied.
# ------------------------------------------------------------

RUN mkdir -p \
      crates/tachyon-core/src \
      crates/tachyon-storage/src \
      crates/tachyon-index/src \
      crates/tachyon-query/src \
      crates/tachyon-engine/src \
      crates/tachyon-server/src \
      crates/tachyon-bench/src \
 && for crate in core storage index query engine; do \
      echo "" > crates/tachyon-$crate/src/lib.rs; \
    done \
 && echo "fn main() {}" > crates/tachyon-server/src/main.rs \
 && echo "" > crates/tachyon-server/src/lib.rs \
 && echo "fn main() {}" > crates/tachyon-bench/src/main.rs

# ------------------------------------------------------------
# Compile dependencies.
# ------------------------------------------------------------

RUN cargo build --release --locked --bin tachyon

# Remove stub sources before copying the real sources.
RUN rm -rf crates/*/src

# ------------------------------------------------------------
# Copy real source.
# ------------------------------------------------------------

COPY crates crates

# Touch real sources so Cargo recognizes them as newer than
# the previously compiled stub sources.
RUN find crates -name '*.rs' -exec touch {} +

# ------------------------------------------------------------
# Build Tachyon.
# ------------------------------------------------------------

RUN cargo build --release --locked --bin tachyon \
 && strip target/release/tachyon


# ============================================================
# Runtime
# ============================================================

FROM alpine:3.21

# ca-certificates: HTTPS/TLS support
# tini: proper PID 1 / signal forwarding / zombie reaping
# wget: HTTP health check
RUN apk add --no-cache \
    ca-certificates \
    tini \
    wget \
 && addgroup -S tachyon \
 && adduser -S -G tachyon -h /data tachyon

# Copy only the final binary from the builder.
COPY --from=builder /src/target/release/tachyon /usr/local/bin/tachyon

# Tachyon persistent data directory.
VOLUME ["/data"]

WORKDIR /data

# Never run Tachyon as root.
USER tachyon

EXPOSE 8108

ENV TACHYON_DATA_DIR=/data \
    TACHYON_LISTEN=0.0.0.0:8108

# tini becomes PID 1 and forwards SIGTERM to Tachyon,
# allowing graceful shutdown and WAL flushing.
ENTRYPOINT ["/sbin/tini", "--", "/usr/local/bin/tachyon"]

# Container health check.
HEALTHCHECK \
    --interval=30s \
    --timeout=3s \
    --start-period=5s \
    --retries=3 \
    CMD wget -q -O- http://127.0.0.1:8108/health || exit 1