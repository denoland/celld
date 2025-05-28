# Build stage
FROM rust:1.86-slim-bookworm AS builder

# Install build dependencies
RUN apt update -y && apt install -y \
    apt-transport-https \
    build-essential \
    pkg-config \
    libssl-dev \
    libsqlite3-dev \
    cmake \
    && rm -rf /var/lib/apt/lists/*

# Create build directory
WORKDIR /build

# Copy only source files
COPY Cargo.toml Cargo.lock ./
COPY src ./src/
COPY tests ./tests/

# Build the actual application
RUN cargo build --release

# Install Deno runtime dependencies
FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Install Deno binary
ENV DENO_INSTALL=/usr/local
RUN curl -fsSL https://deno.land/install.sh | sh

# Download and install Litestream
FROM  debian:bookworm-slim AS litestream
ARG TARGETARCH
WORKDIR /download
RUN apt update -y && apt install -y wget tar
RUN wget https://github.com/benbjohnson/litestream/releases/download/v0.3.9/litestream-v0.3.9-linux-${TARGETARCH}.tar.gz; \
  tar -zxf litestream-v0.3.9-linux-${TARGETARCH}.tar.gz;

# Final stage
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    sqlite3 \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user for the application
RUN groupadd -g 1000 celld && \
    useradd -u 1000 -g celld -s /bin/bash -m celld

# Create data directory and set permissions
RUN mkdir -p /data && chown -R celld:celld /data

# Set the working directory
WORKDIR /app

# Copy the binary from the builder stage
COPY --from=builder /build/target/release/celld /usr/local/bin/
COPY --from=runtime /usr/local/bin/deno /usr/local/bin/deno
COPY --from=litestream /download/litestream /usr/local/bin/litestream

# Switch to non-root user
USER celld

# Environment variables with defaults
ENV RUST_LOG=info
ENV DATA="/data"

# Command to run the application
CMD ["celld"]
