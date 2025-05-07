# Build stage
FROM rust:1.86-slim-bookworm AS builder

# Install build dependencies
RUN apt update -y && apt install -y \
    apt-transport-https \
    build-essential \
    pkg-config \
    libssl-dev \
    cmake \
    && rm -rf /var/lib/apt/lists/*

# Create build directory
WORKDIR /build

# Copy only source files
COPY Cargo.toml Cargo.lock ./
COPY vendor ./vendor/
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

# Final stage
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user for the application
RUN groupadd -g 1000 celld && \
    useradd -u 1000 -g celld -s /bin/bash -m celld

# Create data directory and set permissions
RUN mkdir -p /var/lib/celld/data && chown -R celld:celld /var/lib/celld

# Set the working directory
WORKDIR /app

# Copy the binary from the builder stage
COPY --from=builder /build/target/release/celld /usr/local/bin/
COPY --from=runtime /usr/local/bin/deno /usr/local/bin/deno

# Switch to non-root user
USER celld

# Environment variables with defaults
ENV RUST_LOG=info

# Command to run the application
CMD ["celld"]
