#!/bin/bash
set -euo pipefail

echo "Building production version..."
cargo build --release

echo "Cleaning up any existing processes..."
pkill -f "deno run" || true
sleep 1

echo "Starting benchmark..."
DENO_BENCH=1 ./target/release/roomd
