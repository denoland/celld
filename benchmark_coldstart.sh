#!/bin/bash
set -euo pipefail

echo "Building production version..."
cargo build --release

echo "Cleaning up any existing processes..."
pkill -f "deno run" || true
sleep 1

echo "Removing any stale socket files..."
find ./data -name "*.sock" -type s -delete 2>/dev/null || true

echo "Starting server with tracing disabled..."
RUST_LOG=error ./target/release/self-hosted-deno-deploy &
SERVER_PID=$!

# Give server time to start
sleep 2

echo "Running coldstart benchmark..."
hey -n 100 -c 10 -host ry.local -H "x-single-use-isolate: true" http://127.0.0.1:3000/foo

echo "Cleaning up..."
kill $SERVER_PID
wait $SERVER_PID 2>/dev/null || true

echo "Benchmark complete"