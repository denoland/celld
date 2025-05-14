#!/bin/bash
set -euo pipefail

echo "Building production version..."
cargo build --release

echo "Cleaning up any existing processes..."
pkill -f "deno run" || true
sleep 1

echo "Starting server with timing info enabled..."
RUST_LOG=info ./target/release/celld  &
SERVER_PID=$!

# Give server time to start
sleep 2

echo "Running coldstart benchmark..."

# Using a single cpu to make the benchmark more repeatable
hey -cpus 1 -n 1000 -c 10 \
  -host hello.localhost \
  -H "x-single-use-isolate: true" \
  http://127.0.0.1:8000/cell/foo

echo "Cleaning up..."
kill -9 $SERVER_PID
wait $SERVER_PID 2>/dev/null || true

echo "Benchmark complete"

# Current results on ry's machine:
#
# Summary:
#   Total:	2.1708 secs
#   Slowest:	0.4412 secs
#   Fastest:	0.0000 secs
#   Average:	0.0002 secs
#   Requests/sec:	46066.5377
# 
#   Total data:	2000000 bytes
#   Size/request:	20 bytes
# 
# Response time histogram:
#   0.000 [1]	|
#   0.044 [99989]	|■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
#   0.088 [0]	|
#   0.132 [0]	|
#   0.176 [0]	|
#   0.221 [1]	|
#   0.265 [2]	|
#   0.309 [2]	|
#   0.353 [2]	|
#   0.397 [1]	|
#   0.441 [2]	|
# 
# 
# Latency distribution:
#   10% in 0.0001 secs
#   25% in 0.0001 secs
#   50% in 0.0002 secs
#   75% in 0.0002 secs
#   90% in 0.0003 secs
#   95% in 0.0003 secs
#   99% in 0.0004 secs
# 
# Details (average, fastest, slowest):
#   DNS+dialup:	0.0000 secs, 0.0000 secs, 0.4412 secs
#   DNS-lookup:	0.0000 secs, 0.0000 secs, 0.0000 secs
#   req write:	0.0000 secs, 0.0000 secs, 0.0005 secs
#   resp wait:	0.0002 secs, 0.0000 secs, 0.4400 secs
#   resp read:	0.0000 secs, 0.0000 secs, 0.0003 secs
# 
# Status code distribution:
#   [200]	100000 responses
