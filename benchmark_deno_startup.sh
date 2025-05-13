#!/bin/bash
set -euo pipefail

echo "Building production version..."
cargo build --release

echo "Starting benchmark..."
BENCHMARK_DENO_STARTUP=1 ./target/release/celld
