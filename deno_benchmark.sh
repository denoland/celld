#!/bin/bash
set -euo pipefail

echo "Building production version..."
cargo build --release

echo "Starting benchmark..."
DENO_BENCH=1 ./target/release/celld
