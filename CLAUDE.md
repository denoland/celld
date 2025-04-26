# Self-Hosted Deno Deploy Development Guide

You can find documentation for litestream in the docs/litestream directory

## Build, Run & Test Commands

- Build: `cargo build`
- Run: `cargo run`
- Test All: `cargo test`
- Test Single: `cargo test test_parse_http_headers_simple -- --nocapture`
- Lint: `cargo clippy`

## Code Style Guidelines

- one line per imported symbol in rust
- don't try to be exaustive in corner cases, use unwrap() or panic!() as
  necessary
- Formatting: Use 2-space indentation - run cargo fmt for rust code and deno
  fmt for TypeScript and markdown

## Architecture Overview

See docs/ and README.md for more details.

Look in ~/src/pingora for examples. Especially ~/src/pingora/*/examples
