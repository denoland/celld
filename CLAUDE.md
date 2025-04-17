# Self-Hosted Deno Deploy Development Guide

## Build, Run & Test Commands
- **Build**: `cargo build`
- **Run**: `cargo run`
- **Test All**: `cargo test`
- **Test Single**: `cargo test test_name` (e.g. `cargo test test_parse_http_headers_simple`)
- **Lint**: `cargo clippy`
- **Format Check**: `cargo fmt --check`
- **Format Fix**: `cargo fmt`

## Code Style Guidelines
- **Imports**: Organize in blocks - std, external crates, internal modules
- **Naming**: Use snake_case for variables/functions, CamelCase for types/structs
- **Error Handling**: Use `Result<T, E>` with descriptive error messages, prefer `?` operator
- **Documentation**: Document public API with triple-slash comments (`///`)
- **Types**: Use strong typing, avoid `unwrap()` in production code
- **Formatting**: Use 2-space indentation
- **Testing**: Write unit tests for all public functions
- **Logging**: Use the `tracing` crate with appropriate log levels

## Architecture Overview
This is a Rust proxy for Deno processes that routes HTTP/WebSocket requests to isolated
Deno subprocesses based on the Host header or path parameters.