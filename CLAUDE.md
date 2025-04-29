# Self-Hosted Deno Deploy Development Guide

See docs/ and README.md for more details.

You can find documentation for litestream in the docs/litestream directory

Look in ~/src/pingora for examples. Especially ~/src/pingora/*/examples

I will often provide you with checklists. I want you to proceed one check box at
a time, ensuring the tests are running well, code is formatted and linted,
before checking the checkbox and making a commit. Then you stop to await further
instructions.

## Build, Run & Test Commands

- Build: `cargo build`
- Run: `cargo run`
- Test All: `cargo test`
- Test Single: `cargo test test_parse_http_headers_simple -- --nocapture`
- Lint: `cargo clippy`
- Format markdown or TS: `deno fmt file`

## Code Style Guidelines

- one line per imported symbol in rust
- don't try to be exaustive in corner cases, use unwrap() and unimplemented!()
  liberally.
- Formatting: Use 2-space indentation - run cargo fmt for rust code and deno fmt
  for TypeScript and markdown
- prefer single line commit messages
