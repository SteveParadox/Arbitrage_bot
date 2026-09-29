#!/usr/bin/env sh
set -eu

(cd python && python -m pytest && ruff check .)
(cd rust && cargo fmt --check && cargo clippy --workspace --all-targets --all-features -- -D warnings && cargo test --workspace)
(cd frontend && npm install && npm run lint && npm run build)
