# Arbitrage Bot

A multi-language monorepo for researching, simulating, monitoring, and eventually executing arbitrage strategies.

## Architecture

- **Python**: strategy research, simulation, analytics, orchestration, and APIs.
- **Rust**: latency-sensitive market data, order-book processing, scanning, execution, and risk controls.
- **TypeScript + React**: operational dashboard and frontend.

## Implemented phases

### Phase 1: Foundation

Environment configuration, logging, Docker, CI, tests, coding standards, and secret handling.

### Phase 2: Bybit market data

Rust consumes Bybit V5 public order books, trades, tickers, and instrument metadata with
heartbeat, reconnection, stale-feed detection, and sequence validation.

### Phase 3: Local order-book engine

Rust maintains per-symbol books and calculates depth-aware executable prices rather than assuming
an entire order fills at the best bid or ask.

### Phase 4: Triangle discovery

Python now builds the Bybit spot asset graph and discovers every fully connected 3-asset cycle.

For each leg:

```text
base -> quote = SELL
quote -> base = BUY
```

Every starting asset and both directions are retained by default, or routes can be filtered to a
capital asset such as USDT.

Generate the route configuration:

```bash
cd python
python -m strategy.triangle_discovery --start-assets USDT
```

The generated file is:

```text
shared/config/triangles.json
```

Rust's `scanner` crate loads and structurally validates that file:

```bash
cd rust
cargo run -p scanner --bin validate-triangles
```

See `docs/TRIANGLE_DISCOVERY.md` for the route model.

## Security

Bybit credentials are never hardcoded. Phases 2 through 4 use public market information only.
Live trading remains disabled:

```env
ARB_LIVE_TRADING_ENABLED=false
```

## Testing

```bash
cd python
pytest
ruff check .

cd ../rust
cargo fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
```

## Current status

Phase 4 structural triangle discovery and Rust route loading are implemented.
**No trade execution is enabled.**
