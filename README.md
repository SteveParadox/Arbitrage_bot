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

Python discovers valid Bybit spot three-asset cycles and writes explicit BUY/SELL route semantics
to `shared/config/triangles.json`. Rust validates and loads those routes.

### Phase 5: Arbitrage scanner

The Rust scanner indexes routes by symbol. Every relevant order-book update therefore scans only
the affected routes.

Each three-leg path uses the actual Phase 3 executable-price calculations:

```text
configured start amount
        |
        v
leg 1 actual fill output
        |
        v
leg 2 actual fill output
        |
        v
leg 3 actual fill output
        |
        v
gross final amount / P&L
```

The scanner records profitable, unprofitable, missing-book, and insufficient-liquidity evaluations
as append-only NDJSON.

Default scanner notional:

```text
USDT = 450
```

Configure it in `shared/config/scanner.json`.

Generate current USDT triangles:

```bash
cd python
python -m strategy.triangle_discovery --start-assets USDT
```

Then run the live Rust pipeline from `rust/`:

```bash
cargo run -p market-data | cargo run -p scanner --bin scan-live
```

Scan evidence is appended to:

```text
data/scans/arbitrage_scans.ndjson
```

See `docs/SCANNER.md` for the record format and event flow.

## Security

Bybit credentials are never hardcoded. The implemented scanner is observational only.
Live trading remains disabled:

```env
ARB_LIVE_TRADING_ENABLED=false
```

Phase 5 does not submit orders and scan records explicitly report `execution_enabled: false`.

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

Phase 5 event-driven gross arbitrage scanning and journaling are implemented.
**No trade execution is enabled.**
