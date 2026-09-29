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

The Rust scanner indexes routes by symbol and recalculates only affected triangles on each
order-book update. Every gross route evaluation is journaled.

### Phase 6: Fee and profitability engine

Python defines the reference financial model and Rust mirrors it in the live scanner.

For each completed route:

```text
gross profit
- compounded Bybit spot fees
- extra slippage allowance
- rounding-loss allowance
- latency buffer
- safety margin
= expected net profit
```

The reference configuration is:

```text
shared/config/profitability.json
```

and defaults to three 10 bps spot-taker fees plus configurable execution buffers.

The Phase 3 order-book walk already includes visible depth slippage. Phase 6's slippage allowance
is additional adverse execution beyond the current snapshot, avoiding double-counting.

Python/Rust parity uses shared exact-decimal fixtures:

```bash
python scripts/check_profitability_parity.py
```

See `docs/PROFITABILITY.md`.

## Live scanner

Generate current USDT triangles:

```bash
cd python
python -m strategy.triangle_discovery --start-assets USDT
```

Then run from `rust/`:

```bash
cargo run -p market-data | cargo run -p scanner --bin scan-live
```

Scan evidence is appended to:

```text
data/scans/arbitrage_scans.ndjson
```

## Security

Bybit credentials are never hardcoded. Live trading remains disabled:

```env
ARB_LIVE_TRADING_ENABLED=false
```

Phase 6 calculates expected profitability only. It does not submit orders.

## Testing

```bash
cd python
pytest
ruff check .

cd ../rust
cargo fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace

cd ..
python scripts/check_profitability_parity.py
```

## Current status

Phase 6 fee-aware expected-net profitability and Python/Rust parity are implemented.
**No trade execution is enabled.**
