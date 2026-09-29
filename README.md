# Arbitrage Bot

A multi-language monorepo for researching, simulating, monitoring, and eventually executing arbitrage strategies.

## Architecture

- **Python**: strategy research, simulation, analytics, orchestration, and APIs.
- **Rust**: latency-sensitive market data, order-book processing, scanning, execution, and risk controls.
- **TypeScript + React**: operational dashboard and frontend.
- **Shared**: cross-service schemas and configuration contracts.

## Current phases

### Phase 1: Foundation

Environment configuration, logging, coding standards, Docker, CI, tests, and secret handling.

### Phase 2: Bybit market data

The Rust `market-data` service consumes Bybit V5 public order books, trades, tickers, and instrument metadata with heartbeat, stale-feed detection, sequence validation, and reconnect handling.

### Phase 3: Local order-book engine

The dedicated Rust `orderbook` crate now maintains per-symbol in-memory books with:

- best bid / ask;
- top-N bid and ask levels;
- available base and quote liquidity;
- timestamp, update ID, and sequence;
- snapshot and delta application;
- depth-aware executable-price estimates;
- slippage;
- partial-fill detection.

For example, a request to spend `400 USDT` buying ETH is priced by walking the ask side of `ETHUSDT`, rather than assuming the entire order fills at the best ask.

The engine exposes:

```text
buy_with_quote(symbol, quote_quantity)
buy_base(symbol, base_quantity)
sell_base(symbol, base_quantity)
```

The actual filled output from one conversion can therefore become the input to a later triangular-arbitrage leg.

See `docs/ORDER_BOOK.md` for the execution-price model and `docs/MARKET_DATA.md` for the Bybit feed.

## Security

Bybit keys are never hardcoded. Phase 2 and Phase 3 use public market data only, and live trading remains disabled:

```env
ARB_LIVE_TRADING_ENABLED=false
```

## Local development

### Rust

```bash
cd rust
cargo fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
```

Run market data:

```bash
cd rust
cargo run -p market-data
```

### Python

```bash
cd python
python -m venv .venv
# Windows: .venv\Scripts\activate
# macOS/Linux: source .venv/bin/activate
pip install -e ".[dev]"
pytest
ruff check .
```

### Frontend

```bash
cd frontend
npm install
npm run dev
```

### Docker

```bash
cp .env.example .env
docker compose -f docker/docker-compose.yml up --build
```

## Current status

Phase 3 local order-book and executable-pricing engine implemented. **No trading is enabled.**
