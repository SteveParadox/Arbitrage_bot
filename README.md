# Arbitrage Bot

A multi-language monorepo for researching, simulating, monitoring, and eventually executing arbitrage strategies.

## Architecture

- **Python**: strategy research, persistence, analytics, paper simulation, orchestration, and APIs.
- **Rust**: latency-sensitive market data, order books, scanning, execution, and risk controls.
- **PostgreSQL**: opportunity history, archived order-book events, and simulation results.
- **TypeScript + React**: operational dashboard and frontend.

## Implemented phases

### Phase 1: Foundation
Environment configuration, logging, Docker, CI, tests, coding standards, and secret handling.

### Phase 2: Bybit market data
Rust consumes public Bybit V5 feeds with reconnect, heartbeat, stale-data, and sequence controls.

### Phase 3: Local order-book engine
Rust maintains depth and calculates executable rather than decorative prices.

### Phase 4: Triangle discovery
Python discovers valid spot triangles and Rust validates the route file.

### Phase 5: Arbitrage scanner
Rust recalculates only triangles affected by an order-book update and records every scan.

### Phase 6: Fee and profitability engine
Python defines the financial reference model and Rust mirrors it with shared parity fixtures.

### Phase 7: Opportunity database and analytics
PostgreSQL records every detected route evaluation and separates detected, executable, and
accepted opportunities.

### Phase 8: Paper trading simulator

Python now replays opportunities against historical order-book states after configurable execution
delays.

Default latency scenarios:

```text
25 ms
50 ms
100 ms
200 ms
500 ms
```

The three legs execute at:

```text
t0 + L
t0 + 2L
t0 + 3L
```

using the archived book at each timestamp. Fees are applied between legs, partial liquidity causes
a failed simulation, and stale historical books are rejected. Large runs load history in bounded
opportunity chunks so thousands of candidates do not require the full experiment's book archive in
memory at once.

Capture market data while scanning, ingest it, then replay thousands of opportunities:

```bash
# Capture while scanning
cd rust
cargo run -p market-data \
  | tee ../data/market/market_data.ndjson \
  | cargo run -p scanner --bin scan-live \
  | (cd ../python && python -m analytics.opportunity_ingest)

# Archive captured order books
cd ../python
python -m simulator.book_ingest --file ../data/market/market_data.ndjson

# Simulate 10k opportunities across five latency assumptions
alembic upgrade head
python -m simulator.paper_trade --hours 24 --limit 10000
```

Metrics include expected profit, simulated profit, fill rate, failure rate, opportunity lifetime,
execution drift/slippage, and failure reasons.

See `docs/PAPER_TRADING.md`.

### Phase 9: Risk engine

Rust now provides a mandatory pre-execution risk gate.

Every future trade intent must pass:

```text
market freshness
net edge
slippage
liquidity
balance
trade-size limit
symbol precision
total exposure
daily-loss limit
API health
exchange health
```

System failures latch circuit breakers. The default policy stops approval when market data is more
than 500 ms old and after 3 execution failures inside 5 minutes.

A restart-safe manual kill switch is available:

```bash
cd rust
cargo run -p risk --bin riskctl -- kill "operator emergency stop"
cargo run -p risk --bin riskctl -- status
```

The execution crate now requires a short-lived `RiskApproval` token and re-checks kill-switch /
breaker state immediately before preparing execution.

See `docs/RISK_ENGINE.md`.

## Security

Bybit credentials and PostgreSQL production credentials must come from environment variables or a
secrets manager. Live trading remains disabled:

```env
ARB_LIVE_TRADING_ENABLED=false
```

Phase 8 performs historical simulation only. Phase 9 adds risk gating but still does not submit
orders.

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

Phase 9 pre-trade risk gating and circuit breakers are implemented.
**No trade execution is enabled.**
