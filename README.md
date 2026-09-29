# Arbitrage Bot

A multi-language monorepo for researching, simulating, monitoring, and eventually executing arbitrage strategies.

## Architecture

- **Python**: strategy research, persistence, analytics, orchestration, and APIs.
- **Rust**: latency-sensitive market data, order books, scanning, execution, and risk controls.
- **PostgreSQL**: full-fidelity opportunity history and analytics.
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
Python defines the financial reference model and Rust mirrors it, including fees and execution
allowances with cross-language parity tests.

### Phase 7: Opportunity database and analytics

Every detected scan can now be streamed into PostgreSQL.

The analytics funnel separates:

```text
detected -> executable -> accepted
```

where accepted means the route was fully executable and passed the configured expected-net
threshold. It does **not** mean an order was submitted.

PostgreSQL records gross edge, fees, slippage allowance, rounding allowance, latency buffer,
safety margin, net edge, liquidity, rejection reason, raw scan evidence, and continuous
opportunity duration.

Start PostgreSQL and migrate:

```bash
docker compose -f docker/docker-compose.yml up -d postgres
cd python
alembic upgrade head
```

Live pipeline:

```bash
cd rust
cargo run -p market-data \
  | cargo run -p scanner --bin scan-live \
  | (cd ../python && python -m analytics.opportunity_ingest)
```

Backfill an existing journal:

```bash
cd python
python -m analytics.opportunity_ingest --file ../data/scans/arbitrage_scans.ndjson
```

Analytics endpoints:

```text
GET /analytics/opportunities/summary
GET /analytics/opportunities/rejections
GET /analytics/opportunities/triangles
```

See `docs/OPPORTUNITY_ANALYTICS.md`.

## Security

Bybit credentials and PostgreSQL production credentials must come from environment variables or a
secrets manager. Live trading remains disabled:

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

cd ..
python scripts/check_profitability_parity.py
```

## Current status

Phase 7 PostgreSQL opportunity persistence and funnel analytics are implemented.
**No trade execution is enabled.**
