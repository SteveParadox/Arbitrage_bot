# Arbitrage Bot

A multi-language monorepo for researching, simulating, monitoring, and eventually executing arbitrage strategies.

## Architecture

- **Python**: strategy research, simulation, analytics, orchestration, and APIs.
- **Rust**: latency-sensitive market data, order-book processing, scanning, execution, and risk controls.
- **TypeScript + React**: operational dashboard and frontend.
- **Shared**: cross-service schemas and configuration contracts.

## Repository layout

```text
arbitrage-bot/
├── python/
│   ├── strategy/
│   ├── simulator/
│   ├── analytics/
│   ├── api/
│   └── tests/
├── rust/
│   ├── market-data/
│   ├── orderbook/
│   ├── scanner/
│   ├── execution/
│   └── risk/
├── frontend/
├── shared/
│   ├── schemas/
│   └── config/
├── docker/
├── scripts/
├── docs/
└── README.md
```

## Phase 1: Foundation

The foundation establishes environment configuration, structured logging, coding standards, Docker development, CI, tests, and secret handling.

## Phase 2: Bybit market data

The Rust `market-data` service now consumes Bybit V5 public feeds for:

- order books;
- best bid / ask;
- public trades;
- ticker updates;
- instrument metadata.

It maintains local books from snapshots/deltas, validates sequence monotonicity, sends heartbeats, detects stale data, and reconnects with bounded exponential backoff.

Run it with:

```bash
cd rust
cargo run -p market-data
```

Default configuration uses Bybit testnet, `linear`, `BTCUSDT,ETHUSDT`, and depth 50. See `docs/MARKET_DATA.md` and `.env.example`.

The service emits newline-delimited normalized JSON. Example:

```json
{
  "type": "quote",
  "symbol": "BTCUSDT",
  "bid": 68250.1,
  "ask": 68250.2,
  "timestamp": 1790549000000
}
```

## Security

**Never commit exchange credentials.** Bybit API keys, secrets, signing keys, account IDs, and other credentials must come from environment variables or a secrets manager.

Phase 2 requires no API key because it uses public market data only. API credential variables remain placeholders for later private/trading phases.

Live trading remains disabled:

```env
ARB_LIVE_TRADING_ENABLED=false
```

## Local development

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

### Rust

```bash
cd rust
cargo fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
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

## Coding standards

- Python: Ruff, pytest, type hints, Black-compatible formatting.
- Rust: rustfmt, Clippy with warnings denied, unit/integration tests.
- TypeScript: strict TypeScript, ESLint, React best practices.
- Pull requests: CI must pass before merge.

## Current status

Phase 2 market-data ingestion implemented. **No trading is enabled.**
