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

## Phase 1 goals

The foundation phase establishes:

- environment-variable based configuration;
- structured logging;
- coding and formatting standards;
- Docker development setup;
- CI checks for Python, Rust, and TypeScript;
- API-key and secret handling rules;
- starter health endpoints and test scaffolding.

## Security

**Never commit exchange credentials.** Bybit API keys, secrets, signing keys, account IDs, and other credentials must come from environment variables or a secrets manager.

1. Copy `.env.example` to `.env` locally.
2. Add real credentials only to `.env` or your deployment platform's secret store.
3. `.env`, private keys, credentials, and common secret artifacts are excluded by `.gitignore`.
4. CI uses GitHub Actions secrets where credentials are ever required.
5. Start with Bybit testnet credentials. Live trading should remain disabled until explicit risk controls and operational safeguards are implemented.

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

Run the API:

```bash
uvicorn api.main:app --reload --host 0.0.0.0 --port 8000
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

## Configuration conventions

Environment variables use the `ARB_` prefix for application-level settings and `BYBIT_` for exchange credentials.

Sensitive values are never logged. Configuration parsing should fail fast when required production values are absent.

## Coding standards

- Python: Ruff, pytest, type hints, Black-compatible formatting.
- Rust: rustfmt, Clippy with warnings denied, unit/integration tests.
- TypeScript: strict TypeScript, ESLint, React best practices.
- Commits: concise imperative messages, ideally Conventional Commits.
- Pull requests: CI must pass before merge.

## Current status

Phase 1 scaffold only. No production trading logic is enabled.
