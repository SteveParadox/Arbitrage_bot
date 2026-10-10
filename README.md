# Arbitrage Bot

A Rust + Python triangular-arbitrage research, simulation, paper-trading, monitoring, and controlled execution platform for Bybit, with a FastAPI control plane, Redis Streams event transport, PostgreSQL analytics, and a React + TypeScript operations dashboard.

> **Project status:** research and controlled-testing platform. Live-order primitives and a three-leg coordinator exist in Rust, but the repository does not currently package a single autonomous production live-trading process. Live trading is disabled by default and remains behind deployment, runtime-control, and risk gates.
>
> **No profitability claim:** technical correctness, backtests, replay results, shadow observations, or canary calibration do not guarantee profitable trading. Real results depend on fees, bid/ask spread, liquidity, queue position, latency, slippage, precision constraints, partial fills, outages, and exchange behavior.

The current control/reconciliation correction matrix, test evidence, merge blocker and readiness assessment are in [docs/PR2_CRITICAL_AUDIT.md](docs/PR2_CRITICAL_AUDIT.md).

The Phase 1-8 technical audit is available in [docs/AUDIT_REPORT_2026-09-30.md](docs/AUDIT_REPORT_2026-09-30.md). More focused implementation notes are linked throughout this README.

## Contents

- [What the project does](#what-the-project-does)
- [Implemented capabilities](#implemented-capabilities)
- [Technology stack](#technology-stack)
- [Runtime architecture](#runtime-architecture)
- [How triangular arbitrage is evaluated](#how-triangular-arbitrage-is-evaluated)
- [Rust engine](#rust-engine)
- [Python backend](#python-backend)
- [Frontend dashboard](#frontend-dashboard)
- [Command and event architecture](#command-and-event-architecture)
- [Control state and health](#control-state-and-health)
- [Risk and execution safety](#risk-and-execution-safety)
- [Project structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Environment configuration](#environment-configuration)
- [Database, Redis, and gRPC setup](#database-redis-and-grpc-setup)
- [Running the platform](#running-the-platform)
- [Paper trading, shadow mode, and micro-live calibration](#paper-trading-shadow-mode-and-micro-live-calibration)
- [API reference](#api-reference)
- [Testing and CI](#testing-and-ci)
- [Failure handling and recovery](#failure-handling-and-recovery)
- [Logging and observability](#logging-and-observability)
- [Security](#security)
- [Deployment](#deployment)
- [Troubleshooting](#troubleshooting)
- [Operational safety checklist](#operational-safety-checklist)
- [Known limitations](#known-limitations)
- [Development progression](#development-progression)
- [Contributing](#contributing)
- [Disclaimer](#disclaimer)

## What the project does

Triangular arbitrage evaluates a closed conversion cycle across three spot markets. A simple route is:

~~~text
USDT -> BTC -> ETH -> USDT
~~~

The scanner starts with an amount of the route's base asset, walks executable order-book depth through all three legs, applies configured trading costs and safety allowances, and compares the final amount with the initial amount.

This repository separates latency-sensitive work from analytics and control:

- **Rust** handles Bybit public market data, local order books, route scanning, risk checks, authenticated execution primitives, three-leg coordination, shadow/canary components, gRPC control, and Redis event publication.
- **Python** handles triangle discovery, financial reference calculations, PostgreSQL persistence, replay simulation, analytics, FastAPI, gRPC client calls, and Redis event consumption.
- **React + TypeScript** provides an operations dashboard that talks only to FastAPI.
- **PostgreSQL** stores opportunities, archived market books, simulation runs, shadow/canary data, and engine events.
- **Redis Streams** carries asynchronous Rust-to-Python engine telemetry.
- **gRPC** carries synchronous Python-to-Rust control commands.

The design deliberately does not embed Rust inside Python.

## Implemented capabilities

The default branch currently includes:

- Bybit V5 public market-data ingestion with reconnect, heartbeat, per-book staleness checks, and normalized order-book events.
- Local depth-aware order books with snapshot/delta continuity checks.
- Spot triangle discovery and shared route configuration.
- Incremental arbitrage scanning of routes affected by changed books.
- Executable bid/ask depth traversal instead of mid-price arithmetic.
- Fee-aware profitability calculations with Python/Rust parity fixtures.
- PostgreSQL opportunity ledger and analytics.
- Historical order-book archiving and delayed paper replay.
- Rust risk engine with stale-data, edge, slippage, liquidity, balance, precision, exposure, loss, service-health, kill-switch, and circuit-breaker checks.
- Authenticated Bybit V5 spot order primitives with conservative retry/reconciliation behavior.
- Three-leg execution coordinator with actual-fill propagation and risk-reducing unwind logic.
- Mainnet shadow mode with delayed-book sampling.
- Micro-live candidate generation and manual reconciliation analytics.
- FastAPI read/control endpoints.
- Rust gRPC control service.
- Redis Streams event publishing and a PostgreSQL-backed Python consumer group.
- React + TypeScript operations dashboard.
- Cross-language Rust/Python/Redis integration coverage in CI.
- Docker Compose for PostgreSQL, Redis, the Rust engine-control service, FastAPI, and the frontend.

Not everything above is one executable process. In particular, the Compose stack runs the **control plane**; the market-data and scanner pipeline is launched separately, and the repository does not currently expose an autonomous full live-execution runner.

## Technology stack

| Layer | Technology | Purpose |
|---|---|---|
| Market/execution engine | Rust 2021 workspace | Market data, books, scanning, risk, execution, coordination |
| Backend/control plane | Python 3.12 + FastAPI | API, orchestration, analytics, persistence |
| RPC | gRPC + Protocol Buffers | Python -> Rust commands |
| Event transport | Redis 7 Streams | Rust -> Python asynchronous events |
| Database | PostgreSQL 16 + SQLAlchemy + Alembic | Durable analytics and event persistence |
| Frontend | React 19 + TypeScript + Vite 6 | Operations dashboard |
| Exchange | Bybit V5 | Public market data, account reads, spot execution primitives |
| Containers | Docker + Docker Compose | Local control-plane stack |
| CI | GitHub Actions | Python, Rust, frontend, parity, PostgreSQL, integration checks |

## Runtime architecture

~~~mermaid
flowchart TD
    UI[React + TypeScript Dashboard] -->|HTTP| API[FastAPI]
    API --> DB[(PostgreSQL)]
    API -->|gRPC commands| CTRL[Rust engine-service]
    CTRL --> STATE[Shared control/risk state files]

    MD[Bybit V5 Public WebSocket] --> RMD[Rust market-data]
    RMD -->|NDJSON order-book events| SCAN[Rust scanner]
    SCAN -->|opportunity events| REDIS[(Redis Streams)]

    COORD[Rust coordinator] --> EXEC[Rust execution]
    EXEC -->|authenticated REST| BYBIT[Bybit V5]
    COORD -->|trade events| REDIS
    EXEC -->|order/balance events| REDIS
    CTRL -->|engine health events| REDIS

    REDIS --> CONSUMER[FastAPI event consumer]
    CONSUMER --> DB
    STATE --> RISK[Rust risk engine]
    RISK --> EXEC
~~~

### Important runtime distinction

The repository currently has two related but separate operational shapes:

1. **Control-plane stack:** PostgreSQL + Redis + Rust engine-control + FastAPI + frontend. Docker Compose runs this.
2. **Market/research pipeline:** Bybit market-data -> Rust scanner -> opportunity ingestion/Redis events -> PostgreSQL. This is launched with host-side Rust/Python commands.

The Rust execution and coordinator crates provide live-capable primitives, but they are libraries, not a complete production daemon that automatically consumes every detected opportunity and sends a three-leg order sequence.

## How triangular arbitrage is evaluated

For a route such as:

~~~text
USDT -> BTC -> ETH -> USDT
~~~

the scanner conceptually evaluates:

~~~text
starting capital
  -> leg 1 executable depth
  -> leg 1 fee
  -> leg 2 executable depth
  -> leg 2 fee
  -> leg 3 executable depth
  -> leg 3 fee
  -> final capital
~~~

Current scanner/profitability configuration also includes expected slippage, latency, rounding-loss, and safety-margin allowances. Default reference values are in [shared/config/profitability.json](shared/config/profitability.json).

The scanner rejects incomplete, stale, or excessively skewed book states and recalculates only routes affected by a changed symbol. Complete route evaluations are recorded as NDJSON and complete opportunities are published as Redis events.

The scanner's default USDT start amount is currently **450 USDT** in [shared/config/scanner.json](shared/config/scanner.json). This is configuration, not a recommended account size.

See:

- [docs/ORDER_BOOK.md](docs/ORDER_BOOK.md)
- [docs/TRIANGLE_DISCOVERY.md](docs/TRIANGLE_DISCOVERY.md)
- [docs/SCANNER.md](docs/SCANNER.md)
- [docs/PROFITABILITY.md](docs/PROFITABILITY.md)

## Rust engine

The Rust workspace lives under **rust/**.

| Crate | Responsibility |
|---|---|
| **market-data** | Bybit public WebSocket connection, metadata, heartbeat, reconnect, stale-feed detection |
| **orderbook** | Local snapshot/delta order books and depth execution |
| **scanner** | Route indexing, book validation, profitability evaluation, opportunity recording |
| **risk** | Mandatory pre-execution checks, approvals, kill switch, circuit breakers |
| **execution** | Authenticated Bybit spot order placement, monitoring, cancellation, fills, balances |
| **coordinator** | Three-leg sequencing, actual-fill propagation, exposure ledger, unwind/reconciliation |
| **shadow** | Mainnet read-only strategy observation with delayed-book samples |
| **micro-canary** | Small read-only candidate generation for manual calibration |
| **event-bus** | Background Redis Streams publisher |
| **engine-service** | gRPC command/control service and engine-control heartbeat |

Latency-sensitive paths are kept in Rust so market-data parsing, book mutation, route scanning, risk validation, and execution state transitions do not depend on Python request handling or garbage collection.

### Bybit market data

The **market-data** binary connects to Bybit V5 public WebSockets and emits normalized JSON events to stdout. Defaults are spot markets, BTCUSDT/ETHUSDT, order-book depth 50, 20-second heartbeats, and a 10-second stale threshold.

### Scanner

The **scan-live** binary reads normalized market-data JSON from stdin, maintains scanner state, records scan results, and publishes **opportunity.detected** for complete route evaluations.

### Execution

The **execution** crate implements authenticated spot REST requests. Important safety behavior includes:

- explicit **marketUnit=baseCoin** for spot market orders;
- unique **orderLinkId** reconciliation before retrying ambiguous creates;
- order acknowledgement treated as acknowledgement, not fill confirmation;
- REST polling until terminal state;
- fill-history confirmation and per-currency fee accounting;
- cancellation followed by cancellation-state confirmation;
- bounded retries;
- Phase 9 risk approval revalidation before submission/retry;
- mainnet private execution rejected unless **ARB_LIVE_TRADING_ENABLED=true**.

### Three-leg coordinator

The coordinator executes sequentially from actual confirmed fills, not planned quantities. If a later leg definitively fails and leaves a positive intermediate exposure, it can attempt a configured risk-reducing unwind. If an exchange-accepted order becomes ambiguous and final state cannot be confirmed, it avoids sending a blind opposite order and escalates to reconciliation/kill-switch behavior.

See [docs/THREE_LEG_COORDINATOR.md](docs/THREE_LEG_COORDINATOR.md).

## Python backend

The Python package lives under **python/** and requires Python 3.12 or newer.

Key areas:

- **strategy/**: triangle discovery and Python profitability reference model.
- **simulator/**: historical order-book archive and delayed paper replay.
- **analytics/**: SQLAlchemy models, ingestion, opportunity/shadow/canary/performance analytics.
- **api/**: FastAPI routes, settings, Rust gRPC client, Redis consumer, and runtime control helpers.
- **alembic/**: PostgreSQL schema migrations.
- **tests/**: unit, PostgreSQL, API, regression, and cross-language integration tests.

FastAPI starts the Redis event consumer during application lifespan. The browser-facing API remains the public control plane; the browser does not talk directly to Rust or Bybit private APIs.

## Frontend dashboard

The frontend is a Vite/React/TypeScript application under **frontend/**.

Implemented dashboard data includes:

- account balance/equity/exposure snapshot;
- today's and seven-day P&L;
- net return;
- detected and rejected opportunities;
- executed/reconciled trades;
- success rate;
- average net edge and execution latency;
- recent opportunities;
- recent execution/canary flow;
- API/database/market-activity health;
- runtime/deployment trading gates;
- risk and circuit-breaker state;
- performance funnel and distributions.

The dashboard polls primary operational data every **5 seconds** and heavier performance analytics every **30 seconds**.

In development, **VITE_API_URL** defaults to **http://localhost:8000**. In the production frontend build it defaults to **/api**, which the included Nginx config proxies to the FastAPI service.

See [docs/DASHBOARD.md](docs/DASHBOARD.md).

## Command and event architecture

Commands and events intentionally use different transports.

### Python -> Rust commands: gRPC

~~~text
FastAPI
  |
  | gRPC
  v
Rust engine-service
~~~

The protobuf contract is [shared/proto/engine_control.proto](shared/proto/engine_control.proto).

Implemented RPCs:

- **StartTrading**
- **StopTrading**
- **UpdateLimits**
- **ReloadStrategy**
- **GetStatus**

The Python client generates a request ID for every call and rejects acknowledgements with a mismatched request ID, command name, rejected status, or missing application timestamp.

### Current command-reliability boundary

Rust persists command request IDs and fingerprints and exposes command status. Python retries transient failures with the same operation ID and checks status after ambiguous deadlines. A timeout remains uncertain until actual state is verified. Reusing an ID with different parameters is rejected.

Stop persists a shared disabled intent before dispatch. Fresh engine status and a matching engine-authored disabled record are required for confirmation. Unconfirmed stops block starts, survive reconnection, and can be safely retried with the same ID. Confirmation disables new normal trades; it does not claim orders cancelled or positions closed.

### Rust -> Python events: Redis Streams

~~~text
Rust scanner / coordinator / execution / engine-service
  |
  | Redis Streams
  v
FastAPI consumer group
  |
  v
PostgreSQL engine_events
~~~

Current event types include:

- **opportunity.detected**
- **trade.attempted**
- **trade.executed**
- **trade.failed**
- **order.executed**
- **order.failed**
- **balance.updated**
- **engine.health**

Each event contains:

~~~text
event_id
event_type
occurred_at_ms
source
schema_version
payload
~~~

The Python consumer:

1. creates the consumer group if needed;
2. reclaims stale pending entries with XAUTOCLAIM;
3. validates the event envelope;
4. commits valid events to PostgreSQL;
5. acknowledges Redis after persistence;
6. sends malformed or permanently database-rejected events to **arb.events.dlq**;
7. uses PostgreSQL conflict-ignore behavior to make replay of stored event IDs idempotent.

### Current event-reliability boundary

The Rust publisher retries Redis connection/write failures after an event has entered its process-local bounded queue. However, publication uses a bounded synchronous channel with non-blocking **try_send**. If that local queue is full or its worker disconnects, the event is logged and dropped.

The current default branch therefore does **not** provide a durable Rust-side outbox or guaranteed delivery for events that cannot enter the local publisher queue. Redis/PostgreSQL replay protection begins only after Redis accepted the event.

Trading safety logic does not rely on Redis event delivery, but audit/analytics completeness can be affected by this limitation.

See [docs/RUST_PYTHON_BOUNDARY.md](docs/RUST_PYTHON_BOUNDARY.md).

## Control state and health

### Runtime control state

The current shared runtime state is:

~~~text
data/control/trading_state.json
~~~

Rust owns authoritative activation. FastAPI persists disabled intent before requesting a stop. Both writers share an OS advisory lock and atomic file replacement. Missing, malformed, oversized, unsupported, future-dated or expired enabled state fails closed. Restart requires explicit activation. An unresolved or corrupt `trading_state.stop.json` latch blocks starts and normal risk approvals.

### GET /health

Health separately reports infrastructure availability, fresh gRPC engine status, Redis consumer lag, persisted worker heartbeat, event outbox, command store, risk readiness, operator-token configuration, market data and trading eligibility. The dashboard uses this same calculation. Every required dependency must pass before `effective_enabled` is true; operational availability alone is insufficient.

Market states are `connected_and_fresh`, `connected_but_stale`, `disconnected`, `resynchronizing`, `degraded` and `unknown`. Readiness requires acknowledged subscriptions and every configured required symbol's initialized, synchronized book within both exchange and receive-time age limits. Opportunity activity is not used as a feed-health proxy. Health thresholds are configurable, and market freshness cannot be looser than the Rust risk configuration.

Canonical response schemas are exported to `shared/schemas/health-response.schema.json` and `trading-control-response.schema.json`. The frontend validates them at runtime and displays unknown or unconfirmed outcomes conservatively. See [CONTROL_API.md](docs/CONTROL_API.md).

## Risk and execution safety

The Rust risk engine is a mandatory approval gate for normal execution.

Default policy in [shared/config/risk.json](shared/config/risk.json):

| Control | Default |
|---|---:|
| Maximum market-data age | 500 ms |
| Minimum net edge | 5 bps |
| Maximum estimated slippage | 10 bps |
| Minimum liquidity ratio | 1.0 |
| Maximum trade size | 500 |
| Maximum total exposure | 1000 |
| Maximum daily loss | 50 |
| Execution failure threshold | 3 |
| Failure window | 300000 ms |
| API health max age | 5000 ms |
| Exchange health max age | 5000 ms |
| Approval TTL | 100 ms |
| Emergency-unwind market-data age | 2000 ms |

These are research defaults, not recommended production limits.

### Manual kill switch

From **rust/**:

~~~bash
cargo run -p risk --bin riskctl -- status
cargo run -p risk --bin riskctl -- kill "operator emergency stop"
cargo run -p risk --bin riskctl -- clear-kill "incident investigated and resolved"
cargo run -p risk --bin riskctl -- reset-breaker "root cause fixed and services healthy"
~~~

Default persistent files:

~~~text
data/risk/KILL_SWITCH
data/risk/risk_state.json
~~~

### Live-trading gates

Normal live execution requires all applicable gates to pass:

~~~text
ARB_LIVE_TRADING_ENABLED=true
        +
runtime trading state enabled
        +
risk approval valid
        +
kill switch clear
        +
circuit breaker clear
        +
fresh market/account/service inputs
~~~

Emergency unwind has a separate constrained risk-reducing approval so stopping new exposure does not automatically trap already-open intermediate spot exposure.

See [docs/RISK_ENGINE.md](docs/RISK_ENGINE.md) and [docs/EXECUTION_ENGINE.md](docs/EXECUTION_ENGINE.md).

## Project structure

~~~text
Arbitrage_bot/
├── .github/workflows/ci.yml
├── docker/
│   ├── Dockerfile.engine
│   ├── Dockerfile.python
│   ├── Dockerfile.frontend
│   ├── docker-compose.yml
│   └── nginx.frontend.conf
├── docs/
├── frontend/
│   ├── src/
│   └── package.json
├── python/
│   ├── alembic/
│   ├── analytics/
│   ├── api/
│   ├── simulator/
│   ├── strategy/
│   ├── tests/
│   └── pyproject.toml
├── rust/
│   ├── coordinator/
│   ├── engine-service/
│   ├── event-bus/
│   ├── execution/
│   ├── market-data/
│   ├── micro-canary/
│   ├── orderbook/
│   ├── risk/
│   ├── scanner/
│   ├── shadow/
│   └── Cargo.toml
├── scripts/
├── shared/
│   ├── config/
│   ├── proto/
│   ├── schemas/
│   └── tests/
├── .env.example
└── README.md
~~~

Runtime **data/** directories are intentionally not part of the committed source tree.

## Prerequisites

For host development:

- Git
- Python **3.12+**
- Rust stable toolchain with Cargo, rustfmt, and Clippy
- Node.js **22** for parity with CI
- npm
- PostgreSQL **16** for parity with CI/Compose
- Redis **7** for parity with Compose
- Docker + Docker Compose, recommended for local infrastructure

A separate system **protoc** installation is not required for the Rust engine-service build: its build script uses **protoc-bin-vendored**. Python protobuf bindings are committed under **python/api/grpc/**.

## Installation

Clone:

~~~bash
git clone https://github.com/SteveParadox/Arbitrage_bot.git
cd Arbitrage_bot
cp .env.example .env
~~~

Generate independent high-entropy operator and internal gRPC secrets:

~~~bash
python -c "import secrets; print('ARB_CONTROL_API_TOKEN='+secrets.token_urlsafe(48)); print('ARB_ENGINE_GRPC_TOKEN='+secrets.token_urlsafe(48))"
~~~

Copy those two generated assignments into **.env**. Do not commit the populated file.

### Python

~~~bash
cd python
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
cd ..
~~~

Windows PowerShell activation:

~~~powershell
cd python
py -3.12 -m venv .venv
.venv\Scripts\Activate.ps1
pip install -e ".[dev]"
cd ..
~~~

### Rust

~~~bash
cargo build --manifest-path rust/Cargo.toml --workspace
~~~

### Frontend

~~~bash
cd frontend
npm install
cd ..
~~~

## Environment configuration

The canonical example is [.env.example](.env.example). The table below documents variables present in that file.

### Application and control

| Variable | Purpose | Default/example |
|---|---|---|
| ARB_ENV | Environment label | development |
| ARB_LOG_LEVEL | Application/Rust log level | INFO |
| ARB_API_HOST | FastAPI bind host | 0.0.0.0 |
| ARB_API_PORT | FastAPI port | 8000 |
| ARB_CORS_ORIGINS | Comma-separated browser origins | http://localhost:5173 |
| ARB_LIVE_TRADING_ENABLED | Master live-execution gate | false |
| ARB_CONTROL_API_TOKEN | Bearer token for mutating HTTP controls; minimum 32 bytes | generate locally |
| ARB_CONTROL_STATE_FILE | Shared runtime control file | data/control/trading_state.json |

### gRPC and Redis

| Variable | Purpose | Default/example |
|---|---|---|
| ARB_ENGINE_GRPC_ADDR | Rust gRPC bind address | 0.0.0.0:50051 |
| ARB_ENGINE_GRPC_TARGET | Python gRPC target | 127.0.0.1:50051 |
| ARB_ENGINE_GRPC_TOKEN | Internal gRPC metadata token; minimum 32 bytes | generate locally |
| ARB_ENGINE_GRPC_TIMEOUT_SECONDS | Python RPC deadline | 2 |
| ARB_REDIS_URL | Redis connection URL | redis://127.0.0.1:6379/0 |
| ARB_EVENT_STREAM | Main engine stream | arb.events |
| ARB_EVENT_CONSUMER_GROUP | Python consumer group | python-api |
| ARB_EVENT_DEAD_LETTER_STREAM | Dead-letter stream | arb.events.dlq |
| ARB_EVENT_BATCH_SIZE | Consumer batch size | 100 |
| ARB_EVENT_RETRY_SECONDS | Consumer reconnect delay | 2 |
| ARB_EVENT_QUEUE_CAPACITY | Rust local publisher queue | 4096 |
| ARB_EVENT_STREAM_MAXLEN | Approximate Redis stream trim target | 1000000 |
| ARB_RUNTIME_LIMITS_FILE | Runtime risk overrides | data/control/risk_limits.json |
| ARB_STRATEGY_RELOAD_FILE | Strategy generation signal | data/control/strategy_reload.json |

### PostgreSQL and analytics

| Variable | Purpose | Default/example |
|---|---|---|
| ARB_DATABASE_URL | SQLAlchemy PostgreSQL URL | postgresql+psycopg://arbitrage:arbitrage@localhost:5432/arbitrage |
| ARB_OPPORTUNITY_MIN_NET_BPS | Opportunity acceptance floor | 0 |
| ARB_OPPORTUNITY_MAX_GAP_MS | Opportunity window gap | 2000 |
| ARB_OPPORTUNITY_BATCH_SIZE | Opportunity ingest batch | 250 |
| ARB_SHADOW_BATCH_SIZE | Shadow ingest batch | 250 |
| ARB_MICRO_LIVE_BATCH_SIZE | Micro-live ingest batch | 100 |
| ARB_PERFORMANCE_SAMPLE_LIMIT | Analytics distribution sample cap | 100000 |

### Bybit market data and execution

| Variable | Purpose | Default/example |
|---|---|---|
| BYBIT_TESTNET | General/testnet market setting | true |
| BYBIT_API_KEY | Private execution API key | empty |
| BYBIT_API_SECRET | Private execution secret | empty |
| BYBIT_MARKET_CATEGORY | Public feed category | spot |
| BYBIT_MARKET_SYMBOLS | Public feed symbols | BTCUSDT,ETHUSDT |
| BYBIT_ORDERBOOK_DEPTH | Order-book depth | 50 |
| BYBIT_SUBSCRIBE_TRADES | Subscribe public trades | true |
| BYBIT_SUBSCRIBE_TICKERS | Subscribe tickers | true |
| BYBIT_HEARTBEAT_SECONDS | WebSocket heartbeat interval | 20 |
| BYBIT_STALE_AFTER_SECONDS | Market-data stale threshold | 10 |
| BYBIT_RECONNECT_MIN_MS | Reconnect minimum backoff | 500 |
| BYBIT_RECONNECT_MAX_SECONDS | Reconnect maximum backoff | 30 |
| BYBIT_EXECUTION_TESTNET | Private execution target | true |
| BYBIT_EXECUTION_RECV_WINDOW_MS | Bybit receive window | 5000 |
| BYBIT_EXECUTION_REQUEST_TIMEOUT_MS | HTTP timeout | 3000 |
| BYBIT_EXECUTION_MAX_RETRIES | Private request retry bound | 2 |
| BYBIT_EXECUTION_RETRY_BASE_MS | Retry base delay | 100 |
| BYBIT_EXECUTION_POLL_INTERVAL_MS | Order polling interval | 50 |
| BYBIT_EXECUTION_ORDER_TIMEOUT_MS | Order terminal-state timeout | 3000 |
| BYBIT_EXECUTION_CANCEL_TIMEOUT_MS | Cancel confirmation timeout | 2000 |
| BYBIT_EXECUTION_FILL_CONFIRM_TIMEOUT_MS | Fill-history confirmation timeout | 2000 |
| BYBIT_EXECUTION_MAX_ORDER_NOTIONAL | Per-order execution cap | 5 |
| BYBIT_EXECUTION_MAX_EXECUTION_PAGES | Execution-history page bound | 20 |
| BYBIT_EXECUTION_ACCOUNT_TYPE | Supported account model | UNIFIED |
| BYBIT_EXECUTION_CANCEL_ON_TIMEOUT | Cancel after monitor timeout | true |

### Strategy, risk, shadow, and canary

| Variable | Purpose | Default/example |
|---|---|---|
| ARB_TRIANGLE_CONFIG | Route configuration | shared/config/triangles.json |
| ARB_SCANNER_CONFIG | Scanner configuration | shared/config/scanner.json |
| ARB_PROFITABILITY_CONFIG | Profitability assumptions | shared/config/profitability.json |
| ARB_RISK_CONFIG | Static risk policy | shared/config/risk.json |
| ARB_COORDINATOR_CONFIG | Three-leg coordinator policy | shared/config/coordinator.json |
| ARB_SHADOW_CONFIG | Shadow experiment settings | shared/config/shadow.json |
| BYBIT_SHADOW_API_KEY | Read-only mainnet shadow key | empty |
| BYBIT_SHADOW_API_SECRET | Read-only mainnet shadow secret | empty |
| BYBIT_SHADOW_RECV_WINDOW_MS | Shadow authenticated receive window | 5000 |
| BYBIT_SHADOW_REQUEST_TIMEOUT_MS | Shadow account request timeout | 3000 |
| ARB_MICRO_CANARY_CONFIG | Micro-canary configuration | shared/config/micro_canary.json |
| ARB_MICRO_CANARY_SHADOW_READY | Operator acknowledgement that shadow threshold is met | false |

Never copy credentials from a real local environment into documentation, issues, screenshots, or commits.

## Database, Redis, and gRPC setup

### PostgreSQL

Start the Compose database:

~~~bash
docker compose -f docker/docker-compose.yml up -d postgres
~~~

Run migrations from the Python environment:

~~~bash
cd python
alembic upgrade head
cd ..
~~~

Or, when using containerized Python after PostgreSQL/Redis are running:

~~~bash
docker compose -f docker/docker-compose.yml run --rm api alembic upgrade head
~~~

Current migrations include opportunity, paper-simulation, shadow, micro-live, and engine-event schemas.

### Redis

Start Redis:

~~~bash
docker compose -f docker/docker-compose.yml up -d redis
~~~

Compose binds host Redis to **127.0.0.1:6379** and enables append-only persistence.

### Protobuf and gRPC

Contract:

~~~text
shared/proto/engine_control.proto
~~~

Rust generation is performed by **rust/engine-service/build.rs** during Cargo build using vendored protoc. Python generated files are checked in under **python/api/grpc/**, so normal installation does not require a manual protobuf generation step.

The internal gRPC service listens on **50051** by default and expects **x-engine-token** metadata derived from **ARB_ENGINE_GRPC_TOKEN**.

## Running the platform

### Option A: Docker control-plane stack

The included Compose file starts:

- PostgreSQL on host port 5432;
- Redis on host loopback port 6379;
- Rust **engine-service** on host loopback port 50051;
- FastAPI on port 8000;
- frontend/Nginx on port 5173.

It does **not** start the market-data, scanner, shadow, micro-canary, coordinator, or autonomous execution pipeline.

Recommended sequence:

~~~bash
docker compose -f docker/docker-compose.yml up -d postgres redis engine-control
docker compose -f docker/docker-compose.yml run --rm api alembic upgrade head
docker compose -f docker/docker-compose.yml up -d api frontend
~~~

Open:

~~~text
Frontend: http://localhost:5173
FastAPI:  http://localhost:8000
Swagger:  http://localhost:8000/docs
ReDoc:    http://localhost:8000/redoc
~~~

Stop:

~~~bash
docker compose -f docker/docker-compose.yml down
~~~

To also delete local PostgreSQL/Redis volumes:

~~~bash
docker compose -f docker/docker-compose.yml down -v
~~~

### Option B: Host development

Start PostgreSQL and Redis:

~~~bash
docker compose -f docker/docker-compose.yml up -d postgres redis
~~~

Export the root **.env** into the shell before starting **engine-service**. The engine-service binary itself does not load the root dotenv file.

POSIX shell:

~~~bash
set -a
. ./.env
set +a
cargo run --manifest-path rust/Cargo.toml -p engine-service
~~~

In another terminal:

~~~bash
cd python
source .venv/bin/activate
alembic upgrade head
uvicorn api.main:app --reload --host 0.0.0.0 --port 8000
~~~

Frontend:

~~~bash
cd frontend
npm run dev
~~~

### Market-data -> scanner -> PostgreSQL research pipeline

For lowest replay distortion, capture the raw market-data stream while scanning:

~~~bash
mkdir -p data/market

cargo run --manifest-path rust/Cargo.toml -p market-data \
  | tee data/market/market_data.ndjson \
  | cargo run --manifest-path rust/Cargo.toml -p scanner --bin scan-live \
  | (cd python && python -m analytics.opportunity_ingest)
~~~

Then archive captured books:

~~~bash
cd python
python -m simulator.book_ingest --file ../data/market/market_data.ndjson
~~~

If Redis is not on the default localhost URL, export **ARB_REDIS_URL** before launching scanner/execution processes.

## Paper trading, shadow mode, and micro-live calibration

### Paper trading

Paper trading is delayed historical replay. It submits **no exchange orders** and does not require private Bybit execution credentials.

Default latency scenarios are 25, 50, 100, 200, and 500 ms. The three simulated legs execute at t0+L, t0+2L, and t0+3L against archived book state.

After capturing/ingesting book history and running migrations:

~~~bash
cd python
python -m simulator.paper_trade --hours 24 --limit 10000
~~~

Explicit latency set:

~~~bash
python -m simulator.paper_trade \
  --hours 24 \
  --limit 10000 \
  --latencies 25,50,100,200,500
~~~

Current simulation limitation: exact exchange quantity rounding/order constraints are not fully replayed as realized exchange behavior.

See [docs/PAPER_TRADING.md](docs/PAPER_TRADING.md).

### Live shadow mode

Shadow mode is designed for mainnet public market observation plus authenticated read-only account context. It does not expose order-create/cancel behavior and refuses to run when the live-trading deployment gate is enabled.

Default experiment settings include 50/100/250 ms delayed samples and a 5,000-observation analysis threshold.

See [docs/LIVE_SHADOW.md](docs/LIVE_SHADOW.md).

### Micro-live calibration

The micro-canary crate generates small mainnet candidates for manual execution/reconciliation. Default cycle notional is 10 USDT and the configured hard safety design caps candidates at small size. Candidate generation itself does not autonomously submit orders.

Manual results can be reconciled through:

~~~text
POST /analytics/micro-live/reconcile/{trade_id}
~~~

See [docs/MICRO_LIVE.md](docs/MICRO_LIVE.md).

## API reference

FastAPI automatically exposes OpenAPI documentation at **/docs** and **/redoc**.

### Primary control-plane surface

| Method | Route | Purpose | Auth |
|---|---|---|---|
| GET | /opportunities | Recent opportunity ledger | none |
| GET | /trades | Recent canary/reconciled trade records | none |
| GET | /performance | Dashboard performance snapshot | none |
| GET | /balances | Latest account snapshot | none |
| GET | /health | Aggregated API/control health | none |
| POST | /trading/start | Request Rust runtime start | Bearer |
| POST | /trading/stop | Request Rust runtime stop | Bearer |
| POST | /engine/limits | Update runtime risk overrides | Bearer |
| POST | /engine/reload-strategy | Validate/reload route generation | Bearer |

The mutating endpoints use **ARB_CONTROL_API_TOKEN**. Missing/short configuration fails closed.

### Additional analytics routes

Implemented route groups include:

~~~text
/analytics/opportunities/*
/analytics/paper-trading/*
/analytics/performance
/analytics/shadow/*
/analytics/micro-live/*
/operations/*
~~~

Use OpenAPI for the complete request/response schema rather than duplicating every model here.

## Testing and CI

### Local checks

Python:

~~~bash
cd python
pytest
ruff check .
~~~

Rust:

~~~bash
cd rust
cargo fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
~~~

Frontend:

~~~bash
cd frontend
npm install
npm run lint
npm run build
~~~

Profitability parity:

~~~bash
python scripts/check_profitability_parity.py
~~~

Repository convenience script:

~~~bash
sh scripts/check.sh
~~~

### PostgreSQL tests

With a PostgreSQL test database configured through **TEST_DATABASE_URL** and **ARB_DATABASE_URL**:

~~~bash
cd python
alembic upgrade head
pytest tests/test_opportunity_postgres.py
~~~

### Cross-language Rust/Python/Redis integration

The dedicated integration test requires:

- Redis;
- a running Rust **engine-service**;
- matching gRPC/Redis environment variables;
- **ARB_RUN_GRPC_INTEGRATION=1**.

Canonical CI configuration is in **.github/workflows/ci.yml**. The test exercises real Python gRPC calls against the Rust server and verifies the corresponding Redis **engine.health** command event.

Run the test after starting those dependencies:

~~~bash
cd python
ARB_RUN_GRPC_INTEGRATION=1 pytest tests/test_engine_grpc_integration.py -q
~~~

### CI jobs

GitHub Actions currently runs:

- Python install, Ruff, and pytest;
- Rust fmt, Clippy, and workspace tests;
- frontend install, lint, and production build;
- Python/Rust profitability parity;
- PostgreSQL opportunity migration/test job;
- Rust <-> Python gRPC + Redis integration job.

CI targets Python 3.12, Node 22, PostgreSQL 16, Redis 7, and the stable Rust toolchain.

## Failure handling and recovery

| Failure | Current behavior |
|---|---|
| Redis unavailable after publisher queueing | Rust publisher retries connection/write in its background thread |
| Critical Rust event handoff unavailable | Durable outbox retains critical events; new execution fails closed if handoff cannot be guaranteed |
| PostgreSQL unavailable during Redis consumption | Consumer session fails; unacknowledged Redis messages remain pending and can be reclaimed |
| Malformed Redis event | Copied to dead-letter stream, then acknowledged |
| Permanent PostgreSQL data rejection | Isolated, copied to dead-letter stream, then acknowledged |
| Rust gRPC unavailable on start/update/reload | FastAPI returns service-unavailable-style failure |
| Rust gRPC unavailable on stop | FastAPI writes local stopped state fail-closed and reports engine state unconfirmed |
| gRPC deadline after mutation may have executed | Retry same operation ID, query command status, and independently verify actual trading state; uncertainty stays disabled |
| Runtime control file missing | Fail-closed stopped state |
| Runtime control file corrupt/unsupported | Rust status unhealthy; normal live approval fails closed |
| Runtime limits invalid | Rust status unhealthy; new risk evaluation fails closed |
| Bybit public book stale | Market-data/scanner reject or reconnect rather than treating stale data as fresh |
| Bybit private create failure with ambiguous outcome | Execution queries unique orderLinkId before eligible retry |
| Order monitoring timeout | Optional cancel request followed by state confirmation |
| Definitive later-leg failure | Coordinator can attempt configured risk-reducing unwind |
| Accepted order with unknown final state | Coordinator avoids blind opposite order and requires reconciliation/kill-switch handling |
| Process restart | Durable event outbox and command store recover; runtime activation requires an explicit start and unresolved stop remains blocking |

## Logging and observability

Rust services use **tracing** / **tracing-subscriber** with JSON output in the engine service and structured fields in other crates. FastAPI/Python uses the project logging setup under **python/api/logging.py**.

Useful identifiers carried by different paths include:

- request ID for gRPC commands;
- event ID and Redis stream ID;
- trade ID;
- route/triangle ID;
- Bybit order ID and orderLinkId;
- execution ID.

Redis events are persisted in PostgreSQL **engine_events**. Analytics APIs expose opportunity, paper, shadow, canary, and performance views.

The project does not currently include a Prometheus/Grafana/OpenTelemetry deployment.

## Security

- Never commit **.env** or real exchange credentials.
- Keep **ARB_CONTROL_API_TOKEN** and **ARB_ENGINE_GRPC_TOKEN** separate.
- Both control tokens should contain at least 32 high-entropy bytes.
- Never expose the operator Bearer token to Vite/browser environment variables or browser storage.
- Keep Redis and gRPC on loopback/private networks unless transport security is added.
- The current gRPC channel is plaintext; the metadata token is not a substitute for TLS across an untrusted network.
- Use least-privilege Bybit keys.
- Disable withdrawal permissions on trading keys.
- Use read-only keys for shadow/account-observation paths.
- Use testnet and paper replay during development.
- Rotate any credential that appears in source control, logs, screenshots, chat, or issue trackers.
- Protect FastAPI mutating endpoints at the network layer in addition to application Bearer auth when deployed.

## Deployment

### Implemented deployment path

The repository currently ships Dockerfiles and **docker/docker-compose.yml**. There are no committed Kubernetes manifests, Terraform modules, Railway configuration, systemd units, or AWS deployment definitions on the default branch.

The Compose deployment is suitable for local/integration control-plane operation. It runs PostgreSQL, Redis, engine-control, FastAPI, and frontend/Nginx.

Production deployment still needs explicit decisions for:

- TLS termination;
- private networking for Redis/gRPC/PostgreSQL;
- secret management;
- persistent volumes/backups;
- process supervision for market-data/scanner and any future execution runner;
- direct dependency health/alerting;
- clock synchronization;
- log/metric aggregation;
- disaster recovery;
- controlled rollout and rollback.

Do not describe the current Compose stack as a hardened production live-trading deployment.

## Troubleshooting

### Rust cannot publish Redis events

Check:

~~~bash
docker compose -f docker/docker-compose.yml ps redis
redis-cli -u redis://127.0.0.1:6379/0 ping
~~~

Confirm **ARB_REDIS_URL** is visible to the Rust process. A Redis outage causes publisher retry; a full local publisher queue can drop events on current main.

### Python cannot reach Rust gRPC

Check that **engine-service** is running on 50051, **ARB_ENGINE_GRPC_TARGET** points to it, and both processes use the same **ARB_ENGINE_GRPC_TOKEN**.

In Docker, Python uses **engine-control:50051**. On the host, the default target is **127.0.0.1:50051**.

### GET /health reports engine gRPC offline

The engine service may be unavailable, the gRPC token may be missing/short/mismatched, or its runtime control/risk configuration may be invalid. Check engine-service JSON logs and the control/risk state files.

### PostgreSQL connection failure

Verify PostgreSQL is healthy and **ARB_DATABASE_URL** points to the correct host. Inside Compose the API uses **postgres** as the hostname; host development uses localhost.

Run migrations:

~~~bash
cd python
alembic upgrade head
~~~

### Market data is stale/offline

Check the Rust market-data process, Bybit public connectivity, configured symbols/category, reconnect logs, and system clock. The FastAPI health route uses recent opportunity activity as a market proxy, so a quiet/no-opportunity period can also affect that status.

### Bybit authentication error

Confirm API key/secret pairing, testnet versus mainnet selection, account type, permissions, and local clock synchronization. Do not loosen receive-window or risk checks merely to hide clock drift.

### Frontend cannot reach FastAPI

Development defaults to **http://localhost:8000**. Verify **VITE_API_URL** and **ARB_CORS_ORIGINS**. The Docker production build uses **/api** through the included Nginx reverse proxy.

### Trading start returns conflict

**POST /trading/start** requires **ARB_LIVE_TRADING_ENABLED=true** in addition to valid Rust control/risk state. It can also be blocked by kill-switch/circuit-breaker conditions. Enabling the environment flag does not bypass the Rust risk gate.

### gRPC command timed out

Treat a mutating timeout as **ambiguous** on current main. Do not assume the command failed before application. Query **GET /health** / engine status and inspect the shared control state before deciding what to do next. Avoid blind command retries.

### Trading state is invalid or unknown

Inspect **data/control/trading_state.json**. Missing state is fail-closed. Corrupt/unsupported state makes Rust status unhealthy. An authenticated stop command can repair the state if Rust gRPC is available; FastAPI also performs a stopped-state fallback when a stop RPC cannot be confirmed.

## Operational safety checklist

Before any live validation, verify all of the following:

- system clock is synchronized;
- the intended Bybit environment/account is selected;
- withdrawal permissions are disabled on trading credentials;
- balances/exposure are current;
- public market data is fresh and sequence-valid;
- scanner books are initialized from fresh snapshots;
- PostgreSQL is healthy and migrations are current;
- Redis is healthy and event backlog is understood;
- Rust gRPC control status is healthy;
- control and risk state files are readable and shared by the intended processes;
- manual kill switch behavior has been tested;
- circuit-breaker reset procedure is understood;
- risk limits match the account and symbols;
- testnet order reconciliation has been exercised;
- paper/shadow behavior has been reviewed against real market conditions;
- ambiguous order/command recovery procedures are understood;
- logs are being retained outside ephemeral containers;
- live enablement is deliberate and reversible.

## Known limitations

The following are current implementation limits on **main**, not hypothetical future concerns:

1. **Durability depends on persistent mounts.** Critical event outbox and command idempotency records must survive container replacement; noncritical telemetry may be dropped under pressure.
2. **Ambiguous exchange outcomes still require investigation.** Command status and verified stops do not prove positions flat or replace exchange reconciliation.
3. **Health requires complete telemetry configuration.** Required symbols must cover deployed routes; missing or stale telemetry blocks eligibility. No live Bybit dependency probe was executed by this audit.
4. **Manual reconciliation is operator reported.** It is authenticated, PostgreSQL-locked and retry-safe, but does not independently validate exchange reports.
5. **Control state is shared-file based.** Strict validation, activation expiry and a stop latch are implemented; a durable shared filesystem remains required. There is no distributed consensus or monotonic engine-epoch protocol.
6. **Compose is a control-plane stack.** It does not run the market-data/scanner/shadow/canary/coordinator pipeline end to end.
7. **No autonomous production live runner is packaged.** Execution and coordinator logic exist as Rust components, but current repository wiring does not turn every scanner opportunity into an unattended live route execution daemon.
8. **Paper replay is not exchange-perfect.** Exact quantity rounding/order constraints are not fully modeled as realized exchange behavior, and very large histories still need stronger memory/streaming bounds.
9. **Bybit is the only exchange integration.**
10. **Private execution monitoring is REST-polling based.** Private WebSocket execution/order streams are not currently the correctness path.
11. **No production metrics stack is included.** Logging and database analytics exist, but Prometheus/Grafana/OpenTelemetry deployment is absent.
12. **Plaintext internal gRPC.** Private networking is required unless TLS/mTLS is added.

These limitations are reasons for controlled testnet/shadow validation before live deployment, not invitations to disable safety checks.

## Development progression

The implemented project has progressed through:

~~~text
Foundation
  -> Bybit market data
  -> local order books
  -> triangle discovery/scanning
  -> fee/profitability parity
  -> opportunity persistence
  -> paper replay
  -> risk engine
  -> Bybit execution primitives
  -> three-leg coordinator
  -> live shadow
  -> micro-live calibration
  -> operations dashboard
  -> FastAPI control plane
  -> Rust/Python gRPC + Redis boundary
  -> performance analytics
~~~

The next production-hardening work should focus on the verified limitations above: deployed lifecycle verification, current exchange instrument constraints, shared-capital reservation, packaging the runtime topology, and production observability/security.

## Contributing

1. Create a focused branch from the current integration branch.
2. Keep changes scoped and preserve fail-closed trading behavior.
3. Add or update regression tests for behavioral changes.
4. Run the relevant Python, Rust, frontend, parity, and integration checks.
5. Do not commit credentials, generated runtime state, or captured private account data.
6. Open a pull request describing behavior, safety impact, tests, and any migration/configuration changes.

Prefer small reviewable commits and explicit safety semantics over “works on my machine” optimism.

## Disclaimer

This project is intended for research, development, simulation, and controlled testing. Cryptocurrency trading involves substantial financial risk. Software correctness, historical data, simulated results, shadow observations, and previous trading performance do not guarantee future profitability. Operators are responsible for exchange permissions, risk limits, infrastructure, monitoring, and any decision to enable real-money execution.
