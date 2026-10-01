# Arbitrage Bot

A multi-language monorepo for researching, simulating, monitoring, and eventually executing arbitrage strategies.

Audit: [Phase 1–8 technical audit, 2026-09-30](docs/AUDIT_REPORT_2026-09-30.md).
Accepted observations are research candidates, not proof of exchange-executable profit.

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
a failed simulation, and stale historical books are rejected. History is loaded in opportunity
chunks with an event budget; opportunities and results are still retained for the entire run.
This is not a strict memory bound. Exchange quantity rounding and order constraints remain unmodeled.

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

### Phase 10: Bybit execution engine

Rust now contains authenticated Bybit V5 spot execution support behind the Phase 9 risk gate.

The private client implements:

```text
HMAC authentication
order creation
order cancellation
order monitoring
fill confirmation
fee aggregation
balance synchronization
retry/backoff handling
```

Order creation acknowledgements are treated only as acknowledgements. The engine polls order state
and execution history until it can distinguish requested quantity, cumulative filled quantity,
remaining quantity, weighted average fill price, per-currency fees, and terminal status.

Phase 10 is testnet-first:

```env
BYBIT_EXECUTION_TESTNET=true
BYBIT_EXECUTION_MAX_ORDER_NOTIONAL=5
ARB_LIVE_TRADING_ENABLED=false
```

Market orders explicitly use `marketUnit=baseCoin` so requested and filled quantities stay in the
same unit. Ambiguous create failures check the unique `orderLinkId` before any retry to reduce the
risk of duplicate orders.

See `docs/EXECUTION_ENGINE.md`.

### Phase 11: Three-leg execution coordinator

Rust now coordinates the complete triangular sequence:

```text
risk-approved Leg 1
      ↓
confirmed actual fill
      ↓
recalculate Leg 2 from actual net output
      ↓
confirmed actual fill
      ↓
risk-reducing Leg 3 back to base asset
      ↓
realized P&L
```

The coordinator maintains a route-local holdings ledger and applies each confirmed fill plus
fee currency before sizing the next leg.

If a terminal partial fill or definitive later-leg rejection leaves intermediate assets, the
coordinator unwinds those positive exposures back to the route base asset. For the standard
three-asset route it can unwind:

```text
asset 1 intermediate -> reverse Leg 1 -> base
asset 2 intermediate -> Leg 3 -> base
```

If the exchange has accepted an order but its final state cannot be confirmed, the coordinator
does not place a blind opposite order. It engages the manual kill switch and requires
reconciliation first.

Emergency unwind is explicitly risk-reducing and may proceed even while the ordinary kill switch
or another circuit breaker blocks new exposure, but it still requires sufficiently recent market
data plus healthy API/exchange status.

See `docs/THREE_LEG_COORDINATOR.md`.

### Phase 12: Live shadow mode

Rust now runs the strategy against **mainnet market data and a real account without exposing any
order endpoint**.

The shadow process emits:

```text
DETECTED
APPROVED
WOULD EXECUTE
EXPECTED PROFIT
```

for gross-profitable route detections, then evaluates the same route against books received at or
before:

```text
+50 ms
+100 ms
+250 ms
```

The default experiment requires **5,000 opportunities with all configured latency samples**
before marking the run ready for analysis.

Shadow mode uses a dedicated GET-only authenticated account client for balance/equity context and
does not depend on the Phase 10 execution crate. It also refuses to start if
`ARB_LIVE_TRADING_ENABLED=true`.

Delayed samples retain fees, rounding, safety margin, and the additional slippage assumption, but
remove the synthetic Phase 6 latency buffer because the actual delayed book now represents that
price movement. Historical book selection is based on local receipt time and never uses a book
received after the latency target.

PostgreSQL stores runs, decisions, and latency samples. Analytics are available at:

```text
GET /analytics/shadow/runs
GET /analytics/shadow/summary
GET /analytics/shadow/latencies
GET /analytics/shadow/routes
```

See `docs/LIVE_SHADOW.md`.

### Phase 13: Micro-live canary telemetry

Phase 13 adds a mainnet canary layer for calibrating predicted versus realized performance at
small sizes without adding an autonomous order-submission path.

The Rust canary uses mainnet order books plus the account's authenticated read-only spot taker fee
rates. Candidate size defaults to 10 USDT and is hard-limited to 25 USDT by the canary config
validator.

Each candidate records:

```text
expected P&L
expected fees
expected slippage
expected net edge
detection leg prices
real account balance/exposure snapshot
```

After a small trade is executed manually, reconcile it through:

```text
POST /analytics/micro-live/reconcile/{trade_id}
```

The server computes the primary calibration metric itself:

```text
prediction_error = realized_pnl - expected_pnl
```

PostgreSQL stores both the candidate and reconciled result. Summary analytics expose mean
prediction error, mean absolute prediction error, fee error, slippage error, and execution time.

See `docs/MICRO_LIVE.md`.

### Phase 14: TypeScript operations dashboard

The React + TypeScript frontend is an operational dashboard backed by FastAPI.

The main screen includes:

```text
account balance
today / weekly P&L
net return
detected opportunities
executed trades
rejected opportunities
success rate
average net edge
average execution latency
```

It also exposes recent opportunity rows, the latest micro-live execution flow, API/market-stream
health, trading state, risk state, circuit-breaker state, and the manual kill switch.

The dashboard polls every five seconds and uses `VITE_API_URL` for the backend address.

See `docs/DASHBOARD.md`.

### Phase 15: Python control API

FastAPI is now the public control plane between React, PostgreSQL analytics, and the Rust engine.

The stable Phase 15 surface is:

```text
GET  /opportunities
GET  /trades
GET  /performance
GET  /balances
GET  /health
POST /trading/start
POST /trading/stop
```

The Phase 14 React client now consumes the Phase 15 read routes rather than the older aggregate
operations endpoints.

Only the two mutating trading-control endpoints require a Bearer token. The runtime trading state
is written atomically to `data/control/trading_state.json` and is a second gate in the Rust risk
engine. Normal live approvals require both the static deployment flag and the runtime FastAPI
control state. Shadow preview remains unaffected, and emergency unwind remains available.

See `docs/CONTROL_API.md`.

### Phase 16: Rust ↔ Python service boundary

Python and Rust now communicate through explicit service contracts rather than process embedding.

~~~text
Python -> Rust: gRPC commands
Rust -> Python: Redis Streams events
~~~

gRPC supports trading start/stop, runtime risk-limit updates, strategy reloads, and engine status.
Redis Streams carries opportunity, execution, balance, failure, and engine-health events. FastAPI
persists consumed events to PostgreSQL before acknowledging them.

See docs/RUST_PYTHON_BOUNDARY.md.

## Security

Bybit credentials and PostgreSQL production credentials must come from environment variables or a
secrets manager. Live trading remains disabled:

```env
ARB_LIVE_TRADING_ENABLED=false
```

Phase 12 shadow mode can read the real account and mainnet public market data, but its Rust crate
contains no order-create or cancel client. Use a Bybit API key with no trading permissions for
shadow runs.

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

Phase 16 is implemented as an explicit Rust/Python service boundary. FastAPI sends control
commands to Rust over gRPC, while Rust publishes operational telemetry through Redis Streams for
durable Python consumption and PostgreSQL persistence.

Phase 15 remains the public control plane for the React dashboard. Normal live execution still
requires the deployment gate, the runtime trading gate, and the Phase 9 risk gate. Redis delivery
is telemetry-only and is not allowed to weaken trading safety when the stream is unavailable.

See `docs/RUST_PYTHON_BOUNDARY.md` for the command, event, failure, and deployment semantics.
