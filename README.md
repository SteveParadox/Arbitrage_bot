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

Phase 12 live shadow measurement is implemented above the scanner/risk layers. It uses mainnet
books plus GET-only real-account context while keeping order submission structurally absent from
the shadow crate. **Live trading remains disabled by default.**
