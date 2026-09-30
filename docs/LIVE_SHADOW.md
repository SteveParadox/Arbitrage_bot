# Phase 12: Live Shadow Mode

Phase 12 answers a specific question before any mainnet order is permitted:

> Does a triangular opportunity that looks profitable at detection still exist after realistic
> network and processing delay?

The shadow subsystem runs against Bybit mainnet public order books and authenticated real-account
balance data, but it cannot create or cancel an order.

## Hard safety boundary

The Rust `shadow` crate does not depend on the Phase 10 `execution` crate.

Its authenticated client implements only:

```text
GET /v5/account/wallet-balance
```

There is no generic authenticated POST method and no create-order or cancel-order function.

The binary also refuses to start when:

```env
ARB_LIVE_TRADING_ENABLED=true
```

For additional exchange-side protection, use a Bybit API key that has no trading permissions.
Shadow credentials are separate from the Phase 10 execution credentials so a testnet execution key
cannot be confused with the mainnet read-only account key.

## Mainnet inputs

Shadow mode forces the public market-data connector to:

```text
mainnet
spot
all symbols required by the loaded triangle routes
```

The committed `shared/config/triangles.json` may be empty until route discovery is run against
current Bybit mainnet listings.

Generate USDT-starting routes first:

```bash
cd python
BYBIT_TESTNET=false python -m strategy.triangle_discovery --start-assets USDT
```

PowerShell:

```powershell
cd python
$env:BYBIT_TESTNET = "false"
python -m strategy.triangle_discovery --start-assets USDT
```

Shadow mode rejects a triangle file marked as testnet and rejects an empty route set.

## What counts as DETECTED

A shadow observation is created when the Rust scanner has:

- all three books;
- sufficient visible depth for the configured starting capital;
- a complete three-leg conversion path;
- positive gross profit before Phase 6 costs.

This deliberately keeps `DETECTED` separate from `APPROVED`.

## APPROVED

Phase 12 calls the real Phase 9 risk logic through:

```rust
RiskEngine::preview(...)
```

Preview performs the same candidate checks and respects an already-active manual kill switch or
circuit breaker, but it does not latch a new breaker and its approval token is discarded.

The shadow process therefore cannot turn an approval into an execution.

Inputs include:

```text
market freshness
expected net edge
visible-depth slippage
visible liquidity
real base-asset account balance
trade-size limit
symbol precision
real non-base account exposure in USD
daily-loss proxy
API health
exchange health
```

## Real-account context

The GET-only wallet sync captures:

```text
base-asset available balance
total account equity in USD
total wallet value in USD
non-base asset exposure in USD
```

For the configured USDT base asset:

```text
available =
max(walletBalance - locked - spotBorrow, 0)
```

Non-base exposure is the sum of absolute USD values of non-USDT assets returned by the account.

### Daily-loss limitation

The wallet endpoint does not provide a clean bot-only daily realized P&L value.

Shadow mode therefore uses:

```text
current total equity
-
equity at shadow-session start
```

as a conservative session P&L proxy for the Phase 9 daily-loss check.

Every observation stores this separately as:

```text
session_pnl_proxy_usd
```

Do not interpret it as audited realized P&L. It can include unrealized account movement or activity
unrelated to this bot.

## WOULD EXECUTE

`WOULD EXECUTE=true` means the detected route passed the Phase 9 preview using the current real
account context.

It still causes no order request.

The initial JSON event therefore contains fields equivalent to:

```json
{
  "detected": true,
  "approved": true,
  "would_execute": true,
  "expected_profit": "0.81"
}
```

## Latency experiment

Defaults:

```text
50 ms
100 ms
250 ms
```

For every detected gross-profitable opportunity, Phase 12 schedules all three samples.

The clock is based on **local market-data receipt time**, not merely exchange timestamp.

For a detection at local time `T`:

```text
T + 50 ms
T + 100 ms
T + 250 ms
```

the system reconstructs the route using the newest book for each required symbol that was actually
received at or before the target.

## No lookahead bias

Suppose a 50 ms sample target is:

```text
10:00:00.050
```

and a new ETHUSDT book arrives locally at:

```text
10:00:00.053
```

Even if the scheduler wakes at 55 ms, the 53 ms book is **not** eligible for the 50 ms result.

Short per-symbol book histories are keyed by local receipt time specifically to enforce this rule.

The latency record also stores scheduler lag so operating-system scheduling delays remain visible.

## Stale book protection

A historical book selected for a latency target must still satisfy the Phase 9 market-data
freshness ceiling.

A 250 ms target therefore does not silently reuse a two-second-old cross book and call the route
executable.

Such samples are stored as invalid with a failure reason.

## Detection prices versus delayed prices

Every observation stores the three depth-weighted average execution prices seen at detection.

Every delayed sample stores:

```text
three delayed average execution prices
route final amount
route-final drift in bps
per-leg adverse price drift in bps
net profit
net edge
profit drift from detection
```

Per-leg adverse drift uses direction-aware signs:

```text
BUY:  higher delayed price = positive adverse drift
SELL: lower delayed price  = positive adverse drift
```

This makes it possible to identify which leg destroys the edge.

## Latency-buffer treatment

The ordinary detection-time expected profit uses the full Phase 6 model, including the configured
latency allowance.

For delayed samples, the system uses the actual later books, so it sets:

```text
latency_buffer_bps = 0
```

for the delayed calculation.

Fees, additional expected slippage, rounding allowance, and safety margin remain.

This avoids charging the synthetic latency penalty a second time after latency has already been
represented by real market movement.

For clean drift comparison, Phase 12 also stores a latency-neutral detection profit calculated from
the detection books with the same zero-latency-buffer model.

## Several-thousand-opportunity requirement

Default configuration:

```json
{
  "latency_ms": [50, 100, 250],
  "minimum_observations": 5000,
  "max_pending_observations": 50000
}
```

A run is marked:

```text
ready_for_analysis = true
```

only after at least 5,000 observations have completed all configured delayed samples. The
configuration validator will not allow fewer than 3,000 completed observations.

A detection that never received its latency samples does not satisfy the threshold.

## Non-blocking measurement architecture

The main shadow loop handles market updates and the 5 ms sampling clock.

Potentially slow work is isolated:

- account HTTP synchronization runs in its own task;
- NDJSON output runs in its own writer task;
- the public market connector already runs independently.

This prevents a slow wallet response or PostgreSQL consumer from masquerading as market latency.

The output event still records `scheduler_lag_ms` so host overload can be detected.

## PostgreSQL schema

Phase 12 adds:

```text
shadow_runs
shadow_observations
shadow_latency_samples
```

Apply migrations:

```bash
cd python
alembic upgrade head
```

The migration chain is now:

```text
0001 opportunity ledger
0002 paper simulation
0003 remaining opportunity lifetime
0004 live shadow
```

## Running the shadow experiment

Required environment:

```env
ARB_LIVE_TRADING_ENABLED=false
BYBIT_SHADOW_API_KEY=<mainnet read-only key>
BYBIT_SHADOW_API_SECRET=<mainnet read-only secret>
BYBIT_ORDERBOOK_DEPTH=50
```

From a Bash-like shell:

```bash
cd rust
cargo run -p shadow --bin shadow-live \
  | (cd ../python && python -m analytics.shadow_ingest)
```

For Windows/PowerShell, capturing first is often easier:

```powershell
New-Item -ItemType Directory -Force ..\data\shadow | Out-Null
cargo run -p shadow --bin shadow-live |
  Tee-Object -FilePath ..\data\shadow\live.ndjson
```

Then ingest from the repository's `python` directory:

```powershell
cd ..\python
python -m analytics.shadow_ingest --file ..\data\shadow\live.ndjson
```

The Rust process writes structured logs to stderr and shadow events to stdout, so redirecting stdout
does not mix normal logs into NDJSON.

Shadow NDJSON and the PostgreSQL shadow tables contain real-account balance/exposure context and
should be treated as private operational data. The normal analytics summary API intentionally
redacts the numeric wallet snapshot and returns only account-health status/timing.

## Analytics API

After ingestion:

```text
GET /analytics/shadow/runs
GET /analytics/shadow/summary
GET /analytics/shadow/latencies
GET /analytics/shadow/routes
```

The latency breakdown includes, for each delay:

```text
sample count
valid sample rate
profitable-after-latency rate
still-meets-min-edge rate
average net profit
average net edge
average profit drift
average route-final price drift in bps
average scheduler lag
```

## Interpretation

The useful funnel is now:

```text
gross opportunity detected
        ↓
Phase 9 preview approved
        ↓
WOULD EXECUTE
        ↓
still profitable after 50 ms?
        ↓
still profitable after 100 ms?
        ↓
still profitable after 250 ms?
```

If thousands of detections collapse before 50 or 100 ms, the strategy has a latency problem even
if the original scanner edge looks attractive.

If the edge persists but risk approval rejects most cases, the limiting factor is different:
balance, exposure, precision, liquidity, health, or configured risk thresholds.

## What Phase 12 does not prove

Shadow mode does not measure:

- actual queue position;
- actual market-order fill probability beyond visible depth;
- private-order gateway latency;
- exchange matching latency for this account;
- three-leg order submission overhead;
- emergency-unwind performance under real fills.

Those questions require controlled testnet/small-size execution evidence.

Phase 12 exists to reject weak strategies before paying real money to learn those lessons.


## Spot WebSocket subscription batching

Bybit limits a single Spot subscription request to at most 10 args. The shared market-data
connector therefore splits large Spot topic sets into multiple subscription requests on the same
connection. This is required when shadow mode loads many triangle symbols at once.


## Order-book-only shadow feed

The normal market-data service still supports order books, public trades, and tickers. Shadow mode
overrides that configuration and subscribes only to order-book topics for its required symbols.
This reduces bandwidth and event-loop noise while measuring the 50/100/250 ms book movement.
