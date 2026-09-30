# Phase 9: Rust Risk Engine

Every future execution must pass the Rust risk gate before the execution crate can prepare it.

The execution crate accepts a `RiskApproval` token. That token has private fields and can only be
created by `RiskEngine::evaluate` after all configured checks pass.

There is still no live order-entry implementation in this phase.

## Pre-trade checks

Every `TradeIntent` is validated for:

- market-data freshness;
- minimum expected net edge;
- maximum estimated slippage;
- available liquidity and liquidity ratio;
- account balance;
- maximum trade size;
- symbol quantity/price precision;
- projected peak route exposure plus current account exposure;
- maximum daily realized loss;
- API health;
- exchange health.

The intent contains the oldest market timestamp used by the opportunity. This prevents a recent
update on one leg from hiding stale data on another leg.

Financial values and symbol precision use `rust_decimal::Decimal` in the risk layer.

## Default policy

`shared/config/risk.json` currently contains conservative research defaults:

```text
maximum market-data age      500 ms
minimum net edge               5 bps
maximum estimated slippage    10 bps
minimum liquidity ratio      1.0
maximum trade size            500
maximum total exposure       1000
maximum daily loss             50
execution failure threshold     3
failure window             300000 ms (5 min)
API health max age           5000 ms
exchange health max age      5000 ms
approval lifetime             100 ms
```

Trade size, projected peak exposure, current exposure, balance, and daily-loss amounts must be
expressed in the same account/notional currency by the execution orchestrator. For a three-leg
route, `projected_peak_exposure` should represent the largest marked notional held during any leg,
not merely the starting capital.

These values are configuration, not exchange constants. They must be calibrated to the actual
account before any live-execution phase.

## Candidate rejection versus circuit breaker

Normal candidate-specific failures reject that trade only:

- net edge below minimum;
- slippage above maximum;
- insufficient liquidity;
- insufficient account balance;
- trade too large;
- precision violation;
- exposure too high.

System-health failures latch a circuit breaker:

- market data older than 500 ms;
- market timestamp ahead of local time;
- maximum daily loss reached;
- API unhealthy/stale;
- exchange unhealthy/stale;
- configured execution-failure threshold reached.

A latched breaker does not clear itself merely because the next market update looks healthy.
An operator must inspect the cause and reset it explicitly.

## Three failures in five minutes

Execution code must report failed execution attempts:

```rust
risk_engine.record_execution_failure(now_ms, "leg 2 rejected by exchange")?;
```

With the default policy, the third failure inside five minutes persists:

```text
breaker = execution_failures
```

and all subsequent risk evaluations are rejected.

Successful executions may call:

```rust
risk_engine.record_execution_success(now_ms)?;
```

A success prunes old failure timestamps but does not clear an already-latched breaker.

## Manual kill switch

The kill switch is file-backed so it survives process restarts and can be engaged independently of
the trading process.

From the `rust` directory:

```bash
cargo run -p risk --bin riskctl -- status

cargo run -p risk --bin riskctl --   kill "operator emergency stop"

cargo run -p risk --bin riskctl --   clear-kill "incident investigated and resolved"

cargo run -p risk --bin riskctl --   reset-breaker "root cause fixed and services healthy"
```

The default sentinel is:

```text
data/risk/KILL_SWITCH
```

(relative to the repository after configuration-path resolution).

Creating the sentinel blocks approval. Clearing it requires a non-empty operator note through the
CLI.

## Persistent breaker state

Circuit-breaker state and recent execution-failure timestamps are stored in:

```text
data/risk/risk_state.json
```

The risk engine reloads this file before evaluation and immediately before an approval crosses the
execution boundary. This allows an external `riskctl` reset to take effect without restarting the
process.

## Approval expiry and second gate

Approval is deliberately short lived.

The default approval TTL is 100 ms.

The execution crate performs a second check immediately before preparing execution:

```text
Risk evaluation
      |
      v
RiskApproval
      |
      | <= 100 ms
      v
Execution prepare
      |
      +-- kill switch clear?
      +-- breaker clear?
      +-- same trade id?
      +-- approval not expired?
```

This catches a kill switch or breaker that becomes active after the initial risk calculation.

## Symbol precision

Every proposed leg contains:

- quantity;
- optional limit price;
- `qty_step`;
- `min_order_qty`;
- `tick_size`.

The risk engine rejects:

- quantity below minimum;
- quantity not an exact multiple of quantity step;
- non-positive quantities;
- invalid symbol rules;
- limit price not aligned to tick size.

Market orders may omit a limit price, but quantity precision is still mandatory.

## Service health

The caller supplies explicit `ServiceHealth` records for:

- private/API connectivity;
- exchange health.

Each contains:

```text
healthy
last_ok_ms
detail
```

Both the boolean health state and heartbeat age must pass.

This keeps the risk engine independent from any one HTTP health endpoint while still making
execution fail closed when connectivity information is stale.

## Execution integration

The only current execution preparation API is:

```rust
prepare_execution(
    &mut risk_engine,
    trade_id,
    approval,
    now_ms,
    ExecutionMode::Live,
    live_enabled,
)
```

It rejects:

- missing/invalid risk approval;
- expired approval;
- approval for another trade;
- newly engaged kill switch;
- newly active circuit breaker;
- live mode when live trading is disabled.

`ARB_LIVE_TRADING_ENABLED=false` remains the repository default.

## Important limitation

Phase 9 supplies the gate and state machinery, but there is still no private Bybit account/execution
connector in the repository.

A future execution orchestrator must populate `RiskContext` from real authenticated account,
balance, exposure, daily-P&L, and health sources immediately before evaluation. Missing or stale
health/account information must not be substituted with optimistic defaults.


## Phase 11 emergency unwind authorization

Phase 11 adds a second approval class:

```text
RiskApprovalKind::EmergencyUnwind
```

It exists so an operator kill switch or a latched ordinary circuit breaker does not trap an
already-open intermediate spot exposure.

Emergency unwind approval is strictly constrained:

- the exposure asset must differ from the route base asset;
- known exposure notional must be positive;
- requested unwind notional must not exceed known exposure;
- API health must be current and healthy;
- exchange health must be current and healthy;
- market data must not be newer than local time;
- market data must be no older than `emergency_max_market_data_age_ms`.

The default emergency freshness ceiling is 2000 ms, compared with 500 ms for opening/continuing
normal risk.

Emergency approvals bypass the manual kill switch and ordinary latched breakers only for the
short-lived risk-reducing order token. They do not re-enable ordinary trading.
