# Phase 13: Micro-Live Canary Telemetry

Phase 13 is the calibration layer between live shadow research and larger-capital deployment.

The canary is intentionally small:

```text
default candidate size: 10 USDT
absolute candidate cap: 25 USDT
```

The Rust `micro-canary` crate does not depend on the private execution crate and refuses to start
while `ARB_LIVE_TRADING_ENABLED=true`.

## Purpose

For every real-market candidate, compare the model's prediction with the result of a deliberately
small manually executed cycle.

The primary metric is:

```text
prediction_error = realized_pnl - expected_pnl
```

A well-calibrated model should make the distribution of that error narrow and centered near zero.

## Candidate generation

The canary consumes:

- Bybit mainnet spot order books;
- current structural triangle routes;
- the Phase 6 cost model;
- authenticated read-only wallet information;
- authenticated read-only spot fee rates.

Before emitting a candidate it:

1. limits scanner capital to the configured canary amount;
2. verifies complete depth-aware books;
3. recalculates profitability using the account's actual taker fee rate for each leg;
4. requires positive expected net profit;
5. refreshes the real account balance;
6. suppresses the candidate if available USDT is below the configured amount.

It then emits a `micro_live_candidate` NDJSON record.

No order-create or cancel endpoint exists in this crate.

## Configuration

`shared/config/micro_canary.json`:

```json
{
  "version": 1,
  "base_asset": "USDT",
  "cycle_notional": "10",
  "max_candidates_per_session": 20,
  "cooldown_ms": 5000,
  "require_shadow_ready_ack": true
}
```

The Rust validator rejects any `cycle_notional` greater than 25 USDT.

When `require_shadow_ready_ack=true`, the operator must explicitly set:

```env
ARB_MICRO_CANARY_SHADOW_READY=true
```

after reviewing Phase 12 results.

The process also refuses to run if:

```env
ARB_LIVE_TRADING_ENABLED=true
```

## Running candidate capture

Use the same read-only mainnet credentials as Phase 12:

```env
BYBIT_SHADOW_API_KEY=<read-only key>
BYBIT_SHADOW_API_SECRET=<read-only secret>
ARB_LIVE_TRADING_ENABLED=false
ARB_MICRO_CANARY_SHADOW_READY=true
```

Generate current mainnet triangles first, then:

```bash
cd rust
cargo run -p micro-canary --bin micro-canary \
  | tee ../data/micro_live/candidates.ndjson
```

Persist them:

```bash
cd ../python
alembic upgrade head
python -m analytics.micro_live_ingest \
  --file ../data/micro_live/candidates.ndjson
```

## Manual reconciliation

After a candidate is manually executed, submit the observed result:

```json
{
  "realized_pnl": "0.031",
  "actual_fee_amount_base": "0.029",
  "actual_fees_by_currency": {
    "USDT": "0.029"
  },
  "actual_slippage": "0.008",
  "actual_slippage_bps": "8",
  "execution_time_ms": 214,
  "execution_status": "completed",
  "notes": "three legs completed"
}
```

to:

```text
POST /analytics/micro-live/reconcile/{trade_id}
```

The API does not accept a caller-supplied prediction error. It calculates:

```text
realized_pnl - expected_pnl
```

from the stored prediction.

Use `Authorization: Bearer <ARB_CONTROL_API_TOKEN>` from a privileged operator or authorized job; query-string credentials are ignored. Requests should carry a stable ASCII `request_id` for retries.

PostgreSQL is required. Cycle and session rows are locked in one transaction with bounded lock/statement timeouts. Identical normalized financial payloads return the stored result with `duplicate=true` without changing counters or P&L. Different payloads, legacy already-reconciled records, or a request ID reused for another trade return HTTP 409. Transient database failures roll back and return HTTP 503; retry the same ID and payload. Migration `0007_reconciliation_idempotency` adds the digest and unique request identity without altering prior financial data.

Amounts must be finite and fit database precision; fee maps, notes and identities are bounded. Responses explicitly report `source=operator_reported` and `exchange_confirmed=false`. This manual calibration endpoint does not query exchange orders/fills, cancel orders, close positions, or activate trading. Existing bearer authorization is a privileged capability; there is no separate JWT expiry/role service introduced here.

## PostgreSQL

Migration `0005_micro_live` adds:

```text
micro_live_runs
micro_live_cycles
```

Candidate records preserve the expected side of the experiment. Reconciliation fills in:

```text
realized P&L
actual fee amount
actual fees by currency
actual slippage
actual slippage bps
execution time
execution status
notes
prediction error
```

## Analytics

Endpoints:

```text
GET  /analytics/micro-live/summary
GET  /analytics/micro-live/cycles
POST /analytics/micro-live/reconcile/{trade_id}
```

The summary reports:

```text
candidate count
reconciled count
average prediction error
mean absolute prediction error
average fee error
average slippage error
average execution time
```

## Interpretation

For a prediction:

```text
expected P&L = +0.050 USDT
realized P&L = +0.031 USDT
```

the stored error is:

```text
0.031 - 0.050 = -0.019 USDT
```

Negative error means reality underperformed the model. Positive error means reality exceeded it.

The useful progression is not simply "was the trade profitable?" It is whether the prediction error
stays small and stable across enough independent canary cycles.

Large persistent negative errors indicate that one or more modeled costs are understated, usually
fees, latency, slippage, rounding, or depth assumptions.
