# Phase 15: Python Control API

Phase 15 makes FastAPI the public control plane between the React dashboard, PostgreSQL analytics,
and the Rust trading engine.

```text
React / TypeScript
        │
        ▼
     FastAPI
        │
 ┌──────┴────────┐
 │               │
PostgreSQL      Rust
Analytics       Engine
                  │
                  ▼
                Bybit
```

The browser does not call the Rust engine or Bybit private endpoints directly.

## API surface

Read endpoints:

```text
GET /opportunities
GET /trades
GET /performance
GET /balances
GET /health
```

Control endpoints:

```text
POST /trading/start
POST /trading/stop
```

The existing `/analytics/*` and `/operations/*` routes remain available for compatibility and
deeper analysis.

## Authentication

Only the two mutating control endpoints require authentication.

Configure:

```env
ARB_CONTROL_API_TOKEN=<high-entropy secret of at least 32 bytes>
```

Requests use:

```text
Authorization: Bearer <token>
```

The API compares the token with a constant-time comparison. If the token is absent or shorter than
32 bytes, control endpoints fail closed with HTTP 503. Invalid or missing credentials return
HTTP 401.

The token is never returned by `/health`; health only reports whether control authentication is
configured.

## Two trading gates

Starting runtime trading requires two independent gates:

```text
ARB_LIVE_TRADING_ENABLED=true
          AND
runtime control state enabled
```

The static environment flag is the deployment/operator permission. The runtime state is the
FastAPI-controlled switch.

`POST /trading/start` cannot override a disabled deployment gate, active manual kill switch, or
active circuit breaker.

`POST /trading/stop` is idempotent and always writes the runtime state to stopped after successful
authentication.

## Shared runtime state

FastAPI writes:

```text
data/control/trading_state.json
```

using a temporary file, fsync, and atomic replace.

Example:

```json
{
  "version": 1,
  "enabled": true,
  "updated_at": "2026-10-01T08:00:00+00:00",
  "reason": "operator started trading",
  "source": "fastapi_control"
}
```

Missing, unreadable, invalid, or unsupported state fails closed.

The Rust risk engine reads the same file before every normal live risk evaluation and again when an
existing normal approval crosses the execution gate. The execution client also revalidates that
approval before each order retry. This means a stop request invalidates both a freshly issued
approval and any later retry before another submission is attempted.

If a previous submission failed ambiguously, a newly closed gate returns the original ambiguous
execution error instead of pretending the exchange definitely received nothing. The coordinator
therefore preserves its existing unknown-order reconciliation behavior.

Shadow `preview()` does not require the runtime live gate, so Phase 12 research continues while
live trading is stopped.

Emergency unwind does not require the runtime start state, preserving the ability to reduce
exposure after a stop.

## Start

Example:

```bash
curl -X POST http://localhost:8000/trading/start \
  -H "Authorization: Bearer $ARB_CONTROL_API_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"reason":"operator approved micro-live session"}'
```

A successful response includes the new runtime state.

Start returns HTTP 409 if:

- the static live deployment gate is disabled;
- the risk runtime directory is unavailable;
- the manual kill switch is engaged; or
- a circuit breaker is active.

## Stop

```bash
curl -X POST http://localhost:8000/trading/stop \
  -H "Authorization: Bearer $ARB_CONTROL_API_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"reason":"operator stop"}'
```

Stopping does not clear the manual kill switch or a circuit breaker.

## Health

`GET /health` reports:

```text
API status
database status
market-stream activity
risk state
deployment trading gate
runtime trading gate
risk permission for new orders
effective trading state
control-auth configured
```

It does not expose API secrets or exchange credentials.

## Read endpoints

### GET /opportunities

Returns the recent opportunity ledger with gross edge, net edge, capital, decision status, and
rejection reason.

### GET /trades

Returns Phase 13 canary/reconciled trade records, including expected and realized P&L, prediction
error, latency, and slippage.

### GET /performance

Returns the dashboard performance metrics without running unrelated balance or system queries.

### GET /balances

Returns the most recent account snapshot captured by the read-only Phase 13 account path.

## Docker

The API service mounts:

```text
data/risk     read-only
data/control  read-write
```

The Rust process and FastAPI must reference the same underlying `data/control` directory for the
runtime gate to work across processes.

## Frontend

The Phase 14 React client now uses only the Phase 15 read surface:

```text
/opportunities
/trades
/performance
/balances
/health
```

Typed `startTrading()` and `stopTrading()` client functions are also available. They require a
Bearer token supplied at call time. The dashboard does not persist that control token in browser
storage.
