# Phase 14: TypeScript Operations Dashboard

Phase 14 replaces the original frontend scaffold with a React + TypeScript operations console.

## Stack

```text
React 19
TypeScript
Vite
FastAPI operations API
PostgreSQL analytics tables
```

No charting or component library is required for the current dashboard. The UI is built with typed
React components and responsive CSS.

## Main metrics

The dashboard shows:

```text
Account balance
Today's P&L
Weekly P&L
Net return
Detected opportunities
Executed trades
Rejected opportunities
Success rate
Average net edge
Average latency
```

Metric semantics are deliberately explicit:

- Account balance is the latest balance snapshot captured with a Phase 13 canary candidate.
- Today's and weekly P&L use reconciled Phase 13 results.
- Net return is today's realized P&L divided by today's reconciled starting capital.
- Detected and rejected opportunity counts use the Phase 7 opportunity ledger over the last 24h.
- Executed trades count reconciled Phase 13 cycles for the current UTC day.
- Success rate is profitable reconciled cycles divided by reconciled cycles.
- Average net edge is calculated from accepted opportunity observations.
- Average latency is actual execution time from reconciled Phase 13 cycles.

## Opportunity table

Endpoint:

```text
GET /operations/opportunities
```

Columns:

```text
Time
Triangle
Gross edge
Net edge
Capital
Status
Reason rejected
```

Rows are ordered newest first.

## Execution view

Endpoint:

```text
GET /operations/executions
```

The UI renders the route as a vertical asset path, for example:

```text
USDT
 ↓
BTC
 ↓
ETH
 ↓
USDT
```

Each connector displays the detection-time average execution price when available. The panel also
shows expected P&L, realized P&L, prediction error, execution time, actual slippage, capital, and
expected net edge.

The current database does not contain a trustworthy realized percentage return for every
individual leg, so the dashboard does not invent per-leg percentages.

## System status

Endpoint:

```text
GET /operations/dashboard
```

The status strip shows:

```text
API status
market-stream activity
trading enabled/disabled
risk state
kill switch
```

### Market-stream status

The Python API is not the owner of the Rust WebSocket connection, so it cannot truthfully expose a
socket handle state.

Phase 14 therefore uses recent opportunity arrival as an activity proxy:

```text
<= 5 seconds   connected
<= 30 seconds  stale
> 30 seconds   offline
```

The UI labels this source explicitly.

### Risk and kill switch

The operations API reads the same persisted risk-state and kill-switch files used by the Rust risk
engine. It reports:

```text
ready
halted
circuit_breaker
```

plus the active breaker and kill-switch detail when present.

## API configuration

Backend:

```env
ARB_CORS_ORIGINS=http://localhost:5173
```

Multiple frontend origins can be comma-separated.

Frontend:

```env
VITE_API_URL=http://localhost:8000
```

Copy `frontend/.env.example` to `frontend/.env` when a non-default backend address is required.

## Run locally

Backend:

```bash
cd python
uvicorn api.main:app --host 0.0.0.0 --port 8000
```

Frontend:

```bash
cd frontend
npm install
npm run dev
```

Open the Vite URL, normally:

```text
http://localhost:5173
```

## Production build

```bash
cd frontend
npm run build
```

The generated static assets are written to `frontend/dist`.

## Refresh model

The dashboard loads its three operational endpoints in parallel and refreshes every five seconds.

A manual Refresh button is also available. If the backend is unavailable, the dashboard preserves
the last successful data set and displays an API error banner instead of replacing operational
data with fabricated zeros.
