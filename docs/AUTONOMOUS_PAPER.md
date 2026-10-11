# Managed autonomous paper trading

This path runs in `engine-service`, alongside the existing managed public-data observer. It uses the existing Rust scanner, risk engine and three-leg coordinator. Its concrete venue is `paper::PaperVenue`; it has no HTTP order client or credential provider. Existing authenticated Bybit execution remains a separate coordinator adapter and is never selected by autonomous startup.

The fixture prices in the demonstration are synthetic, deliberately profitable executable books. Paper fills are not Bybit executions and paper returns are not a live-profitability claim.

## Modes and authorization

Defaults remain `ARB_TRADING_MODE=observe`, `ARB_PAPER_TRADING_ENABLED=false`, `ARB_LIVE_TRADING_ENABLED=false`, `ARB_ENGINE_AUTOSTART=false`.

An autonomous paper deployment requires all of:

```dotenv
ARB_TRADING_MODE=paper
ARB_PAPER_TRADING_ENABLED=true
ARB_LIVE_TRADING_ENABLED=false
ARB_ENGINE_AUTOSTART=true
ARB_PAPER_ACCOUNT=demo
```

These deployment flags do not authorize spending by themselves. Create a virtual account, wait for its runtime to finish recovery, then issue the authenticated paper start command. Restart always disables the account before recovery. TESTNET and LIVE are rejected by this startup path. No Bybit API keys are needed.

The existing global kill switch, malformed control/stop records, pending emergency stop and confirmed global stop also prevent new paper exposure. They are checked at dispatch, between normal legs and before normal order submission. A previously submitted paper intent can finish; risk-approved close/unwind operations may reduce already-held exposure. Stop does not credit fictitious starting funds. Existing live gRPC authorization and PR #6 control-state handling are preserved.

## Integration and dependency map

| Existing component | Integration |
|---|---|
| `market_data::connector::run_with_recovery` | Observer subscribes to every catalog symbol, refreshes metadata, validates session generations and feed readiness |
| `OrderBookEngine` / `ArbitrageScanner::on_book_update` | Same normalized updates populate scanner and paper books; existing age/skew/subscription gates remain |
| `observer::candidate_event` / `EventPublisher::publish_critical_at` | Candidate is durably accepted by the existing outbox before paper dispatch |
| `paper_runtime::Runtime::dispatch` | Bounded queue of 32 candidates; one sequential worker per account |
| `RiskEngine::evaluate_with_portfolio` | Existing execution checks plus portfolio limits, before capital reservation |
| `paper::Ledger::reserve` | PostgreSQL account lock, epoch fencing, balance update, cycle intent and journal in one transaction |
| `ThreeLegCoordinator::execute_route` | Existing actual-fill propagation, emergency authorization, unwind, failure and dust logic |
| `CoordinatorVenue` | Already abstracted; new paper adapter and existing Bybit adapter reuse the same coordinator |
| `PaperVenue::submit` / `matching::match_order` | Durable intent, modeled delay, fresh synchronized executable depth, exact decimal simulated receipt |
| `Ledger::fill` | Atomic receipt, holdings changes, fee transfers and durable financial event |
| `Ledger::settle` / `reconcile` | Idempotent actual-inventory settlement, balanced transfer reconciliation and closed-cycle P&L |
| Existing Redis consumer / PostgreSQL event journal | Observer/coordinator outbox events retain redelivery and conflict protection; paper financial tables are directly authoritative |
| `api.autonomous_paper` / `PaperPanel` | Authenticated controls, balances, cycles, orders, fees, performance, valuation and audit events |

Existing Python historical simulator/analytics remain useful for archived-data research; they are not used as a second autonomous coordinator or capital ledger. Shadow and micro-canary modules remain separate. New autonomous results use `paper.trade.*`, preventing simulated receipts from being classified as real legacy trades.

## Short offline demonstration

With Docker Compose available, from the repository root:

1. Copy `.env.example` to `.env`. Set the four explicit paper flags above, `ARB_ENV=development`, and strong **different** `ARB_CONTROL_API_TOKEN` and `ARB_ENGINE_GRPC_TOKEN`. Leave all exchange credentials empty.
2. Start the integrated offline platform:

```bash
docker compose --env-file .env -f docker/docker-compose.yml \
  -f docker/paper.compose.yml -f docker/paper.offline.compose.yml up --build -d
```

3. Initialize the virtual account through the API. Substitute your control token and fresh request IDs:

```bash
curl -fsS http://127.0.0.1:8000/paper/accounts \
  -H "Authorization: Bearer $ARB_CONTROL_API_TOKEN" -H 'Content-Type: application/json' \
  -d '{"account_id":"demo","asset":"USDT","amount":"500","request_id":"demo-funding-001"}'

curl -fsS http://127.0.0.1:8000/paper/demo/status \
  -H "Authorization: Bearer $ARB_CONTROL_API_TOKEN"

curl -fsS http://127.0.0.1:8000/paper/demo/control/start \
  -H "Authorization: Bearer $ARB_CONTROL_API_TOKEN" -H 'Content-Type: application/json' \
  -d '{"request_id":"demo-start-001","reason":"explicit synthetic paper demonstration"}'
```

Wait for `runtime_online=true` and recovery to finish before start. The fixture supplies about five minutes of market events. Open `http://127.0.0.1:5173`, connect the `demo` paper account with the control token and inspect the three simulated legs. API controls use the established bearer authorization; the token stays in dashboard memory.

To stop new exposure:

```bash
curl -fsS http://127.0.0.1:8000/paper/demo/control/stop \
  -H "Authorization: Bearer $ARB_CONTROL_API_TOKEN" -H 'Content-Type: application/json' \
  -d '{"request_id":"demo-stop-001","reason":"stop demonstration"}'
docker compose --env-file .env -f docker/docker-compose.yml \
  -f docker/paper.compose.yml -f docker/paper.offline.compose.yml down
```

The engine receives SIGINT and finishes/reconciles its active cycle before graceful shutdown. Persistent PostgreSQL, Redis, outbox and paper state survive ordinary `down`; removing volumes destroys test history. There is no balance reset endpoint. Existing accounts cannot be funded again or silently reset.

Docker execution could not be verified in the implementation environment. See the validation report before relying on these container instructions.

## Public catalog and feed

The committed empty catalog is a safe placeholder, not a trading configuration. The paper Compose overlay generates a validated catalog before engine startup. Without the offline overlay it calls the **public** V5 instrument endpoint and fails startup if discovery fails or finds no valid routes. Choose mainnet public data with `BYBIT_TESTNET=false`, or testnet public data with `BYBIT_TESTNET=true`; catalog and connector environments must match. Testnet private execution is not enabled.

Outside Docker:

```bash
cd python
BYBIT_TESTNET=false python -m strategy.triangle_discovery --start-assets USDT \
  --output ../data/catalog/triangles.json
```

Set `ARB_TRIANGLE_CONFIG` to that absolute path. Generation excludes inactive instruments, generates directed buy/sell routes, rejects duplicates/empty output and writes an atomic replacement with timestamp and SHA256. A failed refresh leaves the previous file intact but does not silently start trading with it. Paper startup rejects catalogs older than 24 hours or future-dated catalogs. Catalog changes require a controlled managed restart; an active cycle carries its immutable route/configuration evidence.

Current spot metadata must include active status, `basePrecision`, `quotePrecision`, `minOrderAmt`, `maxMarketOrderQty` and `tickSize`. Missing/invalid metadata fails closed. Runtime metadata expires after five minutes and reconnect refresh is inherited from the managed observer. Source: [official Bybit V5 instruments](https://bybit-exchange.github.io/docs/v5/market/instrument).

## Risk and candidate gates

The dispatcher requires a complete, positive net scanner result, recent candidate, synchronized subscribed books, decimal revaluation after modeled fees, sufficient depth, precision/amount filters, minimum edge, Rust risk approval, healthy durable outbox, Redis and PostgreSQL. The existing risk engine enforces control state, kill/breaker, market freshness, minimum edge, slippage, liquidity, balance, trade size, symbol precision, total exposure, daily loss and service health.

`shared/config/risk.json` adds `paper_portfolio_limits`: maximum per-asset exposure in the account starting asset, current closed-equity drawdown, and consecutive closed losses. Checks run before approval, before further normal exposure, and again under the reservation lock. Current intermediate inventory is repriced using actual holdings; remaining dust is conservatively valued at zero for the new-exposure profitability gate. The final close and unwind may realize losses rather than retaining avoidable inventory. Daily loss and performance days use UTC.

One active cycle per account is an intentional conservative concurrency limit. Different accounts can run independently, with isolated balances, risk state and ownership. Asset exposure bounds use planned executable valuations; unresolved residual holdings block all new exposure. There are no added news inputs because the existing engine does not provide a verified news restriction source.

Candidate and immutable economic-evidence uniqueness are enforced in PostgreSQL. Sequence/timestamp churn alone cannot replay an unchanged attractive quote. Configurable `ARB_PAPER_ROUTE_COOLDOWN_MS` defaults to 1000; queue overflow is recorded. Rejections contain candidate identity, reason code and details; Rust risk denials preserve configuration version and measured check results.

## Execution and accounting assumptions

Only market IOC orders are modeled. Buys walk asks using actual quote budget; sells walk bids. Matching consumes multiple levels, floors base quantities, validates ticks and caps, and rejects minimum amounts or inadequate evidence. Quote spend is rounded up and quote proceeds down to quote precision, with rounding dust journaled separately; rounded buy budgets cannot overspend. Depth-weighted prices remain the actual book prices. The output asset pays the modeled taker fee, rounded up to its precision. Buy fees are base asset; sell fees are quote asset. Maker orders and third-asset fee discounts are not modeled. They must not be enabled by silently reusing this fee model.

`ARB_PAPER_FEE_RATE` defaults to 0.001. `ARB_PAPER_LATENCY_MS` defaults to 50. `ARB_PAPER_ORDER_TIMEOUT_MS` defaults to 1000 and is bounded to 5000. The submission book is journaled; matching uses a new synchronized snapshot after delay. The IOC price cap remains tied to submission evidence, so an adverse price jump cannot silently reset its slippage reference. Receipts expose submission-reference price and signed price slippage in basis points when evidence supports them. Missing/stale depth produces no fill. Timeout produces a terminal cancellation with no fill. Current-book simulated consumption is retained until a newer book supersedes it; simulated fills never consume invented quantities.

Every order has a stable client/order ID, parent cycle ID, leg number (or recovery role), request, submission evidence, final receipt, fees, remaining quantity and timestamp. Fills are aggregate depth-weighted IOC receipts with stable `paper:<order>:fill` IDs. Acknowledgement alone is never used as a fill. Leg 2/3 use the coordinator's actual post-fee holdings.

Ledger transfers carry one positive amount between two scopes **in the same asset**. Conversion has one input transfer and one output transfer; fees are separate transfers, once. Counterparty `simulated_venue` is explicit rather than treating different currencies as interchangeable debits/credits. Available balances reconcile to ledger transfers; active holdings reconcile to cycle transfers; reservations reconcile to unsettled intents. Nonnegative database checks and transactional account locks prevent overspending.

Reserved starting capital is a **commitment memorandum**, not extra spendable starting-asset inventory after conversion. Actual equity is available assets plus unsettled actual holdings, never available + reserved + holdings. Settlement removes the commitment and credits only the assets actually held. A reservation/cycle is settled once.

Closed-cycle realized P&L is ending starting-asset holdings minus committed starting amount. Fees are already deducted from holdings and are not subtracted again. Percentage/basis-point returns, gross received starting asset, per-asset fees, expected-versus-realized difference and duration are exposed in cycle metrics. The difference between expected and realized net profit is not falsely labeled pure slippage.

Unclosed cycles have `realized_pnl=null`, explicit cash delta and remaining inventory. Conservative executable-depth valuations have source, timestamp, amount and modeled liquidation-fee assumptions. Missing, stale or quantity-mismatched marks cause `estimated_equity=null`; no residual value is invented. Estimated unrealized P&L is separate from realized closed-cycle P&L.

## Durable lifecycle and recovery

Cycle states: RESERVED → EXECUTING → terminal COMPLETED / RECOVERED / FAILED / RECOVERY_REQUIRED. Individual order intent and terminal receipts plus leg events provide durable three-leg progression. Invalid submit-after-settlement and conflicting order/fill identities are rejected. Reservation, order, fill, fee and settlement side effects are transactional and retry safe.

The account owner holds a PostgreSQL session advisory lock; every financial write checks its epoch. A second runtime cannot claim the same account. Losing the connection loses authority; the worker exits and managed restart is required. The new runtime increments epoch and disables execution before inspecting unfinished cycles.

Paper fills have **no external side effect** separate from their PostgreSQL transaction. Recovery can therefore expire intents lacking a committed fill without risking a later exchange fill. Previously committed receipts and actual holdings are settled exactly once. Intermediate assets are quarantined as RECOVERY_REQUIRED and block authorization; they are not converted back into phantom cash. Recovery does not resubmit an ambiguous intent. This policy intentionally favors quarantine over restart-driven liquidation against unverified depth.

During a running cycle the existing coordinator can safely unwind failed/partial legs with risk-reducing authorization. If fresh executable unwind evidence is unavailable, holdings remain explicit and the account is blocked. A losing unwind records the loss. Reconciliation discrepancies also latch the account block.

Strategy reload is detected between cycles, disables new dispatch, and requires a recovery-first managed restart. Automatic process supervisors may restart it; standalone operators restart `engine-service`. Active cycles retain their original route.

## APIs and dashboard

All new routes reuse `require_control_auth`:

| Method | Path | Result |
|---|---|---|
| POST | `/paper/accounts` | Idempotent initial virtual funding; no reset |
| POST | `/paper/{account}/control/start` | Explicit deployment/account authorization, recovery/heartbeat/block gates |
| POST | `/paper/{account}/control/stop` | Audited, idempotent prevention of new exposure |
| GET | `/paper/{account}/status` | Runtime/lifecycle state, balances and committed reservations |
| GET | `/paper/{account}/cycles` | Complete/failed/recovered/current cycles, actual holdings and exact P&L metrics |
| GET | `/paper/{account}/orders` | Durable pending/terminal simulated orders |
| GET | `/paper/{account}/fills` | Stable simulated receipts |
| GET | `/paper/{account}/reservations` | Derived reservation lifecycle |
| GET | `/paper/{account}/reconciliation` | Independent balance/reservation differences |
| GET | `/paper/{account}/performance` | All-history realized P&L, daily P&L, win/loss metrics, drawdown, duration |
| GET | `/paper/{account}/valuation` | Timestamped conservative inventory estimates or missing evidence |
| GET | `/paper/{account}/events` | Risk, capital, fill, reconciliation, control and recovery journal |

Reads are bounded to 500 activity records; performance uses authoritative full closed history. The React panel is additive to existing observation/analytics UI and validates the shared status contract. It shows paper mode, authorization, runtime, active/pending counts, asset availability/commitment, actual holdings, fees, realized and estimated unrealized results. Existing operational observer health remains separate: SCANNING does not mean EXECUTING, and COMPLETED does not imply profitable.

## Local validation and bounded replay

Build Rust and migrate isolated PostgreSQL before selecting integration tests:

```bash
cargo fmt --manifest-path rust/Cargo.toml --all -- --check
cargo clippy --manifest-path rust/Cargo.toml --workspace --all-targets --all-features -- -D warnings
cargo build --manifest-path rust/Cargo.toml --workspace
cargo test --manifest-path rust/Cargo.toml --workspace
cargo test --manifest-path rust/Cargo.toml -p event-bus -- --ignored --test-threads=1
cd python
python -m pip install -e '.[dev]'
python -m alembic upgrade head
python -m ruff check .
ARB_RUN_PAPER_INTEGRATION=1 python -m pytest -q tests/test_autonomous_paper.py
cd ../frontend
npm ci
npm run lint
npm test
npm run build
```

Set `ARB_DATABASE_URL` and `ARB_REDIS_URL` to isolated services; integration tests mutate those databases and launch actual Rust processes. Other existing reliability tests additionally require `TEST_DATABASE_URL`, `ARB_RUN_RELIABILITY_INTEGRATION=1`, `ARB_RUN_GRPC_INTEGRATION=1`, `ARB_RUN_OBSERVER_INTEGRATION=1`, a separately running isolated control engine and its PID/log/control/reload paths. The lost-response gRPC test uses existing development-only `ARB_TEST_GRPC_RESPONSE_DELAY_MS=250`; expired deadlines can surface as the specific `CANCELLED: Timeout expired` on current Tonic/grpcio.

From the repository root, with explicit development paper flags and control token:

```bash
python scripts/verify_paper_migrations.py
python scripts/run_paper_soak.py --seconds 60 --latencies 50 --output data/paper/soak.json
python scripts/run_paper_soak.py --seconds 5 --latencies 25,50,100,200,500 \
  --output data/paper/latencies.json
```

The migration verifier creates and drops its own temporary PostgreSQL database. The bounded runner launches FastAPI and the actual managed Rust entry point, creates isolated accounts and reports measured duration, resource usage, lifecycle outcomes and actual receipts. It is not a direct coordinator unit invocation. Development-only crash injection points and the reservation probe exist solely for isolated integration tests; probe reservations cannot submit orders and require explicit paper/live-disabled flags. Replay fixtures may use `wait_for_paper_leg` (1–3) to wait, at most ten seconds, for a durable pending paper order before supplying changed depth. This read-only barrier requires explicit paper authorization and makes partial-fill tests independent of process scheduling; it is inaccessible to public-feed startup.

## Deployment and remaining verification

The Compose overlay orders healthy PostgreSQL/Redis, successful migration/catalog generation, managed recovery, public subscription and explicit account enablement. Internal databases and gRPC bind loopback, as do development API/frontend ports. Persistent volumes retain financial and control state. Engine process health is liveness only; execution eligibility comes from the authenticated paper API and observer readiness.

Production limits remain: no verified Docker full-stack startup in the implementation environment, no successful public Bybit connectivity run, no multi-hour soak, and no exchange-confirmed private execution tests. This research paper adapter models terminal IOC receipts, not maker queues or private exchange uncertainty. Future testnet integration must use the existing separate Bybit adapter and authenticated private order/execution streams plus REST recovery, fees and real balance reconciliation. Accepted create-order requests are not confirmed fills. See [create-order](https://bybit-exchange.github.io/docs/v5/order/create-order) and [private executions](https://bybit-exchange.github.io/docs/v5/websocket/private/execution).

Do not enable live trading from these paper results. Container validation, long-duration operational testing, current-market filter verification and deployment/security review remain merge/deployment evidence gates; see [the implementation validation report](AUTONOMOUS_PAPER_VALIDATION.md).
