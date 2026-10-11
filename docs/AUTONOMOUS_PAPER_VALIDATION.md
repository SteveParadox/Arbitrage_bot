# Autonomous paper implementation validation

This branch integrates candidate observation with an autonomous **paper-only** execution lifecycle. The code is reviewable and the local service-based regression is clean. Full acceptance is still **blocked** by unverified Docker startup, unavailable public Bybit connectivity and the absence of a multi-hour soak. Keep the PR draft until those operational gates are verified. No authenticated exchange order was submitted.

All financial examples below use intentionally profitable **synthetic executable-depth fixtures**. They are simulated results, not exchange-confirmed fills or evidence of live profitability.

## A. Implementation and integration

- `engine-service::observer` hosts `paper_runtime::Runtime`; the existing connector, synchronized scanner, durable candidate outbox and bounded paper dispatcher share one managed process. Observe defaults and PR #6 control/stop protections remain.
- `rust/paper` adds the concrete `PaperVenue` behind the existing `CoordinatorVenue` interface. Existing `BybitVenue` remains separate and cannot be selected by paper startup. Market IOC matching walks asks/bids with exact decimal quantities, depth, fees, quote/base rounding, caps, minimum amounts, submission price caps, latency, partial fills, cancellations and dust.
- `RiskEngine::evaluate_with_portfolio` extends the existing risk engine with per-asset exposure, drawdown and loss-streak checks. Approval precedes reservation; locked reservation and per-leg checks revalidate mutable restrictions. Global stop/kill prevents new exposure and preserves risk-reducing unwind.
- `Ledger` adds transactionally durable account ownership/fencing, reservations, cycle intent, stable order/fill identities, same-asset balanced transfers, receipts, fees, settlement, conservative inventory marks and reconciliation. One active cycle per account is the enforced concurrency ceiling.
- The existing `ThreeLegCoordinator` propagates actual post-fee fill output through all legs, unwinds partial/failing execution when supported by evidence, and preserves unresolved inventory. Restart disables spending, reconstructs durable receipts, expires uncommitted paper intents, settles once and quarantines intermediate assets.
- Migrations `0008_autonomous_paper` and `0009_paper_valuation` add isolated paper financial/control tables, constraints and indexes. Existing observation/event tables and Redis conflict/redelivery handling are retained.
- `api.autonomous_paper` uses existing authentication for account creation, idempotent start/stop, status, cycles, simulated orders/fills, reservations, reconciliation, valuations, performance and events. `PaperPanel` adds paper lifecycle/portfolio/performance/audit views and a shared validated status contract.
- Triangle discovery validates nonempty directed catalogs and writes atomic replacements with timestamp/environment/hash. Compose overlays supply migration/catalog dependencies, persistent state, explicit authorization, resource limits and shutdown. Offline fixtures and bounded demonstration scripts use the actual managed entry point.

The dependency map, configuration, operational policy and short startup sequence are in [AUTONOMOUS_PAPER.md](AUTONOMOUS_PAPER.md).

## B. Verified execution chain

| Connection | Source → destination | Executed evidence |
|---|---|---|
| Market events → synchronized books | `connector::run_with_recovery` / `observer::replay_events` → `ArbitrageScanner::on_book_update` and `PaperBooks::update` | Real deployed replay, managed observer generation/recovery integration; public network session not verified |
| Scanner → candidate journal | scanner records → `observer::candidate_event` → `EventPublisher::publish_critical_at` | Profitable entry-point test asserts candidate ID in PostgreSQL `engine_events`; real Redis consumer |
| Candidate → dispatcher | `observer::run_observation` → `Runtime::dispatch` → bounded worker | All five `test_autonomous_entry_point` scenarios |
| Dispatcher → risk | worker → `RiskEngine::evaluate_with_portfolio` | Risk denials for asset/total exposure, drawdown, loss streak; no cycle on denial |
| Risk → capital | approved decision → `Ledger::reserve` | Independent-process competition at 2/10/50/100; ownership and duplicate settlement |
| Capital → coordinator | committed cycle intent → `ThreeLegCoordinator::execute_route` | Reservation/crash boundaries and profitable entry-point replay |
| Coordinator → each simulated order | coordinator actual holdings → `PaperVenue::execute` / `submit` → `match_order` | Three exact receipts; partial second leg; adverse movement cap; buy/sell depth unit tests |
| Simulated fills → durable inventory | `PaperVenue::submit` → `Ledger::fill` | Fees applied once; partial/unwind/crash receipt identities; PostgreSQL outage recovery |
| Inventory → reconciliation/P&L | coordinator report → `Ledger::settle` → `Ledger::reconcile` | Exact closed-cycle delta, losing unwind, residual quarantine, tamper-block test |
| PostgreSQL → API | authoritative tables → `autonomous_paper.status/cycles/performance/reconciliation/valuation` | Real HTTP assertions from deployed integration; four captured financial records |
| API → dashboard | `PaperPanel.refresh` → `validatePaperStatus` and activity rendering | 17 frontend tests, including an actual service-evidence status contract; lint and build. Browser/Docker full-stack interaction not verified |

## C. Test report

Rows below overlap. They are executed suite/case counts, not additive totals. The three workspace-ignored Redis tests were separately run and passed.

| Category | Passed | Failed | Skipped / unverified | Evidence |
|---|---:|---:|---|---|
| Rust unit/workspace | 101 | 0 | 3 initially ignored, then passed separately | `rust-validation.log`; format, clippy all features / warnings denied, build |
| Python unit + service integration | 250 | 0 | 0 | `python-validation.log`; includes 31 paper integration cases with real services |
| Frontend | 17 | 0 | 0 | Vitest 3 files, ESLint, TypeScript/Vite build |
| Capital concurrency | 4 | 0 | 0 | 2/10/50/100 independent processes, different route requests, duplicate settlement |
| Paper execution unit cases | 9 | 0 | 0 | `paper` crate matching/mode/stop cases in Rust log |
| Three-leg entry-point scenarios | 5 | 0 | 0 | Profitable, unprofitable, capital denial, partial recovery, crash |
| Database integration | 31 + 1 migration verifier | 0 | 0 | Paper suite; fresh upgrade → 0007 rollback → re-upgrade; negative-balance constraint |
| Redis explicit reliability | 3 | 0 | 0 | `redis-validation.log`: outage recovery, restart delivery, bounded backlog preservation |
| Crash/restart and DB session recovery | 11 | 0 | 0 | Seven crash boundaries, first-leg crash, partial/unwind crashes, database-session outage |
| Financial reconciliation | 4 financial captures + tamper test | 0 | 0 | Four exact ledger comparisons; discrepancy blocks execution |
| P&L/accounting financial captures | 4 | 0 | 0 | Profitable, partial recovered, losing recovered, unresolved residual |
| End-to-end A–H scenarios | 8 covered | 0 | 0 | Actual Rust process/HTTP/PG/Redis replay, capital competition, stale feed and persistence outages |
| Docker startup | 0 | 0 code failures established | Blocked | Build could not reach daemon; daemon socket operation prohibited |
| Bounded sustained replay | 1 soak + 5 latency runs | 0 | Multi-hour run not performed | `paper-soak.json`, `paper-latencies.json` |

One Python warning concerns Starlette's deprecated `httpx` TestClient support. Earlier exploratory failures were corrected; this report uses the final rebuilt binary and clean final regression. GitHub Actions billing/runner state was not used as a pass/fail substitute.

## D. Exact financial demonstration

[paper-financial-cases.json](evidence/paper-financial-cases.json) contains the actual cycle, durable intent/receipt, final balances and ledger comparison for every example.

**Closed successful paper cycle:** initial virtual account 500 USDT; commit/reserve 100 USDT; untouched available amount 400 USDT; scanner expected net profit 19.51035988 USDT. This large synthetic edge is intentional fixture design.

| Leg | Side / market | Executed base | Depth price | Quote spent/received | Fee | Net output |
|---|---|---:|---:|---:|---|---|
| 1 | BUY BTCUSDT | 1 BTC | 100 USDT/BTC | spent 100 USDT | 0.001 BTC | 0.999 BTC |
| 2 | BUY ETHBTC | 19.98 ETH | 0.05 BTC/ETH | spent 0.999 BTC | 0.01998 ETH | 19.96002 ETH |
| 3 | SELL ETHUSDT | 19.96002 ETH | 6 USDT/ETH | received 119.76012 USDT | 0.11977 USDT | 119.64035 USDT |

Settlement: cycle ending holdings 119.64035 USDT; net paper realized P&L **+19.64035 USDT**, return **19.64035%** (1964.035 bps); account available **519.64035 USDT**, all reservations zero, BTC/ETH zero. Independent account ledger and reservation differences are exactly zero. Scanner expected net and realized simulation differ because of modeled-cost assumptions; their difference is not labeled pure execution slippage.

**Partial second-leg recovery:** first leg produces 0.999 BTC; second leg fills 10 ETH, pays 0.01 ETH, cancels its 9.98 ETH remainder and preserves 0.499 BTC. Unwind sells 9.99 ETH at 6 (fee 0.05994 USDT) and 0.499 BTC at 99 (fee 0.04941 USDT). Final cycle value **109.23165 USDT**, account **509.23165 USDT**, state **RECOVERED**, residual holdings zero and exact reconciliation balanced.

**Losing unwind:** a kill switch after leg one forces recovery. Selling 0.999 BTC at 99 with fee 0.09891 USDT returns **98.80209 USDT**. Realized paper P&L **−1.19791 USDT**; account **498.80209 USDT**, state RECOVERED, balances reconcile. Failed planned execution is not reported as a normal three-leg completion.

**Crash with unresolved inventory:** after the first committed fill, restart preserves **400 USDT + 0.999 BTC**. Starting-asset realized P&L is **null**, state RECOVERY_REQUIRED, no duplicate order exists, reservations are zero after inventory settlement, new exposure is disabled and the account is blocked. There is no fabricated 500 USDT cash balance or realized profit. Residual marks require fresh executable evidence.

## E. Sustained operation and remaining blockers

The final synthetic run lasted **60.034 seconds** at 50 ms modeled latency. It had 50 completed cycles and one in progress at sampling; graceful shutdown ended with **51 closed cycles**, zero active cycles/pending orders/reservations and balanced reconciliation. Peak engine RSS was **38064 KiB**, measured engine CPU **2.739 seconds**, and one PostgreSQL probe **0.479 ms** (not a transaction-latency distribution). Queue/outbox/Redis telemetry is retained in the JSON. Synthetic net paper P&L was 1030.631150000000 USDT; it says nothing about achievable market returns.

Latency scenarios below each run for approximately five seconds and finish their active cycle during shutdown. Arrival/scheduling affects cycle count, so total P&L is not a matched per-opportunity performance estimate. The 100 ms run includes a loss-making recovery; 200 ms leaves explicit residual exposure; 500 ms prevents stale fills.

| Modeled latency ms | Measured seconds | Closed cycles | Realized paper P&L USDT | Unresolved cycles | Final account state |
|---:|---:|---:|---:|---:|---|
| 25 | 5.003 | 5 | 98.401170000000 | 0 | stopped |
| 50 | 5.003 | 4 | 78.720940000000 | 0 | stopped |
| 100 | 5.003 | 4 | 57.802910000000 | 0 | stopped |
| 200 | 5.003 | 0 | 0 | 1 | blocked |
| 500 | 5.008 | 5 | 0 | 0 | stopped |

Merge/deployment blockers: actual Docker builds/startup/shutdown; live public Bybit session and current filter validation; multi-hour resource/recovery soak; production deployment and security review. The 60-second run is not a 24-hour or production soak.

Future exchange trading is separate and unverified: private authentication, exchange-confirmed fills, real balances, ambiguous order outcomes/timeouts, order/execution streams, REST reconciliation and fee-currency handling. Existing exchange receipt schemas/Bybit adapter are retained, but this change does not claim exchange integration tests or activate testnet/live execution. Paper mode needs no exchange credentials. Maker queues and third-asset fee discounts are not simulated.

Conservative scope decisions: one active cycle per paper account; restart quarantines intermediate inventory instead of resubmitting orders or liquidating against missing evidence; there is no account balance-reset endpoint. Book inputs retain the existing normalized market-data representation, then matching and authoritative accounting use Decimal. Public catalog refresh is atomic and startup rejects stale/empty/mismatched catalogs.

## F. Delivery, commands and audit

Feature branch: `codex/autonomous-paper-trading-engine`, target `main`, base `5528b27d23f82c9b9e4137d88ac772149b5cbb36`. Runtime/test/infrastructure code commit: `63ad9a5`. Documentation/evidence follows in a separate commit. The final PR URL and commit SHA are recorded in the delivery message and GitHub PR metadata. No automatic merge is authorized.

Executed local gates:

```bash
cargo fmt --manifest-path rust/Cargo.toml --all -- --check
cargo clippy --manifest-path rust/Cargo.toml --workspace --all-targets --all-features -- -D warnings
cargo build --manifest-path rust/Cargo.toml --workspace
cargo test --manifest-path rust/Cargo.toml --workspace
cargo test --manifest-path rust/Cargo.toml -p event-bus -- --ignored --test-threads=1
cd python
python -m pip install -e '.[dev]'
python -m ruff check .
python -m pytest -q
cd ../frontend
npm ci
npm run lint
npm test
npm run build
```

Python integrations used isolated PostgreSQL 16, Redis 7, service flags `ARB_RUN_{PAPER,OBSERVER,GRPC,RELIABILITY}_INTEGRATION=1`, an independently running test control engine and the rebuilt managed engine binary. Migration and demonstration commands:

```bash
python scripts/verify_paper_migrations.py
python -m alembic upgrade head  # from python/
python scripts/run_paper_soak.py --seconds 5 --latencies 25,50,100,200,500 --output docs/evidence/paper-latencies.json
python scripts/run_paper_soak.py --seconds 60 --latencies 50 --output docs/evidence/paper-soak.json
```

Docker image build was attempted and blocked by daemon permissions. Mainnet/testnet public catalog fetches were attempted and failed; offline fixtures were used. No exchange credentials or authenticated order requests were used.

The final self-audit corrected demonstrated defects: globally colliding ledger operation keys across cycles; submission-price reference reset after latency; missing quote precision accounting; route skew revalidation; projected intermediate exposure; portfolio risk/state isolation; fixture timing races; fixed-port collisions; and execution-book update ordering that could bypass managed scanner resynchronization. Regression coverage and the final clean run substantiate these fixes. [validation-results.json](evidence/validation-results.json) records counts and the deployed binary hash; detailed logs and financial/latency/soak records are alongside it.
