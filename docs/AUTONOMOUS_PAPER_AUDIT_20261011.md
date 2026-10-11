# Autonomous paper engine audit — 11 October 2026

**Delivery status: corrected draft, not merge-ready under the requested acceptance criteria.** The actual application runtime completes and durably accounts for synthetic paper cycles. Eight reproduced defects were corrected and eleven regression cases added. Docker execution, current public Bybit validation, strict virtual-clock replay determinism, exhaustive fault coverage and production/multi-user security remain outstanding. No private exchange order was submitted; `ARB_LIVE_TRADING_ENABLED=false` throughout.

This audit distinguishes executed evidence from inspection and unverified requirements. Synthetic returns below are deliberately constructed test results, not achievable Bybit profits. Earlier implementation evidence remains historical; the new evidence is under [evidence/paper-audit-20261011](evidence/paper-audit-20261011).

## A. Verified repository state and connection map

| Item | Verified value |
|---|---|
| Repository | `SteveParadox/Arbitrage_bot` |
| Inspected main | `5528b27d23f82c9b9e4137d88ac772149b5cbb36` — PR #8 merge, including PR #6 foundation |
| Actual implementation | [PR #9](https://github.com/SteveParadox/Arbitrage_bot/pull/9), open draft, not merged |
| Initial implementation head | `08bf8a7ad2d3b6ac0d38536dbcaefe17817f9af0` |
| Implementation branch | `codex/autonomous-paper-trading-engine` |
| Isolated local audit branch | `audit/paper-corrections-20261011` |
| Tested local correction commit | `9790510fc4f03170028afa7bf8f9d1f9937f3cac` |
| Tested correction tree | `da3a58b66b072ebdcce3c9418762b693306fb6ec` |
| Final corrected code SHA | `211b5b695fc70c6a979710b93431210ed8b66a35`; final PR head is its documentation/evidence-only child. See publication evidence and final PR description. |
| Migration | New reversible `0010_paper_finite`, following `0009_paper_valuation`; finite numeric checks, no data rewrite |

Production changes: `rust/paper/src/{venue,ledger}.rs`, `python/api/autonomous_paper.py`, the new migration and `frontend/src/PaperPanel.tsx`. Tests: `frontend/tests/paper.test.tsx`, `python/tests/test_autonomous_paper.py`, new `test_paper_audit.py`, `paper_reference.py` and `capture_paper_audit.py`. Documentation/evidence are separate additions. No working private execution code or prior control/outbox implementations were rewritten.

The integrated entry point is `rust/engine-service/src/main.rs`, managed observation in `observer::run_observation`, with `paper_runtime::Runtime::start` in the same process. A direct coordinator demo was not substituted for this path.

| Stage | Actual implementation | Called by | Calls | Verification |
|---|---|---|---|---|
| Market data | `market-data` public connector; observer development replay | managed observer | normalized metadata, book, health events | connector/orderbook/managed replay tests; live public session blocked |
| Scanner | `scanner::engine`, synchronized snapshots | observer event loop | triangle evaluation/candidate records | real runtime produces durable candidate and cycle |
| Dispatcher | `paper_runtime::Runtime::dispatch`, bounded queue 32 | observer after durable critical candidate publication | worker reprice/age/readiness/dedup checks | real process profitable/stale/capital tests |
| Risk engine | `RiskEngine::evaluate_with_portfolio`; coordinator authorizer | dispatcher and each normal/reducing order | mandatory approval/TTL/control/portfolio checks | portfolio denials and global kill tests |
| Reservation | `Ledger::reserve` | approved runtime worker | account `FOR UPDATE`, authoritative revalidation, unique cycle/evidence, transfer | 2/10/50/100 separate-process probes; bounded lock test |
| Coordinator | `ThreeLegCoordinator` | runtime worker after reservation | concrete planner, venue, authorizer, bounded unwind | three-leg, partial, losing and restart cases |
| Paper venue | concrete `PaperVenue::execute/submit` | coordinator | fresh `PaperBooks`, Decimal `match_order`, durable intent/fill | paper crate arithmetic tests; actual three receipts |
| Fill reconciliation | `Ledger::fill`, `cycle_holdings`, coordinator actual outputs | venue/coordinator | atomic receipt, transfer, fee and holdings commit | independent receipt/ledger verifier |
| Settlement | `Ledger::settle` | runtime coordinator result / restart recovery | account lock, actual assets, reservation removal, terminal state | repeat settlement; crash boundaries; partial assets |
| P&L | `Ledger::settle`; API performance/cycle metrics | same financial transaction / reads | closed cash delta, nullable residual P&L | exact positive, negative and residual vectors |
| PostgreSQL | `paper_accounts/balances/cycles/orders/ledger/events/valuations/commands` | Rust ledger / authenticated API | transactional financial authority | migrations, concurrency, rollback, restart tests |
| FastAPI | `api/autonomous_paper.py`; `api.main` lifecycle | HTTP client/UI | authenticated controls, snapshot-consistent SQL reads | real Uvicorn HTTP integration and read isolation regression |
| Event consumer | Python Redis consumer / existing critical disk outbox | FastAPI lifecycle / Rust event publisher | persist `engine_events`, acknowledge after commit | candidate seen in DB; Redis outage/restart/backlog tests |
| Frontend | `PaperPanel.tsx`, shared response contract | React app polling | paper endpoints / explicit controls | 18 Vitest cases, lint/build; browser/container visual QA not executed |

Paper accounting is committed directly to PostgreSQL, not reconstructed from Redis. `paper_events` commits with each financial mutation. Redis carries existing candidate/coordinator transport events and has the established durable outbox. There is no separate paper fill daemon with an unconnected financial authority. The historical Python research simulator remains a separate replay/reporting tool and is not autonomous runtime execution.

Runtime re-evaluates Decimal executable depth, original candidate age, route/metadata direction, market readiness and skew, current fees, minimum edge, slippage, runtime controls and portfolio stats. The reservation lock rechecks authoritative availability, active execution, account authorization and limits. Single active cycle per account is the conservative concurrency policy; tests at 100 contenders use at most 20 worker threads and separate engine probe processes, not 100 simultaneously executing legs.

## B. Reproduced defect report

| ID / severity | File / function | Root cause | Reproduction | Correction | Regression / result |
|---|---|---|---|---|---|
| D1 HIGH | `venue.rs`, `PaperBooks::update` | Every unrelated book delta reset all consumed simulated depth | Consume asks, update bids, then asks were fully available again | Consumption tracked by symbol and side/price; only explicitly refreshed levels reset; full snapshot resets symbol | `unrelated_delta_does_not_replenish_consumed_liquidity`: failed before, passed after |
| D2 HIGH | `venue.rs`, `PaperBooks::consume` | Consumption allocation counted sub-lot depth that matching never executed | First level 0.015 with step 0.01; subsequent level wrongly retained an executable lot | Allocate consumption in complete executed lots using the same floor rule | `consumption_uses_executed_lots_not_substep_dust`: failed before, passed after |
| D3 CRITICAL | `ledger.rs::reconcile`, API reconciliation | Checks began from balance rows, so deleting a funded asset row hid its ledger discrepancy | Delete funded USDT balance; API reported balanced | Reconcile union of balance, ledger and active reservation assets, treating missing balance as zero; runtime latches block | API missing-row and actual engine startup recovery regressions passed; startup refuses new orders |
| D4 CRITICAL | paper NUMERIC constraints | PostgreSQL accepts NaN/Infinity; `available >= 0` did not imply a finite balance | SQL UPDATE available to NaN / Infinity succeeded | Migration 0010 rejects non-finite balances, cycle totals, ledger amounts and valuations; fails on invalid existing rows | Both independent SQL rejection tests failed before / passed after; upgrade/downgrade/re-upgrade passed |
| D5 HIGH | API `_runtime_online`, start control | Future heartbeat had negative age and was treated as fresh | Set heartbeat one hour ahead; status online and start accepted | Require heartbeat age in [0, 5] seconds | Future heartbeat regression failed before / passed after; start returns 409 |
| D6 MEDIUM | paper GET dependencies | READ COMMITTED queries could mix two financial commits in one response | Commit funding from a second connection between response queries | REPEATABLE READ READ ONLY transaction for each paper GET response | Concurrent-writer snapshot regression passed; response stays 500, next response sees 501 |
| D7 HIGH | `Ledger::connect`, API transaction setup | PostgreSQL lock acquisition and statement execution lacked server-side bounds | Hold account lock in one session; separate reservation probe exceeded five-second subprocess deadline | Rust connect 3s, lock 2s, statement 5s, idle transaction 10s; API lock 2s / statement 5s | Bounded reservation regression failed before / passed after; no cycle and balance remains 500/0 |
| D8 MEDIUM | `PaperPanel::refresh` | A delayed response could restore stale authorization after disconnect / overwrite newer polling | Resolve old deferred request after disconnect; controls reappeared | Refresh generation fence, invalidate on disconnect/cleanup, validate response before publishing status | React regression failed before / passed after |

Evidence includes the original failing reproduction logs and focused passing logs. D6 was verified with a new concurrent-session test rather than claiming an unexecuted baseline failure. Timeout values deliberately fail closed under contention; deployments must size them for their measured workload. A timeout is not an approval to retry a financial mutation with a new operation ID.

No balancing entries, deleted tests, disabled assertions, unjustified skips or relaxed live-trading gate were used. New completed-loss fixture explicitly sets prediction buffers to zero while retaining actual fees and the unchanged risk minimum and IOC price cap; this is isolated test configuration, not a production configuration change.

## C. Financial correctness answers

These answers apply to the executed synthetic IOC scenarios, not an exhaustive proof for every production input.

| Question | Answer and verified evidence |
|---|---|
| 1. Can virtual capital be double-spent? | No double spend in tested 2/10/50/100 separate-process races; exactly one account reservation wins. Account row lock and one-active-cycle revalidation serialize spending. |
| 2. Are reservations atomic? | Yes in tested transactions: 500/0 becomes 400/100 with one durable cycle and transfer; lock timeout leaves 500/0 with no cycle. |
| 3. Are balances mathematically consistent? | All executed complete, recovered and quarantined traces agree exactly with the independent rational verifier; missing funded rows now fail reconciliation. |
| 4. Are fees correct? | Modeled output-asset taker fees are rounded up once and journaled separately. Independent exact positive/negative fixtures agree. Third-asset fee discounts are unsupported, not verified. |
| 5. Can duplicate simulated fills occur? | No duplicates in tested stable order identity/restart/redelivery cases. Conflicting identities are rejected; intent/result and holdings commit together. Exhaustive adversarial schedule proof is absent. |
| 6. Can settlement happen twice? | Repeated same-ID settlement has one financial effect in separate-process tests; uniqueness, settled-state check and account locking protect it. |
| 7. Are partial fills accounted for? | Yes: partial second leg buys 10 ETH, spends 0.50 BTC, charges 0.010 ETH and leaves 0.499 BTC, with IOC canceled remainder 9.980 ETH. |
| 8. Are intermediate assets preserved? | Yes: after first-leg crash, 400 USDT + 0.999 BTC are preserved; no restoration of the original 100 USDT. Account remains blocked. |
| 9. Are failed-cycle losses reported? | Yes: losing unwind closes at −1.19791 USDT; completed three-leg movement loss is −0.04048 USDT. Performance includes these negative values. |
| 10. Does independent P&L agree with ledger? | Yes for executed integration reconciliation cases and all captured traces. `paper_reference.py` uses exact Fractions, persisted receipts and scope transfers, and imports no production financial calculation. |
| 11. Can interrupted cycles recover? | Tested crash points recover committed identities and settle/quarantine actual holdings without duplicate receipts. Restart does not automatically resume or liquidate ambiguous intermediate inventory. |
| 12. Can paper mode submit real orders? | Inspected runtime binds concrete PaperVenue with no order HTTP client/credential provider. Mode/replay fail-closed tests pass; paper runs require no credentials. No private HTTP attempt was made. Network-wide zero-order egress instrumentation was not performed. |

Reservation is a commitment memorandum. It is not extra USDT equity after USDT has become BTC/ETH. Conversion transfers debit/credit scopes within the same asset, with explicit simulated counterparty and fee scopes. Closed realized P&L is actual final starting-asset quantity minus committed starting quantity. Residual inventory has nullable realized P&L and independent timestamped marks; missing marks produce null estimated equity. Binary floats remain in upstream market normalization/scanner; financial holdings use Decimal and SQL NUMERIC. Lossless original exchange-string precision for arbitrary extreme metadata is not claimed.

## D. Executed tests and operational measurements

Final raw logs accompany this report. Rows overlap; do not sum subsystem rows into a new test total. “Covered” means included in a passed named suite, without inventing a separate run count.

| Test category | Passed | Failed | Skipped | Status |
|---|---:|---:|---:|---|
| Rust formatting | 1 command | 0 | 0 | `cargo fmt --all -- --check` |
| Rust Clippy | 1 command | 0 | 0 | workspace/all-targets/all-features, warnings denied |
| Rust workspace tests | 103 | 0 | 3 initially ignored | All three Redis tests separately executed and passed |
| Python tests | 258 | 0 | 0 | Real PostgreSQL, Redis, gRPC/control process, observer and paper integration flags |
| Rust/Python profitability parity | 10 shared fixtures | 0 | 0 | Independent cross-language fixture command |
| Python Ruff | 1 command | 0 | 0 | All checks passed |
| Frontend tests | 18 | 0 | 0 | npm ci, lint, Vitest and production build passed |
| Capital reservation | 4 contention sizes + timeout case | 0 | 0 | 2/10/50/100 independent processes; unchanged funds on timeout |
| Financial arithmetic | 7 matching cases + 2 liquidity regressions | 0 | 0 | Exact buy/sell/depth/partial/budget/fee/rounding/filters tests |
| Coordinator state machine | 3 coordinator integration tests + runtime cases | 0 | 0 | Actual quantities and bounded failure/unwind |
| Partial fills | Covered | 0 | 0 | Partial IOC and partial/unwind restart cases |
| Failed-leg recovery | Covered | 0 | 0 | Losing unwind, residual quarantine and actual-asset reconciliation |
| PostgreSQL concurrency | Covered | 0 | 0 | Different sessions/processes, ownership, conflicts, read snapshots |
| Redis reliability | 3 explicit ignored tests + Python cases | 0 | 0 | Pause, restart, backlog, outage and durable events |
| Restart recovery | Covered | 0 | 0 | Seven parameterized crash checkpoints, leg-one, partial/unwind and DB-session outage; not all requested windows |
| Ledger reconciliation | Independent verifier + API/runtime tamper cases | 0 | 0 | Exact union-asset recomputation; missing row blocked |
| P&L validation | Five captured traces + runtime tests | 0 | 0 | Profitable, completed loss, losing unwind, partial recovery and residual |
| Security controls | Covered | 0 | 0 | Bearer rejection, conflicting requests, mode/replay gates and portfolio denials; no tenant/RBAC pentest |
| End-to-end paper trading | 34 real-service paper cases + 5 API audit cases | 0 | 0 | Managed Rust runtime, scanner/risk/venue/coordinator/DB/FastAPI/consumer |
| Database migration | 1 verifier | 0 | 0 | Fresh upgrade → downgrade to 0007 → re-upgrade; negative and finite checks |
| Docker integration | Configuration parsed | Not classified as code failure | Build/start blocked | Daemon Unix socket returns operation not permitted; full stack not run |
| Sustained operation | 60.029s + five ~5s profiles | 0 observed process crashes | Long soak not run | Bounded synthetic run only |

The 60.028670392-second 50ms run closed 46 cycles after graceful shutdown, with zero active cycles, pending orders or reserved funds and balanced DB reconciliation. Child resource accounting measured 38,056 KiB max RSS and 2.168643 CPU seconds; the single PostgreSQL probe took 0.465295ms. These are smoke measurements, not scanner throughput, reservation latency percentiles, queue growth bounds or a long-term leak study. Large synthetic fixture profits do not establish real-market profitability.

Five separate profiles ran at 25/50/100/200/500ms, each about five seconds. They settled respectively 4/3/3/1/5 cycles; settlement does not mean profitable completion, particularly when the delay outlives freshness evidence. Each final account reconciled. The independent rational verifier also recomputed all 62 persisted cycles in the six soak/latency accounts after shutdown (`soak-independent.json`), with no discrepancies. Raw states/rejections, partial orders, API/consumer transport health and shutdown records are in `latencies.json`.

Exploratory full-suite setup failures were retained separately: one gRPC lost-response test initially lacked its development response-delay injection; five analytics failures occurred when reusing a populated analytics database. Supplying the existing injection and a fresh separate TEST_DATABASE_URL resolved those setup errors without changing assertions. One Starlette/httpx deprecation warning remains. An initial Rust 1.90 build could not use the repository's required API; the validated Rust toolchain was 1.99, matching the Dockerfile.

GitHub CI on initial head: run `38118799508`, six jobs report failure with no executed steps in returned job metadata. They are not counted as local code failures or passing checks. Historical account billing restrictions are known, but the exact current failure reason was not independently retrieved. Branch protection was not changed.

Test environment: stock PostgreSQL 16 and Redis 7 were launched together with application/test processes in one execution namespace. This host prohibits Unix sockets and UID switching, so PostgreSQL used TCP and a test-only UID/stat shim; production code and SQL transactions were unchanged. No container user/permission isolation conclusion is drawn from that adaptation. Tool downloads, shim, data directories and harness are scratch test infrastructure, not deployment changes.

## E. Actual end-to-end financial demonstrations

Fresh complete persisted receipts, orders, intents, cycles, account balances, ledger transfers, API snapshots and independent-verifier outputs are in `financial-traces.json`. Every account starts at 500 USDT, reserves 100, leaving 400 available. All fees below are quantities in the named fee currency. Weighted price equals the single consumed level in these fixtures; multi-level vectors are covered separately in Rust tests.

### 1. Profitable complete cycle

| Order | Market/side | Filled base | Execution price | Quote value | Fee | Holdings after fill |
|---|---|---:|---:|---:|---|---|
| Leg 1 | BTCUSDT BUY | 1 BTC | 100 USDT/BTC | 100 USDT spent | 0.001 BTC | 0.999 BTC |
| Leg 2 | ETHBTC BUY | 19.980 ETH | 0.05 BTC/ETH | 0.999 BTC spent | 0.019980 ETH | 19.960020 ETH |
| Leg 3 | ETHUSDT SELL | 19.960020 ETH | 6 USDT/ETH | 119.760120 USDT received | 0.119770 USDT | 119.640350 USDT |

`COMPLETED`; final account **519.640350 USDT**, zero BTC/ETH and zero reserve. Realized P&L **+19.640350 USDT**, unrealized P&L zero. No recovery trade. API reconciliation and independent ledger/receipt recomputation both agree exactly.

### 2. Full three-leg completed loss

Fixture admission bid 5.018 has positive executable net edge. Only fixture prediction buffers are zero; three 0.1% fees and mandatory Rust risk remain active. After durable Leg 3 intent, bid moves to 5.013, within its unchanged 0.1% IOC cap. Leg 1 buys 1 BTC at 100 with 0.001 BTC fee; Leg 2 buys 19.980 ETH at 0.05 with 0.019980 ETH fee. Leg 3 sells **19.960020 ETH at 5.013**, receives **100.059580 USDT**, fee **0.100060 USDT**, leaving **99.959520 USDT** in cycle holdings.

`COMPLETED`, three filled orders; final account **499.959520 USDT**, no intermediate asset, zero reserve. Realized P&L **−0.040480 USDT**; unrealized zero. Expected profit is not copied into realized P&L. Exact independent recomputation and API performance agree.

### 3. Partial second leg requiring recovery

| Order | Market/side | Filled base | Price | Quote value | Fee | Intermediate holdings |
|---|---|---:|---:|---:|---|---|
| Leg 1 | BTCUSDT BUY | 1 BTC | 100 | 100 USDT spent | 0.001 BTC | 0.999 BTC |
| Partial Leg 2 | ETHBTC BUY | 10 ETH of 19.980 requested | 0.05 | 0.50 BTC spent | 0.010 ETH | 0.499 BTC + 9.990 ETH; 9.980 ETH canceled |
| Recovery | ETHUSDT SELL | 9.990 ETH | 6 | 59.940 USDT received | 0.059940 USDT | 0.499 BTC + 59.880060 USDT |
| Recovery | BTCUSDT SELL | 0.499 BTC | 99 | 49.401 USDT received | 0.049410 USDT | 109.231650 USDT |

`RECOVERED`; final account **509.231650 USDT**, no intermediate inventory, zero reserve. Realized P&L **+9.231650 USDT**, unrealized zero. Four actual simulated receipts and ledger transfers agree with DB/API reconciliation. The original 100 USDT was never refunded independently of these conversions.

Additional losing unwind: buy 1 BTC at 100, fee 0.001 BTC; global kill stops further exposure; sell actual 0.999 BTC at 99, receive 98.901 USDT, fee 0.098910 USDT. `RECOVERED`, final **498.802090 USDT**, realized **−1.197910 USDT**, zero unrealized/reserve, exact reconciliation.

Additional crash: after committed Leg 1, terminate and restart actual engine. Exactly one existing receipt survives. Final **400 USDT + 0.999 BTC**, zero reserve, `RECOVERY_REQUIRED` and blocked. Cycle realized P&L is **null**, cash delta −100 USDT; unrealized and total estimated equity are **null** because synchronized valuation evidence is unavailable at capture. No fabricated recovery conversion or realized −100 loss was reported. Independent ownership/accounting reconciliation passes.

## F. Remaining limitations and acceptance blockers

| Requirement / severity | Status and reason |
|---|---|
| HIGH: strict deterministic autonomous replay | Observer rebases replay timestamps to current runtime time and uses Tokio sleeps. Barrier fixtures make specific numerical cases repeatable, but a scheduler-independent virtual clock is not implemented. This acceptance requirement remains open. |
| HIGH: complete Docker startup/restart | Compose config parses; build fails at daemon socket operation not permitted. PostgreSQL/Redis/Rust/API were exercised as processes, not images. Image startup, volumes/permissions, health ordering and full container restart remain unverified. |
| Current public Bybit data/filter verification | Public instruments request returned HTML “Site Unavailable”; no live public session/catalog validation completed. Offline discovery/filter tests pass. |
| Exhaustive requested adversarial matrix | Existing and added tests cover many meaningful boundaries, but every leg rejection/partial/timeout/cancel schedule, every listed crash point, actual disk-full, gRPC ambiguous financial command, route mutation and real network reconnection is not independently injected. |
| Normal-user multi-account RBAC/rate limits | All paper endpoints require one privileged global control bearer. There is no ordinary-user identity/tenant authorization or per-user role model; privileged operators can manage all paper accounts. This is not verified multi-user production access control. |
| Precision/model breadth | Financial balances are exact Decimal/NUMERIC, but normalized public market numbers still pass through f64. Extreme original-string precision, maker queues and supported-third-asset fees are not implemented/verified. |
| Restart policy | Preserves and quarantines intermediate assets instead of auto-resuming or liquidating them. Manual approved operational recovery is still needed for blocked residual accounts. |
| Performance/stability | Short measured smoke only; no multi-hour/24-hour soak, latency distributions, sustained backlog saturation, per-stage throughput benchmark or leak proof. |
| Browser/dashboard/deployment security | Vitest/TypeScript/build passed; actual responsive browser/container rendering, production TLS/secrets/volume/network security and production operators are unverified. |
| Real trading | Authenticated private exchange orders, exchange-confirmed fills, live balance reconciliation and production live authorization were not attempted. Live remains disabled; paper is not verified real-money execution. |

Current official [Bybit V5 instrument documentation](https://bybit-exchange.github.io/docs/v5/market/instrument) was consulted for basePrecision, quotePrecision, minOrderAmt, maxMarketOrderQty, tickSize and trading status. Deprecated minOrderQty is not substituted for minOrderAmt. Public filters constrain this model; they do not establish live-order eligibility or account fee rates.

No demonstrated D1–D8 defect remains unresolved in its regression scenarios. That statement does not close the HIGH acceptance blockers above or establish absence of all critical/high defects. The full requested audit is therefore **incomplete**, and the PR remains draft.

## G. Delivery and reproduction

Eight scoped defects corrected, eleven regression cases added, independent exact-rational accounting verifier integrated into actual process tests, and five fresh trace scenarios captured. The original implementation PR is updated rather than merged or replaced. Useful implementation history is preserved. The corrected published SHA and tested tree identity are recorded with publication evidence and the PR description.

Use Rust 1.99, Python 3.12, Node 22-compatible tooling, PostgreSQL 16 and Redis 7. Start disposable isolated PostgreSQL/Redis first; the tests create/tamper with accounts and must not target production databases. Use two fresh databases, one for application/paper tests and one for analytics tests.

```bash
export ARB_LIVE_TRADING_ENABLED=false
export ARB_ENV=development
export ARB_DATABASE_URL=postgresql+psycopg://arbitrage:arbitrage@127.0.0.1:5432/arbitrage_audit
export TEST_DATABASE_URL=postgresql+psycopg://arbitrage:arbitrage@127.0.0.1:5432/arbitrage_test
export ARB_REDIS_URL=redis://127.0.0.1:6379/0
export ARB_ENGINE_BINARY="$PWD/rust/target/debug/engine-service"
cargo fmt --manifest-path rust/Cargo.toml --all -- --check
cargo clippy --manifest-path rust/Cargo.toml --workspace --all-targets --all-features -- -D warnings
cargo build --manifest-path rust/Cargo.toml --workspace
python scripts/check_profitability_parity.py
cargo test --manifest-path rust/Cargo.toml --workspace
cargo test --manifest-path rust/Cargo.toml -p event-bus -- --ignored --test-threads=1
(cd python && python -m pip install -e '.[dev]')
(cd python && python -m alembic upgrade head)
(cd python && ARB_DATABASE_URL="$TEST_DATABASE_URL" python -m alembic upgrade head)
(cd python && python -m ruff check .)
ARB_RUN_PAPER_INTEGRATION=1 python -m pytest -q python/tests/test_autonomous_paper.py python/tests/test_paper_audit.py
python scripts/verify_paper_migrations.py
python python/tests/capture_paper_audit.py --output data/paper/audit-financial-traces.json
```

For the full Python suite including old gRPC reliability tests, start a separate observe-only control engine with the same isolated Redis/app DB and these paths/flags, then run all tests from `python/`. These test tokens are examples, not production secrets.

```bash
export ARB_ENGINE_GRPC_ADDR=127.0.0.1:55451
export ARB_ENGINE_GRPC_TARGET=127.0.0.1:55451
export ARB_ENGINE_GRPC_TOKEN=audit-internal-control-token-0123456789
export ARB_CONTROL_API_TOKEN=audit-public-control-token-0123456789
export ARB_TEST_GRPC_RESPONSE_DELAY_MS=200
export ARB_ENGINE_AUTOSTART=false ARB_TRADING_MODE=observe ARB_PAPER_TRADING_ENABLED=false
export ARB_CONTROL_STATE_FILE="$PWD/data/audit/control.json"
export ARB_RUNTIME_LIMITS_FILE="$PWD/data/audit/limits.json"
export ARB_STRATEGY_RELOAD_FILE="$PWD/data/audit/reload.json"
export ARB_GRPC_IDEMPOTENCY_STORE="$PWD/data/audit/grpc"
export ARB_EVENT_OUTBOX_PATH="$PWD/data/audit/outbox"
export ARB_ENGINE_PID_FILE="$PWD/data/audit/engine.pid"
export ARB_ENGINE_LOG_FILE="$PWD/data/audit/engine.log"
mkdir -p data/audit
"$ARB_ENGINE_BINARY" > "$ARB_ENGINE_LOG_FILE" 2>&1 &
echo $! > "$ARB_ENGINE_PID_FILE"
export ARB_RUN_PAPER_INTEGRATION=1 ARB_RUN_OBSERVER_INTEGRATION=1
export ARB_RUN_GRPC_INTEGRATION=1 ARB_RUN_RELIABILITY_INTEGRATION=1
(cd python && python -m pytest -q)
kill "$(cat "$ARB_ENGINE_PID_FILE")"
```

The process-restart test can replace the control PID, so stop the PID currently recorded, not only the initial background PID. Use clean databases/paths on repeated validation runs. Wait for gRPC readiness before tests (the test fixture performs readiness retries).

```bash
export ARB_TRADING_MODE=paper ARB_PAPER_TRADING_ENABLED=true
export ARB_LIVE_TRADING_ENABLED=false ARB_ENV=development
python scripts/run_paper_soak.py --seconds 60 --latencies 50 --output data/paper/audit-soak.json
python scripts/run_paper_soak.py --seconds 5 --latencies 25,50,100,200,500 --output data/paper/audit-latencies.json
(cd frontend && npm ci && npm run lint && npm test && npm run build)
docker compose -f docker-compose.yml -f docker-compose.paper.yml -f docker-compose.paper-offline.yml config
# Unverified here: requires a functioning Docker daemon.
docker compose -f docker-compose.yml -f docker-compose.paper.yml -f docker-compose.paper-offline.yml build
```

Re-run strict virtual-clock and remaining adversarial/container/security validation before changing merge readiness. No automatic merge was performed.
