# Virtual-clock and safety continuation — 2026-10-11

## A. Verified repository state

Main inspected: `5528b27d23f82c9b9e4137d88ac772149b5cbb36`.
Implementation: [draft PR #9](https://github.com/SteveParadox/Arbitrage_bot/pull/9),
branch `codex/autonomous-paper-trading-engine`; open and unmerged.
Clock published at `aa636dd980df7246882a088199fa0dc1e861c84c`, scoped-account fix
at `ad3587f0e36b808920dacdf6d64871a324794a59`, security/fault continuation at
`3a587d7cf2f251eaef7fa9606c7b4ee964e2565e`.

Tested final source commit: `051bbb59007415986a917b73c9d4de947d1c7927`, tree
`be8a86e8111897c02b0b50cc88db36b81034dac1`. The following delivery commit adds
documentation/evidence only; PR metadata identifies that head. No migration is
added by this continuation. Existing migrations through 0010 were exercised.

Primary changes: new `rust/runtime-clock`; scoped time in risk/scanner/coordinator,
paper ledger/venue/engine runtime; `engine-service/src/virtual_replay.rs`; Python
status/valuation and React clock/permission contracts; new Python/Rust security
modules; expanded replay/security/crash tests; replay and soak scripts; Compose
state path/liveness corrections. Clock mode lives in account JSON config.

This is a continuation of the historical A–G audit, not a claim that every
original acceptance criterion is now satisfied. See PAPER_VIRTUAL_REPLAY.md and
PAPER_SECURITY.md for implementation policies and configuration.

## B. Reproduced defects and corrections

| Severity | File / root cause | Reproduction | Correction and regression | Result |
|---|---|---|---|---|
| High | `paper/ledger.rs`, account clock UPDATE omitted `WHERE id=$1` | Actual first runtime startup failed its SQL bind; PostgreSQL rolled back | Scope UPDATE to account; repeated replay verifies unrelated account config remains unchanged | Passed |
| High | `event-bus`, JSON float parsing changed persisted payload values | Retry of exact financial payload raised false event-ID conflict; before/after logs retained | Enable `serde_json/float_roundtrip`; retry pending/delivered/reopened event, still reject truly changed payload | Failed before, passed after |
| High | `virtual_replay.rs`, EOF ID exceeded critical-event 64-character limit | Completed losing replay failed terminal critical publication | Stable SHA-256 ID; real EOF regressions for profitable/losing/partial paths | Passed |
| Medium | `make_paper_replay.py`, float addition emitted off-tick synthetic prices | 310-frame profile completed 306 cycles; four planning rejections for invalid precision | Decimal price increments; assert exact generated price grid and all 310 completed cycles | Failed before, passed after; filters unchanged |
| Low | `test_virtual_paper.py`, plugin-only fixture registration depended on collection scope | Full suite produced 17 missing-fixture setup errors though focused run passed | Explicit fixture import; full-suite execution | Passed |
| Low | `test_paper_security.py`, production config fixture inherited development fault environment | Full suite rejected supposedly safe production config | Isolate fixture inputs; add five explicit forbidden-hook regressions | Passed; production checks retained |
| Medium | `run_paper_soak.py`, optional procfs read aborted accounting diagnostics | Sandbox child PID absent from `/proc`, FileNotFoundError during first sample | Record unavailable sampled metrics; retain mandatory ledger checks and measured wait4 final resources | Measured run passed |

Additional delivered hardening: own-account viewer/operator/admin API permissions,
global-admin-only funding, server-derived UI control permission, shared atomic
Redis quotas, verified emergency-stop exception, bounded streamed bodies,
production mode/credential/origin/host/role checks, secret-file support and
development-only leg/commit crash hooks. These are tested controls, not an
external penetration test or deployed security certification.

## C. Financial correctness evidence

1. No double spending occurred in the executed 2/10/50/100-contender process tests.
   Their pool is bounded at 20 workers, not 100 simultaneous workers. SQL account
   locks, ownership fencing and one-active-cycle policy remain authoritative.
2. Reservation and settlement tests passed against real PostgreSQL, including
   contention rollback and duplicate settlement. No in-memory-only substitute.
3. Independent exact-rational reconstruction agrees with persisted balances,
   fees, holdings and realized P&L for repeated virtual cases, all 310 logical
   cycles and all 46 final wall-soak cycles.
4. No duplicate committed fill or settlement appeared in crash/restart tests.
   Added leg intent/matching/fill-commit and P&L boundaries passed. The matrix is
   not exhaustive.
5. Partial fills preserve actual intermediate quantities. Recoverable inventory
   is sold through simulated recovery orders; unresolved restart inventory is
   quarantined with nullable realized P&L, not fabricated starting cash.
6. A completed loss remains a loss in SQL/API performance. All modeled fees in
   these fixtures are charged once in the received asset. Third-asset fee
   discounts and exchange-confirmed fee accounting remain outside this model.
7. Virtual IOC latency is exact at 25/50/100/200/500ms; same-time market batches
   precede fills. Repeated financial results/timestamps match despite SQL delay.
8. Paper uses only the simulated venue. Mode gates and live=false checks remain
   enforced; no private exchange request, live balance or exchange-confirmed fill
   was used. This is not verification of real-money execution.

## D. Executed tests

| Category | Executed result | Evidence / scope |
|---|---|---|
| Rust format | Passed | `cargo fmt ... --check` on final source |
| Strict Clippy / build | Passed | workspace, all targets/features, warnings denied; logs |
| Rust workspace | 108 passed, 0 failed, 3 ignored by default | `rust-tests.log` |
| Ignored Redis reliability | 3 passed explicitly | Full service log; no unprocessed critical trimming, pause, restart |
| Python Ruff | Passed | Entire python tree; changed generator/soak scripts also checked |
| Python complete suite | 314 passed, 0 failed, 0 skipped; 1 deprecation warning | 358.42 seconds; `python-full-services.log` |
| Virtual replay focused | 17 passed | 197.92 seconds; includes independent 310-cycle reconciliation |
| Frontend | Lint, 21 tests, build passed | `frontend.log`; no browser visual/responsive claim |
| PostgreSQL migration | Fresh upgrade → downgrade to 0007 → re-upgrade passed | Fresh disposable DB; negative-balance constraint exercised |
| Reservation / arithmetic / coordinator | Passed included cases | Real processes/SQL; exact fixture expectations and Rust tests |
| Partial fills / unwind / restart | Passed included cases | Before/after every normal leg intent/fill commit, settlement hooks, preserved residuals |
| Security | 20 unit cases + 2 real-worker cases passed in full suite | Own-account authorization; shared Redis quota; stop availability |
| End-to-end | Included actual-runtime cases passed | Observer → scanner → risk → SQL reservation → fills → settlement → API/consumer |
| Profitability parity | 10 shared cases passed | Actual Rust/Python parity script |
| Sustained operation | 60.127633316-second wall run passed | 46 final closed cycles, 138 orders; six periodic whole-ledger checks |
| Docker runtime / production deployment | Not executed | No Docker executable or daemon socket; YAML parses and live gates false |
| Exhaustive faults / multi-hour stability | Not verified | Remaining matrix and long-run deployment acceptance below |

The disposable stock PostgreSQL 16 server used a test-only UID/ownership shim to
start in this root-only sandbox. SQL locks/transactions were real; this is not
production filesystem/UID permission validation. Redis 7, built Rust binaries,
gRPC, actual FastAPI workers and the event consumer were exercised together.

Failed runs are not hidden: initial SQL bind failure; reproduced outbox identity
failure; a 120-second replay test budget timeout; expanded run 89 passed/1 fixture
failure; first full run 291 passed/1 fixture failure/17 setup errors; first soak
procfs failure. The corrected full suite and soak above are subsequent results.

## E. Actual simulated financial traces

All three accounts began with 500 USDT and reserved 100 USDT. Their full SQL
orders, fills, cycle holdings, ledger and independent verification are retained
in `evidence/paper-virtual-20261011/virtual-financial-traces.json`. Prices are
deliberately synthetic, not live quotes or evidence of market profitability.

| Scenario / order | Simulated quantity | Execution price | Fee |
|---|---|---|---|
| Profitable: buy BTCUSDT | 1 BTC | 100 USDT/BTC | 0.001 BTC |
| Buy ETHBTC | 19.980 ETH | 0.05 BTC/ETH | 0.019980 ETH |
| Sell ETHUSDT | 19.960020 ETH | 6 USDT/ETH | 0.119770 USDT |
| Losing: buy BTCUSDT | 1 BTC | 100 USDT/BTC | 0.001 BTC |
| Buy ETHBTC | 19.980 ETH | 0.05 BTC/ETH | 0.019980 ETH |
| Sell ETHUSDT after adverse movement | 19.960020 ETH | 5.013 USDT/ETH | 0.100060 USDT |
| Partial: buy BTCUSDT | 1 BTC | 100 USDT/BTC | 0.001 BTC |
| Partially filled buy ETHBTC | 10 ETH | 0.05 BTC/ETH | 0.010 ETH |
| Recovery: sell ETHUSDT | 9.990 ETH | 6 USDT/ETH | 0.059940 USDT |
| Recovery: sell BTCUSDT | 0.499 BTC | 99 USDT/BTC | 0.049410 USDT |

Profitable intermediate holdings: 0.999 BTC → 19.960020 ETH → 119.640350 USDT
inside the cycle. Ending account: **519.640350 USDT**, reserved zero, residual BTC
and ETH zero; realized **+19.640350 USDT**, unmarked residual inventory zero.

Losing intermediate holdings: 0.999 BTC → 19.960020 ETH → 99.959520 USDT.
Ending account: **499.959520 USDT**, reserved zero, other assets zero; realized
**−0.040480 USDT**. Scanner prediction did not replace the actual loss.

Partial intermediate holdings after leg two: **0.499 BTC + 9.990 ETH**. Both
recovery orders liquidate actual quantities, producing 109.231650 USDT in the
cycle. Ending account: **509.231650 USDT**, reserved zero, other assets zero;
state RECOVERED, realized **+9.231650 USDT**. No starting reservation is released
as cash while BTC/ETH still exist. Independent SQL reconstruction passes all
three. No unrealized residual profit is added to realized totals.

Wall-soak result: 60.127633316 seconds, 46 closed cycles/138 simulated orders;
independent final reconciliation balanced, zero active cycles/pending orders/
reserved capital. Final wait4 resources: 41,208 KiB peak RSS, 4.1435 CPU seconds.
Per-sample procfs memory is explicitly unavailable in this sandbox. At the
pre-shutdown sample 45 cycles were closed and one executing; graceful shutdown
completed that last cycle. This does not prove a memory-growth trend or
multi-hour stability. `wall-soak.json` preserves both before/after state.

## F. Remaining acceptance steps, in order

1. Complete fault tests: real outbox ENOSPC/fsync errors, cancellation/fill races,
   release/settlement races, concurrent recovery/fencing, reordered/delayed
   accounting messages, clock-skew profiles and saturation. Selected passing
   crash hooks are not exhaustive coverage.
2. Validate real Docker image/start/migration/volume behavior and active-cycle
   restart, health/readiness, non-root identity, secrets and graceful shutdown.
3. Deploy and test TLS/proxy trust, credential rotation, DB ACLs/private networks,
   request-abuse protections and backup/restore. Inspect all permissions beyond
   the implemented superuser/create-role/create-DB rejection.
4. Run 30-minute wall soak with samples/restarts, then multi-hour and 24-hour
   orchestration. Current CLI is bounded to one hour/profile; longer restart-aware
   orchestration needs implementation. Review resource trends, pending ownership,
   Redis lag/outbox age, database growth and independent accounting throughout.
   API illustrative cycle/order samples are capped at 500; whole-ledger sample
   and final independent counts are not truncated.
5. Validate accessible current official Bybit public metadata/session/filter
   behavior separately and browser responsive/polling/error rendering.
6. Re-run complete suites on the eventual final implementation head and keep
   draft while acceptance is blocked. No merge is authorized.

Private Bybit integration, exchange-confirmed fills, live balances, live trading
authorization, production security certification and long-duration operational
stability remain unverified. `ARB_LIVE_TRADING_ENABLED=false` throughout.

## G. Reproduction and delivery

Code and communication are on PR #9. Evidence is reviewable in this repository;
the delivery is not merge-ready. Use disposable databases/services, unset all
private exchange credentials, and distinct test control/internal credentials.

```sh
cargo fmt --manifest-path rust/Cargo.toml --all -- --check
cargo clippy --manifest-path rust/Cargo.toml --workspace --all-targets --all-features -- -D warnings
cargo test --manifest-path rust/Cargo.toml --workspace
cargo build --manifest-path rust/Cargo.toml --workspace
# With ARB_REDIS_URL pointing to disposable Redis:
cargo test --manifest-path rust/Cargo.toml -p event-bus -- --ignored --test-threads=1
python scripts/check_profitability_parity.py
cd python
python -m pip install -e ".[dev]"
python -m ruff check .
python -m alembic upgrade head
python -m pytest -q
cd ../frontend
npm ci
npm run lint
npm test
npm run build
```

For all Python integration categories set ARB_RUN_PAPER_INTEGRATION=1,
ARB_RUN_GRPC_INTEGRATION=1, ARB_RUN_RELIABILITY_INTEGRATION=1,
ARB_RUN_OBSERVER_INTEGRATION=1, ARB_ENV=development and live=false. Configure
ARB_DATABASE_URL and a separate disposable TEST_DATABASE_URL, ARB_REDIS_URL,
ARB_ENGINE_BINARY (absolute built engine-service path), distinct strong
ARB_CONTROL_API_TOKEN/ARB_ENGINE_GRPC_TOKEN and the gRPC control target. Start an
observer-disabled control engine with matching token/target, isolated writable
control/limit/reload/idempotency/outbox paths and an ARB_ENGINE_PID_FILE containing
its actual PID. The restart test uses that PID. Set
ARB_TEST_GRPC_RESPONSE_DELAY_MS=100 for the ambiguous-response test. Paper and
observer fixtures start their own actual processes. This control engine must
have live=false and remain observer-disabled.

After stopping that control engine, clear response-delay/fault hooks. With explicit
development paper authorization and migrated disposable services, run:

```sh
python scripts/verify_paper_migrations.py
python scripts/run_paper_soak.py --seconds 60 --latencies 50 --sample-seconds 10 --output /tmp/paper-soak.json
# Longer bounded next step (not a claimed executed result):
python scripts/run_paper_soak.py --seconds 1800 --latencies 50 --sample-seconds 30 --output /tmp/paper-soak-30min.json
```

Never point migration/fault/soak tests at a production database or use real trading
credentials. Public observation defaults to wall time; fresh development paper
fixtures default to virtual time. The soak explicitly selects wall time.
