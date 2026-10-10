# Autonomous trading engine audit: evidence and merge gate (2026-10-10)

## Scope and provenance

Repository: `SteveParadox/Arbitrage_bot`. Latest autonomous integration is **draft PR #8**, `codex/autonomous-observation-integration`. Its recorded base is the merged PR #6 main commit `0d2803cb9af97d972dc5c6a284f8534efb555fc7`. The original PR #2 is open, conflicted, superseded and **not** an eligible merge source. PR #7 is a parallel read-only observer draft, not an autonomous execution implementation.

Evidence tiers are separated:
1. **Direct code inspection / connector read**: current Rust observer and scanner, Python consumer and observer health, PostgreSQL event model, Docker startup and existing risk/coordinator/execution entry points.
2. **Prior PR-author-reported tests**: PR #8 reported fmt, Clippy, workspace tests, Python 189 passed/8 skipped, frontend build, and public-data replay through Redis/PostgreSQL. These reports describe the *pre-audit* PR revision; they were **not reproduced for the audit commits**.
3. **Current GitHub CI**: run 38031565148 on audit head `5fd7756835afa655da49f1df6eefeb9f4cc44c10` showed all six jobs failed with no executed steps; prior runs documented a GitHub account billing lock. Green CI is unavailable.
4. **Unavailable execution environment**: this audit environment has no Rust/Cargo, Docker, Redis or PostgreSQL service, and cannot clone GitHub over the network. It cannot legitimately claim full Rust, database, exchange, container or end-to-end tests passed.

No production orders were sent, no live trading was enabled, and no financial results were produced in this audit.

## Actual entry points and component map

| Component | Inspected implementation | Runtime integration / evidence | Gate |
|---|---|---|---|
| Autonomous supervisor | `rust/engine-service/src/main.rs` spawns `observer::run` behind `ARB_ENGINE_AUTOSTART` | Managed **observation only**; exits if observer errors | Paper observation replay still requires current-green CI |
| Bybit public market data | `rust/market-data/src/connector.rs::run`, `run_connection` | Used by `observer::run` | Live connection/reconnect not reproduced |
| Order books | `orderbook::OrderBookEngine`, `scanner::ArbitrageScanner::on_book_update` | Used in observation route | Book synchronization and time-skew tests exist; audit revision not run |
| Triangle discovery | `strategy.triangle_discovery`, `shared/config/triangles.json` | Catalog **committed empty** | Must generate versioned catalog before ordinary startup |
| Scanner / profitability | `rust/scanner/src/engine.rs::ArbitrageScanner` and profitability config | Emits observational records, NOT orders | Current Bybit trading filters must be verified |
| Execution risk | `rust/risk/src/engine.rs::RiskEngine` | Standalone module, **not in observer dispatch** | Mandatory execution gate not integrated |
| Capital reservation | No atomic, cross-instance reserve-and-consume integration identified | **Missing from autonomous path** | CRITICAL |
| Three-leg coordinator | `rust/coordinator/src/coordinator.rs::ThreeLegCoordinator::execute_route` | Standalone, not called by `observer::run` | CRITICAL |
| Bybit execution | `rust/execution/src/lib.rs::BybitExecutionClient`, `prepare_execution` | Standalone, no call from observer | No automatic Bybit orders |
| Fill recovery | Existing coordinator/execution methods | No full private-fill-to-reservation recovery connection | CRITICAL |
| Durable events | `event_bus::EventPublisher`, `python/api/event_consumer.py::_persist` | Observer candidates accepted to outbox and PostgreSQL | Verify conflict handling in PostgreSQL |
| Reconciliation | `python/api/micro_live.py::reconcile_cycle` | Authenticated *manual* reporting, not exchange-confirmed autonomous reconciliation | CRITICAL |
| P&L | `python/analytics/micro_live_analytics.py` and existing trade records | No demonstrated fill-based autonomous final accounting | CRITICAL |
| FastAPI monitoring | `python/api/health.py::_observer_status`, `health_snapshot` | Observer telemetry exposed | Do not equate observation with execution authorization |
| React | `frontend/src/App.tsx` and `frontend/src/api.ts` | Displays observation state | No authoritative autonomous execution states |
| Docker | `docker/docker-compose.yml` | Starts control engine; observer disabled by default | No autonomous execution service |
| gRPC control | `rust/engine-service/src/main.rs` | Existing control commands; not an automatic execution supervisor | Mode control is not fill recovery |
| Secrets and exposure | Compose binds PostgreSQL/Redis to loopback; live defaults false | Partial defensive posture | Exchange credentials/control deployment not validated |

## Confirmed corrections committed on PR #8

**H-01: Conflicting event identities silently suppressed by PostgreSQL ON CONFLICT.** File `python/api/event_consumer.py`, method `_persist`. The insert suppressed conflicts on *either* the primary event ID or unique Redis stream ID, then counted any zero-insert as an ordinary replay. Different events could be dropped without audit trace. **Fix:** inspect the existing row and compare the immutable event ID, event type, source, schema version, occurred-at timestamp and payload. Allow identical event-ID redelivery (including a distinct Redis stream position); reject mismatched identities to the established dead-letter flow. **Regression added:** parameterized `test_conflicting_event_identity_is_rejected_not_silently_dropped` in `python/tests/test_event_idempotency_integration.py`, covering event-ID and stream-ID collisions, and preserving the stored record. **Status:** committed; PostgreSQL integration tests NOT EXECUTED on this revision.

**M-02: Observer readiness relied on a top-level connected-and-fresh flag.** File `rust/engine-service/src/observer.rs`, helper `feed_ready`. A malformed/contradictory health message could claim readiness without showing all required books synchronized or satisfying exchange and receive-time freshness. **Fix:** require the configured symbol set, per-symbol initialized and synchronized flags, valid exchange timestamp, receive-age clock and bounded telemetry age, with checked time subtraction/addition. **Regression strengthened:** missing symbol, stale exchange timestamp, stale receive clock, unsynchronized book, future and expired health, and unconfirmed subscriptions. **Status:** committed; Rust tests NOT EXECUTED on this revision.

## Outstanding blocking findings

**CRITICAL: no end-to-end autonomous execution lifecycle.** The top-level managed observer does not call the risk engine, atomic reservation, three-leg coordinator or Bybit execution client. Presence of these crates is not evidence of connectivity. Consequently, no authentic capital reservation, partial-fill recovery, restart reconciliation, final P&L or automated release can be demonstrated.

**CRITICAL: fail-safe live order authorization, capital reservation and uncertain-order recovery not demonstrated.** No real exchange order was attempted. A safe integration needs durable per-trade state prior to side effects, stable Bybit client order IDs, authoritative status lookup before retries, per-asset atomic reservations and cross-instance ownership. These are mandatory before paper execution can represent intended production behavior.

**HIGH: private fill reconciliation and accounting incomplete.** Manual micro-live reconciliation is not an exchange-truth source. No evidence for full 3-leg confirmed-fill ledger with fee currency, dust, residual asset valuation, rollback and delayed private fills.

**HIGH: public instrument metadata refresh on reconnect requires work.** `market-data::connector::run` calls `fetch_and_emit_instruments` once before entering the WebSocket reconnection loop. Exchange filters can change while the service remains alive. Revalidation should occur before a future order is authorized, and reconnect/periodic metadata freshness must be explicitly tested. Current Bybit V5 instrument docs state spot filters and market quantity limits; see https://bybit-exchange.github.io/docs/v5/market/instrument .

**HIGH: runner and multi-instance safety not verified.** PR #8 is read-only and has no atomic reservation or authoritative multi-engine execution lease. A file lock in separate draft PR #7 is not a substitute for cross-host transaction ownership.

**HIGH: CI unavailable.** All required CI jobs must execute successfully on the *final* commit. Jobs failing before startup are not successful tests. An account billing restriction has been recorded in prior runs. Do not bypass this gate.

**MEDIUM: startup catalog missing.** `shared/config/triangles.json` contains zero routes. Ordinary observation autostart will refuse to start until a real environment-matched catalog is created and reviewed.

## Failure-injection and financial test matrix

| Scenario | Current audit verdict |
|---|---|
| Top-level subscriber/observer replay | Prior PR reported a synthetic accepted and rejected observation; not rerun on audit head |
| Malformed health flags / stale symbol clocks | New Rust tests committed; not executed |
| Event-ID and Redis-stream-ID collision | New PostgreSQL regression tests committed; not executed |
| Stale/reordered book and synchronized snapshot gating | Existing scanner tests noted; not rerun |
| Duplicate order after uncertain Bybit timeout | Not demonstrable in current supervisor |
| 10/50/100 competing capital reservations | Not demonstrable; authoritative reservation absent |
| Partial leg fill, leg 2/3 rejection, failed unwind | Not demonstrable through runtime entry point |
| Duplicate private fill, missing fill, failed P&L write | Not demonstrable through runtime entry point |
| Crash after exchange acceptance / restart recovery | Not demonstrable through runtime entry point |
| Kill-switch racing execution | Existing control logic; no autonomous order dispatcher to test |
| Two independent engine instances trading | No cross-instance execution owner |
| UI error and observer-only status | Frontend branch includes observer display; current tests not rerun |
| Full actual-entrypoint paper cycle with PostgreSQL/Redis/API/UI | **NOT PROVEN** |
| Sustained operation, actual latency and resource measurements | **NOT MEASURED** |

## Reproducible validation commands (NOT executed for audit head)

```sh
cargo fmt --manifest-path rust/Cargo.toml --all -- --check
cargo clippy --manifest-path rust/Cargo.toml --workspace --all-targets --all-features -- -D warnings
cargo test --manifest-path rust/Cargo.toml --workspace
cd python && python -m pytest tests/test_event_idempotency_integration.py -q
cd python && python -m pytest -q
cd frontend && npm ci && npm run lint && npm run build
```

The PostgreSQL integration test requires the repository's `ARB_RUN_RELIABILITY_INTEGRATION=1` configuration and a migrated PostgreSQL database. Verify actual commands and CI environment from the repository workflow before execution. Run the destructive exchange failure matrix only with a deterministic paper adapter. Never use production funds/credentials for this audit.

## Financial evidence and performance

Opportunities detected/rejected/approved/executed: **not measured in this audit**. Prior PR #8 describes a synthetic replay with one accepted and one rejected observation, not executed trades. Filled amounts, realized P&L, residual exposure, outstanding orders, financial parity, scanner/risk/reservation/order latency, CPU/memory and backlog: **not measured or not available**. Zero real orders were intentionally placed.

## Readiness and final merge decision

- **Local code-level observation development:** Partially implemented; outstanding regression execution.
- **Observation-only paper replay:** Prior PR reports passing replay, but audit head still needs repeatable CI and service-run confirmation.
- **End-to-end autonomous paper trading:** **BLOCKED**, missing execution chain and capital/recovery authority.
- **Bybit testnet order execution:** **BLOCKED**, not wired to supervisor or validated.
- **Micro-live:** **BLOCKED**, unresolved capital, ordering, fill and restart guarantees.
- **Production real-money:** **BLOCKED**, no safe merge basis or operational soak evidence.

**Decision: do not merge PR #8** under the requested full autonomous-engine safety gate. Leave draft open; retain PR #6 corrections already merged on main; do not merge superseded PR #2. After completing integration, reproduce the full real-entrypoint paper lifecycle, independent accounting, failure injection, restart tests, security checks and green CI before considering any merge. A merge would still not authorize live trading.
