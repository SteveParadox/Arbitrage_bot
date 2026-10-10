# PR #2 critical audit integration — 2026-10-10

## Integration status

- Repository: SteveParadox/Arbitrage_bot; target: main.
- Original PR: #2, `audit/end-to-end-2026-10-01`, head `19cd13a9ae49dc916c9c1e51139bf31ea96a7316`.
- Inspected main and integration base: `26ad6f2a503703b5b4f38b3b88b8a262b75b8c5b` (re-fetched after implementation; unchanged).
- Original merge base: `f17ef7dab86f3342540bbaa08e10376629979db8`; PR contains 20 commits, 12 changed files, 423 additions and 52 deletions. The complete patch, history, comments, reviews and checks were inspected. No review submissions or outstanding discussion comments were present. The draft PR is conflicted against current main.
- Replacement branch: `audit/pr2-critical-integration`, built from latest main; no collaborator branch was rewritten.
- PR #2 is superseded by the replacement implementation. Its branch and review history are preserved pending the replacement merge.
- Merge: **not performed**. There is no merge SHA. GitHub Actions on the inspected main failed before any steps ran: “The job was not started because your account is locked due to a billing issue.” Main workflow run: [37336708566](https://github.com/SteveParadox/Arbitrage_bot/actions/runs/37336708566).
- Resolve the GitHub billing lock, rerun every check on the replacement head, inspect any CI failures and current main changes, and satisfy the remaining merge gates before merging. Local success does not substitute for GitHub checks. No branch-protection or security check was bypassed.

## Correction matrix

| Correction | Existing main / original PR condition | Implemented correction | Evidence | Final local status |
|---|---|---|---|---|
| Malformed control state | Risk reader accepted incomplete enabled records; PR added typed fields but no expiry/shared stop state | Shared strict Rust/Python schemas; reject duplicate keys, invalid fields, sources, IDs, versions, future timestamps and oversized records; enabled authorization expires; missing/unreadable state disables; retain invalid file for diagnosis; restart requires explicit activation | Rust control/risk tests, Python critical-control tests, real restart boundary tests | Passed; default disabled |
| Emergency-stop uncertainty | Disabled fallback written after RPC; incomplete proof of actual engine state; PR corrected error wording only | Durable disabled intent before dispatch; OS lock shared with Rust; operation IDs and existing durable gRPC dedup retained; fresh independent engine status plus matching engine-authored disabled record required; pending/unknown stop blocks start; safe retry and status-based recovery | Timeout/lost-response/concurrent-start tests, newer-stop protection, recovery proof tests, real gRPC duplicate/restart suite | Passed; exposure is never claimed flat |
| Market-data monitoring | Opportunity activity proxy; socket connection not sufficient; PR still used that proxy | Subscription acknowledgements, required snapshots, per-symbol synchronization and exchange/receive clocks; market.health Redis events; reset on reconnect; expiring health required by live scanner; existing age/skew/sequence gates retained | Market-health and scanner Rust tests; Python missing/stale/unsynchronized/disconnected telemetry tests; captured-book scanner/paper tests | Passed with recorded/mocked data |
| Engine-health monitoring | Inconsistent dashboard/API eligibility; incomplete dependency checks | Canonical bounded health aggregation of DB, fresh authenticated gRPC status, Redis group/backlog, persisted consumer heartbeat, outbox, command store, risk, runtime state, operator auth and stop latch; separate operational/trading states | Partial-failure tests, real Rust/Redis/PostgreSQL boundary health test | Passed locally |
| Micro-live authentication | Mutation public; PR added existing bearer dependency | Existing >=32-byte operator bearer required server-side; no query credentials; finite DB-bounded amounts, IDs and request sizes; bounded DB statements/locks; sanitized audit diagnostics | HTTP missing/invalid/wrong-scheme/query-token tests and authenticated boundary tests | Passed; existing operator-secret privilege model preserved |
| Reconciliation locking | Unlocked read/update; PR cycle and run row locks but no commit/retry fingerprint | PostgreSQL cycle/run row locks, atomic updates, transaction-local timeouts; additive digest and unique request-ID migration; identical retries return stored result; conflicting/replayed requests reject; rollback on failure | Six real multi-session PostgreSQL contention, replay, lost-response, timeout and rollback tests | Passed; no double increment or P&L update |
| API response contracts | Frontend expected obsolete control object and boolean success; dashboard inferred eligibility separately | Canonical Pydantic safety responses exported to shared JSON schemas; frontend Ajv runtime validation and consistency checks; nullable uncertain outcomes; shared unknown/unconfirmed display | Schema-export tests, backend tests, five frontend schema/component tests, TypeScript/build | Passed |
| PostgreSQL exposure | Development host port exposed on every interface; no migration startup gate | Bind 127.0.0.1; dedicated migration service and API dependency; private Compose service connectivity preserved; duplicate-key YAML/config regression test | Parsed Compose regression and actual PostgreSQL migration upgrade/downgrade/reupgrade | Passed structurally; Docker daemon not exercised |

## Disposition of original PR changes

All remaining valid intentions are retained: strict runtime validation, truthful uncertain stops, engine-event monitoring, privileged reconciliation, cycle/run locking, command response typing, localhost PostgreSQL and migration-before-API startup. Current main already contains the PostgreSQL-backed Rust/Python CI boundary, durable event outbox, durable gRPC idempotency/status recovery, runtime risk-limit validation and consumer idempotency/dead-letter handling. Those newer implementations are retained rather than replaced by the older PR versions. Historical fixed-date control fixtures are replaced with valid current timestamps. We do not merge duplicate implementations just to close #2.

## Additional defects corrected

| Severity | File / function | Root cause and potential impact | Correction and regression evidence |
|---|---|---|---|
| High | python/api/micro_live.py::_digest | Decimal.normalize uses ambient precision; distinct 38-digit financial payloads could share a fingerprint | Exact bounded decimal serialization; test distinguishes values after digit 28 and preserves equivalent retries |
| High | rust/execution/src/client.rs::list_executions / build_state | Timestamp-first adjacent dedup did not validate conflicts for one execution ID; cumulative fill used >= and pagination exhaustion could silently truncate | Canonical execution-ID map; identical replay counted once; conflicting timestamp/fee/quantity and wrong order rejected; pagination exhaustion and excess fill remain unconfirmed; Rust regression tests |
| High | python/api/operations.py::dashboard | Dashboard reported trading enabled from two flags without mandatory dependency health | Reuse canonical health; dependency failure tests and frontend unknown display |
| High | rust/scanner/src/bin/scan_live.rs::main | Last healthy feed record could remain authoritative indefinitely while newer books arrived | Health timestamp/ack validation and receipt expiry using scanner max_book_age_ms; Rust stale/unknown/ack regression test |
| Medium | python/api/operations.py::_risk_status / _risk_config_path | Alternate configured risk path ignored; malformed scalar/history/breaker could crash or appear ready | Honor configured path and reject malformed state; Python dashboard/health tests |
| Medium | rust/shadow/src/lib.rs / bin/shadow_live.rs | Missing public account exports and malformed bail invocation prevented workspace compilation | Restore exports and valid message formatting; workspace build/tests and strict Clippy pass |
| Medium | python/analytics/performance_analytics.py::_load_cycles | Mixed timezone-naive and aware dates caused comparison errors | Normalize sort timestamps to UTC; existing analytics regression passes |
| Medium | PostgreSQL test fixtures | One suite dropped all shared tables; another inserted FK children before parents | Isolated opportunity schema, flush parent rows before children; whole PostgreSQL-enabled Python suite passes |
| Medium | docker/docker-compose.yml (integration edit) | Migration insertion initially duplicated service/dependency keys | Correct structure; unique-key YAML loader plus exact dependency assertions catches recurrence |

Rustfmt-only coordinator changes satisfy the repository-wide formatting gate. Cargo.lock and frontend/package-lock.json provide reproducible dependency resolution. Generated gRPC bindings had one unused import removed for the existing Ruff gate.

## Executed verification

Validation used Python 3.12.14, stable Rust 1.99.0, Node tooling, PostgreSQL 16.15 and Redis 7 in isolated disposable local services. Live deployment was disabled; no Bybit credentials, exchange orders or real funds were used.

| Check | Actual result |
|---|---|
| cargo fmt --all -- --check | Passed |
| cargo clippy --workspace --all-targets --all-features -- -D warnings | Passed |
| cargo test --workspace | 78 passed, 0 failed; two Redis tests ignored by default, then separately executed successfully |
| cargo build -p engine-service -p scanner | Passed |
| Python Ruff | Passed |
| Full pytest with real PostgreSQL | 188 passed, 8 skipped; skipped boundary tests executed separately below |
| Real Rust/FastAPI gRPC + Redis/PostgreSQL boundary suite | 8 passed |
| Real PostgreSQL reconciliation concurrency/retry/rollback | 6 passed, included in full pytest total |
| Redis publisher outage/restart tests (--ignored) | Both passed |
| Alembic empty database -> head; 0007 -> 0006; 0006 -> head | All passed against PostgreSQL 16 |
| Shared backend/frontend API schema exports | Passed in Python suite |
| Frontend npm run lint (tsc --noEmit) | Passed |
| Frontend npm test | 5 passed |
| Frontend npm run build | TypeScript and production Vite build passed |
| scripts/check_profitability_parity.py | Passed |
| Compose duplicate-key/schema structure regression | Passed |
| Git diff whitespace/conflict check | Passed |
| GitHub CI | Blocked/failed before execution on inspected main by account billing lock; replacement result tracked in PR |

A FastAPI/Starlette httpx deprecation warning remains; it did not fail tests. No failing local checks remain at publication.

## Lifecycle coverage and limits

Captured snapshot/delta data exercises book initialization/sequence integrity, triangle validation, Rust opportunity scanning, fee/depth calculations, PostgreSQL opportunity persistence and paper latency/P&L replay. Rust coordinator mock-executor tests cover sequential actual fills, fees, failed later legs, residual exposure and reconciliation/kill-switch escalation. Real service tests exercise command authentication, retries, duplicate IDs, delayed/lost responses, engine restart, durable disabled intent, status verification, Redis delivery, PostgreSQL persistence and health responses. Frontend tests validate unknown/unconfirmed safety rendering.

These are composed component and boundary tests. The repository has no packaged autonomous runner joining every requested stage into one deployed market-data-to-exchange-to-dashboard transaction. A real Bybit WebSocket/testnet session, Docker image builds/Compose startup, load/soak tests, network partition during an in-flight exchange order, and browser-driven live dashboard testing were **not executed**. A simulated pre-commit database disconnect verifies rollback; actual PostgreSQL process death during commit was not exercised. Linux control-lock behavior was tested; Windows cross-language locking was not.

The micro-live endpoint remains manual calibration: source=operator_reported and exchange_confirmed=false. It neither cancels orders nor closes positions nor independently verifies exchange fills. Existing exchange execution polling/reconciliation remains the source of truth for order outcomes; uncertainty preserves kill-switch/investigation behavior. Operator bearer authorization is the existing privileged capability, not a newly invented expiring JWT/RBAC identity system. Production ingress rate limiting, identity lifecycle, TLS/mTLS, durable shared control mounts, backups and monitoring still require deployment configuration.

## Trading readiness

| Stage | Evidence-based assessment / blockers |
|---|---|
| Local development | Applicable local checks pass. PostgreSQL host access restricted; migrations validated. Full Docker startup still untested. |
| Paper trading | Recorded-data scanner/replay and mocked execution safety paths pass. Runtime live permission remains false. Whole-stack deployed lifecycle/soak validation still required. |
| Exchange sandbox | Not validated in this task. Requires sandbox credentials, current instrument-rule validation, testnet partial-fill/order reconciliation and failure recovery tests. |
| Micro-live | Not approved. Manual calibration mutation is protected and concurrency-safe, but operator reports are not exchange confirmation. Sandbox validation, current symbol constraints, capital coordination and operational runbooks remain prerequisites. |
| Production real-money | Not approved. No autonomous production runner; complete exchange-order constraints and atomic shared-capital reservation are not established end to end; private execution uses REST polling; production security/observability and soak testing remain outstanding. |

The eight requested correction areas have local regression evidence. This report does not attest that every broader pre-existing live-trading limitation is resolved or that the platform is production-ready. Neither deployment permissions nor runtime live authorization were enabled by this change.
