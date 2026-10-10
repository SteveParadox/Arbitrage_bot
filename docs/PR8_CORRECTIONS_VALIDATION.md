# PR #8 observation corrections and local validation

## Repository and scope

Inspected main: `0d2803cb9af97d972dc5c6a284f8534efb555fc7` (merged PR #6).
Original PR #8 head: `28db2d8dd329c64eb21ac32c8cb60f018226927b`,
branch `codex/autonomous-observation-integration`.
PR #7 head: `1eca97f7b69c531d36f4bf3efeafc4900e971fbc`.
Legacy PR #2 head: `19cd13a9ae49dc916c9c1e51139bf31ea96a7316`.

One managed `engine-service` observer remains authoritative. PR #7's exclusive
instance lock, strict observation/live-disabled checks, depth validation,
public order-book-only subscriptions and shutdown cleanup were incorporated or
were already present. Its separate executable and weaker best-effort journal
were not imported. PR #6's control authentication, stop intents, risk controls,
command idempotency and reconciliation transactions remain authoritative.
Legacy #2 is superseded by #6; its older health and control implementations are
not applied. GitHub Actions billing failures are not used as local test evidence.
No workflow, protection rule or execution permission was loosened.

## Corrected findings

| Severity | File/function | Root cause and reproduction | Correction | Regression evidence |
|---|---|---|---|---|
| High | `rust/engine-service/src/observer.rs`, `candidate_event`; scanner `on_book_update` | Replaying the same trigger retained candidate ID but changed scan/envelope time. Trigger-only identity omitted other books/configuration. | Identity v2 includes strategy/configuration, route, all book versions and instrument filter configuration. Immutable market timestamps; explicit processing audit field. Full price/depth fingerprint remains strict. | Actual process restart retains both IDs and exactly two ledger observations; changed market/profit/direction/time conflicts reject. |
| High | `python/api/event_consumer.py`, `_persist` | `limit(1)` could hide a second stream-ID collision; driver rowcount could be unreliable. | `INSERT RETURNING`, inspect every row matching either identity, reject ambiguous/mutated events; event and candidate persist in one savepoint/transaction. | Same/different stream delivery, dual-row collision, concurrent delivery, rollback/retry and conflicting payload cases. |
| High | `rust/event-bus/src/lib.rs`, `Outbox` | Same ID could produce repeated files/publications; another publisher could race writes/delivery. Worker could see renamed files before directory fsync. | Per-process publisher sharing, exclusive outbox process lock, serialized durable writes, retained identity receipts, deterministic envelope timestamps and strict conflict checks. Publisher enumeration waits for durable acceptance. | Concurrent duplicates, pending/delivered/restart duplicate tests, outbox write failure and real Redis restart recovery. |
| High | event bus `publish_to_redis` | Approximate MAXLEN trimming could remove unconsumed/uncommitted critical events, including when telemetry used the stream. | Atomic Lua append/capacity check; safe MINID boundary across all consumer groups and pending entries. Unsafe/full streams retry from disk. | Real Redis backlog saturation retains pending events and resumes after acknowledgements. |
| High | connector `run_supervised`, observer lifecycle and scanner reset | Scanner book reset left the connector emitting deltas without new snapshots. | Bounded recovery request channel cancels/reconnects the connector with exponential bounded backoff. Clear metadata/books; reject deltas until fresh snapshots for all route instruments. | Invalid/regressed/crossed/locked/negative-level fault tests; real local mock WebSocket reconnection; actual managed-generation replay recovery. |
| Medium/High | connector metadata and `Lifecycle` | Cached metadata survived reconnect; clearing it on connected would erase just-fetched metadata. | Generation starts before fetch. Fetch complete snapshot before emitting any metadata; reject old generations; connected acknowledges without clearing fresh metadata. Check instrument assets/status/positive filters/freshness and request a fresh generation when metadata expires during a healthy connection. | Atomic missing-instrument fetch test, lifecycle unit tests and replayed late generation messages. |
| Medium | health/contracts/dashboard | Socket/gRPC health did not prove scanner readiness; degraded/reconnecting/failed states were unsupported. | Require connector and scanner books plus receive clocks, conservative heartbeat expiry and typed observer health distinct from execution. Preserve layout. | Backend state/staleness tests, exported schema comparison, eight frontend observer display tests. |
| Medium | `OpportunityStore.record_scan` | Journal key used processing time; conflicting observation keys could silently disappear. | Use candidate ID when provided, reject different immutable raw scans under one key. | PostgreSQL candidate single-effect, conflicting identity and transaction rollback tests. |

## Local validation commands

Executed in an isolated checkout with Rust 1.99, Python 3.12, compatible Node 24,
PostgreSQL 16.15 and Redis 7.0.15. The environment maps only uid 0 and prohibits
Unix sockets/privilege changes: disposable PostgreSQL used TCP loopback and a
test-only UID/ownership shim outside the repository. These tests are not a
validation of production service ownership or Docker deployment.

```
cargo fmt --manifest-path rust/Cargo.toml --all -- --check
cargo clippy --manifest-path rust/Cargo.toml --workspace --all-targets --all-features -- -D warnings
cargo test --manifest-path rust/Cargo.toml --workspace
cargo build --manifest-path rust/Cargo.toml --workspace
cargo test --manifest-path rust/Cargo.toml -p event-bus -- --ignored --test-threads=1
python -m pip install -e ".[dev]"
python -m ruff check .
ARB_RUN_RELIABILITY_INTEGRATION=1 ARB_RUN_GRPC_INTEGRATION=1 ARB_RUN_OBSERVER_INTEGRATION=1 python -m pytest -q
npm ci
npm run lint
npm test
npm run build
```

Service runs use a new, migrated disposable database, real Redis, real Rust gRPC
and observer executables, and the actual Uvicorn API/consumer process. No user
or production database, exchange credentials or real orders are used.

The synthetic replay produces one profitable and one rejected candidate.
Generation two recovers after an invalid book while old-generation messages are
ignored. Restarting the engine/outbox and consumer retains one observation per
candidate. PostgreSQL unavailability leaves candidate records pending in Redis;
recovery commits them and acknowledges them. Conflicts enter the dead-letter
stream. Database observations and HTTP opportunities/health agree; execution
and deployment remain disabled. Synthetic estimates are not realized profits.

Final counts and merge SHA are reported with the task outcome after the final
commit is independently revalidated. Prior successful runs do not substitute
for that validation.

## Limits and operational requirements

This is observation-only. Atomic capital reservation, risk-approved automatic
dispatch, complete three-leg automatic execution, private fill tracking, balance
reconciliation, restart order recovery, exchange-confirmed realized P&L and a
complete paper lifecycle remain separate work. Existing execution primitives do
not establish autonomous readiness.

No live public Bybit connection or exchange order test was performed. Transport
recovery uses a local mock public WebSocket and synthetic event replay. Production
soak, Docker image/startup and security deployment validation remain outside this
local observation correction. Fault tests cover the corrected failure classes,
not every possible exchange/network failure combination.

Keep outbox identity receipts on durable storage and monitor disk growth.
Abandoned consumer groups can deliberately block safe stream trimming; recover
or retire them through an explicit operational procedure. Do not remove receipts
while events can be replayed, or count Redis publication as PostgreSQL commit.
