# Strict virtual-clock paper replay

Implementation checkpoint, 2026-10-11. This supersedes the historical audit's statement that strict virtual-clock replay is unimplemented. PR remains draft: publishing code is not a production-readiness claim.

## Runtime behavior

Development paper fixtures default to ARB_REPLAY_CLOCK=virtual. Set wall explicitly for real-time soak tests; public observation and gRPC/control tasks remain on wall time. Virtual requires development, an explicit replay file, paper deployment authorization and ARB_LIVE_TRADING_ENABLED=false. No live venue is constructed.

Financial time is Tokio task-scoped across the actual managed observer, scanner, dispatcher, risk engine, coordinator and paper venue. The driver acknowledges each applied market frame, drains current financial database work to a known IOC timer, and advances only to the next market event or fill deadline. All zero-delay frames at an instant are applied before approval or an IOC fill at that instant. Database delays and OS scheduling do not advance financial time.

The optional ARB_REPLAY_EPOCH_MS overrides the first event's timestamp. delay_ms is cumulative logical delay; replay_clock=true stamps exchange/health timestamps at logical receive time; false preserves supplied timestamps for freshness fault injection. wait_for_paper_leg requires a real committed pending intent. It does not fabricate liquidity. Replay limits: 64 MiB, 100,000 frames, 2 MiB/line, 60 seconds/frame, 24 hours total delay, 1024 drain iterations. Operational watchdogs retain wall time.

Financial SQL event/ledger/intent/fill/cycle/valuation timestamps, cooldowns and daily-loss queries use the same clock. Heartbeats, authenticated start/stop, control-file freshness, shutdown and database timeouts intentionally use wall time. Opening funding and externally submitted controls are operational events, not deterministic market fixture events.

EOF disables execution, reconciles PostgreSQL, requires no pending financial work, marks final valuations using available evidence and records paper.replay_finished with fixture SHA-256 and logical end time. API and dashboard label virtual time; marks without an audited logical reference are not treated as fresh wall-clock prices.

Account clock mode is persisted in existing JSON config and immutable. Restart restores committed financial time, applies existing fenced recovery, disables spending and quarantines historical replay rather than replaying liquidity over existing fills. Use a fresh account for a repeat. No schema migration is required. ARB_PAPER_STATE_DIR allows account-isolated persistent risk/control files.

## Validation status

This checkpoint was reconstructed after the temporary environment reset twice. The prior local reconstruction passed format, strict Clippy, 106 Rust tests and 18 frontend tests; those results are historical and are not a final test attestation for this published checkpoint. Its first financial integration run passed 51 tests and failed four new tests: three decimal-string scale assertions and one refresh-fixture trigger error. The assertions now compare exact Decimal values, and the trigger explicitly references the book before metadata refresh. A rerun was interrupted by the environment reset; no result is claimed.

The new regression suite covers repeated profitable/loss/partial replays with pg_sleep order delays, 25/50/100/200/500ms logical latency, crash quarantine, malformed schedules, same-time market/fill ordering and 310 logical cycles with metadata refresh. Final validation results will be recorded separately.

## Reproduction

Use isolated migrated PostgreSQL and Redis and a built Rust engine. Keep all exchange credentials unset.

```sh
cargo fmt --manifest-path rust/Cargo.toml --all -- --check
cargo clippy --manifest-path rust/Cargo.toml --workspace --all-targets --all-features -- -D warnings
cargo test --manifest-path rust/Cargo.toml --workspace
cargo build --manifest-path rust/Cargo.toml --workspace
cd python
python -m pip install -e ".[dev]"
python -m ruff check .
ARB_ENV=development ARB_TRADING_MODE=paper ARB_PAPER_TRADING_ENABLED=true ARB_LIVE_TRADING_ENABLED=false ARB_RUN_PAPER_INTEGRATION=1 python -m pytest -q tests/test_virtual_paper.py tests/test_autonomous_paper.py tests/test_paper_audit.py
```

Also provide ARB_DATABASE_URL, ARB_REDIS_URL and separate strong ARB_CONTROL_API_TOKEN/ARB_ENGINE_GRPC_TOKEN. Do not use production accounts or services.

## Remaining work in order

1. Validate the exact published clock head, resolve all failures and publish independent persisted traces.
2. Extend and run the fault matrix: every leg/transaction crash boundary, cancellation/fill races, concurrent recovery, real disk-full/outbox failure, reordered messages and infrastructure outages.
3. Restore/validate production security hardening: tenant-scoped viewer/operator/admin permissions, distributed fail-closed limits with emergency-stop availability, bounded requests, secret files, restricted PostgreSQL roles, host/origin/TLS controls and non-root deployment. Those earlier unpushed changes were lost; they are not delivered by this clock checkpoint.
4. Start the real Docker stack; verify migrations, persistent state paths, health checks, graceful shutdown and restart under active cycles. Validate public Bybit metadata/session separately.
5. Implement persistent soak diagnostics and periodic independent whole-ledger checks; run measured real-time 30-minute, multi-hour, then 24-hour profiles with active restarts, resources/backlog/accounting recorded. Accelerated logical time is not wall-time stability evidence.
6. Rerun full Rust/Python/frontend, PostgreSQL/Redis/gRPC reliability, migrations and browser checks on final PR head. Keep draft while critical/high findings or acceptance blockers remain.

Real private Bybit execution, exchange-confirmed fills, live balance reconciliation and production stability are not verified. Live trading stays disabled; no merge is authorized by this task.
