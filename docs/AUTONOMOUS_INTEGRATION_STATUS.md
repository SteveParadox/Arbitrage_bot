# Autonomous engine integration: initial read-only supervisor (draft)

This branch is a **partial implementation**, not the requested full autonomous trading engine.
It provides one in-process Rust flow:

`Bybit public V5 stream -> market-data connector -> synchronized books ->
 existing incremental scanner -> scan journal + best-effort Redis events + NDJSON output`

**It cannot submit orders.** The binary has no dependency on the exchange order client,
the three-leg coordinator, or capital reservation. Nothing here authorizes real-money use.

## Repository audit at implementation time (2026-10-10)

| Component | Existing | Integrated by this branch | Outstanding |
|---|---|---|---|
| Bybit market data | Rust market-data | Yes | Live connectivity not tested |
| Book synchronization | Connector and orderbook crate | Yes | Recovery under real disconnect not tested |
| Incremental scanner | scanner crate | Yes | Live replay not tested |
| Depth/fee profitability | scanner crate | Reused | Executable exchange constraints not verified |
| Risk engine | risk crate | No | Required in execution path |
| Atomic capital reservation | Not identified as complete | No | Durable, cross-instance reservation required |
| Three-leg coordinator | coordinator crate | No | Wire after safety approval/reservation |
| Bybit order execution | execution crate | No | Order/fill tracking and unknown-outcome recovery |
| PostgreSQL reconciliation | Existing Python services | No | Automatic cycle-to-fill/reconcile integration |
| P&L accounting | Existing partial analytics | No | Confirmed fill-based realized P&L |
| gRPC controls | engine-service | No | Must share one authoritative execution controller |
| Frontend monitoring | React dashboard | Existing | Authoritative execution state not yet connected |
| Restart recovery | Partial existing controls | No | Exchange order discovery before resuming |

PR #6 containing critical PR #2 integration corrections was merged into main on
2026-10-10. Original PR #2 remains open and should not be merged blindly.

## Running the observer

Generate a current catalog of directed triangles first; the repository's committed
`shared/config/triangles.json` is intentionally empty.

```bash
cd python
python -m strategy.triangle_discovery --start-assets USDT
cd ..
export ARB_TRADING_MODE=observe
export ARB_LIVE_TRADING_ENABLED=false
export BYBIT_TESTNET=false
export BYBIT_MARKET_CATEGORY=spot
export BYBIT_ORDERBOOK_DEPTH=50
# Configure Redis/outbox via the existing .env configuration.
cd rust
cargo run -p scanner --bin auto-observe
```

The environment must match the generated triangle catalog. To use testnet public
market data, use a testnet-generated triangle catalog and set BYBIT_TESTNET=true.

On startup, the runner obtains an exclusive lock, verifies configuration and
required routes, subscribes to the required symbols, and waits for subscription
acknowledgements, synchronized books, fresh exchange timestamps, and active
instrument metadata. It emits candidate observations only after readiness.
On reconnection the scanner resets its books. Unexpected termination of the
connector or failure to write the scan journal terminates the runner.
Ctrl-C aborts the feed task and releases the observer lock.

Records are **opportunity estimates**, not fills or achieved profit. They are
written using the existing scanner record path and published as Redis telemetry.
Redis telemetry is best-effort; do not use these observations as an execution ledger.

This binary refuses PAPER, TESTNET, MICRO_LIVE and LIVE trading modes and refuses
deployment with ARB_LIVE_TRADING_ENABLED other than the literal `false`.
It does not change, start, or authorize production trading controls.

## Checks to run

```bash
cd rust
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo run -p scanner --bin auto-observe
```

The final command requires a nonempty route catalog and network/Redis access;
it is a smoke test, not evidence that live trading is safe.

**Verification status:** New Rust unit tests were added for mode rejection and
market-health gates. Neither cargo nor a running Bybit/Redis environment was
available in the assistant's container to execute them. GitHub Actions on main
showed jobs failing before any steps executed. No end-to-end test has passed for
this branch. This change should remain a draft PR pending independent compilation
and review.

## Remaining implementation gates, in dependency order

1. Build a single authoritative execution-control supervisor, not a second
   independently permissioned engine. Preserve the merged PR #6 safety rules.
2. Add venue metadata/precision validation and a Decimal-based executable
   preflight; prohibit reuse of estimated fills as real balances.
3. Add durable, transactionally isolated reservations and account-scope fencing
   so multiple processes cannot double-spend funds.
4. Connect risk approvals and current trading controls to the coordinator.
5. Track deterministic order IDs, actual fills, partial fills, uncertain
   submission outcomes, cancellations, and exchange-confirmed balances.
6. Reconcile every fill idempotently to PostgreSQL and calculate actual realized
   P&L separately from residual/unrealized exposure.
7. Add restart and failed-leg recovery and circuit breakers; halt on uncertain
   exposure or missing critical persistence.
8. Wire authoritative state into gRPC, FastAPI, and React.
9. Pass paper replay and full runtime integration, testnet, safety/concurrency,
   and security tests, then apply a separately authorized micro-live gate.

Do not upgrade this PR from draft or claim production readiness based on an
observer's ability to detect attractive-looking price differences.
