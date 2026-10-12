# Authenticated execution implementation and review

## A. Repository and delivery

- Inspected main: `df3b077544132b0315ad6ece91c18063395652a8` (merged PR #9).
- Implementation: [draft PR #10](https://github.com/SteveParadox/Arbitrage_bot/pull/10).
- Branch: `codex/autonomous-bybit-execution-runner`.
- Reviewed initial PR head: `4e5e120ff17f561db3c340f2d0712ea87f27b45b`.
- Corrected tested source: `87b2f6634e0419da4c40a6d724aa1549969f4b29` (source tree `f650c807dbfea71b6edd07ce9ab78e46200037d7`).
- A final evidence-only commit records this report and logs without changing tested source.
- Review corrections are commits `db3b590`, `a3af134` and `87b2f66`; changed paths are recorded in [evidence/authenticated-review-20261011/changed-files.txt](evidence/authenticated-review-20261011/changed-files.txt).
- Migration `0011_exchange_execution` adds eight isolated `execution_*` tables,
  account fencing, one active commitment per account, account/candidate and
  market-evidence uniqueness, immutable intents, fill deduplication, movements,
  reconciliation, commands and transactional audit outbox. It does not mutate
  paper history. This review adjusted the unmerged PR migration; it did not add
  another migration revision.

The local scratch environment was reset during the work. The PR branch and a
saved Git tree were recovered; source corrections were reapplied and tests were
rerun. Earlier ephemeral logs are not presented as final validation evidence.

## B. Review findings and corrections

| Severity | File / cause | Reproduction and correction | Regression evidence | Result |
| --- | --- | --- | --- | --- |
| HIGH | `authenticated/ledger.rs::settle`, `exchange_runtime::start`: any recent healthy reconciliation could mask a later discrepancy | Crash after reservation, change the mock wallet, restart immediately. Require the **latest** reconciliation after order evidence; a startup discrepancy preserves the commitment. | `test_startup_discrepancy_cannot_release_recent_reservation` | PASS (offline regression) |
| HIGH | `Ledger::apply`: exchange side and requested quantity were not matched to durable intent | Return the opposite side or an altered quantity. Check side, quantity and filled/remaining bounds before financial writes. | `test_exchange_order_must_match_durable_economic_intent` (side, quantity and out-of-limit price cases) | PASS (offline regression) |
| HIGH | `Ledger::apply`: later evidence could overwrite confirmed fills with a smaller or nonterminal result | Crash after Leg 1; return an empty cancelled record on restart. Reject filled-quantity regression, removed/changed prior fills and terminal/confirmation downgrade. | `test_restart_rejects_regressed_fill_evidence` | PASS (offline regression) |
| HIGH | `Ledger::flush_events`: a retry changed `occurred_at` for the same durable event ID | Crash after event fsync before SQL publication acknowledgement, restart. Use the event's persisted creation timestamp on every retry. | `test_financial_outbox_retry_keeps_the_original_event` | PASS (offline regression) |
| HIGH | `authenticated::Gate`: a missing global control file was treated as clear by the shared paper stop guard | Remove the fixture's global control record. Authenticated new exposure now requires a valid existing record, plus stop/kill checks. | `test_missing_global_control_is_not_execution_permission` | PASS (offline regression) |
| HIGH | `BybitExecutionClient::verify_account`: unrelated trading permissions were accepted | Return a ContractTrade permission alongside SpotTrade. Require exactly SpotTrade and no other nonempty permissions. | `test_account_preflight_blocks_unsafe_exchange_evidence[overprivileged]` | PASS (offline regression) |
| HIGH | Execution history parser ignored additional fee records | Return nonempty `extraFees`. Unsupported additional fees deny confirmation and preserve unresolved capital rather than silently ignoring costs. | `test_uncertain_exchange_evidence_preserves_commitment[extra_fees]` | PASS (offline regression) |
| HIGH | Profitability depended solely on a configured fee rate | Return a higher account fee tier before reservation or after Leg 1. Verify current symbol/account taker fees against the modeled ceiling before reservation and each order; persist evidence. | `test_account_fee_evidence_is_required_before_each_order` (2 cases) | PASS (offline regression) |
| MEDIUM | Execution controls lacked durable market-readiness evidence | Track observer readiness and its timestamp in the account; start requires current synchronized markets. | Autonomous runtime start and missing-control fixtures; API/frontend permission tests | PASS (offline regression) |
| MEDIUM | API mode configuration accepted contradictory flags; frontend accepted malformed/duplicate records | Reject conflicting modes and permission flags; validate decimal strings, record identities and duplicate rows. | `test_conflicting_execution_modes_fail_api_startup`, frontend contract tests | PASS (offline regression) |
| MEDIUM | Paper adverse-movement fixture used scheduler timing before reservation | Trigger movement after the first persisted leg, during modeled latency; preserve assertions for no fill, refunded capital and zero P&L. | `test_submission_price_cap_blocks_adverse_movement_during_latency` | PASS (offline regression) |
| CRITICAL | `ExchangePlanner`, authenticated venue: market slippage used the exchange arrival-time ask, permitting spend beyond local ownership | Ask moves 100→101 after reservation; old market buy spends **100.79940299 USDT** from a 100-USDT cycle. Use explicit Limit IOC, conservative tick rounding, quantity/fee budgets, exact limit quantity metadata and transaction-time capital checks. | `test_buy_price_movement_cannot_spend_unreserved_wallet`; four exact `limit_tests`; final order spends zero and wallet is unchanged | PASS (offline regression) |
| HIGH | `exchange_runtime`: exact map equality treated an absent asset and a zero holding as a discrepancy | Confirm a cancelled Leg 1 with zero fills; coordinator inserts BTC=0 while the ledger has no BTC movement. Compare the union of asset values with absent values equal to zero; retain nonzero discrepancy rejection. | Same end-to-end price-movement fixture plus `zero_fill_asset_keys_are_economically_equal`; reservation released with P&L zero | PASS (offline regression) |
| MEDIUM | Paper shutdown left a fresh heartbeat and exchange status accepted fresh terminal FAILED/STOPPED states | Reproduction produced three failed liveness tests: start accepted after exit/terminal state. Revoke heartbeat/authorization on exit; require READY and display terminal failures accurately. | `test_graceful_shutdown_immediately_revokes_runtime_liveness`; `test_terminal_runtime_state_is_offline_and_cannot_authorize_start` (2 cases); all passed after correction | PASS (offline regression) |
| MEDIUM | Authenticated integration fixtures equated DB READY with synchronized-market readiness | Full run had 372 passes and one start-time 409. READY is written before observer market synchronization. Wait for actual API market, wallet and worker start gates, preserving the 200 assertion after readiness. | `wait_execution_ready`; startup-discrepancy regression passes without weakening production gates | PASS (offline regression) |

All listed financial and recovery regression fixtures passed in the focused
authenticated suite. Full regression validation is recorded in section F.

## C. Source integration map and modes

See [the exact connection map and deployment guide](AUTONOMOUS_EXECUTION.md).
The existing managed observer remains the only connector/scanner. Its durable
candidate publication precedes `exchange_runtime::Runtime::dispatch`. The bounded
worker uses the existing `RiskEngine`, `ThreeLegCoordinator`, risk authorizer,
decimal book planner and `BybitExecutionClient`; `authenticated::Venue` adds the
durable intent and account gates around the network boundary.

| Mode | Venue and required authorization |
| --- | --- |
| OBSERVE | No execution worker; live flag explicitly false |
| PAPER | Existing PaperVenue, explicit paper/account authorization, isolated ledger; no private exchange methods |
| TESTNET | Dedicated testnet client/credentials, testnet flag, account/UID permit, authenticated account start, healthy recovery/market/wallet/risk; live false |
| LIVE | Dedicated mainnet client, live flag plus all deployment attestations and explicit mainnet operator authorization; restart disables new exposure |

Offline exchange fixtures use literal loopback, dummy credentials and development
testnet mode. Virtual-clock replay cannot access private exchange methods. No
actual testnet or mainnet exchange order was authorized or submitted in this work.

## D. Capital and independent financial validation

Account row locks and PostgreSQL uniqueness serialize commitments; ownership uses
a session advisory lock plus a monotonically increasing epoch. Separate database
sessions at 2/10/50/100 contenders each admitted one 100-USDT reservation from
100 USDT, rejecting all other attempts. A competing owner and stale epoch were
rejected. A reservation is a local commitment, not an exchange wallet lock.

Confirmed individual execution IDs determine movements, fee assets and actual
post-fee holdings. Repeated identical executions do not add movements; conflicting
execution identity denies processing. Atomic ledger transactions retain inventory
and expected wallet changes. Settlement is idempotent and requires terminal,
confirmed, cash-closed holdings plus the latest healthy wallet reconciliation.

`test_exchange_runtime.py::verify_accounting` independently uses rational
arithmetic to reconstruct the account wallet and each cycle from persisted
movements and opening capital, compare with the mock's separately maintained
fill-derived wallet, and recompute closed-cycle P&L. It imports no production
financial calculation. Unknown or unsupported external expenses/inventory retain
the commitment with null realized P&L. No balancing entries are invented.

### Financial audit answers

| Question | Verified answer and scope |
| --- | --- |
| Can committed capital be double-spent? | No in executed 2/10/50/100 independent-session contention and owner/fence fixtures. Buy price movement cannot consume unreserved wallet capital in the bounded IOC regression. |
| Are reservations atomic? | Yes: account row lock, constraints and a single PostgreSQL reservation transaction; failed attempts have no order or commitment. |
| Are balances consistent? | Independent rational reconstruction agrees with persisted movements and the mock's fill-derived wallet for closed and recovered fixtures. Unsupported external fees quarantine attribution rather than fabricate cash. |
| Are fees correct? | Confirmed execution fees are deducted once in their stated assets; current taker-rate ceilings are checked per order. Actual exchange fee denomination/rounding remains an authenticated testnet gate. |
| Can duplicate fills occur locally? | Repeated execution IDs produce no extra movements; conflicting duplicates stop processing. This is local idempotency, not exactly-once network delivery. |
| Can settlement happen twice? | Repeated settlement is idempotent; crash-after-settlement restart does not change totals. |
| Are partial fills accounted for? | Yes: confirmed quantity/value/fees determine holdings; the partial Leg 2 fixture preserves and separately unwinds both intermediate assets. |
| Are intermediate assets preserved? | Yes: unresolved exposure retains commitments; safe confirmed unwind creates separate orders and actual movements. |
| Are failed-cycle losses reported? | Yes: rejected-leg and adverse-market recovery fixtures retain negative closed-cycle realized P&L. |
| Does independent P&L agree? | Yes for listed closed fixtures; starting capital plus movements independently reproduces ending cash and P&L. |
| Can interruptions recover? | Tested crash boundaries reconcile original IDs without duplicate POST; uncertainty and residual inventory remain quarantined. Automatic restart continuation is intentionally unavailable. |
| Can paper mode submit real orders? | Managed paper venue has no authenticated client; virtual replay cannot reach private methods. Mode/credential/endpoint isolation fixtures pass. No actual exchange orders were used. |

### Three actual offline traces

These fixtures are mock evidence, not exchange-confirmed trades or profitability.
Each starts with wallet USDT 1000, BTC 0, ETH 0, MNT 1 and reserves 100 USDT.
The fixture deliberately specifies a fixed 0.001-USDT fee per fill to exercise
accounting; it does not demonstrate an actual Bybit fee-denomination policy.

| Operation | Profitable cycle | Losing cycle | Partial second leg and recovery |
| --- | --- | --- | --- |
| Leg 1 | Buy 0.99800299 BTC @ 100 USDT | Same | Same |
| After Leg 1 | Cycle USDT 0.198701; BTC 0.99800299 | Same | Same |
| Leg 2 | Buy 19.92019 ETH @ 0.05 BTC | Same | Buy 9.96009 ETH @ 0.05 BTC; remainder cancelled |
| After Leg 2 | USDT 0.197701; BTC 0.00199349; ETH 19.92019 | Same | USDT 0.197701; BTC 0.49999849; ETH 9.96009 |
| Leg 3 / recovery | Sell 19.92019 ETH @ 6 USDT | IOC floor 5.994 rejects moved 4.9 bid; confirmed zero fill. Then authorized unwind sells 19.92019 ETH @ 4.9 | Unwind 9.96009 ETH @ 6 USDT |
| Cleanup / recovery | Sell BTC dust 0.00199349 @ 99 USDT | Same | Unwind 0.49999849 BTC @ 99 USDT |
| Total recorded fees | 0.004 USDT | 0.004 USDT | 0.004 USDT |
| Ending cycle cash | 119.91419651 USDT | 98.00198751 USDT | 109.45609151 USDT |
| Ending account wallet | 1019.91419651 USDT | 998.00198751 USDT | 1009.45609151 USDT |
| Realized P&L | 19.91419651 USDT | -1.99801249 USDT | 9.45609151 USDT |
| Residual route holdings | Zero | Zero | Zero |
| Unresolved route valuation | None required | None required | None required after recovery |
| Final state | COMPLETED | RECOVERED (cash-closed loss) | RECOVERED |
| Independent reconciliation | PASS | PASS | PASS |

Full persisted order intents (including prices and spend budgets), individual execution IDs/fees and ending holdings are in [the three evidence JSON traces](evidence/authenticated-review-20261011/README.md).

The losing fixture is cash-closed after recovery; it does not claim three normally completed profitable legs survived a price outside their limits.

The intermediate BTC after a failed leg is owned inventory until a separately
confirmed unwind. It is never treated as a refund. Failed and loss-making cycles
remain in reporting. Foreign fee expenses unsupported by cycle cash are instead
quarantined; their P&L remains unresolved.

## E. Recovery coverage

Executed process-crash fixtures cover reservation commit, intent commit, before
submission, acknowledgement, exchange execution before journal application,
confirmed Leg 1, before/after settlement, and outbox fsync before acknowledgement.
Restart queries original IDs without replaying POST or automatically re-enabling
the account. Unresolved orders or inventory quarantine the account.

Graceful shutdown and terminal-state liveness fixtures also verify that exited workers cannot accept new authorization. Out-of-limit fill evidence is rejected before ledger mutation.

Other fixtures cover lost/unknown acknowledgements, delayed/paginated fills,
conflicting executions, cancellation uncertainty, HTTP 429, account/permission/
borrow/clock failures, emergency stop after a fill, PostgreSQL connection loss,
and Redis pause after a fill. These are bounded scenarios, not exhaustive fault
coverage or proof of exactly-once network execution.

## F. Executed checks

| Check | Passed | Failed | Ignored / skipped | Evidence |
| --- | ---: | ---: | ---: | --- |
| Rust formatting | 1 command | 0 | 0 | `cargo fmt --all -- --check` |
| Rust Clippy | 1 command | 0 | 0 | Workspace/all-targets/all-features with warnings denied |
| Rust build | 1 command | 0 | 0 | Workspace build |
| Rust default tests | 115 | 0 | 3 ignored | Full workspace tests |
| Authenticated integration | 44 | 0 | 0 | Included in full suite; real PostgreSQL/Redis/engine/FastAPI, local V5 mock |
| Permission/security tests | 36 | 0 | 0 | Included in full suite; execution and paper permission suites |
| Frontend tests | 26 | 0 | 0 | npm ci, lint, test and build passed |
| Python Ruff | 1 command | 0 | 0 | `python -m ruff check .` |
| Migration upgrade/rollback/re-upgrade | 1 workflow | 0 | 0 | Fresh disposable DB; execution tables and default-disabled constraints checked |
| Full Python/integration suite | 375 | 0 | 0 | 306.79 seconds; 1 Starlette deprecation warning |
| Ignored Redis reliability tests executed | 3 | 0 | 0 | Real Redis; durable replay, backlog and outage recovery |
| Docker integration | 0 | 0 code failures established | Unverified | `docker compose ... config` could not run: Docker executable absent (exit 127) |
| Authorized authenticated testnet/mainnet | 0 | 0 | Unverified | No credentials or order authorization supplied |
| Short synthetic paper soak | 1 run / 50 cycles | 0 | 0 | 60.127985 seconds at 50-ms latency; final ledger balanced, no pending orders/reservations, offline after exit |
| Extended stability/security assessment | 0 | 0 | Unverified | Not established by unit tests or short fixtures |

Detailed command logs and JSON traces are committed in [evidence/authenticated-review-20261011/README.md](evidence/authenticated-review-20261011/README.md). Focused integration/security rows are subsets of the 375-test Python total.

GitHub CI run [38180568476](https://github.com/SteveParadox/Arbitrage_bot/actions/runs/38180568476) failed all six jobs before any steps/logs were available. Queried Rust/Python step lists were empty. This provides no executed CI test result; local evidence above is authoritative for this review. Branch protection was not changed.

The root-only execution container required a test-only UID/stat shim to launch
stock PostgreSQL 16.15; its monetary transactions and row locks were real. Redis
7.0.15 was real. This environment workaround is not deployment/security evidence.
Tests use disposable local databases, loopback mock HTTP and no real credentials.

The post-fix wall-clock paper soak ran **60.127985 seconds**, closed 50 synthetic
cycles and independently reconciled 150 simulated orders. Graceful shutdown left
zero active cycles, pending orders and reservations; heartbeat was null and the
API reported offline/unauthorized immediately. Opening USDT 500 ended at
1511.309840 with no BTC/ETH residual. These intentionally profitable synthetic
prices do not establish exchange profitability. Engine maximum RSS was 41144 KiB,
CPU time 2.594728 seconds and one PostgreSQL probe took 0.395098 ms; these are
smoke measurements, not production performance benchmarks. The generic health
view reported degraded because its risk/control files were not mounted in this
isolated API fixture; no full-stack healthy deployment claim is made.

## G. Remaining risks and readiness gates

- **HIGH operational gate:** authorized authenticated testnet fills, fee policies,
  balances, cancellation/eventual consistency and exchange restart behavior remain
  unverified. Mock results do not establish real-money operational readiness.
- **HIGH deployment/security gate:** Docker startup, production secrets, transport
  isolation/TLS and a production security review remain unverified. The Rust
  journal uses `NoTls`; the supplied development overlay is not a production
  security configuration. Do not activate real trading on these results.
- **HIGH operational gate:** long-duration authenticated testnet soak and
  exhaustive disk/network/process failure coverage remain incomplete.
- **MEDIUM:** startup quarantines intermediate inventory rather than automatically
  resuming trading; operator resolution and broader recovery routing are not
  implemented. This preserves capital and blocks conflicting new exposure.
- **MEDIUM:** only dedicated unleveraged HMAC spot accounts, USDT starting capital,
  and one active cycle per account are supported. Unsupported foreign-asset fees
  and residual valuations prevent final settlement.
- **MEDIUM:** reconciliation re-queries active cycle orders and checks the complete
  wallet, but does not provide a general completed-order/external-transaction
  history cursor. Unexplained wallet differences halt rather than overwrite history.

Offline mocked execution is validated for the listed scenarios. Authenticated
testnet readiness, production deployment readiness and real-money operational
readiness remain unverified. Live-capable code is present, with its gates disabled
in actual validation and example deployment. The PR stays draft and is not
merge-ready for real-money activation.

## H. Reproduction

Current official contracts checked: [Bybit order creation](https://bybit-exchange.github.io/docs/v5/order/create-order), [spot instrument filters](https://bybit-exchange.github.io/docs/v5/market/instrument) and [account fee rates](https://bybit-exchange.github.io/docs/v5/account/fee-rate). These documents establish API contracts, not successful private exchange execution.

Use the commands in [AUTONOMOUS_EXECUTION.md](AUTONOMOUS_EXECUTION.md). For the
complete disposable integration run, after building and installing dependencies:

```bash
python scripts/validate_exchange_offline.py \
  --database-url postgresql://audit@127.0.0.1:55432/postgres \
  --redis-url redis://127.0.0.1:56379/0 --full
```

Use PostgreSQL with enough connections for 100 contenders; free ports 55052,
55055 and 58085. This is a test environment, not a production service command.
Do not inject actual exchange credentials into it. No merge or exchange
activation is part of this delivery.
