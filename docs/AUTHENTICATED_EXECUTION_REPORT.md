# Authenticated execution implementation and review

## A. Repository and delivery

- Inspected main: `df3b077544132b0315ad6ece91c18063395652a8` (merged PR #9).
- Implementation: [draft PR #10](https://github.com/SteveParadox/Arbitrage_bot/pull/10).
- Branch: `codex/autonomous-bybit-execution-runner`.
- Reviewed initial PR head: `4e5e120ff17f561db3c340f2d0712ea87f27b45b`.
- Corrected source commit and final validation results are recorded below after the final run.
- Migration `0011_exchange_execution` adds eight isolated `execution_*` tables,
  account fencing, one active commitment per account, account/candidate and
  market-evidence uniqueness, immutable intents, fill deduplication, movements,
  reconciliation, commands and transactional audit outbox. It does not mutate
  paper history. The unpublished migration was extended before merging.

The local scratch environment was reset during the work. The PR branch and a
saved Git tree were recovered; source corrections were reapplied and tests were
rerun. Earlier ephemeral logs are not presented as final validation evidence.

## B. Review findings and corrections

| Severity | File / cause | Reproduction and correction | Regression evidence |
| --- | --- | --- | --- |
| HIGH | `authenticated/ledger.rs::settle`, `exchange_runtime::start`: any recent healthy reconciliation could mask a later discrepancy | Crash after reservation, change the mock wallet, restart immediately. Require the **latest** reconciliation after order evidence; a startup discrepancy preserves the commitment. | `test_startup_discrepancy_cannot_release_recent_reservation` |
| HIGH | `Ledger::apply`: exchange side and requested quantity were not matched to durable intent | Return the opposite side or an altered quantity. Check side, quantity and filled/remaining bounds before financial writes. | `test_exchange_order_must_match_durable_economic_intent` (2 cases) |
| HIGH | `Ledger::apply`: later evidence could overwrite confirmed fills with a smaller or nonterminal result | Crash after Leg 1; return an empty cancelled record on restart. Reject filled-quantity regression, removed/changed prior fills and terminal/confirmation downgrade. | `test_restart_rejects_regressed_fill_evidence` |
| HIGH | `Ledger::flush_events`: a retry changed `occurred_at` for the same durable event ID | Crash after event fsync before SQL publication acknowledgement, restart. Use the event's persisted creation timestamp on every retry. | `test_financial_outbox_retry_keeps_the_original_event` |
| HIGH | `authenticated::Gate`: a missing global control file was treated as clear by the shared paper stop guard | Remove the fixture's global control record. Authenticated new exposure now requires a valid existing record, plus stop/kill checks. | `test_missing_global_control_is_not_execution_permission` |
| HIGH | `BybitExecutionClient::verify_account`: unrelated trading permissions were accepted | Return a ContractTrade permission alongside SpotTrade. Require exactly SpotTrade and no other nonempty permissions. | `test_account_preflight_blocks_unsafe_exchange_evidence[overprivileged]` |
| HIGH | Execution history parser ignored additional fee records | Return nonempty `extraFees`. Unsupported additional fees deny confirmation and preserve unresolved capital rather than silently ignoring costs. | `test_uncertain_exchange_evidence_preserves_commitment[extra_fees]` |
| HIGH | Profitability depended solely on a configured fee rate | Return a higher account fee tier before reservation or after Leg 1. Verify current symbol/account taker fees against the modeled ceiling before reservation and each order; persist evidence. | `test_account_fee_evidence_is_required_before_each_order` (2 cases) |
| MEDIUM | Execution controls lacked durable market-readiness evidence | Track observer readiness and its timestamp in the account; start requires current synchronized markets. | Autonomous runtime start and missing-control fixtures; API/frontend permission tests |
| MEDIUM | API mode configuration accepted contradictory flags; frontend accepted malformed/duplicate records | Reject conflicting modes and permission flags; validate decimal strings, record identities and duplicate rows. | `test_conflicting_execution_modes_fail_api_startup`, frontend contract tests |
| MEDIUM | Paper adverse-movement fixture used scheduler timing before reservation | Trigger movement after the first persisted leg, during modeled latency; preserve assertions for no fill, refunded capital and zero P&L. | `test_submission_price_cap_blocks_adverse_movement_during_latency` |

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

### Three actual offline traces

These fixtures are mock evidence, not exchange-confirmed trades or profitability.
Each starts with wallet USDT 1000, BTC 0, ETH 0, MNT 1 and reserves 100 USDT.
The fixture deliberately specifies a fixed 0.001-USDT fee per fill to exercise
accounting; it does not demonstrate an actual Bybit fee-denomination policy.

| Operation | Profitable cycle | Losing cycle | Partial second leg and recovery |
| --- | --- | --- | --- |
| Leg 1 | Buy 0.99800399 BTC @ 100 USDT | Same | Same |
| After Leg 1 | Cycle USDT 0.198601; BTC 0.99800399 | Same | Same |
| Leg 2 | Buy 19.92023 ETH @ 0.05 BTC | Same | Buy 9.96011 ETH @ 0.05 BTC; remainder cancelled |
| After Leg 2 | USDT 0.197601; BTC 0.00199249; ETH 19.92023 | Same | USDT 0.197601; BTC 0.49999849; ETH 9.96011 |
| Leg 3 / recovery | Sell 19.92023 ETH @ 6 USDT | Sell 19.92023 ETH @ 4.9 USDT | Unwind 9.96011 ETH @ 6 USDT |
| Cleanup / recovery | Sell BTC dust 0.00199249 @ 99 USDT | Same | Unwind 0.49999849 BTC @ 99 USDT |
| Total recorded fees | 0.004 USDT | 0.004 USDT | 0.004 USDT |
| Ending cycle cash | 119.91423751 USDT | 98.00198451 USDT | 109.45611151 USDT |
| Ending account wallet | 1019.91423751 USDT | 998.00198451 USDT | 1009.45611151 USDT |
| Realized P&L | 19.91423751 USDT | -1.99801549 USDT | 9.45611151 USDT |
| Residual route holdings | Zero | Zero | Zero |
| Unresolved route valuation | None required | None required | None required after recovery |
| Independent reconciliation | PASS | PASS | PASS |

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
| Rust default tests | 110 | 0 | 3 ignored | Full workspace tests |
| Authenticated integration | 40 | 0 | 0 | 63.82 seconds; real PostgreSQL/Redis/engine/FastAPI, local V5 mock |
| Permission/security focused tests | 36 | 0 | 0 | Execution and paper permission suites |
| Frontend tests | 26 | 0 | 0 | npm ci, lint, test and build passed |
| Migration upgrade/rollback/re-upgrade | 1 workflow | 0 | 0 | Fresh disposable DB; execution tables and default-disabled constraints checked |
| Full Python/integration suite | Pending final run | — | — | Do not interpret this pending row as a pass |
| Docker integration | 0 | 0 code failures established | Unverified | `docker compose ... config` could not run: Docker executable absent (exit 127) |
| Authorized authenticated testnet/mainnet | 0 | 0 | Unverified | No credentials or order authorization supplied |
| Extended stability/security assessment | 0 | 0 | Unverified | Not established by unit tests or short fixtures |

The root-only execution container required a test-only UID/stat shim to launch
stock PostgreSQL 16.15; its monetary transactions and row locks were real. Redis
7.0.15 was real. This environment workaround is not deployment/security evidence.
Tests use disposable local databases, loopback mock HTTP and no real credentials.

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
