# Managed authenticated execution

The managed observer owns the public connector, scanner, synchronized books and
candidate journal. It selects either the existing paper worker or the authenticated
worker. There is no second scanner or independent risk engine.

This implementation has only been exercised against a loopback V5 mock. No
testnet or mainnet orders were authorized or submitted. Live execution remains
disabled in example configuration and the Docker overlay. See
[the implementation report](AUTHENTICATED_EXECUTION_REPORT.md) for evidence and
remaining acceptance gates.

## Source connection map

| Stage | Actual connection |
| --- | --- |
| Startup | `engine-service/main.rs` → `observer::run` |
| Market and scanner | Existing observer normalization/readiness → `ArbitrageScanner::on_book_update` → scan completion |
| Durable candidate | Existing observer `EventPublisher` critical candidate event before dispatch |
| Mode routing | `ExecutionMode::from_str`; `paper_runtime::Runtime::start` or `exchange_runtime::Runtime::start` |
| Bounded dispatcher | `exchange_runtime::Runtime::dispatch` → bounded 32-item worker queue; rejected candidates recorded in `execution_events` |
| Risk | Existing `RiskEngine::evaluate`, approval expiry and `RiskEngineAuthorizer` through the existing coordinator |
| Capital | `authenticated::Ledger::reserve` → account row lock, account/candidate uniqueness, one active commitment |
| Coordinator | Existing `ThreeLegCoordinator::execute_route_for_opportunity`; `ExchangePlanner` reuses decimal depth/filter planning |
| Venue | `authenticated::Venue::execute` → account, wallet, market, risk, stop, environment and durable intent gates |
| HTTP | Existing `BybitExecutionClient::execute_with_journal`; intent and acknowledgement hooks |
| Confirmation | Existing order monitor + paginated execution history; exact execution-ID deduplication |
| Financial record | `Ledger::apply` records fills, fee/input/output movements, cycle holdings and expected wallet in one transaction |
| Reconciliation | `sync_balances` → `Ledger::reconcile`; discrepancies retain original expected history and block exposure |
| Settlement | `Ledger::settle` requires terminal confirmed orders, closed inventory and fresh agreeing wallet evidence |
| Events | PostgreSQL `execution_events` outbox → durable existing critical-event publisher → Redis |
| API/UI | `api/execution.py` account-scoped routes → `ExecutionPanel.tsx`; separate paper and exchange panels |

## Mode and authorization matrix

| Mode | Venue | Required authorization |
| --- | --- | --- |
| `observe` | None | Never creates orders |
| `paper` | Existing simulated venue | Existing paper deployment/account/runtime controls; live gate false; no private credentials required |
| `testnet` | Existing testnet Bybit client | Explicit testnet flag, live false, dedicated testnet secret, matching catalog and UID, deployment permit, authenticated account start, completed startup reconciliation, current books/wallet and risk approval |
| `live` | Existing mainnet Bybit client | Explicit live flag, testnet false, dedicated live secret, matching UID/environment, deployment permit with every readiness attestation, authenticated mainnet start, healthy reconciliation/books/dependencies and expiring risk approval |

Credentials alone cannot authorize the managed worker. Account ownership resets
`enabled=false` on every restart. A failed worker disables authorization and
quarantines committed cycles. Replay with a virtual clock cannot instantiate an
authenticated venue. Wall-clock replay can exercise private HTTP only with the
development loopback mock and literal dummy credentials; it cannot target a public
exchange endpoint.

Authenticated exposure also requires an existing valid global control record.
Missing or corrupt state blocks the worker; startup does not silently invent a
clear emergency-stop state. Deployment bootstrap must explicitly provision the
existing version-1 control schema with `enabled=false`, no request ID and a current
timestamp, using the established control-state tooling. This alone cannot start
trading. A recorded stop intent must be resolved through the control procedure;
removing its file is not an authorized clear operation.

## Account, secret and permit setup

Provision a dedicated **unleveraged, exclusive spot account**. Other trading,
withdrawal or transfer permissions, open orders, borrowed capital, margin exposure
or incomplete wallet evidence are rejected. The worker supports system-generated
HMAC credentials; RSA credentials are not implemented. Bybit account and endpoint
compatibility must still be validated on authorized testnet access.

Engine-only secret sources are `BYBIT_TESTNET_API_KEY_FILE` /
`BYBIT_TESTNET_API_SECRET_FILE` or the corresponding `BYBIT_LIVE_*` sources.
Generic `BYBIT_API_KEY` credentials are not used by this runner. Never inject
private exchange secrets into the frontend or API. Secret files must not be
writable by other users. Protect the containing directory and use the deployment
secret manager for production; a local file is not a complete production secret
management solution.

`ARB_EXECUTION_API_KEYS_JSON` (or `_FILE`) contains a separate HTTP credential
catalog keyed by SHA-256 of high-entropy bearer tokens. Each grant has exactly
`subject`, `role` (`viewer`, `operator`, `admin`), `accounts` (explicit IDs), and
`modes` (`testnet`, `live`). Paper/control credentials cannot be promoted into this
catalog. Admin account provisioning is `POST /execution/accounts`; operator start
requires `authorization=authorize_testnet` or `authorize_mainnet` and a stable
`request_id`. Read-only credentials cannot start execution. The dashboard requires
the corresponding typed authorization phrase and never starts during polling.

The read-only deployment permit is JSON:

```json
{
  "version": 1,
  "account_id": "dedicated-test-account",
  "mode": "testnet",
  "exchange_uid": "123456",
  "expires_at_ms": 0,
  "offline_tests": false,
  "testnet_execution_verified": false,
  "reconciliation_verified": false,
  "crash_recovery_verified": false,
  "security_verified": false,
  "soak_verified": false,
  "human_mainnet_authorization": false
}
```

This intentionally expired example authorizes nothing. Permits must match the
account, environment and UID, expire within 24 hours, and remain protected by the
deployment operator. All seven attestations must be true for live execution;
setting them without performing the checks is not evidence of readiness. A permit
file is an operator trust boundary, not a cryptographically signed certification.

## Financial and recovery policy

Migration `0011_exchange_execution` is additive and reversible. `execution_*`
entities are isolated from `paper_*`. PostgreSQL owns local intentions,
commitments, processing state and history; Bybit owns wallet and fill evidence.
There is one active cycle per account, deliberately prioritizing reconciliation
over concurrency. Separate account IDs cannot claim the same mode/UID identity.
Session advisory ownership, epochs, row locks and uniqueness constraints provide
account fencing and atomic reservations; application mutexes are not the capital
authority. Exchange wallet availability and local commitments are both considered.
A local commitment is not an exchange-side wallet lock.

Order intents commit before the order POST. The same stable client ID is queried
after uncertain submission; the POST is never blindly retried. Acknowledgement is
not a fill. Individual execution records determine actual post-fee holdings and
the next leg. Fill identity conflicts, missing fees, extra fees or incomplete
pagination deny unsupported settlement. HTTP and database operations cannot be
made atomic; durable uncertainty is preserved for reconciliation.

Closed cycles report `ending USDT - starting USDT`, including recorded fees and
recovery trades. Unsupported third-asset expenses or residual inventory preserve
the commitment and return null realized P&L, rather than invented cash refunds or
mark-to-market profit. No automated third-asset valuation is supplied. The mock
tests independently recompute persisted movements using rational arithmetic.

The runner queries `/v5/account/fee-rate` for each spot symbol before reserving
capital and again before each order. The configured fee rate is a conservative
ceiling: missing, mismatched or higher account taker fees deny execution. Verified
rates and the modeled ceiling are journaled. Settlement still uses individual
execution fee amounts and currencies, rather than the quoted fee tier.

The existing coordinator may perform bounded, separately risk-authorized unwind
orders only after confirmed terminal evidence. It cannot unwind against a still
uncertain order. Startup queries the original client IDs and fills, checks the
wallet, settles fully closed evidence, and quarantines other inventory. It does
not replay order submission or automatically resume exposure. Operator-directed
resolution of quarantined inventory remains an operational gate; there is no
unsafe capital-release/reset endpoint.

`stop` revokes new exposure. `emergency-stop` additionally latches the account
stop. A committed command response explicitly does not claim active orders were
cancelled. Account stop repair must follow reconciled operator procedures; no
automatic clear-on-restart exists.

## Local validation

Build the binaries, then use disposable local PostgreSQL 16 and Redis 7 services.
PostgreSQL needs at least 110 available connections for the 100-contender test;
the validation role needs permission to create/drop **test databases only**.

```bash
cargo fmt --manifest-path rust/Cargo.toml --all -- --check
cargo clippy --manifest-path rust/Cargo.toml --workspace --all-targets --all-features -- -D warnings
cargo build --manifest-path rust/Cargo.toml --workspace
cargo test --manifest-path rust/Cargo.toml --workspace
cd python
python -m pip install -e '.[dev]'
python -m ruff check .
cd ..
python scripts/validate_exchange_offline.py \
  --database-url postgresql://audit@127.0.0.1:55432/postgres \
  --redis-url redis://127.0.0.1:56379/0
cd frontend
npm ci
npm run lint
npm test
npm run build
```

The validator creates and drops isolated databases, never flushes Redis, strips
exchange secrets and forces live=false. It runs actual engine/API/HTTP mock
processes and migrations. `--full` also starts an isolated Rust gRPC service and
enables paper/observer/gRPC/reliability integration tests, plus the ignored Rust
Redis tests. Ports 55052, 55055 and 58085 must be free. A missing integration
fixture is a skipped gate, not a passing integration result.

## Docker and deployment

`docker/execution.compose.yml` extends the existing development stack. It adds
public catalog generation, persistent account/outbox state, startup dependencies,
testnet secret injection, a read-only permit mount, and resource/liveness settings.
It does **not** provide a production security certification or enable live trading.
Create `.env` from `.env.example` and local secret files; observation mode can use
empty testnet secret files and `{}` as the grant catalog. Example command:

```bash
docker compose -f docker/docker-compose.yml -f docker/execution.compose.yml config
docker compose -f docker/docker-compose.yml -f docker/execution.compose.yml up --build
```

Defaults remain `observe`, autostart=false and live=false. For a separately
authorized testnet deployment, explicitly select mode/testnet environment, enable
the testnet flag and managed observation, provision the account and permit, and
issue the authenticated account start only after READY. The catalog must be current
and match testnet. Do not combine this overlay with the paper overlay.

The base stack uses development database passwords and an administrative database
role. Production startup rejects that role and weak secrets. Production requires
separate migration/runtime roles, authenticated isolated Redis/PostgreSQL,
restricted ingress, TLS termination, protected state volumes, secret rotation and
operational monitoring. The current Rust PostgreSQL connector uses `NoTls`; remote
production database TLS support remains unverified/unimplemented. Do not expose
this development stack as a production trading deployment.

Docker build/startup, exchange-confirmed testnet acceptance, extended testnet soak,
production security review and explicit human mainnet authorization remain required
before future activation. No Docker daemon is available in the audit environment.
