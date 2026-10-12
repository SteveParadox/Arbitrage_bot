# Production operations

This configuration is live-disabled. Infrastructure provisioning is not exchange execution authorization. PR #11 remains a draft until the deployment, security and reliability gates in `PRODUCTION_INFRASTRUCTURE_REPORT.md` pass.

## Trust boundaries and transports

Use `docker/production/compose.yml` as a standalone stack, never as an overlay on the development Compose file. PostgreSQL, Redis, engine gRPC and the API have no published administrative ports. The internal data network is private. Only the frontend HTTPS listener is published, on literal loopback port 8443. Engine/public-catalog egress is a separate network. The API HTTP hop to nginx and Prometheus is permitted only inside this isolated Docker network. Do not publish it externally.

Rust `infrastructure::connect_database` and Python `api.infrastructure.postgres_options` require production CA trust and hostname verification. Production uses PostgreSQL verify-full, authenticated verified Redis TLS, and gRPC mutual TLS plus the existing control token. Missing CA material, insecure URL overrides, untrusted certificates and mismatched names fail closed. Alembic uses the same Python transport policy. Connection and SQL deadlines/pool limits are bounded.

The shared admission policy requires fresh, typed independent-integrity and monitoring proofs. Missing, future-dated or expired proofs deny new exposure. Restore markers deny execution until reviewed recovery is complete. Healthy process liveness is not financial readiness. Runtime status includes this distinction.

## Provisioning and startup

Requirements: Docker Engine 26+ and current Compose (volume subpaths), a private deployment host, Python 3.12 and project dependencies for provisioning, controlled DNS, trusted certificates, private backup storage, and sufficient measured disk capacity. Do not deploy the root-only PostgreSQL test shim used in constrained CI environments.

Provision secrets on the deployment host:

```sh
python scripts/provision_production.py /srv/arbitrage/secrets \
  --https-host dashboard.example.com --assign-container-owners
export ARB_SECRETS_DIR=/srv/arbitrage/secrets
export ARB_CORS_ORIGINS=https://dashboard.example.com
export ARB_TRUSTED_HOSTS=dashboard.example.com,api,localhost,127.0.0.1
export ARB_TRADING_MODE=observe
export ARB_ENGINE_AUTOSTART=false
export ARB_PAPER_TRADING_ENABLED=false
```

Owner assignment requires host permission to set numeric container UIDs. Application files belong to UID 10001, PostgreSQL/Redis private files to UID 999, and the separate monitoring token/webhook to UID 65534. Verify the actual image UIDs before deployment. Private files are mode 0400; CA certificates are public. CA signing material stays offline and is never mounted in services. Provisioning refuses an existing destination and creates no exchange credentials.

The generated HTTPS leaf uses a private CA. Install that trust only on authorized clients or replace the HTTPS certificate with an operator-trusted leaf/chain before public exposure. Provisioning alone does not create public certificate trust. Configure an approved Alertmanager receiver; the default `monitoring.invalid` URL deliberately does not deliver alerts.

```sh
docker compose -f docker/production/compose.yml \
  -f docker/production/monitoring.yml config --format json > compose-model.json
python scripts/check_production_compose.py compose-model.json
docker compose -f docker/production/compose.yml \
  -f docker/production/monitoring.yml build
docker compose -f docker/production/compose.yml \
  -f docker/production/monitoring.yml up -d
```

Model validation is not startup validation. Inspect service logs, verified DB/Redis sessions, mTLS connectivity, readiness proofs, migrations and persistent volumes. The public catalog refresh must produce validated nonempty routes. Failed public metadata retrieval blocks the engine; the API remains available to operators. The root initializer has no network and only prepares bounded volume ownership; runtime containers drop capabilities, use non-root users, read-only roots, PID/memory/CPU limits and bounded logs.

For paper operation select paper mode and its explicit deployment flag, initialize an account through the existing authenticated API, assign account grants, and send an explicit start command after readiness and recovery. Never use exchange wallet capital for paper accounting. Testnet uses the dedicated overlay and credentials, separate deployment/runtime authorizations and account reconciliation. No testnet orders were authorized or placed in this work. Live remains false in shipped configurations. Replay and fault injection are development-only.

## Mounted secrets and rotation

Use `_FILE` configuration for DB/Redis URIs, control and gRPC tokens, API account grants, and generic or mode-specific Bybit credentials. Reject conflicting inline/file values. Secret readers reject symlinks, oversized/malformed values and group/other-readable private files. Do not export secrets into frontend variables, metrics labels or diagnostic dumps. Log/event redaction includes configured credentials and URI passwords, but never substitutes for restricted access to audit records.

Rotation is restart-based; no hot-reload guarantee is made:

1. Disable execution through authenticated controls. Confirm durable stop, ownership shutdown, known order outcomes and safe inventory. Physically fence obsolete deployments.
2. Provision the new DB/Redis credentials or TLS material through an approved administrator. Stage overlapping CA trust when necessary. Keep the old backup encryption key for historical backups.
3. Atomically replace secret files, preserving private modes and correct UIDs. Update the paired Prometheus metrics token separately. Never place CA signing keys in service mounts.
4. **Force-recreate** affected containers. An ordinary restart may retain the old inode of a bind-mounted file. Rotate DB/Redis server users and reconnect pools, gRPC peers, API clients or exchange sessions together as applicable.
5. Verify authentication, certificate names, financial recovery and current readiness. Require explicit execution reauthorization. A successful rotation must not restore an old live-enabled state.

## Monitoring and incident response

`/metrics` requires a distinct metrics token. Do not expose it publicly. It includes bounded-label API counters, DB/Redis probes, pool/lock evidence, engine resources, finite REST categories, scanner/publisher freshness, event backlog, incomplete/recovery cycles, reservations, reconciliation and backup/certificate age. Account identities and order/candidate IDs belong in protected audit records, not metric labels. Monetary Prometheus gauges are operational approximations; Decimal/Fraction financial records remain authoritative.

Prometheus and Alertmanager are private; their UI bindings are loopback. Import `docker/monitoring/grafana-dashboard.json` into a separately secured Grafana instance. This repository does not deploy Grafana. Seventeen alert rules are supplied; only three rule fixtures were executed locally. Receiver delivery and whole-stack alert propagation remain unverified.

| Alert family | Safe operator action |
| --- | --- |
| Engine, database or event durability unavailable | Keep new exposure disabled; preserve intent/outbox; restore the dependency and reconcile before authorization. |
| Financial integrity, capital inconsistency or recovery required | Quarantine the account; independently inspect fills, fees and holdings. Never insert balancing refunds or erase records. |
| Unknown exchange order | Query the original stable order identity and executions. Do not submit a replacement or conflicting unwind while outcomes are ambiguous. |
| Stale market evidence or growing backlog | Keep execution blocked, inspect connector generations/consumer acknowledgements and drain durable backlog without unsafe trimming. |
| Certificate expiry, authentication anomalies, resource pressure | Rotate/recreate or investigate access/capacity; never bypass TLS, authentication or persistence checks. |
| Backup/reconciliation age | Verify backup/reconciliation evidence and storage before reopening exposure. |

Initial design targets: probes/timeouts bounded at seconds, metrics every 15 seconds, financial proof every 60 seconds, freshness policy enforced by admission, daily verified backups and critical alerts within their configured evaluation/hold windows. These are targets, not measured production SLOs.

## Backup, restore and rollback

Quiesced backups include PostgreSQL custom dumps and runtime outbox/control/risk state. AES-256-GCM authenticates the format, nonce and archive; manifests verify individual files. The signing CA, credentials and deployment execution permits are excluded. Retention defaults to fourteen snapshots. Schedule `scripts/production_backup.sh` from a protected host scheduler; scheduling/off-site replication is not provisioned here.

```sh
sh scripts/production_backup.sh
```

The wrapper stops engine/API/integrity first, requires disabled accounts and unowned advisory locks, and leaves services stopped on failure. Backups use bounded tmpfs scratch; size that separately from expected dumps, runtime journals and encrypted archives. Large datasets require a reviewed encrypted scratch strategy. Copy verified backups off-site with restricted access, retain the key securely, and periodically restore-test. No WAL archiving or point-in-time recovery is implemented.

Restore only to a fenced, empty database and empty/unpopulated state volume, with compatible PostgreSQL major version and schema/code. Using the operations-profile backup service:

```sh
docker compose -f docker/production/compose.yml --profile operations \
  run --rm --no-deps backup restore \
  --backup /backups/REVIEWED_BACKUP.arbbackup \
  --key-file /run/secrets/backup-key --state /app/data \
  --status-file /app/data/observability/restore.json
```

A durable restore marker precedes restore effects. Authentication precedes extraction; paths, duplicates and archive sizes are checked. Database restoration is transactional, accounts are disabled/quarantined, ownership epochs advance, and runtime control is reinitialized disabled. The marker survives failure. Keep exposure blocked while reconstructing intent, fills, holdings and unresolved orders. Obtain fresh exchange evidence only with separately authorized access.

After all incomplete cycles have been recovered or safely handled and all restored exchange accounts have fresh reconciliation, an operator may invoke `scripts/acknowledge_restore.py --previous-site-fenced --confirm-reconciliation-reviewed` in the configured environment. It checks stopped owners, independent integrity and closed dependencies; it clears the barrier only after committing disabled control state. It never enables trading.

Rollback code only when compatible with the current database schema and event contracts. Stop/fence owners and preserve financial records first. Do not automatically downgrade populated financial tables or restore an old snapshot over active accounts. Unsafe rollback requires forward repair/reconciliation rather than deleting newer history.

## Reproducible offline validation

Run as an ordinary user with PostgreSQL 16 binaries, Redis, a built Rust workspace, Python development dependencies and no exchange secrets:

```sh
cargo fmt --manifest-path rust/Cargo.toml --all -- --check
cargo clippy --manifest-path rust/Cargo.toml --workspace --all-targets --all-features -- -D warnings
cargo build --manifest-path rust/Cargo.toml --workspace
cargo test --manifest-path rust/Cargo.toml --workspace
(cd python && python -m ruff check . && python -m pytest -q)
(cd frontend && npm ci && npm run lint && npm test && npm run build)
ARB_RUN_TLS_INTEGRATION=1 python -m pytest -q \
  python/tests/test_infrastructure.py python/tests/test_infrastructure_transport.py \
  python/tests/test_production_operations.py
python scripts/run_disposable_validation.py full --output evidence/full
python scripts/run_disposable_validation.py soak --profile smoke --output evidence/smoke
python scripts/run_disposable_validation.py soak --profile extended --output evidence/30m
python scripts/run_disposable_validation.py soak --profile medium --output evidence/4h
python scripts/run_disposable_validation.py soak --profile long --output evidence/24h
python scripts/run_disposable_validation.py soak --profile restart --output evidence/restart
python scripts/run_disposable_validation.py soak --profile fault --output evidence/fault
python scripts/verify_financial_integrity.py --output evidence/integrity.json
python scripts/scan_secrets.py --output evidence/secrets.json
promtool check rules docker/monitoring/alerts.yml
(cd docker/monitoring && promtool test rules rules.test.yml)
```

The native harness uses disposable development loopback services and the actual supervised engine, scanner/risk/paper coordinator, event consumer, API and independent accounting verifier. Secure transport is a separate real-server suite. Soak duration is monotonic wall time; virtual financial time is separate. Explicit `--seconds` is allowed but must be reported as the shortened duration. Extended CI supports 30 minutes/4 hours/24 hours; 24-hour execution requires the designated self-hosted runner.

No host capacity recommendation is verified yet. Compose limits are design bounds, not measured minimum host requirements. Short observed engine RSS does not establish a safe whole-stack host size or rule out long-duration leaks. Measure database, Redis, API, monitoring, logs, backups, file descriptors, queue delay and headroom together before sizing a production host.
