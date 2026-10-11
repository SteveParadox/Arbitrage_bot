# Paper-service security controls

These controls harden the paper service; they do not certify a production deployment.
Live submission remains disabled. Production startup requires explicit
`ARB_ENV=production`, paper/observe mode and `ARB_LIVE_TRADING_ENABLED=false`.
Private Bybit credentials, development fixtures, crash hooks and virtual replay
are rejected in production. Paper remains a simulated venue.

## Account authorization

The distinct control and internal gRPC tokens must be randomly generated, at least
32 bytes, and must not use example credentials. The global control token is an
administrator credential. Do not give it to ordinary dashboard users.

`ARB_PAPER_API_KEYS_JSON` is a JSON object keyed by each credential's lowercase
SHA-256 digest. Each grant contains exactly `subject`, `role` and `accounts`:

```json
{"<64-character SHA-256 digest>":{"subject":"analyst","role":"viewer","accounts":["paper-account"]}}
```

Generate high-entropy credentials outside source control; retain only their
digests in this catalog. A viewer can read only named accounts. An operator or
account admin can start/stop only named accounts. Account creation remains a
global-admin action. No tenant reset/history-clear/forced-release endpoint is
introduced. API status returns `can_control`; the dashboard fails closed if this
permission is absent. Authorization is enforced in the API, not just the UI.

## Request and credential protection

Production requires positive `ARB_API_RATE_LIMIT_PER_MINUTE`, explicit HTTPS
`ARB_CORS_ORIGINS` and non-wildcard `ARB_TRUSTED_HOSTS`. The quota is an atomic
Redis counter with a 60-second expiry, shared across API workers. Invalid
credentials share their source-IP quota, preventing arbitrary forged tokens from
creating unbounded quota keys. Redis failures reject protected requests. Verified
own-account/global emergency stop stays available through quota exhaustion or a
Redis outage; authentication and database mutation checks still apply.

Bodies are limited by both Content-Length and actual streamed bytes, with a
10-second receive watchdog. Default `ARB_API_MAX_BODY_BYTES=65536`. Production
non-paper application routes require the global administrator credential.
`/livez` is minimal unauthenticated process liveness, not financial readiness.
API responses use no-store/nosniff/frame-denial/referrer headers. Production
interactive docs/OpenAPI are disabled; validation errors hide input values.

API `_FILE` sources: `ARB_DATABASE_URL`, `ARB_REDIS_URL`,
`ARB_CONTROL_API_TOKEN`, `ARB_ENGINE_GRPC_TOKEN`, `ARB_PAPER_API_KEYS_JSON`.
Engine supports its database, Redis and internal-token files. Inline/file conflicts, oversized files
and group/other-writable files fail startup. Do not also load development inline
secrets when using files. No credential is returned in status/config summaries.

Production PostgreSQL and Redis require authenticated URLs with decoded password
length at least 16. API and paper ledger reject database roles with superuser,
create-role or create-database privileges. Provision schema migrations separately;
grant the runtime only the required application tables/sequences. This check does
not prove all ACLs are least privilege.

## Deployment acceptance still required

Validate TLS termination, proxy/source-IP trust, secret rotation/revocation,
container non-root identity and writable volume permissions, application/table
ACLs, authenticated private service networks, resource limits and monitored
backup/restore. Docker startup and external penetration testing remain unverified.
Do not expose PostgreSQL, Redis or gRPC publicly. Readiness must inspect financial
reconciliation and runtime authorization, not just `/livez`.
