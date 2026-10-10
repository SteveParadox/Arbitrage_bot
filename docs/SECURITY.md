# Security

## Exchange credentials

- Never hardcode Bybit API keys or secrets.
- Never commit credentials to Git history, examples, screenshots, logs, fixtures, or tests.
- Use local environment variables for development and a managed secret store for deployment.
- Prefer testnet keys until production-readiness criteria are met.
- Use the minimum exchange permissions required.
- Do not grant withdrawal permission to trading API keys.
- Rotate credentials immediately if exposure is suspected.

## Logging

Secrets, authorization headers, signed payloads, private keys, and complete credential-bearing URLs must not be logged.

## Live trading

Live trading is disabled by default with `ARB_LIVE_TRADING_ENABLED=false`. Enabling it later must require explicit configuration plus risk-layer checks.

## Development services and privileged reconciliation

Development PostgreSQL is published only at `127.0.0.1:5432`; containers use the internal `postgres:5432` service. Redis and engine gRPC host mappings also bind localhost. Compose runs migrations before API startup. The bundled `arbitrage` database credentials are development-only; production must supply separate credentials/secrets and private database networking instead of copying this Compose file unchanged.

Trading, engine mutation and micro-live reconciliation require the existing high-entropy operator bearer. Internal engine calls use the separate gRPC metadata credential. Never send secrets in query parameters or Vite variables. Operator jobs must attach the HTTP Authorization header and keep reconciliation operation IDs stable for retries. PostgreSQL locks, unique operation identities, finite precision-bounded amounts and transaction timeouts protect reconciliation from concurrent/replayed side effects.

Public production ingress requires deployment-level TLS, authentication policy and rate limiting. Internal gRPC requires a trusted private network or TLS/mTLS. No live trading or production-security readiness is established merely by passing local tests; see [PR2_CRITICAL_AUDIT.md](PR2_CRITICAL_AUDIT.md).
