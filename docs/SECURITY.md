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
