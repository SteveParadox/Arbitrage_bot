# Phase 2: Bybit Market Data

The Rust `market-data` service consumes Bybit V5 public market data. It does **not** authenticate and contains no order-placement path.

## Inputs

WebSocket subscriptions per configured symbol:

- `orderbook.{depth}.{symbol}`
- `publicTrade.{symbol}`
- `tickers.{symbol}`

Instrument specifications are loaded from `GET /v5/market/instruments-info` at startup because Bybit exposes those specifications through the V5 market REST API.

## Reliability

The connector:

- sends Bybit JSON heartbeats on a configurable interval;
- responds to protocol-level WebSocket ping frames;
- reconnects with bounded exponential backoff;
- resets books on snapshots and Bybit update-id reset `u=1`;
- rejects regressing cross-sequence or update IDs on deltas;
- treats a feed as stale when no market event arrives within the configured window;
- reconnects after stale detection or connection failure.

A cross sequence is checked for monotonicity rather than contiguity because Bybit documents it as a cross-stream ordering value. Order-book update IDs are also required to move forward for deltas.

## Output

Events are emitted as newline-delimited JSON on stdout.

Example normalized quote:

```json
{
  "type": "quote",
  "symbol": "BTCUSDT",
  "bid": 68250.1,
  "ask": 68250.2,
  "timestamp": 1790549000000
}
```

Other event types are `trade`, `ticker`, `instrument`, and `status`.

## Run

From the repository root:

```bash
cp .env.example .env
cd rust
cargo run -p market-data
```

Set environment variables in your shell or deployment service. Rust does not read secrets from source files.

## No trading

This phase uses public market-data endpoints only. It does not call the Bybit private WebSocket, WebSocket order-entry endpoint, or order REST endpoints.


## Scanner stream

Order-book snapshots and deltas are also emitted as normalized `order_book` events. This lets the
Phase 5 scanner rebuild the same local books from the public data stream.

Application logs are written to stderr while market-data events are written to stdout, so the
stream can be piped safely into another process.
