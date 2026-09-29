# Phase 5: Arbitrage Scanner

The Rust scanner is event-driven. It does not poll every triangle after every market event.

## Hot path

For each configured route, startup builds an index:

```text
symbol -> affected route indexes
```

A live update therefore follows:

```text
order-book update
      |
      v
apply snapshot/delta to local book
      |
      v
lookup only routes containing that symbol
      |
      v
simulate three executable conversions
      |
      v
record gross result
```

Unrelated symbols produce no scanner work.

## Conversion model

Each leg uses the Phase 3 depth-aware order-book engine:

- `BUY`: input is quote currency and uses `buy_with_quote`; output is actually fillable base quantity.
- `SELL`: input is base currency and uses `sell_base`; output is actually fillable quote quantity.

The actual output of each leg becomes the next leg's input.

For a configured `450 USDT` route, a completed record can therefore represent:

```text
450 USDT
  -> 0.00652 BTC
  -> 0.119 ETH
  -> 452.80 USDT
```

The scanner computes:

- final amount;
- gross profit;
- gross return percentage;
- gross return basis points;
- whether the gross result is positive.

This phase intentionally excludes fees. Every record contains `fees_included: false`.

## Failure evidence

Relevant route evaluations are recorded even when they cannot complete. Status values are:

- `complete`;
- `missing_book`;
- `insufficient_liquidity`;
- `start_amount_not_configured`;
- `calculation_error`.

This prevents startup gaps and shallow-book failures from silently disappearing.

## Recorded leg evidence

Each completed or partially completed leg stores:

- symbol and BUY/SELL side;
- input/output asset;
- input/output amount;
- depth-aware execution estimate;
- average execution price;
- best/worst price touched;
- slippage in basis points;
- levels consumed;
- order-book timestamp;
- update ID;
- sequence.

The route record also includes the triggering update and the timestamp skew across the books used.

## Configuration

`shared/config/scanner.json` controls simulated starting amounts and the journal:

```json
{
  "version": 1,
  "start_amounts": {
    "USDT": 450.0
  },
  "record_path": "data/scans/arbitrage_scans.ndjson"
}
```

These are scanner notionals only. No funds are moved.

Generate triangles first:

```bash
cd python
python -m strategy.triangle_discovery --start-assets USDT
```

Then ensure `BYBIT_MARKET_SYMBOLS` contains every required symbol from the generated route set.

## Live pipeline

From the `rust` directory:

```bash
cargo run -p market-data | cargo run -p scanner --bin scan-live
```

The market-data service writes structured logs to stderr and normalized market events to stdout.
The scanner consumes only `order_book` events.

Every affected route evaluation is appended to:

```text
data/scans/arbitrage_scans.ndjson
```

and complete scan records are also emitted on scanner stdout for later APIs/dashboard integration.

## No execution

The scanner contains no order-placement call. Every record explicitly contains:

```json
{
  "execution_enabled": false
}
```
