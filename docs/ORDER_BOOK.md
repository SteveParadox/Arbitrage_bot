# Phase 3: Local Order-Book Engine

The `orderbook` Rust crate is the in-memory source of truth for depth-aware prices.

## Per-symbol state

For each subscribed market such as `BTCUSDT`, `ETHBTC`, or `ETHUSDT`, the engine maintains:

- best bid and best ask;
- full received bid/ask depth;
- top-N book views;
- base and quote liquidity across those top-N levels;
- exchange timestamp;
- Bybit update ID;
- Bybit cross sequence.

Snapshots replace a book. Deltas update individual levels. A quantity of zero removes a level.

## Executable pricing

A visible best price is not assumed to be available for the entire requested size.

For a quote-notional buy, the engine walks asks from cheapest to most expensive:

```text
buy_with_quote("ETHUSDT", 400)
```

If the book is:

```text
Ask 2500.00 x 0.05 ETH
Ask 2501.00 x 0.20 ETH
Ask 2502.00 x 1.00 ETH
```

the first USD 125 fills at 2500 and the remainder fills at 2501. The result includes:

- filled base quantity;
- filled quote quantity;
- average execution price;
- best and worst touched prices;
- slippage in basis points versus the initial best price;
- number of levels consumed;
- whether available depth could completely fill the request;
- book timestamp, update ID, and sequence used for the estimate.

The crate also supports:

```text
buy_base(symbol, base_quantity)
sell_base(symbol, base_quantity)
```

These primitives are designed for the later triangular-arbitrage scanner, where one leg's actual output quantity becomes the next leg's input quantity.

## Partial liquidity

If requested size exceeds visible depth, estimates return `complete: false` instead of pretending the missing liquidity exists.

## Integration

The Phase 2 Bybit connector now feeds this shared `orderbook` crate directly. The old private book implementation inside `market-data` is no longer used.

No order placement occurs in this phase.
