# Phase 4: Triangle Discovery

Phase 4 discovers structural triangular spot routes in Python and loads the verified route file in Rust.

## Market graph

Each Bybit spot instrument forms a two-way conversion edge.

For a pair with:

```text
base = BTC
quote = USDT
symbol = BTCUSDT
```

the conversion directions are:

```text
USDT -> BTC = BUY BTCUSDT
BTC -> USDT = SELL BTCUSDT
```

This rule is applied to every leg. BUY/SELL directions are therefore derived from the
instrument's base/quote definition rather than inferred from the symbol name.

## Discovery

The Python generator requests Bybit V5 spot instruments with `status=Trading`, builds an
asset graph, and finds every fully connected set of three distinct assets.

By default, every valid starting asset and both directions are retained. One 3-asset triangle
can therefore produce six directed routes.

Example:

```text
USDT -> BTC -> ETH -> USDT
USDT -> ETH -> BTC -> USDT
BTC  -> ETH -> USDT -> BTC
BTC  -> USDT -> ETH -> BTC
ETH  -> BTC -> USDT -> ETH
ETH  -> USDT -> BTC -> ETH
```

To keep only routes that start in USDT:

```bash
cd python
python -m strategy.triangle_discovery --start-assets USDT
```

To discover every directed route:

```bash
cd python
python -m strategy.triangle_discovery
```

The result is written to:

```text
shared/config/triangles.json
```

The committed file is intentionally initialized empty. Generate it from live Bybit instrument
metadata before scanning so delisted or newly listed symbols are not baked into source control as
assumptions.

For offline/reproducible discovery, provide a captured Bybit response:

```bash
python -m strategy.triangle_discovery \
  --instruments-file ./instruments.json \
  --start-assets USDT
```

## Route structure

A route contains both the compact pair representation and explicit leg semantics:

```json
{
  "id": "USDT>BTC>ETH>USDT",
  "triangle_id": "BTC-ETH-USDT",
  "start_asset": "USDT",
  "assets": ["USDT", "BTC", "ETH", "USDT"],
  "pair1": "BTCUSDT",
  "pair2": "ETHBTC",
  "pair3": "ETHUSDT",
  "legs": [
    {
      "symbol": "BTCUSDT",
      "from_asset": "USDT",
      "to_asset": "BTC",
      "side": "BUY",
      "base_asset": "BTC",
      "quote_asset": "USDT"
    }
  ]
}
```

## Rust loading

The `scanner` crate deserializes and validates the generated file before use. Validation checks:

- config version;
- route count;
- duplicate route IDs;
- exactly three distinct assets;
- return to the starting asset;
- leg continuity;
- pair fields matching leg symbols;
- BUY semantics: quote -> base;
- SELL semantics: base -> quote.

Validate the current file with:

```bash
cd rust
cargo run -p scanner --bin validate-triangles
```

The loader also exposes `required_symbols()`, which gives the later scanner/market-data
orchestration layer the exact symbol set that must have live order books.

## No profitability calculation yet

Phase 4 discovers route topology only. It does not claim that a route is profitable. Profitability
will require Phase 3 executable prices, fees, slippage, freshness checks, and later execution/risk
logic.
