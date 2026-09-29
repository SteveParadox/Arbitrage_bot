# Phase 6: Fee and Profitability Engine

Phase 6 converts a gross three-leg route result into an expected net result.

## Reference model

Python is the reference implementation in:

```text
python/strategy/profitability.py
```

Rust mirrors the same model in:

```text
rust/scanner/src/profitability.rs
```

Both use decimal arithmetic for the financial model and canonicalize shared-test output to eight
decimal places.

## Formula

The scanner already walks live order-book depth in Phase 3, so current visible-book slippage is
already included in the gross final amount.

Phase 6 applies:

```text
gross profit
- compounded spot trading fees
- additional expected slippage allowance
- rounding-loss allowance
- latency buffer
- safety margin
= expected net profit
```

Trading fees are compounded because each leg's fee reduces the asset received and therefore the
quantity available to the next leg.

Other allowances are expressed in basis points of the starting capital.

## Default reference assumptions

`shared/config/profitability.json` currently uses:

```text
3 x taker fee        10 bps each
extra slippage        5 bps
rounding loss          0 bps
latency buffer         3 bps
safety margin          5 bps
```

The 10 bps per-leg fee is a reference for standard Bybit crypto-spot VIP 0 taker pricing. Actual
account fees can differ by VIP tier and region, so this value must be configured to the account
before any future execution phase.

The rounding allowance is supported now but defaults to zero until exact per-symbol quantity-step
rounding is fed into the scanner. It can be raised conservatively in configuration meanwhile.

## Example

With:

```text
start amount       450.000 USDT
gross final        452.745 USDT
gross edge           0.610%
```

the default assumptions produce approximately:

```text
gross edge                 61.0000 bps
nominal fee schedule      -30.0000 bps
extra slippage             -5.0000 bps
latency                    -3.0000 bps
safety margin              -5.0000 bps
---------------------------------------
expected net               17.8472 bps
                           0.17847%
```

The actual fee cost measured against starting capital is slightly above 30 bps in this example
because the three fees are compounded against the gross route value.

## Scanner integration

Completed Phase 5 scans now include:

- expected net profit;
- expected net return percentage;
- expected net return basis points;
- expected final amount;
- net-profitable flag;
- full cost breakdown.

Incomplete scans still record their failure state but do not fabricate a net result.

## Cross-language parity

Shared cases live at:

```text
shared/tests/profitability_cases.json
```

Python and Rust both evaluate those inputs. The parity runner executes both implementations and
requires their canonical JSON output to be exactly identical:

```bash
python scripts/check_profitability_parity.py
```

This is also included as a CI job.

## No execution

Profitability is still observational. A positive expected net result does not place an order.
