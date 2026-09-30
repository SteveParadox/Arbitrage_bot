# Phase 11: Three-Leg Execution Coordinator

Phase 11 sits above the Phase 10 single-order execution client.

Its job is not merely to call three orders. It maintains a route-local asset ledger across the
entire triangular sequence and only advances after the current order has a terminal, confirmed
execution state.

## Sequence

```text
Opportunity
    |
    v
Leg 1 normal risk approval
    |
    v
execute + confirm fills
    |
    v
apply actual fill + actual fee currency
    |
    v
re-plan Leg 2 from actual net output
    |
    v
fresh normal risk approval
    |
    v
execute + confirm fills
    |
    v
apply actual fill + actual fee currency
    |
    v
Leg 3 risk-reducing close approval
    |
    v
execute + confirm fills
    |
    v
realized base P&L + residual valuation
```

The coordinator does not carry forward scanner-estimated quantity after a real fill.

## Actual quantity propagation

After each confirmed order, holdings are adjusted from the execution records.

For a BUY:

```text
input spent   = sum(execValue)
gross output  = filled base quantity
```

For a SELL:

```text
input spent   = filled base quantity
gross output  = sum(execValue)
```

Then every reported fee is subtracted from its actual fee currency.

So if Leg 1 was expected to produce:

```text
0.400 BTC
```

but execution produced:

```text
0.400 BTC
-0.010 BTC fee
-----------
0.390 BTC route holding
```

Leg 2 is planned from 0.390 BTC, not 0.400 BTC.

## Reauthorization policy

Leg 1 and Leg 2 use normal Phase 9 authorization.

This is intentionally stricter than reusing the original 100 ms approval token. After a fill:

- balances changed;
- exposure changed;
- market prices changed;
- liquidity changed;
- stale-data state may have changed.

The application layer implements `RiskIntentProvider` to supply fresh account/risk context for
each normal leg.

Leg 3 is different. Once Leg 2 has succeeded, the route is already holding the second
intermediate asset. Returning that asset to the base currency is risk reducing, so Leg 3 uses
the emergency-unwind approval class.

## Live-book planner

`LiveBookPlanner` uses the Rust local order-book engine plus exchange symbol rules.

BUY planning:

1. walk asks using the available quote budget;
2. calculate affordable base quantity;
3. round base quantity down to `qty_step`;
4. re-price the rounded quantity;
5. ensure the rounded quantity does not overspend the quote budget.

SELL planning:

1. round available base quantity down to `qty_step`;
2. walk bids for the rounded quantity.

Normal route planning requires the intended conversion to be visible in the book.

Emergency unwind may deliberately size down to the currently visible fillable quantity and retry.

## Emergency unwind paths

For a route:

```text
A -> B -> C -> A
```

the coordinator has deterministic direct recovery paths:

```text
B -> A = reverse of Leg 1
C -> A = Leg 3
```

This matters for partial Leg 2 fills because holdings may be split:

```text
some B remained unspent
+
some C was acquired
```

The coordinator unwinds C through Leg 3 and B through reverse Leg 1.

## Partial fills

A terminal partial fill is not treated as a completed leg.

The confirmed partial executions are first applied to the holdings ledger. Then the coordinator
starts emergency unwind from the resulting split exposure.

Example:

```text
after Leg 1:
0.40 BTC

Leg 2 requests:
0.80 ETH using 0.40 BTC

Leg 2 fills only:
0.40 ETH using 0.20 BTC

route holdings:
0.20 BTC
0.40 ETH

recovery:
0.40 ETH -> USDT
0.20 BTC -> USDT
```

## Unresolved order state

An unresolved exchange state is deliberately different from a terminal failed/partial order.

Examples:

- create acknowledgement received but monitoring fails;
- cancellation acknowledgement received but terminal status cannot be confirmed;
- fill history cannot be reconciled.

In that state the coordinator does **not** submit a blind opposite trade.

The original order may still fill later. Sending the opposite order could therefore create a new
position rather than remove one.

The coordinator instead:

1. stops the route;
2. engages the Phase 9 manual kill switch;
3. returns `HaltedUnresolved`;
4. requires exchange-state reconciliation.

## Emergency unwind authorization

Normal kill switches and circuit breakers stop new risk.

They do not block a short-lived emergency unwind token.

Emergency unwind still requires:

- known positive exposure;
- unwind notional no greater than known exposure;
- healthy/recent API heartbeat;
- healthy/recent exchange heartbeat;
- market data within the emergency freshness ceiling.

Defaults:

```text
normal market freshness       500 ms
emergency freshness ceiling  2000 ms
normal slippage tolerance     0.10%
emergency slippage tolerance  0.50%
unwind attempts per asset       3
base-value dust threshold      0.01
```

These are configuration defaults, not universal optimal values.

## Dust and residual holdings

Quantity-step rounding can leave small route-local residuals.

If positive non-base holdings exceed `max_dust_notional_base`, Phase 11 attempts cleanup.

Below the threshold they are reported as residual value instead of creating uneconomic cleanup
orders.

Signed residual value is included in economic P&L. This matters when a fee is charged in an
intermediate route asset and leaves a negative route-local balance delta.

If a fee is reported in an asset outside the triangle and the coordinator has no route-local
conversion for that asset, economic P&L is reported as unknown rather than silently ignoring the
fee.

## P&L

The report separates:

```text
final_base_amount
realized_base_pnl
signed residual value in base
economic_pnl
```

So:

```text
realized_base_pnl = final base - starting base

economic_pnl =
    realized_base_pnl
    + signed marked value of non-base route residuals
```

If exchange state is unresolved, economic P&L is intentionally left unknown.

## Coordinator outcomes

```text
Completed
CompletedWithResidualCleanup
RecoveredByUnwind
NotStarted
HaltedUnresolved
UnwindFailed
```

`RecoveredByUnwind` means the planned triangle did not finish normally, but known positive
intermediate exposure was converted back toward the base asset.

It does not imply the attempt was profitable.

## Current integration boundary

The coordinator contains:

- execution sequencing;
- actual fill propagation;
- route-local holdings;
- order-book planning;
- emergency unwind;
- residual cleanup;
- P&L accounting;
- unresolved-state protection.

The application layer must still provide `RiskIntentProvider`, because fresh account balance,
total account exposure, daily P&L, and remaining-route profitability are runtime account/strategy
data rather than constants the coordinator should invent.

## Test coverage

Phase 11 regression tests cover:

- actual post-fee Leg 1 quantity becoming the Leg 2 size;
- successful three-leg completion;
- terminal partial Leg 2;
- split BTC/ETH exposure after partial Leg 2;
- two-asset emergency unwind;
- unresolved accepted Leg 2;
- kill-switch engagement without blind unwind;
- BUY quantity planning and precision rounding.
