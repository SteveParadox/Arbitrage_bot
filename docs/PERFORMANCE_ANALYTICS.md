# Phase 17: Performance Analytics

Phase 17 answers a more useful question than "did the scanner find opportunities?":

> Where does expected profit disappear between observation and realized execution?

The implementation is Python-first and reads existing PostgreSQL data from Phases 7, 13, and 16.

## Endpoint

```text
GET /analytics/performance
```

Query parameters:

```text
days        1..90, default 7
base_asset  default USDT
bins        5..30, default 10
```

Example:

```text
GET /analytics/performance?days=7&base_asset=USDT&bins=10
```

No new database migration is required. Phase 17 uses existing opportunity, micro-live, and engine
event tables.

## Core profit metrics

### Profit per cycle

```text
sum(realized P&L with known outcome)
-----------------------------------
number of cycles with known P&L
```

Known outcomes include terminal Rust engine trades and reconciled Phase 13 canary cycles.

If the same trade ID exists in both sources, the engine terminal event wins and the canary row is
excluded from the combined total.

### Average profit per day

```text
total realized P&L / selected window days
```

The API also returns a UTC calendar-day series. Days with no realized profit are explicitly
returned as zero instead of disappearing from the chart.

### Profit per $1,000 turnover

Exact exchange turnover is not reconstructable from every historical record currently stored.

Phase 17 therefore labels this as an estimate.

For engine terminal events:

```text
estimated turnover = starting capital × completed leg count
```

For reconciled three-leg micro-canary cycles:

```text
estimated turnover = starting capital × 3
```

Then:

```text
profit per $1,000 turnover
=
total realized P&L / estimated turnover × 1,000
```

This is a strategy-efficiency estimate, not an accounting statement or exchange-volume report.

## Funnel

The funnel is:

```text
Observed opportunity
        ↓
Expected profitable opportunity
        ↓
Trade attempted
        ↓
Actual profit known
```

### Observed opportunity

Count of distinct Phase 7 `opportunity_windows` beginning inside the selected period.

This deliberately avoids treating every order-book update as a brand-new economic opportunity.

### Expected profitable opportunity

Count of those windows whose maximum expected net profit is positive.

Expected-profit dollars are:

```text
sum(max expected net profit per opportunity window)
```

This measures the best modeled edge observed within each opportunity lifetime.

### Trade attempted

Count of distinct selected-base-asset terminal Rust route events plus reconciled micro-canary trade
IDs.

Order-level events with no terminal route event are not silently assigned to USDT or another base
asset because the order event alone does not contain enough route context. Those orphan IDs are
reported separately under `data_quality`.

### Actual profit

Count and total realized P&L for attempted cycles where economic/realized P&L is known.

## Net-edge distribution

Source:

```text
opportunity_observations.net_edge_bps
```

The response provides:

```text
count
sample_count
min
P25
median
P75
P95
max
mean
histogram bins
```

## Win/loss distribution

Known realized outcomes are classified as:

```text
win        P&L > 0
loss       P&L < 0
breakeven  P&L = 0
unknown    attempt exists but P&L is unavailable
```

Win rate uses only resolved P&L outcomes.

## Opportunity survival time

Source:

```text
opportunity_windows.duration_ms
```

Only closed opportunity windows enter the duration distribution because open windows are
right-censored. The response reports the number of currently open windows separately.

## Latency distribution

New terminal Rust `trade.executed` and `trade.failed` events now contain:

```text
execution_time_ms
```

measured around the complete coordinator route attempt.

Reconciled Phase 13 cycles already contain `execution_time_ms` and are included when their trade ID
is not already represented by an engine event.

## Slippage distribution

Current actual slippage comes from reconciled Phase 13 canary cycles:

```text
micro_live_cycles.actual_slippage_bps
```

The engine event stream does not yet expose enough planned-vs-filled per-leg data to reconstruct
historical live slippage without guesswork, so Phase 17 does not fabricate it.

## Sampling

Distribution queries can involve many individual observations. The setting:

```env
ARB_PERFORMANCE_SAMPLE_LIMIT=100000
```

caps in-memory rows used to build histograms and quantiles.

The effective limit is clamped between 1,000 and 1,000,000.

Aggregate opportunity counts and expected-profit sums remain database aggregates. Response
`data_quality` fields indicate when a distribution is sampled.

## Dashboard

The TypeScript dashboard refreshes core operational telemetry every five seconds.

Phase 17 analytics refresh every 30 seconds because distribution queries are heavier.

The new section includes:

```text
Profit per cycle
Average profit per day
Profit per $1,000 estimated turnover
Total realized P&L

Observed → Expected → Attempted → Actual funnel

Net-edge histogram
Opportunity-survival histogram
Execution-latency histogram
Actual-slippage histogram
Win/loss summary
Daily realized P&L
```

## Data-quality signals

The API explicitly reports:

```text
sample limit
sampled distribution flags
engine terminal event count
micro-canary cycle count
deduplicated canary IDs
orphan order attempts without terminal route events
```

An orphan order attempt is not automatically counted in the selected-base-asset funnel because
doing so would invent route context that is not present in the event.

## Interpretation

The useful comparison is not just expected versus actual P&L.

For example:

```text
1,000 distinct opportunities observed
  120 had positive modeled profit
   18 reached a trade attempt
   16 produced known realized P&L
```

That immediately separates:

```text
scanner abundance
from
risk/execution selectivity
from
execution completion
from
realized economics
```

If expected-profit opportunities are plentiful but attempts are scarce, the bottleneck is before
execution.

If attempts are frequent but actual P&L materially underperforms expected P&L, fees, latency,
slippage, rounding, fill behavior, or opportunity decay deserve investigation.

That is the point of Phase 17: stop asking whether the bot "finds opportunities" and measure where
the money actually goes.
