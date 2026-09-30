# Phase 8: Paper Trading Simulator

Phase 8 replays accepted arbitrage opportunities against the **future order-book states that
actually arrived after detection**. It does not reuse the detection-time book and call that
latency testing.

## Execution timeline

For a latency scenario `L`:

```text
detection at t0
   |
   + L -> leg 1 uses book at or immediately before t0 + L
   |
   + L -> leg 2 uses book at or immediately before t0 + 2L
   |
   + L -> leg 3 uses book at or immediately before t0 + 3L
```

The default scenarios are loaded from `shared/config/paper-simulation.json`:

```text
25 ms
50 ms
100 ms
200 ms
500 ms
```

Every leg walks the archived depth at its delayed timestamp. A partial book cannot magically fill
a full order.

## Historical book archive

Phase 7 stored opportunity observations but not the future market states required by replay.
Phase 8 adds `market_book_events` to PostgreSQL and a capture ingester for normalized Rust
`order_book` events.

For lowest distortion, capture the raw market-data stream while the scanner is running and ingest
it afterward. On macOS/Linux:

```bash
mkdir -p data/market

cargo run --manifest-path rust/Cargo.toml -p market-data \
  | tee data/market/market_data.ndjson \
  | cargo run --manifest-path rust/Cargo.toml -p scanner --bin scan-live \
  | (cd python && python -m analytics.opportunity_ingest)
```

Then archive the captured book stream:

```bash
cd python
python -m simulator.book_ingest --file ../data/market/market_data.ndjson
```

The ingester is idempotent.

A live passthrough mode also exists:

```bash
python -m simulator.book_ingest --passthrough
```

but synchronous database work in a hot market-data pipe can itself add latency, so captured-file
ingestion is preferable for latency experiments. Humans finally get punished for measuring
latency by adding latency to the measurement.

## Replay model

The replay engine reconstructs local books from Bybit snapshots and deltas.

To avoid materializing a full deep-book copy for every update, each symbol stores periodic
checkpoints. A random historical lookup restores the nearest checkpoint and replays only the
remaining deltas.

Large experiments are processed in opportunity chunks, but reconstruction can extend back to an
old snapshot. A 100,000-event budget per chunk marks oversized symbol histories as
`history_limit_exceeded`; it never silently truncates a book. Persistent periodic checkpoints and
streaming result aggregation are still needed: all selected opportunities/results remain in
memory, and event-count limits are not strict byte limits.

Default checkpoint interval:

```text
100 events
```

Books older than the configured maximum age at a simulated execution timestamp cause a
`stale_book_state` failure rather than silently using obsolete prices.

## Costs during simulated execution

Each simulated leg applies the configured Phase 6 trading fee to the asset received before that
quantity becomes the input to the next leg.

The simulator does **not** subtract the Phase 6 latency or safety buffers as realized costs. Those
are prediction allowances. Instead it measures the actual effect of the delayed book replay.

The configured rounding-loss allowance is a prediction assumption, not a replayed cash charge.
Exact exchange quantity rounding is NOT modeled. Run metadata records this limitation; simulated
profit must not be described as exchange-accurate realized P&L.

## Metrics

For every opportunity × latency scenario the simulator records:

- Phase 6 expected profit;
- simulated final amount;
- actual simulated profit;
- simulated net edge;
- fill ratio;
- completion/failure;
- failure leg and reason;
- per-leg delayed timestamp;
- delayed average execution price;
- fee quantity;
- per-leg adverse slippage versus the detected average price;
- aggregate adverse/favorable price drift from the three delayed leg prices, excluding fees;
- expected-vs-simulated profit error;
- total opportunity-window lifetime;
- lifetime remaining when that specific detection occurred;
- whether the three-leg sequence lasted longer than the lifetime remaining at detection;
- maximum age of any book used.

Aggregate metrics per latency include:

- attempts;
- fills;
- fill rate;
- failure rate;
- average/total expected profit;
- average/total simulated profit;
- average execution drift;
- average opportunity lifetime;
- average lifetime remaining at detection;
- outlived-opportunity count/rate;
- simulated-profitable count/rate;
- average expected-vs-simulated profit error;
- failure-reason distribution.

## Run thousands of opportunities

After migrations and historical capture:

```bash
cd python
alembic upgrade head

python -m simulator.paper_trade \
  --hours 24 \
  --limit 10000 \
  --latencies 25,50,100,200,500
```

That produces up to 50,000 replay scenarios for 10,000 opportunities.

By default only Phase 7 `accepted` opportunities are simulated. Use `--include-rejected` to
replay every structurally executable observation.

## Database tables

`paper_simulation_runs` stores run configuration and aggregate summary.

`paper_simulation_results` stores every opportunity/latency result.

## Analytics API

```text
GET /analytics/paper-trading/runs
GET /analytics/paper-trading/runs/{run_id}
GET /analytics/paper-trading/runs/{run_id}/failures
```

No order is submitted at any point.


### Windows capture

In PowerShell, capture the raw market-data stream with `Tee-Object` before later ingestion:

```powershell
cargo run -p market-data |
  Tee-Object -FilePath ..\data\market\market_data.ndjson |
  cargo run -p scanner --bin scan-live
```

Keep the opportunity ingester attached to the scanner output in the process layout you use for
forward testing. The important requirement is that the raw `order_book` stream is captured
without putting synchronous PostgreSQL writes between the Rust feed and Rust scanner.
